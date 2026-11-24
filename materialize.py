"""Bring the bytes a virtual store points at into the store, each range once.

  python materialize.py <store root> [--array NAME ...] [--sel DIM=LO:HI ...]
                        [--plan] [--compare] [--target public] [--base DIR]
                        [--jobs 4] [--gap KB] [--most MB] [--batch MB]

A virtual store is rows of path, offset, size. Reading it for real work
means fetching those ranges from wherever they are, every time. To
materialize is to fetch them once and keep them: the bytes go into packs in
this store and the rows are rewritten to point at the packs, as one commit a
batch. After it, a read of what was materialized asks the source for nothing.

The obvious way to do this (loop over the chunks wanted, fetch each) asks
for the same bytes again wherever two chunks point at the same range, and
makes one request a chunk where neighbours could go as one. So this plans
from the rows, not the chunks:

  1. the rows of the selection that are not in a pack already;
  2. the distinct (path, offset, size) among them: a range many chunks
     point at is fetched once;
  3. neighbouring ranges of one file joined into one request, when the gap
     between them is at most --gap (32 KB) and the whole at most --most (16 MB);
  4. each request made once, cut back into its ranges, packed (equal bytes
     share one range of the pack), and every row that named a range rewritten.

The plan is known before any byte moves: --plan prints it and stops.

Selection: --array (repeatable; default every referenced data array) and
--sel DIM=LO:HI in cells, half open, applied to each array that has DIM
(--sel time=0:31 --sel yt_ocean=100:400). Whole chunks are kept: a
selection picks the chunks it touches. Chunks keep the source's shape and
encoding, byte for byte; a different chunking or a subset inside a chunk is
a derivation (derive_levels.py is one), which also reads each source chunk
once.

The other mode is in the library: Keeper(store, ...).read(array, key)
returns a chunk's bytes, from a pack if it is held, else from the source,
and then holds it: fetched on first read, kept from then on. Its rows are
committed every --batch megabytes and on close().

--compare also times the obvious way on the same selection (one request a
chunk row, nothing written), for the difference. Source bytes are fetched as
derive_bottom.py fetches them (--target names the remap rule); --jobs is
capped at 10, which NCI's THREDDS is thought to take.

What is reported: rows, distinct ranges, requests; bytes named by the rows,
bytes of distinct ranges, bytes asked for (with gaps), bytes packed; seconds.
"""
import argparse, hashlib, io, json, os, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import brstore

GAP, MOST, BATCH = 32 * 1024, 16 * 2**20, 256 * 2**20


def source_reader(table, target="public", base=""):
    """read(path, offset, size) for a source file as the rows name it, after
    the table's remap rule for target. Counts what it is asked for."""
    stats = {"requests": 0, "bytes": 0}

    def remap(path):
        for r in table.get("remap", []):
            if r["target"] == target and path.startswith(r["prefix"]):
                path = r["replacement"] + path[len(r["prefix"]):]
        return path if "://" in path or os.path.isabs(path) else os.path.join(base, path)

    def read(path, offset, size):
        loc = remap(path)
        if "://" in loc:
            req = urllib.request.Request(loc, headers={"Range": f"bytes={offset}-{offset + size - 1}"})
            data = urllib.request.urlopen(req, timeout=300).read()
        else:
            with open(loc, "rb") as f:
                f.seek(offset)
                data = f.read(size)
        if len(data) != size:
            raise IOError(f"short read of {path} at {offset}: {len(data)} of {size} bytes")
        stats["requests"] += 1
        stats["bytes"] += size
        return data

    read.stats = stats
    return read


def chunk_ranges(a, sel):
    """For each dimension of array a, the [lo, hi) range of chunk indexes the
    selection touches (sel: {dim: (lo, hi)} in cells), or None for all."""
    out = []
    for d, n, c in zip(a["dims"], a["shape"], a["chunks"]):
        if d in sel:
            lo, hi = max(0, sel[d][0]), min(n, sel[d][1])
            out.append((lo // c, -(-hi // c)) if hi > lo else (0, 0))
        else:
            out.append(None)
    return out


def planned_rows(s, m, arrays, sel):
    """Tables of the selection's rows that name a range outside the packs, one
    per fragment, each with an 'array' column. Nothing is fetched."""
    for a in arrays:
        nd = len(a["shape"])
        rng = chunk_ranges(a, sel)
        for f in s.frags(m, a["array"]):
            if rng[0] is not None and (f["hi"] <= rng[0][0] or f["lo"] >= rng[0][1]):
                continue
            t = pq.read_table(io.BytesIO(s.o.get(f["path"])))
            keep = pc.and_(pc.equal(t["present"], 1), pc.is_valid(t["offset"]))
            keep = pc.and_(keep, pc.invert(pc.starts_with(pc.fill_null(t["path"], ""), "packs/")))
            for d in range(nd):
                if rng[d] is not None:
                    keep = pc.and_(keep, pc.and_(pc.greater_equal(t[f"dim_{d}"], rng[d][0]), pc.less(t[f"dim_{d}"], rng[d][1])))
            t = t.filter(keep)
            if t.num_rows:
                yield a["array"], nd, t


def runs_of(ranges, gap=GAP, most=MOST):
    """ranges: distinct (path, offset, size). Requests: (path, offset, size,
    [the ranges inside]), neighbours of one path joined."""
    out = []
    for path, off, size in sorted(ranges):
        r = out[-1] if out else None
        if r and r[0] == path and off - (r[1] + r[2]) <= gap and max(r[1] + r[2], off + size) - r[1] <= most:
            r[2] = max(r[1] + r[2], off + size) - r[1]
            r[3].append((path, off, size))
        else:
            out.append([path, off, size, [(path, off, size)]])
    return out


def batches(s, m, arrays, sel, batch=BATCH):
    """The selection's rows in the batches they are fetched and committed in:
    (tables, the distinct ranges not in an earlier batch). A batch is closed
    once its new ranges reach batch bytes, so memory is bounded by that and
    by the largest fragment."""
    known, tables, new, size = set(), [], set(), 0
    for item in planned_rows(s, m, arrays, sel):
        t = item[2]
        for r in set(zip(t["path"].to_pylist(), t["offset"].to_pylist(), t["size"].to_pylist())):
            if r not in known:
                known.add(r)
                new.add(r)
                size += r[2]
        tables.append(item)
        if size >= batch:
            yield tables, new
            tables, new, size = [], set(), 0
    if tables:
        yield tables, new


def plan(s, m, arrays, sel, gap=GAP, most=MOST, batch=BATCH):
    """The cost of materializing a selection, from the rows alone."""
    out = {"rows": 0, "distinct_ranges": 0, "requests": 0, "commits": 0}
    named = distinct = asked = 0
    files = set()
    for tables, new in batches(s, m, arrays, sel, batch):
        for _, _, t in tables:
            out["rows"] += t.num_rows
            named += pc.sum(t["size"]).as_py()
        runs = runs_of(new, gap, most)
        out["distinct_ranges"] += len(new)
        out["requests"] += len(runs)
        out["commits"] += 1
        distinct += sum(r[2] for r in new)
        asked += sum(r[2] for r in runs)
        files |= {r[0] for r in new}
    out.update(files=len(files), mb_named_by_rows=round(named / 2**20, 3), mb_distinct=round(distinct / 2**20, 3),
               mb_to_ask_for=round(asked / 2**20, 3), bytes_to_ask_for=asked)
    return out


class Packer:
    """Ranges fetched, waiting to be committed: one pack and the rows that
    point into it. Equal bytes share one range of the pack. It remembers
    where the ranges of its earlier commits went, so a later row that names
    one of those is pointed there and nothing is fetched for it."""

    def __init__(self, store, writer="materialize"):
        self.s, self.writer = store, writer
        self.done = {}  # range -> (pack, offset, size), committed
        self._clear()

    def _clear(self):
        self.buf, self.seen, self.where, self.rows = bytearray(), {}, {}, []

    def knows(self, rng):
        return rng in self.where or rng in self.done

    def add(self, rng, data):
        h = hashlib.sha256(data).digest()
        if h not in self.seen:
            self.seen[h] = (len(self.buf), len(data))
            self.buf += data
        self.where[rng] = self.seen[h]

    def point(self, array, key, rng, info):
        self.rows.append((array, tuple(key), rng, info))

    def commit(self, message):
        """Write the pack, rewrite the rows, commit: (version, bytes packed), or None."""
        if not self.rows:
            return None
        data, name = bytes(self.buf), None
        txn = self.s.begin(writer=self.writer)
        if data:
            digest = brstore.sha256(data)
            name = f"packs/{digest}.pack"
            self.s.o.put_new(name, data)
            txn.put_source(name, len(data), None, "sha256:" + digest)
        for rng, (off, size) in self.where.items():
            self.done[rng] = (name, off, size)
        for array, key, rng, info in self.rows:
            pack, off, size = self.done[rng]
            txn.put_ref(array, key, pack, off, size, info)
        v = txn.commit(message.format(rows=len(self.rows)))
        self._clear()
        return v, len(data)


def materialize(s, arrays, sel, read, jobs=4, gap=GAP, most=MOST, batch=BATCH):
    """Fetch each distinct range of the selection once and commit it to the
    store, a batch at a time. Returns what it did."""
    m = s.manifest(s.latest())
    out = {"from_version": m["version"], "rows": 0, "distinct_ranges": 0, "requests": 0, "bytes_named_by_rows": 0,
           "bytes_asked_for": 0, "bytes_packed": 0, "versions": []}
    packer = Packer(s)
    message = (f"materialize: {{rows}} rows of {', '.join(sorted(a['array'] for a in arrays))} from version {m['version']}, "
               f"their bytes now in packs")

    def flush():
        got = packer.commit(message)
        if got:
            out["versions"].append(got[0])
            out["bytes_packed"] += got[1]

    t0 = time.time()
    with ThreadPoolExecutor(jobs) as pool:
        for tables, new in batches(s, m, arrays, sel, batch):
            runs = runs_of(new, gap, most)
            out["distinct_ranges"] += len(new)
            out["requests"] += len(runs)
            for run, data in zip(runs, pool.map(lambda r: read(r[0], r[1], r[2]), runs)):
                out["bytes_asked_for"] += run[2]
                for rng in run[3]:
                    packer.add(rng, data[rng[1] - run[1] : rng[1] - run[1] + rng[2]])
            for array, nd, t in tables:
                cols = [t[f"dim_{d}"].to_pylist() for d in range(nd)]
                paths, offs, sizes, infos = (t[c].to_pylist() for c in ("path", "offset", "size", "info"))
                out["rows"] += t.num_rows
                out["bytes_named_by_rows"] += sum(sizes)
                for i in range(t.num_rows):
                    packer.point(array, tuple(c[i] for c in cols), (paths[i], offs[i], sizes[i]), infos[i])
            flush()
    out["seconds"] = round(time.time() - t0, 2)
    return out


def naive(s, arrays, sel, read, jobs=4):
    """The obvious way, for comparison: one request for every chunk row,
    nothing shared, nothing written."""
    m = s.manifest(s.latest())
    n = b = 0
    t0 = time.time()
    with ThreadPoolExecutor(jobs) as pool:
        for _, _, t in planned_rows(s, m, arrays, sel):
            asks = list(zip(t["path"].to_pylist(), t["offset"].to_pylist(), t["size"].to_pylist()))
            for data in pool.map(lambda r: read(*r), asks):
                n += 1
                b += len(data)
    return {"requests": n, "bytes_asked_for": b, "seconds": round(time.time() - t0, 2)}


class Keeper:
    """Chunks fetched on first read and kept. read(array, key) gives the
    chunk's stored bytes: from a pack if the store holds it, else from the
    source, after which the store holds it (committed every batch bytes and
    on close). A range another key points at is not fetched for that key."""

    def __init__(self, store, read, batch=BATCH):
        self.s, self.source, self.batch = store, read, batch
        self.m = store.manifest(store.latest())
        self.packer = Packer(store, writer="keeper")
        self.bytes = {}  # range -> bytes fetched and not yet committed
        self.pending = set()
        self.stats = {"reads": 0, "from_packs": 0, "from_source": 0, "shared": 0, "versions": []}

    def read(self, array, key):
        key = tuple(key)
        self.stats["reads"] += 1
        row = self.s.chunk_row(self.m, array, key)
        if row is None or not row["present"]:
            return None
        if row["offset"] is None:
            return row["data"]
        if row["path"].startswith("packs/"):
            self.stats["from_packs"] += 1
            return self.s.o.get(row["path"], row["offset"], row["size"])
        rng = (row["path"], row["offset"], row["size"])
        if rng in self.bytes:
            self.stats["shared"] += 1
            data = self.bytes[rng]
        elif rng in self.packer.done:  # another key's read put this range in a pack
            self.stats["shared"] += 1
            data = self.s.o.get(*self.packer.done[rng])
        else:
            data = self.bytes[rng] = self.source(*rng)
            self.packer.add(rng, data)
            self.stats["from_source"] += 1
        if (array, key) not in self.pending:
            self.pending.add((array, key))
            self.packer.point(array, key, rng, row["info"])
        if len(self.packer.buf) >= self.batch:
            self.flush()
        return data

    def flush(self):
        got = self.packer.commit(f"keeper: {{rows}} chunks read through version {self.m['version']}, their bytes now in packs")
        if got:
            self.stats["versions"].append(got[0])
            self.m = self.s.manifest(self.s.latest())
        self.bytes, self.pending = {}, set()

    close = flush

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.flush()


def parse_sel(items):
    sel = {}
    for it in items or []:
        d, _, r = it.partition("=")
        lo, _, hi = r.partition(":")
        sel[d] = (int(lo or 0), int(hi) if hi else 2**62)
    return sel


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--array", action="append")
    ap.add_argument("--sel", action="append", help="DIM=LO:HI in cells, half open")
    ap.add_argument("--plan", action="store_true", help="print the plan and stop")
    ap.add_argument("--compare", action="store_true", help="also time one request a chunk row, writing nothing")
    ap.add_argument("--target", default="public")
    ap.add_argument("--base", default="")
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--gap", type=float, default=GAP / 1024, help="KB between neighbours joined into one request")
    ap.add_argument("--most", type=float, default=MOST / 2**20, help="MB in one request at most")
    ap.add_argument("--batch", type=float, default=BATCH / 2**20, help="MB in one pack and commit")
    A = ap.parse_args(argv)
    jobs = max(1, min(A.jobs, 10))  # NCI THREDDS: ten at once is thought safe, not yet tested in detail
    gap, most, batch = int(A.gap * 1024), int(A.most * 2**20), int(A.batch * 2**20)

    s = brstore.Store(A.root)
    m = s.manifest(s.latest())
    table = m["table"]
    arrays = [a for a in table["arrays"] if a["storage"] == "referenced" and (a["array"] in A.array if A.array else a["kind"] == "data")]
    if not arrays:
        sys.exit("no referenced array" + (f" named {', '.join(A.array)}" if A.array else ""))
    sel = parse_sel(A.sel)
    unknown = [d for d in sel if not any(d in x["dims"] for x in arrays)]
    if unknown:  # a misspelt dimension must not turn a small selection into the whole store
        sys.exit(f"no selected array has a dimension named {', '.join(unknown)}; they have {sorted({d for x in arrays for d in x['dims']})}")
    p = plan(s, m, arrays, sel, gap, most, batch)
    print(f"version {m['version']}: {', '.join(a['array'] for a in arrays)}" + (f", {A.sel}" if A.sel else ""))
    print(json.dumps({"plan": p}, indent=1))
    if A.plan or not p["rows"]:
        if not p["rows"]:
            print("nothing to fetch: the selection's rows are in packs already (or it has none)")
        return 0
    out = {"plan": p}
    if A.compare:
        out["one_request_a_row"] = naive(s, arrays, sel, source_reader(table, A.target, A.base), jobs)
    r = materialize(s, arrays, sel, source_reader(table, A.target, A.base), jobs, gap, most, batch)
    r.update({k.replace("bytes_", "mb_"): round(r.pop(k) / 2**20, 3) for k in [k for k in r if k.startswith("bytes_")]})
    out["materialized"] = r
    print(json.dumps(out, indent=1))
    print(f"committed version{'s' if len(r['versions']) > 1 else ''} {', '.join(map(str, r['versions']))}: "
          f"{r['rows']} rows now point at packs in this store")
    return 0


if __name__ == "__main__":
    sys.exit(main())
