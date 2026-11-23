"""Replay a real published byterefs export as a history, and prove the rows.

  python real_rows.py <export_dir> <workdir> [--commits N] [--tail N] [--root s3://bucket/prefix]

<export_dir> holds arrays.json and refs_<array>.parquet: the flat refs
store raad.virtualize renders to work/<dataset>/out/flat and publishes as
<dataset>/flat/. It may be an https URL of a published one, which is
fetched first.

A small table (to 2 million rows): the first half of dim_0 is loaded as
version 1 and the rest appended in up to --commits commits (default 60).
A larger one, or with --tail N: everything but the last N steps of dim_0
(default 60) is loaded as version 1, then one commit per step, which is
how a daily store grows. Every array is in each commit.

Then the latest version is compared with the export: by SQL EXCEPT both
ways for a small table, by row count and a sum of row hashes per array
for a large one.

The bulk load streams: tables are read in batches and written as
fragments of about --frag-rows rows, so memory does not grow with the
table. Only the rows of the appended steps are held as Python objects.
"""
import argparse, json, os, shutil, statistics, time

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import brstore
from brstore import Store

SMALL = 2_000_000
ap = argparse.ArgumentParser()
ap.add_argument("export"), ap.add_argument("work")
ap.add_argument("--commits", type=int, default=60)
ap.add_argument("--tail", type=int)
ap.add_argument("--frag-rows", type=int, default=200_000)
ap.add_argument("--root")
A = ap.parse_args()
if A.export.startswith(("http://", "https://")):
    # a published flat refs store: fetch arrays.json, then each referenced array's table
    import urllib.request

    base, dest = A.export.rstrip("/"), os.path.join(os.path.abspath(A.work), "export")
    os.makedirs(dest, exist_ok=True)

    def fetch(name, required=True):
        try:
            urllib.request.urlretrieve(f"{base}/{name}", os.path.join(dest, name))
        except Exception as e:  # noqa
            if required:
                raise SystemExit(f"cannot fetch {base}/{name}: {e}")

    fetch("arrays.json")
    for a in json.load(open(os.path.join(dest, "arrays.json")))["arrays"]:
        if a["storage"] == "referenced":
            fetch(f"refs_{a['array']}.parquet")
    fetch("sources.parquet", required=False)
    A.export = dest
table = json.load(open(os.path.join(A.export, "arrays.json")))
root = A.root or os.path.join(os.path.abspath(A.work), "real-store")
if not A.root:
    shutil.rmtree(root, ignore_errors=True)
os.makedirs(A.work, exist_ok=True)
names = [a["array"] for a in table["arrays"] if a["storage"] == "referenced"]
nd = {a["array"]: len(a["shape"]) for a in table["arrays"]}
src = {n: os.path.join(A.export, f"refs_{n}.parquet") for n in names}
nrows = {n: pq.ParquetFile(src[n]).metadata.num_rows for n in names}
total = sum(nrows.values())
flat = sum(os.path.getsize(p) for p in src.values())
con = duckdb.connect()
con.execute(f"SET temp_directory='{os.path.join(os.path.abspath(A.work), 'duckdb-tmp')}'")
steps = [r[0] for r in con.execute("SELECT DISTINCT dim_0 FROM read_parquet(?) ORDER BY 1", [list(src.values())]).fetchall()]
large = total > SMALL or A.tail is not None
if large:
    ntail = min(A.tail or 60, len(steps) - 1)
    split, per = steps[-ntail], 1
else:
    split = steps[len(steps) // 2]
    per = max(1, -(-sum(k >= split for k in steps) // A.commits))
tail = [k for k in steps if k >= split]
print(f"{len(names)} arrays, {total} rows, {len(steps)} steps of dim_0; version 1 is steps below {split}, "
      f"then {-(-len(tail) // per)} commits of {per} step(s)", flush=True)


def norm(t, schema):
    """An export table in the store's column types and order."""
    cols = [t[f.name].cast(f.type) if f.name in t.schema.names else pa.nulls(t.num_rows, f.type) for f in schema]
    return pa.table(cols, schema=schema)


class Unsorted(Exception):
    pass


def bulk(s, path, ndim, ps, split):
    """Stream one array's rows below split into fragments of ps steps each."""
    schema, keys = brstore.refs_schema(ndim), [(f"dim_{i}", "ascending") for i in range(ndim)]
    out, carry, top = [], None, -1

    def emit(t):
        if t.num_rows == 0:
            return
        p = int(t["dim_0"][0].as_py()) // ps
        out.append(dict(lo=p * ps, hi=min((p + 1) * ps, split), path=s.put_frag(t.sort_by(keys), schema), rows=t.num_rows))

    for b in pq.ParquetFile(path).iter_batches(batch_size=1 << 18):
        t = norm(pa.Table.from_batches([b]), schema)
        d = t["dim_0"].to_numpy()
        if len(d) == 0:
            continue
        if d[0] < top or (np.diff(d) < 0).any():
            raise Unsorted(path)
        top = int(d[-1])
        t = t.filter(pc.less(t["dim_0"], split))
        if carry is not None:
            t = pa.concat_tables([carry, t])
        if t.num_rows == 0:
            continue
        part = t["dim_0"].to_numpy() // ps
        cuts = np.flatnonzero(np.diff(part)) + 1
        edges = [0, *cuts.tolist(), len(part)]
        for i, j in zip(edges[:-2], edges[1:-1]):  # every part but the last, which may continue
            emit(t.slice(i, j - i))
        carry = t.slice(edges[-2]).combine_chunks()
    if carry is not None:
        emit(carry)
    return out


srcs = []
sp = os.path.join(A.export, "sources.parquet")
if os.path.exists(sp):
    keep = ("source", "size", "mtime", "etag")
    srcs = [{k: r.get(k) for k in keep} for r in pq.read_table(sp).to_pylist()]
    for r in srcs:
        r["mtime"] = None if r["mtime"] is None else int(r["mtime"])

s = Store(root)
t0 = time.perf_counter()
refs = {}
for n in names:
    ps = per if not large else max(1, A.frag_rows // max(1, nrows[n] // len(steps)))
    try:
        refs[n] = bulk(s, src[n], nd[n], ps, split)
    except Unsorted:
        # not in dim_0 order: sort it once, out of core, and stream that
        tmp = os.path.join(os.path.abspath(A.work), f"sorted_{n}.parquet")
        order = ", ".join(f"dim_{i}" for i in range(nd[n]))
        con.execute(f"COPY (SELECT * FROM read_parquet('{src[n]}') ORDER BY {order}) TO '{tmp}' (FORMAT parquet)")
        refs[n] = bulk(s, tmp, nd[n], ps, split)
        os.remove(tmp)
    print(f"   {n}: {sum(f['rows'] for f in refs[n])} rows in {len(refs[n])} fragments of {ps} step(s)", flush=True)
brstore.init_fragments(s, table, refs, srcs)
out = dict(export=A.export, arrays=len(names), rows=total, flat_refs_mb=round(flat / 1e6, 2), dim_0_steps=len(steps),
           bulk_rows=sum(f["rows"] for v in refs.values() for f in v), bulk_s=round(time.perf_counter() - t0, 2),
           steps_per_commit=per)

by = {n: {} for n in names}
for n in names:
    for r in norm(pq.read_table(src[n], filters=[("dim_0", ">=", split)]), brstore.refs_schema(nd[n])).to_pylist():
        by[n].setdefault(r["dim_0"], []).append(r)
times, appended = [], 0
for i in range(0, len(tail), per):
    t0 = time.perf_counter()
    t = s.begin("append")
    for n in names:
        for k0 in tail[i : i + per]:
            for r in by[n].get(k0, []):
                key = tuple(r[f"dim_{j}"] for j in range(nd[n]))
                appended += 1
                if r["present"]:
                    t.rows[(n, key)] = dict(present=r["present"], path=r["path"], offset=r["offset"], size=r["size"], info=r["info"], data=r["data"])
                else:
                    t.put_absent(n, key)
    t.commit(f"dim_0 {tail[i]} to {tail[min(i + per, len(tail)) - 1]}")
    times.append(time.perf_counter() - t0)
out["commits"] = len(times)
out["rows_per_commit"] = round(appended / max(1, len(times)))
out["commit_s_median"] = round(statistics.median(times), 4) if times else None
out["commit_s_max"] = round(max(times), 4) if times else None

m = s.manifest(s.latest())
bad = 0
for n in names:
    a, b = s.frag_paths(m, n), [src[n]]
    if large:
        cols = ", ".join([f"dim_{i}::BIGINT" for i in range(nd[n])] + ["present::INT", "path", '"offset"::BIGINT', "size::BIGINT", "info"])
        q = f"SELECT count(*), sum(hash({cols})::HUGEINT) FROM read_parquet(?)"
        bad += con.execute(q, [a]).fetchone() != con.execute(q, [b]).fetchone()
    else:
        q = "SELECT count(*) FROM (SELECT * EXCLUDE (data) FROM read_parquet(?) EXCEPT SELECT * EXCLUDE (data) FROM read_parquet(?))"
        bad += con.execute(q, [a, b]).fetchone()[0] + con.execute(q, [b, a]).fetchone()[0]
out["compared_by"] = "row count and sum of row hashes per array" if large else "EXCEPT both ways"
out["rows_differing_from_export" if not large else "arrays_differing_from_export"] = bad
count = {p: len(s.o.list(p)) for p in ("versions", "meta", "lists", "frags")}
size = {p: sum(s.o.size(k) for k in s.o.list(p)) for p in ("versions", "meta", "lists", "frags")}
out["objects"] = count
out["history_mb"] = {k: round(v / 1e6, 2) for k, v in size.items()}
out["latest_manifest_kb"] = round(s.o.size(f"versions/{s.latest():08d}.json") / 1e3, 1)
import resource

out["peak_memory_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)
print(json.dumps(out, indent=1))
json.dump(out, open(os.path.join(A.work, "real_rows.json"), "w"), indent=1)
assert bad == 0, "latest version differs from the export"
print("latest version equals the export, row for row")
