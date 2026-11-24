"""A derived array committed beside the references it came from: the bottom.

  python derive_bottom.py <store root> [--array NAME] [--steps 2] [--first N]
                          [--target public] [--base DIR] [--jobs 4] [--deepest last]

For an array of (time, level, y, x), the bottom at a cell is the value at
its deepest level that has one. Which level that is depends on the data, so
a reader has to fetch every level to find it: the most a view can cost. This
script does that once per time step, through a committed version's rows,
and commits the answer to the same store as two small arrays:

  <array>_bottom        (time, y, x)  the value, as stored in the source
  <array>_bottom_level  (time, y, x)  the level index it came from, -1 for none

Their chunks go into a pack and are recorded as ordinary rows, so every
reader of the store (the Zarr session, the kerchunk render, a checkout)
sees them as arrays like any other. A reader that wants the bottom then
reads one small chunk a step and no levels at all.

Lineage: each array's attributes say what it was derived from and by which
rule, and each commit's message says which steps it wrote, from which
version, and the identity of the source array's rows in that version (the
key of its fragment list, a content hash: it changes only when they do).

Steps: the last --steps of the array by default, or --steps of them from
--first. Steps the store already holds a bottom for are skipped. Source
bytes are fetched as pixels.py fetches them (--target names the remap rule;
a name with no rule reads the paths as stored), --jobs at a time. Keep
--jobs low against a server: NCI's THREDDS refuses somewhere past ten.
"""
import argparse, io, json, os, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import brstore

CODEC = lambda itemsize: {"compressor": {"id": "zlib", "level": 5}, "filters": [{"id": "shuffle", "elementsize": itemsize}]}  # noqa: E731
CELLS = 4_000_000  # the most cells in a derived chunk


def refs_of(m, name):
    """The identity of one array's rows in a version: the key of its fragment
    list, which is the hash of its content. It changes only when those rows do."""
    return os.path.basename(m["refs"][name]).split(".")[0][:16]


def number(x):
    """x as a number where it is one (a description may hold a fill as text)."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return None if v != v else (int(v) if v == int(v) else v)


def missing_values(a):
    """The stored values that mean no value: the array's fill and the CF attributes."""
    out = []
    for v in [a.get("fill"), a["attrs"].get("_FillValue"), a["attrs"].get("missing_value")]:
        out += [number(x) for x in (v if isinstance(v, list) else [v]) if number(x) is not None]
    return sorted(set(out))


def encode(block, itemsize):
    import numcodecs

    data = numcodecs.Shuffle(itemsize).encode(np.ascontiguousarray(block).tobytes())
    return numcodecs.Zlib(5).encode(data)


def bottom_of(levels, missing, deepest_last=True):
    """levels: (nz, ny, nx). The value at each cell's deepest level that has
    one, and that level's index (-1 where no level has a value)."""
    nz = levels.shape[0]
    ok = np.ones(levels.shape, dtype=bool)
    if levels.dtype.kind == "f":
        ok &= np.isfinite(levels)
    for v in missing:
        ok &= levels != v
    order = ok[::-1] if deepest_last else ok
    first = order.argmax(axis=0)
    level = np.where(order.any(axis=0), (nz - 1 - first) if deepest_last else first, -1).astype("<i2")
    value = np.take_along_axis(levels, np.maximum(level, 0)[None].astype("i8"), axis=0)[0]
    return value, level


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--array")
    ap.add_argument("--steps", type=int, default=2)
    ap.add_argument("--first", type=int)
    ap.add_argument("--target", default="public")
    ap.add_argument("--base", default="")
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--deepest", choices=["last", "first"], default="last",
                    help="which end of the level dimension is the deepest")
    A = ap.parse_args(argv)
    jobs = max(1, min(A.jobs, 8))

    s = brstore.Store(A.root)
    m = s.manifest(s.latest())
    table = m["table"]
    cands = [a for a in table["arrays"] if a["storage"] == "referenced" and a["kind"] == "data" and len(a["shape"]) == 4
             and "derived" not in a["attrs"]]
    a = next((x for x in cands if x["array"] == A.array), None) if A.array else (cands[0] if cands else None)
    if a is None:
        sys.exit("no referenced array of four dimensions (time, level, y, x)" + (f" named {A.array}" if A.array else ""))
    name, (nt, nz, ny, nx), (ct, cz, cy, cx) = a["array"], a["shape"], a["chunks"]
    dt = np.dtype(a["dtype"])
    missing = missing_values(a)
    fill = number(a.get("fill"))
    if fill is None:
        fill = float("nan") if dt.kind == "f" else (missing[0] if missing else 0)
    out_name, lvl_name = name + "_bottom", name + "_bottom_level"

    # derived chunks: whole multiples of the source's, up to CELLS cells
    k = 1
    while (cy * (k + 1)) * (cx * (k + 1)) <= CELLS and (cy * k < ny or cx * k < nx):
        k += 1
    oy, ox = min(ny, cy * k), min(nx, cx * k)

    def remap(path):
        for r in table.get("remap", []):
            if r["target"] == A.target and path.startswith(r["prefix"]):
                path = r["replacement"] + path[len(r["prefix"]):]
        return path if "://" in path or os.path.isabs(path) else os.path.join(A.base, path)

    def fetch(row):
        if row["offset"] is None:
            return row["data"]
        if row["path"].startswith("packs/"):
            return s.o.get(row["path"], row["offset"], row["size"])
        loc = remap(row["path"])
        if "://" in loc:
            req = urllib.request.Request(loc, headers={"Range": f"bytes={row['offset']}-{row['offset'] + row['size'] - 1}"})
            return urllib.request.urlopen(req, timeout=300).read()
        with open(loc, "rb") as f:
            f.seek(row["offset"])
            return f.read(row["size"])

    frags, frag_tables = s.frags(m, name), {}

    def rows_at(k0):
        """The rows of the chunks at index k0 of dim_0, by (level, y, x) chunk."""
        i = s.covering(frags, k0)
        if i is None:
            return {}
        if i not in frag_tables:
            frag_tables.clear()
            frag_tables[i] = pq.read_table(io.BytesIO(s.o.get(frags[i]["path"])))
        t = frag_tables[i]
        t = t.filter(pc.equal(t["dim_0"], k0))
        return {(r["dim_1"], r["dim_2"], r["dim_3"]): r for r in t.to_pylist() if r["present"]}

    # which steps: not those the store already has
    have = set()
    if any(x["array"] == out_name for x in table["arrays"]):
        for f in s.frags(m, out_name):
            t = pq.read_table(io.BytesIO(s.o.get(f["path"])), columns=["dim_0", "present"])
            have |= {d for d, p in zip(t["dim_0"].to_pylist(), t["present"].to_pylist()) if p}
    first = A.first if A.first is not None else max(0, nt - A.steps)
    steps = [t for t in range(first, min(nt, first + A.steps)) if t not in have]
    print(f"{name}: shape {a['shape']}, chunks {a['chunks']}, version {m['version']} (rows {refs_of(m, name)}); "
          f"missing is {missing if missing else 'not a number only'}")
    if not steps:
        print(f"{out_name}: steps {first} to {min(nt, first + A.steps) - 1} are in the store already")
        return 0

    def lineage(what):
        return {"of": name, "rule": f"{what} at the {A.deepest} index along {a['dims'][1]} that has a value",
                "written_by": "derive_bottom.py"}

    desc = json.loads(json.dumps(a))
    desc.update(array=out_name, dims=[a["dims"][0], a["dims"][2], a["dims"][3]], shape=[nt, ny, nx], chunks=[1, oy, ox],
                codec=CODEC(dt.itemsize), fill=a.get("fill"))
    desc["attrs"] = dict(a["attrs"], derived=lineage("the value"))
    if "long_name" in a["attrs"]:
        desc["attrs"]["long_name"] = f"{a['attrs']['long_name']} at the bottom"
    lvl = json.loads(json.dumps(desc))
    lvl.update(array=lvl_name, dtype="<i2", fill=-1, codec=CODEC(2))
    lvl["attrs"] = {"long_name": f"index along {a['dims'][1]} of the bottom of {name}", "derived": lineage("the index")}

    txn = s.begin(writer="derive")
    txn.put_array(desc)
    txn.put_array(lvl)
    nzc, nyc, nxc = -(-nz // cz), -(-ny // cy), -(-nx // cx)
    read_bytes = read_chunks = wrote = 0
    t0 = time.time()
    with ThreadPoolExecutor(jobs) as pool:
        for t in steps:
            rows, within = rows_at(t // ct), t % ct
            value = np.full((ny, nx), fill, dtype=dt)
            level = np.full((ny, nx), -1, dtype="<i2")
            for j in range(nyc):
                for i in range(nxc):
                    y0, x0 = j * cy, i * cx
                    h, w = min(cy, ny - y0), min(cx, nx - x0)
                    col = np.full((nz, h, w), fill, dtype=dt)
                    todo = [(z, rows[(z, j, i)]) for z in range(nzc) if (z, j, i) in rows]
                    for (z, r), data in zip(todo, pool.map(fetch, [r for _, r in todo])):
                        if len(data) != (r["size"] if r["offset"] is not None else len(data)):
                            sys.exit(f"short read of {r['path']} at {r['offset']}")
                        block = np.frombuffer(brstore.decode_v2(a, data), dtype=dt).reshape(ct, cz, cy, cx)[within]
                        d = min(cz, nz - z * cz)
                        col[z * cz : z * cz + d] = block[:d, :h, :w]
                        read_bytes += len(data)
                        read_chunks += 1
                    if not todo:
                        continue
                    v, l = bottom_of(col, missing, A.deepest == "last")
                    value[y0 : y0 + h, x0 : x0 + w] = np.where(l >= 0, v, fill)
                    level[y0 : y0 + h, x0 : x0 + w] = l
            for j in range(-(-ny // oy)):
                for i in range(-(-nx // ox)):
                    for arr, nm, f, size in ((value, out_name, fill, dt.itemsize), (level, lvl_name, -1, 2)):
                        block = np.full((1, oy, ox), f, dtype=arr.dtype)
                        part = arr[j * oy : (j + 1) * oy, i * ox : (i + 1) * ox]
                        block[0, : part.shape[0], : part.shape[1]] = part
                        enc = encode(block, size)
                        txn.write_chunk(nm, (t, j, i), enc)
                        wrote += len(enc)
            print(f"   step {t}: {int((level >= 0).sum())} of {ny * nx} cells have a bottom, deepest index {int(level.max())}")
    msg = (f"{out_name}: steps {steps[0]} to {steps[-1]} from {name} at version {m['version']} "
           f"(rows {refs_of(m, name)}), the {A.deepest} index along {a['dims'][1]} with a value")
    v = txn.commit(msg)
    out = {"array": name, "derived": [out_name, lvl_name], "steps": steps, "from_version": m["version"], "version": v,
           "source_chunks_read": read_chunks, "source_mb_read": round(read_bytes / 2**20, 2),
           "derived_chunk_shape": [1, oy, ox], "derived_mb_written": round(wrote / 2**20, 3),
           "bottom_view_chunks_per_step": {"through_levels": nzc * nyc * nxc, "through_derived": -(-ny // oy) * -(-nx // ox)},
           "seconds": round(time.time() - t0, 1)}
    print(json.dumps(out, indent=1))
    print(f"committed version {v}: {msg}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
