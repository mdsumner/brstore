"""Levels a source never had, committed beside it as a chain of derived arrays.

  python derive_levels.py <store root> [--array NAME] [--levels N] [--steps 2]
                          [--first N] [--target public] [--base DIR] [--jobs 4]

Model output in netCDF has one resolution. A reader that wants a quick look
at a time step (a slider being dragged, a small map) still has to fetch the
whole step. This script writes the coarser levels the source lacks: for an
array of (time, y, x) or (time, level, y, x),

  <array>_x2   each cell the mean of the 2 by 2 source cells that have a value
  <array>_x4   the same, of <array>_x2
  <array>_x8   ... and so on: a chain, each level made from the one before

Only the first link reads the source, once per time step; every other level
is made from the level before it. Their chunks go into a pack and are
recorded as ordinary rows, so every reader of the store sees them as arrays.
Each has its own y and x dimensions (<dim>_x2, ...) with coordinates at the
means of the cells joined, where the source's are one-dimensional.

Values stay as stored: a packed integer array stays packed, with the same
scale and offset, rounded to the nearest stored value. A cell with no valid
source cell holds the fill. An odd edge is the mean of what is there.

Lineage is as derive_bottom.py records it: each array's attributes name the
array it was made from, the rule and the factor from the source, and the
commit message names the steps, the version read and the identity of the
source array's rows in it.

Steps: the last --steps by default, or --steps of them from --first; steps
the store already holds at the first level are skipped. --levels: how many
(default: until a level is at most 256 cells on its longer side). Source
bytes are fetched as derive_bottom.py fetches them; --jobs is capped at 10
for the same reason.
"""
import argparse, io, json, os, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import brstore
from derive_bottom import CODEC, encode, missing_values, number, refs_of

TILE = 1024  # the most cells along y or x in a derived chunk
SMALL = 256  # stop when a level is no longer than this on its longer side
RULE = "the mean of the 2 by 2 cells along {y}, {x} that have a value"


def halve(a, missing, fill):
    """a: (..., ny, nx) as stored. Each output cell is the mean of the 2 by 2
    input cells that have a value (an odd edge: of the 1 or 2 there are), in
    a's dtype, rounded if that is an integer; fill where none has one."""
    ny, nx = a.shape[-2:]
    ok = np.ones(a.shape, dtype=bool)
    if a.dtype.kind == "f":
        ok &= np.isfinite(a)
    for v in missing:
        ok &= a != v
    py, px = ny % 2, nx % 2
    if py or px:
        pad = [(0, 0)] * (a.ndim - 2) + [(0, py), (0, px)]
        a, ok = np.pad(a, pad), np.pad(ok, pad)
    lead = a.shape[:-2]
    shape = lead + (a.shape[-2] // 2, 2, a.shape[-1] // 2, 2)
    v = np.where(ok, a, 0).astype("f8").reshape(shape).sum(axis=(-3, -1))
    n = ok.reshape(shape).sum(axis=(-3, -1))
    mean = v / np.maximum(n, 1)
    if a.dtype.kind in "iu":
        mean = np.rint(mean)
    with np.errstate(invalid="ignore"):
        return np.where(n > 0, mean, fill).astype(a.dtype)


def halve_coord(v):
    """A coordinate at the next level: the mean of each pair, an odd last one kept."""
    v = np.asarray(v, dtype="f8")
    out = v[: len(v) // 2 * 2].reshape(-1, 2).mean(axis=1)
    return np.concatenate([out, v[-1:]]) if len(v) % 2 else out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--array")
    ap.add_argument("--levels", type=int)
    ap.add_argument("--steps", type=int, default=2)
    ap.add_argument("--first", type=int)
    ap.add_argument("--target", default="public")
    ap.add_argument("--base", default="")
    ap.add_argument("--jobs", type=int, default=4)
    A = ap.parse_args(argv)
    jobs = max(1, min(A.jobs, 10))  # NCI THREDDS: ten at once is thought safe, not yet tested in detail

    s = brstore.Store(A.root)
    m = s.manifest(s.latest())
    table = m["table"]
    cands = [a for a in table["arrays"] if a["storage"] == "referenced" and a["kind"] == "data" and len(a["shape"]) in (3, 4)
             and "derived" not in a["attrs"]]
    a = next((x for x in cands if x["array"] == A.array), None) if A.array else (cands[0] if cands else None)
    if a is None:
        sys.exit("no referenced array of (time, y, x) or (time, level, y, x)" + (f" named {A.array}" if A.array else ""))
    name, nd = a["array"], len(a["shape"])
    nt, (ny, nx), ct, (cy, cx) = a["shape"][0], a["shape"][-2:], a["chunks"][0], a["chunks"][-2:]
    nz, cz = (a["shape"][1], a["chunks"][1]) if nd == 4 else (1, 1)
    ydim, xdim = a["dims"][-2:]
    dt = np.dtype(a["dtype"])
    missing = missing_values(a)
    fill = number(a.get("fill"))
    if fill is None:
        fill = float("nan") if dt.kind == "f" else (missing[0] if missing else 0)

    # the chain: sizes of each level, until one is small
    sizes, h, w = [], ny, nx
    while (len(sizes) < A.levels) if A.levels else (max(h, w) > SMALL and len(sizes) < 12):
        h, w = -(-h // 2), -(-w // 2)
        sizes.append((h, w))
        if min(h, w) == 1:
            break
    if not sizes:
        sys.exit(f"{name} is {ny} by {nx}: at most {SMALL} cells a side already; name --levels to derive some anyway")
    names = [f"{name}_x{2 ** (k + 1)}" for k in range(len(sizes))]

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
        """The rows of the chunks at index k0 of dim_0, by their other chunk indexes."""
        i = s.covering(frags, k0)
        if i is None:
            return {}
        if i not in frag_tables:
            frag_tables.clear()
            frag_tables[i] = pq.read_table(io.BytesIO(s.o.get(frags[i]["path"])))
        t = frag_tables[i]
        t = t.filter(pc.equal(t["dim_0"], k0))
        return {tuple(r[f"dim_{d}"] for d in range(1, nd)): r for r in t.to_pylist() if r["present"]}

    # which steps: not those the store already has at the first level
    have = set()
    if any(x["array"] == names[0] for x in table["arrays"]):
        for f in s.frags(m, names[0]):
            t = pq.read_table(io.BytesIO(s.o.get(f["path"])), columns=["dim_0", "present"])
            have |= {d for d, p in zip(t["dim_0"].to_pylist(), t["present"].to_pylist()) if p}
    first = A.first if A.first is not None else max(0, nt - A.steps)
    steps = [t for t in range(first, min(nt, first + A.steps)) if t not in have]
    print(f"{name}: shape {a['shape']}, chunks {a['chunks']}, version {m['version']} (rows {refs_of(m, name)}); "
          f"missing is {missing if missing else 'not a number only'}")
    print(f"   levels: " + ", ".join(f"{n} {h} by {w}" for n, (h, w) in zip(names, sizes)))
    if not steps:
        print(f"{names[0]}: steps {first} to {min(nt, first + A.steps) - 1} are in the store already")
        return 0

    txn = s.begin(writer="derive")
    by_name = {x["array"]: x for x in table["arrays"]}
    coords = {}
    for d in (ydim, xdim):  # a one-dimensional coordinate of the dimension, if the store has one
        c = by_name.get(d)
        if c is not None and c["dims"] == [d]:
            coords[d] = (np.asarray(s.coord_values(m, d), dtype="f8"), c["attrs"])
    descs = []
    for k, (nm, (h, w)) in enumerate(zip(names, sizes)):
        f = 2 ** (k + 1)
        yd, xd = f"{ydim}_x{f}", f"{xdim}_x{f}"
        tile = (min(h, TILE), min(w, TILE))
        desc = json.loads(json.dumps(a))
        desc.update(array=nm, dims=a["dims"][:-2] + [yd, xd], shape=a["shape"][:-2] + [h, w],
                    chunks=([1, cz] if nd == 4 else [1]) + list(tile), codec=CODEC(dt.itemsize), fill=a.get("fill"))
        desc["attrs"] = dict(a["attrs"], derived={"of": names[k - 1] if k else name, "source": name, "factor": [f, f],
                                                  "rule": RULE.format(y=ydim, x=xdim), "written_by": "derive_levels.py"})
        txn.put_array(desc)
        descs.append((nm, h, w, tile))
        for d, dn in ((ydim, yd), (xdim, xd)):
            if d in coords:
                v, attrs = coords[d]
                coords[d] = (halve_coord(v), attrs)
                if dn not in by_name:
                    txn.put_coord(dn, coords[d][0], attrs=dict(attrs, derived={"of": d, "factor": f, "rule": "the mean of each pair"}))

    nzc, nyc, nxc = -(-nz // cz), -(-ny // cy), -(-nx // cx)
    read_bytes = read_chunks = 0
    wrote = [0] * len(names)
    t0 = time.time()
    with ThreadPoolExecutor(jobs) as pool:
        for t in steps:
            rows, within = rows_at(t // ct), t % ct
            for z in range(nzc):  # one chunk of levels at a time: the source's own slab
                d = min(cz, nz - z * cz)
                slab = np.full((d, ny, nx), fill, dtype=dt)
                keys = [((z, j, i) if nd == 4 else (j, i), j, i) for j in range(nyc) for i in range(nxc)]
                todo = [(j, i, rows[k]) for k, j, i in keys if k in rows]
                for (j, i, r), data in zip(todo, pool.map(fetch, [r for _, _, r in todo])):
                    if r["offset"] is not None and len(data) != r["size"]:
                        sys.exit(f"short read of {r['path']} at {r['offset']}")
                    block = np.frombuffer(brstore.decode_v2(a, data), dtype=dt).reshape(a["chunks"])[within]
                    if nd == 3:
                        block = block[None]
                    y0, x0 = j * cy, i * cx
                    hh, ww = min(cy, ny - y0), min(cx, nx - x0)
                    slab[:, y0 : y0 + hh, x0 : x0 + ww] = block[:d, :hh, :ww]
                    read_bytes += len(data)
                    read_chunks += 1
                if not todo:
                    continue
                for k, (nm, h, w, (oy, ox)) in enumerate(descs):
                    slab = halve(slab, missing, fill)  # each level from the one before
                    for j in range(-(-h // oy)):
                        for i in range(-(-w // ox)):
                            block = np.full((cz, oy, ox), fill, dtype=dt)
                            part = slab[:, j * oy : (j + 1) * oy, i * ox : (i + 1) * ox]
                            block[: part.shape[0], : part.shape[1], : part.shape[2]] = part
                            enc = encode(block[None] if nd == 4 else block[:1], dt.itemsize)
                            txn.write_chunk(nm, (t, z, j, i) if nd == 4 else (t, j, i), enc)
                            wrote[k] += len(enc)
            print(f"   step {t}: {len(names)} levels written")
    msg = (f"{names[0]} to {names[-1]}: steps {steps[0]} to {steps[-1]} from {name} at version {m['version']} "
           f"(rows {refs_of(m, name)}), each level the 2 by 2 mean of the one before")
    v = txn.commit(msg)
    n = len(steps)
    out = {"array": name, "derived": names, "steps": steps, "from_version": m["version"], "version": v,
           "source_chunks_read": read_chunks, "source_mb_read": round(read_bytes / 2**20, 2),
           "mb_per_step": {"source": round(read_bytes / n / 2**20, 3),
                           **{nm: round(b / n / 2**20, 4) for nm, b in zip(names, wrote)}},
           "chunks_per_step": {"source": nzc * nyc * nxc,
                               **{nm: nzc * -(-h // oy) * -(-w // ox) for nm, h, w, (oy, ox) in descs}},
           "seconds": round(time.time() - t0, 1)}
    print(json.dumps(out, indent=1))
    print(f"committed version {v}: {msg}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
