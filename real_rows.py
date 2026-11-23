"""Replay a real published byterefs export as a history, and prove the rows.

  python real_rows.py <export_dir> <workdir> [--commits N] [--root s3://bucket/prefix]

<export_dir> holds arrays.json and refs_<array>.parquet: the flat refs
store raad.virtualize renders to work/<dataset>/out/flat and publishes as
<dataset>/flat/. It may be an https URL of a published one, which is
fetched first. The first half of dim_0 is loaded as
version 1; the rest is appended in up to N commits (default 60), every
array in each commit. Then the latest version is compared with the export,
row for row, by SQL: both EXCEPTs must be empty.

Row dicts are held in memory: fine to a few million rows, not for the
37M-row Bluelink table.
"""
import argparse, json, os, shutil, statistics, time

import duckdb
import pyarrow.parquet as pq

import brstore
from brstore import Store

ap = argparse.ArgumentParser()
ap.add_argument("export"), ap.add_argument("work")
ap.add_argument("--commits", type=int, default=60)
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
rows = {n: pq.read_table(os.path.join(A.export, f"refs_{n}.parquet")).to_pylist() for n in names}
total = sum(len(v) for v in rows.values())
flat = sum(os.path.getsize(os.path.join(A.export, f"refs_{n}.parquet")) for n in names)
steps = sorted({r["dim_0"] for v in rows.values() for r in v})
split = steps[len(steps) // 2]
tail = [k for k in steps if k >= split]
per = max(1, -(-len(tail) // A.commits))
srcs = []
sp = os.path.join(A.export, "sources.parquet")
if os.path.exists(sp):
    keep = ("source", "size", "mtime", "etag")
    srcs = [{k: r.get(k) for k in keep} for r in pq.read_table(sp).to_pylist()]
    for r in srcs:
        r["mtime"] = None if r["mtime"] is None else int(r["mtime"])

s = Store(root)
t0 = time.perf_counter()
brstore.init(s, table, {n: [r for r in rows[n] if r["dim_0"] < split] for n in names}, srcs, part_size=per)
out = dict(export=A.export, arrays=len(names), rows=total, flat_refs_mb=round(flat / 1e6, 2), dim_0_steps=len(steps),
           bulk_s=round(time.perf_counter() - t0, 2), steps_per_commit=per)
by = {n: {} for n in names}
for n in names:
    for r in rows[n]:
        if r["dim_0"] >= split:
            by[n].setdefault(r["dim_0"], []).append(r)
times = []
for i in range(0, len(tail), per):
    t0 = time.perf_counter()
    t = s.begin("append")
    for n in names:
        for k0 in tail[i : i + per]:
            for r in by[n].get(k0, []):
                key = tuple(r[f"dim_{j}"] for j in range(nd[n]))
                if r["present"]:
                    t.rows[(n, key)] = dict(present=r["present"], path=r["path"], offset=r["offset"], size=r["size"], info=r["info"], data=r["data"])
                else:
                    t.put_absent(n, key)
    t.commit(f"dim_0 {tail[i]} to {tail[min(i + per, len(tail)) - 1]}")
    times.append(time.perf_counter() - t0)
out["commits"] = len(times)
out["commit_s_median"] = round(statistics.median(times), 4) if times else None
m = s.manifest(s.latest())
con = duckdb.connect()
bad = 0
for n in names:
    q = "SELECT count(*) FROM (SELECT * EXCLUDE (data) FROM read_parquet(?) EXCEPT SELECT * EXCLUDE (data) FROM read_parquet(?))"
    a, b = s.frag_paths(m, n), [os.path.join(A.export, f"refs_{n}.parquet")]
    bad += con.execute(q, [a, b]).fetchone()[0] + con.execute(q, [b, a]).fetchone()[0]
out["rows_differing_from_export"] = bad
size = {p: sum(s.o.size(k) for k in s.o.list(p)) for p in ("versions", "meta", "lists", "frags")}
out["history_mb"] = {k: round(v / 1e6, 2) for k, v in size.items()}
out["latest_manifest_kb"] = round(s.o.size(f"versions/{s.latest():08d}.json") / 1e3, 1)
print(json.dumps(out, indent=1))
json.dump(out, open(os.path.join(A.work, "real_rows.json"), "w"), indent=1)
assert bad == 0, "latest version differs from the export"
print("latest version equals the export, row for row")
