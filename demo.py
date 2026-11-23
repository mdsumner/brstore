"""Walk the prototype through the history the plan asked for, on the wxylib
synthetic fixture (three OISST-shaped days), checking every read.

  python demo.py <wxylib/fixtures/mdimrefs> <workdir>

Checks are asserts: the script stops at the first thing that is not true.
"""

import hashlib
import json
import os
import shutil
import sys
import threading

import duckdb
import h5py
import netCDF4
import numcodecs
import numpy as np

import brstore
from brstore import Conflict, StaleSource, Store, import_export

FIX, WORK = os.path.abspath(sys.argv[1]), os.path.abspath(sys.argv[2])
MIRROR = os.path.join(WORK, "mirror")  # a copy we are free to change in place
ROOT = os.environ.get("BRSTORE_ROOT") or os.path.join(WORK, "store")  # or s3://bucket/prefix
shutil.rmtree(WORK, ignore_errors=True)
shutil.copytree(os.path.join(FIX, "nc"), MIRROR)
FIXMAN = json.load(open(os.path.join(FIX, "manifest.json")))
REPORT = {}


def resolve(path):
    assert path.startswith("/mirror/oisst/"), path
    return os.path.join(MIRROR, path[len("/mirror/oisst/") :])


def say(title):
    print("\n== " + title)


def sha(a):
    return hashlib.sha256(np.ascontiguousarray(a).tobytes()).hexdigest()[:16]


# ------------------------------------------------- a reader: table rows in, values out

ZLIB, SHUF = numcodecs.Zlib(level=1), numcodecs.Shuffle(elementsize=2)


def encode(a):
    return ZLIB.encode(SHUF.encode(np.ascontiguousarray(a.astype("<i2"))))


def decode(b, shape):
    return np.frombuffer(SHUF.decode(ZLIB.decode(b)), dtype="<i2").reshape(shape)


def read(s, version, name, start=None, count=None, verify=True):
    m = s.manifest(version)
    a = s.array(m, name)
    assert a["codec"]["compressor"]["id"] == "zlib" and a["codec"]["filters"][0]["id"] == "shuffle"
    shape, chunks = a["shape"], a["chunks"]
    start = start or [0] * len(shape)
    count = count or shape
    out = np.full(shape, int(a["fill"]), dtype="<i2")
    rows = {tuple(r[f"dim_{i}"] for i in range(len(shape))): r for r in s.refs_rows(m, name)}
    for key in np.ndindex(*[-(-n // c) for n, c in zip(shape, chunks)]):
        if not all(st // c <= k <= (st + n - 1) // c for k, st, n, c in zip(key, start, count, chunks)):
            continue
        b = s.fetch(m, rows.get(key), resolve, verify=verify)
        if b is not None:
            sl = tuple(slice(k * c, k * c + c) for k, c in zip(key, chunks))
            out[sl] = decode(b, chunks)
    return out[tuple(slice(st, st + n) for st, n in zip(start, count))]


def check_oracle(s, version, label):
    """The fixture's five GDAL 3.13 window reads of the source mosaic."""
    for w in FIXMAN["windows"]:
        want = np.fromfile(os.path.join(FIX, w["file"]), dtype="<i2").reshape(w["count"])
        got = read(s, version, w["array"], w["start"], w["count"])
        assert np.array_equal(got, want), (label, w["file"])
    print(f"   {label}: 5 of 5 GDAL oracle windows equal")


def average_x2(a, fill=-999):
    """A derived overview level: 2x2 mean ignoring fill. Stands for any computed chunk."""
    t = a.reshape(a.shape[0], a.shape[1], a.shape[2] // 2, 2, a.shape[3] // 2, 2).astype("f8")
    ok = t != fill
    n = ok.sum(axis=(3, 5))
    v = np.where(n > 0, np.where(ok, t, 0).sum(axis=(3, 5)) / np.maximum(n, 1), fill)
    return np.rint(v).astype("<i2")


def nearest_x2(a):
    return np.ascontiguousarray(a[:, :, ::2, ::2]).astype("<i2")


def level_desc(base, nt):
    d = json.loads(json.dumps(base))
    d.update(array="sst_x2", dims=["time", "zlev", "lat_x2", "lon_x2"], shape=[nt, 1, 18, 36], chunks=[1, 1, 18, 36])
    d["attrs"] = dict(base["attrs"], overview_of="sst", overview_factor=[2, 2], overview_resampling="AVERAGE")
    return d


def coord(name, n, start, step, units):
    return dict(array=name, kind="coord", storage="affine", dims=[name], shape=[n], chunks=[n], dtype="<f8",
                fill=None, codec=None, attrs=dict(units=units), affine=dict(start=start, step=step), values=None)


def store_size(s):
    out = {}
    for p in ("versions", "meta", "lists", "frags", "packs", "tags"):
        ks = s.o.list(p)
        out[p] = (len(ks), sum(s.o.size(k) for k in ks))
    return out


# ------------------------------------------------- 1. import: the table as version 1

say("1. import the fixture's three-file export as version 1")
s = Store(ROOT)
import_export(s, FIX, part_size=1, resolve=resolve)
s.tag("scan", 1)
check_oracle(s, "scan", "tag scan")
print("   objects:", store_size(s))

# ------------------------------------------------- 2. a write is a harvest

say("2. write a derived overview level; its chunks go to a pack, its rows are ordinary refs")
m1 = s.manifest(1)
sst = read(s, 1, "sst")
t = s.begin("producer")
t.put_array(level_desc(s.array(m1, "sst"), 3))
t.put_array(coord("lat_x2", 18, -85.0, 10.0, "degrees_north"))
t.put_array(coord("lon_x2", 36, 5.0, 10.0, "degrees_east"))
lvl = average_x2(sst)
for d in range(3):
    t.write_chunk("sst_x2", (d, 0, 0, 0), encode(lvl[d : d + 1]))
v = t.commit("sst_x2: AVERAGE level for days 0 to 2")
s.tag("level-a", v)
assert np.array_equal(read(s, v, "sst_x2"), lvl)
row = s.chunk_row(s.manifest(v), "sst_x2", (1, 0, 0, 0))
print(f"   version {v}; row for day 1: path={row['path'][:22]}... offset={row['offset']} size={row['size']}")
LEVEL_A = sha(read(s, "level-a", "sst_x2"))
check_oracle(s, v, f"version {v}")

# ------------------------------------------------- 3. two writers, disjoint chunks: both land

say("3. two writers from one base, different chunks: the second rebases and both land")
ta, tb = s.begin("A"), s.begin("B")
a0, b2 = nearest_x2(sst[0:1]), nearest_x2(sst[2:3])
ta.write_chunk("sst_x2", (0, 0, 0, 0), encode(a0))
tb.write_chunk("sst_x2", (2, 0, 0, 0), encode(b2))
va = ta.commit("A: day 0 reprocessed")
vb = tb.commit("B: day 2 reprocessed")
got = read(s, vb, "sst_x2")
assert np.array_equal(got[0:1], a0) and np.array_equal(got[2:3], b2) and np.array_equal(got[1], lvl[1])
print(f"   A committed version {va}; B started from {tb.base}, rebased over {tb.rebased_over}, committed version {vb}")

say("3b. two writers, the same chunk: the second is refused")
tc, td = s.begin("C"), s.begin("D")
tc.write_chunk("sst_x2", (1, 0, 0, 0), encode(nearest_x2(sst[1:2])))
td.write_chunk("sst_x2", (1, 0, 0, 0), encode(lvl[1:2] + 1))
vc = tc.commit("C: day 1 reprocessed")
try:
    td.commit("D: day 1 reprocessed differently")
    raise AssertionError("D should have been refused")
except Conflict as e:
    print(f"   C committed version {vc}; D refused: {e}")
assert s.latest() == vc

say("3c. sixteen threads, one chunk each of a scratch array, all racing for the next version")
tz = s.begin("setup")
scratch = level_desc(s.array(m1, "sst"), 16)
scratch.update(array="scratch", attrs={})
tz.put_array(scratch)
base = tz.commit("scratch array")
errs, done = [], []


def worker(i):
    try:
        w = s.begin(f"t{i}")
        w.write_chunk("scratch", (i, 0, 0, 0), encode(np.full((1, 1, 18, 36), i)))
        done.append((w.commit(f"t{i}", max_tries=200), w.rebased_over))
    except Exception as e:  # noqa
        errs.append(e)


th = [threading.Thread(target=worker, args=(i,)) for i in range(16)]
[x.start() for x in th]
[x.join() for x in th]
assert not errs, errs
assert sorted(v for v, _ in done) == list(range(base + 1, base + 17))
sc = read(s, s.latest(), "scratch")
assert all((sc[i] == i).all() for i in range(16))
print(f"   16 commits landed as versions {base + 1} to {base + 16}, no gaps, no lost writes; "
      f"{sum(1 for _, r in done if r)} of them rebased")
REPORT["race_rebased"] = sum(1 for _, r in done if r)

# ------------------------------------------------- 4. time travel reproduces bytes

say("4. the tag from before the rewrites still reads the same bytes")
assert sha(read(s, "level-a", "sst_x2")) == LEVEL_A
latest = read(s, s.latest(), "sst_x2")
differs = [d for d in range(3) if not np.array_equal(latest[d], lvl[d])]
print(f"   tag level-a hash {LEVEL_A} unchanged; latest differs from it on days {differs}")
assert differs == [0, 1, 2]

# ------------------------------------------------- 5. append: refs and derived bytes in one commit

say("5. a daily append: a new source file's refs and a computed level chunk, one atomic commit")
p4 = os.path.join(MIRROR, "198109", "oisst-avhrr-v02r01.19810904.nc")
rng = np.random.default_rng(4)
day4 = {"sst": rng.integers(-180, 3000, (1, 1, 36, 72)).astype("i2"), "ice": rng.integers(0, 100, (1, 1, 36, 72)).astype("i2")}
with netCDF4.Dataset(p4, "w", format="NETCDF4") as nc:
    for dn, n in (("time", 1), ("zlev", 1), ("lat", 36), ("lon", 72)):
        nc.createDimension(dn, n)
    for name, val in day4.items():
        var = nc.createVariable(name, "i2", ("time", "zlev", "lat", "lon"), zlib=True, shuffle=True, complevel=1,
                                chunksizes=(1, 1, 36, 72), fill_value=-999)
        var[:] = val
src4 = "/mirror/oisst/198109/oisst-avhrr-v02r01.19810904.nc"
t = s.begin("daily")
mb = s.manifest(t.base)
with h5py.File(p4, "r") as h:  # the harvest: where is each chunk
    for name in ("sst", "ice"):
        ci = h[name].id.get_chunk_info(0)
        t.put_ref(name, (3, 0, 0, 0), src4, ci.byte_offset, ci.size, brstore.PACK_INFO)
        d = json.loads(json.dumps(s.array(mb, name)))
        d["shape"][0] = 4
        t.put_array(d)
t.put_source(src4, os.path.getsize(p4), int(os.path.getmtime(p4)), brstore.file_etag(p4))
tm = json.loads(json.dumps(s.array(mb, "time")))
tm.update(shape=[4], chunks=[4], values=tm["values"] + [tm["values"][-1] + 1])
t.put_array(tm)
lv = json.loads(json.dumps(s.array(mb, "sst_x2")))
lv["shape"][0] = 4
t.put_array(lv)
t.write_chunk("sst_x2", (3, 0, 0, 0), encode(average_x2(day4["sst"])))
v5 = t.commit("append 1981-09-04")
assert np.array_equal(read(s, v5, "sst", [3, 0, 0, 0], [1, 1, 36, 72]), day4["sst"])
assert np.array_equal(read(s, v5, "ice", [3, 0, 0, 0], [1, 1, 36, 72]), day4["ice"])
assert np.array_equal(read(s, v5, "sst_x2")[3:4], average_x2(day4["sst"]))
assert s.array(s.manifest("scan"), "sst")["shape"][0] == 3
check_oracle(s, v5, f"version {v5} (4 days)")
print(f"   version {v5}: sst and ice are 4 days, the level has day 3; tag scan still says 3 days")

# ------------------------------------------------- 6. a source that changes in place (m16 section 6a)

say("6. hold day 2 of sst, then the provider rewrites that file under the same name")
t = s.begin("holder")
mb = s.manifest(t.base)
t.write_chunk("sst", (2, 0, 0, 0), s.fetch(mb, s.chunk_row(mb, "sst", (2, 0, 0, 0)), resolve))
vh = t.commit("hold sst day 2")
s.tag("held", vh)
want2 = np.fromfile(os.path.join(FIX, "oracle/sst-w2.bin"), dtype="<i2").reshape(1, 1, 6, 12)
p3 = resolve("/mirror/oisst/198109/oisst-avhrr-v02r01.19810903.nc")
shutil.copyfile(p4, p3)  # same name, different bytes, different offsets
try:
    read(s, "scan", "sst", [2, 0, 30, 60], [1, 1, 6, 12])
    raise AssertionError("a stale read was accepted")
except StaleSource as e:
    print(f"   tag scan (refs only): refused, {str(e)[-58:]}")
try:
    unchecked = read(s, "scan", "sst", [2, 0, 30, 60], [1, 1, 6, 12], verify=False)
    outcome = "returns values, equal to oracle = %s" % bool(np.array_equal(unchecked, want2))
except Exception as e:  # noqa: old offsets into new bytes
    outcome = "reads the old byte range of the new file and fails to decode (%s)" % type(e).__name__
print(f"   the same read without the source check: {outcome}")
got = read(s, "held", "sst", [2, 0, 30, 60], [1, 1, 6, 12])
assert np.array_equal(got, want2)
print("   tag held: reads the held bytes, equal to the GDAL oracle window sst-w2")
REPORT["stale_unchecked"] = outcome

# ------------------------------------------------- 7. the table operations stay plain SQL

say("7. diff, a chunk index and a content summary are SQL over the fragments, no library")
con = duckdb.connect()
A, B = s.manifest("level-a"), s.manifest(s.latest())
q = ("SELECT count(*) FROM (SELECT * EXCLUDE (data) FROM read_parquet(?) "
     "EXCEPT SELECT * EXCLUDE (data) FROM read_parquet(?))")
n_new = con.execute(q, [s.frag_paths(B, "sst_x2"), s.frag_paths(A, "sst_x2")]).fetchone()[0]
n_gone = con.execute(q, [s.frag_paths(A, "sst_x2"), s.frag_paths(B, "sst_x2")]).fetchone()[0]
print(f"   sst_x2, tag level-a to latest: {n_new} rows new or changed, {n_gone} rows replaced")
assert (n_new, n_gone) == (4, 3)
summ = con.execute(
    "SELECT count(*), sum(present), sum(size), count(DISTINCT path) FILTER (path LIKE 'packs/%'), "
    "count(DISTINCT path) FILTER (path NOT LIKE 'packs/%') FROM read_parquet(?)", [s.frag_paths(B, "sst")]
).fetchone()
print(f"   sst at latest: {summ[0]} chunks, {summ[1]} present, {summ[2]} bytes, in {summ[3]} pack and {summ[4]} source files")
dup = con.execute("SELECT count(*) FROM (SELECT dim_0, dim_1, dim_2, dim_3 FROM read_parquet(?) GROUP BY ALL HAVING count(*) > 1)",
                  [s.frag_paths(B, "sst_x2")]).fetchone()[0]
assert dup == 0
print("   one row per chunk key in a version: checked, 0 duplicates")

# ------------------------------------------------- 8. checkout: the form existing readers take

say("8. checkout of latest and of each tag as the three-file export (what wxy-refs and zaro read)")
for name in ("latest", "level-a", "held"):
    v = s.checkout(s.latest() if name == "latest" else name, os.path.join(WORK, "checkout", name), "fixture",
                   [("/mirror/oisst/", MIRROR + "/")])
    print(f"   checkout/{name}: version {v}")

# ------------------------------------------------- 9. GC is an anti-join

say("9. garbage collection: keep the tags and latest, delete what they do not reach")
before = store_size(s)
keep = sorted({s.latest()} | {json.loads(s.o.get(k))["version"] for k in s.o.list("tags")})
dead, freed = s.gc(keep)
after = store_size(s)
print(f"   kept versions {keep} of {before['versions'][0]}; deleted {len(dead)} objects, {freed} bytes")
print("   before:", before)
print("   after: ", after)
assert sha(read(s, "level-a", "sst_x2")) == LEVEL_A
assert np.array_equal(read(s, "held", "sst", [2, 0, 30, 60], [1, 1, 6, 12]), want2)
assert np.array_equal(read(s, s.latest(), "sst_x2")[3:4], average_x2(day4["sst"]))
print("   every kept version still reads the same values")
REPORT.update(before=before, after=after, kept=keep, deleted=len(dead), freed=freed, latest=s.latest())
json.dump(REPORT, open(os.path.join(WORK, "report.json"), "w"), indent=1)
print("\nall checks passed")
