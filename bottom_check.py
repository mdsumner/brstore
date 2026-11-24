"""derive_bottom.py on a fixture store, every result an assert.

  python bottom_check.py <fixture dir> <workdir>

The fixture is a three-file export with its netCDF sources beside it and a
"fixture" remap rule (wxylib/fixtures mdimrefs/bluelink-synthetic: temp and
salt of time, level, y, x). It is imported as version 1; the bottom of temp
is derived for the last two steps and committed; then

 - the derived arrays equal the bottom computed from the netCDF files
   themselves, read with the netCDF library (not through the store),
 - xarray opens the new version through a brzarr session and sees them as
   variables on the store's own coordinates,
 - the version before is unchanged, a second run writes nothing, and a
   third run adds the steps that were missing.
"""
import json, os, shutil, sys, warnings

import netCDF4
import numpy as np
import xarray as xr

import brstore, brzarr, derive_bottom

warnings.simplefilter("ignore")
FIX, WORK = os.path.abspath(sys.argv[1]), os.path.abspath(sys.argv[2])
shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(WORK)
table = json.load(open(os.path.join(FIX, "arrays.json")))
rule = next(r for r in table["remap"] if r["target"] == "fixture")
resolve = lambda p: os.path.join(FIX, rule["replacement"] + p[len(rule["prefix"]):])  # noqa: E731

s = brstore.Store(os.path.join(WORK, "store"))
v1 = brstore.import_export(s, FIX, resolve=resolve)
m1 = s.manifest(v1)
a = s.array(m1, "temp")
nt, nz, ny, nx = a["shape"]
print(f"1. imported {FIX} as version {v1}: temp {a['shape']} in chunks {a['chunks']}")

# the oracle: every level from the source files, by the netCDF library, as stored
start = {}
for r in s.refs_rows(m1, "temp"):
    if r["path"]:
        start[r["path"]] = min(start.get(r["path"], r["dim_0"]), r["dim_0"])
paths = sorted(start, key=start.get)  # in the order they join along time
stored = []
for p in paths:
    with netCDF4.Dataset(resolve(p)) as nc:
        nc.set_auto_maskandscale(False)
        stored.append(np.asarray(nc["temp"][:]))
stored = np.concatenate(stored)
assert stored.shape == (nt, nz, ny, nx), stored.shape
missing = int(a["fill"])  # the netCDF fill value, which the export keeps as the array's fill
ok = stored != missing
want_level = np.where(ok.any(axis=1), nz - 1 - ok[:, ::-1].argmax(axis=1), -1)
want = np.where(want_level >= 0, np.take_along_axis(stored, np.maximum(want_level, 0)[:, None], axis=1)[:, 0], missing)
levels = sorted(set(want_level.ravel().tolist()))
assert len(levels) > 2, f"the fixture's bottom is at one level only: {levels}"
print(f"   the sources' own bottom lies at level indexes {levels} (-1: no value at any level)")


def held(version, name):
    """An array of (time, y, x) read through a version's rows, as stored."""
    m = s.manifest(version)
    d = s.array(m, name)
    out = np.full(d["shape"], brstore_number(d["fill"]), dtype=d["dtype"])
    got = set()
    for r in s.refs_rows(m, name):
        if not r["present"]:
            continue
        assert r["path"].startswith("packs/"), r["path"]
        t, j, i = r["dim_0"], r["dim_1"], r["dim_2"]
        block = np.frombuffer(brstore.decode_v2(d, s.fetch(m, r, None)), dtype=d["dtype"]).reshape(d["chunks"])
        y0, x0 = j * d["chunks"][1], i * d["chunks"][2]
        part = block[0, : d["shape"][1] - y0, : d["shape"][2] - x0]
        out[t, y0 : y0 + part.shape[0], x0 : x0 + part.shape[1]] = part
        got.add(t)
    return out, sorted(got)


brstore_number = derive_bottom.number

print("2. the bottom of the last two steps, derived through version 1 and committed")
args = [os.path.join(WORK, "store"), "--array", "temp", "--target", "fixture", "--base", FIX]
assert derive_bottom.main(args) == 0
v2 = s.latest()
assert v2 == v1 + 1
m2 = s.manifest(v2)
value, steps = held(v2, "temp_bottom")
level, lsteps = held(v2, "temp_bottom_level")
assert steps == lsteps == [nt - 2, nt - 1], steps
assert np.array_equal(value[steps], want[steps]) and np.array_equal(level[steps], want_level[steps])
d = s.array(m2, "temp_bottom")
assert d["attrs"]["derived"]["of"] == "temp" and d["dims"] == [a["dims"][0], a["dims"][2], a["dims"][3]]
assert all(d["attrs"][k] == v for k, v in a["attrs"].items() if k != "long_name") and d["dtype"] == a["dtype"], "the source's attributes and type carry over"
assert f"version {v1}" in m2["message"], m2["message"]
assert [x["array"] for x in m1["table"]["arrays"]] == [x["array"] for x in s.manifest(v1)["table"]["arrays"]]
assert not any(x["array"].startswith("temp_bottom") for x in s.manifest(v1)["table"]["arrays"]), "version 1 is as it was"
print(f"   version {v2}: temp_bottom and temp_bottom_level equal the sources' own bottom at steps {steps}")
print(f"   {m2['message']}")

print("3. xarray reads the derived arrays through a brzarr session")
ds = xr.open_zarr(brzarr.Session(s, version=v2, read_only=True, zarr_format=2), zarr_format=2, decode_times=False, mask_and_scale=False)
assert ds.temp_bottom.dims == (a["dims"][0], a["dims"][2], a["dims"][3]) and ds.temp_bottom.shape == (nt, ny, nx)
assert np.array_equal(ds.temp_bottom.values[steps], want[steps])
assert np.array_equal(ds.temp_bottom_level.values[steps], want_level[steps])
assert set(ds.temp_bottom.coords) >= {a["dims"][2], a["dims"][3]}, "on the store's own coordinates"
print(f"   temp_bottom{ds.temp_bottom.dims} on the coordinates of temp; one chunk a step where temp has {nz * -(-ny // a['chunks'][2]) * -(-nx // a['chunks'][3])}")

print("4. a second run writes nothing; a third adds the steps that were missing")
assert derive_bottom.main(args) == 0 and s.latest() == v2
assert derive_bottom.main(args + ["--first", "0", "--steps", str(nt)]) == 0
v3 = s.latest()
assert v3 == v2 + 1
value, steps = held(v3, "temp_bottom")
level, _ = held(v3, "temp_bottom_level")
assert steps == list(range(nt)) and np.array_equal(value, want) and np.array_equal(level, want_level)
row2, row3 = s.chunk_row(m2, "temp_bottom", (nt - 1, 0, 0)), s.chunk_row(s.manifest(v3), "temp_bottom", (nt - 1, 0, 0))
assert (row2["path"], row2["offset"], row2["size"]) == (row3["path"], row3["offset"], row3["size"]), "steps already held keep their rows"
print(f"   version {v3}: every step held, equal to the sources' own bottom; the rows of version {v2} are unchanged in it")
print("ALL STEPS PASSED")
