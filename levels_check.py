"""derive_levels.py on a fixture store, every result an assert.

  python levels_check.py <fixture dir> <workdir>

The fixture is as bottom_check.py's (wxylib/fixtures mdimrefs/bluelink-synthetic:
temp and salt of time, level, y, x, netCDF sources beside the export). It is
imported as version 1; the levels of temp are derived for the last two steps
and committed; then

 - each level (the last has an odd edge) equals the same means computed, cell by cell, from the netCDF
   files themselves read with the netCDF library (not through the store),
 - xarray opens the new version through a brzarr session and sees each level
   as a variable on its own, coarser, coordinates,
 - the version before is unchanged, a second run writes nothing, and a
   third run adds the steps that were missing without touching the others.
"""
import json, os, shutil, sys, warnings

import netCDF4
import numpy as np
import xarray as xr

import brstore, brzarr, derive_levels

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
ydim, xdim = a["dims"][2:]
print(f"1. imported {FIX} as version {v1}: temp {a['shape']} in chunks {a['chunks']}")

# the oracle: the source files by the netCDF library, as stored, and each
# level from the one before by a loop over cells (not derive_levels' arithmetic)
start = {}
for r in s.refs_rows(m1, "temp"):
    if r["path"]:
        start[r["path"]] = min(start.get(r["path"], r["dim_0"]), r["dim_0"])
stored = []
for p in sorted(start, key=start.get):
    with netCDF4.Dataset(resolve(p)) as nc:
        nc.set_auto_maskandscale(False)
        stored.append(np.asarray(nc["temp"][:]))
stored = np.concatenate(stored)
assert stored.shape == (nt, nz, ny, nx), stored.shape
missing = int(a["fill"])
assert (stored == missing).any() and (stored != missing).any(), "the fixture has cells with and without a value"


def by_cells(v):
    h, w = -(-v.shape[-2] // 2), -(-v.shape[-1] // 2)
    out = np.full(v.shape[:-2] + (h, w), missing, dtype=v.dtype)
    for j in range(h):
        for i in range(w):
            cell = v[..., 2 * j : 2 * j + 2, 2 * i : 2 * i + 2].reshape(v.shape[:-2] + (-1,)).astype("f8")
            ok = cell != missing
            n = ok.sum(axis=-1)
            mean = np.rint(np.where(ok, cell, 0).sum(axis=-1) / np.maximum(n, 1))
            out[..., j, i] = np.where(n > 0, mean, missing)
    return out


NAMES = ["temp_x2", "temp_x4", "temp_x8"]  # the last from an odd width: 15 cells to 8
want, v = {}, stored
for nm in NAMES:
    v = by_cells(v)
    want[nm] = v
print(f"   the sources' own levels: " + ", ".join(f"{nm} {w.shape[2]} by {w.shape[3]}" for nm, w in want.items()))


def held(version, name):
    """An array read through a version's rows, as stored, and the steps it has."""
    m = s.manifest(version)
    d = s.array(m, name)
    out = np.full(d["shape"], missing, dtype=d["dtype"])
    got = set()
    cz, cy, cx = d["chunks"][1:]
    for r in s.refs_rows(m, name):
        if not r["present"]:
            continue
        assert r["path"].startswith("packs/"), r["path"]
        t, z, j, i = (r[f"dim_{k}"] for k in range(4))
        block = np.frombuffer(brstore.decode_v2(d, s.fetch(m, r, None)), dtype=d["dtype"]).reshape(d["chunks"])[0]
        part = block[: d["shape"][1] - z * cz, : d["shape"][2] - j * cy, : d["shape"][3] - i * cx]
        out[t, z * cz : z * cz + part.shape[0], j * cy : j * cy + part.shape[1], i * cx : i * cx + part.shape[2]] = part
        got.add(t)
    return out, sorted(got)


print("2. three levels of the last two steps, derived through version 1 and committed")
args = [os.path.join(WORK, "store"), "--array", "temp", "--levels", "3", "--target", "fixture", "--base", FIX]
assert derive_levels.main(args) == 0
v2 = s.latest()
assert v2 == v1 + 1
m2 = s.manifest(v2)
for k, nm in enumerate(NAMES):
    got, steps = held(v2, nm)
    assert steps == [nt - 2, nt - 1], steps
    assert got.shape == want[nm].shape and np.array_equal(got[steps], want[nm][steps]), nm
    d = s.array(m2, nm)
    f = 2 ** (k + 1)
    assert d["attrs"]["derived"] == dict(d["attrs"]["derived"], of=NAMES[k - 1] if k else "temp", source="temp", factor=[f, f])
    assert d["dims"] == a["dims"][:2] + [f"{ydim}_x{f}", f"{xdim}_x{f}"] and d["dtype"] == a["dtype"]
    assert all(d["attrs"][key] == val for key, val in a["attrs"].items()), "the source's attributes carry over (its packing with them)"
assert f"version {v1}" in m2["message"], m2["message"]
assert not any(x["array"].startswith("temp_x") for x in s.manifest(v1)["table"]["arrays"]), "version 1 is as it was"
print(f"   version {v2}: {' and '.join(NAMES)} equal the sources' own levels at steps {steps}")
print(f"   {m2['message']}")

print("3. xarray reads the levels through a brzarr session, each on its own coordinates")
ds = xr.open_zarr(brzarr.Session(s, version=v2, read_only=True, zarr_format=2), zarr_format=2, decode_times=False, mask_and_scale=False)
y, x = np.asarray(ds[ydim].values, dtype="f8"), np.asarray(ds[xdim].values, dtype="f8")
for k, nm in enumerate(NAMES):
    f = 2 ** (k + 1)
    yd, xd = f"{ydim}_x{f}", f"{xdim}_x{f}"
    assert ds[nm].dims == (a["dims"][0], a["dims"][1], yd, xd) and np.array_equal(ds[nm].values[steps], want[nm][steps])
    for full, coarse in ((y, ds[yd].values), (x, ds[xd].values)):
        expect = [full[i * f : (i + 1) * f].mean() for i in range(len(coarse))]
        assert len(coarse) == -(-len(full) // f) and np.allclose(coarse, expect), (nm, coarse, expect)
print(f"   " + ", ".join(f"{nm}{tuple(ds[nm].shape)}" for nm in NAMES) + f" beside temp{tuple(ds.temp.shape)}; coordinates are the means of the cells joined")

print("4. a second run writes nothing; a third adds the steps that were missing")
assert derive_levels.main(args) == 0 and s.latest() == v2
assert derive_levels.main(args + ["--first", "0", "--steps", str(nt)]) == 0
v3 = s.latest()
assert v3 == v2 + 1
for nm in NAMES:
    got, steps = held(v3, nm)
    assert steps == list(range(nt)) and np.array_equal(got, want[nm]), nm
key = (nt - 1, 0, 0, 0)
row2, row3 = s.chunk_row(m2, "temp_x2", key), s.chunk_row(s.manifest(v3), "temp_x2", key)
assert (row2["path"], row2["offset"], row2["size"]) == (row3["path"], row3["offset"], row3["size"]), "steps already held keep their rows"
print(f"   version {v3}: every step held at every level, equal to the sources' own; the rows of version {v2} are unchanged in it")
print("ALL STEPS PASSED")
