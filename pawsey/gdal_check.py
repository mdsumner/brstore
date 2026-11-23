"""Read committed versions with GDAL itself (3.11 or later), through a
kerchunk JSON render of each tagged version, and compare with the oracle.

  python gdal_check.py <wxylib/fixtures/mdimrefs> <workdir>

Run demo.py and wxy_check.py into <workdir> first (a local store). The sst
windows are the fixture's GDAL reads of the source files; the sst_x2
windows are the values the demo wrote. NOT yet run anywhere: the box this
was written on only had GDAL 3.8.4.
"""
import hashlib, json, os, sys

import numpy as np
from osgeo import gdal

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import brstore

gdal.UseExceptions()
FIX, WORK = os.path.abspath(sys.argv[1]), os.path.abspath(sys.argv[2])
print("GDAL", gdal.__version__)
s = brstore.Store(os.path.join(WORK, "store"))
mirror = os.path.join(WORK, "mirror")


def url_for(path):
    if path.startswith("packs/"):
        return s.o.local(path)
    assert path.startswith("/mirror/oisst/"), path
    return os.path.join(mirror, path[len("/mirror/oisst/"):])


bad = 0
for tag, man in (("held", "held-sst"), ("level-a", "level-a-sst_x2"), ("latest", "latest-sst_x2")):
    version = s.latest() if tag == "latest" else tag
    ref = os.path.join(WORK, f"kerchunk-{tag}.json")
    n = brstore.render_kerchunk_json(s, version, ref, url_for)
    want = json.load(open(os.path.join(WORK, man + ".json")))
    ds = gdal.OpenEx('ZARR:"/vsikerchunk_json_ref/{%s}"' % ref, gdal.OF_MULTIDIM_RASTER)
    arr = ds.GetRootGroup().OpenMDArray(want["array"])
    print(f"{tag}: {n} keys; {want['array']} shape {[d.GetSize() for d in arr.GetDimensions()]}")
    # the time coordinate is a referenced array in a pack: GDAL must read its values too
    t = ds.GetRootGroup().OpenMDArray("time").ReadAsArray()
    ok = [float(x) for x in t] == [1339.0 + i for i in range(arr.GetDimensions()[0].GetSize())]
    bad += not ok
    print(f"   time coordinate {[float(x) for x in t]}: {'as written' if ok else 'DIFFERENT'}")
    for name, w in want["oracle"].items():
        got = arr.ReadAsArray(array_start_idx=w["start"], count=w["count"])
        h = hashlib.sha256(np.ascontiguousarray(got.astype("<i2")).tobytes()).hexdigest()
        ok = h == w["sha256"]
        bad += not ok
        print(f"   {name}: {'identical' if ok else 'DIFFERENT'}")
print("GDAL reads every window identically" if not bad else f"{bad} windows differ")
sys.exit(1 if bad else 0)
