"""Read committed versions with zarr-python over fsspec, through the kerchunk
JSON render, and compare with the oracle manifests wxy_check.py wrote.

  python zarr_check.py <workdir>      (after demo.py and wxy_check.py)
"""
import hashlib, json, os, sys, warnings

import fsspec
import numpy as np
import zarr
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper

import brstore

warnings.simplefilter("ignore")
WORK = os.path.abspath(sys.argv[1])
s = brstore.Store(os.path.join(WORK, "store"))
mirror = os.path.join(WORK, "mirror")
url = lambda p: s.o.local(p) if p.startswith("packs/") else os.path.join(mirror, p[len("/mirror/oisst/"):])
bad = 0
for tag, man in (("held", "held-sst"), ("level-a", "level-a-sst_x2"), ("latest", "latest-sst_x2")):
    ref = os.path.join(WORK, f"kerchunk-{tag}.json")
    brstore.render_kerchunk_json(s, s.latest() if tag == "latest" else tag, ref, url)
    want = json.load(open(os.path.join(WORK, man + ".json")))
    fs = AsyncFileSystemWrapper(fsspec.filesystem("reference", fo=ref))
    grp = zarr.open_group(zarr.storage.FsspecStore(fs, read_only=True, path=""), mode="r", zarr_format=2)
    arr = grp[want["array"]]
    time = grp["time"][:]  # a referenced coordinate: chunks in a pack, read like any array
    ok = list(time) == [1339.0 + i for i in range(arr.shape[0])]
    bad += not ok
    print(f"{tag} time coordinate {[float(x) for x in time]}: {'as written' if ok else 'DIFFERENT'}")
    for name, w in want["oracle"].items():
        got = arr[tuple(slice(a, a + n) for a, n in zip(w["start"], w["count"]))]
        ok = hashlib.sha256(np.ascontiguousarray(got.astype("<i2")).tobytes()).hexdigest() == w["sha256"]
        bad += not ok
        print(f"{tag} {name}: {'identical' if ok else 'DIFFERENT'}")
sys.exit(1 if bad else 0)
