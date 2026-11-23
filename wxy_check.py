"""Write oracle manifests for the checkouts, in the form wxylib/core's
refs_windows example takes, so the unmodified Rust reader can be held to them.

  python wxy_check.py <wxylib/fixtures/mdimrefs> <workdir>

sst windows: the fixture's GDAL 3.13 reads. sst_x2 windows: the values the
demo computed and wrote, recomputed here from the GDAL reads.
"""
import hashlib, json, os, sys
import numpy as np

FIX, WORK = sys.argv[1], sys.argv[2]
man = json.load(open(os.path.join(FIX, "manifest.json")))
h = lambda a: hashlib.sha256(np.ascontiguousarray(a.astype("<i2")).tobytes()).hexdigest()

def avg(a, fill=-999):
    t = a.reshape(a.shape[0], a.shape[1], a.shape[2] // 2, 2, a.shape[3] // 2, 2).astype("f8")
    ok = t != fill; n = ok.sum(axis=(3, 5))
    return np.rint(np.where(n > 0, np.where(ok, t, 0).sum(axis=(3, 5)) / np.maximum(n, 1), fill)).astype("<i2")

sst = {}
for w in man["windows"]:
    if w["array"] == "sst":
        sst[w["file"]] = (w, np.fromfile(os.path.join(FIX, w["file"]), dtype="<i2").reshape(w["count"]))
out = {}
out["held-sst"] = dict(array="sst", oracle={os.path.basename(f): dict(start=w["start"], count=w["count"], sha256=h(v)) for f, (w, v) in sst.items()})
day0 = sst["oracle/sst-w0.bin"][1]
out["level-a-sst_x2"] = dict(array="sst_x2", oracle={"day0-average": dict(start=[0, 0, 0, 0], count=[1, 1, 18, 36], sha256=h(avg(day0)))})
out["latest-sst_x2"] = dict(array="sst_x2", oracle={"day0-nearest": dict(start=[0, 0, 0, 0], count=[1, 1, 18, 36], sha256=h(day0[:, :, ::2, ::2]))})
for k, v in out.items():
    json.dump(v, open(os.path.join(WORK, k + ".json"), "w"), indent=1)
print(sorted(out))
