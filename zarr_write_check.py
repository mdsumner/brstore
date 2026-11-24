"""xarray writes into a brstore store through brzarr.Session, in Zarr format
3 and then format 2, and the versions it makes are read back by other
readers. Every result is an assert.

  python zarr_write_check.py <workdir> [<demo workdir>]

With a demo workdir (after demo.py), also writes an xarray variable into
the store demo.py made, beside its references to netCDF files.
BRSTORE_ROOT=s3://bucket/prefix puts the stores on object storage (steps 1
to 5; the prefix must be empty).
"""
import json, os, shutil, sys, warnings

import duckdb
import fsspec
import numcodecs
import numpy as np
import xarray as xr
import zarr
import zarr.codecs.numcodecs as v3codecs
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper

import brstore, brzarr

warnings.simplefilter("ignore")
WORK = os.path.abspath(sys.argv[1])
shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(WORK)
ROOT = os.environ.get("BRSTORE_ROOT") or WORK  # or s3://bucket/prefix
ROW_INFO = "COMPRESSION=DEFLATE; FILTER=SHUFFLE; ENDIANNESS=LITTLE"
# the same bytes either way: little-endian int16, shuffled, deflated
CODEC = {2: dict(compressors=[numcodecs.Zlib(1)], filters=[numcodecs.Shuffle(2)]),
         3: dict(compressors=[v3codecs.Shuffle(elementsize=2), v3codecs.Zlib(level=1)])}


def day(t0, n, seed):
    rng = np.random.default_rng(seed)
    sst = np.round(rng.uniform(-2, 30, (n, 4, 8)), 2)
    sst[:, 0, :3] = np.nan
    return xr.Dataset(
        dict(sst=(("time", "lat", "lon"), sst, dict(units="Celsius")),
             wind=(("time", "lat", "lon"), rng.normal(size=(n, 4, 8)).astype("f4"))),  # wind: whatever xarray defaults to
        coords=dict(time=("time", np.arange(t0, t0 + n, dtype="f8"), dict(units="days since 1978-01-01")),
                    lat=("lat", np.linspace(-60, 60, 4)), lon=("lon", np.linspace(0, 315, 8))),
        attrs=dict(title="written by xarray"),
    )


def walk(fmt):
    s = brstore.Store(ROOT.rstrip("/") + f"/store-v{fmt}")
    enc = dict(sst=dict(CODEC[fmt], dtype="<i2", scale_factor=0.01, _FillValue=-999, chunks=(1, 4, 8)))
    session = lambda **kw: brzarr.Session(s, zarr_format=fmt, **kw)

    def opened(version, f=fmt, **kw):
        z = brzarr.Session(s, version=version, read_only=True, zarr_format=f)
        return xr.open_zarr(z, zarr_format=f, decode_times=False, **kw).load()

    print(f"== Zarr format {fmt}")
    print("1. xarray.to_zarr into an empty store")
    a = day(0, 3, 1)
    z = session(writer="xarray")
    a.to_zarr(z, zarr_format=fmt, encoding=enc)
    assert s.latest() == 0 and not s.o.list("packs"), "nothing is stored before commit"
    v1 = z.commit("three days")
    assert v1 == 1 and len(s.o.list("packs")) == 1
    got = opened(v1)
    xr.testing.assert_allclose(got, a, atol=0.006)
    assert got.attrs == a.attrs and got.sst.attrs["units"] == "Celsius"
    m = s.manifest(v1)
    print(f"   version {v1}: {len(m['table']['arrays'])} arrays, one pack of {s.o.size(s.o.list('packs')[0])} bytes")
    row = s.chunk_row(m, "sst", (1, 0, 0))
    assert row["path"].startswith("packs/") and row["info"] == ROW_INFO, row
    assert s.array(m, "sst")["codec"] == {"compressor": {"id": "zlib", "level": 1}, "filters": [{"id": "shuffle", "elementsize": 2}]}
    print(f"   sst chunk (1,0,0) is the row {row['path'][:20]}... +{row['offset']} {row['size']} bytes, {row['info']}")
    print(f"   wind, left to xarray's default, is held as {json.dumps(s.array(m, 'wind')['codec'])}")

    print("2. append along time: one commit, the old version unchanged")
    b = day(3, 2, 2)
    b.to_zarr(z, zarr_format=fmt, append_dim="time")
    assert s.latest() == v1
    v2 = z.commit("two more days")
    xr.testing.assert_allclose(opened(v2), xr.concat([a, b], "time"), atol=0.006)
    xr.testing.assert_allclose(opened(v1), a, atol=0.006)
    assert opened(v2).sizes["time"] == 5 and opened(v1).sizes["time"] == 3
    changed = s.manifest(v2)["meta_changed"]
    assert "lat" not in changed and "lon" not in changed, changed
    print(f"   version {v2} has 5 days, version {v1} still has 3; descriptions changed: {changed}")

    print("3. two sessions write different days, then the same day")
    za, zb = session(writer="A"), session(writer="B")
    ra, rb = day(0, 1, 11), day(4, 1, 12)
    ra.drop_vars(["lat", "lon"]).to_zarr(za, zarr_format=fmt, region=dict(time=slice(0, 1)))
    rb.drop_vars(["lat", "lon"]).to_zarr(zb, zarr_format=fmt, region=dict(time=slice(4, 5)))
    va = za.commit("A: day 0")
    vb = zb.commit("B: day 4")
    assert zb.rebased_over == 1
    got = opened(vb)
    np.testing.assert_allclose(got.sst[0], ra.sst[0], atol=0.006)
    np.testing.assert_allclose(got.sst[4], rb.sst[0], atol=0.006)
    print(f"   A made version {va}; B rebased over it to version {vb}; both days are there")
    zc, zd = session(writer="C"), session(writer="D")
    for zz, seed in ((zc, 21), (zd, 22)):
        day(2, 1, seed).drop_vars(["lat", "lon"]).to_zarr(zz, zarr_format=fmt, region=dict(time=slice(2, 3)))
    vc = zc.commit("C: day 2")
    try:
        zd.commit("D: day 2")
        raise SystemExit("the second writer of day 2 should have been refused")
    except brstore.Conflict as e:
        print(f"   C made version {vc}; D refused: {e}")
    assert s.latest() == vc

    print("4. the same version through the other Zarr format")
    other = 5 - fmt
    # stored values: xarray spells a missing value differently in the two formats (fill_value against an attribute)
    xr.testing.assert_identical(opened(vc, other, mask_and_scale=False).drop_attrs(),
                                opened(vc, mask_and_scale=False).drop_attrs())
    print(f"   written as format {fmt}, read as format {other}: identical stored values")

    print("5. the same version through readers that know nothing of this module")
    ref = os.path.join(WORK, f"kerchunk-v{fmt}.json")
    brstore.render_kerchunk_json(s, vc, ref, lambda p: s.o.local(p))
    fs = AsyncFileSystemWrapper(fsspec.filesystem("reference", fo=ref))
    g = zarr.open_group(zarr.storage.FsspecStore(fs, read_only=True, path=""), mode="r", zarr_format=2)
    want = opened(vc)
    raw = np.where(np.isnan(want.sst.values), -999, np.round(want.sst.values / 0.01)).astype("<i2")
    assert np.array_equal(g["sst"][:], raw) and np.array_equal(g["time"][:], want.time.values)
    assert np.array_equal(g["wind"][:], want.wind.values)
    print("   zarr-python over the kerchunk render: identical")
    out = os.path.join(WORK, f"checkout-v{fmt}")
    s.checkout(vc, out, "local", [])
    t = json.load(open(os.path.join(out, "arrays.json")))
    assert {x["array"]: x["storage"] for x in t["arrays"]} == dict(lat="inline", lon="inline", sst="referenced",
                                                                    time="inline", wind="referenced")
    n, packs = duckdb.connect().execute(
        f"select count(*), count(distinct path) from '{out}/refs_sst.parquet' where present").fetchone()
    assert n == 5
    print(f"   three-file checkout: sst has {n} rows over {packs} packs (DuckDB); coordinates rendered inline")

    if len(sys.argv) > 2 and not ROOT.startswith("s3://"):
        print("6. an xarray write beside references to netCDF files")
        DEMO = os.path.abspath(sys.argv[2])
        store2 = os.path.join(WORK, f"store-demo-v{fmt}")
        shutil.copytree(os.path.join(DEMO, "store"), store2)
        s2 = brstore.Store(store2)
        base = s2.latest()
        mirror = os.path.join(DEMO, "mirror")
        resolve = lambda p: os.path.join(mirror, p[len("/mirror/oisst/"):])
        kw = dict(zarr_format=fmt, decode_times=False, mask_and_scale=False, drop_variables=["scratch"])
        z2 = brzarr.Session(s2, writer="xarray", resolve=resolve, zarr_format=fmt)
        ds = xr.open_zarr(z2, **kw)
        nt = ds.sizes["time"]
        anom = (ds.sst.isel(zlev=0) - ds.sst.isel(zlev=0).mean("time")).astype("f4").rename("sst_anom").load()
        anom.attrs = dict(long_name="sst minus its mean over time, raw counts")
        anom.to_dataset().drop_vars(["time", "lat", "lon", "zlev"], errors="ignore").to_zarr(
            z2, zarr_format=fmt, mode="a", encoding=dict(sst_anom=dict(chunks=(1, 36, 72))))
        v = z2.commit("sst_anom from xarray")
        m2 = s2.manifest(v)
        assert m2["meta_changed"] == ["sst_anom"], m2["meta_changed"]
        kinds = {}
        for x in m2["table"]["arrays"]:
            if x["storage"] == "referenced":
                for r in s2.refs_rows(m2, x["array"]):
                    if r["present"]:
                        kinds.setdefault(x["array"], set()).add("pack" if r["path"].startswith("packs/") else "netCDF")
        assert kinds["sst_anom"] == {"pack"} and "netCDF" in kinds["sst"]
        back = xr.open_zarr(brzarr.Session(s2, version=v, resolve=resolve, zarr_format=fmt), **kw)
        assert np.allclose(back.sst_anom.values, anom.values) and back.sst_anom.shape == (nt, 36, 72)
        assert s2.manifest(base)["table"]["arrays"] != m2["table"]["arrays"]
        print(f"   version {v}: sst_anom is {nt} chunks in a pack; sst is still {sorted(kinds['sst'])}; one table; "
              f"only sst_anom's description changed")


walk(3)
walk(2)
print("ALL STEPS PASSED")
