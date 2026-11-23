"""Step 3: the m16 history at its real row counts, through three stores.

  python scale.py <workdir> [mur] [oisst] [icechunk] [delta]

mur    GHRSST MUR analysed_sst 2024 as m16 replayed it: 2,556 chunks a day,
       182 days bulk (465,192 refs), 184 daily commits, one day rescanned.
oisst  OISST-shaped: 16,387 daily sources, 4 arrays, one chunk a day each;
       16,000 days bulk, 387 daily commits.

The rows are SYNTHETIC: real shapes and counts, made-up offsets and sizes.
Nothing is read back as pixels here; this measures the cost of holding
history, which depends on row counts and layout, not on the bytes.
"""

import json
import os
import shutil
import statistics
import sys
import time

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import brstore
from brstore import Store

WORK = os.path.abspath(sys.argv[1])
WHAT = set(sys.argv[2:]) or {"mur", "oisst", "icechunk", "delta"}
os.makedirs(WORK, exist_ok=True)
OUT = {}
INFO = "COMPRESSION=ZSTD; PREDICTOR=2; ENDIANNESS=LITTLE"
BUCKET = "s3://us-west-2.opendata.source.coop/ausantarctic/ghrsst-mur-v2/"


def du(path):
    n = b = 0
    for d, _, fs in os.walk(path):
        for f in fs:
            n += 1
            b += os.path.getsize(os.path.join(d, f))
    return n, b


def classes(s):
    return {p: (len(s.o.list(p)), sum(s.o.size(k) for k in s.o.list(p))) for p in ("versions", "meta", "lists", "frags", "packs", "tags")}


def mb(b):
    return round(b / 1e6, 2)


# ------------------------------------------------- the MUR-shaped rows

NY, NX = 71, 36  # 2,556 chunks a day


def mur_path(day):
    return f"{BUCKET}2024/{day // 31 + 1:02d}/2024{day:03d}090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1_analysed_sst.tif"


def mur_day(day, seed=0):
    """One day's rows as columns: (dim_1, dim_2, offset, size)."""
    rng = np.random.default_rng(day * 7919 + seed)
    size = rng.lognormal(11.6, 0.5, NY * NX).astype("i8") + 200
    off = 4096 + np.concatenate([[0], np.cumsum(size)[:-1]])
    d1, d2 = np.divmod(np.arange(NY * NX), NX)
    return d1, d2, off, size


def mur_rows(day, seed=0):
    d1, d2, off, size = mur_day(day, seed)
    p = mur_path(day)
    return [dict(dim_0=day, dim_1=int(a), dim_2=int(b), present=1, path=p, offset=int(o), size=int(z), info=INFO, data=None)
            for a, b, o, z in zip(d1, d2, off, size)]


def mur_table(days, seed_for=lambda d: 0):
    cols = {k: [] for k in ("dim_0", "dim_1", "dim_2", "path", "offset", "size")}
    for d in days:
        d1, d2, off, size = mur_day(d, seed_for(d))
        cols["dim_0"].append(np.full(NY * NX, d))
        cols["dim_1"].append(d1), cols["dim_2"].append(d2), cols["offset"].append(off), cols["size"].append(size)
        cols["path"].append(np.full(NY * NX, mur_path(d)))
    c = {k: np.concatenate(v) for k, v in cols.items()}
    n = len(c["dim_0"])
    return pa.table(dict(dim_0=c["dim_0"], dim_1=c["dim_1"], dim_2=c["dim_2"], present=pa.array(np.ones(n, "i1")),
                         path=pa.array(c["path"]).dictionary_encode(), offset=c["offset"], size=c["size"],
                         info=pa.array([INFO] * n).dictionary_encode(), data=pa.nulls(n, pa.binary())))


MUR_TABLE = dict(format_version="byterefs/0.1", remap=[], arrays=[dict(
    array="analysed_sst", kind="data", storage="referenced", dims=["time", "y", "x"], shape=[366, NY * 512, NX * 1024],
    chunks=[1, 512, 1024], dtype="<i2", fill="-32768",
    codec={"compressor": {"id": "zstd", "level": 9}, "filters": [{"id": "imagecodecs_delta", "axis": -1, "dist": 1}]},
    attrs={}, affine=None, values=None)])


def src_row(day):
    return dict(source=mur_path(day), size=335_000_000 + day, mtime=1_700_000_000 + day * 86400, etag=f'"{day:032x}"')


def flat_export_size(days):
    p = os.path.join(WORK, "flat.parquet")
    pq.write_table(mur_table(days), p, compression="zstd")
    return os.path.getsize(p)


def mur_brstore(label, compact_every=None, fanout=None):
    root = os.path.join(WORK, "br-" + label)
    shutil.rmtree(root, ignore_errors=True)
    s = Store(root, fanout=fanout)
    r = {}
    t0 = time.perf_counter()
    brstore.init(s, MUR_TABLE, {"analysed_sst": [x for d in range(182) for x in mur_rows(d)]},
                 [src_row(d) for d in range(182)], part_size=1)
    r["bulk_s"] = round(time.perf_counter() - t0, 2)
    s.tag("bulk", 1)
    r["after_bulk"] = classes(s)
    times, man = [], []
    for day in range(182, 366):
        t0 = time.perf_counter()
        t = s.begin("daily")
        for x in mur_rows(day):
            t.put_ref("analysed_sst", (x["dim_0"], x["dim_1"], x["dim_2"]), x["path"], x["offset"], x["size"], INFO)
        t.put_source(**src_row(day))
        if compact_every and day % compact_every == 0:
            t.compact("analysed_sst", day - compact_every, day)
            t.compact_sources()
        v = t.commit(f"day {day}")
        times.append(time.perf_counter() - t0)
        man.append(s.o.size(f"versions/{v:08d}.json"))
    s.tag("year", s.latest())
    r["daily_commit_s_median"] = round(statistics.median(times), 4)
    r["daily_commit_s_max"] = round(max(times), 4)
    r["manifest_bytes_first_last"] = [man[0], man[-1]]
    # the rescan: day 100 re-read, every offset changed
    t0 = time.perf_counter()
    t = s.begin("rescan")
    for x in mur_rows(100, seed=1):
        t.put_ref("analysed_sst", (x["dim_0"], x["dim_1"], x["dim_2"]), x["path"], x["offset"], x["size"], INFO)
    v = t.commit("rescan day 100")
    r["rescan_s"] = round(time.perf_counter() - t0, 4)
    s.tag("rescan", v)
    r["versions"] = s.latest()
    r["all_history"] = classes(s)
    r["all_history_mb"] = mb(sum(b for _, b in r["all_history"].values()))
    # table operations, by SQL over the fragment lists
    con = duckdb.connect()
    my, mr, mbk = s.manifest("year"), s.manifest("rescan"), s.manifest("bulk")
    t0 = time.perf_counter()
    n = con.execute("SELECT count(*), count(DISTINCT (dim_0, dim_1, dim_2)) FROM read_parquet(?)", [s.frag_paths(mr, "analysed_sst")]).fetchone()
    r["scan_latest_s"] = round(time.perf_counter() - t0, 3)
    assert n == (935496, 935496), n
    q = "SELECT count(*) FROM (SELECT * EXCLUDE (data) FROM read_parquet(?) EXCEPT SELECT * EXCLUDE (data) FROM read_parquet(?))"
    t0 = time.perf_counter()
    r["diff_bulk_to_year_rows"] = con.execute(q, [s.frag_paths(my, "analysed_sst"), s.frag_paths(mbk, "analysed_sst")]).fetchone()[0]
    r["diff_s"] = round(time.perf_counter() - t0, 3)
    r["diff_year_to_rescan_rows"] = con.execute(q, [s.frag_paths(mr, "analysed_sst"), s.frag_paths(my, "analysed_sst")]).fetchone()[0]
    assert r["diff_bulk_to_year_rows"] == 470304 and r["diff_year_to_rescan_rows"] == 2556
    r["rows_at_tags"] = [con.execute("SELECT count(*) FROM read_parquet(?)", [s.frag_paths(m, "analysed_sst")]).fetchone()[0] for m in (mbk, my, mr)]
    ts = []
    for k in [(5, 3, 3), (100, 40, 20), (200, 70, 35), (365, 0, 0)] * 5:
        t0 = time.perf_counter()
        m = s.manifest(s.latest())
        assert s.chunk_row(m, "analysed_sst", k)["present"] == 1
        ts.append(time.perf_counter() - t0)
    r["one_chunk_by_key_ms"] = round(1000 * statistics.median(ts), 2)
    # keep only what the three tags reach
    keep = sorted({json.loads(s.o.get(k))["version"] for k in s.o.list("tags")})
    t0 = time.perf_counter()
    dead, freed = s.gc(keep)
    r["gc_s"] = round(time.perf_counter() - t0, 2)
    r["three_tags"] = classes(s)
    r["three_tags_mb"] = mb(sum(b for _, b in r["three_tags"].values()))
    r["latest_fragments"] = len(s.frags(mr, "analysed_sst"))
    r["latest_only_frag_mb"] = mb(sum(os.path.getsize(p) for p in s.frag_paths(mr, "analysed_sst")))
    OUT["mur_brstore_" + label] = r
    print(label, json.dumps(r))


# ------------------------------------------------- Icechunk, the same history


def mur_icechunk(split):
    import icechunk as ic
    import zarr

    root = os.path.join(WORK, f"ic-{split}")
    shutil.rmtree(root, ignore_errors=True)
    cfg = ic.RepositoryConfig.default()
    cfg.set_virtual_chunk_container(ic.VirtualChunkContainer("s3://us-west-2.opendata.source.coop/", ic.s3_store(region="us-west-2", anonymous=True)))
    cfg.manifest = ic.ManifestConfig(splitting=ic.ManifestSplittingConfig.from_dict(
        {ic.ManifestSplitCondition.AnyArray(): {ic.ManifestSplitDimCondition.Axis(0): split}}))
    repo = ic.Repository.create(ic.local_filesystem_storage(root), cfg)
    r = {}

    def specs(day, seed=0):
        d1, d2, off, size = mur_day(day, seed)
        p = mur_path(day)
        return [ic.VirtualChunkSpec(index=[day, int(a), int(b)], location=p, offset=int(o), length=int(z)) for a, b, o, z in zip(d1, d2, off, size)]

    t0 = time.perf_counter()
    ses = repo.writable_session("main")
    zarr.create_array(ses.store, name="analysed_sst", shape=(366, NY, NX), chunks=(1, 1, 1), dtype="int16",
                      dimension_names=["time", "y", "x"], fill_value=-32768, compressors=None)
    for d in range(182):
        ses.store.set_virtual_refs("analysed_sst", specs(d), validate_containers=False)
    ses.commit("bulk")
    r["bulk_s"] = round(time.perf_counter() - t0, 2)
    r["after_bulk_mb"] = mb(du(root)[1])
    times = []
    for d in range(182, 366):
        t0 = time.perf_counter()
        ses = repo.writable_session("main")
        ses.store.set_virtual_refs("analysed_sst", specs(d), validate_containers=False)
        ses.commit(f"day {d}")
        times.append(time.perf_counter() - t0)
    r["daily_commit_s_median"] = round(statistics.median(times), 4)
    r["daily_commit_s_max"] = round(max(times), 4)
    t0 = time.perf_counter()
    ses = repo.writable_session("main")
    ses.store.set_virtual_refs("analysed_sst", specs(100, 1), validate_containers=False)
    ses.commit("rescan day 100")
    r["rescan_s"] = round(time.perf_counter() - t0, 4)
    r["all_history_objects"], b = du(root)
    r["all_history_mb"] = mb(b)
    OUT[f"mur_icechunk_split{split}"] = r
    print("icechunk split", split, json.dumps(r))


# ------------------------------------------------- Delta, the same history


def mur_delta():
    from deltalake import DeltaTable, write_deltalake

    root = os.path.join(WORK, "delta")
    shutil.rmtree(root, ignore_errors=True)
    plain = lambda t: t.drop_columns(["data"]).cast(pa.schema([(f.name, pa.string() if pa.types.is_dictionary(f.type) else f.type) for f in t.schema if f.name != "data"]))
    r = {}
    t0 = time.perf_counter()
    for d in range(182):  # a file per day, as m16 section 3b found necessary
        write_deltalake(root, plain(mur_table([d])), mode="append")
    r["bulk_s"] = round(time.perf_counter() - t0, 2)
    times = []
    for d in range(182, 366):
        t0 = time.perf_counter()
        write_deltalake(root, plain(mur_table([d])), mode="append")
        times.append(time.perf_counter() - t0)
    r["daily_commit_s_median"] = round(statistics.median(times), 4)
    t0 = time.perf_counter()
    write_deltalake(root, plain(mur_table([100], lambda d: 1)), mode="overwrite", predicate="dim_0 = 100")
    r["rescan_s"] = round(time.perf_counter() - t0, 4)
    r["versions"] = DeltaTable(root).version()
    r["all_history_objects"], b = du(root)
    r["all_history_mb"] = mb(b)
    r["log_mb"] = mb(du(os.path.join(root, "_delta_log"))[1])
    OUT["mur_delta"] = r
    print("delta", json.dumps(r))


# ------------------------------------------------- OISST-shaped: many sources, few chunks each

OARR = ["sst", "anom", "err", "ice"]


def oisst_path(d):
    return f"/vsicurl/https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/{1981 + d // 365}{d % 365 // 31 + 1:02d}/oisst-avhrr-v02r01.{d:08d}.nc"


def oisst_rows(d, k):
    return dict(dim_0=d, dim_1=0, dim_2=0, dim_3=0, present=1, path=oisst_path(d), offset=20000 + 400000 * k + d % 977,
                size=350000 + (d * 31 + k) % 9973, info="COMPRESSION=DEFLATE; FILTER=SHUFFLE; ENDIANNESS=LITTLE", data=None)


def oisst(label, bulk_part, compact_every, fanout=None):
    root = os.path.join(WORK, "oisst-" + label)
    shutil.rmtree(root, ignore_errors=True)
    s = Store(root, fanout=fanout)
    table = dict(format_version="byterefs/0.1", remap=[], arrays=[dict(
        array=a, kind="data", storage="referenced", dims=["time", "zlev", "lat", "lon"], shape=[16387, 1, 720, 1440],
        chunks=[1, 1, 720, 1440], dtype="<i2", fill="-999", codec={"compressor": {"id": "zlib", "level": 4}, "filters": [{"id": "shuffle", "elementsize": 2}]},
        attrs={}, affine=None, values=None) for a in OARR])
    r = {}
    t0 = time.perf_counter()
    brstore.init(s, table, {a: [oisst_rows(d, k) for d in range(16000)] for k, a in enumerate(OARR)},
                 [dict(source=oisst_path(d), size=1_700_000, mtime=d, etag=f'"{d:x}"') for d in range(16000)], part_size=bulk_part)
    r["bulk_s"] = round(time.perf_counter() - t0, 2)
    r["after_bulk"] = classes(s)
    times, man = [], []
    for d in range(16000, 16387):
        t0 = time.perf_counter()
        t = s.begin("daily")
        for k, a in enumerate(OARR):
            x = oisst_rows(d, k)
            t.put_ref(a, (d, 0, 0, 0), x["path"], x["offset"], x["size"], x["info"])
        t.put_source(oisst_path(d), 1_700_000, d, f'"{d:x}"')
        if compact_every and d % compact_every == 0:
            for a in OARR:
                t.compact(a, d - compact_every, d)
            t.compact_sources()
        v = t.commit(f"day {d}")
        times.append(time.perf_counter() - t0)
        man.append(s.o.size(f"versions/{v:08d}.json"))
    r["daily_commit_s_median"] = round(statistics.median(times), 4)
    r["manifest_bytes_first_last_max"] = [man[0], man[-1], max(man)]
    r["all_history"] = classes(s)
    r["all_history_mb"] = mb(sum(b for _, b in r["all_history"].values()))
    m = s.manifest(s.latest())
    n = duckdb.execute("SELECT count(*) FROM read_parquet(?)", [[p for a in OARR for p in s.frag_paths(m, a)]]).fetchone()[0]
    assert n == 16387 * 4
    dead, freed = s.gc([1, s.latest()])
    r["bulk_and_latest_only"] = classes(s)
    r["bulk_and_latest_only_mb"] = mb(sum(b for _, b in r["bulk_and_latest_only"].values()))
    OUT["oisst_" + label] = r
    print("oisst", label, json.dumps(r))


if "mur" in WHAT:
    OUT["mur_flat_export_mb"] = mb(flat_export_size(range(366)))
    print("flat export of the year, one Parquet file:", OUT["mur_flat_export_mb"], "MB")
    mur_brstore("daily")
    mur_brstore("monthly", compact_every=30)
if "tiered" in WHAT:
    mur_brstore("tiered8", fanout=8)
    oisst("tiered8", 365, None, fanout=8)
if "icechunk" in WHAT:
    for split in (1, 30, 365):
        mur_icechunk(split)
if "delta" in WHAT:
    mur_delta()
if "oisst" in WHAT:
    oisst("yearly-then-daily", 365, None)
    oisst("yearly-then-monthly", 365, 30)
p = os.path.join(WORK, "scale.json")
old = json.load(open(p)) if os.path.exists(p) else {}
old.update(OUT)
json.dump(old, open(p, "w"), indent=1)
