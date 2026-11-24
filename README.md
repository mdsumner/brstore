# brstore

A prototype of what a [byterefs](https://github.com/byterefs/spec) table
still lacks beside Icechunk: accepting a write, reproducing bytes rather
than rows, and committing a whole store atomically with more than one
writer. Experimental; nothing here is a format proposal yet.

Two ideas, argued in
[docs/0015-draft-owned-bytes-and-store-commits.md](docs/0015-draft-owned-bytes-and-store-commits.md):

1. **A write is a harvest.** A written or held chunk goes into an
   immutable, content-addressed pack (concatenated bytes, no header, no
   index) and is recorded as the ordinary reference row a scan of that
   pack would give. Native and virtual chunks are one kind of thing, and
   no reader changes.
2. **Commit the store, not the table.** One small create-only root
   manifest per version pins every array's description, its fragment
   list, and the sources. The only storage primitive is "create this
   object if it does not exist". A version is a plain byterefs table:
   any Parquet reader reads it, with no merge rule.

## What is in a store

```
packs/<sha256>.pack       encoded chunk bytes
frags/<sha256>.parquet    table fragments: refs rows, sources rows, change keys
lists/<sha256>.json       an array's fragment list, as a tree of shared leaves
meta/<sha256>.json        an array's description; the table header
versions/<n>.json         the root manifest of version n
tags/<name>.json          a name for a version
```

Nothing is ever overwritten.

## Files

| File | What it is |
|---|---|
| `brstore.py` | the store: local directory or S3 (through obstore), transactions, conflict check, checkout, kerchunk JSON render, GC |
| `demo.py` | the whole history on wxylib/fixtures `mdimrefs/`; every result is an assert |
| `real_rows.py` | replay any flat refs store (directory or https URL) as a history, then compare with it row for row |
| `scale.py` | the wxylib/design m16 history at its row counts (synthetic rows) through brstore, Icechunk and delta-rs |
| `wxy_check.py` | oracle manifests for wxylib/core's `refs_windows` example |
| `zarr_check.py` | zarr-python over fsspec reads the committed versions |
| `brzarr.py` | a zarr-python store over one version: `xarray.to_zarr` becomes one atomic commit |
| `zarr_write_check.py` | xarray writes, appends and races through `brzarr.py`, in Zarr format 3 and 2; other readers read the result |
| `pawsey/run.sh` | one command for Setonix: the object-storage probe on Acacia, the walk with the store on Acacia, a GDAL read |
| `pawsey/pixels.py` | chunks read through a committed version, compared with the source files' own blocks |
| `nci/run.sh` | the replay and the pixel check on NCI Gadi, reading `/g/data` sources in place |
| `results/` | raw numbers behind the tables in the design record |

## Run

Python 3 with pyarrow, duckdb, numcodecs, numpy, netCDF4 and h5py (plus
fsspec and zarr for `zarr_check.py`, xarray for `zarr_write_check.py`,
obstore for S3):

```sh
git clone --depth 1 https://github.com/wxylib/fixtures ../fixtures
python demo.py ../fixtures/mdimrefs work
python wxy_check.py ../fixtures/mdimrefs work && python zarr_check.py work
python real_rows.py ../fixtures/mdimrefs/bran2023-temp-201001 work-bran
python zarr_write_check.py work-zw work
```

Writing with xarray:

```python
import brstore, brzarr
z = brzarr.Session(brstore.Store("my-store"), writer="me")
ds.to_zarr(z)                          # held in memory (Zarr format 3; 2 on request)
version = z.commit("first write")      # one pack, one new version
```

With the store on S3-compatible storage (credentials from `AWS_*`,
endpoint from `AWS_ENDPOINT_URL`):

```sh
python pawsey/probe.py s3://bucket/prefix        # is the commit protocol safe here?
BRSTORE_ROOT=s3://bucket/prefix/walk python demo.py ../fixtures/mdimrefs work
```

## State (2026-11-23)

Shown: the walk passes on a local disk, on a moto server and on Pawsey's
Acacia; committed versions read identically through GDAL 3.13.3,
unmodified wxylib/core `wxy-refs`, zarr-python and DuckDB; four
published flat stores (OISST, ESA CCI chlorophyll, GHRSST MUR at 30.7
million rows, Bluelink salinity at 37.5 million) replay to versions equal to them row for row, and chunks read through
one of them over https equal GDAL's read of the source files; `xarray.to_zarr` (write, append, region; Zarr format 3 or 2; local or
on Acacia) lands as one atomic commit each; history costs about what Icechunk (manifests split per day) and
delta-rs cost.

Not shown: pixels beyond a six-chunk sample of one real store, writers on separate machines, any
service but Acacia, branches. The design record lists these in full.

## Licence

Apache-2.0.
