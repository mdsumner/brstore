# 0015 (draft): Owned bytes as packs, and one commit for the whole store

- Status: draft for discussion, not yet proposed to byterefs/spec
- Date: 2026-11-23
- Revisits: decision 0013 (two of its three triggers: "a store needs held
  bytes" and "more than one concurrent writer")
- Target version: none yet; non-normative producer practice, as 0013 is
- Prototype: `brstore.py` (about 750 lines), walked by `demo.py`;
  cost measured by `scale.py`

Content is ASCII only.

## Context

"Bytes in place" sorts each Icechunk feature into column, view, layout or
storage, and everything except storage is built. What is left is exactly
what a byte-ref table cannot yet do:

1. **Accept a write.** A chunk with no source file (a derived overview
   level, a regridded product) has nowhere to live. 0006 leaves held or
   derived levels waiting on "m16's held-bytes work".
2. **Reproduce bytes, not rows.** m16 section 6a: a tag on a refs table
   brings back rows; if the provider rewrites the file, the rows point at
   other bytes.
3. **Commit atomically across arrays, with more than one writer.** 0013
   allows one writer, and m16 found that a reader pinning the chunk table
   but not `sources` accepts old offsets into a new file, silently.

0013 expects Lance to answer 1 and 2 later, through a blob column. This
draft answers all three without a new dependency, a new reader, or a new
column.

## The two ideas

### A. A write is a harvest

To write a chunk, append its encoded bytes to an object the store owns (a
**pack**), then record the row a scan of that pack would give:
`present=1, path=packs/<sha256>.pack, offset, size, info`.

- "Native" and "virtual" chunks stop being two kinds of thing. Every chunk
  is a reference; some references point at files the store wrote.
- **No reader changes.** A row into a pack is a row. The prototype's
  written chunks are read by the unmodified `wxy-refs` (below).
- **No blob column, no Lance.** Bytes are never inside Parquet, so the
  row-group cost m16 measured (29 ms against Lance's 1 ms for one held
  chunk) does not arise: one chunk is one range read of a pack.
- **A pack has no format.** It is concatenated chunk bytes: no header, no
  index, no footer. The index is the table. This is what keeps commit
  criterion 4 (no new file format) intact.
- **A pack is named by the SHA-256 of its content,** so it is immutable,
  self-verifying, safe to retry, and its `sources` row needs no trust in a
  provider's ETag.
- **Deduplication is free.** Two rows may carry the same
  `(path, offset, size)`; the prototype writes equal chunks once per pack.
  (This is m05's point about fill-only tiles, without per-chunk objects.)
- **Holding is the same operation.** To keep what was read from a source
  that may change, copy those ranges into a pack and commit the rows. That
  is m05's `ref` to `copy` promotion, and it gives a third answer to m16
  open question 5 (neither a blob per chunk nor a copy of the whole file).
- A producer that wants a pack to be re-harvestable if the table were
  lost can write a real file instead (a Zarr v3 shard, covered by decision
  0009, or a COG) and scan it. The store does not care which.

### B. Commit the store, not the table

A version is one small **root manifest**, `versions/<n>.json`, created
with create-if-absent (`If-None-Match: *`; the same primitive Icechunk,
Lance and Delta use). It lists, for that version:

- the `arrays.json` content (meta, arrays, remap), inline;
- for each array, the root of its fragment-list tree (see "After
  fixing the manifest"); each fragment covers a range `[lo, hi)`
  of `dim_0` chunk indexes. An append starts a new fragment (the append
  unit, as m16 found for both Parquet and Icechunk); compaction merges
  old ones into wider ranges, and changes no row;
- the `sources` fragments (append-only; compaction merges them);
- a `changes` fragment: the chunk keys this commit wrote.

Rules:

1. **Every object is create-only.** Packs, fragments, manifests and tags
   are never overwritten. Fragments are content-addressed like packs.
2. **A version is a plain byterefs table.** Within one version each chunk
   key occurs in at most one row. An overwrite replaces the fragment for
   that partition (copy on write); it never adds a second row and relies
   on reading order. So `read_parquet([fragments])` is the table, with no
   merge rule, and no "painting order" (quiet-operations).
3. **Everything is pinned together.** `sources`, array metadata and every
   array's rows are named by one manifest, so the partly pinned reader of
   m16 section 6a cannot be constructed.
4. **Latest is the highest `n`.** There is no mutable pointer.
5. **Conflict is a join.** A writer that loses the race for `n` reads the
   `changes` of each version that landed since its base. If no chunk key
   or array metadata it touched was touched there, it replays its
   operations on the new latest and tries `n+1`. Otherwise it is refused.
6. **A tag is a create-only object** naming a version and the hash of its
   manifest.
7. **GC is an anti-join** of the object listing against what the kept
   versions (tags, latest, a retention window) reach.

The manifest is the only thing this draft grows, against the reminder
"borrow a table format, do not grow one". The case for it: neither Lance
nor Delta commits several tables at once (m16 open question 1 and its
"cross-table consistency rests on a convention"), and that is the one
property a store of many arrays needs. The manifest could equally pin
Delta or Lance table versions instead of fragment lists; it is the
store-wide cut that matters, not what it points at.

## What was run (2026-11-23)

`demo.py` on wxylib/fixtures `mdimrefs/` (three OISST-shaped days,
`sst` and `ice`), local filesystem, create-if-absent by `link(2)`. Every
line is an assert in the script.

| Step | Result |
|---|---|
| Import the three-file export as version 1, tag `scan` | 5 of 5 GDAL 3.13 oracle windows equal |
| Write a derived level `sst_x2` (0006 attributes) as 3 chunks | 1 pack, version 2, tag `level-a`; rows are ordinary refs |
| Writers A and B from one base, different chunks | both land; B rebased over 1 version |
| Writers C and D, the same chunk | C lands; D refused, naming the chunk |
| 16 threads, one chunk each, racing | versions 7 to 22, no gaps, no lost writes; 15 rebased |
| Read tag `level-a` after every chunk was rewritten | hash unchanged; latest differs on all 3 days |
| Daily append: a new netCDF's refs (harvested with h5py) plus its computed level chunk | one commit; `sst`, `ice`, `time` and `sst_x2` grow together; tag `scan` still 3 days |
| Hold `sst` day 2 into a pack (tag `held`), then overwrite that netCDF in place | tag `scan` refused as stale; unchecked, the old range of the new file fails to decode; tag `held` equals the GDAL oracle window |
| Diff of two versions | DuckDB `EXCEPT` over the two fragment lists: 4 rows new or changed, 3 replaced |
| GC keeping the 3 tagged or latest versions of 24 | 47 of 126 objects deleted; every kept version reads the same values |

**An existing reader, unchanged.** `checkout` writes one version as the
three-file export with a remap rule for `packs/`. wxylib/core at `cf9ac14`
(`refs_windows --direct`, built as is) reads from it:

- `sst` at tag `held`: all three fixture windows identical to GDAL, with
  days 0 and 1 from the netCDF files and day 2 from a pack, after the
  day 2 file had been overwritten;
- `sst_x2` at tag `level-a` and at latest: identical to the values
  written (the AVERAGE level, then the reprocessed one).

**A third reader, and S3.** `render_kerchunk_json` writes a version as
kerchunk JSON; zarr-python 3.4 over fsspec reads the same windows from it
identically. The store also runs on S3 through obstore (create-if-absent
as `If-None-Match: *`): the whole walk passes against a local moto
server, which says the code path works and nothing about a real service.

**Acacia and GDAL (run by Michael on Setonix, 2026-11-23 11:01 UTC).**
`pawsey/run.sh`, image `gdal-r-python-extras_latest.sif`, store at
`s3://aad-derived/raad-scratch/brstore-test/20261123T110110Z`:

- Acacia refuses a create-if-absent PUT of an existing key; of 16
  writers racing for one key exactly one won and the object held its
  body; 20 of 20 new keys were listed straight after the PUT. A
  conditional PUT took 96 ms (median), a listing 4 ms.
- The whole walk passed with the store on Acacia, the 16-thread commit
  race included (versions 7 to 22, no gaps, 15 rebased).
- GDAL 3.13.3 read every window identically through the kerchunk JSON
  render: `sst` at tag `held` (two days from netCDF, one from a pack),
  `sst_x2` at tag `level-a` and at latest.

**Real rows, small.** `real_rows.py` loads half of a published export,
appends the rest as commits, and compares the latest version with the
export by SQL. On the fixtures' real tables (OISST September 1981, 120
rows; BRAN2023 `temp` January 2010, 97,920 rows) both differences are
empty, and the BRAN fragments are 1.14 MB against 1.22 MB flat.

## What it costs (2026-11-23)

`scale.py` replays the history m16 used, at its row counts: GHRSST
MUR `analysed_sst` 2024, 2,556 chunks a day, 182 days as a bulk load
(465,192 refs), 184 daily commits, then day 100 rescanned (2,556 rows
changed). **The rows are synthetic**: real shapes and counts, made-up
offsets and sizes. Local disk. The year as one flat Parquet file is
6.5 MB.

| Store | All history | Daily commit, median | Rescan |
|---|---|---|---|
| Prototype, a fragment per day | 19.6 MB (7.6 manifests, 12.0 fragments) | 0.05 s | 0.21 s |
| Prototype, compacted every 30 days | 21.3 MB (4.7 manifests, 16.6 fragments) | 0.04 s | 0.05 s |
| delta-rs 1.7.0, a file per day | 15.2 MB (0.9 log) | 0.05 s | 0.09 s |
| Icechunk 2.3.1, manifests split per day | 23.5 MB | 0.03 s | 0.08 s |
| Icechunk, split per 30 days | 133.9 MB | 0.06 s | 0.10 s |
| Icechunk, split per 365 days | 2,155 MB | 0.88 s | 1.33 s |

The Icechunk rows repeat m16 section 4 (22 MB, 116 MB, 1.84 GB there),
which is some evidence the synthetic rows cost what real ones do.

For the prototype on the same store: rows at the three tags 465,192 /
935,496 / 935,496; diff of bulk against year 470,304 rows in 0.27 s and
year against rescan 2,556 rows, both a DuckDB `EXCEPT`; a count over all
366 fragments 0.09 s; one chunk by key 4 to 7 ms (the manifest and one
fragment); after GC to the three tags, 10.4 to 11.0 MB.

OISST-shaped (16,387 daily sources, four arrays, one chunk a day each;
16,000 days as yearly fragments, then 387 daily commits):

| Layout | Manifest, first to last | All history |
|---|---|---|
| New fragment per day, never compacted | 23 KB to 213 KB | 48.8 MB (41.0 manifests) |
| Compacted every 30 days | 23 KB to 32 KB (peak 45 KB) | 22.2 MB (12.6 manifests) |

Readings:

- **Cost does not separate the three.** With the append unit as the
  fragment, the prototype, Delta and Icechunk-split-per-day are within
  1.6 times of each other, about 2.3 to 3.6 times the flat export, and
  all commit in a few hundredths of a second. The case for this design
  is not that it is cheaper.
- **The manifest is the weak part, as predicted.** It restates the whole
  fragment list on every commit, so it costs O(fragments) per version:
  39 percent of the MUR history and 84 percent of the uncompacted OISST
  one. Compaction bounds it but does not remove it. Delta's log is
  incremental and costs 0.9 MB for twice as many commits.
- **Compaction is not optional**, and it has the Icechunk lesson
  inverted: fragments must be narrow where rows still change and wide
  where they do not.

### After fixing the manifest (same day)

The fragment list of each array is now its own tree of content-addressed
objects, 64 entries a leaf, cut at fixed positions. The root manifest
names one tree per array (about 0.8 KB for the MUR store), an append
rewrites the last leaf and the path above it, and every other leaf is
shared with the version before. No data is rewritten.

| History | Before | List tree | List tree, fragments also merged in tiers of 8 |
|---|---|---|---|
| MUR shape, fragment per day | 19.6 MB | 13.6 MB (1.4 lists, 12.0 fragments) | 31.1 MB (0.4 lists, 30.5 fragments) |
| OISST shape, never compacted | 48.8 MB | 16.9 MB (8.3 lists, 7.8 fragments) | 13.3 MB (3.3 lists, 9.3 fragments) |

- The list tree alone puts the MUR history under delta-rs (15.2 MB) and
  removes the need to compact for the manifest's sake.
- Merging data fragments in tiers is a separate choice and cuts both
  ways. Where an append is a few rows (OISST) it helps. Where an append
  is thousands of rows (MUR) every merge is kept by history, and the
  whole costs more than it saves; a rescan also rewrites a wide fragment
  (0.37 s against 0.14 s). It stays off by default. It does shrink the
  latest version (16 fragments instead of 366) for readers that list.
- Retried commits leave orphan lists and fragments behind; GC collects
  them.

### Real rows, and what they exposed (run by Michael on Setonix, 11:11 UTC)

`run.sh real` on the published OISST flat store
(`ausantarctic-scratch/oisst/oisst-v2.1-daily/flat`: 4 arrays, 65,928
rows, 16,482 days, 0.76 MB of Parquet): half loaded as version 1, the
rest appended in 60 commits of 138 days, 0.04 s each. The latest version
equals the published table row for row (both `EXCEPT`s empty).

It also showed a flaw the synthetic test could not: each manifest was
95 KB (5.8 MB for 61 versions, against 2.6 MB of fragments), because the
manifest carried `arrays.json` inline and the `time` coordinate holds
16,482 inline values. Fixed the same day: each array's description is
its own content-addressed object under `meta/`, and the manifest names
them, so a commit restates only the descriptions that changed (the BRAN
replay's manifests fell to 0.5 KB).

That fix was partial: a real daily append changes `time` every day, so
its description, values included, was still rewritten whole on each
commit.

### Coordinates as referenced arrays

"A write is a harvest" applies to coordinates too. `put_coord` holds a
coordinate as a chunked referenced array (128 values a chunk, raw
little-endian, each chunk padded to full length) whose chunks go into a
pack; its description then carries no values. `append_coord` rewrites
only the last, partly filled chunk. In byterefs terms the array has
`kind = coord` and `storage = referenced`, which the 0.1 text does not
forbid but no producer writes today.

Measured (`scale.py coord`): a 16,000-value time coordinate extended by
one value in each of 387 commits.

| How `time` is held | Added per append |
|---|---|
| Inline in its description | 119.1 KB |
| Referenced array | 9.2 KB |

The 9.2 KB is no longer the coordinate. It is the fixed cost of a
commit's small objects: one list leaf (3.8 KB), three small Parquet
fragments (3.4 KB), a pack (1.4 KB), a description and a manifest.

Readers: a checkout renders the coordinate back inline, so the
three-file export is unchanged and `wxy-refs` reads it as before. The
kerchunk render leaves it referenced, and zarr-python reads its values
from the pack like any array (`zarr_check.py`). GDAL has not been run
on this form.

## What this does not show

- **Object storage beyond Acacia.** The conditional PUT is shown on
  Acacia (Ceph) only, with 16 writers in one process. AWS S3, source.coop's
  proxy, and writers on separate machines are untested.
- **Real rows beyond OISST.** The cost tables use synthetic rows; the
  one real store replayed has one chunk per source. A many-chunks store
  (GHRSST, BRAN) has not been replayed. Pixels were checked against GDAL
  only on the 27-reference fixture.
- **GDAL on a referenced coordinate.** Read here by zarr-python and, through
  the checkout, by `wxy-refs`; the GDAL read in `pawsey/gdal_check.py` has not
  been rerun since coordinates changed.
- **Held bytes at scale.** No pack larger than a few kilobytes was
  written; pack sizing and compaction are untested.
- **Concurrent writers at scale,** and Icechunk or Delta on the OISST
  shape.
- **Not attempted at all:** branches, credentials per prefix, a Zarr
  write interface (zarr-python or zarrs `set` into a pack), a grace
  period in GC.

## Open questions

1. **Lineage of a held chunk.** After a hold, the row points at the pack
   and no longer says which source range it was copied from. m05 keeps
   both (`held = copy`). Options: two nullable columns, or a row in the
   `changes` fragment. Not decided; the prototype drops it.
2. **Is the root manifest ours, or Delta's?** Measured above: Delta is
   somewhat smaller and its log is far cheaper per commit. Against it:
   a Delta commit covers one table, so a store-wide cut over refs,
   `sources` and array metadata comes back as a convention (commit
   `sources` first, pin what you saw), which is the weakness m16 named;
   and reading needs a Delta reader, where fragments need only Parquet.
   Leaning: keep the root manifest, and fix its cost (question 3). Not
   settled; a second producer's preference should count.
3. **Manifest shape.** Answered for the list (see "After fixing the
   manifest"): a tree of shared leaves, no log needed. Still open: when
   to merge data fragments, which depends on rows per append and on how
   much history is kept, and should probably be a producer setting.
4. **Source identity.** The prototype keys `sources` by `source` within a
   version and compares `size` and `etag` on read. m32 section 3.6 asks
   for `(source, etag)` in the format; this draft is one more reason.
5. **Relative `packs/` paths.** They need decision 0004's open question
   on relative paths closed; the checkout works around it with a remap
   rule.
6. **Where the code lives.** A second implementation in Rust beside
   `wxy-cache` (which already holds content-addressed bytes as a table)
   would make `wxy-refs` a writer. Whether that is wxylib's to own is the
   commit-criteria question, not a technical one.
