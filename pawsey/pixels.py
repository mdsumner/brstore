"""Pixels from a real store: read chunks through a committed version and
compare them with the same blocks read from the source file itself.

  python pawsey/pixels.py <store root> [--array NAME] [--chunks 6] [--target public] [--base DIR]

For each chosen chunk the version's row gives (path, offset, size). The
path goes through the table's remap rules for --target, the byte range is
fetched (an https Range request, or a local read), decoded with the
array's codec, and compared with that block read independently: by GDAL's
multidimensional API when GDAL can open the file, else by the HDF5 library
through h5py. Where GDAL offers GetRawBlockInfo, the row's offset and
size are compared with what GDAL reports as well.

A --target that names no remap rule (say "none") leaves the paths as the
table stores them: on the machine that holds the source files, that reads
them in place.

Chunks are taken from the first fragment, a middle one and the last, which
an append commit wrote. Sources must be netCDF-4 or HDF5 files joined
along dim_0 only. --base is prepended to a remapped path that is relative.
"""
import argparse, io, os, sys, urllib.request

import duckdb
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import brstore

ap = argparse.ArgumentParser()
ap.add_argument("root")
ap.add_argument("--array")
ap.add_argument("--chunks", type=int, default=6)
ap.add_argument("--target", default="public")
ap.add_argument("--base", default="")
A = ap.parse_args()

s = brstore.Store(A.root)
m = s.manifest(s.latest())
table = m["table"]
data = [a for a in table["arrays"] if a["storage"] == "referenced" and a["kind"] == "data"]
a = next(x for x in data if x["array"] == A.array) if A.array else data[0]
name, nd = a["array"], len(a["shape"])


def remap(path):
    for r in table.get("remap", []):
        if r["target"] == A.target and path.startswith(r["prefix"]):
            path = r["replacement"] + path[len(r["prefix"]):]
    return path if "://" in path or os.path.isabs(path) else os.path.join(A.base, path)


class RangeFile(io.RawIOBase):
    """A read-only file over https Range requests, cached in 1 MB blocks."""

    B = 1 << 20

    def __init__(self, url):
        self.url, self.pos, self.blocks = url, 0, {}
        req = urllib.request.Request(url, method="HEAD")
        self.size = int(urllib.request.urlopen(req, timeout=120).headers["Content-Length"])

    def _get(self, lo, hi):
        req = urllib.request.Request(self.url, headers={"Range": f"bytes={lo}-{hi - 1}"})
        return urllib.request.urlopen(req, timeout=300).read()

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.pos

    def seek(self, off, whence=0):
        self.pos = off if whence == 0 else (self.pos + off if whence == 1 else self.size + off)
        return self.pos

    def readinto(self, b):
        n = min(len(b), self.size - self.pos)
        out, pos = bytearray(), self.pos
        while len(out) < n:
            k = pos // self.B
            if k not in self.blocks:
                self.blocks[k] = self._get(k * self.B, min((k + 1) * self.B, self.size))
            blk = self.blocks[k]
            out += blk[pos - k * self.B : pos - k * self.B + n - len(out)]
            pos = self.pos + len(out)
        b[:n] = out
        self.pos += n
        return n


def fetch(loc, offset, size):
    if "://" in loc:
        req = urllib.request.Request(loc, headers={"Range": f"bytes={offset}-{offset + size - 1}"})
        return urllib.request.urlopen(req, timeout=300).read()
    with open(loc, "rb") as f:
        f.seek(offset)
        return f.read(size)


def oracle_gdal(loc, local_key, start, count):
    from osgeo import gdal

    gdal.UseExceptions()
    ds = gdal.OpenEx(("/vsicurl/" + loc) if "://" in loc else loc, gdal.OF_MULTIDIM_RASTER)
    arr = ds.GetRootGroup().OpenMDArray(name)
    block = arr.ReadAsArray(array_start_idx=start, count=count)
    raw = None
    if hasattr(arr, "GetRawBlockInfo"):
        try:
            info = arr.GetRawBlockInfo(list(local_key))
            raw = (info.GetOffset(), info.GetSize())
        except Exception:  # noqa
            raw = None
    return np.asarray(block), raw, "GDAL " + gdal.__version__


def oracle_h5py(loc, local_key, start, count):
    import h5py

    with h5py.File(RangeFile(loc) if "://" in loc else loc, "r") as f:
        d = f[name]
        sel = tuple(slice(i, i + n) for i, n in zip(start, count))
        block = d.fields(d.dtype.names)[sel] if d.dtype.names else d[sel]
        try:
            ci = d.id.get_chunk_info_by_coord(tuple(start))
            raw = (ci.byte_offset, ci.size)
        except Exception:  # noqa
            raw = None
    return np.asarray(block), raw, "HDF5 through h5py " + h5py.__version__


frags = s.frags(m, name)
pick = [frags[0], frags[len(frags) // 2], frags[-1]]
con = duckdb.connect()
rng = np.random.default_rng(0)
rows = []
for f in pick:
    t = [r for r in brstore.from_parquet(s.o.get(f["path"])) if r["present"] and r["offset"] is not None
         and not r["path"].startswith("packs/")]  # a chunk in a pack has no source file to compare with
    for i in rng.choice(len(t), size=min(len(t), -(-A.chunks // len(pick))), replace=False) if t else []:
        rows.append(t[int(i)])
rows = rows[: A.chunks]
import hashlib, json

# what the version holds, apart from when and by whom it was committed: equal at two sites means the same rows
content = hashlib.sha256(json.dumps([m["table_keys"], m["refs"], m.get("sources")], sort_keys=True).encode()).hexdigest()
print(f"{name}: shape {a['shape']}, chunks {a['chunks']}, version {m['version']}, {len(frags)} fragments; "
      f"checking {len(rows)} chunks")
print(f"version content {content[:16]}; paths read as {'stored' if not any(r['target'] == A.target for r in table.get('remap', [])) else 'remapped for ' + A.target}")
bad, first = 0, {}
for r in rows:
    key = tuple(r[f"dim_{i}"] for i in range(nd))
    loc = remap(r["path"])
    if r["path"] not in first:  # the file's first step of dim_0 in the whole array
        first[r["path"]] = con.execute("SELECT min(dim_0) FROM read_parquet(?) WHERE path = ?",
                                       [s.frag_paths(m, name), r["path"]]).fetchone()[0]
    local_key = (key[0] - first[r["path"]],) + key[1:]
    start = [k * c for k, c in zip(local_key, a["chunks"])]
    count = [min(c, n - k * c) for k, c, n in zip(key, a["chunks"], a["shape"])]
    count[0] = a["chunks"][0]
    try:
        got = fetch(loc, r["offset"], r["size"])
        assert len(got) == r["size"], "short read"
    except Exception as e:  # noqa
        print(f"   {key}: cannot fetch {loc}: {type(e).__name__}: {str(e)[:120]}")
        bad += 1
        continue
    mine = np.frombuffer(brstore.decode_v2(a, got), dtype=a["dtype"]).reshape(a["chunks"])
    mine = mine[tuple(slice(0, n) for n in count)]
    want = None
    for oracle in (oracle_gdal, oracle_h5py):
        try:
            want, raw, how = oracle(loc, local_key, start, count)
            break
        except Exception as e:  # noqa
            why = f"{oracle.__name__}: {type(e).__name__}: {str(e)[:120]}"
            print(f"   ({why})")
    if want is None:
        print(f"   {key}: no reader could open {loc}")
        bad += 1
        continue
    same = mine.shape == want.shape and np.array_equal(mine, want.astype(mine.dtype), equal_nan=mine.dtype.kind == "f")
    range_ok = "" if raw is None else (", byte range as the reader reports" if raw == (r["offset"], r["size"]) else
                                       f", BYTE RANGE DIFFERS: reader says {raw}")
    bad += (not same) + ("DIFFERS" in range_ok)
    print(f"   {key} {os.path.basename(r['path'])} +{r['offset']} {r['size']} bytes: "
          f"{'identical' if same else 'DIFFERENT'} to {how}{range_ok}")
print("every chunk read through the version equals the source file's own block" if not bad else f"{bad} problems")
sys.exit(1 if bad else 0)
