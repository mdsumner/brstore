"""brstore: a prototype of owned bytes and store-wide commits over a byterefs table.

Not a format proposal on its own; see docs/0015-draft-owned-bytes-and-store-commits.md.

Everything in a store is an immutable, create-only object:

  packs/<sha256>.pack       encoded chunk bytes, concatenated, no header, no index
  frags/<sha256>.parquet    table fragments (refs rows, sources rows, change keys);
                            a refs fragment covers a range [lo, hi) of dim_0
  meta/<sha256>.json        one array's description, and the table header that
                            names them; a commit restates only what changed
  lists/<sha256>.json       the fragment list of one array (or of sources), as
                            a tree of small objects; a commit restates only
                            the leaves it changed
  versions/<n>.json         one root manifest per version, created only if absent
  tags/<name>.json          a name for a version, created only if absent

A version is a plain byterefs table: the union of the fragments its manifest
lists has at most one row per chunk key, so any Parquet reader reads it with
no merge rule. A written chunk is a row that points into a pack, exactly the
row a scan of that pack would give. Nothing is ever overwritten.

The only storage primitive used is "create this object if it does not exist"
(O_EXCL here; If-None-Match: * on object storage).
"""

import bisect
import hashlib
import io
import json
import os
import time

import pyarrow as pa
import pyarrow.parquet as pq

FORMAT = "byterefs-store/draft-4"
PACK_INFO = "COMPRESSION=DEFLATE; FILTER=SHUFFLE; ENDIANNESS=LITTLE"
RAW_INFO = "ENDIANNESS=LITTLE"
COORD_CHUNK = 128  # values per chunk of a referenced coordinate: an append rewrites one chunk


class Conflict(Exception):
    pass


class StaleSource(Exception):
    pass


def sha256(b):
    return hashlib.sha256(b).hexdigest()


def keystr(key):
    return ".".join(str(int(k)) for k in key)


# ---------------------------------------------------------------- objects


class Objects:
    """A directory used as an object store with create-if-absent."""

    def __init__(self, root):
        self.root = os.path.abspath(root)

    def _p(self, key):
        return os.path.join(self.root, key)

    def put_new(self, key, data):
        """Create key holding data. False if it already exists (HTTP 412)."""
        p = self._p(key)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        tmp = p + ".tmp-%d-%d" % (os.getpid(), time.monotonic_ns())
        with open(tmp, "wb") as f:
            f.write(data)
        try:
            os.link(tmp, p)  # atomic, fails if p exists
            return True
        except FileExistsError:
            return False
        finally:
            os.unlink(tmp)

    def get(self, key, offset=None, size=None):
        with open(self._p(key), "rb") as f:
            if offset is None:
                return f.read()
            f.seek(offset)
            return f.read(size)

    def exists(self, key):
        return os.path.exists(self._p(key))

    def list(self, prefix):
        d = self._p(prefix)
        if not os.path.isdir(d):
            return []
        return sorted(prefix + "/" + n for n in os.listdir(d) if ".tmp-" not in n)

    def delete(self, key):
        os.unlink(self._p(key))

    def size(self, key):
        return os.path.getsize(self._p(key))

    def local(self, key):
        """A local file holding the object (for engines that want a path)."""
        return self._p(key)

    def url(self, prefix):
        return self._p(prefix)


class S3Objects:
    """The same interface on S3-compatible storage, through obstore.

    Create-if-absent is a PUT with If-None-Match: * (obstore mode="create").
    Credentials come from the AWS_* environment; AWS_ENDPOINT_URL (or
    AWS_S3_ENDPOINT) names a non-AWS endpoint, addressed path style.
    Objects are immutable, so local() keeps a download cache.
    """

    def __init__(self, url, cache=None):
        import obstore
        from obstore.store import S3Store
        from urllib.parse import urlparse

        u = urlparse(url)
        self.ob, self.bucket, self.prefix = obstore, u.netloc, u.path.strip("/")
        kw = {}
        ep = os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("AWS_S3_ENDPOINT")
        if ep:
            kw["endpoint"] = ep if "://" in ep else "https://" + ep
            kw["virtual_hosted_style_request"] = False
            if kw["endpoint"].startswith("http://"):
                kw["client_options"] = {"allow_http": True}
        self.store = S3Store(self.bucket, prefix=self.prefix, region=os.environ.get("AWS_REGION", "us-east-1"), **kw)
        self.cache = cache or os.path.join(os.environ.get("TMPDIR", "/tmp"), "brstore-cache", self.bucket, self.prefix)

    def put_new(self, key, data):
        try:
            self.ob.put(self.store, key, data, mode="create")
            return True
        except self.ob.exceptions.AlreadyExistsError:
            return False

    def get(self, key, offset=None, size=None):
        if offset is None:
            return bytes(self.ob.get(self.store, key).bytes())
        return bytes(self.ob.get_range(self.store, key, start=offset, length=size))

    def list(self, prefix):
        return sorted(o["path"] for batch in self.ob.list(self.store, prefix=prefix + "/") for o in batch)

    def delete(self, key):
        self.ob.delete(self.store, key)

    def size(self, key):
        return self.ob.head(self.store, key)["size"]

    def local(self, key):
        p = os.path.join(self.cache, key)
        if not os.path.exists(p):
            os.makedirs(os.path.dirname(p), exist_ok=True)
            tmp = p + ".tmp-%d-%d" % (os.getpid(), time.monotonic_ns())
            with open(tmp, "wb") as f:
                f.write(self.get(key))
            os.replace(tmp, p)
        return p

    def url(self, prefix):
        return f"s3://{self.bucket}/{self.prefix}/{prefix}"


# ---------------------------------------------------------------- tables


def decode_v2(a, data):
    """Undo an array's Zarr V2 codec on one chunk's bytes."""
    codec = a.get("codec") or {}
    if isinstance(codec, list):
        raise NotImplementedError(f"{a['array']}: a V3 codec chain is decoded only through a brzarr session")
    if not (codec.get("compressor") or codec.get("filters")):
        return data
    import numcodecs

    if codec.get("compressor"):
        data = numcodecs.get_codec(codec["compressor"]).decode(data)
    for f in reversed(codec.get("filters") or []):
        data = numcodecs.get_codec(f).decode(data)
    return bytes(data)


def refs_schema(ndim):
    return pa.schema(
        [(f"dim_{i}", pa.int64()) for i in range(ndim)]
        + [
            ("present", pa.int8()),
            ("path", pa.string()),
            ("offset", pa.int64()),
            ("size", pa.int64()),
            ("info", pa.string()),
            ("data", pa.binary()),
        ]
    )


SOURCES_SCHEMA = pa.schema(
    [("source", pa.string()), ("size", pa.int64()), ("mtime", pa.int64()), ("etag", pa.string())]
)
CHANGES_SCHEMA = pa.schema([("array", pa.string()), ("key", pa.string())])


def to_parquet(rows, schema):
    buf = io.BytesIO()
    # fixed writer settings so equal rows give equal bytes, hence equal names
    pq.write_table(
        rows if isinstance(rows, pa.Table) else pa.Table.from_pylist(rows, schema=schema),
        buf,
        compression="zstd",
        write_statistics=True,
        store_schema=False,
    )
    return buf.getvalue()


def from_parquet(data):
    return pq.read_table(io.BytesIO(data)).to_pylist()


def _tier(n, fanout):
    t = 0
    while n >= fanout:
        n //= fanout
        t += 1
    return t


def tiered(frags, fanout, size, merge):
    """Merge every run of `fanout` adjacent fragments of one size tier, until
    none is left: a list of N units settles at about fanout * log(N) entries."""
    again = True
    while again:
        again, i = False, 0
        while i + fanout <= len(frags):
            run = frags[i : i + fanout]
            if len({_tier(size(f), fanout) for f in run}) == 1:
                frags[i : i + fanout] = [merge(run)]
                again = True
            else:
                i += 1
    return frags


# ---------------------------------------------------------------- store


def _dims(r, nd):
    return tuple(r[f"dim_{i}"] for i in range(nd))


class Store:
    def __init__(self, root, fanout=None):
        self.o = S3Objects(root) if root.startswith("s3://") else Objects(root)
        self._src_cache = {}
        self._lists = {}
        self._meta = {}
        self.fanout = fanout  # None: no automatic compaction

    # -- versions and tags

    def latest(self):
        v = self.o.list("versions")
        return int(os.path.basename(v[-1]).split(".")[0]) if v else 0

    def manifest(self, version):
        if isinstance(version, str):
            version = json.loads(self.o.get(f"tags/{version}.json"))["version"]
        if version == 0:  # before the first commit: an empty table
            return dict(version=0, table=dict(format_version="mdim-refs/0.2", remap=[], arrays=[]), table_keys={},
                        refs={}, sources=None)
        m = json.loads(self.o.get(f"versions/{version:08d}.json"))
        m["table_keys"] = self._table_keys(m["table"])
        m["table"] = self._get_table(m["table"])
        return m

    # -- array descriptions: one content-addressed object each, so a commit
    #    restates only the arrays whose description changed

    def _put_meta(self, obj):
        data = json.dumps(obj, separators=(",", ":"), sort_keys=True).encode()
        key = f"meta/{sha256(data)}.json"
        if key not in self._meta:
            self.o.put_new(key, data)
            self._meta[key] = obj
        return key

    def _get_meta(self, key):
        if key not in self._meta:
            self._meta[key] = json.loads(self.o.get(key))
        return self._meta[key]

    def put_table(self, table):
        head = {k: v for k, v in table.items() if k != "arrays"}
        head["arrays"] = {a["array"]: self._put_meta(a) for a in table["arrays"]}
        return self._put_meta(head)

    def _table_keys(self, key):
        return [key] + list(self._get_meta(key)["arrays"].values())

    def _get_table(self, key):
        head = self._get_meta(key)
        return dict({k: v for k, v in head.items() if k != "arrays"},
                    arrays=[json.loads(json.dumps(self._get_meta(k))) for _, k in sorted(head["arrays"].items())])

    def tag(self, name, version):
        m = self.o.get(f"versions/{version:08d}.json")
        body = json.dumps({"version": version, "manifest_sha256": sha256(m)}).encode()
        if not self.o.put_new(f"tags/{name}.json", body):
            raise Conflict(f"tag {name} exists")

    def put_frag(self, rows, schema):
        data = to_parquet(rows, schema)
        key = f"frags/{sha256(data)}.parquet"
        self.o.put_new(key, data)  # content-addressed: existing means identical
        return key

    LEAF = 64  # entries per list object

    def _put_json(self, obj):
        data = json.dumps(obj, separators=(",", ":")).encode()
        key = f"lists/{sha256(data)}.json"
        if key not in self._lists:
            self.o.put_new(key, data)
            self._lists[key] = None
        return key

    def put_list(self, entries):
        """A fragment list as a tree of content-addressed objects, LEAF entries
        each, cut at fixed positions: an append rewrites the last leaf and the
        path above it, and every other leaf is shared with the version before."""
        if len(entries) <= self.LEAF:
            return self._put_json(entries)
        subs = [{"list": self._put_json(entries[i : i + self.LEAF])} for i in range(0, len(entries), self.LEAF)]
        return self.put_list(subs)

    def list_keys(self, key):
        """Every object of the list tree under key."""
        if key is None:
            return []
        node = json.loads(self.o.get(key))
        out = [key]
        if node and "list" in node[0]:
            for e in node:
                out += self.list_keys(e["list"])
        return out

    def get_list(self, key):
        if key is None:
            return []
        if self._lists.get(key) is None:
            node = json.loads(self.o.get(key))
            if node and "list" in node[0]:
                node = [f for e in node for f in self.get_list(e["list"])]
            self._lists[key] = node
        return self._lists[key]

    def frags(self, m, name):
        """The fragments of one array in a version: [{lo, hi, path, rows}], by lo."""
        return self.get_list(m["refs"].get(name))

    def merge_frags(self, run):
        """Several fragments as one, rows unchanged and still in key order."""
        tabs = [pq.read_table(io.BytesIO(self.o.get(f["path"]))) for f in run]
        buf = io.BytesIO()
        pq.write_table(pa.concat_tables(tabs), buf, compression="zstd", write_statistics=True, store_schema=False)
        data = buf.getvalue()
        key = f"frags/{sha256(data)}.parquet"
        self.o.put_new(key, data)
        out = dict(path=key, rows=sum(f["rows"] for f in run))
        if "lo" in run[0]:
            out = dict(lo=run[0]["lo"], hi=run[-1]["hi"], **out)
        return out

    # -- reading a version as a table

    def array(self, m, name):
        for a in m["table"]["arrays"]:
            if a["array"] == name:
                return a
        raise KeyError(name)

    def refs_rows(self, m, name):
        rows = []
        for f in self.frags(m, name):
            rows += from_parquet(self.o.get(f["path"]))
        return rows

    def frag_paths(self, m, name):
        return [self.o.local(f["path"]) for f in self.frags(m, name)]

    def sources(self, m):
        key = tuple(f["path"] for f in self.get_list(m.get("sources")))
        if key not in self._src_cache:
            out = {}
            for p in key:
                for r in from_parquet(self.o.get(p)):
                    out[r["source"]] = r
            self._src_cache = {key: out}
        return self._src_cache[key]

    @staticmethod
    def covering(frags, k0):
        """Index of the fragment whose [lo, hi) range of dim_0 holds k0, or None."""
        i = bisect.bisect_right([f["lo"] for f in frags], k0) - 1
        return i if i >= 0 and k0 < frags[i]["hi"] else None

    def chunk_row(self, m, name, key):
        frags = self.frags(m, name)
        i = self.covering(frags, int(key[0]))
        if i is None:
            return None
        for r in from_parquet(self.o.get(frags[i]["path"])):
            if all(r[f"dim_{j}"] == k for j, k in enumerate(key)):
                return r
        return None

    def coord_values(self, m, name):
        """A coordinate's values, however the version holds them."""
        import numpy as np

        a = self.array(m, name)
        n = a["shape"][0]
        if a["storage"] == "affine":
            return a["affine"]["start"] + a["affine"]["step"] * np.arange(n)
        if a["storage"] == "inline":
            return np.asarray(a["values"], dtype=a["dtype"])
        rows = sorted(self.refs_rows(m, name), key=lambda r: r["dim_0"])
        buf = b"".join(decode_v2(a, self.o.get(r["path"], r["offset"], r["size"])) for r in rows if r["present"])
        return np.frombuffer(buf, dtype=a["dtype"])[:n].copy()

    # -- transactions

    def begin(self, writer="w"):
        return Txn(self, self.latest(), writer)

    # -- fetch of stored bytes for one row, with the source check

    def fetch(self, m, row, resolve, verify=True):
        if row is None or not row["present"]:
            return None
        if row["offset"] is None:
            return row["data"]
        path = row["path"]
        if path.startswith("packs/"):
            return self.o.get(path, row["offset"], row["size"])
        local = resolve(path)
        if verify:
            s = self.sources(m).get(path)
            if s is None:
                raise StaleSource(f"{path}: no sources row in this version")
            if os.path.getsize(local) != s["size"] or file_etag(local) != s["etag"]:
                raise StaleSource(f"{path}: changed since it was scanned (etag {s['etag'][:19]})")
        with open(local, "rb") as f:
            f.seek(row["offset"])
            return f.read(row["size"])

    # -- renders and operations that are views

    def checkout(self, version, outdir, remap_target, extra_remap):
        """The three-file export of one version, for readers that know only that."""
        m = self.manifest(version)
        os.makedirs(outdir, exist_ok=True)
        t = json.loads(json.dumps(m["table"]))
        t["remap"] = [r for r in t.get("remap", []) if r["target"] != remap_target]
        t["remap"] += [dict(target=remap_target, prefix=p, replacement=r) for p, r in extra_remap]
        t["remap"].append(dict(target=remap_target, prefix="packs/", replacement=self.o.url("packs") + "/"))
        for a in t["arrays"]:
            if a["kind"] == "coord" and a["storage"] == "referenced":
                # readers of the export expect a coordinate's values inline
                v = self.coord_values(m, a["array"])
                a.update(storage="inline", values=[float(x) if "f" in a["dtype"] else int(x) for x in v],
                         chunks=[len(v)], codec=None)
        with open(os.path.join(outdir, "arrays.json"), "w") as f:
            json.dump(t, f, indent=1)
        for a in t["arrays"]:
            if a["storage"] != "referenced":
                continue
            rows = self.refs_rows(m, a["array"])
            pq.write_table(
                pa.Table.from_pylist(rows, schema=refs_schema(len(a["shape"]))),
                os.path.join(outdir, f"refs_{a['array']}.parquet"),
            )
        return m["version"]

    def reachable(self, versions):
        keep = set()
        import duckdb

        seen = set()
        for v in versions:
            m = self.manifest(v)
            keep.add(f"versions/{v:08d}.json")
            keep.update(m["table_keys"])
            frags = [f["path"] for a in m["refs"] for f in self.frags(m, a)]
            keep.update(frags)
            for k in list(m["refs"].values()) + [m.get("sources")]:
                if k not in seen:
                    seen.add(k)
                    keep.update(self.list_keys(k))
            keep.update(f["path"] for f in self.get_list(m.get("sources")))
            if m.get("changes"):
                keep.add(m["changes"])
            new = [f for f in frags if f not in seen]
            seen.update(new)
            if new:
                q = "SELECT DISTINCT path FROM read_parquet(?, union_by_name=true) WHERE path LIKE 'packs/%'"
                keep.update(r[0] for r in duckdb.execute(q, [[self.o.local(f) for f in new]]).fetchall())
        return keep

    def gc(self, keep_versions, dry_run=False):
        """Delete what no kept version reaches: an anti-join of listing and reachable.
        A real store also spares objects younger than a grace period, so an
        uncommitted writer's packs are not collected under it."""
        keep = self.reachable(keep_versions)
        listed = self.o.list("versions") + self.o.list("meta") + self.o.list("lists") + self.o.list("frags") + self.o.list("packs")
        dead = [k for k in listed if k not in keep]
        freed = sum(self.o.size(k) for k in dead)
        if not dry_run:
            for k in dead:
                self.o.delete(k)
        return dead, freed


def file_etag(path):
    with open(path, "rb") as f:
        return "sha256:" + sha256(f.read())


# ---------------------------------------------------------------- txn


class Txn:
    """A list of operations, replayed on whatever version it finally commits over."""

    def __init__(self, store, base, writer):
        self.s, self.base, self.writer = store, base, writer
        self.arrays = {}  # name -> desc: upserts of array metadata
        self.rows = {}  # (array, key) -> row dict without dims
        self.bytes = {}  # (array, key) -> encoded bytes to pack
        self.srcs = {}  # source -> row
        self.compactions = []  # (array, lo, hi): merge fragments inside that range
        self.merge_sources = False
        self.group_attrs = None  # attributes of the whole table, when set

    def put_group_attrs(self, attrs):
        """Attributes of the table as a whole (a Zarr group's, a netCDF file's
        global ones). The last writer wins; this conflicts with nothing."""
        self.group_attrs = dict(attrs)

    def put_array(self, desc):
        self.arrays[desc["array"]] = desc

    def write_chunk(self, array, key, encoded, info=PACK_INFO):
        self.bytes[(array, tuple(key))] = (bytes(encoded), info)

    def put_coord(self, name, values, attrs=None, dtype="<f8", chunk=COORD_CHUNK):
        """A coordinate as a chunked referenced array: its values go into a pack
        like any other write, raw and little-endian, each chunk padded to full
        length. Its description then holds no values, so it stays small."""
        import numpy as np

        v = np.asarray(values, dtype=dtype)
        self.put_array(dict(array=name, kind="coord", storage="referenced", dims=[name], shape=[len(v)], chunks=[chunk],
                            dtype=dtype, fill=None, codec={"compressor": None, "filters": None}, attrs=attrs or {},
                            affine=None, values=None))
        self._coord_chunks(name, v, 0, chunk, dtype)

    def append_coord(self, name, values):
        """Extend a referenced coordinate: rewrites its last, partly filled chunk
        and adds new ones. Nothing else of the coordinate is touched."""
        import numpy as np

        m = self.s.manifest(self.base)
        a = json.loads(json.dumps(self.s.array(m, name)))
        if a["kind"] != "coord" or a["storage"] != "referenced":
            raise ValueError(f"{name} is not a referenced coordinate; use put_coord first")
        n0, c = a["shape"][0], a["chunks"][0]
        k = n0 // c
        head = self.s.coord_values(m, name)[k * c :]
        v = np.concatenate([head, np.asarray(values, dtype=a["dtype"])])
        a["shape"] = [n0 + len(values)]
        self.put_array(a)
        self._coord_chunks(name, v, k, c, a["dtype"])

    def _coord_chunks(self, name, v, first, chunk, dtype):
        import numpy as np

        for i in range(0, len(v), chunk):
            part = v[i : i + chunk]
            pad = np.full(chunk - len(part), np.nan if "f" in dtype else 0, dtype=dtype)
            self.write_chunk(name, (first + i // chunk,), np.concatenate([part, pad]).astype(dtype).tobytes(), RAW_INFO)

    def put_ref(self, array, key, path, offset, size, info):
        self.rows[(array, tuple(key))] = dict(present=1, path=path, offset=offset, size=size, info=info, data=None)

    def put_absent(self, array, key):
        self.rows[(array, tuple(key))] = dict(present=0, path=None, offset=None, size=None, info=None, data=None)

    def put_source(self, source, size, mtime, etag):
        self.srcs[source] = dict(source=source, size=size, mtime=mtime, etag=etag)

    def compact(self, array, lo, hi):
        """Merge the fragments of array lying inside chunk range [lo, hi) of dim_0.
        Changes no row, so it conflicts with nothing."""
        self.compactions.append((array, lo, hi))

    def compact_sources(self):
        self.merge_sources = True

    def _pack(self):
        """One pack per commit. Equal chunks share one byte range."""
        if not self.bytes:
            return
        seen, buf = {}, bytearray()
        for ak in sorted(self.bytes):
            b = self.bytes[ak][0]
            h = sha256(b)
            if h not in seen:
                seen[h] = (len(buf), len(b))
                buf += b
        data = bytes(buf)
        name = f"packs/{sha256(data)}.pack"
        self.s.o.put_new(name, data)
        for ak, (b, info) in self.bytes.items():
            off, size = seen[sha256(b)]
            self.rows[ak] = dict(present=1, path=name, offset=off, size=size, info=info, data=None)
        self.srcs[name] = dict(source=name, size=len(data), mtime=None, etag="sha256:" + sha256(data))
        self.bytes = {}

    def _conflicts(self, since, upto):
        mine_keys = {(a, keystr(k)) for a, k in self.rows}
        mine_arrays = {a for a, _ in self.rows}
        mine_meta = set(self.arrays)
        for v in range(since + 1, upto + 1):
            m = self.s.manifest(v)
            theirs = from_parquet(self.s.o.get(m["changes"])) if m.get("changes") else []
            their_keys = {(r["array"], r["key"]) for r in theirs}
            their_arrays = {r["array"] for r in theirs}
            their_meta = set(m.get("meta_changed", []))
            hit = mine_keys & their_keys
            if hit:
                raise Conflict(f"version {v} ({m['writer']}) wrote the same chunks: {sorted(hit)[:3]}")
            hit = (mine_meta & (their_meta | their_arrays)) | (their_meta & mine_arrays)
            if hit:
                raise Conflict(f"version {v} ({m['writer']}) changed array metadata under this write: {sorted(hit)}")

    def _build(self, base):
        s = self.s
        m = s.manifest(base)
        table = json.loads(json.dumps(m["table"]))
        for name, desc in self.arrays.items():
            table["arrays"] = [a for a in table["arrays"] if a["array"] != name] + [desc]
        table["arrays"].sort(key=lambda a: a["array"])
        if self.group_attrs is not None:
            table["attrs"] = self.group_attrs
        ndim = {a["array"]: len(a["shape"]) for a in table["arrays"]}
        refs = {a: list(s.frags(m, a)) for a in m["refs"]}

        # rows: rewrite the fragment that covers a key (copy on write), or
        # start a new fragment for keys no fragment covers (an append)
        touched = {}
        for (a, k), row in self.rows.items():
            frags = refs.setdefault(a, [])
            i = s.covering(frags, k[0])
            slot = ("old", i) if i is not None else ("new", bisect.bisect_right([f["lo"] for f in frags], k[0]))
            touched.setdefault((a, slot), {})[k] = dict({f"dim_{j}": v for j, v in enumerate(k)}, **row)
        replace, add = {}, {}
        for (a, (kind, i)), new in touched.items():
            cur = {}
            if kind == "old":
                for r in from_parquet(s.o.get(refs[a][i]["path"])):
                    cur[_dims(r, ndim[a])] = r
            cur.update(new)
            rows = [cur[k] for k in sorted(cur)]
            path = s.put_frag(rows, refs_schema(ndim[a]))
            if kind == "old":
                replace[(a, i)] = dict(refs[a][i], path=path, rows=len(rows))
            else:
                k0 = [k[0] for k in new]
                add.setdefault(a, []).append(dict(lo=min(k0), hi=max(k0) + 1, path=path, rows=len(rows)))
        for (a, i), f in replace.items():
            refs[a][i] = f
        for a, fs in add.items():
            refs[a] = sorted(refs[a] + fs, key=lambda f: f["lo"])

        for a, lo, hi in self.compactions:
            inside = [f for f in refs.get(a, []) if f["lo"] >= lo and f["hi"] <= hi]
            if len(inside) > 1:
                refs[a] = sorted([f for f in refs[a] if f not in inside] + [s.merge_frags(inside)], key=lambda f: f["lo"])
        if s.fanout:
            for a in {a for a, _ in self.rows}:
                refs[a] = tiered(refs[a], s.fanout, lambda f: f["hi"] - f["lo"], s.merge_frags)

        sources = list(s.get_list(m.get("sources")))
        if self.srcs:
            sources.append(dict(path=s.put_frag([self.srcs[k] for k in sorted(self.srcs)], SOURCES_SCHEMA), rows=len(self.srcs)))
        if self.merge_sources and len(sources) > 1:
            sources = [s.merge_frags(sources)]
        elif s.fanout:
            sources = tiered(sources, s.fanout, lambda f: f["rows"], s.merge_frags)
        changes = s.put_frag([dict(array=a, key=keystr(k)) for a, k in sorted(self.rows)], CHANGES_SCHEMA) if self.rows else None
        return dict(table=s.put_table(table), refs={a: s.put_list(f) for a, f in refs.items()}, sources=s.put_list(sources), changes=changes)

    def commit(self, message, max_tries=20):
        self._pack()
        base = self.base
        for _ in range(max_tries):
            body = self._build(base)
            n = base + 1
            man = dict(
                format=FORMAT,
                version=n,
                parent=base,
                writer=self.writer,
                message=message,
                created=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                meta_changed=sorted(self.arrays),
                **body,
            )
            if self.s.o.put_new(f"versions/{n:08d}.json", json.dumps(man, separators=(",", ":")).encode()):
                self.rebased_over = n - 1 - self.base
                return n
            latest = self.s.latest()
            self._conflicts(base, latest)  # raises if the same chunks or arrays moved
            base = latest
        raise Conflict("gave up after %d tries" % max_tries)


def init(store, table, rows_by_array, sources, part_size=1, writer="import", message="import"):
    """Version 1 from table metadata (the arrays.json content), an iterable of
    refs rows per array, and sources rows. part_size is how many dim_0 chunk
    indexes one fragment covers."""
    s = store
    refs, changes = {}, []
    for a in table["arrays"]:
        name, nd = a["array"], len(a["shape"])
        if name not in rows_by_array:
            continue
        parts = {}
        for r in rows_by_array[name]:
            parts.setdefault(r["dim_0"] // part_size, []).append(r)
            changes.append(dict(array=name, key=keystr(_dims(r, nd))))
        refs[name] = s.put_list([
            dict(lo=p * part_size, hi=(p + 1) * part_size,
                 path=s.put_frag(sorted(v, key=lambda r: _dims(r, nd)), refs_schema(nd)), rows=len(v))
            for p, v in sorted(parts.items())
        ])
    man = dict(
        format=FORMAT,
        version=1,
        parent=0,
        writer=writer,
        message=message,
        created=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        meta_changed=sorted(a["array"] for a in table["arrays"]),
        table=s.put_table(table),
        refs=refs,
        sources=s.put_list([dict(path=s.put_frag(sorted(sources, key=lambda r: r["source"]), SOURCES_SCHEMA), rows=len(sources))]),
        changes=s.put_frag(changes, CHANGES_SCHEMA),
    )
    if not s.o.put_new("versions/00000001.json", json.dumps(man, separators=(",", ":")).encode()):
        raise Conflict("store already has a version 1")
    return 1


def init_fragments(store, table, refs, sources, writer="import", message="import"):
    """Version 1 from fragments already stored: refs maps an array to its
    fragment entries [{lo, hi, path, rows}]. For a bulk load too large to
    pass through init as rows. No change keys are recorded: nothing can have
    started before version 1, so nothing can conflict with it."""
    s = store
    man = dict(
        format=FORMAT,
        version=1,
        parent=0,
        writer=writer,
        message=message,
        created=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        meta_changed=sorted(a["array"] for a in table["arrays"]),
        table=s.put_table(table),
        refs={a: s.put_list(sorted(f, key=lambda f: f["lo"])) for a, f in refs.items()},
        sources=s.put_list([dict(path=s.put_frag(sorted(sources, key=lambda r: r["source"]), SOURCES_SCHEMA), rows=len(sources))]),
        changes=None,
    )
    if not s.o.put_new("versions/00000001.json", json.dumps(man, separators=(",", ":")).encode()):
        raise Conflict("store already has a version 1")
    return 1


def import_export(store, export_dir, part_size=1, resolve=None, writer="import", message="import"):
    """Version 1 from a three-file byterefs export (arrays.json, refs_<array>.parquet)."""
    with open(os.path.join(export_dir, "arrays.json")) as f:
        table = json.load(f)
    rows, srcs = {}, {}
    for a in table["arrays"]:
        if a["storage"] != "referenced":
            continue
        rows[a["array"]] = pq.read_table(os.path.join(export_dir, f"refs_{a['array']}.parquet")).to_pylist()
        for r in rows[a["array"]]:
            if r["path"] and resolve and r["path"] not in srcs:
                p = resolve(r["path"])
                srcs[r["path"]] = dict(source=r["path"], size=os.path.getsize(p), mtime=int(os.path.getmtime(p)), etag=file_etag(p))
    return init(store, table, rows, list(srcs.values()), part_size, writer, message)


def _fill_json(fill, dtype):
    """A stored fill value as Zarr V2 metadata spells it."""
    if fill is None:
        return None
    if dtype[1] in "fc":
        f = float(fill)
        return "NaN" if f != f else ("Infinity" if f == float("inf") else ("-Infinity" if f == float("-inf") else f))
    return int(float(fill)) if dtype[1] in "iu" else fill


def render_kerchunk_json(store, version, out_path, url_for):
    """One version as kerchunk JSON (version 1) over Zarr v2 metadata: a render,
    like any other, so GDAL (3.11 or later, /vsikerchunk_json_ref/) and
    fsspec can open it. url_for maps a row's path to what the reader opens.
    Only the Zarr V2 form of arrays.codec is handled here."""
    import base64

    import numpy as np

    m = store.manifest(version)
    refs = {".zgroup": json.dumps({"zarr_format": 2})}
    for a in m["table"]["arrays"]:
        name, nd = a["array"], len(a["shape"])
        codec = a.get("codec") or {}
        if isinstance(codec, list):
            raise ValueError(f"{name}: V3 codec chain not handled by this render")
        fill = a.get("fill")
        zarray = dict(zarr_format=2, shape=a["shape"], chunks=a["chunks"], dtype=a["dtype"], order="C",
                      compressor=codec.get("compressor"), filters=codec.get("filters") or None,
                      fill_value=_fill_json(fill, a["dtype"]),
                      dimension_separator=".")
        refs[f"{name}/.zarray"] = json.dumps(zarray)
        refs[f"{name}/.zattrs"] = json.dumps(dict(a.get("attrs") or {}, _ARRAY_DIMENSIONS=a["dims"]))
        if a["storage"] == "referenced":
            for r in store.refs_rows(m, name):
                if not r["present"]:
                    continue
                key = name + "/" + ".".join(str(r[f"dim_{i}"]) for i in range(nd))
                if r["offset"] is None:
                    refs[key] = "base64:" + base64.b64encode(r["data"]).decode()
                else:
                    refs[key] = [url_for(r["path"]), r["offset"], r["size"]]
        else:
            if a["storage"] == "affine":
                v = a["affine"]["start"] + a["affine"]["step"] * np.arange(a["shape"][0])
            else:
                v = np.asarray(a["values"])
            refs[f"{name}/0"] = "base64:" + base64.b64encode(v.astype(a["dtype"]).tobytes()).decode()
    with open(out_path, "w") as f:
        json.dump({"version": 1, "refs": refs}, f)
    return len(refs)
