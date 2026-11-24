"""A Zarr write interface over a brstore store.

A Session is a zarr-python (3.x) Store, in Zarr format 2 or 3. Reads come
from one committed version. Writes are held in memory until commit(), which
puts the chunk bytes in one pack and makes one new version: everything a
single xarray.Dataset.to_zarr() call wrote lands atomically, or not at all.

  s = brstore.Store(root)                    # a directory, or s3://bucket/prefix
  z = brzarr.Session(s, writer="me")         # zarr_format=3 unless told 2
  ds.to_zarr(z)
  version = z.commit("first write")

The format is how a session speaks, not how the store holds an array: an
array written in one format reads in the other, as stored values (xarray
spells a missing value differently in the two formats, and that is not
translated). An array's codec is kept
in its Zarr V2 spelling whenever it has one (bytes, then zstd, gzip, blosc
or zlib, with or without shuffle), so every reader of the table can use
it; any other V3 chain is kept as written and reads through a format 3
session only.

Not handled: nested groups, zero-dimensional arrays, Fortran order, sharding,
deleting an array.
"""
import json

from zarr.abc.store import Store as ZarrStore
from zarr.core.buffer import default_buffer_prototype

import brstore

META = {2: (".zgroup", ".zattrs", ".zarray", ".zmetadata"), 3: ("zarr.json",)}
V3_TYPES = {"bool": "b1", "int8": "i1", "int16": "i2", "int32": "i4", "int64": "i8", "uint8": "u1", "uint16": "u2",
            "uint32": "u4", "uint64": "u8", "float32": "f4", "float64": "f8"}
V3_NAMES = {v: k for k, v in V3_TYPES.items()}
BLOSC_SHUFFLE = ["noshuffle", "shuffle", "bitshuffle"]


def _info(a):
    """The per-row block-info text for chunks of this array."""
    parts, codec = [], a.get("codec") or {}
    if isinstance(codec, list):
        parts += ["COMPRESSION=" + c["name"].split(".")[-1].upper() for c in codec[1:]]
    else:
        comp = (codec.get("compressor") or {}).get("id")
        if comp:
            parts.append("COMPRESSION=" + {"zlib": "DEFLATE", "gzip": "DEFLATE"}.get(comp, comp.upper()))
        for f in codec.get("filters") or []:
            parts.append("FILTER=" + f["id"].upper())
    if a["dtype"][0] in "<>":
        parts.append("ENDIANNESS=" + ("LITTLE" if a["dtype"][0] == "<" else "BIG"))
    return "; ".join(parts)


# -- Zarr format 2 metadata


def zarray_of(a):
    """Zarr v2 array metadata for one array description."""
    codec = a.get("codec") or {}
    if isinstance(codec, list):
        raise NotImplementedError(f"{a['array']}: its codec has no Zarr V2 spelling; open the session with zarr_format=3")
    return dict(zarr_format=2, shape=a["shape"], chunks=a["chunks"], dtype=a["dtype"], order="C",
                compressor=codec.get("compressor"), filters=codec.get("filters") or None,
                fill_value=brstore._fill_json(a.get("fill"), a["dtype"]), dimension_separator=".")


def desc_of(name, zarray, zattrs):
    """The array description for Zarr v2 metadata as zarr-python wrote it."""
    if zarray.get("order", "C") != "C":
        raise NotImplementedError(f"{name}: only C order")
    if not zarray["shape"]:
        raise NotImplementedError(f"{name}: zero-dimensional arrays are not handled yet")
    attrs = dict(zattrs or {})
    dims = attrs.pop("_ARRAY_DIMENSIONS", None) or [f"{name}_dim{i}" for i in range(len(zarray["shape"]))]
    fill = zarray.get("fill_value")
    return dict(array=name, kind="coord" if dims == [name] else "data", storage="referenced", dims=dims,
                shape=list(zarray["shape"]), chunks=list(zarray["chunks"]), dtype=zarray["dtype"],
                fill=None if fill is None else str(fill),
                codec={"compressor": zarray.get("compressor"), "filters": zarray.get("filters")},
                attrs=attrs, affine=None, values=None)


# -- Zarr format 3 metadata


def _v3_codecs(a):
    """The V3 codec chain for an array description."""
    codec, size = a.get("codec") or {}, int(a["dtype"][2:])
    if isinstance(codec, list):
        return codec
    out = [{"name": "bytes", "configuration": {"endian": "big" if a["dtype"][0] == ">" else "little"}} if size > 1
           else {"name": "bytes"}]
    for f in codec.get("filters") or []:
        out.append({"name": "numcodecs." + f["id"], "configuration": {k: v for k, v in f.items() if k != "id"}})
    c = codec.get("compressor")
    if c:
        cfg = {k: v for k, v in c.items() if k != "id"}
        if c["id"] == "zstd":
            cfg = {"level": cfg.get("level", 0), "checksum": bool(cfg.get("checksum", False))}
        elif c["id"] == "gzip":
            cfg = {"level": cfg.get("level", 5)}
        elif c["id"] == "blosc":
            cfg = {"cname": cfg.get("cname", "zstd"), "clevel": cfg.get("clevel", 5), "typesize": size,
                   "shuffle": BLOSC_SHUFFLE[cfg.get("shuffle", 1)], "blocksize": cfg.get("blocksize", 0)}
        out.append({"name": c["id"] if c["id"] in ("zstd", "gzip", "blosc") else "numcodecs." + c["id"],
                    "configuration": cfg})
    return out


def _v2_codec(codecs):
    """(endian, V2 codec object) for a V3 chain that has a V2 spelling, else None."""
    if not codecs or codecs[0]["name"] != "bytes":
        return None
    endian = (codecs[0].get("configuration") or {}).get("endian", "little")
    filters, comp = [], None
    for c in codecs[1:]:
        n, cfg = c["name"], {k: v for k, v in (c.get("configuration") or {}).items() if k != "id"}
        if comp is not None:  # V2 has nothing after the compressor
            return None
        if n == "numcodecs.shuffle":
            filters.append({"id": "shuffle", **cfg})
        elif n == "zstd" and not cfg.get("checksum"):
            comp = {"id": "zstd", "level": cfg.get("level", 0)}
        elif n == "gzip":
            comp = {"id": "gzip", "level": cfg.get("level", 5)}
        elif n == "blosc":
            comp = {"id": "blosc", "cname": cfg.get("cname", "zstd"), "clevel": cfg.get("clevel", 5),
                    "shuffle": BLOSC_SHUFFLE.index(cfg.get("shuffle", "noshuffle")), "blocksize": cfg.get("blocksize", 0)}
        elif n == "numcodecs.zlib":
            comp = {"id": "zlib", **cfg}
        else:
            return None
    return endian, {"compressor": comp, "filters": filters or None}


def _v3_fill(a):
    fill = brstore._fill_json(a.get("fill"), a["dtype"])
    if fill is None:  # V3 requires one
        return {"f": 0.0, "b": False}.get(a["dtype"][1], 0)
    return fill


def v3_of(a):
    """Zarr v3 array metadata for one array description."""
    if a["dtype"][1:] not in V3_NAMES:
        raise NotImplementedError(f"{a['array']}: dtype {a['dtype']} has no Zarr V3 data type here")
    return dict(shape=a["shape"], data_type=V3_NAMES[a["dtype"][1:]],
                chunk_grid={"name": "regular", "configuration": {"chunk_shape": a["chunks"]}},
                chunk_key_encoding={"name": "default", "configuration": {"separator": "/"}},
                fill_value=_v3_fill(a), codecs=_v3_codecs(a), attributes=dict(a.get("attrs") or {}),
                dimension_names=a["dims"], zarr_format=3, node_type="array", storage_transformers=[])


def desc_of_v3(name, doc, old=None):
    """The array description for Zarr v3 metadata as zarr-python wrote it."""
    if not doc["shape"]:
        raise NotImplementedError(f"{name}: zero-dimensional arrays are not handled yet")
    if doc["chunk_grid"]["name"] != "regular" or doc.get("storage_transformers"):
        raise NotImplementedError(f"{name}: only a regular chunk grid with no storage transformer")
    if doc["data_type"] not in V3_TYPES:
        raise NotImplementedError(f"{name}: data type {doc['data_type']} is not handled yet")
    if any(c["name"] in ("sharding_indexed", "transpose") for c in doc["codecs"]):
        raise NotImplementedError(f"{name}: sharding and transpose are not handled")
    kind = V3_TYPES[doc["data_type"]]
    v2 = _v2_codec(doc["codecs"])
    endian = "|" if kind[1] == "1" else ("<" if (v2[0] if v2 else (doc["codecs"][0].get("configuration") or {}).get(
        "endian", "little")) == "little" else ">")
    dims = doc.get("dimension_names") or [f"{name}_dim{i}" for i in range(len(doc["shape"]))]
    desc = dict(array=name, kind="coord" if dims == [name] else "data", storage="referenced", dims=dims,
                shape=list(doc["shape"]), chunks=list(doc["chunk_grid"]["configuration"]["chunk_shape"]),
                dtype=endian + kind, fill=str(doc["fill_value"]), codec=v2[1] if v2 else doc["codecs"],
                attrs=dict(doc.get("attributes") or {}), affine=None, values=None)
    if old is not None and old.get("fill") is None and doc["fill_value"] == _v3_fill(old):
        desc["fill"] = None  # the fill this session invented to serve the array; the store still has none
    return desc


class Session(ZarrStore):
    supports_writes = True
    supports_deletes = True
    supports_listing = True
    supports_partial_writes = False

    def __init__(self, store, writer="zarr", version=None, resolve=None, read_only=False, zarr_format=3):
        """resolve maps the path of a row outside the store to something open()
        can read; without it only chunks held in packs can be read back."""
        super().__init__(read_only=read_only)
        if zarr_format not in (2, 3):
            raise ValueError("zarr_format is 2 or 3")
        self.s, self.writer, self.resolve, self.fmt = store, writer, resolve, zarr_format
        self._reset(store.latest() if version is None else store.manifest(version)["version"])

    def _reset(self, base):
        self.base = base
        self.m = self.s.manifest(base)
        self.descs = {a["array"]: a for a in self.m["table"]["arrays"]}
        self.meta = {}  # key -> bytes: metadata documents written in this session
        self.chunks = {}  # (array, key tuple) -> bytes, or None for a delete
        self._frag = {}

    def with_read_only(self, read_only=False):
        return Session(self.s, self.writer, self.base, self.resolve, read_only, self.fmt)

    def __eq__(self, other):
        return isinstance(other, Session) and other.s.o is self.s.o and other.base == self.base and other.fmt == self.fmt

    def __hash__(self):
        return id(self)

    # -- keys

    def _split(self, key):
        """key -> (array or '', metadata name or None, chunk key tuple or None)."""
        if key.rpartition("/")[2] in META[self.fmt]:
            name, _, rest = key.rpartition("/")
            return name, rest, None
        name, _, rest = key.partition("/")
        if self.fmt == 3:
            if not (rest == "c" or rest[:2] in ("c/", "c.")):
                return name, None, None
            rest = rest[2:]
        parts = rest.replace("/", ".").split(".")
        return name, None, tuple(int(p) for p in parts) if all(p.isdigit() for p in parts) else None

    def _array_doc_key(self, name):
        return f"{name}/.zarray" if self.fmt == 2 else f"{name}/zarr.json"

    def _names(self):
        tail = "/" + self._array_doc_key("")[1:]
        new = {k[: -len(tail)] for k in self.meta if k.endswith(tail)}
        return sorted(set(self.descs) | new)

    # -- what the session currently looks like, as Zarr metadata documents

    def _array_doc(self, name):
        k = self._array_doc_key(name)
        if k in self.meta:
            return json.loads(self.meta[k])
        if name not in self.descs:
            return None
        return zarray_of(self.descs[name]) if self.fmt == 2 else v3_of(self.descs[name])

    def _zattrs(self, name):
        k = f"{name}/.zattrs"
        if k in self.meta:
            return json.loads(self.meta[k])
        if name in self.descs:
            a = self.descs[name]
            return dict(a.get("attrs") or {}, _ARRAY_DIMENSIONS=a["dims"])
        return None

    def _group_attrs(self):
        if self.fmt == 2 and ".zattrs" in self.meta:
            return json.loads(self.meta[".zattrs"])
        if self.fmt == 3 and "zarr.json" in self.meta:
            return json.loads(self.meta["zarr.json"]).get("attributes") or {}
        return self.m["table"].get("attrs") or {}

    def _meta_doc(self, name, which):
        if name == "" and not self._names():
            return None
        if self.fmt == 3:
            if name:
                doc = self._array_doc(name)
            else:  # the group, with every array's metadata inline so a reader opens it in one read
                arrays = {n: self._array_doc(n) for n in self._names()}
                doc = dict(attributes=self._group_attrs(), zarr_format=3, node_type="group",
                           consolidated_metadata=dict(kind="inline", must_understand=False, metadata=arrays))
        elif which == ".zmetadata":
            if name:
                return None
            docs = {k: json.loads(self._meta_doc(*self._split(k)[:2])) for k in self._meta_keys() if k != ".zmetadata"}
            doc = {"zarr_consolidated_format": 1, "metadata": docs}
        elif name == "":
            doc = None if which == ".zarray" else ({"zarr_format": 2} if which == ".zgroup" else self._group_attrs())
        elif which == ".zgroup":
            doc = None
        else:
            doc = self._array_doc(name) if which == ".zarray" else self._zattrs(name)
        return None if doc is None else json.dumps(doc).encode()

    def _meta_keys(self):
        if not self._names():
            return []
        if self.fmt == 3:
            return ["zarr.json"] + [f"{n}/zarr.json" for n in self._names()]
        return [".zattrs", ".zgroup", ".zmetadata"] + [f"{n}/{w}" for n in self._names() for w in (".zarray", ".zattrs")]

    def _rows(self, name):
        """Committed rows of one array, by key."""
        if name not in self._frag:
            nd = len(self.descs[name]["shape"])
            self._frag[name] = {tuple(r[f"dim_{i}"] for i in range(nd)): r for r in self.s.refs_rows(self.m, name)}
        return self._frag[name]

    def _chunk(self, name, key):
        if (name, key) in self.chunks:
            return self.chunks[(name, key)]
        a = self.descs.get(name)
        if a is None:
            return None
        if a["storage"] != "referenced":  # a coordinate held as values or as start and step
            return self.s.coord_values(self.m, name).astype(a["dtype"]).tobytes() if key == (0,) else None
        r = self._rows(name).get(key)
        if r is None or not r["present"]:
            return None
        if r["offset"] is None:
            return bytes(r["data"])
        if r["path"].startswith("packs/"):
            return self.s.o.get(r["path"], r["offset"], r["size"])
        if self.resolve is None:
            raise OSError(f"{name}{key} is a reference to {r['path']}; give Session a resolve function to read it")
        with open(self.resolve(r["path"]), "rb") as f:
            f.seek(r["offset"])
            return f.read(r["size"])

    def _chunk_keys(self):
        gone = {k for k, v in self.chunks.items() if v is None}
        out = {k for k, v in self.chunks.items() if v is not None}
        for name, a in self.descs.items():
            if a["storage"] != "referenced":
                out.add((name, (0,)))
            else:
                out |= {(name, k) for k, r in self._rows(name).items() if r["present"]}
        if self.fmt == 3:
            return sorted(f"{n}/c/" + "/".join(map(str, k)) for n, k in out - gone)
        return sorted(f"{n}/" + ".".join(map(str, k)) for n, k in out - gone)

    # -- the zarr Store interface

    async def get(self, key, prototype=None, byte_range=None):
        name, which, ck = self._split(key)
        b = self._meta_doc(name, which) if which else (self._chunk(name, ck) if ck else None)
        if b is None:
            return None
        if byte_range is not None:
            b = _slice(b, byte_range)
        return (prototype or default_buffer_prototype()).buffer.from_bytes(b)

    async def get_partial_values(self, prototype, key_ranges):
        return [await self.get(k, prototype, r) for k, r in key_ranges]

    async def exists(self, key):
        return await self.get(key) is not None

    async def set(self, key, value):
        self._check_writable()
        name, which, ck = self._split(key)
        data = value.to_bytes()
        if which:
            nested = name and (which == ".zgroup" or (self.fmt == 3 and json.loads(data).get("node_type") == "group"))
            if nested or "/" in name:
                raise NotImplementedError(f"nested group {name!r} is not handled")
            self.meta[key] = data
        elif ck:
            self.chunks[(name, ck)] = data
        else:
            raise KeyError(f"not a Zarr format {self.fmt} key: {key}")

    async def delete(self, key):
        self._check_writable()
        name, which, ck = self._split(key)
        if which:
            if key not in self.meta and name in self.descs:
                raise NotImplementedError(f"deleting the committed array {name!r} is not handled")
            self.meta.pop(key, None)
        elif ck:
            self.chunks[(name, ck)] = None

    async def list(self):
        for k in self._meta_keys() + self._chunk_keys():
            yield k

    async def list_prefix(self, prefix):
        async for k in self.list():
            if k.startswith(prefix):
                yield k

    async def list_dir(self, prefix):
        prefix = prefix.rstrip("/")
        pre = prefix + "/" if prefix else ""
        seen = set()
        async for k in self.list():
            if k.startswith(pre):
                head = k[len(pre) :].split("/")[0]
                if head not in seen:
                    seen.add(head)
                    yield head

    # -- commit

    def _pending_desc(self, name):
        """The description an array's written metadata asks for, or None when
        it is what the session served for the committed array."""
        old = self.descs.get(name)
        if self.fmt == 3:
            doc = self._array_doc(name)
            if old is not None and doc == v3_of(old):
                return None
            return desc_of_v3(name, doc, old)
        return desc_of(name, self._array_doc(name), self._zattrs(name))

    def commit(self, message, max_tries=20):
        """Everything written since the session opened becomes one version."""
        t = brstore.Txn(self.s, self.base, self.writer)
        changed = {k.rpartition("/")[0] for k in self.meta if "/" in k}
        for name in sorted(changed):
            desc, old = self._pending_desc(name), self.descs.get(name)
            if desc is None or desc == old:
                continue
            if old and len(desc["shape"]) != len(old["shape"]):
                raise NotImplementedError(f"{name}: changing the number of dimensions is not handled")
            if old and old["storage"] != "referenced" and (name, (0,)) not in self.chunks:
                # an inline coordinate being extended: its values become chunk 0
                self.chunks[(name, (0,))] = self._chunk(name, (0,))
            t.put_array(desc)
            self.descs[name] = desc
        if self._group_attrs() != (self.m["table"].get("attrs") or {}):
            t.put_group_attrs(self._group_attrs())
        for (name, key), data in sorted(self.chunks.items()):
            if name not in self.descs:
                raise KeyError(f"chunk written for {name!r}, which has no metadata")
            if self.descs[name]["storage"] != "referenced":
                raise NotImplementedError(f"{name}: writing into a coordinate held inline; rewrite it whole")
            if data is None:
                t.put_absent(name, key)
            else:
                t.write_chunk(name, key, data, _info(self.descs[name]))
        if not (t.arrays or t.bytes or t.rows or t.group_attrs is not None):
            return self.base
        v = t.commit(message, max_tries)
        self.rebased_over = t.rebased_over
        self._reset(v)
        return v


def _slice(b, r):
    name = type(r).__name__
    if name == "RangeByteRequest":
        return b[r.start : r.end]
    if name == "OffsetByteRequest":
        return b[r.offset :]
    if name == "SuffixByteRequest":
        return b[-r.suffix :]
    raise ValueError(r)
