"""A Zarr write interface over a brstore store.

A Session is a zarr-python (3.x) Store speaking Zarr format 2. Reads come
from one committed version. Writes are held in memory until commit(), which
puts the chunk bytes in one pack and makes one new version: everything a
single xarray.Dataset.to_zarr() call wrote lands atomically, or not at all.

  s = brstore.Store(root)
  z = brzarr.Session(s, writer="me")
  ds.to_zarr(z, zarr_format=2)
  version = z.commit("first write")

Not handled: nested groups, zero-dimensional arrays, Fortran order, deleting
an array, Zarr format 3.
"""
import json

import numpy as np
from zarr.abc.store import Store as ZarrStore
from zarr.core.buffer import default_buffer_prototype

import brstore

META = (".zgroup", ".zattrs", ".zarray", ".zmetadata")


def _info(zarray):
    """The per-row block-info text for chunks of this array."""
    parts = []
    comp = (zarray.get("compressor") or {}).get("id")
    if comp:
        parts.append("COMPRESSION=" + {"zlib": "DEFLATE", "gzip": "DEFLATE"}.get(comp, comp.upper()))
    for f in zarray.get("filters") or []:
        parts.append("FILTER=" + f["id"].upper())
    if zarray["dtype"][0] in "<>":
        parts.append("ENDIANNESS=" + ("LITTLE" if zarray["dtype"][0] == "<" else "BIG"))
    return "; ".join(parts)


def zarray_of(a):
    """Zarr v2 array metadata for one array description."""
    codec = a.get("codec") or {}
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


def _split(key):
    """key -> (array or '', rest). Chunk keys come back with '.' separators."""
    if "/" not in key:
        return ("", key) if key in META else (key, "")
    name, rest = key.split("/", 1)
    return name, rest if rest in META else rest.replace("/", ".")


class Session(ZarrStore):
    supports_writes = True
    supports_deletes = True
    supports_listing = True
    supports_partial_writes = False

    def __init__(self, store, writer="zarr", version=None, resolve=None, read_only=False):
        """resolve maps the path of a row outside the store to something open()
        can read; without it only chunks held in packs can be read back."""
        super().__init__(read_only=read_only)
        self.s, self.writer, self.resolve = store, writer, resolve
        self._reset(store.latest() if version is None else store.manifest(version)["version"])

    def _reset(self, base):
        self.base = base
        self.m = self.s.manifest(base)
        self.descs = {a["array"]: a for a in self.m["table"]["arrays"]}
        self.meta = {}  # key -> bytes: metadata documents written in this session
        self.chunks = {}  # (array, key tuple) -> bytes, or None for a delete
        self._frag = {}

    def with_read_only(self, read_only=False):
        return Session(self.s, self.writer, self.base, self.resolve, read_only)

    def __eq__(self, other):
        return isinstance(other, Session) and other.s.o is self.s.o and other.base == self.base

    def __hash__(self):
        return id(self)

    # -- what the session currently looks like, as Zarr v2 keys

    def _zarray(self, name):
        k = f"{name}/.zarray"
        if k in self.meta:
            return json.loads(self.meta[k])
        return zarray_of(self.descs[name]) if name in self.descs else None

    def _zattrs(self, name):
        k = f"{name}/.zattrs"
        if k in self.meta:
            return json.loads(self.meta[k])
        if name in self.descs:
            a = self.descs[name]
            return dict(a.get("attrs") or {}, _ARRAY_DIMENSIONS=a["dims"])
        return None

    def _names(self):
        new = {k.split("/")[0] for k in self.meta if k.endswith("/.zarray")}
        return sorted(set(self.descs) | new)

    def _meta_doc(self, key):
        name, rest = _split(key)
        if rest == ".zmetadata":
            if key in self.meta:
                return self.meta[key]
            docs = {k: json.loads(self._meta_doc(k)) for k in self._meta_keys() if k != ".zmetadata"}
            return json.dumps({"zarr_consolidated_format": 1, "metadata": docs}).encode() if docs else None
        if key in self.meta:
            return self.meta[key]
        if name == "":
            if not self._names() or rest == ".zarray":
                return None
            doc = {"zarr_format": 2} if rest == ".zgroup" else self.m["table"].get("attrs") or {}
        else:
            doc = self._zarray(name) if rest == ".zarray" else self._zattrs(name)
        return None if doc is None else json.dumps(doc).encode()

    def _meta_keys(self):
        keys = set(self.meta)
        if self._names():
            keys |= {".zgroup", ".zattrs", ".zmetadata"}
        for n in self.descs:
            keys |= {f"{n}/.zarray", f"{n}/.zattrs"}
        return sorted(keys)

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
        return sorted(f"{n}/" + ".".join(map(str, k)) for n, k in out - gone)

    # -- the zarr Store interface

    async def get(self, key, prototype=None, byte_range=None):
        name, rest = _split(key)
        if rest in META:
            b = self._meta_doc(key)
        elif rest and all(p.isdigit() for p in rest.split(".")):
            b = self._chunk(name, tuple(int(p) for p in rest.split(".")))
        else:
            b = None
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
        name, rest = _split(key)
        data = value.to_bytes()
        if rest in META:
            if name == "" and rest == ".zgroup" or rest != ".zgroup":
                self.meta[key] = data
            else:
                raise NotImplementedError(f"nested group {name!r} is not handled")
        else:
            self.chunks[(name, tuple(int(p) for p in rest.split(".")))] = data

    async def delete(self, key):
        self._check_writable()
        name, rest = _split(key)
        if rest in META:
            if key not in self.meta and name in self.descs:
                raise NotImplementedError(f"deleting the committed array {name!r} is not handled")
            self.meta.pop(key, None)
        elif rest:
            self.chunks[(name, tuple(int(p) for p in rest.split(".")))] = None

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

    def commit(self, message, max_tries=20):
        """Everything written since the session opened becomes one version."""
        t = brstore.Txn(self.s, self.base, self.writer)
        changed = {k.split("/")[0] for k in self.meta if "/" in k and not k.endswith(".zmetadata")}
        for name in sorted(changed):
            desc = desc_of(name, self._zarray(name), self._zattrs(name))
            if desc != self.descs.get(name):
                if name in self.descs and len(desc["shape"]) != len(self.descs[name]["shape"]):
                    raise NotImplementedError(f"{name}: changing the number of dimensions is not handled")
                old = self.descs.get(name)
                if old and old["storage"] != "referenced" and (name, (0,)) not in self.chunks:
                    # an inline coordinate being extended: its values become chunk 0
                    self.chunks[(name, (0,))] = self._chunk(name, (0,))
                t.put_array(desc)
                self.descs[name] = desc
        if ".zattrs" in self.meta:
            attrs = json.loads(self.meta[".zattrs"])
            if attrs != (self.m["table"].get("attrs") or {}):
                t.put_group_attrs(attrs)
        for (name, key), data in sorted(self.chunks.items()):
            if name not in self.descs:
                raise KeyError(f"chunk written for {name!r}, which has no metadata")
            if self.descs[name]["storage"] != "referenced":
                raise NotImplementedError(f"{name}: writing into a coordinate held inline; rewrite it whole")
            if data is None:
                t.put_absent(name, key)
            else:
                t.write_chunk(name, key, data, _info(zarray_of(self.descs[name])))
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
