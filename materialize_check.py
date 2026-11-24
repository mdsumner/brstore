"""materialize.py on a fixture store behind a counting HTTP server, every result an assert.

  python materialize_check.py <fixture dir> <workdir>

The fixture is as bottom_check.py's (wxylib/fixtures mdimrefs/bluelink-synthetic).
Its netCDF sources are served over HTTP by a server that counts requests and
bytes, so what a way of fetching costs is measured, not estimated. To the
imported store is added temp_same: a virtual array whose every time step
points at the byte ranges of step 0 of temp, the case of many chunks naming
one range. Then

 - the plan, made from rows alone, says what the fetch will cost, and the
   server sees exactly that;
 - one request a chunk row (the obvious way) is measured on the same selection;
 - after materializing, every chunk read through the store equals the bytes
   of the source file, no row of the selection names a source, a second run
   asks for nothing, and the version before still names the sources;
 - a selection materializes only the chunks it touches; in small batches a
   range held by an earlier commit is not fetched again; a stride holds
   every Nth step and a finer one then fetches only the steps between;
 - a Keeper fetches on first read, shares a range between keys, and a new
   Keeper on the same store asks the server for nothing.
"""
import http.server, json, os, shutil, sys, threading

import brstore, materialize

FIX, WORK = os.path.abspath(sys.argv[1]), os.path.abspath(sys.argv[2])
shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(WORK)
table = json.load(open(os.path.join(FIX, "arrays.json")))
rule = next(r for r in table["remap"] if r["target"] == "fixture")
resolve = lambda p: os.path.join(FIX, rule["replacement"] + p[len(rule["prefix"]):])  # noqa: E731

seen = []  # (path, offset, size) of every request the server answered


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        path = os.path.join(FIX, self.path.lstrip("/"))
        a, b = (int(x) for x in self.headers["Range"].split("=")[1].split("-"))
        with open(path, "rb") as f:
            f.seek(a)
            data = f.read(b - a + 1)
        seen.append((self.path, a, len(data)))
        self.send_response(206)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *a):
        pass


server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
BASE = f"http://127.0.0.1:{server.server_address[1]}"


def build(name):
    """The fixture imported, with temp_same added: (version, store)."""
    s = brstore.Store(os.path.join(WORK, name))
    v = brstore.import_export(s, FIX, resolve=resolve)
    m = s.manifest(v)
    a = json.loads(json.dumps(s.array(m, "temp")))
    a["array"] = "temp_same"
    t = s.begin("virtualize")
    t.put_array(a)
    step0 = {(r["dim_1"], r["dim_2"], r["dim_3"]): r for r in s.refs_rows(m, "temp") if r["dim_0"] == 0 and r["present"]}
    for k in range(a["shape"][0]):
        for key, r in step0.items():
            t.put_ref("temp_same", (k,) + key, r["path"], r["offset"], r["size"], r["info"])
    return s, t.commit("temp_same: every step points at the ranges of step 0 of temp")


def from_file(row):
    with open(resolve(row["path"]), "rb") as f:
        f.seek(row["offset"])
        return f.read(row["size"])


s, v1 = build("store")
m1 = s.manifest(v1)
A = s.array(m1, "temp")
nt, nz, ny, nx = A["shape"]
arrays = [s.array(m1, n) for n in ("temp", "temp_same")]
want = {(n, tuple(r[f"dim_{d}"] for d in range(4))): from_file(r) for n in ("temp", "temp_same") for r in s.refs_rows(m1, n) if r["present"]}
print(f"1. version {v1}: temp {A['shape']} in chunks {A['chunks']}, and temp_same, every step naming step 0's ranges: {len(want)} chunk rows")

print("2. the plan, from rows alone")
p = materialize.plan(s, m1, arrays, {})
n_temp = sum(1 for k in want if k[0] == "temp")
n_step0 = sum(1 for k in want if k[0] == "temp" and k[1][0] == 0)
assert p["rows"] == len(want) == n_temp + nt * n_step0 and p["distinct_ranges"] == n_temp, p
assert p["requests"] < p["distinct_ranges"] < p["rows"], p
print(f"   {p['rows']} rows name {p['distinct_ranges']} distinct ranges ({p['mb_named_by_rows']} MB named, {p['mb_distinct']} MB distinct), to go as {p['requests']} requests")

print("3. the obvious way, measured: one request for every chunk row")
read = materialize.source_reader(m1["table"], "fixture", BASE)
n = materialize.naive(s, arrays, {}, read)
assert n["requests"] == len(seen) == p["rows"] and sum(z for _, _, z in seen) == n["bytes_asked_for"]
assert s.latest() == v1, "it writes nothing"
naive_requests, naive_bytes = len(seen), sum(z for _, _, z in seen)
print(f"   the server answered {naive_requests} requests, {naive_bytes} bytes")

print("4. materialized: each distinct range once, neighbours joined")
seen.clear()
args = [os.path.join(WORK, "store"), "--array", "temp", "--array", "temp_same", "--target", "fixture", "--base", BASE]
r = materialize.materialize(s, arrays, {}, materialize.source_reader(m1["table"], "fixture", BASE))
assert len(seen) == r["requests"] == p["requests"], (len(seen), r, p)
assert sum(z for _, _, z in seen) == r["bytes_asked_for"] == p["bytes_to_ask_for"]
assert r["rows"] == p["rows"] and r["distinct_ranges"] == p["distinct_ranges"]
v2 = s.latest()
m2 = s.manifest(v2)
assert r["versions"] == [v2] and v2 == v1 + 1
for name in ("temp", "temp_same"):
    for row in s.refs_rows(m2, name):
        if row["present"]:
            assert row["path"].startswith("packs/"), row
            key = (name, tuple(row[f"dim_{d}"] for d in range(4)))
            assert s.o.get(row["path"], row["offset"], row["size"]) == want[key], key
shared = {(row["path"], row["offset"], row["size"]) for row in s.refs_rows(m2, "temp_same")}
assert len(shared) == n_step0, "the steps of temp_same share one range of the pack each"
assert all(not row["path"].startswith("packs/") for row in s.refs_rows(m1, "temp") if row["present"]), "version 1 still names the sources"
assert r["bytes_packed"] <= sum(len(b) for k, b in want.items() if k[0] == "temp")
print(f"   the server answered {len(seen)} requests, {sum(z for _, _, z in seen)} bytes (the obvious way: {naive_requests} and {naive_bytes});")
print(f"   version {v2}: all {r['rows']} rows point at one pack of {r['bytes_packed']} bytes, every chunk equal to the source file's bytes")
print(f"   {m2['message']}")

print("5. a second run asks for nothing; salt, not selected, still names its sources")
seen.clear()
assert materialize.main(args) == 0 and s.latest() == v2 and not seen
assert all(not row["path"].startswith("packs/") for row in s.refs_rows(m2, "salt") if row["present"])
print("   no request, no new version")

print("6. a selection: two time steps and part of the grid, on a fresh store")
s2, w1 = build("store-sel")
seen.clear()
cy, cx = A["chunks"][2:]
sel = [f"{A['dims'][0]}=1:3", f"{A['dims'][2]}=0:{cy + 1}", f"{A['dims'][3]}={cx}:{2 * cx}"]
assert materialize.main([os.path.join(WORK, "store-sel"), "--array", "temp", "--target", "fixture", "--base", BASE] + [x for q in sel for x in ("--sel", q)]) == 0
mm = s2.manifest(s2.latest())
held = {tuple(row[f"dim_{d}"] for d in range(4)) for row in s2.refs_rows(mm, "temp") if row["present"] and row["path"].startswith("packs/")}
touched = {(t, z, j, 1) for t in (1, 2) for z in range(-(-nz // A["chunks"][1])) for j in (0, 1)}
assert held == {k for n, k in want if n == "temp" and k in touched} and len(held) > 4, sorted(held)
assert len(seen) <= len(held)
print(f"   {sel}: {len(held)} chunks held (the chunks the selection touches), requests: {len(seen)}")

print("6b. in small batches: several commits, and a range held by an earlier one is not fetched again")
s4, b1 = build("store-batch")
mb = s4.manifest(b1)
arrs = [s4.array(mb, n) for n in ("temp", "temp_same")]
pb = materialize.plan(s4, mb, arrs, {}, batch=1)
seen.clear()
rb = materialize.materialize(s4, arrs, {}, materialize.source_reader(mb["table"], "fixture", BASE), batch=1)
assert pb["commits"] == len(rb["versions"]) > 1 and len(seen) == rb["requests"] == pb["requests"], (pb, rb)
assert rb["distinct_ranges"] == n_temp and sum(z for _, _, z in seen) == pb["bytes_to_ask_for"]
me = s4.manifest(s4.latest())
for name in ("temp", "temp_same"):
    for row in s4.refs_rows(me, name):
        if row["present"]:
            assert row["path"].startswith("packs/") and s4.o.get(row["path"], row["offset"], row["size"]) == want[(name, tuple(row[f"dim_{d}"] for d in range(4)))]
print(f"   {len(rb['versions'])} commits, {rb['requests']} requests as planned, {rb['distinct_ranges']} distinct ranges fetched once each; every chunk equal to the source's")

print("6c. a stride: every other step first, then every step fetches only the ones between")
s5, c1 = build("store-stride")
T = A["dims"][0]
root5 = [os.path.join(WORK, "store-stride"), "--array", "temp", "--target", "fixture", "--base", BASE, "--gap", "0"]
steps_held = lambda: sorted({row["dim_0"] for row in s5.refs_rows(s5.manifest(s5.latest()), "temp") if row["present"] and row["path"].startswith("packs/")})  # noqa: E731
seen.clear()
assert materialize.main(root5 + ["--sel", f"{T}=0:{nt}:2"]) == 0
assert steps_held() == list(range(0, nt, 2)), steps_held()
coarse = len(seen)
seen.clear()
assert materialize.main(root5 + ["--sel", f"{T}=0:{nt}:1"]) == 0
assert steps_held() == list(range(nt)) and 0 < len(seen), steps_held()
between = sum(1 for k in want if k[0] == "temp" and k[1][0] % 2 == 1)
assert sum(z for _, _, z in seen) == sum(len(b) for k, b in want.items() if k[0] == "temp" and k[1][0] % 2 == 1), "only the steps between were fetched"
print(f"   stride 2 held steps {list(range(0, nt, 2))} in {coarse} requests; stride 1 then fetched only the {between} chunks of the steps between, in {len(seen)}")

print("7. a Keeper: fetched on first read, kept from then on")
s3, k1 = build("store-keep")
seen.clear()
keys = [(0, 0, 0, 0), (0, 1, 0, 0), (3, 0, 1, 1)]
with materialize.Keeper(s3, materialize.source_reader(s3.manifest(k1)["table"], "fixture", BASE)) as keep:
    for key in keys:
        assert keep.read("temp", key) == want[("temp", key)]
    assert len(seen) == 3
    assert keep.read("temp", keys[0]) == want[("temp", keys[0])] and len(seen) == 3, "a second read of a chunk asks for nothing"
    assert keep.read("temp_same", (2, 0, 0, 0)) == want[("temp", (0, 0, 0, 0))] and len(seen) == 3, "nor does another key that names the same range"
    assert s3.latest() == k1, "nothing is committed until it is flushed or closed"
k2 = s3.latest()
assert k2 == k1 + 1
with materialize.Keeper(s3, materialize.source_reader(s3.manifest(k2)["table"], "fixture", BASE)) as keep:
    for key in keys:
        assert keep.read("temp", key) == want[("temp", key)]
    assert keep.read("temp_same", (2, 0, 0, 0)) == want[("temp", (0, 0, 0, 0))]
    assert keep.read("temp_same", (4, 0, 0, 0)) == want[("temp", (0, 0, 0, 0))], "a key not read before, naming a range already held"
    assert keep.stats["from_source"] == 1 and len(seen) == 4, (keep.stats, len(seen))
print(f"   3 chunks read: 3 requests; read again, and by another key naming the same range: none; version {k2} holds them;")
print("   a new Keeper on that version read them from the pack. A key it had not read, naming a held range, cost one request:")
print("   the store knows a range is held by the rows that were read, not by every row that names it")
server.shutdown()
print("ALL STEPS PASSED")
