"""What only real object storage can answer, asked of one S3 prefix.

  python probe.py s3://bucket/prefix

1. Does a create-if-absent PUT (If-None-Match: *) refuse an existing key?
2. With 16 writers racing for one key, does exactly one win, and is the
   object the winner's body?
3. Is a new key visible to a listing straight after the PUT?
4. How long does one small conditional PUT take?

Everything it writes is under <prefix>/probe/ and is deleted at the end.
Exit status 0 only if 1, 2 and 3 hold: the commit protocol rests on them.
"""
import json, os, statistics, sys, threading, time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from brstore import S3Objects

o = S3Objects(sys.argv[1].rstrip("/"))
tag = "%d-%d" % (os.getpid(), int(time.time()))
out, made = {}, []

k = f"probe/{tag}-once"
made.append(k)
try:
    first, second = o.put_new(k, b"first"), o.put_new(k, b"second")
except Exception as e:  # noqa: an endpoint that rejects the header outright
    print(json.dumps({"conditional_put_error": f"{type(e).__name__}: {e}"[:600], "commit_protocol_safe_here": False}, indent=1))
    sys.exit(1)
out["create_then_create"] = [first, second]
out["second_put_refused"] = bool(first and not second and o.get(k) == b"first")

k = f"probe/{tag}-race"
made.append(k)
wins = []
gate = threading.Barrier(16)


def racer(i):
    gate.wait()
    if o.put_new(k, b"writer-%02d" % i):
        wins.append(i)


th = [threading.Thread(target=racer, args=(i,)) for i in range(16)]
[t.start() for t in th]
[t.join() for t in th]
body = o.get(k)
out["race_winners"] = len(wins)
out["race_one_winner_and_its_body"] = len(wins) == 1 and body == b"writer-%02d" % wins[0]

seen, times = 0, []
for i in range(20):
    k = f"probe/{tag}-list-{i:02d}"
    made.append(k)
    t0 = time.perf_counter()
    assert o.put_new(k, b"x" * 2000)
    times.append(time.perf_counter() - t0)
    seen += k in o.list("probe")
out["listed_straight_after_put"] = f"{seen} of 20"
out["conditional_put_ms_median"] = round(1000 * statistics.median(times), 1)
t0 = time.perf_counter()
o.list("probe")
out["list_ms"] = round(1000 * (time.perf_counter() - t0), 1)
for k in made:
    o.delete(k)
ok = out["second_put_refused"] and out["race_one_winner_and_its_body"] and seen == 20
out["commit_protocol_safe_here"] = bool(ok)
print(json.dumps(out, indent=1))
sys.exit(0 if ok else 1)
