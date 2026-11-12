#!/usr/bin/env bash
# usage: prof.sh label  -> trader throughput + inclusive profile buckets over 300k ticks
cd /mnt/d/self_projects/pyquant-live
P=~/pq-venv/bin/python
curl -s -XPOST "localhost:8090/reset?latency_ms=0" >/dev/null; rm -f /tmp/ps.db*
t0=$(date +%s.%N)
$P -m pyquant.trader --db /tmp/ps.db --exit-when-idle --block-ms 300 2>/dev/null | tail -1 > /tmp/ps.out
t1=$(date +%s.%N)
curl -s -XPOST "localhost:8090/reset?latency_ms=0" >/dev/null; rm -f /tmp/ps.db*
~/pq-venv/bin/py-spy record -r 500 -f raw -o /tmp/pyspy-$1.txt -- $P -m pyquant.trader --db /tmp/ps.db --exit-when-idle --block-ms 300 >/dev/null 2>&1
$P - "$1" "$t0" "$t1" <<'PY'
import sys, collections
lab, t0, t1 = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
b = collections.Counter(); tot = 0
for line in open(f"/tmp/pyspy-{lab}.txt"):
    stack, n = line.rsplit(" ", 1); n = int(n); tot += n
    if "Store." in stack: b["sqlite_store"] += n
    elif "redis/" in stack: b["redis_client"] += n
    elif "on_price" in stack: b["strategy"] += n
    elif "_run_once" in stack or "select" in stack: b["event_loop"] += n
    else: b["other(import/startup)"] += n
print(lab, "wall_s=%.2f" % (t1 - t0), open("/tmp/ps.out").read().strip())
print("  " + "  ".join(f"{k}={100*v/tot:.1f}%" for k, v in b.most_common()))
PY
