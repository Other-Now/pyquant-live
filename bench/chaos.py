"""kill -9 chaos test for the trader (Linux/WSL; needs scripts/stack.sh up).

1. Reference: one uninterrupted trader run over the replayed stream -> order set R.
2. Chaos: same stream, fresh book; start the trader, SIGKILL it at a random moment,
   restart, repeat K times, then let it finish -> order set C (from the exchange's log).
3. Compare by the tick that caused each order:
     lost = ticks with an order in R but none in C
     dup  = extra exchange orders in C (same tick ordered twice, or ticks R never ordered on)
   plus the final exchange position vs the reference.

Run for both --mode safe and --mode naive (the control).
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import collections
import json
import os
import random
import signal
import subprocess
import sys
import time
from pathlib import Path

from pyquant.exchange import Ack, MockClient

PY = sys.executable
STRAT = ["--window", "50", "--entry", "1.5", "--exit", "0.3", "--max-rate", "1e9"]


def trader(mode: str, db: str, finish: bool) -> subprocess.Popen[bytes]:
    cmd = [PY, "-m", "pyquant.trader", "--db", db, "--mode", mode, "--block-ms", "300", *STRAT]
    if finish:
        cmd.append("--exit-when-idle")
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def fresh(db: str) -> None:
    for suf in ("", "-wal", "-shm"):
        Path(db + suf).unlink(missing_ok=True)


async def exch_state(reset_latency: float | None = None) -> tuple[list[Ack], int]:
    c = MockClient()
    try:
        if reset_latency is not None:
            await c.reset(reset_latency)
            return [], 0
        return await c.orders(), await c.position()
    finally:
        await c.close()


def run_mode(mode: str, kills: int, lat_ms: float, seed: int, lo: float, hi: float) -> dict[str, object]:
    db = f"/tmp/pq-chaos-{mode}.db"
    # reference
    fresh(db)
    asyncio.run(exch_state(lat_ms))
    t0 = time.time()
    p = trader(mode, db, finish=True)
    p.wait()
    ref_s = time.time() - t0
    ref_orders, ref_pos = asyncio.run(exch_state())
    ref = {o.tag: (o.side, o.lots) for o in ref_orders}

    # chaos
    fresh(db)
    asyncio.run(exch_state(lat_ms))
    rng = random.Random(seed)
    starts: list[str] = []
    for _ in range(kills):
        p = trader(mode, db, finish=False)
        assert p.stderr is not None
        starts.append(p.stderr.readline().decode())  # blocks until rebuild+reconcile done
        time.sleep(rng.uniform(lo, hi))  # so every kill lands while trading
        p.send_signal(signal.SIGKILL)
        starts += p.communicate()[1].decode().splitlines()
    p = trader(mode, db, finish=True)
    _, err = p.communicate()
    starts += err.decode().splitlines()
    rec = collections.Counter[str]()
    for line in starts:
        if "reconcile=" in line:
            d = ast.literal_eval(line.split("reconcile=")[1].split(" pos=")[0])
            rec.update({k: abs(v) for k, v in d.items()})
    got, pos = asyncio.run(exch_state())

    by_tag = collections.Counter(o.tag for o in got)
    lost = sum(1 for t in ref if t not in by_tag)
    dup = sum(c - 1 for c in by_tag.values()) + sum(1 for t in by_tag if t not in ref)
    mismatched = sum(1 for o in got if o.tag in ref and ref[o.tag] != (o.side, o.lots))
    return {"mode": mode, "kills": kills, "ref_orders": len(ref), "ref_seconds": round(ref_s, 1),
            "chaos_orders": len(got), "lost": lost, "dup": dup, "changed": mismatched,
            "ref_pos": ref_pos, "chaos_pos": pos, "identical": [(o.tag, o.side, o.lots) for o in got]
            == [(o.tag, o.side, o.lots) for o in ref_orders],
            "reconcile_totals": dict(rec), "starts": len(starts)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--kills", type=int, default=20)
    ap.add_argument("--latency-ms", type=float, default=5.0)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--lo", type=float, default=0.1)
    ap.add_argument("--hi", type=float, default=1.5)
    ap.add_argument("--modes", default="safe,naive")
    ap.add_argument("--out", default="results/chaos.json")
    a = ap.parse_args()
    res = [run_mode(m, a.kills, a.latency_ms, a.seed, a.lo, a.hi) for m in a.modes.split(",")]
    for r in res:
        print(json.dumps(r))
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    Path(a.out).write_text(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
