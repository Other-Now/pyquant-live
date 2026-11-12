"""End-to-end tick latency: md publish (XADD, stamped) -> Redis -> trader decision.

Replays N real ticks at a fixed rate into a fresh stream while a trader consumes it,
for asyncio vs uvloop event loops (both processes switched together).
Needs scripts/stack.sh up.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import subprocess
import sys
import time
from pathlib import Path

PY = sys.executable


def once(loop: str, n: int, rate: float, csv: str, spin: bool = False) -> dict[str, float]:
    db = f"/tmp/pq-e2e-{loop}.db"
    for suf in ("", "-wal", "-shm"):
        Path(db + suf).unlink(missing_ok=True)
    subprocess.run(["redis-cli", "-p", "6390", "del", "ticks:BTCUSDT"], check=True, capture_output=True)
    flag = ["--uvloop"] if loop == "uvloop" else []
    spin_f = ["--spin"] if spin else []
    log = open(db + ".log", "w")  # noqa: SIM115 -- lives as long as the child
    tr = subprocess.Popen([PY, "-m", "pyquant.trader", "--db", db, "--exit-when-idle", "--block-ms", "10000",
                           *flag, *spin_f],
                          stdout=subprocess.PIPE, stderr=log)
    time.sleep(1.5)  # trader imports + blocks on XREAD
    subprocess.run([PY, "-m", "pyquant.md", "replay", "--csv", csv, "--limit", str(n), "--rate", str(rate), *flag],
                   check=True, capture_output=True)
    out, _ = tr.communicate()
    log.close()
    d: dict[str, float] = ast.literal_eval(out.decode().strip().splitlines()[-1])
    d["loop"] = loop  # type: ignore[assignment]
    d["rate"] = rate
    d["spin"] = spin
    return d


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=30_000)
    ap.add_argument("--rate", type=float, default=2000)
    ap.add_argument("--csv", default="data/BTCUSDT-aggTrades-2026-10-15.csv")
    ap.add_argument("--out", default="results/e2e.json")
    a = ap.parse_args()
    res = [once(lp, a.n, a.rate, a.csv) for lp in ("asyncio", "uvloop", "asyncio", "uvloop")]
    # quiet-market effect: latency vs publish rate, blocking XREAD vs busy-poll
    for rate in (100, 500, 2000):
        for spin in (False, True):
            res.append(once("asyncio", min(a.n, int(rate * 15)), rate, a.csv, spin))
    for r in res:
        print(r)
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    Path(a.out).write_text(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
