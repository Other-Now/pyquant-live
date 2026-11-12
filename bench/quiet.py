"""Quiet-market latency, repeated: blocking XREAD vs busy-poll at low tick rates (3 reps each)."""

from __future__ import annotations

import json
import statistics
import sys

from bench.e2e import once

CSV = sys.argv[1] if len(sys.argv) > 1 else "data/BTCUSDT-aggTrades-2026-10-15.csv"
rows = []
for rep in range(3):
    for rate in (100, 500, 2000):
        for spin in (False, True):
            d = once("asyncio", int(rate * 12), rate, CSV, spin)
            rows.append({"rep": rep, "rate": rate, "spin": spin, "p50_us": d["lat_p50_us"], "p99_us": d["lat_p99_us"]})
            print(rows[-1], flush=True)
summary = {}
for rate in (100, 500, 2000):
    for spin in (False, True):
        v = [r["p50_us"] for r in rows if r["rate"] == rate and r["spin"] == spin]
        summary[f"{rate}/s {'spin' if spin else 'block'}"] = {"p50_us_median": statistics.median(v),
                                                              "p50_us_runs": v}
print(json.dumps(summary, indent=1))
with open("results/e2e_quiet.json", "w") as fh:
    json.dump({"runs": rows, "summary": summary}, fh, indent=2)
