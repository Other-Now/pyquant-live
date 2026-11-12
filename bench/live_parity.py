"""Did the live trader decide exactly what the backtest decides on the same ticks?

Dumps the live run's recorded stream (Redis db 1) to results/live_ticks.csv, replays it
through the event backtest, and compares target changes with the orders the live trader
journaled in its SQLite book.
"""

from __future__ import annotations

import json
import sqlite3
import sys

import numpy as np
import redis

from pyquant.research import targets_event
from pyquant.strategy import Params


def main() -> None:
    url = sys.argv[1] if len(sys.argv) > 1 else "redis://127.0.0.1:6390/1"
    db = sys.argv[2] if len(sys.argv) > 2 else "/tmp/pq-live.db"
    rows = redis.Redis.from_url(url).xrange("ticks:BTCUSDT")
    ids = [int(f[b"id"]) for _, f in rows]
    px = np.array([float(f[b"p"]) for _, f in rows])
    with open("results/live_ticks.csv", "w") as fh:
        fh.write("trade_id,price,qty,exch_ms,publish_ns\n")
        for _, f in rows:
            fh.write(f"{f[b'id'].decode()},{f[b'p'].decode()},{f[b'q'].decode()},{f[b't'].decode()},"
                     f"{f[b'r'].decode()}\n")
    ev = targets_event(px, Params())
    want = [ids[i] for i in range(len(ev)) if ev[i] != (ev[i - 1] if i else 0)]
    got = [r[0] for r in sqlite3.connect(db).execute("SELECT tag FROM orders ORDER BY ts_ns")]
    out = {"ticks": len(ids), "backtest_orders": len(want), "live_orders": len(got), "identical": want == got,
           "first_last_price": [px[0], px[-1]]}
    print(json.dumps(out))
    with open("results/live_parity.json", "w") as fh:
        json.dump(out, fh, indent=2)


if __name__ == "__main__":
    main()
