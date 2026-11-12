"""Market-data process: Binance public trade stream (live) or a CSV day (replay) -> Redis.

Every tick goes to the stream ``ticks:<SYM>`` with fields
    p=price  q=qty  id=trade id  t=exchange time (ms)  r=publish time (ns, for latency)
and the latest price is cached under ``px:<SYM>`` for the risk price band.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import time
from collections.abc import Callable
from typing import Any

import orjson
import redis.asyncio as aioredis

WS = "wss://data-stream.binance.vision/ws/{sym}@trade"


async def live(a: argparse.Namespace) -> None:
    import websockets

    r = aioredis.Redis.from_url(a.redis)
    loads: Callable[[bytes | str], Any] = json.loads if a.json == "std" else orjson.loads
    stream, key = f"ticks:{a.symbol}", f"px:{a.symbol}"
    n, backoff = 0, 1.0
    t_end = time.monotonic() + a.seconds if a.seconds else float("inf")
    while time.monotonic() < t_end:
        try:
            async with websockets.connect(WS.format(sym=a.symbol.lower()), ping_interval=20) as ws:
                backoff = 1.0
                async for raw in ws:
                    m = loads(raw)
                    pipe = r.pipeline(transaction=False)
                    pipe.xadd(stream, {"p": m["p"], "q": m["q"], "id": m["t"], "t": m["T"], "r": time.time_ns()},
                              maxlen=2_000_000, approximate=True)
                    pipe.set(key, m["p"])
                    await pipe.execute()
                    n += 1
                    if time.monotonic() >= t_end:
                        break
        except (OSError, Exception) as e:  # reconnect with jittered backoff
            if time.monotonic() >= t_end:
                break
            print(f"ws error {e!r}; reconnect in {backoff:.1f}s", flush=True)
            await asyncio.sleep(backoff * (0.5 + random.random()))
            backoff = min(backoff * 2, 30.0)
    print({"published": n}, flush=True)
    await r.aclose()


async def replay(a: argparse.Namespace) -> None:
    import polars as pl

    from .research import load_polars

    df = load_polars(a.csv)
    if a.limit:
        df = df.head(a.limit)
    r = aioredis.Redis.from_url(a.redis)
    stream = f"ticks:{a.symbol}"
    if a.fresh:
        await r.delete(stream, f"px:{a.symbol}")
    # t = exchange time in ms, same unit as the live stream's "T"
    rows = df.select(pl.col("price").cast(pl.Utf8), pl.col("qty").cast(pl.Utf8), "agg_id",
                     pl.col("ts_us") // 1000).rows()
    if a.rate <= 0:  # bulk load, pipelined
        for i in range(0, len(rows), 5000):
            pipe = r.pipeline(transaction=False)
            for p, q, tid, ts in rows[i:i + 5000]:
                pipe.xadd(stream, {"p": p, "q": q, "id": tid, "t": ts})
            await pipe.execute()
    else:  # paced, one XADD per tick, stamped for latency measurement
        gap = 1.0 / a.rate
        t0 = time.perf_counter()
        for i, (p, q, tid, ts) in enumerate(rows):
            wait = t0 + i * gap - time.perf_counter()
            if wait > 0:
                await asyncio.sleep(wait)
            await r.xadd(stream, {"p": p, "q": q, "id": tid, "t": ts, "r": time.time_ns()})
    print({"published": len(rows)}, flush=True)
    await r.aclose()


def main() -> None:
    ap = argparse.ArgumentParser(description="pyquant market data")
    ap.add_argument("cmd", choices=["live", "replay"])
    ap.add_argument("--redis", default=os.environ.get("PQ_REDIS", "redis://127.0.0.1:6390"))
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--json", choices=["std", "orjson"], default="orjson")
    ap.add_argument("--seconds", type=float, default=0)
    ap.add_argument("--csv", default="data/BTCUSDT-aggTrades-2026-10-15.csv")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--rate", type=float, default=0, help="ticks/s; 0 = bulk load")
    ap.add_argument("--fresh", action="store_true")
    ap.add_argument("--uvloop", action="store_true")
    a = ap.parse_args()
    coro = live(a) if a.cmd == "live" else replay(a)
    if a.uvloop:
        import uvloop

        uvloop.run(coro)
    else:
        asyncio.run(coro)


if __name__ == "__main__":
    main()
