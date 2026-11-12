"""Trader process: Redis stream of ticks -> strategy -> risk -> OMS -> exchange.

Restart path (any crash, incl. kill -9):
  1. read the committed stream cursor from SQLite;
  2. rebuild strategy state by replaying the stream up to the cursor (no orders);
  3. reconcile PENDING orders with the exchange by client order id;
  4. continue reading after the cursor. Ticks after the cursor are re-decided;
     the decision is deterministic, so the order id is the same and cannot double.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from typing import Any, cast

import redis.asyncio as aioredis
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from .exchange import BinanceTestnet, Exchange, MockClient
from .oms import OMS, Store
from .risk import Limits, Risk
from .strategy import MeanReversion, Params

TICKS = Counter("pq_ticks_total", "ticks processed")
ORDERS = Counter("pq_orders_total", "orders acked", ["dup"])
REJECTS = Counter("pq_risk_rejects_total", "risk rejects", ["reason"])
LAT = Histogram("pq_tick_to_decision_seconds", "publish -> decision latency",
                buckets=(5e-5, 1e-4, 2e-4, 5e-4, 1e-3, 2e-3, 5e-3, 1e-2, 5e-2))
POS = Gauge("pq_position_lots", "position (lots)")
MISMATCH = Gauge("pq_reconcile_mismatch_lots", "exchange minus book position after reconcile")


Entry = tuple[bytes, dict[bytes, bytes]]


async def xrange(r: aioredis.Redis, stream: str, lo: str, hi: str, count: int) -> list[Entry]:
    return cast(list[Entry], await r.xrange(stream, min=lo, max=hi, count=count))


async def xread(r: aioredis.Redis, stream: str, cursor: str, block: int | None) -> list[Entry]:
    res: Any = await r.xread({stream: cursor}, count=500, block=block)
    return cast(list[Entry], res[0][1]) if res else []


async def run(a: argparse.Namespace) -> dict[str, float]:
    # redis-py 8 defaults socket_timeout below a long XREAD BLOCK; a quiet market
    # then raises TimeoutError and kills the trader. Keep the timeout > block.
    r = aioredis.Redis.from_url(a.redis, socket_timeout=a.block_ms / 1000 + 5)
    store = Store(a.db)
    exch: Exchange = BinanceTestnet(a.symbol, a.lot) if a.exchange == "binance" else MockClient(a.exchange)
    oms = OMS(store, exch, a.mode)
    p = Params(window=a.window, entry=a.entry, exit=a.exit, lot=a.lot)
    strat = MeanReversion(p)
    risk = Risk(Limits(max_orders_per_s=a.max_rate))
    stream = f"ticks:{a.symbol}"
    cursor = store.get("cursor", "0-0") or "0-0"

    # 2. rebuild strategy state from the stream (deterministic -> same targets)
    rebuilt = 0
    if cursor != "0-0":
        start = "-"
        while True:
            rows = await xrange(r, stream, start, cursor, 10_000)
            for _id, f in rows:
                strat.on_price(float(f[b"p"]))
            rebuilt += len(rows)
            if len(rows) < 10_000:
                break
            start = "(" + rows[-1][0].decode()
    # 3. reconcile
    rec = await oms.reconcile() if a.mode == "safe" else {"confirmed": 0, "resent": 0, "mismatch": 0}
    MISMATCH.set(rec["mismatch"])
    print(f"start cursor={cursor} rebuilt={rebuilt} reconcile={rec} pos={oms.position}", file=sys.stderr, flush=True)

    lats: list[int] = []
    idle_ns = 0
    recv: list[int] = []  # publish -> XREAD returned (Redis + wakeup only)
    commits: list[int] = []  # SQLite cursor commit per batch
    n = 0
    while True:
        if a.spin:  # busy-poll: no BLOCK, never sleep in epoll (costs a core)
            res = await xread(r, stream, cursor, None)
            if not res:
                idle_ns = idle_ns or time.monotonic_ns()
                if a.exit_when_idle and time.monotonic_ns() - idle_ns > a.block_ms * 1_000_000:
                    break
                continue
            idle_ns = 0
        else:
            res = await xread(r, stream, cursor, a.block_ms)
        t_recv = time.time_ns()
        if not res:
            if a.exit_when_idle:
                break
            continue
        for sid, f in res:
            sid_s = sid.decode()
            px = float(f[b"p"])
            target = strat.on_price(px)
            if b"r" in f:
                d = time.time_ns() - int(f[b"r"])
                lats.append(d)
                recv.append(t_recv - int(f[b"r"]))
                LAT.observe(d / 1e9)
            if target != oms.position:
                ref_b = await r.get(f"px:{a.symbol}") if a.band_check else None
                why = risk.check(target, oms.position, px, float(ref_b) if ref_b else None, int(f[b"t"]) / 1e3)
                if why:
                    REJECTS.labels(why).inc()
                else:
                    ack = await oms.submit(int(f[b"id"]), target, px, sid_s)
                    if ack is not None:
                        ORDERS.labels(str(ack.duplicate)).inc()
                        POS.set(oms.position)
                        await r.hset(f"pq:state:{a.symbol}", mapping={"pos": oms.position, "target": target,
                                                                     "px": px, "tick": sid_s})
            cursor = sid_s
            n += 1
        TICKS.inc(len(res))
        t_c = time.perf_counter_ns()
        store.set("cursor", cursor)  # batched cursor commit; replays after it are idempotent
        commits.append(time.perf_counter_ns() - t_c)
    out: dict[str, float] = {"ticks": n, "orders": oms.sent, "pos": oms.position}
    await exch.close()
    await r.aclose()
    store.close()
    if lats:
        lats.sort()
        recv.sort()
        commits.sort()
        out |= {"lat_p50_us": lats[len(lats) // 2] / 1e3, "lat_p99_us": lats[int(len(lats) * 0.99)] / 1e3,
                "recv_p50_us": recv[len(recv) // 2] / 1e3, "recv_p99_us": recv[int(len(recv) * 0.99)] / 1e3,
                "commit_p50_us": commits[len(commits) // 2] / 1e3, "lat_n": len(lats)}
    return out


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="pyquant trader")
    ap.add_argument("--redis", default=os.environ.get("PQ_REDIS", "redis://127.0.0.1:6390"))
    ap.add_argument("--db", default="trader.db")
    ap.add_argument("--exchange", default="http://127.0.0.1:8090", help="mock URL or 'binance'")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--mode", choices=["safe", "naive"], default="safe")
    ap.add_argument("--window", type=int, default=300)
    ap.add_argument("--entry", type=float, default=2.5)
    ap.add_argument("--exit", type=float, default=0.5)
    ap.add_argument("--lot", type=float, default=0.001)
    ap.add_argument("--max-rate", type=float, default=5.0, help="orders/s in exchange time")
    ap.add_argument("--band-check", action="store_true")
    ap.add_argument("--block-ms", type=int, default=1000)
    ap.add_argument("--exit-when-idle", action="store_true")
    ap.add_argument("--spin", action="store_true", help="busy-poll XREAD instead of BLOCK")
    ap.add_argument("--uvloop", action="store_true")
    ap.add_argument("--metrics-port", type=int, default=0)
    return ap


def main() -> None:
    a = parser().parse_args()
    if a.metrics_port:
        start_http_server(a.metrics_port)
    if a.uvloop:
        import uvloop

        out = uvloop.run(run(a))
    else:
        out = asyncio.run(run(a))
    print(out, flush=True)


if __name__ == "__main__":
    main()
