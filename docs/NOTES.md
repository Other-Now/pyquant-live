# Study sheet: pyquant-live (read this the night before)

## 30-second pitch
A small live trading system in Python. Binance trades come in over asyncio WebSocket into a
Redis Stream. A trader process consumes them and runs a mean-reversion strategy, pre-trade
risk checks and an OMS that journals to SQLite. Orders go to an exchange: a mock, or the
Binance spot testnet client. Three properties are proven with numbers:
1. **Crash safety.** kill -9 thirty times mid-trading: 0 lost, 0 duplicate orders, and the
   same order sequence as an uninterrupted run. The naive control sends 22 duplicates and
   ends at +5 lots against a 1-lot limit.
2. **Backtest = live.** Vectorized Polars/pandas and the live class agree on every one of
   1.1M real ticks.
3. **Measured Python performance work.** py-spy → hiredis gave 5.9× trader throughput,
   O(1) stats gave ~21× per tick, and the free-threaded/GIL comparison is measured.
4. **Live run.** 10 min of the real Binance stream (5,609 ticks): 15 orders, and they are
   identical to a backtest of the recorded ticks.

## Architecture (draw this)
```
Binance WS ──► md.py ──XADD──► Redis Stream ticks:BTCUSDT ──XREAD(cursor)──► trader.py
  (orjson)     px cache ─► Redis px:BTCUSDT ─────────────────── band check ─┤
                                                                            strategy (O(1) z-score)
                                                                            risk (event-time)
                                                                            OMS ──► SQLite (WAL) journal
                                                                             │        orders + cursor + pos
                                                                             └──HTTP──► exchange (mock | testnet)
                                                         Prometheus /metrics ◄── trader
```
Why separate processes: md keeps publishing while the trader restarts, and the stream is
the replay log.

## The crash-safety argument (be able to say it in 4 lines)
- coid = `pq<tick id>`: the same decision always gets the same id.
- Journal PENDING + cursor in one transaction, *then* send.
- Restart: rebuild the strategy by replaying the stream up to the cursor, then resolve each
  PENDING order by coid (found → confirm; missing → resend with the same id). The exchange
  rejects a duplicate coid.
- Ticks after the cursor are re-decided. The decision is deterministic, so either the order
  already exists (skip) or it never left (send).
- Across two 30-kill runs, reconcile **confirmed 16 and 25** orders (killed after the
  exchange filled them, before the ack was journaled) and **resent 2 and 0** (killed after
  journaling, before the request left).
- Naive control: 18 and 22 duplicates. In the final run it ended at **+5 lots with a 1-lot
  limit**. Risk limits check the *book*, so once the book drifts from the exchange the
  limits stop protecting you. That is why reconcile compares positions.

Follow-ups they may ask:
- *Why not a consumer group + XACK?* Then there are two commits (SQLite and Redis) that
  can't be atomic. One SQLite transaction holds both the cursor and the order state.
- *Exactly-once?* Not exactly-once delivery. It is an exactly-once **effect** through an
  idempotent key. The exchange's dedup on client order id is the other half.
- *Partial fills (IOC on testnet)?* The trader trades toward the target whenever
  `position != target`. A partial fill just leaves a remainder, which the next tick re-orders.
- *Power loss?* WAL + `synchronous=NORMAL` survives a process crash but not an OS crash.
  Set `synchronous=FULL` (an fsync per commit) for that.
- *Weak spots:* rebuild-by-replay grows with the stream (0.8 s per 200k ticks; snapshot would fix it); the rate
  limiter bucket resets on restart; testnet path not run (no API key).

## Python performance: what you measured, in order
1. **Per-tick hot path** (200k real ticks, p50):
   naive deque O(W) 17.7 µs → numpy ring buffer 13.5 µs → O(1) pure Python 0.75–0.84 µs →
   numba 0.65 µs.
   - Numpy barely helps at W=300 because per-call overhead (~µs) dominates.
   - Numba barely beats O(1) Python because of dispatch overhead.
   - The algorithm change was the ~21× win.
2. **Correctness bug from the optimisation.** The O(1) sliding variance drifted from numpy
   by up to 5e-3 in z on *near-flat windows* (std 0.0006 USDT at a price of 81,682).
   Catastrophic cancellation. Fix: store x − K, re-centre K to the window mean at each exact
   recompute → 1.5e-7. A Hypothesis test pins it.
3. **orjson vs json:** 2.5 → 0.75 µs per Binance message.
4. **py-spy on the trader** (300k ticks): 89% of samples in redis-py's pure-Python RESP3
   parser. `pip install hiredis` took the replay from 10.6 s to 2.25 s. The earlier SQLite
   position-read fix gave 13.2 → 10.6 s.
5. **uvloop:** ≤10% on p50 (asyncio 347–352 µs vs uvloop 313–318 µs in the final run; no
   difference in an earlier run), so it's within noise. The latency is the Redis hop (recv
   ≈ 97% of publish→decision; trader work + SQLite cursor commit ~25 µs).
6. **Quiet-market latency** (median of 3 runs, `results/e2e_quiet.json`): at 100 ticks/s
   p50 is 619 µs, *worse* than at 2,000/s (337 µs), because processes go idle and pay a
   wake-up. A busy-poll XREAD gives 387 µs at 100/s and 374 µs at 500/s, at the cost of a
   core. It doesn't help at 2,000/s. This is the same reason HFT systems spin. A single
   earlier run showed 1.4 ms, so WSL noise is large: quote medians.

## Concurrency (GIL), measured on 4 vCPU WSL
Pure-Python strategy over 8 chunks:
| | 3.13 (GIL) | 3.13t (free-threaded) |
|---|---|---|
| sequential | 0.85 s | 1.71 s (single-thread ~2× slower in 3.13t) |
| asyncio.gather | 0.84 s (no parallelism: coroutines share one thread) | 1.78 s |
| 4 threads | 0.81 s (GIL serialises) | 0.75 s (2.3× over its own sequential) |
| 4 processes | **0.32 s** (2.7×) | 0.73 s |

Numpy job (releases the GIL): threads 0.73 s vs sequential 0.98 s even with the GIL. That
is only 1.3×, because the job is memory-bandwidth bound.

Rules of thumb:
- I/O-bound work → asyncio.
- CPU-bound pure Python → processes, until free-threading's single-thread penalty shrinks.
- numpy/C extensions that release the GIL → threads are fine.
- asyncio.to_thread is just a thread pool.

Story about measurement: the first numpy-threads run was 3× *slower*. Each thread built a
~290 MB sliding-window temporary on a 2.9 GB VM, so it was memory thrash, not the GIL.
Processing in blocks fixed it.

## Data processing (1,106,978 real ticks, best of 3)
- Polars 0.33–0.41 s vs pandas 0.32–0.37 s end to end (two runs). It's a wash at this size:
  Polars loads faster (0.11–0.17 vs 0.20–0.23 s), pandas' rolling+ffill is faster (0.09–0.12
  vs 0.19–0.22 s).
- The per-tick live class does 0.8–1.0M ticks/s, ~3–4× slower than vectorized. That is why
  research runs vectorized and parity is tested.
- Measurement story: the first run said Polars was 10× slower to load. It was a cold read
  across the WSL↔Windows (9P) boundary, and pandas then hit the warm page cache.

## Things not to over-claim
- Orders went to a **mock** exchange. The Binance testnet client is written against the
  documented API but was never run (no key). Market data in the live run is real.
- The strategy loses money after fees on the test day (net −38 USDT per 0.001-BTC lot). This
  is a systems project, not alpha.
- Latency numbers are WSL2 on a laptop. p99 is noisy (5–9 ms tails), so quote p50.
