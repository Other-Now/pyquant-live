# pyquant-live

A small live systematic-trading system in Python. Real Binance market data arrives over
asyncio WebSockets into a Redis Stream. A trader process runs a mean-reversion strategy,
pre-trade risk checks, and an OMS that journals to SQLite and survives `kill -9`. The same
strategy code runs in a vectorized Polars/pandas backtest, and parity is checked on every
tick.

The project exists to answer four questions with measurements: is it crash-safe? does the
backtest match live? where does Python time go? and what does the GIL actually cost?

```
Binance WS ─► md.py ──XADD──► Redis Stream ticks:BTCUSDT ──XREAD(cursor)──► trader.py
 (orjson)      └─SET px cache ─► Redis px:BTCUSDT ──────────── price band ─┤
                                                                            strategy  O(1) z-score
                                                                            risk      event-time limits
                                                                            OMS ─► SQLite WAL (orders, cursor, pos)
                                                                             └─HTTP─► exchange: mock | Binance testnet
                                                 Prometheus /metrics ◄── trader
```

| Module | What |
|---|---|
| `pyquant/md.py` | Binance public trade stream (reconnect with jittered backoff) or CSV replay → Redis |
| `pyquant/trader.py` | rebuild → reconcile → consume loop; Prometheus metrics |
| `pyquant/strategy.py` | `MeanReversion`, the one strategy class used live, in replay and in the event backtest |
| `pyquant/stats.py` | rolling z-score four ways (O(W) deque, numpy, O(1) shifted, numba) |
| `pyquant/research.py` | the same strategy vectorized in Polars and pandas + PnL |
| `pyquant/risk.py` | max position / order size / price band / rate limit (on exchange time) |
| `pyquant/oms.py` | SQLite journal, deterministic client order ids, restart reconcile; `naive` mode as control |
| `pyquant/exchange.py` | mock exchange (aiohttp), its client, Binance spot-testnet signed REST client |
| `docs/adr/` | 4 decision records: bus, idempotent orders, deterministic replay, perf |

## Results

All numbers come from `results/`. They were measured on a 4-vCPU WSL2 VM on a laptop,
using real Binance BTCUSDT data: one day of aggTrades (2026-10-15, **1,106,978 ticks**)
plus a 10-minute live capture.

### 1. Crash safety: `kill -9` chaos (`bench/chaos.py`, `results/chaos.json`)
The test replays 200k real ticks through the mock exchange at 5 ms latency. The trader is
SIGKILLed 30 times at random moments *while trading*, then restarted each time. The run is
compared with an uninterrupted reference (2,971 orders).

| OMS mode | lost | duplicate | order sequence vs reference | final position (ref −1) |
|---|---|---|---|---|
| **safe**: journal-before-send, `pq<tick id>` order ids, reconcile | **0** | **0** | identical | −1 |
| naive control: random ids, record-after-ack | 0 | **22** | differs | **+5** (limit is 1) |

Reconcile on restart confirmed 25 orders that had filled but whose ack was never
journaled. An earlier 30-kill run gave safe 0/0 (16 confirmed, 2 resent) and naive 18
duplicates. The naive run breaks its own 1-lot limit because the risk checks read a book
that has drifted from the exchange.

### 2. Backtest = live (`bench/dataproc.py`, `bench/live_parity.py`)
- **0 mismatched ticks out of 1,106,978**: the Polars and pandas vectorized targets equal
  the live class's tick-by-tick targets. Hypothesis also checks this on random walks.
- **Live paper run** (`results/live_paper.txt`): 10 min of the real Binance stream, 5,609
  ticks, 15 orders to the mock exchange, reconcile mismatch 0, publish→decision p50 373 µs.
  A backtest of the recorded ticks produces **the same 15 orders on the same trade ids**.

### 3. Python performance (`bench/hotpath.py`, `scripts/profile.sh`)
Per-tick hot path on 200k real ticks (p50):

| Step | p50 / tick | Note |
|---|---|---|
| deque + `sum()` every tick, O(W) | 17.7 µs | |
| numpy ring buffer | 13.5 µs | per-call overhead dominates at W=300 |
| **O(1) sliding mean/M2, pure Python** | **0.75–0.84 µs** | ~21×: the algorithm, not the tool |
| same in numba | 0.65 µs | dispatch overhead; not worth a JIT warm-up on every restart |
| `json.loads` → `orjson.loads` per message | 2.3 → 0.75 µs | |

**Correctness bug found by benchmarking.** The O(1) variance differed from numpy by up to
**5e-3 in z** on near-flat windows: std 0.0006 USDT at a price of 81,682, which is
catastrophic cancellation. Storing `x − K` and re-centring K every window brought it to
**1.5e-7** at the same speed. A regression test pins it.

**Profile-driven:** py-spy on the trader (300k ticks, 581 orders) put **89% of samples in
redis-py's pure-Python RESP3 parser**. Installing `hiredis` took the replay from 10.6 s to
**2.25 s** (13.2 s at the original baseline, so 5.9× overall).

End-to-end publish→Redis→decision (`bench/e2e.py`, `bench/quiet.py`, median of 3):

| ticks/s | blocking XREAD p50 | busy-poll p50 |
|---|---|---|
| 100 | 619 µs | 387 µs |
| 500 | 578 µs | 374 µs |
| 2,000 | 337 µs | 400 µs |

Quiet markets are *slower* (idle wake-up cost), and spinning buys it back at the cost of a
core. uvloop vs asyncio: ≤10%, within run-to-run noise. Trader work plus the SQLite cursor
commit is ~25 µs of the total; the rest is the Redis hop. p99 on WSL is noisy (1–15 ms, with one 96 ms
outlier), so p50 is the quoted figure.

### 4. Concurrency and the GIL (`bench/concurrency.py`)
The same strategy runs over 8 slices of real ticks with 4 workers. CPython 3.13 is compared
with the free-threaded 3.13t build.

| | 3.13 (GIL) | 3.13t (no GIL) |
|---|---|---|
| sequential | 0.85 s | 1.71 s (single thread ~2× slower) |
| `asyncio.gather` | 0.84 s | 1.78 s |
| 4 threads | 0.81 s | 0.75 s |
| 4 processes | **0.32 s** | 0.73 s |
| numpy job (releases GIL): threads vs sequential | 0.73 vs 0.98 s | 0.81 vs 1.06 s |

asyncio gives no parallelism for CPU work. Threads only scale under 3.13t, and even there
they lose to processes on the GIL build because of 3.13t's single-thread penalty.

### 5. Data processing (`bench/dataproc.py`, best of 3)
One day (1.1M rows) through load → rolling z → targets → PnL: **Polars 0.33–0.41 s vs pandas
0.32–0.37 s**, a wash at this size. Polars loads faster; pandas' rolling+ffill is faster.
The tick-by-tick class runs at 0.8–1.0M ticks/s, about 3–4× slower than vectorized. That
is why research is vectorized and parity is tested.

The strategy itself loses money after fees on this day (net −38 USDT per 0.001-BTC lot,
2,190 trades). This is a systems project, not alpha.

## Quality
- `pytest`: 21 tests, including Hypothesis property tests (O(1) stats vs numpy,
  vectorized == event-driven, risk limits never exceeded, deterministic rate limiter) and OMS
  crash windows against the real mock-exchange HTTP app. One integration test runs the
  trader over real Redis with a restart in the middle.
- `mypy --strict`, `ruff`, coverage gate 75% (currently 80%).
- CI runs all of the above, a 10-kill chaos run that asserts 0 lost / 0 duplicate, and the
  benchmarks.

## Run it (Linux/WSL)
```bash
pip install -e ".[dev]"            # Python 3.12+; 3.13 used for results
bash scripts/get_data.sh           # 16 MB zip from data.binance.vision
bash scripts/stack.sh up           # redis :6390 + mock exchange :8090
python -m pyquant.md replay --limit 200000 --fresh
python -m pyquant.trader --exit-when-idle --metrics-port 9108
python -m bench.chaos --kills 30   # safe vs naive
bash scripts/live_paper.sh 600     # real Binance stream -> paper trading
bash scripts/bench_all.sh          # regenerate results/
```

## Not done / limits
- **Orders never reached a real venue.** `BinanceTestnet` is written against the documented
  spot API (IOC limit, `newClientOrderId`, lookup by `origClientOrderId`). It was not run,
  because that needs `PQ_API_KEY`/`PQ_API_SECRET`. Live *market data* is real.
- Restart rebuilds strategy state by replaying the stream: 0.8 s for 200k ticks, plus
  0.3 s of imports. A periodic snapshot would bound that. The rate-limiter bucket is not rebuilt.
- SQLite `synchronous=NORMAL` survives a process crash, not power loss.
- One symbol, one strategy, and no Grafana dashboard (Prometheus metrics only).
