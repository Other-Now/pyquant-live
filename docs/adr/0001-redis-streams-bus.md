# ADR 0001 — Redis Streams as the message bus

**Status:** accepted

## Context
Market data, the trader and (later) more strategies run as separate processes. The bus must
(a) survive a consumer crash without losing ticks, (b) let a restarted consumer re-read
history to rebuild state, and (c) be cheap to run on one box.

## Decision
Use one Redis Stream per symbol (`ticks:<SYM>`). The market-data process `XADD`s; the trader
`XREAD`s from a cursor that it stores **in its own SQLite journal**, not in a Redis consumer
group. The same Redis holds the latest price (`px:<SYM>`, used by the risk price band) and a
position snapshot (`pq:state:<SYM>`) for dashboards.

## Consequences
- The cursor commits in the same SQLite transaction as order state. Bus position and trading
  state cannot disagree after a crash. A consumer group's `XACK` would be a second,
  non-atomic commit.
- The stream is the replay log, so restart = `XRANGE` up to the cursor.
- Kafka would give the same semantics plus partitioning and retention. It is heavier than
  needed for one symbol on one host. Swapping it in only touches `md.py` and the two read
  helpers in `trader.py`.
- Measured: Redis is most of the tick latency (p50 ~0.32–0.37 ms publish→decision at
  2,000 ticks/s, ~97% of it before XREAD returns; the strategy itself is ~1 µs). See ADR 0004.
