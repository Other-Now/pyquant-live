# ADR 0003 — One strategy, deterministic everywhere (backtest = replay = live)

**Status:** accepted

## Context
Research runs vectorized (Polars/pandas). Live runs tick by tick. Restarts rebuild state by
replay. If any of these disagree, the backtest says nothing about production.

## Decision
- The live `MeanReversion` class is a pure function of the tick sequence. The vectorized
  versions express the same hysteresis as "emit on entry/exit, else null, then forward-fill".
- Parity is a test, not a hope. Hypothesis random walks, plus all 1,106,978 real ticks in
  `bench/dataproc.py`: 0 mismatches.
- Anything time-based runs on **exchange time** from the tick, not the wall clock. The
  risk rate limiter originally used `time.monotonic()`, and replaying the same 300k ticks
  gave 581 orders on slow code and 112 after hiredis. That was the bug that forced this rule.
- On restart, rebuild strategy state by replaying the stream rather than snapshotting it.

## Consequences
- Rebuild time grows with the stream (0.8 s for 200k ticks with hiredis). A periodic strategy
  snapshot would bound it. Not done yet.
- The rate limiter's bucket is not rebuilt on restart (it starts full). The chaos test runs
  with the limiter disabled so that the reference and chaos runs stay comparable.
