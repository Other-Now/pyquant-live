# ADR 0004 — Where Python performance work actually paid off

**Status:** accepted

## Decision (each step measured, results/)
| Change | Effect | Kept? |
|---|---|---|
| O(W) rolling stats → O(1) sliding mean/M2 | 17.7 µs → 0.75–0.84 µs p50 per tick | yes |
| …then numba | → 0.65 µs (dispatch overhead dominates at O(1)) | benchmarks only; trader stays pure Python (no JIT warm-up on restart) |
| shifted coordinates in O(1) variance | max z error vs numpy 5e-3 → 1.5e-7 at the same speed | yes (correctness) |
| `json` → `orjson` for WS messages | 2.3–2.5 µs → 0.75 µs per message | yes |
| redis-py pure-Python RESP3 parser → `hiredis` | 300k-tick replay 10.6 s → 2.25 s (py-spy: 89% of CPU in the parser) | yes |
| asyncio → uvloop | ≤10% p50, within run-to-run noise (Redis hop dominates) | optional flag |
| blocking `XREAD` → busy-poll at 100 ticks/s | p50 619 → 387 µs (median of 3), costs a core; no gain at 2,000/s | flag, off by default |

## Consequences
The profiler, not intuition, picked the big win (hiredis). The O(1) rewrite brought a
numerical bug that only a property test against numpy caught.
