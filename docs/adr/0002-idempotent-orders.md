# ADR 0002 — Exactly-once order effect: journal first, deterministic client order id

**Status:** accepted

## Context
The trader can die at any instruction (kill -9, OOM, host reboot). The dangerous window is
between "the exchange filled my order" and "I wrote that down". A naive restart re-decides
the same tick and sends a second order.

## Decision
1. Client order id = `pq<tick id>`. It is derived from the market event that caused the
   decision, so a re-decision produces the *same* id.
2. Write the order as `PENDING` (with the stream cursor) to SQLite **before** sending.
3. On restart, look up each `PENDING` id on the exchange. Mark it filled if found; otherwise
   resend it with the same id. Binance rejects a duplicate `newClientOrderId`, and the mock
   exchange does the same.
4. Compare book position with exchange position (minus the offset stored at first start)
   and export the difference as `pq_reconcile_mismatch_lots`.

## Consequences
- Measured with `bench/chaos.py` (two runs of 30 kills mid-trading): 0 lost, 0 duplicate,
  and an order sequence identical to an uninterrupted run. The naive control (random id,
  record after ack, no reconcile) sent 18 and 22 duplicates. In the second run it ended at
  +5 lots against a 1-lot limit, because the risk check reads the drifted book.
- Costs one extra SQLite commit per order (WAL, `synchronous=NORMAL`: survives a process
  crash, not power loss).
