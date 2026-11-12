"""Pre-trade risk checks. Pure functions of their inputs (plus a token bucket).

The rate limiter runs on the *tick's exchange timestamp*, not the wall clock. On the
wall clock, a faster trader (e.g. after switching to hiredis) hit the limit more often
during replay, so the same ticks produced 581 vs 112 orders. Event time makes replay
deterministic and is ~ wall time when live.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Limits:
    max_pos_lots: int = 1
    max_order_lots: int = 2
    band_pct: float = 1.0  # order price must be within this % of the cached reference price
    max_orders_per_s: float = 5.0  # in exchange time


class Risk:
    def __init__(self, lim: Limits) -> None:
        self.lim = lim
        self.tokens = lim.max_orders_per_s
        self.last: float | None = None

    def check(self, target: int, pos: int, price: float, ref: float | None, now: float) -> str | None:
        """Return a reject reason, or None if the order toward `target` may go."""
        lim = self.lim
        if abs(target) > lim.max_pos_lots:
            return "max_pos"
        if abs(target - pos) > lim.max_order_lots:
            return "max_order"
        if ref is not None and abs(price - ref) > ref * lim.band_pct / 100:
            return "price_band"
        if self.last is not None:
            self.tokens = min(lim.max_orders_per_s, self.tokens + max(0.0, now - self.last) * lim.max_orders_per_s)
        self.last = now
        if self.tokens < 1.0:
            return "rate"
        self.tokens -= 1.0
        return None
