from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from pyquant.risk import Limits, Risk


@given(st.integers(-5, 5), st.integers(-3, 3))
def test_never_exceeds_position_limits(target: int, pos: int) -> None:
    r = Risk(Limits(max_pos_lots=1, max_order_lots=2, max_orders_per_s=1e9))
    ok = r.check(target, pos, 100.0, None, now=0.0) is None
    if ok:
        assert abs(target) <= 1 and abs(target - pos) <= 2


def test_price_band() -> None:
    r = Risk(Limits(band_pct=1.0))
    assert r.check(1, 0, 102.0, 100.0, 0.0) == "price_band"
    assert r.check(1, 0, 100.5, 100.0, 0.0) is None


def test_rate_limit_runs_on_event_time() -> None:
    r = Risk(Limits(max_orders_per_s=2))
    t = 1000.0
    assert [r.check(1, 0, 1.0, None, t) for _ in range(3)] == [None, None, "rate"]
    assert r.check(1, 0, 1.0, None, t + 0.5) is None  # refilled one token in 0.5 s of event time


@given(st.lists(st.floats(0, 0.3), min_size=1, max_size=200))
def test_rate_limit_is_deterministic(gaps: list[float]) -> None:
    def run() -> list[str | None]:
        r, t, out = Risk(Limits(max_orders_per_s=5)), 0.0, []
        for g in gaps:
            t += g
            out.append(r.check(1, 0, 1.0, None, t))
        return out

    assert run() == run()
