from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from pyquant import research as R
from pyquant.strategy import Params, next_target

P = Params(window=30, entry=1.5, exit=0.3)
DATA = Path(__file__).resolve().parents[1] / "data" / "BTCUSDT-aggTrades-2026-10-15.csv"


@given(st.integers(-1, 1), st.one_of(st.none(), st.floats(-10, 10)))
def test_next_target_rules(prev: int, z: float | None) -> None:
    t = next_target(prev, z, P)
    assert t in (-1, 0, 1)
    if z is None:
        assert t == prev
    elif z > P.entry:
        assert t == -1
    elif z < -P.entry:
        assert t == 1
    elif abs(z) < P.exit:
        assert t == 0
    else:
        assert t == prev


def frames(prices: np.ndarray) -> tuple[pl.DataFrame, pd.DataFrame]:
    n = len(prices)
    d = {"agg_id": np.arange(n), "price": prices, "qty": np.ones(n), "ts_us": np.arange(n)}
    return pl.DataFrame(d), pd.DataFrame(d)


@settings(max_examples=60, deadline=None)
@given(st.lists(st.sampled_from([0.0, 0.01, -0.01, 0.5, -0.5, 5.0, -5.0]), min_size=50, max_size=800))
def test_vectorized_equals_event_driven(mv: list[float]) -> None:
    prices = 81_000 + np.cumsum(mv)
    dpl, dpd = frames(prices)
    ev = R.targets_event(prices, P)
    assert np.array_equal(R.targets_polars(dpl, P)["target"].to_numpy(), ev)
    assert np.array_equal(R.targets_pandas(dpd, P)["target"].to_numpy(), ev)


@pytest.mark.skipif(not DATA.exists(), reason="real data not downloaded")
def test_parity_on_real_ticks() -> None:
    df = R.load_polars(DATA).head(100_000)
    p = Params()
    ev = R.targets_event(df["price"].to_numpy(), p)
    assert np.array_equal(R.targets_polars(df, p)["target"].to_numpy(), ev)
    assert int(np.sum(np.diff(ev) != 0)) > 50  # the strategy actually trades


def test_pnl_engines_agree() -> None:
    prices = 81_000 + np.cumsum(np.tile([0.5, -0.5, 3.0, -2.0, 0.0], 200))
    dpl, dpd = frames(prices)
    a = R.pnl_polars(R.targets_polars(dpl, P), P)
    b = R.pnl_pandas(R.targets_pandas(dpd, P), P)
    assert a == pytest.approx(b)
