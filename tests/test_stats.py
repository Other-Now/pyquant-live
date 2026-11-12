from __future__ import annotations

import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from pyquant.stats import IncrementalZ, NaiveZ, NumpyZ

# BTC-like prices: a level near 80k plus small cent-level moves (incl. long flat runs)
levels = st.floats(1_000, 120_000)
moves = st.lists(st.sampled_from([0.0, 0.0, 0.0, 0.01, -0.01, 0.5, -0.5, 7.0, -7.0]), min_size=60, max_size=600)


def run(z: object, xs: list[float]) -> list[float | None]:
    assert hasattr(z, "update")
    return [z.update(x) for x in xs]


def path(level: float, mv: list[float]) -> list[float]:
    return list(level + np.cumsum(mv))


@settings(max_examples=150, deadline=None)
@given(levels, moves, st.integers(5, 50))
def test_incremental_matches_numpy(level: float, mv: list[float], w: int) -> None:
    xs = path(level, mv)
    a, b = run(IncrementalZ(w), xs), run(NumpyZ(w), xs)
    for za, zb in zip(a, b, strict=True):
        if za is None or zb is None:
            continue  # flat-window guard can differ by rounding exactly at the threshold
        assert za == pytest.approx(zb, rel=1e-6, abs=1e-6)


@settings(max_examples=100, deadline=None)
@given(levels, moves, st.integers(5, 50))
def test_naive_matches_numpy(level: float, mv: list[float], w: int) -> None:
    xs = path(level, mv)
    for za, zb in zip(run(NaiveZ(w), xs), run(NumpyZ(w), xs), strict=True):
        if za is not None and zb is not None:
            assert za == pytest.approx(zb, rel=1e-6, abs=1e-6)


def test_none_until_window_full_and_on_flat() -> None:
    z = IncrementalZ(5)
    assert [z.update(100.0) for _ in range(10)] == [None] * 10


def near_flat_case() -> list[float]:
    # the shape found in real data: long stretch at one price, then a 1-cent print
    rng = np.random.default_rng(1)
    base = 81682.02
    xs = [base + 0.01 * float(rng.integers(0, 2)) for _ in range(2_000)]
    xs += [base] * 299 + [base - 0.01]
    return xs


def worst_err(shift: bool) -> float:
    xs = near_flat_case()
    a, b = run(IncrementalZ(300, shift=shift), xs), run(NumpyZ(300), xs)
    return max((abs(x - y) for x, y in zip(a, b, strict=True) if x is not None and y is not None), default=0.0)


def test_shift_fixes_near_flat_drift() -> None:
    """Regression for the bug the benchmark found: unshifted O(1) variance drifts."""
    assert worst_err(shift=True) < 1e-6
    assert worst_err(shift=False) > 100 * worst_err(shift=True)


def test_numba_matches() -> None:
    pytest.importorskip("numba")
    from pyquant.stats import NumbaZ

    xs = path(81682.0, [0.01, -0.01, 0.0, 3.0, -3.0] * 400)
    for za, zb in zip(run(NumbaZ(50), xs), run(NumpyZ(50), xs), strict=True):
        if za is not None and zb is not None:
            assert za == pytest.approx(zb, rel=1e-5, abs=1e-5)
