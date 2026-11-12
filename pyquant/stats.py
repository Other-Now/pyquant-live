"""Rolling z-score of the last `window` prices: the per-tick hot path.

Four implementations with one interface, `update(x) -> z | None`:

* ``NaiveZ``       -- deque + sum() every tick, O(W). The "obvious" first version.
* ``NumpyZ``       -- ring buffer + np.mean/np.std, O(W) in C but ~µs call overhead.
* ``IncrementalZ`` -- sliding-window mean/M2, O(1), pure Python. Used live.
* ``NumbaZ``       -- same O(1) update compiled with numba (imported lazily so the
                      trader process and the free-threaded venv never load numba).

All return ``None`` until the window is full or when the window is (numerically) flat.
"""

from __future__ import annotations

import math
from collections import deque
from typing import Any, Protocol

import numpy as np

# Window counts as flat when std <= FLAT_REL * |mean|. The vectorized research code
# applies the same rule so live and backtest agree.
FLAT_REL = 1e-9


class ZScore(Protocol):
    def update(self, x: float) -> float | None: ...


class NaiveZ:
    def __init__(self, window: int) -> None:
        self.w = window
        self.buf: deque[float] = deque(maxlen=window)

    def update(self, x: float) -> float | None:
        self.buf.append(x)
        if len(self.buf) < self.w:
            return None
        n = self.w
        m = sum(self.buf) / n
        var = sum((v - m) * (v - m) for v in self.buf) / n
        sd = math.sqrt(var)
        if sd <= FLAT_REL * abs(m):
            return None
        return (x - m) / sd


class NumpyZ:
    def __init__(self, window: int) -> None:
        self.w = window
        self.buf = np.zeros(window)
        self.n = 0

    def update(self, x: float) -> float | None:
        self.buf[self.n % self.w] = x
        self.n += 1
        if self.n < self.w:
            return None
        m = float(self.buf.mean())
        sd = float(self.buf.std())
        if sd <= FLAT_REL * abs(m):
            return None
        return (x - m) / sd


class IncrementalZ:
    """O(1) sliding mean / sum-of-squared-deviations, in shifted coordinates.

    Values are stored as ``x - K``. Every ``window`` updates the stats are recomputed
    exactly (math.fsum) and K is moved to the current mean, so the sliding updates work
    on numbers near 0 instead of near 81,000. Without the shift the error vs numpy
    reached 5e-3 in z on near-flat windows (std ~ 6e-4 USDT) of real BTCUSDT data.
    """

    def __init__(self, window: int, shift: bool = True) -> None:
        self.w = window
        self.shift = shift
        self.buf = [0.0] * window  # raw prices
        self.n = 0
        self.k = 0.0  # shift
        self.mean = 0.0  # mean of (x - k)
        self.m2 = 0.0

    def update(self, x: float) -> float | None:
        w = self.w
        i = self.n % w
        if self.n == 0 and self.shift:
            self.k = x
        if self.n < w:  # filling: plain Welford on x - k
            self.buf[i] = x
            self.n += 1
            y = x - self.k
            d = y - self.mean
            self.mean += d / self.n
            self.m2 += d * (y - self.mean)
            if self.n < w:
                return None
        else:
            out = self.buf[i] - self.k
            self.buf[i] = x
            self.n += 1
            if i == w - 1:  # exact recompute once per window, re-centre the shift
                if self.shift:
                    self.k = math.fsum(self.buf) / w
                m = math.fsum(v - self.k for v in self.buf) / w
                self.mean = m
                self.m2 = math.fsum((v - self.k - m) ** 2 for v in self.buf)
            else:
                y = x - self.k
                old = self.mean
                self.mean = old + (y - out) / w
                self.m2 += (y - out) * (y - self.mean + out - old)
        var = self.m2 / w
        sd = math.sqrt(var) if var > 0.0 else 0.0
        if sd <= FLAT_REL * abs(self.mean + self.k):
            return None
        return (x - self.k - self.mean) / sd


_numba_update: Any = None


def _compile() -> Any:
    global _numba_update
    if _numba_update is None:
        from numba import njit  # lazy: keeps numba out of the trader / 3.13t venv

        @njit(cache=True)
        def upd(buf: Any, st: Any, x: float) -> float:
            # st = [n, mean(x-k), m2, k]; same algorithm as IncrementalZ
            w = buf.shape[0]
            n = int(st[0])
            i = n % w
            if n == 0:
                st[3] = x
            if n < w:
                buf[i] = x
                n += 1
                y = x - st[3]
                d = y - st[1]
                st[1] += d / n
                st[2] += d * (y - st[1])
                st[0] = n
                if n < w:
                    return np.nan
            else:
                out = buf[i] - st[3]
                buf[i] = x
                st[0] = n + 1
                if i == w - 1:
                    st[3] = buf.sum() / w
                    c = buf - st[3]
                    m = c.sum() / w
                    st[1] = m
                    st[2] = ((c - m) ** 2).sum()
                else:
                    y = x - st[3]
                    old = st[1]
                    st[1] = old + (y - out) / w
                    st[2] += (y - out) * (y - st[1] + out - old)
            var = st[2] / w
            sd = np.sqrt(var) if var > 0.0 else 0.0
            if sd <= 1e-9 * abs(st[1] + st[3]):
                return np.nan
            return float((x - st[3] - st[1]) / sd)

        _numba_update = upd
    return _numba_update


class NumbaZ:
    def __init__(self, window: int) -> None:
        self.buf = np.zeros(window)
        self.st = np.zeros(4)
        self._f = _compile()

    def update(self, x: float) -> float | None:
        z: float = float(self._f(self.buf, self.st, x))
        return None if math.isnan(z) else z
