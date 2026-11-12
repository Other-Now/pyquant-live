"""Mean-reversion strategy. The SAME class runs in the event backtest and the live trader.

Target position in lots, one of {-1, 0, +1}:
    z >  entry      -> -1   (price stretched up, fade it)
    z < -entry      -> +1
    |z| < exit      ->  0   (back to the mean, flatten)
    otherwise / no z -> keep previous target (hysteresis)
"""

from __future__ import annotations

from dataclasses import dataclass

from .stats import IncrementalZ, ZScore


@dataclass(frozen=True)
class Params:
    window: int = 300
    entry: float = 2.5
    exit: float = 0.5
    lot: float = 0.001  # BTC per lot


def next_target(prev: int, z: float | None, p: Params) -> int:
    if z is None:
        return prev
    if z > p.entry:
        return -1
    if z < -p.entry:
        return 1
    if abs(z) < p.exit:
        return 0
    return prev


class MeanReversion:
    def __init__(self, p: Params, z: ZScore | None = None) -> None:
        self.p = p
        self.z: ZScore = z if z is not None else IncrementalZ(p.window)
        self.target = 0

    def on_price(self, px: float) -> int:
        self.target = next_target(self.target, self.z.update(px), self.p)
        return self.target
