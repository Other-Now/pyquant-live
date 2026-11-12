"""Full-day research backtest: pandas vs Polars vs the live event-driven class, plus parity.

Pipeline per engine: read CSV -> rolling z -> hysteresis targets -> PnL.
Parity: the per-tick target series from Polars and pandas must equal the series the
live ``MeanReversion`` class produces when fed the same ticks one by one.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

import numpy as np

from pyquant import research as R
from pyquant.strategy import Params

T = TypeVar("T")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="data/BTCUSDT-aggTrades-2026-10-15.csv")
    ap.add_argument("--out", default="results/dataproc.json")
    a = ap.parse_args()
    p = Params()
    res: dict[str, object] = {"params": p.__dict__}

    def best(fn: Callable[[], T], reps: int = 3) -> tuple[T, float]:
        bt, out = float("inf"), None
        for _ in range(reps):
            t = time.perf_counter()
            out = fn()
            bt = min(bt, time.perf_counter() - t)
        assert out is not None
        return out, bt

    R.load_polars(a.csv)  # warm the page cache once so neither engine pays the cold read
    dpl, l_pl = best(lambda: R.load_polars(a.csv))
    tpl, s_pl = best(lambda: R.targets_polars(dpl, p))
    pnl_pl, p_pl = best(lambda: R.pnl_polars(tpl, p))
    dpd, l_pd = best(lambda: R.load_pandas(a.csv))
    tpd, s_pd = best(lambda: R.targets_pandas(dpd, p))
    pnl_pd, p_pd = best(lambda: R.pnl_pandas(tpd, p))
    rows = dpl.height
    prices = dpl["price"].to_numpy()
    tev, s_ev = best(lambda: R.targets_event(prices, p), reps=1)

    def eng(load: float, sig: float, pnl: float) -> dict[str, float]:
        tot = load + sig + pnl
        return {"load_s": load, "signal_s": sig, "pnl_s": pnl, "total_s": tot, "rows_per_s": rows / tot,
                "signal_rows_per_s": rows / sig}

    res["rows"] = rows
    res["polars"] = eng(l_pl, s_pl, p_pl)
    res["pandas"] = eng(l_pd, s_pd, p_pd)
    res["event_class"] = {"signal_s": s_ev, "signal_rows_per_s": rows / s_ev}
    a_pl = tpl["target"].to_numpy()
    a_pd = tpd["target"].to_numpy()
    res["parity"] = {
        "polars_vs_event_mismatch_ticks": int(np.sum(a_pl != tev)),
        "pandas_vs_event_mismatch_ticks": int(np.sum(a_pd != tev)),
        "target_changes": int(np.sum(np.diff(tev.astype(np.int64)) != 0) + (tev[0] != 0)),
    }
    res["pnl_usdt_1lot_0.001btc"] = {"polars": pnl_pl, "pandas": pnl_pd}
    print(json.dumps(res, indent=1))
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    Path(a.out).write_text(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
