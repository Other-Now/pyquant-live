"""Vectorized research backtest of the same strategy, in Polars and in pandas.

The hysteresis state machine in ``strategy.next_target`` vectorizes as:
entry/exit conditions emit a value, everything else is null, then forward-fill.
That is exactly "keep previous target" -- and the parity test checks it tick by tick
against the event-driven ``MeanReversion`` class used live.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

from .stats import FLAT_REL
from .strategy import MeanReversion, Params

AGG_COLS = ["agg_id", "price", "qty", "first_id", "last_id", "ts_us", "is_buyer_maker", "best"]


def load_polars(path: str | Path) -> pl.DataFrame:
    return pl.read_csv(path, has_header=False, new_columns=AGG_COLS).select("agg_id", "price", "qty", "ts_us")


def load_pandas(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path, header=None, names=AGG_COLS, usecols=["agg_id", "price", "qty", "ts_us"])


def targets_polars(df: pl.DataFrame, p: Params) -> pl.DataFrame:
    px = pl.col("price")
    m = px.rolling_mean(p.window)
    sd = px.rolling_std(p.window, ddof=0)
    z = pl.when(sd > FLAT_REL * m.abs()).then((px - m) / sd).otherwise(None)
    sig = (
        pl.when(z > p.entry).then(-1)
        .when(z < -p.entry).then(1)
        .when(z.abs() < p.exit).then(0)
        .otherwise(None)
    )
    return df.with_columns(target=sig.forward_fill().fill_null(0).cast(pl.Int8))


def targets_pandas(df: pd.DataFrame, p: Params) -> pd.DataFrame:
    r = df["price"].rolling(p.window)
    m = r.mean()
    sd = r.std(ddof=0)
    z = ((df["price"] - m) / sd).where(sd > FLAT_REL * m.abs())
    sig = pd.Series(np.nan, index=df.index)
    sig[z.abs() < p.exit] = 0
    sig[z < -p.entry] = 1
    sig[z > p.entry] = -1
    out = df.copy()
    out["target"] = sig.ffill().fillna(0).astype("int8")
    return out


def pnl_polars(df: pl.DataFrame, p: Params, fee_bps: float = 1.0) -> dict[str, float]:
    """Mark-to-market PnL in USDT; target held from the tick it is decided on."""
    d = df.with_columns(
        pos=pl.col("target").cast(pl.Float64) * p.lot,
        dpx=pl.col("price").diff().shift(-1).fill_null(0.0),
    ).with_columns(trade=(pl.col("pos").diff().fill_null(pl.col("pos"))).abs())
    gross = d.select((pl.col("pos") * pl.col("dpx")).sum()).item()
    traded = d.select((pl.col("trade") * pl.col("price")).sum()).item()
    n = d.select((pl.col("trade") > 0).sum()).item()
    return {"gross": gross, "fees": traded * fee_bps / 1e4, "net": gross - traded * fee_bps / 1e4, "trades": n}


def pnl_pandas(df: pd.DataFrame, p: Params, fee_bps: float = 1.0) -> dict[str, float]:
    pos = df["target"].astype(float) * p.lot
    dpx = df["price"].diff().shift(-1).fillna(0.0)
    trade = pos.diff().fillna(pos).abs()
    gross = float((pos * dpx).sum())
    traded = float((trade * df["price"]).sum())
    return {"gross": gross, "fees": traded * fee_bps / 1e4, "net": gross - traded * fee_bps / 1e4,
            "trades": int((trade > 0).sum())}


def targets_event(prices: np.ndarray, p: Params) -> np.ndarray:
    """The live strategy class, driven tick by tick."""
    s = MeanReversion(p)
    out = np.empty(len(prices), dtype=np.int8)
    for i, x in enumerate(prices.tolist()):
        out[i] = s.on_price(x)
    return out
