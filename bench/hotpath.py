"""Per-tick hot path, measured step by step on real BTCUSDT prices.

Hot path = parse the exchange message -> update rolling z-score -> strategy decision.
Each variant runs over the same 200k ticks; per-tick time is measured with
perf_counter_ns around each call and reported as p50 / p99 / mean.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import orjson

from pyquant.research import load_polars
from pyquant.stats import IncrementalZ, NaiveZ, NumbaZ, NumpyZ, ZScore
from pyquant.strategy import Params, next_target


def pct(a: np.ndarray, q: float) -> float:
    return float(np.percentile(a, q))


def time_each(fn: Callable[[Any], object], items: list[Any]) -> np.ndarray:
    out = np.empty(len(items), dtype=np.int64)
    pc = time.perf_counter_ns
    for i, x in enumerate(items):
        t = pc()
        fn(x)
        out[i] = pc() - t
    return out


def summary(name: str, ns: np.ndarray, skip: int) -> dict[str, float | str]:
    a = ns[skip:]
    return {"variant": name, "p50_ns": pct(a, 50), "p99_ns": pct(a, 99), "mean_ns": float(a.mean()),
            "ticks_per_s": 1e9 / float(a.mean())}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="data/BTCUSDT-aggTrades-2026-10-15.csv")
    ap.add_argument("--n", type=int, default=200_000)
    ap.add_argument("--window", type=int, default=300)
    ap.add_argument("--out", default="results/hotpath.json")
    a = ap.parse_args()
    df = load_polars(a.csv).head(a.n)
    prices = df["price"].to_list()
    p = Params(window=a.window)
    res: list[dict[str, float | str]] = []

    # 1) z-score + decision, four implementations
    impls: list[tuple[str, Callable[[int], ZScore]]] = [
        ("naive_deque_O(W)", NaiveZ), ("numpy_ringbuf_O(W)", NumpyZ),
        ("incremental_O(1)_unshifted", lambda w: IncrementalZ(w, shift=False)),
        ("incremental_O(1)", IncrementalZ), ("numba_O(1)", NumbaZ)]
    zs: dict[str, list[float | None]] = {}
    for name, cls in impls:
        z = cls(a.window)
        if name.startswith("numba"):
            z.update(1.0)  # JIT compile outside the timing
            z = cls(a.window)
        st = {"t": 0}
        got: list[float | None] = []

        def step(x: float, z: ZScore = z, st: dict[str, int] = st, got: list[float | None] = got) -> None:
            v = z.update(x)
            got.append(v)
            st["t"] = next_target(st["t"], v, p)

        res.append(summary(name, time_each(step, prices), a.window))
        zs[name] = got
    ref = np.array([np.nan if v is None else v for v in zs["numpy_ringbuf_O(W)"]])
    for name in zs:
        v = np.array([np.nan if x is None else x for x in zs[name]])
        m = ~np.isnan(ref) & ~np.isnan(v)
        res.append({"variant": f"maxabs_dz[{name}]", "value": float(np.max(np.abs(v[m] - ref[m])))})

    # 2) message parse: json vs orjson on Binance trade messages built from the real ticks
    msgs = [json.dumps({"e": "trade", "E": 1, "s": "BTCUSDT", "t": int(r[0]), "p": f"{r[1]:.8f}",
                        "q": f"{r[2]:.8f}", "T": int(r[3]) // 1000, "m": True, "M": True}).encode()
            for r in df.rows()]
    res.append(summary("parse_json_stdlib", time_each(json.loads, msgs), 0))
    res.append(summary("parse_orjson", time_each(orjson.loads, msgs), 0))

    for r in res:
        print(r)
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    Path(a.out).write_text(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
