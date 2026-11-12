"""Same job, five ways: sequential, asyncio, threads, asyncio.to_thread, processes.

Job = run the live strategy (pure-Python O(1) z-score) over 8 "symbols" of real ticks
(8 slices of the BTCUSDT day). CPU-bound pure Python -> the GIL decides the result.
A second job does the same maths in numpy (vectorised rolling std), which releases the
GIL inside numpy, to show where threads *do* help on a GIL build.

Run it under both interpreters:
    ~/pq-venv/bin/python    -m bench.concurrency   (CPython 3.13, GIL)
    ~/pq-venv-ft/bin/python -m bench.concurrency   (CPython 3.13t, free-threaded)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import sysconfig
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

from pyquant.strategy import MeanReversion, Params

W = 300


def job_python(chunk: np.ndarray) -> int:
    s = MeanReversion(Params(window=W))
    changes, prev = 0, 0
    for x in chunk.tolist():
        t = s.on_price(x)
        changes += t != prev
        prev = t
    return changes


def job_numpy(chunk: np.ndarray) -> int:
    # exact rolling std over windows, in 8k-row blocks so temporaries stay ~20 MB/thread
    win = sliding_window_view(chunk, W)
    n = 0
    for i in range(0, len(win), 8192):
        b = win[i:i + 8192]
        sd = b.std(axis=1)
        z = (b[:, -1] - b.mean(axis=1)) / np.where(sd > 0, sd, np.nan)
        n += int(np.nansum(np.abs(z) > 2.5))
    return n


async def _coro(f: object, c: np.ndarray) -> int:
    assert callable(f)
    return int(f(c))


def bench(job: object, chunks: list[np.ndarray], workers: int) -> dict[str, float]:
    assert callable(job)
    out: dict[str, float] = {}

    def t(fn: object, reps: int = 3) -> float:  # best of 3
        assert callable(fn)
        best = float("inf")
        for _ in range(reps):
            t0 = time.perf_counter()
            fn()
            best = min(best, time.perf_counter() - t0)
        return best

    out["sequential"] = t(lambda: [job(c) for c in chunks])

    async def gather() -> list[int]:
        return await asyncio.gather(*(_coro(job, c) for c in chunks))

    out["asyncio_gather"] = t(lambda: asyncio.run(gather()))
    with ThreadPoolExecutor(workers) as ex:
        out["threads"] = t(lambda: list(ex.map(job, chunks)))

    async def to_thread() -> list[int]:
        return await asyncio.gather(*(asyncio.to_thread(job, c) for c in chunks))

    out["asyncio_to_thread"] = t(lambda: asyncio.run(to_thread()))
    with ProcessPoolExecutor(workers) as pex:
        list(pex.map(job, chunks[:workers]))  # warm the pool (fork + imports)
        out["processes"] = t(lambda: list(pex.map(job, chunks)))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prices", default="data/prices.npy")
    ap.add_argument("--chunks", type=int, default=8)
    ap.add_argument("--per-chunk", type=int, default=120_000)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--out", default="")
    a = ap.parse_args()
    if not Path(a.prices).exists():
        from pyquant.research import load_polars

        np.save(a.prices, load_polars("data/BTCUSDT-aggTrades-2026-10-15.csv")["price"].to_numpy())
    px = np.load(a.prices)
    chunks = [np.ascontiguousarray(px[i * a.per_chunk:(i + 1) * a.per_chunk]) for i in range(a.chunks)]
    gil = getattr(sys, "_is_gil_enabled", lambda: True)()
    build = "free-threaded" if sysconfig.get_config_var("Py_GIL_DISABLED") else "gil"
    res = {"python": sys.version.split()[0], "build": build, "gil_enabled_at_runtime": gil,
           "cpus": os.cpu_count(), "workers": a.workers,
           "pure_python_s": bench(job_python, chunks, a.workers),
           "numpy_s": bench(job_numpy, chunks, a.workers)}
    print(json.dumps(res, indent=1))
    out = a.out or f"results/concurrency_{build}.json"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    Path(out).write_text(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
