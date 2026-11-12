"""Trader end to end over a real Redis (skipped when none is reachable; CI provides one)."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import numpy as np
import pytest
import redis
from aiohttp.test_utils import TestServer

from pyquant import trader
from pyquant.exchange import MockExchange
from pyquant.research import targets_event
from pyquant.strategy import Params

URL = os.environ.get("PQ_REDIS", "redis://127.0.0.1:6390")


def _redis() -> redis.Redis:
    r = redis.Redis.from_url(URL)
    try:
        r.ping()
    except redis.exceptions.ConnectionError:
        pytest.skip("no redis at " + URL)
    return r


def _load(r: redis.Redis, sym: str, prices: np.ndarray, start: int) -> None:
    pipe = r.pipeline()
    for i, p in enumerate(prices.tolist()):
        pipe.xadd(f"ticks:{sym}", {"p": repr(p), "q": "1", "id": start + i, "t": (start + i) * 100})
    pipe.execute()


async def _run(url: str, sym: str, db: str) -> dict[str, float]:
    a = trader.parser().parse_args(["--redis", URL, "--exchange", url, "--symbol", sym, "--db", db,
                                    "--window", "30", "--entry", "1.5", "--exit", "0.3", "--max-rate", "1e9",
                                    "--exit-when-idle", "--block-ms", "200"])
    return await trader.run(a)


async def test_trader_orders_match_backtest_and_resume(tmp_path: Path) -> None:
    r = _redis()
    sym = "T" + uuid.uuid4().hex[:8].upper()
    rng = np.random.default_rng(3)
    prices = 81_000 + np.cumsum(rng.choice([0.0, 0.01, -0.01, 2.0, -2.0], size=3_000))
    mx = MockExchange()
    srv = TestServer(mx.app())
    await srv.start_server()
    url = str(srv.make_url("")).rstrip("/")
    db = str(tmp_path / "t.db")
    try:
        _load(r, sym, prices[:1_500], 0)
        await _run(url, sym, db)
        _load(r, sym, prices[1_500:], 1_500)  # more market data arrives; restart resumes at cursor
        out = await _run(url, sym, db)
    finally:
        await srv.close()
        r.delete(f"ticks:{sym}")
    ev = targets_event(prices, Params(window=30, entry=1.5, exit=0.3))
    want = [i for i in range(len(ev)) if ev[i] != (ev[i - 1] if i else 0)]
    assert [o.tag for o in mx.log] == want  # one order per target change, none lost or doubled
    assert mx.pos == int(ev[-1]) == out["pos"]
