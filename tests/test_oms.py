"""OMS crash/restart semantics against the real mock-exchange app (in-process HTTP)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest_asyncio
from aiohttp.test_utils import TestServer

from pyquant.exchange import Ack, MockClient, MockExchange
from pyquant.oms import OMS, Order, Store


@pytest_asyncio.fixture
async def exch() -> AsyncIterator[tuple[MockExchange, MockClient]]:
    mx = MockExchange()
    srv = TestServer(mx.app())
    await srv.start_server()
    cl = MockClient(str(srv.make_url("")).rstrip("/"))
    yield mx, cl
    await cl.close()
    await srv.close()


async def test_submit_is_idempotent_per_tick(exch: tuple[MockExchange, MockClient], tmp_path: Path) -> None:
    mx, cl = exch
    oms = OMS(Store(str(tmp_path / "a.db")), cl)
    assert await oms.submit(tag=1, target=1, price=100.0, cursor="1-0") is not None
    assert await oms.submit(tag=1, target=1, price=100.0, cursor="1-0") is None  # already at target
    assert mx.pos == 1 and oms.position == 1


async def test_duplicate_coid_rejected_by_exchange(exch: tuple[MockExchange, MockClient]) -> None:
    mx, cl = exch
    a = await cl.place("pq7", "BUY", 1, 100.0, 7)
    b = await cl.place("pq7", "BUY", 1, 100.0, 7)
    assert not a.duplicate and b.duplicate and mx.pos == 1 and len(mx.log) == 1


async def test_crash_after_send_is_confirmed_not_resent(exch: tuple[MockExchange, MockClient],
                                                         tmp_path: Path) -> None:
    """Kill between 'exchange filled' and 'book updated': reconcile confirms by coid."""
    mx, cl = exch
    db = str(tmp_path / "b.db")
    s = Store(db)
    o = Order("pq42", 42, "BUY", 1, 100.0)
    s.insert_pending(o, "42-0")
    await cl.place(o.coid, o.side, o.lots, o.price, o.tag)  # reached the exchange...
    s.close()  # ...then the process died before mark_filled
    oms = OMS(Store(db), cl)
    rec = await oms.reconcile()
    assert rec == {"confirmed": 1, "resent": 0, "mismatch": 0}
    assert mx.pos == 1 and len(mx.log) == 1 and oms.position == 1


async def test_crash_before_send_is_resent_once(exch: tuple[MockExchange, MockClient], tmp_path: Path) -> None:
    mx, cl = exch
    db = str(tmp_path / "c.db")
    s = Store(db)
    s.insert_pending(Order("pq43", 43, "SELL", 1, 100.0), "43-0")
    s.close()  # died before the request left
    oms = OMS(Store(db), cl)
    assert (await oms.reconcile())["resent"] == 1
    assert (await oms.reconcile())["resent"] == 0  # second restart: nothing pending
    assert mx.pos == -1 and len(mx.log) == 1


async def test_naive_mode_duplicates_on_replay(exch: tuple[MockExchange, MockClient], tmp_path: Path) -> None:
    """The control: no journal before send, random coid -> re-deciding a tick doubles it."""
    mx, cl = exch
    oms = OMS(Store(str(tmp_path / "d.db")), cl, mode="naive")
    await cl.place("lost-ack", "BUY", 1, 100.0, 9)  # what a naive trader sent before dying
    await oms.submit(tag=9, target=1, price=100.0, cursor="9-0")  # restart re-decides tick 9
    assert mx.pos == 2 and len(mx.log) == 2


async def test_reconcile_detects_position_mismatch(exch: tuple[MockExchange, MockClient], tmp_path: Path) -> None:
    _, cl = exch
    oms = OMS(Store(str(tmp_path / "e.db")), cl)
    assert (await oms.reconcile())["mismatch"] == 0  # stores offset 0
    await cl.place("manual", "BUY", 1, 1.0, 0)  # someone traded the account behind our back
    assert (await oms.reconcile())["mismatch"] == 1


def test_store_position_survives_reopen(tmp_path: Path) -> None:
    db = str(tmp_path / "f.db")
    s = Store(db)
    s.insert_pending(Order("x", 1, "SELL", 1, 1.0), "1-0")
    s.mark_filled("x", Ack("x", "SELL", 1, 1.0, 1))
    s.close()
    s2 = Store(db)
    assert s2.position() == -1 and s2.get("cursor") == "1-0" and s2.pending() == []
