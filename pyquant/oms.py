"""Order management: SQLite journal + idempotent submit + restart reconciliation.

Two modes, so the chaos test has a control:

safe  -- client order id is derived from the tick that caused it (``pq<tick_id>``),
         the order is journaled PENDING (together with the stream cursor) *before*
         it is sent, and on restart every PENDING order is looked up on the exchange
         by that id and either confirmed or re-sent with the same id.
naive -- random uuid order id, order recorded only after the ack, no reconcile.
         A kill between "exchange filled it" and "we wrote it down" makes the
         restarted process re-decide the same tick and send a second order.
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
import uuid
from dataclasses import dataclass

from .exchange import Ack, Exchange, ExchangeError

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders(
  coid TEXT PRIMARY KEY, tag INTEGER, side TEXT, lots INTEGER, price REAL,
  status TEXT, filled INTEGER DEFAULT 0, ts_ns INTEGER);
CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
"""


@dataclass(frozen=True)
class Order:
    coid: str
    tag: int
    side: str
    lots: int
    price: float


class Store:
    def __init__(self, path: str) -> None:
        self.db = sqlite3.connect(path, isolation_level=None)
        # WAL + synchronous=NORMAL survives a process kill -9 (not power loss).
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(SCHEMA)
        # Position is read on every tick; py-spy showed the SELECT at 12% of trader CPU.
        # Cache it in memory -- this process is the only writer, and every write path
        # below updates the cache only after its transaction commits.
        self._pos = int(self.get("pos", "0") or 0)

    def get(self, k: str, default: str | None = None) -> str | None:
        row = self.db.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
        return default if row is None else str(row[0])

    def _set(self, k: str, v: str) -> None:
        self.db.execute("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))

    def set(self, k: str, v: str) -> None:
        self._set(k, v)

    def position(self) -> int:
        return self._pos

    def has(self, coid: str) -> bool:
        return self.db.execute("SELECT 1 FROM orders WHERE coid=?", (coid,)).fetchone() is not None

    def pending(self) -> list[Order]:
        rows = self.db.execute("SELECT coid,tag,side,lots,price FROM orders WHERE status='PENDING'").fetchall()
        return [Order(*r) for r in rows]

    def insert_pending(self, o: Order, cursor: str) -> None:
        with self.db:  # BEGIN ... COMMIT
            self.db.execute("BEGIN")
            self.db.execute("INSERT INTO orders VALUES(?,?,?,?,?,'PENDING',0,?)",
                            (o.coid, o.tag, o.side, o.lots, o.price, time.time_ns()))
            self._set("cursor", cursor)

    def mark_filled(self, coid: str, a: Ack) -> None:
        signed = a.lots if a.side == "BUY" else -a.lots
        with self.db:
            self.db.execute("BEGIN")
            self.db.execute("UPDATE orders SET status='FILLED', filled=? WHERE coid=?", (a.lots, coid))
            self._set("pos", str(self._pos + signed))
        self._pos += signed

    def insert_filled(self, o: Order, a: Ack, cursor: str) -> None:
        signed = a.lots if a.side == "BUY" else -a.lots
        with self.db:
            self.db.execute("BEGIN")
            self.db.execute("INSERT INTO orders VALUES(?,?,?,?,?,'FILLED',?,?)",
                            (o.coid, o.tag, o.side, o.lots, o.price, a.lots, time.time_ns()))
            self._set("pos", str(self._pos + signed))
            self._set("cursor", cursor)
        self._pos += signed

    def close(self) -> None:
        self.db.close()


class OMS:
    def __init__(self, store: Store, exch: Exchange, mode: str = "safe", retries: int = 5) -> None:
        assert mode in ("safe", "naive")
        self.store, self.exch, self.mode, self.retries = store, exch, mode, retries
        self.sent = 0

    @property
    def position(self) -> int:
        return self.store.position()

    async def _send(self, o: Order) -> Ack:
        delay = 0.05
        for attempt in range(self.retries):
            try:
                return await self.exch.place(o.coid, o.side, o.lots, o.price, o.tag)
            except (TimeoutError, ExchangeError, OSError):
                if attempt == self.retries - 1 or self.mode == "naive":
                    raise
                await asyncio.sleep(delay)  # same coid on retry: safe to resend
                delay *= 2
        raise AssertionError("unreachable")

    async def submit(self, tag: int, target: int, price: float, cursor: str) -> Ack | None:
        delta = target - self.position
        if delta == 0:
            return None
        side = "BUY" if delta > 0 else "SELL"
        if self.mode == "safe":
            o = Order(f"pq{tag}", tag, side, abs(delta), price)
            if self.store.has(o.coid):  # tick re-processed after restart; already handled
                return None
            self.store.insert_pending(o, cursor)
            a = await self._send(o)
            self.store.mark_filled(o.coid, a)
        else:
            o = Order("pq" + uuid.uuid4().hex[:20], tag, side, abs(delta), price)
            a = await self._send(o)
            self.store.insert_filled(o, a, cursor)
        self.sent += 1
        return a

    async def reconcile(self) -> dict[str, int]:
        """Resolve every PENDING order by id, then compare positions with the exchange."""
        confirmed = resent = 0
        for o in self.store.pending():
            a = await self.exch.get_order(o.coid)
            if a is None:  # never reached the exchange: send it now, same id
                a = await self._send(o)
                resent += 1
            else:
                confirmed += 1
            self.store.mark_filled(o.coid, a)
        ex = await self.exch.position()
        off = self.store.get("exch_off")
        if off is None:  # first start: remember how the account differed from our book
            self.store.set("exch_off", str(ex - self.position))
            off = str(ex - self.position)
        mismatch = (ex - int(off)) - self.position
        return {"confirmed": confirmed, "resent": resent, "mismatch": mismatch}
