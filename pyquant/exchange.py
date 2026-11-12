"""Exchange side: a mock exchange server, its client, and a Binance spot-testnet client.

Both clients implement the same 3-call ``Exchange`` protocol. The property the OMS
relies on is the one real Binance has: a client order id (``newClientOrderId``) can be
looked up later, and resending a live id is rejected as a duplicate rather than
creating a second order.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any, Protocol

import aiohttp
import orjson
from aiohttp import web


@dataclass(frozen=True)
class Ack:
    coid: str
    side: str  # BUY / SELL
    lots: int
    price: float
    tag: int  # tick id that caused the order (for audit only)
    duplicate: bool = False


class Exchange(Protocol):
    async def place(self, coid: str, side: str, lots: int, price: float, tag: int) -> Ack: ...
    async def get_order(self, coid: str) -> Ack | None: ...
    async def position(self) -> int: ...
    async def close(self) -> None: ...


# --------------------------------------------------------------------------- mock server
class MockExchange:
    """In-memory exchange. Market-able orders fill fully at the given price.

    ``latency_s`` is split around the fill: the order is applied half-way through the
    request, so a client killed mid-request leaves a real fill it never saw acked --
    the exact window that produces duplicate orders in a naive OMS.
    """

    def __init__(self, latency_s: float = 0.0) -> None:
        self.latency_s = latency_s
        self.orders: dict[str, Ack] = {}
        self.log: list[Ack] = []
        self.pos = 0

    def app(self) -> web.Application:
        a = web.Application()
        a.router.add_post("/order", self._place)
        a.router.add_get("/order/{coid}", self._get)
        a.router.add_get("/position", self._position)
        a.router.add_get("/orders", self._orders)
        a.router.add_post("/reset", self._reset)
        return a

    async def _place(self, req: web.Request) -> web.Response:
        b = orjson.loads(await req.read())
        if self.latency_s:
            await asyncio.sleep(self.latency_s / 2)
        coid = b["coid"]
        if coid in self.orders:
            o = self.orders[coid]
            return web.Response(status=409, body=orjson.dumps({"code": -2010, "msg": "Duplicate order sent.",
                                                               **_ack_dict(o)}))
        o = Ack(coid, b["side"], int(b["lots"]), float(b["price"]), int(b["tag"]))
        self.orders[coid] = o
        self.log.append(o)
        self.pos += o.lots if o.side == "BUY" else -o.lots
        if self.latency_s:
            await asyncio.sleep(self.latency_s / 2)
        return web.Response(body=orjson.dumps(_ack_dict(o)), content_type="application/json")

    async def _get(self, req: web.Request) -> web.Response:
        o = self.orders.get(req.match_info["coid"])
        if o is None:
            return web.Response(status=404, body=b'{"code":-2013,"msg":"Order does not exist."}')
        return web.Response(body=orjson.dumps(_ack_dict(o)), content_type="application/json")

    async def _position(self, _: web.Request) -> web.Response:
        return web.Response(body=orjson.dumps({"lots": self.pos}), content_type="application/json")

    async def _orders(self, _: web.Request) -> web.Response:
        return web.Response(body=orjson.dumps([_ack_dict(o) for o in self.log]), content_type="application/json")

    async def _reset(self, req: web.Request) -> web.Response:
        if "latency_ms" in req.query:
            self.latency_s = float(req.query["latency_ms"]) / 1000
        self.orders.clear()
        self.log.clear()
        self.pos = 0
        return web.Response(text="ok")


def _ack_dict(o: Ack) -> dict[str, Any]:
    return {"coid": o.coid, "side": o.side, "lots": o.lots, "price": o.price, "tag": o.tag}


def _ack(d: dict[str, Any], duplicate: bool = False) -> Ack:
    return Ack(d["coid"], d["side"], int(d["lots"]), float(d["price"]), int(d["tag"]), duplicate)


class ExchangeError(Exception):
    pass


class MockClient:
    def __init__(self, base: str = "http://127.0.0.1:8090") -> None:
        self.base = base
        self.s = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5))

    async def place(self, coid: str, side: str, lots: int, price: float, tag: int) -> Ack:
        body = orjson.dumps({"coid": coid, "side": side, "lots": lots, "price": price, "tag": tag})
        async with self.s.post(self.base + "/order", data=body) as r:
            d = orjson.loads(await r.read())
            if r.status == 409:
                return _ack(d, duplicate=True)
            if r.status != 200:
                raise ExchangeError(f"{r.status} {d}")
            return _ack(d)

    async def get_order(self, coid: str) -> Ack | None:
        async with self.s.get(f"{self.base}/order/{coid}") as r:
            if r.status == 404:
                return None
            if r.status != 200:
                raise ExchangeError(str(r.status))
            return _ack(orjson.loads(await r.read()))

    async def position(self) -> int:
        async with self.s.get(self.base + "/position") as r:
            return int(orjson.loads(await r.read())["lots"])

    async def orders(self) -> list[Ack]:
        async with self.s.get(self.base + "/orders") as r:
            return [_ack(d) for d in orjson.loads(await r.read())]

    async def reset(self, latency_ms: float = 0.0) -> None:
        async with self.s.post(f"{self.base}/reset?latency_ms={latency_ms}") as r:
            r.raise_for_status()

    async def close(self) -> None:
        await self.s.close()


# --------------------------------------------------------------------------- Binance testnet
class BinanceTestnet:
    """Signed REST client for https://testnet.binance.vision (spot).

    Orders are IOC LIMIT at the decision price, so an unfilled remainder never rests.
    The position is tracked as the base-asset balance delta from start-up.
    NOTE: written against the documented API; not exercised in this repo's results
    (needs PQ_API_KEY / PQ_API_SECRET).
    """

    BASE = "https://testnet.binance.vision"

    def __init__(self, symbol: str = "BTCUSDT", lot: float = 0.001) -> None:
        self.key = os.environ["PQ_API_KEY"]
        self.secret = os.environ["PQ_API_SECRET"].encode()
        self.symbol, self.lot = symbol, lot
        self.s = aiohttp.ClientSession(headers={"X-MBX-APIKEY": self.key})

    def _signed(self, params: dict[str, Any]) -> str:
        params = {**params, "timestamp": int(time.time() * 1000), "recvWindow": 5000}
        q = urllib.parse.urlencode(params)
        sig = hmac.new(self.secret, q.encode(), hashlib.sha256).hexdigest()
        return f"{q}&signature={sig}"

    async def place(self, coid: str, side: str, lots: int, price: float, tag: int) -> Ack:
        q = self._signed({"symbol": self.symbol, "side": side, "type": "LIMIT", "timeInForce": "IOC",
                          "quantity": f"{lots * self.lot:.6f}", "price": f"{price:.2f}",
                          "newClientOrderId": coid})
        async with self.s.post(f"{self.BASE}/api/v3/order?{q}") as r:
            d = orjson.loads(await r.read())
            if r.status == 400 and d.get("code") == -2010 and "Duplicate" in d.get("msg", ""):
                got = await self.get_order(coid)
                if got is None:
                    raise ExchangeError(str(d))
                return Ack(got.coid, got.side, got.lots, got.price, tag, duplicate=True)
            if r.status != 200:
                raise ExchangeError(str(d))
            filled = round(float(d["executedQty"]) / self.lot)
            return Ack(coid, side, filled, price, tag)

    async def get_order(self, coid: str) -> Ack | None:
        q = self._signed({"symbol": self.symbol, "origClientOrderId": coid})
        async with self.s.get(f"{self.BASE}/api/v3/order?{q}") as r:
            d = orjson.loads(await r.read())
            if r.status == 400 and d.get("code") == -2013:
                return None
            if r.status != 200:
                raise ExchangeError(str(d))
            return Ack(coid, d["side"], round(float(d["executedQty"]) / self.lot), float(d["price"]), 0)

    async def position(self) -> int:
        q = self._signed({"omitZeroBalances": "true"})
        async with self.s.get(f"{self.BASE}/api/v3/account?{q}") as r:
            d = orjson.loads(await r.read())
        base = self.symbol.removesuffix("USDT")
        bal = sum(float(b["free"]) + float(b["locked"]) for b in d["balances"] if b["asset"] == base)
        return round(bal / self.lot)  # absolute; the OMS stores the offset at first start

    async def close(self) -> None:
        await self.s.close()


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="mock exchange")
    ap.add_argument("--port", type=int, default=8090)
    ap.add_argument("--latency-ms", type=float, default=0.0)
    a = ap.parse_args()
    web.run_app(MockExchange(a.latency_ms / 1000).app(), host="127.0.0.1", port=a.port, print=None)


if __name__ == "__main__":
    main()
