"""
Delta Exchange India REST client for order operations.
WebSocket handles data streams; all order I/O goes through here.
"""
from __future__ import annotations
import hashlib
import hmac
import json
import logging
import time
from typing import Any

import aiohttp

from config import cfg

logger = logging.getLogger(__name__)


def _sign(method: str, path: str, body: str = "") -> dict[str, str]:
    timestamp = str(int(time.time()))
    prehash = method + timestamp + path + body
    sig = hmac.new(
        cfg.API_SECRET.encode(),
        prehash.encode(),
        hashlib.sha256,
    ).hexdigest()
    return {
        "api-key": cfg.API_KEY,
        "signature": sig,
        "timestamp": timestamp,
        "Content-Type": "application/json",
    }


class DeltaRestClient:
    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(base_url=cfg.active_rest_url)
        logger.info("REST client started. Base: %s", cfg.active_rest_url)

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    def _s(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("REST client not started — call start() first")
        return self._session

    @staticmethod
    async def _read(resp: aiohttp.ClientResponse) -> dict[str, Any]:
        """Parse a Delta response, surfacing non-JSON/error bodies (5xx, HTML,
        CloudFront pages) instead of letting resp.json() raise opaquely."""
        text = await resp.text()
        try:
            return json.loads(text)
        except (ValueError, json.JSONDecodeError):
            raise RuntimeError(f"Non-JSON response (HTTP {resp.status}): {text[:200]}")

    # ------------------------------------------------------------------
    # Market info
    # ------------------------------------------------------------------

    async def get_products(self) -> list[dict[str, Any]]:
        # /v2/products is public — no auth headers needed.
        async with self._s().get("/v2/products") as resp:
            data = await self._read(resp)
            return data.get("result", [])

    # ------------------------------------------------------------------
    # Leverage
    # ------------------------------------------------------------------

    async def set_leverage(self, delta_symbol: str, leverage: int) -> None:
        path = "/v2/products/leverage"
        body = json.dumps({"product_symbol": delta_symbol, "leverage": leverage})
        headers = _sign("POST", path, body)
        async with self._s().post(path, data=body, headers=headers) as resp:
            data = await self._read(resp)
            if data.get("success"):
                logger.info("[%s] Leverage set to %dx", delta_symbol, leverage)
            else:
                raise RuntimeError(f"[{delta_symbol}] set_leverage failed: {data}")

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------

    async def place_order(
        self,
        delta_symbol: str,
        side: str,
        size: float,
        order_type: str = "market_order",
        limit_price: float | None = None,
        stop_price: float | None = None,
        reduce_only: bool = False,
    ) -> dict[str, Any]:
        path = "/v2/orders"
        payload: dict[str, Any] = {
            "product_symbol": delta_symbol,
            "side": side,
            "size": size,
            "order_type": order_type,
            "reduce_only": reduce_only,
        }
        if limit_price is not None:
            payload["limit_price"] = str(limit_price)
        if stop_price is not None:
            payload["stop_price"] = str(stop_price)

        body = json.dumps(payload)
        headers = _sign("POST", path, body)
        t0 = time.monotonic()
        async with self._s().post(path, data=body, headers=headers) as resp:
            data = await self._read(resp)
            latency_ms = (time.monotonic() - t0) * 1000
            if data.get("success"):
                order_id = data["result"]["id"]
                logger.info(
                    "[%s] Order placed | id=%s type=%s side=%s size=%.6f latency=%.1fms",
                    delta_symbol, order_id, order_type, side, size, latency_ms,
                )
                return data["result"]
            else:
                logger.error("[%s] Order failed: %s", delta_symbol, data)
                raise RuntimeError(f"Order placement failed: {data}")

    async def place_bracket(
        self,
        delta_symbol: str,
        side: str,
        size: int,
        sl_trigger: float,
        sl_limit: float,
        tp_trigger: float,
        tp_limit: float,
        trigger_method: str = "last_traded_price",
    ) -> dict[str, Any]:
        """
        Places a market entry on POST /v2/orders with bracket SL/TP attached;
        Delta auto-creates the bracket exits on fill. Bracket exits are
        stop-limit, so the limit prices are offset past the triggers by the
        caller so they fill like a market on a fast move.
        """
        path = "/v2/orders"
        payload = {
            "product_symbol": delta_symbol,
            "side": side,
            "size": size,
            "order_type": "market_order",
            "bracket_stop_loss_price": str(sl_trigger),
            "bracket_stop_loss_limit_price": str(sl_limit),
            "bracket_take_profit_price": str(tp_trigger),
            "bracket_take_profit_limit_price": str(tp_limit),
            "bracket_stop_trigger_method": trigger_method,
        }
        body = json.dumps(payload)
        headers = _sign("POST", path, body)
        t0 = time.monotonic()
        async with self._s().post(path, data=body, headers=headers) as resp:
            data = await self._read(resp)
            latency_ms = (time.monotonic() - t0) * 1000
            if data.get("success"):
                order_id = data["result"]["id"]
                logger.info(
                    "[%s] Bracket order | id=%s side=%s size=%d sl=%.4f/%.4f tp=%.4f/%.4f latency=%.1fms",
                    delta_symbol, order_id, side, size,
                    sl_trigger, sl_limit, tp_trigger, tp_limit, latency_ms,
                )
                return data["result"]
            else:
                logger.error("[%s] Bracket order failed: %s", delta_symbol, data)
                raise RuntimeError(f"Bracket order failed: {data}")

    async def cancel_order(self, order_id: str, delta_symbol: str) -> None:
        path = "/v2/orders"
        payload = {"id": order_id, "product_symbol": delta_symbol}
        body = json.dumps(payload)
        headers = _sign("DELETE", path, body)
        async with self._s().delete(path, data=body, headers=headers) as resp:
            data = await self._read(resp)
            if data.get("success"):
                logger.info("[%s] Order %s cancelled.", delta_symbol, order_id)
            else:
                raise RuntimeError(f"[{delta_symbol}] cancel_order {order_id} failed: {data}")

    async def close_position(self, delta_symbol: str, side: str, size: float) -> dict[str, Any]:
        close_side = "sell" if side == "buy" else "buy"
        return await self.place_order(
            delta_symbol=delta_symbol,
            side=close_side,
            size=size,
            order_type="market_order",
            reduce_only=True,
        )
