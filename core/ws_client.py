"""
Delta Exchange India native WebSocket client.
Two connections: public (ob_l2) and private (positions/orders/margins).
Auth: key-auth with HMAC-SHA256 signature (Oct 2025 protocol).
"""
from __future__ import annotations
import asyncio
import hashlib
import hmac
import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any, Callable

import websockets
from websockets.exceptions import ConnectionClosed

from config import cfg

logger = logging.getLogger(__name__)

MessageCallback = Callable[[dict[str, Any]], None]


def _ws_signature() -> tuple[str, str]:
    """Returns (timestamp, signature) for key-auth.
    Prehash = method + timestamp + path = "GET" + timestamp + "/live".
    Note: "/live" is the signature path only; WS URL has no /live suffix.
    """
    timestamp = str(int(time.time()))
    message = "GET" + timestamp + "/live"
    sig = hmac.new(
        cfg.API_SECRET.encode(),
        message.encode(),
        hashlib.sha256,
    ).hexdigest()
    return timestamp, sig


class DeltaWSClient:
    """
    Manages two persistent WebSocket connections to Delta India:
    - Public:  subscribes to ob_l2 for all configured symbols
    - Private: authenticates, subscribes to positions/orders/margins
    """

    def __init__(self) -> None:
        self._last_public: float = time.monotonic()
        self._last_private: float = time.monotonic()
        self._public_conn: websockets.WebSocketClientProtocol | None = None
        self._private_conn: websockets.WebSocketClientProtocol | None = None

    # ------------------------------------------------------------------
    # Liveness
    # ------------------------------------------------------------------

    def record_public(self) -> None:
        self._last_public = time.monotonic()

    def record_private(self) -> None:
        self._last_private = time.monotonic()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _open(self) -> websockets.WebSocketClientProtocol:
        return await websockets.connect(
            cfg.active_ws_url,
            ping_interval=cfg.WS_HEARTBEAT_INTERVAL,
            ping_timeout=cfg.WS_HEARTBEAT_INTERVAL,
            close_timeout=10,
        )

    async def _authenticate(self, ws: websockets.WebSocketClientProtocol) -> None:
        timestamp, sig = _ws_signature()
        auth_msg = {
            "type": "key-auth",
            "payload": {
                "api-key": cfg.API_KEY,
                "signature": sig,
                "timestamp": timestamp,
            },
        }
        await ws.send(json.dumps(auth_msg))
        raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
        resp = json.loads(raw)
        if resp.get("type") == "key-auth" and resp.get("success"):
            logger.info("Private auth success.")
        else:
            raise RuntimeError(f"WS auth failed: {resp}")

    async def _subscribe_public(self, ws: websockets.WebSocketClientProtocol) -> None:
        delta_symbols = [cfg.to_delta_symbol(s) for s in cfg.SYMBOLS]
        msg = {
            "type": "subscribe",
            "payload": {
                "channels": [{"name": "l2_orderbook", "symbols": delta_symbols}]
            },
        }
        await ws.send(json.dumps(msg))
        logger.info("Subscribed to l2_orderbook: %s", delta_symbols)

    async def _subscribe_private(self, ws: websockets.WebSocketClientProtocol) -> None:
        msg = {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {"name": "positions", "symbols": []},
                    {"name": "orders",    "symbols": []},
                    {"name": "margins",   "symbols": []},
                ]
            },
        }
        await ws.send(json.dumps(msg))
        logger.info("Subscribed to private channels: positions, orders, margins.")

    # ------------------------------------------------------------------
    # Public stream
    # ------------------------------------------------------------------

    async def listen_public(self) -> AsyncIterator[dict[str, Any]]:
        """
        Async generator. Yields parsed ob_l2 messages.
        Reconnects on disconnect with exponential backoff.
        """
        delay = cfg.WS_RECONNECT_BASE_DELAY
        while True:
            try:
                async with await self._open() as ws:
                    self._public_conn = ws
                    self.record_public()  # seed so a fresh stream isn't flagged stale
                    await self._subscribe_public(ws)
                    delay = cfg.WS_RECONNECT_BASE_DELAY
                    async for raw in ws:
                        self.record_public()
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if msg.get("type") in ("l2_orderbook", "subscriptions"):
                            yield msg
            except asyncio.CancelledError:
                logger.info("Public stream cancelled.")
                raise
            except ConnectionClosed as exc:
                logger.warning("Public WS closed: %s. Reconnecting in %.1fs.", exc, delay)
            except Exception as exc:
                logger.error("Public WS error: %s. Reconnecting in %.1fs.", exc, delay)
            finally:
                self._public_conn = None

            await asyncio.sleep(delay)
            delay = min(delay * 2, cfg.WS_RECONNECT_MAX_DELAY)

    # ------------------------------------------------------------------
    # Private stream
    # ------------------------------------------------------------------

    async def listen_private(self) -> AsyncIterator[dict[str, Any]]:
        """
        Async generator. Yields parsed positions/orders/margins messages.
        Reconnects and re-authenticates on disconnect.
        """
        delay = cfg.WS_RECONNECT_BASE_DELAY
        while True:
            try:
                async with await self._open() as ws:
                    self._private_conn = ws
                    self.record_private()  # seed so a fresh stream isn't flagged stale
                    await self._authenticate(ws)
                    await self._subscribe_private(ws)
                    delay = cfg.WS_RECONNECT_BASE_DELAY
                    async for raw in ws:
                        self.record_private()
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        msg_type = msg.get("type", "")
                        if msg_type in ("positions", "orders", "margins", "subscriptions"):
                            yield msg
            except asyncio.CancelledError:
                logger.info("Private stream cancelled.")
                raise
            except ConnectionClosed as exc:
                logger.warning("Private WS closed: %s. Reconnecting in %.1fs.", exc, delay)
            except Exception as exc:
                logger.error("Private WS error: %s. Reconnecting in %.1fs.", exc, delay)
            finally:
                self._private_conn = None

            await asyncio.sleep(delay)
            delay = min(delay * 2, cfg.WS_RECONNECT_MAX_DELAY)

    # ------------------------------------------------------------------
    # Heartbeat watchdog (external task)
    # ------------------------------------------------------------------

    async def heartbeat_watchdog(self) -> None:
        """Per-stream liveness. Closes a stalled connection so its listen loop
        exits and reconnects. Catches exchange-side data stalls that the
        websockets ping/pong (live TCP) would not."""
        threshold = cfg.WS_HEARTBEAT_INTERVAL * 2
        while True:
            await asyncio.sleep(cfg.WS_HEARTBEAT_INTERVAL)
            now = time.monotonic()
            public_age = now - self._last_public
            private_age = now - self._last_private
            if public_age > threshold and self._public_conn is not None:
                logger.warning("Public stream stale %.0fs — forcing reconnect.", public_age)
                await self._public_conn.close()
            if private_age > threshold and self._private_conn is not None:
                logger.warning("Private stream stale %.0fs — forcing reconnect.", private_age)
                await self._private_conn.close()
            logger.debug("Heartbeat: public %.1fs ago, private %.1fs ago.", public_age, private_age)
