"""
Main engine. Spawns concurrent async tasks:
- 5 tasks routing ob_l2 ticks to per-symbol SymbolEngines (from shared public WS)
- 1 task routing private WS messages (positions/orders/margins) to RiskManager
- 1 heartbeat watchdog
"""
from __future__ import annotations
import asyncio
import datetime
import logging
import logging.handlers
import sys
import time
from typing import Any

from config import cfg
from core.ws_client import DeltaWSClient
from core.rest_client import DeltaRestClient
from core.risk_manager import RiskManager
from core.trade_tracker import TradeTracker
from strategy.indicators import Tick
from strategy.signal_engine import SymbolEngine


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging() -> None:
    level = getattr(logging, cfg.LOG_LEVEL.upper(), logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(level)

    fh = logging.handlers.RotatingFileHandler(
        cfg.LOG_FILE, maxBytes=50 * 1024 * 1024, backupCount=5
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Market info: tick sizes via REST
# ---------------------------------------------------------------------------

async def _load_tick_sizes(
    rest: DeltaRestClient,
    engines: dict[str, SymbolEngine],
) -> None:
    try:
        products = await rest.get_products()
    except Exception as exc:
        logger.warning("Could not fetch products (tick sizes default to 0.01): %s", exc)
        return

    # Debug: log all perpetual contract symbols so we can verify SYMBOL_MAP
    perp_symbols = [p["symbol"] for p in products if p.get("contract_type") == "perpetual_futures"]
    logger.debug("Available perpetuals: %s", perp_symbols)
    if not perp_symbols:
        all_symbols = [p.get("symbol") for p in products[:20]]
        logger.warning("No perpetuals found — first 20 product symbols: %s", all_symbols)

    # Build lookup by symbol
    by_symbol = {p["symbol"]: p for p in products if "symbol" in p}
    for internal_sym in cfg.SYMBOLS:
        delta_sym = cfg.to_delta_symbol(internal_sym)
        product = by_symbol.get(delta_sym)
        if product:
            tick_size = float(product.get("tick_size", 0.01))
            contract_value = float(product.get("contract_value", 1.0))
            engines[internal_sym].set_tick_size(tick_size)
            engines[internal_sym].set_contract_value(contract_value)
            logger.info(
                "[%s] tick_size=%.6f contract_value=%.6f leverage=%dx",
                internal_sym, tick_size, contract_value, cfg.LEVERAGE[internal_sym],
            )
        else:
            logger.warning("[%s] Product '%s' not found in exchange products — using tick_size=0.01", internal_sym, delta_sym)


# ---------------------------------------------------------------------------
# Public order book stream → all symbols multiplexed on one connection
# ---------------------------------------------------------------------------

async def _stream_public(
    ws_client: DeltaWSClient,
    rest: DeltaRestClient,
    engines: dict[str, SymbolEngine],
    risk_mgr: RiskManager,
    tracker: TradeTracker,
) -> None:
    """
    Single task that reads the shared public WS stream.
    Routes each ob_l2 message to the correct SymbolEngine by symbol.
    """
    async for msg in ws_client.listen_public():
        if msg.get("type") != "l2_orderbook":
            continue

        delta_symbol = msg.get("symbol", "")
        internal_symbol = cfg.from_delta_symbol(delta_symbol)
        if internal_symbol not in engines:
            continue

        buys = msg.get("buy", [])
        sells = msg.get("sell", [])
        if not buys or not sells:
            continue

        try:
            best_bid = float(buys[0]["price"])
            best_ask = float(sells[0]["price"])
            if "quantity" not in buys[0] or "quantity" not in sells[0]:
                logger.warning("[%s] ob_l2 missing quantity field — skipping tick", internal_symbol)
                continue
            bid_vol = float(buys[0]["quantity"])
            ask_vol = float(sells[0]["quantity"])
        except (KeyError, ValueError, IndexError):
            continue

        mid = (best_bid + best_ask) / 2.0
        vol = bid_vol + ask_vol
        raw_ts = msg.get("timestamp")
        if isinstance(raw_ts, str):
            try:
                dt = datetime.datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                ts_ms = dt.timestamp() * 1000
            except ValueError:
                ts_ms = time.time() * 1000
        else:
            ts_ms = float(raw_ts) if raw_ts is not None else time.time() * 1000

        tick = Tick(price=mid, volume=vol, timestamp=ts_ms)
        engine = engines[internal_symbol]
        try:
            # Resolve any open paper trade for this symbol against the new price.
            tracker.on_tick(internal_symbol, mid, ts_ms)
            signal = engine.on_tick(tick)
            if signal is not None:
                placed = await risk_mgr.place_bracket(signal)
                if placed:
                    tracker.open_paper(signal, signal.gates)
        except Exception:
            # A handler bug must not kill the whole stream task.
            logger.exception("[%s] Error processing tick", internal_symbol)


# ---------------------------------------------------------------------------
# Private stream → positions / orders / margins
# ---------------------------------------------------------------------------

async def _stream_private(
    ws_client: DeltaWSClient,
    risk_mgr: RiskManager,
) -> None:
    async for msg in ws_client.listen_private():
        msg_type = msg.get("type", "")
        try:
            if msg_type == "positions":
                await risk_mgr.on_position_update(msg)
            elif msg_type == "margins":
                await risk_mgr.on_balance_update(msg)
            elif msg_type == "orders":
                await risk_mgr.on_order_update(msg)
        except Exception:
            # A handler bug must not kill the whole stream task.
            logger.exception("Error processing private msg type=%s", msg_type)


# ---------------------------------------------------------------------------
# Periodic summary — gate counts, signals, paper/live outcomes, equity
# ---------------------------------------------------------------------------

async def _summary_loop(
    engines: dict[str, SymbolEngine],
    tracker: TradeTracker,
    risk_mgr: RiskManager,
) -> None:
    prev: dict[str, dict[str, int]] = {s: {} for s in engines}
    while True:
        await asyncio.sleep(cfg.SUMMARY_INTERVAL)
        parts: list[str] = []
        for sym, engine in engines.items():
            cur = engine.gate_stats
            p = prev[sym]
            d = {k: cur[k] - p.get(k, 0) for k in cur}
            prev[sym] = cur
            if d.get("candles", 0) == 0 and d.get("band", 0) == 0:
                continue
            parts.append(
                f"{sym}[c={d['candles']} band={d['band']} rsi={d['rsi']} "
                f"wick={d['wick']} sig={d['signals']}]"
            )
        logger.info(
            "SUMMARY | %s | paper W/L=%d/%d (%.0f%%) open=%d | live W/L=%d/%d | equity=%.2f",
            " ".join(parts) if parts else "(no closed candles)",
            tracker.paper_wins, tracker.paper_losses, tracker.win_rate(),
            tracker.open_paper_count, tracker.live_wins, tracker.live_losses,
            risk_mgr.current_equity,
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    _setup_logging()

    if cfg.DRY_RUN:
        logger.warning("=== DRY RUN MODE: no real orders will be placed ===")
    if cfg.USE_TESTNET:
        logger.warning("=== TESTNET MODE ===")

    try:
        cfg.validate()
    except EnvironmentError as exc:
        logger.critical("Config error: %s", exc)
        sys.exit(1)

    rest = DeltaRestClient()
    await rest.start()

    engines: dict[str, SymbolEngine] = {s: SymbolEngine(s) for s in cfg.SYMBOLS}
    await _load_tick_sizes(rest, engines)

    tracker = TradeTracker()
    risk_mgr = RiskManager(rest, engines, tracker)

    if not cfg.DRY_RUN:
        for symbol in cfg.SYMBOLS:
            delta_sym = cfg.to_delta_symbol(symbol)
            try:
                await rest.set_leverage(delta_sym, cfg.LEVERAGE[symbol])
            except Exception as exc:
                logger.warning("[%s] set_leverage failed: %s", symbol, exc)

    ws_client = DeltaWSClient()

    tasks: list[asyncio.Task] = [
        asyncio.create_task(_stream_public(ws_client, rest, engines, risk_mgr, tracker), name="public_ob"),
        asyncio.create_task(ws_client.heartbeat_watchdog(), name="heartbeat"),
        asyncio.create_task(_summary_loop(engines, tracker, risk_mgr), name="summary"),
    ]
    if not cfg.DRY_RUN:
        tasks.append(asyncio.create_task(_stream_private(ws_client, risk_mgr), name="private"))

    logger.info("Bot running. WS: %s | Symbols: %s", cfg.active_ws_url, cfg.SYMBOLS)

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutdown requested.")
    finally:
        for t in tasks:
            t.cancel()
        try:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("Tasks did not exit within 10s — forcing shutdown.")
        await rest.close()
        logger.info("Bot stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
