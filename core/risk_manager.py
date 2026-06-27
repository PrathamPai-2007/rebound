"""
Risk management:
- Bracket order construction via REST
- Cross-margin equity guard → emergency market-close all positions
- Per-symbol SL/TP fill tracking via WS positions stream
"""
from __future__ import annotations
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from config import cfg
from core.rest_client import DeltaRestClient
from core.trade_tracker import TradeTracker
from strategy.signal_engine import Signal, SignalResult, SymbolEngine

logger = logging.getLogger(__name__)


@dataclass
class ActiveTrade:
    symbol: str           # internal e.g. BTC/USDT
    delta_symbol: str     # exchange e.g. BTCUSDT
    side: str             # "buy" | "sell"
    order_id: str
    entry_price: float
    sl_price: float
    tp_price: float
    size: int             # integer contracts
    opened_at: float = field(default_factory=time.time)


class RiskManager:
    def __init__(
        self,
        rest: DeltaRestClient,
        engines: dict[str, SymbolEngine],
        tracker: TradeTracker | None = None,
    ) -> None:
        self._rest = rest
        self._engines = engines
        self._tracker = tracker
        self._trades: dict[str, ActiveTrade] = {}   # internal symbol → ActiveTrade
        self._realised_pnl: dict[str, float] = {}   # delta_symbol → latest realised_pnl
        self._initial_equity: float | None = None
        self._emergency_triggered: bool = False
        self._cooldown_tasks: set[asyncio.Task] = set()  # hold refs so they aren't GC'd
        # Equity tracking: equity = wallet balance + unrealized PnL
        self._wallet_balance: float = 0.0
        self._unrealized: float = 0.0
        self._current_equity: float = 0.0
        self._margins_has_upnl: bool = False  # margins stream is authoritative for upnl

    @property
    def current_equity(self) -> float:
        if cfg.DRY_RUN and self._current_equity <= 0:
            return cfg.PAPER_EQUITY
        return self._current_equity

    # ------------------------------------------------------------------
    # Order sizing
    # ------------------------------------------------------------------

    def _calc_size(self, symbol: str, equity: float, entry: float, sl: float) -> int:
        """Risk-based sizing in integer contracts, with a margin sanity check."""
        risk_usd = equity * (cfg.for_symbol(symbol, 'RISK_PER_TRADE_PCT') / 100.0)
        sl_distance = abs(entry - sl)
        if sl_distance == 0:
            return 0
        coin_qty = risk_usd / sl_distance          # coin units risked (loss == risk_usd)
        cval = self._engines[symbol].contract_value
        if cval <= 0:
            return 0
        size = max(1, round(coin_qty / cval))      # integer contracts
        # Leverage / margin sanity: required margin must not exceed equity
        notional = entry * size * cval
        leverage = cfg.LEVERAGE[symbol]
        req_margin = notional / leverage
        if req_margin > equity:
            logger.warning(
                "[%s] size=%d needs margin %.2f > equity %.2f — skipping.",
                symbol, size, req_margin, equity,
            )
            return 0
        return size

    def _round_to_tick(self, price: float, tick_size: float) -> float:
        if tick_size <= 0:
            return price
        return round(round(price / tick_size) * tick_size, 8)

    # ------------------------------------------------------------------
    # Bracket order submission
    # ------------------------------------------------------------------

    async def place_bracket(self, sig: SignalResult) -> bool:
        symbol = sig.symbol
        if symbol in self._trades:
            logger.warning("[%s] Already in trade, skipping signal.", symbol)
            return False

        equity = self.current_equity
        if equity <= 0:
            logger.warning("[%s] Equity unknown/zero, skipping signal.", symbol)
            return False

        side = "buy" if sig.signal == Signal.LONG else "sell"
        size = self._calc_size(symbol, equity, sig.entry_price, sig.sl_price)
        if size <= 0:
            logger.error("[%s] Computed size=0, skipping.", symbol)
            return False

        delta_symbol = cfg.to_delta_symbol(symbol)

        # Bracket exits are stop-limit; offset the limit past the trigger so a
        # fast move still fills (acts like a market exit). TP limit = trigger
        # (favorable side, always fills).
        tick_size = self._engines[symbol].tick_size
        buf = cfg.BRACKET_SLIPPAGE_TICKS * tick_size

        entry_price = self._round_to_tick(sig.entry_price, tick_size)
        sl_trigger = self._round_to_tick(sig.sl_price, tick_size)
        if side == "buy":          # long: exits are sells, SL triggers below
            sl_limit = self._round_to_tick(sig.sl_price - buf, tick_size)
        else:                      # short: exits are buys, SL triggers above
            sl_limit = self._round_to_tick(sig.sl_price + buf, tick_size)

        tp_trigger = self._round_to_tick(sig.tp_price, tick_size)
        tp_limit = tp_trigger

        if cfg.DRY_RUN:
            logger.info(
                "[DRY_RUN][%s] Would place bracket | side=%s size=%d entry=%.4f "
                "sl=%.4f/%.4f tp=%.4f/%.4f",
                symbol, side, size, entry_price,
                sl_trigger, sl_limit, tp_trigger, tp_limit,
            )
            return True

        try:
            result = await self._rest.place_bracket(
                delta_symbol=delta_symbol,
                side=side,
                size=size,
                sl_trigger=sl_trigger,
                sl_limit=sl_limit,
                tp_trigger=tp_trigger,
                tp_limit=tp_limit,
                trigger_method=cfg.BRACKET_TRIGGER_METHOD,
            )
            order_id = str(result.get("id", ""))
            if not order_id:
                raise RuntimeError(f"Bracket order response missing id: {result}")
            self._trades[symbol] = ActiveTrade(
                symbol=symbol,
                delta_symbol=delta_symbol,
                side=side,
                order_id=order_id,
                entry_price=entry_price,
                sl_price=sl_trigger,
                tp_price=tp_trigger,
                size=size,
            )
            return True
        except Exception as exc:
            logger.error("[%s] Bracket order failed: %s", symbol, exc)
            if symbol in self._engines:
                self._engines[symbol].enter_cooldown()
                task = asyncio.create_task(self._schedule_cooldown_exit(symbol, delay=cfg.COOLDOWN_SEC))
                self._cooldown_tasks.add(task)
                task.add_done_callback(self._cooldown_tasks.discard)
            return False

    # ------------------------------------------------------------------
    # Private stream handlers
    # ------------------------------------------------------------------

    async def on_position_update(self, msg: dict[str, Any]) -> None:
        """Called when positions WS message arrives."""
        positions = msg.get("result", [])
        if not isinstance(positions, list):
            return
        # Track latest realised_pnl per symbol so we can attribute a P&L to a
        # position when it closes (the close snapshot has size=0).
        for p in positions:
            sym = p.get("product_symbol")
            if sym is not None and p.get("realised_pnl") is not None:
                try:
                    self._realised_pnl[sym] = float(p["realised_pnl"])
                except (ValueError, TypeError):
                    pass
        await self._check_sl_tp_fills(positions)
        # Fallback unrealized source: only when margins stream doesn't carry upnl.
        if not self._margins_has_upnl:
            self._unrealized = sum(
                float(p.get("unrealised_pnl", 0) or 0) for p in positions
            )
            await self._recompute_equity()

    async def on_balance_update(self, msg: dict[str, Any]) -> None:
        """Called when margins WS message arrives."""
        result = msg.get("result", {})
        # Delta margins message: result may be a list (snapshot) or dict
        if isinstance(result, list):
            result = result[0] if result else {}
        if not isinstance(result, dict):
            return
        # result.balance or result.available_balance
        balance_str = result.get("balance")
        if balance_str is None:
            balance_str = result.get("available_balance", "0")
        try:
            self._wallet_balance = float(balance_str)
        except (ValueError, TypeError):
            return

        # margins is the authoritative aggregate for unrealized PnL when present
        if "unrealised_pnl" in result and result["unrealised_pnl"] is not None:
            self._margins_has_upnl = True
            try:
                self._unrealized = float(result["unrealised_pnl"])
            except (ValueError, TypeError):
                pass

        await self._recompute_equity()

    async def _recompute_equity(self) -> None:
        """equity = wallet balance + unrealized PnL. Drives the drawdown guard."""
        self._current_equity = self._wallet_balance + self._unrealized
        if self._initial_equity is None and self._current_equity > 0:
            self._initial_equity = self._current_equity
            logger.info(
                "Initial equity recorded: %.4f USDT (balance=%.4f upnl=%.4f)",
                self._initial_equity, self._wallet_balance, self._unrealized,
            )
        await self._check_equity_guard(self._current_equity)

    async def on_order_update(self, msg: dict[str, Any]) -> None:
        """Called when orders WS message arrives. Used for fill logging.
        Delta `orders` channel delivers result as a list of order dicts
        (snapshot) or a single dict (incremental); handle both.
        """
        result = msg.get("result", {})
        orders = result if isinstance(result, list) else [result]
        for order in orders:
            if not isinstance(order, dict):
                continue
            order_id = order.get("id", "?")
            state = order.get("state", "?")
            symbol = order.get("product_symbol", "?")
            logger.info("[%s] Order update | id=%s state=%s", symbol, order_id, state)

    # ------------------------------------------------------------------
    # Equity guard
    # ------------------------------------------------------------------

    async def _check_equity_guard(self, current_equity: float) -> None:
        if self._emergency_triggered or self._initial_equity is None:
            return
        if current_equity <= 0:
            logger.critical(
                "EQUITY GUARD: equity %.4f <= 0. Emergency close ALL.", current_equity
            )
            self._emergency_triggered = True
            await self._emergency_close_all()
            return
        drawdown_pct = (1.0 - current_equity / self._initial_equity) * 100.0
        if drawdown_pct >= cfg.MAX_EQUITY_DRAWDOWN_PCT:
            logger.critical(
                "EQUITY GUARD: drawdown %.2f%% >= threshold %.2f%%. Emergency close ALL.",
                drawdown_pct, cfg.MAX_EQUITY_DRAWDOWN_PCT,
            )
            self._emergency_triggered = True
            await self._emergency_close_all()

    async def _emergency_close_all(self) -> None:
        if cfg.DRY_RUN:
            logger.critical("[DRY_RUN] Emergency close all — skipped in dry run.")
            return
        trade_items = list(self._trades.items())
        tasks = [
            self._rest.close_position(trade.delta_symbol, trade.side, trade.size)
            for _, trade in trade_items
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for (symbol, _), result in zip(trade_items, results):
            if isinstance(result, Exception):
                logger.error("[%s] Emergency close failed: %s", symbol, result)
            else:
                logger.critical("[%s] Emergency close executed: %s", symbol, result.get("id"))
        self._trades.clear()

    # ------------------------------------------------------------------
    # SL/TP fill detection
    # ------------------------------------------------------------------

    async def _check_sl_tp_fills(self, positions: list[dict[str, Any]]) -> None:
        # Map updated symbol to its new size
        updated_positions = {
            p["product_symbol"]: float(p["size"])
            for p in positions
            if "product_symbol" in p and "size" in p
        }
        for symbol, trade in list(self._trades.items()):
            if trade.delta_symbol in updated_positions:
                size = updated_positions[trade.delta_symbol]
                if size == 0:
                    logger.info("[%s] Position closed (SL or TP filled). Entering cooldown.", symbol)
                    if self._tracker is not None:
                        self._tracker.record_live(
                            trade,
                            realised_pnl=self._realised_pnl.get(trade.delta_symbol),
                            contract_value=self._engines[symbol].contract_value,
                        )
                    if symbol in self._engines:
                        self._engines[symbol].enter_cooldown()
                    del self._trades[symbol]
                    task = asyncio.create_task(self._schedule_cooldown_exit(symbol, delay=cfg.COOLDOWN_SEC))
                    self._cooldown_tasks.add(task)
                    task.add_done_callback(self._cooldown_tasks.discard)

    async def _schedule_cooldown_exit(self, symbol: str, delay: float) -> None:
        await asyncio.sleep(delay)
        if symbol in self._engines:
            self._engines[symbol].exit_cooldown()
