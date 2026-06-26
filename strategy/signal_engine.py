"""
Per-symbol signal engine. Holds indicator state, evaluates entry conditions.
Thread-safe within asyncio single-thread model.
"""
from __future__ import annotations
import logging
from dataclasses import dataclass
from enum import Enum

from config import cfg
from strategy.indicators import CandleBuffer, RSIIndicator, Tick, VWAPIndicator, VWAPResult

logger = logging.getLogger(__name__)


class Signal(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NONE = "NONE"


@dataclass
class SignalResult:
    signal: Signal
    entry_price: float
    sl_price: float
    tp_price: float
    vwap: VWAPResult
    rsi: float
    symbol: str
    gates: tuple[bool, bool, bool] = (True, True, True)  # (band, rsi, wick)


class SymbolEngine:
    """Stateful per-symbol evaluator."""

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self._vwap = VWAPIndicator(window=cfg.VWAP_WINDOW)
        self._rsi = RSIIndicator(period=cfg.RSI_PERIOD)
        self._candles = CandleBuffer()
        self._in_cooldown: bool = False   # standdown after SL hit
        self._tick_size: float | None = None  # set externally after market info fetch
        self._contract_value: float = 1.0  # coin per contract, set after market info fetch
        # Cumulative gate pass counters for the periodic summary / binding-gate analysis.
        self._gate_counts: dict[str, int] = {
            "candles": 0, "band": 0, "rsi": 0, "wick": 0, "signals": 0,
        }

    @property
    def gate_stats(self) -> dict[str, int]:
        return dict(self._gate_counts)

    def set_tick_size(self, tick_size: float) -> None:
        self._tick_size = tick_size

    @property
    def tick_size(self) -> float:
        return self._tick_size or 0.01

    def set_contract_value(self, contract_value: float) -> None:
        self._contract_value = contract_value

    @property
    def contract_value(self) -> float:
        return self._contract_value

    def enter_cooldown(self) -> None:
        self._in_cooldown = True
        logger.warning("[%s] Entering cooldown after SL hit.", self.symbol)

    def exit_cooldown(self) -> None:
        self._in_cooldown = False
        logger.info("[%s] Cooldown cleared. Watching for new setup.", self.symbol)

    def on_tick(self, tick: Tick) -> SignalResult | None:
        if self._in_cooldown:
            return None

        vwap_result = self._vwap.update(tick)
        closed_candle = self._candles.feed(tick)

        if not closed_candle:
            return None

        rsi_val = self._rsi.update(closed_candle.close)

        if vwap_result is None or rsi_val is None:
            return None

        # Evaluate the three entry gates as named booleans for each direction.
        long_band = closed_candle.close <= vwap_result.lower2
        long_rsi = rsi_val < cfg.RSI_OVERSOLD
        long_wick = self._candles.has_bullish_wick_rejection()
        short_band = closed_candle.close >= vwap_result.upper2
        short_rsi = rsi_val > cfg.RSI_OVERBOUGHT
        short_wick = self._candles.has_bearish_wick_rejection()

        # Gate stats
        band = long_band or short_band
        rsi_pass = long_rsi or short_rsi
        wick = long_wick or short_wick
        
        self._gate_counts["candles"] += 1
        self._gate_counts["band"] += int(band)
        self._gate_counts["rsi"] += int(rsi_pass)
        self._gate_counts["wick"] += int(wick)
        
        if band:  # near-setup — worth a breakdown line
            logger.debug(
                "[%s] gates band=%s rsi=%s wick=%s rsi_val=%.1f",
                self.symbol, band, rsi_pass, wick, rsi_val,
            )

        tick_size = self.tick_size
        sl_offset = cfg.SL_TICKS * tick_size

        if long_band and long_rsi and long_wick:
            entry = closed_candle.close
            sl = closed_candle.low - sl_offset
            tp = vwap_result.upper1
            logger.info(
                "[%s] LONG signal | price=%.4f vwap=%.4f lower2=%.4f rsi=%.2f sl=%.4f tp=%.4f",
                self.symbol, entry, vwap_result.vwap, vwap_result.lower2, rsi_val, sl, tp,
            )
            self._gate_counts["signals"] += 1
            return SignalResult(
                Signal.LONG, entry, sl, tp, vwap_result, rsi_val, self.symbol,
                gates=(long_band, long_rsi, long_wick),
            )

        if short_band and short_rsi and short_wick:
            entry = closed_candle.close
            sl = closed_candle.high + sl_offset
            tp = vwap_result.lower1
            logger.info(
                "[%s] SHORT signal | price=%.4f vwap=%.4f upper2=%.4f rsi=%.2f sl=%.4f tp=%.4f",
                self.symbol, entry, vwap_result.vwap, vwap_result.upper2, rsi_val, sl, tp,
            )
            self._gate_counts["signals"] += 1
            return SignalResult(
                Signal.SHORT, entry, sl, tp, vwap_result, rsi_val, self.symbol,
                gates=(short_band, short_rsi, short_wick),
            )

        return None
