"""
Rolling VWAP + SD Bands + RSI(7) computed from live tick deques.
All math is pure numpy — no pandas dependency.
"""
from __future__ import annotations
from collections import deque
from dataclasses import dataclass, field
import numpy as np

from config import cfg


@dataclass
class Tick:
    price: float
    volume: float
    timestamp: float   # unix ms


@dataclass
class Candle:
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: float


@dataclass
class VWAPResult:
    vwap: float
    upper1: float   # VWAP + 1 SD
    lower1: float   # VWAP - 1 SD
    upper2: float   # VWAP + 2 SD
    lower2: float   # VWAP - 2 SD


class VWAPIndicator:
    """Volume-weighted average price with rolling SD bands."""

    def __init__(self, window: int) -> None:
        self.window = window
        self._ticks: deque[Tick] = deque(maxlen=window)

    def update(self, tick: Tick) -> VWAPResult | None:
        self._ticks.append(tick)
        if len(self._ticks) < 5:
            return None

        prices = np.array([t.price for t in self._ticks])
        volumes = np.array([t.volume for t in self._ticks])
        total_vol = volumes.sum()
        if total_vol == 0:
            return None

        vwap = float(np.dot(prices, volumes) / total_vol)
        # Volume-weighted variance
        variance = float(np.dot(volumes, (prices - vwap) ** 2) / total_vol)
        sd = float(np.sqrt(max(0.0, variance)))

        mult = cfg.VWAP_BAND_SD
        return VWAPResult(
            vwap=vwap,
            upper1=vwap + sd,
            lower1=vwap - sd,
            upper2=vwap + mult * sd,
            lower2=vwap - mult * sd,
        )


class RSIIndicator:
    """Wilder-smoothed RSI using incremental update (no full recalc per tick)."""

    def __init__(self, period: int = 7) -> None:
        self.period = period
        self._closes: deque[float] = deque(maxlen=period + 1)
        self._avg_gain: float | None = None
        self._avg_loss: float | None = None
        self._seed_count: int = 0

    def update(self, close: float) -> float | None:
        self._closes.append(close)
        if len(self._closes) < 2:
            return None

        delta = self._closes[-1] - self._closes[-2]
        gain = max(delta, 0.0)
        loss = abs(min(delta, 0.0))

        if self._avg_gain is None:
            self._seed_count += 1
            if self._seed_count < self.period:
                return None
            # First average: simple mean over first `period` deltas
            closes_list = list(self._closes)
            deltas = np.diff(closes_list[-self.period - 1:])
            gains = np.where(deltas > 0, deltas, 0.0)
            losses = np.where(deltas < 0, -deltas, 0.0)
            self._avg_gain = float(gains.mean())
            self._avg_loss = float(losses.mean())
        else:
            # Wilder smoothing
            self._avg_gain = (self._avg_gain * (self.period - 1) + gain) / self.period
            self._avg_loss = (self._avg_loss * (self.period - 1) + loss) / self.period

        if self._avg_loss == 0:
            return 100.0
        rs = self._avg_gain / self._avg_loss
        return 100.0 - (100.0 / (1.0 + rs))


@dataclass
class CandleBuffer:
    """Aggregates ticks into OHLCV candles for wick-rejection check."""
    _candles: deque[Candle] = field(default_factory=lambda: deque(maxlen=3))
    _open: float | None = None
    _high: float = -np.inf
    _low: float = np.inf
    _close: float | None = None
    _volume: float = 0.0
    _ts_open: float = 0.0
    _candle_ms: int = 60_000   # 1-minute candle

    def feed(self, tick: Tick) -> Candle | None:
        """Returns completed candle when new candle starts."""
        bucket = int(tick.timestamp // self._candle_ms) * self._candle_ms

        if self._open is None:
            self._ts_open = bucket
            self._open = tick.price

        if bucket != self._ts_open:
            # Candle closed
            closed = Candle(
                open=self._open,
                high=self._high,
                low=self._low,
                close=self._close,
                volume=self._volume,
                timestamp=self._ts_open,
            )
            self._candles.append(closed)
            # Reset for new candle
            self._ts_open = bucket
            self._open = tick.price
            self._high = tick.price
            self._low = tick.price
            self._close = tick.price
            self._volume = tick.volume
            return closed

        self._high = max(self._high, tick.price)
        self._low = min(self._low, tick.price)
        self._close = tick.price
        self._volume += tick.volume
        return None

    @property
    def last(self) -> Candle | None:
        return self._candles[-1] if self._candles else None

    def has_bullish_wick_rejection(self) -> bool:
        """Lower wick >= ratio x body, close in upper half of candle range."""
        c = self.last
        if c is None:
            return False
        body = abs(c.close - c.open)
        if body < 1e-8:  # doji — ambiguous direction, skip
            return False
        lower_wick = min(c.open, c.close) - c.low
        candle_range = c.high - c.low
        if candle_range == 0:
            return False
        return (lower_wick >= cfg.WICK_RATIO * body) and (c.close > c.low + candle_range * 0.5)

    def has_bearish_wick_rejection(self) -> bool:
        """Upper wick >= ratio x body, close in lower half of candle range."""
        c = self.last
        if c is None:
            return False
        body = abs(c.close - c.open)
        if body < 1e-8:  # doji — ambiguous direction, skip
            return False
        upper_wick = c.high - max(c.open, c.close)
        candle_range = c.high - c.low
        if candle_range == 0:
            return False
        return (upper_wick >= cfg.WICK_RATIO * body) and (c.close < c.low + candle_range * 0.5)
