"""
Historical backtest for Rebound strategy.

Replays parquet candle data through SymbolEngine (same code as live bot) and
writes outcomes to trades.csv with mode=backtest, readable by analyse.py.

Usage:
    python backtest.py                              # all 5 symbols, 1m, data/
    python backtest.py --symbols BTCUSD,ETHUSD
    python backtest.py --resolution 5m
    python backtest.py --out backtest_trades.csv
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

from config import cfg
from core.trade_tracker import CSV_FIELDS
from strategy.indicators import Tick
from strategy.signal_engine import Signal, SignalResult, SymbolEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

ALL_SYMBOLS = ["BTCUSD", "SOLUSD", "ETHUSD", "XAUTUSD", "BNBUSD"]


@dataclass
class _Trade:
    symbol: str
    side: str          # "buy" | "sell"
    entry: float
    sl: float
    tp: float
    risk: float
    opened_ts: int     # unix ms
    rsi_at_entry: float
    vwap_at_entry: float
    gate_band: bool
    gate_rsi: bool
    gate_wick: bool
    mfe_r: float = 0.0
    mae_r: float = 0.0


def _synth_ticks(candle: dict) -> list[Tick]:
    """4 ticks from OHLCV candle that reconstruct the same candle in CandleBuffer.

    Order: open (first), high, low, close (last) — CandleBuffer uses max/min for
    high/low so order of middle ticks doesn't matter; first/last determine open/close.
    All timestamps land in the same 60s bucket so the candle stays open until
    the next candle's first tick triggers the bucket close.
    """
    ts = int(candle["time"]) * 1000
    o, h, l, c, v = candle["open"], candle["high"], candle["low"], candle["close"], candle["volume"]
    q = v / 4.0
    return [
        Tick(price=o, volume=q, timestamp=ts),
        Tick(price=h, volume=q, timestamp=ts + 15_000),
        Tick(price=l, volume=q, timestamp=ts + 30_000),
        Tick(price=c, volume=q, timestamp=ts + 45_000),
    ]


def _resolve(trade: _Trade, candle: dict) -> tuple[str, float] | None:
    h, l = candle["high"], candle["low"]
    if trade.side == "buy":
        hit_sl = l <= trade.sl
        hit_tp = h >= trade.tp
    else:
        hit_sl = h >= trade.sl
        hit_tp = l <= trade.tp
    if hit_sl:        # SL wins when both hit (conservative)
        return "SL", trade.sl
    if hit_tp:
        return "TP", trade.tp
    return None


def _update_excursion(trade: _Trade, candle: dict) -> None:
    h, l = candle["high"], candle["low"]
    if trade.side == "buy":
        fav = (h - trade.entry) / trade.risk
        adv = (trade.entry - l) / trade.risk
    else:
        fav = (trade.entry - l) / trade.risk
        adv = (h - trade.entry) / trade.risk
    trade.mfe_r = max(trade.mfe_r, fav)
    trade.mae_r = max(trade.mae_r, adv)


def _write(writer: csv.DictWriter, trade: _Trade, outcome: str, exit_price: float, close_ts: int) -> None:
    sign = 1.0 if trade.side == "buy" else -1.0
    pnl_r = sign * (exit_price - trade.entry) / trade.risk
    rr = abs(trade.tp - trade.entry) / trade.risk
    writer.writerow({
        "mode":          "backtest",
        "symbol":        trade.symbol,
        "side":          trade.side,
        "opened_at":     f"{trade.opened_ts / 1000:.3f}",
        "closed_at":     f"{close_ts / 1000:.3f}",
        "duration_s":    f"{(close_ts - trade.opened_ts) / 1000:.1f}",
        "entry":         f"{trade.entry:.6f}",
        "sl":            f"{trade.sl:.6f}",
        "tp":            f"{trade.tp:.6f}",
        "exit":          f"{exit_price:.6f}",
        "outcome":       outcome,
        "risk":          f"{trade.risk:.6f}",
        "pnl_r":         f"{pnl_r:.3f}",
        "reward_risk":   f"{rr:.3f}",
        "mfe_r":         f"{trade.mfe_r:.3f}",
        "mae_r":         f"{trade.mae_r:.3f}",
        "rsi_at_entry":  f"{trade.rsi_at_entry:.2f}",
        "vwap_at_entry": f"{trade.vwap_at_entry:.6f}",
        "gate_band":     int(trade.gate_band),
        "gate_rsi":      int(trade.gate_rsi),
        "gate_wick":     int(trade.gate_wick),
    })


def _close_trade(
    writer: csv.DictWriter,
    trade: _Trade,
    outcome: str,
    exit_price: float,
    ts_ms: int,
    wins: list[int],
    losses: list[int],
    cooldown: list[int],
) -> None:
    _write(writer, trade, outcome, exit_price, ts_ms)
    if outcome == "TP":
        wins[0] += 1
    else:
        losses[0] += 1
    cooldown[0] = ts_ms + int(cfg.COOLDOWN_SEC * 1000)
    logger.debug("[%s] %s %+.3fR (entry=%.4f exit=%.4f)", trade.symbol, outcome,
                 (1 if trade.side == "buy" else -1) * (exit_price - trade.entry) / trade.risk,
                 trade.entry, exit_price)


def run_symbol(symbol: str, candles: list[dict], out_path: str) -> tuple[int, int, int]:
    """Replay one symbol's candles. Returns (signals, wins, losses)."""
    internal = cfg.from_delta_symbol(symbol)
    engine = SymbolEngine(internal)
    # ponytail: tick_size left at default 0.01; SL offset (SL_TICKS * 0.01) is
    # negligible vs. live prices — acceptable for strategy-level backtest signal fidelity.

    open_trade: _Trade | None = None
    signals = 0
    wins = [0]
    losses = [0]
    cooldown_until = [0]  # unix ms; mutable via list so _close_trade can set it

    exists = os.path.exists(out_path) and os.path.getsize(out_path) > 0
    with open(out_path, "a", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        if not exists:
            writer.writeheader()

        for candle in candles:
            ts_ms = int(candle["time"]) * 1000

            # Feed ticks; signal fires on the first tick of this candle (closes prev bucket).
            sig: SignalResult | None = None
            for tick in _synth_ticks(candle):
                r = engine.on_tick(tick)
                if r is not None:
                    sig = r

            # Resolve existing trade against this candle's range.
            if open_trade is not None:
                _update_excursion(open_trade, candle)
                res = _resolve(open_trade, candle)
                if res is not None:
                    _close_trade(writer, open_trade, res[0], res[1], ts_ms, wins, losses, cooldown_until)
                    open_trade = None

            # Open new trade from signal (this candle is its first candle of exposure).
            if sig is not None and open_trade is None and ts_ms >= cooldown_until[0]:
                side = "buy" if sig.signal == Signal.LONG else "sell"
                risk = abs(sig.entry_price - sig.sl_price)
                if risk > 0:
                    g = sig.gates
                    open_trade = _Trade(
                        symbol=internal, side=side,
                        entry=sig.entry_price, sl=sig.sl_price, tp=sig.tp_price, risk=risk,
                        opened_ts=ts_ms, rsi_at_entry=sig.rsi, vwap_at_entry=sig.vwap.vwap,
                        gate_band=g[0], gate_rsi=g[1], gate_wick=g[2],
                    )
                    signals += 1
                    logger.info("[%s] %s signal | entry=%.4f sl=%.4f tp=%.4f",
                                internal, sig.signal.value, sig.entry_price, sig.sl_price, sig.tp_price)
                    # Check if this candle immediately hits SL/TP.
                    _update_excursion(open_trade, candle)
                    res = _resolve(open_trade, candle)
                    if res is not None:
                        _close_trade(writer, open_trade, res[0], res[1], ts_ms, wins, losses, cooldown_until)
                        open_trade = None

    return signals, wins[0], losses[0]


def main() -> None:
    ap = argparse.ArgumentParser(description="Backtest Rebound strategy on historical parquet candles")
    ap.add_argument("--symbols", default=",".join(ALL_SYMBOLS),
                    help="Comma-separated Delta symbols (default: all 5)")
    ap.add_argument("--resolution", default="1m",
                    choices=["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "1d"],
                    help="Candle resolution — must match fetched data (default: 1m)")
    ap.add_argument("--data-dir", default="data",
                    help="Directory containing parquet files (default: data/)")
    ap.add_argument("--out", default=cfg.TRADES_CSV,
                    help=f"Output CSV path (default: {cfg.TRADES_CSV})")
    args = ap.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    data_dir = Path(args.data_dir)

    total_sigs = total_wins = total_losses = 0
    for symbol in symbols:
        path = data_dir / f"{symbol}_{args.resolution}.parquet"
        if not path.exists():
            logger.warning("[%s] Missing %s — run fetch.py first", symbol, path)
            continue

        tbl = pq.read_table(path)
        d = tbl.to_pydict()
        n = len(d["time"])
        candles = [
            {"time": d["time"][i], "open": d["open"][i], "high": d["high"][i],
             "low": d["low"][i], "close": d["close"][i], "volume": d["volume"][i]}
            for i in range(n)
        ]

        logger.info("[%s] Replaying %d candles ...", symbol, n)
        sigs, wins, losses = run_symbol(symbol, candles, args.out)
        resolved = wins + losses
        wr = wins / resolved * 100.0 if resolved else 0.0
        logger.info("[%s] signals=%d resolved=%d win%%=%.1f W/L=%d/%d",
                    symbol, sigs, resolved, wr, wins, losses)
        total_sigs += sigs
        total_wins += wins
        total_losses += losses

    total_resolved = total_wins + total_losses
    total_wr = total_wins / total_resolved * 100.0 if total_resolved else 0.0
    logger.info("TOTAL signals=%d resolved=%d win%%=%.1f W/L=%d/%d → %s",
                total_sigs, total_resolved, total_wr, total_wins, total_losses, args.out)


if __name__ == "__main__":
    main()
