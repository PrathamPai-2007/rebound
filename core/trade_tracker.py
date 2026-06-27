"""
Trade outcome tracker.

Two outcome sources, both feeding one rich `trades.csv`:
- **paper** — simulated. On each signal a virtual trade opens and resolves
  against subsequent live ticks (first touch of TP = win, SL = loss). Works in
  DRY_RUN where no real orders exist, so the strategy can be forward-tested and
  tuned without risking capital. Mirrors the live post-close cooldown so the
  dry-run firing rate matches what live would actually do.
- **live** — real. RiskManager calls record_live() when a real position closes.

R-multiple convention: risk = |entry - sl| (1R). A trade hitting SL = -1.0R; a
trade hitting TP = +reward_risk R. MFE/MAE are the best/worst excursions in R.
"""
from __future__ import annotations
import csv
import json
import logging
import os
import threading
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Any

from config import cfg

logger = logging.getLogger(__name__)


def _notify(msg: str) -> None:
    token = cfg.TELEGRAM_TOKEN
    chat_id = cfg.TELEGRAM_CHAT_ID
    if not token or not chat_id:
        return

    def _send() -> None:
        try:
            data = json.dumps({"chat_id": chat_id, "text": msg}).encode()
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=data, headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception:
            pass

    threading.Thread(target=_send, daemon=True).start()


CSV_FIELDS = [
    "mode", "symbol", "side", "opened_at", "closed_at", "duration_s",
    "entry", "sl", "tp", "exit", "outcome", "risk", "pnl_r", "reward_risk",
    "mfe_r", "mae_r", "rsi_at_entry", "vwap_at_entry",
    "gate_band", "gate_rsi", "gate_wick",
]


@dataclass
class _VirtualTrade:
    symbol: str
    side: str               # "buy" | "sell"
    entry: float
    sl: float
    tp: float
    risk: float             # |entry - sl|, one R
    opened_at: float        # wall-clock (time.time)
    opened_mono: float      # monotonic, for duration
    rsi_at_entry: float
    vwap_at_entry: float
    gate_band: bool
    gate_rsi: bool
    gate_wick: bool
    mfe_r: float = 0.0       # max favorable excursion, R
    mae_r: float = 0.0       # max adverse excursion, R (>= 0 magnitude)

    def excursion_r(self, price: float) -> float:
        """Signed R move from entry in the trade's favor."""
        sign = 1.0 if self.side == "buy" else -1.0
        return sign * (price - self.entry) / self.risk


class TradeTracker:
    def __init__(self) -> None:
        self._open: dict[str, _VirtualTrade] = {}
        self._paper_cooldown: dict[str, float] = {}   # symbol -> monotonic expiry
        self.paper_wins: int = 0
        self.paper_losses: int = 0
        self.live_wins: int = 0
        self.live_losses: int = 0
        self._ensure_header()

    # ------------------------------------------------------------------
    # CSV
    # ------------------------------------------------------------------

    def _ensure_header(self) -> None:
        exists = os.path.exists(cfg.TRADES_CSV) and os.path.getsize(cfg.TRADES_CSV) > 0
        if not exists:
            try:
                with open(cfg.TRADES_CSV, "w", newline="") as f:
                    csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()
            except OSError as exc:
                logger.error("Could not init %s: %s", cfg.TRADES_CSV, exc)

    def _write_row(self, row: dict[str, Any]) -> None:
        try:
            with open(cfg.TRADES_CSV, "a", newline="") as f:
                csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow(row)
        except OSError as exc:
            logger.error("Could not append to %s: %s", cfg.TRADES_CSV, exc)

    # ------------------------------------------------------------------
    # Paper trades
    # ------------------------------------------------------------------

    def open_paper(self, sig: Any, gates: tuple[bool, bool, bool]) -> None:
        """Open a virtual trade for a fired signal. No-op if one is already open
        for the symbol or the symbol is in paper cooldown."""
        symbol = sig.symbol
        now = time.monotonic()
        if symbol in self._open:
            return
        if now < self._paper_cooldown.get(symbol, 0.0):
            return

        side = "buy" if sig.signal.value == "LONG" else "sell"
        risk = abs(sig.entry_price - sig.sl_price)
        if risk <= 0:
            return
        g_band, g_rsi, g_wick = gates
        self._open[symbol] = _VirtualTrade(
            symbol=symbol, side=side,
            entry=sig.entry_price, sl=sig.sl_price, tp=sig.tp_price, risk=risk,
            opened_at=time.time(), opened_mono=now,
            rsi_at_entry=sig.rsi, vwap_at_entry=sig.vwap.vwap,
            gate_band=g_band, gate_rsi=g_rsi, gate_wick=g_wick,
        )
        rr = abs(sig.tp_price - sig.entry_price) / risk
        logger.info(
            "[%s] PAPER %s open | entry=%.4f sl=%.4f tp=%.4f RR=%.2f",
            symbol, side, sig.entry_price, sig.sl_price, sig.tp_price, rr,
        )
        _notify(f"[{symbol} {side.upper()}] paper open\nentry={sig.entry_price:.2f}  sl={sig.sl_price:.2f}  tp={sig.tp_price:.2f}  RR={rr:.2f}")

    def on_tick(self, symbol: str, price: float, ts_ms: float) -> None:
        """Advance/resolve the open paper trade for this symbol on a new price."""
        vt = self._open.get(symbol)
        if vt is None:
            return

        exc_r = vt.excursion_r(price)
        vt.mfe_r = max(vt.mfe_r, exc_r)
        vt.mae_r = max(vt.mae_r, -exc_r)

        if vt.side == "buy":
            hit_sl = price <= vt.sl
            hit_tp = price >= vt.tp
        else:
            hit_sl = price >= vt.sl
            hit_tp = price <= vt.tp

        if not (hit_sl or hit_tp):
            return

        # Gap beyond both in one tick → assume SL (conservative).
        if hit_sl:
            outcome, exit_price = "SL", vt.sl
        else:
            outcome, exit_price = "TP", vt.tp
        self._resolve_paper(vt, outcome, exit_price)

    def _resolve_paper(self, vt: _VirtualTrade, outcome: str, exit_price: float) -> None:
        sign = 1.0 if vt.side == "buy" else -1.0
        pnl_r = sign * (exit_price - vt.entry) / vt.risk
        duration = time.monotonic() - vt.opened_mono
        if pnl_r >= 0:
            self.paper_wins += 1
        else:
            self.paper_losses += 1
        self._write_row({
            "mode": "paper", "symbol": vt.symbol, "side": vt.side,
            "opened_at": f"{vt.opened_at:.3f}", "closed_at": f"{time.time():.3f}",
            "duration_s": f"{duration:.1f}",
            "entry": f"{vt.entry:.6f}", "sl": f"{vt.sl:.6f}", "tp": f"{vt.tp:.6f}",
            "exit": f"{exit_price:.6f}", "outcome": outcome,
            "risk": f"{vt.risk:.6f}", "pnl_r": f"{pnl_r:.3f}",
            "reward_risk": f"{abs(vt.tp - vt.entry) / vt.risk:.3f}",
            "mfe_r": f"{vt.mfe_r:.3f}", "mae_r": f"{vt.mae_r:.3f}",
            "rsi_at_entry": f"{vt.rsi_at_entry:.2f}", "vwap_at_entry": f"{vt.vwap_at_entry:.6f}",
            "gate_band": int(vt.gate_band), "gate_rsi": int(vt.gate_rsi), "gate_wick": int(vt.gate_wick),
        })
        logger.info(
            "[%s] PAPER %s %+.2fR dur=%.0fs (%s)",
            vt.symbol, "WIN" if pnl_r >= 0 else "LOSS", pnl_r, duration, outcome,
        )
        _notify(f"[{vt.symbol} {vt.side.upper()}] paper {'WIN' if pnl_r >= 0 else 'LOSS'} {pnl_r:+.2f}R ({outcome})\nentry={vt.entry:.2f} → exit={exit_price:.2f}  dur={duration/60:.0f}m")
        del self._open[vt.symbol]
        self._paper_cooldown[vt.symbol] = time.monotonic() + cfg.PAPER_COOLDOWN_SEC

    # ------------------------------------------------------------------
    # Live trades
    # ------------------------------------------------------------------

    def record_live(
        self,
        trade: Any,
        realised_pnl: float | None,
        contract_value: float,
        exit_price: float | None = None,
    ) -> None:
        """Write a live outcome row when a real position closes. Exit price is
        not in the close snapshot, so outcome is inferred from realised_pnl sign."""
        risk = abs(trade.entry_price - trade.sl_price)
        pnl_r = 0.0
        if realised_pnl is not None and risk > 0 and contract_value > 0:
            pnl_r = realised_pnl / (risk * trade.size * contract_value)
        outcome = "TP" if pnl_r > 0 else "SL"
        if pnl_r >= 0:
            self.live_wins += 1
        else:
            self.live_losses += 1
        self._write_row({
            "mode": "live", "symbol": trade.symbol, "side": trade.side,
            "opened_at": f"{trade.opened_at:.3f}", "closed_at": f"{time.time():.3f}",
            "duration_s": f"{time.time() - trade.opened_at:.1f}",
            "entry": f"{trade.entry_price:.6f}", "sl": f"{trade.sl_price:.6f}",
            "tp": f"{trade.tp_price:.6f}",
            "exit": "" if exit_price is None else f"{exit_price:.6f}",
            "outcome": outcome, "risk": f"{risk:.6f}", "pnl_r": f"{pnl_r:.3f}",
            "reward_risk": f"{abs(trade.tp_price - trade.entry_price) / risk:.3f}" if risk > 0 else "",
            "mfe_r": "", "mae_r": "", "rsi_at_entry": "", "vwap_at_entry": "",
            "gate_band": "", "gate_rsi": "", "gate_wick": "",
        })
        logger.info(
            "[%s] LIVE close | pnl=%.4f (%+.2fR) outcome=%s",
            trade.symbol, realised_pnl or 0.0, pnl_r, outcome,
        )
        _notify(f"[{trade.symbol} {trade.side.upper()}] live {outcome} {pnl_r:+.2f}R")

    # ------------------------------------------------------------------
    # Summary helpers
    # ------------------------------------------------------------------

    @property
    def open_paper_count(self) -> int:
        return len(self._open)

    def win_rate(self) -> float:
        total = self.paper_wins + self.paper_losses
        return (self.paper_wins / total * 100.0) if total else 0.0
