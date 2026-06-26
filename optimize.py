"""
Parameter sweep for symbol-specific backtesting.

Runs backtest.py with varying RSI thresholds, VWAP band width, and wick ratio,
then ranks results by expectancy. Use this to find settings that work for
symbols where the default config performs poorly.

Usage:
    python optimize.py                        # BTC + XAUT, 1m, data/
    python optimize.py --symbols BTCUSD
    python optimize.py --resolution 5m
"""
from __future__ import annotations

import argparse
import csv
import itertools
import os
import subprocess
import sys
import tempfile


def _f(row: dict, key: str) -> float | None:
    v = row.get(key, "")
    try:
        return float(v) if v not in (None, "") else None
    except ValueError:
        return None


def stats_from_csv(path: str) -> dict:
    try:
        with open(path, newline="") as fh:
            rows = list(csv.DictReader(fh))
    except FileNotFoundError:
        return {"n": 0}
    rs = [_f(r, "pnl_r") for r in rows if _f(r, "pnl_r") is not None]
    n = len(rs)
    if n == 0:
        return {"n": 0}
    wins = [x for x in rs if x > 0]
    losses = [x for x in rs if x < 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    cum = peak = max_dd = 0.0
    for x in rs:
        cum += x
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)
    return {
        "n": n,
        "wins": len(wins),
        "win_pct": len(wins) / n * 100,
        "exp_r": sum(rs) / n,
        "total_r": sum(rs),
        "pf": (gross_win / gross_loss) if gross_loss > 0 else float("inf"),
        "max_dd": max_dd,
    }


def run_one(symbol: str, params: dict, resolution: str, data_dir: str) -> dict:
    env = os.environ.copy()
    env.update({k: str(v) for k, v in params.items()})
    fd, tmp = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    try:
        r = subprocess.run(
            [sys.executable, "backtest.py",
             "--symbols", symbol,
             "--resolution", resolution,
             "--data-dir", data_dir,
             "--out", tmp],
            env=env,
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            print(f"Error running backtest.py:\n{r.stderr}", file=sys.stderr)
            return {"n": 0}
        return stats_from_csv(tmp)
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass


def main() -> None:
    ap = argparse.ArgumentParser(description="Parameter sweep for Rebound backtest")
    ap.add_argument("--symbols", default="BTCUSD,XAUTUSD")
    ap.add_argument("--resolution", default="1m",
                    choices=["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "1d"])
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--min-trades", type=int, default=15,
                    help="Minimum trades to include in top-5 ranking (default: 15)")
    args = ap.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    # Grid: linked RSI pairs, VWAP band multiplier, wick ratio
    rsi_pairs   = [(25, 75), (30, 70), (35, 65), (40, 60)]
    band_sds    = [1.25, 1.50, 1.75, 2.00, 2.25]
    wick_ratios = [1.0, 1.5, 2.0]

    combos = list(itertools.product(rsi_pairs, band_sds, wick_ratios))
    total = len(combos) * len(symbols)
    print(f"Running {len(combos)} combos x {len(symbols)} symbols = {total} experiments\n")

    for symbol in symbols:
        print(f"{'=' * 72}")
        print(f"  {symbol}")
        print(f"{'=' * 72}")
        print(f"  {'OS':>4} {'OB':>4} {'SD':>5} {'WICK':>5} | "
              f"{'n':>4} {'win%':>5} {'exp_R':>7} {'total_R':>8} {'PF':>5} {'maxDD':>6}")
        print(f"  {'-' * 65}")

        results: list[tuple[dict, dict]] = []
        for (rsi_os, rsi_ob), band_sd, wick in combos:
            params = {
                "RSI_OVERSOLD":  rsi_os,
                "RSI_OVERBOUGHT": rsi_ob,
                "VWAP_BAND_SD":  band_sd,
                "WICK_RATIO":    wick,
            }
            s = run_one(symbol, params, args.resolution, args.data_dir)
            results.append((params, s))

            if s["n"] == 0:
                print(f"  {rsi_os:>4} {rsi_ob:>4} {band_sd:>5.2f} {wick:>5.1f} | (no trades)")
                continue

            pf_str = f"{s['pf']:5.2f}" if s["pf"] != float("inf") else "  inf"
            print(f"  {rsi_os:>4} {rsi_ob:>4} {band_sd:>5.2f} {wick:>5.1f} | "
                  f"{s['n']:>4} {s['win_pct']:>4.1f}% {s['exp_r']:>+7.3f} "
                  f"{s['total_r']:>+8.2f} {pf_str} {s['max_dd']:>6.2f}")

        valid = [
            (p, s) for p, s in results
            if s.get("n", 0) >= args.min_trades and s.get("exp_r", -999) > 0
        ]
        valid.sort(key=lambda x: x[1]["exp_r"], reverse=True)
        print(f"\n  TOP 5 by expectancy (n>={args.min_trades}, exp>0):")
        if not valid:
            print("    (none — strategy has no edge for this symbol with these params)")
        for p, s in valid[:5]:
            pf_str = f"{s['pf']:.2f}" if s["pf"] != float("inf") else "inf"
            print(f"    RSI={p['RSI_OVERSOLD']}/{p['RSI_OVERBOUGHT']}  "
                  f"BAND={p['VWAP_BAND_SD']:.2f}  WICK={p['WICK_RATIO']:.1f}  ->  "
                  f"n={s['n']}  win%={s['win_pct']:.1f}  exp={s['exp_r']:+.3f}R  "
                  f"total={s['total_r']:+.2f}R  PF={pf_str}  maxDD={s['max_dd']:.2f}R")
        print()


if __name__ == "__main__":
    main()
