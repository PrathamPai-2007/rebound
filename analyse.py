"""
Analyse trades.csv produced by core/trade_tracker.py.

Reports win rate, expectancy (avg R), profit factor, MFE/MAE, and per-symbol /
per-side breakdowns so the strategy can be tuned on real outcome data instead
of guessing. Pure stdlib — no pandas.

Usage:
    python analyse.py                  # reads ./trades.csv, all rows
    python analyse.py --file x.csv     # custom path
    python analyse.py --mode paper     # only paper (forward-test) rows
    python analyse.py --mode live      # only real fills
    python analyse.py --mode backtest  # only backtest rows

Note on gates: executed trades always have all three gate columns = 1 (a signal
only fires when all pass). The *binding* gate — which one is usually the last to
pass — lives in the periodic SUMMARY log line, not here. This script analyses
outcomes; use the bot's summary for gate-frequency tuning.
"""
from __future__ import annotations
import argparse
import csv
import math
import os
import sys
from collections import defaultdict


def _f(row: dict, key: str) -> float | None:
    v = row.get(key, "")
    if v is None or v == "":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def load(path: str, mode: str | None) -> list[dict]:
    if not os.path.exists(path):
        sys.exit(f"No such file: {path}")
    with open(path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    if mode:
        rows = [r for r in rows if r.get("mode") == mode]
    # Keep only rows with a usable pnl_r.
    return [r for r in rows if _f(r, "pnl_r") is not None]


def stats(rows: list[dict]) -> dict:
    rs = [_f(r, "pnl_r") for r in rows]
    rs = [x for x in rs if x is not None]
    n = len(rs)
    if n == 0:
        return {"n": 0}
    wins = [x for x in rs if x > 0]
    losses = [x for x in rs if x < 0]
    breakevens = n - len(wins) - len(losses)
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    durations = [d for d in (_f(r, "duration_s") for r in rows) if d is not None]
    mfes = [m for m in (_f(r, "mfe_r") for r in rows) if m is not None]
    maes = [m for m in (_f(r, "mae_r") for r in rows) if m is not None]

    # Max drawdown over the cumulative-R equity curve.
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for x in rs:
        cum += x
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)

    return {
        "n": n,
        "wins": len(wins),
        "losses": len(losses),
        "breakevens": breakevens,
        "win_rate": len(wins) / n * 100.0,
        "total_r": sum(rs),
        "expectancy_r": sum(rs) / n,
        "avg_win_r": (gross_win / len(wins)) if wins else 0.0,
        "avg_loss_r": (-gross_loss / len(losses)) if losses else 0.0,
        "profit_factor": (gross_win / gross_loss) if gross_loss > 0 else math.inf,
        "avg_dur_s": (sum(durations) / len(durations)) if durations else 0.0,
        "avg_mfe_r": (sum(mfes) / len(mfes)) if mfes else 0.0,
        "avg_mae_r": (sum(maes) / len(maes)) if maes else 0.0,
        "max_dd_r": max_dd,
    }


def fmt(s: dict) -> str:
    if s["n"] == 0:
        return "  (no trades)"
    pf = "inf" if s["profit_factor"] == math.inf else f"{s['profit_factor']:.2f}"
    return (
        f"  trades={s['n']}  win%={s['win_rate']:.1f}  W/L/BE={s['wins']}/{s['losses']}/{s['breakevens']}\n"
        f"  expectancy={s['expectancy_r']:+.3f}R/trade  total={s['total_r']:+.2f}R  PF={pf}\n"
        f"  avg_win={s['avg_win_r']:+.2f}R  avg_loss={s['avg_loss_r']:+.2f}R  max_dd={s['max_dd_r']:.2f}R\n"
        f"  avg_dur={s['avg_dur_s']:.0f}s  avg_MFE={s['avg_mfe_r']:.2f}R  avg_MAE={s['avg_mae_r']:.2f}R"
    )


def group(rows: list[dict], key: str) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        out[r.get(key, "?")].append(r)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyse Rebound trades.csv")
    ap.add_argument("--file", default="trades.csv")
    ap.add_argument("--mode", choices=["paper", "live", "backtest"], default=None)
    args = ap.parse_args()

    rows = load(args.file, args.mode)
    label = args.mode or "all"
    print(f"=== {args.file} | mode={label} ===\n")

    print("OVERALL")
    print(fmt(stats(rows)))

    print("\nBY SYMBOL")
    for sym, rs in sorted(group(rows, "symbol").items()):
        s = stats(rs)
        if s["n"]:
            pf = "inf" if s["profit_factor"] == math.inf else f"{s['profit_factor']:.2f}"
            print(f"  {sym:<12} n={s['n']:<4} win%={s['win_rate']:5.1f}  "
                  f"exp={s['expectancy_r']:+.3f}R  total={s['total_r']:+.2f}R  PF={pf}")

    print("\nBY SIDE")
    for side, rs in sorted(group(rows, "side").items()):
        s = stats(rs)
        if s["n"]:
            print(f"  {side:<5} n={s['n']:<4} win%={s['win_rate']:5.1f}  "
                  f"exp={s['expectancy_r']:+.3f}R  total={s['total_r']:+.2f}R")

    # Mode split only when not already filtered.
    if args.mode is None:
        print("\nBY MODE")
        for m, rs in sorted(group(rows, "mode").items()):
            s = stats(rs)
            if s["n"]:
                print(f"  {m:<6} n={s['n']:<4} win%={s['win_rate']:5.1f}  "
                      f"exp={s['expectancy_r']:+.3f}R  total={s['total_r']:+.2f}R")


if __name__ == "__main__":
    main()
