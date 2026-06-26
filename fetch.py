"""
Historical candle fetcher for Delta Exchange India.

Pulls OHLCV data via GET /v2/history/candles and stores per-symbol parquet files
in data/{symbol}_{resolution}.parquet. Re-runs append and deduplicate by timestamp.

Usage:
    python fetch.py                                          # all 5 symbols, 1m, last 30 days
    python fetch.py --symbols BTCUSD,SOLUSD                 # subset
    python fetch.py --resolution 5m --start 2026-01-01      # custom resolution / range
    python fetch.py --start 2026-05-01 --end 2026-06-01     # explicit window
"""
from __future__ import annotations

import argparse
import datetime
import logging
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

BASE_URL = "https://api.india.delta.exchange/v2/history/candles"
CHUNK_SEC = 7 * 24 * 3600   # 7-day windows to stay within API result limits
COLS = ["time", "open", "high", "low", "close", "volume"]
SCHEMA = pa.schema([
    ("time",   pa.int64()),
    ("open",   pa.float64()),
    ("high",   pa.float64()),
    ("low",    pa.float64()),
    ("close",  pa.float64()),
    ("volume", pa.float64()),
])
ALL_SYMBOLS = ["BTCUSD", "SOLUSD", "ETHUSD", "XAUTUSD", "BNBUSD"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _parse_date(s: str) -> int:
    """ISO date string or epoch int → unix timestamp (seconds)."""
    try:
        return int(s)
    except ValueError:
        pass
    dt = datetime.datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp())


def fetch_candles(symbol: str, resolution: str, start_ts: int, end_ts: int) -> list[dict]:
    """Fetch all candles in [start_ts, end_ts] using chunked requests."""
    seen: dict[int, dict] = {}
    t = start_ts
    while t < end_ts:
        chunk_end = min(t + CHUNK_SEC, end_ts)
        try:
            resp = requests.get(
                BASE_URL,
                params={
                    "symbol": symbol,
                    "resolution": resolution,
                    "start": t,
                    "end": chunk_end,
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            logger.error("[%s] Request failed (%s → %s): %s", symbol, t, chunk_end, exc)
            t += CHUNK_SEC
            continue

        rows = data.get("result", [])
        for row in rows:
            ts = int(row["time"])
            seen[ts] = row
        logger.debug("[%s] chunk %s→%s: %d rows", symbol, t, chunk_end, len(rows))
        t += CHUNK_SEC
        time.sleep(0.1)   # polite rate-limiting

    return sorted(seen.values(), key=lambda r: r["time"])


def _to_table(rows: list[dict]) -> pa.Table:
    return pa.table(
        {
            "time":   pa.array([int(r["time"])    for r in rows], type=pa.int64()),
            "open":   pa.array([float(r["open"])  for r in rows], type=pa.float64()),
            "high":   pa.array([float(r["high"])  for r in rows], type=pa.float64()),
            "low":    pa.array([float(r["low"])   for r in rows], type=pa.float64()),
            "close":  pa.array([float(r["close"]) for r in rows], type=pa.float64()),
            "volume": pa.array([float(r["volume"])for r in rows], type=pa.float64()),
        },
        schema=SCHEMA,
    )


def save(rows: list[dict], path: Path) -> int:
    """Merge new rows with existing parquet file (if any). Returns total row count."""
    new_table = _to_table(rows)

    if path.exists():
        old_table = pq.read_table(path, schema=SCHEMA)
        combined = pa.concat_tables([old_table, new_table])
        # Dedupe: keep last occurrence per timestamp, then sort.
        times = combined.column("time").to_pylist()
        idx_by_time: dict[int, int] = {}
        for i, t in enumerate(times):
            idx_by_time[t] = i
        keep = sorted(idx_by_time.values())
        combined = combined.take(keep)
    else:
        combined = new_table

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(combined, path)
    return len(combined)


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch historical candles from Delta Exchange India")
    ap.add_argument(
        "--symbols",
        default=",".join(ALL_SYMBOLS),
        help="Comma-separated Delta symbols (default: all 5)",
    )
    ap.add_argument(
        "--resolution",
        default="1m",
        choices=["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "1d"],
        help="Candle resolution (default: 1m)",
    )
    ap.add_argument(
        "--start",
        default=None,
        help="Start date YYYY-MM-DD or epoch seconds (default: 30 days ago)",
    )
    ap.add_argument(
        "--end",
        default=None,
        help="End date YYYY-MM-DD or epoch seconds (default: now)",
    )
    ap.add_argument(
        "--out-dir",
        default="data",
        help="Output directory (default: data/)",
    )
    args = ap.parse_args()

    now = int(time.time())
    end_ts   = _parse_date(args.end)   if args.end   else now
    start_ts = _parse_date(args.start) if args.start else now - 30 * 24 * 3600

    if start_ts >= end_ts:
        sys.exit(f"--start ({start_ts}) must be before --end ({end_ts})")

    out_dir = Path(args.out_dir)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    logger.info(
        "Fetching %s candles for %s | %s → %s",
        args.resolution,
        symbols,
        datetime.datetime.fromtimestamp(start_ts, datetime.timezone.utc).strftime("%Y-%m-%d"),
        datetime.datetime.fromtimestamp(end_ts, datetime.timezone.utc).strftime("%Y-%m-%d"),
    )

    for symbol in symbols:
        logger.info("[%s] Fetching ...", symbol)
        rows = fetch_candles(symbol, args.resolution, start_ts, end_ts)
        if not rows:
            logger.warning("[%s] No data returned.", symbol)
            continue
        path = out_dir / f"{symbol}_{args.resolution}.parquet"
        total = save(rows, path)
        logger.info("[%s] %d new rows → %s (%d total)", symbol, len(rows), path, total)

    logger.info("Done.")


if __name__ == "__main__":
    main()
