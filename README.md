# Rebound

Async mean-reversion scalping bot for **Delta Exchange India**. Market data over native WebSocket; orders via REST — no polling, no ccxt.

---

## Strategy

Trades 5 perpetuals simultaneously: `BTC/USDT`, `SOL/USDT`, `ETH/USDT`, `XAUT/USDT` (Tether Gold), `BNB/USDT`. Mapped to Delta's `USD`-suffix format (`BTCUSD` etc.).

Looks for price stretched beyond its volume-weighted center and bets on the snap back.

**Long entry** (all three must pass on a closed candle):
- Price at or below lower 2nd SD band of rolling VWAP
- RSI(7) ≤ `RSI_OVERSOLD` (default 30)
- Bullish wick rejection on last candle

**Short entry** (mirror):
- Price at or above upper 2nd SD band
- RSI(7) ≥ `RSI_OVERBOUGHT` (default 70)
- Bearish wick rejection

**Exit:** TP at 1st SD band (mean-reversion target), SL 3 ticks past entry wick via bracket order.

---

## Risk Management

- **Bracket orders** — market entry via `POST /v2/orders` with `bracket_*` params; Delta auto-creates SL/TP on fill. Exits are stop-limit with `BRACKET_SLIPPAGE_TICKS` offset past trigger so they fill like market on fast moves.
- **Sizing** — risk-based integer contracts: `(equity × RISK_PER_TRADE_PCT/100) / |entry−sl| / contract_value`, rounded, min 1. Rejected if required margin exceeds equity.
- **Cooldown** — 120s on a symbol after position closes; one signal per candle.
- **Leverage** — 50× crypto, 30× commodities (configurable).
- **Equity guard** — if drawdown from initial equity exceeds `MAX_EQUITY_DRAWDOWN_PCT`, all positions market-closed immediately.

---

## Architecture

```
bot.py
├── _stream_public()     — ob_l2 WebSocket, fans out to 5 SymbolEngines
├── _stream_private()    — positions/orders/margins WS → RiskManager
├── heartbeat_watchdog() — per-stream liveness, force-reconnects stalled streams
└── _summary_loop()      — every SUMMARY_INTERVAL: gate counts, signals, W/L, equity

core/
├── ws_client.py         — DeltaWSClient, key-auth HMAC-SHA256, exponential backoff
├── rest_client.py       — DeltaRestClient, aiohttp, signed REST for orders + market info
├── risk_manager.py      — bracket sizing, equity guard, cooldown scheduling
└── trade_tracker.py     — outcome sink → trades.csv (paper + live rows)

strategy/
├── indicators.py        — rolling VWAP + SD bands, RSI(7) Wilder, CandleBuffer OHLCV
└── signal_engine.py     — per-symbol stateful evaluator, gate counters, cooldown

fetch.py                 — pull historical OHLCV candles → data/*.parquet
backtest.py              — replay parquet through live SymbolEngine → trades.csv
analyse.py               — reads trades.csv, reports win%, expectancy, profit factor
optimize.py              — runs parameter sweep grid search to find optimal settings
```

Both WS streams reconnect with exponential backoff (1s → 60s). Private stream re-authenticates on every reconnect. `heartbeat_watchdog` catches exchange-side data stalls that TCP ping/pong misses.

---

## Setup

```bash
pip install -r requirements.txt
```

Create `.env`:
```
DELTA_API_KEY=your_key_here
DELTA_API_SECRET=your_secret_here
```

Get keys at [india.delta.exchange](https://india.delta.exchange) → Settings → API Keys. Enable **Read + Trade**.

---

## Running

```bash
# Dry run (default) — streams live data, logs signals, no orders placed
python bot.py

# Live trading
DRY_RUN=false python bot.py

# Testnet
USE_TESTNET=true python bot.py

# Verbose tick logging
LOG_LEVEL=DEBUG python bot.py
```

Logs → `trading_bot.log` (rotating, 50 MB, 5 backups) + stdout.

---

## Configuration

All parameters in `config.py`, sourced from env vars:

| Variable | Default | Description |
|---|---|---|
| `VWAP_WINDOW` | `150` | Tick window for VWAP + SD bands |
| `VWAP_BAND_SD` | `1.75` | SD multiplier for entry bands |
| `RSI_PERIOD` | `7` | RSI lookback |
| `RSI_OVERSOLD` | `30` | Long threshold |
| `RSI_OVERBOUGHT` | `70` | Short threshold |
| `WICK_RATIO` | `1.5` | Min wick-to-body ratio |
| `RISK_PER_TRADE_PCT` | `1.0` | % equity risked per trade |
| `BRACKET_SLIPPAGE_TICKS` | `5` | SL limit offset past trigger |
| `MAX_EQUITY_DRAWDOWN_PCT` | `10.0` | Emergency-close threshold |
| `PAPER_EQUITY` | `10000.0` | Simulated equity in dry run |
| `COOLDOWN_SEC` | `120` | Post-close signal block per symbol |
| `SUMMARY_INTERVAL_SEC` | `300` | Summary log frequency |
| `DRY_RUN` | `true` | Skip order submission |
| `USE_TESTNET` | `false` | Delta testnet endpoints |
| `LOG_LEVEL` | `INFO` | `DEBUG` for per-candle gate breakdown |

---

## Outcome Tracking

Every signal opens a virtual paper trade (resolved against later ticks: first TP touch = win, SL touch = loss). Real position closes also write rows. Both land in **`trades.csv`** with columns: `mode`, `symbol`, `side`, `pnl_r`, `mfe_r`, `mae_r`, `duration_s`, `rsi_at_entry`, `vwap_at_entry`, gate flags.

```bash
python analyse.py                  # all rows
python analyse.py --mode paper     # dry-run signals only
python analyse.py --mode live      # real fills only
python analyse.py --mode backtest  # backtest results only
```

Reports win%, expectancy (R/trade), profit factor, avg win/loss, max drawdown (R), MFE/MAE — broken down by symbol, side, mode.

---

## Historical Data & Backtesting

Pull OHLCV candles from the public Delta API and replay them through the live strategy code:

```bash
# Fetch — default: all 5 symbols, 1m, last 30 days
python fetch.py
python fetch.py --symbols BTCUSD,ETHUSD --resolution 5m --start 2026-01-01

# Backtest — replays parquet through SymbolEngine, writes mode=backtest rows to trades.csv
python backtest.py
python backtest.py --symbols BTCUSD --resolution 1m

# Analyse backtest results
python analyse.py --mode backtest
```

Parquet files stored in `data/{symbol}_{resolution}.parquet`. Re-runs append and deduplicate by timestamp. Each OHLCV candle is synthesized into 4 ticks (O→H→L→C within the same bucket) so `CandleBuffer` reconstructs identical wick geometry. Cooldown uses simulated candle timestamps, not wall clock.

---

## Tuning Loop

1. Fetch historical candle data → `python fetch.py`
2. Run parameter optimizer to find the best settings → `python optimize.py --symbols BTCUSD`
3. Run bot in dry run to test live behavior → `python analyse.py --mode paper`
4. Read summary logs for gate pass counts — lowest count = binding gate, loosen that one first
5. Adjust `RSI_OVERSOLD/OVERBOUGHT`, `VWAP_WINDOW`, or `VWAP_BAND_SD` in `.env` → repeat

---

## Symbol Mapping

Delta Exchange India uses `USD` suffix, not `USDT`:

| Internal | Delta API |
|---|---|
| `BTC/USDT` | `BTCUSD` |
| `SOL/USDT` | `SOLUSD` |
| `ETH/USDT` | `ETHUSD` |
| `XAUT/USDT` | `XAUTUSD` |
| `BNB/USDT` | `BNBUSD` |

XAG (silver) and CL (crude oil) have no perpetuals on Delta India.

---

> High-leverage trading carries significant risk of total capital loss. Always validate on dry run and backtest before deploying capital.
