# Rebound

Async mean-reversion scalping bot for **Delta Exchange India**. Trades 5 perpetual futures simultaneously using live WebSocket market data and REST order placement — no polling, no third-party trading libraries.

---

## Strategy

Looks for price stretched beyond its volume-weighted mean and bets on the snap back. Three gates must all pass on a closed candle before a trade is opened:

| Gate | Long condition | Short condition |
|---|---|---|
| **VWAP band** | Price ≤ lower 2nd SD band | Price ≥ upper 2nd SD band |
| **RSI(7)** | RSI ≤ `RSI_OVERSOLD` | RSI ≥ `RSI_OVERBOUGHT` |
| **Wick rejection** | Bullish wick-to-body ratio ≥ `WICK_RATIO` | Bearish wick-to-body ratio ≥ `WICK_RATIO` |

**Entry:** market order with bracket exits attached. **TP** at the 1st SD band (mean-reversion target). **SL** 3 ticks past the entry candle's wick, placed as a stop-limit with a slippage offset so it fills like a market on a fast move.

---

## Risk Management

- **Sizing** — risk-based integer contracts: `(equity × RISK_PER_TRADE_PCT / 100) / |entry − sl| / contract_value`, rounded to nearest integer, minimum 1. Rejected if required margin exceeds available equity.
- **Cooldown** — 120 seconds after any position closes on a symbol; at most one signal per closed candle.
- **Equity guard** — if drawdown from initial equity exceeds `MAX_EQUITY_DRAWDOWN_PCT`, all positions are market-closed immediately.
- **Leverage** — 50× for crypto, 30× for commodities (configurable via env vars).

---

## Active Symbols

| Internal | Delta API symbol | Contract value |
|---|---|---|
| `BTC/USDT` | `BTCUSD` | 0.001 BTC |
| `SOL/USDT` | `SOLUSD` | 1.0 SOL |
| `XAUT/USDT` | `XAUTUSD` | 0.001 XAU (Tether Gold) |
| `BNB/USDT` | `BNBUSD` | 0.1 BNB |
| `LTC/USDT` | `LTCUSD` | 1.0 LTC |

Delta Exchange India uses a `USD` suffix on all symbols, not `USDT`. Each symbol has its own tuned config in `config/{DELTASYM}.json`.

---

## Architecture

```
bot.py
├── _stream_public()     — ob_l2 WebSocket, fans out ticks to 5 SymbolEngines
├── _stream_private()    — positions/orders/margins stream → RiskManager
├── heartbeat_watchdog() — force-reconnects streams silent > 60s
└── _summary_loop()      — every 5 min: gate counts, signals, W/L, equity

core/
├── ws_client.py         — DeltaWSClient, key-auth HMAC-SHA256, exponential backoff
├── rest_client.py       — DeltaRestClient, aiohttp, signed REST for orders
├── risk_manager.py      — bracket sizing, equity guard, cooldown scheduling
└── trade_tracker.py     — outcome sink → trades.csv (paper + live rows)

strategy/
├── indicators.py        — rolling VWAP + SD bands, RSI(7) Wilder, CandleBuffer OHLCV
└── signal_engine.py     — per-symbol gate evaluator, signal deduplication, cooldown

fetch.py                 — pull historical OHLCV candles → data/*.parquet
backtest.py              — replay parquet through live SymbolEngine → trades.csv
analyse.py               — reads trades.csv, reports win%, expectancy, profit factor
optimize.py              — parallel grid search to find optimal per-symbol parameters
```

Two persistent WebSocket connections to `wss://socket.india.delta.exchange`. All 5 symbols share one public connection (multiplexed by `product_symbol`); the private connection handles positions, margins, and order updates. Both streams reconnect with exponential backoff (1s → 60s) and the heartbeat watchdog catches exchange-side data stalls that TCP ping/pong misses.

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Get API keys

Go to [india.delta.exchange](https://india.delta.exchange) → top-right avatar → **Settings** → **API Keys** → **Create API Key**. Enable **Read** and **Trade** permissions. Copy the key and secret immediately — the secret is shown only once.

### 3. Create `.env`

```
DELTA_API_KEY=your_key_here
DELTA_API_SECRET=your_secret_here
```

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

Logs go to `trading_bot.log` (rotating, 50 MB, 5 backups) and stdout. `DRY_RUN=true` is the default — no orders are ever placed unless explicitly disabled.

---

## Outcome Tracking

Every signal opens a virtual paper trade tracked against subsequent ticks (first TP touch = win, first SL touch = loss). Real position closes are also recorded. Both land in `trades.csv`.

```bash
python analyse.py                  # all rows
python analyse.py --mode paper     # dry-run signals only
python analyse.py --mode live      # real fills only
python analyse.py --mode backtest  # backtest results only
```

Reports win%, expectancy (R/trade), profit factor, average win/loss, max drawdown (R), MFE/MAE — broken down by symbol, side, and mode.

---

## Historical Data & Backtesting

```bash
# Fetch — all active symbols, 1m candles, last 30 days (default)
python fetch.py

# Custom range
python fetch.py --symbols BTCUSD,LTCUSD --start 2025-12-27

# Backtest — replays parquet through SymbolEngine, writes mode=backtest rows
python backtest.py --out trades_6m.csv

# Analyse results
python analyse.py --file trades_6m.csv --mode backtest
```

Parquet files are stored in `data/{symbol}_{resolution}.parquet`. Re-runs append and deduplicate by timestamp. Each OHLCV candle is synthesized into 4 ticks (open → high → low → close) so `CandleBuffer` reconstructs the full wick geometry.

---

## Parameter Tuning

```bash
# Run optimizer for a symbol (parallelised across all CPU cores)
python optimize.py --symbols BTCUSD --min-trades 50

# Full grid search for all active symbols
python optimize.py --symbols BTCUSD,SOLUSD,XAUTUSD,BNBUSD,LTCUSD --min-trades 50
```

Searches 60 combinations per symbol: 4 RSI pairs × 5 VWAP band widths × 3 wick ratios. Results are ranked by expectancy (R/trade). Once best params are identified, write them to `config/{DELTASYM}.json`.

The binding gate is the one with the lowest pass count in the summary log — loosen that threshold first when the signal frequency is too low.

---

> High-leverage trading carries significant risk of total capital loss. Always validate on dry run and backtest before deploying real capital.
