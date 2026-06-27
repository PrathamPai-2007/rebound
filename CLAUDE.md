# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the bot

```bash
pip install -r requirements.txt

# Dry run (default) — streams live data, logs signals, no orders placed
python bot.py

# Live trading
DRY_RUN=false python bot.py

# Testnet
USE_TESTNET=true python bot.py

# Verbose tick logging
LOG_LEVEL=DEBUG python bot.py
```

`DRY_RUN=true` is the default. Only `DELTA_API_KEY` and `DELTA_API_SECRET` in `.env` are mandatory.

## Architecture

Two persistent WebSocket connections to `wss://socket.india.delta.exchange`, plus one `aiohttp` REST session. Bot runs **3 async tasks** in dry-run mode, **4 in live** (`bot.py`):

- **`_stream_public`** — single task reading the public `ob_l2` channel. All 5 symbols are multiplexed on one connection; messages are fanned out by `product_symbol` to the correct `SymbolEngine`.
- **`_stream_private`** — *(live only, skipped in DRY_RUN)* single task reading the private channel (after `key-auth`). Routes by `msg["type"]`: `positions` → `RiskManager.on_position_update`, `margins` → `on_balance_update`, `orders` → `on_order_update`. Each handler dispatch is wrapped in try/except so a parse bug logs instead of killing the stream.
- **`heartbeat_watchdog`** — per-stream liveness. `DeltaWSClient` tracks `_last_public`/`_last_private` (monotonic); if a stream goes silent > `WS_HEARTBEAT_INTERVAL * 2` it force-closes that connection so its listen loop reconnects. Catches exchange-side data stalls that the `websockets` ping/pong (live-TCP only) misses.
- **`_summary_loop`** — every `SUMMARY_INTERVAL` logs one INFO line: per-symbol gate pass counts (delta), signals, paper W/L + win-rate, live W/L, equity.

Order placement is REST-only (Delta WS does not support order submission). `DeltaRestClient` in `core/rest_client.py` handles all order calls with HMAC-SHA256 signed headers.

## Key data flow

```
ob_l2 WS message (Delta format: buy[]/sell[] with price/quantity strings)
  → Tick(price=mid, volume=top-of-book sum, timestamp)
  → SymbolEngine.on_tick()
      → VWAPIndicator   (rolling volume-weighted mean + SD bands)
      → RSIIndicator    (Wilder-smoothed, incremental)
      → CandleBuffer    (OHLCV aggregation + wick-rejection check)
  → SignalResult (LONG/SHORT + entry/sl/tp prices)   [one per closed candle — see signal dedupe]
  → RiskManager.place_bracket()
      → _calc_size() → integer contracts (risk-based, margin-checked)
      → DeltaRestClient.place_bracket() → POST /v2/orders (market entry + bracket_* attach)
```

## Signal dedupe

`SymbolEngine` emits at most one signal per closed candle. `CandleBuffer.feed()` returns a non-None candle only on the tick that closes the bucket (timestamp boundary), so `on_tick` evaluates gates exactly once per candle. The symbol-level cooldown (`_in_cooldown`) is independent — it blocks all signals for a symbol after a position closes, not just duplicate signals.

## Outcome tracking & instrumentation

`core/trade_tracker.py` `TradeTracker` is the single sink for outcomes → `cfg.TRADES_CSV` (rich rows: R-multiple, MFE/MAE, duration, entry RSI/VWAP, gate flags, `mode`):
- **paper** — every signal opens a virtual trade resolved against later ticks (`on_tick`: first touch TP=win, SL=loss; gap-beyond-both → SL). Works in `DRY_RUN` where no real orders exist. A paper cooldown (`PAPER_COOLDOWN_SEC`) mirrors the live post-close cooldown so dry-run firing rate ≈ live.
- **live** — `RiskManager.record_live()` on real position close; `pnl_r` from `realised_pnl` (tracked per symbol off the positions stream), outcome inferred from pnl sign.

The three entry gates are evaluated as named booleans in `SymbolEngine.on_tick`; `_gate_counts` accumulates per-closed-candle pass counts (exposed via `gate_stats`). The binding gate = lowest pass count → loosen that one first. `analyse.py` (pure stdlib) reads `trades.csv` → win%, expectancy (R), profit factor, max drawdown, per-symbol/side/mode; `--mode paper|live` filters.

## Order sizing & equity

`RiskManager._calc_size()` returns **integer contracts**, not coin/USD:
`coin_qty = (equity * RISK_PER_TRADE_PCT/100) / |entry - sl|`, then `size = max(1, round(coin_qty / contract_value))`. Rejects (returns 0) if required margin (`entry * size * contract_value / leverage`) exceeds equity. `contract_value` is loaded per symbol at startup from `/v2/products` (BTC 0.001, SOL 1.0, XAUT 0.001, BNB 0.1, LTC 1.0).

Equity = wallet `balance` + `unrealised_pnl`. The `margins` stream is authoritative for upnl; positions-stream sum is only a fallback when margins doesn't carry it. `_initial_equity` is latched once on the first positive equity reading and drives the drawdown guard.

## WebSocket auth

Private connection uses `key-auth` message type (required since Oct 2025 — old `auth` type is dead). Signature: `HMAC-SHA256(api_secret, "GET" + timestamp + "/live")`. The `/live` is part of the **signature payload only** — it is NOT a URL suffix (the WS URL has no path). Getting this wrong is the `invalid_signature` error. Both streams reconnect with exponential backoff (1s → 60s); private stream re-authenticates on every reconnect.

WS URL: `wss://socket.india.delta.exchange` (no `/live` suffix — that path returns 404).

## Symbol mapping

Delta Exchange India uses `USD` suffix (not `USDT`). Internal symbols map to Delta format via `cfg.SYMBOL_MAP` and helpers `cfg.to_delta_symbol()` / `cfg.from_delta_symbol()`:

| Internal | Delta API |
|---|---|
| `BTC/USDT` | `BTCUSD` |
| `SOL/USDT` | `SOLUSD` |
| `XAUT/USDT` | `XAUTUSD` (Tether Gold — only gold perp available) |
| `BNB/USDT` | `BNBUSD` |
| `LTC/USDT` | `LTCUSD` |

ETH, DOGE, and XRP were evaluated and dropped: ETH had negative expectancy (−0.052R/trade over 130 trades), DOGE and XRP had sub-0.03R/trade edge that evaporates with any spread.

## Per-symbol config files

Each active symbol has a JSON file in `config/{DELTASYM}.json` (e.g. `config/BNBUSD.json`) that overrides the 8 tunable strategy params for that symbol. Loaded at startup by `cfg._load_symbol_configs()` into `cfg._symbol_configs`.

**Priority**: explicit env var > JSON file > global `cfg.*` default. This means the optimizer (which sets env vars per subprocess) always wins over JSON, so optimizer sweeps work correctly.

Use `cfg.for_symbol(internal_symbol, key)` anywhere you need a per-symbol param — `SymbolEngine`, `RiskManager._calc_size()`, etc. already use it.

Tunable keys: `RSI_PERIOD`, `RSI_OVERSOLD`, `RSI_OVERBOUGHT`, `VWAP_WINDOW`, `VWAP_BAND_SD`, `WICK_RATIO`, `SL_TICKS`, `RISK_PER_TRADE_PCT`.

## Strategy parameters

Global defaults in `config.py` as `cfg.*`, sourced from env vars. Per-symbol JSON files override these — see above.

| `cfg` field | Env var | Default | What it controls |
|---|---|---|---|
| `VWAP_WINDOW` | `VWAP_WINDOW` | 150 ticks | Rolling window for VWAP + SD bands |
| `VWAP_BAND_SD` | `VWAP_BAND_SD` | 1.75 | SD multiplier for upper2/lower2 entry bands |
| `RSI_PERIOD` | `RSI_PERIOD` | 7 | RSI lookback (Wilder smoothing) |
| `RSI_OVERSOLD` / `RSI_OVERBOUGHT` | `RSI_OVERSOLD` / `RSI_OVERBOUGHT` | 35 / 65 | Signal thresholds |
| `CANDLE_TIMEFRAME` | `CANDLE_TIMEFRAME` | `1m` | Candle bucket size for `CandleBuffer` |
| `WICK_RATIO` | `WICK_RATIO` | 1.5 | Min wick-to-body ratio for wick-rejection gate |
| `SL_TICKS` | `SL_TICKS` | 3 | Ticks past candle wick for stop-loss |
| `RISK_PER_TRADE_PCT` | `RISK_PER_TRADE_PCT` | 1.0 | Fraction of equity risked per trade (drives sizing) |
| `BRACKET_SLIPPAGE_TICKS` | `BRACKET_SLIPPAGE_TICKS` | 5 | Stop-limit exit limit price offset past trigger, so SL fills like a market on a fast move |
| `MAX_EQUITY_DRAWDOWN_PCT` | `MAX_EQUITY_DRAWDOWN_PCT` | 10.0 | Triggers emergency market-close of all positions |
| `PAPER_EQUITY` | `PAPER_EQUITY` | 10000.0 | Simulated equity used for sizing in DRY_RUN mode |
| `SUMMARY_INTERVAL` | `SUMMARY_INTERVAL_SEC` | 300s | How often the summary log line fires |
| `LEVERAGE` (crypto) | `CRYPTO_LEVERAGE` | 50 | Leverage for BTC/SOL/BNB/LTC |
| `LEVERAGE` (commodity) | `COMMODITY_LEVERAGE` | 30 | Leverage for XAUT |
| `BLOCKED_HOURS_UTC` | `BLOCKED_HOURS_UTC` | `4,11,15` | Comma-separated UTC hours where signals are suppressed (market opens where mean-reversion edge flips negative: India open, London→NY handoff, US open) |

## Cooldown mechanism

After any position closes (detected by `size == 0` in the `positions` WS message), `SymbolEngine.enter_cooldown()` blocks signals for that symbol. `RiskManager._schedule_cooldown_exit()` clears it after `COOLDOWN_SEC` (default **0s** — effectively disabled). Paper trades use a separate `PAPER_COOLDOWN_SEC` (default **0s**) inside `TradeTracker`. Set either to a non-zero value via env var to re-enable.

## Time-based trading filter

`SymbolEngine.on_tick()` checks `int(tick.timestamp // 3_600_000) % 24` against `cfg.BLOCKED_HOURS_UTC` and returns `None` immediately for blocked hours. Uses the tick's own timestamp (not wall clock) so the backtester respects it too. Default blocked hours: 4, 11, 15 UTC (09:30, 16:30, 20:30 IST) — the three hours where backtest expectancy was negative.

Override: `BLOCKED_HOURS_UTC=4,11,15,16 python bot.py` or clear with `BLOCKED_HOURS_UTC= python bot.py`.

## Telegram notifications

`core/trade_tracker.py` sends a Telegram message on paper trade open, paper trade close, and live trade close via `_notify()` — a module-level function using `urllib.request` (stdlib, no new dependency). No-op if `TELEGRAM_TOKEN` or `TELEGRAM_CHAT_ID` env vars are not set.

Add to `.env`:
```
TELEGRAM_TOKEN=<bot token from @BotFather>
TELEGRAM_CHAT_ID=<your chat id from getUpdates>
```

## Adding a new symbol

1. Add to `cfg.SYMBOLS`, `cfg.LEVERAGE`, `cfg.SYMBOL_MAP`, and `cfg._reverse_symbol_map` in `config.py`.
2. Use the `USD`-suffix symbol from `/v2/products` (contract_type=`perpetual_futures`).
3. Create `config/{DELTASYM}.json` with tuned params (copy any existing file as a template; defaults are fine to start).
4. Run `python fetch.py --symbols {DELTASYM}` then `python optimize.py --symbols {DELTASYM} --min-trades 50` to find best params, then update the JSON.

## REST API notes

`DeltaRestClient.place_bracket()` posts a **market entry to `POST /v2/orders`** with bracket exits attached: `bracket_stop_loss_price`/`bracket_stop_loss_limit_price`, `bracket_take_profit_price`/`bracket_take_profit_limit_price`, `bracket_stop_trigger_method`. Delta auto-creates the exits on fill. Note: the standalone `/v2/orders/bracket` endpoint attaches SL/TP to an *existing* position — it is NOT an entry+bracket combo, so it's not used here. Bracket exits are **stop-limit** (no market exit), which is why `RiskManager` offsets the limit price past the trigger by `BRACKET_SLIPPAGE_TICKS`.

`get_products()` is **unsigned** (public endpoint). All other calls go through `_sign()`; prehash = `method + timestamp + path + body` (REST auth differs from WS auth). `_read()` surfaces non-JSON / 5xx bodies instead of letting `resp.json()` raise opaquely.

**Live order acceptance is unverified until the account is funded** — the first live bracket must be eyeballed on the exchange UI.

## trades.csv schema

Columns written by `core/trade_tracker.py` (defined in `CSV_FIELDS`):

`mode, symbol, side, opened_at, closed_at, duration_s, entry, sl, tp, exit, outcome, risk, pnl_r, reward_risk, mfe_r, mae_r, rsi_at_entry, vwap_at_entry, gate_band, gate_rsi, gate_wick`

- `mode`: `paper`, `live`, or `backtest`
- `pnl_r`: R-multiple (1R = |entry − sl|). Hit SL → −1.0, hit TP → +reward_risk
- `mfe_r` / `mae_r`: max favorable / adverse excursion in R (paper/backtest only; blank for live)
- `gate_*`: 0/1 booleans for which gates triggered the signal

## Historical data fetcher

`fetch.py` pulls OHLCV candles from `GET /v2/history/candles` (public, no auth) and stores them as Parquet files in `data/{symbol}_{resolution}.parquet`. Re-runs append and deduplicate by timestamp — safe to run incrementally.

```bash
# All active symbols, 1m, last 30 days (default)
python fetch.py

# Subset, custom range
python fetch.py --symbols BTCUSD,LTCUSD --resolution 1m --start 2025-12-27
```

Parquet schema: `time` (int64 unix s), `open/high/low/close/volume` (float64). Uses pure `pyarrow` — no pandas. Fetches in 7-day chunks to avoid API result-count limits.

## Backtester

`backtest.py` replays parquet candles through the live `SymbolEngine` (same indicators, gates, SL/TP logic) and writes outcomes to a CSV with `mode=backtest`.

```bash
# Fetch data first, then backtest all active symbols
python fetch.py --start 2025-12-27
python backtest.py --out trades_6m.csv
python analyse.py --file trades_6m.csv --mode backtest
```

Key design: each OHLCV candle is synthesized into 4 `Tick` objects (open→high→low→close, all within the same 60s bucket) so `CandleBuffer` reconstructs identical OHLCV including wick geometry. The signal fires on the first tick of the *next* candle (natural bucket boundary), matching live behavior. SL/TP resolution checks each subsequent candle's `high`/`low` range. Cooldown uses simulated candle timestamps (`COOLDOWN_SEC` from config), not wall clock.

## Parameter sweep (Optimizer)

`optimize.py` runs a grid search over RSI thresholds, VWAP band width, and wick ratio to find parameter combinations with the highest expectancy for a given symbol. Combos run in parallel via `ThreadPoolExecutor` (uses all CPU cores); results table prints when the full symbol sweep completes.

```bash
python optimize.py                               # BTCUSD + XAUTUSD, 1m, data/
python optimize.py --symbols LTCUSD
python optimize.py --symbols BTCUSD,LTCUSD --resolution 5m
python optimize.py --min-trades 50              # raise minimum trade count filter (default 15)
```

Grid searched: RSI pairs `[(30,70),(35,65),(40,60)]` × VWAP band SDs `[1.00,1.25,1.50]` × wick ratios `[0.0,0.5,1.0]` = **27 combos per symbol**. Each combo spawns `backtest.py` as a subprocess with env-var overrides, reads the output CSV via `stats_from_csv()`. Workers default to `cpu_count() // 2`. After choosing best params, update the symbol's `config/{DELTASYM}.json`.

Note on `wick_ratio=0.0`: the wick size check (`lower_wick >= ratio * body`) becomes `>= 0`, always true — only the directional close check (close in upper/lower half of candle range) remains active.

## Smoke test

No test suite exists. Import check as sanity gate:

```bash
python -c "import bot, config, core.risk_manager, core.rest_client, strategy.indicators, strategy.signal_engine, analyse, optimize; print('OK')"
```
