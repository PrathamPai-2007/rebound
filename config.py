import json
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


_SYMBOL_CONFIG_KEYS = frozenset({
    'RSI_PERIOD', 'RSI_OVERSOLD', 'RSI_OVERBOUGHT',
    'VWAP_WINDOW', 'VWAP_BAND_SD', 'WICK_RATIO',
    'SL_TICKS', 'RISK_PER_TRADE_PCT',
})


class Config:
    # Credentials
    API_KEY: str = os.environ.get("DELTA_API_KEY", "")
    API_SECRET: str = os.environ.get("DELTA_API_SECRET", "")

    # Transport
    USE_TESTNET: bool = os.environ.get("USE_TESTNET", "false").lower() == "true"
    WS_URL: str = "wss://socket.india.delta.exchange"
    WS_TESTNET_URL: str = "wss://socket.testnet.delta.exchange"
    REST_BASE_URL: str = "https://api.india.delta.exchange"
    REST_TESTNET_URL: str = "https://cdn-ind.testnet.deltaex.org"

    # Internal symbol → Delta Exchange India symbol (all use USD, not USDT)
    # XAU/USDT → XAUTUSD (Tether Gold perp, closest gold equivalent on Delta India)
    SYMBOL_MAP: dict[str, str] = {
        "BTC/USDT":  "BTCUSD",
        "SOL/USDT":  "SOLUSD",
        "XAUT/USDT": "XAUTUSD",
        "BNB/USDT":  "BNBUSD",
        "LTC/USDT":  "LTCUSD",
    }
    _reverse_symbol_map: dict[str, str] = {
        "BTCUSD":  "BTC/USDT",
        "SOLUSD":  "SOL/USDT",
        "XAUTUSD": "XAUT/USDT",
        "BNBUSD":  "BNB/USDT",
        "LTCUSD":  "LTC/USDT",
    }

    # Assets and their leverage
    SYMBOLS: list[str] = ["BTC/USDT", "SOL/USDT", "XAUT/USDT", "BNB/USDT", "LTC/USDT"]
    LEVERAGE: dict[str, int] = {
        "BTC/USDT":  int(os.environ.get("CRYPTO_LEVERAGE", 50)),
        "SOL/USDT":  int(os.environ.get("CRYPTO_LEVERAGE", 50)),
        "XAUT/USDT": int(os.environ.get("COMMODITY_LEVERAGE", 30)),
        "BNB/USDT":  int(os.environ.get("CRYPTO_LEVERAGE", 50)),
        "LTC/USDT":  int(os.environ.get("CRYPTO_LEVERAGE", 50)),
    }

    # Strategy
    CANDLE_TIMEFRAME: str = os.environ.get("CANDLE_TIMEFRAME", "1m")
    VWAP_WINDOW: int = int(os.environ.get("VWAP_WINDOW", 150))
    VWAP_BAND_SD: float = float(os.environ.get("VWAP_BAND_SD", 1.75))
    RSI_PERIOD: int = int(os.environ.get("RSI_PERIOD", 7))
    RSI_OVERSOLD: float = float(os.environ.get("RSI_OVERSOLD", 35))
    RSI_OVERBOUGHT: float = float(os.environ.get("RSI_OVERBOUGHT", 65))
    WICK_RATIO: float = float(os.environ.get("WICK_RATIO", 1.5))

    # Order / risk
    RISK_PER_TRADE_PCT: float = float(os.environ.get("RISK_PER_TRADE_PCT", 1.0))
    MAX_EQUITY_DRAWDOWN_PCT: float = float(os.environ.get("MAX_EQUITY_DRAWDOWN_PCT", 10.0))
    SL_TICKS: int = int(os.environ.get("SL_TICKS", 3))
    TP_SD_BAND: int = 1
    # Bracket exit limit price = trigger ± this many ticks, so the stop-limit
    # fills like a market on a fast move instead of being left behind.
    BRACKET_SLIPPAGE_TICKS: int = int(os.environ.get("BRACKET_SLIPPAGE_TICKS", 5))
    BRACKET_TRIGGER_METHOD: str = "last_traded_price"

    # WebSocket resilience
    WS_RECONNECT_BASE_DELAY: float = 1.0
    WS_RECONNECT_MAX_DELAY: float = 60.0
    WS_HEARTBEAT_INTERVAL: float = 30.0    # websockets lib ping_interval

    # Misc
    LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO")
    LOG_FILE: str = "trading_bot.log"
    DRY_RUN: bool = os.environ.get("DRY_RUN", "true").lower() == "true"

    # Outcome tracking / instrumentation
    TRADES_CSV: str = os.environ.get("TRADES_CSV", "trades.csv")
    SUMMARY_INTERVAL: float = float(os.environ.get("SUMMARY_INTERVAL_SEC", 300))
    COOLDOWN_SEC: float = float(os.environ.get("COOLDOWN_SEC", 120.0))
    PAPER_COOLDOWN_SEC: float = float(os.environ.get("PAPER_COOLDOWN_SEC", 120.0))
    PAPER_EQUITY: float = float(os.environ.get("PAPER_EQUITY", 10000.0))

    # Populated by _load_symbol_configs() at module load time
    _symbol_configs: dict = {}

    def _load_symbol_configs(self) -> None:
        cfg_dir = Path(__file__).parent / "config"
        for delta_sym in self.SYMBOL_MAP.values():
            f = cfg_dir / f"{delta_sym}.json"
            if not f.exists():
                continue
            try:
                data = json.loads(f.read_text())
                self._symbol_configs[delta_sym] = {
                    k: v for k, v in data.items() if k in _SYMBOL_CONFIG_KEYS and v is not None
                }
            except Exception as exc:
                import logging
                logging.getLogger(__name__).warning("Failed to load %s: %s", f, exc)

    def for_symbol(self, symbol: str, key: str):
        """Return per-symbol override for key, falling back to global cfg attribute.
        Explicit env vars always win so the optimizer can override per-symbol configs."""
        if key in os.environ:
            return getattr(self, key)
        delta_sym = self.SYMBOL_MAP.get(symbol, symbol)
        return self._symbol_configs.get(delta_sym, {}).get(key, getattr(self, key))

    @property
    def active_ws_url(self) -> str:
        return self.WS_TESTNET_URL if self.USE_TESTNET else self.WS_URL

    @property
    def active_rest_url(self) -> str:
        return self.REST_TESTNET_URL if self.USE_TESTNET else self.REST_BASE_URL

    def to_delta_symbol(self, symbol: str) -> str:
        if symbol not in self.SYMBOL_MAP:
            raise KeyError(f"Unknown symbol '{symbol}' — add it to SYMBOL_MAP in config.py")
        return self.SYMBOL_MAP[symbol]

    def from_delta_symbol(self, delta_symbol: str) -> str:
        return self._reverse_symbol_map.get(delta_symbol, delta_symbol)

    def validate(self) -> None:
        if not self.DRY_RUN:
            if not self.API_KEY or not self.API_SECRET:
                raise EnvironmentError("DELTA_API_KEY and DELTA_API_SECRET must be set when DRY_RUN is false.")


cfg = Config()
cfg._load_symbol_configs()
