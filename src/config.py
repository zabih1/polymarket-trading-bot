"""Runtime configuration for the Polymarket research and trading system.

Configuration is read once at import time. Secrets remain in the environment;
only non-secret defaults live in source control.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env", override=False)

DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Public Polymarket endpoints.
GAMMA_API_URL = "https://gamma-api.polymarket.com/events"
CLOB_REST_URL = "https://clob.polymarket.com/book"
CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYMARKET_LIVE_DATA_WS_URL = "wss://ws-live-data.polymarket.com"
PRICE_API_URL = "https://polymarket.com/api/crypto/crypto-price"

# Reference-price venues.
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"

SUPPORTED_ASSETS = ("btc", "eth", "xrp", "sol", "doge", "hype", "bnb")
INTERVAL_SECONDS = {"5m": 300, "15m": 900}
BASE_TIMESTAMP = 1_772_568_900

DEFAULT_ASSET = os.getenv("MARKET_ASSET", "btc").strip().lower()
DEFAULT_WINDOW = os.getenv("MARKET_WINDOW", "5m").strip().lower()


def _csv_env(name: str, default: str) -> tuple[str, ...]:
    return tuple(
        item.strip().lower() for item in os.getenv(name, default).split(",") if item.strip()
    )


# Late-entry needs Binance and Coinbase to calculate the reference divergence
# guard. Polymarket remains the preferred directional feed.
DEFAULT_PRICE_FEEDS = _csv_env("PRICE_FEEDS", "polymarket,binance,coinbase")
PRICE_FEED_PRIORITY = _csv_env("PRICE_FEED_PRIORITY", "polymarket,binance,coinbase,okx,bybit")
MAX_PRICE_STALENESS_SECONDS = float(os.getenv("MAX_PRICE_STALENESS_SECONDS", "2.0"))

SIM_DELAY_MS = int(os.getenv("SIM_DELAY_MS", "150"))
SIM_SETTLEMENT_DELAY_MS = int(os.getenv("SIM_SETTLEMENT_DELAY_MS", "4000"))
BUY_MAX_RETRIES = int(os.getenv("BUY_MAX_RETRIES", "30"))
BUY_RETRY_DELAY_MS = int(os.getenv("BUY_RETRY_DELAY_MS", "500"))


def validate_market(asset: str, window: str) -> None:
    if asset not in SUPPORTED_ASSETS:
        supported = ", ".join(SUPPORTED_ASSETS)
        raise ValueError(f"Unsupported asset {asset!r}; expected one of: {supported}")
    if window not in INTERVAL_SECONDS:
        supported = ", ".join(INTERVAL_SECONDS)
        raise ValueError(f"Unsupported window {window!r}; expected one of: {supported}")
