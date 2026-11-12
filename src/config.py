"""
Configuration constants and endpoints for the Polymarket Trading System.
"""

from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Polymarket API Endpoints
GAMMA_API_URL = "https://gamma-api.polymarket.com/events"
CLOB_REST_URL = "https://clob.polymarket.com/book"
CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PRICE_API_URL = "https://polymarket.com/api/crypto/crypto-price"

# Proxy Exchanges
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"

# Market Defaults
DEFAULT_ASSET = "btc"
DEFAULT_WINDOW = "5m"
INTERVAL_SECONDS = {
    "5m": 300,
    "15m": 900
}
BASE_TIMESTAMP = 1772568900
