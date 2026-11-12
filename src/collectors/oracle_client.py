"""
Polymarket Oracle & Settlement Price API Client.
"""

import logging
from typing import Dict, Any, Optional
import requests

try:
    from ..config import PRICE_API_URL
except (ImportError, ValueError):
    from src.config import PRICE_API_URL

logger = logging.getLogger(__name__)

class OracleClient:
    """
    Client for querying Polymarket's Chainlink-settled crypto prices
    and verifying window resolution results.
    """

    def __init__(self, api_url: str = PRICE_API_URL):
        self.api_url = api_url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        })

    def fetch_settlement_price(
        self,
        start_time: int,
        end_time: int,
        symbol: str = "BTC",
        window: str = "5m",
        timeout: float = 10.0
    ) -> Optional[Dict[str, Any]]:
        """
        Fetches open price (Price to Beat) and settlement close price for a market slot.
        """
        variant = "fiveminute" if window == "5m" else "fifteen"
        params = {
            "symbol": symbol.upper(),
            "variant": variant,
            "eventStartTime": str(start_time),
            "endDate": str(end_time)
        }

        try:
            resp = self.session.get(self.api_url, params=params, timeout=timeout)
            if resp.status_code == 200:
                data = resp.json()
                open_price = data.get("openPrice")
                close_price = data.get("closePrice")
                completed = bool(data.get("completed", False))

                winner = None
                if completed and open_price is not None and close_price is not None:
                    winner = "UP" if close_price >= open_price else "DOWN"

                return {
                    "start_time": start_time,
                    "end_time": end_time,
                    "symbol": symbol.upper(),
                    "open_price": open_price,
                    "close_price": close_price,
                    "completed": completed,
                    "winner": winner
                }
            else:
                logger.warning(f"Oracle price API returned HTTP {resp.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error querying settlement price ({start_time}-{end_time}): {e}")
            return None
