"""
Polymarket Gamma API Client for market metadata and discovery.
"""

import json
import logging
from typing import Dict, Any, Optional
import requests

try:
    from ..config import GAMMA_API_URL
except (ImportError, ValueError):
    from src.config import GAMMA_API_URL

logger = logging.getLogger(__name__)

class GammaClient:
    """
    Client for interacting with Polymarket Gamma API to discover
    market conditions, token IDs, and outcomes for rolling windows.
    """

    def __init__(self, api_url: str = GAMMA_API_URL):
        self.api_url = api_url
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def fetch_market_metadata(self, slug: str, timeout: float = 10.0) -> Optional[Dict[str, Any]]:
        """
        Fetches event and market metadata for a specific market slug.
        Returns parsed condition ID, UP token ID, DOWN token ID, and outcome details.
        """
        url = f"{self.api_url}?slug={slug}"
        try:
            resp = self.session.get(url, timeout=timeout)
            if resp.status_code != 200:
                logger.warning(f"Gamma API returned HTTP {resp.status_code} for {slug}")
                return None

            data = resp.json()
            if not data or not isinstance(data, list):
                logger.warning(f"No market records found for slug: {slug}")
                return None

            event = data[0]
            markets = event.get("markets", [])
            if not markets:
                logger.warning(f"No active markets attached to event {event.get('id')}")
                return None

            m = markets[0]
            raw_tokens = m.get("clobTokenIds", "[]")
            clob_tokens = json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens

            raw_outcomes = m.get("outcomes", "[]")
            outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes

            raw_prices = m.get("outcomePrices", "[]")
            outcome_prices = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices

            up_id = clob_tokens[0] if len(clob_tokens) > 0 else None
            down_id = clob_tokens[1] if len(clob_tokens) > 1 else None

            return {
                "slug": slug,
                "event_id": event.get("id"),
                "title": event.get("title"),
                "condition_id": m.get("conditionId"),
                "up_token_id": up_id,
                "down_token_id": down_id,
                "outcomes": outcomes,
                "outcome_prices": outcome_prices,
                "active": event.get("active"),
                "closed": event.get("closed"),
            }
        except Exception as e:
            logger.error(f"Failed to fetch market metadata for {slug}: {e}")
            return None
