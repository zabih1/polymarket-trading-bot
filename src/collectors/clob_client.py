"""
Polymarket CLOB REST and WebSocket Client for Level-2 Order Book Tracking.
"""

import asyncio
import json
import logging
import time
from typing import Any

import requests
import websockets

try:
    from ..config import CLOB_REST_URL, CLOB_WS_URL
except (ImportError, ValueError):
    from src.config import CLOB_REST_URL, CLOB_WS_URL

logger = logging.getLogger(__name__)


class LocalOrderBook:
    """
    In-memory Level-2 order book maintained via snapshot and delta updates.
    Bids are sorted descending (highest bid first).
    Asks are sorted ascending (lowest ask first).
    """

    def __init__(self, token_id: str):
        self.token_id = token_id
        self.bids: dict[float, float] = {}  # price -> size
        self.asks: dict[float, float] = {}  # price -> size
        self.tick_size: float = 0.01
        self.fee_rate_bps: float = 0.0
        self.last_update_time: float = 0.0
        self.is_ready: bool = False

    def reset(self):
        """Clears existing depth."""
        self.bids.clear()
        self.asks.clear()
        self.is_ready = False

    def apply_snapshot(
        self, bids: list[dict[str, str]], asks: list[dict[str, str]], tick_size: str | None = None
    ):
        """Replaces full book state from snapshot."""
        self.bids.clear()
        self.asks.clear()
        for b in bids:
            self.bids[float(b["price"])] = float(b["size"])
        for a in asks:
            self.asks[float(a["price"])] = float(a["size"])
        if tick_size:
            try:
                self.tick_size = float(tick_size)
            except (ValueError, TypeError):
                pass
        self.last_update_time = time.time()
        self.is_ready = True

    def apply_price_change(self, side: str, price: float, size: float):
        """Applies incremental delta update to the book."""
        target = self.bids if side == "BUY" else self.asks
        if size <= 0:
            target.pop(price, None)
        else:
            target[price] = size
        self.last_update_time = time.time()

    def get_best_bid(self) -> float | None:
        return max(self.bids.keys()) if self.bids else None

    def get_best_ask(self) -> float | None:
        return min(self.asks.keys()) if self.asks else None

    def get_spread(self) -> float | None:
        bid = self.get_best_bid()
        ask = self.get_best_ask()
        return round(ask - bid, 4) if (bid is not None and ask is not None) else None

    def get_best_bid_liquidity(self) -> float | None:
        """USDC liquidity available at the best bid level."""
        price = self.get_best_bid()
        return price * self.bids[price] if price is not None else None

    def get_best_ask_liquidity(self) -> float | None:
        """USDC liquidity available at the best ask level."""
        price = self.get_best_ask()
        return price * self.asks[price] if price is not None else None

    def get_depth_summary(self, levels: int = 5) -> dict[str, Any]:
        """Returns sorted top levels and total liquidity."""
        sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:levels]
        sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])[:levels]
        return {
            "best_bid": self.get_best_bid(),
            "best_ask": self.get_best_ask(),
            "best_bid_liquidity": self.get_best_bid_liquidity(),
            "best_ask_liquidity": self.get_best_ask_liquidity(),
            "spread": self.get_spread(),
            "top_bids": sorted_bids,
            "top_asks": sorted_asks,
            "total_bid_shares": sum(self.bids.values()),
            "total_ask_shares": sum(self.asks.values()),
            "total_bid_liquidity": sum(price * size for price, size in self.bids.items()),
            "total_ask_liquidity": sum(price * size for price, size in self.asks.items()),
            "tick_size": self.tick_size,
            "fee_rate_bps": self.fee_rate_bps,
        }


class ClobClient:
    """
    Manages REST snapshot fetching and continuous WebSocket streaming
    for market token order books.
    """

    def __init__(self, rest_url: str = CLOB_REST_URL, ws_url: str = CLOB_WS_URL):
        self.rest_url = rest_url
        self.ws_url = ws_url
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def fetch_orderbook_rest(self, token_id: str, timeout: float = 5.0) -> dict[str, Any] | None:
        """Fetches instantaneous L2 snapshot via REST endpoint."""
        url = f"{self.rest_url}?token_id={token_id}"
        try:
            resp = self.session.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"CLOB REST book returned HTTP {resp.status_code}")
        except Exception as e:
            logger.error(f"CLOB REST fetch failed for {token_id}: {e}")
        return None

    async def stream_orderbooks(
        self,
        token_ids: list[str],
        up_book: LocalOrderBook,
        down_book: LocalOrderBook,
        up_token_id: str,
        stop_event: asyncio.Event,
    ):
        """
        Subscribes to CLOB WebSocket and maintains live order books
        until stop_event is set. Automatically reconnects on network interruptions.
        """
        while not stop_event.is_set():
            try:
                # ping_interval=None avoids client-side timeout mismatches with Polymarket server
                async with websockets.connect(self.ws_url, ping_interval=None) as ws:
                    sub_msg = {
                        "type": "market",
                        "assets_ids": token_ids,
                        "custom_feature_enabled": True,
                    }
                    await ws.send(json.dumps(sub_msg))
                    logger.debug(f"Subscribed CLOB WS for tokens: {token_ids}")

                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                            data = json.loads(raw)
                            if isinstance(data, list):
                                for item in data:
                                    self._route_event(item, up_book, down_book, up_token_id)
                            elif isinstance(data, dict):
                                self._route_event(data, up_book, down_book, up_token_id)
                        except TimeoutError:
                            continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                if stop_event.is_set():
                    break
                logger.warning(f"CLOB WS connection dropped ({e}), reconnecting in 2s...")
                await asyncio.sleep(2.0)

    def _route_event(
        self,
        data: dict[str, Any],
        up_book: LocalOrderBook,
        down_book: LocalOrderBook,
        up_token_id: str,
    ):
        event_type = data.get("event_type")
        asset_id = data.get("asset_id")
        books = {up_token_id: up_book, down_book.token_id: down_book}
        target_book = books.get(asset_id)

        if event_type == "book":
            if target_book:
                target_book.apply_snapshot(
                    bids=data.get("bids", []),
                    asks=data.get("asks", []),
                    tick_size=data.get("tick_size"),
                )
        elif event_type == "price_change":
            for change in data.get("price_changes", []):
                aid = change.get("asset_id")
                book = books.get(aid)
                if book:
                    try:
                        book.apply_price_change(
                            side=change.get("side", ""),
                            price=float(change.get("price", 0)),
                            size=float(change.get("size", 0)),
                        )
                    except (ValueError, TypeError):
                        pass
        elif event_type == "tick_size_change" and target_book:
            try:
                target_book.tick_size = float(data.get("new_tick_size", target_book.tick_size))
            except (TypeError, ValueError):
                pass
        elif event_type == "last_trade_price" and target_book:
            try:
                target_book.fee_rate_bps = float(data.get("fee_rate_bps", 0.0))
            except (TypeError, ValueError):
                pass
