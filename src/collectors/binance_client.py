"""
Binance WebSocket Reference Price Feed Client.
"""

import asyncio
import json
import logging
import time
from typing import Optional
import websockets

try:
    from ..config import BINANCE_WS_URL
except (ImportError, ValueError):
    from src.config import BINANCE_WS_URL

logger = logging.getLogger(__name__)

class BinanceTickerTracker:
    """
    Maintains the latest spot price from Binance WebSocket ticker stream.
    """

    def __init__(self, symbol: str = "btc", base_ws_url: str = BINANCE_WS_URL):
        self.symbol = symbol.lower()
        self.base_ws_url = base_ws_url
        self.stream_name = f"{self.symbol}usdt@ticker"
        self.current_price: Optional[float] = None
        self.last_update_time: float = 0.0
        self.is_ready: bool = False

    async def stream_ticker(self, stop_event: asyncio.Event):
        """
        Continuously reads Binance ticker WebSocket and updates price.
        Automatically reconnects if the socket disconnects.
        """
        url = f"{self.base_ws_url}/{self.stream_name}"
        while not stop_event.is_set():
            try:
                async with websockets.connect(url, ping_interval=None) as ws:
                    logger.debug(f"Connected to Binance WS: {self.stream_name}")
                    while not stop_event.is_set():
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                            data = json.loads(msg)
                            price = float(data.get("c", 0))
                            if price > 0:
                                self.current_price = price
                                self.last_update_time = time.time()
                                self.is_ready = True
                        except asyncio.TimeoutError:
                            continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                if stop_event.is_set():
                    break
                logger.warning(f"Binance WS dropped ({e}), reconnecting in 2s...")
                await asyncio.sleep(2.0)
