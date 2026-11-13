"""Backward-compatible Binance-only price tracker.

New code should use :class:`src.collectors.price_tracker.PriceTracker`.
"""

from __future__ import annotations

import asyncio

from .price_tracker import PriceTracker


class BinanceTickerTracker(PriceTracker):
    def __init__(self, symbol: str = "btc", base_ws_url: str | None = None) -> None:
        # ``base_ws_url`` is retained for compatibility with older call sites.
        # Endpoint overrides now belong in runtime configuration.
        del base_ws_url
        super().__init__(symbol=symbol, feeds=("binance",), priority=("binance",))

    async def stream_ticker(self, stop_event: asyncio.Event) -> None:
        await self.stream(stop_event)
