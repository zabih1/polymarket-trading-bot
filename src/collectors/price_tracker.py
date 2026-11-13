"""Live reference-price aggregation with cross-venue divergence checks."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Iterable
from typing import Protocol

import websockets

from ..config import (
    BINANCE_WS_URL,
    COINBASE_WS_URL,
    DEFAULT_PRICE_FEEDS,
    MAX_PRICE_STALENESS_SECONDS,
    POLYMARKET_LIVE_DATA_WS_URL,
    PRICE_FEED_PRIORITY,
)

logger = logging.getLogger(__name__)
SUPPORTED_FEEDS = frozenset({"polymarket", "binance", "coinbase"})
POLYMARKET_HEARTBEAT_SECONDS = 5.0


class _TextSender(Protocol):
    async def send(self, message: str) -> object: ...


def _decode_json_object(raw: str | bytes) -> dict[str, object] | None:
    """Return a JSON object, ignoring WebSocket control and malformed frames."""
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            return None

    message = raw.strip()
    if not message or message.upper() in {"PING", "PONG"}:
        return None

    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        logger.debug("Ignoring non-JSON price-stream frame: %r", message[:200])
        return None
    return payload if isinstance(payload, dict) else None


def _polymarket_value(message: dict[str, object], symbol: str) -> float | None:
    """Extract the newest value for ``symbol`` from an RTDS update or snapshot."""
    payload = message.get("payload")
    if not isinstance(payload, dict):
        return None
    if str(payload.get("symbol", "")).lower() != symbol.lower():
        return None

    raw_value = payload.get("value")
    if raw_value is None:
        history = payload.get("data")
        if not isinstance(history, list) or not history:
            return None
        latest = history[-1]
        if not isinstance(latest, dict):
            return None
        raw_value = latest.get("value")

    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


class PriceTracker:
    """Maintain synchronized prices from the feeds used by the strategy.

    ``price`` follows the reference engine's priority semantics. ``divergence``
    is intentionally unavailable until both Binance and Coinbase have data;
    callers must not silently convert a missing safety signal to zero.
    """

    def __init__(
        self,
        symbol: str = "btc",
        feeds: Iterable[str] = DEFAULT_PRICE_FEEDS,
        priority: Iterable[str] = PRICE_FEED_PRIORITY,
        max_staleness_seconds: float = MAX_PRICE_STALENESS_SECONDS,
    ) -> None:
        self.symbol = symbol.lower()
        self.feeds = tuple(dict.fromkeys(feed.lower() for feed in feeds))
        unknown = set(self.feeds) - SUPPORTED_FEEDS
        if unknown:
            raise ValueError(f"Unsupported price feed(s): {', '.join(sorted(unknown))}")

        self.priority = tuple(feed for feed in priority if feed in self.feeds)
        self.max_staleness_seconds = max_staleness_seconds
        self._values: dict[str, float] = {}
        self._updated_monotonic: dict[str, float] = {}

    def _set(self, feed: str, value: float) -> None:
        if value <= 0:
            return
        self._values[feed] = value
        self._updated_monotonic[feed] = time.monotonic()

    def _fresh_value(self, feed: str) -> float | None:
        value = self._values.get(feed)
        updated = self._updated_monotonic.get(feed)
        if value is None or updated is None:
            return None
        if time.monotonic() - updated > self.max_staleness_seconds:
            return None
        return value

    @property
    def price(self) -> float | None:
        for feed in self.priority:
            value = self._fresh_value(feed)
            if value is not None:
                return value
        return None

    @property
    def current_price(self) -> float | None:
        """Compatibility alias for the former Binance-only tracker."""
        return self.price

    @property
    def polymarket_price(self) -> float | None:
        return self._fresh_value("polymarket")

    @property
    def binance_price(self) -> float | None:
        return self._fresh_value("binance")

    @property
    def coinbase_price(self) -> float | None:
        return self._fresh_value("coinbase")

    @property
    def divergence(self) -> float | None:
        binance = self.binance_price
        coinbase = self.coinbase_price
        if binance is None or coinbase is None:
            return None
        return abs(binance - coinbase)

    @property
    def is_ready(self) -> bool:
        return bool(self.feeds) and all(self._fresh_value(feed) is not None for feed in self.feeds)

    async def wait_for_ready(
        self,
        stop_event: asyncio.Event,
        timeout_seconds: float | None = None,
    ) -> bool:
        started = time.monotonic()
        while not stop_event.is_set():
            if self.is_ready:
                return True
            if timeout_seconds is not None and time.monotonic() - started >= timeout_seconds:
                return False
            await asyncio.sleep(0.1)
        return False

    async def stream(self, stop_event: asyncio.Event) -> None:
        streams = {
            "polymarket": self._stream_polymarket,
            "binance": self._stream_binance,
            "coinbase": self._stream_coinbase,
        }
        tasks = [
            asyncio.create_task(streams[feed](stop_event), name=f"price-{feed}")
            for feed in self.feeds
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _stream_binance(self, stop_event: asyncio.Event) -> None:
        url = f"{BINANCE_WS_URL}/{self.symbol}usdt@ticker"
        while not stop_event.is_set():
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    while not stop_event.is_set():
                        try:
                            payload = json.loads(await asyncio.wait_for(ws.recv(), 2.0))
                        except TimeoutError:
                            continue
                        self._set("binance", float(payload.get("c", 0.0)))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Binance price stream dropped: %s", exc)
                await self._backoff(stop_event)

    async def _stream_coinbase(self, stop_event: asyncio.Event) -> None:
        product_id = f"{self.symbol.upper()}-USD"
        while not stop_event.is_set():
            try:
                async with websockets.connect(
                    COINBASE_WS_URL, ping_interval=20, ping_timeout=20
                ) as ws:
                    await ws.send(
                        json.dumps(
                            {
                                "type": "subscribe",
                                "product_ids": [product_id],
                                "channels": ["ticker"],
                            }
                        )
                    )
                    while not stop_event.is_set():
                        try:
                            payload = json.loads(await asyncio.wait_for(ws.recv(), 2.0))
                        except TimeoutError:
                            continue
                        if payload.get("type") == "ticker":
                            self._set("coinbase", float(payload.get("price", 0.0)))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Coinbase price stream dropped: %s", exc)
                await self._backoff(stop_event)

    async def _stream_polymarket(self, stop_event: asyncio.Event) -> None:
        market = f"{self.symbol}/usd"
        subscription = {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "update",
                }
            ],
        }
        while not stop_event.is_set():
            try:
                async with websockets.connect(
                    POLYMARKET_LIVE_DATA_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    await ws.send(json.dumps(subscription))
                    heartbeat_task = asyncio.create_task(
                        self._polymarket_heartbeat(ws, stop_event),
                        name="polymarket-price-heartbeat",
                    )
                    try:
                        while not stop_event.is_set():
                            try:
                                raw = await asyncio.wait_for(ws.recv(), 2.0)
                            except TimeoutError:
                                continue
                            payload = _decode_json_object(raw)
                            if payload is None:
                                continue
                            value = _polymarket_value(payload, market)
                            if value is not None:
                                self._set("polymarket", value)
                    finally:
                        heartbeat_task.cancel()
                        await asyncio.gather(heartbeat_task, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Polymarket price stream dropped: %s", exc)
                await self._backoff(stop_event)

    @staticmethod
    async def _polymarket_heartbeat(ws: _TextSender, stop_event: asyncio.Event) -> None:
        """Send the RTDS application heartbeat until shutdown or disconnection."""
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=POLYMARKET_HEARTBEAT_SECONDS)
            except TimeoutError:
                await ws.send("PING")

    @staticmethod
    async def _backoff(stop_event: asyncio.Event) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=1.0)
        except TimeoutError:
            pass
