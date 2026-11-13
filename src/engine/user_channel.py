"""Simulation fill observer with reference-compatible settlement timing."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..config import SIM_SETTLEMENT_DELAY_MS
from .client import is_sim_filled
from .types import BookSnapshot, OrderRequest

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _TrackedOrder:
    request: OrderRequest
    matched_at_ms: float | None = None


class UserChannel:
    """Observe resting simulated orders and dispatch one settled fill event.

    Order expiration is intentionally owned by ``MarketLifecycle`` because an
    exchange cancellation must complete before wallet reservations are released.
    """

    def __init__(
        self,
        book_provider: Callable[[str], BookSnapshot],
        settlement_delay_ms: int = SIM_SETTLEMENT_DELAY_MS,
        time_ms: Callable[[], float] | None = None,
    ) -> None:
        self._book_provider = book_provider
        self._settlement_delay_ms = settlement_delay_ms
        self._time_ms = time_ms or (lambda: time.time() * 1000.0)
        self._tracked: dict[str, _TrackedOrder] = {}

    async def start(self, condition_id: str) -> None:
        """Simulation needs no authenticated stream."""

    def track_order(
        self,
        order_id: str,
        request: OrderRequest,
        *,
        matched: bool = False,
    ) -> None:
        self._tracked[order_id] = _TrackedOrder(
            request=request,
            matched_at_ms=self._time_ms() if matched else None,
        )

    def untrack_order(self, order_id: str) -> None:
        self._tracked.pop(order_id, None)

    def is_tracked(self, order_id: str) -> bool:
        return order_id in self._tracked

    def is_matched(self, order_id: str) -> bool:
        tracked = self._tracked.get(order_id)
        return tracked is not None and tracked.matched_at_ms is not None

    def get_matched_so_far(self, order_id: str) -> float:
        """Return settled partial shares; simulation fills are all-or-nothing."""
        return 0.0

    def check_fills(self) -> None:
        now_ms = self._time_ms()
        completed: list[str] = []

        for order_id, tracked in tuple(self._tracked.items()):
            request = tracked.request
            if tracked.matched_at_ms is None and is_sim_filled(
                request.action,
                request.price,
                request.shares,
                self._book_provider(request.token_id),
            ):
                tracked.matched_at_ms = now_ms

            if tracked.matched_at_ms is None:
                continue
            if now_ms - tracked.matched_at_ms < self._settlement_delay_ms:
                continue

            completed.append(order_id)
            if request.on_filled is not None:
                try:
                    request.on_filled(request.shares)
                except Exception:
                    logger.exception("Fill callback failed for simulated order %s", order_id)

        for order_id in completed:
            self.untrack_order(order_id)

    def check_fills_and_expiries(self) -> None:
        """Compatibility alias; expirations are no longer processed here."""
        self.check_fills()

    async def close(self) -> None:
        self._tracked.clear()


@dataclass(slots=True)
class _LiveTrackedOrder:
    request: OrderRequest
    matched: bool
    associated_trades: set[str]
    mined_amounts: dict[str, float]


class LiveUserChannel:
    """Consume authenticated order/trade events from the unified SDK."""

    def __init__(self, subscribe: Callable[[str], Any]) -> None:
        self._subscribe = subscribe
        self._tracked: dict[str, _LiveTrackedOrder] = {}
        self._pending_matches: dict[str, set[str]] = {}
        self._pending_mined: dict[str, dict[str, float]] = {}
        self._handle: Any = None
        self._consumer: asyncio.Task[None] | None = None

    async def start(self, condition_id: str) -> None:
        self._handle = await self._subscribe(condition_id)
        self._consumer = asyncio.create_task(
            self._consume(), name=f"user-events-{condition_id[:10]}"
        )

    def track_order(
        self,
        order_id: str,
        request: OrderRequest,
        *,
        matched: bool = False,
    ) -> None:
        associated = self._pending_matches.pop(order_id, set())
        mined = self._pending_mined.pop(order_id, {})
        self._tracked[order_id] = _LiveTrackedOrder(
            request=request,
            matched=matched or bool(associated),
            associated_trades=associated,
            mined_amounts=mined,
        )
        self._try_settle(order_id)

    def untrack_order(self, order_id: str) -> None:
        tracked = self._tracked.get(order_id)
        if tracked is not None and tracked.matched:
            return
        self._tracked.pop(order_id, None)
        self._pending_matches.pop(order_id, None)
        self._pending_mined.pop(order_id, None)

    def is_tracked(self, order_id: str) -> bool:
        return order_id in self._tracked

    def is_matched(self, order_id: str) -> bool:
        tracked = self._tracked.get(order_id)
        return tracked is not None and tracked.matched

    def get_matched_so_far(self, order_id: str) -> float:
        tracked = self._tracked.get(order_id)
        return sum(tracked.mined_amounts.values()) if tracked is not None else 0.0

    def check_fills(self) -> None:
        """Live fills are event-driven."""

    async def _consume(self) -> None:
        try:
            async for event in self._handle:
                payload = event.payload
                if event.type == "order":
                    self._process_order(payload)
                elif event.type == "trade":
                    self._process_trade(payload)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Authenticated Polymarket user stream stopped")

    @staticmethod
    def _value(value: Any) -> str:
        return str(getattr(value, "value", value)).upper()

    def _process_order(self, payload: Any) -> None:
        order_id = str(payload.id)
        event_type = self._value(payload.order_event_type)
        status = self._value(payload.status)
        if event_type == "UPDATE" and status == "MATCHED":
            trades = tuple(payload.associate_trades or ())
            tracked = self._tracked.get(order_id)
            if tracked is None:
                self._pending_matches.setdefault(order_id, set()).update(trades)
                return
            tracked.matched = True
            tracked.associated_trades.update(trades)
            self._try_settle(order_id)
        elif event_type == "CANCELLATION":
            tracked = self._tracked.get(order_id)
            if tracked is None:
                return
            self._tracked.pop(order_id, None)
            if tracked.request.on_failed is not None:
                tracked.request.on_failed(status.lower())

    def _mark_matched(self, order_id: str, trade_id: str) -> None:
        if not order_id:
            return
        tracked = self._tracked.get(order_id)
        if tracked is not None:
            tracked.matched = True
            tracked.associated_trades.add(trade_id)
            self._try_settle(order_id)
            return
        self._pending_matches.setdefault(order_id, set()).add(trade_id)

    def _store_mined(self, order_id: str, trade_id: str, amount: float) -> None:
        tracked = self._tracked.get(order_id)
        if tracked is not None:
            tracked.mined_amounts[trade_id] = amount
            self._try_settle(order_id)
            return
        if order_id in self._pending_matches:
            self._pending_mined.setdefault(order_id, {})[trade_id] = amount

    def _process_trade(self, payload: Any) -> None:
        trade_id = str(payload.id)
        status = self._value(payload.status)
        maker_orders = payload.maker_orders or ()
        if status == "MATCHED":
            self._mark_matched(str(payload.taker_order_id or ""), trade_id)
            for maker in maker_orders:
                self._mark_matched(str(maker.order_id), trade_id)
            return
        if status != "MINED":
            return
        taker_id = str(payload.taker_order_id or "")
        if taker_id:
            self._store_mined(taker_id, trade_id, float(payload.size))
        for maker in maker_orders:
            self._store_mined(str(maker.order_id), trade_id, float(maker.matched_amount))

    def _try_settle(self, order_id: str) -> None:
        tracked = self._tracked.get(order_id)
        if tracked is None or not tracked.matched or not tracked.associated_trades:
            return
        if not tracked.associated_trades.issubset(tracked.mined_amounts):
            return
        shares = sum(tracked.mined_amounts[trade_id] for trade_id in tracked.associated_trades)
        self._tracked.pop(order_id, None)
        if tracked.request.on_filled is not None:
            tracked.request.on_filled(shares)

    async def close(self) -> None:
        if self._consumer is not None:
            self._consumer.cancel()
            await asyncio.gather(self._consumer, return_exceptions=True)
            self._consumer = None
        if self._handle is not None:
            await self._handle.close()
            self._handle = None
        self._tracked.clear()
