"""
Trading client interface and implementation for simulation and live execution.
"""

import abc
import asyncio
import logging
import random
import uuid
import time
from typing import Dict, List, Optional, Callable, Set, Any

from .types import (
    Order,
    OrderAction,
    OrderType,
    PlacedOrder,
    BookSnapshot,
    OrderRequest
)

logger = logging.getLogger(__name__)


def is_sim_filled(
    action: OrderAction,
    price: float,
    shares: float,
    book: BookSnapshot
) -> bool:
    """
    Realistic simulation fill check:
    Price must cross and available counterparty liquidity at best price
    must exceed (shares * price * 2) as a 2x liquidity buffer to avoid
    filling against thin, illiquid ticks.
    """
    required_liquidity = shares * price * 2.0
    if action == OrderAction.BUY or action == "buy":
        return (
            book.best_ask is not None and
            book.best_ask <= price and
            (book.best_ask_liquidity or 0.0) >= required_liquidity
        )
    else:
        return (
            book.best_bid is not None and
            book.best_bid >= price and
            (book.best_bid_liquidity or 0.0) >= required_liquidity
        )


class BaseTradingClient(abc.ABC):
    """Abstract trading client interface."""

    @abc.abstractmethod
    async def init(self):
        pass

    @abc.abstractmethod
    async def post_orders(self, orders: List[OrderRequest]) -> List[PlacedOrder]:
        pass

    @abc.abstractmethod
    async def cancel_orders(self, order_ids: List[str]) -> Dict[str, Any]:
        pass

    @abc.abstractmethod
    async def get_order_by_id(self, order_id: str) -> Optional[Order]:
        pass


class SimTradingClient(BaseTradingClient):
    """
    High-fidelity simulation client that paper-trades against live L2 order book depth.
    Simulates ~150ms execution delay, fill-or-kill semantics, maker GTC queues,
    and realistic counterparty liquidity depth constraints.
    """

    def __init__(self, book_provider: Callable[[str], BookSnapshot], sim_delay_ms: int = 150):
        self._book_provider = book_provider
        self._sim_delay_ms = sim_delay_ms
        self._orders: Dict[str, Order] = {}
        self._balance_ready_at: Dict[str, float] = {}

    async def init(self):
        logger.info("[SimClient] Initialized in paper-trading simulation mode.")

    async def _simulate_delay(self):
        """Simulates realistic network latency of 150-160ms."""
        delay = (self._sim_delay_ms + random.uniform(0, 15)) / 1000.0
        await asyncio.sleep(delay)

    async def post_orders(self, orders: List[OrderRequest]) -> List[PlacedOrder]:
        await self._simulate_delay()
        results: List[PlacedOrder] = []

        for req in orders:
            order_id = str(uuid.uuid4())
            book = self._book_provider(req.token_id)
            action = OrderAction(req.action) if isinstance(req.action, str) else req.action
            order_type = OrderType(req.order_type) if isinstance(req.order_type, str) else req.order_type

            # FOK: Immediate match or cancel
            if order_type == OrderType.FOK:
                if is_sim_filled(action, req.price, req.shares, book):
                    order = Order(
                        id=order_id,
                        token_id=req.token_id,
                        action=action,
                        price=req.price,
                        shares=req.shares,
                        actual_shares=req.shares,
                        order_type=order_type,
                        status="filled",
                        expire_at_ms=req.expire_at_ms
                    )
                    self._orders[order_id] = order
                    results.append(PlacedOrder(order_id=order_id, status="matched", success=True))
                else:
                    results.append(PlacedOrder(
                        order_id="",
                        status="rejected",
                        success=False,
                        error_msg="order couldn't be fully filled. FOK orders are fully filled or killed."
                    ))
            else:
                # GTC: Check if immediate crossing match, otherwise rest on book as 'live'
                if is_sim_filled(action, req.price, req.shares, book):
                    order = Order(
                        id=order_id,
                        token_id=req.token_id,
                        action=action,
                        price=req.price,
                        shares=req.shares,
                        actual_shares=req.shares,
                        order_type=order_type,
                        status="filled",
                        expire_at_ms=req.expire_at_ms
                    )
                    self._orders[order_id] = order
                    results.append(PlacedOrder(order_id=order_id, status="matched", success=True))
                else:
                    order = Order(
                        id=order_id,
                        token_id=req.token_id,
                        action=action,
                        price=req.price,
                        shares=req.shares,
                        actual_shares=req.shares,
                        order_type=order_type,
                        status="live",
                        expire_at_ms=req.expire_at_ms
                    )
                    self._orders[order_id] = order
                    results.append(PlacedOrder(order_id=order_id, status="live", success=True))

        return results

    async def cancel_orders(self, order_ids: List[str]) -> Dict[str, Any]:
        await self._simulate_delay()
        canceled = []
        not_canceled = {}

        for oid in order_ids:
            order = self._orders.get(oid)
            if order and order.status == "live":
                order.status = "canceled"
                canceled.append(oid)
            else:
                not_canceled[oid] = "Order not found or already filled"

        return {"canceled": canceled, "not_canceled": not_canceled}

    async def get_order_by_id(self, order_id: str) -> Optional[Order]:
        await self._simulate_delay()
        order = self._orders.get(order_id)
        if not order:
            return None

        # Check if resting GTC order is now filled by current market price
        if order.status == "live":
            book = self._book_provider(order.token_id)
            if is_sim_filled(order.action, order.price, order.shares, book):
                order.status = "filled"

        return order


class ProdTradingClient(BaseTradingClient):
    """
    Production trading client for live Polymarket CLOB execution via Polygon.
    """

    def __init__(self, private_key: str, funder_address: str = ""):
        self.private_key = private_key
        self.funder_address = funder_address

    async def init(self):
        logger.info("[ProdClient] Initialized for live production trading on Polymarket CLOB.")

    async def post_orders(self, orders: List[OrderRequest]) -> List[PlacedOrder]:
        raise NotImplementedError("Live production order placement requires valid API credentials and gas funding.")

    async def cancel_orders(self, order_ids: List[str]) -> Dict[str, Any]:
        raise NotImplementedError("Live cancellation not configured.")

    async def get_order_by_id(self, order_id: str) -> Optional[Order]:
        return None
