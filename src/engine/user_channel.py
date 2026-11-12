"""
User order tracking channel for event-driven fill and expiry processing.
"""

import asyncio
import logging
import time
from typing import Dict, Optional, Callable, Any

from .types import OrderRequest, BookSnapshot, OrderAction
from .client import is_sim_filled

logger = logging.getLogger(__name__)


class UserChannel:
    """
    Monitors order states in real-time and dispatches callbacks
    (on_filled, on_expired, on_failed) to strategies.
    """

    def __init__(self, book_provider: Callable[[str], BookSnapshot]):
        self._book_provider = book_provider
        self._tracked: Dict[str, OrderRequest] = {}
        self._matched_orders: Dict[str, bool] = {}

    def track_order(self, order_id: str, request: OrderRequest):
        """Registers a placed order to track its execution lifecycle."""
        self._tracked[order_id] = request
        self._matched_orders[order_id] = False

    def untrack_order(self, order_id: str):
        """Stops tracking an order once fulfilled or cancelled."""
        self._tracked.pop(order_id, None)
        self._matched_orders.pop(order_id, None)

    def is_tracked(self, order_id: str) -> bool:
        return order_id in self._tracked

    def check_fills_and_expiries(self):
        """
        Evaluates active orders against current book depth and expiration deadlines.
        Triggers corresponding callbacks upon fill or timeout.
        """
        now_ms = time.time() * 1000
        completed_ids = []

        for order_id, req in list(self._tracked.items()):
            book = self._book_provider(req.token_id)

            # Check if order filled
            if is_sim_filled(req.action, req.price, req.shares, book):
                self._matched_orders[order_id] = True
                completed_ids.append(order_id)
                if req.on_filled:
                    try:
                        req.on_filled(req.shares)
                    except Exception as e:
                        logger.error(f"[UserChannel] Error in on_filled callback for {order_id}: {e}")
                continue

            # Check if order expired
            if req.expire_at_ms > 0 and now_ms >= req.expire_at_ms:
                completed_ids.append(order_id)
                if req.on_expired:
                    try:
                        req.on_expired()
                    except Exception as e:
                        logger.error(f"[UserChannel] Error in on_expired callback for {order_id}: {e}")

        for oid in completed_ids:
            self.untrack_order(oid)
