"""
Data structures, enums, and types for the Polymarket Execution Engine.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class OrderAction(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    GTC = "GTC"  # Good-Till-Cancelled (Maker)
    FOK = "FOK"  # Fill-or-Kill (Taker)


class LifecycleState(StrEnum):
    INIT = "INIT"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    DONE = "DONE"


@dataclass
class Order:
    id: str
    token_id: str
    action: OrderAction
    price: float
    shares: float
    actual_shares: float = 0.0
    order_type: OrderType = OrderType.GTC
    status: str = "live"  # live, filled, canceled, expired, failed
    placed_at_ms: float = field(default_factory=lambda: time.time() * 1000)
    expire_at_ms: float = 0.0


@dataclass
class PlacedOrder:
    order_id: str
    status: str
    success: bool
    error_msg: str = ""


@dataclass
class CompletedOrder:
    action: OrderAction
    price: float
    shares: float
    fee: float = 0.0
    token_id: str = ""
    timestamp: float = field(default_factory=time.time)


@dataclass
class OrderRequest:
    token_id: str
    action: OrderAction
    price: float
    shares: float
    order_type: OrderType = OrderType.GTC
    expire_at_ms: float = 0.0
    on_filled: Callable[[float], Any] | None = None
    on_expired: Callable[[], Any] | None = None
    on_failed: Callable[[str], Any] | None = None


@dataclass
class PendingOrder:
    order_id: str
    token_id: str
    action: OrderAction
    price: float
    shares: float
    expire_at_ms: float
    placed_at_ms: float = field(default_factory=lambda: time.time() * 1000)
    order_type: OrderType = OrderType.GTC
    on_filled: Callable[[float], Any] | None = None
    on_expired: Callable[[], Any] | None = None
    on_failed: Callable[[str], Any] | None = None

    def to_snapshot(self) -> dict[str, Any]:
        """Serializable representation without callables."""
        return {
            "order_id": self.order_id,
            "token_id": self.token_id,
            "action": self.action.value if isinstance(self.action, OrderAction) else self.action,
            "price": self.price,
            "shares": self.shares,
            "expire_at_ms": self.expire_at_ms,
            "placed_at_ms": self.placed_at_ms,
            "order_type": self.order_type.value
            if isinstance(self.order_type, OrderType)
            else self.order_type,
        }


@dataclass
class BookSnapshot:
    best_ask: float | None = None
    best_ask_liquidity: float | None = None
    best_bid: float | None = None
    best_bid_liquidity: float | None = None
