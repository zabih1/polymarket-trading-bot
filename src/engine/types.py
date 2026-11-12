"""
Data structures, enums, and types for the Polymarket Execution Engine.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Coroutine, Dict, List, Optional, Any, Set
import time


class OrderAction(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    GTC = "GTC"  # Good-Till-Cancelled (Maker)
    FOK = "FOK"  # Fill-or-Kill (Taker)


class LifecycleState(str, Enum):
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
    on_filled: Optional[Callable[[float], Any]] = None
    on_expired: Optional[Callable[[], Any]] = None
    on_failed: Optional[Callable[[str], Any]] = None


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
    on_filled: Optional[Callable[[float], Any]] = None
    on_expired: Optional[Callable[[], Any]] = None
    on_failed: Optional[Callable[[str], Any]] = None

    def to_snapshot(self) -> Dict[str, Any]:
        """Serializable representation without callables."""
        return {
            "order_id": self.order_id,
            "token_id": self.token_id,
            "action": self.action.value if isinstance(self.action, OrderAction) else self.action,
            "price": self.price,
            "shares": self.shares,
            "expire_at_ms": self.expire_at_ms,
            "placed_at_ms": self.placed_at_ms,
            "order_type": self.order_type.value if isinstance(self.order_type, OrderType) else self.order_type
        }


@dataclass
class BookSnapshot:
    best_ask: Optional[float] = None
    best_ask_liquidity: Optional[float] = None
    best_bid: Optional[float] = None
    best_bid_liquidity: Optional[float] = None
