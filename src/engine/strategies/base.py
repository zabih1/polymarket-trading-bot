"""
Strategy context and base strategy definitions.
"""

from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any

from ..types import BookSnapshot, CompletedOrder, OrderRequest, PendingOrder


class OrderBookAccessor:
    """Provides ergonomic accessor methods for strategy order book queries."""

    def __init__(
        self,
        up_book_provider: Callable[[], BookSnapshot],
        down_book_provider: Callable[[], BookSnapshot],
    ):
        self._up_provider = up_book_provider
        self._down_provider = down_book_provider

    def best_ask_info(self, side: str) -> dict[str, float] | None:
        snap = self._up_provider() if side.upper() == "UP" else self._down_provider()
        if snap.best_ask is not None:
            return {"price": snap.best_ask, "liquidity": snap.best_ask_liquidity or 0.0}
        return None

    def best_bid_price(self, side: str) -> float | None:
        snap = self._up_provider() if side.upper() == "UP" else self._down_provider()
        return snap.best_bid

    def best_bid_info(self, side: str) -> dict[str, float] | None:
        snap = self._up_provider() if side.upper() == "UP" else self._down_provider()
        if snap.best_bid is not None:
            return {"price": snap.best_bid, "liquidity": snap.best_bid_liquidity or 0.0}
        return None


@dataclass
class TickerAccessor:
    """Accessor for real-time asset reference prices."""

    price_provider: Callable[[], float | None]
    binance_provider: Callable[[], float | None]
    divergence_provider: Callable[[], float | None] = lambda: 0.0

    @property
    def price(self) -> float | None:
        return self.price_provider()

    @property
    def binance_price(self) -> float | None:
        return self.binance_provider()

    @property
    def divergence(self) -> float | None:
        return self.divergence_provider()


@dataclass
class StrategyContext:
    """
    Context passed to strategy execution routines providing access
    to live order books, price feeds, order submission, and lifecycle control.
    """

    slug: str
    slot_start_ms: float
    slot_end_ms: float
    clob_token_ids: list[str]  # [UP_TOKEN_ID, DOWN_TOKEN_ID]
    order_book: OrderBookAccessor
    ticker: TickerAccessor

    # Callbacks & lifecycle hooks
    post_orders_fn: Callable[[list[OrderRequest]], None]
    cancel_orders_fn: Callable[[list[str]], Coroutine[Any, Any, dict[str, Any]]]
    emergency_sells_fn: Callable[[list[str]], Coroutine[Any, Any, None]]
    hold_fn: Callable[[], Callable[[], None]]
    block_buys_fn: Callable[[], None]
    block_sells_fn: Callable[[], None]
    market_result_fn: Callable[[], dict[str, Any] | None]
    log_fn: Callable[[str], None]

    pending_orders: list[PendingOrder] = field(default_factory=list)
    order_history: list[CompletedOrder] = field(default_factory=list)
    is_production: bool = False

    def post_orders(self, orders: list[OrderRequest]):
        """Submits orders asynchronously with event-driven callback chaining."""
        self.post_orders_fn(orders)

    async def cancel_orders(self, order_ids: list[str]) -> dict[str, Any]:
        """Cancels orders by ID."""
        return await self.cancel_orders_fn(order_ids)

    async def emergency_sells(self, order_ids: list[str]):
        """Last-resort immediate FOK sell at current best bid."""
        await self.emergency_sells_fn(order_ids)

    def hold(self) -> Callable[[], None]:
        """Locks the lifecycle in RUNNING state until release is called."""
        return self.hold_fn()

    def block_buys(self):
        """Disables further buy orders for this slot."""
        self.block_buys_fn()

    def block_sells(self):
        """Disables further sell orders for this slot."""
        self.block_sells_fn()

    def get_market_result(self) -> dict[str, Any] | None:
        """Returns openPrice (Price to Beat) and closePrice if available."""
        return self.market_result_fn()

    def log(self, msg: str):
        """Outputs an engine log message."""
        self.log_fn(msg)
