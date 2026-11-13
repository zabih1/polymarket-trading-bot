"""Trading clients for deterministic simulation and live CLOB execution."""

from __future__ import annotations

import abc
import asyncio
import logging
import random
import uuid
from collections.abc import Callable, Sequence
from decimal import Decimal
from typing import Any

from .types import BookSnapshot, Order, OrderAction, OrderRequest, OrderType, PlacedOrder

logger = logging.getLogger(__name__)


def _action(value: OrderAction | str) -> OrderAction:
    return value if isinstance(value, OrderAction) else OrderAction(value.lower())


def _order_type(value: OrderType | str) -> OrderType:
    return value if isinstance(value, OrderType) else OrderType(value.upper())


def is_sim_filled(
    action: OrderAction | str,
    price: float,
    shares: float,
    book: BookSnapshot,
) -> bool:
    """Apply the reference engine's strict 2x best-level liquidity rule.

    Liquidity is USDC at the single best counterparty level, not total shares
    or total depth. Equality intentionally does not fill.
    """

    required_usdc = shares * price * 2.0
    if _action(action) is OrderAction.BUY:
        return (
            book.best_ask is not None
            and book.best_ask <= price
            and (book.best_ask_liquidity or 0.0) > required_usdc
        )
    return (
        book.best_bid is not None
        and book.best_bid >= price
        and (book.best_bid_liquidity or 0.0) > required_usdc
    )


class BaseTradingClient(abc.ABC):
    @abc.abstractmethod
    async def init(self) -> None:
        """Prepare the client for use."""

    @abc.abstractmethod
    async def post_orders(self, orders: Sequence[OrderRequest]) -> list[PlacedOrder]:
        """Place one or more orders."""

    @abc.abstractmethod
    async def cancel_orders(self, order_ids: Sequence[str]) -> dict[str, Any]:
        """Cancel orders and report IDs that did and did not cancel."""

    @abc.abstractmethod
    async def get_order_by_id(self, order_id: str) -> Order | None:
        """Return the current order when it remains queryable."""

    async def close(self) -> None:
        """Release client resources."""
        return None


class SimTradingClient(BaseTradingClient):
    """Paper client matching the reference engine's order semantics."""

    def __init__(
        self,
        book_provider: Callable[[str], BookSnapshot],
        sim_delay_ms: int = 150,
    ) -> None:
        self._book_provider = book_provider
        self._sim_delay_ms = sim_delay_ms
        self._orders: dict[str, Order] = {}

    async def init(self) -> None:
        logger.info("[SimClient] Initialized in paper-trading mode.")

    async def _simulate_delay(self) -> None:
        delay = (self._sim_delay_ms + random.uniform(0.0, 10.0)) / 1000.0
        await asyncio.sleep(delay)

    async def post_orders(self, orders: Sequence[OrderRequest]) -> list[PlacedOrder]:
        await self._simulate_delay()
        results: list[PlacedOrder] = []

        for request in orders:
            action = _action(request.action)
            order_type = _order_type(request.order_type)
            order_id = str(uuid.uuid4())

            if order_type is OrderType.FOK and not is_sim_filled(
                action,
                request.price,
                request.shares,
                self._book_provider(request.token_id),
            ):
                results.append(
                    PlacedOrder(
                        order_id="",
                        status="rejected",
                        success=False,
                        error_msg=(
                            "order couldn't be fully filled. FOK orders are fully filled or killed."
                        ),
                    )
                )
                continue

            # The reference simulator always rests GTC orders. UserChannel
            # observes the subsequent book match and applies settlement delay.
            status = "filled" if order_type is OrderType.FOK else "live"
            self._orders[order_id] = Order(
                id=order_id,
                token_id=request.token_id,
                action=action,
                price=request.price,
                shares=request.shares,
                actual_shares=request.shares if status == "filled" else 0.0,
                order_type=order_type,
                status=status,
                expire_at_ms=request.expire_at_ms,
            )
            results.append(
                PlacedOrder(
                    order_id=order_id,
                    status="matched" if status == "filled" else "live",
                    success=True,
                )
            )

        return results

    async def cancel_orders(self, order_ids: Sequence[str]) -> dict[str, Any]:
        await self._simulate_delay()
        canceled: list[str] = []
        not_canceled: dict[str, str] = {}
        for order_id in order_ids:
            order = self._orders.get(order_id)
            if order is not None and order.status == "live":
                order.status = "canceled"
                canceled.append(order_id)
            else:
                not_canceled[order_id] = "Order not found or no longer live"
        return {"canceled": canceled, "not_canceled": not_canceled}

    async def get_order_by_id(self, order_id: str) -> Order | None:
        await self._simulate_delay()
        return self._orders.get(order_id)


class ProdTradingClient(BaseTradingClient):
    """Live adapter for Polymarket's current unified Python SDK.

    The optional SDK dependency is imported lazily so collection, simulation,
    and backtesting continue to work without wallet packages installed.
    """

    def __init__(self, private_key: str, funder_address: str = "") -> None:
        if not private_key:
            raise ValueError("POLYMARKET_PRIVATE_KEY is required when production mode is enabled")
        self.private_key = private_key
        self.funder_address = funder_address or None
        self._client: Any = None

    async def init(self) -> None:
        try:
            from polymarket import AsyncSecureClient
        except ImportError as exc:  # pragma: no cover - depends on optional SDK
            raise RuntimeError(
                "Production mode requires the official 'polymarket-client' package"
            ) from exc

        self._client = await AsyncSecureClient.create(
            private_key=self.private_key,
            wallet=self.funder_address,
            logger=logger,
        )
        logger.info("[ProdClient] Authenticated with the official Polymarket SDK.")

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError("Production client has not been initialized")
        return self._client

    @staticmethod
    def _decimal(value: float) -> Decimal:
        return Decimal(str(value))

    async def _place_one(self, request: OrderRequest) -> PlacedOrder:
        client = self._require_client()
        action = _action(request.action)
        order_type = _order_type(request.order_type)
        side = action.value.upper()

        try:
            if order_type is OrderType.GTC:
                response = await client.place_limit_order(
                    token_id=request.token_id,
                    price=self._decimal(request.price),
                    size=self._decimal(request.shares),
                    side=side,
                )
            elif action is OrderAction.BUY:
                response = await client.place_market_order(
                    token_id=request.token_id,
                    side="BUY",
                    amount=self._decimal(request.price * request.shares),
                    max_price=self._decimal(request.price),
                    order_type="FOK",
                )
            else:
                response = await client.place_market_order(
                    token_id=request.token_id,
                    side="SELL",
                    shares=self._decimal(request.shares),
                    min_price=self._decimal(request.price),
                    order_type="FOK",
                )
        except Exception as exc:
            logger.exception("Live order placement failed")
            return PlacedOrder("", "rejected", False, str(exc))

        if not bool(getattr(response, "ok", False)):
            code = getattr(response, "code", "unknown")
            message = getattr(response, "message", "Order rejected")
            return PlacedOrder("", str(code), False, str(message))

        return PlacedOrder(
            order_id=str(response.order_id),
            status=str(response.status),
            success=True,
        )

    async def post_orders(self, orders: Sequence[OrderRequest]) -> list[PlacedOrder]:
        # Place independently so one rejected order does not discard siblings.
        return list(await asyncio.gather(*(self._place_one(order) for order in orders)))

    async def cancel_orders(self, order_ids: Sequence[str]) -> dict[str, Any]:
        if not order_ids:
            return {"canceled": [], "not_canceled": {}}
        response = await self._require_client().cancel_orders(order_ids=list(order_ids))
        return {
            "canceled": list(getattr(response, "canceled", ())),
            "not_canceled": dict(getattr(response, "not_canceled", {})),
        }

    async def subscribe_user(self, condition_id: str) -> Any:
        """Open the official authenticated user stream for one market."""
        from polymarket.streams import UserSpec

        return await self._require_client().subscribe(UserSpec(markets=[condition_id]))

    async def get_order_by_id(self, order_id: str) -> Order | None:
        try:
            live = await self._require_client().get_order(order_id=order_id)
        except Exception:
            return None

        original_size = float(getattr(live, "original_size", getattr(live, "size", 0.0)))
        size_matched = float(getattr(live, "size_matched", 0.0))
        return Order(
            id=str(getattr(live, "id", order_id)),
            token_id=str(getattr(live, "asset_id", getattr(live, "token_id", ""))),
            action=_action(str(getattr(live, "side", "BUY"))),
            price=float(getattr(live, "price", 0.0)),
            shares=original_size,
            actual_shares=size_matched,
            order_type=OrderType.GTC,
            status="filled" if original_size > 0 and size_matched >= original_size else "live",
        )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
