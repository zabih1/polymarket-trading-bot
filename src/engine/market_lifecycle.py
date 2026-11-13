"""Lifecycle for one rolling Polymarket market."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable
from typing import Any

from ..collectors.clob_client import ClobClient, LocalOrderBook
from ..collectors.gamma_client import GammaClient
from ..collectors.oracle_client import OracleClient
from ..collectors.price_tracker import PriceTracker
from ..config import BUY_MAX_RETRIES, BUY_RETRY_DELAY_MS
from ..utils.slot import slot_from_slug
from .client import BaseTradingClient, ProdTradingClient
from .strategies.base import OrderBookAccessor, StrategyContext, TickerAccessor
from .types import (
    BookSnapshot,
    CompletedOrder,
    LifecycleState,
    OrderAction,
    OrderRequest,
    OrderType,
    PendingOrder,
    PlacedOrder,
)
from .user_channel import LiveUserChannel, UserChannel
from .wallet import WalletTracker

logger = logging.getLogger(__name__)


class MarketLifecycle:
    """Coordinate metadata, feeds, orders, callbacks, and resolution for a slot."""

    def __init__(
        self,
        slug: str,
        client: BaseTradingClient,
        wallet: WalletTracker,
        price_tracker: PriceTracker,
        strategy_name: str,
        strategy_fn: Callable[[StrategyContext], Any],
        log_fn: Callable[[str], None] | None = None,
    ) -> None:
        self.slug = slug
        self.client = client
        self.wallet = wallet
        self.price_tracker = price_tracker
        self.strategy_name = strategy_name
        self.strategy_fn = strategy_fn
        self._log = log_fn or logger.info

        parsed = slot_from_slug(slug)
        self.slot_start_ms = parsed["start_time"] * 1000
        self.slot_end_ms = parsed["end_time"] * 1000
        self.asset = parsed["asset"]
        self.window = parsed["window"]

        self.gamma = GammaClient()
        self.oracle = OracleClient()
        self.clob = ClobClient()

        self.state = LifecycleState.INIT
        self.condition_id: str | None = None
        self.clob_token_ids: list[str] = []
        self.up_book: LocalOrderBook | None = None
        self.down_book: LocalOrderBook | None = None
        self.user_channel: UserChannel | LiveUserChannel | None = None

        # These list identities never change; StrategyContext holds references.
        self.pending_orders: list[PendingOrder] = []
        self.order_history: list[CompletedOrder] = []
        self.buy_blocked = False
        self.sell_blocked = False
        self.strategy_locks = 0
        self.strategy_cleanup: Callable[[], None] | None = None
        self.pnl = 0.0

        self.oracle_data: dict[str, Any] | None = None
        self._fee_rate = 0.0
        self._initialized = False
        self._next_init_retry_ms = 0.0
        self._in_flight = 0
        self._placement_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._clob_stop = asyncio.Event()
        self._clob_task: asyncio.Task[None] | None = None
        self._oracle_task: asyncio.Task[None] | None = None
        self._destroyed = False

    @property
    def remaining_seconds(self) -> float:
        return max(0.0, (self.slot_end_ms - time.time() * 1000.0) / 1000.0)

    def get_book_snapshot(self, token_id: str) -> BookSnapshot:
        if len(self.clob_token_ids) != 2:
            return BookSnapshot()
        if token_id == self.clob_token_ids[0]:
            book = self.up_book
        elif token_id == self.clob_token_ids[1]:
            book = self.down_book
        else:
            return BookSnapshot()
        if book is None:
            return BookSnapshot()
        depth = book.get_depth_summary(levels=1)
        return BookSnapshot(
            best_ask=depth["best_ask"],
            best_ask_liquidity=depth["best_ask_liquidity"],
            best_bid=depth["best_bid"],
            best_bid_liquidity=depth["best_bid_liquidity"],
        )

    async def tick(self) -> None:
        if self.state is LifecycleState.DONE:
            return
        try:
            if self.state is LifecycleState.INIT:
                await self._handle_init()
            elif self.state is LifecycleState.RUNNING:
                await self._handle_running()
            elif self.state is LifecycleState.STOPPING:
                await self._handle_stopping()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Lifecycle tick failed for %s", self.slug)
            self._log(f"[{self.slug}] Lifecycle error: {exc}")

    async def _handle_init(self) -> None:
        now_ms = time.time() * 1000.0
        if not self._initialized:
            if now_ms < self._next_init_retry_ms:
                return
            metadata = await asyncio.to_thread(self.gamma.fetch_market_metadata, self.slug)
            if not metadata or not metadata.get("up_token_id") or not metadata.get("down_token_id"):
                self._next_init_retry_ms = now_ms + 1000.0
                return
            await self._initialize_feeds(metadata)

        assert self.up_book is not None and self.down_book is not None
        if now_ms >= self.slot_end_ms:
            self.state = LifecycleState.STOPPING
            return
        if not (self.up_book.is_ready and self.down_book.is_ready):
            return
        if self.strategy_name == "late-entry" and not self.price_tracker.is_ready:
            return
        if self.price_tracker.price is None:
            return

        context = self._build_strategy_context()
        self._log(f"[{self.slug}] Initializing strategy: {self.strategy_name}")
        cleanup = await self.strategy_fn(context)
        if callable(cleanup):
            self.strategy_cleanup = cleanup
        self.state = LifecycleState.RUNNING
        self._log(f"[{self.slug}] State: INIT -> RUNNING")

    async def _initialize_feeds(self, metadata: dict[str, Any]) -> None:
        self.condition_id = str(metadata["condition_id"])
        self.clob_token_ids.extend([str(metadata["up_token_id"]), str(metadata["down_token_id"])])
        fee_schedule = metadata.get("fee_schedule") or {}
        with contextlib.suppress(TypeError, ValueError):
            self._fee_rate = float(fee_schedule.get("rate", 0.0))

        self.up_book = LocalOrderBook(self.clob_token_ids[0])
        self.down_book = LocalOrderBook(self.clob_token_ids[1])
        if isinstance(self.client, ProdTradingClient):
            self.user_channel = LiveUserChannel(self.client.subscribe_user)
        else:
            self.user_channel = UserChannel(self.get_book_snapshot)
        await self.user_channel.start(self.condition_id)

        self._clob_stop.clear()
        self._clob_task = asyncio.create_task(
            self.clob.stream_orderbooks(
                token_ids=self.clob_token_ids,
                up_book=self.up_book,
                down_book=self.down_book,
                up_token_id=self.clob_token_ids[0],
                stop_event=self._clob_stop,
            ),
            name=f"books-{self.slug}",
        )
        self._oracle_task = asyncio.create_task(self._poll_oracle(), name=f"oracle-{self.slug}")
        self._initialized = True

    async def _poll_oracle(self) -> None:
        delay = max(0.0, (self.slot_start_ms - time.time() * 1000.0) / 1000.0)
        if delay:
            try:
                await asyncio.wait_for(self._clob_stop.wait(), timeout=delay)
                return
            except TimeoutError:
                pass

        start = self.slot_start_ms // 1000
        end = self.slot_end_ms // 1000
        while not self._clob_stop.is_set():
            result = await self.oracle.fetch_settlement_price_async(
                start, end, self.asset, self.window
            )
            if result is not None:
                self.oracle_data = result
                if result.get("completed"):
                    return
            try:
                await asyncio.wait_for(self._clob_stop.wait(), timeout=1.0)
            except TimeoutError:
                pass

    def _build_strategy_context(self) -> StrategyContext:
        return StrategyContext(
            slug=self.slug,
            slot_start_ms=self.slot_start_ms,
            slot_end_ms=self.slot_end_ms,
            clob_token_ids=self.clob_token_ids,
            order_book=OrderBookAccessor(
                up_book_provider=lambda: self.get_book_snapshot(self.clob_token_ids[0]),
                down_book_provider=lambda: self.get_book_snapshot(self.clob_token_ids[1]),
            ),
            ticker=TickerAccessor(
                price_provider=lambda: self.price_tracker.price,
                binance_provider=lambda: self.price_tracker.binance_price,
                divergence_provider=lambda: self.price_tracker.divergence,
            ),
            post_orders_fn=self.post_orders,
            cancel_orders_fn=self.cancel_orders,
            emergency_sells_fn=self.emergency_sells,
            hold_fn=self.create_hold,
            block_buys_fn=self.block_buys,
            block_sells_fn=self.block_sells,
            market_result_fn=lambda: self.oracle_data,
            log_fn=self._log,
            pending_orders=self.pending_orders,
            order_history=self.order_history,
            is_production=isinstance(self.client, ProdTradingClient),
        )

    async def _handle_running(self) -> None:
        if self.user_channel is not None:
            self.user_channel.check_fills()
        await self._check_expiries()

        if time.time() * 1000.0 >= self.slot_end_ms:
            self.buy_blocked = True
            self.state = LifecycleState.STOPPING
            self._log(f"[{self.slug}] State: RUNNING -> STOPPING (slot closed)")
            return

        if (
            not self.pending_orders
            and self._in_flight == 0
            and self.strategy_locks == 0
            and not self._has_positions()
        ):
            self.state = LifecycleState.STOPPING
            self._log(f"[{self.slug}] State: RUNNING -> STOPPING (drained)")

    async def _handle_stopping(self) -> None:
        if self.strategy_cleanup is not None:
            self.strategy_cleanup()
            self.strategy_cleanup = None
        if self.user_channel is not None:
            self.user_channel.check_fills()

        self.buy_blocked = True
        buys = [order.order_id for order in self.pending_orders if order.action is OrderAction.BUY]
        if buys:
            await self.cancel_orders(buys)
        await self._check_expiries()

        now_ms = time.time() * 1000.0
        if now_ms < self.slot_end_ms:
            if self.pending_orders or self._in_flight or self._has_positions():
                return
            await self._finish(settle=False)
            return

        # Once the market closes no resting sell can provide a useful exit.
        remaining = [order.order_id for order in self.pending_orders]
        if remaining:
            response = await self.cancel_orders(remaining)
            for order_id, reason in response.get("not_canceled", {}).items():
                if self.user_channel and self.user_channel.is_matched(order_id):
                    continue
                self._fail_pending(order_id, str(reason), notify=False)
        if self.pending_orders or self._in_flight:
            return
        if self._has_positions() and not (self.oracle_data and self.oracle_data.get("completed")):
            return
        await self._finish(settle=self._has_positions())

    async def _finish(self, *, settle: bool) -> None:
        payout = 0.0
        winner: str | None = None
        if settle:
            assert self.oracle_data is not None
            winner = str(self.oracle_data["winner"])
            winning_token = self.clob_token_ids[0] if winner == "UP" else self.clob_token_ids[1]
            for token_id in self.clob_token_ids:
                payout += self.wallet.credit_settlement(
                    token_id,
                    1.0 if token_id == winning_token else 0.0,
                    label=f"Result: {winner}",
                )

        trading_pnl = 0.0
        for order in self.order_history:
            notional = order.price * order.shares
            trading_pnl += notional if order.action is OrderAction.SELL else -notional
            trading_pnl -= order.fee
        self.pnl = round(trading_pnl + payout, 4)
        self._log(
            f"[{self.slug}] Settled | Winner: {winner or 'No position'} | "
            f"Payout: ${payout:.2f} | PnL: {self.pnl:+.2f}"
        )
        self.state = LifecycleState.DONE
        await self.destroy()

    def post_orders(self, orders: list[OrderRequest]) -> None:
        for request in orders:
            action = self._as_action(request.action)
            if (action is OrderAction.BUY and self.buy_blocked) or (
                action is OrderAction.SELL and self.sell_blocked
            ):
                if request.on_failed is not None:
                    request.on_failed(f"{action.value} blocked")
                continue
            self._in_flight += 1
            task = asyncio.create_task(
                self._place_with_retry(request),
                name=f"place-{action.value}-{self.slug}",
            )
            self._tasks.add(task)
            task.add_done_callback(self._placement_done)

    def _placement_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        self._in_flight = max(0, self._in_flight - 1)
        if not task.cancelled() and (exc := task.exception()) is not None:
            logger.error(
                "Order placement task failed",
                exc_info=(type(exc), exc, exc.__traceback__),
            )

    async def _place_with_retry(self, request: OrderRequest) -> None:
        action = self._as_action(request.action)
        max_retries = BUY_MAX_RETRIES if action is OrderAction.BUY else None
        attempts = 0
        deadline_ms = (
            min(request.expire_at_ms, self.slot_end_ms)
            if request.expire_at_ms > 0
            else self.slot_end_ms
        )
        while time.time() * 1000.0 < deadline_ms:
            if (action is OrderAction.BUY and self.buy_blocked) or (
                action is OrderAction.SELL and self.sell_blocked
            ):
                return

            async with self._placement_lock:
                can_place = (
                    self.wallet.can_place_buy(request.price, request.shares)
                    if action is OrderAction.BUY
                    else self.wallet.can_place_sell(request.token_id, request.shares)
                )
                if can_place:
                    result = (await self.client.post_orders([request]))[0]
                    if result.success and result.order_id:
                        self._register_placed(request, result)
                        return
                    reason = result.error_msg or result.status
                    if "balance" not in reason.lower():
                        if request.on_failed is not None:
                            request.on_failed(reason)
                        return

            attempts += 1
            if max_retries is not None and attempts >= max_retries:
                if request.on_failed is not None:
                    request.on_failed("not enough balance")
                return
            await asyncio.sleep(BUY_RETRY_DELAY_MS / 1000.0)

        if request.on_failed is not None:
            request.on_failed("order expired before placement")

    def _register_placed(self, request: OrderRequest, result: PlacedOrder) -> None:
        action = self._as_action(request.action)
        order_type = self._as_order_type(request.order_type)
        if action is OrderAction.BUY:
            self.wallet.lock_for_buy(result.order_id, request.price, request.shares, self.slug)
        else:
            self.wallet.lock_for_sell(result.order_id, request.token_id, request.shares, self.slug)

        pending = PendingOrder(
            order_id=result.order_id,
            token_id=request.token_id,
            action=action,
            price=request.price,
            shares=request.shares,
            expire_at_ms=request.expire_at_ms,
            order_type=order_type,
            on_filled=request.on_filled,
            on_expired=request.on_expired,
            on_failed=request.on_failed,
        )
        self.pending_orders.append(pending)
        assert self.user_channel is not None
        self.user_channel.track_order(
            result.order_id,
            self._tracking_request(pending),
            matched=result.status == "matched",
        )

    def _tracking_request(self, pending: PendingOrder) -> OrderRequest:
        return OrderRequest(
            token_id=pending.token_id,
            action=pending.action,
            price=pending.price,
            shares=pending.shares,
            order_type=pending.order_type,
            expire_at_ms=pending.expire_at_ms,
            on_filled=lambda shares: self._commit_fill(pending.order_id, shares),
            on_failed=lambda reason: self._fail_pending(pending.order_id, reason),
        )

    def _commit_fill(self, order_id: str, gross_shares: float) -> None:
        pending = self._find_pending(order_id)
        if pending is None:
            return
        self._commit_pending_fill(pending, gross_shares)

    def _commit_pending_fill(self, pending: PendingOrder, gross_shares: float) -> None:
        """Commit a full fill or the settled portion captured before expiry."""
        fee = 0.0
        shares = gross_shares
        if pending.order_type is OrderType.FOK and self._fee_rate > 0:
            fee = gross_shares * self._fee_rate * pending.price * (1.0 - pending.price)
            if pending.action is OrderAction.BUY:
                shares = max(0.0, gross_shares - fee / pending.price)

        if pending.action is OrderAction.BUY:
            self.wallet.on_buy_filled(pending.order_id, pending.token_id, pending.price, shares)
        else:
            self.wallet.on_sell_filled(
                pending.order_id, pending.token_id, pending.price, shares, fee=0.0
            )
        self._remove_pending(pending.order_id)
        self.order_history.append(
            CompletedOrder(
                action=pending.action,
                price=pending.price,
                shares=shares,
                fee=fee,
                token_id=pending.token_id,
            )
        )
        if pending.on_filled is not None:
            pending.on_filled(shares)

    def _fail_pending(self, order_id: str, reason: str, *, notify: bool = True) -> None:
        pending = self._find_pending(order_id)
        if pending is None:
            return
        self._unlock(pending)
        self._remove_pending(order_id)
        if self.user_channel is not None:
            self.user_channel.untrack_order(order_id)
        if notify and pending.on_failed is not None:
            pending.on_failed(reason)

    async def _check_expiries(self) -> None:
        now_ms = time.time() * 1000.0
        for pending in tuple(self.pending_orders):
            if now_ms < pending.expire_at_ms:
                continue
            if self.user_channel and self.user_channel.is_matched(pending.order_id):
                continue
            partial_shares = (
                self.user_channel.get_matched_so_far(pending.order_id)
                if self.user_channel is not None
                else 0.0
            )
            callback = pending.on_expired
            response = await self.cancel_orders([pending.order_id])
            if pending.order_id in response.get("canceled", ()):
                if partial_shares > 0:
                    self._commit_pending_fill(pending, partial_shares)
                elif callback is not None:
                    callback()

    async def cancel_orders(self, order_ids: list[str]) -> dict[str, Any]:
        if self.user_channel is None:
            return {"canceled": [], "not_canceled": {}}
        cancellable = [
            order_id
            for order_id in order_ids
            if self._find_pending(order_id) is not None
            and not self.user_channel.is_matched(order_id)
        ]
        for order_id in cancellable:
            self.user_channel.untrack_order(order_id)
        if not cancellable:
            return {"canceled": [], "not_canceled": {}}

        response = await self.client.cancel_orders(cancellable)
        for order_id in response.get("canceled", ()):
            pending = self._find_pending(order_id)
            if pending is not None:
                self._unlock(pending)
                self._remove_pending(order_id)
        # Preserve monitoring after an uncertain/failed cancellation response.
        for order_id in response.get("not_canceled", {}):
            pending = self._find_pending(order_id)
            if pending is not None:
                self.user_channel.track_order(order_id, self._tracking_request(pending))
        return response

    async def emergency_sells(self, order_ids: list[str]) -> None:
        sells = [
            order
            for order_id in order_ids
            if (order := self._find_pending(order_id)) is not None
            and order.action is OrderAction.SELL
        ]
        response = await self.cancel_orders([order.order_id for order in sells])
        canceled = set(response.get("canceled", ()))
        await asyncio.gather(
            *(self._emergency_sell_loop(order) for order in sells if order.order_id in canceled)
        )

    async def _emergency_sell_loop(self, original: PendingOrder) -> None:
        while time.time() * 1000.0 < self.slot_end_ms:
            side_index = 0 if original.token_id == self.clob_token_ids[0] else 1
            book = self.up_book if side_index == 0 else self.down_book
            best_bid = book.get_best_bid() if book is not None else None
            price = best_bid if best_bid is not None else original.price
            completed: asyncio.Future[bool] = asyncio.get_running_loop().create_future()

            def resolve(value: bool, *, _completed: asyncio.Future[bool] = completed) -> None:
                if not _completed.done():
                    _completed.set_result(value)

            self.post_orders(
                [
                    OrderRequest(
                        token_id=original.token_id,
                        action=OrderAction.SELL,
                        price=price,
                        shares=original.shares,
                        order_type=OrderType.GTC,
                        expire_at_ms=min(self.slot_end_ms, time.time() * 1000.0 + 2000.0),
                        on_filled=lambda _shares: resolve(True),
                        on_expired=lambda: resolve(False),
                        on_failed=lambda _reason: resolve(False),
                    )
                ]
            )
            timeout = max(0.1, (self.slot_end_ms - time.time() * 1000.0) / 1000.0)
            try:
                if await asyncio.wait_for(completed, timeout=timeout):
                    return
            except TimeoutError:
                return

    def _find_pending(self, order_id: str) -> PendingOrder | None:
        return next(
            (order for order in self.pending_orders if order.order_id == order_id),
            None,
        )

    def _remove_pending(self, order_id: str) -> None:
        for index, order in enumerate(self.pending_orders):
            if order.order_id == order_id:
                del self.pending_orders[index]
                return

    def _unlock(self, pending: PendingOrder) -> None:
        if pending.action is OrderAction.BUY:
            self.wallet.unlock_buy(pending.order_id, self.slug)
        else:
            self.wallet.unlock_sell(pending.order_id, self.slug)

    def _has_positions(self) -> bool:
        return any(self.wallet.total_shares(token_id) > 0 for token_id in self.clob_token_ids)

    @staticmethod
    def _as_action(value: OrderAction | str) -> OrderAction:
        return value if isinstance(value, OrderAction) else OrderAction(value.lower())

    @staticmethod
    def _as_order_type(value: OrderType | str) -> OrderType:
        return value if isinstance(value, OrderType) else OrderType(value.upper())

    def create_hold(self) -> Callable[[], None]:
        self.strategy_locks += 1
        released = False

        def release() -> None:
            nonlocal released
            if not released:
                released = True
                self.strategy_locks = max(0, self.strategy_locks - 1)

        return release

    def block_buys(self) -> None:
        self.buy_blocked = True

    def block_sells(self) -> None:
        self.sell_blocked = True

    def shutdown(self) -> None:
        self.buy_blocked = True
        if self.state is LifecycleState.INIT:
            self.state = LifecycleState.STOPPING if self._initialized else LifecycleState.DONE
        elif self.state is LifecycleState.RUNNING:
            self.state = LifecycleState.STOPPING

    async def destroy(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True
        self._clob_stop.set()
        for task in tuple(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        for task in (self._clob_task, self._oracle_task):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (self._clob_task, self._oracle_task) if task is not None),
            return_exceptions=True,
        )
        if self.user_channel is not None:
            await self.user_channel.close()
        self.clob.session.close()
        self.gamma.session.close()
        self.oracle.session.close()
