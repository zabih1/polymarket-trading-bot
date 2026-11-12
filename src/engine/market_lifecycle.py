"""
Market lifecycle manager for single rolling market rounds.
Coordinates state transitions: INIT -> RUNNING -> STOPPING -> DONE.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Callable, Any

from .types import (
    LifecycleState,
    OrderRequest,
    PendingOrder,
    CompletedOrder,
    OrderAction,
    OrderType,
    BookSnapshot
)
from .client import BaseTradingClient, SimTradingClient
from .wallet import WalletTracker
from .user_channel import UserChannel
from .strategies.base import StrategyContext, OrderBookAccessor, TickerAccessor
from ..collectors.gamma_client import GammaClient
from ..collectors.oracle_client import OracleClient
from ..collectors.clob_client import ClobClient, LocalOrderBook
from ..collectors.binance_client import BinanceTickerTracker
from ..utils.slot import slot_from_slug

logger = logging.getLogger(__name__)


class MarketLifecycle:
    """
    Encapsulates the end-to-end lifecycle of a single Polymarket binary market slot.
    """

    def __init__(
        self,
        slug: str,
        client: BaseTradingClient,
        wallet: WalletTracker,
        binance_tracker: BinanceTickerTracker,
        strategy_name: str,
        strategy_fn: Callable[[StrategyContext], Any],
        log_fn: Optional[Callable[[str], None]] = None
    ):
        self.slug = slug
        self.client = client
        self.wallet = wallet
        self.binance_tracker = binance_tracker
        self.strategy_name = strategy_name
        self.strategy_fn = strategy_fn
        self._log = log_fn or logger.info

        # Market Slot timing
        parsed_slot = slot_from_slug(slug)
        self.slot_start_ms = parsed_slot["start_time"] * 1000
        self.slot_end_ms = parsed_slot["end_time"] * 1000
        self.asset = parsed_slot["asset"]
        self.window = parsed_slot["window"]

        # External connectors
        self.gamma = GammaClient()
        self.oracle = OracleClient()
        self.clob = ClobClient()

        # State machine
        self.state = LifecycleState.INIT
        self.condition_id: Optional[str] = None
        self.clob_token_ids: List[str] = []
        self.up_book: Optional[LocalOrderBook] = None
        self.down_book: Optional[LocalOrderBook] = None
        self.user_channel: Optional[UserChannel] = None

        self.pending_orders: List[PendingOrder] = []
        self.order_history: List[CompletedOrder] = []
        self.buy_blocked = False
        self.sell_blocked = False
        self.strategy_locks = 0
        self.strategy_cleanup: Optional[Callable[[], None]] = None
        self.pnl = 0.0

        # Settlement data
        self.oracle_data: Optional[Dict[str, Any]] = None
        self._clob_stop = asyncio.Event()
        self._clob_task: Optional[asyncio.Task] = None

    @property
    def remaining_seconds(self) -> float:
        return max(0.0, (self.slot_end_ms - (time.time() * 1000)) / 1000.0)

    def get_book_snapshot(self, token_id: str) -> BookSnapshot:
        """Returns instantaneous Level-2 snapshot for token."""
        if not self.clob_token_ids or len(self.clob_token_ids) < 2:
            return BookSnapshot()
        book = self.up_book if token_id == self.clob_token_ids[0] else self.down_book
        if not book:
            return BookSnapshot()
        depth = book.get_depth_summary(levels=1)
        return BookSnapshot(
            best_ask=depth.get("best_ask"),
            best_ask_liquidity=depth.get("total_ask_liquidity"),
            best_bid=depth.get("best_bid"),
            best_bid_liquidity=depth.get("total_bid_liquidity")
        )

    async def tick(self):
        """Drives the state machine forward on each engine loop tick."""
        if self.state == LifecycleState.DONE:
            return

        try:
            if self.state == LifecycleState.INIT:
                await self._handle_init()
            elif self.state == LifecycleState.RUNNING:
                await self._handle_running()
            elif self.state == LifecycleState.STOPPING:
                await self._handle_stopping()
        except Exception as e:
            self._log(f"[{self.slug}] Error in lifecycle tick: {e}")

    async def _handle_init(self):
        """Discovers market metadata, starts streams, and launches strategy."""
        # 1. Fetch metadata from Gamma API
        metadata = self.gamma.fetch_market_metadata(self.slug)
        if not metadata or not metadata.get("up_token_id") or not metadata.get("down_token_id"):
            return

        self.condition_id = metadata["condition_id"]
        self.clob_token_ids = [metadata["up_token_id"], metadata["down_token_id"]]

        # 2. Initialize Order Books & User Channel
        self.up_book = LocalOrderBook(self.clob_token_ids[0])
        self.down_book = LocalOrderBook(self.clob_token_ids[1])
        self.user_channel = UserChannel(book_provider=self.get_book_snapshot)

        # 3. Connect CLOB WebSocket
        self._clob_stop.clear()
        self._clob_task = asyncio.create_task(
            self.clob.stream_orderbooks(
                token_ids=self.clob_token_ids,
                up_book=self.up_book,
                down_book=self.down_book,
                up_token_id=self.clob_token_ids[0],
                stop_event=self._clob_stop
            )
        )

        # 4. Fetch Oracle start price
        start_ts = int(self.slot_start_ms / 1000)
        end_ts = int(self.slot_end_ms / 1000)
        self.oracle_data = self.oracle.fetch_settlement_price(start_ts, end_ts, self.asset, self.window)

        # 5. Build Strategy Context
        order_book_accessor = OrderBookAccessor(
            up_book_provider=lambda: self.get_book_snapshot(self.clob_token_ids[0]),
            down_book_provider=lambda: self.get_book_snapshot(self.clob_token_ids[1])
        )
        ticker_accessor = TickerAccessor(
            price_provider=lambda: self.binance_tracker.current_price,
            binance_provider=lambda: self.binance_tracker.current_price,
            divergence_provider=lambda: 0.0
        )

        ctx = StrategyContext(
            slug=self.slug,
            slot_start_ms=self.slot_start_ms,
            slot_end_ms=self.slot_end_ms,
            clob_token_ids=self.clob_token_ids,
            order_book=order_book_accessor,
            ticker=ticker_accessor,
            post_orders_fn=self.post_orders,
            cancel_orders_fn=self.cancel_orders,
            emergency_sells_fn=self.emergency_sells,
            hold_fn=self.create_hold,
            block_buys_fn=self.block_buys,
            block_sells_fn=self.block_sells,
            market_result_fn=lambda: self.oracle_data,
            log_fn=self._log,
            pending_orders=self.pending_orders,
            order_history=self.order_history
        )

        # Wait max 2s for book readiness
        for _ in range(10):
            if self.up_book.is_ready and self.down_book.is_ready:
                break
            await asyncio.sleep(0.2)

        # 6. Execute strategy function
        self._log(f"[{self.slug}] Initializing strategy: {self.strategy_name}")
        cleanup = await self.strategy_fn(ctx)
        if callable(cleanup):
            self.strategy_cleanup = cleanup

        self.state = LifecycleState.RUNNING
        self._log(f"[{self.slug}] State transitioned: INIT -> RUNNING")

    async def _handle_running(self):
        """Monitors fills, order expirations, and triggers transitions."""
        now_ms = time.time() * 1000

        # Check for fills and expiries
        if self.user_channel:
            self.user_channel.check_fills_and_expiries()

        # Check if slot window closed
        if now_ms >= self.slot_end_ms:
            self._log(f"[{self.slug}] Market slot closed. Transitioning: RUNNING -> STOPPING")
            self.state = LifecycleState.STOPPING
            return

        # Check if no orders remain and no strategy hold active
        if len(self.pending_orders) == 0 and self.strategy_locks == 0:
            # Check if any unfilled positions remain
            unfilled_shares = sum(
                self.wallet.total_shares(tid) for tid in self.clob_token_ids
            )
            if unfilled_shares == 0:
                self._log(f"[{self.slug}] All trades and locks settled. Transitioning: RUNNING -> STOPPING")
                self.state = LifecycleState.STOPPING

    async def _handle_stopping(self):
        """Cancels remaining buys, settles terminal positions, and records PnL."""
        if self.strategy_cleanup:
            try:
                self.strategy_cleanup()
            except Exception:
                pass
            self.strategy_cleanup = None

        # Cancel any pending buys
        pending_buys = [o.order_id for o in self.pending_orders if o.action == OrderAction.BUY]
        if pending_buys:
            await self.cancel_orders(pending_buys)

        # Resolve market via Chainlink settlement price
        start_ts = int(self.slot_start_ms / 1000)
        end_ts = int(self.slot_end_ms / 1000)
        res = None
        for _ in range(5):
            res = self.oracle.fetch_settlement_price(start_ts, end_ts, self.asset, self.window)
            if res and res.get("completed"):
                break
            await asyncio.sleep(1.0)

        winner = res.get("winner") if res else None
        winning_token_id = None
        if winner == "UP" and len(self.clob_token_ids) > 0:
            winning_token_id = self.clob_token_ids[0]
        elif winner == "DOWN" and len(self.clob_token_ids) > 1:
            winning_token_id = self.clob_token_ids[1]

        # Settle unclosed shares in wallet: winning shares -> $1.00, losing -> $0.00
        payout = 0.0
        for tid in self.clob_token_ids:
            payout_rate = 1.00 if (tid == winning_token_id) else 0.00
            payout += self.wallet.credit_settlement(tid, payout_rate, label=f"Result: {winner}")

        # Compute net round PnL from completed orders & settlement
        total_spent = sum(o.price * o.shares for o in self.order_history if o.action == OrderAction.BUY)
        total_proceeds = sum(o.price * o.shares - o.fee for o in self.order_history if o.action == OrderAction.SELL)
        self.pnl = round((total_proceeds + payout) - total_spent, 4)

        self._log(
            f"[{self.slug}] Round Settled | Winner: {winner or 'Pending'} | "
            f"Spent: ${total_spent:.2f} | Proceeds: ${total_proceeds:.2f} | Payout: ${payout:.2f} | "
            f"Net PnL: {'+' if self.pnl >= 0 else ''}${self.pnl:.2f}"
        )

        self.destroy()
        self.state = LifecycleState.DONE

    def post_orders(self, orders: List[OrderRequest]):
        """Places orders asynchronously via the client."""
        for req in orders:
            if req.action == OrderAction.BUY and self.buy_blocked:
                self._log(f"[{self.slug}] Buy blocked; dropping order.")
                continue
            if req.action == OrderAction.SELL and self.sell_blocked:
                self._log(f"[{self.slug}] Sell blocked; dropping order.")
                continue

            # Check wallet balances & reserve capital/shares
            if req.action == OrderAction.BUY:
                if not self.wallet.can_place_buy(req.price, req.shares):
                    self._log(f"[{self.slug}] Insufficient cash for buy ({req.price} x {req.shares}); rejected.")
                    if req.on_failed:
                        req.on_failed("insufficient_cash")
                    continue
            else:
                if not self.wallet.can_place_sell(req.token_id, req.shares):
                    self._log(f"[{self.slug}] Insufficient shares for sell ({req.shares}); rejected.")
                    if req.on_failed:
                        req.on_failed("insufficient_shares")
                    continue

            asyncio.create_task(self._execute_placement(req))

    async def _execute_placement(self, req: OrderRequest):
        results = await self.client.post_orders([req])
        if not results:
            return

        res = results[0]
        if not res.success or not res.order_id:
            if req.on_failed:
                req.on_failed(res.error_msg)
            return

        order_id = res.order_id
        action = OrderAction(req.action) if isinstance(req.action, str) else req.action
        order_type = OrderType(req.order_type) if isinstance(req.order_type, str) else req.order_type

        # Reserve in wallet
        if action == OrderAction.BUY:
            self.wallet.lock_for_buy(order_id, req.price, req.shares, label=self.slug)
        else:
            self.wallet.lock_for_sell(order_id, req.token_id, req.shares, label=self.slug)

        pending = PendingOrder(
            order_id=order_id,
            token_id=req.token_id,
            action=action,
            price=req.price,
            shares=req.shares,
            expire_at_ms=req.expire_at_ms,
            order_type=order_type,
            on_filled=self._make_on_filled_wrapper(order_id, req),
            on_expired=self._make_on_expired_wrapper(order_id, req),
            on_failed=self._make_on_failed_wrapper(order_id, req)
        )
        self.pending_orders.append(pending)

        # If already matched on placement (e.g. FOK or crossing GTC)
        if res.status == "matched":
            pending.on_filled(req.shares)
        else:
            if self.user_channel:
                self.user_channel.track_order(order_id, req)

    def _make_on_filled_wrapper(self, order_id: str, req: OrderRequest):
        def wrapper(filled_shares: float):
            action = OrderAction(req.action) if isinstance(req.action, str) else req.action
            fee = (filled_shares * 0.072 * req.price * (1.0 - req.price)) if req.order_type == OrderType.FOK else 0.0

            if action == OrderAction.BUY:
                self.wallet.on_buy_filled(order_id, req.token_id, req.price, filled_shares)
            else:
                self.wallet.on_sell_filled(order_id, req.token_id, req.price, filled_shares, fee=fee)

            self.pending_orders = [o for o in self.pending_orders if o.order_id != order_id]
            self.order_history.append(CompletedOrder(
                action=action,
                price=req.price,
                shares=filled_shares,
                fee=fee,
                token_id=req.token_id
            ))

            if req.on_filled:
                req.on_filled(filled_shares)
        return wrapper

    def _make_on_expired_wrapper(self, order_id: str, req: OrderRequest):
        def wrapper():
            action = OrderAction(req.action) if isinstance(req.action, str) else req.action
            if action == OrderAction.BUY:
                self.wallet.unlock_buy(order_id, label=self.slug)
            else:
                self.wallet.unlock_sell(order_id, label=self.slug)

            self.pending_orders = [o for o in self.pending_orders if o.order_id != order_id]
            if req.on_expired:
                req.on_expired()
        return wrapper

    def _make_on_failed_wrapper(self, order_id: str, req: OrderRequest):
        def wrapper(reason: str):
            action = OrderAction(req.action) if isinstance(req.action, str) else req.action
            if action == OrderAction.BUY:
                self.wallet.unlock_buy(order_id, label=self.slug)
            else:
                self.wallet.unlock_sell(order_id, label=self.slug)

            self.pending_orders = [o for o in self.pending_orders if o.order_id != order_id]
            if req.on_failed:
                req.on_failed(reason)
        return wrapper

    async def cancel_orders(self, order_ids: List[str]) -> Dict[str, Any]:
        """Cancels orders by ID and unlocks reservations."""
        res = await self.client.cancel_orders(order_ids)
        for oid in res.get("canceled", []):
            order = next((o for o in self.pending_orders if o.order_id == oid), None)
            if order:
                if order.action == OrderAction.BUY:
                    self.wallet.unlock_buy(oid, label=self.slug)
                else:
                    self.wallet.unlock_sell(oid, label=self.slug)
                self.pending_orders = [o for o in self.pending_orders if o.order_id != oid]
                if self.user_channel:
                    self.user_channel.untrack_order(oid)
        return res

    async def emergency_sells(self, order_ids: List[str]):
        """Cancels specified limit sells and fires immediate FOK sells at market best bid."""
        self._log(f"[{self.slug}] Executing EMERGENCY SELL for orders: {order_ids}")
        await self.cancel_orders(order_ids)

        for tid in self.clob_token_ids:
            shares = self.wallet.available_shares(tid)
            if shares > 0:
                side = "UP" if tid == self.clob_token_ids[0] else "DOWN"
                book = self.up_book if side == "UP" else self.down_book
                best_bid = book.get_best_bid() if book else 0.01
                price = best_bid or 0.01

                fok_sell = OrderRequest(
                    token_id=tid,
                    action=OrderAction.SELL,
                    price=price,
                    shares=shares,
                    order_type=OrderType.FOK,
                    expire_at_ms=self.slot_end_ms,
                    on_filled=lambda s: self._log(f"[{self.slug}] Emergency FOK SELL filled: {s:.2f} shares @ {price:.2f}")
                )
                self.post_orders([fok_sell])

    def create_hold(self) -> Callable[[], None]:
        """Increments strategy lock and returns release function."""
        self.strategy_locks += 1
        released = False

        def release():
            nonlocal released
            if not released:
                released = True
                self.strategy_locks = max(0, self.strategy_locks - 1)
                self._log(f"[{self.slug}] Strategy hold released. Remaining holds: {self.strategy_locks}")

        return release

    def block_buys(self):
        self.buy_blocked = True

    def block_sells(self):
        self.sell_blocked = True

    def destroy(self):
        """Releases sockets and active timers."""
        self._clob_stop.set()
        if self._clob_task and not self._clob_task.done():
            self._clob_task.cancel()
