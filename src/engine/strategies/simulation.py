"""
Baseline Limit Order Simulation Strategy.
Designed for paper-trading validation: places discount maker buy at 0.49,
takes profit at 0.70 on fill, and emergency exits if unfilled with 30s remaining.
"""

import asyncio
import time
from typing import Callable, Optional
from .base import StrategyContext
from ..types import OrderRequest, OrderAction, OrderType


async def simulation_strategy(ctx: StrategyContext) -> Optional[Callable[[], None]]:
    """
    Simulation Strategy:
    1. Places a GTC buy at 0.49 on the UP side.
    2. When filled, places a take-profit sell at 0.70 expiring 30s before close.
    3. If sell expires, fires emergency sell at market bid to exit cleanly.
    """
    up_token_id = ctx.clob_token_ids[0]
    ctx.log(f"[{ctx.slug}] [SimulationStrategy] Initializing for UP token: {up_token_id[:12]}...")

    cleanup_tasks = []

    def on_buy_filled(filled_shares: float):
        ctx.log(f"[{ctx.slug}] [SimulationStrategy] BUY filled: {filled_shares:.2f} shares @ 0.49. Placing take-profit @ 0.70")

        # Step 2: Place take-profit sell at 0.70 expiring 30s before slot close
        sell_req = OrderRequest(
            token_id=up_token_id,
            action=OrderAction.SELL,
            price=0.70,
            shares=filled_shares,
            order_type=OrderType.GTC,
            expire_at_ms=ctx.slot_end_ms - 30_000,
            on_filled=lambda s: ctx.log(f"[{ctx.slug}] [SimulationStrategy] TAKE-PROFIT SELL filled: {s:.2f} shares @ 0.70!"),
            on_expired=lambda: asyncio.create_task(on_sell_expired(up_token_id, filled_shares))
        )
        ctx.post_orders([sell_req])

    async def on_sell_expired(token_id: str, shares: float):
        ctx.log(f"[{ctx.slug}] [SimulationStrategy] Take-profit sell expired at T-30s. Triggering emergency sell exit.")
        sell_ids = [o.order_id for o in ctx.pending_orders if o.action == OrderAction.SELL]
        if sell_ids:
            await ctx.emergency_sells(sell_ids)

    # Step 1: Place initial discount buy order
    initial_buy = OrderRequest(
        token_id=up_token_id,
        action=OrderAction.BUY,
        price=0.49,
        shares=5.0,
        order_type=OrderType.GTC,
        expire_at_ms=ctx.slot_end_ms - 100_000,
        on_filled=on_buy_filled,
        on_expired=lambda: ctx.log(f"[{ctx.slug}] [SimulationStrategy] Initial buy order expired at T-100s without fill.")
    )
    ctx.post_orders([initial_buy])

    def cleanup():
        for t in cleanup_tasks:
            t.cancel()

    return cleanup
