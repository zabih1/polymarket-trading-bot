"""
Late-Entry High-Conviction Event-Driven Strategy.
Enters late in the market window (T <= 90s) under low volatility (ATR <= 2.0),
strong directional buffer (Gap Safety >= 40.0), and tight trend retention (Peak Gap Ratio >= 0.75).
Protected by dynamic stop-loss suppression at 0.48.
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Callable, Optional, Dict, Any

from .base import StrategyContext
from ..types import OrderRequest, OrderAction, OrderType
from ..indicators import Indicators

SHARES_PER_TRADE = 6.0
STOP_LOSS_PRICE = 0.48


@dataclass
class Position:
    side: str
    token_id: str
    entry_price: float
    shares: float
    stop_loss_price: float


class LateEntryState:
    def __init__(self):
        self.has_entered = False
        self.position: Optional[Position] = None
        self.stop_loss_fired = False


def check_entry_signal(
    remaining: float,
    btc_price: float,
    price_to_beat: float,
    up_info: Optional[Dict[str, float]],
    down_info: Optional[Dict[str, float]],
    atr: Optional[float],
    gap_safety: Optional[float],
    divergence: Optional[float],
    peak_gap_ratio: Optional[float]
) -> Optional[Dict[str, Any]]:
    """Evaluates strict entry filters."""
    if remaining < 5.0:
        return None

    div = divergence if divergence is not None else 0.0

    if (
        remaining <= 90.0 and
        atr is not None and atr <= 2.0 and
        gap_safety is not None and gap_safety >= 40.0 and
        div <= 10.0 and
        peak_gap_ratio is not None and peak_gap_ratio >= 0.75
    ):
        up_certain = up_info is not None and up_info["price"] > 0.85
        down_certain = down_info is not None and down_info["price"] > 0.85

        if up_certain or down_certain:
            side = "UP" if up_certain else "DOWN"
            info = up_info if side == "UP" else down_info

            if info and info.get("liquidity", 0.0) >= 20.0:
                gap = abs(btc_price - price_to_beat)
                return {
                    "side": side,
                    "ask": info["price"],
                    "gap": gap,
                    "liquidity": info["liquidity"],
                    "stop_loss_price": STOP_LOSS_PRICE
                }

    return None


async def late_entry_strategy(ctx: StrategyContext) -> Optional[Callable[[], None]]:
    """
    Late-Entry Event-Driven Strategy implementation.
    """
    ctx.log(f"[{ctx.slug}] [LateEntry] Registered and holding lifecycle lock.")
    release_lock = ctx.hold()

    state = LateEntryState()
    indicators = Indicators()
    is_running = True

    async def strategy_loop():
        nonlocal is_running
        while is_running:
            now_ms = time.time() * 1000
            remaining = (ctx.slot_end_ms - now_ms) / 1000.0

            if remaining <= 0:
                break

            # If within last 5s with no position, exit strategy cleanly
            if remaining <= 5.0 and not state.position:
                ctx.log(f"[{ctx.slug}] [LateEntry] Expiry reached with no entry. Releasing hold.")
                release_lock()
                break

            market_data = ctx.get_market_result()
            price_to_beat = market_data.get("open_price") if market_data else None

            if price_to_beat is not None:
                btc_price = ctx.ticker.price
                gap = (btc_price - price_to_beat) if btc_price is not None else None

                # Tick indicators
                indicators.tick(gap, btc_price)

                # 1. Evaluate Entry
                if not state.has_entered and btc_price is not None:
                    up_info = ctx.order_book.best_ask_info("UP")
                    down_info = ctx.order_book.best_ask_info("DOWN")

                    signal = check_entry_signal(
                        remaining=remaining,
                        btc_price=btc_price,
                        price_to_beat=price_to_beat,
                        up_info=up_info,
                        down_info=down_info,
                        atr=indicators.atr,
                        gap_safety=indicators.gap_safety(gap),
                        divergence=ctx.ticker.divergence,
                        peak_gap_ratio=indicators.peak_gap_ratio(gap)
                    )

                    if signal:
                        state.has_entered = True
                        side = signal["side"]
                        ask = signal["ask"]
                        token_id = ctx.clob_token_ids[0] if side == "UP" else ctx.clob_token_ids[1]

                        ctx.log(
                            f"[{ctx.slug}] [LateEntry] SIGNAL FIRED: {side} @ {ask:.3f} | "
                            f"Gap: ${signal['gap']:.2f} | Liq: ${signal['liquidity']:.1f}"
                        )

                        def on_filled(filled_shares: float):
                            state.position = Position(
                                side=side,
                                token_id=token_id,
                                entry_price=ask,
                                shares=filled_shares,
                                stop_loss_price=signal["stop_loss_price"]
                            )
                            ctx.log(
                                f"[{ctx.slug}] [LateEntry] BUY {side} Filled: {filled_shares:.2f} shares @ {ask:.3f}"
                            )

                        def on_failed(reason: str):
                            ctx.log(f"[{ctx.slug}] [LateEntry] BUY {side} Failed ({reason}). Resetting entry.")
                            state.has_entered = False

                        order = OrderRequest(
                            token_id=token_id,
                            action=OrderAction.BUY,
                            price=ask,
                            shares=SHARES_PER_TRADE,
                            order_type=OrderType.GTC,
                            expire_at_ms=ctx.slot_end_ms,
                            on_filled=on_filled,
                            on_failed=on_failed,
                            on_expired=lambda: setattr(state, 'has_entered', False)
                        )
                        ctx.post_orders([order])

                # 2. Evaluate Dynamic Stop-Loss
                if state.position and not state.stop_loss_fired:
                    pos = state.position
                    side_ask_info = ctx.order_book.best_ask_info(pos.side)
                    best_ask = side_ask_info["price"] if side_ask_info else None
                    best_bid = ctx.order_book.best_bid_price(pos.side)

                    GAP_CONFIRM_THRESHOLD = 5.0
                    gap_confirms = (
                        gap is not None and (
                            (pos.side == "UP" and gap > GAP_CONFIRM_THRESHOLD) or
                            (pos.side == "DOWN" and gap < -GAP_CONFIRM_THRESHOLD)
                        )
                    )
                    rsi = indicators.rsi
                    rsi_confirms = (
                        rsi is not None and (
                            (pos.side == "UP" and rsi >= 50.0) or
                            (pos.side == "DOWN" and rsi <= 50.0)
                        )
                    )

                    should_sell = False
                    if best_ask is not None and best_ask <= pos.stop_loss_price:
                        if 20.0 <= remaining <= 80.0 and not gap_confirms and not rsi_confirms:
                            should_sell = True
                        elif remaining < 20.0 and not gap_confirms:
                            should_sell = True

                    if should_sell:
                        state.stop_loss_fired = True
                        sell_price = round(best_bid + 0.01 if best_bid is not None else pos.stop_loss_price - 0.01, 2)
                        ctx.log(
                            f"[{ctx.slug}] [LateEntry] STOP-LOSS TRIGGERED: Selling {pos.side} @ {sell_price:.2f}"
                        )

                        def on_stop_expired():
                            ctx.log(f"[{ctx.slug}] [LateEntry] Stop-loss limit order expired. Executing emergency sell.")
                            sell_ids = [o.order_id for o in ctx.pending_orders if o.action == OrderAction.SELL]
                            if sell_ids:
                                asyncio.create_task(ctx.emergency_sells(sell_ids))

                        stop_order = OrderRequest(
                            token_id=pos.token_id,
                            action=OrderAction.SELL,
                            price=sell_price,
                            shares=pos.shares,
                            order_type=OrderType.GTC,
                            expire_at_ms=ctx.slot_end_ms - 5000,
                            on_filled=lambda s: ctx.log(f"[{ctx.slug}] [LateEntry] Stop-loss SELL Filled: {s:.2f} shares"),
                            on_expired=on_stop_expired
                        )
                        ctx.post_orders([stop_order])

            await asyncio.sleep(0.5)

    loop_task = asyncio.create_task(strategy_loop())

    def cleanup():
        nonlocal is_running
        is_running = False
        loop_task.cancel()

    return cleanup
