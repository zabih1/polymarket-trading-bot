from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.collectors.price_tracker import PriceTracker
from src.engine.client import ProdTradingClient, SimTradingClient, is_sim_filled
from src.engine.market_lifecycle import MarketLifecycle
from src.engine.strategies.simulation import simulation_strategy
from src.engine.types import BookSnapshot, OrderAction, OrderRequest, OrderType, PendingOrder
from src.engine.user_channel import LiveUserChannel, UserChannel
from src.engine.wallet import WalletTracker


def test_sim_fill_requires_strictly_more_than_two_x_notional() -> None:
    order_notional = 0.9 * 6
    equal = BookSnapshot(best_ask=0.9, best_ask_liquidity=order_notional * 2)
    above = BookSnapshot(best_ask=0.9, best_ask_liquidity=order_notional * 2 + 0.01)

    assert not is_sim_filled(OrderAction.BUY, 0.9, 6, equal)
    assert is_sim_filled(OrderAction.BUY, 0.9, 6, above)


@pytest.mark.asyncio
async def test_crossing_gtc_rests_instead_of_filling_at_placement() -> None:
    book = BookSnapshot(best_ask=0.4, best_ask_liquidity=100)
    client = SimTradingClient(lambda _token: book, sim_delay_ms=0)
    result = await client.post_orders(
        [
            OrderRequest(
                token_id="up",
                action=OrderAction.BUY,
                price=0.5,
                shares=5,
                order_type=OrderType.GTC,
            )
        ]
    )

    assert result[0].success
    assert result[0].status == "live"


class _RecordingSdk:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def place_limit_order(self, **kwargs):
        self.calls.append(("limit", kwargs))
        return SimpleNamespace(ok=True, order_id="limit-1", status="live")

    async def place_market_order(self, **kwargs):
        self.calls.append(("market", kwargs))
        return SimpleNamespace(ok=True, order_id="market-1", status="matched")


@pytest.mark.asyncio
async def test_production_adapter_uses_unified_sdk_order_shapes() -> None:
    sdk = _RecordingSdk()
    client = ProdTradingClient("0x-test")
    client._client = sdk

    results = await client.post_orders(
        [
            OrderRequest("up", OrderAction.BUY, 0.9, 6, OrderType.GTC),
            OrderRequest("down", OrderAction.SELL, 0.4, 3, OrderType.FOK),
        ]
    )

    assert [result.status for result in results] == ["live", "matched"]
    assert sdk.calls[0][0] == "limit"
    assert sdk.calls[0][1]["token_id"] == "up"
    assert sdk.calls[0][1]["side"] == "BUY"
    assert sdk.calls[1][0] == "market"
    assert sdk.calls[1][1]["token_id"] == "down"
    assert sdk.calls[1][1]["side"] == "SELL"
    assert sdk.calls[1][1]["order_type"] == "FOK"


def test_sim_user_channel_waits_for_settlement_delay() -> None:
    now = [1_000.0]
    fills: list[float] = []
    channel = UserChannel(
        lambda _token: BookSnapshot(best_ask=0.4, best_ask_liquidity=100),
        settlement_delay_ms=4_000,
        time_ms=lambda: now[0],
    )
    channel.track_order(
        "order-1",
        OrderRequest(
            token_id="up",
            action=OrderAction.BUY,
            price=0.5,
            shares=5,
            on_filled=fills.append,
        ),
    )

    channel.check_fills()
    assert channel.is_matched("order-1")
    assert fills == []
    now[0] += 3_999
    channel.check_fills()
    assert fills == []
    now[0] += 1
    channel.check_fills()
    assert fills == [5]


def test_live_channel_buffers_trade_before_order_registration() -> None:
    fills: list[float] = []
    channel = LiveUserChannel(lambda _condition: None)
    matched = SimpleNamespace(
        id="trade-1",
        status="MATCHED",
        taker_order_id="order-1",
        size=5,
        maker_orders=(),
    )
    mined = SimpleNamespace(
        id="trade-1",
        status="MINED",
        taker_order_id="order-1",
        size=5,
        maker_orders=(),
    )

    channel._process_trade(matched)
    channel.track_order(
        "order-1",
        OrderRequest(
            token_id="up",
            action=OrderAction.BUY,
            price=0.5,
            shares=5,
            on_filled=fills.append,
        ),
    )
    channel._process_trade(mined)

    assert fills == [5]


class _CancelClient:
    async def cancel_orders(self, order_ids):
        return {"canceled": list(order_ids), "not_canceled": {}}


class _PartialChannel:
    def __init__(self, shares: float) -> None:
        self.shares = shares

    def is_matched(self, _order_id: str) -> bool:
        return False

    def get_matched_so_far(self, _order_id: str) -> float:
        return self.shares

    def untrack_order(self, _order_id: str) -> None:
        return None

    def track_order(self, *_args, **_kwargs) -> None:
        return None

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_expiry_commits_settled_partial_fill_before_cancel() -> None:
    wallet = WalletTracker(50.0)
    lifecycle = MarketLifecycle(
        slug="btc-updown-5m-1800000000",
        client=_CancelClient(),
        wallet=wallet,
        price_tracker=PriceTracker(),
        strategy_name="simulation",
        strategy_fn=simulation_strategy,
    )
    request = OrderRequest(
        token_id="up",
        action=OrderAction.BUY,
        price=0.9,
        shares=6.0,
        expire_at_ms=0.0,
    )
    wallet.lock_for_buy("partial-order", request.price, request.shares)
    lifecycle.pending_orders.append(
        PendingOrder(
            order_id="partial-order",
            token_id=request.token_id,
            action=request.action,
            price=request.price,
            shares=request.shares,
            expire_at_ms=request.expire_at_ms,
        )
    )
    lifecycle.user_channel = _PartialChannel(2.0)

    await lifecycle._check_expiries()

    assert lifecycle.pending_orders == []
    assert lifecycle.order_history[0].shares == 2.0
    assert wallet.total_shares("up") == 2.0
    assert wallet.balance == pytest.approx(48.2)
    await lifecycle.destroy()
