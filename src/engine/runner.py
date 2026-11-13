"""Top-level orchestrator for rolling Polymarket markets."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import Any

from ..collectors.price_tracker import PriceTracker
from ..config import (
    DEFAULT_ASSET,
    DEFAULT_WINDOW,
    PROJECT_ROOT,
    SIM_DELAY_MS,
    validate_market,
)
from ..utils.slot import get_market_slug
from .client import BaseTradingClient, ProdTradingClient, SimTradingClient
from .market_lifecycle import MarketLifecycle
from .state import load_state, save_state
from .strategies import DEFAULT_STRATEGY, STRATEGIES
from .types import BookSnapshot, LifecycleState
from .wallet import WalletTracker

logger = logging.getLogger("TradingEngine")


class TradingEngine:
    def __init__(
        self,
        strategy_name: str = DEFAULT_STRATEGY,
        slot_offset: int = 1,
        prod: bool = False,
        rounds: int | None = None,
        asset: str = DEFAULT_ASSET,
        window: str = DEFAULT_WINDOW,
        wallet_balance: float = 50.0,
        max_session_loss: float = 3.0,
        always_log: bool = False,
        state_dir: Path | None = None,
    ) -> None:
        strategy = STRATEGIES.get(strategy_name)
        if strategy is None:
            raise ValueError(
                f"Unknown strategy {strategy_name!r}; choose from {sorted(STRATEGIES)}"
            )
        asset = asset.lower()
        window = window.lower()
        validate_market(asset, window)
        if slot_offset < 0:
            raise ValueError("slot_offset must be zero or greater")
        if prod and strategy_name in {"late-entry", "simulation"}:
            raise ValueError(
                f"{strategy_name!r} is intentionally paper-only; add and review a "
                "dedicated live strategy before enabling production orders"
            )

        self.strategy_name = strategy_name
        self.strategy_fn = strategy
        self.slot_offset = slot_offset
        self.prod = prod
        self.rounds = None if rounds is None or rounds <= 0 else rounds
        self.asset = asset
        self.window = window
        self.max_session_loss = float(max_session_loss)
        self.always_log = always_log

        self.state_dir = Path(state_dir or PROJECT_ROOT / "state")
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.state_dir / ("engine_prod.json" if prod else "engine_sim.json")

        self.session_pnl = 0.0
        self.session_loss = 0.0
        self.rounds_created = 0
        self.is_running = True
        self.shutting_down = False
        self._last_save_time = 0.0
        self._lifecycles: dict[str, MarketLifecycle] = {}
        self._completed_slugs: set[str] = set()
        self._completed_markets: list[dict[str, Any]] = []

        self.ticker = PriceTracker(symbol=self.asset)
        self.wallet = WalletTracker(wallet_balance, log_fn=logger.info)
        if prod:
            self.client: BaseTradingClient = ProdTradingClient(
                private_key=os.getenv("POLYMARKET_PRIVATE_KEY", ""),
                funder_address=os.getenv("POLYMARKET_DEPOSIT_WALLET", ""),
            )
        else:
            self.client = SimTradingClient(
                book_provider=self._get_book_for_token,
                sim_delay_ms=SIM_DELAY_MS,
            )

    def _get_book_for_token(self, token_id: str) -> BookSnapshot:
        for lifecycle in self._lifecycles.values():
            snapshot = lifecycle.get_book_snapshot(token_id)
            if snapshot.best_ask is not None or snapshot.best_bid is not None:
                return snapshot
        return BookSnapshot()

    async def start(self) -> None:
        logger.info(
            "Starting engine | strategy=%s mode=%s asset=%s window=%s rounds=%s",
            self.strategy_name,
            "production" if self.prod else "simulation",
            self.asset.upper(),
            self.window,
            self.rounds if self.rounds is not None else "unlimited",
        )
        ticker_stop = asyncio.Event()
        ticker_task = asyncio.create_task(self.ticker.stream(ticker_stop), name="reference-prices")

        try:
            await self.client.init()
            self._restore_state()
            ready = await self.ticker.wait_for_ready(ticker_stop, timeout_seconds=30.0)
            if not ready:
                logger.warning(
                    "Price feeds are not all ready; late-entry stays fail-closed "
                    "until Binance and Coinbase are both current"
                )

            while self.is_running:
                await self._engine_tick()
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            raise
        finally:
            ticker_stop.set()
            for lifecycle in self._lifecycles.values():
                await lifecycle.destroy()
            self._lifecycles.clear()
            ticker_task.cancel()
            await asyncio.gather(ticker_task, return_exceptions=True)
            await self.client.close()
            self._persist_state()
            logger.info(
                "Engine stopped | session PnL=%+.2f rounds=%d",
                self.session_pnl,
                len(self._completed_markets),
            )

    def _restore_state(self) -> None:
        saved = load_state(self.state_file)
        if not saved:
            return
        self.session_pnl = float(saved.get("session_pnl", 0.0))
        self.session_loss = float(saved.get("session_loss", 0.0))
        self._completed_markets = list(saved.get("completed_markets", []))
        self._completed_slugs = {
            str(row["slug"])
            for row in self._completed_markets
            if isinstance(row, dict) and row.get("slug")
        }
        wallet = saved.get("wallet")
        if isinstance(wallet, dict):
            self.wallet.restore_snapshot(wallet)
        if saved.get("active_markets"):
            logger.warning(
                "Saved active markets require exchange reconciliation and are not "
                "automatically replayed; wallet reservations remain persisted"
            )
        logger.info("Restored engine state from %s", self.state_file)

    async def _engine_tick(self) -> None:
        now = time.time()
        exhausted = self.rounds is not None and self.rounds_created >= self.rounds

        if not self.shutting_down and not exhausted:
            slug = get_market_slug(self.asset, self.window, self.slot_offset)
            if slug not in self._lifecycles and slug not in self._completed_slugs:
                self._lifecycles[slug] = MarketLifecycle(
                    slug=slug,
                    client=self.client,
                    wallet=self.wallet,
                    price_tracker=self.ticker,
                    strategy_name=self.strategy_name,
                    strategy_fn=self.strategy_fn,
                    log_fn=logger.info,
                )
                self.rounds_created += 1
                logger.info("Created lifecycle %s", slug)

        done: list[str] = []
        for slug, lifecycle in tuple(self._lifecycles.items()):
            await lifecycle.tick()
            if lifecycle.state is LifecycleState.DONE:
                done.append(slug)

        for slug in done:
            lifecycle = self._lifecycles.pop(slug)
            self.session_pnl = round(self.session_pnl + lifecycle.pnl, 4)
            if lifecycle.pnl < 0:
                self.session_loss = round(self.session_loss + lifecycle.pnl, 4)
            self._completed_markets.append(
                {
                    "slug": slug,
                    "strategy": lifecycle.strategy_name,
                    "pnl": lifecycle.pnl,
                    "orders_count": len(lifecycle.order_history),
                }
            )
            self._completed_slugs.add(slug)
            logger.info("Round %s complete | PnL=%+.2f", slug, lifecycle.pnl)

            if abs(self.session_loss) >= self.max_session_loss:
                self.shutdown("session loss limit reached")

        if now - self._last_save_time >= 5.0:
            self._persist_state()
            self._last_save_time = now

        if not self.shutting_down and exhausted and not self._lifecycles:
            self.shutdown("round target reached")
        if self.shutting_down and not self._lifecycles:
            self.is_running = False

    def shutdown(self, reason: str = "shutdown requested") -> None:
        if self.shutting_down:
            return
        logger.info("Shutdown: %s", reason)
        self.shutting_down = True
        for lifecycle in self._lifecycles.values():
            lifecycle.shutdown()

    def _persist_state(self) -> None:
        active = [
            {
                "slug": lifecycle.slug,
                "state": lifecycle.state.value,
                "pending_orders": [order.to_snapshot() for order in lifecycle.pending_orders],
                "order_history": [
                    {
                        "action": order.action.value,
                        "price": order.price,
                        "shares": order.shares,
                        "fee": order.fee,
                        "token_id": order.token_id,
                    }
                    for order in lifecycle.order_history
                ],
                "condition_id": lifecycle.condition_id,
                "clob_token_ids": lifecycle.clob_token_ids,
            }
            for lifecycle in self._lifecycles.values()
        ]
        save_state(
            self.state_file,
            {
                "session_pnl": self.session_pnl,
                "session_loss": self.session_loss,
                "wallet": self.wallet.to_snapshot(),
                "active_markets": active,
                "completed_markets": self._completed_markets,
            },
        )
