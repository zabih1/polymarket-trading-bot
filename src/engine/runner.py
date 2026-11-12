"""
Top-level Trading Engine Orchestrator.
Coordinates market lifecycle progression, session PnL accounting,
risk management circuit-breakers, and state persistence.
"""

import asyncio
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Any

from .types import LifecycleState
from .client import BaseTradingClient, SimTradingClient, ProdTradingClient
from .wallet import WalletTracker
from .market_lifecycle import MarketLifecycle
from .strategies import STRATEGIES, DEFAULT_STRATEGY
from .state import load_state, save_state
from ..collectors.binance_client import BinanceTickerTracker
from ..utils.slot import get_market_slug
from ..config import (
    DEFAULT_ASSET,
    DEFAULT_WINDOW,
    PROJECT_ROOT
)

logger = logging.getLogger("TradingEngine")


class TradingEngine:
    """
    Main engine driving multi-slot automated trading across Polymarket prediction rounds.
    """

    def __init__(
        self,
        strategy_name: str = DEFAULT_STRATEGY,
        slot_offset: int = 1,
        prod: bool = False,
        rounds: Optional[int] = None,
        asset: str = DEFAULT_ASSET,
        window: str = DEFAULT_WINDOW,
        wallet_balance: float = 50.0,
        max_session_loss: float = 3.0,
        always_log: bool = False,
        state_dir: Optional[Path] = None
    ):
        self.strategy_name = strategy_name
        self.strategy_fn = STRATEGIES.get(strategy_name)
        if not self.strategy_fn:
            raise ValueError(f"Unknown strategy: '{strategy_name}'. Available: {list(STRATEGIES.keys())}")

        self.slot_offset = slot_offset
        self.prod = prod
        self.rounds = rounds
        self.asset = asset.lower()
        self.window = window.lower()
        self.max_session_loss = float(max_session_loss)
        self.always_log = always_log

        # State paths
        self.state_dir = Path(state_dir or (PROJECT_ROOT / "state"))
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.state_dir / ("engine_prod.json" if prod else "engine_sim.json")

        # Session tracking
        self.session_pnl = 0.0
        self.session_loss = 0.0
        self.rounds_created = 0
        self.is_running = True
        self.shutting_down = False
        self._last_save_time = 0.0

        # Lifecycles
        self._lifecycles: Dict[str, MarketLifecycle] = {}
        self._completed_slugs: Set[str] = set()
        self._completed_markets: List[Dict[str, Any]] = []

        # Shared Ticker & Wallet
        self.ticker = BinanceTickerTracker(symbol=self.asset)
        self.wallet = WalletTracker(initial_balance=wallet_balance, log_fn=logger.info)

        # Trading Client
        if prod:
            self.client: BaseTradingClient = ProdTradingClient(private_key="")
        else:
            self.client = SimTradingClient(book_provider=self._get_book_for_token)

    def _get_book_for_token(self, token_id: str):
        """Dispatches book snapshot lookup across active market lifecycles."""
        for lc in self._lifecycles.values():
            snap = lc.get_book_snapshot(token_id)
            if snap and (snap.best_ask is not None or snap.best_bid is not None):
                return snap
        return SimTradingClient(lambda t: None)._orders.get(token_id)

    async def start(self):
        """Starts ticker, initializes state, and begins engine main loop."""
        logger.info("=" * 70)
        logger.info("Starting Automated Trading Engine")
        logger.info(f"Strategy: {self.strategy_name.upper()} | Mode: {'PROD (Real Funds)' if self.prod else 'SIMULATION (Paper Trading)'}")
        logger.info(f"Asset: {self.asset.upper()} | Timeframe: {self.window} | Slot Offset: +{self.slot_offset}")
        logger.info(f"Wallet Balance: ${self.wallet.balance:.2f} | Max Loss Limit: -${self.max_session_loss:.2f}")
        logger.info(f"Rounds: {'Unlimited' if self.rounds is None else self.rounds}")
        logger.info("=" * 70)

        # Start Binance Ticker in background
        ticker_stop = asyncio.Event()
        ticker_task = asyncio.create_task(self.ticker.stream_ticker(ticker_stop))

        # Wait for initial ticker update
        for _ in range(20):
            if self.ticker.is_ready:
                logger.info(f"[Startup] Reference Ticker Ready: ${self.ticker.current_price:,.2f}")
                break
            await asyncio.sleep(0.2)

        await self.client.init()

        # Load persisted session state if available
        saved = load_state(self.state_file)
        if saved:
            self.session_pnl = saved.get("session_pnl", 0.0)
            self.session_loss = saved.get("session_loss", 0.0)
            self._completed_markets = saved.get("completed_markets", [])
            logger.info(f"[Startup] Loaded state from {self.state_file} (Prior PnL: ${self.session_pnl:+.2f})")

        # Main Engine Loop
        try:
            while self.is_running:
                await self._engine_tick()
                await asyncio.sleep(0.1)  # 100ms engine cycle
        except asyncio.CancelledError:
            pass
        finally:
            ticker_stop.set()
            try:
                await asyncio.wait_for(ticker_task, timeout=2.0)
            except Exception:
                pass
            self._persist_state()
            logger.info(
                f"[Engine] Stopped. Final Session PnL: {'+' if self.session_pnl >= 0 else ''}${self.session_pnl:.2f} "
                f"across {len(self._completed_markets)} round(s)."
            )

    async def _engine_tick(self):
        """Core engine tick: manages lifecycle creation, state steps, and session PnL."""
        now = time.time()
        rounds_exhausted = (self.rounds is not None and self.rounds_created >= self.rounds)

        # 1. Discover and instantiate new upcoming market slot
        if not self.shutting_down and not rounds_exhausted:
            slug = get_market_slug(asset=self.asset, window=self.window, offset=self.slot_offset)
            if slug not in self._lifecycles and slug not in self._completed_slugs:
                logger.info(f"[Engine] Creating new MarketLifecycle for slot: {slug}")
                lifecycle = MarketLifecycle(
                    slug=slug,
                    client=self.client,
                    wallet=self.wallet,
                    binance_tracker=self.ticker,
                    strategy_name=self.strategy_name,
                    strategy_fn=self.strategy_fn,
                    log_fn=logger.info
                )
                self._lifecycles[slug] = lifecycle
                self.rounds_created += 1

        # 2. Advance all active lifecycles
        done_slugs = []
        for slug, lc in list(self._lifecycles.items()):
            await lc.tick()
            if lc.state == LifecycleState.DONE:
                done_slugs.append(slug)

        # 3. Handle completed lifecycles
        for slug in done_slugs:
            lc = self._lifecycles.pop(slug)
            self.session_pnl = round(self.session_pnl + lc.pnl, 4)
            if lc.pnl < 0:
                self.session_loss = round(self.session_loss + lc.pnl, 4)

            logger.info(
                f"[{slug}] Round Complete! PnL: {lc.pnl:+.2f} | "
                f"Total Session PnL: {'+' if self.session_pnl >= 0 else ''}${self.session_pnl:.2f}"
            )
            self._completed_markets.append({
                "slug": slug,
                "strategy": lc.strategy_name,
                "pnl": lc.pnl,
                "orders_count": len(lc.order_history)
            })
            self._completed_slugs.add(slug)

            # Check risk circuit-breaker
            if abs(self.session_loss) >= self.max_session_loss:
                logger.warning(
                    f"[CircuitBreaker] Session loss limit reached (-${abs(self.session_loss):.2f} >= -${self.max_session_loss:.2f}). "
                    f"Initiating graceful auto-shutdown."
                )
                self.shutdown("MAX_SESSION_LOSS limit reached")

        # 4. Periodic state persistence (every 5 seconds)
        if now - self._last_save_time >= 5.0:
            self._persist_state()
            self._last_save_time = now

        # 5. Check if all rounds completed
        if not self.shutting_down and rounds_exhausted and len(self._lifecycles) == 0:
            logger.info(f"[Engine] All {self.rounds} target round(s) complete. Shutting down.")
            self.shutdown("Rounds target reached")

        # 6. Exit when all settled during shutdown
        if self.shutting_down and len(self._lifecycles) == 0:
            self.is_running = False

    def shutdown(self, reason: str = "Signal received"):
        """Gracefully halts the engine, cancelling buys and settling positions."""
        if self.shutting_down:
            return
        logger.info(f"[Shutdown] {reason}. Cancelling active placements and draining markets...")
        self.shutting_down = True
        for lc in self._lifecycles.values():
            if lc.state == LifecycleState.RUNNING:
                lc.state = LifecycleState.STOPPING

    def _persist_state(self):
        """Writes current session metrics to disk."""
        active = [
            {
                "slug": lc.slug,
                "state": lc.state.value,
                "pending_orders": [o.to_snapshot() for o in lc.pending_orders],
                "unfilled_shares": sum(self.wallet.total_shares(t) for t in lc.clob_token_ids)
            }
            for lc in self._lifecycles.values()
        ]
        data = {
            "session_pnl": self.session_pnl,
            "session_loss": self.session_loss,
            "active_markets": active,
            "completed_markets": self._completed_markets
        }
        save_state(self.state_file, data)
