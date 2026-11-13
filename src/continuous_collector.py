"""
Continuous Multi-Slot Data Collector for Polymarket Binary Markets.
Runs continuously across consecutive market slots, capturing 1-second Level-2
order books, multi-venue reference prices, and Chainlink oracle settlement.
"""

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .collectors.clob_client import ClobClient, LocalOrderBook
from .collectors.gamma_client import GammaClient
from .collectors.oracle_client import OracleClient
from .collectors.price_tracker import PriceTracker
from .config import (
    DEFAULT_ASSET,
    DEFAULT_WINDOW,
    INTERVAL_SECONDS,
    RAW_DATA_DIR,
    validate_market,
)
from .utils.slot import get_slot_timestamps

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ContinuousCollector")


class ContinuousCollector:
    """
    Orchestrates continuous multi-slot market collection across consecutive rolling windows.
    """

    def __init__(
        self,
        asset: str = DEFAULT_ASSET,
        window: str = DEFAULT_WINDOW,
        rounds: int = 0,
        output_dir: Path = RAW_DATA_DIR,
    ):
        self.asset = asset.lower()
        self.window = window.lower()
        validate_market(self.asset, self.window)
        self.rounds = rounds  # 0 = infinite continuous
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.interval = INTERVAL_SECONDS.get(self.window, 300)

        self.gamma = GammaClient()
        self.oracle = OracleClient()
        self.clob = ClobClient()
        self.price_tracker = PriceTracker(symbol=self.asset)

        self.is_running = True
        self.current_slot_records: list[dict[str, Any]] = []
        self.current_slug: str | None = None
        self.rounds_completed = 0

    def shutdown(self, sig_name: str = "SIGINT"):
        """Gracefully halts collection and flushes current data to disk."""
        logger.info(f"Shutdown requested via {sig_name}. Finalizing current slot...")
        self.is_running = False
        if self.current_slug and self.current_slot_records:
            self._save_slot_data(self.current_slug, self.current_slot_records)

    async def run(self):
        """Main multi-slot loop."""
        logger.info("=" * 70)
        logger.info("Starting Continuous Multi-Slot Collector")
        logger.info(f"Asset: {self.asset.upper()} | Window: {self.window} ({self.interval}s)")
        logger.info(f"Rounds: {'Unlimited (Continuous)' if self.rounds <= 0 else self.rounds}")
        logger.info(f"Save Directory: {self.output_dir.resolve()}")
        logger.info("=" * 70)

        # Start the same multi-feed reference-price tracker used by the engine.
        price_stop = asyncio.Event()
        price_task = asyncio.create_task(self.price_tracker.stream(price_stop))

        next_slot = get_slot_timestamps(offset=1, window=self.window)
        try:
            while self.is_running:
                # Check round limit
                if self.rounds > 0 and self.rounds_completed >= self.rounds:
                    logger.info(
                        f"Target rounds reached ({self.rounds_completed}/{self.rounds}). Exiting."
                    )
                    break

                # Start with the upcoming slot so feeds can subscribe before
                # open. Afterwards advance contiguously without skipping a slot.
                slot_start = next_slot["start_time"]
                slot_end = next_slot["end_time"]
                slug = f"{self.asset}-updown-{self.window}-{slot_start}"
                self.current_slug = slug

                now = int(time.time())
                rem = slot_end - now

                logger.info(
                    f"\n>>> [Slot {self.rounds_completed + 1}] Target: {slug} | Remaining: {rem}s"
                )

                # 2. Collect market metadata from Gamma API (with retries)
                metadata = await self._discover_market(slug, slot_end)
                if (
                    not metadata
                    or not metadata.get("up_token_id")
                    or not metadata.get("down_token_id")
                ):
                    logger.warning(f"Could not resolve token IDs for {slug} before it ended.")
                    next_slot = {
                        "start_time": slot_end,
                        "end_time": slot_end + self.interval,
                    }
                    continue

                # 3. Collect the slot
                await self._collect_slot(slug, slot_start, slot_end, metadata)
                self.rounds_completed += 1

                # 4. Fetch resolution after window close
                if self.is_running:
                    resolution = await self._fetch_resolution(slot_start, slot_end)
                    if resolution:
                        for record in self.current_slot_records:
                            record["chainlink_close_price"] = resolution.get("close_price")
                            record["market_winner"] = resolution.get("winner")
                            record["market_completed"] = resolution.get("completed")
                        self._save_slot_data(slug, self.current_slot_records)

                next_slot = {
                    "start_time": slot_end,
                    "end_time": slot_end + self.interval,
                }

        finally:
            price_stop.set()
            price_task.cancel()
            await asyncio.gather(price_task, return_exceptions=True)
            self.clob.session.close()
            self.gamma.session.close()
            self.oracle.session.close()
            logger.info("Continuous collector finished.")

    async def _discover_market(self, slug: str, deadline: int) -> dict[str, Any] | None:
        """Resolve metadata while the target slot is still useful."""
        attempt = 0
        while self.is_running and time.time() < deadline:
            attempt += 1
            metadata = await asyncio.to_thread(self.gamma.fetch_market_metadata, slug)
            if metadata and metadata.get("up_token_id"):
                logger.info(
                    f"Market Discovered: {metadata.get('title')}\n"
                    f"  Condition: {metadata.get('condition_id')}\n"
                    f"  UP Token:   {metadata.get('up_token_id')}\n"
                    f"  DOWN Token: {metadata.get('down_token_id')}"
                )
                return metadata
            if not self.is_running:
                return None
            logger.info(f"Market metadata not yet published (attempt {attempt}). Retrying in 1s...")
            await asyncio.sleep(1.0)
        return None

    async def _collect_slot(
        self, slug: str, slot_start: int, slot_end: int, metadata: dict[str, Any]
    ):
        """Collects 1-second snapshots for a single slot until slot_end."""
        up_token_id = metadata["up_token_id"]
        down_token_id = metadata["down_token_id"]

        up_book = LocalOrderBook(up_token_id)
        down_book = LocalOrderBook(down_token_id)
        self.current_slot_records = []

        # Start CLOB stream for this slot's tokens
        clob_stop = asyncio.Event()
        clob_task = asyncio.create_task(
            self.clob.stream_orderbooks(
                token_ids=[up_token_id, down_token_id],
                up_book=up_book,
                down_book=down_book,
                up_token_id=up_token_id,
                stop_event=clob_stop,
            )
        )

        # Subscribe while the market is upcoming, then begin recording at the
        # exact slot boundary rather than joining an active slot mid-window.
        while self.is_running and time.time() < slot_start:  # noqa: ASYNC110
            await asyncio.sleep(min(0.2, slot_start - time.time()))

        # Query Oracle Price to Beat (initial price)
        oracle_open_price: float | None = None
        while self.is_running and time.time() < slot_end:
            oracle_info = await self.oracle.fetch_settlement_price_async(
                slot_start, slot_end, self.asset, self.window
            )
            if oracle_info and oracle_info.get("open_price") is not None:
                oracle_open_price = oracle_info["open_price"]
                break
            if up_book.is_ready and down_book.is_ready and self.price_tracker.is_ready:
                await asyncio.sleep(0.2)
            else:
                await asyncio.sleep(0.1)

        if oracle_open_price is not None:
            logger.info(f"Price to Beat (Oracle Open): ${oracle_open_price:,.2f}")
        else:
            logger.warning(
                "Oracle Open Price not yet available at slot start; will retry during collection."
            )

        # Do not record half-initialized books or missing safety feeds.
        while self.is_running and time.time() < slot_end:
            if up_book.is_ready and down_book.is_ready and self.price_tracker.is_ready:
                logger.info("Order books and reference price feeds synchronized.")
                break
            await asyncio.sleep(0.2)

        # Snapshot recording loop
        last_log_time = 0.0
        while self.is_running and time.time() < slot_end:
            now_sec = time.time()
            rem = max(0.0, slot_end - now_sec)

            # Lazy poll oracle open price if still missing
            if oracle_open_price is None and int(now_sec) % 5 == 0:
                p_info = await self.oracle.fetch_settlement_price_async(
                    slot_start, slot_end, self.asset, self.window
                )
                if p_info and p_info.get("open_price") is not None:
                    oracle_open_price = p_info["open_price"]
                    logger.info(f"Updated Price to Beat (Oracle Open): ${oracle_open_price:,.2f}")

            now_utc = datetime.now(UTC).isoformat()
            up_depth = up_book.get_depth_summary(levels=5)
            down_depth = down_book.get_depth_summary(levels=5)
            asset_price = self.price_tracker.price
            binance_p = self.price_tracker.binance_price

            record = {
                "current_time": now_utc,
                "market_slug": slug,
                "condition_id": metadata.get("condition_id"),
                "slot_start": slot_start,
                "slot_end": slot_end,
                "chainlink_price_to_beat": oracle_open_price,
                "asset_current_price": asset_price,
                "polymarket_current_price": self.price_tracker.polymarket_price,
                "binance_current_price": binance_p,
                "coinbase_current_price": self.price_tracker.coinbase_price,
                "price_feed_divergence": self.price_tracker.divergence,
                "polymarket_up_best_bid": up_depth.get("best_bid"),
                "polymarket_up_best_ask": up_depth.get("best_ask"),
                "polymarket_up_best_bid_liquidity": up_depth.get("best_bid_liquidity"),
                "polymarket_up_best_ask_liquidity": up_depth.get("best_ask_liquidity"),
                "polymarket_up_spread": up_depth.get("spread"),
                "polymarket_up_bid_liquidity": up_depth.get("total_bid_liquidity"),
                "polymarket_up_ask_liquidity": up_depth.get("total_ask_liquidity"),
                "polymarket_down_best_bid": down_depth.get("best_bid"),
                "polymarket_down_best_ask": down_depth.get("best_ask"),
                "polymarket_down_best_bid_liquidity": down_depth.get("best_bid_liquidity"),
                "polymarket_down_best_ask_liquidity": down_depth.get("best_ask_liquidity"),
                "polymarket_down_spread": down_depth.get("spread"),
                "polymarket_down_bid_liquidity": down_depth.get("total_bid_liquidity"),
                "polymarket_down_ask_liquidity": down_depth.get("total_ask_liquidity"),
                "up_top5_bids": json.dumps(up_depth.get("top_bids", [])),
                "up_top5_asks": json.dumps(up_depth.get("top_asks", [])),
                "down_top5_bids": json.dumps(down_depth.get("top_bids", [])),
                "down_top5_asks": json.dumps(down_depth.get("top_asks", [])),
            }
            self.current_slot_records.append(record)

            # Throttled status log every 30 seconds
            if now_sec - last_log_time >= 30.0:
                gap_str = ""
                if asset_price and oracle_open_price:
                    gap = asset_price - oracle_open_price
                    gap_str = f" | Gap: {gap:+.2f}"
                up_ask = up_depth.get("best_ask")
                down_ask = down_depth.get("best_ask")
                logger.info(
                    f"[{slug}] Rem: {rem:04.1f}s | Snapshots: {len(self.current_slot_records)} | "
                    f"Asset: ${asset_price or 0:,.2f}{gap_str} | "
                    f"UP Ask: {up_ask} | DOWN Ask: {down_ask}"
                )
                last_log_time = now_sec

            await asyncio.sleep(1.0)

        # Stop CLOB WebSocket for this slot
        clob_stop.set()
        clob_task.cancel()
        await asyncio.gather(clob_task, return_exceptions=True)

        # Save slot CSV
        self._save_slot_data(slug, self.current_slot_records)

    def _save_slot_data(self, slug: str, records: list[dict[str, Any]]):
        """Flushes recorded slot records to CSV on disk."""
        if not records:
            logger.warning(f"No records collected for {slug}. Nothing saved.")
            return

        df = pd.DataFrame(records)
        out_csv = self.output_dir / f"{slug}.csv"
        df.to_csv(out_csv, index=False)
        logger.info(f" Saved {len(df)} snapshots to: {out_csv.resolve()}")

    async def _fetch_resolution(self, slot_start: int, slot_end: int) -> dict[str, Any] | None:
        """Wait until Polymarket publishes a completed result."""
        logger.info("Waiting for Polymarket Chainlink settlement...")
        await asyncio.sleep(4.0)

        attempt = 0
        while self.is_running:
            attempt += 1
            res = await self.oracle.fetch_settlement_price_async(
                slot_start, slot_end, self.asset, self.window
            )
            if res and res.get("completed"):
                open_p = res.get("open_price")
                close_p = res.get("close_price")
                winner = res.get("winner")
                diff = (close_p - open_p) if (open_p and close_p) else 0.0
                logger.info(
                    f" [Resolution] Result: {winner} | "
                    f"Open: ${open_p:,.2f} | Close: ${close_p:,.2f} (Delta: {diff:+.2f})"
                )
                return res
            if attempt % 10 == 0:
                logger.info("Settlement still pending after %d checks.", attempt)
            await asyncio.sleep(1.0)
        return None


def parse_args():
    parser = argparse.ArgumentParser(description="Continuous Polymarket Multi-Slot Collector")
    parser.add_argument(
        "--asset", type=str, default=DEFAULT_ASSET, help="Asset ticker (default: btc)"
    )
    parser.add_argument(
        "--window",
        type=str,
        default=DEFAULT_WINDOW,
        choices=["5m", "15m"],
        help="Timeframe window (default: 5m)",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=0,
        help="Number of slots to collect (default: 0 = unlimited continuous)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(RAW_DATA_DIR), help="Output folder for raw CSV files"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    collector = ContinuousCollector(
        asset=args.asset, window=args.window, rounds=args.rounds, output_dir=Path(args.output_dir)
    )

    # Attach signal handlers for graceful exit
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def handle_signal():
        collector.shutdown()

    if sys.platform != "win32":
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, handle_signal)
    else:
        # On Windows, register via signal.signal
        signal.signal(signal.SIGINT, lambda sig, frame: collector.shutdown("SIGINT"))
        signal.signal(signal.SIGTERM, lambda sig, frame: collector.shutdown("SIGTERM"))

    try:
        loop.run_until_complete(collector.run())
    except KeyboardInterrupt:
        collector.shutdown("KeyboardInterrupt")
    finally:
        loop.close()


if __name__ == "__main__":
    main()
