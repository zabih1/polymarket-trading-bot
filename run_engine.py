"""
Top-level entry point for the Automated Polymarket Production Trading Engine.

Usage Examples:
    # Run paper-trading simulation on Late-Entry Strategy for 5 rounds:
    python run_engine.py --strategy late-entry --rounds 5 --slot-offset 1

    # Run Simulation strategy with custom wallet balance and loss limit:
    python run_engine.py --strategy simulation --wallet-balance 100 --max-session-loss 5

    # Built-in strategies are paper-only. Production mode is available only to
    # separately reviewed live strategies registered in STRATEGIES.
"""

import argparse
import asyncio
import logging
import signal
import sys

from src.engine.runner import TradingEngine
from src.engine.strategies import DEFAULT_STRATEGY, STRATEGIES


def parse_args():
    parser = argparse.ArgumentParser(
        description="Automated Trading Engine for Polymarket Binary Prediction Markets"
    )
    parser.add_argument(
        "-s",
        "--strategy",
        type=str,
        default=DEFAULT_STRATEGY,
        choices=list(STRATEGIES.keys()),
        help=f"Strategy to run ({', '.join(STRATEGIES.keys())})",
    )
    parser.add_argument(
        "--slot-offset",
        type=int,
        default=1,
        help="Market slot offset: 1 = next upcoming slot (pre-enter), 0 = active slot",
    )
    parser.add_argument(
        "--prod",
        action="store_true",
        default=False,
        help=(
            "Run a reviewed live strategy against Polymarket CLOB "
            "(requires POLYMARKET_PRIVATE_KEY and POLYMARKET_DEPOSIT_WALLET)"
        ),
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=None,
        help="Number of market rounds to trade then exit (omit or 0 for unlimited continuous)",
    )
    parser.add_argument(
        "--asset", type=str, default="btc", help="Target asset symbol (default: btc)"
    )
    parser.add_argument(
        "--window",
        type=str,
        default="5m",
        choices=["5m", "15m"],
        help="Market duration window (default: 5m)",
    )
    parser.add_argument(
        "--wallet-balance",
        type=float,
        default=50.0,
        help="Initial wallet balance for paper-trading (default: $50.00)",
    )
    parser.add_argument(
        "--max-session-loss",
        type=float,
        default=3.0,
        help=(
            "Maximum cumulative session loss ($) before circuit breaker "
            "auto-shutdown (default: $3.00)"
        ),
    )
    parser.add_argument(
        "--always-log",
        action="store_true",
        default=False,
        help="Always write verbose log snapshots even if no orders placed",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level verbosity",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Configure logging
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    rounds_val = args.rounds if (args.rounds and args.rounds > 0) else None

    # Safety confirmation prompt for production mode
    if args.prod:
        confirm = input(
            "\n[WARNING] Run in PRODUCTION mode with REAL FUNDS on Polygon? Enter 'Y' to confirm: "
        )
        if confirm.strip() != "Y":
            print("Production run aborted.")
            sys.exit(0)

    engine = TradingEngine(
        strategy_name=args.strategy,
        slot_offset=args.slot_offset,
        prod=args.prod,
        rounds=rounds_val,
        asset=args.asset,
        window=args.window,
        wallet_balance=args.wallet_balance,
        max_session_loss=args.max_session_loss,
        always_log=args.always_log,
    )

    # Attach signal handlers for graceful exit
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def handle_signal():
        engine.shutdown("Signal received")

    if sys.platform != "win32":
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, handle_signal)
    else:
        signal.signal(signal.SIGINT, lambda sig, frame: engine.shutdown("SIGINT (Ctrl+C)"))
        signal.signal(signal.SIGTERM, lambda sig, frame: engine.shutdown("SIGTERM"))

    try:
        loop.run_until_complete(engine.start())
    except KeyboardInterrupt:
        engine.shutdown("KeyboardInterrupt")
    finally:
        loop.close()


if __name__ == "__main__":
    main()
