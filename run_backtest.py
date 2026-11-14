"""Run the parity backtest over captured CSV files."""

from __future__ import annotations

import argparse

from src.backtesting import backtest_directory
from src.config import BACKTEST_RESULTS_PATH, RAW_DATA_DIR


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--backfill-resolutions",
        action="store_true",
        help="Fetch authoritative historical open/close prices when absent",
    )
    args = parser.parse_args()
    results = backtest_directory(RAW_DATA_DIR, backfill=args.backfill_resolutions)
    BACKTEST_RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    results.to_csv(BACKTEST_RESULTS_PATH, index=False)
    print(f"Backtest results saved to: {BACKTEST_RESULTS_PATH.resolve()}")
    print(results.to_string(index=False))
    if not results.empty and "pnl" in results:
        pnl = results["pnl"].dropna()
        print(f"\nCompleted: {len(pnl)} | Total PnL: ${pnl.sum():+.2f}")


if __name__ == "__main__":
    main()
