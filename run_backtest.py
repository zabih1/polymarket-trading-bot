"""Run the parity backtest over captured CSV files."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.backtesting import backtest_directory
from src.config import RAW_DATA_DIR


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=RAW_DATA_DIR)
    parser.add_argument(
        "--backfill-resolutions",
        action="store_true",
        help="Fetch authoritative historical open/close prices when absent",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    results = backtest_directory(args.data_dir, backfill=args.backfill_resolutions)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        results.to_csv(args.output, index=False)
    print(results.to_string(index=False))
    if not results.empty and "pnl" in results:
        pnl = results["pnl"].dropna()
        print(f"\nCompleted: {len(pnl)} | Total PnL: ${pnl.sum():+.2f}")


if __name__ == "__main__":
    main()
