"""Validate raw captures and build deterministic processed datasets."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
from src.research_pipeline import process_directory


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", type=Path, default=RAW_DATA_DIR)
    parser.add_argument("--output-dir", type=Path, default=PROCESSED_DATA_DIR)
    args = parser.parse_args()
    print(json.dumps(process_directory(args.raw_dir, args.output_dir), indent=2))


if __name__ == "__main__":
    main()
