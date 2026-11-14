"""Validate raw captures and build deterministic processed datasets."""

from __future__ import annotations

import argparse
import json

from src.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
from src.research_pipeline import process_directory


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process collected market data using the predefined data directories"
    )
    parser.parse_args()
    print(json.dumps(process_directory(RAW_DATA_DIR, PROCESSED_DATA_DIR), indent=2))


if __name__ == "__main__":
    main()
