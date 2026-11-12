"""
Top-level entry point to run the Polymarket Continuous Multi-Slot Data Collector.
Usage:
    python run_collector.py --rounds 10 --window 5m --asset btc
"""

from src.continuous_collector import main

if __name__ == "__main__":
    main()
