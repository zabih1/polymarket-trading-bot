"""
Mathematical slot timestamp and slug calculator for rolling Polymarket crypto markets.
"""

import math
import time
from typing import Any

from ..config import BASE_TIMESTAMP, INTERVAL_SECONDS


def get_slot_timestamps(offset: int = 0, window: str = "5m") -> dict[str, int]:
    """
    Calculates start and end timestamps (seconds) for a rolling market slot.
    offset: 0 = current active window, -1 = previous, +1 = next upcoming.
    """
    interval = INTERVAL_SECONDS.get(window, 300)
    now_sec = int(time.time())
    slot_start = (
        BASE_TIMESTAMP
        + math.floor((now_sec - BASE_TIMESTAMP) / interval) * interval
        + (offset * interval)
    )
    slot_end = slot_start + interval
    return {"start_time": slot_start, "end_time": slot_end}


def get_market_slug(asset: str = "btc", window: str = "5m", offset: int = 0) -> str:
    """
    Generates the unique market slug format e.g. btc-updown-5m-1793930400.
    """
    slot = get_slot_timestamps(offset=offset, window=window)
    return f"{asset.lower()}-updown-{window}-{slot['start_time']}"


def slot_from_slug(slug: str) -> dict[str, Any]:
    """
    Parses market metadata from a slug format (e.g. btc-updown-5m-1793930400).
    """
    parts = slug.split("-")
    asset = parts[0]
    window = parts[2]
    start_time = int(parts[3])
    interval = INTERVAL_SECONDS.get(window, 300)
    return {
        "asset": asset,
        "window": window,
        "start_time": start_time,
        "end_time": start_time + interval,
    }


def get_remaining_seconds(end_time: int) -> float:
    """
    Calculates remaining seconds until slot expiration.
    """
    return max(0.0, float(end_time - time.time()))
