"""Deterministic validation and feature engineering for captured market data."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .config import INTERVAL_SECONDS
from .engine.indicators import ATR, RSI, RTV
from .utils.slot import slot_from_slug


@dataclass(frozen=True, slots=True)
class DataQuality:
    valid: bool
    errors: tuple[str, ...]
    warnings: tuple[str, ...]


REQUIRED_COLUMNS = frozenset(
    {
        "current_time",
        "market_slug",
        "slot_start",
        "slot_end",
        "chainlink_price_to_beat",
        "polymarket_up_best_bid",
        "polymarket_up_best_ask",
        "polymarket_down_best_bid",
        "polymarket_down_best_ask",
        "up_top5_bids",
        "up_top5_asks",
        "down_top5_bids",
        "down_top5_asks",
    }
)


def validate_frame(frame: pd.DataFrame) -> DataQuality:
    errors: list[str] = []
    warnings: list[str] = []
    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        errors.append(f"missing columns: {', '.join(missing)}")
        return DataQuality(False, tuple(errors), tuple(warnings))
    if frame.empty:
        errors.append("file has no rows")
        return DataQuality(False, tuple(errors), tuple(warnings))

    slug = str(frame["market_slug"].iloc[0]) if "market_slug" in frame else ""
    try:
        slot = slot_from_slug(slug)
    except (IndexError, ValueError):
        errors.append(f"invalid market slug: {slug!r}")
        return DataQuality(False, tuple(errors), tuple(warnings))

    starts = pd.to_numeric(frame.get("slot_start"), errors="coerce").dropna()
    ends = pd.to_numeric(frame.get("slot_end"), errors="coerce").dropna()
    if starts.empty or ends.empty:
        errors.append("slot_start/slot_end are not numeric")
    else:
        start = int(starts.iloc[0])
        end = int(ends.iloc[0])
        expected = INTERVAL_SECONDS[slot["window"]]
        if start != slot["start_time"]:
            errors.append("slot_start does not match slug")
        if end - start != expected:
            errors.append(f"duration is {end - start}s but {slot['window']} requires {expected}s")

    timestamps = pd.to_datetime(frame.get("current_time"), utc=True, errors="coerce")
    if timestamps.isna().any():
        errors.append("one or more timestamps are invalid")
    elif not timestamps.is_monotonic_increasing:
        warnings.append("timestamps were not ordered")
    if frame["market_slug"].astype(str).nunique() != 1:
        errors.append("file contains more than one market_slug")

    for side in ("up", "down"):
        missing_ask = (
            pd.to_numeric(frame.get(f"polymarket_{side}_best_ask"), errors="coerce").isna().sum()
        )
        if missing_ask:
            warnings.append(f"{missing_ask} rows lack {side.upper()} best ask")
    if not {"coinbase_current_price", "price_feed_divergence"} & set(frame.columns):
        warnings.append("Coinbase/divergence data is absent; late-entry will fail closed")
    return DataQuality(not errors, tuple(errors), tuple(warnings))


def _levels(value: Any) -> list[tuple[float, float]]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return []
    try:
        raw = json.loads(value) if isinstance(value, str) else value
        return [(float(price), float(size)) for price, size in raw]
    except (TypeError, ValueError, json.JSONDecodeError):
        return []


def best_level_liquidity(row: pd.Series, side: str, book_side: str) -> float | None:
    """Return USDC at one best price level, never aggregate depth."""
    explicit = f"polymarket_{side}_best_{book_side}_liquidity"
    if explicit in row and pd.notna(row[explicit]):
        return float(row[explicit])
    levels = _levels(row.get(f"{side}_top5_{book_side}s"))
    if not levels:
        return None
    price, size = (
        min(levels, key=lambda item: item[0])
        if book_side == "ask"
        else max(levels, key=lambda item: item[0])
    )
    return price * size


def selected_asset_price(row: pd.Series) -> float | None:
    for column in (
        "asset_current_price",
        "polymarket_current_price",
        "binance_current_price",
    ):
        value = row.get(column)
        if pd.notna(value):
            return float(value)
    return None


def feed_divergence(row: pd.Series) -> float | None:
    value = row.get("price_feed_divergence")
    if pd.notna(value):
        return float(value)
    binance = row.get("binance_current_price")
    coinbase = row.get("coinbase_current_price")
    if pd.notna(binance) and pd.notna(coinbase):
        return abs(float(binance) - float(coinbase))
    return None


def enrich_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Compute the same one-second streaming features used by the engine."""
    quality = validate_frame(frame)
    if not quality.valid:
        raise ValueError("; ".join(quality.errors))

    data = frame.copy()
    data["current_time"] = pd.to_datetime(data["current_time"], utc=True)
    data = data.sort_values("current_time", kind="stable").reset_index(drop=True)
    start = float(data["slot_start"].iloc[0])
    end = float(data["slot_end"].iloc[0])

    rsi = RSI(14)
    atr = ATR(14)
    rtv = RTV(30)
    peak_abs_gap = 0.0
    last_second: int | None = None
    rows: list[dict[str, float | None]] = []

    for _, row in data.iterrows():
        timestamp = row["current_time"].timestamp()
        asset_price = selected_asset_price(row)
        open_price = row.get("chainlink_price_to_beat")
        gap = (
            asset_price - float(open_price)
            if asset_price is not None and pd.notna(open_price)
            else None
        )
        second = int(timestamp)
        if second != last_second:
            last_second = second
            if gap is not None:
                rsi.update(gap)
                if atr.value is not None:
                    peak_abs_gap = max(peak_abs_gap, abs(gap))
            if asset_price is not None:
                atr.update(asset_price)
                rtv.update(asset_price)

        rows.append(
            {
                "elapsed_seconds": timestamp - start,
                "remaining_seconds": end - timestamp,
                "asset_current_price": asset_price,
                "price_gap": gap,
                "price_gap_abs": abs(gap) if gap is not None else None,
                "rsi_gap": rsi.value,
                "atr_asset": atr.value,
                "rtv_asset": rtv.value,
                "gap_safety": atr.gap_safety(gap) if gap is not None else None,
                "peak_gap_abs": peak_abs_gap or None,
                "peak_gap_ratio": (
                    abs(gap) / peak_abs_gap if gap is not None and peak_abs_gap > 0 else None
                ),
                "price_feed_divergence": feed_divergence(row),
                "polymarket_up_best_bid_liquidity": best_level_liquidity(row, "up", "bid"),
                "polymarket_up_best_ask_liquidity": best_level_liquidity(row, "up", "ask"),
                "polymarket_down_best_bid_liquidity": best_level_liquidity(row, "down", "bid"),
                "polymarket_down_best_ask_liquidity": best_level_liquidity(row, "down", "ask"),
            }
        )

    features = pd.DataFrame(rows, index=data.index)
    for column in features:
        data[column] = features[column]
    return data


def process_directory(raw_dir: Path, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {"processed": [], "skipped": {}}
    for source in sorted(raw_dir.glob("*.csv")):
        frame = pd.read_csv(source)
        quality = validate_frame(frame)
        if not quality.valid:
            report["skipped"][source.name] = list(quality.errors)
            continue
        enrich_frame(frame).to_csv(output_dir / source.name, index=False)
        report["processed"].append(source.name)
    return report
