"""Event-ordered backtest that mirrors the reference late-entry simulator."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .collectors.oracle_client import OracleClient
from .engine.client import is_sim_filled
from .engine.strategies.late_entry import STOP_LOSS_PRICE, check_entry_signal
from .engine.types import BookSnapshot, OrderAction
from .research_pipeline import enrich_frame, validate_frame
from .utils.slot import slot_from_slug


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    shares: float = 6.0
    placement_delay_seconds: float = 0.150
    settlement_delay_seconds: float = 4.0


@dataclass(slots=True)
class _PendingOrder:
    side: str
    action: OrderAction
    price: float
    shares: float
    available_at: float
    matched_at: float | None = None


@dataclass(frozen=True, slots=True)
class BacktestResult:
    slug: str
    status: str
    side: str | None = None
    signal_time: str | None = None
    entry_limit: float | None = None
    entry_price: float | None = None
    entry_fill_time: str | None = None
    exit_price: float | None = None
    exit_fill_time: str | None = None
    winner: str | None = None
    shares_bought: float = 0.0
    shares_sold: float = 0.0
    payout: float = 0.0
    pnl: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _number(row: pd.Series, column: str) -> float | None:
    value = row.get(column)
    return float(value) if pd.notna(value) else None


def _book(row: pd.Series, side: str) -> BookSnapshot:
    prefix = f"polymarket_{side.lower()}"
    return BookSnapshot(
        best_ask=_number(row, f"{prefix}_best_ask"),
        best_ask_liquidity=_number(row, f"{prefix}_best_ask_liquidity"),
        best_bid=_number(row, f"{prefix}_best_bid"),
        best_bid_liquidity=_number(row, f"{prefix}_best_bid_liquidity"),
    )


def _winner(data: pd.DataFrame) -> str | None:
    if "market_completed" in data:
        completed = data["market_completed"].dropna()
        if completed.empty:
            return None
        last = completed.iloc[-1]
        is_complete = (
            last if isinstance(last, bool) else str(last).strip().lower() in {"true", "1", "yes"}
        )
        if not is_complete:
            return None
    if "market_winner" in data:
        values = data["market_winner"].dropna()
        if not values.empty and str(values.iloc[-1]).upper() in {"UP", "DOWN"}:
            return str(values.iloc[-1]).upper()
    if "chainlink_close_price" not in data:
        return None
    closes = pd.to_numeric(data["chainlink_close_price"], errors="coerce").dropna()
    opens = pd.to_numeric(data["chainlink_price_to_beat"], errors="coerce").dropna()
    if closes.empty or opens.empty:
        return None
    return "UP" if closes.iloc[-1] > opens.iloc[0] else "DOWN"


def run_late_entry(
    frame: pd.DataFrame,
    config: BacktestConfig = BacktestConfig(),
) -> BacktestResult:
    slug = "unknown"
    if not frame.empty and "market_slug" in frame:
        slug = str(frame["market_slug"].iloc[0])
    quality = validate_frame(frame)
    if not quality.valid:
        return BacktestResult(slug=slug, status="invalid: " + "; ".join(quality.errors))

    data = enrich_frame(frame)
    winner = _winner(data)
    entry: _PendingOrder | None = None
    stop: _PendingOrder | None = None
    has_entered = False
    position_side: str | None = None
    position_open = False
    shares_bought = 0.0
    shares_sold = 0.0
    signal_time: str | None = None
    entry_fill_time: str | None = None
    exit_fill_time: str | None = None
    exit_price: float | None = None
    entry_limit: float | None = None

    for _index, row in data.iterrows():
        timestamp = row["current_time"].timestamp()

        # Settlement callbacks become visible before the strategy evaluates
        # this tick, matching the user-channel callback ordering.
        if entry is not None and entry.matched_at is not None:
            if timestamp >= entry.matched_at + config.settlement_delay_seconds:
                shares_bought = entry.shares
                position_side = entry.side
                position_open = True
                entry_fill_time = row["current_time"].isoformat()
                entry = None
        if stop is not None and stop.matched_at is not None:
            if timestamp >= stop.matched_at + config.settlement_delay_seconds:
                shares_sold = stop.shares
                position_open = False
                exit_price = stop.price
                exit_fill_time = row["current_time"].isoformat()
                stop = None

        # Resting GTC orders can match only after their placement latency.
        if entry is not None and entry.matched_at is None:
            if timestamp >= entry.available_at and is_sim_filled(
                OrderAction.BUY, entry.price, entry.shares, _book(row, entry.side)
            ):
                entry.matched_at = timestamp
        if stop is not None and stop.matched_at is None:
            if timestamp >= stop.available_at and is_sim_filled(
                OrderAction.SELL, stop.price, stop.shares, _book(row, stop.side)
            ):
                stop.matched_at = timestamp

        remaining = float(row["remaining_seconds"])
        price = _number(row, "asset_current_price")
        open_price = _number(row, "chainlink_price_to_beat")
        gap = _number(row, "price_gap")

        if not has_entered and price is not None and open_price is not None:
            up_ask = _number(row, "polymarket_up_best_ask")
            down_ask = _number(row, "polymarket_down_best_ask")
            up_info = (
                {
                    "price": up_ask,
                    "liquidity": _number(row, "polymarket_up_best_ask_liquidity") or 0.0,
                }
                if up_ask is not None
                else None
            )
            down_info = (
                {
                    "price": down_ask,
                    "liquidity": _number(row, "polymarket_down_best_ask_liquidity") or 0.0,
                }
                if down_ask is not None
                else None
            )
            signal = check_entry_signal(
                remaining=remaining,
                btc_price=price,
                price_to_beat=open_price,
                up_info=up_info,
                down_info=down_info,
                atr=_number(row, "atr_asset"),
                gap_safety=_number(row, "gap_safety"),
                divergence=_number(row, "price_feed_divergence"),
                peak_gap_ratio=_number(row, "peak_gap_ratio"),
            )
            if signal is not None:
                has_entered = True
                signal_time = row["current_time"].isoformat()
                entry_limit = float(signal["ask"])
                entry = _PendingOrder(
                    side=signal["side"],
                    action=OrderAction.BUY,
                    price=entry_limit,
                    shares=config.shares,
                    available_at=timestamp + config.placement_delay_seconds,
                )

        if position_open and stop is None and position_side is not None:
            book = _book(row, position_side)
            gap_confirms = gap is not None and (
                (position_side == "UP" and gap > 5.0) or (position_side == "DOWN" and gap < -5.0)
            )
            rsi = _number(row, "rsi_gap")
            rsi_confirms = rsi is not None and (
                (position_side == "UP" and rsi >= 50.0) or (position_side == "DOWN" and rsi <= 50.0)
            )
            trigger = (
                book.best_ask is not None
                and book.best_ask <= STOP_LOSS_PRICE
                and (
                    (20.0 <= remaining <= 80.0 and not gap_confirms and not rsi_confirms)
                    or (remaining < 20.0 and not gap_confirms)
                )
            )
            if trigger:
                position_open = False
                stop_price = (
                    book.best_bid + 0.01 if book.best_bid is not None else STOP_LOSS_PRICE - 0.01
                )
                stop = _PendingOrder(
                    side=position_side,
                    action=OrderAction.SELL,
                    price=stop_price,
                    shares=shares_bought,
                    available_at=timestamp + config.placement_delay_seconds,
                )

    # A match before close still settles after close; unmatched orders do not.
    if entry is not None and entry.matched_at is not None:
        shares_bought = entry.shares
        position_side = entry.side
        entry_fill_time = pd.Timestamp(
            entry.matched_at + config.settlement_delay_seconds,
            unit="s",
            tz="UTC",
        ).isoformat()
    if stop is not None and stop.matched_at is not None:
        shares_sold = stop.shares
        exit_price = stop.price
        exit_fill_time = pd.Timestamp(
            stop.matched_at + config.settlement_delay_seconds,
            unit="s",
            tz="UTC",
        ).isoformat()

    if shares_bought == 0:
        return BacktestResult(
            slug=slug,
            status="no_fill" if has_entered else "no_signal",
            side=entry.side if entry is not None else position_side,
            signal_time=signal_time,
            entry_limit=entry_limit,
            winner=winner,
        )
    if winner is None:
        return BacktestResult(
            slug=slug,
            status="missing_resolution",
            side=position_side,
            signal_time=signal_time,
            entry_limit=entry_limit,
            entry_price=entry_limit,
            entry_fill_time=entry_fill_time,
            winner=None,
            shares_bought=shares_bought,
            shares_sold=shares_sold,
        )

    entry_price = entry_limit
    held = max(0.0, shares_bought - shares_sold)
    payout = held if position_side == winner else 0.0
    pnl = -(entry_price or 0.0) * shares_bought
    if exit_price is not None:
        pnl += exit_price * shares_sold
    pnl += payout
    return BacktestResult(
        slug=slug,
        status="completed",
        side=position_side,
        signal_time=signal_time,
        entry_limit=entry_limit,
        entry_price=entry_price,
        entry_fill_time=entry_fill_time,
        exit_price=exit_price,
        exit_fill_time=exit_fill_time,
        winner=winner,
        shares_bought=shares_bought,
        shares_sold=shares_sold,
        payout=payout,
        pnl=round(pnl, 4),
    )


def backfill_resolution(frame: pd.DataFrame, oracle: OracleClient) -> pd.DataFrame:
    """Fetch a missing authoritative open/close result without guessing."""
    if _winner(frame) is not None:
        return frame
    slug = str(frame["market_slug"].iloc[0])
    slot = slot_from_slug(slug)
    result = oracle.fetch_settlement_price(
        slot["start_time"], slot["end_time"], slot["asset"], slot["window"]
    )
    if not result or not result.get("completed"):
        return frame
    data = frame.copy()
    data["chainlink_price_to_beat"] = result["open_price"]
    data["chainlink_close_price"] = result["close_price"]
    data["market_winner"] = result["winner"]
    data["market_completed"] = True
    return data


def backtest_directory(
    data_dir: Path,
    *,
    backfill: bool = False,
    config: BacktestConfig = BacktestConfig(),
) -> pd.DataFrame:
    oracle = OracleClient() if backfill else None
    results: list[dict[str, Any]] = []
    try:
        for path in sorted(data_dir.glob("*.csv")):
            frame = pd.read_csv(path)
            if oracle is not None and validate_frame(frame).valid:
                frame = backfill_resolution(frame, oracle)
            results.append(run_late_entry(frame, config).to_dict())
    finally:
        if oracle is not None:
            oracle.session.close()
    return pd.DataFrame(results)
