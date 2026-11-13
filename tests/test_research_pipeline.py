from __future__ import annotations

import json

import pandas as pd
import pytest

from src.backtesting import run_late_entry
from src.engine.strategies.late_entry import check_entry_signal
from src.research_pipeline import best_level_liquidity, validate_frame


def _frame(window: str = "5m", duration: int = 300) -> pd.DataFrame:
    start = 1_800_000_000
    rows = []
    for offset in range(20):
        rows.append(
            {
                "current_time": pd.Timestamp(start + 210 + offset, unit="s", tz="UTC"),
                "market_slug": f"btc-updown-{window}-{start}",
                "slot_start": start,
                "slot_end": start + duration,
                "chainlink_price_to_beat": 100.0,
                "binance_current_price": 150.0 + (offset % 2),
                "polymarket_up_best_bid": 0.89,
                "polymarket_up_best_ask": 0.90,
                "polymarket_down_best_bid": 0.09,
                "polymarket_down_best_ask": 0.10,
                "up_top5_bids": json.dumps([[0.89, 100]]),
                "up_top5_asks": json.dumps([[0.90, 100]]),
                "down_top5_bids": json.dumps([[0.09, 100]]),
                "down_top5_asks": json.dumps([[0.10, 100]]),
            }
        )
    return pd.DataFrame(rows)


def test_malformed_fifteen_minute_file_is_rejected() -> None:
    quality = validate_frame(_frame(window="15m", duration=300))
    assert not quality.valid
    assert any("requires 900s" in error for error in quality.errors)


def test_best_level_fallback_uses_price_times_size() -> None:
    row = _frame().iloc[0]
    assert best_level_liquidity(row, "up", "ask") == 90.0


def test_missing_divergence_is_not_treated_as_zero() -> None:
    signal = check_entry_signal(
        remaining=60,
        btc_price=150,
        price_to_beat=100,
        up_info={"price": 0.9, "liquidity": 100},
        down_info={"price": 0.1, "liquidity": 100},
        atr=1,
        gap_safety=50,
        divergence=None,
        peak_gap_ratio=1,
    )
    assert signal is None


def test_old_binance_only_capture_fails_closed_in_backtest() -> None:
    result = run_late_entry(_frame())
    assert result.status == "no_signal"


def test_complete_capture_runs_signal_fill_settlement_and_payout() -> None:
    frame = _frame()
    frame["coinbase_current_price"] = frame["binance_current_price"] + 0.5
    frame["chainlink_close_price"] = 160.0
    frame["market_winner"] = "UP"
    frame["market_completed"] = True

    result = run_late_entry(frame)

    assert result.status == "completed"
    assert result.side == "UP"
    assert result.shares_bought == 6.0
    assert result.payout == 6.0
    assert result.pnl == pytest.approx(0.6)
