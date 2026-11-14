from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from typing import Any

import pytest

from src.continuous_collector import ContinuousCollector


@pytest.mark.asyncio
async def test_collect_slot_records_rows_while_feeds_are_unavailable(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    collector = ContinuousCollector(rounds=1)
    collector.output_dir = tmp_path

    async def disconnected_books(*_args: Any, stop_event: asyncio.Event, **_kwargs: Any) -> None:
        await stop_event.wait()

    async def open_price(_slot_start: int, _slot_end: int) -> float:
        return 100.0

    monkeypatch.setattr(collector.clob, "stream_orderbooks", disconnected_books)
    monkeypatch.setattr(collector, "_fetch_open_price", open_price)
    now = time.time()

    try:
        records = await collector._collect_slot(
            "btc-updown-5m-test",
            int(now),
            now + 0.03,
            {"condition_id": "condition", "up_token_id": "up", "down_token_id": "down"},
        )
    finally:
        collector.clob.session.close()
        collector.gamma.session.close()
        collector.oracle.session.close()

    assert records
    assert records[0]["order_books_ready"] is False
    assert records[0]["price_feeds_ready"] is False
    assert records[0]["chainlink_price_to_beat"] == 100.0
    date_partition = datetime.fromtimestamp(int(now), UTC).strftime("%Y%m%d")
    assert (tmp_path / date_partition / "btc-updown-5m-test.csv").exists()


class _ResolutionOracle:
    def __init__(self) -> None:
        self.calls = 0

    async def fetch_settlement_price_async(self, *_args: Any) -> dict[str, Any] | None:
        self.calls += 1
        if self.calls < 3:
            return None
        return {
            "completed": True,
            "open_price": 100.0,
            "close_price": 101.0,
            "winner": "UP",
        }


@pytest.mark.asyncio
async def test_resolution_polling_uses_bounded_retry(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.continuous_collector.RESOLUTION_INITIAL_DELAY_SECONDS", 0.0)
    monkeypatch.setattr("src.continuous_collector.ORACLE_MIN_REQUEST_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr("src.continuous_collector.RESOLUTION_RETRY_INITIAL_SECONDS", 0.001)
    monkeypatch.setattr("src.continuous_collector.RESOLUTION_RETRY_MAX_SECONDS", 0.001)
    monkeypatch.setattr("src.continuous_collector.RESOLUTION_MAX_WAIT_SECONDS", 0.1)
    collector = ContinuousCollector(rounds=1)
    collector.output_dir = tmp_path
    oracle = _ResolutionOracle()

    try:
        result = await collector._fetch_resolution(100, 400, oracle)  # type: ignore[arg-type]
    finally:
        collector.clob.session.close()
        collector.gamma.session.close()
        collector.oracle.session.close()

    assert result is not None
    assert result["winner"] == "UP"
    assert oracle.calls == 3


@pytest.mark.asyncio
async def test_next_slot_does_not_wait_for_previous_resolution(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    collector = ContinuousCollector(rounds=2)
    collector.output_dir = tmp_path
    events: list[str] = []
    release_resolutions = asyncio.Event()

    async def price_stream(stop_event: asyncio.Event) -> None:
        await stop_event.wait()

    async def discover(_slug: str, _deadline: int) -> dict[str, str]:
        return {"up_token_id": "up", "down_token_id": "down"}

    async def collect(
        slug: str, _slot_start: int, _slot_end: int, _metadata: dict[str, Any]
    ) -> list[dict[str, Any]]:
        events.append(f"collect:{slug}")
        await asyncio.sleep(0)
        if len([event for event in events if event.startswith("collect:")]) == 2:
            release_resolutions.set()
        return [{"market_slug": slug}]

    async def resolve(
        slug: str,
        _slot_start: int,
        _slot_end: int,
        _records: list[dict[str, Any]],
    ) -> None:
        events.append(f"resolve-start:{slug}")
        await release_resolutions.wait()
        events.append(f"resolve-done:{slug}")

    monkeypatch.setattr(collector.price_tracker, "stream", price_stream)
    monkeypatch.setattr(collector, "_discover_market", discover)
    monkeypatch.setattr(collector, "_collect_slot", collect)
    monkeypatch.setattr(collector, "_resolve_and_save", resolve)

    await asyncio.wait_for(collector.run(), timeout=1.0)

    collect_indexes = [index for index, event in enumerate(events) if event.startswith("collect:")]
    first_resolution_done = next(
        index for index, event in enumerate(events) if event.startswith("resolve-done:")
    )
    assert len(collect_indexes) == 2
    assert collect_indexes[1] < first_resolution_done
