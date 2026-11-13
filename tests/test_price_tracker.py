from __future__ import annotations

import asyncio

import pytest

from src.collectors.price_tracker import (
    PriceTracker,
    _decode_json_object,
    _polymarket_value,
)


@pytest.mark.parametrize("raw", ["", "   ", "PING", "pong", b"PING", "not-json", "[]"])
def test_decode_json_object_ignores_non_data_frames(raw: str | bytes) -> None:
    assert _decode_json_object(raw) is None


def test_decode_json_object_accepts_text_and_binary_objects() -> None:
    expected = {"payload": {"value": 101.25}}

    assert _decode_json_object('{"payload":{"value":101.25}}') == expected
    assert _decode_json_object(b'{"payload":{"value":101.25}}') == expected


def test_polymarket_value_filters_symbols_locally() -> None:
    message = {"payload": {"symbol": "btc/usd", "value": 101.25}}

    assert _polymarket_value(message, "btc/usd") == 101.25
    assert _polymarket_value(message, "eth/usd") is None


def test_polymarket_value_accepts_latest_history_value() -> None:
    message = {
        "payload": {
            "symbol": "btc/usd",
            "data": [{"value": 100.0}, {"value": "101.25"}],
        }
    }

    assert _polymarket_value(message, "btc/usd") == 101.25


class _RecordingSocket:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send(self, message: str) -> None:
        self.messages.append(message)


@pytest.mark.asyncio
async def test_polymarket_heartbeat_sends_uppercase_ping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.collectors.price_tracker.POLYMARKET_HEARTBEAT_SECONDS", 0.01)
    socket = _RecordingSocket()
    stop_event = asyncio.Event()
    task = asyncio.create_task(PriceTracker._polymarket_heartbeat(socket, stop_event))

    await asyncio.sleep(0.025)
    stop_event.set()
    await task

    assert socket.messages
    assert set(socket.messages) == {"PING"}
