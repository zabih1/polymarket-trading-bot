from __future__ import annotations

import asyncio

import pytest

from src.collectors.clob_client import (
    LocalOrderBook,
    _ClobHeartbeat,
    _decode_clob_frame,
)
from src.collectors.oracle_client import OracleClient


class _Response:
    status_code = 200

    @staticmethod
    def json():
        return {"openPrice": 100.0, "closePrice": 100.0, "completed": True}


class _Session:
    def __init__(self) -> None:
        self.params = None

    def get(self, _url, *, params, timeout):
        self.params = params
        return _Response()


def test_orderbook_liquidity_is_only_the_best_level_not_total_depth() -> None:
    book = LocalOrderBook("up")
    book.apply_snapshot(
        bids=[{"price": "0.50", "size": "10"}, {"price": "0.49", "size": "1000"}],
        asks=[{"price": "0.51", "size": "20"}, {"price": "0.52", "size": "1000"}],
    )
    depth = book.get_depth_summary()

    assert depth["best_bid_liquidity"] == 5.0
    assert depth["best_ask_liquidity"] == 10.2
    assert depth["total_bid_liquidity"] > depth["best_bid_liquidity"]


def test_oracle_uses_milliseconds_and_ties_resolve_down() -> None:
    client = OracleClient()
    session = _Session()
    client.session = session

    result = client.fetch_settlement_price(1_700_000_000, 1_700_000_300, "btc", "5m")

    assert session.params["eventStartTime"] == "1700000000000"
    assert session.params["endDate"] == "1700000300000"
    assert result["winner"] == "DOWN"


@pytest.mark.parametrize("raw", ["", " ", "not-json", b"\xff", "42", "[]"])
def test_clob_decoder_ignores_non_event_frames(raw: str | bytes) -> None:
    assert _decode_clob_frame(raw) == ([], None)


def test_clob_decoder_separates_control_and_data_frames() -> None:
    assert _decode_clob_frame("PONG") == ([], "PONG")
    assert _decode_clob_frame(b"ping") == ([], "PING")
    assert _decode_clob_frame('{"event_type":"book"}') == (
        [{"event_type": "book"}],
        None,
    )
    assert _decode_clob_frame('[{"event_type":"book"},null]') == (
        [{"event_type": "book"}],
        None,
    )


class _RecordingSocket:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send(self, message: str) -> None:
        self.messages.append(message)


@pytest.mark.asyncio
async def test_clob_heartbeat_sends_uppercase_ping() -> None:
    socket = _RecordingSocket()
    stop_event = asyncio.Event()
    heartbeat = _ClobHeartbeat(interval_seconds=0.01, stale_seconds=1.0)
    task = asyncio.create_task(heartbeat.run(socket, stop_event))

    await asyncio.sleep(0.025)
    stop_event.set()
    await task

    assert socket.messages
    assert set(socket.messages) == {"PING"}


@pytest.mark.asyncio
async def test_clob_heartbeat_rejects_stale_connection() -> None:
    socket = _RecordingSocket()
    heartbeat = _ClobHeartbeat(interval_seconds=0.01, stale_seconds=0.001)

    with pytest.raises(ConnectionError, match="timed out waiting for PONG"):
        await heartbeat.run(socket, asyncio.Event())
