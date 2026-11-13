from __future__ import annotations

from src.collectors.clob_client import LocalOrderBook
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
