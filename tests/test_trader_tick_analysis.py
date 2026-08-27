from __future__ import annotations

from datetime import UTC, datetime, timedelta
from queue import Queue
import unittest
from unittest.mock import Mock

from abt.trader.tick_analysis import (
    Tick,
    TickAnalysisError,
    analyze_trends,
    fetch_adaptive_ticks,
    find_cross_opportunities,
    select_active_pair,
    select_safe_pair_workers,
)
from scripts.analyze_active_pair_ticks import TraderJsonlClient


def tick(second: int, bid: float, ask: float) -> Tick:
    return Tick(datetime(2026, 8, 26, tzinfo=UTC) + timedelta(seconds=second), bid, ask)


class TraderTickAnalysisTests(unittest.TestCase):
    def test_rising_run_ignores_declines_at_or_below_tolerance(self) -> None:
        trends = analyze_trends(
            [tick(0, 100, 101), tick(1, 105, 106), tick(2, 104, 105), tick(3, 106, 107)],
            [1.0],
        )

        rise = trends["bid"]["1%"].rise

        self.assertEqual(100, rise.start_price)
        self.assertEqual(106, rise.end_price)
        self.assertEqual(6, rise.change)
        self.assertEqual(tick(3, 106, 107).observed_at, rise.ended_at)

    def test_falling_run_ignores_rises_at_or_below_tolerance(self) -> None:
        trends = analyze_trends(
            [tick(0, 105, 106), tick(1, 100, 101), tick(2, 101, 102), tick(3, 98, 99)],
            [1.0],
        )

        fall = trends["bid"]["1%"].fall

        self.assertEqual(105, fall.start_price)
        self.assertEqual(98, fall.end_price)
        self.assertEqual(7, fall.change)

    def test_finds_both_cross_directions_only_until_quote_staleness(self) -> None:
        opportunities = find_cross_opportunities(
            [tick(0, 101, 102), tick(3, 102, 103)],
            [tick(2, 103, 100)],
            quote_staleness=timedelta(seconds=4),
        )

        self.assertEqual(
            [
                ("first_bid_ge_second_ask", tick(2, 103, 100).observed_at, tick(2, 103, 100).observed_at + timedelta(seconds=4), 2),
                ("second_bid_ge_first_ask", tick(2, 103, 100).observed_at, tick(2, 103, 100).observed_at + timedelta(seconds=4), 1),
            ],
            [(item.direction, item.started_at, item.ended_at, item.maximum_edge) for item in opportunities],
        )

    def test_adaptive_fetch_splits_capped_responses_and_deduplicates_boundaries(self) -> None:
        start = datetime(2026, 8, 26, tzinfo=UTC)
        calls: list[tuple[datetime, datetime]] = []

        def fetch(slice_start: datetime, slice_end: datetime) -> list[dict[str, object]]:
            calls.append((slice_start, slice_end))
            if slice_end - slice_start > timedelta(seconds=1):
                return [{"time_msc": 0, "bid": 1.0, "ask": 1.1}] * 1_000
            return [
                {"time_msc": 0, "bid": 1.0, "ask": 1.1},
                {"time_msc": int((slice_end - start).total_seconds() * 1_000), "bid": 1.0, "ask": 1.1},
            ]

        records = fetch_adaptive_ticks(
            fetch, start=start, end=start + timedelta(seconds=4), minimum_slice=timedelta(seconds=1)
        )

        self.assertEqual(7, len(calls))
        self.assertEqual(5, len(records))

    def test_selects_only_safe_source_workers_for_pair_endpoints(self) -> None:
        pair = {
            "product_pair_id": "pair-1",
            "endpoints": [{"server": "Broker-A", "symbol": "EURUSD"}, {"server": "Broker-B", "symbol": "EURUSD.a"}],
            "source_workers": {
                "first_worker": {"worker_id": "worker-a", "server": "Broker-A"},
                "second_worker": {"worker_id": "worker-b", "server": "Broker-B"},
            },
        }
        workers = [
            {"worker_id": "worker-a", "server": "Broker-A", "connectivity": "connected"},
            {"worker_id": "worker-b", "server": "Broker-B", "connectivity": "connected"},
        ]

        self.assertEqual([("worker-a", "EURUSD"), ("worker-b", "EURUSD.a")], select_safe_pair_workers(pair, workers))
        self.assertEqual(pair, select_active_pair([pair]))
        with self.assertRaisesRegex(TickAnalysisError, "specify --pair-id"):
            select_active_pair([pair, {**pair, "product_pair_id": "pair-2"}])

    def test_uses_the_only_safe_worker_on_an_endpoint_server_when_source_is_unavailable(self) -> None:
        pair = {
            "product_pair_id": "pair-1",
            "endpoints": [{"server": "Broker-A", "symbol": "EURUSD"}, {"server": "Broker-B", "symbol": "EURUSD.a"}],
            "source_workers": {
                "first_worker": {"worker_id": "worker-a", "server": "Broker-A"},
                "second_worker": {"worker_id": "worker-b", "server": "Broker-B"},
            },
        }
        workers = [
            {"worker_id": "worker-a", "server": "Broker-A", "connectivity": "stale"},
            {"worker_id": "worker-a-replacement", "server": "Broker-A", "connectivity": "connected"},
            {"worker_id": "worker-b", "server": "Broker-B", "connectivity": "connected"},
        ]

        self.assertEqual(
            [("worker-a-replacement", "EURUSD"), ("worker-b", "EURUSD.a")],
            select_safe_pair_workers(pair, workers),
        )

    def test_waits_for_completed_rpc_after_recorded_status(self) -> None:
        client = TraderJsonlClient.__new__(TraderJsonlClient)
        client._process = Mock()
        client._process.poll.return_value = None
        client._process.stdin = Mock()
        client._timeout_seconds = 1
        client._responses = Queue()
        client._responses.put(
            '{"type":"trader_rpc_result","request_id":"request-1","status":"recorded","result":null}'
        )
        client._responses.put(
            '{"type":"trader_rpc_result","request_id":"request-1","status":"completed","result":{"ticks":[]}}'
        )

        response = client._request({}, "request-1", "trader_rpc_result")

        self.assertEqual("completed", response["status"])


if __name__ == "__main__":
    unittest.main()
