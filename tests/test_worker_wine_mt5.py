from __future__ import annotations

import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from abt.worker.wine_mt5 import WineMetaTrader5Adapter
from abt.worker.wine_mt5_client import (
    BridgeClientError,
    BridgeRemoteError,
    MutationOutcomeUnknown,
    OneShotMutationClient,
)


class FakeBridgeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.closed = False
        self.results: dict[str, object] = {
            "constants": {"TIMEFRAME_M1": 1, "ORDER_TYPE_BUY": 0},
            "initialize": {"initialized": True, "last_error": [1, "Success"]},
            "login": {"logged_in": True, "last_error": [1, "Success"]},
            "account_info": {"login": 123456, "server": "Broker-Demo"},
            "terminal_info": {"connected": True},
            "orders_get": [],
            "positions_get": [],
            "symbols_get": [],
            "symbol_info": {"name": "EURUSD"},
            "symbol_info_tick": {"bid": 1.1, "ask": 1.2},
            "symbol_select": {"selected": True},
            "copy_rates_range": [{"time": 1}],
            "copy_rates_from_pos": [{"time": 1}],
            "copy_ticks_range": [{"time": 1}],
            "order_calc_margin": 10.0,
            "order_calc_profit": 2.0,
            "order_check": {"retcode": 0},
            "order_send": {"retcode": 10009},
            "history_deals_get": [],
            "last_error": [1, "Success"],
        }

    def request(self, operation: str, params: dict[str, object] | None = None) -> object:
        self.calls.append((operation, {} if params is None else params))
        return self.results[operation]

    def close(self) -> None:
        self.closed = True


class FakeOneShotMutationClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def order_send(
        self,
        request: dict[str, object],
        *,
        expected_login: int,
        expected_server: str,
    ) -> object:
        self.calls.append(
            {
                "request": request,
                "expected_login": expected_login,
                "expected_server": expected_server,
            }
        )
        return {"retcode": 10009}


class WineMetaTrader5AdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bridge = FakeBridgeClient()
        self.mutation = FakeOneShotMutationClient()
        self.mt5 = WineMetaTrader5Adapter(
            client=self.bridge,
            mutation_client_factory=lambda: self.mutation,
        )

    def test_initialization_login_and_constants_cross_the_bridge(self) -> None:
        self.assertTrue(self.mt5.initialize())
        self.assertTrue(self.mt5.login(123456, password="memory-only", server="Broker-Demo"))
        self.assertEqual(1, self.mt5.TIMEFRAME_M1)
        self.assertEqual(0, self.mt5.ORDER_TYPE_BUY)
        self.assertEqual(
            ("login", {"login": 123456, "password": "memory-only", "server": "Broker-Demo"}),
            self.bridge.calls[1],
        )
        self.assertEqual(1, [call[0] for call in self.bridge.calls].count("constants"))

    def test_worker_mt5_surface_maps_to_closed_bridge_operations(self) -> None:
        start = datetime(2026, 8, 20, tzinfo=UTC)
        end = datetime(2026, 8, 20, 0, 1, tzinfo=UTC)

        self.assertEqual({"login": 123456, "server": "Broker-Demo"}, self.mt5.account_info())
        self.assertEqual({"connected": True}, self.mt5.terminal_info())
        self.assertEqual([], self.mt5.orders_get())
        self.assertEqual([], self.mt5.positions_get())
        self.assertEqual([], self.mt5.symbols_get())
        self.assertEqual({"name": "EURUSD"}, self.mt5.symbol_info("EURUSD"))
        self.assertEqual({"bid": 1.1, "ask": 1.2}, self.mt5.symbol_info_tick("EURUSD"))
        self.assertTrue(self.mt5.symbol_select("EURUSD", True))
        self.assertEqual([{"time": 1}], self.mt5.copy_rates_range("EURUSD", 1, start, end))
        self.assertEqual([{"time": 1}], self.mt5.copy_rates_from_pos("EURUSD", 1, 0, 10))
        self.assertEqual([{"time": 1}], self.mt5.copy_ticks_range("EURUSD", start, end, 3))
        self.assertEqual(10.0, self.mt5.order_calc_margin(0, "EURUSD", 0.01, 1.2))
        self.assertEqual(2.0, self.mt5.order_calc_profit(0, "EURUSD", 0.01, 1.2, 1.3))
        self.assertEqual({"retcode": 0}, self.mt5.order_check({"action": 1}))
        self.assertTrue(self.mt5.login(123456, password="memory-only", server="Broker-Demo"))
        self.assertEqual({"retcode": 10009}, self.mt5.order_send({"action": 1}))
        self.assertEqual(
            [
                {
                    "request": {"action": 1},
                    "expected_login": 123456,
                    "expected_server": "Broker-Demo",
                }
            ],
            self.mutation.calls,
        )
        self.assertNotIn("order_send", [operation for operation, _ in self.bridge.calls])
        self.assertEqual([], self.mt5.history_deals_get(start, end))
        self.assertEqual([1, "Success"], self.mt5.last_error())

        calls = dict(self.bridge.calls)
        self.assertEqual(
            {"symbol": "EURUSD", "timeframe": 1, "from": start.isoformat(), "to": end.isoformat()},
            calls["copy_rates_range"],
        )
        self.assertEqual(
            {"symbol": "EURUSD", "timeframe": 1, "start_pos": 0, "count": 10},
            calls["copy_rates_from_pos"],
        )
        self.assertEqual(
            {"symbol": "EURUSD", "from": start.isoformat(), "to": end.isoformat(), "flags": 3},
            calls["copy_ticks_range"],
        )

    def test_shutdown_closes_the_owned_bridge(self) -> None:
        self.mt5.shutdown()
        self.assertTrue(self.bridge.closed)
        self.mt5.shutdown()
        self.assertEqual([], [call for call in self.bridge.calls if call[0] == "shutdown"])

    def test_unknown_constants_fail_closed(self) -> None:
        with self.assertRaises(AttributeError):
            _ = self.mt5.NOT_AN_MT5_CONSTANT


class OneShotMutationClientTests(unittest.TestCase):
    def test_transport_failure_after_send_is_reported_as_unknown_and_never_retried(self) -> None:
        class FailedClient:
            def __init__(self) -> None:
                self.calls = 0
                self.closed = False

            def request(self, operation: str, params: dict[str, object]) -> object:
                self.calls += 1
                raise BridgeClientError("unexpected EOF")

            def close(self) -> None:
                self.closed = True

        failed = FailedClient()
        with patch("abt.worker.wine_mt5_client.BridgeClient", return_value=failed):
            client = OneShotMutationClient(
                wine_prefix=Path("/tmp/wine-prefix"),
                windows_python=r"C:\\Python313\\python.exe",
            )
            with self.assertRaisesRegex(MutationOutcomeUnknown, "do not retry"):
                client.order_send(
                    {"action": 1},
                    expected_login=123456,
                    expected_server="Broker-Demo",
                )

        self.assertEqual(1, failed.calls)
        self.assertTrue(failed.closed)

    def test_structured_bridge_rejection_is_not_mislabeled_as_unknown(self) -> None:
        class RejectedClient:
            def request(self, operation: str, params: dict[str, object]) -> object:
                raise BridgeRemoteError("account mismatch")

            def close(self) -> None:
                pass

        with patch("abt.worker.wine_mt5_client.BridgeClient", return_value=RejectedClient()):
            client = OneShotMutationClient(
                wine_prefix=Path("/tmp/wine-prefix"),
                windows_python=r"C:\\Python313\\python.exe",
            )
            with self.assertRaisesRegex(BridgeRemoteError, "account mismatch"):
                client.order_send(
                    {"action": 1},
                    expected_login=123456,
                    expected_server="Broker-Demo",
                )
