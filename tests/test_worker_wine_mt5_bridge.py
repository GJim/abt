from __future__ import annotations

import unittest
from datetime import datetime

from abt.worker.wine_mt5_bridge import BridgeError, handle_request


class FakeMT5:
    TIMEFRAME_M1 = 1
    ORDER_TYPE_BUY = 0
    TRADE_ACTION_DEAL = 1
    TRADE_RETCODE_DONE = 10009
    COPY_TICKS_ALL = 3

    def __init__(self) -> None:
        self.order_requests: list[dict[str, object]] = []
        self.login_args: tuple[int, str, str] | None = None

    def initialize(self) -> bool:
        return True

    def shutdown(self) -> None:
        pass

    def login(self, login: int, *, password: str, server: str) -> bool:
        self.login_args = (login, password, server)
        return True

    def last_error(self) -> tuple[int, str]:
        return (1, "Success")

    def account_info(self) -> dict[str, object]:
        return {"login": 123456, "server": "Broker-Demo"}

    def copy_rates_range(self, symbol: str, timeframe: int, start: datetime, end: datetime) -> list[dict[str, object]]:
        return [{"symbol": symbol, "timeframe": timeframe, "from": start, "to": end}]

    def copy_rates_from_pos(self, symbol: str, timeframe: int, start_pos: int, count: int) -> list[dict[str, object]]:
        return [{"symbol": symbol, "timeframe": timeframe, "start_pos": start_pos, "count": count}]

    def copy_ticks_range(self, symbol: str, start: datetime, end: datetime, flags: int) -> list[dict[str, object]]:
        return [{"symbol": symbol, "flags": flags, "from": start, "to": end}]

    def symbol_select(self, symbol: str, enable: bool) -> bool:
        return symbol == "EURUSD" and enable

    def order_check(self, request: dict[str, object]) -> dict[str, object]:
        return {"retcode": 0, "request": request}

    def order_send(self, request: dict[str, object]) -> dict[str, object]:
        self.order_requests.append(request)
        return {"retcode": self.TRADE_RETCODE_DONE}

    def history_deals_get(self, *args: datetime, **kwargs: int) -> list[dict[str, object]]:
        return [{"args": len(args), "position": kwargs.get("position")}]


class WineMT5BridgeContractTests(unittest.TestCase):
    def request(self, operation: str, params: dict[str, object] | None = None) -> dict[str, object]:
        return {"version": 1, "id": "request-1", "operation": operation, "params": params or {}}

    def test_login_is_an_explicit_operation(self) -> None:
        mt5 = FakeMT5()
        response = handle_request(
            mt5,
            self.request("login", {"login": 123456, "password": "memory-only", "server": "Broker-Demo"}),
        )
        self.assertEqual({"logged_in": True, "last_error": [1, "Success"]}, response["result"])
        self.assertEqual((123456, "memory-only", "Broker-Demo"), mt5.login_args)

    def test_date_ranges_are_parsed_before_calling_mt5(self) -> None:
        mt5 = FakeMT5()
        start = "2026-08-20T00:00:00+00:00"
        end = "2026-08-20T00:01:00+00:00"
        rates = handle_request(
            mt5,
            self.request("copy_rates_range", {"symbol": "EURUSD", "timeframe": 1, "from": start, "to": end}),
        )
        ticks = handle_request(
            mt5,
            self.request("copy_ticks_range", {"symbol": "EURUSD", "from": start, "to": end, "flags": 3}),
        )
        self.assertEqual(start, rates["result"][0]["from"])
        self.assertEqual(end, ticks["result"][0]["to"])

    def test_constants_returns_only_integer_mt5_constant_prefixes(self) -> None:
        result = handle_request(FakeMT5(), self.request("constants"))["result"]
        self.assertEqual(1, result["TIMEFRAME_M1"])
        self.assertEqual(10009, result["TRADE_RETCODE_DONE"])
        self.assertNotIn("login", result)

    def test_persistent_bridge_rejects_order_send(self) -> None:
        with self.assertRaisesRegex(BridgeError, "mutation operation requires one-shot mode"):
            handle_request(FakeMT5(), self.request("order_send", {"request": {"action": 1}}))

    def test_one_shot_mode_allows_exact_order_request(self) -> None:
        mt5 = FakeMT5()
        response = handle_request(
            mt5,
            self.request(
                "order_send",
                {
                    "request": {"action": 1, "symbol": "EURUSD", "volume": 0.01},
                    "expected_login": 123456,
                    "expected_server": "Broker-Demo",
                },
            ),
            allow_mutation=True,
        )
        self.assertEqual({"retcode": 10009}, response["result"])
        self.assertEqual([{"action": 1, "symbol": "EURUSD", "volume": 0.01}], mt5.order_requests)

    def test_order_requests_reject_unknown_fields(self) -> None:
        with self.assertRaisesRegex(BridgeError, "unknown order request fields"):
            handle_request(
                FakeMT5(),
                self.request(
                    "order_send",
                    {
                        "request": {"action": 1, "password": "forbidden"},
                        "expected_login": 123456,
                        "expected_server": "Broker-Demo",
                    },
                ),
                allow_mutation=True,
            )

    def test_history_deals_supports_closed_position_or_date_range_forms(self) -> None:
        mt5 = FakeMT5()
        by_position = handle_request(mt5, self.request("history_deals_get", {"position": 42}))["result"]
        by_range = handle_request(
            mt5,
            self.request(
                "history_deals_get",
                {"from": "2026-08-20T00:00:00+00:00", "to": "2026-08-20T01:00:00+00:00"},
            ),
        )["result"]
        self.assertEqual(42, by_position[0]["position"])
        self.assertEqual(2, by_range[0]["args"])

    def test_copy_rates_from_pos_resolves_named_constants_or_raw_int(self) -> None:
        mt5 = FakeMT5()
        by_name = handle_request(
            mt5,
            self.request("copy_rates_from_pos", {"symbol": "EURUSD", "timeframe": "TIMEFRAME_M1", "start_pos": 0, "count": 10}),
        )["result"]
        by_int = handle_request(
            mt5,
            self.request("copy_rates_from_pos", {"symbol": "EURUSD", "timeframe": 1, "start_pos": 0, "count": 10}),
        )["result"]
        self.assertEqual(1, by_name[0]["timeframe"])
        self.assertEqual(1, by_int[0]["timeframe"])

    def test_order_requests_reject_comment_and_magic(self) -> None:
        with self.assertRaisesRegex(BridgeError, "orders must not set comment or magic"):
            handle_request(
                FakeMT5(),
                self.request(
                    "order_send",
                    {
                        "request": {"action": 1, "symbol": "EURUSD", "comment": "bot"},
                        "expected_login": 123456,
                        "expected_server": "Broker-Demo",
                    },
                ),
                allow_mutation=True,
            )
        with self.assertRaisesRegex(BridgeError, "orders must not set comment or magic"):
            handle_request(
                FakeMT5(),
                self.request(
                    "order_send",
                    {
                        "request": {"action": 1, "symbol": "EURUSD", "magic": 42},
                        "expected_login": 123456,
                        "expected_server": "Broker-Demo",
                    },
                ),
                allow_mutation=True,
            )

    def test_symbol_select_requires_strict_boolean_enable(self) -> None:
        with self.assertRaisesRegex(BridgeError, "enable must be a boolean"):
            handle_request(FakeMT5(), self.request("symbol_select", {"symbol": "EURUSD", "enable": "yes"}))

    def test_login_rejects_non_positive_login(self) -> None:
        with self.assertRaisesRegex(BridgeError, "must be a positive integer"):
            handle_request(
                FakeMT5(),
                self.request("login", {"login": 0, "password": "p", "server": "s"}),
            )

    def test_unknown_operations_and_parameters_fail_closed(self) -> None:
        with self.assertRaisesRegex(BridgeError, "unsupported operation"):
            handle_request(FakeMT5(), self.request("terminal_path_delete"))
        with self.assertRaisesRegex(BridgeError, "unknown parameters"):
            handle_request(FakeMT5(), self.request("last_error", {"extra": True}))


if __name__ == "__main__":
    unittest.main()
