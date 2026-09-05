from __future__ import annotations

import unittest
from collections import namedtuple

from bridge import BridgeError, handle_request


class FakeMT5:
    ORDER_TYPE_BUY = 0
    TRADE_ACTION_DEAL = 1
    ORDER_TIME_GTC = 0
    ORDER_FILLING_FOK = 0

    def __init__(self) -> None:
        self.initialized = False

    def initialize(self) -> bool:
        self.initialized = True
        return True

    def last_error(self) -> tuple[int, str]:
        return (1, "Success")

    def account_info(self) -> object:
        Account = namedtuple("Account", "login server balance")
        return Account(123, "Demo", 1000.0)

    def orders_get(self) -> tuple[()]:
        return ()

    def positions_get(self) -> tuple[()]:
        return ()


class BridgeContractTests(unittest.TestCase):
    def test_health_does_not_initialize_mt5(self) -> None:
        mt5 = FakeMT5()
        result = handle_request(mt5, {"version": 1, "id": "1", "operation": "health", "params": {}})
        self.assertEqual(result, {"version": 1, "id": "1", "ok": True, "result": {"bridge": "ready"}})
        self.assertFalse(mt5.initialized)

    def test_account_info_serializes_namedtuple(self) -> None:
        result = handle_request(
            FakeMT5(), {"version": 1, "id": "2", "operation": "account_info", "params": {}}
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["result"], {"login": 123, "server": "Demo", "balance": 1000.0})

    def test_constants_returns_only_the_fixed_allowlist(self) -> None:
        result = handle_request(
            FakeMT5(), {"version": 1, "id": "constants", "operation": "constants", "params": {}}
        )
        self.assertEqual(
            result["result"],
            {
                "ORDER_FILLING_FOK": 0,
                "ORDER_TIME_GTC": 0,
                "ORDER_TYPE_BUY": 0,
                "TRADE_ACTION_DEAL": 1,
            },
        )

    def test_unknown_version_is_rejected(self) -> None:
        with self.assertRaisesRegex(BridgeError, "unsupported protocol version"):
            handle_request(FakeMT5(), {"version": 2, "id": "3", "operation": "health", "params": {}})

    def test_boolean_version_is_rejected(self) -> None:
        with self.assertRaisesRegex(BridgeError, "unsupported protocol version"):
            handle_request(FakeMT5(), {"version": True, "id": "3b", "operation": "health", "params": {}})

    def test_unknown_operation_is_rejected(self) -> None:
        with self.assertRaisesRegex(BridgeError, "unsupported operation"):
            handle_request(FakeMT5(), {"version": 1, "id": "4", "operation": "order_send", "params": {}})

    def test_unknown_request_field_is_rejected(self) -> None:
        with self.assertRaisesRegex(BridgeError, "unknown request fields"):
            handle_request(
                FakeMT5(),
                {"version": 1, "id": "5", "operation": "health", "params": {}, "extra": True},
            )

    def test_operation_rejects_unknown_parameters(self) -> None:
        with self.assertRaisesRegex(BridgeError, "unknown parameters"):
            handle_request(
                FakeMT5(),
                {"version": 1, "id": "6", "operation": "positions_get", "params": {"password": "no"}},
            )

    def test_order_check_rejects_unknown_nested_request_fields(self) -> None:
        with self.assertRaisesRegex(BridgeError, "unknown order_check request fields"):
            handle_request(
                FakeMT5(),
                {
                    "version": 1,
                    "id": "7",
                    "operation": "order_check",
                    "params": {"request": {"action": 1, "symbol": "EURUSD", "volume": 0.01, "type": 0, "price": 1.1, "type_time": 0, "type_filling": 0, "password": "no"}},
                },
            )

    def test_rates_reject_order_type_constant_as_timeframe(self) -> None:
        with self.assertRaisesRegex(BridgeError, "unsupported MT5 constant"):
            handle_request(
                FakeMT5(),
                {
                    "version": 1,
                    "id": "8",
                    "operation": "copy_rates_from_pos",
                    "params": {"symbol": "EURUSD", "timeframe": "ORDER_TYPE_BUY", "start_pos": 0, "count": 10},
                },
            )


if __name__ == "__main__":
    unittest.main()
