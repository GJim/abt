from __future__ import annotations

import unittest
from collections import namedtuple

from mutation_roundtrip import MutationError, run_demo_roundtrip

Terminal = namedtuple("Terminal", "connected trade_allowed")
Account = namedtuple("Account", "trade_mode trade_allowed server")
Symbol = namedtuple("Symbol", "name visible select trade_mode volume_min point")
Tick = namedtuple("Tick", "ask bid")
Check = namedtuple("Check", "retcode comment")
Send = namedtuple("Send", "retcode comment order deal")
Position = namedtuple("Position", "ticket symbol volume type")


class FakeMT5:
    ACCOUNT_TRADE_MODE_DEMO = 0
    SYMBOL_TRADE_MODE_FULL = 4
    TRADE_ACTION_DEAL = 1
    ORDER_TYPE_BUY = 0
    ORDER_TYPE_SELL = 1
    ORDER_TIME_GTC = 0
    ORDER_FILLING_FOK = 0
    TRADE_RETCODE_DONE = 10009

    def __init__(self) -> None:
        self.terminal = Terminal(True, True)
        self.account = Account(0, True, "Demo")
        self.symbol = Symbol("EURUSD", True, True, 4, 0.01, 0.00001)
        self.tick = Tick(1.10002, 1.10000)
        self.positions: list[object] = []
        self.orders: list[object] = []
        self.send_requests: list[dict[str, object]] = []
        self.post_close_stale_reads = 0
        self.closed_position: object | None = None

    def initialize(self) -> bool:
        return True

    def shutdown(self) -> None:
        pass

    def last_error(self) -> tuple[int, str]:
        return (1, "Success")

    def terminal_info(self) -> object:
        return self.terminal

    def account_info(self) -> object:
        return self.account

    def orders_get(self) -> tuple[object, ...]:
        return tuple(self.orders)

    def positions_get(self) -> tuple[object, ...]:
        if not self.positions and self.post_close_stale_reads > 0 and self.closed_position is not None:
            self.post_close_stale_reads -= 1
            return (self.closed_position,)
        return tuple(self.positions)

    def symbol_info(self, symbol: str) -> object:
        return self.symbol if symbol == self.symbol.name else None

    def symbol_info_tick(self, symbol: str) -> object:
        return self.tick if symbol == self.symbol.name else None

    def order_check(self, request: dict[str, object]) -> object:
        return Check(0, "Done")

    def order_send(self, request: dict[str, object]) -> object:
        self.send_requests.append(request)
        if request["type"] == self.ORDER_TYPE_BUY:
            self.positions = [Position(42, "EURUSD", 0.01, self.ORDER_TYPE_BUY)]
            return Send(self.TRADE_RETCODE_DONE, "Done", 42, 100)
        self.closed_position = self.positions[0]
        self.positions = []
        return Send(self.TRADE_RETCODE_DONE, "Done", 42, 101)


class MutationRoundtripTests(unittest.TestCase):
    def test_confirmation_is_required_before_initialization(self) -> None:
        mt5 = FakeMT5()
        with self.assertRaisesRegex(MutationError, "explicit confirmation"):
            run_demo_roundtrip(
                mt5,
                "EURUSD",
                confirmation_phrase="WRONG",
                authorization_reference="telegram:test-message",
            )
        self.assertEqual(mt5.send_requests, [])

    def test_authorization_reference_is_required_before_initialization(self) -> None:
        mt5 = FakeMT5()
        with self.assertRaisesRegex(MutationError, "authorization reference"):
            run_demo_roundtrip(
                mt5,
                "EURUSD",
                confirmation_phrase="DEMO_OPEN_CLOSE",
                authorization_reference="",
            )
        self.assertEqual(mt5.send_requests, [])

    def test_non_demo_account_is_rejected(self) -> None:
        mt5 = FakeMT5()
        mt5.account = Account(2, True, "Live")
        with self.assertRaisesRegex(MutationError, "demo account"):
            run_demo_roundtrip(
                mt5,
                "EURUSD",
                confirmation_phrase="DEMO_OPEN_CLOSE",
                authorization_reference="telegram:test-message",
            )
        self.assertEqual(mt5.send_requests, [])

    def test_existing_exposure_is_rejected(self) -> None:
        mt5 = FakeMT5()
        mt5.positions = [Position(7, "EURUSD", 0.01, mt5.ORDER_TYPE_BUY)]
        with self.assertRaisesRegex(MutationError, "zero starting"):
            run_demo_roundtrip(
                mt5,
                "EURUSD",
                confirmation_phrase="DEMO_OPEN_CLOSE",
                authorization_reference="telegram:test-message",
            )
        self.assertEqual(mt5.send_requests, [])

    def test_terminal_trading_must_be_enabled(self) -> None:
        mt5 = FakeMT5()
        mt5.terminal = Terminal(True, False)
        with self.assertRaisesRegex(MutationError, "terminal trading"):
            run_demo_roundtrip(
                mt5,
                "EURUSD",
                confirmation_phrase="DEMO_OPEN_CLOSE",
                authorization_reference="telegram:test-message",
            )
        self.assertEqual(mt5.send_requests, [])

    def test_state_change_after_order_check_is_rejected_before_send(self) -> None:
        mt5 = FakeMT5()

        def change_state(_: dict[str, object]) -> object:
            mt5.positions = [Position(99, "EURUSD", 0.01, mt5.ORDER_TYPE_BUY)]
            return Check(0, "Done")

        mt5.order_check = change_state  # type: ignore[method-assign]
        with self.assertRaisesRegex(MutationError, "changed after order_check"):
            run_demo_roundtrip(
                mt5,
                "EURUSD",
                confirmation_phrase="DEMO_OPEN_CLOSE",
                authorization_reference="telegram:test-message",
            )
        self.assertEqual(mt5.send_requests, [])

    def test_opens_minimum_volume_then_closes_observed_ticket(self) -> None:
        mt5 = FakeMT5()
        result = run_demo_roundtrip(
                mt5,
                "EURUSD",
                confirmation_phrase="DEMO_OPEN_CLOSE",
                authorization_reference="telegram:test-message",
            )

        self.assertEqual(result["verdict"], "demo_roundtrip_validated")
        self.assertEqual(result["authorization_reference"], "telegram:test-message")
        self.assertTrue(result["safety_evidence"]["confirmation_validated"])
        self.assertTrue(result["safety_evidence"]["demo_account"])
        self.assertTrue(result["safety_evidence"]["order_check_passed"])
        self.assertEqual(result["safety_evidence"]["initial_order_count"], 0)
        self.assertEqual(result["safety_evidence"]["initial_position_count"], 0)
        self.assertEqual(len(mt5.send_requests), 2)
        self.assertEqual(mt5.send_requests[0]["volume"], 0.01)
        self.assertNotIn("position", mt5.send_requests[0])
        self.assertEqual(mt5.send_requests[1]["position"], 42)
        self.assertEqual(mt5.send_requests[1]["type"], mt5.ORDER_TYPE_SELL)
        self.assertEqual(mt5.orders_get(), ())
        self.assertEqual(mt5.positions_get(), ())

    def test_successful_close_waits_for_read_model_convergence_without_resend(self) -> None:
        mt5 = FakeMT5()
        mt5.post_close_stale_reads = 1
        result = run_demo_roundtrip(
            mt5,
            "EURUSD",
            confirmation_phrase="DEMO_OPEN_CLOSE",
            authorization_reference="telegram:test-message",
        )
        self.assertEqual(result["verdict"], "demo_roundtrip_validated")
        self.assertEqual(len(mt5.send_requests), 2)
        self.assertEqual(mt5.positions_get(), ())

    def test_unknown_open_result_is_never_retried(self) -> None:
        mt5 = FakeMT5()

        def unknown(_: dict[str, object]) -> None:
            mt5.send_requests.append({"attempt": "open"})
            return None

        mt5.order_send = unknown  # type: ignore[method-assign]
        with self.assertRaisesRegex(MutationError, "unknown open outcome"):
            run_demo_roundtrip(
                mt5,
                "EURUSD",
                confirmation_phrase="DEMO_OPEN_CLOSE",
                authorization_reference="telegram:test-message",
            )
        self.assertEqual(len(mt5.send_requests), 1)


if __name__ == "__main__":
    unittest.main()
