from __future__ import annotations

import unittest

from pydantic import ValidationError

from abt.trader_protocol import MAX_LIVE_SYMBOLS, trader_rpc_request_adapter, trader_rpc_response_adapter


class TraderProtocolTests(unittest.TestCase):
    def test_accepts_a_typed_market_operation(self) -> None:
        request = trader_rpc_request_adapter.validate_python(
            {
                "type": "trader_rpc_request",
                "request_id": "request-1",
                "kind": "operation",
                "payload": {
                    "type": "market",
                    "symbol": "EURUSD",
                    "volume": "0.01",
                    "direction": "LONG",
                    "filling_mode": "FOK",
                },
            }
        )

        self.assertEqual("LONG", request.payload.direction)  # type: ignore[union-attr]

    def test_accepts_an_exact_set_live_symbols_operation(self) -> None:
        request = trader_rpc_request_adapter.validate_python(
            {
                "type": "trader_rpc_request",
                "request_id": "request-1",
                "kind": "operation",
                "payload": {"type": "set_live_symbols", "symbols": ["EURUSD", "GBPUSD"]},
            }
        )

        self.assertEqual(
            {"type": "set_live_symbols", "symbols": ["EURUSD", "GBPUSD"]},
            request.payload.model_dump(),  # type: ignore[union-attr]
        )

    def test_rejects_invalid_set_live_symbols_operation(self) -> None:
        for kind, payload in (
            ("operation", {"type": "set_live_symbols", "symbols": ["EURUSD", "EURUSD"]}),
            ("operation", {"type": "set_live_symbols", "symbols": ["EURUSD"], "extra": True}),
            ("operation", {"type": "set_live_symbols", "symbols": []}),
            ("operation", {"type": "set_live_symbols", "symbols": ["EURUSD"] * (MAX_LIVE_SYMBOLS + 1)}),
            ("read", {"type": "set_live_symbols", "symbols": ["EURUSD"]}),
        ):
            with self.subTest(payload=payload):
                with self.assertRaises(ValidationError):
                    trader_rpc_request_adapter.validate_python(
                        {
                            "type": "trader_rpc_request",
                            "request_id": "request-1",
                            "kind": kind,
                            "payload": payload,
                        }
                    )

    def test_rejects_buy_direction_before_the_worker_calls_mt5(self) -> None:
        with self.assertRaises(ValidationError):
            trader_rpc_request_adapter.validate_python(
                {
                    "type": "trader_rpc_request",
                    "request_id": "request-1",
                    "kind": "operation",
                    "payload": {
                        "type": "market",
                        "symbol": "EURUSD",
                        "volume": "0.01",
                        "direction": "BUY",
                        "filling_mode": "FOK",
                    },
                }
            )

    def test_rejects_read_kind_with_an_operation_payload(self) -> None:
        with self.assertRaises(ValidationError):
            trader_rpc_request_adapter.validate_python(
                {
                    "type": "trader_rpc_request",
                    "request_id": "request-1",
                    "kind": "read",
                    "payload": {
                        "type": "close",
                        "ticket": "123",
                        "volume": "0.01",
                    },
                }
            )

    def test_accepts_a_typed_calculated_margin_read(self) -> None:
        request = trader_rpc_request_adapter.validate_python(
            {
                "type": "trader_rpc_request",
                "request_id": "request-1",
                "kind": "read",
                "payload": {
                    "type": "calc_margin",
                    "symbol": "EURUSD",
                    "volume": "1.00",
                    "direction": "LONG",
                    "price": "1.10000",
                },
            }
        )

        self.assertEqual("calc_margin", request.payload.type)  # type: ignore[union-attr]

    def test_accepts_a_bounded_batch_of_margin_calculations(self) -> None:
        request = trader_rpc_request_adapter.validate_python(
            {
                "type": "trader_rpc_request",
                "request_id": "request-1",
                "kind": "read",
                "payload": {
                    "type": "calc_margin_batch",
                    "calculations": [
                        {
                            "symbol": "EURUSD",
                            "volume": "1.00",
                            "direction": "LONG",
                            "price": "1.10000",
                        },
                        {
                            "symbol": "EURUSD",
                            "volume": "1.00",
                            "direction": "SHORT",
                            "price": "1.09990",
                        },
                    ],
                },
            }
        )

        self.assertEqual("calc_margin_batch", request.payload.type)  # type: ignore[union-attr]

    def test_rejects_an_invalid_margin_calculation_batch_before_mt5(self) -> None:
        for calculations in (
            [],
            [{"symbol": "EURUSD", "volume": "1.00", "direction": "BUY", "price": "1.10000"}],
            [{"symbol": "EURUSD", "volume": "1.00", "direction": "LONG", "price": "1.10000", "extra": True}],
        ):
            with self.subTest(calculations=calculations):
                with self.assertRaises(ValidationError):
                    trader_rpc_request_adapter.validate_python(
                        {
                            "type": "trader_rpc_request",
                            "request_id": "request-1",
                            "kind": "read",
                            "payload": {"type": "calc_margin_batch", "calculations": calculations},
                        }
                    )

    def test_accepts_a_typed_symbols_read(self) -> None:
        request = trader_rpc_request_adapter.validate_python(
            {
                "type": "trader_rpc_request",
                "request_id": "request-1",
                "kind": "read",
                "payload": {"type": "symbols"},
            }
        )

        self.assertEqual("symbols", request.payload.type)  # type: ignore[union-attr]

    def test_requires_exact_response_shape(self) -> None:
        with self.assertRaises(ValidationError):
            trader_rpc_response_adapter.validate_python(
                {
                    "type": "trader_rpc_response",
                    "request_id": "request-1",
                    "kind": "operation",
                    "accepted": True,
                }
            )
