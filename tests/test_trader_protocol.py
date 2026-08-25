from __future__ import annotations

import unittest

from pydantic import ValidationError

from abt.trader_protocol import trader_rpc_request_adapter, trader_rpc_response_adapter


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

