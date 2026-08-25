from __future__ import annotations

import asyncio
import base64
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
import http.cookies
import httpx
import json
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
import uvicorn
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect as websocket_connect

from abt.controlplane.crypto import (
    ProofError,
    device_certificate_payload,
    enrollment_payload,
    trader_certificate_payload,
    trader_proof_payload,
    trader_rotation_payload,
    worker_rotation_payload,
    worker_proof_payload,
)
from abt.controlplane.ledger import LedgerError
from abt.controlplane.secrets import SecretStore, SecretStoreError
from abt.controlplane.service import (
    _analyze_product_catalogs,
    _broadcast_trader_market_data,
    _cancel_intent,
    _dispatch_manual_trade,
    _dispatch_manual_trade_operation,
    _cleanup_frozen_worker,
    _dispatch_accepted_trader_intent,
    _delete_expired_pending_secrets,
    _market_data_statistics,
    _trader_market_snapshots,
    _preflight_management_intent,
    _preflight_trader_intent,
    _request_market_data_with_retry,
    _request_trader_historical_ticks,
    _shared_supported_filling_modes,
    _intent_order,
    TraderIntentPayload,
    _validated_market_data_response,
    _validated_product_catalog_response,
    _TraderSessionConnection,
    _trader_market_subscription,
    create_app,
)
from abt.worker.reconciliation import reconcile_authenticated_worker
from abt.worker.session import AuthenticatedWorkerSession


class MemorySecretStore:
    def __init__(self) -> None:
        self.passwords: dict[str, str] = {}
        self.fail_next_delete = False

    def write_password(self, reference: str, password: str) -> None:
        self.passwords[reference] = password

    def read_password(self, reference: str) -> str:
        return self.passwords[reference]

    def delete_password(self, reference: str) -> None:
        if self.fail_next_delete:
            self.fail_next_delete = False
            raise SecretStoreError("OpenBao is unavailable.")
        del self.passwords[reference]


class TraderMarketSubscriptionTests(unittest.TestCase):
    def test_initial_market_data_excludes_workers_without_an_authenticated_session(self) -> None:
        class Ledger:
            def __init__(self) -> None:
                self.requested_worker_ids: set[str] | None = None

            def trader_market_data(self, worker_ids: set[str] | None) -> list[dict[str, object]]:
                self.requested_worker_ids = worker_ids
                return []

        ledger = Ledger()

        snapshots = _trader_market_snapshots(  # type: ignore[arg-type]
            ledger,
            {},
            None,
        )

        self.assertEqual([], snapshots)
        self.assertEqual(set(), ledger.requested_worker_ids)

    def test_filters_market_data_by_subscription_without_durable_events(self) -> None:
        class Socket:
            def __init__(self) -> None:
                self.messages: list[dict[str, object]] = []

            async def send_json(self, message: dict[str, object]) -> None:
                self.messages.append(message)

        selected_socket = Socket()
        all_socket = Socket()
        ignored_socket = Socket()
        selected = _TraderSessionConnection(selected_socket, {"worker-1"})  # type: ignore[arg-type]
        all_workers = _TraderSessionConnection(all_socket, None)  # type: ignore[arg-type]
        ignored = _TraderSessionConnection(ignored_socket, {"worker-2"})  # type: ignore[arg-type]

        asyncio.run(
            _broadcast_trader_market_data(
                {"trader-1": {selected, all_workers}, "trader-2": {ignored}},
                "worker-1",
                "2026-08-25T00:00:00+00:00",
                [{"symbol": "EURUSD", "bid": 1.1, "ask": 1.2}],
            )
        )

        expected = {
            "type": "market_data",
            "worker_id": "worker-1",
            "observed_at": "2026-08-25T00:00:00+00:00",
            "quotes": [{"symbol": "EURUSD", "bid": 1.1, "ask": 1.2}],
        }
        self.assertEqual([expected], selected_socket.messages)
        self.assertEqual([expected], all_socket.messages)
        self.assertEqual([], ignored_socket.messages)

    def test_accepts_wildcard_or_nonempty_worker_ids_only(self) -> None:
        self.assertIsNone(_trader_market_subscription(["*"]))
        self.assertEqual({"worker-1", "worker-2"}, _trader_market_subscription(["worker-1", "worker-2"]))
        for value in ([], ["*", "worker-1"], [""]):
            with self.assertRaises(ValueError):
                _trader_market_subscription(value)


class MarketDataAlignmentTests(unittest.TestCase):
    def test_aligns_bars_within_the_same_utc_minute_despite_calibration_jitter(self) -> None:
        first = {
            "bars": [
                {"time_utc": "2026-08-10T00:00:01Z", "close": 1.1000},
                {"time_utc": "2026-08-10T00:15:01Z", "close": 1.1010},
                {"time_utc": "2026-08-10T00:30:01Z", "close": 1.1020},
            ]
        }
        second = {
            "bars": [
                {"time_utc": "2026-08-10T00:00:02Z", "close": 1.1001},
                {"time_utc": "2026-08-10T00:15:02Z", "close": 1.1011},
                {"time_utc": "2026-08-10T00:30:02Z", "close": 1.1021},
            ]
        }

        statistics = _market_data_statistics({}, first, second)

        self.assertEqual(3, statistics["aligned_bar_count"])
        self.assertEqual(1.0, statistics["coverage_ratio"])


class MarketDataRequestTests(unittest.IsolatedAsyncioTestCase):
    async def test_uses_full_batch_timeout_and_records_a_nonempty_timeout_reason(self) -> None:
        timeouts: list[int] = []
        retries: list[str] = []

        async def worker_timeout(*_: object, **kwargs: object) -> dict[str, object]:
            timeouts.append(int(kwargs["timeout"]))
            raise asyncio.TimeoutError

        with patch("abt.controlplane.service._request_worker_analysis", side_effect=worker_timeout):
            with self.assertRaisesRegex(LedgerError, "did not respond within 30 seconds"):
                await _request_market_data_with_retry(
                    "analysis-123",
                    first_connection=object(),  # type: ignore[arg-type]
                    second_connection=object(),  # type: ignore[arg-type]
                    first_symbols=["EURUSD"],
                    second_symbols=["EURUSDC"],
                    analysis_period={
                        "started_at_utc": "2026-08-10T00:00:00Z",
                        "ended_at_utc": "2026-08-17T00:00:00Z",
                    },
                    policy={},
                    stage="m1_verification",
                    timeframe="M1",
                    record_retry=retries.append,
                )

        self.assertEqual([30, 30, 30, 30], timeouts)
        self.assertEqual(["Worker did not respond within 30 seconds."], retries)


class TraderHistoricalTickRequestTests(unittest.IsolatedAsyncioTestCase):
    async def test_splits_saturated_ranges_then_deduplicates_and_orders_ticks(self) -> None:
        calls: list[tuple[datetime, datetime]] = []
        active_requests = 0
        maximum_active_requests = 0

        async def request_worker(*_: object, **kwargs: object) -> dict[str, object]:
            nonlocal active_requests, maximum_active_requests
            active_requests += 1
            maximum_active_requests = max(maximum_active_requests, active_requests)
            await asyncio.sleep(0)
            payload = kwargs["message"]["payload"]  # type: ignore[index]
            start = datetime.fromisoformat(payload["from_utc"])  # type: ignore[index]
            end = datetime.fromisoformat(payload["to_utc"])  # type: ignore[index]
            calls.append((start, end))
            try:
                if end - start > timedelta(seconds=2):
                    return {"accepted": True, "result": {"ticks": [{"time_msc": index} for index in range(1000)]}}
                return {
                    "accepted": True,
                    "result": {
                        "ticks": [
                            {"time_msc": int(start.timestamp() * 1000)},
                            {"time_msc": int(end.timestamp() * 1000)},
                        ]
                    },
                }
            finally:
                active_requests -= 1

        start = datetime(2026, 8, 10, tzinfo=UTC)
        with patch("abt.controlplane.service._request_worker_analysis", side_effect=request_worker):
            response = await _request_trader_historical_ticks(
                object(),  # type: ignore[arg-type]
                {
                    "type": "historical_ticks",
                    "symbol": "EURUSD",
                    "from_utc": start.isoformat(),
                    "to_utc": (start + timedelta(seconds=4)).isoformat(),
                    "flags": "all",
                },
            )

        self.assertEqual(
            [
                (start, start + timedelta(seconds=4)),
                (start, start + timedelta(seconds=2)),
                (start + timedelta(seconds=2), start + timedelta(seconds=4)),
            ],
            calls,
        )
        self.assertEqual(1, maximum_active_requests)
        self.assertEqual(
            [
                {"time_msc": int((start + timedelta(seconds=offset)).timestamp() * 1000)}
                for offset in (0, 2, 4)
            ],
            response["result"]["ticks"],  # type: ignore[index]
        )


class TraderIntentPreflightTests(unittest.IsolatedAsyncioTestCase):
    def test_maps_a_shared_entry_and_directional_pip_protections_for_both_legs(self) -> None:
        payload = {
            "type": "intent", "pair_id": "pair-123", "primary_direction": "LONG", "lots": "0.1",
            "entry_price": "1.23450", "stop_loss_pips": "10", "take_profit_pips": "20",
            "filling_mode": "FOK", "expires_at": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
        }
        intent = TraderIntentPayload.model_validate(payload)
        specification = {"point": "0.00001", "digits": 5}

        primary = _intent_order(intent, {"symbol": "EURUSD.a", "specification": specification}, primary=True, execution_id="abt:x")
        hedge = _intent_order(intent, {"symbol": "EURUSD", "specification": specification}, primary=False, execution_id="abt:x")

        self.assertEqual("1.23450", primary["price"])
        self.assertEqual(primary["price"], hedge["price"])
        self.assertEqual(("LONG", "1.23350", "1.23650"), (primary["direction"], primary["sl"], primary["tp"]))
        self.assertEqual(("SHORT", "1.23550", "1.23250"), (hedge["direction"], hedge["sl"], hedge["tp"]))

    async def test_records_the_outcome_from_each_worker_when_one_check_fails(self) -> None:
        first_connection = object()
        second_connection = object()
        payload = {
            "type": "intent",
            "pair_id": "pair-123",
            "primary_direction": "LONG",
            "lots": "0.1",
            "entry_price": "1.2345",
            "stop_loss_pips": "10",
            "take_profit_pips": "20",
            "filling_mode": "FOK",
            "expires_at": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
        }

        class Ledger:
            rejected: tuple[object, ...] | None = None

            def trader_command_result(self, *_: object) -> None:
                return None

            def frozen_worker_ids(self, _worker_ids: list[str]) -> set[str]:
                return set()

            def product_pairs(self) -> list[dict[str, object]]:
                first_specification = {
                    "filling_modes": ["FOK", "IOC"],
                    "allowed_directions": ["LONG"],
                    "trade_stops_level": 5,
                    "volume_min": "0.1",
                    "volume_max": "100",
                    "volume_step": "0.1",
                    "point": "0.00001",
                    "digits": 5,
                }
                second_specification = {**first_specification, "allowed_directions": ["SHORT"]}
                return [
                    {
                        "product_pair_id": "pair-123",
                        "status": "active",
                        "reference_specifications": [
                            {"server": "Broker-A", "symbol": "EURUSD.a", "specification": first_specification},
                            {"server": "Broker-B", "symbol": "EURUSD", "specification": second_specification},
                        ],
                        "source_workers": {
                            "first_worker": {"worker_id": "worker-a", "server": "Broker-A"},
                            "second_worker": {"worker_id": "worker-b", "server": "Broker-B"},
                        },
                    }
                ]

            def reject_trader_command(self, *args: object) -> dict[str, object]:
                self.rejected = args
                return {"status": "rejected_preflight"}

        ledger = Ledger()

        async def check(connection: object, order: dict[str, object], **_: object) -> dict[str, object]:
            if connection is first_connection:
                return {
                    "type": "order_check_response",
                    "analysis_id": "order_check",
                    "request_id": "first",
                    "accepted": True,
                    "order": order,
                }
            return {
                "type": "order_check_response",
                "analysis_id": "order_check",
                "request_id": "second",
                "accepted": False,
                "order": order,
                "diagnostics": {
                    "retcode": 10015,
                    "comment": "Invalid price",
                    "quote": {"bid": 1.2344, "ask": 1.2346},
                },
            }

        with patch("abt.controlplane.service._request_order_check", side_effect=check):
            result = await _preflight_trader_intent(
                ledger,  # type: ignore[arg-type]
                {"worker-a": {first_connection}, "worker-b": {second_connection}},  # type: ignore[arg-type]
                {},
                "trader-123",
                "intent-001",
                payload,
            )

        self.assertEqual({"status": "rejected_preflight"}, result)
        assert ledger.rejected is not None
        outcomes = ledger.rejected[4]
        self.assertEqual(["accepted", "rejected"], [outcome["status"] for outcome in outcomes])  # type: ignore[index]
        self.assertEqual("worker-a", outcomes[0]["worker_id"])  # type: ignore[index]
        self.assertEqual("worker-b", outcomes[1]["worker_id"])  # type: ignore[index]
        self.assertEqual("Invalid price", outcomes[1]["response"]["diagnostics"]["comment"])  # type: ignore[index]
        self.assertEqual("1.23350", outcomes[0]["order"]["sl"])  # type: ignore[index]
        self.assertEqual("1.23650", outcomes[0]["order"]["tp"])  # type: ignore[index]

        disconnected_ledger = Ledger()
        disconnected = await _preflight_trader_intent(
            disconnected_ledger,  # type: ignore[arg-type]
            {"worker-a": {first_connection}},  # type: ignore[arg-type]
            {},
            "trader-123",
            "intent-002",
            payload,
        )

        self.assertEqual({"status": "rejected_preflight"}, disconnected)
        assert disconnected_ledger.rejected is not None
        disconnected_outcomes = disconnected_ledger.rejected[4]
        self.assertEqual(["not_started", "rejected"], [outcome["status"] for outcome in disconnected_outcomes])  # type: ignore[index]

    async def test_rejects_a_filling_mode_not_admitted_by_both_endpoints(self) -> None:
        class Ledger:
            def trader_command_result(self, *_: object) -> None:
                return None

            def product_pairs(self) -> list[dict[str, object]]:
                specification = {
                    "allowed_directions": ["LONG", "SHORT"], "trade_stops_level": 5,
                    "volume_min": "0.1", "volume_max": "100", "volume_step": "0.1",
                    "point": "0.00001", "digits": 5,
                }
                return [{
                    "product_pair_id": "pair-123", "status": "active",
                    "reference_specifications": [
                        {"server": "Broker-A", "symbol": "EURUSD.a", "specification": {**specification, "filling_modes": ["FOK", "IOC"]}},
                        {"server": "Broker-B", "symbol": "EURUSD", "specification": {**specification, "filling_modes": ["IOC"]}},
                    ],
                    "source_workers": {
                        "first_worker": {"worker_id": "worker-a", "server": "Broker-A"},
                        "second_worker": {"worker_id": "worker-b", "server": "Broker-B"},
                    },
                }]

            def reject_trader_command(self, *_: object) -> dict[str, object]:
                return {"status": "rejected_preflight"}

        result = await _preflight_trader_intent(
            Ledger(),  # type: ignore[arg-type]
            {}, {},
            "trader-123", "intent-fok-unshared",
            {
                "type": "intent", "pair_id": "pair-123", "primary_direction": "LONG", "lots": "0.1",
                "entry_price": "1.23450", "stop_loss_pips": "10", "take_profit_pips": "20",
                "filling_mode": "FOK", "expires_at": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
            },
        )

        self.assertEqual({"status": "rejected_preflight"}, result)

    async def test_rejects_a_frozen_worker_before_management_or_trader_preflight_dispatch(self) -> None:
        class Ledger:
            rejected: tuple[object, ...] | None = None
            management_rejected: tuple[object, ...] | None = None

            def trader_command_result(self, *_: object) -> None:
                return None

            def management_command_result(self, *_: object) -> None:
                return None

            def frozen_worker_ids(self, _worker_ids: list[str]) -> set[str]:
                return {"worker-a"}

            def product_pairs(self) -> list[dict[str, object]]:
                specification = {
                    "allowed_directions": ["LONG", "SHORT"], "trade_stops_level": 5,
                    "volume_min": "0.1", "volume_max": "100", "volume_step": "0.1",
                    "point": "0.00001", "digits": 5, "filling_modes": ["FOK", "IOC"],
                }
                return [{
                    "product_pair_id": "pair-123",
                    "status": "active",
                    "reference_specifications": [
                        {"server": "Broker-A", "symbol": "EURUSD.a", "specification": specification},
                        {"server": "Broker-B", "symbol": "EURUSD", "specification": specification},
                    ],
                    "source_workers": {
                        "first_worker": {"worker_id": "worker-a", "server": "Broker-A"},
                        "second_worker": {"worker_id": "worker-b", "server": "Broker-B"},
                    },
                }]

            def reject_trader_command(self, *args: object) -> dict[str, object]:
                self.rejected = args
                return {"status": "rejected_preflight"}

            def reject_management_intent(self, *args: object) -> dict[str, object]:
                self.management_rejected = args
                return {"status": "rejected_preflight"}

        ledger = Ledger()
        payload = {
            "type": "intent", "pair_id": "pair-123", "primary_direction": "LONG", "lots": "0.1",
            "entry_price": "1.23450", "stop_loss_pips": "10", "take_profit_pips": "20",
            "filling_mode": "FOK", "expires_at": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
        }
        result = await _preflight_trader_intent(
            ledger,  # type: ignore[arg-type]
            {}, {},
            "trader-123", "intent-frozen-worker", payload,
        )
        management_result = await _preflight_management_intent(
            ledger,  # type: ignore[arg-type]
            {}, {},
            "ABCDEF", "management-intent-frozen-worker", payload,
        )

        self.assertEqual({"status": "rejected_preflight"}, result)
        self.assertEqual({"status": "rejected_preflight"}, management_result)
        assert ledger.rejected is not None
        assert ledger.management_rejected is not None
        self.assertEqual("Frozen worker cannot be selected for a new trade: worker-a.", ledger.rejected[3])
        self.assertEqual("Frozen worker cannot be selected for a new trade: worker-a.", ledger.management_rejected[3])


class WorkerIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def test_manual_trade_reconciles_zero_price_market_response_before_protection(self) -> None:
        first_connection = object()
        second_connection = object()
        calls: list[str] = []
        activated: list[list[dict[str, object]]] = []
        freezes: list[tuple[str, list[str], dict[str, object]]] = []

        class Ledger:
            def claim_manual_trade_dispatch(self, _manual_trade_id: str) -> bool:
                return True

            def manual_trade_plan(self, _manual_trade_id: str) -> dict[str, object]:
                return {
                    "interval_seconds": 0,
                    "legs": [
                        {
                            "worker_id": "worker-a",
                            "symbol": "EURUSD",
                            "direction": "BUY",
                            "lots": "0.1",
                            "pip_size": "0.0001",
                            "stop_loss_pips": "10",
                            "take_profit_pips": "20",
                        },
                        {
                            "worker_id": "worker-b",
                            "symbol": "EURUSD.a",
                            "direction": "SELL",
                            "lots": "0.2",
                            "pip_size": "0.0001",
                            "stop_loss_pips": "10",
                            "take_profit_pips": "20",
                        },
                    ],
                }

            def frozen_worker_ids(self, _worker_ids: list[str]) -> set[str]:
                return set()

            def record_manual_trade_event(
                self, _manual_trade_id: str, _event_type: str, _payload: dict[str, object], *, status: str | None = None
            ) -> None:
                return None

            def activate_manual_trade(self, _manual_trade_id: str, active_legs: list[dict[str, object]]) -> None:
                activated.append(active_legs)

            def freeze_workers(self, source: str, worker_ids: list[str], audit: dict[str, object]) -> None:
                freezes.append((source, worker_ids, audit))

        async def order_execute(connection: object, order: dict[str, object]) -> dict[str, object]:
            calls.append(str(order["action"]))
            if order["action"] == "protect":
                return {"accepted": True, "order": order}
            ticket = 501 if connection is first_connection else 502
            return {
                "accepted": True,
                "order": order,
                "result": {
                    "retcode": 10009,
                    "order": ticket,
                    "deal": 0,
                    "price": 0.0,
                    "bid": 0.0,
                    "ask": 0.0,
                    "position": (
                        {"ticket": 901, "volume": "0.1", "price_open": "1.1002"}
                        if connection is first_connection
                        else {"ticket": 902, "volume": "0.2", "price_open": "1.0998"}
                    ),
                },
            }

        with (
            patch("abt.controlplane.service._connected_worker_session", side_effect=[first_connection, second_connection]),
            patch("abt.controlplane.service._request_order_execute", side_effect=order_execute),
        ):
            await _dispatch_manual_trade(
                Ledger(),  # type: ignore[arg-type]
                {"worker-a": {object()}, "worker-b": {object()}},  # type: ignore[arg-type]
                {},
                "manual-1",
            )

        self.assertEqual(
            ["market", "protect", "market", "protect"],
            calls,
        )
        self.assertEqual([], freezes)
        self.assertEqual(1, len(activated))
        self.assertEqual(["501", "502"], [leg["market_order_ticket"] for leg in activated[0]])
        self.assertEqual(["901", "902"], [leg["position_ticket"] for leg in activated[0]])
        self.assertEqual(["1.1002", "1.0998"], [leg["fill_price"] for leg in activated[0]])
        for leg in activated[0]:
            reconciliation = leg["reconciliation"]
            self.assertIsInstance(reconciliation, dict)
            self.assertTrue(str(reconciliation["observed_at"]).endswith("+00:00"))
            self.assertEqual(str(leg["position_ticket"]), str(reconciliation["position"]["ticket"]))

    async def test_manual_trade_freezes_before_protection_for_unresolved_reconciliation(self) -> None:
        for positions in (
            [],
            [
                [
                    {"ticket": 901, "volume": "0.1", "price_open": "1.1002"},
                    {"ticket": 902, "volume": "0.1", "price_open": "1.1003"},
                ][0],
                {"ticket": 902, "volume": "0.1", "price_open": "1.1003"},
            ],
            [{"ticket": 901, "volume": "0.1", "price_open": "0"}],
        ):
            with self.subTest(positions=positions):
                first_connection = object()
                calls: list[str] = []
                events: list[tuple[str, str | None, dict[str, object]]] = []
                freezes: list[tuple[str, list[str], dict[str, object]]] = []

                class Ledger:
                    def claim_manual_trade_dispatch(self, _manual_trade_id: str) -> bool:
                        return True

                    def manual_trade_plan(self, _manual_trade_id: str) -> dict[str, object]:
                        return {
                            "interval_seconds": 0,
                            "legs": [
                                {
                                    "worker_id": "worker-a",
                                    "symbol": "EURUSD",
                                    "direction": "BUY",
                                    "lots": "0.1",
                                    "pip_size": "0.0001",
                                    "stop_loss_pips": "10",
                                    "take_profit_pips": "20",
                                },
                                {
                                    "worker_id": "worker-b",
                                    "symbol": "EURUSD.a",
                                    "direction": "SELL",
                                    "lots": "0.2",
                                    "pip_size": "0.0001",
                                    "stop_loss_pips": "10",
                                    "take_profit_pips": "20",
                                },
                            ],
                        }

                    def frozen_worker_ids(self, _worker_ids: list[str]) -> set[str]:
                        return set()

                    def record_manual_trade_event(
                        self, _manual_trade_id: str, event_type: str, payload: dict[str, object], *,
                        status: str | None = None,
                    ) -> None:
                        events.append((event_type, status, payload))

                    def activate_manual_trade(self, _manual_trade_id: str, _active_legs: list[dict[str, object]]) -> None:
                        self.fail("A missing or ambiguous position must not activate the manual trade.")

                    def freeze_workers(self, source: str, worker_ids: list[str], audit: dict[str, object]) -> None:
                        freezes.append((source, worker_ids, audit))

                async def order_execute(_connection: object, order: dict[str, object]) -> dict[str, object]:
                    calls.append(str(order["action"]))
                    return {
                        "accepted": True,
                        "order": order,
                        "result": {
                            "retcode": 10009,
                            "order": 501,
                            "deal": 0,
                            "price": 0.0,
                            "bid": 0.0,
                            "ask": 0.0,
                        },
                    }

                async def reconcile(
                    _connection: object, operation: str, _payload: dict[str, str]
                ) -> dict[str, object]:
                    calls.append("reconcile")
                    return {"accepted": True, "operation": operation, "result": {"orders": [], "positions": positions}}

                with (
                    patch("abt.controlplane.service._connected_worker_session", return_value=first_connection),
                    patch("abt.controlplane.service._request_order_execute", side_effect=order_execute),
                    patch("abt.controlplane.service._execution_recovery_request", side_effect=reconcile),
                ):
                    await _dispatch_manual_trade(
                        Ledger(),  # type: ignore[arg-type]
                        {"worker-a": {object()}, "worker-b": {object()}},  # type: ignore[arg-type]
                        {},
                        "manual-1",
                    )

                self.assertEqual(["market"], calls)
                self.assertEqual([("manual_trade_execution_frozen", "needs_human")], [
                    (event_type, status) for event_type, status, _payload in events
                ])
                self.assertIn("confirmed position evidence", str(events[0][2]["reason"]))
                self.assertEqual(
                    [("manual_trade_execution_anomaly", ["worker-a", "worker-b"])],
                    [(source, worker_ids) for source, worker_ids, _audit in freezes],
                )

    async def test_exit_closes_verified_remaining_leg_and_marks_external_closure_for_human_review(self) -> None:
        first_connection = object()
        second_connection = object()
        closed_tickets: list[str] = []
        events: list[tuple[str, str | None, dict[str, object]]] = []
        freezes: list[tuple[str, list[str], dict[str, object]]] = []

        class Ledger:
            def claim_manual_trade_operation_dispatch(self, _operation_id: str) -> bool:
                return True

            def manual_trade_operation(self, _operation_id: str) -> dict[str, object]:
                return {
                    "manual_trade_id": "manual-1",
                    "operation": "exit",
                    "plan": {
                        "manual_trade_id": "manual-1",
                        "operation": "exit",
                        "legs": [
                            {"worker_id": "worker-a", "position": "101"},
                            {"worker_id": "worker-b", "position": "202"},
                        ],
                    },
                }

            def frozen_worker_ids(self, _worker_ids: list[str]) -> set[str]:
                return set()

            def record_manual_trade_operation(
                self, _operation_id: str, event_type: str, payload: dict[str, object], *, status: str | None = None
            ) -> None:
                events.append((event_type, status, payload))

            def freeze_workers(self, source: str, worker_ids: list[str], audit: dict[str, object]) -> None:
                freezes.append((source, worker_ids, audit))

        async def recovery(
            _connection: object, operation: str, payload: dict[str, str]
        ) -> dict[str, object]:
            if operation == "manual_position_reconcile":
                if not reconciliation_results:
                    self.fail("The operation reconciled more times than expected.")
                result = reconciliation_results[0].pop(0)
                if not reconciliation_results[0]:
                    reconciliation_results.pop(0)
                return {"accepted": True, "operation": operation, "result": result}
            self.assertEqual("execution_close", operation)
            closed_tickets.append(payload["ticket"])
            return {"accepted": True}

        reconciliation_results = [
            [
                {"orders": [], "positions": []},
                {"orders": [], "positions": [{"ticket": "202", "volume": "0.2"}]},
            ],
            [
                {"orders": [], "positions": []},
                {"orders": [], "positions": []},
            ],
        ]
        with (
            patch("abt.controlplane.service._connected_worker_session", side_effect=[first_connection, second_connection]),
            patch("abt.controlplane.service._execution_recovery_request", side_effect=recovery),
        ):
            await _dispatch_manual_trade_operation(
                Ledger(),  # type: ignore[arg-type]
                {"worker-a": {object()}, "worker-b": {object()}},  # type: ignore[arg-type]
                {},
                "exit-1",
            )

        self.assertEqual(["202"], closed_tickets)
        self.assertEqual(
            [("manual_trade_operation_frozen", "needs_human")],
            [(event_type, status) for event_type, status, _payload in events],
        )
        self.assertIn("already closed externally", str(events[0][2]["reason"]))
        self.assertEqual(
            [(
                "manual_trade_operation_anomaly",
                ["worker-a", "worker-b"],
                {
                    "manual_trade_id": "manual-1",
                    "operation_id": "exit-1",
                    "reason": events[0][2]["reason"],
                },
            )],
            freezes,
        )

    async def test_dispatch_does_not_send_orders_when_a_worker_freezes_after_preflight(self) -> None:
        events: list[tuple[str, str | None]] = []

        class Ledger:
            def claim_accepted_intent_dispatch(self, _intent_id: str) -> bool:
                return True

            def frozen_worker_ids(self, _worker_ids: list[str]) -> set[str]:
                return {"worker-a"}

            def record_intent_execution(
                self, _intent_id: str, event_type: str, _payload: dict[str, object], *, status: str | None = None
            ) -> None:
                events.append((event_type, status))

        async def order_execute_must_not_run(*_: object, **__: object) -> dict[str, object]:
            self.fail("a frozen worker must not receive a new order")

        with (
            patch("abt.controlplane.service._connected_worker_session", side_effect=[object(), object()]),
            patch("abt.controlplane.service._request_order_execute", side_effect=order_execute_must_not_run),
        ):
            await _dispatch_accepted_trader_intent(
                Ledger(),  # type: ignore[arg-type]
                {"worker-a": {object()}, "worker-b": {object()}},
                {},
                "intent-123",
                "FOK",
                [
                    {"worker_id": "worker-a", "order": {"symbol": "EURUSD"}},
                    {"worker_id": "worker-b", "order": {"symbol": "EURUSD"}},
                ],
            )

        self.assertEqual(
            [("intent_dispatch_started", None), ("intent_execution_frozen", "needs_human")],
            events,
        )

    async def test_partial_ioc_execution_freezes_workers_without_attempting_pair_recovery(self) -> None:
        events: list[tuple[str, dict[str, object], str | None]] = []
        freezes: list[tuple[str, list[str], dict[str, object]]] = []

        class Ledger:
            def claim_accepted_intent_dispatch(self, _intent_id: str) -> bool:
                return True

            def frozen_worker_ids(self, _worker_ids: list[str]) -> set[str]:
                return set()

            def record_intent_execution(
                self, _intent_id: str, event_type: str, payload: dict[str, object], *, status: str | None = None
            ) -> None:
                events.append((event_type, payload, status))

            def freeze_workers(self, source: str, worker_ids: list[str], audit: dict[str, object]) -> None:
                freezes.append((source, worker_ids, audit))

        async def execute(_connection: object, order: dict[str, object]) -> dict[str, object]:
            return {
                "accepted": order["symbol"] == "EURUSD.a",
                "order": order,
                "result": {"ticket": 101},
            }

        with (
            patch("abt.controlplane.service._connected_worker_session", side_effect=[object(), object()]),
            patch("abt.controlplane.service._request_order_execute", side_effect=execute),
            patch(
                "abt.controlplane.service._execution_recovery_request",
                side_effect=AssertionError("an anomaly must not attempt pair recovery"),
            ),
        ):
            await _dispatch_accepted_trader_intent(
                Ledger(),  # type: ignore[arg-type]
                {"worker-a": {object()}, "worker-b": {object()}},
                {},
                "intent-123",
                "IOC",
                [
                    {"worker_id": "worker-a", "order": {"symbol": "EURUSD.a"}},
                    {"worker_id": "worker-b", "order": {"symbol": "EURUSD"}},
                ],
            )

        self.assertEqual(
            ("intent_execution_frozen", "needs_human"),
            (events[-1][0], events[-1][2]),
        )
        self.assertEqual(
            [(
                "intent_execution_anomaly",
                ["worker-a", "worker-b"],
                {
                    "intent_id": "intent-123",
                    "reason": "Incomplete IOC entry requires worker isolation.",
                },
            )],
            freezes,
        )


class IntentCancellationTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancels_zero_fill_orders_and_emits_terminal_event_after_reconciliation(self) -> None:
        events: list[tuple[str, str | None]] = []
        operations: list[tuple[str, dict[str, str]]] = []

        class Ledger:
            def record_intent_execution(
                self, _intent_id: str, event_type: str, _payload: dict[str, object], *, status: str | None = None
            ) -> None:
                events.append((event_type, status))

        async def recover(_connection: object, operation: str, payload: dict[str, str]) -> dict[str, object]:
            operations.append((operation, payload))
            return {"accepted": True, "operation": operation, "result": {"retcode": 10009}}

        with (
            patch("abt.controlplane.service._connected_worker_session", side_effect=[object(), object()]),
            patch("abt.controlplane.service._reconcile_execution", side_effect=[
                [{"orders": [{"ticket": "101", "volume": "0.1"}], "positions": []}, {"orders": [{"ticket": "102", "volume": "0.1"}], "positions": []}],
                [{"orders": [], "positions": []}, {"orders": [], "positions": []}],
            ]),
            patch("abt.controlplane.service._execution_recovery_request", side_effect=recover),
        ):
            await _cancel_intent(
                Ledger(),  # type: ignore[arg-type]
                {"worker-a": {object()}, "worker-b": {object()}}, {},
                "intent-123",
                [
                    {"worker_id": "worker-a", "order": {"control_plane_command_id": "abt:correlated"}},
                    {"worker_id": "worker-b", "order": {"control_plane_command_id": "abt:correlated"}},
                ],
            )

        self.assertEqual(
            [("execution_cancel", {"ticket": "101", "volume": "0.1"}), ("execution_cancel", {"ticket": "102", "volume": "0.1"})],
            operations,
        )
        self.assertEqual(("cancelled", "cancelled"), events[-1])

    async def test_rejects_cancellation_when_reconciliation_observes_a_fill(self) -> None:
        events: list[tuple[str, str | None]] = []
        freezes: list[tuple[str, list[str], dict[str, object]]] = []

        class Ledger:
            def record_intent_execution(
                self, _intent_id: str, event_type: str, _payload: dict[str, object], *, status: str | None = None
            ) -> None:
                events.append((event_type, status))

            def freeze_workers(self, source: str, worker_ids: list[str], audit: dict[str, object]) -> None:
                freezes.append((source, worker_ids, audit))

        with (
            patch("abt.controlplane.service._connected_worker_session", side_effect=[object(), object()]),
            patch("abt.controlplane.service._reconcile_execution", return_value=[
                {"orders": [], "positions": [{"ticket": "201", "volume": "0.1"}]}, {"orders": [], "positions": []}
            ]),
        ):
            await _cancel_intent(
                Ledger(),  # type: ignore[arg-type]
                {"worker-a": {object()}, "worker-b": {object()}}, {},
                "intent-123",
                [
                    {"worker_id": "worker-a", "order": {"control_plane_command_id": "abt:correlated"}},
                    {"worker_id": "worker-b", "order": {"control_plane_command_id": "abt:correlated"}},
                ],
            )

        self.assertEqual(
            [
                ("intent_execution_anomaly", ["worker-a", "worker-b"], {
                    "intent_id": "intent-123",
                    "reason": "Cancellation observed broker exposure.",
                }),
            ],
            freezes,
        )
        self.assertEqual(("intent_execution_frozen", "needs_human"), events[-1])

    async def test_single_leg_execution_timeout_freezes_participating_workers(self) -> None:
        events: list[tuple[str, str | None]] = []
        freezes: list[tuple[str, list[str], dict[str, object]]] = []

        class Ledger:
            def claim_accepted_intent_dispatch(self, _intent_id: str) -> bool:
                return True

            def frozen_worker_ids(self, _worker_ids: list[str]) -> set[str]:
                return set()

            def record_intent_execution(
                self, _intent_id: str, event_type: str, _payload: dict[str, object], *, status: str | None = None
            ) -> None:
                events.append((event_type, status))

            def freeze_workers(self, source: str, worker_ids: list[str], audit: dict[str, object]) -> None:
                freezes.append((source, worker_ids, audit))

        async def execution(*args: object, **_: object) -> dict[str, object]:
            if args[0] == "first":
                raise asyncio.TimeoutError
            return {"accepted": True, "order": args[1], "result": {"ticket": 12}}

        outcomes = [
            {"worker_id": "worker-a", "order": {"control_plane_command_id": "abt:correlated"}},
            {"worker_id": "worker-b", "order": {"control_plane_command_id": "abt:correlated"}},
        ]
        with (
            patch("abt.controlplane.service._connected_worker_session", side_effect=["first", "second"]),
            patch("abt.controlplane.service._request_order_execute", side_effect=execution),
        ):
            await _dispatch_accepted_trader_intent(
                Ledger(),  # type: ignore[arg-type]
                {"worker-a": {object()}, "worker-b": {object()}}, {}, "intent-123", "FOK", outcomes
            )

        self.assertIn(("incomplete_entry_detected", "working"), events)
        self.assertIn(("intent_execution_frozen", "needs_human"), events)
        self.assertEqual(
            [(
                "intent_execution_anomaly",
                ["worker-a", "worker-b"],
                {
                    "intent_id": "intent-123",
                    "reason": "Incomplete FOK entry requires worker isolation.",
                },
            )],
            freezes,
        )


class MemoryCertificateIssuer:
    def __init__(self) -> None:
        self._key = ec.generate_private_key(ec.SECP256R1())
        self.fail_next_issue = False

    def issue(self, *, worker_id: str, login: int, server: str, public_key_pem: str) -> str:
        if self.fail_next_issue:
            self.fail_next_issue = False
            raise SecretStoreError("OpenBao device certificate signing failed (HTTP 403).")
        issued_at = datetime.now(UTC)
        payload = device_certificate_payload(
            worker_id=worker_id,
            login=login,
            server=server,
            public_key_pem=public_key_pem,
            issued_at=issued_at,
            expires_at=issued_at + timedelta(days=30),
        )
        return json.dumps(
            {
                "payload": base64.b64encode(payload).decode("ascii"),
                "signature": base64.b64encode(self._key.sign(payload, ec.ECDSA(hashes.SHA256()))).decode("ascii"),
            },
            separators=(",", ":"),
            sort_keys=True,
        )

    def verify(self, certificate: str) -> None:
        try:
            envelope = json.loads(certificate)
            payload = base64.b64decode(envelope["payload"], validate=True)
            signature = base64.b64decode(envelope["signature"], validate=True)
            self._key.public_key().verify(signature, payload, ec.ECDSA(hashes.SHA256()))
        except (InvalidSignature, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ProofError("The device certificate signature is invalid.") from error

    def issue_trader(self, *, trader_id: str, strategy_name: str, public_key_pem: str) -> str:
        issued_at = datetime.now(UTC)
        payload = trader_certificate_payload(
            trader_id=trader_id,
            strategy_name=strategy_name,
            public_key_pem=public_key_pem,
            issued_at=issued_at,
            expires_at=issued_at + timedelta(days=30),
        )
        return json.dumps(
            {
                "payload": base64.b64encode(payload).decode("ascii"),
                "signature": base64.b64encode(self._key.sign(payload, ec.ECDSA(hashes.SHA256()))).decode("ascii"),
            },
            separators=(",", ":"),
            sort_keys=True,
        )


class ControlPlaneServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.secret_store = MemorySecretStore()
        self.certificate_issuer = MemoryCertificateIssuer()
        self.app = create_app(
            Path(self._directory.name) / "ledger.duckdb",
            secret_store=self.secret_store,
            certificate_issuer=self.certificate_issuer,
        )
        self.client = TestClient(self.app, base_url="https://testserver")
        self.http_client = TestClient(self.app, base_url="https://testserver")
        self.app.state.ledger.create_admin("ABCDEF", "A-secure-admin-password!")

    def tearDown(self) -> None:
        self.http_client.close()
        self.client.close()
        self._directory.cleanup()

    def test_admin_can_approve_a_signed_worker_enrollment(self) -> None:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        ).decode("utf-8")
        account_info = {"login": 123456, "server": "Broker-Demo", "trade_mode": 0}
        terminal_info = {"build": 5000, "company": "MetaQuotes", "name": "MetaTrader 5"}
        password = "worker-memory-only-password"
        challenge = self._enrollment_challenge()
        signature = private_key.sign(
            enrollment_payload(123456, "Broker-Demo", account_info, terminal_info, password, challenge),
            ec.ECDSA(hashes.SHA256()),
        )
        enrollment_response = self.client.post(
            "/api/enrollments",
            json={
                "registration_invite": self.app.state.ledger.create_registration_invite("ABCDEF", "worker"),
                "login": 123456,
                "server": "Broker-Demo",
                "account_info": account_info,
                "terminal_info": terminal_info,
                "mt5_password": password,
                "enrollment_challenge": challenge,
                "public_key_pem": public_key_pem,
                "proof_signature": base64.b64encode(signature).decode("ascii"),
            },
        )
        self.assertEqual(201, enrollment_response.status_code)
        self.assertNotIn("worker-memory-only-password", enrollment_response.text)

        login_response = self.client.post(
            "/api/admin/login",
            headers={"CF-Connecting-IP": "203.0.113.10"},
            json={"username": "ABCDEF", "password": "A-secure-admin-password!"},
        )
        self.assertEqual(200, login_response.status_code)
        csrf_token = login_response.json()["csrf_token"]
        pending_response = self.client.get("/api/admin/enrollments")
        self.assertEqual(200, pending_response.status_code)
        self.assertEqual(1, len(pending_response.json()))
        self.assertNotIn("pairing_code", pending_response.json()[0])

        approval = self.client.post(
            f"/api/admin/enrollments/{enrollment_response.json()['enrollment_id']}/approve",
            headers={"X-CSRF-Token": csrf_token},
        )
        self.assertEqual(200, approval.status_code)
        self.assertIn("worker_id", approval.json())

    def test_rejects_an_enrollment_without_a_valid_p256_proof(self) -> None:
        response = self.client.post(
            "/api/enrollments",
            json={
                "login": 123456,
                "server": "Broker-Demo",
                "public_key_pem": "not a key",
                "proof_signature": "not-a-signature",
            },
        )
        self.assertEqual(422, response.status_code)

    def test_worker_enrollment_does_not_consume_its_invite_when_registration_fails(self) -> None:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        account_info = {"login": 123456, "server": "Broker-Demo"}
        terminal_info = {"name": "MetaTrader 5"}
        invite = self.app.state.ledger.create_registration_invite("ABCDEF", "worker")
        invalid_challenge = "already-expired-challenge"
        invalid_signature = private_key.sign(
            enrollment_payload(
                123456, "Broker-Demo", account_info, terminal_info, "worker-memory-only-password", invalid_challenge
            ),
            ec.ECDSA(hashes.SHA256()),
        )

        rejected = self.client.post(
            "/api/enrollments",
            json={
                "registration_invite": invite,
                "login": 123456,
                "server": "Broker-Demo",
                "account_info": account_info,
                "terminal_info": terminal_info,
                "mt5_password": "worker-memory-only-password",
                "enrollment_challenge": invalid_challenge,
                "public_key_pem": public_key_pem,
                "proof_signature": base64.b64encode(invalid_signature).decode("ascii"),
            },
        )

        self.assertEqual(409, rejected.status_code)
        self.assertEqual("active", self.app.state.ledger.registration_invites()[0]["status"])

    def test_enrollment_challenge_rejects_replay_and_password_substitution(self) -> None:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        account_info = {"login": 123456, "server": "Broker-Demo"}
        terminal_info = {"name": "MetaTrader 5"}
        password = "worker-memory-only-password"
        challenge = self._enrollment_challenge()
        signature = private_key.sign(
            enrollment_payload(123456, "Broker-Demo", account_info, terminal_info, password, challenge),
            ec.ECDSA(hashes.SHA256()),
        )
        request = {
            "registration_invite": self.app.state.ledger.create_registration_invite("ABCDEF", "worker"),
            "login": 123456, "server": "Broker-Demo",
            "account_info": account_info, "terminal_info": terminal_info, "mt5_password": password,
            "enrollment_challenge": challenge, "public_key_pem": public_key_pem,
            "proof_signature": base64.b64encode(signature).decode("ascii"),
        }
        substituted = dict(request, mt5_password="substituted-password")
        self.assertEqual(422, self.client.post("/api/enrollments", json=substituted).status_code)
        self.assertEqual(201, self.client.post("/api/enrollments", json=request).status_code)
        self.assertEqual(409, self.client.post("/api/enrollments", json=request).status_code)

    def test_administrator_can_view_audit_events_then_logout(self) -> None:
        login_response = self.client.post(
            "/api/admin/login",
            headers={"CF-Connecting-IP": "203.0.113.10"},
            json={"username": "ABCDEF", "password": "A-secure-admin-password!"},
        )
        csrf_token = login_response.json()["csrf_token"]

        events_response = self.client.get("/api/admin/events")
        self.assertEqual(200, events_response.status_code)
        self.assertIn("admin_login_succeeded", [event["event_type"] for event in events_response.json()["items"]])

        logout_response = self.client.post("/api/admin/logout", headers={"X-CSRF-Token": csrf_token})
        self.assertEqual(204, logout_response.status_code)
        self.assertEqual(401, self.client.get("/api/admin/events").status_code)

    def test_administrator_can_issue_a_registration_invite_once(self) -> None:
        login = self.client.post(
            "/api/admin/login",
            json={"username": "ABCDEF", "password": "A-secure-admin-password!"},
        )

        issued = self.client.post(
            "/api/admin/registration-invites",
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
            json={"role": "trader"},
        )

        self.assertEqual(201, issued.status_code)
        self.assertEqual("trader", issued.json()["role"])
        self.assertTrue(issued.json()["invite"])
        self.assertNotIn("invite", self.client.get("/api/admin/registration-invites").json()[0])

    def test_trader_enrollment_accepts_a_trader_invite_and_p256_proof_without_attestation(self) -> None:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        invite = self.app.state.ledger.create_registration_invite("ABCDEF", "trader")
        payload = json.dumps(
            {"claimed_public_ip": "203.0.113.4", "registration_invite": invite, "strategy_name": "mean-reversion"},
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        signature = private_key.sign(payload, ec.ECDSA(hashes.SHA256()))

        response = self.client.post(
            "/api/traders/enrollments",
            json={
                "registration_invite": invite,
                "strategy_name": "mean-reversion",
                "claimed_public_ip": "203.0.113.4",
                "public_key_pem": public_key_pem,
                "proof_signature": base64.b64encode(signature).decode("ascii"),
            },
        )

        self.assertEqual(201, response.status_code)
        self.assertTrue(response.json()["registration_id"])

    def test_administrator_approval_issues_a_30_day_trader_certificate(self) -> None:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        invite = self.app.state.ledger.create_registration_invite("ABCDEF", "trader")
        payload = json.dumps(
            {"claimed_public_ip": "203.0.113.4", "registration_invite": invite, "strategy_name": "mean-reversion"},
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        enrollment = self.client.post(
            "/api/traders/enrollments",
            json={
                "registration_invite": invite,
                "strategy_name": "mean-reversion",
                "claimed_public_ip": "203.0.113.4",
                "public_key_pem": public_key_pem,
                "proof_signature": base64.b64encode(private_key.sign(payload, ec.ECDSA(hashes.SHA256()))).decode("ascii"),
            },
        )
        login = self.client.post(
            "/api/admin/login",
            json={"username": "ABCDEF", "password": "A-secure-admin-password!"},
        )

        approval = self.client.post(
            f"/api/admin/traders/enrollments/{enrollment.json()['registration_id']}/approve",
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
        )

        self.assertEqual(200, approval.status_code)
        self.assertIn("trader_id", approval.json())
        claims = json.loads(base64.b64decode(json.loads(approval.json()["certificate"])["payload"]))
        self.assertEqual(approval.json()["trader_id"], claims["trader_id"])
        self.assertEqual(30, (datetime.fromisoformat(claims["expires_at"]) - datetime.fromisoformat(claims["issued_at"])).days)
        replacement_key = ec.generate_private_key(ec.SECP256R1())
        replacement_public_key_pem = replacement_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        challenge = self.client.post(
            "/api/traders/certificates/rotation-challenge",
            json={
                "trader_id": approval.json()["trader_id"],
                "public_key_pem": replacement_public_key_pem,
            },
        )
        self.assertEqual(200, challenge.status_code)
        rotation_payload = trader_rotation_payload(
            trader_id=approval.json()["trader_id"],
            replacement_public_key_pem=replacement_public_key_pem,
            nonce=challenge.json()["nonce"],
        )
        rotated = self.client.post(
            "/api/traders/certificates/rotate",
            json={
                "trader_id": approval.json()["trader_id"],
                "public_key_pem": replacement_public_key_pem,
                "old_key_signature": base64.b64encode(private_key.sign(rotation_payload, ec.ECDSA(hashes.SHA256()))).decode("ascii"),
                "replacement_key_signature": base64.b64encode(
                    replacement_key.sign(rotation_payload, ec.ECDSA(hashes.SHA256()))
                ).decode("ascii"),
            },
        )
        self.assertEqual(200, rotated.status_code)
        self.assertEqual(replacement_public_key_pem, self.app.state.ledger.active_trader(approval.json()["trader_id"]).public_key_pem)
        with self.client.websocket_connect("/api/traders/session") as old_key_session:
            old_key_session.send_json(
                {"trader_id": approval.json()["trader_id"], "certificate": approval.json()["certificate"]}
            )
            old_challenge = old_key_session.receive_json()
            old_proof = private_key.sign(
                trader_proof_payload(
                    purpose=old_challenge["purpose"], trader_id=old_challenge["trader_id"], nonce=old_challenge["nonce"]
                ),
                ec.ECDSA(hashes.SHA256()),
            )
            old_key_session.send_json({"signature": base64.b64encode(old_proof).decode("ascii")})
            self.assertEqual("authenticated", old_key_session.receive_json()["type"])

        with self.app.state.ledger._transaction():
            self.app.state.ledger._connection.execute(
                "UPDATE certificate_overlaps SET expires_at = ? WHERE role = 'trader' AND identity_id = ?",
                [datetime.now(UTC) - timedelta(seconds=1), approval.json()["trader_id"]],
            )
        with self.client.websocket_connect("/api/traders/session") as old_key_session:
            old_key_session.send_json(
                {"trader_id": approval.json()["trader_id"], "certificate": approval.json()["certificate"]}
            )
            with self.assertRaises(WebSocketDisconnect):
                old_key_session.receive_json()
        self.assertIn(
            "trader_certificate_rotated",
            [event["event_type"] for event in self.app.state.ledger.events()],
        )
        events = self.client.get("/api/admin/events")
        self.assertEqual(200, events.status_code)
        self.assertIn("trader_certificate_rotated", [event["event_type"] for event in events.json()["items"]])

    def test_worker_rotation_requires_both_proofs_and_preserves_one_hour_overlap(self) -> None:
        old_key, worker_id, old_certificate = self._approved_worker(987654, "Broker-Rotation")
        replacement_key = ec.generate_private_key(ec.SECP256R1())
        replacement_public_key_pem = replacement_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")

        challenge = self.client.post(
            "/api/workers/certificates/rotation-challenge",
            json={"worker_id": worker_id, "public_key_pem": replacement_public_key_pem},
        )
        self.assertEqual(200, challenge.status_code)
        payload = worker_rotation_payload(
            worker_id=worker_id, replacement_public_key_pem=replacement_public_key_pem, nonce=challenge.json()["nonce"]
        )
        rejected = self.client.post(
            "/api/workers/certificates/rotate",
            json={
                "worker_id": worker_id,
                "public_key_pem": replacement_public_key_pem,
                "old_key_signature": base64.b64encode(old_key.sign(payload, ec.ECDSA(hashes.SHA256()))).decode("ascii"),
                "replacement_key_signature": "invalid",
            },
        )
        self.assertEqual(409, rejected.status_code)

        challenge = self.client.post(
            "/api/workers/certificates/rotation-challenge",
            json={"worker_id": worker_id, "public_key_pem": replacement_public_key_pem},
        ).json()
        payload = worker_rotation_payload(
            worker_id=worker_id, replacement_public_key_pem=replacement_public_key_pem, nonce=challenge["nonce"]
        )
        rotated = self.client.post(
            "/api/workers/certificates/rotate",
            json={
                "worker_id": worker_id,
                "public_key_pem": replacement_public_key_pem,
                "old_key_signature": base64.b64encode(old_key.sign(payload, ec.ECDSA(hashes.SHA256()))).decode("ascii"),
                "replacement_key_signature": base64.b64encode(
                    replacement_key.sign(payload, ec.ECDSA(hashes.SHA256()))
                ).decode("ascii"),
            },
        )
        self.assertEqual(200, rotated.status_code)
        self.assertEqual(replacement_public_key_pem, self.app.state.ledger.active_worker(worker_id).public_key_pem)
        claims = json.loads(base64.b64decode(json.loads(rotated.json()["certificate"])["payload"]))
        self.assertEqual(30, (datetime.fromisoformat(claims["expires_at"]) - datetime.fromisoformat(claims["issued_at"])).days)

        with self.client.websocket_connect("/api/worker/session") as socket:
            socket.send_json({"worker_id": worker_id, "certificate": old_certificate})
            overlap_challenge = socket.receive_json()
            proof = old_key.sign(
                worker_proof_payload(
                    purpose=overlap_challenge["purpose"], worker_id=worker_id, nonce=overlap_challenge["nonce"]
                ),
                ec.ECDSA(hashes.SHA256()),
            )
            socket.send_json({"signature": base64.b64encode(proof).decode("ascii")})
            self.assertEqual("authenticated", socket.receive_json()["type"])
            with self.app.state.ledger._transaction():
                self.app.state.ledger._connection.execute(
                    "UPDATE certificate_overlaps SET expires_at = ? WHERE role = 'worker' AND identity_id = ?",
                    [datetime.now(UTC) - timedelta(seconds=1), worker_id],
                )
            socket.send_json({"type": "heartbeat"})
            with self.assertRaises(WebSocketDisconnect):
                socket.receive_json()

        with self.app.state.ledger._transaction():
            self.app.state.ledger._connection.execute(
                "UPDATE certificate_overlaps SET expires_at = ? WHERE role = 'worker' AND identity_id = ?",
                [datetime.now(UTC) - timedelta(seconds=1), worker_id],
            )
        with self.client.websocket_connect("/api/worker/session") as socket:
            socket.send_json({"worker_id": worker_id, "certificate": old_certificate})
            with self.assertRaises(WebSocketDisconnect):
                socket.receive_json()
        self.assertIn(
            "worker_certificate_rotated",
            [event["event_type"] for event in self.app.state.ledger.events()],
        )
        login = self.client.post(
            "/api/admin/login",
            json={"username": "ABCDEF", "password": "A-secure-admin-password!"},
        )
        self.assertEqual(200, login.status_code)
        events = self.client.get("/api/admin/events")
        self.assertEqual(200, events.status_code)
        self.assertIn("worker_certificate_rotated", [event["event_type"] for event in events.json()["items"]])

    def test_approved_trader_can_authenticate_to_the_command_websocket(self) -> None:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        invite = self.app.state.ledger.create_registration_invite("ABCDEF", "trader")
        enrollment_payload = json.dumps(
            {"claimed_public_ip": "203.0.113.4", "registration_invite": invite, "strategy_name": "mean-reversion"},
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        enrollment = self.client.post(
            "/api/traders/enrollments",
            json={
                "registration_invite": invite,
                "strategy_name": "mean-reversion",
                "claimed_public_ip": "203.0.113.4",
                "public_key_pem": public_key_pem,
                "proof_signature": base64.b64encode(
                    private_key.sign(enrollment_payload, ec.ECDSA(hashes.SHA256()))
                ).decode("ascii"),
            },
        )
        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        approval = self.client.post(
            f"/api/admin/traders/enrollments/{enrollment.json()['registration_id']}/approve",
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
        ).json()

        with self.client.websocket_connect("/api/traders/session") as websocket:
            websocket.send_json({"trader_id": approval["trader_id"], "certificate": approval["certificate"]})
            challenge = websocket.receive_json()
            proof = private_key.sign(
                trader_proof_payload(
                    purpose=challenge["purpose"], trader_id=challenge["trader_id"], nonce=challenge["nonce"]
                ),
                ec.ECDSA(hashes.SHA256()),
            )
            websocket.send_json({"signature": base64.b64encode(proof).decode("ascii")})
            authenticated = websocket.receive_json()
            websocket.send_json(
                {"type": "command", "command_id": "intent-001", "payload": {"type": "intent", "pair_id": "pair-1"}}
            )
            result = websocket.receive_json()
            event = websocket.receive_json()
            websocket.send_json({"type": "ack", "cursor": event["event_id"]})
            self.assertEqual({"type": "acknowledged", "cursor": event["event_id"]}, websocket.receive_json())
            websocket.send_json(
                {"type": "command", "command_id": "intent-002", "payload": {"type": "intent", "pair_id": "pair-2"}}
            )
            websocket.receive_json()
            unacknowledged_event = websocket.receive_json()

        with self.client.websocket_connect("/api/traders/session") as reconnected:
            reconnected.send_json({"trader_id": approval["trader_id"], "certificate": approval["certificate"]})
            challenge = reconnected.receive_json()
            proof = private_key.sign(
                trader_proof_payload(
                    purpose=challenge["purpose"], trader_id=challenge["trader_id"], nonce=challenge["nonce"]
                ),
                ec.ECDSA(hashes.SHA256()),
            )
            reconnected.send_json({"signature": base64.b64encode(proof).decode("ascii")})
            resumed = reconnected.receive_json()
            automatically_replayed = reconnected.receive_json()

        self.assertEqual({"type": "authenticated", "trader_id": approval["trader_id"], "cursor": 0}, authenticated)
        self.assertEqual("rejected_preflight", result["status"])
        self.assertEqual(result["event_id"], event["event_id"])
        self.assertEqual(event["event_id"], resumed["cursor"])
        self.assertEqual(unacknowledged_event["event_id"], automatically_replayed["event_id"])

    def test_worker_and_trader_reject_invalid_registration_invites(self) -> None:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        worker_invite = self.app.state.ledger.create_registration_invite("ABCDEF", "worker")
        trader_payload = json.dumps(
            {"claimed_public_ip": "203.0.113.4", "registration_invite": worker_invite, "strategy_name": "strategy"},
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        trader_signature = base64.b64encode(private_key.sign(trader_payload, ec.ECDSA(hashes.SHA256()))).decode("ascii")
        self.assertEqual(
            409,
            self.client.post(
                "/api/traders/enrollments",
                json={
                    "registration_invite": worker_invite,
                    "strategy_name": "strategy",
                    "claimed_public_ip": "203.0.113.4",
                    "public_key_pem": public_key_pem,
                    "proof_signature": trader_signature,
                },
            ).status_code,
        )

        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        self.assertEqual([], self.client.get("/api/admin/traders/enrollments").json())
        self.assertEqual(
            422,
            self.client.post("/api/traders/enrollments", json={}).status_code,
        )
        used_invite = self.app.state.ledger.create_registration_invite("ABCDEF", "trader")
        used_payload = json.dumps(
            {"claimed_public_ip": "203.0.113.5", "registration_invite": used_invite, "strategy_name": "other"},
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        used_signature = base64.b64encode(private_key.sign(used_payload, ec.ECDSA(hashes.SHA256()))).decode("ascii")
        request = {
            "registration_invite": used_invite,
            "strategy_name": "other",
            "claimed_public_ip": "203.0.113.5",
            "public_key_pem": public_key_pem,
            "proof_signature": used_signature,
        }
        self.assertEqual(201, self.client.post("/api/traders/enrollments", json=request).status_code)
        self.assertEqual(409, self.client.post("/api/traders/enrollments", json=request).status_code)
        self.assertEqual(1, len(self.client.get("/api/admin/traders/enrollments").json()))
        self.app.state.ledger.revoke_registration_invite(worker_invite, "ABCDEF")
        account_info = {"login": 123456, "server": "Broker-Demo"}
        terminal_info = {"name": "MetaTrader 5"}
        challenge = self._enrollment_challenge()
        worker_signature = base64.b64encode(
            private_key.sign(
                enrollment_payload(
                    123456, "Broker-Demo", account_info, terminal_info, "password", challenge
                ),
                ec.ECDSA(hashes.SHA256()),
            )
        ).decode("ascii")
        self.assertEqual(
            409,
            self.client.post(
                "/api/enrollments",
                json={
                    "registration_invite": worker_invite,
                    "login": 123456,
                    "server": "Broker-Demo",
                    "account_info": account_info,
                    "terminal_info": terminal_info,
                    "mt5_password": "password",
                    "enrollment_challenge": challenge,
                    "public_key_pem": public_key_pem,
                    "proof_signature": worker_signature,
                },
            ).status_code,
        )

    def test_admin_session_resumes_with_a_rotated_csrf_token(self) -> None:
        login_response = self.client.post(
            "/api/admin/login",
            json={"username": "ABCDEF", "password": "A-secure-admin-password!"},
        )
        original_csrf = login_response.json()["csrf_token"]

        resume_response = self.client.get("/api/admin/session")

        self.assertEqual(200, resume_response.status_code)
        resumed_csrf = resume_response.json()["csrf_token"]
        self.assertNotEqual(original_csrf, resumed_csrf)
        self.assertEqual(
            204,
            self.client.post("/api/admin/logout", headers={"X-CSRF-Token": resumed_csrf}).status_code,
        )

    def test_admin_notification_websocket_rejects_unauthenticated_clients(self) -> None:
        with self.assertRaises(WebSocketDisconnect) as error:
            with self.http_client.websocket_connect("/api/admin/notifications"):
                pass
        self.assertEqual(1008, error.exception.code)

    def test_admin_notification_websocket_sends_pending_enrollment_snapshot(self) -> None:
        enrollment_id = self._create_pending_enrollment()
        self.client.post(
            "/api/admin/login",
            json={"username": "ABCDEF", "password": "A-secure-admin-password!"},
        )

        with self.client.websocket_connect("/api/admin/notifications", headers=self._admin_websocket_headers()) as websocket:
            notification = websocket.receive_json()

        self.assertEqual("pending_enrollments", notification["type"])
        self.assertEqual([enrollment_id], [item["enrollment_id"] for item in notification["items"]])
        self.assertNotIn("pairing_code", notification["items"][0])

    def test_admin_notification_websocket_broadcasts_new_pending_enrollment(self) -> None:
        self.client.post(
            "/api/admin/login",
            json={"username": "ABCDEF", "password": "A-secure-admin-password!"},
        )

        with self.client.websocket_connect("/api/admin/notifications", headers=self._admin_websocket_headers()) as first:
            self.assertEqual({"type": "pending_enrollments", "items": []}, first.receive_json())
            with self.client.websocket_connect("/api/admin/notifications", headers=self._admin_websocket_headers()) as second:
                self.assertEqual({"type": "pending_enrollments", "items": []}, second.receive_json())
                enrollment_id = self._create_pending_enrollment()
                notifications = [first.receive_json(), second.receive_json()]

        self.assertEqual(["pending_enrollment", "pending_enrollment"], [item["type"] for item in notifications])
        self.assertEqual([enrollment_id, enrollment_id], [item["item"]["enrollment_id"] for item in notifications])
        self.assertNotIn("pairing_code", notifications[0]["item"])

    def test_approval_logs_a_sanitized_openbao_signing_failure(self) -> None:
        enrollment_id = self._create_pending_enrollment()
        login = self.client.post(
            "/api/admin/login",
            json={"username": "ABCDEF", "password": "A-secure-admin-password!"},
        )
        self.certificate_issuer.fail_next_issue = True

        with self.assertLogs("abt.controlplane.service", level="WARNING") as logs:
            response = self.client.post(
                f"/api/admin/enrollments/{enrollment_id}/approve",
                headers={"X-CSRF-Token": login.json()["csrf_token"]},
            )

        self.assertEqual(409, response.status_code)
        self.assertEqual("OpenBao device certificate signing failed (HTTP 403).", response.json()["detail"])
        self.assertIn(enrollment_id, "\n".join(logs.output))

    def test_login_does_not_apply_an_ip_lock_to_proxied_requests(self) -> None:
        for source_ip in ("198.51.100.1", "198.51.100.2"):
            response = self.client.post(
                "/api/admin/login",
                headers={"CF-Connecting-IP": source_ip},
                json={"username": "ABCDEF", "password": "wrong-password-is-long-enough"},
            )
            self.assertEqual(401, response.status_code)
        response = self.client.post(
            "/api/admin/login",
            headers={"CF-Connecting-IP": "198.51.100.3"},
            json={"username": "ABCDEF", "password": "wrong-password-is-long-enough"},
        )
        self.assertEqual(401, response.status_code)

    def test_controller_can_serve_the_same_origin_spa(self) -> None:
        web_directory = Path(self._directory.name) / "web"
        web_directory.mkdir()
        (web_directory / "index.html").write_text("<h1>Management access</h1>", encoding="utf-8")
        client = TestClient(
            create_app(Path(self._directory.name) / "spa-ledger.duckdb", spa_directory=web_directory),
            base_url="https://testserver",
        )
        try:
            for path in ("/", "/manual-trading"):
                response = client.get(path)
                self.assertEqual(200, response.status_code)
                self.assertIn("Management access", response.text)
        finally:
            client.close()

    def test_health_requires_the_secret_control_plane(self) -> None:
        healthy_client = TestClient(
            create_app(Path(self._directory.name) / "healthy.duckdb", secret_control_healthy=lambda: True)
        )
        unhealthy_client = TestClient(
            create_app(Path(self._directory.name) / "unhealthy.duckdb", secret_control_healthy=lambda: False)
        )
        try:
            self.assertEqual({"status": "ok"}, healthy_client.get("/health").json())
            self.assertEqual(503, unhealthy_client.get("/health").status_code)
        finally:
            healthy_client.close()
            unhealthy_client.close()

    def test_worker_receives_certificate_then_its_own_password_over_proved_websockets(self) -> None:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        account_info = {"login": 123456, "server": "Broker-Demo"}
        terminal_info = {"name": "MetaTrader 5"}
        password = "worker-memory-only-password"
        challenge = self._enrollment_challenge()
        enrollment_signature = private_key.sign(
            enrollment_payload(123456, "Broker-Demo", account_info, terminal_info, password, challenge),
            ec.ECDSA(hashes.SHA256()),
        )
        enrollment = self.client.post(
            "/api/enrollments",
            json={
                "registration_invite": self.app.state.ledger.create_registration_invite("ABCDEF", "worker"),
                "login": 123456,
                "server": "Broker-Demo",
                "account_info": account_info,
                "terminal_info": terminal_info,
                "mt5_password": password,
                "enrollment_challenge": challenge,
                "public_key_pem": public_key_pem,
                "proof_signature": base64.b64encode(enrollment_signature).decode("ascii"),
            },
        )
        login = self.client.post(
            "/api/admin/login",
            json={"username": "ABCDEF", "password": "A-secure-admin-password!"},
        )
        approval = self.client.post(
            f"/api/admin/enrollments/{enrollment.json()['enrollment_id']}/approve",
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
        )
        worker_id = approval.json()["worker_id"]

        with self.client.websocket_connect("/api/worker/certificate") as websocket:
            websocket.send_json({"enrollment_id": enrollment.json()["enrollment_id"]})
            challenge = websocket.receive_json()
            self.assertEqual("certificate_delivery", challenge["purpose"])
            proof = private_key.sign(
                worker_proof_payload(
                    purpose=challenge["purpose"], worker_id=challenge["worker_id"], nonce=challenge["nonce"]
                ),
                ec.ECDSA(hashes.SHA256()),
            )
            websocket.send_json({"signature": base64.b64encode(proof).decode("ascii")})
            delivered = websocket.receive_json()
        self.assertEqual(worker_id, delivered["worker_id"])

        with self.client.websocket_connect("/api/worker/credentials") as websocket:
            websocket.send_json({"worker_id": worker_id, "certificate": delivered["certificate"]})
            challenge = websocket.receive_json()
            self.assertEqual("password_request", challenge["purpose"])
            proof = private_key.sign(
                worker_proof_payload(
                    purpose=challenge["purpose"], worker_id=challenge["worker_id"], nonce=challenge["nonce"]
                ),
                ec.ECDSA(hashes.SHA256()),
            )
            websocket.send_json({"signature": base64.b64encode(proof).decode("ascii")})
            self.assertEqual({"password": "worker-memory-only-password"}, websocket.receive_json())

        tampered_certificate = delivered["certificate"][:-1] + (
            "A" if delivered["certificate"][-1] != "A" else "B"
        )
        with self.client.websocket_connect("/api/worker/credentials") as websocket:
            websocket.send_json({"worker_id": worker_id, "certificate": tampered_certificate})
            with self.assertRaises(Exception) as error:
                websocket.receive_json()
        self.assertEqual(1008, getattr(error.exception, "code", None))

    def test_worker_password_request_rejects_an_invalid_certificate(self) -> None:
        with self.client.websocket_connect("/api/worker/credentials") as websocket:
            websocket.send_json({"worker_id": "not-a-worker", "certificate": "not-a-certificate"})
            with self.assertRaises(Exception) as error:
                websocket.receive_json()
        self.assertEqual(1008, getattr(error.exception, "code", None))

    def test_authenticated_worker_session_returns_only_its_bound_password(self) -> None:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        enrollment = self._enrollment_response(
            private_key, public_key_pem, {"login": 123456, "server": "Broker-Demo"}, {"name": "MetaTrader 5"}
        )
        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        worker_id = self.client.post(
            f"/api/admin/enrollments/{enrollment.json()['enrollment_id']}/approve",
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
        ).json()["worker_id"]

        with self.client.websocket_connect("/api/worker/session") as websocket:
            certificate = self.app.state.ledger.active_worker(worker_id).certificate
            websocket.send_json({"worker_id": worker_id, "certificate": certificate})
            challenge = websocket.receive_json()
            proof = private_key.sign(
                worker_proof_payload(
                    purpose="worker_session", worker_id=worker_id, nonce=challenge["nonce"]
                ),
                ec.ECDSA(hashes.SHA256()),
            )
            websocket.send_json({"signature": base64.b64encode(proof).decode("ascii")})
            self.assertEqual(
                {"type": "authenticated", "worker_id": worker_id, "cursor": 0}, websocket.receive_json()
            )
            websocket.send_json({"type": "password_request"})
            self.assertEqual(
                {"type": "password", "password": "worker-memory-only-password"},
                websocket.receive_json(),
            )
        self.assertEqual("connected", self.client.get("/api/admin/workers").json()[0]["connectivity"])

    def test_management_api_shows_authenticated_worker_health_snapshots_and_deltas(self) -> None:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        enrollment = self._enrollment_response(
            private_key, public_key_pem, {"login": 123456, "server": "Broker-Demo"}, {"name": "MetaTrader 5"}
        )
        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        worker_id = self.client.post(
            f"/api/admin/enrollments/{enrollment.json()['enrollment_id']}/approve",
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
        ).json()["worker_id"]
        certificate = self.app.state.ledger.active_worker(worker_id).certificate

        with self.client.websocket_connect("/api/worker/session") as websocket:
            websocket.send_json({"worker_id": worker_id, "certificate": certificate})
            challenge = websocket.receive_json()
            signature = private_key.sign(
                worker_proof_payload(purpose="worker_session", worker_id=worker_id, nonce=challenge["nonce"]),
                ec.ECDSA(hashes.SHA256()),
            )
            websocket.send_json({"signature": base64.b64encode(signature).decode("ascii")})
            websocket.receive_json()
            websocket.send_json({"type": "snapshot", "cursor": 0, "observed_at": "2026-08-16T00:00:00+00:00",
                                 "account": {"balance": 1000}, "terminal": {"connected": True},
                                 "orders": [], "positions": []})
            self.assertEqual({"type": "accepted", "cursor": 0}, websocket.receive_json())
            websocket.send_json(
                {
                    "type": "live_state_snapshot",
                    "observed_at": "2026-08-16T00:00:00+00:00",
                    "connectivity": True,
                    "quotes": [
                        {
                            "symbol": "EURUSD",
                            "bid": 1.0821,
                            "ask": 1.0823,
                            "broker_time": "2026-08-16T00:00:00+00:00",
                        }
                    ],
                    "orders": [{"ticket": 41}],
                    "positions": [{"ticket": 51}],
                }
            )
            self.assertEqual({"type": "live_state_accepted"}, websocket.receive_json())
            websocket.send_json(
                {
                    "type": "live_state_diff",
                    "observed_at": "2026-08-16T00:00:05+00:00",
                    "entity": "connectivity",
                    "value": False,
                }
            )
            self.assertEqual({"type": "live_state_accepted"}, websocket.receive_json())
            first_snapshot_id = self.client.get("/api/admin/workers").json()[0]["latest_snapshot"]["snapshot_id"]
            websocket.send_json({"type": "snapshot", "cursor": 0, "observed_at": "2026-08-16T00:10:00+00:00",
                                 "account": {"balance": 1000}, "terminal": {"connected": True},
                                 "orders": [], "positions": []})
            self.assertEqual({"type": "accepted", "cursor": 0}, websocket.receive_json())
            second_snapshot_id = self.client.get("/api/admin/workers").json()[0]["latest_snapshot"]["snapshot_id"]
            self.assertNotEqual(first_snapshot_id, second_snapshot_id)
            websocket.send_json({"type": "delta", "cursor": 1, "observed_at": "2026-08-16T00:01:00+00:00",
                                 "entity": "position", "ticket": "51", "change": "volume_changed",
                                 "record": {"ticket": 51, "volume": 1.0}})
            self.assertEqual({"type": "accepted", "cursor": 1}, websocket.receive_json())

        with self.client.websocket_connect("/api/worker/session") as websocket:
            websocket.send_json({"worker_id": worker_id, "certificate": certificate})
            challenge = websocket.receive_json()
            signature = private_key.sign(
                worker_proof_payload(purpose="worker_session", worker_id=worker_id, nonce=challenge["nonce"]),
                ec.ECDSA(hashes.SHA256()),
            )
            websocket.send_json({"signature": base64.b64encode(signature).decode("ascii")})
            self.assertEqual({"type": "authenticated", "worker_id": worker_id, "cursor": 1}, websocket.receive_json())
            websocket.send_json({"type": "snapshot", "cursor": 1, "observed_at": "2026-08-16T00:20:00+00:00",
                                 "account": {"balance": 1000}, "terminal": {"connected": True},
                                 "orders": [], "positions": []})
            self.assertEqual({"type": "accepted", "cursor": 1}, websocket.receive_json())
            websocket.send_json({"type": "delta", "cursor": 2, "observed_at": "2026-08-16T00:21:00+00:00",
                                 "entity": "position", "ticket": "51", "change": "modified",
                                 "record": {"ticket": 51, "volume": 1.0, "tp": 1.5}})
            self.assertEqual({"type": "accepted", "cursor": 2}, websocket.receive_json())
            websocket.send_json(
                {
                    "type": "safety_state",
                    "state": "frozen",
                    "reason": "Broker response could not be verified.",
                }
            )
            self.assertEqual({"type": "accepted", "state": "frozen"}, websocket.receive_json())

        workers = self.client.get("/api/admin/workers").json()
        self.assertEqual("connected", workers[0]["connectivity"])
        self.assertEqual("frozen", workers[0]["safety_state"])
        self.assertEqual("external_broker_change", workers[0]["freeze"]["source"])
        self.assertEqual([worker_id], workers[0]["freeze"]["affected_worker_ids"])
        self.assertEqual(
            {
                "cursor": 1,
                "entity": "position",
                "reason": "Unattributed external broker change requires account isolation.",
                "ticket": "51",
            },
            workers[0]["freeze"]["audit"],
        )
        self.assertEqual({"balance": 1000}, workers[0]["latest_snapshot"]["account"])
        self.assertEqual(["volume_changed", "modified"], [delta["change"] for delta in workers[0]["deltas"]])
        live_state = workers[0]["live_state"]
        self.assertIsNotNone(live_state.pop("received_at"))
        self.assertIsNotNone(live_state["quotes"][0].pop("controller_received_at"))
        self.assertEqual(
            {
                "connectivity": False,
                "quotes": [
                    {
                        "symbol": "EURUSD",
                        "bid": 1.0821,
                        "ask": 1.0823,
                        "broker_time": "2026-08-16T00:00:00+00:00",
                    }
                ],
                "orders": [{"ticket": 41}],
                "positions": [{"ticket": 51}],
                "observed_at": "2026-08-16T00:00:05+00:00",
            },
            live_state,
        )

    def test_admin_can_configure_and_observe_a_persistent_manual_trading_target(self) -> None:
        ledger = self.app.state.ledger
        _first_key, first_worker_id, _first_certificate = self._approved_worker(123456, "Broker-A")
        _second_key, second_worker_id, _second_certificate = self._approved_worker(654321, "Broker-B")
        with ledger._transaction():
            ledger._connection.execute(
                """INSERT INTO product_pairs (
                       product_pair_id, status, endpoint_a_server, endpoint_a_symbol, endpoint_b_server, endpoint_b_symbol,
                       active_pair_key, lot_relationship, policy_snapshot, analysis_period, reference_specifications,
                       approval_evidence, source_workers, built_from_analysis_id, built_from_confirmation_id, built_by, created_at
                   ) VALUES (?, 'active', 'Broker-A', 'EURUSD', 'Broker-B', 'EURUSD.a', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    "pair-1", "Broker-A:EURUSD|Broker-B:EURUSD.a", json.dumps({"first_to_second": "1"}),
                    json.dumps({}), json.dumps({}), json.dumps([]), json.dumps({}),
                    json.dumps({}), "analysis-1", "confirmation-1", "ABCDEF", datetime.now(UTC),
                ],
            )
        for worker_id in (first_worker_id, second_worker_id):
            ledger.record_worker_session(worker_id)
        ledger.record_live_state(
            first_worker_id, "2026-08-22T00:00:00+00:00", True,
            [{"symbol": "EURUSD", "bid": 1.1000, "ask": 1.1002, "broker_time": "2026-08-22T00:00:00+00:00"}],
            [{"ticket": 101, "symbol": "EURUSD"}], [],
        )
        ledger.record_live_state(
            second_worker_id, "2026-08-22T00:00:00+00:00", True,
            [{"symbol": "EURUSD.a", "bid": 1.0998, "ask": 1.1000, "broker_time": "2026-08-22T00:00:00+00:00"}],
            [], [{"ticket": 202, "symbol": "EURUSD.a"}],
        )
        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )

        configured = self.client.put(
            "/api/admin/manual-trading-target",
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
            json={
                "pair_id": "pair-1",
                "buy_worker_id": first_worker_id,
                "sell_worker_id": second_worker_id,
                "leg_order": "buy_to_sell",
                "interval_seconds": 7,
                "expected_revision": 0,
            },
        )

        self.assertEqual(200, configured.status_code)
        target = configured.json()
        self.assertEqual("pair-1", target["pair"]["product_pair_id"])
        self.assertEqual([first_worker_id, second_worker_id], [worker["worker_id"] for worker in target["workers"]])
        self.assertEqual(first_worker_id, target["buy_worker_id"])
        self.assertEqual(second_worker_id, target["sell_worker_id"])
        self.assertEqual("buy_to_sell", target["leg_order"])
        self.assertEqual(7, target["interval_seconds"])
        self.assertEqual("EURUSD", target["workers"][0]["live_state"]["quotes"][0]["symbol"])
        self.assertEqual(target, self.client.get("/api/admin/manual-trading-target").json())
        ledger.freeze_workers("execution_anomaly", [first_worker_id], {"reason": "broker timeout"})
        rejected = self.client.put(
            "/api/admin/manual-trading-target",
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
            json={
                "pair_id": "pair-1",
                "buy_worker_id": first_worker_id,
                "sell_worker_id": second_worker_id,
                "leg_order": "sell_to_buy",
                "interval_seconds": 0,
                "expected_revision": 1,
            },
        )
        self.assertEqual(409, rejected.status_code)
        self.assertEqual("Frozen workers cannot be selected for manual trading.", rejected.json()["detail"])
        ledger.retire_product_pair("pair-1", "ABCDEF")
        self.assertIsNone(self.client.get("/api/admin/manual-trading-target").json())

    def test_admin_can_preview_and_submit_an_idempotent_protected_manual_trade(self) -> None:
        ledger = self.app.state.ledger
        _first_key, first_worker_id, _first_certificate = self._approved_worker(123456, "Broker-A")
        _second_key, second_worker_id, _second_certificate = self._approved_worker(654321, "Broker-B")
        specification = {
            "allowed_directions": ["LONG", "SHORT"],
            "volume_min": "0.1",
            "volume_max": "100",
            "volume_step": "0.1",
            "point": "0.00001",
            "digits": 5,
        }
        with ledger._transaction():
            ledger._connection.execute(
                """INSERT INTO product_pairs (
                       product_pair_id, status, endpoint_a_server, endpoint_a_symbol, endpoint_b_server, endpoint_b_symbol,
                       active_pair_key, lot_relationship, policy_snapshot, analysis_period, reference_specifications,
                       approval_evidence, source_workers, built_from_analysis_id, built_from_confirmation_id, built_by, created_at
                   ) VALUES (?, 'active', 'Broker-A', 'EURUSD', 'Broker-B', 'EURUSD.a', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    "pair-manual", "Broker-A:EURUSD|Broker-B:EURUSD.a",
                    json.dumps({"first_lots": "1", "second_lots": "2"}), json.dumps({}), json.dumps({}),
                    json.dumps([
                        {"server": "Broker-A", "symbol": "EURUSD", "specification": specification},
                        {"server": "Broker-B", "symbol": "EURUSD.a", "specification": specification},
                    ]),
                    json.dumps({}), json.dumps({}), "analysis-1", "confirmation-1", "ABCDEF", datetime.now(UTC),
                ],
            )
        for worker_id, symbol, bid, ask in (
            (first_worker_id, "EURUSD", 1.1000, 1.1002),
            (second_worker_id, "EURUSD.a", 1.0998, 1.1000),
        ):
            ledger.record_worker_session(worker_id)
            ledger.record_live_state(
                worker_id, "2026-08-22T00:00:00+00:00", True,
                [{"symbol": symbol, "bid": bid, "ask": ask, "broker_time": "2026-08-22T00:00:00+00:00"}], [], [],
            )
        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        csrf = {"X-CSRF-Token": login.json()["csrf_token"]}
        configured = self.client.put(
            "/api/admin/manual-trading-target",
            headers=csrf,
            json={
                "pair_id": "pair-manual",
                "buy_worker_id": first_worker_id,
                "sell_worker_id": second_worker_id,
                "leg_order": "buy_to_sell",
                "interval_seconds": 0,
                "expected_revision": 0,
            },
        )
        self.assertEqual(200, configured.status_code)
        active = ledger.request_manual_trade(
            "ABCDEF",
            "manual-entry-active",
            {
                "target_revision": 1,
                "base_lots": "0.1",
                "stop_loss_pips": "10",
                "take_profit_pips": "20",
            },
        )
        ledger.activate_manual_trade(
            active["manual_trade_id"],
            [
                {
                    "worker_id": first_worker_id,
                    "market_order_ticket": "91",
                    "position_ticket": "101",
                    "fill_price": "1.1002",
                },
                {
                    "worker_id": second_worker_id,
                    "market_order_ticket": "92",
                    "position_ticket": "202",
                    "fill_price": "1.0998",
                },
            ],
        )
        updated_target = self.client.put(
            "/api/admin/manual-trading-target",
            headers=csrf,
            json={
                "pair_id": "pair-manual",
                "buy_worker_id": second_worker_id,
                "sell_worker_id": first_worker_id,
                "leg_order": "sell_to_buy",
                "interval_seconds": 0,
                "expected_revision": 1,
            },
        )
        self.assertEqual(200, updated_target.status_code)
        command = {
            "command_id": "manual-entry-1",
            "target_revision": 2,
            "base_lots": "0.1",
            "stop_loss_pips": "10",
            "take_profit_pips": "20",
        }
        preview = self.client.post("/api/admin/manual-trades/preview", headers=csrf, json=command)
        self.assertEqual(200, preview.status_code)
        self.assertEqual(["0.1", "0.2"], [leg["lots"] for leg in preview.json()["legs"]])
        self.assertEqual(["SELL", "BUY"], [leg["direction"] for leg in preview.json()["legs"]])
        submitted = self.client.post("/api/admin/manual-trades", headers=csrf, json=command)
        self.assertEqual(201, submitted.status_code)
        self.assertEqual("scheduled", submitted.json()["status"])
        self.assertEqual(submitted.json(), self.client.post("/api/admin/manual-trades", headers=csrf, json=command).json())
        self.assertEqual(
            {active["manual_trade_id"], submitted.json()["manual_trade_id"]},
            {trade["manual_trade_id"] for trade in self.client.get("/api/admin/manual-trades").json()},
        )

    def test_admin_can_discard_unconfirmed_failed_manual_trade(self) -> None:
        ledger = self.app.state.ledger
        with ledger._transaction():
            ledger._connection.execute(
                """INSERT INTO manual_trades
                   (manual_trade_id, username, command_id, target_revision, pair_id, plan, status, created_at)
                   VALUES ('manual-failed', 'ABCDEF', 'entry-failed', 1, 'pair-1', ?, 'needs_human', ?)""",
                [
                    json.dumps({
                        "legs": [],
                        "active_legs": [{"worker_id": "worker-a"}, {"worker_id": "worker-b"}],
                    }),
                    datetime.now(UTC),
                ],
            )
        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        response = self.client.delete(
            "/api/admin/manual-trades/manual-failed",
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual({"manual_trade_id": "manual-failed", "status": "discarded"}, response.json())
        self.assertEqual([], self.client.get("/api/admin/manual-trades").json())

    def test_admin_cannot_discard_manual_trade_with_broker_evidence(self) -> None:
        ledger = self.app.state.ledger
        with ledger._transaction():
            ledger._connection.execute(
                """INSERT INTO manual_trades
                   (manual_trade_id, username, command_id, target_revision, pair_id, plan, status, created_at)
                   VALUES ('manual-evidenced', 'ABCDEF', 'entry-evidenced', 1, 'pair-1', ?, 'needs_human', ?)""",
                [json.dumps({"legs": [], "active_legs": [{"position_ticket": "123"}]}), datetime.now(UTC)],
            )
        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        response = self.client.delete(
            "/api/admin/manual-trades/manual-evidenced",
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
        )

        self.assertEqual(409, response.status_code)
        self.assertEqual("Manual trade has broker execution evidence and cannot be discarded.", response.json()["detail"])

    def test_admin_can_preview_and_submit_idempotent_active_manual_trade_operations(self) -> None:
        ledger = self.app.state.ledger
        _first_key, first_worker_id, _first_certificate = self._approved_worker(123456, "Broker-A")
        _second_key, second_worker_id, _second_certificate = self._approved_worker(654321, "Broker-B")
        plan = {
            "pair_id": "pair-manual",
            "target_revision": 1,
            "legs": [
                {
                    "worker_id": first_worker_id, "symbol": "EURUSD", "direction": "BUY", "lots": "0.1",
                    "pip_size": "0.0001", "stop_loss_pips": "10", "take_profit_pips": "20",
                },
                {
                    "worker_id": second_worker_id, "symbol": "EURUSD.a", "direction": "SELL", "lots": "0.2",
                    "pip_size": "0.0001", "stop_loss_pips": "10", "take_profit_pips": "20",
                },
            ],
            "active_legs": [
                {
                    "worker_id": first_worker_id,
                    "market_order_ticket": "91",
                    "position_ticket": "101",
                    "fill_price": "1.1002",
                },
                {
                    "worker_id": second_worker_id,
                    "market_order_ticket": "92",
                    "position_ticket": "202",
                    "fill_price": "1.0998",
                },
            ],
        }
        with ledger._transaction():
            ledger._connection.execute(
                """INSERT INTO product_pairs (
                       product_pair_id, status, endpoint_a_server, endpoint_a_symbol, endpoint_b_server, endpoint_b_symbol,
                       active_pair_key, lot_relationship, policy_snapshot, analysis_period, reference_specifications,
                       approval_evidence, source_workers, built_from_analysis_id, built_from_confirmation_id, built_by, created_at
                   ) VALUES (?, 'active', 'Broker-A', 'EURUSD', 'Broker-B', 'EURUSD.a', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    "pair-manual", "Broker-A:EURUSD|Broker-B:EURUSD.a", json.dumps({}), json.dumps({}),
                    json.dumps({}), json.dumps([]), json.dumps({}), json.dumps({}), "analysis-1", "confirmation-1",
                    "ABCDEF", datetime.now(UTC),
                ],
            )
        for worker_id, symbol, position in (
            (first_worker_id, "EURUSD", 101),
            (second_worker_id, "EURUSD.a", 202),
        ):
            ledger.record_worker_session(worker_id)
            ledger.record_live_state(
                worker_id,
                "2026-08-22T00:00:00+00:00",
                True,
                [],
                [],
                [{"ticket": position, "symbol": symbol}],
            )
        with ledger._transaction():
            ledger._connection.execute(
                """INSERT INTO manual_trading_target
                   (singleton, pair_id, first_worker_id, second_worker_id, leg_order, interval_seconds,
                    revision, active_manual_trade_id, configured_by, configured_at)
                   VALUES (TRUE, 'pair-manual', ?, ?, 'buy_to_sell', 0, 1, 'manual-active', 'ABCDEF', ?)""",
                [first_worker_id, second_worker_id, datetime.now(UTC)],
            )
            ledger._connection.execute(
                """INSERT INTO manual_trades
                   (manual_trade_id, username, command_id, target_revision, pair_id, plan, status, created_at)
                   VALUES ('manual-active', 'ABCDEF', 'entry-1', 1, 'pair-manual', ?, 'active', ?)""",
                [json.dumps(plan), datetime.now(UTC)],
            )
        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        csrf = {"X-CSRF-Token": login.json()["csrf_token"]}

        protection_preview = self.client.post(
            "/api/admin/manual-trades/manual-active/protection/preview",
            headers=csrf,
            json={"command_id": "protection-1", "stop_loss_pips": "15", "take_profit_pips": "30"},
        )
        self.assertEqual(200, protection_preview.status_code)
        self.assertEqual(
            [("1.0987", "1.1032"), ("1.1013", "1.0968")],
            [(leg["stop_loss"], leg["take_profit"]) for leg in protection_preview.json()["legs"]],
        )
        protection = self.client.post(
            "/api/admin/manual-trades/manual-active/protection",
            headers=csrf,
            json={"command_id": "protection-1", "stop_loss_pips": "15", "take_profit_pips": "30"},
        )
        self.assertEqual(201, protection.status_code)
        self.assertEqual(
            protection.json(),
            self.client.post(
                "/api/admin/manual-trades/manual-active/protection",
                headers=csrf,
                json={"command_id": "protection-1", "stop_loss_pips": "15", "take_profit_pips": "30"},
            ).json(),
        )
        exit_preview = self.client.post(
            "/api/admin/manual-trades/manual-active/exit/preview", headers=csrf, json={"command_id": "exit-1"}
        )
        self.assertEqual(200, exit_preview.status_code)
        self.assertEqual(["101", "202"], [leg["position"] for leg in exit_preview.json()["legs"]])
        active_trade = self.client.get("/api/admin/manual-trades").json()[0]
        self.assertEqual(["91", "92"], [leg["market_order_ticket"] for leg in active_trade["legs"]])
        self.assertEqual(["101", "202"], [leg["position_ticket"] for leg in active_trade["legs"]])
        self.assertEqual(["open", "open"], [leg["position_status"] for leg in active_trade["legs"]])
        ledger.record_manual_trade_operation(
            protection.json()["operation_id"],
            "manual_trade_operation_frozen",
            {"reason": "A leg closed externally."},
            status="needs_human",
        )
        self.assertEqual("needs_human", self.client.get("/api/admin/manual-trades").json()[0]["status"])

    def test_admin_read_models_paginate_search_and_keep_snapshot_payloads_in_detail(self) -> None:
        _key, worker_id, _certificate = self._approved_worker(123456, "Broker-Search")
        ledger = self.app.state.ledger
        ledger.record_snapshot(
            worker_id, 0, "2026-08-16T00:00:00+00:00",
            {
                "login": 123456, "server": "Broker-Search", "balance": 1000, "equity": 1010,
                "trade_allowed": False, "trade_expert": True,
            },
            {"trade_allowed": False, "trade_expert": True, "tradeapi_disabled": False},
            [{"ticket": 1}], [],
        )
        ledger.record_snapshot(
            worker_id, 0, "2026-08-16T00:01:00+00:00",
            {
                "login": 123456, "server": "Broker-Search", "balance": 1001, "equity": 1011,
                "trade_allowed": True, "trade_expert": False,
            },
            {"trade_allowed": True, "trade_expert": True, "tradeapi_disabled": False},
            [{"ticket": 2}], [],
        )
        snapshots = self.client.get("/api/admin/worker-snapshots", params={"limit": 1, "q": "broker-search"})
        self.assertEqual(200, snapshots.status_code)
        first_page = snapshots.json()
        self.assertEqual(1, len(first_page["items"]))
        self.assertNotIn("account", first_page["items"][0])
        self.assertNotIn("orders", first_page["items"][0])
        self.assertTrue(first_page["items"][0]["trade_allowed"])
        self.assertFalse(first_page["items"][0]["trade_expert"])
        self.assertIsNotNone(first_page["next_cursor"])
        second_page = self.client.get(
            "/api/admin/worker-snapshots", params={"limit": 1, "cursor": first_page["next_cursor"]}
        ).json()
        self.assertEqual(1, len(second_page["items"]))
        numbered_snapshot_page = self.client.get(
            "/api/admin/worker-snapshots",
            params={"limit": 1, "page": 2, "q": "broker-search"},
        ).json()
        self.assertEqual(2, numbered_snapshot_page["total_items"])
        self.assertNotEqual(first_page["items"][0]["snapshot_id"], numbered_snapshot_page["items"][0]["snapshot_id"])
        detail = self.client.get(f"/api/admin/worker-snapshots/{first_page['items'][0]['snapshot_id']}")
        self.assertEqual([{"ticket": 2}], detail.json()["orders"])
        self.assertEqual(422, self.client.get("/api/admin/worker-snapshots", params={"cursor": "invalid"}).status_code)

        ledger._event("read_model_match", {"worker_id": worker_id, "marker": "searchable"})
        ledger._event("read_model_match", {"worker_id": worker_id, "marker": "newer"})
        events = self.client.get("/api/admin/events", params={"limit": 1, "event_type": "read_model_match", "q": "newer"})
        self.assertEqual(["read_model_match"], [item["event_type"] for item in events.json()["items"]])
        self.assertIsNone(events.json()["next_cursor"])
        numbered_events = self.client.get(
            "/api/admin/events",
            params={"limit": 1, "page": 1, "event_type": "read_model_match", "q": "newer"},
        ).json()
        self.assertEqual(1, numbered_events["total_items"])
        self.assertEqual(422, self.client.get("/api/admin/events", params={"limit": 51}).status_code)

        ledger._connection.execute(
            """
            INSERT INTO product_catalog_analyses (
                analysis_id, requested_by, first_worker_id, first_login, first_server, second_worker_id, second_login,
                second_server, policy, status, requested_at
            ) VALUES ('analysis-search', 'ABCDEF', 'worker-a', 1, 'Broker-Search', 'worker-b', 2,
                      'Broker-Other', '{"label":"Search catalog"}', 'succeeded', ?)
            """,
            [datetime(2026, 8, 19, tzinfo=UTC)],
        )
        analyses = self.client.get(
            "/api/admin/product-catalog-analyses", params={"status": "succeeded", "q": "search catalog"}
        ).json()
        self.assertEqual(["analysis-search"], [item["analysis_id"] for item in analyses["items"]])
        self.assertEqual(1, analyses["total_items"])
        date_analyses = self.client.get(
            "/api/admin/product-catalog-analyses", params={"status": "succeeded", "q": "20260819"}
        ).json()
        self.assertEqual(["analysis-search"], [item["analysis_id"] for item in date_analyses["items"]])
        self.assertEqual(1, date_analyses["total_items"])
        self.assertEqual(422, self.client.get("/api/admin/product-catalog-analyses", params={"limit": 0}).status_code)

        ledger._connection.executemany(
            """
            INSERT INTO product_catalog_analyses (
                analysis_id, requested_by, first_worker_id, first_login, first_server, second_worker_id, second_login,
                second_server, policy, status, requested_at
            ) VALUES (?, 'ABCDEF', 'worker-a', 1, 'Broker-Search', 'worker-b', 2,
                      'Broker-Other', '{"label":"Pageable catalog"}', 'succeeded', ?)
            """,
            [
                ("analysis-page-one", datetime(2026, 8, 20, tzinfo=UTC)),
                ("analysis-page-two", datetime(2026, 8, 21, tzinfo=UTC)),
            ],
        )
        first_analysis_page = self.client.get(
            "/api/admin/product-catalog-analyses",
            params={"limit": 1, "page": 1, "q": "pageable catalog"},
        ).json()
        second_analysis_page = self.client.get(
            "/api/admin/product-catalog-analyses",
            params={"limit": 1, "page": 2, "q": "pageable catalog"},
        ).json()
        self.assertEqual(2, first_analysis_page["total_items"])
        self.assertEqual(2, second_analysis_page["total_items"])
        self.assertNotEqual(first_analysis_page["items"][0]["analysis_id"], second_analysis_page["items"][0]["analysis_id"])
        self.assertEqual(
            422,
            self.client.get(
                "/api/admin/product-catalog-analyses",
                params={"cursor": "invalid", "page": 1},
            ).status_code,
        )

        ledger._connection.execute(
            """
            INSERT INTO product_pairs (
                product_pair_id, status, endpoint_a_server, endpoint_a_symbol, endpoint_b_server, endpoint_b_symbol,
                lot_relationship, policy_snapshot, analysis_period, reference_specifications, approval_evidence,
                source_workers, built_from_analysis_id, built_from_confirmation_id, built_by, created_at
            ) VALUES ('pair-search', 'active', 'Broker-Search', 'EURUSD', 'Broker-Other', 'EURUSD.a',
                      '{}', '{}', '{}', '[]', '{}', '[]', 'analysis-search', 'confirmation-search', 'ABCDEF', ?)
            """,
            [datetime.now(UTC)],
        )
        pairs = self.client.get("/api/admin/product-pairs", params={"status": "active", "q": "eurusd.a"}).json()
        self.assertEqual(["pair-search"], [item["product_pair_id"] for item in pairs["items"]])
        self.assertEqual(422, self.client.get("/api/admin/product-pairs", params={"status": "unknown"}).status_code)

    def test_operations_dashboard_requires_an_admin_and_classifies_current_operational_state(self) -> None:
        self.assertEqual(401, self.client.get("/api/admin/operations-dashboard").status_code)

        pending_enrollment_id = self._create_pending_enrollment()
        _private_key, worker_id, _certificate = self._approved_worker(654321, "Broker-Live")
        self.app.state.ledger.record_worker_safety_state(
            worker_id,
            "lost_link_safety",
            "controller_signal_lost",
        )
        self.assertEqual(
            200,
            self.client.post(
                "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
            ).status_code,
        )

        response = self.client.get("/api/admin/operations-dashboard")

        self.assertEqual(200, response.status_code)
        dashboard = response.json()
        self.assertEqual("unavailable", dashboard["paired_trade_lifecycle"]["availability"])
        self.assertEqual(
            "Paired-trade lifecycle records are not available in the control-plane ledger.",
            dashboard["paired_trade_lifecycle"]["reason"],
        )
        self.assertEqual([], dashboard["product_pairs"])
        enrollment_alert = next(
            alert for alert in dashboard["alerts"] if alert["alert_type"] == "worker_enrollment_pending_approval"
        )
        safety_alert = next(alert for alert in dashboard["alerts"] if alert["alert_type"] == "worker_frozen")
        self.assertEqual("intervention_required", enrollment_alert["category"])
        self.assertEqual("administrator_approval_required", enrollment_alert["classification_reason"])
        self.assertEqual(pending_enrollment_id, enrollment_alert["enrollment_id"])
        self.assertEqual("intervention_required", safety_alert["category"])
        self.assertEqual("worker_safety_state", safety_alert["classification_reason"])
        self.assertEqual("intervention_required", dashboard["pending_enrollments"][0]["category"])
        self.assertEqual("approval_required", dashboard["pending_enrollments"][0]["classification_reason"])
        enrollment_intervention = next(
            item for item in dashboard["interventions"] if item["item_type"] == "pending_enrollment"
        )
        self.assertEqual(
            {
                "item_type": "pending_enrollment",
                "item_id": pending_enrollment_id,
                "category": "intervention_required",
                "reason": "approval_required",
            },
            {
                key: value
                for key, value in enrollment_intervention.items()
                if key in {"item_type", "item_id", "category", "reason"}
            },
        )
        alert_intervention = next(item for item in dashboard["interventions"] if item["item_type"] == "worker_alert")
        self.assertEqual(
            {
                "item_type": "worker_alert",
                "item_id": safety_alert["alert_id"],
                "category": "intervention_required",
                "reason": "worker_safety_state",
            },
            {
                key: value
                for key, value in alert_intervention.items()
                if key in {"item_type", "item_id", "category", "reason"}
            },
        )
        worker = next(worker for worker in dashboard["workers"] if worker["worker_id"] == worker_id)
        self.assertEqual("intervention_required", worker["category"])
        self.assertEqual("frozen", worker["classification_reason"])
        self.assertEqual(UTC, datetime.fromisoformat(dashboard["generated_at"]).tzinfo)

    def test_enrollment_does_not_apply_a_client_ip_rate_limit(self) -> None:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        account_info = {"login": 123456, "server": "Broker-Demo"}
        terminal_info = {"name": "MetaTrader 5"}
        for _ in range(3):
            self.assertEqual(
                201,
                self._enrollment_response(private_key, public_key_pem, account_info, terminal_info).status_code,
            )
        self.assertEqual(201, self._enrollment_response(private_key, public_key_pem, account_info, terminal_info).status_code)

    def test_rejection_deletes_the_pending_password_secret(self) -> None:
        enrollment_id = self._create_pending_enrollment()
        secret_ref = self.app.state.ledger.enrollment_password_secret_ref(enrollment_id)
        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )

        response = self.client.post(
            f"/api/admin/enrollments/{enrollment_id}/reject",
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
        )

        self.assertEqual(204, response.status_code)
        self.assertNotIn(secret_ref, self.secret_store.passwords)

    def test_rejection_commits_before_cleanup_failure_and_retries_safely(self) -> None:
        enrollment_id = self._create_pending_enrollment()
        secret_ref = self.app.state.ledger.enrollment_password_secret_ref(enrollment_id)
        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        headers = {"X-CSRF-Token": login.json()["csrf_token"]}
        self.secret_store.fail_next_delete = True

        self.assertEqual(409, self.client.post(f"/api/admin/enrollments/{enrollment_id}/reject", headers=headers).status_code)
        self.assertIn(secret_ref, self.secret_store.passwords)
        self.assertEqual(
            409,
            self.client.post(f"/api/admin/enrollments/{enrollment_id}/approve", headers=headers).status_code,
        )

        self.assertEqual(204, self.client.post(f"/api/admin/enrollments/{enrollment_id}/reject", headers=headers).status_code)
        self.assertNotIn(secret_ref, self.secret_store.passwords)

    def test_expired_password_secret_deletion_retries_after_a_failure(self) -> None:
        enrollment_id = self._create_pending_enrollment()
        secret_ref = self.app.state.ledger.enrollment_password_secret_ref(enrollment_id)
        self.app.state.ledger._connection.execute(
            "UPDATE enrollments SET expires_at = ? WHERE enrollment_id = ?",
            [datetime.now(UTC) - timedelta(seconds=1), enrollment_id],
        )

        class FailOnceSecretStore(MemorySecretStore):
            def __init__(self, passwords: dict[str, str]) -> None:
                super().__init__()
                self.passwords = passwords
                self.failed = False

            def delete_password(self, reference: str) -> None:
                if not self.failed:
                    self.failed = True
                    raise SecretStoreError("OpenBao is unavailable.")
                super().delete_password(reference)

        retrying_store = FailOnceSecretStore(self.secret_store.passwords)
        with self.assertRaises(SecretStoreError):
            _delete_expired_pending_secrets(self.app.state.ledger, retrying_store)
        _delete_expired_pending_secrets(self.app.state.ledger, retrying_store)

        self.assertNotIn(secret_ref, retrying_store.passwords)
        self.assertEqual([], self.app.state.ledger.expire_pending_enrollments())

    def test_unattributed_broker_delta_freezes_worker_and_revocation_blocks_wss(self) -> None:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        enrollment = self._enrollment_response(
            private_key, public_key_pem, {"login": 123456, "server": "Broker-Demo"}, {"name": "MetaTrader 5"}
        )
        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        worker_id = self.client.post(
            f"/api/admin/enrollments/{enrollment.json()['enrollment_id']}/approve",
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
        ).json()["worker_id"]
        certificate = self.app.state.ledger.active_worker(worker_id).certificate

        with self.client.websocket_connect("/api/worker/session") as websocket:
            websocket.send_json({"worker_id": worker_id, "certificate": certificate})
            challenge = websocket.receive_json()
            signature = private_key.sign(
                worker_proof_payload(purpose="worker_session", worker_id=worker_id, nonce=challenge["nonce"]),
                ec.ECDSA(hashes.SHA256()),
            )
            websocket.send_json({"signature": base64.b64encode(signature).decode("ascii")})
            websocket.receive_json()
            websocket.send_json(
                {"type": "delta", "cursor": 1, "observed_at": "2026-08-16T00:01:00+00:00",
                 "entity": "position", "ticket": "51", "change": "created", "record": {"ticket": 51, "volume": 1}}
            )
            self.assertEqual({"type": "accepted", "cursor": 1}, websocket.receive_json())
            worker = self.client.get("/api/admin/workers").json()[0]
            self.assertEqual("frozen", worker["safety_state"])
            alerts = self.client.get("/api/admin/alerts").json()
            self.assertEqual(("high", "worker_frozen"), (alerts[-1]["priority"], alerts[-1]["alert_type"]))
            self.assertEqual(
                204,
                self.client.post(
                    f"/api/admin/workers/{worker_id}/revoke", headers={"X-CSRF-Token": login.json()["csrf_token"]}
                ).status_code,
            )
            with self.assertRaises(Exception) as closed:
                websocket.receive_json()
            self.assertEqual(1008, getattr(closed.exception, "code", None))

        self.assertEqual("revoked", self.client.get("/api/admin/workers").json()[0]["connectivity"])
        self.assertIn("worker_certificate_revoked", [
            event["event_type"] for event in self.client.get("/api/admin/events").json()["items"]
        ])
        with self.client.websocket_connect("/api/worker/session") as websocket:
            websocket.send_json({"worker_id": worker_id, "certificate": certificate})
            with self.assertRaises(Exception) as error:
                websocket.receive_json()
        self.assertEqual(1008, getattr(error.exception, "code", None))

    def test_admin_can_launch_a_catalog_analysis_over_worker_wss_and_read_the_result(self) -> None:
        policy = {
            "label": "FX catalog v1",
            "require_equal_base_currency": True,
            "require_equal_profit_currency": True,
            "minimum_m15_common_coverage": 1.0,
            "minimum_m1_common_coverage": 0.98,
            "minimum_m15_return_correlation": 0.97,
            "minimum_m1_return_correlation": 0.95,
            "maximum_m1_median_price_difference_points": 2.0,
        }
        with self._live_catalog_analysis_harness() as harness:
            first_key, first_worker, first_certificate = harness.approved_worker(123456, "Broker-A")
            second_key, second_worker, second_certificate = harness.approved_worker(654321, "Broker-B")
            first_ready = threading.Event()
            second_ready = threading.Event()
            first_responder = threading.Thread(
                target=harness.respond_to_catalog_analysis,
                args=(
                    first_key,
                    first_worker,
                    first_certificate,
                    [
                        self._forex_symbol("EURUSD.a", 100.0, trade_stops_level=0),
                        self._forex_symbol("GBPUSD.a", 100.0, currency_base="GBP"),
                        self._forex_symbol("AUDUSD.a", 100.0, currency_base="AUD"),
                        self._forex_symbol("XAUUSD.a", 100.0, trade_calc_mode="CFD", currency_base="XAU", digits=2, point=0.01, trade_tick_size=0.01),
                    ],
                ),
                kwargs={
                    "request_key": "first",
                    "ready_event": first_ready,
                    "market_data_by_stage": {
                        "m15_screening": {
                            "EURUSD.a": [
                                {"time": 1000, "open": 1.1000, "high": 1.1020, "low": 1.0990, "close": 1.1010},
                                {"time": 1900, "open": 1.1010, "high": 1.1030, "low": 1.1000, "close": 1.1022},
                                {"time": 2800, "open": 1.1020, "high": 1.1040, "low": 1.1010, "close": 1.1030},
                            ],
                            "GBPUSD.a": [
                                {"time": 1000, "open": 1.2500, "high": 1.2520, "low": 1.2490, "close": 1.2510},
                                {"time": 1900, "open": 1.2510, "high": 1.2530, "low": 1.2500, "close": 1.2525},
                                {"time": 2800, "open": 1.2524, "high": 1.2540, "low": 1.2510, "close": 1.2530},
                            ],
                            "AUDUSD.a": [
                                {"time": 1000, "open": 0.6500, "high": 0.6510, "low": 0.6490, "close": 0.6505},
                                {"time": 1900, "open": 0.6505, "high": 0.6515, "low": 0.6495, "close": 0.6510},
                                {"time": 2800, "open": 0.6510, "high": 0.6520, "low": 0.6500, "close": 0.6515},
                            ],
                        },
                        "m1_verification": {
                            "EURUSD.a": [
                                {"time": 1000, "close": 1.10000},
                                {"time": 1060, "close": 1.10050},
                                {"time": 1120, "close": 1.10120},
                                {"time": 1180, "close": 1.10180},
                            ],
                            "GBPUSD.a": [
                                {"time": 1000, "close": 1.25000},
                                {"time": 1060, "close": 1.25060},
                                {"time": 1120, "close": 1.25110},
                                {"time": 1180, "close": 1.25180},
                            ],
                        },
                    },
                },
            )
            second_responder = threading.Thread(
                target=harness.respond_to_catalog_analysis,
                args=(
                    second_key,
                    second_worker,
                    second_certificate,
                    [
                        {
                            **self._forex_symbol("EURUSD", 50.0, swap_mode=2),
                            "filling_modes": ["IOC"],
                        },
                        self._forex_symbol("GBPUSD", 100.0, currency_base="GBP", volume_step=0.1),
                        self._forex_symbol("AUDUSD", 100.0, currency_base="AUD"),
                        self._forex_symbol("XAUUSD", 100.0, currency_base="XAU", digits=2, point=0.01, trade_tick_size=0.01),
                    ],
                ),
                kwargs={
                    "request_key": "second",
                    "ready_event": second_ready,
                    "market_data_by_stage": {
                        "m15_screening": {
                            "EURUSD": [
                                {"time": 1000, "open": 1.1002, "high": 1.1022, "low": 1.0992, "close": 1.1012},
                                {"time": 1900, "open": 1.1012, "high": 1.1032, "low": 1.1002, "close": 1.1024},
                                {"time": 2800, "open": 1.1021, "high": 1.1041, "low": 1.1011, "close": 1.1032},
                            ],
                            "GBPUSD": [
                                {"time": 1000, "open": 1.2501, "high": 1.2521, "low": 1.2491, "close": 1.2511},
                                {"time": 1900, "open": 1.2511, "high": 1.2531, "low": 1.2501, "close": 1.2526},
                                {"time": 2800, "open": 1.2525, "high": 1.2541, "low": 1.2511, "close": 1.2531},
                            ],
                            "AUDUSD": [
                                {"time": 1000, "open": 0.6505, "high": 0.6515, "low": 0.6495, "close": 0.6510},
                                {"time": 1900, "open": 0.6510, "high": 0.6520, "low": 0.6500, "close": 0.6505},
                                {"time": 2800, "open": 0.6505, "high": 0.6515, "low": 0.6495, "close": 0.6500},
                            ],
                        },
                        "m1_verification": {
                            "EURUSD": [
                                {"time": 1000, "close": 1.10001},
                                {"time": 1060, "close": 1.10052},
                                {"time": 1120, "close": 1.10121},
                                {"time": 1180, "close": 1.10181},
                            ],
                            "GBPUSD": [
                                {"time": 1000, "close": 1.25001},
                                {"time": 1060, "close": 1.25061},
                                {"time": 1120, "close": 1.25112},
                                {"time": 1180, "close": 1.25181},
                            ],
                        },
                    },
                },
            )
            first_responder.start()
            second_responder.start()
            self.assertTrue(first_ready.wait(timeout=5))
            self.assertTrue(second_ready.wait(timeout=5))
            outcome = harness.launch_catalog_analysis(first_worker, second_worker, policy)
            first_responder.join(timeout=5)
            second_responder.join(timeout=5)
            first_requests = harness.requests["first"]
            second_requests = harness.requests["second"]
            analysis = harness.read_catalog_analysis(outcome["analysis_id"])

        first_request, first_m15_request, first_m1_request = first_requests
        second_request, second_m15_request, second_m1_request = second_requests
        self.assertEqual("product_catalog_analysis_request", first_request["type"])
        self.assertEqual("product_catalog_analysis_request", second_request["type"])
        self.assertEqual("catalog", first_request["stage"])
        self.assertEqual("catalog", second_request["stage"])
        self.assertEqual("m15_screening", first_m15_request["stage"])
        self.assertEqual("m15_screening", second_m15_request["stage"])
        self.assertEqual("m1_verification", first_m1_request["stage"])
        self.assertEqual("m1_verification", second_m1_request["stage"])
        self.assertEqual(first_request["analysis_id"], second_request["analysis_id"])
        self.assertEqual(policy, first_request["policy"])
        self.assertEqual(policy, second_request["policy"])
        self.assertEqual(first_m15_request["period_start_utc"], second_m15_request["period_start_utc"])
        self.assertEqual(first_m15_request["period_end_utc"], second_m15_request["period_end_utc"])
        self.assertEqual(["AUDUSD.a", "EURUSD.a", "GBPUSD.a"], first_m15_request["symbols"])
        self.assertEqual(["AUDUSD", "EURUSD", "GBPUSD"], second_m15_request["symbols"])
        self.assertEqual("M15", first_m15_request["timeframe"])
        self.assertEqual("M1", first_m1_request["timeframe"])
        self.assertEqual(["EURUSD.a", "GBPUSD.a"], first_m1_request["symbols"])
        self.assertEqual(["EURUSD", "GBPUSD"], second_m1_request["symbols"])
        self.assertEqual("succeeded", outcome["status"])
        self.assertEqual(policy, outcome["policy"])
        self.assertEqual(3, len(outcome["eligible_candidates"]))
        self.assertNotIn("exceptions", outcome)
        self.assertEqual(["failed", "passed", "passed"], [
            result["screening_status"] for result in outcome["m15_screening_results"]
        ])
        self.assertEqual(2, len(outcome["m1_verification_results"]))
        self.assertEqual(["EURUSD.a", "GBPUSD.a"], [
            result["first_symbol"] for result in outcome["m1_verification_results"]
        ])
        eur_result, gbp_result = outcome["m1_verification_results"]
        self.assertEqual("passed", eur_result["verification_status"])
        self.assertEqual([], eur_result["hard_block_differences"])
        self.assertEqual(["IOC"], eur_result["supported_filling_modes"])
        self.assertEqual(["volume_max", "trade_stops_level", "swap_mode"], [
            difference["field"] for difference in eur_result["warning_differences"]
        ])
        self.assertTrue(all(eur_result["policy_evaluation"].values()))
        self.assertEqual("failed", gbp_result["verification_status"])
        self.assertEqual(["volume_step"], [
            difference["field"] for difference in gbp_result["hard_block_differences"]
        ])
        self.assertEqual([], gbp_result["warning_differences"])
        self.assertTrue(gbp_result["policy_evaluation"]["coverage_passed"])
        self.assertTrue(gbp_result["policy_evaluation"]["return_correlation_passed"])
        self.assertTrue(gbp_result["policy_evaluation"]["median_price_difference_passed"])
        self.assertFalse(gbp_result["policy_evaluation"]["hard_block_differences_passed"])
        self.assertNotIn("bars", json.dumps(outcome["m15_screening_results"][0], sort_keys=True))
        self.assertNotIn("bars", json.dumps(outcome["m1_verification_results"][0], sort_keys=True))
        self.assertEqual(outcome["analysis_id"], analysis["analysis_id"])
        self.assertEqual(policy, analysis["policy"])
        self.assertEqual("Broker-A", analysis["first_worker"]["server"])
        self.assertEqual("Broker-B", analysis["second_worker"]["server"])
        self.assertEqual("M15", analysis["analysis_period"]["timeframe"])
        self.assertIsNotNone(analysis["m1_verified_at"])

    def test_catalog_response_requires_every_comparison_field(self) -> None:
        required_fields = (
            "trade_tick_size",
            "contract_size",
            "volume_min",
            "volume_step",
            "filling_modes",
            "allowed_directions",
            "volume_max",
            "trade_stops_level",
            "trade_freeze_level",
            "trade_tick_value",
            "currency_margin",
            "swap_long",
            "swap_short",
            "swap_rollover3days",
        )
        for field in required_fields:
            with self.subTest(field=field):
                symbol = self._forex_symbol("EURUSD", 100.0)
                del symbol[field]
                with self.assertRaisesRegex(ValueError, "incomplete"):
                    _validated_product_catalog_response(
                        {
                            "type": "product_catalog_analysis_response",
                            "stage": "catalog",
                            "analysis_id": "analysis-123",
                            "collected_at": "2026-08-17T07:05:00+00:00",
                            "symbols": [symbol],
                        },
                        "analysis-123",
                    )

    def test_catalog_response_accepts_legacy_worker_without_swap_mode(self) -> None:
        symbol = self._forex_symbol("EURUSD", 100.0)
        del symbol["swap_mode"]

        response = _validated_product_catalog_response(
            {
                "type": "product_catalog_analysis_response",
                "stage": "catalog",
                "analysis_id": "analysis-123",
                "collected_at": "2026-08-17T07:05:00+00:00",
                "symbols": [symbol],
            },
            "analysis-123",
        )

        self.assertIsNone(response["symbols"][0]["swap_mode"])

    def test_catalog_compares_native_trade_calculation_modes_without_translation(self) -> None:
        first = self._forex_symbol("EURUSD.a", 100.0, trade_calc_mode=0)
        second = self._forex_symbol("EURUSD", 100.0, trade_calc_mode=0)

        candidates = _analyze_product_catalogs([first], [second])

        self.assertEqual([("EURUSD.a", "EURUSD")], [
            (candidate["first_symbol"], candidate["second_symbol"]) for candidate in candidates
        ])
        candidates = _analyze_product_catalogs(
            [first],
            [self._forex_symbol("EURUSD", 100.0, trade_calc_mode=1)],
        )
        self.assertEqual([], candidates)

    def test_verification_accepts_shared_ioc_when_filling_capabilities_differ(self) -> None:
        self.assertEqual(["IOC"], _shared_supported_filling_modes(["FOK", "IOC"], ["IOC"]))
        self.assertEqual([], _shared_supported_filling_modes(["BOC"], ["RETURN"]))

    def test_market_data_response_requires_evidence_from_every_utc_weekday(self) -> None:
        analysis_period = {
            "started_at_utc": "2026-08-10T00:00:00Z",
            "ended_at_utc": "2026-08-17T00:00:00Z",
        }

        def response_for(days: list[int]) -> dict[str, object]:
            return {
                "type": "product_catalog_analysis_response",
                "stage": "m15_screening",
                "analysis_id": "analysis-123",
                "collected_at": "2026-08-17T07:05:00Z",
                "timeframe": "M15",
                "period_start_utc": analysis_period["started_at_utc"],
                "period_end_utc": analysis_period["ended_at_utc"],
                "symbols": [{
                    "symbol": "EURUSD",
                    "time_metadata": {},
                    "bars": [
                        {
                            "time": int(datetime(2026, 8, day, tzinfo=UTC).timestamp()),
                            "time_utc": datetime(2026, 8, day, tzinfo=UTC).isoformat().replace("+00:00", "Z"),
                            "close": 1.1,
                        }
                        for day in days
                    ],
                }],
            }

        accepted = _validated_market_data_response(
            response_for([10, 11, 12, 13, 14]),
            "analysis-123",
            "m15_screening",
            "M15",
            ["EURUSD"],
            analysis_period,
        )
        self.assertEqual(5, len(accepted["symbols"]["EURUSD"]["bars"]))

        for missing_day in (10, 14, 12):
            with self.subTest(missing_day=missing_day):
                days = [day for day in (10, 11, 12, 13, 14) if day != missing_day]
                with self.assertRaisesRegex(ValueError, "every UTC weekday"):
                    _validated_market_data_response(
                        response_for(days),
                        "analysis-123",
                        "m15_screening",
                        "M15",
                        ["EURUSD"],
                        analysis_period,
                    )

    def test_catalog_analysis_retries_m15_once_and_fails_without_partial_screening_results_when_worker_evidence_is_incomplete(self) -> None:
        with self._live_catalog_analysis_harness() as harness:
            first_key, first_worker, first_certificate = harness.approved_worker(123456, "Broker-A")
            second_key, second_worker, second_certificate = harness.approved_worker(654321, "Broker-B")
            first_ready = threading.Event()
            second_ready = threading.Event()
            first_responder = threading.Thread(
                target=harness.respond_to_catalog_analysis,
                args=(
                    first_key,
                    first_worker,
                    first_certificate,
                    [self._forex_symbol("EURUSD.a", 100.0)],
                ),
                kwargs={
                    "request_key": "first",
                    "ready_event": first_ready,
                    "market_data_by_stage": {
                        "m15_screening": {
                            "EURUSD.a": [
                            {"time": 1000, "open": 1.1000, "high": 1.1020, "low": 1.0990, "close": 1.1010},
                            {"time": 1900, "open": 1.1010, "high": 1.1030, "low": 1.1000, "close": 1.1022},
                            ],
                        },
                    },
                },
            )
            second_responder = threading.Thread(
                target=harness.respond_to_catalog_analysis,
                args=(
                    second_key,
                    second_worker,
                    second_certificate,
                    [self._forex_symbol("EURUSD", 100.0)],
                ),
                kwargs={
                    "request_key": "second",
                    "ready_event": second_ready,
                    "market_data_error": "AUDNZDC M15 evidence is unavailable.",
                },
            )
            first_responder.start()
            second_responder.start()
            self.assertTrue(first_ready.wait(timeout=5))
            self.assertTrue(second_ready.wait(timeout=5))
            outcome = harness.launch_catalog_analysis(first_worker, second_worker, {"label": "FX catalog v1"})
            first_responder.join(timeout=5)
            second_responder.join(timeout=5)

        self.assertEqual("failed", outcome["status"])
        self.assertEqual("m15_failed", outcome["current_stage"])
        self.assertEqual(1, outcome["retry_count"])
        self.assertEqual([], outcome["m15_screening_results"])
        self.assertEqual(1, len(outcome["eligible_candidates"]))
        self.assertEqual("AUDNZDC M15 evidence is unavailable.", outcome["failure_reason"])
        self.assertEqual(3, len(harness.requests["second"]))

    def test_catalog_analysis_fails_atomically_when_both_workers_omit_the_same_weekday(self) -> None:
        with self._live_catalog_analysis_harness() as harness:
            first_key, first_worker, first_certificate = harness.approved_worker(123456, "Broker-A")
            second_key, second_worker, second_certificate = harness.approved_worker(654321, "Broker-B")
            first_ready = threading.Event()
            second_ready = threading.Event()
            missing_wednesday = [
                {
                    "time": int(datetime(2026, 8, day, tzinfo=UTC).timestamp()),
                    "time_utc": datetime(2026, 8, day, tzinfo=UTC).isoformat().replace("+00:00", "Z"),
                    "close": 1.1000 + index * 0.0001,
                }
                for index, day in enumerate((10, 11, 13, 14))
            ]
            first_responder = threading.Thread(
                target=harness.respond_to_catalog_analysis,
                args=(first_key, first_worker, first_certificate, [self._forex_symbol("EURUSD.a", 100.0)]),
                kwargs={
                    "request_key": "first",
                    "ready_event": first_ready,
                    "market_data_by_stage": {"m15_screening": {"EURUSD.a": missing_wednesday}},
                },
            )
            second_responder = threading.Thread(
                target=harness.respond_to_catalog_analysis,
                args=(second_key, second_worker, second_certificate, [self._forex_symbol("EURUSD", 100.0)]),
                kwargs={
                    "request_key": "second",
                    "ready_event": second_ready,
                    "market_data_by_stage": {"m15_screening": {"EURUSD": missing_wednesday}},
                },
            )
            first_responder.start()
            second_responder.start()
            self.assertTrue(first_ready.wait(timeout=5))
            self.assertTrue(second_ready.wait(timeout=5))
            outcome = harness.launch_catalog_analysis(first_worker, second_worker, {"label": "FX catalog v1"})
            first_responder.join(timeout=5)
            second_responder.join(timeout=5)

        self.assertEqual("failed", outcome["status"])
        self.assertEqual("m15_failed", outcome["current_stage"])
        self.assertEqual([], outcome["m15_screening_results"])
        self.assertIn("every UTC weekday", outcome["failure_reason"])

    def test_catalog_analysis_retries_m1_once_and_fails_without_partial_verification_results_when_worker_evidence_is_incomplete(self) -> None:
        with self._live_catalog_analysis_harness() as harness:
            first_key, first_worker, first_certificate = harness.approved_worker(123456, "Broker-A")
            second_key, second_worker, second_certificate = harness.approved_worker(654321, "Broker-B")
            first_ready = threading.Event()
            second_ready = threading.Event()
            first_responder = threading.Thread(
                target=harness.respond_to_catalog_analysis,
                args=(
                    first_key,
                    first_worker,
                    first_certificate,
                    [self._forex_symbol("EURUSD.a", 100.0)],
                ),
                kwargs={
                    "request_key": "first",
                    "ready_event": first_ready,
                    "market_data_responses": [
                        {
                            "EURUSD.a": [
                                {"time": 1000, "close": 1.1010},
                                {"time": 1900, "close": 1.1022},
                                {"time": 2800, "close": 1.1030},
                            ]
                        },
                        {
                            "EURUSD.a": [
                                {"time": 1000, "close": 1.1000},
                                {"time": 1060, "close": 1.1005},
                                {"time": 1120, "close": 1.1010},
                            ]
                        },
                    ],
                },
            )
            second_responder = threading.Thread(
                target=harness.respond_to_catalog_analysis,
                args=(
                    second_key,
                    second_worker,
                    second_certificate,
                    [self._forex_symbol("EURUSD", 100.0)],
                ),
                kwargs={
                    "request_key": "second",
                    "ready_event": second_ready,
                    "market_data_by_stage": {
                        "m15_screening": {
                            "EURUSD": [
                                {"time": 1000, "close": 1.1012},
                                {"time": 1900, "close": 1.1024},
                                {"time": 2800, "close": 1.1032},
                            ]
                        },
                    },
                    "market_data_error_by_stage": {
                        "m1_verification": "EURUSD M1 evidence is unavailable.",
                    },
                },
            )
            first_responder.start()
            second_responder.start()
            self.assertTrue(first_ready.wait(timeout=5))
            self.assertTrue(second_ready.wait(timeout=5))
            outcome = harness.launch_catalog_analysis(first_worker, second_worker, {"label": "FX catalog v1"})
            first_responder.join(timeout=5)
            second_responder.join(timeout=5)

        self.assertEqual("failed", outcome["status"])
        self.assertEqual("m1_failed", outcome["current_stage"])
        self.assertEqual(1, outcome["retry_count"])
        self.assertEqual(1, len(outcome["m15_screening_results"]))
        self.assertEqual([], outcome["m1_verification_results"])
        self.assertEqual(4, len(harness.requests["second"]))

    def test_catalog_analysis_queues_shared_worker_work_until_the_running_analysis_completes(self) -> None:
        with self._live_catalog_analysis_harness() as harness:
            first_key, first_worker, first_certificate = harness.approved_worker(123456, "Broker-A")
            second_key, second_worker, second_certificate = harness.approved_worker(654321, "Broker-B")
            third_key, third_worker, third_certificate = harness.approved_worker(777777, "Broker-C")
            first_ready = threading.Event()
            second_ready = threading.Event()
            third_ready = threading.Event()
            release_first_analysis = threading.Event()
            third_received_request = threading.Event()
            first_result: dict[str, object] = {}
            second_result: dict[str, object] = {}

            def shared_worker() -> None:
                with websocket_connect(f"ws://127.0.0.1:{harness.port}/api/worker/session") as websocket:
                    harness.authenticate_worker_socket(websocket, first_key, first_worker, first_certificate)
                    first_ready.set()
                    for sequence, (symbol, currency_base, market_data_by_stage) in enumerate((
                        ("EURUSD.a", "EUR", self._passing_market_data("EURUSD.a")),
                        ("GBPUSD.a", "GBP", self._passing_market_data("GBPUSD.a", m15_base=1.3000, m1_base=1.30000)),
                    ), start=1):
                        catalog_request = json.loads(websocket.recv())
                        harness.requests.setdefault("shared", []).append(catalog_request)
                        websocket.send(json.dumps({
                            "type": "product_catalog_analysis_response",
                            "stage": "catalog",
                            "analysis_id": catalog_request["analysis_id"],
                            "request_id": catalog_request["request_id"],
                            "collected_at": "2026-08-17T07:00:00+00:00",
                            "symbols": [self._forex_symbol(symbol, 100.0, currency_base=currency_base)],
                        }))
                        for stage in ("m15_screening", "m1_verification"):
                            market_request = json.loads(websocket.recv())
                            harness.requests.setdefault("shared", []).append(market_request)
                            if sequence == 1 and stage == "m15_screening":
                                release_first_analysis.wait(timeout=5)
                            websocket.send(json.dumps(harness.market_data_response(
                                market_request,
                                market_data_by_stage[stage],
                            )))

            def dedicated_worker(
                key: ec.EllipticCurvePrivateKey,
                worker_id: str,
                certificate: str,
                request_key: str,
                symbol: str,
                currency_base: str,
                ready_event: threading.Event,
                received_event: threading.Event | None = None,
                *,
                market_data_by_stage: dict[str, dict[str, list[dict[str, object]]]],
            ) -> None:
                with websocket_connect(f"ws://127.0.0.1:{harness.port}/api/worker/session") as websocket:
                    harness.authenticate_worker_socket(websocket, key, worker_id, certificate)
                    ready_event.set()
                    catalog_request = json.loads(websocket.recv())
                    harness.requests.setdefault(request_key, []).append(catalog_request)
                    if received_event is not None:
                        received_event.set()
                    websocket.send(json.dumps({
                        "type": "product_catalog_analysis_response",
                        "stage": "catalog",
                        "analysis_id": catalog_request["analysis_id"],
                        "request_id": catalog_request["request_id"],
                        "collected_at": "2026-08-17T07:00:00+00:00",
                        "symbols": [self._forex_symbol(symbol, 100.0, currency_base=currency_base)],
                    }))
                    for stage in ("m15_screening", "m1_verification"):
                        market_request = json.loads(websocket.recv())
                        harness.requests.setdefault(request_key, []).append(market_request)
                        websocket.send(json.dumps(harness.market_data_response(
                            market_request,
                            market_data_by_stage[stage],
                        )))

            first_responder = threading.Thread(target=shared_worker)
            second_responder = threading.Thread(
                target=dedicated_worker,
                args=(second_key, second_worker, second_certificate, "second", "EURUSD", "EUR", second_ready),
                kwargs={"market_data_by_stage": self._passing_market_data("EURUSD", m15_base=1.1001, m1_base=1.10001)},
            )
            third_responder = threading.Thread(
                target=dedicated_worker,
                args=(third_key, third_worker, third_certificate, "third", "GBPUSD", "GBP", third_ready, third_received_request),
                kwargs={"market_data_by_stage": self._passing_market_data("GBPUSD", m15_base=1.3001, m1_base=1.30001)},
            )
            first_responder.start()
            second_responder.start()
            third_responder.start()
            self.assertTrue(first_ready.wait(timeout=5))
            self.assertTrue(second_ready.wait(timeout=5))
            self.assertTrue(third_ready.wait(timeout=5))

            first_launch = threading.Thread(
                target=lambda: first_result.update(harness.launch_catalog_analysis(
                    first_worker, second_worker, {"label": "FX catalog v1"}
                ))
            )
            first_launch.start()
            wait_deadline = time.monotonic() + 5
            while len(harness.requests.get("shared", [])) < 2 and time.monotonic() < wait_deadline:
                time.sleep(0.05)
            self.assertGreaterEqual(len(harness.requests.get("shared", [])), 2)
            second_launch = threading.Thread(
                target=lambda: second_result.update(harness.launch_catalog_analysis(
                    first_worker, third_worker, {"label": "FX catalog v1"}
                ))
            )
            second_launch.start()
            time.sleep(0.5)
            self.assertFalse(third_received_request.is_set())
            release_first_analysis.set()
            first_launch.join(timeout=10)
            second_launch.join(timeout=10)
            first_responder.join(timeout=5)
            second_responder.join(timeout=5)
            third_responder.join(timeout=5)

        self.assertEqual("succeeded", first_result["status"])
        self.assertEqual("succeeded", second_result["status"])
        self.assertTrue(third_received_request.is_set())

    def test_catalog_analysis_fails_without_partial_candidates_when_worker_evidence_is_incomplete(self) -> None:
        with self._live_catalog_analysis_harness() as harness:
            first_key, first_worker, first_certificate = harness.approved_worker(123456, "Broker-A")
            second_key, second_worker, second_certificate = harness.approved_worker(654321, "Broker-B")
            first_ready = threading.Event()
            second_ready = threading.Event()
            first_responder = threading.Thread(
                target=harness.respond_to_catalog_analysis,
                args=(
                    first_key,
                    first_worker,
                    first_certificate,
                    [
                        {
                            "symbol": "EURUSD.a",
                            "trade_calc_mode": "FOREX",
                            "currency_base": "EUR",
                            "currency_profit": "USD",
                        }
                    ],
                ),
                kwargs={"request_key": "first", "collected_at": "2026-08-17T07:00:00+00:00", "ready_event": first_ready},
            )
            second_responder = threading.Thread(
                target=harness.respond_to_catalog_analysis,
                args=(second_key, second_worker, second_certificate, None),
                kwargs={"request_key": "second", "collected_at": "2026-08-17T07:00:01+00:00", "ready_event": second_ready},
            )
            first_responder.start()
            second_responder.start()
            self.assertTrue(first_ready.wait(timeout=5))
            self.assertTrue(second_ready.wait(timeout=5))
            outcome = harness.launch_catalog_analysis(first_worker, second_worker, {"label": "FX catalog v1"})
            first_responder.join(timeout=5)
            second_responder.join(timeout=5)

        self.assertEqual("failed", outcome["status"])
        self.assertEqual([], outcome["eligible_candidates"])
        self.assertNotIn("exceptions", outcome)
        self.assertIn("incomplete", outcome["failure_reason"])

    def test_catalog_analysis_uses_two_live_reconciliation_workers_without_broker_writes(self) -> None:
        with self._live_catalog_analysis_harness() as harness:
            first_key, first_worker, first_certificate = harness.approved_worker(123456, "Broker-A")
            second_key, second_worker, second_certificate = harness.approved_worker(654321, "Broker-B")
            first_mt5 = RuntimeAnalysisMT5(123456, "Broker-A", "EURUSD.a", 0.0)
            second_mt5 = RuntimeAnalysisMT5(654321, "Broker-B", "EURUSD", 0.00001)
            first_ready = threading.Event()
            second_ready = threading.Event()
            first_runner = threading.Thread(
                target=harness.run_reconciliation_worker,
                args=(first_key, first_worker, first_certificate, first_mt5, first_ready),
                daemon=True,
            )
            second_runner = threading.Thread(
                target=harness.run_reconciliation_worker,
                args=(second_key, second_worker, second_certificate, second_mt5, second_ready),
                daemon=True,
            )
            first_runner.start()
            second_runner.start()
            self.assertTrue(first_ready.wait(timeout=5))
            self.assertTrue(second_ready.wait(timeout=5))

            outcome = harness.launch_catalog_analysis(first_worker, second_worker, {"label": "FX catalog v1"})
            first_runner.join(timeout=5)
            second_runner.join(timeout=5)

        self.assertEqual("succeeded", outcome["status"])
        self.assertEqual(["passed"], [result["screening_status"] for result in outcome["m15_screening_results"]], outcome)
        self.assertEqual(["passed"], [result["verification_status"] for result in outcome["m1_verification_results"]], outcome)
        self.assertFalse(first_runner.is_alive())
        self.assertFalse(second_runner.is_alive())
        self.assertEqual(0, first_mt5.broker_write_calls)
        self.assertEqual(0, second_mt5.broker_write_calls)

    def test_catalog_analysis_rejects_same_server_unhealthy_disconnected_and_revoked_workers_before_dispatch(self) -> None:
        first_key, first_worker, first_certificate = self._approved_worker(123456, "Broker-A")
        second_key, second_worker, second_certificate = self._approved_worker(654321, "Broker-A")
        login = self.client.post("/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"})
        csrf = login.json()["csrf_token"]

        with self.client.websocket_connect("/api/worker/session") as first_socket, self.client.websocket_connect("/api/worker/session") as second_socket:
            self._authenticate_worker_socket(first_socket, first_key, first_worker, first_certificate)
            self._authenticate_worker_socket(second_socket, second_key, second_worker, second_certificate)
            same_server = self.client.post(
                "/api/admin/product-catalog-analyses",
                headers={"X-CSRF-Token": csrf},
                json={"first_worker_id": first_worker, "second_worker_id": second_worker, "policy": {"label": "FX"}},
            )
            self.assertEqual(409, same_server.status_code)

            first_socket.send_json({"type": "safety_state", "state": "needs_human", "reason": "manual_test"})
            self.assertEqual({"type": "accepted", "state": "needs_human"}, first_socket.receive_json())
            unhealthy = self.client.post(
                "/api/admin/product-catalog-analyses",
                headers={"X-CSRF-Token": csrf},
                json={"first_worker_id": first_worker, "second_worker_id": second_worker, "policy": {"label": "FX"}},
            )
            self.assertEqual(409, unhealthy.status_code)

        self.app.state.ledger._connection.execute(
            "UPDATE workers SET last_seen_at = ? WHERE worker_id = ?",
            [datetime.now(UTC) - timedelta(minutes=6), second_worker],
        )
        disconnected = self.client.post(
            "/api/admin/product-catalog-analyses",
            headers={"X-CSRF-Token": csrf},
            json={"first_worker_id": first_worker, "second_worker_id": second_worker, "policy": {"label": "FX"}},
        )
        self.assertEqual(409, disconnected.status_code)

        self.app.state.ledger._connection.execute(
            "UPDATE workers SET safety_state = 'connected', last_seen_at = ? WHERE worker_id = ?",
            [datetime.now(UTC), second_worker],
        )
        self.assertEqual(
            204,
            self.client.post(f"/api/admin/workers/{second_worker}/revoke", headers={"X-CSRF-Token": csrf}).status_code,
        )
        revoked = self.client.post(
            "/api/admin/product-catalog-analyses",
            headers={"X-CSRF-Token": csrf},
            json={"first_worker_id": first_worker, "second_worker_id": second_worker, "policy": {"label": "FX"}},
        )
        self.assertEqual(409, revoked.status_code)

    def test_new_worker_session_takes_over_without_freezing_its_replacement(self) -> None:
        private_key, worker_id, certificate = self._approved_worker(123456, "Broker-A")

        with self.client.websocket_connect("/api/worker/session") as first_socket:
            self._authenticate_worker_socket(first_socket, private_key, worker_id, certificate)
            with self.client.websocket_connect("/api/worker/session") as second_socket:
                self._authenticate_worker_socket(second_socket, private_key, worker_id, certificate)
                with self.assertRaises(WebSocketDisconnect) as closed:
                    first_socket.receive_json()
                self.assertEqual(4001, closed.exception.code)

                second_socket.send_json({"type": "heartbeat"})
                self.assertEqual({"type": "heartbeat_ack"}, second_socket.receive_json())

    def test_build_requires_explicit_confirmation_and_persists_unordered_active_pair_evidence(self) -> None:
        with self._live_catalog_analysis_harness() as harness:
            analysis, first_worker, second_worker = self._create_passing_analysis(
                harness,
                first_login=123456,
                first_server="Broker-B",
                second_login=654321,
                second_server="Broker-A",
                policy={"label": "FX catalog v1"},
            )
            self.assertEqual("passed", analysis["m1_verification_results"][0]["verification_status"])
            self.assertEqual(404, harness.build_product_pair("missing-confirmation", expected_status=404).status_code)

            confirmation = harness.request_product_pair_build_confirmation(
                analysis["analysis_id"],
                "EURUSD.a",
                "EURUSD",
            )
            self.assertEqual(analysis["analysis_id"], confirmation["analysis_id"])
            self.assertEqual(analysis["policy"], confirmation["policy_snapshot"])
            self.assertEqual(analysis["analysis_period"], confirmation["analysis_period"])
            self.assertEqual(first_worker, confirmation["source_workers"]["first_worker"]["worker_id"])
            self.assertEqual(second_worker, confirmation["source_workers"]["second_worker"]["worker_id"])
            self.assertEqual(["Broker-A", "Broker-B"], [
                endpoint["server"] for endpoint in confirmation["reference_specifications"]
            ])
            self.assertEqual("1:1", confirmation["lot_relationship"]["ratio"])
            self.assertEqual("FX_V1", confirmation["lot_relationship"]["version"])

            built_pair = harness.build_product_pair(confirmation["confirmation_id"]).json()
            self.assertEqual("active", built_pair["status"])
            self.assertEqual(["Broker-A", "Broker-B"], [
                endpoint["server"] for endpoint in built_pair["endpoints"]
            ])
            self.assertEqual(["EURUSD", "EURUSD.a"], [
                endpoint["symbol"] for endpoint in built_pair["endpoints"]
            ])
            self.assertEqual(analysis["policy"], built_pair["policy_snapshot"])
            self.assertEqual(analysis["analysis_period"], built_pair["analysis_period"])
            self.assertEqual([], built_pair["approval_evidence"]["hard_block_differences"])
            self.assertEqual(["volume_max"], [
                difference["field"] for difference in built_pair["approval_evidence"]["warning_differences"]
            ])
            self.assertEqual("1:1", built_pair["lot_relationship"]["ratio"])

            pairs = harness.list_product_pairs()
            self.assertEqual(1, len([pair for pair in pairs if pair["status"] == "active"]))
            self.assertEqual(built_pair["product_pair_id"], pairs[0]["product_pair_id"])

            second_confirmation = harness.request_product_pair_build_confirmation(
                analysis["analysis_id"],
                "EURUSD.a",
                "EURUSD",
            )
            self.assertEqual(409, harness.build_product_pair(second_confirmation["confirmation_id"], expected_status=409).status_code)
            self.assertTrue({
                "product_pair_build_confirmation_requested",
                "product_pair_built",
            }.issubset({event["event_type"] for event in harness.list_events()}))

    def test_active_pair_uniqueness_is_enforced_even_if_precheck_is_bypassed(self) -> None:
        with self._live_catalog_analysis_harness() as harness:
            initial_pair = self._build_active_product_pair(
                harness,
                first_login=123456,
                first_server="Broker-B",
                second_login=654321,
                second_server="Broker-A",
                policy={"label": "FX catalog v1"},
            )
            analysis, _first_worker, _second_worker = self._create_passing_analysis(
                harness,
                first_login=123457,
                first_server="Broker-B",
                second_login=654322,
                second_server="Broker-A",
                policy={"label": "FX catalog v2"},
            )
            confirmation = harness.request_product_pair_build_confirmation(analysis["analysis_id"], "EURUSD.a", "EURUSD")

            with patch.object(harness._app.state.ledger, "_require_no_active_product_pair", lambda _endpoints: None):
                response = harness.build_product_pair(confirmation["confirmation_id"], expected_status=409)

            self.assertEqual(409, response.status_code)
            active_pairs = [pair for pair in harness.list_product_pairs() if pair["status"] == "active"]
            self.assertEqual([initial_pair["product_pair_id"]], [pair["product_pair_id"] for pair in active_pairs])

    def test_replace_retires_old_pair_atomically_and_retirement_preserves_audit_history(self) -> None:
        with self._live_catalog_analysis_harness() as harness:
            initial_pair = self._build_active_product_pair(
                harness,
                first_login=123456,
                first_server="Broker-B",
                second_login=654321,
                second_server="Broker-A",
                policy={"label": "FX catalog v1"},
            )

            replacement_pair = self._prepare_replacement_confirmation(
                harness,
                active_pair_id=initial_pair["product_pair_id"],
                first_login=123457,
                first_server="Broker-B",
                second_login=654322,
                second_server="Broker-A",
                policy={"label": "FX catalog v2", "maximum_m1_p99_price_difference_points": 20.0},
                first_volume_max=300.0,
                second_volume_max=320.0,
            )

            pairs_after_replace = {pair["product_pair_id"]: pair for pair in harness.list_product_pairs()}
            self.assertEqual("retired", pairs_after_replace[initial_pair["product_pair_id"]]["status"])
            self.assertEqual(replacement_pair["product_pair_id"], pairs_after_replace[initial_pair["product_pair_id"]]["replaced_by_product_pair_id"])
            self.assertEqual("active", pairs_after_replace[replacement_pair["product_pair_id"]]["status"])
            self.assertEqual(1, len([pair for pair in pairs_after_replace.values() if pair["status"] == "active"]))

            retired = harness.retire_product_pair(replacement_pair["product_pair_id"]).json()
            self.assertEqual("retired", retired["status"])
            self.assertEqual("manual_retirement", retired["retired_reason"])

            retained_pairs = {pair["product_pair_id"]: pair for pair in harness.list_product_pairs()}
            self.assertEqual("retired", retained_pairs[initial_pair["product_pair_id"]]["status"])
            self.assertEqual("retired", retained_pairs[replacement_pair["product_pair_id"]]["status"])
            self.assertEqual(0, len([pair for pair in retained_pairs.values() if pair["status"] == "active"]))
            self.assertTrue({
                "product_pair_replaced",
                "product_pair_retired",
            }.issubset({event["event_type"] for event in harness.list_events()}))

    def test_product_pair_workers_default_to_applicable_and_manual_compatibility_checks_do_not_auto_exclude(self) -> None:
        with self._live_catalog_analysis_harness() as harness:
            built_pair = self._build_active_product_pair(
                harness,
                first_login=123456,
                first_server="Broker-B",
                second_login=654321,
                second_server="Broker-A",
                policy={"label": "FX catalog v1"},
            )
            third_key, third_worker, third_certificate = harness.approved_worker(777777, "Broker-B")
            third_ready = threading.Event()
            third_responder = threading.Thread(
                target=harness.respond_to_catalog_analysis,
                args=(
                    third_key,
                    third_worker,
                    third_certificate,
                    [self._forex_symbol("EURUSD.a", 300.0, volume_step=0.1)],
                ),
                kwargs={"request_key": f"compatibility-{third_worker}", "ready_event": third_ready},
            )
            third_responder.start()
            self.assertTrue(third_ready.wait(timeout=5))

            before_check = next(
                worker
                for worker in harness.list_product_pairs()[0]["worker_applicability"]
                if worker["worker_id"] == third_worker
            )
            self.assertEqual("applicable", before_check["applicability_status"])
            self.assertEqual("uninspected", before_check["inspection_status"])
            self.assertIsNone(before_check["latest_compatibility_check"])
            self.assertIsNone(before_check["exclusion"])

            compatibility = harness.check_product_pair_worker_compatibility(
                built_pair["product_pair_id"],
                third_worker,
            )
            third_responder.join(timeout=5)

            self.assertEqual(built_pair["product_pair_id"], compatibility["product_pair_id"])
            self.assertEqual(third_worker, compatibility["worker_id"])
            self.assertEqual("applicable", compatibility["applicability_status"])
            self.assertEqual("differences_detected", compatibility["inspection_status"])
            self.assertEqual("EURUSD.a", compatibility["reference_symbol"])
            self.assertEqual(["volume_step"], [
                difference["field"] for difference in compatibility["hard_block_differences"]
            ])
            self.assertEqual(["volume_max"], [
                difference["field"] for difference in compatibility["warning_differences"]
            ])

            after_check = next(
                worker
                for worker in harness.list_product_pairs()[0]["worker_applicability"]
                if worker["worker_id"] == third_worker
            )
            self.assertEqual("applicable", after_check["applicability_status"])
            self.assertEqual("differences_detected", after_check["inspection_status"])
            self.assertIsNotNone(after_check["latest_compatibility_check"])
            self.assertIsNone(after_check["exclusion"])

            ledger = harness._app.state.ledger
            original_applicability = ledger._product_pair_with_worker_applicability
            applicability_started = threading.Event()
            allow_applicability = threading.Event()
            events_completed = threading.Event()

            def block_applicability(pair: dict[str, object]) -> dict[str, object]:
                applicability_started.set()
                self.assertTrue(allow_applicability.wait(timeout=5))
                return original_applicability(pair)

            ledger._product_pair_with_worker_applicability = block_applicability
            try:
                pairs_thread = threading.Thread(target=ledger.product_pairs)
                events_thread = threading.Thread(
                    target=lambda: (ledger.events(), events_completed.set()),
                )
                pairs_thread.start()
                self.assertTrue(applicability_started.wait(timeout=5))
                events_thread.start()
                self.assertFalse(events_completed.wait(timeout=0.2))
            finally:
                allow_applicability.set()
                pairs_thread.join(timeout=5)
                events_thread.join(timeout=5)
                ledger._product_pair_with_worker_applicability = original_applicability

            exclusion = harness.exclude_product_pair_worker(built_pair["product_pair_id"], third_worker)
            self.assertEqual("excluded", exclusion["applicability_status"])
            self.assertEqual("ABCDEF", exclusion["exclusion"]["excluded_by"])
            self.assertEqual(
                compatibility["compatibility_check_id"],
                exclusion["exclusion"]["compatibility_check_id"],
            )

            after_exclusion = next(
                worker
                for worker in harness.list_product_pairs()[0]["worker_applicability"]
                if worker["worker_id"] == third_worker
            )
            self.assertEqual("excluded", after_exclusion["applicability_status"])
            self.assertEqual("differences_detected", after_exclusion["inspection_status"])
            self.assertEqual(
                compatibility["compatibility_check_id"],
                after_exclusion["latest_compatibility_check"]["compatibility_check_id"],
            )
            self.assertEqual(
                compatibility["compatibility_check_id"],
                after_exclusion["exclusion"]["compatibility_check_id"],
            )
            still_applicable = {
                worker["worker_id"]: worker["applicability_status"]
                for worker in harness.list_product_pairs()[0]["worker_applicability"]
            }
            self.assertEqual("applicable", still_applicable[built_pair["source_workers"]["first_worker"]["worker_id"]])
            self.assertEqual("applicable", still_applicable[built_pair["source_workers"]["second_worker"]["worker_id"]])
            self.assertTrue({
                "product_pair_worker_compatibility_checked",
                "product_pair_worker_excluded",
            }.issubset({event["event_type"] for event in harness.list_events()}))

    def test_manual_retest_uses_original_policy_records_fresh_evidence_and_preserves_reference_snapshot(self) -> None:
        with self._live_catalog_analysis_harness() as harness:
            built_pair = self._build_active_product_pair(
                harness,
                first_login=123456,
                first_server="Broker-B",
                second_login=654321,
                second_server="Broker-A",
                policy={"label": "FX catalog v1"},
            )
            original_reference_snapshot = json.loads(json.dumps(built_pair["reference_specifications"]))

            first_retest_key, first_retest_worker, first_retest_certificate = harness.approved_worker(777777, "Broker-A")
            second_retest_key, second_retest_worker, second_retest_certificate = harness.approved_worker(888888, "Broker-B")
            first_ready = threading.Event()
            second_ready = threading.Event()
            first_responder = threading.Thread(
                target=harness.respond_to_catalog_analysis,
                args=(
                    first_retest_key,
                    first_retest_worker,
                    first_retest_certificate,
                    [self._forex_symbol("EURUSD", 330.0)],
                ),
                kwargs={
                    "request_key": f"retest-first-{first_retest_worker}",
                    "collected_at": "2026-08-24T07:00:00+00:00",
                    "market_data_collected_at": "2026-08-24T07:05:00+00:00",
                    "ready_event": first_ready,
                    "market_data_by_stage": self._passing_market_data("EURUSD"),
                },
            )
            second_responder = threading.Thread(
                target=harness.respond_to_catalog_analysis,
                args=(
                    second_retest_key,
                    second_retest_worker,
                    second_retest_certificate,
                    [self._forex_symbol("EURUSD.a", 360.0)],
                ),
                kwargs={
                    "request_key": f"retest-second-{second_retest_worker}",
                    "collected_at": "2026-08-24T07:00:01+00:00",
                    "market_data_collected_at": "2026-08-24T07:05:01+00:00",
                    "ready_event": second_ready,
                    "market_data_by_stage": self._passing_market_data("EURUSD.a", m15_base=1.1001, m1_base=1.10001),
                },
            )
            first_responder.start()
            second_responder.start()
            self.assertTrue(first_ready.wait(timeout=5))
            self.assertTrue(second_ready.wait(timeout=5))

            retest = harness.launch_product_pair_retest(
                built_pair["product_pair_id"],
                first_retest_worker,
                second_retest_worker,
            )

            first_responder.join(timeout=5)
            second_responder.join(timeout=5)

            self.assertEqual("passed", retest["status"])
            self.assertEqual(built_pair["product_pair_id"], retest["product_pair_id"])
            self.assertEqual(built_pair["policy_snapshot"], retest["policy_snapshot"])
            self.assertEqual(original_reference_snapshot, retest["reference_specifications"])
            self.assertEqual("2026-08-24T07:00:00+00:00", retest["first_catalog_evidence"]["collected_at"])
            self.assertEqual("2026-08-24T07:00:01+00:00", retest["second_catalog_evidence"]["collected_at"])
            self.assertEqual("2026-08-24T07:05:00+00:00", retest["m15_screening_results"][0]["first_market_data"]["time_metadata"]["calibration"]["calibrated_at_utc"])
            self.assertEqual("2026-08-24T07:05:01+00:00", retest["m15_screening_results"][0]["second_market_data"]["time_metadata"]["calibration"]["calibrated_at_utc"])
            self.assertEqual("2026-08-24T07:05:00+00:00", retest["m1_verification_results"][0]["first_market_data"]["time_metadata"]["calibration"]["calibrated_at_utc"])
            self.assertEqual(first_retest_worker, retest["source_workers"]["first_worker"]["worker_id"])
            self.assertEqual(second_retest_worker, retest["source_workers"]["second_worker"]["worker_id"])
            self.assertEqual(330.0, retest["first_catalog_evidence"]["symbols"][0]["volume_max"])
            self.assertEqual(
                {"Broker-A": 250.0, "Broker-B": 200.0},
                {
                    item["server"]: item["specification"]["volume_max"]
                    for item in original_reference_snapshot
                },
            )

            pair_after_retest = harness.list_product_pairs()[0]
            self.assertEqual("passed", pair_after_retest["latest_retest"]["status"])
            self.assertEqual(retest["retest_id"], pair_after_retest["latest_retest"]["retest_id"])
            self.assertEqual(original_reference_snapshot, pair_after_retest["reference_specifications"])
            self.assertTrue({
                "product_pair_retest_requested",
                "product_pair_retest_succeeded",
            }.issubset({event["event_type"] for event in harness.list_events()}))

    def test_manual_retest_requires_healthy_connected_workers_on_the_pairs_exact_servers(self) -> None:
        with self._live_catalog_analysis_harness() as harness:
            built_pair = self._build_active_product_pair(
                harness,
                first_login=123456,
                first_server="Broker-B",
                second_login=654321,
                second_server="Broker-A",
                policy={"label": "FX catalog v1"},
            )
            healthy_endpoint_worker = harness.approved_worker(777777, "Broker-A")[1]
            wrong_server_worker = harness.approved_worker(888888, "Broker-C")[1]
            harness._app.state.ledger.record_worker_session(healthy_endpoint_worker)
            harness._app.state.ledger.record_worker_session(wrong_server_worker)

            wrong_server = harness.launch_product_pair_retest(
                built_pair["product_pair_id"],
                healthy_endpoint_worker,
                wrong_server_worker,
                expected_status=409,
            )
            self.assertEqual("Selected workers must belong to this product pair's exact MT5 servers.", wrong_server["detail"])

            disconnected_worker = harness.approved_worker(999999, "Broker-B")[1]
            disconnected = harness.launch_product_pair_retest(
                built_pair["product_pair_id"],
                healthy_endpoint_worker,
                disconnected_worker,
                expected_status=409,
            )
            self.assertEqual(
                "Selected worker must be approved, healthy, and connected.",
                disconnected["detail"],
            )

    def test_manual_retest_failure_creates_an_alert_marks_latest_failed_and_keeps_the_pair_active(self) -> None:
        with self._live_catalog_analysis_harness() as harness:
            built_pair = self._build_active_product_pair(
                harness,
                first_login=123456,
                first_server="Broker-B",
                second_login=654321,
                second_server="Broker-A",
                policy={"label": "FX catalog v1"},
            )
            first_retest_key, first_retest_worker, first_retest_certificate = harness.approved_worker(777777, "Broker-A")
            second_retest_key, second_retest_worker, second_retest_certificate = harness.approved_worker(888888, "Broker-B")
            first_ready = threading.Event()
            second_ready = threading.Event()
            first_responder = threading.Thread(
                target=harness.respond_to_catalog_analysis,
                args=(
                    first_retest_key,
                    first_retest_worker,
                    first_retest_certificate,
                    [self._forex_symbol("EURUSD", 330.0)],
                ),
                kwargs={
                    "request_key": f"failed-retest-first-{first_retest_worker}",
                    "ready_event": first_ready,
                    "market_data_by_stage": self._passing_market_data("EURUSD"),
                },
            )
            second_responder = threading.Thread(
                target=harness.respond_to_catalog_analysis,
                args=(
                    second_retest_key,
                    second_retest_worker,
                    second_retest_certificate,
                    [self._forex_symbol("EURUSD.a", 360.0)],
                ),
                kwargs={
                    "request_key": f"failed-retest-second-{second_retest_worker}",
                    "ready_event": second_ready,
                    "market_data_by_stage": self._passing_market_data("EURUSD.a", m15_base=1.1001, m1_base=1.10500),
                },
            )
            first_responder.start()
            second_responder.start()
            self.assertTrue(first_ready.wait(timeout=5))
            self.assertTrue(second_ready.wait(timeout=5))

            retest = harness.launch_product_pair_retest(
                built_pair["product_pair_id"],
                first_retest_worker,
                second_retest_worker,
            )

            first_responder.join(timeout=5)
            second_responder.join(timeout=5)

            self.assertEqual("failed", retest["status"])
            self.assertEqual("failed", retest["m1_verification_results"][0]["verification_status"])
            self.assertEqual("Re-test failed the original analysis policy.", retest["failure_reason"])

            pair_after_failure = harness.list_product_pairs()[0]
            self.assertEqual("active", pair_after_failure["status"])
            self.assertEqual("failed", pair_after_failure["latest_retest"]["status"])
            self.assertEqual(retest["retest_id"], pair_after_failure["latest_retest"]["retest_id"])

            latest_alert = harness.list_alerts()[-1]
            self.assertEqual("product_pair_retest_failed", latest_alert["alert_type"])
            self.assertEqual("latest_retest_failed", latest_alert["reason"])
            self.assertEqual(built_pair["product_pair_id"], latest_alert["product_pair_id"])
            self.assertTrue({
                "product_pair_retest_requested",
                "product_pair_retest_failed",
            }.issubset({event["event_type"] for event in harness.list_events()}))

    def _create_pending_enrollment(self) -> str:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        account_info = {"login": 123456, "server": "Broker-Demo"}
        terminal_info = {"name": "MetaTrader 5"}
        response = self._enrollment_response(private_key, public_key_pem, account_info, terminal_info)
        self.assertEqual(201, response.status_code)
        return response.json()["enrollment_id"]

    def _admin_websocket_headers(self) -> dict[str, str]:
        return {"Cookie": f"abt_admin_session={self.client.cookies['abt_admin_session']}"}

    def _enrollment_challenge(self) -> str:
        response = self.client.get("/api/enrollment-challenge")
        self.assertEqual(200, response.status_code)
        return response.json()["challenge"]

    def _trader_attestation(self, public_key_pem: str, *, provider: str = "TPM") -> str:
        def encode(value: bytes) -> str:
            return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")

        header = encode(json.dumps({"alg": "ES256", "typ": "JWT"}, separators=(",", ":")).encode("utf-8"))
        claims = encode(
            json.dumps(
                {
                    "provider": provider,
                    "public_key_pem": public_key_pem,
                    "non_exportable": True,
                    "iat": int(datetime.now(UTC).timestamp()),
                    "exp": int((datetime.now(UTC) + timedelta(minutes=5)).timestamp()),
                },
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        )
        der_signature = self.attestation_key.sign(f"{header}.{claims}".encode("ascii"), ec.ECDSA(hashes.SHA256()))
        r, s = decode_dss_signature(der_signature)
        return f"{header}.{claims}.{encode(r.to_bytes(32) + s.to_bytes(32))}"

    def _approved_worker(self, login: int, server: str) -> tuple[ec.EllipticCurvePrivateKey, str, str]:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        enrollment = self._enrollment_response(
            private_key,
            public_key_pem,
            {"login": login, "server": server},
            {"name": "MetaTrader 5"},
            password=f"worker-{login}-memory-only-password",
            login=login,
            server=server,
        )
        approval = self.client.post(
            f"/api/admin/enrollments/{enrollment.json()['enrollment_id']}/approve",
            headers={
                "X-CSRF-Token": self.client.post(
                    "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
                ).json()["csrf_token"]
            },
        )
        worker_id = approval.json()["worker_id"]
        return private_key, worker_id, self.app.state.ledger.active_worker(worker_id).certificate

    def _launch_catalog_analysis(
        self, first_worker_id: str, second_worker_id: str, policy: dict[str, object]
    ) -> dict[str, object]:
        login = self.http_client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        response = self.http_client.post(
            "/api/admin/product-catalog-analyses",
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
            json={"first_worker_id": first_worker_id, "second_worker_id": second_worker_id, "policy": policy},
        )
        return response.json()

    def _build_active_product_pair(
        self,
        harness: "_LiveCatalogAnalysisHarness",
        *,
        first_login: int,
        first_server: str,
        second_login: int,
        second_server: str,
        policy: dict[str, object],
        first_volume_max: float = 200.0,
        second_volume_max: float = 250.0,
    ) -> dict[str, object]:
        analysis, _first_worker, _second_worker = self._create_passing_analysis(
            harness,
            first_login=first_login,
            first_server=first_server,
            second_login=second_login,
            second_server=second_server,
            policy=policy,
            first_volume_max=first_volume_max,
            second_volume_max=second_volume_max,
        )
        confirmation = harness.request_product_pair_build_confirmation(analysis["analysis_id"], "EURUSD.a", "EURUSD")
        response = harness.build_product_pair(confirmation["confirmation_id"])
        return response.json()

    def _prepare_replacement_confirmation(
        self,
        harness: "_LiveCatalogAnalysisHarness",
        *,
        active_pair_id: str,
        first_login: int,
        first_server: str,
        second_login: int,
        second_server: str,
        policy: dict[str, object],
        first_volume_max: float,
        second_volume_max: float,
    ) -> dict[str, object]:
        analysis, _first_worker, _second_worker = self._create_passing_analysis(
            harness,
            first_login=first_login,
            first_server=first_server,
            second_login=second_login,
            second_server=second_server,
            policy=policy,
            first_volume_max=first_volume_max,
            second_volume_max=second_volume_max,
        )
        confirmation = harness.request_product_pair_build_confirmation(analysis["analysis_id"], "EURUSD.a", "EURUSD")
        self.assertEqual(409, harness.build_product_pair(confirmation["confirmation_id"], expected_status=409).status_code)
        return harness.replace_product_pair(active_pair_id, confirmation["confirmation_id"]).json()

    def _create_passing_analysis(
        self,
        harness: "_LiveCatalogAnalysisHarness",
        *,
        first_login: int,
        first_server: str,
        second_login: int,
        second_server: str,
        policy: dict[str, object],
        first_volume_max: float = 200.0,
        second_volume_max: float = 250.0,
    ) -> tuple[dict[str, object], str, str]:
        first_key, first_worker, first_certificate = harness.approved_worker(first_login, first_server)
        second_key, second_worker, second_certificate = harness.approved_worker(second_login, second_server)
        first_ready = threading.Event()
        second_ready = threading.Event()
        first_responder = threading.Thread(
            target=harness.respond_to_catalog_analysis,
            args=(
                first_key,
                first_worker,
                first_certificate,
                [self._forex_symbol("EURUSD.a", first_volume_max)],
            ),
            kwargs={
                "request_key": f"first-{first_login}",
                "ready_event": first_ready,
                "market_data_by_stage": self._passing_market_data("EURUSD.a"),
            },
        )
        second_responder = threading.Thread(
            target=harness.respond_to_catalog_analysis,
            args=(
                second_key,
                second_worker,
                second_certificate,
                [self._forex_symbol("EURUSD", second_volume_max)],
            ),
            kwargs={
                "request_key": f"second-{second_login}",
                "ready_event": second_ready,
                "market_data_by_stage": self._passing_market_data("EURUSD", m15_base=1.1001, m1_base=1.10001),
            },
        )
        first_responder.start()
        second_responder.start()
        self.assertTrue(first_ready.wait(timeout=5))
        self.assertTrue(second_ready.wait(timeout=5))
        analysis = harness.launch_catalog_analysis(first_worker, second_worker, policy)
        first_responder.join(timeout=5)
        second_responder.join(timeout=5)
        return analysis, first_worker, second_worker

    def _forex_symbol(
        self,
        symbol: str,
        volume_max: float,
        *,
        volume_step: float = 0.01,
        trade_calc_mode: int | str = "FOREX",
        currency_base: str = "EUR",
        currency_profit: str = "USD",
        digits: int = 5,
        point: float = 0.00001,
        trade_tick_size: float | None = None,
        trade_stops_level: int = 10,
        swap_mode: int = 1,
    ) -> dict[str, object]:
        tick_size = point if trade_tick_size is None else trade_tick_size
        return {
            "symbol": symbol,
            "trade_calc_mode": trade_calc_mode,
            "currency_base": currency_base,
            "currency_profit": currency_profit,
            "digits": digits,
            "point": point,
            "trade_tick_size": tick_size,
            "contract_size": 100000,
            "volume_min": 0.01,
            "volume_step": volume_step,
            "volume_max": volume_max,
            "trade_stops_level": trade_stops_level,
            "trade_freeze_level": 0,
            "trade_tick_value": 1.0,
            "currency_margin": currency_profit,
            "swap_long": -1.5,
            "swap_short": 0.5,
            "swap_mode": swap_mode,
            "swap_rollover3days": 3,
            "filling_modes": ["FOK", "IOC"],
            "allowed_directions": ["LONG", "SHORT"],
        }

    def _passing_market_data(self, symbol: str, *, m15_base: float = 1.1000, m1_base: float = 1.10000) -> dict[str, dict[str, list[dict[str, object]]]]:
        return {
            "m15_screening": {
                symbol: [
                    {"time": 1000, "open": m15_base, "high": round(m15_base + 0.0020, 5), "low": round(m15_base - 0.0010, 5), "close": round(m15_base + 0.0010, 5)},
                    {"time": 1900, "open": round(m15_base + 0.0010, 5), "high": round(m15_base + 0.0030, 5), "low": m15_base, "close": round(m15_base + 0.0022, 5)},
                    {"time": 2800, "open": round(m15_base + 0.0020, 5), "high": round(m15_base + 0.0040, 5), "low": round(m15_base + 0.0010, 5), "close": round(m15_base + 0.0030, 5)},
                ]
            },
            "m1_verification": {
                symbol: [
                    {"time": 1000, "close": m1_base},
                    {"time": 1060, "close": round(m1_base + 0.00050, 5)},
                    {"time": 1120, "close": round(m1_base + 0.00120, 5)},
                    {"time": 1180, "close": round(m1_base + 0.00180, 5)},
                ]
            },
        }

    def _respond_to_catalog_analysis(
        self,
        websocket: object,
        requests: dict[str, dict[str, object]],
        key: str,
        symbols: list[dict[str, object]] | None,
        *,
        collected_at: str = "2026-08-17T07:00:00+00:00",
    ) -> None:
        request = websocket.receive_json()
        requests[key] = request
        response = {
            "type": "product_catalog_analysis_response",
            "analysis_id": request["analysis_id"],
            "request_id": request["request_id"],
            "collected_at": collected_at,
        }
        if symbols is not None:
            response["symbols"] = symbols
        websocket.send_json(response)

    def _authenticate_worker_socket(
        self,
        websocket: object,
        private_key: ec.EllipticCurvePrivateKey,
        worker_id: str,
        certificate: str,
    ) -> None:
        websocket.send_json({"worker_id": worker_id, "certificate": certificate})
        challenge = websocket.receive_json()
        signature = private_key.sign(
            worker_proof_payload(purpose="worker_session", worker_id=worker_id, nonce=challenge["nonce"]),
            ec.ECDSA(hashes.SHA256()),
        )
        websocket.send_json({"signature": base64.b64encode(signature).decode("ascii")})
        self.assertEqual({"type": "authenticated", "worker_id": worker_id, "cursor": 0}, websocket.receive_json())

    @contextmanager
    def _live_catalog_analysis_harness(self):
        secret_store = MemorySecretStore()
        certificate_issuer = MemoryCertificateIssuer()
        app = create_app(
            Path(self._directory.name) / f"live-{uuid4()}.duckdb",
            secret_store=secret_store,
            certificate_issuer=certificate_issuer,
        )
        app.state.ledger.create_admin("ABCDEF", "A-secure-admin-password!")
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{port}"
        for _ in range(50):
            try:
                if httpx.get(f"{base_url}/health", timeout=0.2).status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.1)
        else:
            server.should_exit = True
            thread.join(timeout=5)
            self.fail("live server did not start")
        try:
            yield _LiveCatalogAnalysisHarness(self, app, base_url)
        finally:
            server.should_exit = True
            try:
                httpx.get(f"{base_url}/health", timeout=0.2)
            except httpx.HTTPError:
                pass
            thread.join(timeout=5)
            if thread.is_alive():
                server.force_exit = True
                thread.join(timeout=5)

    def _enrollment_response(
        self,
        private_key: ec.EllipticCurvePrivateKey,
        public_key_pem: str,
        account_info: dict[str, object],
        terminal_info: dict[str, object],
        password: str = "worker-memory-only-password",
        *,
        login: int = 123456,
        server: str = "Broker-Demo",
    ):
        challenge = self._enrollment_challenge()
        signature = private_key.sign(
            enrollment_payload(login, server, account_info, terminal_info, password, challenge),
            ec.ECDSA(hashes.SHA256()),
        )
        return self.client.post(
            "/api/enrollments",
            headers={"CF-Connecting-IP": "203.0.113.11"},
            json={
                "registration_invite": self.app.state.ledger.create_registration_invite("ABCDEF", "worker"),
                "login": login, "server": server,
                "account_info": account_info, "terminal_info": terminal_info, "mt5_password": password,
                "enrollment_challenge": challenge, "public_key_pem": public_key_pem,
                "proof_signature": base64.b64encode(signature).decode("ascii"),
            },
        )


class _LiveCatalogAnalysisHarness:
    def __init__(self, case: ControlPlaneServiceTests, app: object, base_url: str) -> None:
        self._case = case
        self._app = app
        self._base_url = base_url
        self.requests: dict[str, list[dict[str, object]]] = {}

    @property
    def port(self) -> str:
        return self._base_url.rsplit(":", 1)[1]

    def approved_worker(self, login: int, server: str) -> tuple[ec.EllipticCurvePrivateKey, str, str]:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        challenge = httpx.get(f"{self._base_url}/api/enrollment-challenge", timeout=5).json()["challenge"]
        account_info = {"login": login, "server": server}
        terminal_info = {"name": "MetaTrader 5"}
        password = f"worker-{login}-memory-only-password"
        signature = private_key.sign(
            enrollment_payload(login, server, account_info, terminal_info, password, challenge),
            ec.ECDSA(hashes.SHA256()),
        )
        response = httpx.post(
            f"{self._base_url}/api/enrollments",
            json={
                "registration_invite": self._app.state.ledger.create_registration_invite("ABCDEF", "worker"),
                "login": login,
                "server": server,
                "account_info": account_info,
                "terminal_info": terminal_info,
                "mt5_password": password,
                "enrollment_challenge": challenge,
                "public_key_pem": public_key_pem,
                "proof_signature": base64.b64encode(signature).decode("ascii"),
            },
            timeout=5,
        )
        self._case.assertEqual(201, response.status_code)
        session_cookie, csrf = self._admin_session()
        approval = httpx.post(
            f"{self._base_url}/api/admin/enrollments/{response.json()['enrollment_id']}/approve",
            headers={"Cookie": session_cookie, "X-CSRF-Token": csrf},
            timeout=5,
        )
        self._case.assertEqual(200, approval.status_code)
        worker_id = approval.json()["worker_id"]
        certificate = self._app.state.ledger.active_worker(worker_id).certificate
        return private_key, worker_id, certificate

    def launch_catalog_analysis(
        self, first_worker_id: str, second_worker_id: str, policy: dict[str, object]
    ) -> dict[str, object]:
        session_cookie, csrf = self._admin_session()
        response = httpx.post(
            f"{self._base_url}/api/admin/product-catalog-analyses",
            headers={"Cookie": session_cookie, "X-CSRF-Token": csrf},
            json={"first_worker_id": first_worker_id, "second_worker_id": second_worker_id, "policy": policy},
            timeout=10,
        )
        self._case.assertEqual(201, response.status_code)
        return response.json()

    def read_catalog_analysis(self, analysis_id: str) -> dict[str, object]:
        session_cookie, _csrf = self._admin_session()
        response = httpx.get(
            f"{self._base_url}/api/admin/product-catalog-analyses/{analysis_id}",
            headers={"Cookie": session_cookie},
            timeout=5,
        )
        self._case.assertEqual(200, response.status_code)
        return response.json()

    def request_product_pair_build_confirmation(
        self,
        analysis_id: str,
        first_symbol: str,
        second_symbol: str,
    ) -> dict[str, object]:
        session_cookie, csrf = self._admin_session()
        response = httpx.post(
            f"{self._base_url}/api/admin/product-catalog-analyses/{analysis_id}/product-pair-build-confirmations",
            headers={"Cookie": session_cookie, "X-CSRF-Token": csrf},
            json={"first_symbol": first_symbol, "second_symbol": second_symbol},
            timeout=5,
        )
        self._case.assertEqual(201, response.status_code)
        return response.json()

    def build_product_pair(self, confirmation_id: str, *, expected_status: int = 201) -> httpx.Response:
        session_cookie, csrf = self._admin_session()
        response = httpx.post(
            f"{self._base_url}/api/admin/product-pairs",
            headers={"Cookie": session_cookie, "X-CSRF-Token": csrf},
            json={"confirmation_id": confirmation_id},
            timeout=5,
        )
        self._case.assertEqual(expected_status, response.status_code)
        return response

    def replace_product_pair(self, product_pair_id: str, confirmation_id: str, *, expected_status: int = 201) -> httpx.Response:
        session_cookie, csrf = self._admin_session()
        response = httpx.post(
            f"{self._base_url}/api/admin/product-pairs/{product_pair_id}/replace",
            headers={"Cookie": session_cookie, "X-CSRF-Token": csrf},
            json={"confirmation_id": confirmation_id},
            timeout=5,
        )
        self._case.assertEqual(expected_status, response.status_code)
        return response

    def retire_product_pair(self, product_pair_id: str, *, expected_status: int = 200) -> httpx.Response:
        session_cookie, csrf = self._admin_session()
        response = httpx.post(
            f"{self._base_url}/api/admin/product-pairs/{product_pair_id}/retire",
            headers={"Cookie": session_cookie, "X-CSRF-Token": csrf},
            timeout=5,
        )
        self._case.assertEqual(expected_status, response.status_code)
        return response

    def list_product_pairs(self) -> list[dict[str, object]]:
        session_cookie, _csrf = self._admin_session()
        response = httpx.get(
            f"{self._base_url}/api/admin/product-pairs",
            headers={"Cookie": session_cookie},
            timeout=5,
        )
        self._case.assertEqual(200, response.status_code)
        return response.json()["items"]

    def launch_product_pair_retest(
        self,
        product_pair_id: str,
        first_worker_id: str,
        second_worker_id: str,
        *,
        expected_status: int = 201,
    ) -> dict[str, object]:
        session_cookie, csrf = self._admin_session()
        response = httpx.post(
            f"{self._base_url}/api/admin/product-pairs/{product_pair_id}/retests",
            headers={"Cookie": session_cookie, "X-CSRF-Token": csrf},
            json={"first_worker_id": first_worker_id, "second_worker_id": second_worker_id},
            timeout=10,
        )
        self._case.assertEqual(expected_status, response.status_code)
        return response.json()

    def check_product_pair_worker_compatibility(
        self,
        product_pair_id: str,
        worker_id: str,
        *,
        expected_status: int = 200,
    ) -> dict[str, object]:
        session_cookie, csrf = self._admin_session()
        response = httpx.post(
            f"{self._base_url}/api/admin/product-pairs/{product_pair_id}/workers/{worker_id}/compatibility-check",
            headers={"Cookie": session_cookie, "X-CSRF-Token": csrf},
            timeout=10,
        )
        self._case.assertEqual(expected_status, response.status_code)
        return response.json()

    def exclude_product_pair_worker(
        self,
        product_pair_id: str,
        worker_id: str,
        *,
        expected_status: int = 200,
    ) -> dict[str, object]:
        session_cookie, csrf = self._admin_session()
        response = httpx.post(
            f"{self._base_url}/api/admin/product-pairs/{product_pair_id}/workers/{worker_id}/exclude",
            headers={"Cookie": session_cookie, "X-CSRF-Token": csrf},
            timeout=5,
        )
        self._case.assertEqual(expected_status, response.status_code)
        return response.json()

    def list_events(self) -> list[dict[str, object]]:
        session_cookie, _csrf = self._admin_session()
        response = httpx.get(
            f"{self._base_url}/api/admin/events",
            headers={"Cookie": session_cookie},
            timeout=5,
        )
        self._case.assertEqual(200, response.status_code)
        return response.json()["items"]

    def list_alerts(self) -> list[dict[str, object]]:
        session_cookie, _csrf = self._admin_session()
        response = httpx.get(
            f"{self._base_url}/api/admin/alerts",
            headers={"Cookie": session_cookie},
            timeout=5,
        )
        self._case.assertEqual(200, response.status_code)
        return response.json()

    def respond_to_catalog_analysis(
        self,
        private_key: ec.EllipticCurvePrivateKey,
        worker_id: str,
        certificate: str,
        symbols: list[dict[str, object]] | None,
        *,
        request_key: str,
        collected_at: str = "2026-08-17T07:00:00+00:00",
        market_data_collected_at: str = "2026-08-17T07:05:00+00:00",
        market_data_by_symbol: dict[str, list[dict[str, object]]] | None = None,
        market_data_by_stage: dict[str, dict[str, list[dict[str, object]]]] | None = None,
        market_data_responses: list[dict[str, list[dict[str, object]]] | None] | None = None,
        market_data_error: str | None = None,
        market_data_error_by_stage: dict[str, str] | None = None,
        ready_event: threading.Event | None = None,
    ) -> None:
        with websocket_connect(f"ws://127.0.0.1:{self.port}/api/worker/session") as websocket:
            self.authenticate_worker_socket(websocket, private_key, worker_id, certificate)
            if ready_event is not None:
                ready_event.set()
            pending_market_data = list(market_data_responses or [])
            while True:
                try:
                    request = json.loads(websocket.recv())
                except ConnectionClosed:
                    return
                self.requests.setdefault(request_key, []).append(request)
                stage = request.get("stage", "catalog")
                if stage == "catalog":
                    response = {
                        "type": "product_catalog_analysis_response",
                        "stage": "catalog",
                        "analysis_id": request["analysis_id"],
                        "request_id": request["request_id"],
                        "collected_at": collected_at,
                    }
                    if symbols is not None:
                        response["symbols"] = symbols
                    websocket.send(json.dumps(response))
                    if (
                        market_data_by_symbol is None
                        and market_data_by_stage is None
                        and market_data_error is None
                        and market_data_error_by_stage is None
                        and not pending_market_data
                    ):
                        return
                    continue
                if stage in {"m15_screening", "m1_verification"}:
                    error = market_data_error if market_data_error_by_stage is None else market_data_error_by_stage.get(stage)
                    if error is not None:
                        websocket.send(json.dumps({
                            "type": "product_catalog_analysis_error",
                            "analysis_id": request["analysis_id"],
                            "request_id": request["request_id"],
                            "stage": stage,
                            "timeframe": request["timeframe"],
                            "reason": error,
                        }))
                        continue
                    response_payload = (
                        pending_market_data.pop(0)
                        if pending_market_data
                        else None if market_data_by_stage is None else market_data_by_stage.get(stage, {})
                    )
                    if response_payload is None and market_data_by_stage is None:
                        response_payload = market_data_by_symbol
                    websocket.send(json.dumps(self.market_data_response(
                        request,
                        response_payload,
                        collected_at=market_data_collected_at,
                    )))
                    if pending_market_data:
                        continue
                    if market_data_by_stage is not None and stage != "m1_verification":
                        continue
                    if market_data_by_stage is None:
                        return
                    return
                raise AssertionError(f"Unexpected analysis stage: {stage}")

    def run_reconciliation_worker(
        self,
        private_key: ec.EllipticCurvePrivateKey,
        worker_id: str,
        certificate: str,
        mt5: RuntimeAnalysisMT5,
        ready_event: threading.Event,
    ) -> None:
        with websocket_connect(f"ws://127.0.0.1:{self.port}/api/worker/session") as websocket:
            self.authenticate_worker_socket(websocket, private_key, worker_id, certificate)
            ready_event.set()
            try:
                reconcile_authenticated_worker(
                    mt5=mt5,
                    session=FiniteAuthenticatedWorkerSession(websocket, reconciliation_cursor=0),
                    login=mt5.login_id,
                    server=mt5.server,
                )
            except (ConnectionClosed, StopIteration):
                pass

    def authenticate_worker_socket(
        self,
        websocket: object,
        private_key: ec.EllipticCurvePrivateKey,
        worker_id: str,
        certificate: str,
    ) -> None:
        websocket.send(json.dumps({"worker_id": worker_id, "certificate": certificate}))
        challenge = json.loads(websocket.recv())
        signature = private_key.sign(
            worker_proof_payload(purpose="worker_session", worker_id=worker_id, nonce=challenge["nonce"]),
            ec.ECDSA(hashes.SHA256()),
        )
        websocket.send(json.dumps({"signature": base64.b64encode(signature).decode("ascii")}))
        self._case.assertEqual(
            {"type": "authenticated", "worker_id": worker_id, "cursor": 0},
            json.loads(websocket.recv()),
        )

    def market_data_response(
        self,
        request: dict[str, object],
        market_data_by_symbol: dict[str, list[dict[str, object]]] | None,
        *,
        collected_at: str = "2026-08-17T07:05:00+00:00",
    ) -> dict[str, object]:
        response: dict[str, object] = {
            "type": "product_catalog_analysis_response",
            "stage": request["stage"],
            "analysis_id": request["analysis_id"],
            "request_id": request["request_id"],
            "collected_at": collected_at,
            "timeframe": request["timeframe"],
            "period_start_utc": request["period_start_utc"],
            "period_end_utc": request["period_end_utc"],
        }
        if market_data_by_symbol is not None:
            period_start = datetime.fromisoformat(str(request["period_start_utc"]).replace("Z", "+00:00"))
            preserve_timestamps = all(
                all(isinstance(bar.get("time_utc"), str) for bar in bars)
                for bars in market_data_by_symbol.values()
            )
            response["symbols"] = [
                {
                    "symbol": symbol,
                    "bars": [
                        {
                            **bar,
                            "time": int(bar["time"]) if preserve_timestamps else int((period_start + timedelta(days=index)).timestamp()),
                            "time_utc": str(bar["time_utc"]) if preserve_timestamps else (period_start + timedelta(days=index)).isoformat().replace("+00:00", "Z"),
                        }
                        for index, bar in enumerate(
                            bars if preserve_timestamps else (bars + [dict(bars[-1])] * max(0, 5 - len(bars)))[:5]
                        )
                    ],
                    "time_metadata": {
                        "source_family": "market_data",
                        "offset_layer": "market_data_calibration",
                        "offset_seconds_used": 0,
                        "calibration_status": "calibrated",
                        "calibration": {
                            "family": "market_data",
                            "status": "calibrated",
                            "offset_seconds": 0,
                            "offset_layer": "market_data_calibration",
                            "calibrated_local_date": collected_at[:10],
                            "calibrated_at_utc": collected_at,
                            "calibration_symbol": symbol,
                            "sample_count": 3,
                            "samples": [
                                {
                                    "source": "symbol_info_tick.time",
                                    "calibrated_at_utc": collected_at,
                                    "offset_seconds": 0,
                                    "error_seconds": 0.2,
                                    "symbol": symbol,
                                }
                            ],
                        },
                    },
                }
                for symbol, bars in market_data_by_symbol.items()
            ]
        return response

    def _admin_session(self) -> tuple[str, str]:
        response = httpx.post(
            f"{self._base_url}/api/admin/login",
            json={"username": "ABCDEF", "password": "A-secure-admin-password!"},
            timeout=5,
        )
        self._case.assertEqual(200, response.status_code)
        cookie = http.cookies.SimpleCookie()
        cookie.load(response.headers["set-cookie"])
        token = cookie["abt_admin_session"].value
        return f"abt_admin_session={token}", response.json()["csrf_token"]


class FiniteAuthenticatedWorkerSession(AuthenticatedWorkerSession):
    def __init__(self, socket: object, reconciliation_cursor: int) -> None:
        super().__init__(socket, reconciliation_cursor)
        self._analysis_response_count = 0

    def send_product_catalog_analysis(self, **response: object) -> None:
        super().send_product_catalog_analysis(**response)
        self._analysis_response_count += 1
        if self._analysis_response_count == 3:
            raise StopIteration


class RuntimeAnalysisMT5:
    TIMEFRAME_M15 = "M15"
    TIMEFRAME_M1 = "M1"

    def __init__(self, login_id: int, server: str, symbol: str, price_offset: float) -> None:
        self.login_id = login_id
        self.server = server
        self.symbol = symbol
        self.price_offset = price_offset
        self.broker_write_calls = 0

    def initialize(self) -> bool:
        return True

    def login(self, login: int, *, password: str, server: str) -> bool:
        return login == self.login_id and bool(password) and server == self.server

    def shutdown(self) -> None:
        pass

    def account_info(self) -> object:
        return {"login": self.login_id, "server": self.server}

    def terminal_info(self) -> object:
        return {"connected": True, "trade_allowed": False}

    def orders_get(self) -> object:
        return []

    def positions_get(self) -> object:
        return []

    def symbols_get(self) -> object:
        return [{
            "name": self.symbol,
            "trade_calc_mode": "FOREX",
            "currency_base": "EUR",
            "currency_profit": "USD",
            "digits": 5,
            "point": 0.00001,
            "trade_tick_size": 0.00001,
            "trade_contract_size": 100000.0,
            "volume_min": 0.01,
            "volume_step": 0.01,
            "volume_max": 100.0,
            "filling_mode": 3,
            "order_mode": 3,
            "trade_stops_level": 0,
            "trade_freeze_level": 0,
            "trade_tick_value": 1.0,
            "currency_margin": "USD",
            "swap_long": 0.0,
            "swap_short": 0.0,
            "swap_mode": 1,
            "swap_rollover3days": 3,
        }]

    def copy_rates_range(self, symbol: str, timeframe: object, from_time: datetime, to_time: datetime) -> object:
        self._last_market_epoch = int(datetime.now(UTC).timestamp())
        return [
            {
                "time": int((from_time + timedelta(days=offset)).timestamp()),
                "open": 1.10000 + (0.00010 * offset * (offset + 1) / 2) + self.price_offset,
                "high": 1.10100 + (0.00010 * offset * (offset + 1) / 2) + self.price_offset,
                "low": 1.09900 + (0.00010 * offset * (offset + 1) / 2) + self.price_offset,
                "close": 1.10050 + (0.00010 * offset * (offset + 1) / 2) + self.price_offset,
            }
            for offset in range(5)
        ]

    def symbol_info_tick(self, symbol: str) -> object:
        return {"time": getattr(self, "_last_market_epoch", int(datetime.now(UTC).timestamp()))}

    def order_send(self, *_: object, **__: object) -> object:
        self.broker_write_calls += 1
        raise AssertionError("Broker writes are prohibited.")
