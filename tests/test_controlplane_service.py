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
import abt.controlplane.service as controlplane_service
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4
from typing import cast

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
from abt.trader_protocol import MAX_PAIR_RELAY_ENVELOPE_BYTES, PAIR_CELL_CONTROL_MESSAGE_TYPES
from abt.controlplane.service import (
    _broadcast_trader_worker_stream,
    _delete_expired_pending_secrets,
    _is_authoritative_worker_session,
    _request_pair_cell_quarantine_release,
    _TraderSessionConnection,
    _WorkerSessionConnection,
    _trader_market_subscription,
    _validate_worker_stream_envelope,
    _worker_fact_envelope,
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
    def test_controller_validates_only_worker_fact_delivery_metadata(self) -> None:
        envelope = {
            "type": "worker_broker_delta",
            "protocol_version": 1,
            "worker_id": "worker-1",
            "recovery_epoch": "epoch-1",
            "event_id": 9,
            "future_payload": {"unknown_order_semantics": ["opaque", 1]},
        }

        self.assertIs(
            envelope,
            _worker_fact_envelope(
                {"type": "worker_fact", "cursor": 9, "envelope": envelope},
                "worker-1",
            ),
        )

    def test_relays_opaque_worker_stream_by_subscription(self) -> None:
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

        envelope = {
            "type": "live_state_snapshot",
            "future_payload": {"controller_must_not_interpret": [1, "opaque"]},
        }
        _validate_worker_stream_envelope(envelope)
        asyncio.run(
            _broadcast_trader_worker_stream(
                {"trader-1": {selected, all_workers}, "trader-2": {ignored}},
                "worker-1",
                envelope,
            )
        )

        expected = {
            "type": "worker_stream",
            "worker_id": "worker-1",
            "envelope": envelope,
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


class PairCellQuarantineReleaseRequestHelperTests(unittest.TestCase):
    """Focused, non-websocket coverage of the correlated request/response
    helper itself: it must return exactly what the Worker reports, and it
    must never turn "we don't know" (a timeout or a disconnect) into a false
    "applied"."""

    @staticmethod
    def _release() -> dict[str, object]:
        return {
            "symbol": "EURUSD",
            "actor": "alice",
            "reason": "broker confirmed the symbol is tradable again",
            "observed_at": "2024-01-01T00:00:00+00:00",
        }

    def test_returns_the_workers_own_reported_outcome(self) -> None:
        connection = _WorkerSessionConnection(websocket=None)  # type: ignore[arg-type]

        async def _drive() -> str:
            task = asyncio.ensure_future(
                _request_pair_cell_quarantine_release(connection, self._release(), request_id="req-1")
            )
            await asyncio.sleep(0)  # allow the request to be enqueued
            queued = connection.outbound.get_nowait()
            self.assertEqual("req-1", queued.request_id)
            queued.future.set_result(
                {"type": "pair_cell_quarantine_release_result", "request_id": "req-1", "symbol": "EURUSD", "outcome": "rejected"}
            )
            return await task

        self.assertEqual("rejected", asyncio.run(_drive()))

    def test_a_timeout_is_reported_as_unknown_never_applied(self) -> None:
        connection = _WorkerSessionConnection(websocket=None)  # type: ignore[arg-type]
        with patch.object(
            controlplane_service, "_request_worker_relay", side_effect=asyncio.TimeoutError()
        ):
            outcome = asyncio.run(
                _request_pair_cell_quarantine_release(connection, self._release(), request_id="req-1")
            )

        self.assertEqual("unknown", outcome)

    def test_a_mid_flight_disconnect_is_reported_as_unknown_never_applied(self) -> None:
        connection = _WorkerSessionConnection(websocket=None)  # type: ignore[arg-type]
        with patch.object(
            controlplane_service, "_request_worker_relay", side_effect=LedgerError("Worker disconnected.")
        ):
            outcome = asyncio.run(
                _request_pair_cell_quarantine_release(connection, self._release(), request_id="req-1")
            )

        self.assertEqual("unknown", outcome)

    def test_a_malformed_or_unexpected_reply_is_reported_as_unknown(self) -> None:
        connection = _WorkerSessionConnection(websocket=None)  # type: ignore[arg-type]

        async def _drive() -> str:
            task = asyncio.ensure_future(
                _request_pair_cell_quarantine_release(connection, self._release(), request_id="req-1")
            )
            await asyncio.sleep(0)
            queued = connection.outbound.get_nowait()
            queued.future.set_result({"type": "pair_cell_quarantine_release_result", "request_id": "req-1", "symbol": "EURUSD"})
            return await task

        self.assertEqual("unknown", asyncio.run(_drive()))



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

    def test_management_trading_routes_are_removed(self) -> None:
        for method, path in (
            ("GET", "/api/admin/intents"),
            ("POST", "/api/admin/intents"),
            ("GET", "/api/admin/product-catalog-analyses"),
            ("POST", "/api/admin/product-catalog-analyses"),
            ("GET", "/api/admin/product-pairs"),
            ("POST", "/api/admin/product-pairs"),
            ("POST", "/api/admin/manual-trades"),
            ("POST", "/api/admin/trades/cancel"),
            ("POST", "/api/admin/trades/flatten"),
            ("POST", "/api/admin/positions/close"),
            ("GET", "/api/admin/worker-snapshots"),
        ):
            with self.subTest(method=method, path=path):
                self.assertEqual(404, self.client.request(method, path).status_code)
        for name in (
            "TraderIntentPayload",
            "HedgedEntryPayload",
            "_execute_hedged_entry",
            "_preflight_trader_intent",
            "_automatically_converge_accounts",
            "_request_order_execute",
        ):
            self.assertFalse(hasattr(controlplane_service, name), name)

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
            response = client.get("/")
            self.assertEqual(200, response.status_code)
            self.assertIn("Management access", response.text)
            self.assertEqual(404, client.get("/manual-trading").status_code)
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

    def test_operations_dashboard_requires_an_admin_and_classifies_current_operational_state(self) -> None:
        self.assertEqual(401, self.client.get("/api/admin/operations-dashboard").status_code)

        pending_enrollment_id = self._create_pending_enrollment()
        _private_key, worker_id, _certificate = self._approved_worker(654321, "Broker-Live")
        self.assertEqual(
            200,
            self.client.post(
                "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
            ).status_code,
        )

        response = self.client.get("/api/admin/operations-dashboard")

        self.assertEqual(200, response.status_code)
        dashboard = response.json()
        self.assertNotIn("paired_trade_lifecycle", dashboard)
        self.assertNotIn("product_pairs", dashboard)
        enrollment_alert = next(
            alert for alert in dashboard["alerts"] if alert["alert_type"] == "worker_enrollment_pending_approval"
        )
        self.assertEqual("intervention_required", enrollment_alert["category"])
        self.assertEqual("administrator_approval_required", enrollment_alert["classification_reason"])
        self.assertEqual(pending_enrollment_id, enrollment_alert["enrollment_id"])
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
        worker = next(worker for worker in dashboard["workers"] if worker["worker_id"] == worker_id)
        self.assertEqual("intervention_required", worker["category"])
        self.assertEqual("worker_stale", worker["classification_reason"])
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

        lifecycle_events = [
            event for event in self.app.state.ledger.events()
            if event["event_type"] in {"worker_session_authenticated", "worker_session_superseded", "worker_session_disconnected"}
            and event["payload"].get("worker_id") == worker_id
        ]
        authenticated = [event for event in lifecycle_events if event["event_type"] == "worker_session_authenticated"]
        superseded = [event for event in lifecycle_events if event["event_type"] == "worker_session_superseded"]
        disconnected = [
            event for event in lifecycle_events
            if event["event_type"] == "worker_session_disconnected"
            and event["payload"]["disposition"] == "superseded"
        ]
        self.assertGreaterEqual(len(authenticated), 2)
        self.assertEqual(1, len(superseded))
        self.assertEqual(1, len(disconnected))
        self.assertEqual(superseded[0]["payload"]["session_id"], disconnected[0]["payload"]["session_id"])
        self.assertIn(superseded[0]["payload"]["session_id"], {event["payload"]["session_id"] for event in authenticated})
        self.assertIn(superseded[0]["payload"]["replacement_session_id"], {event["payload"]["session_id"] for event in authenticated})

    def test_superseded_worker_session_cannot_write_reconciliation_cursor(self) -> None:
        _private_key, worker_id, _certificate = self._approved_worker(123456, "Broker-A")
        authoritative = _WorkerSessionConnection(object())  # type: ignore[arg-type]
        superseded = _WorkerSessionConnection(object(), superseded=True)  # type: ignore[arg-type]
        connections = {worker_id: {authoritative}}

        self.assertTrue(_is_authoritative_worker_session(connections, worker_id, authoritative))
        self.assertFalse(_is_authoritative_worker_session(connections, worker_id, superseded))

    def _pair_route(self, leader_login: int, follower_login: int) -> tuple[
        ec.EllipticCurvePrivateKey, str, str, ec.EllipticCurvePrivateKey, str, str, str
    ]:
        """Form one durable route through the two-phase pairing workflow.

        There is no administrator route-creation surface, so a test route is
        made exactly the way Workers make one: reserve both Workers under a
        unique proposal, then compare-and-swap the route into existence.
        """

        leader_key, leader_id, leader_certificate = self._approved_worker(leader_login, "Broker-A")
        follower_key, follower_id, follower_certificate = self._approved_worker(follower_login, "Broker-B")
        ledger = self.app.state.ledger
        reservation = ledger.reserve_pair_route_proposal(
            leader_worker_id=leader_id,
            follower_worker_id=follower_id,
            connected_worker_ids={leader_id, follower_id},
        )
        route = ledger.create_pair_route(
            proposal_id=reservation["proposal_id"], connected_worker_ids={leader_id, follower_id}
        )
        return (
            leader_key, leader_id, leader_certificate,
            follower_key, follower_id, follower_certificate, route["route_id"],
        )

    @staticmethod
    def _relay_envelope(
        leader_id: str, follower_id: str, route_id: str, **overrides: object
    ) -> dict[str, object]:
        envelope: dict[str, object] = {
            "type": "pair_cell_relay",
            "protocol_version": 1,
            "route": {
                "route_id": route_id,
                "leader_worker_id": leader_id,
                "follower_worker_id": follower_id,
            },
            "from_worker_id": leader_id,
            "to_worker_id": follower_id,
            "request_id": "message-1",
            "payload_hash": "unused-by-controller",
            "payload": {"kind": "entry_attempt", "opaque_to_controller": True},
        }
        envelope.update(overrides)
        return envelope

    def _pair_cell_request(self, socket: object, message: dict[str, object]) -> dict[str, object]:
        """Send one pairing control message and read its correlated reply."""

        socket.send_json(message)
        while True:
            response = socket.receive_json()
            if response.get("request_id") == message["request_id"]:
                return cast(dict[str, object], response)

    @staticmethod
    def _acceptance_payload(proposal_id: str, follower_worker_id: str) -> dict[str, object]:
        """A payload shaped like the core's canonical Pairing Acceptance.

        The controller must forward it unchanged and never read a field of
        it, so these tests only ever assert that it arrives byte-for-byte.
        """

        return {
            "proposal_id": proposal_id,
            "worker_id": follower_worker_id,
            "startup_balance_usd": "10000.00",
            "account_currency": "USD",
            "maximum_margin_fraction": "0.10",
            "daily_loss_fraction": "0.03",
            "trade_loss_fraction": "0.02",
            "maximum_loss_per_trade_usd": "40",
        }

    def _acceptance(
        self, request_id: str, proposal_id: str, follower_worker_id: str, **overrides: object
    ) -> dict[str, object]:
        message: dict[str, object] = {
            "type": "pair_cell_pairing_decision",
            "request_id": request_id,
            "proposal_id": proposal_id,
            "accepted": True,
            "acceptance_payload": self._acceptance_payload(proposal_id, follower_worker_id),
            "payload_hash": "acceptance-hash-1",
        }
        message.update(overrides)
        return message

    def test_pair_relay_forwards_an_opaque_envelope_unchanged_on_a_live_route(self) -> None:
        """Requirement: the controller authenticates both Workers, checks the
        live route record keyed on ``route_id`` and sending role, and forwards
        the envelope byte-for-byte. It parses no trading field and reports
        only ordinary delivery, never lifecycle state."""

        (
            leader_key, leader_id, leader_certificate,
            follower_key, follower_id, follower_certificate, route_id,
        ) = self._pair_route(111111, 222222)
        envelope = self._relay_envelope(
            leader_id, follower_id, route_id,
            payload={
                "kind": "entry_attempt",
                "attempt": {"attempt_id": "attempt-1", "lots": "0.37", "policy_hash": "abc"},
                "universe": {"universe_generation": "generation-1", "symbols": ["EURUSD"]},
                "protection": {"sl": "1.0942", "tp": "1.1058"},
            },
        )

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)

                leader_socket.send_json({"type": "pair_relay", "request_id": "relay-1", "envelope": envelope})

                delivered = follower_socket.receive_json()
                ack = leader_socket.receive_json()

        self.assertEqual({"type": "pair_relay_deliver", "envelope": envelope}, delivered)
        self.assertEqual(
            {"type": "pair_relay_ack", "request_id": "relay-1", "accepted": True, "result": {"delivered": True}},
            ack,
        )
        self.assertNotIn("trader_id", json.dumps(delivered))

    def test_pair_relay_preserves_emission_order_within_one_live_session(self) -> None:
        """Requirement: order is preserved within each live route session."""

        (
            leader_key, leader_id, leader_certificate,
            follower_key, follower_id, follower_certificate, route_id,
        ) = self._pair_route(112233, 223344)
        envelopes = [
            self._relay_envelope(
                leader_id, follower_id, route_id,
                request_id=f"message-{index}",
                payload={"kind": "quote", "sequence": index},
            )
            for index in range(5)
        ]

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)

                for envelope in envelopes:
                    leader_socket.send_json({"type": "pair_relay", "request_id": envelope["request_id"], "envelope": envelope})
                delivered = [follower_socket.receive_json() for _ in envelopes]

        self.assertEqual([{"type": "pair_relay_deliver", "envelope": envelope} for envelope in envelopes], delivered)

    def test_pair_relay_never_replays_an_execution_envelope_into_a_reconnected_session(self) -> None:
        """Requirement: an envelope that cannot be delivered now is rejected to
        its sender, never buffered. A reconnect opens a brand-new ordered
        session that receives only messages emitted after it began."""

        (
            leader_key, leader_id, leader_certificate,
            follower_key, follower_id, follower_certificate, route_id,
        ) = self._pair_route(334455, 445566)
        missed = self._relay_envelope(
            leader_id, follower_id, route_id, request_id="missed-1", payload={"kind": "entry_attempt", "sequence": 1}
        )
        fresh = self._relay_envelope(
            leader_id, follower_id, route_id, request_id="fresh-1", payload={"kind": "entry_attempt", "sequence": 2}
        )

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)

            leader_socket.send_json({"type": "pair_relay", "request_id": "relay-missed", "envelope": missed})
            rejected = leader_socket.receive_json()

            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)

                leader_socket.send_json({"type": "pair_relay", "request_id": "relay-fresh", "envelope": fresh})
                delivered = follower_socket.receive_json()
                leader_socket.receive_json()

        self.assertFalse(rejected["accepted"])
        self.assertEqual({"type": "pair_relay_deliver", "envelope": fresh}, delivered)

    def test_pair_relay_forwards_a_resent_message_instead_of_deduplicating_it(self) -> None:
        """Requirement: the controller holds no attempt projection, so it never
        suppresses or replays a message. Duplicate suppression belongs to the
        receiving Worker's durable journal."""

        (
            leader_key, leader_id, leader_certificate,
            follower_key, follower_id, follower_certificate, route_id,
        ) = self._pair_route(556677, 667788)
        envelope = self._relay_envelope(leader_id, follower_id, route_id)

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)

                for request_id in ("relay-1", "relay-2"):
                    leader_socket.send_json({"type": "pair_relay", "request_id": request_id, "envelope": envelope})
                delivered = [follower_socket.receive_json() for _ in range(2)]
                acks = [leader_socket.receive_json() for _ in range(2)]

        self.assertEqual([{"type": "pair_relay_deliver", "envelope": envelope}] * 2, delivered)
        self.assertEqual([{"delivered": True}] * 2, [ack["result"] for ack in acks])

    def test_pair_relay_rejects_a_worker_outside_the_route(self) -> None:
        (
            leader_key, leader_id, leader_certificate,
            _follower_key, follower_id, _follower_certificate, route_id,
        ) = self._pair_route(333333, 555555)
        _outsider_key, outsider_id, _outsider_certificate = self._approved_worker(444444, "Broker-C")
        envelope = self._relay_envelope(leader_id, follower_id, route_id, to_worker_id=outsider_id)

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            leader_socket.send_json({"type": "pair_relay", "request_id": "relay-1", "envelope": envelope})
            ack = leader_socket.receive_json()

        self.assertFalse(ack["accepted"])

    def test_pair_relay_rejects_an_unknown_route_id(self) -> None:
        """Requirement: forwarding requires a live route record; two
        authenticated Workers alone are not enough."""

        leader_key, leader_id, leader_certificate = self._approved_worker(717171, "Broker-A")
        _follower_key, follower_id, _follower_certificate = self._approved_worker(727272, "Broker-B")
        envelope = self._relay_envelope(leader_id, follower_id, "route-that-never-existed")

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            leader_socket.send_json({"type": "pair_relay", "request_id": "relay-1", "envelope": envelope})
            ack = leader_socket.receive_json()

        self.assertFalse(ack["accepted"])

    def test_pair_relay_rejects_a_stale_route_id_after_a_safe_unpair(self) -> None:
        """Requirement: a ``route_id`` that named a removed route is refused;
        it is never revived, and it is never reissued to a later pairing."""

        (
            leader_key, leader_id, leader_certificate,
            _follower_key, follower_id, _follower_certificate, route_id,
        ) = self._pair_route(313131, 323232)
        ledger = self.app.state.ledger
        ledger.enter_pair_route_unpairing(route_id=route_id, worker_id=leader_id)
        ledger.record_pair_unpair_assertion(route_id=route_id, worker_id=leader_id, state_version=1)
        ledger.record_pair_unpair_assertion(route_id=route_id, worker_id=follower_id, state_version=1)
        envelope = self._relay_envelope(leader_id, follower_id, route_id)

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            leader_socket.send_json({"type": "pair_relay", "request_id": "relay-1", "envelope": envelope})
            ack = leader_socket.receive_json()

        self.assertFalse(ack["accepted"])

    def test_pair_relay_rejects_a_role_the_route_record_never_assigned(self) -> None:
        """Requirement: the controller's route record is authoritative for
        the assigned role, so a follower cannot promote itself by claiming to
        be the leader."""

        (
            _leader_key, leader_id, _leader_certificate,
            follower_key, follower_id, follower_certificate, route_id,
        ) = self._pair_route(343434, 353535)
        swapped = self._relay_envelope(
            follower_id, leader_id, route_id, from_worker_id=follower_id, to_worker_id=leader_id
        )

        with self.client.websocket_connect("/api/worker/session") as follower_socket:
            self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)
            follower_socket.send_json({"type": "pair_relay", "request_id": "relay-1", "envelope": swapped})
            ack = follower_socket.receive_json()

        self.assertFalse(ack["accepted"])

    def test_pair_relay_rejects_a_sender_impersonating_its_peer(self) -> None:
        """Requirement: both Workers are authenticated and the envelope's
        declared sender must be the authenticated session that sent it."""

        (
            _leader_key, leader_id, _leader_certificate,
            follower_key, follower_id, follower_certificate, route_id,
        ) = self._pair_route(818181, 828282)
        spoofed = self._relay_envelope(
            leader_id, follower_id, route_id, from_worker_id=leader_id, to_worker_id=follower_id
        )

        with self.client.websocket_connect("/api/worker/session") as follower_socket:
            self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)
            follower_socket.send_json({"type": "pair_relay", "request_id": "relay-1", "envelope": spoofed})
            ack = follower_socket.receive_json()

        self.assertFalse(ack["accepted"])

    def test_pair_relay_rejects_an_unsupported_protocol_version(self) -> None:
        (
            leader_key, leader_id, leader_certificate,
            _follower_key, follower_id, _follower_certificate, route_id,
        ) = self._pair_route(838383, 848484)
        envelope = self._relay_envelope(leader_id, follower_id, route_id, protocol_version=2)

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            leader_socket.send_json({"type": "pair_relay", "request_id": "relay-1", "envelope": envelope})
            ack = leader_socket.receive_json()

        self.assertFalse(ack["accepted"])

    def test_pair_relay_rejects_an_envelope_over_the_payload_size_limit(self) -> None:
        (
            leader_key, leader_id, leader_certificate,
            _follower_key, follower_id, _follower_certificate, route_id,
        ) = self._pair_route(858585, 868686)
        envelope = self._relay_envelope(
            leader_id, follower_id, route_id,
            payload={"kind": "entry_attempt", "blob": "x" * (MAX_PAIR_RELAY_ENVELOPE_BYTES + 1)},
        )

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            leader_socket.send_json({"type": "pair_relay", "request_id": "relay-1", "envelope": envelope})
            ack = leader_socket.receive_json()

        self.assertFalse(ack["accepted"])

    def test_pair_relay_rejects_a_residual_lease_or_trader_claim(self) -> None:
        """Requirement: no lease dependency and no Trader identity survives
        anywhere on the Pair Cell wire."""

        (
            leader_key, leader_id, leader_certificate,
            _follower_key, follower_id, _follower_certificate, route_id,
        ) = self._pair_route(878787, 888888)
        legacy = self._relay_envelope(
            leader_id, follower_id, route_id,
            lease={
                "leader_worker_id": leader_id, "follower_worker_id": follower_id,
                "trader_id": "trader-1", "contract_hash": "hash-1", "lease_epoch": 1,
            },
            attempt_id="attempt-1",
        )
        with_trader = self._relay_envelope(
            leader_id, follower_id, route_id,
            route={
                "route_id": route_id, "leader_worker_id": leader_id,
                "follower_worker_id": follower_id, "trader_id": "trader-1",
            },
        )

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            acks = []
            for index, envelope in enumerate((legacy, with_trader)):
                leader_socket.send_json(
                    {"type": "pair_relay", "request_id": f"relay-{index}", "envelope": envelope}
                )
                acks.append(leader_socket.receive_json())

        self.assertEqual([False, False], [ack["accepted"] for ack in acks])

    def test_controller_exposes_no_lease_contract_or_administrator_route_surface(self) -> None:
        """Requirement: Pair Lease issuance, Pair Contract activation, and
        administrator route creation/revocation are gone from the controller
        entirely."""

        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        headers = {"X-CSRF-Token": login.json()["csrf_token"]}

        for path in (
            "/api/admin/pairs/activate",
            "/api/admin/pairs/route",
            "/api/admin/pairs/route/revoke",
        ):
            with self.subTest(path=path):
                self.assertEqual(404, self.client.post(path, headers=headers, json={}).status_code)
        for removed_model in (
            "PairContractActivationRequest", "PairRouteAuthorizationRequest", "PairRouteRevocationRequest",
        ):
            self.assertFalse(hasattr(controlplane_service, removed_model), removed_model)
        for removed in (
            "issue_pair_lease", "activate_pair_contract", "pair_cell_activation_for_worker",
            "authorize_pair_route", "revoke_pair_route", "authorized_pair_route",
        ):
            self.assertFalse(hasattr(self.app.state.ledger, removed), removed)
        self.assertNotIn(
            "/api/admin/pairs/route", {route.path for route in self.app.routes if hasattr(route, "path")}
        )

    def test_the_admin_execution_mode_surface_cannot_create_a_pair_cell_claim(self) -> None:
        """Requirement: Pair Cell exclusivity is claimed only inside the
        two-phase pairing workflow, never by an administrator."""

        _leader_key, leader_id, _leader_certificate = self._approved_worker(363636, "Broker-A")
        _follower_key, follower_id, _follower_certificate = self._approved_worker(373737, "Broker-B")
        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        headers = {"X-CSRF-Token": login.json()["csrf_token"]}

        response = self.client.post(
            "/api/admin/pairs/execution-mode",
            headers=headers,
            json={
                "leader_worker_id": leader_id, "follower_worker_id": follower_id,
                "trader_id": "trader-1", "mode": "pair_execution_cell",
            },
        )

        self.assertEqual(422, response.status_code)

    def test_worker_session_rejects_a_pair_cell_activation_sync_request(self) -> None:
        """Requirement: the controller distributes no Pair Contract, so there
        is nothing for a reconnecting Worker to re-sync."""

        leader_key, leader_id, leader_certificate = self._approved_worker(898989, "Broker-A")

        with self.assertRaises(WebSocketDisconnect):
            with self.client.websocket_connect("/api/worker/session") as leader_socket:
                self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
                leader_socket.send_json({"type": "pair_cell_sync_request"})
                leader_socket.receive_json()

    # ---- Worker-initiated pairing over the authenticated session ---- #

    def test_a_worker_that_declares_no_role_becomes_an_available_follower(self) -> None:
        """Requirement: an omitted role means the Worker is simply an
        available follower, and the leader's listing shows it."""

        leader_key, leader_id, leader_certificate = self._approved_worker(120001, "Broker-A")
        follower_key, follower_id, follower_certificate = self._approved_worker(120002, "Broker-B")

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)
                declared = self._pair_cell_request(
                    follower_socket, {"type": "pair_cell_role", "request_id": "declare-1"}
                )
                self._pair_cell_request(
                    leader_socket, {"type": "pair_cell_role", "request_id": "declare-2", "role": "leader"}
                )
                listed = self._pair_cell_request(
                    leader_socket, {"type": "pair_cell_available_followers", "request_id": "list-1"}
                )

        self.assertTrue(declared["accepted"])
        self.assertEqual("follower", declared["role"])
        self.assertIsNone(declared["declared_role"])
        self.assertIsNone(declared["route"])
        self.assertEqual([follower_id], [entry["worker_id"] for entry in listed["followers"]])
        self.assertNotIn("trader_id", json.dumps(listed))

    def test_only_currently_connected_followers_are_listed(self) -> None:
        """Requirement: the leader lists *currently connected* available
        followers; a known but disconnected Worker is not selectable."""

        leader_key, leader_id, leader_certificate = self._approved_worker(120011, "Broker-A")
        _absent_key, absent_id, _absent_certificate = self._approved_worker(120012, "Broker-B")
        follower_key, follower_id, follower_certificate = self._approved_worker(120013, "Broker-C")

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            alone = self._pair_cell_request(
                leader_socket, {"type": "pair_cell_available_followers", "request_id": "list-1"}
            )
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)
                joined = self._pair_cell_request(
                    leader_socket, {"type": "pair_cell_available_followers", "request_id": "list-2"}
                )

        self.assertEqual([], alone["followers"])
        self.assertEqual([follower_id], [entry["worker_id"] for entry in joined["followers"]])
        self.assertNotIn(absent_id, json.dumps(joined))

    def test_a_leader_pairs_with_a_selected_follower_through_two_phases(self) -> None:
        """Requirement: one transaction reserves both Workers under a unique
        ``proposal_id`` with a 30-second timeout, the controller forwards the
        proposal to the reserved follower, and a second transaction creates
        the durable route."""

        leader_key, leader_id, leader_certificate = self._approved_worker(120021, "Broker-A")
        follower_key, follower_id, follower_certificate = self._approved_worker(120022, "Broker-B")

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)

                proposed = self._pair_cell_request(
                    leader_socket,
                    {
                        "type": "pair_cell_pairing_proposal", "request_id": "propose-1",
                        "follower_worker_id": follower_id,
                    },
                )
                forwarded = follower_socket.receive_json()
                accepted = self._pair_cell_request(
                    follower_socket,
                    self._acceptance("decide-1", forwarded["proposal_id"], follower_id),
                )
                established = follower_socket.receive_json()
                leader_notice = leader_socket.receive_json()

        self.assertTrue(proposed["accepted"])
        self.assertEqual(30, proposed["timeout_seconds"])
        self.assertEqual("pair_cell_pairing_proposed", forwarded["type"])
        self.assertEqual(proposed["proposal_id"], forwarded["proposal_id"])
        self.assertEqual(leader_id, forwarded["leader_worker_id"])
        self.assertEqual("paired", accepted["outcome"])
        self.assertEqual(leader_id, accepted["route"]["leader_worker_id"])
        self.assertEqual(follower_id, accepted["route"]["follower_worker_id"])
        self.assertEqual("ACTIVE", accepted["route"]["state"])
        self.assertEqual("pair_cell_route_established", established["type"])
        self.assertEqual("pair_cell_route_established", leader_notice["type"])
        self.assertEqual(accepted["route"]["route_id"], leader_notice["route"]["route_id"])
        self.assertNotIn("trader_id", json.dumps(accepted))
        route = self.app.state.ledger.pair_route_for_worker(follower_id)
        assert route is not None
        self.assertEqual("follower", route["role"])

    def test_the_acceptance_payload_reaches_the_leader_unchanged(self) -> None:
        """Requirement: the follower authors its own frozen startup balance
        and its own four risk tunables and sends them to the leader through
        the opaque relay, which forwards them unchanged and never parses
        them. The leader may only transcribe them into ``follower_risk``."""

        leader_key, leader_id, leader_certificate = self._approved_worker(120211, "Broker-A")
        follower_key, follower_id, follower_certificate = self._approved_worker(120212, "Broker-B")

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)
                proposed = self._pair_cell_request(
                    leader_socket,
                    {
                        "type": "pair_cell_pairing_proposal", "request_id": "propose-1",
                        "follower_worker_id": follower_id,
                    },
                )
                follower_socket.receive_json()
                proposal_id = cast(str, proposed["proposal_id"])
                payload = {
                    **self._acceptance_payload(proposal_id, follower_id),
                    "unknown_future_block": {"nested": [1, {"deep": True}]},
                }

                accepted = self._pair_cell_request(
                    follower_socket,
                    {
                        "type": "pair_cell_pairing_decision", "request_id": "decide-1",
                        "proposal_id": proposal_id, "accepted": True,
                        "acceptance_payload": payload, "payload_hash": "acceptance-hash-9",
                    },
                )
                follower_notice = follower_socket.receive_json()
                leader_notice = leader_socket.receive_json()

        self.assertEqual("paired", accepted["outcome"])
        self.assertEqual(
            {
                "proposal_id": proposal_id,
                "from_worker_id": follower_id,
                "payload_hash": "acceptance-hash-9",
                "payload": payload,
            },
            leader_notice["acceptance"],
        )
        self.assertEqual(accepted["route"]["route_id"], leader_notice["route"]["route_id"])
        self.assertNotIn("acceptance", follower_notice)
        self.assertNotIn("trader_id", json.dumps(leader_notice))
        recorded = json.dumps(
            [event["payload"] for event in self.app.state.ledger.events()], default=str
        )
        for never_stored in ("startup_balance_usd", "acceptance-hash-9", "10000.00", "unknown_future_block"):
            self.assertNotIn(never_stored, recorded, never_stored)

    def test_an_acceptance_without_its_payload_is_refused_and_creates_no_route(self) -> None:
        """Requirement: the leader fails closed without the payload, so an
        acceptance that omits it never reaches the route-creation step."""

        leader_key, leader_id, leader_certificate = self._approved_worker(120221, "Broker-A")
        follower_key, follower_id, follower_certificate = self._approved_worker(120222, "Broker-B")

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)
                proposed = self._pair_cell_request(
                    leader_socket,
                    {
                        "type": "pair_cell_pairing_proposal", "request_id": "propose-1",
                        "follower_worker_id": follower_id,
                    },
                )
                follower_socket.receive_json()

                bare = self._pair_cell_request(
                    follower_socket,
                    {
                        "type": "pair_cell_pairing_decision", "request_id": "decide-1",
                        "proposal_id": proposed["proposal_id"], "accepted": True,
                    },
                )
                recovered = self._pair_cell_request(
                    follower_socket,
                    self._acceptance("decide-2", cast(str, proposed["proposal_id"]), follower_id),
                )

        self.assertFalse(bare["accepted"])
        self.assertEqual("paired", recovered["outcome"])

    def test_a_refusal_carrying_an_acceptance_payload_is_refused(self) -> None:
        """Requirement: a refusal never becomes an implied acceptance, so it
        may not smuggle the payload that would let a leader publish policy."""

        leader_key, leader_id, leader_certificate = self._approved_worker(120231, "Broker-A")
        follower_key, follower_id, follower_certificate = self._approved_worker(120232, "Broker-B")

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)
                proposed = self._pair_cell_request(
                    leader_socket,
                    {
                        "type": "pair_cell_pairing_proposal", "request_id": "propose-1",
                        "follower_worker_id": follower_id,
                    },
                )
                follower_socket.receive_json()
                proposal_id = cast(str, proposed["proposal_id"])

                contradictory = self._pair_cell_request(
                    follower_socket,
                    {
                        "type": "pair_cell_pairing_decision", "request_id": "decide-1",
                        "proposal_id": proposal_id, "accepted": False, "reason": "not empty",
                        "acceptance_payload": self._acceptance_payload(proposal_id, follower_id),
                        "payload_hash": "acceptance-hash-1",
                    },
                )
                still_reserved = self.app.state.ledger.pair_route_reservation(proposal_id)
                unrouted = self.app.state.ledger.pair_route_for_worker(leader_id)

        self.assertFalse(contradictory["accepted"])
        self.assertIsNotNone(still_reserved)
        self.assertIsNone(unrouted)

    def test_an_oversized_acceptance_payload_releases_the_reservation(self) -> None:
        """Requirement: the controller enforces the protocol and size limits
        even though the payload's content is invisible to it, and a failed
        acceptance releases the reservation entirely and tells the leader."""

        leader_key, leader_id, leader_certificate = self._approved_worker(120241, "Broker-A")
        follower_key, follower_id, follower_certificate = self._approved_worker(120242, "Broker-B")

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)
                proposed = self._pair_cell_request(
                    leader_socket,
                    {
                        "type": "pair_cell_pairing_proposal", "request_id": "propose-1",
                        "follower_worker_id": follower_id,
                    },
                )
                follower_socket.receive_json()
                proposal_id = cast(str, proposed["proposal_id"])

                oversized = self._pair_cell_request(
                    follower_socket,
                    {
                        "type": "pair_cell_pairing_decision", "request_id": "decide-1",
                        "proposal_id": proposal_id, "accepted": True,
                        "acceptance_payload": {"blob": "x" * (MAX_PAIR_RELAY_ENVELOPE_BYTES + 1)},
                        "payload_hash": "acceptance-hash-1",
                    },
                )
                leader_notice = leader_socket.receive_json()
                ledger = self.app.state.ledger
                released = ledger.pair_route_reservation(proposal_id)
                unrouted = ledger.pair_route_for_worker(leader_id)
                unclaimed = ledger.pair_execution_owner(
                    leader_id, follower_id, "pair_execution_cell"
                )

        self.assertFalse(oversized["accepted"])
        self.assertIn("size limit", oversized["reason"])
        self.assertEqual("pair_cell_pairing_failed", leader_notice["type"])
        self.assertEqual(proposal_id, leader_notice["proposal_id"])
        self.assertIsNone(released)
        self.assertIsNone(unrouted)
        self.assertIsNone(unclaimed)

    def test_two_leaders_selecting_the_same_follower_produce_one_route_and_one_conflict(self) -> None:
        """Requirement: exactly one winner and one deterministic conflict at
        the reservation transaction; the loser re-lists and pairs elsewhere
        without waiting out the winner's reservation."""

        winner_key, winner_id, winner_certificate = self._approved_worker(120031, "Broker-A")
        loser_key, loser_id, loser_certificate = self._approved_worker(120032, "Broker-B")
        follower_key, follower_id, follower_certificate = self._approved_worker(120033, "Broker-C")
        spare_key, spare_id, spare_certificate = self._approved_worker(120034, "Broker-D")

        with self.client.websocket_connect("/api/worker/session") as winner_socket:
            self._authenticate_worker_socket(winner_socket, winner_key, winner_id, winner_certificate)
            with self.client.websocket_connect("/api/worker/session") as loser_socket:
                self._authenticate_worker_socket(loser_socket, loser_key, loser_id, loser_certificate)
                with self.client.websocket_connect("/api/worker/session") as follower_socket:
                    self._authenticate_worker_socket(
                        follower_socket, follower_key, follower_id, follower_certificate
                    )
                    with self.client.websocket_connect("/api/worker/session") as spare_socket:
                        self._authenticate_worker_socket(spare_socket, spare_key, spare_id, spare_certificate)

                        won = self._pair_cell_request(
                            winner_socket,
                            {
                                "type": "pair_cell_pairing_proposal", "request_id": "propose-win",
                                "follower_worker_id": follower_id,
                            },
                        )
                        lost = self._pair_cell_request(
                            loser_socket,
                            {
                                "type": "pair_cell_pairing_proposal", "request_id": "propose-lose",
                                "follower_worker_id": follower_id,
                            },
                        )
                        relisted = self._pair_cell_request(
                            loser_socket,
                            {"type": "pair_cell_available_followers", "request_id": "list-lose"},
                        )
                        retried = self._pair_cell_request(
                            loser_socket,
                            {
                                "type": "pair_cell_pairing_proposal", "request_id": "propose-retry",
                                "follower_worker_id": spare_id,
                            },
                        )

        self.assertTrue(won["accepted"])
        self.assertFalse(lost["accepted"])
        self.assertIn("already reserved", lost["reason"])
        self.assertEqual([spare_id], [entry["worker_id"] for entry in relisted["followers"]])
        self.assertTrue(retried["accepted"])
        self.assertNotEqual(won["proposal_id"], retried["proposal_id"])

    def test_a_follower_refusal_releases_the_reservation_and_tells_the_leader(self) -> None:
        """Requirement: a refusal never becomes an implied acceptance, it
        releases the reservation immediately, and the leader re-lists rather
        than retrying blindly."""

        leader_key, leader_id, leader_certificate = self._approved_worker(120041, "Broker-A")
        follower_key, follower_id, follower_certificate = self._approved_worker(120042, "Broker-B")

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)
                proposed = self._pair_cell_request(
                    leader_socket,
                    {
                        "type": "pair_cell_pairing_proposal", "request_id": "propose-1",
                        "follower_worker_id": follower_id,
                    },
                )
                follower_socket.receive_json()

                refused = self._pair_cell_request(
                    follower_socket,
                    {
                        "type": "pair_cell_pairing_decision", "request_id": "decide-1",
                        "proposal_id": proposed["proposal_id"], "accepted": False,
                        "reason": "account is not broker-verified empty",
                    },
                )
                leader_notice = leader_socket.receive_json()
                relisted = self._pair_cell_request(
                    leader_socket, {"type": "pair_cell_available_followers", "request_id": "list-1"}
                )

        self.assertEqual("refused", refused["outcome"])
        self.assertIsNone(refused["route"])
        self.assertEqual("pair_cell_pairing_refused", leader_notice["type"])
        self.assertEqual("account is not broker-verified empty", leader_notice["reason"])
        self.assertEqual([follower_id], [entry["worker_id"] for entry in relisted["followers"]])
        ledger = self.app.state.ledger
        self.assertEqual([], ledger.pair_route_reservations_for_worker(leader_id))
        self.assertIsNone(ledger.pair_route_for_worker(leader_id))
        self.assertIsNone(ledger.pair_execution_owner(leader_id, follower_id, "pair_execution_cell"))

    def test_a_proposal_naming_a_disconnected_follower_fails_cleanly(self) -> None:
        """Requirement: a follower that is not connected at proposal time
        causes a clean proposal failure with no route, no reservation, and no
        partially accepted state on either side."""

        leader_key, leader_id, leader_certificate = self._approved_worker(120051, "Broker-A")
        _absent_key, absent_id, _absent_certificate = self._approved_worker(120052, "Broker-B")

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            refused = self._pair_cell_request(
                leader_socket,
                {
                    "type": "pair_cell_pairing_proposal", "request_id": "propose-1",
                    "follower_worker_id": absent_id,
                },
            )

        self.assertFalse(refused["accepted"])
        self.assertIn("connected", refused["reason"])
        ledger = self.app.state.ledger
        self.assertEqual([], ledger.pair_route_reservations_for_worker(leader_id))
        self.assertIsNone(ledger.pair_route_for_worker(leader_id))
        self.assertIsNone(ledger.pair_execution_owner(leader_id, absent_id, "pair_execution_cell"))

    def test_a_leader_disconnect_before_the_commit_releases_the_reservation(self) -> None:
        """Requirement: the reservation is released when either Worker
        disconnects before the final transaction commits, leaving no route,
        no role assignment, and no retained execution-mode claim."""

        leader_key, leader_id, leader_certificate = self._approved_worker(120061, "Broker-A")
        follower_key, follower_id, follower_certificate = self._approved_worker(120062, "Broker-B")

        with self.client.websocket_connect("/api/worker/session") as follower_socket:
            self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)
            with self.client.websocket_connect("/api/worker/session") as leader_socket:
                self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
                proposed = self._pair_cell_request(
                    leader_socket,
                    {
                        "type": "pair_cell_pairing_proposal", "request_id": "propose-1",
                        "follower_worker_id": follower_id,
                    },
                )
                follower_socket.receive_json()

            released = follower_socket.receive_json()
            stale = self._pair_cell_request(
                follower_socket,
                self._acceptance("decide-1", cast(str, proposed["proposal_id"]), follower_id),
            )

        self.assertEqual("pair_cell_pairing_reservation_released", released["type"])
        self.assertEqual(proposed["proposal_id"], released["proposal_id"])
        self.assertFalse(stale["accepted"])
        ledger = self.app.state.ledger
        self.assertIsNone(ledger.pair_route_for_worker(follower_id))
        self.assertIsNone(ledger.pair_execution_owner(leader_id, follower_id, "pair_execution_cell"))

    def test_a_decision_for_an_expired_reservation_creates_no_route(self) -> None:
        """Requirement: the final compare-and-swap succeeds only while the
        reservation is still valid and unexpired."""

        leader_key, leader_id, leader_certificate = self._approved_worker(120071, "Broker-A")
        follower_key, follower_id, follower_certificate = self._approved_worker(120072, "Broker-B")

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)
                proposed = self._pair_cell_request(
                    leader_socket,
                    {
                        "type": "pair_cell_pairing_proposal", "request_id": "propose-1",
                        "follower_worker_id": follower_id,
                    },
                )
                follower_socket.receive_json()
                self.app.state.ledger.expire_pair_route_reservations(
                    datetime.now(UTC) + timedelta(seconds=31)
                )

                stale = self._pair_cell_request(
                    follower_socket,
                    self._acceptance("decide-1", cast(str, proposed["proposal_id"]), follower_id),
                )

        self.assertFalse(stale["accepted"])
        ledger = self.app.state.ledger
        self.assertIsNone(ledger.pair_route_for_worker(leader_id))
        self.assertIsNone(ledger.pair_execution_owner(leader_id, follower_id, "pair_execution_cell"))

    def test_a_worker_cannot_accept_a_proposal_it_was_not_reserved_for(self) -> None:
        leader_key, leader_id, leader_certificate = self._approved_worker(120081, "Broker-A")
        follower_key, follower_id, follower_certificate = self._approved_worker(120082, "Broker-B")

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)
                proposed = self._pair_cell_request(
                    leader_socket,
                    {
                        "type": "pair_cell_pairing_proposal", "request_id": "propose-1",
                        "follower_worker_id": follower_id,
                    },
                )
                follower_socket.receive_json()

                impostor = self._pair_cell_request(
                    leader_socket,
                    self._acceptance("decide-1", cast(str, proposed["proposal_id"]), follower_id),
                )

        self.assertFalse(impostor["accepted"])
        self.assertIsNone(self.app.state.ledger.pair_route_for_worker(leader_id))

    def test_a_pairing_is_refused_while_the_legacy_strategy_runtime_holds_either_worker(self) -> None:
        """Requirement: execution-mode exclusivity is checked for both
        Workers, and a legacy live Strategy Runtime on either Worker's pair
        rejects the pairing at phase one."""

        leader_key, leader_id, leader_certificate = self._approved_worker(120091, "Broker-A")
        follower_key, follower_id, follower_certificate = self._approved_worker(120092, "Broker-B")
        _third_key, third_id, _third_certificate = self._approved_worker(120093, "Broker-C")
        trader_enrollment = self.app.state.ledger.create_trader_enrollment(
            strategy_name="legacy-strategy-runtime",
            claimed_public_ip="203.0.113.4",
            public_key_pem="trader-public-key-exclusivity",
        )
        trader_id = self.app.state.ledger.approve_trader_enrollment(
            trader_enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        self.app.state.ledger.claim_pair_execution_mode(
            follower_id, third_id, "strategy_runtime", trader_id
        )

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)
                refused = self._pair_cell_request(
                    leader_socket,
                    {
                        "type": "pair_cell_pairing_proposal", "request_id": "propose-1",
                        "follower_worker_id": follower_id,
                    },
                )

        self.assertFalse(refused["accepted"])
        self.assertIn("legacy Strategy Runtime", refused["reason"])
        owner = self.app.state.ledger.pair_execution_owner(follower_id, third_id, "strategy_runtime")
        assert owner is not None
        self.assertEqual(trader_id, owner["trader_id"])

    def test_a_reconnecting_worker_syncs_its_authoritative_route_and_role(self) -> None:
        """Requirement: a Worker that reconnects and finds itself already on
        a route resumes its assigned role and never enters the listing or
        proposal flow, and a contradicting declared role is ignored."""

        (
            _leader_key, leader_id, _leader_certificate,
            follower_key, follower_id, follower_certificate, route_id,
        ) = self._pair_route(120101, 120102)

        with self.client.websocket_connect("/api/worker/session") as follower_socket:
            self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)
            synced = self._pair_cell_request(
                follower_socket, {"type": "pair_cell_route_sync", "request_id": "sync-1"}
            )
            contradicting = self._pair_cell_request(
                follower_socket,
                {"type": "pair_cell_role", "request_id": "declare-1", "role": "leader"},
            )

        self.assertEqual(route_id, synced["route"]["route_id"])
        self.assertEqual("follower", synced["route"]["role"])
        self.assertEqual(leader_id, synced["route"]["leader_worker_id"])
        self.assertEqual("follower", contradicting["route"]["role"])
        self.assertEqual(route_id, contradicting["route"]["route_id"])
        self.assertNotIn("trader_id", json.dumps(synced))

    def test_an_unpaired_worker_syncs_an_absent_route(self) -> None:
        worker_key, worker_id, worker_certificate = self._approved_worker(120111, "Broker-A")

        with self.client.websocket_connect("/api/worker/session") as socket:
            self._authenticate_worker_socket(socket, worker_key, worker_id, worker_certificate)
            synced = self._pair_cell_request(socket, {"type": "pair_cell_route_sync", "request_id": "sync-1"})

        self.assertTrue(synced["accepted"])
        self.assertIsNone(synced["route"])

    def test_either_worker_enters_and_cancels_unpairing_and_both_are_told(self) -> None:
        """Requirement: either authenticated Worker may enter ``UNPAIRING``
        for the current ``route_id``, it takes effect immediately on both
        sides, and an explicit cancel from either Worker restores normal
        operation."""

        (
            leader_key, leader_id, leader_certificate,
            follower_key, follower_id, follower_certificate, route_id,
        ) = self._pair_route(120121, 120122)

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)

                entered = self._pair_cell_request(
                    follower_socket,
                    {
                        "type": "pair_cell_unpair", "request_id": "unpair-1",
                        "route_id": route_id, "action": "enter",
                    },
                )
                leader_notice = leader_socket.receive_json()
                follower_notice = follower_socket.receive_json()

                cancelled = self._pair_cell_request(
                    leader_socket,
                    {
                        "type": "pair_cell_unpair", "request_id": "unpair-2",
                        "route_id": route_id, "action": "cancel",
                    },
                )
                leader_socket.receive_json()
                follower_socket.receive_json()

        self.assertEqual("UNPAIRING", entered["route"]["state"])
        self.assertIsNotNone(entered["route"]["unpairing_at"])
        self.assertEqual("pair_cell_route_state_changed", leader_notice["type"])
        self.assertEqual("UNPAIRING", leader_notice["route"]["state"])
        self.assertEqual(follower_id, leader_notice["requested_by"])
        self.assertEqual("UNPAIRING", follower_notice["route"]["state"])
        self.assertEqual("ACTIVE", cancelled["route"]["state"])
        self.assertIsNone(cancelled["route"]["unpairing_at"])

    def test_two_fresh_assertions_remove_the_route_and_one_sided_assertions_do_not(self) -> None:
        """Requirement: the controller removes the route only on two fresh,
        currently-bound assertions; a one-sided or superseded assertion does
        not unpair, and the controller derives neither emptiness nor
        terminality itself."""

        (
            leader_key, leader_id, leader_certificate,
            follower_key, follower_id, follower_certificate, route_id,
        ) = self._pair_route(120131, 120132)
        ledger = self.app.state.ledger

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)
                self._pair_cell_request(
                    leader_socket,
                    {
                        "type": "pair_cell_unpair", "request_id": "unpair-1",
                        "route_id": route_id, "action": "enter",
                    },
                )
                leader_socket.receive_json()
                follower_socket.receive_json()

                one_sided = self._pair_cell_request(
                    leader_socket,
                    {
                        "type": "pair_cell_unpair_assertion", "request_id": "assert-1",
                        "route_id": route_id, "state_version": 1,
                    },
                )
                still_routed = ledger.pair_route(route_id)
                advanced = self._pair_cell_request(
                    leader_socket,
                    {
                        "type": "pair_cell_state_version", "request_id": "version-1",
                        "route_id": route_id, "state_version": 2,
                    },
                )
                partial = self._pair_cell_request(
                    follower_socket,
                    {
                        "type": "pair_cell_unpair_assertion", "request_id": "assert-2",
                        "route_id": route_id, "state_version": 5,
                    },
                )
                after_invalidation = ledger.pair_route(route_id)
                completed = self._pair_cell_request(
                    leader_socket,
                    {
                        "type": "pair_cell_unpair_assertion", "request_id": "assert-3",
                        "route_id": route_id, "state_version": 2,
                    },
                )
                removals = [leader_socket.receive_json(), follower_socket.receive_json()]

        self.assertFalse(one_sided["route_removed"])
        self.assertIsNotNone(still_routed)
        self.assertTrue(advanced["assertion_invalidated"])
        self.assertFalse(partial["route_removed"])
        self.assertIsNotNone(after_invalidation)
        self.assertTrue(completed["route_removed"])
        self.assertEqual(
            [{"type": "pair_cell_route_removed", "route_id": route_id, "reason": "safe_unpair"}] * 2,
            removals,
        )
        self.assertIsNone(ledger.pair_route(route_id))
        self.assertIsNone(ledger.pair_execution_owner(leader_id, follower_id, "pair_execution_cell"))

    def test_an_assertion_for_a_wrong_route_id_is_refused(self) -> None:
        (
            leader_key, leader_id, leader_certificate,
            _follower_key, _follower_id, _follower_certificate, route_id,
        ) = self._pair_route(120141, 120142)

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            self.app.state.ledger.enter_pair_route_unpairing(route_id=route_id, worker_id=leader_id)
            refused = self._pair_cell_request(
                leader_socket,
                {
                    "type": "pair_cell_unpair_assertion", "request_id": "assert-1",
                    "route_id": "some-other-route", "state_version": 1,
                },
            )

        self.assertFalse(refused["accepted"])
        self.assertIsNotNone(self.app.state.ledger.pair_route(route_id))

    def test_a_worker_outside_the_route_cannot_unpair_it(self) -> None:
        (
            _leader_key, _leader_id, _leader_certificate,
            _follower_key, _follower_id, _follower_certificate, route_id,
        ) = self._pair_route(120151, 120152)
        outsider_key, outsider_id, outsider_certificate = self._approved_worker(120153, "Broker-C")

        with self.client.websocket_connect("/api/worker/session") as outsider_socket:
            self._authenticate_worker_socket(outsider_socket, outsider_key, outsider_id, outsider_certificate)
            refused = self._pair_cell_request(
                outsider_socket,
                {
                    "type": "pair_cell_unpair", "request_id": "unpair-1",
                    "route_id": route_id, "action": "enter",
                },
            )

        self.assertFalse(refused["accepted"])
        route = self.app.state.ledger.pair_route(route_id)
        assert route is not None
        self.assertEqual("ACTIVE", route["state"])

    def test_identity_revocation_removes_relay_without_unpairing_the_route(self) -> None:
        """Requirement: identity revocation removes entry readiness and relay
        but is never a safe unpair and never implies broker ``EMPTY``."""

        (
            leader_key, leader_id, leader_certificate,
            _follower_key, follower_id, _follower_certificate, route_id,
        ) = self._pair_route(120161, 120162)
        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        headers = {"X-CSRF-Token": login.json()["csrf_token"]}
        self.client.post(f"/api/admin/workers/{follower_id}/revoke", headers=headers)
        envelope = self._relay_envelope(leader_id, follower_id, route_id)

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            leader_socket.send_json({"type": "pair_relay", "request_id": "relay-1", "envelope": envelope})
            ack = leader_socket.receive_json()

        self.assertFalse(ack["accepted"])
        surviving = self.app.state.ledger.pair_route(route_id)
        assert surviving is not None
        self.assertEqual("ACTIVE", surviving["state"])

    def test_a_malformed_pairing_control_message_is_refused_without_dropping_the_session(self) -> None:
        worker_key, worker_id, worker_certificate = self._approved_worker(120171, "Broker-A")

        with self.client.websocket_connect("/api/worker/session") as socket:
            self._authenticate_worker_socket(socket, worker_key, worker_id, worker_certificate)
            refused = self._pair_cell_request(
                socket, {"type": "pair_cell_role", "request_id": "declare-1", "role": "coordinator"}
            )
            recovered = self._pair_cell_request(
                socket, {"type": "pair_cell_role", "request_id": "declare-2", "role": "leader"}
            )

        self.assertFalse(refused["accepted"])
        self.assertTrue(recovered["accepted"])
        self.assertEqual("leader", recovered["role"])

    def test_a_pairing_control_message_carrying_a_trader_id_is_refused(self) -> None:
        """Requirement: no Pair Cell controller API accepts a Trader
        identity."""

        worker_key, worker_id, worker_certificate = self._approved_worker(120181, "Broker-A")

        with self.client.websocket_connect("/api/worker/session") as socket:
            self._authenticate_worker_socket(socket, worker_key, worker_id, worker_certificate)
            refused = self._pair_cell_request(
                socket,
                {
                    "type": "pair_cell_role", "request_id": "declare-1",
                    "role": "leader", "trader_id": "trader-1",
                },
            )

        self.assertFalse(refused["accepted"])

    def test_the_controller_never_parses_universe_content_on_a_pairing_message(self) -> None:
        """Requirement: the controller validates ``route_id`` and role only,
        never discovered universe content."""

        worker_key, worker_id, worker_certificate = self._approved_worker(120191, "Broker-A")

        with self.client.websocket_connect("/api/worker/session") as socket:
            self._authenticate_worker_socket(socket, worker_key, worker_id, worker_certificate)
            refused = self._pair_cell_request(
                socket,
                {
                    "type": "pair_cell_available_followers", "request_id": "list-1",
                    "eligible_products": ["EURUSD"],
                },
            )

        self.assertFalse(refused["accepted"])

    def test_every_dispatched_pairing_message_type_has_a_correlated_result_type(self) -> None:
        """The session's dispatch set and the reply table can never drift, or
        a Worker's pairing request would drop its authenticated session."""

        self.assertEqual(
            set(PAIR_CELL_CONTROL_MESSAGE_TYPES),
            set(controlplane_service._PAIR_CELL_RESULT_TYPES),
        )

    # ---- Administrator surfaces that survive ---- #

    def test_admin_pair_execution_mode_switch_is_mutually_exclusive_and_explicit(self) -> None:
        """Requirement: the legacy Strategy Runtime keeps its own operator
        switch and its own Trader identity, unchanged."""

        _leader_key, leader_id, _leader_certificate = self._approved_worker(666666, "Broker-A")
        _follower_key, follower_id, _follower_certificate = self._approved_worker(777777, "Broker-B")
        trader_enrollment = self.app.state.ledger.create_trader_enrollment(
            strategy_name="legacy-strategy-runtime",
            claimed_public_ip="203.0.113.4",
            public_key_pem="trader-public-key-3",
        )
        trader_id = self.app.state.ledger.approve_trader_enrollment(
            trader_enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        headers = {"X-CSRF-Token": login.json()["csrf_token"]}

        claim = self.client.post(
            "/api/admin/pairs/execution-mode",
            headers=headers,
            json={
                "leader_worker_id": leader_id, "follower_worker_id": follower_id,
                "trader_id": trader_id, "mode": "strategy_runtime",
            },
        )
        self.assertEqual(200, claim.status_code, claim.text)

        blocked = self.client.post(
            "/api/admin/pairs/execution-mode",
            headers=headers,
            json={
                "leader_worker_id": leader_id, "follower_worker_id": follower_id,
                "trader_id": "another-trader", "mode": "strategy_runtime",
            },
        )
        self.assertEqual(409, blocked.status_code)

        release = self.client.post(
            "/api/admin/pairs/execution-mode/release",
            headers=headers,
            json={"leader_worker_id": leader_id, "follower_worker_id": follower_id, "mode": "strategy_runtime"},
        )
        self.assertEqual(204, release.status_code)

        reserved = self.app.state.ledger.reserve_pair_route_proposal(
            leader_worker_id=leader_id,
            follower_worker_id=follower_id,
            connected_worker_ids={leader_id, follower_id},
        )
        self.assertEqual(30, reserved["timeout_seconds"])

    def test_admin_cannot_release_a_pair_cell_execution_mode_claim(self) -> None:
        """Requirement: a Pair Cell claim is released only by its reservation
        or its route, so no operator can strand a live route."""

        (
            _leader_key, leader_id, _leader_certificate,
            _follower_key, follower_id, _follower_certificate, route_id,
        ) = self._pair_route(120201, 120202)
        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        headers = {"X-CSRF-Token": login.json()["csrf_token"]}

        response = self.client.post(
            "/api/admin/pairs/execution-mode/release",
            headers=headers,
            json={
                "leader_worker_id": leader_id, "follower_worker_id": follower_id,
                "mode": "pair_execution_cell",
            },
        )

        self.assertEqual(422, response.status_code)
        self.assertIsNotNone(self.app.state.ledger.pair_route(route_id))
        self.assertIsNotNone(
            self.app.state.ledger.pair_execution_owner(leader_id, follower_id, "pair_execution_cell")
        )

    def test_admin_release_of_pair_quarantine_requires_admin_auth(self) -> None:
        response = self.client.post(
            "/api/admin/pairs/quarantine/release",
            json={"route_id": "route-1", "symbol": "EURUSD", "reason": "operator review"},
        )

        self.assertEqual(401, response.status_code)

    def test_admin_release_of_pair_quarantine_names_the_route_and_never_a_trader(self) -> None:
        """Requirement: the authenticated operator quarantine release is
        adapted to ``route_id``; it carries no Trader identity."""

        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        headers = {"X-CSRF-Token": login.json()["csrf_token"]}

        for body in (
            {"route_id": "route-1", "symbol": "EURUSD", "reason": ""},
            {"symbol": "EURUSD", "reason": "operator review"},
            {
                "route_id": "route-1", "symbol": "EURUSD", "reason": "operator review",
                "trader_id": "trader-1",
            },
            {
                "leader_worker_id": "worker-a", "follower_worker_id": "worker-b",
                "symbol": "EURUSD", "reason": "operator review",
            },
        ):
            with self.subTest(body=body):
                response = self.client.post(
                    "/api/admin/pairs/quarantine/release", headers=headers, json=body
                )

                self.assertEqual(422, response.status_code)

    def test_admin_release_of_pair_quarantine_requires_a_live_route(self) -> None:
        """Requirement: quarantine release stays an authenticated route-level
        operation. Without a live route the controller has no standing to
        address the pair at all."""

        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        headers = {"X-CSRF-Token": login.json()["csrf_token"]}

        response = self.client.post(
            "/api/admin/pairs/quarantine/release",
            headers=headers,
            json={"route_id": "no-such-route", "symbol": "EURUSD", "reason": "operator review"},
        )

        self.assertEqual(409, response.status_code)
        self.assertIn("live Worker route", response.json()["detail"])

    def test_admin_release_of_pair_quarantine_requires_both_workers_to_be_connected(self) -> None:
        """Requirement: a quarantine release is a one-time action that the
        controller never persists for later replay, so it must never be
        silently accepted for delivery "later" -- both the leader and
        follower Worker session must be connected now."""

        (
            _leader_key, _leader_id, _leader_certificate,
            _follower_key, _follower_id, _follower_certificate, route_id,
        ) = self._pair_route(929292, 939393)
        login = self.client.post(
            "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
        )
        headers = {"X-CSRF-Token": login.json()["csrf_token"]}

        response = self.client.post(
            "/api/admin/pairs/quarantine/release",
            headers=headers,
            json={"route_id": route_id, "symbol": "EURUSD", "reason": "operator review"},
        )

        self.assertEqual(409, response.status_code)
        self.assertIn("connected", response.json()["detail"])

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
