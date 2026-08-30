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
    _broadcast_trader_worker_stream,
    _delete_expired_pending_secrets,
    _is_authoritative_worker_session,
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

    def test_pair_relay_forwards_an_opaque_envelope_between_leased_workers(self) -> None:
        leader_key, leader_id, leader_certificate = self._approved_worker(111111, "Broker-A")
        follower_key, follower_id, follower_certificate = self._approved_worker(222222, "Broker-B")
        trader_enrollment = self.app.state.ledger.create_trader_enrollment(
            strategy_name="pair-execution-cell", claimed_public_ip="203.0.113.4", public_key_pem="trader-public-key"
        )
        trader_id = self.app.state.ledger.approve_trader_enrollment(
            trader_enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        lease = self.app.state.ledger.issue_pair_lease(
            leader_worker_id=leader_id, follower_worker_id=follower_id,
            trader_id=trader_id, contract_hash="contract-hash-1", ttl_seconds=300,
        )
        envelope = {
            "lease": {
                "leader_worker_id": leader_id, "follower_worker_id": follower_id,
                "trader_id": trader_id, "contract_hash": "contract-hash-1", "lease_epoch": lease["epoch"],
            },
            "from_worker_id": leader_id, "to_worker_id": follower_id,
            "request_id": "message-1",
            "attempt_id": "attempt-1",
            "payload_hash": "unused-by-controller",
            "payload": {"kind": "arm_request", "opaque_to_controller": True},
        }

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            with self.client.websocket_connect("/api/worker/session") as follower_socket:
                self._authenticate_worker_socket(follower_socket, follower_key, follower_id, follower_certificate)

                leader_socket.send_json({"type": "pair_relay", "request_id": "relay-1", "envelope": envelope})

                delivered = follower_socket.receive_json()
                ack = leader_socket.receive_json()

        self.assertEqual({"type": "pair_relay_deliver", "envelope": envelope}, delivered)
        self.assertEqual({"type": "pair_relay_ack", "request_id": "relay-1", "accepted": True, "result": {"delivered": True, "audited": True, "replay": False}}, ack)

    def test_pair_relay_rejects_a_route_outside_the_leased_pair(self) -> None:
        leader_key, leader_id, leader_certificate = self._approved_worker(333333, "Broker-A")
        _outsider_key, outsider_id, _outsider_certificate = self._approved_worker(444444, "Broker-C")
        _follower_key, follower_id, _follower_certificate = self._approved_worker(555555, "Broker-B")
        trader_enrollment = self.app.state.ledger.create_trader_enrollment(
            strategy_name="pair-execution-cell", claimed_public_ip="203.0.113.4", public_key_pem="trader-public-key-2"
        )
        trader_id = self.app.state.ledger.approve_trader_enrollment(
            trader_enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        lease = self.app.state.ledger.issue_pair_lease(
            leader_worker_id=leader_id, follower_worker_id=follower_id,
            trader_id=trader_id, contract_hash="contract-hash-2", ttl_seconds=300,
        )
        envelope = {
            "lease": {
                "leader_worker_id": leader_id, "follower_worker_id": follower_id,
                "trader_id": trader_id, "contract_hash": "contract-hash-2", "lease_epoch": lease["epoch"],
            },
            "from_worker_id": leader_id, "to_worker_id": outsider_id,
            "request_id": "message-1",
            "attempt_id": "attempt-1",
            "payload_hash": "unused-by-controller",
            "payload": {"kind": "arm_request"},
        }

        with self.client.websocket_connect("/api/worker/session") as leader_socket:
            self._authenticate_worker_socket(leader_socket, leader_key, leader_id, leader_certificate)
            leader_socket.send_json({"type": "pair_relay", "request_id": "relay-1", "envelope": envelope})
            ack = leader_socket.receive_json()

        self.assertFalse(ack["accepted"])

    def test_admin_pair_execution_mode_switch_is_mutually_exclusive_and_explicit(self) -> None:
        _leader_key, leader_id, _leader_certificate = self._approved_worker(666666, "Broker-A")
        _follower_key, follower_id, _follower_certificate = self._approved_worker(777777, "Broker-B")
        trader_enrollment = self.app.state.ledger.create_trader_enrollment(
            strategy_name="pair-execution-cell", claimed_public_ip="203.0.113.4", public_key_pem="trader-public-key-3"
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
        self.assertEqual(200, claim.status_code)

        conflict = self.client.post(
            "/api/admin/pairs/execution-mode",
            headers=headers,
            json={
                "leader_worker_id": leader_id, "follower_worker_id": follower_id,
                "trader_id": trader_id, "mode": "pair_execution_cell",
            },
        )
        self.assertEqual(409, conflict.status_code)

        release = self.client.post(
            "/api/admin/pairs/execution-mode/release",
            headers=headers,
            json={"leader_worker_id": leader_id, "follower_worker_id": follower_id, "mode": "strategy_runtime"},
        )
        self.assertEqual(204, release.status_code)

        after_release = self.client.post(
            "/api/admin/pairs/execution-mode",
            headers=headers,
            json={
                "leader_worker_id": leader_id, "follower_worker_id": follower_id,
                "trader_id": trader_id, "mode": "pair_execution_cell",
            },
        )
        self.assertEqual(200, after_release.status_code)

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
