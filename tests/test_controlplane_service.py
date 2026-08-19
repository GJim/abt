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
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
import uvicorn
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect as websocket_connect

from abt.controlplane.crypto import ProofError, device_certificate_payload, enrollment_payload, worker_proof_payload
from abt.controlplane.ledger import LedgerError
from abt.controlplane.secrets import SecretStore, SecretStoreError
from abt.controlplane.service import (
    _analyze_product_catalogs,
    _delete_expired_pending_secrets,
    _market_data_statistics,
    _request_market_data_with_retry,
    _shared_supported_filling_modes,
    _validated_market_data_response,
    _validated_product_catalog_response,
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
            expires_at=issued_at + timedelta(days=1),
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

        workers = self.client.get("/api/admin/workers").json()
        self.assertEqual("connected", workers[0]["connectivity"])
        self.assertEqual({"balance": 1000}, workers[0]["latest_snapshot"]["account"])
        self.assertEqual(["volume_changed", "modified"], [delta["change"] for delta in workers[0]["deltas"]])

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
        detail = self.client.get(f"/api/admin/worker-snapshots/{first_page['items'][0]['snapshot_id']}")
        self.assertEqual([{"ticket": 2}], detail.json()["orders"])
        self.assertEqual(422, self.client.get("/api/admin/worker-snapshots", params={"cursor": "invalid"}).status_code)

        ledger._event("read_model_match", {"worker_id": worker_id, "marker": "searchable"})
        ledger._event("read_model_match", {"worker_id": worker_id, "marker": "newer"})
        events = self.client.get("/api/admin/events", params={"limit": 1, "event_type": "read_model_match", "q": "newer"})
        self.assertEqual(["read_model_match"], [item["event_type"] for item in events.json()["items"]])
        self.assertIsNone(events.json()["next_cursor"])
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
        date_analyses = self.client.get(
            "/api/admin/product-catalog-analyses", params={"status": "succeeded", "q": "20260819"}
        ).json()
        self.assertEqual(["analysis-search"], [item["analysis_id"] for item in date_analyses["items"]])
        self.assertEqual(422, self.client.get("/api/admin/product-catalog-analyses", params={"limit": 0}).status_code)

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
        safety_alert = next(alert for alert in dashboard["alerts"] if alert["alert_type"] == "lost_link_safety")
        self.assertEqual("intervention_required", enrollment_alert["category"])
        self.assertEqual("administrator_approval_required", enrollment_alert["classification_reason"])
        self.assertEqual(pending_enrollment_id, enrollment_alert["enrollment_id"])
        self.assertEqual("intervention_required", safety_alert["category"])
        self.assertEqual("controller_signal_lost", safety_alert["classification_reason"])
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
                "reason": "controller_signal_lost",
            },
            {
                key: value
                for key, value in alert_intervention.items()
                if key in {"item_type", "item_id", "category", "reason"}
            },
        )
        self.assertEqual("intervention_required", dashboard["workers"][0]["category"])
        self.assertEqual("lost_link_safety", dashboard["workers"][0]["classification_reason"])
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

    def test_unattributed_broker_delta_alerts_needs_human_and_revocation_blocks_wss(self) -> None:
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
            self.assertEqual("needs_human", worker["safety_state"])
            alerts = self.client.get("/api/admin/alerts").json()
            self.assertEqual(("high", "external_broker_change"), (alerts[-1]["priority"], alerts[-1]["alert_type"]))
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
