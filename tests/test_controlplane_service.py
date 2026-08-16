from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta
import json
import tempfile
import unittest
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi.testclient import TestClient

from abt.controlplane.crypto import ProofError, device_certificate_payload, enrollment_payload, worker_proof_payload
from abt.controlplane.secrets import SecretStore, SecretStoreError
from abt.controlplane.service import _delete_expired_pending_secrets, create_app


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
        self.app.state.ledger.create_admin("ABCDEF", "A-secure-admin-password!")

    def tearDown(self) -> None:
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
            enrollment_payload(123456, "Broker-Demo", "12345678", account_info, terminal_info, password, challenge),
            ec.ECDSA(hashes.SHA256()),
        )
        enrollment_response = self.client.post(
            "/api/enrollments",
            headers={"CF-Connecting-IP": "203.0.113.11"},
            json={
                "login": 123456,
                "server": "Broker-Demo",
                "pairing_code": "12345678",
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
                "pairing_code": "12345678",
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
            enrollment_payload(123456, "Broker-Demo", "12345678", account_info, terminal_info, password, challenge),
            ec.ECDSA(hashes.SHA256()),
        )
        request = {
            "login": 123456, "server": "Broker-Demo", "pairing_code": "12345678",
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
        self.assertIn("admin_login_succeeded", [event["event_type"] for event in events_response.json()])

        logout_response = self.client.post("/api/admin/logout", headers={"X-CSRF-Token": csrf_token})
        self.assertEqual(204, logout_response.status_code)
        self.assertEqual(401, self.client.get("/api/admin/events").status_code)

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
            enrollment_payload(123456, "Broker-Demo", "12345678", account_info, terminal_info, password, challenge),
            ec.ECDSA(hashes.SHA256()),
        )
        enrollment = self.client.post(
            "/api/enrollments",
            json={
                "login": 123456,
                "server": "Broker-Demo",
                "pairing_code": "12345678",
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
            event["event_type"] for event in self.client.get("/api/admin/events").json()
        ])
        with self.client.websocket_connect("/api/worker/session") as websocket:
            websocket.send_json({"worker_id": worker_id, "certificate": certificate})
            with self.assertRaises(Exception) as error:
                websocket.receive_json()
        self.assertEqual(1008, getattr(error.exception, "code", None))

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

    def _enrollment_challenge(self) -> str:
        response = self.client.get("/api/enrollment-challenge")
        self.assertEqual(200, response.status_code)
        return response.json()["challenge"]

    def _enrollment_response(
        self, private_key: ec.EllipticCurvePrivateKey, public_key_pem: str, account_info: dict[str, object],
        terminal_info: dict[str, object], password: str = "worker-memory-only-password"
    ):
        challenge = self._enrollment_challenge()
        signature = private_key.sign(
            enrollment_payload(123456, "Broker-Demo", "12345678", account_info, terminal_info, password, challenge),
            ec.ECDSA(hashes.SHA256()),
        )
        return self.client.post(
            "/api/enrollments",
            headers={"CF-Connecting-IP": "203.0.113.11"},
            json={
                "login": 123456, "server": "Broker-Demo", "pairing_code": "12345678",
                "account_info": account_info, "terminal_info": terminal_info, "mt5_password": password,
                "enrollment_challenge": challenge, "public_key_pem": public_key_pem,
                "proof_signature": base64.b64encode(signature).decode("ascii"),
            },
        )
