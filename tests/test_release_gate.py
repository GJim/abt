from __future__ import annotations

import json
import tempfile
import unittest
import base64
from contextlib import redirect_stdout
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi.testclient import TestClient

from abt.controlplane.backup import BackupError, BackupManager
from abt.controlplane.console import main
from abt.controlplane.crypto import enrollment_payload, worker_proof_payload
from abt.controlplane.ledger import ControlLedger
from abt.controlplane.service import create_app
from abt.worker.reconciliation import MT5ReconciliationAdapter
from tests.test_controlplane_service import MemoryCertificateIssuer, MemorySecretStore


class ReleaseGateBackupTests(unittest.TestCase):
    def test_console_backup_and_restore_verification_are_explicit_local_operations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raft = root / "raft"
            tokens = root / "tokens"
            raft.mkdir()
            tokens.mkdir()
            (raft / "state").write_text("raft", encoding="utf-8")
            (tokens / "state").write_text("tokens", encoding="utf-8")
            ledger = root / "ledger.duckdb"
            backups = root / "backups"
            output = StringIO()
            with redirect_stdout(output):
                self.assertEqual(0, main([
                    "--ledger", str(ledger), "backup", "--backup-directory", str(backups),
                    "--openbao-raft", str(raft), "--softhsm-tokens", str(tokens),
                ]))
            backup_set = Path(output.getvalue().strip().split("=", 1)[1])
            with redirect_stdout(StringIO()):
                self.assertEqual(0, main([
                    "--ledger", str(ledger), "verify-restore-set", "--backup-directory", str(backups), str(backup_set),
                ]))

    def test_hourly_backup_keeps_24_complete_sets_and_verifies_only_allowed_restore_set(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger_path = root / "ledger.duckdb"
            raft = root / "openbao-raft"
            tokens = root / "softhsm-tokens"
            raft.mkdir()
            tokens.mkdir()
            (raft / "raft.db").write_text("raft", encoding="utf-8")
            (tokens / "token").write_text("token", encoding="utf-8")
            ledger = ControlLedger(ledger_path)
            ledger.create_admin("ABCDEF", "A-secure-admin-password!")
            manager = BackupManager(ledger, root / "backups", raft, tokens)
            try:
                for sequence in range(25):
                    manager.create(f"hourly-{sequence}")
                backups = sorted((root / "backups").iterdir())
                self.assertEqual(24, len(backups))
                manifest = manager.verify_restore_set(backups[-1])
                self.assertEqual(
                    {"ledger-export.tar.gz", "openbao-raft.tar.gz", "softhsm-tokens.tar.gz"},
                    set(manifest["artifacts"]),
                )
                manifest_path = backups[-1] / "manifest.json"
                data = json.loads(manifest_path.read_text(encoding="utf-8"))
                data["artifacts"]["ledger-export.tar.gz"]["sha256"] = "invalid"
                manifest_path.write_text(json.dumps(data), encoding="utf-8")
                with self.assertRaises(BackupError):
                    manager.verify_restore_set(backups[-1])
            finally:
                ledger.close()


class TwoWorkerReleaseExerciseTests(unittest.TestCase):
    def test_two_workers_complete_read_only_control_plane_exercise_without_broker_writes(self) -> None:
        broker = _ReadOnlyBroker()
        MT5ReconciliationAdapter(broker, emit=lambda _: None).poll(datetime(2026, 8, 16, tzinfo=UTC))
        self.assertEqual(0, broker.write_calls)
        with tempfile.TemporaryDirectory() as directory:
            secret_store = MemorySecretStore()
            app = create_app(
                Path(directory) / "ledger.duckdb",
                secret_store=secret_store,
                certificate_issuer=MemoryCertificateIssuer(),
            )
            with TestClient(app, base_url="https://console.example") as client:
                app.state.ledger.create_admin("ABCDEF", "A-secure-admin-password!")
                first_key, first_enrollment = self._enroll(client, 123456, "198.51.100.10")
                second_key, second_enrollment = self._enroll(client, 654321, "203.0.113.20")
                login = client.post(
                    "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
                )
                csrf = login.json()["csrf_token"]
                first_worker = client.post(
                    f"/api/admin/enrollments/{first_enrollment}/approve", headers={"X-CSRF-Token": csrf}
                ).json()["worker_id"]
                second_worker = client.post(
                    f"/api/admin/enrollments/{second_enrollment}/approve", headers={"X-CSRF-Token": csrf}
                ).json()["worker_id"]

                first_certificate = app.state.ledger.active_worker(first_worker).certificate
                second_certificate = app.state.ledger.active_worker(second_worker).certificate
                self._exercise_session(client, first_key, first_worker, first_certificate, cursor=0, external_change=True)
                self._exercise_session(client, second_key, second_worker, second_certificate, cursor=0, external_change=False)

                alerts = client.get("/api/admin/alerts").json()
                self.assertEqual("external_broker_change", alerts[-1]["alert_type"])
                self.assertEqual(
                    204,
                    client.post(f"/api/admin/workers/{first_worker}/revoke", headers={"X-CSRF-Token": csrf}).status_code,
                )
                with client.websocket_connect("/api/worker/session") as websocket:
                    websocket.send_json({"worker_id": first_worker, "certificate": first_certificate})
                    with self.assertRaises(Exception):
                        websocket.receive_json()
                self._exercise_session(client, second_key, second_worker, second_certificate, cursor=0, external_change=False)
                workers = {worker["worker_id"]: worker for worker in client.get("/api/admin/workers").json()}
                self.assertEqual("revoked", workers[first_worker]["connectivity"])
                self.assertEqual("connected", workers[second_worker]["connectivity"])

    def _enroll(self, client: TestClient, login: int, source_ip: str) -> tuple[ec.EllipticCurvePrivateKey, str]:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        account = {"login": login, "server": "Broker-Demo"}
        terminal = {"name": "MetaTrader 5", "trade_allowed": False}
        password = f"worker-{login}-memory-only-password"
        challenge = client.get("/api/enrollment-challenge").json()["challenge"]
        signature = private_key.sign(
            enrollment_payload(login, "Broker-Demo", "12345678", account, terminal, password, challenge),
            ec.ECDSA(hashes.SHA256()),
        )
        response = client.post(
            "/api/enrollments",
            headers={"CF-Connecting-IP": source_ip},
            json={
                "login": login, "server": "Broker-Demo", "pairing_code": "12345678", "account_info": account,
                "terminal_info": terminal, "mt5_password": password, "enrollment_challenge": challenge,
                "public_key_pem": public_key, "proof_signature": base64.b64encode(signature).decode("ascii"),
            },
        )
        self.assertEqual(201, response.status_code)
        return private_key, response.json()["enrollment_id"]

    def _exercise_session(
        self, client: TestClient, key: ec.EllipticCurvePrivateKey, worker_id: str, certificate: str, *, cursor: int,
        external_change: bool,
    ) -> None:
        with client.websocket_connect("/api/worker/session") as websocket:
            websocket.send_json({"worker_id": worker_id, "certificate": certificate})
            challenge = websocket.receive_json()
            signature = key.sign(
                worker_proof_payload(purpose="worker_session", worker_id=worker_id, nonce=challenge["nonce"]),
                ec.ECDSA(hashes.SHA256()),
            )
            websocket.send_json({"signature": base64.b64encode(signature).decode("ascii")})
            self.assertEqual({"type": "authenticated", "worker_id": worker_id, "cursor": cursor}, websocket.receive_json())
            websocket.send_json({"type": "password_request"})
            self.assertTrue(websocket.receive_json()["password"])
            websocket.send_json(
                {"type": "snapshot", "cursor": cursor, "observed_at": "2026-08-16T00:00:00+00:00",
                 "account": {"login": worker_id}, "terminal": {"trade_allowed": False}, "orders": [], "positions": []}
            )
            self.assertEqual({"type": "accepted", "cursor": cursor}, websocket.receive_json())
            if external_change:
                websocket.send_json(
                    {"type": "delta", "cursor": cursor + 1, "observed_at": "2026-08-16T00:01:00+00:00",
                     "entity": "position", "ticket": "51", "change": "created", "record": {"ticket": 51, "volume": 1}}
                )
                self.assertEqual({"type": "accepted", "cursor": cursor + 1}, websocket.receive_json())


class _ReadOnlyBroker:
    def __init__(self) -> None:
        self.write_calls = 0

    def account_info(self) -> dict[str, object]:
        return {"login": 123456, "server": "Broker-Demo"}

    def terminal_info(self) -> dict[str, object]:
        return {"trade_allowed": False}

    def orders_get(self) -> list[object]:
        return []

    def positions_get(self) -> list[object]:
        return []

    def order_send(self, *_: object, **__: object) -> None:
        self.write_calls += 1
        raise AssertionError("The release gate must not write to MT5.")
