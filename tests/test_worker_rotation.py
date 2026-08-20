from __future__ import annotations

import base64
import io
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from abt.controlplane.crypto import device_certificate_payload
from abt.worker.identity import WorkerIdentity, load_identity, save_identity
from abt.worker.rotation import (
    WorkerCertificateRotated,
    WorkerRotationError,
    maintain_worker_certificate,
)


class FakeKeyStore:
    def __init__(self, name: str, *, fail_delete: bool = False) -> None:
        self.name = name
        self.fail_delete = fail_delete
        self.deleted = False

    def public_key_pem(self) -> str:
        return f"public-key-{self.name}"

    def sign(self, payload: bytes) -> bytes:
        return payload

    def delete(self) -> None:
        if self.fail_delete:
            raise RuntimeError("CNG is busy")
        self.deleted = True

    def close(self) -> None:
        pass


class FakeRotationTransport:
    def __init__(self) -> None:
        self.rotate_result: dict[str, object] = {"worker_id": "worker-123", "certificate": "replacement"}
        self.calls: list[str] = []

    def rotation_challenge(self, _: str, worker_id: str, public_key_pem: str) -> dict[str, object]:
        self.calls.append("challenge")
        return {"purpose": "worker_certificate_rotation", "worker_id": worker_id, "nonce": "nonce"}

    def rotate(self, _: str, worker_id: str, public_key_pem: str, old_signature: str, replacement_signature: str) -> dict[str, object]:
        self.calls.append("rotate")
        return self.rotate_result


class WorkerCertificateRotationTests(unittest.TestCase):
    def test_rotates_past_half_life_and_persists_cleanup_only_after_identity_switch(self) -> None:
        now = datetime(2026, 8, 20, tzinfo=UTC)
        with TemporaryDirectory() as directory:
            identity_path = Path(directory) / "worker.json"
            save_identity(identity_path, self._identity(), replace=False)
            keys: dict[str, FakeKeyStore] = {}
            transport = FakeRotationTransport()

            with self.assertRaises(WorkerCertificateRotated):
                maintain_worker_certificate(
                    identity_path=identity_path,
                    identity=self._identity(),
                    worker_id="worker-123",
                    certificate=self._certificate(now - timedelta(days=16), now + timedelta(days=14)),
                    current_key=FakeKeyStore("old-key"),
                    key_store_factory=lambda name: keys.setdefault(name, FakeKeyStore(name)),
                    transport=transport,
                    now=lambda: now,
                    error_output=io.StringIO(),
                )

            replacement = load_identity(identity_path)
            self.assertNotEqual("old-key", replacement.key_name)
            state = json.loads((Path(directory) / "worker.state.json").read_text(encoding="utf-8"))
            self.assertEqual("old-key", state["retired_keys"][0]["key_name"])
            self.assertEqual((now + timedelta(hours=1)).isoformat(), state["retired_keys"][0]["eligible_at"])
            self.assertEqual(["challenge", "rotate"], transport.calls)

    def test_failed_rotation_leaves_current_identity_usable(self) -> None:
        now = datetime(2026, 8, 20, tzinfo=UTC)
        with TemporaryDirectory() as directory:
            identity_path = Path(directory) / "worker.json"
            save_identity(identity_path, self._identity(), replace=False)
            transport = FakeRotationTransport()
            transport.rotate_result = {"worker_id": "worker-123"}
            replacement_keys: list[FakeKeyStore] = []

            with self.assertRaisesRegex(WorkerRotationError, "invalid rotation response"):
                maintain_worker_certificate(
                    identity_path=identity_path,
                    identity=self._identity(),
                    worker_id="worker-123",
                    certificate=self._certificate(now - timedelta(days=16), now + timedelta(days=14)),
                    current_key=FakeKeyStore("old-key"),
                    key_store_factory=lambda name: replacement_keys.append(FakeKeyStore(name)) or replacement_keys[-1],
                    transport=transport,
                    now=lambda: now,
                    error_output=io.StringIO(),
                )

            self.assertEqual("old-key", load_identity(identity_path).key_name)
            self.assertTrue(replacement_keys[0].deleted)

    def test_recovers_retired_key_cleanup_from_a_rotation_journal_after_state_write_failure(self) -> None:
        now = datetime(2026, 8, 20, tzinfo=UTC)
        with TemporaryDirectory() as directory:
            identity_path = Path(directory) / "worker.json"
            save_identity(identity_path, self._identity(), replace=False)
            transport = FakeRotationTransport()
            keys: dict[str, FakeKeyStore] = {}

            with (
                patch("abt.worker.rotation._add_retired_key", side_effect=WorkerRotationError("disk unavailable")),
                self.assertRaises(WorkerCertificateRotated),
            ):
                maintain_worker_certificate(
                    identity_path=identity_path,
                    identity=self._identity(),
                    worker_id="worker-123",
                    certificate=self._certificate(now - timedelta(days=16), now + timedelta(days=14)),
                    current_key=FakeKeyStore("old-key"),
                    key_store_factory=lambda name: keys.setdefault(name, FakeKeyStore(name)),
                    transport=transport,
                    now=lambda: now,
                    error_output=io.StringIO(),
                )

            replacement = load_identity(identity_path)
            maintain_worker_certificate(
                identity_path=identity_path,
                identity=replacement,
                worker_id="worker-123",
                certificate=self._certificate(now - timedelta(days=1), now + timedelta(days=29)),
                current_key=keys[replacement.key_name],
                key_store_factory=lambda name: keys.setdefault(name, FakeKeyStore(name)),
                transport=transport,
                now=lambda: now,
                error_output=io.StringIO(),
            )

            state = json.loads((Path(directory) / "worker.state.json").read_text(encoding="utf-8"))
            self.assertEqual("old-key", state["retired_keys"][0]["key_name"])
            self.assertFalse((Path(directory) / "worker.rotation.pending.json").exists())

    def test_retries_retired_key_cleanup_daily_without_changing_active_identity(self) -> None:
        now = datetime(2026, 8, 20, tzinfo=UTC)
        with TemporaryDirectory() as directory:
            identity_path = Path(directory) / "worker.json"
            save_identity(identity_path, self._identity(key_name="new-key"), replace=False)
            state_path = Path(directory) / "worker.state.json"
            state_path.write_text(
                json.dumps({"version": 1, "retired_keys": [{"key_name": "old-key", "eligible_at": (now - timedelta(hours=1)).isoformat()}]}),
                encoding="utf-8",
            )
            errors = io.StringIO()
            retired = FakeKeyStore("old-key", fail_delete=True)

            maintain_worker_certificate(
                identity_path=identity_path,
                identity=self._identity(key_name="new-key"),
                worker_id="worker-123",
                certificate=self._certificate(now - timedelta(days=1), now + timedelta(days=29)),
                current_key=FakeKeyStore("new-key"),
                key_store_factory=lambda _: retired,
                transport=FakeRotationTransport(),
                now=lambda: now,
                error_output=errors,
            )

            self.assertEqual("new-key", load_identity(identity_path).key_name)
            self.assertIn("retired CNG key cleanup failed", errors.getvalue())
            retained = json.loads(state_path.read_text(encoding="utf-8"))["retired_keys"][0]
            self.assertEqual((now + timedelta(days=1)).isoformat(), retained["retry_at"])

    def test_expired_certificate_requires_operator_action(self) -> None:
        now = datetime(2026, 8, 20, tzinfo=UTC)
        with TemporaryDirectory() as directory:
            identity_path = Path(directory) / "worker.json"
            save_identity(identity_path, self._identity(), replace=False)

            with self.assertRaisesRegex(WorkerRotationError, "expired.*action is required"):
                maintain_worker_certificate(
                    identity_path=identity_path,
                    identity=self._identity(),
                    worker_id="worker-123",
                    certificate=self._certificate(now - timedelta(days=31), now - timedelta(days=1)),
                    current_key=FakeKeyStore("old-key"),
                    key_store_factory=FakeKeyStore,
                    transport=FakeRotationTransport(),
                    now=lambda: now,
                    error_output=io.StringIO(),
                )

    def _identity(self, *, key_name: str = "old-key") -> WorkerIdentity:
        return WorkerIdentity("https://controller.example", "enrollment-123", 123456, "Broker-Demo", key_name)

    def _certificate(self, issued_at: datetime, expires_at: datetime) -> str:
        payload = device_certificate_payload(
            worker_id="worker-123",
            login=123456,
            server="Broker-Demo",
            public_key_pem="public-key-old-key",
            issued_at=issued_at,
            expires_at=expires_at,
        )
        return json.dumps({"payload": base64.b64encode(payload).decode("ascii"), "signature": "ignored"})
