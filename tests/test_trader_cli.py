from __future__ import annotations

import io
import json
import base64
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from abt.controlplane.crypto import trader_certificate_payload
from abt.trader.cli import main
from abt.trader.identity import TraderIdentity, load_identity, save_identity
from abt.trader.rotation import (
    TraderCertificateRotated,
    TraderRotationError,
    maintain_trader_certificate,
)
from abt.trader.session import deliver_trader_events, run_trader_session


class FakeKeyStore:
    def __init__(self, name: str = "key", *, fail_delete: bool = False) -> None:
        self.name = name
        self.fail_delete = fail_delete
        self.deleted = False

    def public_key_pem(self) -> str:
        return f"public-key-{self.name}"

    def sign(self, _: bytes) -> bytes:
        return b"signature"

    def delete(self) -> None:
        if self.fail_delete:
            raise RuntimeError("CNG is busy")
        self.deleted = True

    def close(self) -> None:
        pass


class FakeTransport:
    def enroll(self, _: str, __: dict[str, object]) -> dict[str, object]:
        return {"registration_id": "registration-123", "expires_at": "2026-08-20T12:00:00+00:00"}

    def enrollment_status(self, _: str, __: str) -> dict[str, object]:
        return {"status": "approved"}

    def close(self) -> None:
        pass


class FakeEcho:
    def public_ip(self) -> str:
        return "203.0.113.4"


class TraderCommandTests(unittest.TestCase):
    def test_enroll_confirms_detected_ip_and_saves_only_non_secrets(self) -> None:
        with TemporaryDirectory() as directory:
            config_path = Path(directory) / "trader.json"
            prompts = iter(["https://controller.example", "trader-invite", "mean-reversion", "yes"])
            output = io.StringIO()
            errors = io.StringIO()

            with patch("abt.trader.cli.sys.platform", "win32"):
                exit_code = main(
                    ["enroll", "--config", str(config_path)],
                    transport_factory=FakeTransport,
                    key_store_factory=lambda _: FakeKeyStore(),
                    public_ip_discovery=FakeEcho(),
                    input_prompt=lambda _: next(prompts),
                    output=output,
                )

            self.assertEqual(0, exit_code)
            self.assertIn("pending_approval", output.getvalue())
            self.assertEqual(
                {
                    "version": 1,
                    "controller_url": "https://controller.example",
                    "enrollment_id": "registration-123",
                    "strategy_name": "mean-reversion",
                    "public_ip": "203.0.113.4",
                    "key_name": "abt-trader-device-key",
                },
                json.loads(config_path.read_text(encoding="utf-8")),
            )
            saved = config_path.read_text(encoding="utf-8")
            self.assertNotIn("trader-invite", saved)
            self.assertNotIn("signature", saved)

    def test_enroll_uses_explicit_ip_without_discovery_or_prompt(self) -> None:
        with TemporaryDirectory() as directory:
            config_path = Path(directory) / "trader.json"

            with patch("abt.trader.cli.sys.platform", "win32"):
                exit_code = main(
                    [
                        "enroll", "--config", str(config_path), "--controller-url", "https://controller.example",
                        "--registration-invite", "trader-invite", "--strategy-name", "mean-reversion",
                        "--public-ip", "203.0.113.4",
                    ],
                    transport_factory=FakeTransport,
                    key_store_factory=lambda _: FakeKeyStore(),
                    public_ip_discovery=lambda: (_ for _ in ()).throw(AssertionError("unexpected discovery")),
                    input_prompt=lambda _: (_ for _ in ()).throw(AssertionError("unexpected prompt")),
                )

            self.assertEqual(0, exit_code)

    def test_enroll_falls_back_to_manual_ip_when_discovery_fails(self) -> None:
        with TemporaryDirectory() as directory:
            config_path = Path(directory) / "trader.json"
            prompts = iter(["https://controller.example", "trader-invite", "mean-reversion", "203.0.113.5"])

            with patch("abt.trader.cli.sys.platform", "win32"):
                exit_code = main(
                    ["enroll", "--config", str(config_path)],
                    transport_factory=FakeTransport,
                    key_store_factory=lambda _: FakeKeyStore(),
                    public_ip_discovery=lambda: (_ for _ in ()).throw(RuntimeError("echo unavailable")),
                    input_prompt=lambda _: next(prompts),
                )

            self.assertEqual(0, exit_code)
            self.assertEqual("203.0.113.5", json.loads(config_path.read_text(encoding="utf-8"))["public_ip"])

    def test_connect_reports_pending_approval_without_opening_a_session(self) -> None:
        class PendingTransport(FakeTransport):
            def enrollment_status(self, _: str, __: str) -> dict[str, object]:
                return {"status": "pending"}

        with TemporaryDirectory() as directory:
            config_path = Path(directory) / "trader.json"
            config_path.write_text(
                '{"version":1,"controller_url":"https://controller.example","enrollment_id":"registration-123",'
                '"strategy_name":"mean-reversion","public_ip":"203.0.113.4","key_name":"saved-key"}\n',
                encoding="utf-8",
            )
            output = io.StringIO()
            errors = io.StringIO()

            with (
                patch("abt.trader.cli.sys.platform", "win32"),
                patch("abt.trader.cli.connect_trader", side_effect=AssertionError("unexpected connection")),
            ):
                exit_code = main(
                    ["connect", "--config", str(config_path)],
                    transport_factory=PendingTransport,
                    output=output,
                    error_output=errors,
                )

            self.assertEqual(0, exit_code)
            self.assertEqual("", output.getvalue())
            self.assertIn("pending administrator approval", errors.getvalue())

    def test_connect_writes_event_before_acknowledging_and_mirrors_acknowledged_cursor(self) -> None:
        output = io.StringIO()
        acknowledged: list[int] = []
        state: list[int] = []

        deliver_trader_events(
            [{"type": "event", "event_id": 17, "payload": {"state": "accepted"}}],
            output=output,
            acknowledge=acknowledged.append,
            save_cursor=state.append,
        )

        self.assertEqual('{"event_id":17,"payload":{"state":"accepted"},"type":"event"}\n', output.getvalue())
        self.assertEqual([17], acknowledged)
        self.assertEqual([17], state)

    def test_connect_acknowledges_replayed_events_after_their_json_lines_output(self) -> None:
        class ReplaySocket:
            def __init__(self) -> None:
                self.messages = iter(
                    [
                        '{"type":"event","event_id":17,"payload":{"state":"accepted"}}',
                        '{"type":"event","event_id":18,"payload":{"state":"executing"}}',
                        '{"type":"acknowledged","cursor":17}',
                        '{"type":"acknowledged","cursor":18}',
                    ]
                )
                self.sent: list[str] = []

            def recv(self, timeout: float | None = None) -> str:
                return next(self.messages)

            def send(self, message: str) -> None:
                self.sent.append(message)

        socket = ReplaySocket()
        output = io.StringIO()
        cursors: list[int] = []

        with self.assertRaises(StopIteration):
            run_trader_session(socket, output=output, save_cursor=cursors.append)  # type: ignore[arg-type]

        self.assertEqual([17, 18], cursors)
        self.assertEqual(
            [
                '{"cursor":17,"type":"ack"}',
                '{"cursor":18,"type":"ack"}',
            ],
            socket.sent,
        )
        self.assertEqual(
            '{"event_id":17,"payload":{"state":"accepted"},"type":"event"}\n'
            '{"event_id":18,"payload":{"state":"executing"},"type":"event"}\n',
            output.getvalue(),
        )


class FakeRotationTransport:
    def __init__(self) -> None:
        self.rotate_result: dict[str, object] = {"trader_id": "trader-123", "certificate": "replacement"}

    def rotation_challenge(self, _: str, trader_id: str, __: str) -> dict[str, object]:
        return {"purpose": "trader_certificate_rotation", "trader_id": trader_id, "nonce": "nonce"}

    def rotate(self, _: str, trader_id: str, __: str, ___: str, ____: str) -> dict[str, object]:
        return self.rotate_result


class TraderCertificateRotationTests(unittest.TestCase):
    def test_rotates_past_half_life_without_advancing_acknowledged_cursor(self) -> None:
        now = datetime(2026, 8, 20, tzinfo=UTC)
        with TemporaryDirectory() as directory:
            identity_path = Path(directory) / "trader.json"
            save_identity(identity_path, self._identity(), replace=False)
            state_path = Path(directory) / "trader.state.json"
            state_path.write_text('{"version":1,"acknowledged_cursor":17}\n', encoding="utf-8")
            keys: dict[str, FakeKeyStore] = {}

            with self.assertRaises(TraderCertificateRotated):
                maintain_trader_certificate(
                    identity_path=identity_path,
                    identity=self._identity(),
                    trader_id="trader-123",
                    certificate=self._certificate(now - timedelta(days=16), now + timedelta(days=14)),
                    current_key=FakeKeyStore("old-key"),
                    key_store_factory=lambda name: keys.setdefault(name, FakeKeyStore(name)),
                    transport=FakeRotationTransport(),
                    now=lambda: now,
                    error_output=io.StringIO(),
                )

            self.assertNotEqual("old-key", load_identity(identity_path).key_name)
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(17, state["acknowledged_cursor"])
            self.assertEqual("old-key", state["retired_keys"][0]["key_name"])
            self.assertEqual((now + timedelta(hours=1)).isoformat(), state["retired_keys"][0]["eligible_at"])

    def test_failed_rotation_retains_the_current_trader_identity(self) -> None:
        now = datetime(2026, 8, 20, tzinfo=UTC)
        with TemporaryDirectory() as directory:
            identity_path = Path(directory) / "trader.json"
            save_identity(identity_path, self._identity(), replace=False)
            transport = FakeRotationTransport()
            transport.rotate_result = {"trader_id": "trader-123"}
            replacements: list[FakeKeyStore] = []

            with self.assertRaisesRegex(TraderRotationError, "invalid rotation response"):
                maintain_trader_certificate(
                    identity_path=identity_path,
                    identity=self._identity(),
                    trader_id="trader-123",
                    certificate=self._certificate(now - timedelta(days=16), now + timedelta(days=14)),
                    current_key=FakeKeyStore("old-key"),
                    key_store_factory=lambda name: replacements.append(FakeKeyStore(name)) or replacements[-1],
                    transport=transport,
                    now=lambda: now,
                    error_output=io.StringIO(),
                )

            self.assertEqual("old-key", load_identity(identity_path).key_name)
            self.assertTrue(replacements[0].deleted)

    def test_retries_retired_key_cleanup_daily_without_overwriting_cursor(self) -> None:
        now = datetime(2026, 8, 20, tzinfo=UTC)
        with TemporaryDirectory() as directory:
            identity_path = Path(directory) / "trader.json"
            save_identity(identity_path, self._identity(key_name="new-key"), replace=False)
            state_path = Path(directory) / "trader.state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "version": 2,
                        "acknowledged_cursor": 29,
                        "retired_keys": [{"key_name": "old-key", "eligible_at": (now - timedelta(hours=1)).isoformat()}],
                    }
                ),
                encoding="utf-8",
            )
            errors = io.StringIO()

            maintain_trader_certificate(
                identity_path=identity_path,
                identity=self._identity(key_name="new-key"),
                trader_id="trader-123",
                certificate=self._certificate(now - timedelta(days=1), now + timedelta(days=29)),
                current_key=FakeKeyStore("new-key"),
                key_store_factory=lambda _: FakeKeyStore("old-key", fail_delete=True),
                transport=FakeRotationTransport(),
                now=lambda: now,
                error_output=errors,
            )

            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(29, state["acknowledged_cursor"])
            self.assertEqual((now + timedelta(days=1)).isoformat(), state["retired_keys"][0]["retry_at"])
            self.assertIn("retired CNG key cleanup failed", errors.getvalue())

    def test_recovers_retired_key_cleanup_after_an_identity_switch(self) -> None:
        now = datetime(2026, 8, 20, tzinfo=UTC)
        with TemporaryDirectory() as directory:
            identity_path = Path(directory) / "trader.json"
            save_identity(identity_path, self._identity(), replace=False)
            keys: dict[str, FakeKeyStore] = {}

            with (
                patch("abt.trader.rotation._add_retired_key", side_effect=TraderRotationError("disk unavailable")),
                self.assertRaises(TraderCertificateRotated),
            ):
                maintain_trader_certificate(
                    identity_path=identity_path,
                    identity=self._identity(),
                    trader_id="trader-123",
                    certificate=self._certificate(now - timedelta(days=16), now + timedelta(days=14)),
                    current_key=FakeKeyStore("old-key"),
                    key_store_factory=lambda name: keys.setdefault(name, FakeKeyStore(name)),
                    transport=FakeRotationTransport(),
                    now=lambda: now,
                    error_output=io.StringIO(),
                )

            replacement = load_identity(identity_path)
            maintain_trader_certificate(
                identity_path=identity_path,
                identity=replacement,
                trader_id="trader-123",
                certificate=self._certificate(now - timedelta(days=1), now + timedelta(days=29)),
                current_key=keys[replacement.key_name],
                key_store_factory=lambda name: keys.setdefault(name, FakeKeyStore(name)),
                transport=FakeRotationTransport(),
                now=lambda: now,
                error_output=io.StringIO(),
            )

            self.assertEqual("old-key", json.loads((Path(directory) / "trader.state.json").read_text())["retired_keys"][0]["key_name"])
            self.assertFalse((Path(directory) / "trader.rotation.pending.json").exists())

    def _identity(self, *, key_name: str = "old-key") -> TraderIdentity:
        return TraderIdentity("https://controller.example", "enrollment-123", "mean-reversion", "203.0.113.4", key_name)

    def _certificate(self, issued_at: datetime, expires_at: datetime) -> str:
        payload = trader_certificate_payload(
            trader_id="trader-123",
            strategy_name="mean-reversion",
            public_key_pem="public-key-old-key",
            issued_at=issued_at,
            expires_at=expires_at,
        )
        return json.dumps({"payload": base64.b64encode(payload).decode("ascii"), "signature": "ignored"})
