from __future__ import annotations

import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from abt.trader.cli import main
from abt.trader.session import deliver_trader_events


class FakeKeyStore:
    def public_key_pem(self) -> str:
        return "public-key"

    def sign(self, _: bytes) -> bytes:
        return b"signature"

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

            with (
                patch("abt.trader.cli.sys.platform", "win32"),
                patch("abt.trader.cli.connect_trader", side_effect=AssertionError("unexpected connection")),
            ):
                exit_code = main(
                    ["connect", "--config", str(config_path)],
                    transport_factory=PendingTransport,
                    output=output,
                )

            self.assertEqual(0, exit_code)
            self.assertIn("pending administrator approval", output.getvalue())

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
