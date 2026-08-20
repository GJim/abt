from __future__ import annotations

import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from abt.worker.cli import main


class FakeMT5:
    def initialize(self) -> bool:
        return True

    def login(self, login: int, *, password: str, server: str) -> bool:
        return login == 123456 and password == "memory-only" and server == "Broker-Demo"

    def account_info(self) -> object:
        return {"login": 123456, "server": "Broker-Demo"}

    def terminal_info(self) -> object:
        return {"name": "MetaTrader 5"}

    def shutdown(self) -> None:
        pass


class FakeKeyStore:
    def public_key_pem(self) -> str:
        return "public-key"

    def sign(self, _: bytes) -> bytes:
        return b"signature"

    def close(self) -> None:
        pass


class FakeTransport:
    def enrollment_challenge(self, _: str) -> dict[str, object]:
        return {"challenge": "challenge"}

    def enroll(self, _: str, __: dict[str, object]) -> dict[str, object]:
        return {"enrollment_id": "registration-123", "expires_at": "2026-08-20T12:00:00+00:00"}

    def enrollment_status(self, _: str, __: str) -> dict[str, object]:
        return {"status": "approved"}

    def close(self) -> None:
        pass


class WorkerIdentityCommandTests(unittest.TestCase):
    def test_enroll_prompts_for_missing_identity_and_saves_only_non_secrets(self) -> None:
        with TemporaryDirectory() as directory:
            config_path = Path(directory) / "worker-identity.json"
            prompts = iter(["https://controller.example", "123456", "Broker-Demo", "worker-invite"])
            output = io.StringIO()

            with patch("abt.worker.cli.sys.platform", "win32"):
                exit_code = main(
                    ["enroll", "--config", str(config_path)],
                    mt5_factory=FakeMT5,
                    transport_factory=FakeTransport,
                    key_store_factory=lambda _: FakeKeyStore(),
                    input_prompt=lambda _: next(prompts),
                    password_prompt=lambda _: "memory-only",
                    output=output,
                )

            saved = json.loads(config_path.read_text(encoding="utf-8"))
            self.assertEqual(
                {
                    "version": 1,
                    "controller_url": "https://controller.example",
                    "enrollment_id": "registration-123",
                    "login": 123456,
                    "server": "Broker-Demo",
                    "key_name": "abt-worker-device-key",
                },
                saved,
            )
            self.assertEqual(0, exit_code)
            self.assertIn("pending_approval", output.getvalue())
            self.assertNotIn("memory-only", config_path.read_text(encoding="utf-8"))
            self.assertNotIn("worker-invite", config_path.read_text(encoding="utf-8"))

    def test_enroll_flags_take_precedence_over_prompts(self) -> None:
        with TemporaryDirectory() as directory:
            config_path = Path(directory) / "worker-identity.json"

            with patch("abt.worker.cli.sys.platform", "win32"):
                exit_code = main(
                    [
                        "enroll",
                        "--config",
                        str(config_path),
                        "--controller-url",
                        "https://controller.example",
                        "--login",
                        "123456",
                        "--server",
                        "Broker-Demo",
                        "--registration-invite",
                        "worker-invite",
                    ],
                    mt5_factory=FakeMT5,
                    transport_factory=FakeTransport,
                    key_store_factory=lambda _: FakeKeyStore(),
                    input_prompt=lambda _: (_ for _ in ()).throw(AssertionError("unexpected prompt")),
                    password_prompt=lambda _: "memory-only",
                )

            self.assertEqual(0, exit_code)

    def test_enroll_preserves_existing_identity_without_replace(self) -> None:
        with TemporaryDirectory() as directory:
            config_path = Path(directory) / "worker-identity.json"
            original = '{"version":1,"controller_url":"https://existing.example","enrollment_id":"existing","login":1,"server":"existing","key_name":"existing"}\n'
            config_path.write_text(original, encoding="utf-8")
            errors = io.StringIO()

            with patch("abt.worker.cli.sys.platform", "win32"):
                exit_code = main(
                    ["enroll", "--config", str(config_path)],
                    input_prompt=lambda _: (_ for _ in ()).throw(AssertionError("unexpected prompt")),
                    error_output=errors,
                )

            self.assertEqual(1, exit_code)
            self.assertEqual(original, config_path.read_text(encoding="utf-8"))
            self.assertIn("already exists", errors.getvalue())

    def test_enroll_replace_overwrites_existing_identity(self) -> None:
        with TemporaryDirectory() as directory:
            config_path = Path(directory) / "worker-identity.json"
            config_path.write_text(
                '{"version":1,"controller_url":"https://existing.example","enrollment_id":"existing","login":1,"server":"existing","key_name":"existing"}\n',
                encoding="utf-8",
            )

            with patch("abt.worker.cli.sys.platform", "win32"):
                exit_code = main(
                    [
                        "enroll",
                        "--replace-config",
                        "--config",
                        str(config_path),
                        "--controller-url",
                        "https://controller.example",
                        "--login",
                        "123456",
                        "--server",
                        "Broker-Demo",
                        "--registration-invite",
                        "worker-invite",
                    ],
                    mt5_factory=FakeMT5,
                    transport_factory=FakeTransport,
                    key_store_factory=lambda _: FakeKeyStore(),
                    password_prompt=lambda _: "memory-only",
                )

            self.assertEqual(0, exit_code)
            saved = json.loads(config_path.read_text(encoding="utf-8"))
            self.assertEqual("existing", saved["key_name"])
            self.assertEqual("existing", saved["enrollment_id"])
            self.assertEqual(
                "registration-123",
                json.loads((Path(directory) / "worker-identity.pending.json").read_text(encoding="utf-8"))["enrollment_id"],
            )

    def test_reconcile_reads_identity_from_configuration(self) -> None:
        with TemporaryDirectory() as directory:
            config_path = Path(directory) / "worker-identity.json"
            config_path.write_text(
                '{"version":1,"controller_url":"https://controller.example","enrollment_id":"registration-123","login":123456,"server":"Broker-Demo","key_name":"saved-key"}\n',
                encoding="utf-8",
            )
            opened: dict[str, object] = {}

            def open_session(**kwargs: object) -> object:
                opened.update(kwargs)
                return object()

            def reconnect(**kwargs: object) -> None:
                kwargs["open_session"]()

            with (
                patch("abt.worker.cli.sys.platform", "win32"),
                patch("abt.worker.cli.open_authenticated_worker_session", side_effect=open_session),
                patch("abt.worker.cli.reconnect_worker_session", side_effect=reconnect),
            ):
                exit_code = main(
                    ["reconcile", "--config", str(config_path)],
                    mt5_factory=FakeMT5,
                    transport_factory=FakeTransport,
                    key_store_factory=lambda name: FakeKeyStore() if name == "saved-key" else None,
                )

            self.assertEqual(0, exit_code)
            self.assertEqual("https://controller.example", opened["controller_url"])
            self.assertEqual("registration-123", opened["enrollment_id"])

    def test_reconcile_fails_safely_when_identity_configuration_is_missing(self) -> None:
        with TemporaryDirectory() as directory:
            errors = io.StringIO()

            with patch("abt.worker.cli.sys.platform", "win32"):
                exit_code = main(["reconcile", "--config", str(Path(directory) / "missing.json")], error_output=errors)

            self.assertEqual(1, exit_code)
            self.assertIn("Worker reconciliation failed:", errors.getvalue())
            self.assertIn("does not exist", errors.getvalue())

    def test_reconcile_reports_pending_approval_without_opening_a_session(self) -> None:
        class PendingTransport(FakeTransport):
            def enrollment_status(self, _: str, __: str) -> dict[str, object]:
                return {"status": "pending"}

        with TemporaryDirectory() as directory:
            config_path = Path(directory) / "worker-identity.json"
            config_path.write_text(
                '{"version":1,"controller_url":"https://controller.example","enrollment_id":"registration-123","login":123456,"server":"Broker-Demo","key_name":"saved-key"}\n',
                encoding="utf-8",
            )
            output = io.StringIO()

            with (
                patch("abt.worker.cli.sys.platform", "win32"),
                patch("abt.worker.cli.reconnect_worker_session", side_effect=AssertionError("unexpected session")),
            ):
                exit_code = main(
                    ["reconcile", "--config", str(config_path)],
                    transport_factory=PendingTransport,
                    output=output,
                )

            self.assertEqual(0, exit_code)
            self.assertIn("pending administrator approval", output.getvalue())
