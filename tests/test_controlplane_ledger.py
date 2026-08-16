from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from abt.controlplane.ledger import AuthenticationError, ControlLedger, IpLockedError, LedgerError


class ControlLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.ledger = ControlLedger(Path(self._directory.name) / "ledger.duckdb")

    def tearDown(self) -> None:
        self.ledger.close()
        self._directory.cleanup()

    def test_locks_an_ip_after_three_failed_logins_until_cli_unlock(self) -> None:
        self.ledger.create_admin("ABCDEF", "long-admin-password")

        for _ in range(2):
            with self.assertRaises(AuthenticationError):
                self.ledger.authenticate_admin("ABCDEF", "wrong", "203.0.113.10")
        with self.assertRaises(IpLockedError):
            self.ledger.authenticate_admin("ABCDEF", "wrong", "203.0.113.10")
        with self.assertRaises(IpLockedError):
            self.ledger.authenticate_admin("ABCDEF", "long-admin-password", "203.0.113.10")

        self.ledger.unlock_ip("203.0.113.10")
        session = self.ledger.authenticate_admin("ABCDEF", "long-admin-password", "203.0.113.10")
        self.assertEqual("ABCDEF", self.ledger.validate_session(session.token, session.csrf_token, require_csrf=True))

    def test_approved_enrollment_exclusively_binds_an_mt5_account(self) -> None:
        first = self.ledger.create_enrollment(
            login=123456,
            server="Broker-Demo",
            pairing_code="12345678",
            public_key_pem="public-key-one",
            source_ip="203.0.113.11",
            account_info={"login": 123456},
            terminal_info={"name": "MetaTrader 5"},
            password_secret_ref="abt/data/mt5/pending/one",
        )
        worker_id = self.ledger.approve_enrollment(
            first.enrollment_id, "ABCDEF", lambda worker_id, *_: f"certificate:{worker_id}"
        )
        self.assertTrue(worker_id)

        second = self.ledger.create_enrollment(
            login=123456,
            server="Broker-Demo",
            pairing_code="87654321",
            public_key_pem="public-key-two",
            source_ip="203.0.113.12",
            account_info={"login": 123456},
            terminal_info={"name": "MetaTrader 5"},
            password_secret_ref="abt/data/mt5/pending/two",
        )
        with self.assertRaisesRegex(LedgerError, "active worker"):
            self.ledger.approve_enrollment(
                second.enrollment_id, "ABCDEF", lambda worker_id, *_: f"certificate:{worker_id}"
            )

    def test_rejected_enrollment_cannot_be_reactivated(self) -> None:
        enrollment = self.ledger.create_enrollment(
            login=123456,
            server="Broker-Demo",
            pairing_code="12345678",
            public_key_pem="public-key",
            source_ip="203.0.113.11",
            account_info={"login": 123456},
            terminal_info={"name": "MetaTrader 5"},
            password_secret_ref="abt/data/mt5/pending/rejected",
        )
        self.ledger.reject_enrollment(enrollment.enrollment_id, "ABCDEF")
        with self.assertRaisesRegex(LedgerError, "no longer pending"):
            self.ledger.approve_enrollment(
                enrollment.enrollment_id, "ABCDEF", lambda worker_id, *_: f"certificate:{worker_id}"
            )
