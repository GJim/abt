from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from abt.controlplane.ledger import AuthenticationError, ControlLedger, LedgerError


class ControlLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.ledger = ControlLedger(Path(self._directory.name) / "ledger.duckdb")

    def tearDown(self) -> None:
        self.ledger.close()
        self._directory.cleanup()

    def test_repeated_failed_logins_do_not_lock_an_ip(self) -> None:
        self.ledger.create_admin("ABCDEF", "long-admin-password")

        for _ in range(3):
            with self.assertRaises(AuthenticationError):
                self.ledger.authenticate_admin("ABCDEF", "wrong")

        session = self.ledger.authenticate_admin("ABCDEF", "long-admin-password")
        self.assertEqual("ABCDEF", self.ledger.validate_session(session.token, session.csrf_token, require_csrf=True))

    def test_approved_enrollment_exclusively_binds_an_mt5_account(self) -> None:
        first_challenge, _ = self.ledger.issue_enrollment_challenge()
        first = self.ledger.create_enrollment(
            login=123456,
            server="Broker-Demo",
            pairing_code="12345678",
            public_key_pem="public-key-one",
            account_info={"login": 123456},
            terminal_info={"name": "MetaTrader 5"},
            password_secret_ref="abt/data/mt5/pending/one",
            enrollment_challenge=first_challenge,
        )
        worker_id = self.ledger.approve_enrollment(
            first.enrollment_id, "ABCDEF", lambda worker_id, *_: f"certificate:{worker_id}"
        )
        self.assertTrue(worker_id)

        second_challenge, _ = self.ledger.issue_enrollment_challenge()
        second = self.ledger.create_enrollment(
            login=123456,
            server="Broker-Demo",
            pairing_code="87654321",
            public_key_pem="public-key-two",
            account_info={"login": 123456},
            terminal_info={"name": "MetaTrader 5"},
            password_secret_ref="abt/data/mt5/pending/two",
            enrollment_challenge=second_challenge,
        )
        with self.assertRaisesRegex(LedgerError, "active worker"):
            self.ledger.approve_enrollment(
                second.enrollment_id, "ABCDEF", lambda worker_id, *_: f"certificate:{worker_id}"
            )

    def test_rejected_enrollment_cannot_be_reactivated(self) -> None:
        challenge, _ = self.ledger.issue_enrollment_challenge()
        enrollment = self.ledger.create_enrollment(
            login=123456,
            server="Broker-Demo",
            pairing_code="12345678",
            public_key_pem="public-key",
            account_info={"login": 123456},
            terminal_info={"name": "MetaTrader 5"},
            password_secret_ref="abt/data/mt5/pending/rejected",
            enrollment_challenge=challenge,
        )
        self.ledger.reject_enrollment(enrollment.enrollment_id, "ABCDEF")
        with self.assertRaisesRegex(LedgerError, "no longer pending"):
            self.ledger.approve_enrollment(
                enrollment.enrollment_id, "ABCDEF", lambda worker_id, *_: f"certificate:{worker_id}"
            )
