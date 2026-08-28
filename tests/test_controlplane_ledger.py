from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

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

    def test_controller_records_relay_envelopes_without_interpreting_trading_payload(self) -> None:
        enrollment = self.ledger.create_trader_enrollment(
            strategy_name="realtime-arbitrage",
            claimed_public_ip="203.0.113.4",
            public_key_pem="public-key",
        )
        trader_id = self.ledger.approve_trader_enrollment(
            enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        challenge, _ = self.ledger.issue_enrollment_challenge()
        worker_enrollment = self.ledger.create_enrollment(
            login=123456,
            server="Broker-A",
            public_key_pem="worker-key",
            account_info={"login": 123456},
            terminal_info={"name": "MetaTrader 5"},
            password_secret_ref="abt/data/mt5/pending/worker",
            enrollment_challenge=challenge,
        )
        worker_id = self.ledger.approve_enrollment(
            worker_enrollment.enrollment_id, "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        envelope = {
            "type": "worker_effect_requested",
            "protocol_version": 1,
            "trader_id": trader_id,
            "worker_id": worker_id,
            "runtime_command_id": "command-1",
            "request_id": "request-1",
            "effect_id": "effect-1",
            "expires_at": "2026-08-28T08:00:00+00:00",
            "payload_hash": "opaque-to-controller",
            "correlation": {"purpose": "entry"},
            "payload": {"future_protocol_operation": {"controller_must_not_parse": True}},
        }

        admitted = self.ledger.record_trader_relay(
            trader_id, "request-1", worker_id, envelope
        )
        redispatch = self.ledger.record_trader_relay(
            trader_id, "request-1", worker_id, envelope
        )
        outcome = {
            "type": "worker_effect_outcome",
            "request_id": "request-1",
            "worker_id": worker_id,
            "accepted": True,
        }
        completed = self.ledger.complete_trader_relay(trader_id, "request-1", outcome)
        replay = self.ledger.record_trader_relay(
            trader_id, "request-1", worker_id, envelope
        )

        self.assertTrue(admitted["dispatch"])
        self.assertTrue(redispatch["dispatch"])
        self.assertIsNone(redispatch["outcome"])
        self.assertEqual(outcome, completed)
        self.assertFalse(replay["dispatch"])
        self.assertEqual(outcome, replay["outcome"])
        self.ledger.acknowledge_worker_fact(trader_id, worker_id, "epoch-1", 8)
        self.ledger.acknowledge_worker_fact(trader_id, worker_id, "epoch-1", 7)
        cursor = self.ledger._connection.execute(
            "SELECT event_id FROM trader_worker_fact_cursors WHERE trader_id = ? AND worker_id = ?",
            [trader_id, worker_id],
        ).fetchone()
        self.assertEqual(8, cursor[0])

        first_fact = {
            "type": "worker_snapshot",
            "protocol_version": 1,
            "worker_id": worker_id,
            "recovery_epoch": "epoch-1",
            "snapshot_baseline": 4,
            "observed_at": "2026-08-28T08:00:00+00:00",
            "future_payload": {"controller_must_not_parse": ["orders", "positions"]},
        }
        second_fact = {
            "type": "worker_broker_delta",
            "protocol_version": 1,
            "worker_id": worker_id,
            "recovery_epoch": "epoch-1",
            "event_id": 5,
            "observed_at": "2026-08-28T08:00:01+00:00",
            "future_payload": {"unknown_change": True},
        }
        self.ledger.record_worker_fact_audit(worker_id, first_fact)
        self.ledger.record_worker_fact_audit(worker_id, second_fact)

        resumed = self.ledger.worker_relay_resume(worker_id, "epoch-1", 4)
        gap = self.ledger.worker_relay_resume(worker_id, "old-epoch", 4)
        self.ledger.acknowledge_worker_fact(trader_id, worker_id, "epoch-1", 5)

        self.assertEqual([second_fact], resumed["facts"])
        self.assertEqual("gap", gap["status"])
        self.assertTrue(any(
            event["event_type"] == "trader_worker_fact_acknowledged"
            for event in self.ledger.events()
        ))
        for name in (
            "accept_trader_intent",
            "begin_hedged_entry_command",
            "begin_empty_convergence",
            "register_protected_pair",
            "record_delta",
            "build_product_pair",
        ):
            self.assertFalse(hasattr(self.ledger, name), name)

    def test_migration_removes_legacy_manual_and_freeze_schema(self) -> None:
        self.ledger.close()
        path = Path(self._directory.name) / "ledger.duckdb"
        connection = duckdb.connect(str(path))
        try:
            connection.execute("ALTER TABLE enrollments ADD COLUMN pairing_code VARCHAR")
            connection.execute("ALTER TABLE workers ADD COLUMN safety_state VARCHAR")
            for table in (
                "worker_freezes",
                "manual_trading_target",
                "manual_trades",
                "manual_trade_commands",
                "manual_trade_operations",
                "manual_trade_operation_commands",
                "worker_recovery_commands",
            ):
                connection.execute(f"CREATE TABLE {table} (legacy_record VARCHAR)")
        finally:
            connection.close()

        self.ledger = ControlLedger(path)

        enrollment_columns = {row[1] for row in self.ledger._connection.execute("PRAGMA table_info('enrollments')").fetchall()}
        worker_columns = {row[1] for row in self.ledger._connection.execute("PRAGMA table_info('workers')").fetchall()}
        tables = {row[0] for row in self.ledger._connection.execute("SHOW TABLES").fetchall()}
        self.assertNotIn("pairing_code", enrollment_columns)
        self.assertNotIn("safety_state", worker_columns)
        self.assertTrue(tables.isdisjoint({
            "worker_freezes",
            "manual_trading_target",
            "manual_trades",
            "manual_trade_commands",
            "manual_trade_operations",
            "manual_trade_operation_commands",
            "worker_recovery_commands",
        }))

    def test_approved_enrollment_exclusively_binds_an_mt5_account(self) -> None:
        first_challenge, _ = self.ledger.issue_enrollment_challenge()
        first = self.ledger.create_enrollment(
            login=123456,
            server="Broker-Demo",
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

    def test_registration_invite_is_role_bound_and_single_use(self) -> None:
        invite = self.ledger.create_registration_invite("ABCDEF", "trader")

        self.assertEqual("trader", self.ledger.consume_registration_invite(invite, "trader"))
        with self.assertRaisesRegex(LedgerError, "already used"):
            self.ledger.consume_registration_invite(invite, "trader")

        worker_invite = self.ledger.create_registration_invite("ABCDEF", "worker")
        with self.assertRaisesRegex(LedgerError, "role"):
            self.ledger.consume_registration_invite(worker_invite, "trader")

    def test_registration_invite_list_exposes_an_opaque_revoke_handle_not_the_secret(self) -> None:
        invite = self.ledger.create_registration_invite("ABCDEF", "trader")

        record = self.ledger.registration_invites()[0]
        self.assertEqual({"invite_id", "role", "issued_by", "issued_at", "expires_at", "status", "used_at", "revoked_at"}, set(record))
        self.assertNotEqual(invite, record["invite_id"])

        self.ledger.revoke_registration_invite(record["invite_id"], "ABCDEF")

        self.assertEqual("revoked", self.ledger.registration_invites()[0]["status"])
        with self.assertRaisesRegex(LedgerError, "no longer active"):
            self.ledger.consume_registration_invite(invite, "trader")

    def test_trader_ack_cannot_skip_an_event_unseen_by_this_connection(self) -> None:
        with self.ledger._transaction():
            first_event = self.ledger._event("intent_accepted", {})
            second_event = self.ledger._event("intent_accepted", {})
            self.ledger._connection.execute(
                "INSERT INTO trader_events (trader_id, event_id) VALUES (?, ?), (?, ?)",
                ["trader-123", first_event, "trader-123", second_event],
            )

        with self.assertRaisesRegex(LedgerError, "skip an unseen event"):
            self.ledger.acknowledge_trader_events("trader-123", second_event, 0, {second_event})

        self.ledger.acknowledge_trader_events("trader-123", second_event, 0, {first_event, second_event})
        self.assertEqual(second_event, self.ledger.trader_event_cursor("trader-123"))

    def test_registration_invite_can_be_revoked_only_before_use(self) -> None:
        invite = self.ledger.create_registration_invite("ABCDEF", "worker")

        self.ledger.revoke_registration_invite(invite, "ABCDEF")
        with self.assertRaisesRegex(LedgerError, "no longer active"):
            self.ledger.consume_registration_invite(invite, "worker")
