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

    def test_rejected_management_intent_returns_its_preflight_evidence(self) -> None:
        preflight = [{
            "worker_id": "worker-b",
            "server": "Broker-B",
            "status": "rejected",
            "order": {"symbol": "USDJPY"},
            "response": {"diagnostics": {"retcode": 10015, "comment": "Invalid price"}},
        }]

        result = self.ledger.reject_management_intent(
            "ABCDEF",
            "command-123",
            {"type": "intent", "pair_id": "pair-123"},
            "A broker rejected the intent order check.",
            preflight,
        )

        self.assertEqual("rejected_preflight", result["status"])
        self.assertEqual("A broker rejected the intent order check.", result["reason"])
        self.assertEqual(preflight, result["preflight"])

    def test_migration_removes_legacy_pairing_code_column(self) -> None:
        self.ledger.close()
        path = Path(self._directory.name) / "ledger.duckdb"
        connection = duckdb.connect(str(path))
        try:
            connection.execute("DROP TABLE enrollments")
            connection.execute("CREATE TABLE enrollments (pairing_code VARCHAR, source_ip VARCHAR)")
        finally:
            connection.close()

        self.ledger = ControlLedger(path)

        columns = {row[1] for row in self.ledger._connection.execute("PRAGMA table_info('enrollments')").fetchall()}
        self.assertNotIn("pairing_code", columns)

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

    def test_management_intents_are_idempotent_and_visible_with_their_origin(self) -> None:
        payload = {
            "type": "intent",
            "pair_id": "pair-123",
            "primary_direction": "LONG",
            "lots": "0.10",
            "entry_price": "1.23450",
            "stop_loss_pips": "10",
            "take_profit_pips": "20",
            "filling_mode": "FOK",
            "expires_at": "2026-09-01T12:00:00+00:00",
        }

        accepted = self.ledger.accept_management_intent("ABCDEF", "create-001", payload, [])
        replay = self.ledger.accept_management_intent("ABCDEF", "create-001", payload, [])

        self.assertEqual(accepted, replay)
        self.assertEqual(
            [{"origin": "management", "originator": "ABCDEF", "intent_id": accepted["intent_id"]}],
            [
                {key: value for key, value in item.items() if key in {"origin", "originator", "intent_id"}}
                for item in self.ledger.intents(active_only=True, status_filter=None)
            ],
        )
        with self.assertRaisesRegex(LedgerError, "different payload"):
            self.ledger.accept_management_intent("ABCDEF", "create-001", {**payload, "lots": "0.20"}, [])

    def test_management_operation_requires_zero_fills_to_cancel_and_fills_to_flatten(self) -> None:
        payload = {
            "type": "intent",
            "pair_id": "pair-123",
            "primary_direction": "LONG",
            "lots": "0.10",
            "entry_price": "1.23450",
            "stop_loss_pips": "10",
            "take_profit_pips": "20",
            "filling_mode": "FOK",
            "expires_at": "2026-09-01T12:00:00+00:00",
        }
        zero_fill = self.ledger.accept_management_intent("ABCDEF", "create-zero", payload, [])
        result, _, scheduled = self.ledger.request_management_intent_operation(
            "ABCDEF", "cancel-zero", str(zero_fill["intent_id"]), "cancel"
        )
        self.assertEqual("cancellation_scheduled", result["status"])
        self.assertTrue(scheduled)

        filled = self.ledger.accept_management_intent("ABCDEF", "create-filled", {**payload, "pair_id": "pair-456"}, [])
        self.ledger.record_intent_execution(str(filled["intent_id"]), "intent_leg_filled", {}, status="working")
        with self.assertRaisesRegex(LedgerError, "zero-fill"):
            self.ledger.request_management_intent_operation("ABCDEF", "cancel-filled", str(filled["intent_id"]), "cancel")
        result, _, scheduled = self.ledger.request_management_intent_operation(
            "ABCDEF", "flatten-filled", str(filled["intent_id"]), "emergency_flatten"
        )
        self.assertEqual("emergency_flatten_scheduled", result["status"])
        self.assertTrue(scheduled)

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

    def test_accepted_intent_is_durable_and_idempotent(self) -> None:
        enrollment = self.ledger.create_trader_enrollment(
            strategy_name="mean-reversion",
            claimed_public_ip="203.0.113.4",
            public_key_pem="public-key",
        )
        trader_id = self.ledger.approve_trader_enrollment(
            enrollment["registration_id"], "ABCDEF", lambda trader_id, *_: f"certificate:{trader_id}"
        )
        payload = {
            "type": "intent",
            "pair_id": "pair-123",
            "primary_direction": "LONG",
            "lots": "0.1",
            "entry_price": "1.2345",
            "stop_loss_pips": "10",
            "take_profit_pips": "20",
            "filling_mode": "FOK",
            "expires_at": "2026-08-20T00:05:00+00:00",
        }
        preflight = [
            {"worker_id": "worker-a", "status": "accepted", "order": {"symbol": "EURUSD.a"}},
            {"worker_id": "worker-b", "status": "accepted", "order": {"symbol": "EURUSD"}},
        ]

        accepted = self.ledger.accept_trader_intent(trader_id, "intent-001", payload, preflight)
        replay = self.ledger.accept_trader_intent(trader_id, "intent-001", payload, preflight)

        self.assertEqual(accepted, replay)
        self.assertEqual("accepted", accepted["status"])
        self.assertTrue(accepted["intent_id"])
        self.assertEqual(
            1,
            self.ledger._connection.execute(
                "SELECT count(*) FROM trader_intents WHERE trader_id = ? AND command_id = ?",
                [trader_id, "intent-001"],
            ).fetchone()[0],
        )
        with self.assertRaisesRegex(LedgerError, "different payload"):
            self.ledger.accept_trader_intent(
                trader_id,
                "intent-001",
                {**payload, "lots": "0.2"},
                preflight,
            )

    def test_trader_cancellation_atomically_blocks_undispatched_intent_and_is_idempotent(self) -> None:
        enrollment = self.ledger.create_trader_enrollment(
            strategy_name="mean-reversion",
            claimed_public_ip="203.0.113.4",
            public_key_pem="public-key",
        )
        trader_id = self.ledger.approve_trader_enrollment(
            enrollment["registration_id"], "ABCDEF", lambda trader_id, *_: f"certificate:{trader_id}"
        )
        accepted = self.ledger.accept_trader_intent(
            trader_id,
            "intent-001",
            {
                "type": "intent", "pair_id": "pair-123", "primary_direction": "LONG", "lots": "0.1",
                "entry_price": "1.2345", "stop_loss_pips": "10", "take_profit_pips": "20",
                "filling_mode": "FOK", "expires_at": "2026-08-20T00:05:00+00:00",
            },
            [
                {"worker_id": "worker-a", "status": "accepted", "order": {"control_plane_command_id": "abt:one"}},
                {"worker_id": "worker-b", "status": "accepted", "order": {"control_plane_command_id": "abt:one"}},
            ],
        )
        payload = {"type": "cancel", "intent_id": accepted["intent_id"]}

        result, preflight, scheduled = self.ledger.request_trader_intent_cancellation(trader_id, "cancel-001", payload)
        replay, replay_preflight, replay_scheduled = self.ledger.request_trader_intent_cancellation(
            trader_id, "cancel-001", payload
        )

        self.assertEqual("cancellation_scheduled", result["status"])
        self.assertEqual(result, replay)
        self.assertEqual(preflight, replay_preflight)
        self.assertTrue(scheduled)
        self.assertFalse(replay_scheduled)
        self.assertFalse(self.ledger.claim_accepted_intent_dispatch(accepted["intent_id"]))
        self.assertEqual("cancelling", self.ledger.intent_record(accepted["intent_id"])["status"])

    def test_execution_records_are_immutable_and_visible_to_the_originating_trader(self) -> None:
        enrollment = self.ledger.create_trader_enrollment(
            strategy_name="mean-reversion",
            claimed_public_ip="203.0.113.4",
            public_key_pem="public-key",
        )
        trader_id = self.ledger.approve_trader_enrollment(
            enrollment["registration_id"], "ABCDEF", lambda trader_id, *_: f"certificate:{trader_id}"
        )
        accepted = self.ledger.accept_trader_intent(
            trader_id,
            "intent-execute-001",
            {
                "type": "intent", "pair_id": "pair-123", "primary_direction": "LONG", "lots": "0.1",
                "entry_price": "1.2345", "stop_loss_pips": "10", "take_profit_pips": "20",
                "filling_mode": "IOC", "expires_at": "2026-08-20T00:05:00+00:00",
            },
            [{"worker_id": "worker-a", "status": "accepted"}, {"worker_id": "worker-b", "status": "accepted"}],
        )

        event_id = self.ledger.record_intent_execution(
            accepted["intent_id"], "ioc_matched_volume_retained", {"volume": "0.1"}, status="working"
        )

        event = next(event for event in self.ledger.trader_events_after(trader_id, 0) if event["event_id"] == event_id)
        self.assertEqual("ioc_matched_volume_retained", event["event_type"])
        self.assertEqual("0.1", event["payload"]["volume"])

    def test_management_intent_record_preserves_exact_broker_request_response_and_timestamps(self) -> None:
        enrollment = self.ledger.create_trader_enrollment(
            strategy_name="mean-reversion",
            claimed_public_ip="203.0.113.4",
            public_key_pem="public-key",
        )
        trader_id = self.ledger.approve_trader_enrollment(
            enrollment["registration_id"], "ABCDEF", lambda trader_id, *_: f"certificate:{trader_id}"
        )
        accepted = self.ledger.accept_trader_intent(
            trader_id, "intent-record-001",
            {
                "type": "intent", "pair_id": "pair-123", "primary_direction": "LONG", "lots": "0.1",
                "entry_price": "1.23450", "stop_loss_pips": "10", "take_profit_pips": "20",
                "filling_mode": "FOK", "expires_at": "2026-08-20T00:05:00+00:00",
            },
            [{"worker_id": "worker-a", "status": "accepted"}, {"worker_id": "worker-b", "status": "accepted"}],
        )
        broker_record = {
            "worker_id": "worker-a",
            "order": {"price": "1.23450", "volume": "0.1", "control_plane_command_id": "abt:one"},
            "response": {"ticket": 17, "price": 1.2345, "volume": 0.1, "time": 1724112000},
        }
        self.ledger.record_intent_execution(accepted["intent_id"], "intent_leg_dispatched", broker_record)
        broker_record["response"]["ticket"] = 99

        record = self.ledger.intent_record(accepted["intent_id"])

        self.assertEqual("1.23450", record["intent"]["entry_price"])
        self.assertEqual(17, record["execution_records"][0]["payload"]["response"]["ticket"])
        self.assertIsNotNone(record["accepted_at"])
        self.assertIsNotNone(record["execution_records"][0]["recorded_at"])
        self.assertIsNotNone(record["execution_records"][0]["occurred_at"])

    def test_external_broker_change_freezes_related_working_intent_for_human_recovery(self) -> None:
        enrollment = self.ledger.create_trader_enrollment(
            strategy_name="mean-reversion",
            claimed_public_ip="203.0.113.4",
            public_key_pem="public-key",
        )
        trader_id = self.ledger.approve_trader_enrollment(
            enrollment["registration_id"], "ABCDEF", lambda trader_id, *_: f"certificate:{trader_id}"
        )
        accepted = self.ledger.accept_trader_intent(
            trader_id, "intent-external-change",
            {
                "type": "intent", "pair_id": "pair-123", "primary_direction": "LONG", "lots": "0.1",
                "entry_price": "1.23450", "stop_loss_pips": "10", "take_profit_pips": "20",
                "filling_mode": "IOC", "expires_at": "2026-08-20T00:05:00+00:00",
            },
            [{"worker_id": "worker-a", "status": "accepted"}, {"worker_id": "worker-b", "status": "accepted"}],
        )
        self.ledger.record_intent_execution(accepted["intent_id"], "ioc_orders_placed", {}, status="working")

        self.ledger.record_delta(
            "worker-a", 1, "2026-08-20T00:00:00+00:00", "position", "42", "created", {"volume": 0.1}
        )

        record = self.ledger.intent_record(accepted["intent_id"])
        self.assertEqual("needs_human", record["status"])
        self.assertEqual("intent_execution_frozen", record["execution_records"][-1]["event_type"])
        events = self.ledger.trader_events_after(trader_id, 0)
        self.assertIn("intent_execution_frozen", [event["event_type"] for event in events])

    def test_requires_fresh_reconciliation_from_every_worker_before_restart_recovery(self) -> None:
        started_at = datetime.now(UTC)
        stale_at = started_at - timedelta(seconds=1)
        fresh_at = started_at + timedelta(seconds=1)
        snapshot = (
            "snapshot", 0, "2026-08-20T00:00:00+00:00", "{}", "{}", "[]", "[]"
        )
        with self.ledger._transaction():
            self.ledger._connection.execute(
                """INSERT INTO reconciliation_snapshots
                   (worker_id, snapshot_id, cursor, observed_at, account, terminal, orders, positions, received_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ["worker-a", *snapshot, stale_at],
            )
            self.ledger._connection.execute(
                """INSERT INTO reconciliation_snapshots
                   (worker_id, snapshot_id, cursor, observed_at, account, terminal, orders, positions, received_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ["worker-b", *snapshot, fresh_at],
            )

        self.assertFalse(self.ledger.workers_reconciled_since(["worker-a", "worker-b"], started_at))
        with self.ledger._transaction():
            self.ledger._connection.execute(
                """INSERT INTO reconciliation_snapshots
                   (worker_id, snapshot_id, cursor, observed_at, account, terminal, orders, positions, received_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ["worker-a", "fresh-snapshot", *snapshot[1:], fresh_at],
            )
        self.assertTrue(self.ledger.workers_reconciled_since(["worker-a", "worker-b"], started_at))

    def test_registration_invite_can_be_revoked_only_before_use(self) -> None:
        invite = self.ledger.create_registration_invite("ABCDEF", "worker")

        self.ledger.revoke_registration_invite(invite, "ABCDEF")
        with self.assertRaisesRegex(LedgerError, "no longer active"):
            self.ledger.consume_registration_invite(invite, "worker")
