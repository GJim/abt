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

    def test_hedged_entry_persists_complete_pair_outcome_idempotently(self) -> None:
        enrollment = self.ledger.create_trader_enrollment(
            strategy_name="realtime-arbitrage",
            claimed_public_ip="203.0.113.4",
            public_key_pem="public-key",
        )
        trader_id = self.ledger.approve_trader_enrollment(
            enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        payload = {
            "type": "hedged_entry",
            "expires_at": "2026-08-27T10:00:05+00:00",
            "first": {"worker_id": "worker-a", "symbol": "EURUSD", "volume": "0.10", "direction": "LONG"},
            "second": {"worker_id": "worker-b", "symbol": "EURUSD", "volume": "0.10", "direction": "SHORT"},
        }
        outcome = {
            "type": "command_result",
            "command_id": "entry-001",
            "status": "accepted",
            "legs": [
                {"worker_id": "worker-a", "preflight": {"status": "accepted"}, "dispatch": {"status": "accepted"}},
                {"worker_id": "worker-b", "preflight": {"status": "accepted"}, "dispatch": {"status": "accepted"}},
            ],
            "timings": {"elapsed_ms": 15},
        }

        in_progress, started = self.ledger.begin_hedged_entry_command(trader_id, "entry-001", payload)
        self.assertTrue(started)
        assert in_progress is not None
        self.assertEqual("in_progress", in_progress["status"])
        recorded = self.ledger.record_hedged_entry_command(trader_id, "entry-001", payload, outcome)
        replay = self.ledger.record_hedged_entry_command(trader_id, "entry-001", payload, {"status": "contained"})

        self.assertEqual(recorded, replay)
        self.assertEqual(recorded, self.ledger.trader_command_result(trader_id, "entry-001", payload))
        event = next(event for event in self.ledger.events() if event["event_type"] == "hedged_entry_completed")
        self.assertEqual(outcome["legs"], event["payload"]["outcome"]["legs"])
        with self.assertRaisesRegex(LedgerError, "different payload"):
            self.ledger.record_hedged_entry_command(
                trader_id, "entry-001", {**payload, "first": {**payload["first"], "volume": "0.20"}}, outcome
            )

    def test_accepted_hedged_entry_binds_controller_protection_before_paired_verification(self) -> None:
        with self.ledger._transaction():
            for worker_id, login, server in (("worker-a", 123456, "Broker-A"), ("worker-b", 654321, "Broker-B")):
                self.ledger._connection.execute(
                    """INSERT INTO workers
                       (worker_id, enrollment_id, login, server, certificate, status, approved_at)
                       VALUES (?, ?, ?, ?, ?, 'active', ?)""",
                    [worker_id, f"enrollment-{worker_id}", login, server, f"certificate:{worker_id}", datetime.now(UTC)],
                )
        owner_enrollment = self.ledger.create_trader_enrollment(
            strategy_name="realtime-arbitrage", claimed_public_ip="203.0.113.4", public_key_pem="owner-key"
        )
        owner_id = self.ledger.approve_trader_enrollment(
            owner_enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        other_enrollment = self.ledger.create_trader_enrollment(
            strategy_name="other-strategy", claimed_public_ip="203.0.113.5", public_key_pem="other-key"
        )
        other_id = self.ledger.approve_trader_enrollment(
            other_enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        legs = [
            {
                "worker_id": "worker-a",
                "ticket": "10",
                "symbol": "EURUSD",
                "side": "0",
                "volume": "0.10",
                "stop_loss": "1.09000",
                "take_profit": "1.11000",
            },
            {
                "worker_id": "worker-b",
                "ticket": "20",
                "symbol": "EURUSD",
                "side": "1",
                "volume": "0.10",
                "stop_loss": "1.11000",
                "take_profit": "1.09000",
            },
        ]

        self.ledger.begin_entry_unconfirmed(
            ["worker-a", "worker-b"],
            "entry-protected",
            "hedged_entry_dispatch_started",
            {"trader_id": owner_id, "command_id": "entry-protected", "reason": "Dispatch is about to begin."},
        )
        self.assertEqual(
            ("entry-protected", "ENTRY_UNCONFIRMED"),
            (
                self.ledger._connection.execute(
                    "SELECT pair_id, lifecycle_state FROM recovery_pairs WHERE pair_id = ?",
                    ["entry-protected"],
                ).fetchone()
            ),
        )
        with self.assertRaisesRegex(LedgerError, "Workers are already managed"):
            self.ledger.register_protected_pair(other_id, "other-pair", legs)

        accepted = self.ledger.activate_hedged_entry_protected_pair(owner_id, "entry-protected", legs)

        self.assertEqual(
            ("entry-protected", "CATCHING_UP", 0),
            (accepted["pair_id"], accepted["lifecycle_state"], accepted["revision"]),
        )
        self.assertEqual(
            {("CATCHING_UP", "PROTECTED_LEG")},
            {(record["lifecycle_state"], record["desired_state"]) for record in self.ledger.account_recoveries()},
        )
        self.ledger.record_worker_recovery_sync("worker-a", "epoch-a", [])
        self.ledger.record_worker_recovery_sync("worker-b", "epoch-b", [])
        self.ledger.record_snapshot(
            "worker-a", 0, "2026-08-27T12:00:00+00:00", {}, {}, [],
            [{"ticket": 10, "symbol": "EURUSD", "type": 0, "volume": 0.1, "sl": 1.09, "tp": 1.11}],
            recovery_epoch="superseded-epoch",
        )
        self.assertEqual(
            {"CATCHING_UP"},
            {record["lifecycle_state"] for record in self.ledger.account_recoveries()},
        )
        self.ledger.record_snapshot(
            "worker-a", 0, "2026-08-27T12:00:00+00:00", {}, {}, [],
            [{"ticket": 10, "symbol": "EURUSD", "type": 0, "volume": 0.1, "sl": 1.09, "tp": 1.11}],
            recovery_epoch="epoch-a",
        )
        self.assertEqual(
            {"CATCHING_UP"},
            {record["lifecycle_state"] for record in self.ledger.account_recoveries()},
        )
        self.ledger.record_snapshot(
            "worker-b", 0, "2026-08-27T12:00:00+00:00", {}, {}, [],
            [{"ticket": 20, "symbol": "EURUSD", "type": 1, "volume": 0.1, "sl": 1.11, "tp": 1.09}],
        )

        self.assertEqual(
            {"ACTIVE_VERIFIED"},
            {record["lifecycle_state"] for record in self.ledger.account_recoveries()},
        )
        self.assertIn("hedged_entry_protection_accepted", [event["event_type"] for event in self.ledger.events()])

    def test_hedged_entry_ticket_marks_commentless_position_delta_as_controlled(self) -> None:
        with self.ledger._transaction():
            for worker_id, login, server in [
                ("worker-a", 123456, "Broker-A"),
                ("worker-b", 654321, "Broker-B"),
            ]:
                self.ledger._connection.execute(
                    """INSERT INTO workers
                       (worker_id, enrollment_id, login, server, certificate, status, approved_at)
                       VALUES (?, ?, ?, ?, ?, 'active', ?)""",
                    [worker_id, f"enrollment-{worker_id}", login, server, f"certificate:{worker_id}", datetime.now(UTC)],
                )
        enrollment = self.ledger.create_trader_enrollment(
            strategy_name="realtime-arbitrage",
            claimed_public_ip="203.0.113.4",
            public_key_pem="public-key",
        )
        trader_id = self.ledger.approve_trader_enrollment(
            enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        payload = {
            "type": "hedged_entry",
            "expires_at": "2026-08-27T10:00:05+00:00",
            "first": {"worker_id": "worker-a", "symbol": "EURUSD", "volume": "0.10", "direction": "LONG"},
            "second": {"worker_id": "worker-b", "symbol": "EURUSD", "volume": "0.10", "direction": "SHORT"},
        }
        outcome = {
            "type": "command_result",
            "command_id": "entry-ticket-001",
            "status": "accepted",
            "legs": [
                {
                    "worker_id": "worker-a",
                    "dispatch": {
                        "status": "accepted",
                        "response": {"accepted": True, "result": {"position": {"ticket": 42}}},
                    },
                },
                {
                    "worker_id": "worker-b",
                    "dispatch": {
                        "status": "accepted",
                        "response": {"accepted": True, "result": {"position": {"ticket": 84}}},
                    },
                },
            ],
        }
        self.ledger.begin_hedged_entry_command(trader_id, "entry-ticket-001", payload)
        self.ledger.record_hedged_entry_command(trader_id, "entry-ticket-001", payload, outcome)

        self.ledger.record_delta(
            "worker-a",
            1,
            "2026-08-20T00:00:00+00:00",
            "position",
            "42",
            "created",
            {"ticket": 42, "volume": 0.1},
        )

        self.assertNotIn("external_broker_change", [event["event_type"] for event in self.ledger.events()])
        self.assertEqual([], self.ledger.account_recoveries())
        self.ledger.record_delta(
            "worker-a",
            2,
            "2026-08-20T00:01:00+00:00",
            "position",
            "42",
            "modified",
            {"ticket": 42, "volume": 0.1, "tp": 1.3},
        )
        self.assertIn("external_broker_change", [event["event_type"] for event in self.ledger.events()])

    def test_hedged_entry_delta_before_receipt_is_held_then_resolved_as_controlled(self) -> None:
        with self.ledger._transaction():
            self.ledger._connection.execute(
                """INSERT INTO workers
                   (worker_id, enrollment_id, login, server, certificate, status, approved_at)
                   VALUES ('worker-a', 'enrollment-a', 123456, 'Broker-A', 'certificate:a', 'active', ?)""",
                [datetime.now(UTC)],
            )
        enrollment = self.ledger.create_trader_enrollment(
            strategy_name="realtime-arbitrage",
            claimed_public_ip="203.0.113.4",
            public_key_pem="public-key",
        )
        trader_id = self.ledger.approve_trader_enrollment(
            enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        payload = {
            "type": "hedged_entry",
            "expires_at": "2026-08-27T10:00:05+00:00",
            "first": {"worker_id": "worker-a", "symbol": "EURUSD", "volume": "0.10", "direction": "LONG"},
            "second": {"worker_id": "worker-b", "symbol": "EURUSD", "volume": "0.10", "direction": "SHORT"},
        }
        self.ledger.begin_hedged_entry_command(trader_id, "entry-pending-001", payload)
        self.ledger.record_delta(
            "worker-a",
            1,
            "2026-08-20T00:00:00+00:00",
            "position",
            "42",
            "created",
            {"ticket": 42, "volume": 0.1},
        )
        pending = self.ledger._connection.execute(
            """SELECT state FROM trader_ticket_transitions
               WHERE worker_id = 'worker-a' AND ticket = '42'"""
        ).fetchone()
        self.assertEqual(("pending",), pending)
        self.assertNotIn("external_broker_change", [event["event_type"] for event in self.ledger.events()])

        self.ledger.record_hedged_entry_command(
            trader_id,
            "entry-pending-001",
            payload,
            {
                "type": "command_result",
                "command_id": "entry-pending-001",
                "status": "accepted",
                "legs": [{
                    "worker_id": "worker-a",
                    "dispatch": {
                        "status": "accepted",
                        "response": {"accepted": True, "result": {"position": {"ticket": 42}}},
                    },
                }],
            },
        )

        resolved = self.ledger._connection.execute(
            """SELECT state FROM trader_ticket_transitions
               WHERE worker_id = 'worker-a' AND ticket = '42'"""
        ).fetchone()
        self.assertEqual(("bound",), resolved)
        self.assertIn("trader_ticket_attribution_resolved", [event["event_type"] for event in self.ledger.events()])

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

    def test_migration_converts_legacy_freeze_to_empty_convergence(self) -> None:
        self.ledger.close()
        path = Path(self._directory.name) / "ledger.duckdb"
        connection = duckdb.connect(str(path))
        try:
            connection.execute(
                """INSERT INTO workers
                   (worker_id, enrollment_id, login, server, certificate, status, approved_at)
                   VALUES ('legacy-worker', 'legacy-enrollment', 123456, 'Broker-Demo', 'certificate:legacy',
                           'active', ?)""",
                [datetime.now(UTC)],
            )
            connection.execute(
                """CREATE TABLE worker_freezes (
                       freeze_id VARCHAR, worker_id VARCHAR, source VARCHAR,
                       affected_worker_ids JSON, audit JSON, frozen_at TIMESTAMPTZ, event_id BIGINT
                   )"""
            )
            connection.execute(
                """INSERT INTO worker_freezes VALUES
                   ('freeze-1', 'legacy-worker', 'legacy', '["legacy-worker"]', '{}', ?, 1)""",
                [datetime.now(UTC)],
            )
        finally:
            connection.close()

        self.ledger = ControlLedger(path)

        [recovery] = self.ledger.account_recoveries()
        self.assertEqual("legacy-worker", recovery["worker_id"])
        self.assertEqual(("CONVERGING_EMPTY", "EMPTY", "REQUEST_SNAPSHOT"), (
            recovery["lifecycle_state"], recovery["desired_state"], recovery["directive"]["kind"]
        ))

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


    def test_unknown_entry_converges_accounts_to_empty(self) -> None:
        with self.ledger._transaction():
            self.ledger._connection.execute(
                """INSERT INTO workers
                   (worker_id, enrollment_id, login, server, certificate, status, approved_at)
                   VALUES ('worker-a', 'enrollment-a', 123456, 'Broker-A', 'certificate:a', 'active', ?)""",
                [datetime.now(UTC)],
            )

        [recovery] = self.ledger.begin_empty_convergence(
            ["worker-a"],
            "hedged_entry_execution_anomaly",
            {"reason": "Broker receipt was unavailable."},
        )
        self.assertEqual(("CONVERGING_EMPTY", "EMPTY", "REQUEST_SNAPSHOT"), (
            recovery["lifecycle_state"], recovery["desired_state"], recovery["directive"]["kind"]
        ))
        self.assertTrue(
            self.ledger.recover_worker_disconnect(
                "worker-a", "worker_session_disconnected", {"reason": "Worker connection was lost."}
            )
        )
        self.assertEqual({"worker-a"}, self.ledger.recovery_admission_blocked(["worker-a"]))
        self.ledger.record_delta(
            "worker-a",
            1,
            "2026-08-20T00:00:00+00:00",
            "position",
            "20",
            "removed",
            {"ticket": 20},
        )
        self.assertIn("account_recovery_effect_observed", [event["event_type"] for event in self.ledger.events()])
        self.assertNotIn("external_broker_change", [event["event_type"] for event in self.ledger.events()])
        cancelling = self.ledger.record_recovery_observation(
            "worker-a", [{"ticket": 10, "volume_current": 0.1}], [{"ticket": 20, "volume": 0.1}]
        )
        assert cancelling is not None
        self.assertEqual(("CANCEL_ORDERS", [ "10" ]), (
            cancelling["directive"]["kind"], cancelling["directive"]["tickets"]
        ))
        closing = self.ledger.record_recovery_observation("worker-a", [], [{"ticket": 20, "volume": 0.1}])
        assert closing is not None
        self.assertEqual("CLOSE_POSITIONS", closing["directive"]["kind"])
        ready = self.ledger.record_recovery_observation("worker-a", [], [])
        assert ready is not None
        self.assertEqual("READY", ready["lifecycle_state"])
        self.assertEqual(set(), self.ledger.recovery_admission_blocked(["worker-a"]))

    def test_post_dispatch_entry_uncertainty_is_durable_before_empty_convergence(self) -> None:
        with self.ledger._transaction():
            for worker_id, login, server in (("worker-a", 123456, "Broker-A"), ("worker-b", 654321, "Broker-B")):
                self.ledger._connection.execute(
                    """INSERT INTO workers
                       (worker_id, enrollment_id, login, server, certificate, status, approved_at)
                       VALUES (?, ?, ?, ?, ?, 'active', ?)""",
                    [worker_id, f"enrollment-{worker_id}", login, server, f"certificate:{worker_id}", datetime.now(UTC)],
                )
        audit = {
            "trader_id": "trader-1",
            "command_id": "entry-1",
            "reason": "A market-leg outcome was failed or unknown after paired dispatch.",
            "legs": [],
        }

        recoveries = self.ledger.begin_entry_unconfirmed(
            ["worker-a", "worker-b"], "hedged-entry:entry-1", "hedged_entry_execution_anomaly", audit
        )

        self.assertEqual(
            {("ENTRY_UNCONFIRMED", "EMPTY", "REQUEST_SNAPSHOT")},
            {(record["lifecycle_state"], record["desired_state"], record["directive"]["kind"]) for record in recoveries},
        )
        transitions = [
            event
            for event in self.ledger.events()
            if event["event_type"] == "account_recovery_transition"
            and event["payload"]["source"] == "hedged_entry_execution_anomaly"
        ]
        self.assertEqual({"worker-a", "worker-b"}, {event["payload"]["worker_id"] for event in transitions})
        event = next(event for event in self.ledger.events() if event["event_type"] == "hedged_entry_unconfirmed")
        self.assertEqual(("hedged-entry:entry-1", ["worker-a", "worker-b"]), (
            event["payload"]["incident_id"], event["payload"]["worker_ids"]
        ))

    def test_single_leg_direct_observation_cannot_verify_a_protected_pair(self) -> None:
        enrollment = self.ledger.create_trader_enrollment(
            strategy_name="realtime-arbitrage", claimed_public_ip="203.0.113.4", public_key_pem="trader-key"
        )
        trader_id = self.ledger.approve_trader_enrollment(
            enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        with self.ledger._transaction():
            for worker_id, login, server in (("worker-a", 123456, "Broker-A"), ("worker-b", 654321, "Broker-B")):
                self.ledger._connection.execute(
                    """INSERT INTO workers
                       (worker_id, enrollment_id, login, server, certificate, status, approved_at)
                       VALUES (?, ?, ?, ?, ?, 'active', ?)""",
                    [worker_id, f"enrollment-{worker_id}", login, server, f"certificate:{worker_id}", datetime.now(UTC)],
                )
        self.ledger.register_protected_pair(
            trader_id,
            "pair-1",
            [
                {"worker_id": "worker-a", "ticket": "10", "symbol": "EURUSD.a", "side": "0", "volume": "0.1", "stop_loss": "1.0", "take_profit": "1.2"},
                {"worker_id": "worker-b", "ticket": "20", "symbol": "EURUSD", "side": "1", "volume": "0.1", "stop_loss": "1.3", "take_profit": "1.1"},
            ],
        )

        observed = self.ledger.record_recovery_observation(
            "worker-a", [], [{"ticket": 10, "symbol": "EURUSD.a", "type": 0, "volume": 0.1, "sl": 1.0, "tp": 1.2}]
        )

        assert observed is not None
        self.assertEqual(
            ("CATCHING_UP", "PROTECTED_LEG", "REQUEST_SNAPSHOT"),
            (observed["lifecycle_state"], observed["desired_state"], observed["directive"]["kind"]),
        )

    def test_revocation_remains_a_terminal_account_recovery_state(self) -> None:
        with self.ledger._transaction():
            for worker_id, login, server in (("worker-a", 123456, "Broker-A"), ("worker-b", 654321, "Broker-B")):
                self.ledger._connection.execute(
                    """INSERT INTO workers
                       (worker_id, enrollment_id, login, server, certificate, status, approved_at)
                       VALUES (?, ?, ?, ?, ?, 'active', ?)""",
                    [worker_id, f"enrollment-{worker_id}", login, server, f"certificate:{worker_id}", datetime.now(UTC)],
                )

        self.ledger.revoke_worker("worker-a", "ABCDEF")
        recoveries = self.ledger.begin_empty_convergence(
            ["worker-a", "worker-b"],
            "intent_execution_anomaly",
            {"reason": "A paired broker outcome is uncertain."},
        )

        states = {record["worker_id"]: record["lifecycle_state"] for record in recoveries}
        self.assertEqual({"worker-a": "REVOKED", "worker-b": "CONVERGING_EMPTY"}, states)
        observed = self.ledger.record_recovery_observation("worker-a", [], [])
        assert observed is not None
        self.assertEqual("REVOKED", observed["lifecycle_state"])
        self.assertEqual({"worker-a", "worker-b"}, self.ledger.recovery_admission_blocked(["worker-a", "worker-b"]))

    def test_protected_pair_requires_fresh_matching_snapshots_from_both_workers(self) -> None:
        enrollment = self.ledger.create_trader_enrollment(
            strategy_name="realtime-arbitrage", claimed_public_ip="203.0.113.4", public_key_pem="trader-key"
        )
        trader_id = self.ledger.approve_trader_enrollment(
            enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        with self.ledger._transaction():
            for worker_id, login, server in (("worker-a", 123456, "Broker-A"), ("worker-b", 654321, "Broker-B")):
                self.ledger._connection.execute(
                    """INSERT INTO workers
                       (worker_id, enrollment_id, login, server, certificate, status, approved_at)
                       VALUES (?, ?, ?, ?, ?, 'active', ?)""",
                    [worker_id, f"enrollment-{worker_id}", login, server, f"certificate:{worker_id}", datetime.now(UTC)],
                )
        self.ledger.register_protected_pair(
            trader_id,
            "pair-1",
            [
                {"worker_id": "worker-a", "ticket": "10", "symbol": "EURUSD.a", "side": "0", "volume": "0.1", "stop_loss": "1.0", "take_profit": "1.2"},
                {"worker_id": "worker-b", "ticket": "20", "symbol": "EURUSD", "side": "1", "volume": "0.1", "stop_loss": "1.3", "take_profit": "1.1"},
            ],
        )
        self.ledger.record_worker_recovery_sync("worker-a", "epoch-a", [])
        self.ledger.record_worker_recovery_sync("worker-b", "epoch-b", [])
        self.ledger.record_snapshot(
            "worker-a", 0, "2026-08-27T12:00:00+00:00", {}, {}, [],
            [{"ticket": 10, "symbol": "EURUSD.a", "type": 0, "volume": 0.1, "sl": 1.0, "tp": 1.2}],
        )
        self.assertEqual("CATCHING_UP", self.ledger.account_recoveries()[0]["lifecycle_state"])
        self.ledger.record_snapshot(
            "worker-b", 0, "2026-08-27T12:00:00+00:00", {}, {}, [],
            [{"ticket": 20, "symbol": "EURUSD", "type": 1, "volume": 0.1, "sl": 1.3, "tp": 1.1}],
        )
        states = {item["worker_id"]: item["lifecycle_state"] for item in self.ledger.account_recoveries()}
        self.assertEqual({"worker-a": "ACTIVE_VERIFIED", "worker-b": "ACTIVE_VERIFIED"}, states)

        self.ledger.record_snapshot(
            "worker-a", 0, "2026-08-27T12:01:00+00:00", {}, {}, [],
            [{"ticket": 10, "symbol": "EURUSD.a", "type": 0, "volume": 0.2, "sl": 1.0, "tp": 1.2}],
        )
        states = {item["worker_id"]: item["lifecycle_state"] for item in self.ledger.account_recoveries()}
        self.assertEqual({"worker-a": "CONVERGING_EMPTY", "worker-b": "CONVERGING_EMPTY"}, states)

    def test_generic_trader_operations_cannot_manage_registered_protected_pair_accounts(self) -> None:
        enrollment = self.ledger.create_trader_enrollment(
            strategy_name="realtime-arbitrage", claimed_public_ip="203.0.113.4", public_key_pem="trader-key"
        )
        trader_id = self.ledger.approve_trader_enrollment(
            enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        other_enrollment = self.ledger.create_trader_enrollment(
            strategy_name="other-strategy", claimed_public_ip="203.0.113.5", public_key_pem="other-trader-key"
        )
        other_trader_id = self.ledger.approve_trader_enrollment(
            other_enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        worker_ids = []
        for login, server in ((123456, "Broker-A"), (654321, "Broker-B")):
            challenge, _ = self.ledger.issue_enrollment_challenge()
            enrollment_record = self.ledger.create_enrollment(
                login=login,
                server=server,
                public_key_pem=f"worker-key-{login}",
                account_info={"login": login},
                terminal_info={"name": "MetaTrader 5"},
                password_secret_ref=f"abt/data/mt5/pending/{login}",
                enrollment_challenge=challenge,
            )
            worker_ids.append(
                self.ledger.approve_enrollment(
                    enrollment_record.enrollment_id, "ABCDEF", lambda worker_id, *_: f"certificate:{worker_id}"
                )
            )
        first_worker_id, second_worker_id = worker_ids
        self.ledger.register_protected_pair(
            trader_id,
            "pair-1",
            [
                {"worker_id": first_worker_id, "ticket": "10", "symbol": "EURUSD.a", "side": "0", "volume": "0.1", "stop_loss": "1.0", "take_profit": "1.2"},
                {"worker_id": second_worker_id, "ticket": "20", "symbol": "EURUSD", "side": "1", "volume": "0.1", "stop_loss": "1.3", "take_profit": "1.1"},
            ],
        )
        self.ledger._connection.execute(
            """INSERT INTO trader_ticket_transitions
                   (worker_id, ticket, trader_id, command_id, expected_entity, expected_change, state, recorded_at)
               VALUES (?, '10', ?, 'entry-1', 'position', 'created', 'bound', ?)""",
            [first_worker_id, trader_id, datetime.now(UTC)],
        )
        self.ledger.record_worker_recovery_sync(first_worker_id, "epoch-a", [])
        self.ledger.record_worker_recovery_sync(second_worker_id, "epoch-b", [])
        self.ledger.record_snapshot(
            first_worker_id, 0, "2026-08-27T12:00:00+00:00", {}, {}, [],
            [{"ticket": 10, "symbol": "EURUSD.a", "type": 0, "volume": 0.1, "sl": 1.0, "tp": 1.2}],
        )
        self.ledger.record_snapshot(
            second_worker_id, 0, "2026-08-27T12:00:00+00:00", {}, {}, [],
            [{"ticket": 20, "symbol": "EURUSD", "type": 1, "volume": 0.1, "sl": 1.3, "tp": 1.1}],
        )
        self.ledger.record_worker_session(first_worker_id)

        close = self.ledger.request_trader_worker_rpc(
            trader_id, "close-expected", first_worker_id, "operation", {"type": "close", "ticket": "10", "volume": "0.1"}
        )
        entry = self.ledger.request_trader_worker_rpc(
            trader_id, "new-entry", first_worker_id, "operation", {"type": "market", "symbol": "EURUSD.a", "volume": "0.1"}
        )
        other_close = self.ledger.request_trader_worker_rpc(
            trader_id, "close-other", first_worker_id, "operation", {"type": "close", "ticket": "11", "volume": "0.1"}
        )
        unowned_close = self.ledger.request_trader_worker_rpc(
            other_trader_id, "close-unowned", first_worker_id, "operation", {"type": "close", "ticket": "10", "volume": "0.1"}
        )

        self.assertFalse(close["dispatch"])
        self.assertFalse(entry["dispatch"])
        self.assertFalse(other_close["dispatch"])
        self.assertFalse(unowned_close["dispatch"])
        pair_close = self.ledger.request_protected_pair_close(
            trader_id, "pair-close-active", {"type": "protected_pair_close", "pair_id": "pair-1"}
        )
        self.assertEqual(
            ("accepted", "CONVERGING_EMPTY"),
            (pair_close["status"], pair_close["lifecycle_state"]),
        )
        self.assertEqual([first_worker_id, second_worker_id], pair_close["worker_ids"])
        ready = self.ledger.record_recovery_observation(first_worker_id, [], [])
        assert ready is not None
        self.assertEqual("READY", ready["lifecycle_state"])
        after_empty = self.ledger.request_trader_worker_rpc(
            trader_id,
            "write-after-empty",
            first_worker_id,
            "operation",
            {"type": "market", "symbol": "EURUSD.a", "volume": "0.1"},
        )
        self.assertFalse(after_empty["dispatch"])
        second_ready = self.ledger.record_recovery_observation(second_worker_id, [], [])
        assert second_ready is not None
        self.assertEqual("READY", second_ready["lifecycle_state"])
        after_pair_empty = self.ledger.request_trader_worker_rpc(
            trader_id,
            "write-after-pair-empty",
            first_worker_id,
            "operation",
            {"type": "market", "symbol": "EURUSD.a", "volume": "0.1"},
        )
        self.assertTrue(after_pair_empty["dispatch"])
        completion = next(event for event in self.ledger.events() if event["event_type"] == "recovery_pair_empty_verified")
        self.assertEqual((trader_id, "pair-1"), (completion["payload"]["trader_id"], completion["payload"]["pair_id"]))
        self.assertIn(
            completion["event_id"],
            [event["event_id"] for event in self.ledger.trader_events_after(trader_id, 0)],
        )
        self.assertEqual(
            ("expired",),
            self.ledger._connection.execute(
                """SELECT state FROM trader_ticket_transitions
                   WHERE worker_id = ? AND ticket = '10'""",
                [first_worker_id],
            ).fetchone(),
        )
        repeat_close = self.ledger.request_protected_pair_close(
            trader_id, "pair-close-empty", {"type": "protected_pair_close", "pair_id": "pair-1"}
        )
        self.assertEqual(
            ("accepted", "already_empty_verified", "EMPTY_VERIFIED"),
            (repeat_close["status"], repeat_close["reason_code"], repeat_close["lifecycle_state"]),
        )
        after_terminal_pair_close = self.ledger.request_trader_worker_rpc(
            trader_id,
            "write-after-terminal-pair-close",
            first_worker_id,
            "operation",
            {"type": "market", "symbol": "EURUSD.a", "volume": "0.1"},
        )
        self.assertTrue(after_terminal_pair_close["dispatch"])
        self.ledger.record_delta(
            first_worker_id,
            1,
            "2026-08-20T00:00:00+00:00",
            "position",
            "10",
            "modified",
            {"ticket": 10, "volume": 0.1, "tp": 1.3},
        )
        self.assertIn("external_broker_change", [event["event_type"] for event in self.ledger.events()])
        self.ledger.report_worker_recovery_state(first_worker_id, "needs_human", "Broker outcome is uncertain.")
        terminal_close = self.ledger.request_protected_pair_close(
            trader_id, "pair-close-needs-human", {"type": "protected_pair_close", "pair_id": "pair-1"}
        )
        self.assertEqual(
            ("rejected", "protected_pair_terminal"),
            (terminal_close["status"], terminal_close["reason_code"]),
        )

    def test_protected_pair_close_is_owner_scoped_idempotent_empty_convergence(self) -> None:
        enrollment = self.ledger.create_trader_enrollment(
            strategy_name="realtime-arbitrage", claimed_public_ip="203.0.113.4", public_key_pem="trader-key"
        )
        trader_id = self.ledger.approve_trader_enrollment(
            enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        other_enrollment = self.ledger.create_trader_enrollment(
            strategy_name="other-strategy", claimed_public_ip="203.0.113.5", public_key_pem="other-trader-key"
        )
        other_trader_id = self.ledger.approve_trader_enrollment(
            other_enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        with self.ledger._transaction():
            for worker_id, login, server in (("worker-a", 123456, "Broker-A"), ("worker-b", 654321, "Broker-B")):
                self.ledger._connection.execute(
                    """INSERT INTO workers
                       (worker_id, enrollment_id, login, server, certificate, status, approved_at)
                       VALUES (?, ?, ?, ?, ?, 'active', ?)""",
                    [worker_id, f"enrollment-{worker_id}", login, server, f"certificate:{worker_id}", datetime.now(UTC)],
                )
        self.ledger.register_protected_pair(
            trader_id,
            "pair-close",
            [
                {"worker_id": "worker-a", "ticket": "10", "symbol": "EURUSD.a", "side": "0", "volume": "0.1", "stop_loss": "1.0", "take_profit": "1.2"},
                {"worker_id": "worker-b", "ticket": "20", "symbol": "EURUSD", "side": "1", "volume": "0.1", "stop_loss": "1.3", "take_profit": "1.1"},
            ],
        )
        payload = {"type": "protected_pair_close", "pair_id": "pair-close"}

        accepted = self.ledger.request_protected_pair_close(trader_id, "close-001", payload)
        replay = self.ledger.request_protected_pair_close(trader_id, "close-001", payload)

        self.assertEqual(accepted, replay)
        self.assertEqual(
            ("accepted", "desired_empty_requested", "pair-close", 1, "CONVERGING_EMPTY"),
            tuple(accepted[key] for key in ("status", "reason_code", "pair_id", "revision", "lifecycle_state")),
        )
        self.assertEqual(
            {("CONVERGING_EMPTY", "EMPTY"), ("CONVERGING_EMPTY", "EMPTY")},
            {(record["lifecycle_state"], record["desired_state"]) for record in self.ledger.account_recoveries()},
        )
        event = next(event for event in self.ledger.events() if event["event_type"] == "protected_pair_close_requested")
        self.assertEqual(trader_id, event["payload"]["trader_id"])
        self.assertEqual("pair-close", event["payload"]["outcome"]["pair_id"])

        unowned = self.ledger.request_protected_pair_close(
            other_trader_id, "close-unowned", {"type": "protected_pair_close", "pair_id": "pair-close"}
        )
        missing = self.ledger.request_protected_pair_close(
            trader_id, "close-missing", {"type": "protected_pair_close", "pair_id": "pair-missing"}
        )
        self.ledger.revoke_worker("worker-a", "ABCDEF")
        terminal = self.ledger.request_protected_pair_close(
            trader_id, "close-terminal", {"type": "protected_pair_close", "pair_id": "pair-close"}
        )

        self.assertEqual(("rejected", "protected_pair_not_owned"), (unowned["status"], unowned["reason_code"]))
        self.assertEqual(("rejected", "protected_pair_not_found", "MISSING"), (
            missing["status"], missing["reason_code"], missing["lifecycle_state"]
        ))
        self.assertEqual(("rejected", "protected_pair_terminal"), (terminal["status"], terminal["reason_code"]))

    def test_protected_pair_disconnect_waits_for_snapshots(self) -> None:
        enrollment = self.ledger.create_trader_enrollment(
            strategy_name="realtime-arbitrage", claimed_public_ip="203.0.113.4", public_key_pem="trader-key"
        )
        trader_id = self.ledger.approve_trader_enrollment(
            enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )
        with self.ledger._transaction():
            for worker_id, login, server in (("worker-a", 123456, "Broker-A"), ("worker-b", 654321, "Broker-B")):
                self.ledger._connection.execute(
                    """INSERT INTO workers
                       (worker_id, enrollment_id, login, server, certificate, status, approved_at)
                       VALUES (?, ?, ?, ?, ?, 'active', ?)""",
                    [worker_id, f"enrollment-{worker_id}", login, server, f"certificate:{worker_id}", datetime.now(UTC)],
                )
        self.ledger.register_protected_pair(
            trader_id,
            "pair-1",
            [
                {"worker_id": "worker-a", "ticket": "10", "symbol": "EURUSD.a", "side": "0", "volume": "0.1", "stop_loss": "1.0", "take_profit": "1.2"},
                {"worker_id": "worker-b", "ticket": "20", "symbol": "EURUSD", "side": "1", "volume": "0.1", "stop_loss": "1.3", "take_profit": "1.1"},
            ],
        )
        self.ledger.record_worker_recovery_sync("worker-a", "epoch-a", [])
        self.ledger.record_worker_recovery_sync("worker-b", "epoch-b", [])
        self.ledger.record_snapshot(
            "worker-a", 0, "2026-08-27T12:00:00+00:00", {}, {}, [],
            [{"ticket": 10, "symbol": "EURUSD.a", "type": 0, "volume": 0.1, "sl": 1.0, "tp": 1.2}],
        )
        self.ledger.record_snapshot(
            "worker-b", 0, "2026-08-27T12:00:00+00:00", {}, {}, [],
            [{"ticket": 20, "symbol": "EURUSD", "type": 1, "volume": 0.1, "sl": 1.3, "tp": 1.1}],
        )

        self.assertTrue(self.ledger.recover_worker_disconnect("worker-a", "worker_session_disconnected", {"reason": "lost"}))

        states = {item["worker_id"]: item["lifecycle_state"] for item in self.ledger.account_recoveries()}
        self.assertEqual({"worker-a": "CATCHING_UP", "worker-b": "CATCHING_UP"}, states)
        self.assertEqual({"worker-a", "worker-b"}, self.ledger.recovery_admission_blocked(["worker-a", "worker-b"]))
        self.ledger.record_worker_recovery_sync("worker-a", "epoch-a-reconnected", [])
        self.ledger.record_snapshot(
            "worker-a", 0, "2026-08-27T12:01:00+00:00", {}, {}, [],
            [{"ticket": 10, "symbol": "EURUSD.a", "type": 0, "volume": 0.1, "sl": 1.0, "tp": 1.2}],
        )
        states = {item["worker_id"]: item["lifecycle_state"] for item in self.ledger.account_recoveries()}
        self.assertEqual({"worker-a": "CATCHING_UP", "worker-b": "CATCHING_UP"}, states)




    def test_management_operation_accepts_only_zero_fill_cancellation(self) -> None:
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
        with self.assertRaisesRegex(LedgerError, "operation is invalid"):
            self.ledger.request_management_intent_operation(
                "ABCDEF", "flatten-filled", str(filled["intent_id"]), "emergency_flatten"
            )

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

    def test_trader_worker_rpc_allows_only_its_first_admission_to_dispatch(self) -> None:
        trader_enrollment = self.ledger.create_trader_enrollment(
            strategy_name="mean-reversion", claimed_public_ip="203.0.113.4", public_key_pem="trader-key"
        )
        trader_id = self.ledger.approve_trader_enrollment(
            trader_enrollment["registration_id"], "ABCDEF", lambda trader_id, *_: f"certificate:{trader_id}"
        )
        challenge, _ = self.ledger.issue_enrollment_challenge()
        worker_enrollment = self.ledger.create_enrollment(
            login=123456,
            server="Broker-Demo",
            public_key_pem="worker-key",
            account_info={"login": 123456},
            terminal_info={"name": "MetaTrader 5"},
            password_secret_ref="abt/data/mt5/pending/worker",
            enrollment_challenge=challenge,
        )
        worker_id = self.ledger.approve_enrollment(
            worker_enrollment.enrollment_id, "ABCDEF", lambda worker_id, *_: f"certificate:{worker_id}"
        )
        payload = {"type": "market", "symbol": "EURUSD", "volume": "0.1"}

        first = self.ledger.request_trader_worker_rpc(trader_id, "request-1", worker_id, "operation", payload)
        retry = self.ledger.request_trader_worker_rpc(trader_id, "request-1", worker_id, "operation", payload)

        self.assertTrue(first["dispatch"])
        self.assertFalse(retry["dispatch"])
        self.assertEqual("recorded", retry["status"])

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

    def test_external_broker_change_starts_related_account_recovery(self) -> None:
        with self.ledger._transaction():
            for worker_id, login, server in [
                ("worker-a", 123456, "Broker-A"),
                ("worker-b", 654321, "Broker-B"),
            ]:
                self.ledger._connection.execute(
                    """INSERT INTO workers
                       (worker_id, enrollment_id, login, server, certificate, status, approved_at)
                       VALUES (?, ?, ?, ?, ?, 'active', ?)""",
                    [worker_id, f"enrollment-{worker_id}", login, server, f"certificate:{worker_id}", datetime.now(UTC)],
                )
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
            [
                {"worker_id": "worker-a", "status": "accepted", "order": {"symbol": "EURUSD.a"}},
                {"worker_id": "worker-b", "status": "accepted", "order": {"symbol": "EURUSD"}},
            ],
        )
        self.ledger.record_intent_execution(accepted["intent_id"], "ioc_orders_placed", {}, status="working")

        self.ledger.record_delta(
            "worker-a", 1, "2026-08-20T00:00:00+00:00", "position", "42", "created", {"volume": 0.1}
        )

        record = self.ledger.intent_record(accepted["intent_id"])
        self.assertEqual("needs_human", record["status"])
        self.assertEqual("intent_account_recovery_started", record["execution_records"][-1]["event_type"])
        recoveries = {item["worker_id"]: item["lifecycle_state"] for item in self.ledger.account_recoveries()}
        self.assertEqual({"worker-a": "CONVERGING_EMPTY", "worker-b": "CONVERGING_EMPTY"}, recoveries)
        events = self.ledger.trader_events_after(trader_id, 0)
        self.assertIn("intent_account_recovery_started", [event["event_type"] for event in events])

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
