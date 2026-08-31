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


class PairExecutionCellLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.ledger = ControlLedger(Path(self._directory.name) / "ledger.duckdb")
        self.trader_id = self._active_trader()
        self.worker_a = self._active_worker(111111, "Broker-A")
        self.worker_b = self._active_worker(222222, "Broker-B")

    def tearDown(self) -> None:
        self.ledger.close()
        self._directory.cleanup()

    def _active_trader(self) -> str:
        enrollment = self.ledger.create_trader_enrollment(
            strategy_name="pair-execution-cell",
            claimed_public_ip="203.0.113.4",
            public_key_pem="public-key",
        )
        return self.ledger.approve_trader_enrollment(
            enrollment["registration_id"], "ABCDEF", lambda value, *_: f"certificate:{value}"
        )

    def _active_worker(self, login: int, server: str) -> str:
        challenge, _ = self.ledger.issue_enrollment_challenge()
        enrollment = self.ledger.create_enrollment(
            login=login,
            server=server,
            public_key_pem=f"worker-key-{login}",
            account_info={"login": login},
            terminal_info={"name": "MetaTrader 5"},
            password_secret_ref=f"abt/data/mt5/pending/{login}",
            enrollment_challenge=challenge,
        )
        return self.ledger.approve_enrollment(
            enrollment.enrollment_id, "ABCDEF", lambda value, *_: f"certificate:{value}"
        )

    def test_pair_execution_cell_mode_is_mutually_exclusive_with_strategy_runtime(self) -> None:
        self.ledger.claim_pair_execution_mode(self.worker_a, self.worker_b, "strategy_runtime", self.trader_id)

        with self.assertRaises(LedgerError):
            self.ledger.claim_pair_execution_mode(self.worker_a, self.worker_b, "pair_execution_cell", self.trader_id)

        # An explicit release allows the operator to then opt the pair in.
        self.ledger.release_pair_execution_mode(self.worker_a, self.worker_b, "strategy_runtime")
        self.ledger.claim_pair_execution_mode(self.worker_a, self.worker_b, "pair_execution_cell", self.trader_id)

    def test_shadow_mode_may_run_alongside_a_live_owner(self) -> None:
        self.ledger.claim_pair_execution_mode(self.worker_a, self.worker_b, "strategy_runtime", self.trader_id)

        self.ledger.claim_pair_execution_mode(self.worker_a, self.worker_b, "shadow", self.trader_id)  # does not raise

    def test_issue_pair_lease_strictly_increases_epoch_and_requires_claim(self) -> None:
        first = self.ledger.issue_pair_lease(
            leader_worker_id=self.worker_a, follower_worker_id=self.worker_b,
            trader_id=self.trader_id, contract_hash="hash-1", ttl_seconds=60,
        )
        second = self.ledger.issue_pair_lease(
            leader_worker_id=self.worker_a, follower_worker_id=self.worker_b,
            trader_id=self.trader_id, contract_hash="hash-1", ttl_seconds=60,
        )

        self.assertEqual(1, first["epoch"])
        self.assertEqual(2, second["epoch"])

    def test_lease_issuance_blocked_while_legacy_runtime_owns_the_pair(self) -> None:
        self.ledger.claim_pair_execution_mode(self.worker_a, self.worker_b, "strategy_runtime", self.trader_id)

        with self.assertRaises(LedgerError):
            self.ledger.issue_pair_lease(
                leader_worker_id=self.worker_a, follower_worker_id=self.worker_b,
                trader_id=self.trader_id, contract_hash="hash-1", ttl_seconds=60,
            )

    def test_release_pair_quarantine_records_an_audit_event_and_returns_the_push_payload(self) -> None:
        release = self.ledger.release_pair_quarantine(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            symbol="EURUSD",
            actor="alice",
            reason="broker confirmed the symbol is tradable again",
        )

        self.assertEqual("EURUSD", release["symbol"])
        self.assertEqual("alice", release["actor"])
        self.assertEqual("broker confirmed the symbol is tradable again", release["reason"])
        self.assertIn("observed_at", release)
        self.assertTrue(any(
            event["event_type"] == "pair_quarantine_release_requested"
            and event["payload"]["symbol"] == "EURUSD"
            and event["payload"]["actor"] == "alice"
            and event["payload"]["reason"] == "broker confirmed the symbol is tradable again"
            and event["payload"]["leader_worker_id"] == self.worker_a
            and event["payload"]["follower_worker_id"] == self.worker_b
            for event in self.ledger.events()
        ))

    def test_release_pair_quarantine_rejects_the_same_worker_as_leader_and_follower(self) -> None:
        with self.assertRaises(LedgerError):
            self.ledger.release_pair_quarantine(
                leader_worker_id=self.worker_a,
                follower_worker_id=self.worker_a,
                symbol="EURUSD",
                actor="alice",
                reason="operator review",
            )

    def test_release_pair_quarantine_rejects_an_unknown_worker(self) -> None:
        with self.assertRaises(LedgerError):
            self.ledger.release_pair_quarantine(
                leader_worker_id=self.worker_a,
                follower_worker_id="no-such-worker",
                symbol="EURUSD",
                actor="alice",
                reason="operator review",
            )

    def test_record_pair_quarantine_release_outcome_audits_the_per_worker_result_unconditionally(self) -> None:
        self.ledger.record_pair_quarantine_release_outcome(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            symbol="EURUSD",
            actor="alice",
            leader_outcome="applied",
            follower_outcome="rejected",
            applied=False,
        )

        self.assertTrue(any(
            event["event_type"] == "pair_quarantine_release_outcome"
            and event["payload"]["symbol"] == "EURUSD"
            and event["payload"]["actor"] == "alice"
            and event["payload"]["leader_worker_id"] == self.worker_a
            and event["payload"]["follower_worker_id"] == self.worker_b
            and event["payload"]["leader_outcome"] == "applied"
            and event["payload"]["follower_outcome"] == "rejected"
            and event["payload"]["applied"] is False
            for event in self.ledger.events()
        ))

    def test_record_pair_quarantine_release_outcome_records_a_genuine_success_too(self) -> None:
        """The audit trail is unconditional: a fully-applied release must be
        recorded, not just failures, so operators can trust it as the single
        source of truth for what happened."""

        self.ledger.record_pair_quarantine_release_outcome(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            symbol="EURUSD",
            actor="alice",
            leader_outcome="applied",
            follower_outcome="applied",
            applied=True,
        )

        self.assertTrue(any(
            event["event_type"] == "pair_quarantine_release_outcome"
            and event["payload"]["leader_outcome"] == "applied"
            and event["payload"]["follower_outcome"] == "applied"
            and event["payload"]["applied"] is True
            for event in self.ledger.events()
        ))

    def test_activate_pair_contract_creates_and_distributes_the_full_contract_and_lease(self) -> None:
        contract = {
            "contract_id": "contract-1", "leader_worker_id": self.worker_a, "follower_worker_id": self.worker_b,
            "trader_id": self.trader_id, "symbol": "EURUSD", "edge_threshold": "0.0005",
        }

        activation = self.ledger.activate_pair_contract(
            contract=contract, contract_hash="hash-1", trader_id=self.trader_id, ttl_seconds=300,
        )

        self.assertEqual(contract, activation["contract"])
        self.assertEqual(1, activation["lease"]["epoch"])
        self.assertEqual(
            activation, self.ledger.pair_cell_activation_for_worker(self.worker_a)
        )
        self.assertEqual(
            activation, self.ledger.pair_cell_activation_for_worker(self.worker_b)
        )

    def test_activate_pair_contract_is_blocked_while_a_legacy_strategy_runtime_owns_the_pair(self) -> None:
        self.ledger.claim_pair_execution_mode(self.worker_a, self.worker_b, "strategy_runtime", self.trader_id)
        contract = {
            "contract_id": "contract-1", "leader_worker_id": self.worker_a, "follower_worker_id": self.worker_b,
            "trader_id": self.trader_id, "symbol": "EURUSD",
        }

        with self.assertRaises(LedgerError):
            self.ledger.activate_pair_contract(
                contract=contract, contract_hash="hash-1", trader_id=self.trader_id, ttl_seconds=300,
            )

    def test_activate_pair_contract_stores_an_automatic_entry_edge_points_contract_opaquely(self) -> None:
        """Requirement: an automatic (``entry_edge_points``/``eligible_symbols``)
        policy contract is stored and distributed exactly like a legacy one --
        the ledger never interprets or requires a fixed symbol/direction."""

        contract = {
            "contract_id": "contract-2", "leader_worker_id": self.worker_a, "follower_worker_id": self.worker_b,
            "trader_id": self.trader_id, "entry_edge_points": "1.5",
            "eligible_symbols": ["EURUSD", "GBPUSD", "XAUUSD"],
        }

        activation = self.ledger.activate_pair_contract(
            contract=contract, contract_hash="hash-2", trader_id=self.trader_id, ttl_seconds=300,
        )

        self.assertEqual(contract, activation["contract"])
        self.assertEqual(["EURUSD", "GBPUSD", "XAUUSD"], activation["contract"]["eligible_symbols"])
        self.assertEqual(
            activation, self.ledger.pair_cell_activation_for_worker(self.worker_a)
        )

    def test_pair_relay_authorizes_and_audits_attempt_scoped_envelopes_without_interpreting_payload(self) -> None:
        lease = self.ledger.issue_pair_lease(
            leader_worker_id=self.worker_a, follower_worker_id=self.worker_b,
            trader_id=self.trader_id, contract_hash="hash-1", ttl_seconds=60,
        )
        envelope = {
            "lease": {
                "leader_worker_id": self.worker_a, "follower_worker_id": self.worker_b,
                "trader_id": self.trader_id, "contract_hash": "hash-1", "lease_epoch": lease["epoch"],
            },
            "from_worker_id": self.worker_a, "to_worker_id": self.worker_b,
            "request_id": "message-1",
            "attempt_id": "attempt-1",
            "payload": {"kind": "arm_request", "opaque_to_controller": True},
        }

        first = self.ledger.record_pair_relay(envelope)
        replay = self.ledger.record_pair_relay(envelope)
        changed = dict(envelope, payload={"kind": "arm_request", "opaque_to_controller": False})
        with self.assertRaises(LedgerError):
            self.ledger.record_pair_relay(changed)

        self.assertTrue(first["audited"])
        self.assertFalse(first.get("replay", False))
        self.assertTrue(replay["replay"])

    def test_pair_relay_permits_distinct_messages_sharing_one_attempt_id(self) -> None:
        """arm, arm-ack, commit, and leg-status all legitimately share one
        attempt_id; dedup must key on the per-message request_id instead, or
        this ordinary sequence would be rejected as reused content."""

        lease = self.ledger.issue_pair_lease(
            leader_worker_id=self.worker_a, follower_worker_id=self.worker_b,
            trader_id=self.trader_id, contract_hash="hash-1", ttl_seconds=60,
        )
        base = {
            "lease": {
                "leader_worker_id": self.worker_a, "follower_worker_id": self.worker_b,
                "trader_id": self.trader_id, "contract_hash": "hash-1", "lease_epoch": lease["epoch"],
            },
            "from_worker_id": self.worker_a, "to_worker_id": self.worker_b,
            "attempt_id": "attempt-1",
        }
        arm = self.ledger.record_pair_relay(
            {**base, "request_id": "message-arm", "payload": {"kind": "arm_request"}}
        )
        commit = self.ledger.record_pair_relay(
            {**base, "request_id": "message-commit", "payload": {"kind": "commit"}}
        )
        leg_status = self.ledger.record_pair_relay(
            {**base, "request_id": "message-leg-status", "payload": {"kind": "leg_status"}}
        )

        self.assertTrue(arm["audited"] and commit["audited"] and leg_status["audited"])
        self.assertFalse(arm.get("replay", False))
        self.assertFalse(commit.get("replay", False))
        self.assertFalse(leg_status.get("replay", False))

    def test_pair_relay_rejects_a_route_outside_the_leased_pair(self) -> None:
        lease = self.ledger.issue_pair_lease(
            leader_worker_id=self.worker_a, follower_worker_id=self.worker_b,
            trader_id=self.trader_id, contract_hash="hash-1", ttl_seconds=60,
        )
        envelope = {
            "lease": {
                "leader_worker_id": self.worker_a, "follower_worker_id": self.worker_b,
                "trader_id": self.trader_id, "contract_hash": "hash-1", "lease_epoch": lease["epoch"],
            },
            "from_worker_id": self.worker_a, "to_worker_id": "not-in-this-pair",
            "request_id": "message-1",
            "attempt_id": "attempt-1",
            "payload": {"kind": "quote"},
        }

        with self.assertRaises(LedgerError):
            self.ledger.record_pair_relay(envelope)

    def test_pair_relay_rejects_a_stale_lease_epoch(self) -> None:
        lease = self.ledger.issue_pair_lease(
            leader_worker_id=self.worker_a, follower_worker_id=self.worker_b,
            trader_id=self.trader_id, contract_hash="hash-1", ttl_seconds=60,
        )
        self.ledger.issue_pair_lease(  # bumps the epoch again
            leader_worker_id=self.worker_a, follower_worker_id=self.worker_b,
            trader_id=self.trader_id, contract_hash="hash-1", ttl_seconds=60,
        )
        envelope = {
            "lease": {
                "leader_worker_id": self.worker_a, "follower_worker_id": self.worker_b,
                "trader_id": self.trader_id, "contract_hash": "hash-1", "lease_epoch": lease["epoch"],
            },
            "from_worker_id": self.worker_a, "to_worker_id": self.worker_b,
            "request_id": "message-1",
            "payload": {"kind": "quote"},
        }

        with self.assertRaises(LedgerError):
            self.ledger.record_pair_relay(envelope)

    def test_pair_relay_does_not_durably_audit_unscoped_quote_messages(self) -> None:
        lease = self.ledger.issue_pair_lease(
            leader_worker_id=self.worker_a, follower_worker_id=self.worker_b,
            trader_id=self.trader_id, contract_hash="hash-1", ttl_seconds=60,
        )
        envelope = {
            "lease": {
                "leader_worker_id": self.worker_a, "follower_worker_id": self.worker_b,
                "trader_id": self.trader_id, "contract_hash": "hash-1", "lease_epoch": lease["epoch"],
            },
            "from_worker_id": self.worker_a, "to_worker_id": self.worker_b,
            "request_id": "message-1",
            "payload": {"kind": "quote", "bid": "1.1000"},
        }

        result = self.ledger.record_pair_relay(envelope)

        self.assertTrue(result["delivered"])
        self.assertFalse(result["audited"])
