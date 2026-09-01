from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from abt.controlplane.ledger import AuthenticationError, ControlLedger, LedgerError
from abt.trader_protocol import MAX_PAIR_RELAY_ENVELOPE_BYTES


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
    """The controller's whole Pair Execution Cell authority: authenticate the
    Workers, arbitrate exclusive route ownership through the two-phase pairing
    workflow, and relay opaque envelopes. No Trader identity, no lease, no
    lifecycle state, no administrator route creation."""

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
            strategy_name="legacy-strategy-runtime",
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

    def _pair(
        self, leader_worker_id: str | None = None, follower_worker_id: str | None = None
    ) -> dict[str, object]:
        """Run the whole two-phase workflow and return the durable route."""

        leader = leader_worker_id or self.worker_a
        follower = follower_worker_id or self.worker_b
        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=leader, follower_worker_id=follower, connected_worker_ids={leader, follower}
        )
        return self.ledger.create_pair_route(
            proposal_id=str(reservation["proposal_id"]), connected_worker_ids={leader, follower}
        )

    def _envelope(self, route_id: str, **overrides: object) -> dict[str, object]:
        envelope: dict[str, object] = {
            "type": "pair_cell_relay",
            "protocol_version": 1,
            "route": {
                "route_id": route_id,
                "leader_worker_id": self.worker_a,
                "follower_worker_id": self.worker_b,
            },
            "from_worker_id": self.worker_a,
            "to_worker_id": self.worker_b,
            "request_id": "message-1",
            "payload_hash": "unused-by-controller",
            "payload": {"kind": "entry_attempt", "opaque_to_controller": True},
        }
        envelope.update(overrides)
        return envelope

    # ---- Role declaration and availability ---- #

    def test_a_worker_that_declares_no_role_is_an_available_follower(self) -> None:
        """Requirement: an omitted role means the Worker is simply an
        available follower -- authenticated, connected, unpaired, unreserved,
        and willing to be selected."""

        declaration = self.ledger.declare_pair_cell_role(self.worker_b)

        self.assertEqual("follower", declaration["role"])
        self.assertIsNone(declaration["declared_role"])
        self.assertIsNone(declaration["route"])
        self.assertEqual(
            [self.worker_b],
            [
                follower["worker_id"]
                for follower in self.ledger.available_pair_followers(
                    requesting_worker_id=self.worker_a,
                    connected_worker_ids={self.worker_a, self.worker_b},
                )
            ],
        )

    def test_a_connected_worker_that_never_declared_anything_is_still_listed(self) -> None:
        """Default availability: no declaration at all is the same statement
        as an omitted role, so a fresh Worker is selectable without any
        startup flag."""

        followers = self.ledger.available_pair_followers(
            requesting_worker_id=self.worker_a, connected_worker_ids={self.worker_a, self.worker_b}
        )

        self.assertEqual([self.worker_b], [follower["worker_id"] for follower in followers])
        self.assertIsNone(followers[0]["declared_role"])
        self.assertEqual(222222, followers[0]["login"])

    def test_a_declared_leader_is_not_offered_as_a_follower(self) -> None:
        self.ledger.declare_pair_cell_role(self.worker_b, "leader")

        self.assertEqual(
            [],
            self.ledger.available_pair_followers(
                requesting_worker_id=self.worker_a,
                connected_worker_ids={self.worker_a, self.worker_b},
            ),
        )

    def test_only_currently_connected_followers_are_listed(self) -> None:
        """Requirement: the leader lists *currently connected* followers. A
        known, authenticated, unpaired Worker that is not connected right now
        is not selectable."""

        self.ledger.declare_pair_cell_role(self.worker_b, "follower")

        self.assertEqual(
            [],
            self.ledger.available_pair_followers(
                requesting_worker_id=self.worker_a, connected_worker_ids={self.worker_a}
            ),
        )

    def test_a_routed_or_reserved_follower_is_not_listed(self) -> None:
        third = self._active_worker(333333, "Broker-C")
        fourth = self._active_worker(444444, "Broker-D")
        self._pair(self.worker_a, self.worker_b)
        self.ledger.reserve_pair_route_proposal(
            leader_worker_id=third, follower_worker_id=fourth, connected_worker_ids={third, fourth}
        )

        followers = self.ledger.available_pair_followers(
            requesting_worker_id=third,
            connected_worker_ids={self.worker_a, self.worker_b, third, fourth},
        )

        self.assertEqual([], followers)

    def test_a_leader_never_lists_itself(self) -> None:
        self.assertEqual(
            [],
            self.ledger.available_pair_followers(
                requesting_worker_id=self.worker_a, connected_worker_ids={self.worker_a}
            ),
        )

    def test_a_revoked_worker_cannot_declare_a_role_or_list_followers(self) -> None:
        self.ledger.revoke_worker(self.worker_a, "ABCDEF")

        with self.assertRaises(LedgerError):
            self.ledger.declare_pair_cell_role(self.worker_a, "leader")
        with self.assertRaises(LedgerError):
            self.ledger.available_pair_followers(
                requesting_worker_id=self.worker_a, connected_worker_ids={self.worker_a}
            )

    def test_a_declared_role_never_overrides_a_durable_route(self) -> None:
        """Requirement: an existing durable route always wins over startup
        role defaults and flags. A contradicting declaration is a diagnostic,
        not a role change."""

        route = self._pair()

        declaration = self.ledger.declare_pair_cell_role(self.worker_b, "leader")

        assigned = declaration["route"]
        assert isinstance(assigned, dict)
        self.assertEqual(route["route_id"], assigned["route_id"])
        self.assertEqual("follower", assigned["role"])
        self.assertEqual(self.worker_a, assigned["leader_worker_id"])

    # ---- Phase one: reservation ---- #

    def test_the_reservation_reserves_both_workers_under_a_unique_proposal(self) -> None:
        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )

        self.assertEqual(
            {
                "proposal_id", "leader_worker_id", "follower_worker_id",
                "reserved_at", "expires_at", "timeout_seconds",
            },
            set(reservation),
        )
        self.assertEqual(30, reservation["timeout_seconds"])
        self.assertEqual(
            timedelta(seconds=30), reservation["expires_at"] - reservation["reserved_at"]
        )
        self.assertNotIn("trader_id", reservation)
        held = self.ledger.pair_route_reservation(str(reservation["proposal_id"]))
        assert held is not None
        self.assertEqual(self.worker_a, held["leader_worker_id"])
        self.assertEqual(self.worker_b, held["follower_worker_id"])

    def test_every_reservation_receives_a_distinct_proposal_id(self) -> None:
        third = self._active_worker(333333, "Broker-C")
        fourth = self._active_worker(444444, "Broker-D")

        first = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )
        second = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=third, follower_worker_id=fourth, connected_worker_ids={third, fourth}
        )

        self.assertNotEqual(first["proposal_id"], second["proposal_id"])

    def test_the_reservation_claims_pair_execution_exclusivity_for_both_workers(self) -> None:
        """Requirement: ``pair_execution_cell`` exclusivity is claimed for
        both Workers inside the reservation transaction, with the fixed owner
        kind and no ``trader_id``."""

        self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )

        owner = self.ledger.pair_execution_owner(self.worker_a, self.worker_b, "pair_execution_cell")
        assert owner is not None
        self.assertEqual("pair_execution_cell", owner["owner_kind"])
        self.assertIsNone(owner["trader_id"])

    def test_two_leaders_selecting_the_same_follower_produce_one_deterministic_conflict(self) -> None:
        """Requirement: exactly one winner and one deterministic conflict,
        detected at phase one. The loser never waits out the winner's
        30-second reservation."""

        rival_leader = self._active_worker(333333, "Broker-C")
        connected = {self.worker_a, self.worker_b, rival_leader}

        winner = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids=connected,
        )
        with self.assertRaisesRegex(LedgerError, "already reserved"):
            self.ledger.reserve_pair_route_proposal(
                leader_worker_id=rival_leader,
                follower_worker_id=self.worker_b,
                connected_worker_ids=connected,
            )

        self.assertIsNotNone(self.ledger.pair_route_reservation(str(winner["proposal_id"])))
        self.assertEqual([], self.ledger.pair_route_reservations_for_worker(rival_leader))

    def test_the_losing_leader_can_immediately_pair_with_a_different_follower(self) -> None:
        """Requirement: the loser re-lists and selects again rather than
        waiting out the winner's reservation."""

        rival_leader = self._active_worker(333333, "Broker-C")
        spare_follower = self._active_worker(444444, "Broker-D")
        connected = {self.worker_a, self.worker_b, rival_leader, spare_follower}
        self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids=connected,
        )
        with self.assertRaises(LedgerError):
            self.ledger.reserve_pair_route_proposal(
                leader_worker_id=rival_leader,
                follower_worker_id=self.worker_b,
                connected_worker_ids=connected,
            )

        relisted = self.ledger.available_pair_followers(
            requesting_worker_id=rival_leader, connected_worker_ids=connected
        )
        route = self._pair(rival_leader, spare_follower)

        self.assertEqual([spare_follower], [follower["worker_id"] for follower in relisted])
        self.assertEqual(rival_leader, route["leader_worker_id"])

    def test_a_worker_already_on_a_route_cannot_be_reserved_again(self) -> None:
        third = self._active_worker(333333, "Broker-C")
        self._pair()

        for leader, follower in ((third, self.worker_b), (self.worker_a, third)):
            with self.subTest(leader=leader, follower=follower):
                with self.assertRaisesRegex(LedgerError, "already appears in a Pair Execution Cell route"):
                    self.ledger.reserve_pair_route_proposal(
                        leader_worker_id=leader,
                        follower_worker_id=follower,
                        connected_worker_ids={leader, follower},
                    )

    def test_a_reservation_requires_both_workers_to_be_connected(self) -> None:
        """Requirement: a follower that disconnects between listing and
        proposal causes a clean proposal failure with no route, no
        reservation, and no partially accepted state."""

        with self.assertRaisesRegex(LedgerError, "connected"):
            self.ledger.reserve_pair_route_proposal(
                leader_worker_id=self.worker_a,
                follower_worker_id=self.worker_b,
                connected_worker_ids={self.worker_a},
            )

        self.assertEqual([], self.ledger.pair_route_reservations_for_worker(self.worker_a))
        self.assertIsNone(self.ledger.pair_route_for_worker(self.worker_a))
        self.assertIsNone(
            self.ledger.pair_execution_owner(self.worker_a, self.worker_b, "pair_execution_cell")
        )

    def test_a_reservation_requires_two_distinct_authenticated_workers(self) -> None:
        with self.assertRaises(LedgerError):
            self.ledger.reserve_pair_route_proposal(
                leader_worker_id=self.worker_a,
                follower_worker_id=self.worker_a,
                connected_worker_ids={self.worker_a},
            )
        with self.assertRaises(LedgerError):
            self.ledger.reserve_pair_route_proposal(
                leader_worker_id=self.worker_a,
                follower_worker_id="no-such-worker",
                connected_worker_ids={self.worker_a, "no-such-worker"},
            )

    def test_a_live_legacy_strategy_runtime_blocks_the_pairing_for_either_worker(self) -> None:
        """Requirement: a pairing is rejected at phase one if either Worker's
        pair is still held live by the legacy Strategy Runtime."""

        third = self._active_worker(333333, "Broker-C")
        fourth = self._active_worker(444444, "Broker-D")
        self.ledger.claim_pair_execution_mode(self.worker_a, third, "strategy_runtime", self.trader_id)
        self.ledger.claim_pair_execution_mode(self.worker_b, fourth, "strategy_runtime", self.trader_id)

        for leader, follower in ((self.worker_a, self.worker_b), (self.worker_b, self.worker_a)):
            with self.subTest(leader=leader):
                with self.assertRaisesRegex(LedgerError, "legacy Strategy Runtime"):
                    self.ledger.reserve_pair_route_proposal(
                        leader_worker_id=leader,
                        follower_worker_id=follower,
                        connected_worker_ids={leader, follower},
                    )

    def test_releasing_the_legacy_claim_lets_the_pair_form(self) -> None:
        third = self._active_worker(333333, "Broker-C")
        self.ledger.claim_pair_execution_mode(self.worker_a, third, "strategy_runtime", self.trader_id)
        with self.assertRaises(LedgerError):
            self._pair()

        self.ledger.release_pair_execution_mode(self.worker_a, third, "strategy_runtime")
        route = self._pair()

        self.assertEqual("ACTIVE", route["state"])

    def test_the_legacy_strategy_runtime_keeps_its_own_trader_identity(self) -> None:
        """Requirement: removing ``trader_id`` is scoped to the Pair
        Execution Cell. The legacy Strategy Runtime claim still carries and
        still requires its Trader."""

        self.ledger.claim_pair_execution_mode(
            self.worker_a, self.worker_b, "strategy_runtime", self.trader_id
        )

        owner = self.ledger.pair_execution_owner(self.worker_a, self.worker_b, "strategy_runtime")
        assert owner is not None
        self.assertEqual(self.trader_id, owner["trader_id"])
        self.assertEqual("strategy_runtime", owner["owner_kind"])
        with self.assertRaisesRegex(LedgerError, "requires its Trader identity"):
            self.ledger.claim_pair_execution_mode(self.worker_a, self.worker_b, "strategy_runtime")

    def test_a_shadow_owner_may_run_alongside_a_live_owner(self) -> None:
        self.ledger.claim_pair_execution_mode(
            self.worker_a, self.worker_b, "strategy_runtime", self.trader_id
        )

        self.ledger.claim_pair_execution_mode(self.worker_a, self.worker_b, "shadow", self.trader_id)

    def test_no_caller_outside_the_pairing_workflow_may_claim_or_release_pair_cell_mode(self) -> None:
        """Requirement: Pair Cell exclusivity is taken in the reservation and
        confirmed in the final commit. There is no operator or Trader surface
        that can create one or drop one out from under a live route."""

        with self.assertRaisesRegex(LedgerError, "two-phase pairing workflow"):
            self.ledger.claim_pair_execution_mode(
                self.worker_a, self.worker_b, "pair_execution_cell", None
            )
        self._pair()
        with self.assertRaisesRegex(LedgerError, "released only by its reservation or route"):
            self.ledger.release_pair_execution_mode(
                self.worker_a, self.worker_b, "pair_execution_cell"
            )
        self.assertIsNotNone(
            self.ledger.pair_execution_owner(self.worker_a, self.worker_b, "pair_execution_cell")
        )

    # ---- Reservation release: refusal, timeout, disconnect, failed commit ---- #

    def test_a_refusal_releases_the_reservation_entirely(self) -> None:
        """Requirement: a refusal never becomes an implied acceptance and
        leaves no route, no role assignment, and no retained execution-mode
        claim."""

        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )

        released = self.ledger.release_pair_route_reservation(
            proposal_id=str(reservation["proposal_id"]), reason="follower_refused"
        )

        self.assertTrue(released)
        self._assert_nothing_retained()
        self.assertFalse(
            self.ledger.release_pair_route_reservation(
                proposal_id=str(reservation["proposal_id"]), reason="follower_refused"
            )
        )

    def test_the_thirty_second_timeout_releases_the_reservation(self) -> None:
        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )

        expired = self.ledger.expire_pair_route_reservations(
            reservation["expires_at"] + timedelta(seconds=1)
        )

        self.assertEqual([reservation["proposal_id"]], expired)
        self._assert_nothing_retained()

    def test_the_timeout_never_expires_an_established_route_an_attempt_or_an_effect(self) -> None:
        """Requirement: the pairing-reservation timeout is control metadata.
        It never fences, expires, or invalidates an established route."""

        route = self._pair()

        self.ledger.expire_pair_route_reservations(datetime.now(UTC) + timedelta(days=365))

        live = self.ledger.pair_route(str(route["route_id"]))
        assert live is not None
        self.assertEqual("ACTIVE", live["state"])
        self.assertNotIn("expires_at", live)
        self.assertNotIn("ttl_seconds", live)
        self.assertEqual({"delivered": True}, self.ledger.authorize_pair_relay(
            self._envelope(str(route["route_id"]))
        ))

    def test_a_reservation_not_yet_expired_survives_the_sweep(self) -> None:
        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )

        self.assertEqual(
            [], self.ledger.expire_pair_route_reservations(reservation["expires_at"] - timedelta(seconds=1))
        )
        self.assertIsNotNone(self.ledger.pair_route_reservation(str(reservation["proposal_id"])))

    def test_either_workers_disconnect_releases_the_reservation(self) -> None:
        for disconnecting in ("leader", "follower"):
            with self.subTest(disconnecting=disconnecting):
                reservation = self.ledger.reserve_pair_route_proposal(
                    leader_worker_id=self.worker_a,
                    follower_worker_id=self.worker_b,
                    connected_worker_ids={self.worker_a, self.worker_b},
                )
                worker_id = self.worker_a if disconnecting == "leader" else self.worker_b

                released = self.ledger.release_pair_route_reservations_for_worker(
                    worker_id, "worker_disconnected"
                )

                self.assertEqual([reservation["proposal_id"]], released)
                self._assert_nothing_retained()

    def test_a_failed_final_commit_releases_the_reservation(self) -> None:
        """Requirement: a failed compare-and-swap releases the reservation
        with no route, no role assignment, and no retained claim."""

        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )

        with self.assertRaisesRegex(LedgerError, "connected"):
            self.ledger.create_pair_route(
                proposal_id=str(reservation["proposal_id"]), connected_worker_ids={self.worker_a}
            )

        self._assert_nothing_retained()

    def test_a_commit_after_the_timeout_creates_no_route(self) -> None:
        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )

        with self.assertRaisesRegex(LedgerError, "no longer valid"):
            self.ledger.create_pair_route(
                proposal_id=str(reservation["proposal_id"]),
                connected_worker_ids={self.worker_a, self.worker_b},
                now=reservation["expires_at"] + timedelta(seconds=1),
            )

        self._assert_nothing_retained()

    def test_a_commit_for_an_unknown_proposal_creates_no_route(self) -> None:
        with self.assertRaisesRegex(LedgerError, "no longer valid"):
            self.ledger.create_pair_route(
                proposal_id="never-reserved", connected_worker_ids={self.worker_a, self.worker_b}
            )

        self._assert_nothing_retained()

    def test_a_commit_is_rejected_when_a_worker_became_ineligible(self) -> None:
        """Requirement: the final commit succeeds only while both Workers
        remain eligible under the same uniqueness constraints."""

        third = self._active_worker(333333, "Broker-C")
        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )
        self.ledger.claim_pair_execution_mode(self.worker_b, third, "strategy_runtime", self.trader_id)

        with self.assertRaisesRegex(LedgerError, "legacy Strategy Runtime"):
            self.ledger.create_pair_route(
                proposal_id=str(reservation["proposal_id"]),
                connected_worker_ids={self.worker_a, self.worker_b},
            )

        self.assertIsNone(self.ledger.pair_route_for_worker(self.worker_a))
        self.assertIsNone(
            self.ledger.pair_execution_owner(self.worker_a, self.worker_b, "pair_execution_cell")
        )

    def test_a_commit_is_rejected_when_a_revoked_worker_lost_authentication(self) -> None:
        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )
        self.ledger.revoke_worker(self.worker_b, "ABCDEF")

        with self.assertRaises(LedgerError):
            self.ledger.create_pair_route(
                proposal_id=str(reservation["proposal_id"]),
                connected_worker_ids={self.worker_a, self.worker_b},
            )

        self.assertIsNone(self.ledger.pair_route_for_worker(self.worker_a))
        self.assertEqual([], self.ledger.pair_route_reservations_for_worker(self.worker_a))

    def _assert_nothing_retained(self) -> None:
        self.assertEqual([], self.ledger.pair_route_reservations_for_worker(self.worker_a))
        self.assertEqual([], self.ledger.pair_route_reservations_for_worker(self.worker_b))
        self.assertIsNone(self.ledger.pair_route_for_worker(self.worker_a))
        self.assertIsNone(self.ledger.pair_route_for_worker(self.worker_b))
        self.assertIsNone(
            self.ledger.pair_execution_owner(self.worker_a, self.worker_b, "pair_execution_cell")
        )

    # ---- Phase two: durable route identity and authority ---- #

    def test_the_final_commit_creates_a_durable_route_with_ordered_roles(self) -> None:
        route = self._pair()

        self.assertEqual(
            {"route_id", "leader_worker_id", "follower_worker_id", "state", "created_at", "unpairing_at"},
            set(route),
        )
        self.assertEqual(self.worker_a, route["leader_worker_id"])
        self.assertEqual(self.worker_b, route["follower_worker_id"])
        self.assertEqual("ACTIVE", route["state"])
        self.assertIsNone(route["unpairing_at"])
        self.assertNotIn("trader_id", route)

    def test_a_committed_route_leaves_no_reservation_behind(self) -> None:
        route = self._pair()

        self.assertEqual([], self.ledger.pair_route_reservations_for_worker(self.worker_a))
        self.assertEqual([], self.ledger.pair_route_reservations_for_worker(self.worker_b))
        self.assertIsNotNone(self.ledger.pair_route(str(route["route_id"])))

    def test_the_route_record_is_authoritative_for_role_on_reconnect(self) -> None:
        """Requirement: a reconnecting Worker resumes the role the controller
        assigned for that ``route_id`` and never re-enters selection."""

        route = self._pair()

        leader_view = self.ledger.pair_route_for_worker(self.worker_a)
        follower_view = self.ledger.pair_route_for_worker(self.worker_b)

        assert leader_view is not None and follower_view is not None
        self.assertEqual("leader", leader_view["role"])
        self.assertEqual("follower", follower_view["role"])
        self.assertEqual(route["route_id"], leader_view["route_id"])
        self.assertEqual(route["route_id"], follower_view["route_id"])

    def test_the_route_survives_reopening_the_ledger(self) -> None:
        """Requirement: the route is durable across reconnect and Worker
        restart until an explicit safe unpair."""

        route = self._pair()
        path = Path(self._directory.name) / "ledger.duckdb"
        self.ledger.close()

        self.ledger = ControlLedger(path)

        reopened = self.ledger.pair_route_for_worker(self.worker_b)
        assert reopened is not None
        self.assertEqual(route["route_id"], reopened["route_id"])
        self.assertEqual("follower", reopened["role"])

    def test_a_route_is_not_matched_in_the_opposite_role_order(self) -> None:
        self._pair()

        self.assertIsNotNone(self.ledger.pair_route_between(self.worker_a, self.worker_b))
        self.assertIsNone(self.ledger.pair_route_between(self.worker_b, self.worker_a))

    def test_a_route_id_is_never_reused_even_by_the_same_two_workers(self) -> None:
        """Requirement: every new pairing produces a new ``route_id``; it is
        never reused, not even for the same two Workers pairing again after a
        safe unpair."""

        issued = []
        for _ in range(3):
            route = self._pair()
            issued.append(str(route["route_id"]))
            self._safe_unpair(route)

        self.assertEqual(3, len(set(issued)))
        registered = {
            str(row[0])
            for row in self.ledger._connection.execute(
                "SELECT route_id FROM pair_route_identifiers"
            ).fetchall()
        }
        self.assertTrue(set(issued) <= registered)

    def test_a_route_id_is_not_a_lease_epoch(self) -> None:
        """Requirement: ``route_id`` has no TTL, no renewal, no fencing, and
        is never compared against a Worker's own ``recovery_epoch``."""

        route = self._pair()
        record = self.ledger.pair_route(str(route["route_id"]))

        assert record is not None
        for lease_field in ("expires_at", "ttl_seconds", "lease_epoch", "renewed_at", "recovery_epoch"):
            self.assertNotIn(lease_field, record)
        for removed in (
            "issue_pair_lease", "activate_pair_contract", "current_pair_lease_epoch",
            "pair_cell_activation_for_worker", "record_pair_relay",
            "authorize_pair_route", "revoke_pair_route", "authorized_pair_route",
        ):
            self.assertFalse(hasattr(self.ledger, removed), removed)

    def _safe_unpair(self, route: dict[str, object]) -> None:
        route_id = str(route["route_id"])
        self.ledger.enter_pair_route_unpairing(route_id=route_id, worker_id=self.worker_a)
        self.ledger.record_pair_unpair_assertion(
            route_id=route_id, worker_id=self.worker_a, state_version=1
        )
        self.ledger.record_pair_unpair_assertion(
            route_id=route_id, worker_id=self.worker_b, state_version=1
        )

    # ---- Safe unpair through the explicit UNPAIRING state ---- #

    def test_either_worker_may_enter_unpairing_for_the_current_route(self) -> None:
        """Requirement: there is no proposer privilege -- the follower may
        initiate exactly as the leader may."""

        for initiator in (self.worker_a, self.worker_b):
            with self.subTest(initiator=initiator):
                route = self._pair()

                unpairing = self.ledger.enter_pair_route_unpairing(
                    route_id=str(route["route_id"]), worker_id=initiator
                )

                self.assertEqual("UNPAIRING", unpairing["state"])
                self.assertIsNotNone(unpairing["unpairing_at"])
                self._safe_unpair(route)

    def test_entering_unpairing_is_visible_to_both_workers_immediately(self) -> None:
        route = self._pair()

        self.ledger.enter_pair_route_unpairing(route_id=str(route["route_id"]), worker_id=self.worker_b)

        for worker_id in (self.worker_a, self.worker_b):
            view = self.ledger.pair_route_for_worker(worker_id)
            assert view is not None
            self.assertEqual("UNPAIRING", view["state"])

    def test_a_worker_outside_the_route_cannot_enter_unpairing(self) -> None:
        third = self._active_worker(333333, "Broker-C")
        route = self._pair()

        with self.assertRaisesRegex(LedgerError, "not on that Pair Execution Cell route"):
            self.ledger.enter_pair_route_unpairing(route_id=str(route["route_id"]), worker_id=third)

    def test_an_unknown_route_id_cannot_be_unpaired(self) -> None:
        with self.assertRaisesRegex(LedgerError, "route identifier"):
            self.ledger.enter_pair_route_unpairing(route_id="no-such-route", worker_id=self.worker_a)

    def test_relay_continues_while_unpairing_so_containment_is_never_blocked(self) -> None:
        """Requirement: ``UNPAIRING`` never prevents either Worker from
        protecting, reducing, or containing exposure it already owns."""

        route = self._pair()
        self.ledger.enter_pair_route_unpairing(route_id=str(route["route_id"]), worker_id=self.worker_a)

        self.assertEqual(
            {"delivered": True}, self.ledger.authorize_pair_relay(self._envelope(str(route["route_id"])))
        )

    def test_two_fresh_bound_assertions_remove_the_route_and_release_both_claims(self) -> None:
        route = self._pair()
        route_id = str(route["route_id"])
        self.ledger.enter_pair_route_unpairing(route_id=route_id, worker_id=self.worker_a)

        first = self.ledger.record_pair_unpair_assertion(
            route_id=route_id, worker_id=self.worker_a, state_version=4
        )
        second = self.ledger.record_pair_unpair_assertion(
            route_id=route_id, worker_id=self.worker_b, state_version=9
        )

        self.assertFalse(first["route_removed"])
        self.assertTrue(second["route_removed"])
        self.assertIsNone(self.ledger.pair_route(route_id))
        self.assertIsNone(self.ledger.pair_route_for_worker(self.worker_a))
        self.assertIsNone(
            self.ledger.pair_execution_owner(self.worker_a, self.worker_b, "pair_execution_cell")
        )

    def test_one_sided_assertions_never_unpair(self) -> None:
        route = self._pair()
        route_id = str(route["route_id"])
        self.ledger.enter_pair_route_unpairing(route_id=route_id, worker_id=self.worker_a)

        self.ledger.record_pair_unpair_assertion(
            route_id=route_id, worker_id=self.worker_a, state_version=1
        )
        self.ledger.record_pair_unpair_assertion(
            route_id=route_id, worker_id=self.worker_a, state_version=2
        )

        live = self.ledger.pair_route(route_id)
        assert live is not None
        self.assertEqual("UNPAIRING", live["state"])
        self.assertEqual(
            [self.worker_a], [held["worker_id"] for held in self.ledger.pair_unpair_assertions(route_id)]
        )

    def test_a_new_local_effect_invalidates_an_outstanding_assertion(self) -> None:
        """Requirement: any new or changed local effect advances the state
        version and invalidates that Worker's outstanding assertion, which
        must be re-produced from fresh evidence."""

        route = self._pair()
        route_id = str(route["route_id"])
        self.ledger.enter_pair_route_unpairing(route_id=route_id, worker_id=self.worker_a)
        self.ledger.record_pair_unpair_assertion(
            route_id=route_id, worker_id=self.worker_a, state_version=1
        )

        advanced = self.ledger.record_pair_worker_state_version(
            route_id=route_id, worker_id=self.worker_a, state_version=2
        )
        self.ledger.record_pair_unpair_assertion(
            route_id=route_id, worker_id=self.worker_b, state_version=1
        )

        self.assertTrue(advanced["assertion_invalidated"])
        self.assertEqual(
            [self.worker_b], [held["worker_id"] for held in self.ledger.pair_unpair_assertions(route_id)]
        )
        live = self.ledger.pair_route(route_id)
        assert live is not None
        self.assertEqual("UNPAIRING", live["state"])

        removed = self.ledger.record_pair_unpair_assertion(
            route_id=route_id, worker_id=self.worker_a, state_version=2
        )
        self.assertTrue(removed["route_removed"])

    def test_an_assertion_bound_to_a_superseded_state_version_is_refused(self) -> None:
        route = self._pair()
        route_id = str(route["route_id"])
        self.ledger.enter_pair_route_unpairing(route_id=route_id, worker_id=self.worker_a)
        self.ledger.record_pair_worker_state_version(
            route_id=route_id, worker_id=self.worker_a, state_version=5
        )

        with self.assertRaisesRegex(LedgerError, "superseded local state version"):
            self.ledger.record_pair_unpair_assertion(
                route_id=route_id, worker_id=self.worker_a, state_version=4
            )

        self.assertEqual([], self.ledger.pair_unpair_assertions(route_id))

    def test_an_assertion_bound_to_a_different_route_id_does_not_unpair(self) -> None:
        route = self._pair()
        route_id = str(route["route_id"])
        self.ledger.enter_pair_route_unpairing(route_id=route_id, worker_id=self.worker_a)
        self.ledger.record_pair_unpair_assertion(
            route_id=route_id, worker_id=self.worker_a, state_version=1
        )

        with self.assertRaisesRegex(LedgerError, "route identifier"):
            self.ledger.record_pair_unpair_assertion(
                route_id="some-other-route", worker_id=self.worker_b, state_version=1
            )

        self.assertIsNotNone(self.ledger.pair_route(route_id))

    def test_an_assertion_is_refused_while_the_route_is_active(self) -> None:
        """Requirement: safety is proved inside the explicit ``UNPAIRING``
        state, after entry readiness is removed -- never against a moving
        target."""

        route = self._pair()

        with self.assertRaisesRegex(LedgerError, "only while its route is UNPAIRING"):
            self.ledger.record_pair_unpair_assertion(
                route_id=str(route["route_id"]), worker_id=self.worker_a, state_version=1
            )

    def test_a_state_version_never_moves_backwards(self) -> None:
        route = self._pair()
        route_id = str(route["route_id"])
        self.ledger.record_pair_worker_state_version(
            route_id=route_id, worker_id=self.worker_a, state_version=7
        )

        with self.assertRaisesRegex(LedgerError, "never moves backwards"):
            self.ledger.record_pair_worker_state_version(
                route_id=route_id, worker_id=self.worker_a, state_version=6
            )
        self.assertEqual(7, self.ledger.pair_worker_state_version(route_id, self.worker_a))

    def test_a_state_version_must_be_a_non_negative_integer(self) -> None:
        route = self._pair()
        route_id = str(route["route_id"])

        for invalid in (-1, True, "3", 1.5):
            with self.subTest(state_version=invalid), self.assertRaises(LedgerError):
                self.ledger.record_pair_worker_state_version(
                    route_id=route_id, worker_id=self.worker_a, state_version=invalid  # type: ignore[arg-type]
                )

    def test_cancelling_unpairing_returns_the_route_to_normal_operation(self) -> None:
        route = self._pair()
        route_id = str(route["route_id"])
        self.ledger.enter_pair_route_unpairing(route_id=route_id, worker_id=self.worker_a)
        self.ledger.record_pair_unpair_assertion(
            route_id=route_id, worker_id=self.worker_a, state_version=1
        )

        cancelled = self.ledger.cancel_pair_route_unpairing(route_id=route_id, worker_id=self.worker_b)

        self.assertEqual("ACTIVE", cancelled["state"])
        self.assertIsNone(cancelled["unpairing_at"])
        self.assertEqual([], self.ledger.pair_unpair_assertions(route_id))
        self.assertIsNotNone(
            self.ledger.pair_execution_owner(self.worker_a, self.worker_b, "pair_execution_cell")
        )

    def test_cancelling_an_active_route_is_refused(self) -> None:
        route = self._pair()

        with self.assertRaisesRegex(LedgerError, "not unpairing"):
            self.ledger.cancel_pair_route_unpairing(
                route_id=str(route["route_id"]), worker_id=self.worker_a
            )

    def test_re_entering_unpairing_discards_a_stale_assertion(self) -> None:
        route = self._pair()
        route_id = str(route["route_id"])
        self.ledger.enter_pair_route_unpairing(route_id=route_id, worker_id=self.worker_a)
        self.ledger.record_pair_unpair_assertion(
            route_id=route_id, worker_id=self.worker_a, state_version=1
        )
        self.ledger.cancel_pair_route_unpairing(route_id=route_id, worker_id=self.worker_a)

        self.ledger.enter_pair_route_unpairing(route_id=route_id, worker_id=self.worker_a)

        self.assertEqual([], self.ledger.pair_unpair_assertions(route_id))

    def test_after_a_safe_unpair_the_same_workers_may_pair_again(self) -> None:
        first = self._pair()
        self._safe_unpair(first)

        second = self._pair()

        self.assertNotEqual(first["route_id"], second["route_id"])
        self.assertEqual("ACTIVE", second["state"])

    def test_identity_revocation_is_never_a_safe_unpair(self) -> None:
        """Requirement: revoking a device credential removes authentication
        and relay, but never removes the route as a safe unpair and never
        implies broker ``EMPTY``."""

        route = self._pair()

        self.ledger.revoke_worker(self.worker_b, "ABCDEF")

        surviving = self.ledger.pair_route(str(route["route_id"]))
        assert surviving is not None
        self.assertEqual("ACTIVE", surviving["state"])
        self.assertIsNotNone(
            self.ledger.pair_execution_owner(self.worker_a, self.worker_b, "pair_execution_cell")
        )
        with self.assertRaises(LedgerError):
            self.ledger.authorize_pair_relay(self._envelope(str(route["route_id"])))
        self.assertEqual(
            [],
            [
                event for event in self.ledger.events()
                if event["event_type"] == "pair_route_removed"
            ],
        )

    # ---- Opaque relay ---- #

    def test_pair_relay_authorizes_an_opaque_envelope_without_interpreting_or_storing_it(self) -> None:
        """Requirement: the relay is opaque and non-durable. It authorizes
        identity, route, version, and size, and writes nothing at all -- no
        attempt projection, no envelope copy, no lifecycle event."""

        route = self._pair()
        envelope = self._envelope(
            str(route["route_id"]),
            payload={
                "kind": "entry_attempt",
                "attempt": {"attempt_id": "attempt-1", "lots": "0.37", "policy_hash": "abc"},
                "universe": {"universe_generation": "generation-1", "symbols": ["EURUSD"]},
                "protection": {"sl": "1.0942", "tp": "1.1058"},
            },
        )
        before = len(self.ledger.events())

        result = self.ledger.authorize_pair_relay(envelope)

        self.assertEqual({"delivered": True}, result)
        self.assertEqual(before, len(self.ledger.events()))

    def test_pair_relay_never_deduplicates_or_replays_a_repeated_message(self) -> None:
        """Requirement: the controller keeps no durable relay row, so a resend
        -- even one reusing a request_id with changed content -- is simply
        forwarded again. Duplicate suppression is the receiving Worker's own
        durable responsibility, not controller lifecycle evidence."""

        route = self._pair()
        route_id = str(route["route_id"])
        first = self.ledger.authorize_pair_relay(self._envelope(route_id))
        again = self.ledger.authorize_pair_relay(self._envelope(route_id))
        changed = self.ledger.authorize_pair_relay(
            self._envelope(route_id, payload={"kind": "entry_attempt", "opaque_to_controller": False})
        )

        for result in (first, again, changed):
            self.assertEqual({"delivered": True}, result)
        self.assertNotIn("replay", first)
        self.assertNotIn("audited", first)

    def test_pair_relay_requires_a_live_route(self) -> None:
        with self.assertRaises(LedgerError):
            self.ledger.authorize_pair_relay(self._envelope("never-created"))

    def test_pair_relay_rejects_a_stale_route_id_after_a_safe_unpair(self) -> None:
        """Requirement: the controller forwards only when the ``route_id``
        matches a live route record."""

        route = self._pair()
        route_id = str(route["route_id"])
        self._safe_unpair(route)

        with self.assertRaisesRegex(LedgerError, "does not match a live Worker route"):
            self.ledger.authorize_pair_relay(self._envelope(route_id))

    def test_pair_relay_rejects_a_route_id_from_a_different_pairing(self) -> None:
        third = self._active_worker(333333, "Broker-C")
        fourth = self._active_worker(444444, "Broker-D")
        route = self._pair()
        other = self._pair(third, fourth)

        with self.assertRaises(LedgerError):
            self.ledger.authorize_pair_relay(self._envelope(str(other["route_id"])))
        self.assertEqual(
            {"delivered": True}, self.ledger.authorize_pair_relay(self._envelope(str(route["route_id"])))
        )

    def test_pair_relay_rejects_a_worker_outside_the_route(self) -> None:
        route = self._pair()
        outsider = self._active_worker(333333, "Broker-C")

        with self.assertRaises(LedgerError):
            self.ledger.authorize_pair_relay(
                self._envelope(str(route["route_id"]), to_worker_id=outsider)
            )

    def test_pair_relay_rejects_a_worker_claiming_a_role_the_route_never_assigned(self) -> None:
        """Requirement: the controller's route record is authoritative for
        assigned role. A Worker cannot promote itself by asserting the roles
        it wants."""

        route = self._pair()
        swapped = self._envelope(
            str(route["route_id"]),
            route={
                "route_id": route["route_id"],
                "leader_worker_id": self.worker_b,
                "follower_worker_id": self.worker_a,
            },
            from_worker_id=self.worker_b,
            to_worker_id=self.worker_a,
        )

        with self.assertRaises(LedgerError):
            self.ledger.authorize_pair_relay(swapped)

    def test_pair_relay_rejects_a_trader_id_anywhere_on_the_envelope(self) -> None:
        """Requirement: there is no ``trader_id`` in Pair Cell envelopes."""

        route = self._pair()
        route_id = str(route["route_id"])

        with self.assertRaises(LedgerError):
            self.ledger.authorize_pair_relay(self._envelope(route_id, trader_id=self.trader_id))
        with self.assertRaises(LedgerError):
            self.ledger.authorize_pair_relay(
                self._envelope(
                    route_id,
                    route={
                        "route_id": route_id,
                        "leader_worker_id": self.worker_a,
                        "follower_worker_id": self.worker_b,
                        "trader_id": self.trader_id,
                    },
                )
            )

    def test_pair_relay_rejects_a_residual_lease_or_attempt_claim(self) -> None:
        route = self._pair()
        route_id = str(route["route_id"])

        for residual in ("lease", "lease_epoch", "contract_hash", "attempt_id", "expires_at"):
            with self.subTest(field=residual), self.assertRaises(LedgerError):
                self.ledger.authorize_pair_relay(self._envelope(route_id, **{residual: "value"}))

    def test_pair_relay_rejects_a_route_to_self(self) -> None:
        route = self._pair()

        with self.assertRaises(LedgerError):
            self.ledger.authorize_pair_relay(
                self._envelope(str(route["route_id"]), to_worker_id=self.worker_a)
            )

    def test_pair_relay_rejects_an_unsupported_protocol_version(self) -> None:
        route = self._pair()

        with self.assertRaises(LedgerError):
            self.ledger.authorize_pair_relay(self._envelope(str(route["route_id"]), protocol_version=2))

    def test_pair_relay_rejects_an_envelope_over_the_size_limit(self) -> None:
        route = self._pair()

        with self.assertRaises(LedgerError):
            self.ledger.authorize_pair_relay(
                self._envelope(
                    str(route["route_id"]),
                    payload={"kind": "entry_attempt", "blob": "x" * (MAX_PAIR_RELAY_ENVELOPE_BYTES + 1)},
                )
            )

    def test_pair_relay_rejects_a_revoked_worker(self) -> None:
        route = self._pair()
        self.ledger.revoke_worker(self.worker_b, "ABCDEF")

        with self.assertRaises(LedgerError):
            self.ledger.authorize_pair_relay(self._envelope(str(route["route_id"])))

    # ---- Opaque Pairing Acceptance payload forwarding ---- #

    def _acceptance_payload(self, proposal_id: str, worker_id: str | None = None) -> dict[str, object]:
        """A payload shaped like the core's canonical Pairing Acceptance.

        The controller must treat it as an opaque mapping, so these tests
        assert only that it survives unchanged -- never that the controller
        understood a single field of it.
        """

        return {
            "proposal_id": proposal_id,
            "worker_id": worker_id or self.worker_b,
            "startup_balance_usd": "10000.00",
            "account_currency": "USD",
            "maximum_margin_fraction": "0.10",
            "daily_loss_fraction": "0.03",
            "trade_loss_fraction": "0.02",
            "maximum_loss_per_trade_usd": "40",
        }

    def test_an_acceptance_payload_is_authorized_and_returned_unchanged(self) -> None:
        """Requirement: the follower authors its own budget and risk tunables
        and the controller forwards them to the reserved leader unchanged."""

        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )
        proposal_id = str(reservation["proposal_id"])
        payload = self._acceptance_payload(proposal_id)

        authorized = self.ledger.authorize_pair_acceptance_payload(
            proposal_id=proposal_id,
            from_worker_id=self.worker_b,
            acceptance_payload=payload,
            payload_hash="acceptance-hash-1",
        )

        self.assertTrue(authorized["delivered"])
        self.assertEqual(self.worker_a, authorized["leader_worker_id"])
        self.assertEqual(self.worker_b, authorized["follower_worker_id"])
        self.assertEqual(
            {
                "proposal_id": proposal_id,
                "from_worker_id": self.worker_b,
                "payload_hash": "acceptance-hash-1",
                "payload": payload,
            },
            authorized["acceptance"],
        )
        self.assertIs(payload, authorized["acceptance"]["payload"])

    def test_an_acceptance_payload_of_any_shape_survives_byte_for_byte(self) -> None:
        """Requirement: the controller parses no trading or risk field, so a
        payload shape it has never heard of passes through untouched."""

        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )
        payload = {
            "worker_id": self.worker_b,
            "startup_balance_usd": "-1",
            "account_currency": "JPY",
            "unknown_future_block": {"nested": [1, {"deep": True}]},
        }

        authorized = self.ledger.authorize_pair_acceptance_payload(
            proposal_id=str(reservation["proposal_id"]),
            from_worker_id=self.worker_b,
            acceptance_payload=payload,
            payload_hash="hash-2",
        )

        self.assertEqual(payload, authorized["acceptance"]["payload"])

    def test_authorizing_an_acceptance_payload_persists_nothing_about_it(self) -> None:
        """Requirement: ordinary delivery diagnostics only. The controller
        keeps no authoritative trading projection of the acceptance."""

        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )
        proposal_id = str(reservation["proposal_id"])
        before = len(self.ledger.events())

        self.ledger.authorize_pair_acceptance_payload(
            proposal_id=proposal_id,
            from_worker_id=self.worker_b,
            acceptance_payload=self._acceptance_payload(proposal_id),
            payload_hash="acceptance-hash-1",
        )
        after_authorization = len(self.ledger.events())
        self.ledger.create_pair_route(
            proposal_id=proposal_id, connected_worker_ids={self.worker_a, self.worker_b}
        )

        self.assertEqual(before, after_authorization)
        recorded = json.dumps([event["payload"] for event in self.ledger.events()], default=str)
        for never_stored in (
            "startup_balance_usd", "account_currency", "maximum_margin_fraction",
            "daily_loss_fraction", "trade_loss_fraction", "maximum_loss_per_trade_usd",
            "acceptance", "acceptance-hash-1", "10000.00",
        ):
            self.assertNotIn(never_stored, recorded, never_stored)
        tables = {row[0] for row in self.ledger._connection.execute("SHOW TABLES").fetchall()}
        self.assertNotIn("pair_acceptance_payloads", tables)

    def test_an_acceptance_payload_over_the_size_limit_is_refused(self) -> None:
        """Requirement: the controller enforces protocol and size limits even
        though the payload's content is invisible to it."""

        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )

        with self.assertRaisesRegex(LedgerError, "exceeds the size limit"):
            self.ledger.authorize_pair_acceptance_payload(
                proposal_id=str(reservation["proposal_id"]),
                from_worker_id=self.worker_b,
                acceptance_payload={"blob": "x" * (MAX_PAIR_RELAY_ENVELOPE_BYTES + 1)},
                payload_hash="hash-3",
            )

    def test_an_unencodable_acceptance_payload_is_refused(self) -> None:
        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )

        with self.assertRaisesRegex(LedgerError, "not encodable"):
            self.ledger.authorize_pair_acceptance_payload(
                proposal_id=str(reservation["proposal_id"]),
                from_worker_id=self.worker_b,
                acceptance_payload={"balance": object()},
                payload_hash="hash-4",
            )

    def test_an_acceptance_payload_must_be_a_mapping_with_a_hash(self) -> None:
        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )
        proposal_id = str(reservation["proposal_id"])

        for payload, payload_hash in (
            (None, "hash-5"),
            ("a string", "hash-5"),
            ([("worker_id", self.worker_b)], "hash-5"),
            ({"worker_id": self.worker_b}, None),
            ({"worker_id": self.worker_b}, ""),
            ({"worker_id": self.worker_b}, 7),
        ):
            with self.subTest(payload=payload, payload_hash=payload_hash):
                with self.assertRaises(LedgerError):
                    self.ledger.authorize_pair_acceptance_payload(
                        proposal_id=proposal_id,
                        from_worker_id=self.worker_b,
                        acceptance_payload=payload,
                        payload_hash=payload_hash,
                    )

    def test_only_the_reserved_follower_may_send_an_acceptance_payload(self) -> None:
        third = self._active_worker(333333, "Broker-C")
        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )
        proposal_id = str(reservation["proposal_id"])

        for sender in (self.worker_a, third):
            with self.subTest(sender=sender):
                with self.assertRaisesRegex(LedgerError, "names this Worker as its follower"):
                    self.ledger.authorize_pair_acceptance_payload(
                        proposal_id=proposal_id,
                        from_worker_id=sender,
                        acceptance_payload=self._acceptance_payload(proposal_id),
                        payload_hash="hash-6",
                    )

    def test_an_acceptance_payload_for_a_released_reservation_is_refused(self) -> None:
        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )
        proposal_id = str(reservation["proposal_id"])
        self.ledger.expire_pair_route_reservations(
            reservation["expires_at"] + timedelta(seconds=1)
        )

        with self.assertRaises(LedgerError):
            self.ledger.authorize_pair_acceptance_payload(
                proposal_id=proposal_id,
                from_worker_id=self.worker_b,
                acceptance_payload=self._acceptance_payload(proposal_id),
                payload_hash="hash-7",
            )

    def test_an_acceptance_payload_from_a_revoked_follower_is_refused(self) -> None:
        reservation = self.ledger.reserve_pair_route_proposal(
            leader_worker_id=self.worker_a,
            follower_worker_id=self.worker_b,
            connected_worker_ids={self.worker_a, self.worker_b},
        )
        proposal_id = str(reservation["proposal_id"])
        self.ledger.revoke_worker(self.worker_b, "ABCDEF")

        with self.assertRaises(LedgerError):
            self.ledger.authorize_pair_acceptance_payload(
                proposal_id=proposal_id,
                from_worker_id=self.worker_b,
                acceptance_payload=self._acceptance_payload(proposal_id),
                payload_hash="hash-8",
            )

    # ---- Quarantine release ---- #

    def test_release_pair_quarantine_records_an_audit_event_and_returns_the_push_payload(self) -> None:
        route = self._pair()

        release = self.ledger.release_pair_quarantine(
            route_id=str(route["route_id"]),
            symbol="EURUSD",
            actor="alice",
            reason="broker confirmed the symbol is tradable again",
        )

        self.assertEqual(route["route_id"], release["route_id"])
        self.assertEqual(self.worker_a, release["leader_worker_id"])
        self.assertEqual(self.worker_b, release["follower_worker_id"])
        self.assertEqual("EURUSD", release["symbol"])
        self.assertNotIn("trader_id", release)
        self.assertTrue(any(
            event["event_type"] == "pair_quarantine_release_requested"
            and event["payload"]["route_id"] == route["route_id"]
            and event["payload"]["symbol"] == "EURUSD"
            and event["payload"]["actor"] == "alice"
            and "trader_id" not in event["payload"]
            for event in self.ledger.events()
        ))

    def test_release_pair_quarantine_requires_a_live_route(self) -> None:
        with self.assertRaisesRegex(LedgerError, "live Worker route"):
            self.ledger.release_pair_quarantine(
                route_id="no-such-route", symbol="EURUSD", actor="alice", reason="operator review"
            )

    def test_release_pair_quarantine_requires_both_workers_to_be_authenticated(self) -> None:
        route = self._pair()
        self.ledger.revoke_worker(self.worker_b, "ABCDEF")

        with self.assertRaises(LedgerError):
            self.ledger.release_pair_quarantine(
                route_id=str(route["route_id"]),
                symbol="EURUSD",
                actor="alice",
                reason="operator review",
            )

    def test_record_pair_quarantine_release_outcome_audits_the_result_unconditionally(self) -> None:
        for leader_outcome, follower_outcome, applied in (
            ("applied", "rejected", False),
            ("applied", "applied", True),
        ):
            with self.subTest(applied=applied):
                self.ledger.record_pair_quarantine_release_outcome(
                    route_id="route-1",
                    symbol="EURUSD",
                    actor="alice",
                    leader_outcome=leader_outcome,
                    follower_outcome=follower_outcome,
                    applied=applied,
                )

                self.assertTrue(any(
                    event["event_type"] == "pair_quarantine_release_outcome"
                    and event["payload"]["leader_outcome"] == leader_outcome
                    and event["payload"]["follower_outcome"] == follower_outcome
                    and event["payload"]["applied"] is applied
                    and "trader_id" not in event["payload"]
                    for event in self.ledger.events()
                ))

    # ---- Schema and migration ---- #

    def test_legacy_lease_contract_and_relay_projections_are_dropped(self) -> None:
        """Requirement: the controller stores no authoritative attempt/lease
        projection at all -- the old tables are removed, not merely unused."""

        tables = {row[0] for row in self.ledger._connection.execute("SHOW TABLES").fetchall()}

        self.assertNotIn("pair_leases", tables)
        self.assertNotIn("pair_contracts", tables)
        self.assertNotIn("pair_relay", tables)
        self.assertIn("pair_routes", tables)
        self.assertIn("pair_route_reservations", tables)
        self.assertIn("pair_route_identifiers", tables)
        self.assertIn("pair_unpair_assertions", tables)

    def test_no_pair_route_column_carries_a_trader_identity(self) -> None:
        """Requirement: ``trader_id`` is removed from the ``pair_routes``
        schema and from the Pair Cell path of the ownership records."""

        route_columns = {
            str(row[1]) for row in self.ledger._connection.execute("PRAGMA table_info('pair_routes')").fetchall()
        }
        self._pair()

        self.assertNotIn("trader_id", route_columns)
        self.assertEqual(
            [(None,)],
            self.ledger._connection.execute(
                "SELECT trader_id FROM pair_execution_owners WHERE mode = 'pair_execution_cell'"
            ).fetchall(),
        )


class PairExecutionCellLedgerMigrationTests(unittest.TestCase):
    """An existing ledger carrying administrator-created, ``trader_id``-bound
    routes must open on the revised schema without losing the pairings it
    already had and without keeping a Trader binding."""

    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.path = Path(self._directory.name) / "ledger.duckdb"

    def tearDown(self) -> None:
        self._directory.cleanup()

    def _write_legacy_schema(self, *, rows: list[tuple[str, str, str]]) -> None:
        connection = duckdb.connect(str(self.path))
        connection.execute(
            """
            CREATE TABLE pair_execution_owners (
                pair_key VARCHAR NOT NULL,
                mode VARCHAR NOT NULL,
                trader_id VARCHAR NOT NULL,
                claimed_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (pair_key, mode)
            );
            CREATE TABLE pair_routes (
                pair_key VARCHAR PRIMARY KEY,
                leader_worker_id VARCHAR NOT NULL,
                follower_worker_id VARCHAR NOT NULL,
                trader_id VARCHAR NOT NULL,
                authorized_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE pair_leases (lease_epoch BIGINT);
            CREATE TABLE pair_contracts (contract_hash VARCHAR);
            """
        )
        moment = datetime.now(UTC)
        for pair_key, leader_worker_id, follower_worker_id in rows:
            connection.execute(
                "INSERT INTO pair_routes VALUES (?, ?, ?, ?, ?)",
                [pair_key, leader_worker_id, follower_worker_id, "trader-1", moment],
            )
            connection.execute(
                "INSERT INTO pair_execution_owners VALUES (?, 'pair_execution_cell', 'trader-1', ?)",
                [pair_key, moment],
            )
        connection.close()

    def test_an_existing_route_keeps_its_ordered_roles_and_gains_a_route_id(self) -> None:
        self._write_legacy_schema(rows=[("worker-a|worker-b", "worker-a", "worker-b")])

        ledger = ControlLedger(self.path)
        try:
            migrated = ledger.pair_route_for_worker("worker-a")
            assert migrated is not None
            self.assertEqual("leader", migrated["role"])
            self.assertEqual("worker-b", migrated["follower_worker_id"])
            self.assertEqual("ACTIVE", migrated["state"])
            self.assertNotIn("trader_id", migrated)
            self.assertTrue(migrated["route_id"])
            self.assertEqual(
                [(migrated["route_id"],)],
                ledger._connection.execute("SELECT route_id FROM pair_route_identifiers").fetchall(),
            )
        finally:
            ledger.close()

    def test_the_migrated_pair_cell_claim_loses_its_trader_binding(self) -> None:
        self._write_legacy_schema(rows=[("worker-a|worker-b", "worker-a", "worker-b")])

        ledger = ControlLedger(self.path)
        try:
            owner = ledger.pair_execution_owner("worker-a", "worker-b", "pair_execution_cell")
            assert owner is not None
            self.assertIsNone(owner["trader_id"])
            self.assertEqual("pair_execution_cell", owner["owner_kind"])
        finally:
            ledger.close()

    def test_a_pair_cell_claim_left_without_a_route_is_dropped(self) -> None:
        """A claim stranded by a revoked administrator route would otherwise
        permanently block those two Workers from ever pairing again."""

        self._write_legacy_schema(rows=[])
        connection = duckdb.connect(str(self.path))
        connection.execute(
            "INSERT INTO pair_execution_owners VALUES ('worker-c|worker-d', 'pair_execution_cell', 'trader-1', ?)",
            [datetime.now(UTC)],
        )
        connection.close()

        ledger = ControlLedger(self.path)
        try:
            self.assertIsNone(ledger.pair_execution_owner("worker-c", "worker-d", "pair_execution_cell"))
        finally:
            ledger.close()

    def test_a_legacy_strategy_runtime_claim_keeps_its_trader_identity(self) -> None:
        self._write_legacy_schema(rows=[])
        connection = duckdb.connect(str(self.path))
        connection.execute(
            "INSERT INTO pair_execution_owners VALUES ('worker-e|worker-f', 'strategy_runtime', 'trader-9', ?)",
            [datetime.now(UTC)],
        )
        connection.close()

        ledger = ControlLedger(self.path)
        try:
            owner = ledger.pair_execution_owner("worker-e", "worker-f", "strategy_runtime")
            assert owner is not None
            self.assertEqual("trader-9", owner["trader_id"])
        finally:
            ledger.close()

    def test_the_migration_is_idempotent_across_restarts(self) -> None:
        self._write_legacy_schema(
            rows=[("worker-a|worker-b", "worker-a", "worker-b"), ("worker-c|worker-d", "worker-d", "worker-c")]
        )

        first = ControlLedger(self.path)
        route_ids = {
            str(row[0]) for row in first._connection.execute("SELECT route_id FROM pair_routes").fetchall()
        }
        first.close()

        second = ControlLedger(self.path)
        try:
            self.assertEqual(2, len(route_ids))
            self.assertEqual(
                route_ids,
                {str(row[0]) for row in second._connection.execute("SELECT route_id FROM pair_routes").fetchall()},
            )
            leader = second.pair_route_for_worker("worker-d")
            assert leader is not None
            self.assertEqual("leader", leader["role"])
        finally:
            second.close()
