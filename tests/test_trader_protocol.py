from __future__ import annotations

import unittest

from pydantic import ValidationError

from abt.trader_protocol import (
    MAX_LIVE_SYMBOLS,
    PAIR_CELL_CONTROL_MESSAGE_TYPES,
    pair_cell_control_message_adapter,
    pair_cell_relay_envelope_adapter,
)


class PairCellRelayEnvelopeTests(unittest.TestCase):
    """The relay envelope is the controller's entire view of a Pair Execution
    Cell: two Worker identities, a live ``route_id``, a version, and an opaque
    payload. It carries no lease, no epoch, no attempt, no ``trader_id``, and
    no trading field."""

    def _envelope(self, **overrides: object) -> dict[str, object]:
        envelope: dict[str, object] = {
            "type": "pair_cell_relay",
            "protocol_version": 1,
            "route": {
                "route_id": "route-1",
                "leader_worker_id": "worker-a",
                "follower_worker_id": "worker-b",
            },
            "from_worker_id": "worker-a",
            "to_worker_id": "worker-b",
            "request_id": "message-1",
            "payload_hash": "abc",
            "payload": {"kind": "quote", "symbol": "EURUSD"},
        }
        envelope.update(overrides)
        return envelope

    def test_rejects_a_missing_message_id(self) -> None:
        envelope = self._envelope()
        del envelope["request_id"]
        with self.assertRaises(ValidationError):
            pair_cell_relay_envelope_adapter.validate_python(envelope)

    def test_accepts_a_well_formed_opaque_envelope(self) -> None:
        envelope = pair_cell_relay_envelope_adapter.validate_python(self._envelope())

        self.assertEqual("worker-a", envelope.from_worker_id)
        self.assertEqual("route-1", envelope.route.route_id)
        self.assertEqual({"kind": "quote", "symbol": "EURUSD"}, envelope.payload)

    def test_the_route_claim_carries_route_id_and_no_trader_identity(self) -> None:
        """Requirement: there is no ``trader_id`` on the Pair Execution Cell
        route, in its envelopes, in its ownership records, or in its
        controller APIs. Durable route identity is the ordered Worker pair
        plus the controller-generated ``route_id``."""

        envelope = pair_cell_relay_envelope_adapter.validate_python(self._envelope())

        self.assertEqual(
            {"route_id", "leader_worker_id", "follower_worker_id"},
            set(envelope.route.model_dump()),
        )
        self.assertNotIn("trader_id", envelope.model_dump_json())

    def test_rejects_a_trader_id_smuggled_onto_the_route_claim(self) -> None:
        with self.assertRaises(ValidationError):
            pair_cell_relay_envelope_adapter.validate_python(
                self._envelope(
                    route={
                        "route_id": "route-1",
                        "leader_worker_id": "worker-a",
                        "follower_worker_id": "worker-b",
                        "trader_id": "trader-1",
                    }
                )
            )

    def test_rejects_a_trader_id_smuggled_onto_the_envelope(self) -> None:
        with self.assertRaises(ValidationError):
            pair_cell_relay_envelope_adapter.validate_python(self._envelope(trader_id="trader-1"))

    def test_rejects_a_missing_or_empty_route_id(self) -> None:
        for route_id in (None, ""):
            claim = {"leader_worker_id": "worker-a", "follower_worker_id": "worker-b"}
            if route_id is not None:
                claim["route_id"] = route_id
            with self.subTest(route_id=route_id), self.assertRaises(ValidationError):
                pair_cell_relay_envelope_adapter.validate_python(self._envelope(route=claim))

    def test_accepts_any_opaque_payload_without_declaring_trading_fields(self) -> None:
        """Requirement: the controller parses no trading field. A payload the
        controller has never heard of -- policy, attempt, discovered universe,
        protection, exact broker evidence -- validates and stays an untouched
        mapping."""

        payload = {
            "kind": "entry_attempt",
            "attempt": {"attempt_id": "attempt-1", "lots": "0.37", "directions": ["LONG", "SHORT"]},
            "universe": {"universe_generation": "generation-1", "symbols": ["EURUSD", "GBPUSD"]},
            "protection": {"sl": "1.0942", "tp": "1.1058"},
            "unknown_future_field": [1, {"nested": True}],
        }

        envelope = pair_cell_relay_envelope_adapter.validate_python(self._envelope(payload=payload))

        self.assertEqual(payload, envelope.payload)
        self.assertEqual(
            {"type", "protocol_version", "route", "from_worker_id", "to_worker_id", "request_id",
             "payload_hash", "payload"},
            set(envelope.model_dump()),
        )

    def test_carries_no_lease_epoch_attempt_or_trading_fields(self) -> None:
        """Requirement: lease/contract/attempt fencing is gone from the wire.
        Sending any of them is a hard validation failure, not a silent
        ignore, so no Worker can reintroduce controller lifecycle state."""

        for removed in (
            "lease", "lease_epoch", "contract_hash", "attempt_id", "expires_at", "ttl_seconds",
            "symbol", "volume", "universe_generation", "recovery_epoch",
        ):
            with self.subTest(field=removed), self.assertRaises(ValidationError):
                pair_cell_relay_envelope_adapter.validate_python(self._envelope(**{removed: "value"}))

    def test_rejects_a_route_outside_the_routed_pair(self) -> None:
        with self.assertRaises(ValidationError):
            pair_cell_relay_envelope_adapter.validate_python(
                self._envelope(to_worker_id="worker-c")
            )

    def test_rejects_a_sender_outside_the_routed_pair(self) -> None:
        with self.assertRaises(ValidationError):
            pair_cell_relay_envelope_adapter.validate_python(
                self._envelope(from_worker_id="worker-c", to_worker_id="worker-b")
            )

    def test_rejects_a_route_to_self(self) -> None:
        with self.assertRaises(ValidationError):
            pair_cell_relay_envelope_adapter.validate_python(
                self._envelope(from_worker_id="worker-a", to_worker_id="worker-a")
            )

    def test_rejects_an_unknown_protocol_version(self) -> None:
        with self.assertRaises(ValidationError):
            pair_cell_relay_envelope_adapter.validate_python(self._envelope(protocol_version=2))

    def test_rejects_extra_fields(self) -> None:
        with self.assertRaises(ValidationError):
            pair_cell_relay_envelope_adapter.validate_python(self._envelope(unexpected="value"))


class PairCellControlMessageTests(unittest.TestCase):
    """The Worker-facing pairing control plane: role declaration, follower
    listing, the two-phase proposal/decision exchange, route sync, and the
    ``UNPAIRING`` commands. None of it carries a ``trader_id``."""

    def test_an_omitted_role_declaration_means_available_follower(self) -> None:
        """Requirement: a Worker declares a desired role on its first
        unpaired connection, and an omitted role means it is simply an
        available follower. The wire permits the omission."""

        message = pair_cell_control_message_adapter.validate_python(
            {"type": "pair_cell_role", "request_id": "request-1"}
        )

        self.assertIsNone(message.role)

    def test_a_declared_role_is_carried_verbatim(self) -> None:
        for role in ("leader", "follower"):
            with self.subTest(role=role):
                message = pair_cell_control_message_adapter.validate_python(
                    {"type": "pair_cell_role", "request_id": "request-1", "role": role}
                )

                self.assertEqual(role, message.role)

    def test_rejects_an_unknown_role(self) -> None:
        with self.assertRaises(ValidationError):
            pair_cell_control_message_adapter.validate_python(
                {"type": "pair_cell_role", "request_id": "request-1", "role": "coordinator"}
            )

    def test_no_pairing_control_message_accepts_a_trader_id(self) -> None:
        """Requirement: ``trader_id`` is removed from every Pair Execution
        Cell controller API, not merely ignored on one of them."""

        messages = (
            {"type": "pair_cell_role", "request_id": "r"},
            {"type": "pair_cell_available_followers", "request_id": "r"},
            {"type": "pair_cell_pairing_proposal", "request_id": "r", "follower_worker_id": "worker-b"},
            {
                "type": "pair_cell_pairing_decision", "request_id": "r", "proposal_id": "p",
                "accepted": True, "acceptance_payload": {"worker_id": "worker-b"}, "payload_hash": "abc",
            },
            {"type": "pair_cell_route_sync", "request_id": "r"},
            {"type": "pair_cell_unpair", "request_id": "r", "route_id": "route-1", "action": "enter"},
            {"type": "pair_cell_state_version", "request_id": "r", "route_id": "route-1", "state_version": 1},
            {"type": "pair_cell_unpair_assertion", "request_id": "r", "route_id": "route-1", "state_version": 1},
        )

        for message in messages:
            with self.subTest(message=message["type"]):
                pair_cell_control_message_adapter.validate_python(message)
                with self.assertRaises(ValidationError):
                    pair_cell_control_message_adapter.validate_python({**message, "trader_id": "trader-1"})

    def test_every_declared_control_message_type_is_reachable(self) -> None:
        """The controller's dispatch set and the wire union never drift."""

        reachable = {
            message["type"]
            for message in (
                {"type": "pair_cell_role", "request_id": "r"},
                {"type": "pair_cell_available_followers", "request_id": "r"},
                {"type": "pair_cell_pairing_proposal", "request_id": "r", "follower_worker_id": "worker-b"},
                {"type": "pair_cell_pairing_decision", "request_id": "r", "proposal_id": "p", "accepted": False},
                {"type": "pair_cell_route_sync", "request_id": "r"},
                {"type": "pair_cell_unpair", "request_id": "r", "route_id": "route-1", "action": "cancel"},
                {"type": "pair_cell_state_version", "request_id": "r", "route_id": "route-1", "state_version": 0},
                {"type": "pair_cell_unpair_assertion", "request_id": "r", "route_id": "route-1", "state_version": 0},
            )
            if pair_cell_control_message_adapter.validate_python(message) is not None
        }

        self.assertEqual(set(PAIR_CELL_CONTROL_MESSAGE_TYPES), reachable)

    def test_a_pairing_decision_is_an_explicit_boolean(self) -> None:
        """Requirement: a refusal never becomes an implied acceptance. There
        is no default and no truthy coercion."""

        with self.assertRaises(ValidationError):
            pair_cell_control_message_adapter.validate_python(
                {"type": "pair_cell_pairing_decision", "request_id": "r", "proposal_id": "p"}
            )
        for accepted in ("true", 1, "yes"):
            with self.subTest(accepted=accepted), self.assertRaises(ValidationError):
                pair_cell_control_message_adapter.validate_python(
                    {
                        "type": "pair_cell_pairing_decision", "request_id": "r",
                        "proposal_id": "p", "accepted": accepted,
                    }
                )

    def test_an_acceptance_carries_the_opaque_pairing_acceptance_payload(self) -> None:
        """Requirement: an acceptance is not a bare yes. It carries the
        follower's frozen startup balance and its four authored risk tunables
        so the leader can copy them verbatim into ``follower_risk``."""

        payload = {
            "proposal_id": "p", "worker_id": "worker-b",
            "startup_balance_usd": "10000.00", "account_currency": "USD",
            "maximum_margin_fraction": "0.10", "daily_loss_fraction": "0.03",
            "trade_loss_fraction": "0.02", "maximum_loss_per_trade_usd": "40",
        }

        message = pair_cell_control_message_adapter.validate_python(
            {
                "type": "pair_cell_pairing_decision", "request_id": "r", "proposal_id": "p",
                "accepted": True, "acceptance_payload": payload, "payload_hash": "hash-1",
            }
        )

        self.assertEqual(payload, message.acceptance_payload)
        self.assertEqual("hash-1", message.payload_hash)
        self.assertIsNone(message.reason)

    def test_the_acceptance_payload_stays_an_untouched_opaque_mapping(self) -> None:
        """Requirement: the controller never parses the payload, so a shape
        it has never heard of validates and survives byte-for-byte."""

        payload = {
            "proposal_id": "p", "worker_id": "worker-b",
            "startup_balance_usd": "10000.00", "account_currency": "USD",
            "unknown_future_block": {"nested": [1, 2, {"deep": True}]},
        }

        message = pair_cell_control_message_adapter.validate_python(
            {
                "type": "pair_cell_pairing_decision", "request_id": "r", "proposal_id": "p",
                "accepted": True, "acceptance_payload": payload, "payload_hash": "hash-1",
            }
        )

        self.assertEqual(payload, message.acceptance_payload)
        self.assertEqual(payload, message.model_dump()["acceptance_payload"])

    def test_an_acceptance_without_its_payload_or_hash_is_rejected(self) -> None:
        """Requirement: the leader fails closed without the payload, so an
        acceptance that omits it must never reach the route-creation step."""

        for incomplete in (
            {"accepted": True},
            {"accepted": True, "acceptance_payload": {"worker_id": "worker-b"}},
            {"accepted": True, "payload_hash": "hash-1"},
            {"accepted": True, "acceptance_payload": {"worker_id": "worker-b"}, "payload_hash": ""},
            {"accepted": True, "acceptance_payload": None, "payload_hash": "hash-1"},
        ):
            with self.subTest(message=incomplete), self.assertRaises(ValidationError):
                pair_cell_control_message_adapter.validate_python(
                    {
                        "type": "pair_cell_pairing_decision", "request_id": "r",
                        "proposal_id": "p", **incomplete,
                    }
                )

    def test_an_acceptance_payload_must_be_a_mapping(self) -> None:
        for payload in ("a string", ["a", "list"], 7):
            with self.subTest(payload=payload), self.assertRaises(ValidationError):
                pair_cell_control_message_adapter.validate_python(
                    {
                        "type": "pair_cell_pairing_decision", "request_id": "r", "proposal_id": "p",
                        "accepted": True, "acceptance_payload": payload, "payload_hash": "hash-1",
                    }
                )

    def test_a_refusal_carrying_an_acceptance_payload_is_rejected(self) -> None:
        """Requirement: a refusal never becomes an implied acceptance, so it
        may not smuggle the payload that would let a leader publish policy."""

        for smuggled in (
            {"acceptance_payload": {"worker_id": "worker-b"}},
            {"payload_hash": "hash-1"},
            {"acceptance_payload": {"worker_id": "worker-b"}, "payload_hash": "hash-1"},
        ):
            with self.subTest(message=smuggled), self.assertRaises(ValidationError):
                pair_cell_control_message_adapter.validate_python(
                    {
                        "type": "pair_cell_pairing_decision", "request_id": "r", "proposal_id": "p",
                        "accepted": False, "reason": "not empty", **smuggled,
                    }
                )

    def test_an_acceptance_carrying_a_refusal_reason_is_rejected(self) -> None:
        """``reason`` is refusal semantics only; an accepted decision that
        also states a reason is contradictory, not a partial acceptance."""

        with self.assertRaises(ValidationError):
            pair_cell_control_message_adapter.validate_python(
                {
                    "type": "pair_cell_pairing_decision", "request_id": "r", "proposal_id": "p",
                    "accepted": True, "acceptance_payload": {"worker_id": "worker-b"},
                    "payload_hash": "hash-1", "reason": "but also no",
                }
            )

    def test_a_refusal_may_carry_an_opaque_reason(self) -> None:
        message = pair_cell_control_message_adapter.validate_python(
            {
                "type": "pair_cell_pairing_decision", "request_id": "r", "proposal_id": "p",
                "accepted": False, "reason": "account is not broker-verified empty",
            }
        )

        self.assertFalse(message.accepted)
        self.assertEqual("account is not broker-verified empty", message.reason)
        self.assertIsNone(message.acceptance_payload)
        self.assertIsNone(message.payload_hash)

    def test_unpair_commands_name_the_current_route_and_an_explicit_action(self) -> None:
        for action in ("enter", "cancel"):
            with self.subTest(action=action):
                message = pair_cell_control_message_adapter.validate_python(
                    {"type": "pair_cell_unpair", "request_id": "r", "route_id": "route-1", "action": action}
                )

                self.assertEqual(action, message.action)
                self.assertEqual("route-1", message.route_id)

        with self.assertRaises(ValidationError):
            pair_cell_control_message_adapter.validate_python(
                {"type": "pair_cell_unpair", "request_id": "r", "route_id": "route-1", "action": "force"}
            )

    def test_a_safe_unpair_assertion_binds_to_a_route_and_a_state_version(self) -> None:
        """Requirement: each assertion binds to ``route_id`` plus that
        Worker's current local attempt/effect state version. Neither binding
        is optional."""

        message = pair_cell_control_message_adapter.validate_python(
            {
                "type": "pair_cell_unpair_assertion", "request_id": "r",
                "route_id": "route-1", "state_version": 7,
            }
        )

        self.assertEqual("route-1", message.route_id)
        self.assertEqual(7, message.state_version)
        for incomplete in (
            {"type": "pair_cell_unpair_assertion", "request_id": "r", "state_version": 7},
            {"type": "pair_cell_unpair_assertion", "request_id": "r", "route_id": "route-1"},
            {
                "type": "pair_cell_unpair_assertion", "request_id": "r",
                "route_id": "route-1", "state_version": -1,
            },
        ):
            with self.subTest(message=incomplete), self.assertRaises(ValidationError):
                pair_cell_control_message_adapter.validate_python(incomplete)

    def test_pairing_control_messages_carry_no_lease_ttl_or_universe_content(self) -> None:
        """Requirement: the pairing workflow issues no lease and the
        controller validates no discovered universe content."""

        base = {"type": "pair_cell_pairing_proposal", "request_id": "r", "follower_worker_id": "worker-b"}
        for smuggled in ("ttl_seconds", "lease_epoch", "contract_hash", "eligible_products", "built_products"):
            with self.subTest(field=smuggled), self.assertRaises(ValidationError):
                pair_cell_control_message_adapter.validate_python({**base, smuggled: "value"})
