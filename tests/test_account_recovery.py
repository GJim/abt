from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from abt.account_recovery import (
    AccountRecovery,
    ProtectedLeg,
    RecoveryPair,
    _matches_protected_leg,
    converge_empty,
    entry_unconfirmed,
    observe,
    observe_pair,
)
from abt.worker.effect_journal import EffectJournalError, WorkerEffectJournal


class AccountRecoveryTests(unittest.TestCase):
    def test_unknown_entry_requires_observation_then_cancels_before_closing(self) -> None:
        started = entry_unconfirmed(AccountRecovery("worker-a"), "incident-1")

        self.assertEqual(("ENTRY_UNCONFIRMED", "EMPTY", "REQUEST_SNAPSHOT"), (
            started.account.state, started.account.desired_state, started.directive.kind
        ))
        cancelling = observe(
            started.account,
            orders=[{"ticket": 10, "volume_current": 0.1}],
            positions=[{"ticket": 20, "volume": 0.1}],
        )
        self.assertEqual(("CONVERGING_EMPTY", "CANCEL_ORDERS", ("10",)), (
            cancelling.account.state, cancelling.directive.kind, cancelling.directive.tickets
        ))
        closing = observe(cancelling.account, orders=[], positions=[{"ticket": 20, "volume": 0.1}])
        self.assertEqual(("CLOSE_POSITIONS", ("20",)), (closing.directive.kind, closing.directive.tickets))
        ready = observe(closing.account, orders=[], positions=[])
        self.assertEqual(("READY", "NONE"), (ready.account.state, ready.directive.kind))

    def test_protected_leg_divergence_converges_to_empty(self) -> None:
        decision = converge_empty(AccountRecovery("worker-a"), "incident-1", "external broker change")

        self.assertEqual("EMPTY", decision.account.desired_state)
        self.assertEqual("REQUEST_SNAPSHOT", decision.directive.kind)

    def test_pair_requires_two_matching_snapshots_and_converges_both_on_mismatch(self) -> None:
        pair = RecoveryPair(
            "pair-1",
            ("worker-a", "worker-b"),
            (
                ProtectedLeg("10", "EURUSD.a", "0", "0.1", "1.0", "1.2"),
                ProtectedLeg("20", "EURUSD", "1", "0.1", "1.3", "1.1"),
            ),
        )
        waiting = observe_pair(pair, {"worker-a": ([], [{"ticket": 10}])})
        self.assertEqual("CATCHING_UP", waiting.pair.state)
        verified = observe_pair(
            pair,
            {
                "worker-a": ([], [{"ticket": 10, "symbol": "EURUSD.a", "type": 0, "volume": 0.1, "sl": 1.0, "tp": 1.2}]),
                "worker-b": ([], [{"ticket": 20, "symbol": "EURUSD", "type": 1, "volume": 0.1, "sl": 1.3, "tp": 1.1}]),
            },
        )
        self.assertEqual(("ACTIVE_VERIFIED", ()), (verified.pair.state, verified.converge_worker_ids))
        mismatched = observe_pair(
            pair,
            {
                "worker-a": ([], [{"ticket": 10, "symbol": "EURUSD.a", "type": 0, "volume": 0.2, "sl": 1.0, "tp": 1.2}]),
                "worker-b": ([], [{"ticket": 20, "symbol": "EURUSD", "type": 1, "volume": 0.1, "sl": 1.3, "tp": 1.1}]),
            },
        )
        self.assertEqual(("CONVERGING_EMPTY", ("worker-a", "worker-b")), (
            mismatched.pair.state, mismatched.converge_worker_ids
        ))

    def test_protected_leg_numeric_evidence_matches_canonical_values(self) -> None:
        expected = ProtectedLeg("10", "EURUSD", "0", "0.10", "1.20000", "2")

        matches = _matches_protected_leg(
            expected,
            [],
            [{"ticket": 10, "symbol": "EURUSD", "type": 0, "volume": "0.1", "sl": 1.2, "tp": 2}],
        )

        self.assertTrue(matches)

    def test_protected_leg_numeric_evidence_rejects_invalid_values(self) -> None:
        expected = ProtectedLeg("10", "EURUSD", "0", "0.1", "1.2", "2")
        valid_position = {"ticket": 10, "symbol": "EURUSD", "type": 0, "volume": 0.1, "sl": 1.2, "tp": 2}

        for field, invalid_value in (
            ("volume", True),
            ("volume", float("nan")),
            ("sl", float("inf")),
            ("tp", "not-a-number"),
        ):
            with self.subTest(field=field, invalid_value=invalid_value):
                position = valid_position | {field: invalid_value}
                self.assertFalse(_matches_protected_leg(expected, [], [position]))

        invalid_expected = ProtectedLeg("10", "EURUSD", "0", "invalid", "1.2", "2")
        self.assertFalse(_matches_protected_leg(invalid_expected, [], [valid_position]))


class WorkerEffectJournalTests(unittest.TestCase):
    def test_send_started_effect_cannot_be_sent_again_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "effects.sqlite"
            journal = WorkerEffectJournal(path)
            self.assertEqual("prepared", journal.prepare("effect-1", {"action": "market", "symbol": "EURUSD"}))
            journal.mark_send_started("effect-1")
            journal.close()

            restarted = WorkerEffectJournal(path)
            self.assertEqual(
                [{"effect_id": "effect-1", "payload": {"action": "market", "symbol": "EURUSD"}, "state": "send_started"}],
                restarted.unresolved(),
            )
            with self.assertRaisesRegex(EffectJournalError, "must not be sent again"):
                restarted.mark_send_started("effect-1")
            with self.assertRaisesRegex(EffectJournalError, "different payload"):
                restarted.prepare("effect-1", {"action": "market", "symbol": "GBPUSD"})
            restarted.record_observation("effect-1", {"positions": []})
            self.assertEqual([], restarted.unresolved())
            restarted.close()
