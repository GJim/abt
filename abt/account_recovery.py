"""Durable account-recovery lifecycle decisions.

Broker snapshots are facts.  This module only decides the desired account
state and next recovery action; transports and MT5 access remain adapters.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


LifecycleState = Literal[
    "ENTRY_UNCONFIRMED",
    "ACTIVE_VERIFIED",
    "CATCHING_UP",
    "CONVERGING_EMPTY",
    "READY",
    "NEEDS_HUMAN",
    "REVOKED",
]
DesiredState = Literal["EMPTY", "PROTECTED_LEG"]
DirectiveKind = Literal["REQUEST_SNAPSHOT", "CANCEL_ORDERS", "CLOSE_POSITIONS", "NONE"]


@dataclass(frozen=True)
class ProtectedLeg:
    ticket: str
    symbol: str
    side: str
    volume: str
    stop_loss: str
    take_profit: str


@dataclass(frozen=True)
class AccountRecovery:
    worker_id: str
    state: LifecycleState = "CATCHING_UP"
    desired_state: DesiredState = "EMPTY"
    revision: int = 0
    incident_id: str | None = None
    expected_leg: ProtectedLeg | None = None


@dataclass(frozen=True)
class RecoveryDirective:
    kind: DirectiveKind
    reason: str
    revision: int
    tickets: tuple[str, ...] = ()


@dataclass(frozen=True)
class RecoveryDecision:
    account: AccountRecovery
    directive: RecoveryDirective


@dataclass(frozen=True)
class RecoveryPair:
    pair_id: str
    worker_ids: tuple[str, str]
    expected_legs: tuple[ProtectedLeg, ProtectedLeg]
    state: Literal["CATCHING_UP", "ACTIVE_VERIFIED", "CONVERGING_EMPTY"] = "CATCHING_UP"
    revision: int = 0


@dataclass(frozen=True)
class PairRecoveryDecision:
    pair: RecoveryPair
    converge_worker_ids: tuple[str, ...] = ()


def observe_pair(
    pair: RecoveryPair,
    observations: dict[str, tuple[list[dict[str, object]], list[dict[str, object]]]],
) -> PairRecoveryDecision:
    """Compare both full snapshots; partial evidence never verifies a pair."""

    if set(observations) != set(pair.worker_ids):
        return PairRecoveryDecision(pair)
    matches = all(
        _matches_protected_leg(expected, *observations[worker_id])
        for worker_id, expected in zip(pair.worker_ids, pair.expected_legs, strict=True)
    )
    if matches:
        return PairRecoveryDecision(
            RecoveryPair(
                pair_id=pair.pair_id,
                worker_ids=pair.worker_ids,
                expected_legs=pair.expected_legs,
                state="ACTIVE_VERIFIED",
                revision=pair.revision,
            )
        )
    return PairRecoveryDecision(
        RecoveryPair(
            pair_id=pair.pair_id,
            worker_ids=pair.worker_ids,
            expected_legs=pair.expected_legs,
            state="CONVERGING_EMPTY",
            revision=pair.revision + 1,
        ),
        pair.worker_ids,
    )


def entry_unconfirmed(account: AccountRecovery, incident_id: str) -> RecoveryDecision:
    """Block admission while a possible market effect is resolved by evidence."""

    return RecoveryDecision(
        AccountRecovery(
            worker_id=account.worker_id,
            state="ENTRY_UNCONFIRMED",
            desired_state="EMPTY",
            revision=account.revision + 1,
            incident_id=incident_id,
        ),
        RecoveryDirective("REQUEST_SNAPSHOT", "A broker entry outcome is unconfirmed.", account.revision + 1),
    )


def catching_up(account: AccountRecovery, reason: str) -> RecoveryDecision:
    """Require a new full broker snapshot before any account is admitted."""

    return RecoveryDecision(
        AccountRecovery(
            worker_id=account.worker_id,
            state="CATCHING_UP",
            desired_state=account.desired_state,
            revision=account.revision + 1,
            incident_id=account.incident_id,
            expected_leg=account.expected_leg,
        ),
        RecoveryDirective("REQUEST_SNAPSHOT", reason, account.revision + 1),
    )


def converge_empty(account: AccountRecovery, incident_id: str, reason: str) -> RecoveryDecision:
    """Make broker-observed account exposure converge to EMPTY."""

    return RecoveryDecision(
        AccountRecovery(
            worker_id=account.worker_id,
            state="CONVERGING_EMPTY",
            desired_state="EMPTY",
            revision=account.revision + 1,
            incident_id=incident_id,
        ),
        RecoveryDirective("REQUEST_SNAPSHOT", reason, account.revision + 1),
    )


def observe(
    account: AccountRecovery,
    *,
    orders: list[dict[str, object]],
    positions: list[dict[str, object]],
) -> RecoveryDecision:
    """Reduce a current full broker snapshot into the next safe directive."""

    order_tickets = _tickets(orders)
    position_tickets = _tickets(positions)
    if account.state == "REVOKED":
        return _revoked_empty_directive(account, order_tickets, position_tickets)
    if account.desired_state == "EMPTY":
        return _empty_directive(account, order_tickets, position_tickets, "The desired account state is empty.")
    if account.expected_leg is not None and _matches_protected_leg(account.expected_leg, orders, positions):
        verified = AccountRecovery(
            worker_id=account.worker_id,
            state="ACTIVE_VERIFIED",
            desired_state="PROTECTED_LEG",
            revision=account.revision,
            incident_id=account.incident_id,
            expected_leg=account.expected_leg,
        )
        return RecoveryDecision(verified, RecoveryDirective("NONE", "The protected broker leg matches.", verified.revision))
    return converge_empty(account, account.incident_id or "recovery-divergence", "Broker state diverged from the protected leg.")


def _empty_directive(
    account: AccountRecovery, order_tickets: tuple[str, ...], position_tickets: tuple[str, ...], reason: str
) -> RecoveryDecision:
    if order_tickets:
        converging = _converging(account)
        return RecoveryDecision(converging, RecoveryDirective("CANCEL_ORDERS", reason, converging.revision, order_tickets))
    if position_tickets:
        converging = _converging(account)
        return RecoveryDecision(converging, RecoveryDirective("CLOSE_POSITIONS", reason, converging.revision, position_tickets))
    ready = AccountRecovery(
        worker_id=account.worker_id,
        state="READY",
        desired_state="EMPTY",
        revision=account.revision,
        incident_id=account.incident_id,
    )
    return RecoveryDecision(ready, RecoveryDirective("NONE", "The broker account is verified empty.", ready.revision))


def _revoked_empty_directive(
    account: AccountRecovery, order_tickets: tuple[str, ...], position_tickets: tuple[str, ...]
) -> RecoveryDecision:
    if order_tickets:
        return RecoveryDecision(
            account,
            RecoveryDirective("CANCEL_ORDERS", "The revoked Worker account must be empty.", account.revision, order_tickets),
        )
    if position_tickets:
        return RecoveryDecision(
            account,
            RecoveryDirective("CLOSE_POSITIONS", "The revoked Worker account must be empty.", account.revision, position_tickets),
        )
    return RecoveryDecision(
        account,
        RecoveryDirective("NONE", "The revoked Worker account is broker-verified empty.", account.revision),
    )


def _converging(account: AccountRecovery) -> AccountRecovery:
    return AccountRecovery(
        worker_id=account.worker_id,
        state="CONVERGING_EMPTY",
        desired_state="EMPTY",
        revision=account.revision,
        incident_id=account.incident_id,
    )


def _tickets(records: list[dict[str, object]]) -> tuple[str, ...]:
    return tuple(
        str(record["ticket"])
        for record in records
        if isinstance(record.get("ticket"), (str, int)) and not isinstance(record.get("ticket"), bool)
    )


def _matches_protected_leg(
    expected: ProtectedLeg, orders: list[dict[str, object]], positions: list[dict[str, object]]
) -> bool:
    if orders or len(positions) != 1:
        return False
    position = positions[0]
    return (
        str(position.get("ticket")) == expected.ticket
        and str(position.get("symbol")) == expected.symbol
        and str(position.get("type", position.get("side"))) == expected.side
        and str(position.get("volume")) == expected.volume
        and str(position.get("sl", position.get("stop_loss"))) == expected.stop_loss
        and str(position.get("tp", position.get("take_profit"))) == expected.take_profit
    )
