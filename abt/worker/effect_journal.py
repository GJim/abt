"""Worker-local durable evidence for MT5 writes."""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Literal


EffectState = Literal["prepared", "send_started", "receipt", "observed"]


class EffectJournalError(RuntimeError):
    """Raised when an effect would violate durable no-blind-retry semantics."""


class WorkerEffectJournal:
    """SQLite/WAL journal for broker side effects issued by one Worker."""

    def __init__(self, path: Path) -> None:
        self._connection = sqlite3.connect(path)
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS broker_effects (
                effect_id TEXT PRIMARY KEY,
                payload_hash TEXT NOT NULL,
                payload TEXT NOT NULL,
                state TEXT NOT NULL,
                evidence TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()

    def prepare(self, effect_id: str, payload: dict[str, object]) -> str:
        """Persist intent before MT5 access and reject conflicting reuse."""

        if not effect_id:
            raise EffectJournalError("A broker effect ID is required.")
        canonical = _canonical(payload)
        payload_hash = _hash(canonical)
        row = self._connection.execute(
            "SELECT payload_hash, state FROM broker_effects WHERE effect_id = ?", (effect_id,)
        ).fetchone()
        if row is not None:
            if row[0] != payload_hash:
                raise EffectJournalError("Broker effect ID was reused with a different payload.")
            return str(row[1])
        self._connection.execute(
            "INSERT INTO broker_effects (effect_id, payload_hash, payload, state, updated_at) VALUES (?, ?, ?, 'prepared', ?)",
            (effect_id, payload_hash, canonical, _now()),
        )
        self._connection.commit()
        return "prepared"

    def mark_send_started(self, effect_id: str) -> None:
        """Cross the irreversible broker-send boundary exactly once."""

        row = self._connection.execute(
            "SELECT state FROM broker_effects WHERE effect_id = ?", (effect_id,)
        ).fetchone()
        if row is None:
            raise EffectJournalError("Broker effect was not prepared.")
        if row[0] != "prepared":
            raise EffectJournalError("A prepared broker effect must not be sent again.")
        self._update(effect_id, "send_started", None)

    def record_receipt(self, effect_id: str, receipt: dict[str, object]) -> None:
        self._complete(effect_id, "receipt", receipt)

    def record_observation(self, effect_id: str, observation: dict[str, object]) -> None:
        self._complete(effect_id, "observed", observation)

    def unresolved(self) -> list[dict[str, object]]:
        rows = self._connection.execute(
            "SELECT effect_id, payload, state FROM broker_effects WHERE state IN ('prepared', 'send_started') ORDER BY updated_at"
        ).fetchall()
        return [{"effect_id": row[0], "payload": json.loads(row[1]), "state": row[2]} for row in rows]

    def evidence(self, effect_id: str) -> dict[str, object]:
        row = self._connection.execute(
            "SELECT state, evidence FROM broker_effects WHERE effect_id = ?", (effect_id,)
        ).fetchone()
        if row is None or row[0] not in {"receipt", "observed"} or row[1] is None:
            raise EffectJournalError("Broker effect has no conclusive replay evidence.")
        value = json.loads(row[1])
        if not isinstance(value, dict):
            raise EffectJournalError("Broker effect replay evidence is invalid.")
        return value

    def _complete(self, effect_id: str, state: EffectState, evidence: dict[str, object]) -> None:
        row = self._connection.execute("SELECT state FROM broker_effects WHERE effect_id = ?", (effect_id,)).fetchone()
        if row is None:
            raise EffectJournalError("Broker effect was not prepared.")
        if row[0] not in {"prepared", "send_started", state}:
            raise EffectJournalError("Broker effect already has conflicting evidence.")
        self._update(effect_id, state, evidence)

    def _update(self, effect_id: str, state: EffectState, evidence: dict[str, object] | None) -> None:
        self._connection.execute(
            "UPDATE broker_effects SET state = ?, evidence = ?, updated_at = ? WHERE effect_id = ?",
            (state, None if evidence is None else _canonical(evidence), _now(), effect_id),
        )
        self._connection.commit()


def _canonical(payload: dict[str, object]) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, allow_nan=False)


def _hash(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).isoformat()
