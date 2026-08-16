from __future__ import annotations

import hashlib
import json
import secrets
import shutil
import tarfile
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

import duckdb
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError


class LedgerError(RuntimeError):
    """Raised when a control-plane state transition is invalid."""


class AuthenticationError(LedgerError):
    """Raised when an administrator cannot be authenticated."""


@dataclass(frozen=True)
class AdminSession:
    token: str
    csrf_token: str
    expires_at: datetime


@dataclass(frozen=True)
class Enrollment:
    enrollment_id: str
    login: int
    server: str
    pairing_code: str
    public_key_pem: str
    expires_at: datetime
    status: str


@dataclass(frozen=True)
class ActiveWorker:
    worker_id: str
    login: int
    server: str
    certificate: str
    public_key_pem: str
    password_secret_ref: str


class ControlLedger:
    """Single-process DuckDB authority for control-plane state and events."""

    def __init__(self, path: Path) -> None:
        self._connection = duckdb.connect(str(path))
        self._lock = threading.RLock()
        self._passwords = PasswordHasher()
        self._initialize()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def snapshot(self, destination: Path) -> None:
        """Export a consistent DuckDB snapshot while this process owns the ledger."""

        with self._lock:
            export_directory = destination.with_name(f"{destination.name}.export")
            shutil.rmtree(export_directory, ignore_errors=True)
            try:
                escaped = str(export_directory).replace("'", "''")
                self._connection.execute(f"EXPORT DATABASE '{escaped}'")
                with tarfile.open(destination, "w:gz") as archive:
                    for path in sorted(export_directory.rglob("*")):
                        archive.add(path, arcname=path.relative_to(export_directory), recursive=False)
            finally:
                shutil.rmtree(export_directory, ignore_errors=True)

    def create_admin(self, username: str, password: str) -> None:
        password_hash = self._passwords.hash(password)
        with self._transaction():
            existing = self._connection.execute(
                "SELECT 1 FROM admins WHERE username = ?",
                [username],
            ).fetchone()
            if existing is not None:
                raise LedgerError(f"Administrator {username!r} already exists.")
            self._connection.execute(
                "INSERT INTO admins (username, password_hash, created_at) VALUES (?, ?, ?)",
                [username, password_hash, _utc_now()],
            )
            self._event("admin_created", {"username": username})

    def reset_admin_password(self, username: str, password: str) -> None:
        with self._transaction():
            changed = self._connection.execute(
                "UPDATE admins SET password_hash = ? WHERE username = ?",
                [self._passwords.hash(password), username],
            ).rowcount
            if changed != 1:
                raise LedgerError(f"Administrator {username!r} does not exist.")
            self._connection.execute("DELETE FROM admin_sessions WHERE username = ?", [username])
            self._event("admin_password_reset", {"username": username})

    def authenticate_admin(self, username: str, password: str) -> AdminSession:
        now = _utc_now()
        with self._transaction():
            row = self._connection.execute(
                "SELECT password_hash FROM admins WHERE username = ?",
                [username],
            ).fetchone()
            verified = False
            if row is not None:
                try:
                    verified = self._passwords.verify(row[0], password)
                except (InvalidHashError, VerifyMismatchError):
                    verified = False
            if not verified:
                self._event("admin_login_failed", {"username": username})
                raise AuthenticationError("Invalid administrator credentials.")

            token = secrets.token_urlsafe(32)
            csrf_token = secrets.token_urlsafe(32)
            expires_at = now + timedelta(hours=8)
            self._connection.execute(
                """
                INSERT INTO admin_sessions (token_hash, username, csrf_hash, issued_at, last_seen_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [_hash(token), username, _hash(csrf_token), now, now, expires_at],
            )
            self._event("admin_login_succeeded", {"username": username})
            return AdminSession(token, csrf_token, expires_at)

    def logout_admin(self, token: str) -> None:
        with self._transaction():
            row = self._connection.execute(
                "SELECT username FROM admin_sessions WHERE token_hash = ?",
                [_hash(token)],
            ).fetchone()
            if row is None:
                raise AuthenticationError("Invalid administrator session.")
            self._connection.execute("DELETE FROM admin_sessions WHERE token_hash = ?", [_hash(token)])
            self._event("admin_logout", {"username": row[0]})

    def validate_session(self, token: str, csrf_token: str | None = None, *, require_csrf: bool = False) -> str:
        now = _utc_now()
        with self._transaction():
            row = self._connection.execute(
                """
                SELECT username, csrf_hash, last_seen_at, expires_at
                FROM admin_sessions
                WHERE token_hash = ?
                """,
                [_hash(token)],
            ).fetchone()
            if row is None:
                raise AuthenticationError("Invalid administrator session.")
            username, csrf_hash, last_seen_at, expires_at = row
            if now >= expires_at or now - last_seen_at > timedelta(minutes=30):
                self._connection.execute("DELETE FROM admin_sessions WHERE token_hash = ?", [_hash(token)])
                raise AuthenticationError("Administrator session has expired.")
            if require_csrf and (csrf_token is None or not secrets.compare_digest(_hash(csrf_token), csrf_hash)):
                raise AuthenticationError("CSRF validation failed.")
            self._connection.execute(
                "UPDATE admin_sessions SET last_seen_at = ? WHERE token_hash = ?",
                [now, _hash(token)],
            )
            return username

    def resume_admin_session(self, token: str) -> str:
        self.validate_session(token)
        csrf_token = secrets.token_urlsafe(32)
        with self._transaction():
            self._connection.execute(
                "UPDATE admin_sessions SET csrf_hash = ? WHERE token_hash = ?",
                [_hash(csrf_token), _hash(token)],
            )
        return csrf_token

    def create_enrollment(
        self,
        *,
        login: int,
        server: str,
        pairing_code: str,
        public_key_pem: str,
        account_info: dict[str, object],
        terminal_info: dict[str, object],
        password_secret_ref: str,
        enrollment_challenge: str,
    ) -> Enrollment:
        now = _utc_now()
        expires_at = now + timedelta(minutes=15)
        enrollment = Enrollment(
            enrollment_id=str(uuid4()),
            login=login,
            server=server,
            pairing_code=pairing_code,
            public_key_pem=public_key_pem,
            expires_at=expires_at,
            status="pending",
        )
        with self._transaction():
            self._consume_enrollment_challenge(enrollment_challenge, now)
            self._connection.execute(
                """
                INSERT INTO enrollments (
                    enrollment_id, login, server, pairing_code, public_key_pem, source_ip, account_info, terminal_info, password_secret_ref,
                    status, created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                [
                    enrollment.enrollment_id,
                    login,
                    server,
                    pairing_code,
                    public_key_pem,
                    "not-collected",
                    json.dumps(account_info, sort_keys=True),
                    json.dumps(terminal_info, sort_keys=True),
                    password_secret_ref,
                    now,
                    expires_at,
                ],
            )
            self._event(
                "worker_enrollment_requested",
                {"enrollment_id": enrollment.enrollment_id, "login": login, "server": server},
            )
        return enrollment

    def issue_enrollment_challenge(self) -> tuple[str, datetime]:
        challenge = secrets.token_urlsafe(32)
        now = _utc_now()
        expires_at = now + timedelta(minutes=2)
        with self._transaction():
            self._connection.execute("DELETE FROM enrollment_challenges WHERE expires_at <= ?", [now])
            self._connection.execute(
                "INSERT INTO enrollment_challenges (challenge_hash, issued_at, expires_at) VALUES (?, ?, ?)",
                [_hash(challenge), now, expires_at],
            )
        return challenge, expires_at

    def _consume_enrollment_challenge(self, challenge: str, now: datetime) -> None:
        consumed = self._connection.execute(
            """
            UPDATE enrollment_challenges
            SET consumed_at = ?
            WHERE challenge_hash = ? AND consumed_at IS NULL AND expires_at > ?
            RETURNING challenge_hash
            """,
            [now, _hash(challenge), now],
        ).fetchone()
        if consumed is None:
            raise LedgerError("Enrollment challenge is invalid, expired, or already used.")

    def approve_enrollment(
        self,
        enrollment_id: str,
        approved_by: str,
        issue_certificate: Callable[[str, int, str, str], str],
    ) -> str:
        now = _utc_now()
        with self._transaction():
            row = self._connection.execute(
                """
                SELECT login, server, public_key_pem, status, expires_at
                FROM enrollments WHERE enrollment_id = ?
                """,
                [enrollment_id],
            ).fetchone()
            if row is None:
                raise LedgerError("Enrollment does not exist.")
            login, server, public_key_pem, status, expires_at = row
            if status != "pending" or now >= expires_at:
                raise LedgerError("Enrollment is no longer pending.")
            active = self._connection.execute(
                "SELECT worker_id FROM workers WHERE login = ? AND server = ? AND status = 'active'",
                [login, server],
            ).fetchone()
            if active is not None:
                raise LedgerError("This MT5 account already has an active worker.")
            worker_id = str(uuid4())
            certificate = issue_certificate(worker_id, login, server, public_key_pem)
            self._connection.execute(
                """
                INSERT INTO workers (worker_id, enrollment_id, login, server, certificate, status, approved_at)
                VALUES (?, ?, ?, ?, ?, 'active', ?)
                """,
                [worker_id, enrollment_id, login, server, certificate, now],
            )
            self._connection.execute(
                "UPDATE enrollments SET status = 'approved', approved_by = ?, approved_at = ? WHERE enrollment_id = ?",
                [approved_by, now, enrollment_id],
            )
            self._event(
                "worker_enrollment_approved",
                {"enrollment_id": enrollment_id, "worker_id": worker_id, "approved_by": approved_by},
            )
            return worker_id

    def enrollment_password_secret_ref(self, enrollment_id: str) -> str:
        with self._lock:
            row = self._connection.execute(
                "SELECT password_secret_ref FROM enrollments WHERE enrollment_id = ? AND status = 'pending'",
                [enrollment_id],
            ).fetchone()
        if row is None:
            raise LedgerError("Enrollment is no longer pending.")
        return row[0]

    def active_worker_for_enrollment(self, enrollment_id: str) -> ActiveWorker:
        return self._active_worker(
            """
            SELECT w.worker_id, w.login, w.server, w.certificate, e.public_key_pem, e.password_secret_ref
            FROM workers w JOIN enrollments e ON e.enrollment_id = w.enrollment_id
            WHERE w.enrollment_id = ? AND w.status = 'active'
            """,
            [enrollment_id],
        )

    def active_worker(self, worker_id: str) -> ActiveWorker:
        return self._active_worker(
            """
            SELECT w.worker_id, w.login, w.server, w.certificate, e.public_key_pem, e.password_secret_ref
            FROM workers w JOIN enrollments e ON e.enrollment_id = w.enrollment_id
            WHERE w.worker_id = ? AND w.status = 'active'
            """,
            [worker_id],
        )

    def _active_worker(self, query: str, parameters: list[str]) -> ActiveWorker:
        with self._lock:
            row = self._connection.execute(query, parameters).fetchone()
        if row is None:
            raise LedgerError("Worker is not active.")
        return ActiveWorker(*row)

    def record_snapshot(
        self,
        worker_id: str,
        cursor: int,
        observed_at: str,
        account: dict[str, object],
        terminal: dict[str, object],
        orders: list[object],
        positions: list[object],
    ) -> None:
        with self._transaction():
            self._require_reconciliation_cursor(worker_id, cursor)
            snapshot_id = str(uuid4())
            self._connection.execute(
                """
                INSERT INTO reconciliation_snapshots
                    (worker_id, snapshot_id, cursor, observed_at, account, terminal, orders, positions, received_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    worker_id, snapshot_id, cursor, observed_at, json.dumps(account, sort_keys=True), json.dumps(terminal, sort_keys=True),
                    json.dumps(orders, sort_keys=True), json.dumps(positions, sort_keys=True), _utc_now(),
                ],
            )
            self._connection.execute("UPDATE workers SET last_seen_at = ? WHERE worker_id = ?", [_utc_now(), worker_id])
            self._event("worker_reconciliation_snapshot", {"worker_id": worker_id, "cursor": cursor})

    def record_worker_session(self, worker_id: str) -> None:
        with self._transaction():
            active = self._connection.execute(
                "SELECT 1 FROM workers WHERE worker_id = ? AND status = 'active'", [worker_id]
            ).fetchone()
            if active is None:
                raise LedgerError("Worker is not active.")
            self._connection.execute("UPDATE workers SET last_seen_at = ? WHERE worker_id = ?", [_utc_now(), worker_id])
            self._event("worker_session_authenticated", {"worker_id": worker_id})

    def record_worker_heartbeat(self, worker_id: str) -> None:
        with self._transaction():
            self.active_worker(worker_id)
            self._connection.execute("UPDATE workers SET last_seen_at = ? WHERE worker_id = ?", [_utc_now(), worker_id])
            self._event("worker_heartbeat_received", {"worker_id": worker_id})

    def record_worker_safety_state(self, worker_id: str, state: str, reason: str) -> None:
        if state not in {"connected", "lost_link_safety", "needs_human"}:
            raise LedgerError("Worker safety state is invalid.")
        with self._transaction():
            self.active_worker(worker_id)
            self._connection.execute(
                "UPDATE workers SET safety_state = ?, last_seen_at = ? WHERE worker_id = ?",
                [state, _utc_now(), worker_id],
            )
            self._event("worker_safety_state_changed", {"worker_id": worker_id, "state": state, "reason": reason})
            if state in {"lost_link_safety", "needs_human"}:
                self._alert(worker_id, "high", state, reason)

    def revoke_worker(self, worker_id: str, revoked_by: str) -> None:
        with self._transaction():
            active = self._connection.execute(
                "SELECT 1 FROM workers WHERE worker_id = ? AND status = 'active'", [worker_id]
            ).fetchone()
            if active is None:
                raise LedgerError("Worker is not active.")
            self._connection.execute(
                """
                UPDATE workers SET status = 'revoked', safety_state = 'revoked', revoked_at = ?
                WHERE worker_id = ? AND status = 'active'
                """,
                [_utc_now(), worker_id],
            )
            self._event("worker_certificate_revoked", {"worker_id": worker_id, "revoked_by": revoked_by})
            self._alert(worker_id, "high", "certificate_revoked", "administrator_revocation")

    def record_delta(
        self, worker_id: str, cursor: int, observed_at: str, entity: str, ticket: str, change: str, record: dict[str, object]
    ) -> None:
        with self._transaction():
            expected = self._current_cursor(worker_id) + 1
            if cursor != expected:
                raise LedgerError("Worker reconciliation cursor is not continuous.")
            self._connection.execute(
                """
                INSERT INTO reconciliation_deltas
                    (worker_id, cursor, observed_at, entity, ticket, change, record, received_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [worker_id, cursor, observed_at, entity, ticket, change, json.dumps(record, sort_keys=True), _utc_now()],
            )
            self._connection.execute("UPDATE workers SET last_seen_at = ? WHERE worker_id = ?", [_utc_now(), worker_id])
            self._event(
                "worker_reconciliation_delta",
                {"worker_id": worker_id, "cursor": cursor, "entity": entity, "ticket": ticket, "change": change},
            )
            if not isinstance(record.get("control_plane_command_id"), str) or not record["control_plane_command_id"]:
                self._connection.execute(
                    "UPDATE workers SET safety_state = 'needs_human' WHERE worker_id = ?", [worker_id]
                )
                self._event(
                    "external_broker_change",
                    {"worker_id": worker_id, "cursor": cursor, "entity": entity, "ticket": ticket, "change": change},
                )
                self._alert(worker_id, "high", "external_broker_change", "unattributed_broker_change")

    def worker_reconciliation(self) -> list[dict[str, Any]]:
        now = _utc_now()
        with self._lock:
            workers = self._connection.execute(
                "SELECT worker_id, login, server, status, safety_state, last_seen_at FROM workers ORDER BY approved_at"
            ).fetchall()
            result = []
            for worker_id, login, server, status, safety_state, last_seen_at in workers:
                snapshot = self._connection.execute(
                    """
                    SELECT snapshot_id, cursor, observed_at, account, terminal, orders, positions
                    FROM reconciliation_snapshots WHERE worker_id = ? ORDER BY received_at DESC LIMIT 1
                    """,
                    [worker_id],
                ).fetchone()
                deltas = self._connection.execute(
                    """
                    SELECT cursor, observed_at, entity, ticket, change, record
                    FROM reconciliation_deltas WHERE worker_id = ? ORDER BY cursor DESC LIMIT 50
                    """,
                    [worker_id],
                ).fetchall()
                result.append(
                    {
                        "worker_id": worker_id,
                        "login": login,
                        "server": server,
                        "connectivity": (
                            "revoked" if status == "revoked"
                            else "connected" if last_seen_at and now - last_seen_at <= timedelta(minutes=5) else "stale"
                        ),
                        "safety_state": safety_state,
                        "latest_snapshot": None if snapshot is None else {
                            "snapshot_id": snapshot[0], "cursor": snapshot[1], "observed_at": snapshot[2],
                            "account": json.loads(snapshot[3]), "terminal": json.loads(snapshot[4]),
                            "orders": json.loads(snapshot[5]), "positions": json.loads(snapshot[6]),
                        },
                        "deltas": [
                            {"cursor": row[0], "observed_at": row[1], "entity": row[2], "ticket": row[3],
                             "change": row[4], "record": json.loads(row[5])}
                            for row in reversed(deltas)
                        ],
                    }
                )
        return result

    def alerts(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT alert_id, worker_id, priority, alert_type, reason, occurred_at FROM alerts ORDER BY alert_id"
            ).fetchall()
        return [
            {"alert_id": row[0], "worker_id": row[1], "priority": row[2], "alert_type": row[3],
             "reason": row[4], "occurred_at": row[5]}
            for row in rows
        ]

    def reconciliation_cursor(self, worker_id: str) -> int:
        with self._lock:
            self.active_worker(worker_id)
            return self._current_cursor(worker_id)

    def _require_reconciliation_cursor(self, worker_id: str, cursor: int) -> None:
        if cursor != self._current_cursor(worker_id):
            raise LedgerError("Worker reconciliation cursor is not continuous.")

    def _current_cursor(self, worker_id: str) -> int:
        row = self._connection.execute(
            """
            SELECT greatest(
                coalesce((SELECT max(cursor) FROM reconciliation_snapshots WHERE worker_id = ?), 0),
                coalesce((SELECT max(cursor) FROM reconciliation_deltas WHERE worker_id = ?), 0)
            )
            """,
            [worker_id, worker_id],
        ).fetchone()
        return int(row[0])

    def reject_enrollment(self, enrollment_id: str, rejected_by: str) -> str:
        with self._transaction():
            row = self._connection.execute(
                "SELECT status, password_secret_ref FROM enrollments WHERE enrollment_id = ?",
                [enrollment_id],
            ).fetchone()
            if row is None:
                raise LedgerError("Enrollment is no longer pending.")
            if row[0] == "rejected":
                return row[1]
            if row[0] != "pending":
                raise LedgerError("Enrollment is no longer pending.")
            self._connection.execute(
                "UPDATE enrollments SET status = 'rejected', approved_by = ?, approved_at = ? WHERE enrollment_id = ?",
                [rejected_by, _utc_now(), enrollment_id],
            )
            self._event("worker_enrollment_rejected", {"enrollment_id": enrollment_id, "rejected_by": rejected_by})
            return row[1]

    def expire_pending_enrollments(self) -> list[str]:
        now = _utc_now()
        with self._transaction():
            rows = self._connection.execute(
                """
                SELECT enrollment_id, password_secret_ref
                FROM enrollments
                WHERE (status = 'pending' AND expires_at <= ?)
                   OR (status = 'expired' AND password_secret_deleted_at IS NULL)
                """,
                [now],
            ).fetchall()
            self._connection.execute(
                "UPDATE enrollments SET status = 'expired' WHERE status = 'pending' AND expires_at <= ?",
                [now],
            )
            for enrollment_id, _ in rows:
                self._event("worker_enrollment_expired", {"enrollment_id": enrollment_id})
            return [row[1] for row in rows]

    def mark_pending_password_deleted(self, password_secret_ref: str) -> None:
        with self._transaction():
            self._connection.execute(
                """
                UPDATE enrollments
                SET password_secret_deleted_at = ?
                WHERE status IN ('expired', 'rejected')
                  AND password_secret_ref = ?
                  AND password_secret_deleted_at IS NULL
                """,
                [_utc_now(), password_secret_ref],
            )

    def mark_expired_password_deleted(self, password_secret_ref: str) -> None:
        with self._transaction():
            self._connection.execute(
                """
                UPDATE enrollments
                SET password_secret_deleted_at = ?
                WHERE status = 'expired' AND password_secret_ref = ?
                """,
                [_utc_now(), password_secret_ref],
            )

    def pending_enrollments(self) -> list[dict[str, Any]]:
        with self._transaction():
            rows = self._connection.execute(
                """
                SELECT enrollment_id, login, server, pairing_code, account_info, terminal_info, created_at, expires_at
                FROM enrollments WHERE status = 'pending' ORDER BY created_at
                """
            ).fetchall()
            return [
                {
                    "enrollment_id": row[0],
                    "login": row[1],
                    "server": row[2],
                    "pairing_code": row[3],
                    "account_info": json.loads(row[4]),
                    "terminal_info": json.loads(row[5]),
                    "created_at": row[6],
                    "expires_at": row[7],
                }
                for row in rows
            ]

    def events(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT event_id, event_type, payload, occurred_at FROM events ORDER BY event_id"
            ).fetchall()
        return [
            {"event_id": row[0], "event_type": row[1], "payload": json.loads(row[2]), "occurred_at": row[3]}
            for row in rows
        ]

    def _event(self, event_type: str, payload: dict[str, Any]) -> None:
        self._connection.execute(
            "INSERT INTO events (event_type, payload, occurred_at) VALUES (?, ?, ?)",
            [event_type, json.dumps(payload, sort_keys=True), _utc_now()],
        )

    def _alert(self, worker_id: str, priority: str, alert_type: str, reason: str) -> None:
        self._connection.execute(
            "INSERT INTO alerts (worker_id, priority, alert_type, reason, occurred_at) VALUES (?, ?, ?, ?, ?)",
            [worker_id, priority, alert_type, reason, _utc_now()],
        )

    def _initialize(self) -> None:
        with self._lock:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS admins (
                    username VARCHAR PRIMARY KEY,
                    password_hash VARCHAR NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL
                );
                CREATE TABLE IF NOT EXISTS admin_sessions (
                    token_hash VARCHAR PRIMARY KEY,
                    username VARCHAR NOT NULL,
                    csrf_hash VARCHAR NOT NULL,
                    issued_at TIMESTAMPTZ NOT NULL,
                    last_seen_at TIMESTAMPTZ NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL
                );
                CREATE TABLE IF NOT EXISTS login_failures (
                    source_ip VARCHAR PRIMARY KEY,
                    attempts INTEGER NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ip_locks (
                    source_ip VARCHAR PRIMARY KEY,
                    locked_at TIMESTAMPTZ NOT NULL
                );
                CREATE TABLE IF NOT EXISTS enrollments (
                    enrollment_id VARCHAR PRIMARY KEY,
                    login BIGINT NOT NULL,
                    server VARCHAR NOT NULL,
                    pairing_code VARCHAR NOT NULL,
                    public_key_pem VARCHAR NOT NULL,
                    source_ip VARCHAR NOT NULL,
                    account_info JSON NOT NULL,
                    terminal_info JSON NOT NULL,
                    password_secret_ref VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL,
                    approved_by VARCHAR,
                    approved_at TIMESTAMPTZ,
                    password_secret_deleted_at TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS registration_attempts (
                    source_ip VARCHAR NOT NULL,
                    attempted_at TIMESTAMPTZ NOT NULL
                );
                CREATE TABLE IF NOT EXISTS enrollment_challenges (
                    challenge_hash VARCHAR PRIMARY KEY,
                    issued_at TIMESTAMPTZ NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL,
                    consumed_at TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id VARCHAR PRIMARY KEY,
                    enrollment_id VARCHAR UNIQUE NOT NULL,
                    login BIGINT NOT NULL,
                    server VARCHAR NOT NULL,
                    certificate VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    approved_at TIMESTAMPTZ NOT NULL,
                    last_seen_at TIMESTAMPTZ,
                    safety_state VARCHAR NOT NULL DEFAULT 'connected',
                    revoked_at TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS reconciliation_snapshots (
                    worker_id VARCHAR NOT NULL,
                    snapshot_id VARCHAR NOT NULL,
                    cursor BIGINT NOT NULL,
                    observed_at VARCHAR NOT NULL,
                    account JSON NOT NULL,
                    terminal JSON NOT NULL,
                    orders JSON NOT NULL,
                    positions JSON NOT NULL,
                    received_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (worker_id, snapshot_id)
                );
                CREATE TABLE IF NOT EXISTS reconciliation_deltas (
                    worker_id VARCHAR NOT NULL,
                    cursor BIGINT NOT NULL,
                    observed_at VARCHAR NOT NULL,
                    entity VARCHAR NOT NULL,
                    ticket VARCHAR NOT NULL,
                    change VARCHAR NOT NULL,
                    record JSON NOT NULL,
                    received_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (worker_id, cursor)
                );
                CREATE SEQUENCE IF NOT EXISTS events_sequence START 1;
                CREATE TABLE IF NOT EXISTS events (
                    event_id BIGINT PRIMARY KEY DEFAULT nextval('events_sequence'),
                    event_type VARCHAR NOT NULL,
                    payload JSON NOT NULL,
                    occurred_at TIMESTAMPTZ NOT NULL
                );
                CREATE SEQUENCE IF NOT EXISTS alerts_sequence START 1;
                CREATE TABLE IF NOT EXISTS alerts (
                    alert_id BIGINT PRIMARY KEY DEFAULT nextval('alerts_sequence'),
                    worker_id VARCHAR NOT NULL,
                    priority VARCHAR NOT NULL,
                    alert_type VARCHAR NOT NULL,
                    reason VARCHAR NOT NULL,
                    occurred_at TIMESTAMPTZ NOT NULL
                );
                """
            )
            self._connection.execute(
                "ALTER TABLE enrollments ADD COLUMN IF NOT EXISTS password_secret_deleted_at TIMESTAMPTZ"
            )
            self._connection.execute("ALTER TABLE workers ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ")
            self._connection.execute("ALTER TABLE workers ADD COLUMN IF NOT EXISTS safety_state VARCHAR DEFAULT 'connected'")
            self._connection.execute("ALTER TABLE workers ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ")
            self._connection.execute("UPDATE workers SET safety_state = 'connected' WHERE safety_state IS NULL")
            self._migrate_reconciliation_snapshots()

    def _migrate_reconciliation_snapshots(self) -> None:
        columns = {
            row[1]
            for row in self._connection.execute("PRAGMA table_info('reconciliation_snapshots')").fetchall()
        }
        if "snapshot_id" in columns:
            return
        self._connection.execute("ALTER TABLE reconciliation_snapshots RENAME TO reconciliation_snapshots_legacy")
        self._connection.execute(
            """
            CREATE TABLE reconciliation_snapshots (
                worker_id VARCHAR NOT NULL,
                snapshot_id VARCHAR NOT NULL,
                cursor BIGINT NOT NULL,
                observed_at VARCHAR NOT NULL,
                account JSON NOT NULL,
                terminal JSON NOT NULL,
                orders JSON NOT NULL,
                positions JSON NOT NULL,
                received_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (worker_id, snapshot_id)
            )
            """
        )
        self._connection.execute(
            """
            INSERT INTO reconciliation_snapshots
                (worker_id, snapshot_id, cursor, observed_at, account, terminal, orders, positions, received_at)
            SELECT worker_id, worker_id || ':' || CAST(cursor AS VARCHAR), cursor, observed_at, account, terminal,
                   orders, positions, received_at
            FROM reconciliation_snapshots_legacy
            """
        )
        self._connection.execute("DROP TABLE reconciliation_snapshots_legacy")

    def _transaction(self):
        return _Transaction(self._lock, self._connection)


class _Transaction:
    def __init__(self, lock: threading.RLock, connection: duckdb.DuckDBPyConnection) -> None:
        self._lock = lock
        self._connection = connection

    def __enter__(self) -> None:
        self._lock.acquire()
        self._connection.execute("BEGIN TRANSACTION")

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        commit_authentication_state = isinstance(exc, AuthenticationError)
        self._connection.execute("COMMIT" if exc_type is None or commit_authentication_state else "ROLLBACK")
        self._lock.release()
        return False


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _utc_now() -> datetime:
    return datetime.now(UTC)
