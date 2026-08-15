from __future__ import annotations

import hashlib
import json
import secrets
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError


class LedgerError(RuntimeError):
    """Raised when a control-plane state transition is invalid."""


class AuthenticationError(LedgerError):
    """Raised when an administrator cannot be authenticated."""


class IpLockedError(AuthenticationError):
    """Raised when the source IP has exhausted its login attempts."""


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

    def authenticate_admin(self, username: str, password: str, source_ip: str) -> AdminSession:
        now = _utc_now()
        with self._transaction():
            lock = self._connection.execute(
                "SELECT 1 FROM ip_locks WHERE source_ip = ?",
                [source_ip],
            ).fetchone()
            if lock is not None:
                self._event("admin_login_rejected_locked_ip", {"username": username, "source_ip": source_ip})
                raise IpLockedError("This IP address is locked.")

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
                attempts = self._record_login_failure(source_ip, now)
                self._event(
                    "admin_login_failed",
                    {"username": username, "source_ip": source_ip, "attempts": attempts},
                )
                if attempts >= 3:
                    self._connection.execute(
                        "INSERT INTO ip_locks (source_ip, locked_at) VALUES (?, ?)",
                        [source_ip, now],
                    )
                    self._event("admin_ip_locked", {"source_ip": source_ip})
                    raise IpLockedError("This IP address is locked.")
                raise AuthenticationError("Invalid administrator credentials.")

            self._connection.execute("DELETE FROM login_failures WHERE source_ip = ?", [source_ip])
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
            self._event("admin_login_succeeded", {"username": username, "source_ip": source_ip})
            return AdminSession(token, csrf_token, expires_at)

    def unlock_ip(self, source_ip: str) -> None:
        with self._transaction():
            self._connection.execute("DELETE FROM ip_locks WHERE source_ip = ?", [source_ip])
            self._connection.execute("DELETE FROM login_failures WHERE source_ip = ?", [source_ip])
            self._event("admin_ip_unlocked", {"source_ip": source_ip})

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

    def create_enrollment(
        self,
        *,
        login: int,
        server: str,
        pairing_code: str,
        public_key_pem: str,
        source_ip: str,
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
            self._connection.execute(
                """
                INSERT INTO enrollments (
                    enrollment_id, login, server, pairing_code, public_key_pem, source_ip,
                    status, created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                [
                    enrollment.enrollment_id,
                    login,
                    server,
                    pairing_code,
                    public_key_pem,
                    source_ip,
                    now,
                    expires_at,
                ],
            )
            self._event(
                "worker_enrollment_requested",
                {"enrollment_id": enrollment.enrollment_id, "login": login, "server": server, "source_ip": source_ip},
            )
        return enrollment

    def approve_enrollment(self, enrollment_id: str, approved_by: str) -> str:
        now = _utc_now()
        with self._transaction():
            row = self._connection.execute(
                """
                SELECT login, server, status, expires_at
                FROM enrollments WHERE enrollment_id = ?
                """,
                [enrollment_id],
            ).fetchone()
            if row is None:
                raise LedgerError("Enrollment does not exist.")
            login, server, status, expires_at = row
            if status != "pending" or now >= expires_at:
                raise LedgerError("Enrollment is no longer pending.")
            active = self._connection.execute(
                "SELECT worker_id FROM workers WHERE login = ? AND server = ? AND status = 'active'",
                [login, server],
            ).fetchone()
            if active is not None:
                raise LedgerError("This MT5 account already has an active worker.")
            worker_id = str(uuid4())
            self._connection.execute(
                """
                INSERT INTO workers (worker_id, enrollment_id, login, server, status, approved_at)
                VALUES (?, ?, ?, ?, 'active', ?)
                """,
                [worker_id, enrollment_id, login, server, now],
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

    def reject_enrollment(self, enrollment_id: str, rejected_by: str) -> None:
        with self._transaction():
            row = self._connection.execute(
                "SELECT status FROM enrollments WHERE enrollment_id = ?",
                [enrollment_id],
            ).fetchone()
            if row is None or row[0] != "pending":
                raise LedgerError("Enrollment is no longer pending.")
            self._connection.execute(
                "UPDATE enrollments SET status = 'rejected', approved_by = ?, approved_at = ? WHERE enrollment_id = ?",
                [rejected_by, _utc_now(), enrollment_id],
            )
            self._event("worker_enrollment_rejected", {"enrollment_id": enrollment_id, "rejected_by": rejected_by})

    def pending_enrollments(self) -> list[dict[str, Any]]:
        now = _utc_now()
        with self._transaction():
            self._connection.execute(
                "UPDATE enrollments SET status = 'expired' WHERE status = 'pending' AND expires_at <= ?",
                [now],
            )
            rows = self._connection.execute(
                """
                SELECT enrollment_id, login, server, pairing_code, source_ip, created_at, expires_at
                FROM enrollments WHERE status = 'pending' ORDER BY created_at
                """
            ).fetchall()
            return [
                {
                    "enrollment_id": row[0],
                    "login": row[1],
                    "server": row[2],
                    "pairing_code": row[3],
                    "source_ip": row[4],
                    "created_at": row[5],
                    "expires_at": row[6],
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

    def _record_login_failure(self, source_ip: str, now: datetime) -> int:
        row = self._connection.execute(
            "SELECT attempts FROM login_failures WHERE source_ip = ?",
            [source_ip],
        ).fetchone()
        attempts = 1 if row is None else row[0] + 1
        self._connection.execute(
            """
            INSERT INTO login_failures (source_ip, attempts, updated_at) VALUES (?, ?, ?)
            ON CONFLICT (source_ip) DO UPDATE SET attempts = excluded.attempts, updated_at = excluded.updated_at
            """,
            [source_ip, attempts, now],
        )
        return attempts

    def _event(self, event_type: str, payload: dict[str, Any]) -> None:
        self._connection.execute(
            "INSERT INTO events (event_type, payload, occurred_at) VALUES (?, ?, ?)",
            [event_type, json.dumps(payload, sort_keys=True), _utc_now()],
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
                    status VARCHAR NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL,
                    approved_by VARCHAR,
                    approved_at TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id VARCHAR PRIMARY KEY,
                    enrollment_id VARCHAR UNIQUE NOT NULL,
                    login BIGINT NOT NULL,
                    server VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    approved_at TIMESTAMPTZ NOT NULL
                );
                CREATE SEQUENCE IF NOT EXISTS events_sequence START 1;
                CREATE TABLE IF NOT EXISTS events (
                    event_id BIGINT PRIMARY KEY DEFAULT nextval('events_sequence'),
                    event_type VARCHAR NOT NULL,
                    payload JSON NOT NULL,
                    occurred_at TIMESTAMPTZ NOT NULL
                );
                """
            )

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
