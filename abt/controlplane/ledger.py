from __future__ import annotations

import hashlib
import json
import secrets
import shutil
import tarfile
import threading
from base64 import urlsafe_b64decode, urlsafe_b64encode
from binascii import Error as Base64Error
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

import duckdb
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError

from ..trader_protocol import MAX_PAIR_RELAY_ENVELOPE_BYTES, PAIR_CELL_PROTOCOL_VERSION

# The pairing-reservation timeout is control-plane bookkeeping that stops a
# leader vanishing mid-proposal from stranding a follower.  It is deliberately
# not a trading lease, not a TTL on any route, and it never fences, authorizes,
# or expires a broker effect, an attempt, or an established route.
PAIRING_RESERVATION_TIMEOUT_SECONDS = 30

_PAIR_ROUTE_ACTIVE = "ACTIVE"
_PAIR_ROUTE_UNPAIRING = "UNPAIRING"

_PAIR_ROUTE_CLAIM_FIELDS = frozenset({"route_id", "leader_worker_id", "follower_worker_id"})
_PAIR_RELAY_ENVELOPE_FIELDS = frozenset(
    {
        "type",
        "protocol_version",
        "route",
        "from_worker_id",
        "to_worker_id",
        "request_id",
        "payload_hash",
        "payload",
    }
)


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


@dataclass(frozen=True)
class ActiveTrader:
    trader_id: str
    registration_id: str
    strategy_name: str
    certificate: str
    public_key_pem: str


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

    def create_registration_invite(self, issued_by: str, role: str) -> str:
        if role not in {"worker", "trader"}:
            raise LedgerError("Registration invite role is invalid.")
        invite = secrets.token_urlsafe(32)
        now = _utc_now()
        with self._transaction():
            self._connection.execute(
                """
                INSERT INTO registration_invites (invite_hash, role, issued_by, issued_at, expires_at, status)
                VALUES (?, ?, ?, ?, ?, 'active')
                """,
                [_hash(invite), role, issued_by, now, now + timedelta(hours=1)],
            )
            self._event("registration_invite_issued", {"issued_by": issued_by, "role": role})
        return invite

    def consume_registration_invite(self, invite: str, role: str) -> str:
        now = _utc_now()
        with self._transaction():
            self._consume_registration_invite(invite, role, now)
        return role

    def _consume_registration_invite(self, invite: str, role: str, now: datetime) -> None:
        row = self._connection.execute(
            "SELECT role, status, expires_at FROM registration_invites WHERE invite_hash = ?",
            [_hash(invite)],
        ).fetchone()
        if row is None:
            raise LedgerError("Registration invite is invalid.")
        invite_role, status, expires_at = row
        if invite_role != role:
            raise LedgerError("Registration invite role does not match.")
        if status == "used":
            raise LedgerError("Registration invite is already used.")
        if status != "active":
            raise LedgerError("Registration invite is no longer active.")
        if now >= expires_at:
            self._connection.execute(
                "UPDATE registration_invites SET status = 'expired' WHERE invite_hash = ?",
                [_hash(invite)],
            )
            self._event("registration_invite_expired", {"role": role})
            raise LedgerError("Registration invite is expired.")
        self._connection.execute(
            "UPDATE registration_invites SET status = 'used', used_at = ? WHERE invite_hash = ?",
            [now, _hash(invite)],
        )
        self._event("registration_invite_used", {"role": role})

    def registration_invites(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT invite_hash, role, issued_by, issued_at, expires_at, status, used_at, revoked_at
                FROM registration_invites
                ORDER BY issued_at DESC
                """
            ).fetchall()
        return [
            {
                "invite_id": row[0],
                "role": row[1],
                "issued_by": row[2],
                "issued_at": row[3],
                "expires_at": row[4],
                "status": row[5],
                "used_at": row[6],
                "revoked_at": row[7],
            }
            for row in rows
        ]

    def revoke_registration_invite(self, invite: str, revoked_by: str) -> None:
        invite_hash = invite if len(invite) == 64 and all(character in "0123456789abcdef" for character in invite) else _hash(invite)
        with self._transaction():
            changed = self._connection.execute(
                """
                UPDATE registration_invites
                SET status = 'revoked', revoked_at = ?
                WHERE invite_hash = ? AND status = 'active' AND expires_at > ?
                RETURNING invite_hash
                """,
                [_utc_now(), invite_hash, _utc_now()],
            ).fetchone()
            if changed is None:
                raise LedgerError("Registration invite is no longer active.")
            self._event("registration_invite_revoked", {"revoked_by": revoked_by})

    def create_enrollment(
        self,
        *,
        login: int,
        server: str,
        public_key_pem: str,
        account_info: dict[str, object],
        terminal_info: dict[str, object],
        password_secret_ref: str,
        enrollment_challenge: str,
        registration_invite: str | None = None,
        registration_invite_hash: str | None = None,
    ) -> Enrollment:
        now = _utc_now()
        expires_at = now + timedelta(minutes=15)
        enrollment = Enrollment(
            enrollment_id=str(uuid4()),
            login=login,
            server=server,
            public_key_pem=public_key_pem,
            expires_at=expires_at,
            status="pending",
        )
        with self._transaction():
            if registration_invite is not None:
                self._consume_registration_invite(registration_invite, "worker", now)
                registration_invite_hash = _hash(registration_invite)
            self._consume_enrollment_challenge(enrollment_challenge, now)
            self._connection.execute(
                """
                INSERT INTO enrollments (
                    enrollment_id, login, server, public_key_pem, source_ip, account_info, terminal_info, password_secret_ref,
                    status, created_at, expires_at, registration_invite_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                """,
                [
                    enrollment.enrollment_id,
                    login,
                    server,
                    public_key_pem,
                    "not-collected",
                    json.dumps(account_info, sort_keys=True),
                    json.dumps(terminal_info, sort_keys=True),
                    password_secret_ref,
                    now,
                    expires_at,
                    registration_invite_hash,
                ],
            )
            event_id = self._event(
                "worker_enrollment_requested",
                {"enrollment_id": enrollment.enrollment_id, "login": login, "server": server},
            )
            self._alert(
                None,
                "high",
                "worker_enrollment_pending_approval",
                "administrator_approval_required",
                enrollment_id=enrollment.enrollment_id,
            )
        return enrollment

    def create_trader_enrollment(
        self, *, strategy_name: str, claimed_public_ip: str, public_key_pem: str
    ) -> dict[str, Any]:
        now = _utc_now()
        registration_id = str(uuid4())
        with self._transaction():
            self._connection.execute(
                """
                INSERT INTO trader_enrollments
                    (registration_id, strategy_name, claimed_public_ip, public_key_pem, attestation_provider, status, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                [registration_id, strategy_name, claimed_public_ip, public_key_pem, "CNG", now, now + timedelta(minutes=15)],
            )
            self._event("trader_enrollment_requested", {"registration_id": registration_id, "strategy_name": strategy_name})
        return {"registration_id": registration_id, "expires_at": now + timedelta(minutes=15)}

    def pending_trader_enrollments(self) -> list[dict[str, Any]]:
        now = _utc_now()
        with self._transaction():
            self._connection.execute(
                "UPDATE trader_enrollments SET status = 'expired' WHERE status = 'pending' AND expires_at <= ?",
                [now],
            )
            rows = self._connection.execute(
                """
                SELECT registration_id, strategy_name, claimed_public_ip, created_at, expires_at
                FROM trader_enrollments WHERE status = 'pending' ORDER BY created_at
                """
            ).fetchall()
        return [
            {
                "registration_id": row[0],
                "strategy_name": row[1],
                "claimed_public_ip": row[2],
                "created_at": row[3],
                "expires_at": row[4],
            }
            for row in rows
        ]

    def traders(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """SELECT trader_id, registration_id, strategy_name, status, approved_at, revoked_at
                   FROM traders ORDER BY approved_at DESC"""
            ).fetchall()
        return [
            {
                "trader_id": row[0],
                "registration_id": row[1],
                "strategy_name": row[2],
                "status": row[3],
                "approved_at": row[4],
                "revoked_at": row[5],
            }
            for row in rows
        ]

    def approve_trader_enrollment(
        self,
        registration_id: str,
        approved_by: str,
        issue_certificate: Callable[[str, str, str], str],
    ) -> str:
        now = _utc_now()
        with self._transaction():
            row = self._connection.execute(
                """
                SELECT strategy_name, public_key_pem, status, expires_at
                FROM trader_enrollments WHERE registration_id = ?
                """,
                [registration_id],
            ).fetchone()
            if row is None:
                raise LedgerError("Trader enrollment does not exist.")
            strategy_name, public_key_pem, enrollment_status, expires_at = row
            if enrollment_status != "pending" or now >= expires_at:
                raise LedgerError("Trader enrollment is no longer pending.")
            trader_id = str(uuid4())
            certificate = issue_certificate(trader_id, strategy_name, public_key_pem)
            self._connection.execute(
                """
                INSERT INTO traders (trader_id, registration_id, strategy_name, certificate, status, approved_at)
                VALUES (?, ?, ?, ?, 'active', ?)
                """,
                [trader_id, registration_id, strategy_name, certificate, now],
            )
            self._connection.execute(
                """
                UPDATE trader_enrollments
                SET status = 'approved', approved_by = ?, approved_at = ?
                WHERE registration_id = ?
                """,
                [approved_by, now, registration_id],
            )
            event_id = self._event(
                "trader_enrollment_approved",
                {"registration_id": registration_id, "trader_id": trader_id, "approved_by": approved_by},
            )
            return trader_id

    def reject_trader_enrollment(self, registration_id: str, rejected_by: str) -> None:
        with self._transaction():
            changed = self._connection.execute(
                """
                UPDATE trader_enrollments SET status = 'rejected', approved_by = ?, approved_at = ?
                WHERE registration_id = ? AND status = 'pending' AND expires_at > ?
                RETURNING registration_id
                """,
                [rejected_by, _utc_now(), registration_id, _utc_now()],
            ).fetchone()
            if changed is None:
                raise LedgerError("Trader enrollment is no longer pending.")
            self._event(
                "trader_enrollment_rejected",
                {"registration_id": registration_id, "rejected_by": rejected_by},
            )

    def active_trader_for_enrollment(self, registration_id: str) -> ActiveTrader:
        return self._active_trader(
            """
            SELECT t.trader_id, t.registration_id, t.strategy_name, t.certificate, e.public_key_pem
            FROM traders t JOIN trader_enrollments e ON e.registration_id = t.registration_id
            WHERE t.registration_id = ? AND t.status = 'active'
            """,
            [registration_id],
        )

    def trader_enrollment_status(self, registration_id: str) -> str:
        """Return the current status for a Trader's external enrollment identifier."""

        with self._lock:
            row = self._connection.execute(
                "SELECT status FROM trader_enrollments WHERE registration_id = ?", [registration_id]
            ).fetchone()
        if row is None:
            raise LedgerError("Trader enrollment does not exist.")
        return str(row[0])

    def active_trader(self, trader_id: str) -> ActiveTrader:
        return self._active_trader(
            """
            SELECT t.trader_id, t.registration_id, t.strategy_name, t.certificate, e.public_key_pem
            FROM traders t JOIN trader_enrollments e ON e.registration_id = t.registration_id
            WHERE t.trader_id = ? AND t.status = 'active'
            """,
            [trader_id],
        )

    def _active_trader(self, query: str, parameters: list[str]) -> ActiveTrader:
        with self._lock:
            row = self._connection.execute(query, parameters).fetchone()
        if row is None:
            raise LedgerError("Trader is not active.")
        return ActiveTrader(*row)

    def revoke_trader(self, trader_id: str, revoked_by: str) -> None:
        with self._transaction():
            changed = self._connection.execute(
                """
                UPDATE traders SET status = 'revoked', revoked_at = ?
                WHERE trader_id = ? AND status = 'active'
                RETURNING trader_id
                """,
                [_utc_now(), trader_id],
            ).fetchone()
            if changed is None:
                raise LedgerError("Trader is not active.")
            self._event("trader_certificate_revoked", {"trader_id": trader_id, "revoked_by": revoked_by})

    def rotate_trader_certificate(
        self, trader_id: str, public_key_pem: str, issue_certificate: Callable[[str, str, str], str]
    ) -> str:
        """Replace an active Trader's key only after the service verified both proofs."""

        with self._transaction():
            row = self._connection.execute(
                "SELECT registration_id, strategy_name FROM traders WHERE trader_id = ? AND status = 'active'", [trader_id]
            ).fetchone()
            if row is None:
                raise LedgerError("Trader is not active.")
            registration_id, strategy_name = row
            predecessor = self.active_trader(trader_id)
            certificate = issue_certificate(trader_id, strategy_name, public_key_pem)
            self._connection.execute(
                """
                INSERT INTO certificate_overlaps (role, identity_id, certificate, public_key_pem, expires_at)
                VALUES ('trader', ?, ?, ?, ?)
                """,
                [trader_id, predecessor.certificate, predecessor.public_key_pem, _utc_now() + timedelta(hours=1)],
            )
            self._connection.execute(
                "UPDATE trader_enrollments SET public_key_pem = ? WHERE registration_id = ?",
                [public_key_pem, registration_id],
            )
            self._connection.execute("UPDATE traders SET certificate = ? WHERE trader_id = ?", [certificate, trader_id])
            self._event(
                "trader_certificate_rotated",
                {"trader_id": trader_id},
            )
            return certificate

    def trader_for_certificate(self, trader_id: str, certificate: str) -> ActiveTrader:
        trader = self.active_trader(trader_id)
        if certificate == trader.certificate:
            return trader
        with self._lock:
            row = self._connection.execute(
                """
                SELECT public_key_pem FROM certificate_overlaps
                WHERE role = 'trader' AND identity_id = ? AND certificate = ? AND expires_at > ?
                """,
                [trader_id, certificate, _utc_now()],
            ).fetchone()
        if row is None:
            raise LedgerError("Trader certificate is no longer active.")
        return replace(trader, certificate=certificate, public_key_pem=row[0])

    def open_trader_session(self, trader_id: str) -> str:
        session_id = str(uuid4())
        now = _utc_now()
        with self._transaction():
            self._connection.execute(
                """INSERT INTO trader_sessions (session_id, trader_id, status, last_valid_signal_at, opened_at)
                   VALUES (?, ?, 'connected', ?, ?)""",
                [session_id, trader_id, now, now],
            )
            self._event("trader_session_authenticated", {"trader_id": trader_id, "session_id": session_id})
        return session_id

    def record_trader_signal(self, session_id: str) -> None:
        with self._transaction():
            self._connection.execute(
                "UPDATE trader_sessions SET status = 'connected', last_valid_signal_at = ? WHERE session_id = ?",
                [_utc_now(), session_id],
            )

    def sweep_stale_trader_sessions(self) -> int:
        """Durably mark every disconnected/silent Trader session stale."""

        with self._transaction():
            now = _utc_now()
            changed = self._connection.execute(
                """UPDATE trader_sessions SET status = 'stale', stale_at = ?
                   WHERE status = 'connected' AND last_valid_signal_at <= ?
                   RETURNING trader_id, session_id""",
                [now, now - timedelta(minutes=5)],
            ).fetchall()
            for trader_id, session_id in changed:
                self._event("trader_session_stale", {"trader_id": trader_id, "session_id": session_id})
            return len(changed)

    def record_trader_relay(
        self, trader_id: str, request_id: str, worker_id: str, envelope: dict[str, Any]
    ) -> dict[str, Any]:
        """Record an opaque end-to-end envelope without interpreting its trading payload."""

        canonical = json.dumps(envelope, separators=(",", ":"), sort_keys=True, allow_nan=False)
        envelope_hash = _hash(canonical)
        with self._transaction():
            self.active_trader(trader_id)
            self.active_worker(worker_id)
            existing = self._connection.execute(
                """SELECT envelope_hash, status, outcome FROM trader_worker_relay
                   WHERE trader_id = ? AND request_id = ?""",
                [trader_id, request_id],
            ).fetchone()
            if existing is not None:
                if existing[0] != envelope_hash:
                    raise LedgerError("Trader relay request ID was reused with a different envelope.")
                return {
                    "status": existing[1],
                    "outcome": None if existing[2] is None else json.loads(existing[2]),
                    "dispatch": existing[1] == "recorded",
                }
            event_id = self._event(
                "trader_worker_relay_recorded",
                {
                    "trader_id": trader_id,
                    "worker_id": worker_id,
                    "request_id": request_id,
                    "envelope": envelope,
                },
            )
            self._connection.execute(
                """INSERT INTO trader_worker_relay
                   (trader_id, request_id, worker_id, envelope_hash, envelope, status, requested_event_id, requested_at)
                   VALUES (?, ?, ?, ?, ?, 'recorded', ?, ?)""",
                [trader_id, request_id, worker_id, envelope_hash, canonical, event_id, _utc_now()],
            )
            return {"status": "recorded", "outcome": None, "dispatch": True}

    def complete_trader_relay(
        self, trader_id: str, request_id: str, outcome: dict[str, Any]
    ) -> dict[str, Any]:
        """Persist an opaque Worker outcome before relaying it to the Strategy Runtime."""

        canonical = json.dumps(outcome, separators=(",", ":"), sort_keys=True, allow_nan=False)
        with self._transaction():
            row = self._connection.execute(
                """SELECT worker_id, status, outcome FROM trader_worker_relay
                   WHERE trader_id = ? AND request_id = ?""",
                [trader_id, request_id],
            ).fetchone()
            if row is None:
                raise LedgerError("Trader relay request does not exist.")
            if row[1] == "completed":
                if row[2] != canonical:
                    raise LedgerError("Trader relay request already has a different outcome.")
                return json.loads(row[2])
            event_id = self._event(
                "trader_worker_relay_completed",
                {
                    "trader_id": trader_id,
                    "worker_id": row[0],
                    "request_id": request_id,
                    "outcome": outcome,
                },
            )
            self._connection.execute(
                """UPDATE trader_worker_relay SET status = 'completed', outcome = ?,
                   completed_event_id = ?, completed_at = ?
                   WHERE trader_id = ? AND request_id = ?""",
                [canonical, event_id, _utc_now(), trader_id, request_id],
            )
            return outcome

    def record_worker_fact_audit(self, worker_id: str, envelope: dict[str, Any]) -> int:
        """Append an immutable opaque Worker fact without reducing trading state."""

        recovery_epoch = envelope.get("recovery_epoch")
        event_id = envelope.get("event_id", envelope.get("snapshot_baseline"))
        if (
            not isinstance(recovery_epoch, str)
            or not recovery_epoch
            or isinstance(event_id, bool)
            or not isinstance(event_id, int)
        ):
            raise LedgerError("Worker fact delivery metadata is invalid.")
        canonical = json.dumps(envelope, separators=(",", ":"), sort_keys=True, allow_nan=False)
        envelope_hash = _hash(canonical)
        with self._transaction():
            if self._connection.execute(
                "SELECT 1 FROM workers WHERE worker_id = ?", [worker_id]
            ).fetchone() is None:
                raise LedgerError("Worker does not exist.")
            existing = self._connection.execute(
                """SELECT envelope_hash FROM worker_relay_facts
                   WHERE worker_id = ? AND recovery_epoch = ? AND event_id = ?""",
                [worker_id, recovery_epoch, event_id],
            ).fetchone()
            if existing is not None:
                if existing[0] != envelope_hash:
                    raise LedgerError("Worker fact event ID was reused with different content.")
                return 0
            audit_event_id = self._event(
                "worker_fact_relayed",
                {"worker_id": worker_id, "envelope": envelope},
            )
            self._connection.execute(
                """INSERT INTO worker_relay_facts
                   (worker_id, recovery_epoch, event_id, envelope_hash, envelope, audit_event_id, recorded_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                [worker_id, recovery_epoch, event_id, envelope_hash, canonical, audit_event_id, _utc_now()],
            )
            return audit_event_id

    def worker_relay_resume(
        self, worker_id: str, recovery_epoch: str | None, event_id: int
    ) -> dict[str, Any]:
        """Return immutable Worker envelopes after a cursor or report a stream gap."""

        with self._lock:
            latest = self._connection.execute(
                """SELECT recovery_epoch FROM worker_relay_facts
                   WHERE worker_id = ? ORDER BY recorded_at DESC LIMIT 1""",
                [worker_id],
            ).fetchone()
            if latest is None:
                return {"status": "resumed", "recovery_epoch": recovery_epoch or "", "facts": []}
            latest_epoch = str(latest[0])
            if recovery_epoch is not None and recovery_epoch != latest_epoch:
                return {"status": "gap", "recovery_epoch": latest_epoch, "facts": []}
            rows = self._connection.execute(
                """SELECT event_id, envelope FROM worker_relay_facts
                   WHERE worker_id = ? AND recovery_epoch = ? AND event_id > ?
                   ORDER BY event_id""",
                [worker_id, latest_epoch, event_id],
            ).fetchall()
            minimum = self._connection.execute(
                """SELECT MIN(event_id) FROM worker_relay_facts
                   WHERE worker_id = ? AND recovery_epoch = ?""",
                [worker_id, latest_epoch],
            ).fetchone()[0]
        if minimum is not None and event_id + 1 < int(minimum):
            return {"status": "gap", "recovery_epoch": latest_epoch, "facts": []}
        return {
            "status": "resumed",
            "recovery_epoch": latest_epoch,
            "facts": [json.loads(row[1]) for row in rows],
        }

    def acknowledge_worker_fact(
        self,
        trader_id: str,
        worker_id: str,
        recovery_epoch: str,
        event_id: int,
    ) -> None:
        with self._transaction():
            self.active_trader(trader_id)
            self._connection.execute(
                """INSERT INTO trader_worker_fact_cursors
                   (trader_id, worker_id, recovery_epoch, event_id, acknowledged_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(trader_id, worker_id) DO UPDATE SET
                     recovery_epoch = excluded.recovery_epoch,
                     event_id = excluded.event_id,
                     acknowledged_at = excluded.acknowledged_at
                   WHERE excluded.recovery_epoch != trader_worker_fact_cursors.recovery_epoch
                      OR excluded.event_id > trader_worker_fact_cursors.event_id""",
                [trader_id, worker_id, recovery_epoch, event_id, _utc_now()],
            )
            self._event(
                "trader_worker_fact_acknowledged",
                {
                    "trader_id": trader_id,
                    "worker_id": worker_id,
                    "recovery_epoch": recovery_epoch,
                    "event_id": event_id,
                },
            )

    def record_worker_connection_audit(self, worker_id: str, source: str, reason: str) -> int:
        with self._transaction():
            if self._connection.execute(
                "SELECT 1 FROM workers WHERE worker_id = ?", [worker_id]
            ).fetchone() is None:
                raise LedgerError("Worker does not exist.")
            return self._event(
                "worker_connection_changed",
                {"worker_id": worker_id, "source": source, "reason": reason},
            )

    # ---- Pair Execution Cell: pairing, route ownership, opaque relay ---- #
    #
    # Pairing is Worker-initiated.  The controller is an identity and
    # authentication authority, the arbiter of exclusive route ownership
    # through a two-phase reservation workflow, and an opaque relay.  It is
    # never a trading coordinator: it issues no lease or contract, keeps no
    # attempt projection, parses no payload, and derives neither broker
    # emptiness nor attempt terminality.

    def claim_pair_execution_mode(
        self, leader_worker_id: str, follower_worker_id: str, mode: str, trader_id: str | None = None
    ) -> None:
        """Enforce that a Worker pair has exactly one live *authorized* owner.

        This is the legacy Strategy Runtime's rollout/authorization control,
        not lifecycle coordination: ``strategy_runtime`` and
        ``pair_execution_cell`` are mutually exclusive for the same pair,
        while ``shadow`` may run alongside either.

        A ``pair_execution_cell`` claim is deliberately *not* reachable here.
        It is taken for both Workers inside the pairing reservation
        transaction and confirmed inside the final route-creation
        compare-and-swap, so no operator or Trader surface can create,
        contradict, or strand one.
        """

        if mode == "pair_execution_cell":
            raise LedgerError(
                "A Pair Execution Cell execution-mode claim is taken only by the two-phase pairing workflow."
            )
        with self._transaction():
            self._claim_pair_execution_mode_locked(leader_worker_id, follower_worker_id, mode, trader_id)

    def _claim_pair_execution_mode_locked(
        self, leader_worker_id: str, follower_worker_id: str, mode: str, trader_id: str | None
    ) -> None:
        if mode not in ("strategy_runtime", "pair_execution_cell", "shadow"):
            raise LedgerError("Invalid Pair Execution Cell execution mode.")
        if mode == "pair_execution_cell" and trader_id is not None:
            raise LedgerError("A Pair Execution Cell execution-mode claim carries no Trader identity.")
        if mode == "strategy_runtime" and not trader_id:
            raise LedgerError("A legacy Strategy Runtime execution-mode claim requires its Trader identity.")
        pair_key = _pair_key(leader_worker_id, follower_worker_id)
        self.active_worker(leader_worker_id)
        self.active_worker(follower_worker_id)
        if mode != "shadow":
            existing = self._connection.execute(
                "SELECT mode, trader_id FROM pair_execution_owners WHERE pair_key = ? AND mode != 'shadow'",
                [pair_key],
            ).fetchone()
            if existing is not None and (existing[0] != mode or existing[1] != trader_id):
                raise LedgerError(
                    "This Worker pair already has a different live execution owner; "
                    "the legacy Strategy Runtime and a Pair Execution Cell cannot both run for the same pair."
                )
        self._connection.execute(
            """INSERT INTO pair_execution_owners (pair_key, mode, owner_kind, trader_id, claimed_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT (pair_key, mode) DO UPDATE SET
                 owner_kind = excluded.owner_kind,
                 trader_id = excluded.trader_id,
                 claimed_at = excluded.claimed_at""",
            [pair_key, mode, mode, trader_id, _utc_now()],
        )
        self._event(
            "pair_execution_mode_claimed",
            {
                "leader_worker_id": leader_worker_id,
                "follower_worker_id": follower_worker_id,
                "mode": mode,
                "owner_kind": mode,
                "trader_id": trader_id,
            },
        )

    def release_pair_execution_mode(self, leader_worker_id: str, follower_worker_id: str, mode: str) -> None:
        """Release a legacy Strategy Runtime or shadow execution-mode claim.

        A ``pair_execution_cell`` claim is owned by the pairing workflow and
        is released only when its reservation is released or its route is
        removed, so it can never be dropped out from under a live route.
        """

        if mode == "pair_execution_cell":
            raise LedgerError(
                "A Pair Execution Cell execution-mode claim is released only by its reservation or route."
            )
        with self._transaction():
            self._release_pair_execution_mode_locked(
                leader_worker_id, follower_worker_id, mode, reason="explicit_release"
            )

    def _release_pair_execution_mode_locked(
        self, leader_worker_id: str, follower_worker_id: str, mode: str, *, reason: str
    ) -> None:
        pair_key = _pair_key(leader_worker_id, follower_worker_id)
        self._connection.execute(
            "DELETE FROM pair_execution_owners WHERE pair_key = ? AND mode = ?", [pair_key, mode]
        )
        self._event(
            "pair_execution_mode_released",
            {
                "leader_worker_id": leader_worker_id,
                "follower_worker_id": follower_worker_id,
                "mode": mode,
                "reason": reason,
            },
        )

    def pair_execution_owner(
        self, leader_worker_id: str, follower_worker_id: str, mode: str
    ) -> dict[str, Any] | None:
        """One pair's live execution-owner record, for assertions and diagnostics."""

        with self._lock:
            row = self._connection.execute(
                """SELECT mode, owner_kind, trader_id, claimed_at FROM pair_execution_owners
                   WHERE pair_key = ? AND mode = ?""",
                [_pair_key(leader_worker_id, follower_worker_id), mode],
            ).fetchone()
        if row is None:
            return None
        return {"mode": row[0], "owner_kind": row[1], "trader_id": row[2], "claimed_at": row[3]}

    # ---- Worker-initiated pairing: role declaration and availability ---- #

    def declare_pair_cell_role(self, worker_id: str, role: str | None = None) -> dict[str, Any]:
        """Record an authenticated Worker's desired Pair Execution Cell role.

        An omitted ``role`` means the Worker is simply an *available
        follower*.  A durable route always wins: a Worker already on a route
        resumes its assigned role, and a declaration that contradicts the
        route is kept as a diagnostic rather than silently changing who
        leads.
        """

        declared_role = role
        resolved_role = "follower" if role is None else role
        if resolved_role not in ("leader", "follower"):
            raise LedgerError("A Pair Execution Cell role declaration must be 'leader' or 'follower'.")
        with self._transaction():
            self.active_worker(worker_id)
            self._connection.execute(
                """INSERT INTO pair_cell_role_declarations (worker_id, role, declared, declared_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT (worker_id) DO UPDATE SET
                     role = excluded.role, declared = excluded.declared, declared_at = excluded.declared_at""",
                [worker_id, resolved_role, declared_role is not None, _utc_now()],
            )
            route = self._pair_route_for_worker_locked(worker_id)
            self._event(
                "pair_cell_role_declared",
                {
                    "worker_id": worker_id,
                    "role": resolved_role,
                    "declared": declared_role is not None,
                    "route_id": None if route is None else route["route_id"],
                    "assigned_role": None if route is None else route["role"],
                },
            )
        return {
            "worker_id": worker_id,
            "role": resolved_role,
            "declared_role": declared_role,
            "route": route,
        }

    def available_pair_followers(
        self, *, requesting_worker_id: str, connected_worker_ids: Any
    ) -> list[dict[str, Any]]:
        """The currently connected, available, unpaired followers.

        Available means: authenticated and active, connected *now*, declared
        follower (or having declared no role at all, which means the same
        thing), on no pair route, and held by no live pairing reservation.
        Connectivity is the caller's live session registry -- the controller
        never lists a Worker it cannot reach right now.
        """

        connected = {worker_id for worker_id in connected_worker_ids if isinstance(worker_id, str)}
        with self._transaction():
            self.active_worker(requesting_worker_id)
            self._expire_pair_route_reservations_locked(_utc_now())
            routed = self._pair_routed_worker_ids_locked()
            reserved = self._pair_reserved_worker_ids_locked()
            rows = self._connection.execute(
                """SELECT w.worker_id, w.login, w.server, d.role, d.declared, d.declared_at
                   FROM workers w
                   LEFT JOIN pair_cell_role_declarations d ON d.worker_id = w.worker_id
                   WHERE w.status = 'active' ORDER BY w.worker_id"""
            ).fetchall()
        return [
            {
                "worker_id": row[0],
                "login": row[1],
                "server": row[2],
                "declared_role": row[3] if row[4] else None,
                "declared_at": row[5],
            }
            for row in rows
            if row[0] != requesting_worker_id
            and row[0] in connected
            and (row[3] is None or row[3] == "follower")
            and row[0] not in routed
            and row[0] not in reserved
        ]

    # ---- Two-phase pairing: reservation, then final route creation ---- #

    def reserve_pair_route_proposal(
        self,
        *,
        leader_worker_id: str,
        follower_worker_id: str,
        connected_worker_ids: Any,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Phase one: atomically reserve **both** Workers for one proposal.

        One transaction generates a unique ``proposal_id`` and reserves the
        proposing leader and the selected follower together, succeeding only
        if, at that instant, both are authenticated and connected, appear in
        no other route and no other live reservation, hold no conflicting
        live strategy-runtime ownership, and can claim
        ``pair_execution_cell`` execution-mode exclusivity -- which is taken
        for both of them here.

        The reservation carries a 30-second pairing-reservation timeout.
        That timeout is pairing *control* metadata: it is not a trading
        lease, not a TTL on any route, and it never fences, authorizes, or
        expires a broker effect, an attempt, or an established route.

        Two leaders selecting the same follower therefore produce exactly one
        winner and one deterministic conflict here, at phase one; the loser
        re-lists and selects again rather than waiting the winner out.
        """

        if leader_worker_id == follower_worker_id:
            raise LedgerError("A Pair Execution Cell pairing must use two distinct Workers.")
        connected = {worker_id for worker_id in connected_worker_ids if isinstance(worker_id, str)}
        moment = now or _utc_now()
        with self._transaction():
            self._expire_pair_route_reservations_locked(moment)
            self.active_worker(leader_worker_id)
            self.active_worker(follower_worker_id)
            for worker_id in (leader_worker_id, follower_worker_id):
                if worker_id not in connected:
                    raise LedgerError(
                        "A Pair Execution Cell pairing requires both Workers to be connected right now."
                    )
            self._require_pair_worker_eligibility_locked(leader_worker_id, follower_worker_id)
            self._claim_pair_execution_mode_locked(
                leader_worker_id, follower_worker_id, "pair_execution_cell", None
            )
            proposal_id = str(uuid4())
            expires_at = moment + timedelta(seconds=PAIRING_RESERVATION_TIMEOUT_SECONDS)
            self._connection.execute(
                """INSERT INTO pair_route_reservations
                   (proposal_id, leader_worker_id, follower_worker_id, reserved_at, expires_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [proposal_id, leader_worker_id, follower_worker_id, moment, expires_at],
            )
            self._event(
                "pair_route_reserved",
                {
                    "proposal_id": proposal_id,
                    "leader_worker_id": leader_worker_id,
                    "follower_worker_id": follower_worker_id,
                    "timeout_seconds": PAIRING_RESERVATION_TIMEOUT_SECONDS,
                },
            )
        return {
            "proposal_id": proposal_id,
            "leader_worker_id": leader_worker_id,
            "follower_worker_id": follower_worker_id,
            "reserved_at": moment,
            "expires_at": expires_at,
            "timeout_seconds": PAIRING_RESERVATION_TIMEOUT_SECONDS,
        }

    def pair_route_reservation(self, proposal_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                """SELECT proposal_id, leader_worker_id, follower_worker_id, reserved_at, expires_at
                   FROM pair_route_reservations WHERE proposal_id = ?""",
                [proposal_id],
            ).fetchone()
        if row is None:
            return None
        return {
            "proposal_id": row[0],
            "leader_worker_id": row[1],
            "follower_worker_id": row[2],
            "reserved_at": row[3],
            "expires_at": row[4],
        }

    def pair_route_reservations_for_worker(self, worker_id: str) -> list[dict[str, Any]]:
        """Every live reservation holding this Worker, for peer notification."""

        with self._lock:
            rows = self._connection.execute(
                """SELECT proposal_id, leader_worker_id, follower_worker_id, reserved_at, expires_at
                   FROM pair_route_reservations
                   WHERE leader_worker_id = ? OR follower_worker_id = ?
                   ORDER BY reserved_at, proposal_id""",
                [worker_id, worker_id],
            ).fetchall()
        return [
            {
                "proposal_id": row[0],
                "leader_worker_id": row[1],
                "follower_worker_id": row[2],
                "reserved_at": row[3],
                "expires_at": row[4],
            }
            for row in rows
        ]

    def authorize_pair_acceptance_payload(
        self,
        *,
        proposal_id: str,
        from_worker_id: str,
        acceptance_payload: Any,
        payload_hash: Any,
    ) -> dict[str, Any]:
        """Authorize one opaque Pairing Acceptance payload for forwarding.

        The follower authors its own frozen startup balance and its own risk
        tunables and sends them to the leader through the controller, which
        forwards them **unchanged**.  Exactly four things are checked here:
        the payload is an encodable mapping, its hash is a non-empty string,
        the sending Worker is the follower of a still-live reservation for
        this ``proposal_id``, and the encoded record is within the same size
        limit as any relayed envelope.

        Nothing inside ``acceptance_payload`` is read.  The balance, the
        account currency, the four risk tunables, and the hash's meaning are
        all invisible to the controller -- it is not a trading coordinator
        and may not recompute, clamp, validate, or store any of them.
        Nothing about the payload is persisted either: this is ordinary
        delivery authorization, never an authoritative trading projection.
        """

        if not isinstance(acceptance_payload, dict):
            raise LedgerError("Pairing Acceptance payload must be an opaque mapping.")
        if not isinstance(payload_hash, str) or not payload_hash:
            raise LedgerError("Pairing Acceptance payload hash is invalid.")
        reservation = self.pair_route_reservation(proposal_id)
        if reservation is None or reservation["follower_worker_id"] != from_worker_id:
            raise LedgerError("No live pairing reservation names this Worker as its follower.")
        forwarded = {
            "proposal_id": proposal_id,
            "from_worker_id": from_worker_id,
            "payload_hash": payload_hash,
            "payload": acceptance_payload,
        }
        try:
            encoded = json.dumps(forwarded, separators=(",", ":"), ensure_ascii=False)
        except (TypeError, ValueError) as error:
            raise LedgerError("Pairing Acceptance payload is not encodable.") from error
        if len(encoded.encode("utf-8")) > MAX_PAIR_RELAY_ENVELOPE_BYTES:
            raise LedgerError("Pairing Acceptance payload exceeds the size limit.")
        self.active_worker(from_worker_id)
        self.active_worker(str(reservation["leader_worker_id"]))
        return {
            "delivered": True,
            "leader_worker_id": reservation["leader_worker_id"],
            "follower_worker_id": from_worker_id,
            "acceptance": forwarded,
        }

    def create_pair_route(        self, *, proposal_id: str, connected_worker_ids: Any, now: datetime | None = None
    ) -> dict[str, Any]:
        """Phase two: compare-and-swap the durable route into existence.

        This succeeds only while the named reservation is still valid and
        unexpired and both Workers remain eligible under the same per-Worker
        uniqueness constraints.  It generates the route's globally unique,
        never-reused ``route_id``, records the ordered leader/follower pair
        and their assigned roles, and confirms the ``pair_execution_cell``
        execution-mode claim.

        A failed commit releases the reservation entirely: no route, no
        partial role assignment, and no retained execution-mode claim.
        """

        connected = {worker_id for worker_id in connected_worker_ids if isinstance(worker_id, str)}
        moment = now or _utc_now()
        try:
            with self._transaction():
                self._expire_pair_route_reservations_locked(moment)
                reservation = self._connection.execute(
                    """SELECT leader_worker_id, follower_worker_id FROM pair_route_reservations
                       WHERE proposal_id = ?""",
                    [proposal_id],
                ).fetchone()
                if reservation is None:
                    raise LedgerError("The pairing reservation is no longer valid.")
                leader_worker_id, follower_worker_id = str(reservation[0]), str(reservation[1])
                self.active_worker(leader_worker_id)
                self.active_worker(follower_worker_id)
                for worker_id in (leader_worker_id, follower_worker_id):
                    if worker_id not in connected:
                        raise LedgerError(
                            "A Pair Execution Cell pairing requires both Workers to be connected right now."
                        )
                self._require_pair_worker_eligibility_locked(
                    leader_worker_id, follower_worker_id, ignore_proposal_id=proposal_id
                )
                self._claim_pair_execution_mode_locked(
                    leader_worker_id, follower_worker_id, "pair_execution_cell", None
                )
                route_id = self._issue_pair_route_id_locked(moment)
                self._connection.execute(
                    """INSERT INTO pair_routes
                       (route_id, pair_key, leader_worker_id, follower_worker_id, state, created_at, unpairing_at)
                       VALUES (?, ?, ?, ?, ?, ?, NULL)""",
                    [
                        route_id,
                        _pair_key(leader_worker_id, follower_worker_id),
                        leader_worker_id,
                        follower_worker_id,
                        _PAIR_ROUTE_ACTIVE,
                        moment,
                    ],
                )
                self._connection.execute(
                    "DELETE FROM pair_route_reservations WHERE proposal_id = ?", [proposal_id]
                )
                self._event(
                    "pair_route_created",
                    {
                        "route_id": route_id,
                        "proposal_id": proposal_id,
                        "leader_worker_id": leader_worker_id,
                        "follower_worker_id": follower_worker_id,
                        "state": _PAIR_ROUTE_ACTIVE,
                        "owner_kind": "pair_execution_cell",
                    },
                )
        except LedgerError:
            self.release_pair_route_reservation(proposal_id=proposal_id, reason="pairing_commit_failed")
            raise
        return {
            "route_id": route_id,
            "leader_worker_id": leader_worker_id,
            "follower_worker_id": follower_worker_id,
            "state": _PAIR_ROUTE_ACTIVE,
            "created_at": moment,
            "unpairing_at": None,
        }

    def release_pair_route_reservation(self, *, proposal_id: str, reason: str) -> bool:
        """Release one reservation and everything it held.

        A released reservation leaves no route, no partial role assignment,
        and no retained execution-mode claim.  Releasing an already-released
        reservation is a safe no-op.
        """

        with self._transaction():
            return self._release_pair_route_reservation_locked(proposal_id, reason)

    def release_pair_route_reservations_for_worker(self, worker_id: str, reason: str) -> list[str]:
        """Release every live reservation holding this Worker.

        Called when a Worker disconnects between the reservation and the
        final commit: neither the leader nor the follower may be stranded by
        the other's vanishing.
        """

        with self._transaction():
            rows = self._connection.execute(
                """SELECT proposal_id FROM pair_route_reservations
                   WHERE leader_worker_id = ? OR follower_worker_id = ?""",
                [worker_id, worker_id],
            ).fetchall()
            return [
                str(row[0])
                for row in rows
                if self._release_pair_route_reservation_locked(str(row[0]), reason)
            ]

    def expire_pair_route_reservations(self, now: datetime | None = None) -> list[str]:
        """Release every reservation past its 30-second pairing timeout."""

        with self._transaction():
            return self._expire_pair_route_reservations_locked(now or _utc_now())

    # ---- Durable route: identity, authority, reconnect, safe unpair ---- #

    def pair_route(self, route_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._pair_route_locked(route_id)

    def pair_route_for_worker(self, worker_id: str) -> dict[str, Any] | None:
        """This Worker's authoritative route record and its *assigned* role.

        The controller's record -- not the Worker's durable copy and not a
        startup flag -- decides who leads.  A reconnecting Worker resumes the
        role named here and never re-enters the listing or proposal flow.
        """

        with self._lock:
            return self._pair_route_for_worker_locked(worker_id)

    def pair_route_between(self, leader_worker_id: str, follower_worker_id: str) -> dict[str, Any] | None:
        """The live route for this *ordered* leader/follower pair, if any.

        A pair routed in the opposite role order is not a match: the route
        record, not the message, decides who may lead.
        """

        with self._lock:
            row = self._connection.execute(
                "SELECT route_id FROM pair_routes WHERE pair_key = ?",
                [_pair_key(leader_worker_id, follower_worker_id)],
            ).fetchone()
            if row is None:
                return None
            route = self._pair_route_locked(str(row[0]))
        if route is None or route["leader_worker_id"] != leader_worker_id:
            return None
        return route

    def enter_pair_route_unpairing(self, *, route_id: str, worker_id: str) -> dict[str, Any]:
        """Either routed Worker enters the explicit ``UNPAIRING`` state.

        There is no proposer privilege: the follower may initiate exactly as
        the leader may.  Entering takes effect immediately on both sides and
        forbids starting any new attempt, but it never prevents either Worker
        from protecting, reducing, or containing exposure it already owns.
        Any assertion outstanding from an earlier ``UNPAIRING`` episode is
        discarded, so safety is always re-proved from fresh evidence.
        """

        with self._transaction():
            route = self._require_routed_worker_locked(route_id, worker_id)
            if route["state"] != _PAIR_ROUTE_UNPAIRING:
                self._connection.execute(
                    "UPDATE pair_routes SET state = ?, unpairing_at = ? WHERE route_id = ?",
                    [_PAIR_ROUTE_UNPAIRING, _utc_now(), route_id],
                )
            self._connection.execute("DELETE FROM pair_unpair_assertions WHERE route_id = ?", [route_id])
            self._event(
                "pair_route_unpairing_entered",
                {
                    "route_id": route_id,
                    "requested_by": worker_id,
                    "leader_worker_id": route["leader_worker_id"],
                    "follower_worker_id": route["follower_worker_id"],
                },
            )
            updated = self._pair_route_locked(route_id)
        assert updated is not None
        return updated

    def cancel_pair_route_unpairing(self, *, route_id: str, worker_id: str) -> dict[str, Any]:
        """Either routed Worker cancels ``UNPAIRING`` and returns to normal.

        Restored entry readiness is still subject to the ordinary admission
        chain; cancelling only removes the pairing-level block.
        """

        with self._transaction():
            route = self._require_routed_worker_locked(route_id, worker_id)
            if route["state"] != _PAIR_ROUTE_UNPAIRING:
                raise LedgerError("This Pair Execution Cell route is not unpairing.")
            self._connection.execute(
                "UPDATE pair_routes SET state = ?, unpairing_at = NULL WHERE route_id = ?",
                [_PAIR_ROUTE_ACTIVE, route_id],
            )
            self._connection.execute("DELETE FROM pair_unpair_assertions WHERE route_id = ?", [route_id])
            self._event(
                "pair_route_unpairing_cancelled",
                {
                    "route_id": route_id,
                    "requested_by": worker_id,
                    "leader_worker_id": route["leader_worker_id"],
                    "follower_worker_id": route["follower_worker_id"],
                },
            )
            updated = self._pair_route_locked(route_id)
        assert updated is not None
        return updated

    def record_pair_worker_state_version(
        self, *, route_id: str, worker_id: str, state_version: int
    ) -> dict[str, Any]:
        """Record one Worker's current local attempt/effect state version.

        The version advances on any new or changed local attempt or broker
        effect.  Advancing it immediately invalidates that Worker's
        outstanding safe-unpair assertion, which must then be re-produced
        from fresh evidence.  The controller stores and compares the number;
        it never derives emptiness or terminality from it.
        """

        if isinstance(state_version, bool) or not isinstance(state_version, int) or state_version < 0:
            raise LedgerError("A Pair Execution Cell state version must be a non-negative integer.")
        with self._transaction():
            self._require_routed_worker_locked(route_id, worker_id)
            current = self._pair_worker_state_version_locked(route_id, worker_id)
            if current is not None and state_version < current:
                raise LedgerError("A Pair Execution Cell state version never moves backwards.")
            invalidated = self._set_pair_worker_state_version_locked(route_id, worker_id, state_version)
            self._event(
                "pair_route_state_version_recorded",
                {
                    "route_id": route_id,
                    "worker_id": worker_id,
                    "state_version": state_version,
                    "assertion_invalidated": invalidated,
                },
            )
        return {
            "route_id": route_id,
            "worker_id": worker_id,
            "state_version": state_version,
            "assertion_invalidated": invalidated,
        }

    def pair_worker_state_version(self, route_id: str, worker_id: str) -> int | None:
        with self._lock:
            return self._pair_worker_state_version_locked(route_id, worker_id)

    def pair_unpair_assertions(self, route_id: str) -> list[dict[str, Any]]:
        """The currently *fresh* assertions held for this route.

        An assertion bound to a superseded state version is not returned: it
        no longer says anything true about its Worker.
        """

        with self._lock:
            rows = self._connection.execute(
                """SELECT a.worker_id, a.state_version, a.asserted_at, v.state_version
                   FROM pair_unpair_assertions a
                   LEFT JOIN pair_route_worker_state_versions v
                     ON v.route_id = a.route_id AND v.worker_id = a.worker_id
                   WHERE a.route_id = ? ORDER BY a.worker_id""",
                [route_id],
            ).fetchall()
        return [
            {"worker_id": row[0], "state_version": int(row[1]), "asserted_at": row[2]}
            for row in rows
            if row[3] is not None and int(row[3]) == int(row[1])
        ]

    def record_pair_unpair_assertion(
        self, *, route_id: str, worker_id: str, state_version: int
    ) -> dict[str, Any]:
        """Record one Worker's safe-unpair assertion and arbitrate the route.

        The assertion binds to ``route_id`` plus that Worker's current local
        attempt/effect state version, and is valid only while both bindings
        hold.  The route is removed only when the controller holds two fresh
        assertions, one per Worker, both bound to this ``route_id`` and each
        still matching its Worker's current state version.  A stale
        assertion, a one-sided assertion, or one bound to a superseded
        version or a different ``route_id`` does not unpair.

        The controller does not infer, verify, or derive emptiness or
        terminality -- it records two Worker assertions, checks their
        bindings, and arbitrates the route record.
        """

        if isinstance(state_version, bool) or not isinstance(state_version, int) or state_version < 0:
            raise LedgerError("A Pair Execution Cell state version must be a non-negative integer.")
        with self._transaction():
            route = self._require_routed_worker_locked(route_id, worker_id)
            if route["state"] != _PAIR_ROUTE_UNPAIRING:
                raise LedgerError(
                    "A safe-unpair assertion is accepted only while its route is UNPAIRING."
                )
            current = self._pair_worker_state_version_locked(route_id, worker_id)
            if current is not None and state_version < current:
                raise LedgerError(
                    "This safe-unpair assertion is bound to a superseded local state version."
                )
            self._set_pair_worker_state_version_locked(route_id, worker_id, state_version)
            now = _utc_now()
            self._connection.execute(
                """INSERT INTO pair_unpair_assertions (route_id, worker_id, state_version, asserted_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT (route_id, worker_id) DO UPDATE SET
                     state_version = excluded.state_version, asserted_at = excluded.asserted_at""",
                [route_id, worker_id, state_version, now],
            )
            self._event(
                "pair_route_unpair_asserted",
                {"route_id": route_id, "worker_id": worker_id, "state_version": state_version},
            )
            fresh = self._fresh_pair_unpair_assertions_locked(route_id)
            required = {route["leader_worker_id"], route["follower_worker_id"]}
            removed = set(fresh) == required
            if removed:
                self._remove_pair_route_locked(route, reason="safe_unpair")
        return {
            "route_id": route_id,
            "worker_id": worker_id,
            "state_version": state_version,
            "leader_worker_id": route["leader_worker_id"],
            "follower_worker_id": route["follower_worker_id"],
            "asserted_worker_ids": sorted(fresh),
            "route_removed": removed,
        }

    # ---- Quarantine release: authenticated operator action on a route ---- #

    def release_pair_quarantine(
        self, *, route_id: str, symbol: str, actor: str, reason: str
    ) -> dict[str, Any]:
        """Durably audit-log one operator's explicit product quarantine release
        and return the payload the caller must push live to both Workers.

        This stays an authenticated *route-level* operation and confers no
        lifecycle ownership: it is authorized against the same live route
        record as any relayed envelope, names no Trader, and the controller
        neither knows which products are quarantined nor decides whether a
        release may take effect. A product quarantine is Worker-local durable
        state, and a release is a one-time action rather than idempotent
        latest-state: replaying an old release to a Worker that reconnects
        later could incorrectly release a *newer* quarantine of the same
        symbol. So the controller never persists a release for later replay --
        it only records the immutable audit event and returns the payload; the
        caller is responsible for delivering it to both currently-connected
        Worker sessions now.
        """

        with self._transaction():
            route = self._pair_route_locked(route_id)
            if route is None:
                raise LedgerError(
                    "A Pair Execution Cell quarantine release requires a live Worker route."
                )
            self.active_worker(route["leader_worker_id"])
            self.active_worker(route["follower_worker_id"])
            observed_at = _utc_now()
            self._event(
                "pair_quarantine_release_requested",
                {
                    "route_id": route_id,
                    "leader_worker_id": route["leader_worker_id"],
                    "follower_worker_id": route["follower_worker_id"],
                    "symbol": symbol,
                    "actor": actor,
                    "reason": reason,
                },
            )
        return {
            "route_id": route_id,
            "leader_worker_id": route["leader_worker_id"],
            "follower_worker_id": route["follower_worker_id"],
            "symbol": symbol,
            "actor": actor,
            "reason": reason,
            "observed_at": observed_at,
        }

    def record_pair_quarantine_release_outcome(
        self,
        *,
        route_id: str,
        symbol: str,
        actor: str,
        leader_outcome: str,
        follower_outcome: str,
        applied: bool,
    ) -> int:
        """Durably audit-log the delivered per-Worker outcome of one
        quarantine release request, alongside its overall applied/rejected
        result.

        Recorded unconditionally -- whether both Workers genuinely applied
        the release, one or both rejected it (an unresolved attempt), or a
        Worker's outcome could not be confirmed (disconnect/timeout) -- so
        the audit trail always reflects what actually happened rather than
        only ever recording success.
        """

        with self._transaction():
            return self._event(
                "pair_quarantine_release_outcome",
                {
                    "route_id": route_id,
                    "symbol": symbol,
                    "actor": actor,
                    "leader_outcome": leader_outcome,
                    "follower_outcome": follower_outcome,
                    "applied": applied,
                },
            )

    def authorize_pair_relay(self, envelope: dict[str, Any]) -> dict[str, Any]:
        """Authorize one opaque Worker-to-Worker envelope for immediate forwarding.

        Exactly four things are checked: both Workers are authenticated and
        active, the envelope's ``route_id`` names a live route record on
        which the sending Worker genuinely holds the sending role, the
        protocol version is supported, and the encoded envelope is within the
        size limit.  The **route record is authoritative**: a Worker cannot
        promote itself by asserting the roles it wants, and a stale or
        unknown ``route_id`` is refused outright.

        The opaque ``payload`` is never parsed -- policy, quotes, attempts,
        discovered universe content, legs, protection and outcomes are all
        invisible here -- and nothing about it is persisted.  There is no
        lease epoch to fence against, no attempt projection, and no durable
        relay row, so the controller cannot replay an execution envelope into
        a later session and cannot become authoritative evidence for any pair
        lifecycle state.
        """

        if envelope.get("type") != "pair_cell_relay":
            raise LedgerError("Pair relay envelope type is invalid.")
        if set(envelope) != _PAIR_RELAY_ENVELOPE_FIELDS:
            raise LedgerError("Pair relay envelope carries unexpected or missing fields.")
        if envelope.get("protocol_version") != PAIR_CELL_PROTOCOL_VERSION:
            raise LedgerError("Pair relay envelope protocol version is unsupported.")
        route_claim = envelope.get("route")
        if not isinstance(route_claim, dict) or set(route_claim) != _PAIR_ROUTE_CLAIM_FIELDS:
            raise LedgerError("Pair relay envelope route claim is invalid.")
        route_id = route_claim.get("route_id")
        leader_worker_id = route_claim.get("leader_worker_id")
        follower_worker_id = route_claim.get("follower_worker_id")
        from_worker_id, to_worker_id = envelope.get("from_worker_id"), envelope.get("to_worker_id")
        request_id = envelope.get("request_id")
        if (
            not isinstance(route_id, str)
            or not route_id
            or not isinstance(leader_worker_id, str)
            or not isinstance(follower_worker_id, str)
            or from_worker_id not in (leader_worker_id, follower_worker_id)
            or to_worker_id not in (leader_worker_id, follower_worker_id)
            or from_worker_id == to_worker_id
            or not isinstance(request_id, str)
            or not request_id
        ):
            raise LedgerError("Pair relay envelope identity or route is invalid.")
        if (
            len(json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
            > MAX_PAIR_RELAY_ENVELOPE_BYTES
        ):
            raise LedgerError("Pair relay envelope exceeds the size limit.")
        route = self.pair_route(route_id)
        if (
            route is None
            or route["leader_worker_id"] != leader_worker_id
            or route["follower_worker_id"] != follower_worker_id
        ):
            raise LedgerError("Pair relay envelope does not match a live Worker route.")
        self.active_worker(from_worker_id)
        self.active_worker(to_worker_id)
        return {"delivered": True}

    # ---- Pair Execution Cell internals ---- #

    def _pair_route_locked(self, route_id: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            """SELECT route_id, leader_worker_id, follower_worker_id, state, created_at, unpairing_at
               FROM pair_routes WHERE route_id = ?""",
            [route_id],
        ).fetchone()
        if row is None:
            return None
        return {
            "route_id": row[0],
            "leader_worker_id": row[1],
            "follower_worker_id": row[2],
            "state": row[3],
            "created_at": row[4],
            "unpairing_at": row[5],
        }

    def _pair_route_for_worker_locked(self, worker_id: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            "SELECT route_id FROM pair_routes WHERE leader_worker_id = ? OR follower_worker_id = ?",
            [worker_id, worker_id],
        ).fetchone()
        if row is None:
            return None
        route = self._pair_route_locked(str(row[0]))
        if route is None:
            return None
        return {
            **route,
            "role": "leader" if route["leader_worker_id"] == worker_id else "follower",
        }

    def _require_routed_worker_locked(self, route_id: str, worker_id: str) -> dict[str, Any]:
        route = self._pair_route_locked(route_id)
        if route is None:
            raise LedgerError("No live Pair Execution Cell route has this route identifier.")
        if worker_id not in (route["leader_worker_id"], route["follower_worker_id"]):
            raise LedgerError("This Worker is not on that Pair Execution Cell route.")
        return route

    def _pair_routed_worker_ids_locked(self) -> set[str]:
        return {
            str(worker_id)
            for row in self._connection.execute(
                "SELECT leader_worker_id, follower_worker_id FROM pair_routes"
            ).fetchall()
            for worker_id in row
        }

    def _pair_reserved_worker_ids_locked(self) -> set[str]:
        return {
            str(worker_id)
            for row in self._connection.execute(
                "SELECT leader_worker_id, follower_worker_id FROM pair_route_reservations"
            ).fetchall()
            for worker_id in row
        }

    def _require_pair_worker_eligibility_locked(
        self, leader_worker_id: str, follower_worker_id: str, *, ignore_proposal_id: str | None = None
    ) -> None:
        """Per-Worker uniqueness, enforced identically in both transactions.

        Neither Worker may appear in another route, in another live pairing
        reservation, or in a conflicting live execution ownership -- including
        a legacy Strategy Runtime that still holds either Worker's pair.
        """

        pair = (leader_worker_id, follower_worker_id)
        routed = self._pair_routed_worker_ids_locked()
        for worker_id in pair:
            if worker_id in routed:
                raise LedgerError(
                    "This Worker already appears in a Pair Execution Cell route."
                )
        for row in self._connection.execute(
            """SELECT proposal_id, leader_worker_id, follower_worker_id FROM pair_route_reservations"""
        ).fetchall():
            if str(row[0]) == ignore_proposal_id:
                continue
            if leader_worker_id in (row[1], row[2]) or follower_worker_id in (row[1], row[2]):
                raise LedgerError(
                    "This Worker is already reserved by another live pairing proposal."
                )
        pair_key = _pair_key(leader_worker_id, follower_worker_id)
        for row in self._connection.execute(
            """SELECT pair_key, mode FROM pair_execution_owners WHERE mode != 'shadow'"""
        ).fetchall():
            owner_pair, owner_mode = str(row[0]), str(row[1])
            members = set(owner_pair.split("|"))
            if not members & set(pair):
                continue
            if owner_mode == "strategy_runtime":
                raise LedgerError(
                    "The legacy Strategy Runtime still holds this Worker's pair live; "
                    "the legacy Strategy Runtime and a Pair Execution Cell cannot both run for the same pair."
                )
            if owner_pair != pair_key:
                raise LedgerError(
                    "This Worker already holds a Pair Execution Cell execution-mode claim for another pair."
                )

    def _issue_pair_route_id_locked(self, now: datetime) -> str:
        """Generate a globally unique ``route_id`` that was never issued before.

        Every new pairing produces a new one -- not even the same two Workers
        pairing again after a safe unpair may reuse an old identifier -- so
        every identifier ever issued is remembered, including those whose
        routes have since been removed.
        """

        for _ in range(8):
            candidate = str(uuid4())
            if self._connection.execute(
                "SELECT 1 FROM pair_route_identifiers WHERE route_id = ?", [candidate]
            ).fetchone() is None:
                self._connection.execute(
                    "INSERT INTO pair_route_identifiers (route_id, issued_at) VALUES (?, ?)",
                    [candidate, now],
                )
                return candidate
        raise LedgerError("Could not generate an unused Pair Execution Cell route identifier.")

    def _release_pair_route_reservation_locked(self, proposal_id: str, reason: str) -> bool:
        row = self._connection.execute(
            """SELECT leader_worker_id, follower_worker_id FROM pair_route_reservations
               WHERE proposal_id = ?""",
            [proposal_id],
        ).fetchone()
        if row is None:
            return False
        leader_worker_id, follower_worker_id = str(row[0]), str(row[1])
        self._connection.execute(
            "DELETE FROM pair_route_reservations WHERE proposal_id = ?", [proposal_id]
        )
        if self._connection.execute(
            "SELECT 1 FROM pair_routes WHERE pair_key = ?",
            [_pair_key(leader_worker_id, follower_worker_id)],
        ).fetchone() is None:
            self._release_pair_execution_mode_locked(
                leader_worker_id, follower_worker_id, "pair_execution_cell", reason=reason
            )
        self._event(
            "pair_route_reservation_released",
            {
                "proposal_id": proposal_id,
                "leader_worker_id": leader_worker_id,
                "follower_worker_id": follower_worker_id,
                "reason": reason,
            },
        )
        return True

    def _expire_pair_route_reservations_locked(self, now: datetime) -> list[str]:
        expired = [
            str(row[0])
            for row in self._connection.execute(
                "SELECT proposal_id FROM pair_route_reservations WHERE expires_at <= ?", [now]
            ).fetchall()
        ]
        return [
            proposal_id
            for proposal_id in expired
            if self._release_pair_route_reservation_locked(proposal_id, "pairing_reservation_timeout")
        ]

    def _pair_worker_state_version_locked(self, route_id: str, worker_id: str) -> int | None:
        row = self._connection.execute(
            """SELECT state_version FROM pair_route_worker_state_versions
               WHERE route_id = ? AND worker_id = ?""",
            [route_id, worker_id],
        ).fetchone()
        return None if row is None else int(row[0])

    def _set_pair_worker_state_version_locked(
        self, route_id: str, worker_id: str, state_version: int
    ) -> bool:
        current = self._pair_worker_state_version_locked(route_id, worker_id)
        self._connection.execute(
            """INSERT INTO pair_route_worker_state_versions (route_id, worker_id, state_version, observed_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT (route_id, worker_id) DO UPDATE SET
                 state_version = excluded.state_version, observed_at = excluded.observed_at""",
            [route_id, worker_id, state_version, _utc_now()],
        )
        if current is None or state_version == current:
            return False
        invalidated = self._connection.execute(
            """SELECT 1 FROM pair_unpair_assertions
               WHERE route_id = ? AND worker_id = ? AND state_version != ?""",
            [route_id, worker_id, state_version],
        ).fetchone() is not None
        if invalidated:
            self._connection.execute(
                "DELETE FROM pair_unpair_assertions WHERE route_id = ? AND worker_id = ?",
                [route_id, worker_id],
            )
        return invalidated

    def _fresh_pair_unpair_assertions_locked(self, route_id: str) -> set[str]:
        return {
            str(row[0])
            for row in self._connection.execute(
                """SELECT a.worker_id FROM pair_unpair_assertions a
                   JOIN pair_route_worker_state_versions v
                     ON v.route_id = a.route_id AND v.worker_id = a.worker_id
                   WHERE a.route_id = ? AND v.state_version = a.state_version""",
                [route_id],
            ).fetchall()
        }

    def _remove_pair_route_locked(self, route: dict[str, Any], *, reason: str) -> None:
        route_id = str(route["route_id"])
        self._connection.execute("DELETE FROM pair_routes WHERE route_id = ?", [route_id])
        self._connection.execute("DELETE FROM pair_unpair_assertions WHERE route_id = ?", [route_id])
        self._connection.execute(
            "DELETE FROM pair_route_worker_state_versions WHERE route_id = ?", [route_id]
        )
        self._release_pair_execution_mode_locked(
            str(route["leader_worker_id"]),
            str(route["follower_worker_id"]),
            "pair_execution_cell",
            reason=reason,
        )
        self._event(
            "pair_route_removed",
            {
                "route_id": route_id,
                "leader_worker_id": route["leader_worker_id"],
                "follower_worker_id": route["follower_worker_id"],
                "reason": reason,
            },
        )

    def acknowledge_trader_events(
        self, trader_id: str, cursor: int, connection_cursor: int, delivered_event_ids: set[int]
    ) -> None:
        if cursor < connection_cursor or (cursor != connection_cursor and cursor not in delivered_event_ids):
            raise LedgerError("Trader event ACK cursor was not delivered by this session.")
        with self._transaction():
            required = self._connection.execute(
                """SELECT event_id FROM trader_events
                   WHERE trader_id = ? AND event_id > ? AND event_id <= ? ORDER BY event_id""",
                [trader_id, connection_cursor, cursor],
            ).fetchall()
            if any(event_id not in delivered_event_ids for (event_id,) in required):
                raise LedgerError("Trader event ACK cursor would skip an unseen event.")
            self._connection.execute(
                """INSERT INTO trader_event_cursors (trader_id, cursor, acknowledged_at) VALUES (?, ?, ?)
                   ON CONFLICT (trader_id) DO UPDATE SET cursor = greatest(trader_event_cursors.cursor, excluded.cursor),
                       acknowledged_at = excluded.acknowledged_at""",
                [trader_id, cursor, _utc_now()],
            )

    def trader_event_cursor(self, trader_id: str) -> int:
        with self._lock:
            row = self._connection.execute(
                "SELECT cursor FROM trader_event_cursors WHERE trader_id = ?", [trader_id]
            ).fetchone()
        return 0 if row is None else int(row[0])

    def trader_events_after(self, trader_id: str, cursor: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """SELECT e.event_id, e.event_type, e.payload, e.occurred_at FROM trader_events te
                   JOIN events e ON e.event_id = te.event_id
                   WHERE te.trader_id = ? AND e.event_id > ? ORDER BY e.event_id""",
                [trader_id, cursor],
            ).fetchall()
        return [{"event_id": row[0], "event_type": row[1], "payload": json.loads(row[2]), "occurred_at": row[3]} for row in rows]

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

    def enrollment_status(self, enrollment_id: str) -> str:
        with self._lock:
            row = self._connection.execute(
                "SELECT status FROM enrollments WHERE enrollment_id = ?",
                [enrollment_id],
            ).fetchone()
        if row is None:
            raise LedgerError("Enrollment does not exist.")
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

    def rotate_worker_certificate(
        self, worker_id: str, public_key_pem: str, issue_certificate: Callable[[str, int, str, str], str]
    ) -> str:
        """Replace an active Worker's key only after the service verified both proofs."""

        with self._transaction():
            worker = self.active_worker(worker_id)
            certificate = issue_certificate(worker_id, worker.login, worker.server, public_key_pem)
            self._connection.execute(
                """
                INSERT INTO certificate_overlaps (role, identity_id, certificate, public_key_pem, expires_at)
                VALUES ('worker', ?, ?, ?, ?)
                """,
                [worker_id, worker.certificate, worker.public_key_pem, _utc_now() + timedelta(hours=1)],
            )
            self._connection.execute(
                "UPDATE enrollments SET public_key_pem = ? WHERE enrollment_id = ?",
                [public_key_pem, self._worker_enrollment_id(worker_id)],
            )
            self._connection.execute("UPDATE workers SET certificate = ? WHERE worker_id = ?", [certificate, worker_id])
            self._event("worker_certificate_rotated", {"worker_id": worker_id})
            return certificate

    def _worker_enrollment_id(self, worker_id: str) -> str:
        row = self._connection.execute("SELECT enrollment_id FROM workers WHERE worker_id = ?", [worker_id]).fetchone()
        assert row is not None
        return str(row[0])

    def worker_for_certificate(self, worker_id: str, certificate: str) -> ActiveWorker:
        worker = self.active_worker(worker_id)
        if certificate == worker.certificate:
            return worker
        with self._lock:
            row = self._connection.execute(
                """
                SELECT public_key_pem FROM certificate_overlaps
                WHERE role = 'worker' AND identity_id = ? AND certificate = ? AND expires_at > ?
                """,
                [worker_id, certificate, _utc_now()],
            ).fetchone()
        if row is None:
            raise LedgerError("Worker certificate is no longer active.")
        return replace(worker, certificate=certificate, public_key_pem=row[0])

    def _active_worker(self, query: str, parameters: list[str]) -> ActiveWorker:
        with self._lock:
            row = self._connection.execute(query, parameters).fetchone()
        if row is None:
            raise LedgerError("Worker is not active.")
        return ActiveWorker(*row)

    def record_worker_session(self, worker_id: str, session_id: str | None = None) -> None:
        with self._transaction():
            active = self._connection.execute(
                "SELECT 1 FROM workers WHERE worker_id = ? AND status = 'active'", [worker_id]
            ).fetchone()
            if active is None:
                raise LedgerError("Worker is not active.")
            self._connection.execute("UPDATE workers SET last_seen_at = ? WHERE worker_id = ?", [_utc_now(), worker_id])
            payload: dict[str, object] = {"worker_id": worker_id}
            if session_id is not None:
                payload["session_id"] = session_id
            self._event("worker_session_authenticated", payload)

    def record_worker_session_lifecycle(
        self, event_type: str, worker_id: str, session_id: str, **details: object
    ) -> None:
        if event_type not in {"worker_session_superseded", "worker_session_disconnected", "worker_session_failed"}:
            raise LedgerError("Worker session lifecycle event is invalid.")
        if not session_id:
            raise LedgerError("Worker session ID is required.")
        with self._transaction():
            active = self._connection.execute(
                "SELECT 1 FROM workers WHERE worker_id = ? AND status IN ('active', 'revoked')", [worker_id]
            ).fetchone()
            if active is None:
                raise LedgerError("Worker does not exist.")
            self._event(event_type, {"worker_id": worker_id, "session_id": session_id, **details})

    def trader_active_workers(self) -> list[dict[str, Any]]:
        """Return the non-sensitive identities and live status of active Workers."""

        return [
            {
                "worker_id": worker["worker_id"],
                "server": worker["server"],
                "connectivity": worker["connectivity"],
            }
            for worker in self.worker_reconciliation()
            if worker["connectivity"] != "revoked"
        ]

    def record_worker_heartbeat(self, worker_id: str) -> None:
        with self._transaction():
            self.active_worker(worker_id)
            self._connection.execute("UPDATE workers SET last_seen_at = ? WHERE worker_id = ?", [_utc_now(), worker_id])
            self._event("worker_heartbeat_received", {"worker_id": worker_id})

    def revoke_worker(self, worker_id: str, revoked_by: str) -> None:
        with self._transaction():
            active = self._connection.execute(
                "SELECT 1 FROM workers WHERE worker_id = ? AND status = 'active'", [worker_id]
            ).fetchone()
            if active is None:
                raise LedgerError("Worker is not active.")
            self._connection.execute(
                """
                UPDATE workers SET status = 'revoked', revoked_at = ?
                WHERE worker_id = ? AND status = 'active'
                """,
                [_utc_now(), worker_id],
            )
            self._event("worker_certificate_revoked", {"worker_id": worker_id, "revoked_by": revoked_by})
            self._alert(worker_id, "high", "certificate_revoked", "administrator_revocation")

    def worker_reconciliation(self) -> list[dict[str, Any]]:
        now = _utc_now()
        with self._lock:
            workers = self._connection.execute(
                "SELECT worker_id, login, server, status, last_seen_at FROM workers ORDER BY approved_at"
            ).fetchall()
            result = []
            for worker_id, login, server, status, last_seen_at in workers:
                result.append(
                    {
                        "worker_id": worker_id,
                        "login": login,
                        "server": server,
                        "connectivity": (
                            "revoked" if status == "revoked"
                            else "connected" if last_seen_at and now - last_seen_at <= timedelta(minutes=5) else "stale"
                        ),
                        "last_seen_at": last_seen_at,
                    }
                )
        return result

    def alerts(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT alert_id, worker_id, enrollment_id, priority, alert_type, reason, occurred_at
                FROM alerts
                ORDER BY alert_id
                """
            ).fetchall()
        return [
            {"alert_id": row[0], "worker_id": row[1], "enrollment_id": row[2], "priority": row[3], "alert_type": row[4],
             "reason": row[5], "occurred_at": row[6]}
            for row in rows
        ]

    def reconciliation_cursor(self, worker_id: str) -> int:
        with self._lock:
            self.active_worker(worker_id)
            row = self._connection.execute(
                """SELECT event_id FROM worker_relay_facts
                   WHERE worker_id = ? ORDER BY recorded_at DESC LIMIT 1""",
                [worker_id],
            ).fetchone()
            return 0 if row is None else int(row[0])

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
                SELECT enrollment_id, login, server, account_info, terminal_info, created_at, expires_at
                FROM enrollments WHERE status = 'pending' ORDER BY created_at
                """
            ).fetchall()
            return [
                {
                    "enrollment_id": row[0],
                    "login": row[1],
                    "server": row[2],
                    "account_info": json.loads(row[3]),
                    "terminal_info": json.loads(row[4]),
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

    def event_page(
        self, *, limit: int, cursor: str | None, page: int | None, event_type: str | None, query: str | None
    ) -> dict[str, Any]:
        if cursor is not None and page is not None:
            raise LedgerError("Specify either a cursor or a page, not both.")
        cursor_values = _decode_page_cursor(cursor, "events")
        filter_clauses: list[str] = []
        filter_parameters: list[Any] = []
        if event_type is not None:
            filter_clauses.append("event_type = ?")
            filter_parameters.append(event_type)
        if query is not None:
            filter_clauses.append("lower(concat_ws(' ', event_type, CAST(payload AS VARCHAR))) LIKE ?")
            filter_parameters.append(f"%{query.lower()}%")
        clauses = list(filter_clauses)
        parameters = list(filter_parameters)
        if cursor_values is not None:
            clauses.append("(occurred_at < ? OR (occurred_at = ? AND event_id < ?))")
            parameters.extend([cursor_values["timestamp"], cursor_values["timestamp"], cursor_values["id"]])
        where = "" if not clauses else f"WHERE {' AND '.join(clauses)}"
        filter_where = "" if not filter_clauses else f"WHERE {' AND '.join(filter_clauses)}"
        with self._lock:
            total_items = self._connection.execute(
                f"SELECT count(*) FROM events {filter_where}",
                filter_parameters,
            ).fetchone()[0]
            row_limit = limit if page is not None else limit + 1
            query_parameters = [*parameters, row_limit]
            offset = ""
            if page is not None:
                offset = "OFFSET ?"
                query_parameters.append((page - 1) * limit)
            rows = self._connection.execute(
                f"""
                SELECT event_id, event_type, payload, occurred_at FROM events {where}
                ORDER BY occurred_at DESC, event_id DESC
                LIMIT ?
                {offset}
                """,
                query_parameters,
            ).fetchall()
        has_more = page * limit < total_items if page is not None else len(rows) > limit
        items = [
            {"event_id": row[0], "event_type": row[1], "payload": json.loads(row[2]), "occurred_at": row[3]}
            for row in rows[:limit]
        ]
        result = _page(items, has_more, "events", "occurred_at", "event_id")
        result["total_items"] = total_items
        return result

    def _event(self, event_type: str, payload: dict[str, Any]) -> int:
        row = self._connection.execute(
            "INSERT INTO events (event_type, payload, occurred_at) VALUES (?, ?, ?) RETURNING event_id",
            [event_type, json.dumps(payload, sort_keys=True), _utc_now()],
        ).fetchone()
        assert row is not None
        return int(row[0])

    def _alert(
        self,
        worker_id: str | None,
        priority: str,
        alert_type: str,
        reason: str,
        *,
        enrollment_id: str | None = None,
    ) -> None:
        self._connection.execute(
            """
            INSERT INTO alerts (worker_id, enrollment_id, priority, alert_type, reason, occurred_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [worker_id, enrollment_id, priority, alert_type, reason, _utc_now()],
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
                CREATE TABLE IF NOT EXISTS registration_invites (
                    invite_hash VARCHAR PRIMARY KEY,
                    role VARCHAR NOT NULL,
                    issued_by VARCHAR NOT NULL,
                    issued_at TIMESTAMPTZ NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL,
                    status VARCHAR NOT NULL,
                    used_at TIMESTAMPTZ,
                    revoked_at TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS trader_sessions (
                    session_id VARCHAR PRIMARY KEY,
                    trader_id VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    last_valid_signal_at TIMESTAMPTZ NOT NULL,
                    opened_at TIMESTAMPTZ NOT NULL,
                    stale_at TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS trader_commands (
                    trader_id VARCHAR NOT NULL,
                    command_id VARCHAR NOT NULL,
                    payload_hash VARCHAR NOT NULL,
                    result JSON NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (trader_id, command_id)
                );
                CREATE TABLE IF NOT EXISTS trader_worker_requests (
                    trader_id VARCHAR NOT NULL,
                    request_id VARCHAR NOT NULL,
                    worker_id VARCHAR NOT NULL,
                    kind VARCHAR NOT NULL,
                    payload_hash VARCHAR NOT NULL,
                    payload JSON NOT NULL,
                    status VARCHAR NOT NULL,
                    result JSON,
                    requested_event_id BIGINT NOT NULL,
                    completed_event_id BIGINT,
                    requested_at TIMESTAMPTZ NOT NULL,
                    completed_at TIMESTAMPTZ,
                    PRIMARY KEY (trader_id, request_id)
                );
                CREATE TABLE IF NOT EXISTS trader_worker_relay (
                    trader_id VARCHAR NOT NULL,
                    request_id VARCHAR NOT NULL,
                    worker_id VARCHAR NOT NULL,
                    envelope_hash VARCHAR NOT NULL,
                    envelope JSON NOT NULL,
                    status VARCHAR NOT NULL,
                    outcome JSON,
                    requested_event_id BIGINT NOT NULL,
                    completed_event_id BIGINT,
                    requested_at TIMESTAMPTZ NOT NULL,
                    completed_at TIMESTAMPTZ,
                    PRIMARY KEY (trader_id, request_id)
                );
                CREATE TABLE IF NOT EXISTS worker_relay_facts (
                    worker_id VARCHAR NOT NULL,
                    recovery_epoch VARCHAR NOT NULL,
                    event_id BIGINT NOT NULL,
                    envelope_hash VARCHAR NOT NULL,
                    envelope JSON NOT NULL,
                    audit_event_id BIGINT NOT NULL,
                    recorded_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (worker_id, recovery_epoch, event_id)
                );
                CREATE TABLE IF NOT EXISTS trader_worker_fact_cursors (
                    trader_id VARCHAR NOT NULL,
                    worker_id VARCHAR NOT NULL,
                    recovery_epoch VARCHAR NOT NULL,
                    event_id BIGINT NOT NULL,
                    acknowledged_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (trader_id, worker_id)
                );
                CREATE TABLE IF NOT EXISTS trader_events (
                    trader_id VARCHAR NOT NULL,
                    event_id BIGINT PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS trader_event_cursors (
                    trader_id VARCHAR PRIMARY KEY,
                    cursor BIGINT NOT NULL,
                    acknowledged_at TIMESTAMPTZ NOT NULL
                );
                CREATE TABLE IF NOT EXISTS trader_enrollments (
                    registration_id VARCHAR PRIMARY KEY,
                    strategy_name VARCHAR NOT NULL,
                    claimed_public_ip VARCHAR NOT NULL,
                    public_key_pem VARCHAR NOT NULL,
                    attestation_provider VARCHAR NOT NULL DEFAULT 'unverified',
                    status VARCHAR NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL,
                    approved_by VARCHAR,
                    approved_at TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS traders (
                    trader_id VARCHAR PRIMARY KEY,
                    registration_id VARCHAR UNIQUE NOT NULL,
                    strategy_name VARCHAR NOT NULL,
                    certificate VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    approved_at TIMESTAMPTZ NOT NULL,
                    revoked_at TIMESTAMPTZ
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
                    revoked_at TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS certificate_overlaps (
                    role VARCHAR NOT NULL,
                    identity_id VARCHAR NOT NULL,
                    certificate VARCHAR NOT NULL,
                    public_key_pem VARCHAR NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (role, identity_id, certificate)
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
                    worker_id VARCHAR,
                    enrollment_id VARCHAR,
                    priority VARCHAR NOT NULL,
                    alert_type VARCHAR NOT NULL,
                    reason VARCHAR NOT NULL,
                    occurred_at TIMESTAMPTZ NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pair_execution_owners (
                    pair_key VARCHAR NOT NULL,
                    mode VARCHAR NOT NULL,
                    owner_kind VARCHAR,
                    trader_id VARCHAR,
                    claimed_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (pair_key, mode)
                );
                CREATE TABLE IF NOT EXISTS pair_routes (
                    route_id VARCHAR PRIMARY KEY,
                    pair_key VARCHAR UNIQUE NOT NULL,
                    leader_worker_id VARCHAR NOT NULL,
                    follower_worker_id VARCHAR NOT NULL,
                    state VARCHAR NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    unpairing_at TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS pair_route_identifiers (
                    route_id VARCHAR PRIMARY KEY,
                    issued_at TIMESTAMPTZ NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pair_route_reservations (
                    proposal_id VARCHAR PRIMARY KEY,
                    leader_worker_id VARCHAR NOT NULL,
                    follower_worker_id VARCHAR NOT NULL,
                    reserved_at TIMESTAMPTZ NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pair_cell_role_declarations (
                    worker_id VARCHAR PRIMARY KEY,
                    role VARCHAR NOT NULL,
                    declared BOOLEAN NOT NULL,
                    declared_at TIMESTAMPTZ NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pair_route_worker_state_versions (
                    route_id VARCHAR NOT NULL,
                    worker_id VARCHAR NOT NULL,
                    state_version BIGINT NOT NULL,
                    observed_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (route_id, worker_id)
                );
                CREATE TABLE IF NOT EXISTS pair_unpair_assertions (
                    route_id VARCHAR NOT NULL,
                    worker_id VARCHAR NOT NULL,
                    state_version BIGINT NOT NULL,
                    asserted_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (route_id, worker_id)
                );
                """
            )
            # The controller is an authenticated opaque relay: it no longer
            # issues Pair Leases, activates Pair Contracts, or keeps an
            # authoritative attempt projection to replay, so their tables are
            # dropped rather than left as stale execution evidence.
            self._connection.execute(
                """
                DROP TABLE IF EXISTS pair_leases;
                DROP TABLE IF EXISTS pair_contracts;
                DROP TABLE IF EXISTS pair_relay;
                """
            )
            self._migrate_pair_execution_cell_schema()
            self._connection.execute("ALTER TABLE enrollments ADD COLUMN IF NOT EXISTS registration_invite_hash VARCHAR")
            enrollment_columns = {
                row[1] for row in self._connection.execute("PRAGMA table_info('enrollments')").fetchall()
            }
            if "status" in enrollment_columns:
                self._connection.execute(
                    """
                    UPDATE enrollments SET status = 'expired'
                    WHERE status = 'pending' AND registration_invite_hash IS NULL
                    """
                )
            self._connection.execute("ALTER TABLE alerts DROP COLUMN IF EXISTS product_pair_id")
            self._connection.execute("ALTER TABLE enrollments DROP COLUMN IF EXISTS pairing_code")
            self._connection.execute(
                "ALTER TABLE enrollments ADD COLUMN IF NOT EXISTS password_secret_deleted_at TIMESTAMPTZ"
            )
            self._connection.execute("ALTER TABLE workers ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ")
            self._connection.execute("ALTER TABLE workers ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ")
            self._connection.execute("ALTER TABLE workers DROP COLUMN IF EXISTS safety_state")
            for table in (
                "worker_freezes",
                "manual_trading_target",
                "manual_trades",
                "manual_trade_commands",
                "manual_trade_operations",
                "manual_trade_operation_commands",
                "worker_recovery_commands",
                "trader_broker_tickets",
                "trader_hedged_entry_legs",
                "trader_ticket_transitions",
                "management_intent_commands",
                "trader_intents",
                "trader_intent_execution_records",
                "reconciliation_snapshots",
                "reconciliation_deltas",
                "account_recoveries",
                "recovery_pairs",
                "recovery_pair_observations",
                "worker_recovery_syncs",
                "worker_live_state",
                "product_catalog_analyses",
                "product_pair_build_confirmations",
                "product_pairs",
                "product_pair_worker_compatibility_checks",
                "product_pair_worker_exclusions",
                "product_pair_retests",
            ):
                self._connection.execute(f"DROP TABLE IF EXISTS {table}")
            self._connection.execute(
                "ALTER TABLE trader_enrollments ADD COLUMN IF NOT EXISTS approved_by VARCHAR"
            )
            self._connection.execute(
                "ALTER TABLE trader_enrollments ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ"
            )
            self._connection.execute(
                "ALTER TABLE trader_enrollments ADD COLUMN IF NOT EXISTS attestation_provider VARCHAR DEFAULT 'unverified'"
            )
            self._migrate_alert_enrollment_reference()

    def _table_columns(self, table_name: str) -> set[str]:
        return {
            str(row[0])
            for row in self._connection.execute(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema = 'main' AND table_name = ?""",
                [table_name],
            ).fetchall()
        }

    def _migrate_pair_execution_cell_schema(self) -> None:
        """Carry an existing ledger onto the Worker-initiated pairing schema.

        Two shapes change. ``pair_execution_owners`` gains an explicit
        ``owner_kind`` and loses its mandatory ``trader_id``, because a Pair
        Execution Cell claim carries the fixed owner kind and no Trader
        identity while the legacy Strategy Runtime keeps its own. And
        ``pair_routes`` moves from an administrator-created, ``trader_id``-
        bound row keyed by pair to a Worker-formed route keyed by a
        controller-generated ``route_id`` with an explicit
        ``ACTIVE``/``UNPAIRING`` state.

        Existing pairings are preserved rather than dropped: each legacy row
        keeps its ordered leader/follower Workers, is issued a fresh globally
        unique ``route_id`` recorded in the never-reuse register, and starts
        ``ACTIVE`` with its Trader binding discarded. Nothing here touches a
        Worker's own durable state, so a migrated Worker reconciles its route
        metadata on reconnect exactly as it would after any disagreement.
        """

        owner_columns = self._table_columns("pair_execution_owners")
        if "owner_kind" not in owner_columns:
            self._connection.execute("ALTER TABLE pair_execution_owners ADD COLUMN owner_kind VARCHAR")
        self._connection.execute(
            "UPDATE pair_execution_owners SET owner_kind = mode WHERE owner_kind IS NULL"
        )
        trader_nullability = self._connection.execute(
            """SELECT is_nullable FROM information_schema.columns
               WHERE table_schema = 'main' AND table_name = 'pair_execution_owners'
                 AND column_name = 'trader_id'""",
        ).fetchone()
        if trader_nullability is not None and str(trader_nullability[0]).upper() == "NO":
            self._connection.execute("ALTER TABLE pair_execution_owners ALTER trader_id DROP NOT NULL")

        route_columns = self._table_columns("pair_routes")
        if not route_columns or "route_id" in route_columns:
            self._drop_orphaned_pair_cell_claims()
            return
        legacy = self._connection.execute(
            "SELECT pair_key, leader_worker_id, follower_worker_id FROM pair_routes"
        ).fetchall()
        self._connection.execute("DROP TABLE pair_routes")
        self._connection.execute(
            """
            CREATE TABLE pair_routes (
                route_id VARCHAR PRIMARY KEY,
                pair_key VARCHAR UNIQUE NOT NULL,
                leader_worker_id VARCHAR NOT NULL,
                follower_worker_id VARCHAR NOT NULL,
                state VARCHAR NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                unpairing_at TIMESTAMPTZ
            );
            """
        )
        migrated_at = _utc_now()
        for pair_key, leader_worker_id, follower_worker_id in legacy:
            route_id = self._issue_pair_route_id_locked(migrated_at)
            self._connection.execute(
                """INSERT INTO pair_routes
                   (route_id, pair_key, leader_worker_id, follower_worker_id, state, created_at, unpairing_at)
                   VALUES (?, ?, ?, ?, ?, ?, NULL)""",
                [route_id, pair_key, leader_worker_id, follower_worker_id, _PAIR_ROUTE_ACTIVE, migrated_at],
            )
        self._drop_orphaned_pair_cell_claims()

    def _drop_orphaned_pair_cell_claims(self) -> None:
        """Strip the Trader binding from Pair Cell claims and drop stray ones.

        A ``pair_execution_cell`` claim now carries the fixed owner kind and
        no Trader identity, and exists only for the lifetime of a reservation
        or a route.  Any claim left behind by an administrator-created route
        would otherwise permanently block those two Workers from pairing.
        """

        self._connection.execute(
            "UPDATE pair_execution_owners SET trader_id = NULL WHERE mode = 'pair_execution_cell'"
        )
        self._connection.execute(
            """DELETE FROM pair_execution_owners WHERE mode = 'pair_execution_cell'
               AND pair_key NOT IN (SELECT pair_key FROM pair_routes)"""
        )

    def _migrate_alert_enrollment_reference(self) -> None:
        columns = {
            row[1]
            for row in self._connection.execute("PRAGMA table_info('alerts')").fetchall()
        }
        if "enrollment_id" not in columns:
            self._connection.execute("ALTER TABLE alerts ADD COLUMN enrollment_id VARCHAR")
        worker_column = next(
            row for row in self._connection.execute("PRAGMA table_info('alerts')").fetchall() if row[1] == "worker_id"
        )
        if worker_column[3]:
            self._connection.execute("ALTER TABLE alerts ALTER worker_id DROP NOT NULL")

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


def _page(
    items: list[dict[str, Any]], has_more: bool, resource: str, timestamp_key: str, identifier_key: str
) -> dict[str, Any]:
    next_cursor = None
    if has_more and items:
        next_cursor = _encode_page_cursor(resource, items[-1][timestamp_key], items[-1][identifier_key])
    return {"items": items, "next_cursor": next_cursor}


def _encode_page_cursor(resource: str, timestamp: datetime, identifier: str | int) -> str:
    payload = json.dumps(
        {"resource": resource, "timestamp": timestamp.isoformat(), "id": identifier},
        separators=(",", ":"),
    ).encode("utf-8")
    return urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_page_cursor(cursor: str | None, resource: str) -> dict[str, Any] | None:
    if cursor is None:
        return None
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        payload = json.loads(urlsafe_b64decode(padded.encode("ascii")))
        timestamp = datetime.fromisoformat(payload["timestamp"])
        identifier = payload["id"]
        if payload["resource"] != resource or timestamp.tzinfo is None or not isinstance(identifier, (str, int)):
            raise ValueError
    except (Base64Error, KeyError, TypeError, ValueError, UnicodeDecodeError):
        raise LedgerError("Invalid pagination cursor.") from None
    return {"timestamp": timestamp, "id": identifier}





def _summary_string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _summary_number(value: object) -> int | float | None:
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _summary_boolean(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _pair_key(leader_worker_id: str, follower_worker_id: str) -> str:
    """A canonical, order-independent key identifying one Worker pair."""

    return "|".join(sorted((leader_worker_id, follower_worker_id)))


def _utc_now() -> datetime:
    return datetime.now(UTC)
