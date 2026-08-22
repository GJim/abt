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

    def submit_trader_command(self, trader_id: str, command_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Persist idempotency and the minimal accepted/cancel lifecycle event."""

        canonical_payload = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        payload_hash = _hash(canonical_payload)
        command_type = payload.get("type")
        if command_type not in {"intent", "cancel"}:
            raise LedgerError("Trader command type is invalid.")
        with self._transaction():
            if self._connection.execute(
                "SELECT 1 FROM traders WHERE trader_id = ? AND status = 'active'", [trader_id]
            ).fetchone() is None:
                raise LedgerError("Trader is not active.")
            existing = self._connection.execute(
                "SELECT payload_hash, result FROM trader_commands WHERE trader_id = ? AND command_id = ?",
                [trader_id, command_id],
            ).fetchone()
            if existing is not None:
                if existing[0] != payload_hash:
                    raise LedgerError("Trader command ID was reused with a different payload.")
                return json.loads(existing[1])
            event_type = "intent_accepted" if command_type == "intent" else "cancel_accepted"
            event_id = self._event(event_type, {"trader_id": trader_id, "command_id": command_id, "command": payload})
            self._connection.execute(
                "INSERT INTO trader_events (trader_id, event_id) VALUES (?, ?)", [trader_id, event_id]
            )
            result = {"type": "command_result", "command_id": command_id, "status": "accepted", "event_id": event_id}
            self._connection.execute(
                "INSERT INTO trader_commands (trader_id, command_id, payload_hash, result, created_at) VALUES (?, ?, ?, ?, ?)",
                [trader_id, command_id, payload_hash, json.dumps(result, sort_keys=True), _utc_now()],
            )
            return result

    def accept_trader_intent(
        self,
        trader_id: str,
        command_id: str,
        payload: dict[str, Any],
        preflight: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Persist an executable intent only after both broker checks succeeded."""

        canonical_payload = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        payload_hash = _hash(canonical_payload)
        if payload.get("type") != "intent":
            raise LedgerError("Trader command type is invalid.")
        with self._transaction():
            if self._connection.execute(
                "SELECT 1 FROM traders WHERE trader_id = ? AND status = 'active'", [trader_id]
            ).fetchone() is None:
                raise LedgerError("Trader is not active.")
            existing = self._connection.execute(
                "SELECT payload_hash, result FROM trader_commands WHERE trader_id = ? AND command_id = ?",
                [trader_id, command_id],
            ).fetchone()
            if existing is not None:
                if existing[0] != payload_hash:
                    raise LedgerError("Trader command ID was reused with a different payload.")
                return json.loads(existing[1])

            intent_id = str(uuid4())
            self._connection.execute(
                """
                INSERT INTO trader_intents (intent_id, trader_id, command_id, pair_id, intent, preflight, status, accepted_at)
                VALUES (?, ?, ?, ?, ?, ?, 'accepted', ?)
                """,
                [
                    intent_id,
                    trader_id,
                    command_id,
                    payload["pair_id"],
                    canonical_payload,
                    json.dumps(preflight, separators=(",", ":"), sort_keys=True),
                    _utc_now(),
                ],
            )
            event_id = self._event(
                "intent_accepted",
                {
                    "trader_id": trader_id,
                    "command_id": command_id,
                    "intent_id": intent_id,
                    "command": payload,
                    "preflight": preflight,
                    "preflight_notice": "Broker order checks do not reserve liquidity or guarantee later execution.",
                },
            )
            self._connection.execute(
                "INSERT INTO trader_events (trader_id, event_id) VALUES (?, ?)", [trader_id, event_id]
            )
            result = {
                "type": "command_result",
                "command_id": command_id,
                "status": "accepted",
                "intent_id": intent_id,
                "event_id": event_id,
            }
            self._connection.execute(
                "INSERT INTO trader_commands (trader_id, command_id, payload_hash, result, created_at) VALUES (?, ?, ?, ?, ?)",
                [trader_id, command_id, payload_hash, json.dumps(result, sort_keys=True), _utc_now()],
            )
            return result

    def reject_trader_command(
        self, trader_id: str, command_id: str, payload: dict[str, Any], reason: str, preflight: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Durably record a non-executable intent without ever accepting it."""

        canonical_payload = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        payload_hash = _hash(canonical_payload)
        with self._transaction():
            existing = self._connection.execute(
                "SELECT payload_hash, result FROM trader_commands WHERE trader_id = ? AND command_id = ?",
                [trader_id, command_id],
            ).fetchone()
            if existing is not None:
                if existing[0] != payload_hash:
                    raise LedgerError("Trader command ID was reused with a different payload.")
                return json.loads(existing[1])
            event_id = self._event(
                "rejected_preflight",
                {"trader_id": trader_id, "command_id": command_id, "command": payload, "reason": reason, "preflight": preflight},
            )
            self._connection.execute("INSERT INTO trader_events (trader_id, event_id) VALUES (?, ?)", [trader_id, event_id])
            result = {"type": "command_result", "command_id": command_id, "status": "rejected_preflight", "event_id": event_id}
            self._connection.execute(
                "INSERT INTO trader_commands (trader_id, command_id, payload_hash, result, created_at) VALUES (?, ?, ?, ?, ?)",
                [trader_id, command_id, payload_hash, json.dumps(result, sort_keys=True), _utc_now()],
            )
            return result

    def management_command_result(self, username: str, command_id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        payload_hash = _hash(json.dumps(payload, separators=(",", ":"), sort_keys=True))
        with self._lock:
            existing = self._connection.execute(
                "SELECT payload_hash, result FROM management_intent_commands WHERE username = ? AND command_id = ?",
                [username, command_id],
            ).fetchone()
        if existing is None:
            return None
        if existing[0] != payload_hash:
            raise LedgerError("Management command ID was reused with a different payload.")
        return json.loads(existing[1])

    def accept_management_intent(
        self, username: str, command_id: str, payload: dict[str, Any], preflight: list[dict[str, Any]]
    ) -> dict[str, Any]:
        return self._record_management_intent(username, command_id, payload, preflight, reason=None)

    def reject_management_intent(
        self, username: str, command_id: str, payload: dict[str, Any], reason: str, preflight: list[dict[str, Any]]
    ) -> dict[str, Any]:
        return self._record_management_intent(username, command_id, payload, preflight, reason=reason)

    def _record_management_intent(
        self, username: str, command_id: str, payload: dict[str, Any], preflight: list[dict[str, Any]], *, reason: str | None
    ) -> dict[str, Any]:
        canonical_payload = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        payload_hash = _hash(canonical_payload)
        with self._transaction():
            existing = self._connection.execute(
                "SELECT payload_hash, result FROM management_intent_commands WHERE username = ? AND command_id = ?",
                [username, command_id],
            ).fetchone()
            if existing is not None:
                if existing[0] != payload_hash:
                    raise LedgerError("Management command ID was reused with a different payload.")
                return json.loads(existing[1])
            event_type = "rejected_preflight" if reason is not None else "intent_accepted"
            event_id = self._event(
                event_type,
                {"username": username, "command_id": command_id, "command": payload, "reason": reason, "preflight": preflight},
            )
            result: dict[str, Any] = {
                "command_id": command_id,
                "status": "rejected_preflight" if reason is not None else "accepted",
                "event_id": event_id,
            }
            if reason is not None:
                result["reason"] = reason
                result["preflight"] = preflight
            if reason is None:
                intent_id = str(uuid4())
                self._connection.execute(
                    """INSERT INTO trader_intents (intent_id, trader_id, command_id, pair_id, intent, preflight, status, accepted_at)
                       VALUES (?, ?, ?, ?, ?, ?, 'accepted', ?)""",
                    [intent_id, f"management:{username}", command_id, payload["pair_id"], canonical_payload,
                     json.dumps(preflight, separators=(",", ":"), sort_keys=True), _utc_now()],
                )
                result["intent_id"] = intent_id
            self._connection.execute(
                """INSERT INTO management_intent_commands (username, command_id, payload_hash, result, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [username, command_id, payload_hash, json.dumps(result, sort_keys=True), _utc_now()],
            )
            return result

    def request_trader_intent_cancellation(
        self, trader_id: str, command_id: str, payload: dict[str, Any]
    ) -> tuple[dict[str, Any], list[dict[str, Any]], bool]:
        """Atomically suppress undispatched work before scheduling a Trader's cancellation."""

        if payload.get("type") != "cancel" or not isinstance(payload.get("intent_id"), str):
            raise LedgerError("Trader cancellation command is invalid.")
        canonical_payload = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        payload_hash = _hash(canonical_payload)
        intent_id = payload["intent_id"]
        with self._transaction():
            existing = self._connection.execute(
                "SELECT payload_hash, result FROM trader_commands WHERE trader_id = ? AND command_id = ?",
                [trader_id, command_id],
            ).fetchone()
            if existing is not None:
                if existing[0] != payload_hash:
                    raise LedgerError("Trader command ID was reused with a different payload.")
                result = json.loads(existing[1])
                return result, self._intent_preflight(intent_id), False
            intent = self._connection.execute(
                """SELECT preflight, status FROM trader_intents
                   WHERE intent_id = ? AND trader_id = ?""",
                [intent_id, trader_id],
            ).fetchone()
            if intent is None:
                raise LedgerError("Trader cannot cancel an intent it does not own.")
            if intent[1] not in {"accepted", "dispatching", "working"}:
                raise LedgerError("Intent is not eligible for ordinary cancellation.")
            self._connection.execute(
                "UPDATE trader_intents SET status = 'cancelling' WHERE intent_id = ?", [intent_id]
            )
            event_id = self._event(
                "cancellation_scheduled",
                {"trader_id": trader_id, "command_id": command_id, "intent_id": intent_id},
            )
            self._connection.execute(
                "INSERT INTO trader_events (trader_id, event_id) VALUES (?, ?)", [trader_id, event_id]
            )
            self._connection.execute(
                """INSERT INTO trader_intent_execution_records (intent_id, event_id, event_type, payload, recorded_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [intent_id, event_id, "cancellation_scheduled", "{}", _utc_now()],
            )
            result = {
                "type": "command_result",
                "command_id": command_id,
                "status": "cancellation_scheduled",
                "intent_id": intent_id,
                "event_id": event_id,
            }
            self._connection.execute(
                "INSERT INTO trader_commands (trader_id, command_id, payload_hash, result, created_at) VALUES (?, ?, ?, ?, ?)",
                [trader_id, command_id, payload_hash, json.dumps(result, sort_keys=True), _utc_now()],
            )
            return result, json.loads(intent[0]), True

    def request_management_intent_operation(
        self, username: str, command_id: str, intent_id: str, operation: str
    ) -> tuple[dict[str, Any], list[dict[str, Any]], bool]:
        """Durably schedule an administrator cancellation of a zero-fill intent."""

        if operation != "cancel":
            raise LedgerError("Management intent operation is invalid.")
        payload = {"intent_id": intent_id, "type": operation}
        payload_hash = _hash(json.dumps(payload, separators=(",", ":"), sort_keys=True))
        with self._transaction():
            existing = self._connection.execute(
                "SELECT payload_hash, result FROM management_intent_commands WHERE username = ? AND command_id = ?",
                [username, command_id],
            ).fetchone()
            if existing is not None:
                if existing[0] != payload_hash:
                    raise LedgerError("Management command ID was reused with a different payload.")
                return json.loads(existing[1]), self._intent_preflight(intent_id), False
            intent = self._connection.execute(
                "SELECT trader_id, preflight, status FROM trader_intents WHERE intent_id = ?", [intent_id]
            ).fetchone()
            if intent is None:
                raise LedgerError("Intent does not exist.")
            has_fill = self._intent_has_fill(intent_id)
            if operation == "cancel" and (has_fill or intent[2] not in {"accepted", "dispatching", "working"}):
                raise LedgerError("Only zero-fill intents are eligible for ordinary cancellation.")
            self._connection.execute(
                "UPDATE trader_intents SET status = 'cancelling' WHERE intent_id = ?", [intent_id]
            )
            event_type = "cancellation_scheduled"
            event_id = self._event(
                event_type, {"username": username, "command_id": command_id, "intent_id": intent_id}
            )
            self._connection.execute(
                "INSERT INTO trader_events (trader_id, event_id) VALUES (?, ?)", [intent[0], event_id]
            )
            self._connection.execute(
                """INSERT INTO trader_intent_execution_records (intent_id, event_id, event_type, payload, recorded_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [intent_id, event_id, event_type, "{}", _utc_now()],
            )
            result = {
                "command_id": command_id,
                "status": "cancellation_scheduled",
                "intent_id": intent_id,
                "event_id": event_id,
            }
            self._connection.execute(
                """INSERT INTO management_intent_commands (username, command_id, payload_hash, result, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [username, command_id, payload_hash, json.dumps(result, sort_keys=True), _utc_now()],
            )
            return result, json.loads(intent[1]), True

    def _intent_has_fill(self, intent_id: str) -> bool:
        row = self._connection.execute(
            """SELECT 1 FROM trader_intent_execution_records
               WHERE intent_id = ? AND event_type LIKE '%fill%' LIMIT 1""",
            [intent_id],
        ).fetchone()
        return row is not None

    def claim_scheduled_intent_operation(self, intent_id: str) -> bool:
        """Ensure exactly one process executes a persisted cancellation request."""

        with self._transaction():
            claimed = self._connection.execute(
                """UPDATE trader_intents SET status = 'cancellation_executing'
                   WHERE intent_id = ? AND status = 'cancelling'
                   RETURNING intent_id""",
                [intent_id],
            ).fetchone()
            return claimed is not None

    def _intent_preflight(self, intent_id: str) -> list[dict[str, Any]]:
        row = self._connection.execute(
            "SELECT preflight FROM trader_intents WHERE intent_id = ?", [intent_id]
        ).fetchone()
        if row is None:
            raise LedgerError("Intent does not exist.")
        return json.loads(row[0])

    def record_intent_execution(
        self,
        intent_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        status: str | None = None,
    ) -> int:
        """Append an immutable broker-observable lifecycle record for an accepted intent."""

        with self._transaction():
            row = self._connection.execute(
                "SELECT trader_id FROM trader_intents WHERE intent_id = ?", [intent_id]
            ).fetchone()
            if row is None:
                raise LedgerError("Intent does not exist.")
            trader_id = str(row[0])
            event_id = self._event(event_type, {"intent_id": intent_id, **payload})
            self._connection.execute(
                """INSERT INTO trader_intent_execution_records (intent_id, event_id, event_type, payload, recorded_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [intent_id, event_id, event_type, json.dumps(payload, separators=(",", ":"), sort_keys=True), _utc_now()],
            )
            self._connection.execute(
                "INSERT INTO trader_events (trader_id, event_id) VALUES (?, ?)", [trader_id, event_id]
            )
            if status is not None:
                self._connection.execute(
                    "UPDATE trader_intents SET status = ? WHERE intent_id = ?", [status, intent_id]
                )
            return event_id

    def intent_record(self, intent_id: str) -> dict[str, Any]:
        """Return the complete append-only execution record for management review."""

        with self._lock:
            intent = self._connection.execute(
                """SELECT intent_id, trader_id, command_id, pair_id, intent, preflight, status, accepted_at
                   FROM trader_intents WHERE intent_id = ?""",
                [intent_id],
            ).fetchone()
            if intent is None:
                raise LedgerError("Intent does not exist.")
            records = self._connection.execute(
                """SELECT r.event_id, r.event_type, r.payload, r.recorded_at, e.occurred_at
                   FROM trader_intent_execution_records r
                   JOIN events e ON e.event_id = r.event_id
                   WHERE r.intent_id = ?
                   ORDER BY r.event_id""",
                [intent_id],
            ).fetchall()
        return {
            "intent_id": intent[0],
            "trader_id": intent[1],
            "command_id": intent[2],
            "pair_id": intent[3],
            "intent": json.loads(intent[4]),
            "preflight": json.loads(intent[5]),
            "status": intent[6],
            "accepted_at": intent[7],
            "execution_records": [
                {
                    "event_id": row[0],
                    "event_type": row[1],
                    "payload": json.loads(row[2]),
                    "recorded_at": row[3],
                    "occurred_at": row[4],
                }
                for row in records
            ],
        }

    def intents(self, *, active_only: bool, status_filter: str | None) -> list[dict[str, Any]]:
        statuses = {"accepted", "dispatching", "working", "cancelling", "cancellation_executing", "needs_human"}
        with self._lock:
            rows = self._connection.execute(
                """SELECT i.intent_id, i.trader_id, i.command_id, i.pair_id, i.intent, i.status, i.accepted_at,
                          EXISTS (SELECT 1 FROM trader_intent_execution_records r
                                  WHERE r.intent_id = i.intent_id AND r.event_type LIKE '%fill%')
                   FROM trader_intents i
                   ORDER BY i.accepted_at DESC"""
            ).fetchall()
        items = [
            {
                "intent_id": row[0],
                "origin": "management" if str(row[1]).startswith("management:") else "trader",
                "originator": str(row[1]).removeprefix("management:"),
                "command_id": row[2],
                "pair_id": row[3],
                "intent": json.loads(row[4]),
                "status": row[5],
                "accepted_at": row[6],
                "has_fill": bool(row[7]),
            }
            for row in rows
        ]
        if status_filter is not None:
            return [item for item in items if item["status"] == status_filter]
        return [item for item in items if item["status"] in statuses] if active_only else items

    def claim_accepted_intent_dispatch(self, intent_id: str) -> bool:
        """Atomically reserve an accepted intent for one dispatcher."""

        with self._transaction():
            claimed = self._connection.execute(
                """UPDATE trader_intents SET status = 'dispatching'
                   WHERE intent_id = ? AND status = 'accepted'
                   RETURNING intent_id""",
                [intent_id],
            ).fetchone()
            return claimed is not None

    def accepted_intents_for_dispatch(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """SELECT intent_id, intent, preflight, status FROM trader_intents
                   WHERE status IN ('accepted', 'dispatching', 'cancelling') ORDER BY accepted_at"""
            ).fetchall()
        return [
            {"intent_id": row[0], "intent": json.loads(row[1]), "preflight": json.loads(row[2]), "status": row[3]}
            for row in rows
        ]

    def workers_reconciled_since(self, worker_ids: list[str], started_at: datetime) -> bool:
        if not worker_ids:
            return False
        placeholders = ", ".join("?" for _ in worker_ids)
        with self._lock:
            rows = self._connection.execute(
                f"""SELECT DISTINCT worker_id FROM reconciliation_snapshots
                    WHERE worker_id IN ({placeholders}) AND received_at >= ?""",
                [*worker_ids, started_at],
            ).fetchall()
        return {str(row[0]) for row in rows} == set(worker_ids)

    def trader_command_result(self, trader_id: str, command_id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        payload_hash = _hash(json.dumps(payload, separators=(",", ":"), sort_keys=True))
        with self._lock:
            existing = self._connection.execute(
                "SELECT payload_hash, result FROM trader_commands WHERE trader_id = ? AND command_id = ?",
                [trader_id, command_id],
            ).fetchone()
        if existing is None:
            return None
        if existing[0] != payload_hash:
            raise LedgerError("Trader command ID was reused with a different payload.")
        return json.loads(existing[1])

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

    def record_live_state(
        self,
        worker_id: str,
        observed_at: str,
        connectivity: bool,
        quotes: list[object],
        orders: list[object],
        positions: list[object],
    ) -> None:
        with self._transaction():
            self.active_worker(worker_id)
            received_at = _utc_now()
            quoted = [_quote_with_receipt(quote, received_at) for quote in quotes]
            self._connection.execute(
                """
                INSERT INTO worker_live_state
                    (worker_id, observed_at, connectivity, quotes, orders, positions, received_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (worker_id) DO UPDATE SET
                    observed_at = excluded.observed_at, connectivity = excluded.connectivity, quotes = excluded.quotes,
                    orders = excluded.orders, positions = excluded.positions, received_at = excluded.received_at
                """,
                [
                    worker_id, observed_at, connectivity, json.dumps(quoted, sort_keys=True), json.dumps(orders, sort_keys=True),
                    json.dumps(positions, sort_keys=True), received_at,
                ],
            )
            self._connection.execute("UPDATE workers SET last_seen_at = ? WHERE worker_id = ?", [received_at, worker_id])
            self._event("worker_live_state_snapshot", {"worker_id": worker_id})

    def record_live_state_diff(self, worker_id: str, observed_at: str, entity: str, value: object) -> None:
        if entity not in {"connectivity", "quotes", "orders", "positions"}:
            raise LedgerError("Worker live-state entity is invalid.")
        with self._transaction():
            current = self._connection.execute(
                "SELECT connectivity, quotes, orders, positions FROM worker_live_state WHERE worker_id = ?", [worker_id]
            ).fetchone()
            if current is None:
                raise LedgerError("Worker live-state snapshot is required before a diff.")
            state = {
                "connectivity": bool(current[0]),
                "quotes": json.loads(current[1]),
                "orders": json.loads(current[2]),
                "positions": json.loads(current[3]),
            }
            state[entity] = value
            received_at = _utc_now()
            if entity == "quotes":
                state["quotes"] = [_quote_with_receipt(quote, received_at) for quote in value]
            self._connection.execute(
                """UPDATE worker_live_state
                   SET observed_at = ?, connectivity = ?, quotes = ?, orders = ?, positions = ?, received_at = ?
                   WHERE worker_id = ?""",
                [
                    observed_at, state["connectivity"], json.dumps(state["quotes"], sort_keys=True),
                    json.dumps(state["orders"], sort_keys=True), json.dumps(state["positions"], sort_keys=True),
                    received_at, worker_id,
                ],
            )
            self._connection.execute("UPDATE workers SET last_seen_at = ? WHERE worker_id = ?", [received_at, worker_id])
            self._event("worker_live_state_diff", {"worker_id": worker_id, "entity": entity})

    def record_worker_heartbeat(self, worker_id: str) -> None:
        with self._transaction():
            self.active_worker(worker_id)
            self._connection.execute("UPDATE workers SET last_seen_at = ? WHERE worker_id = ?", [_utc_now(), worker_id])
            self._event("worker_heartbeat_received", {"worker_id": worker_id})

    def record_worker_safety_state(self, worker_id: str, state: str, reason: str) -> None:
        if state not in {"connected", "lost_link_safety", "needs_human"}:
            raise LedgerError("Worker safety state is invalid.")
        if state in {"lost_link_safety", "needs_human"}:
            self.freeze_worker_and_active_counterparts(
                worker_id,
                "worker_safety_state",
                {"reason": reason, "reported_state": state},
            )
            return
        with self._transaction():
            self.active_worker(worker_id)
            current = self._connection.execute(
                "SELECT safety_state FROM workers WHERE worker_id = ?", [worker_id]
            ).fetchone()
            if current is not None and current[0] == "frozen":
                self._event(
                    "worker_safety_state_ignored",
                    {"worker_id": worker_id, "state": state, "reason": reason},
                )
                return
            self._connection.execute(
                "UPDATE workers SET safety_state = ?, last_seen_at = ? WHERE worker_id = ?",
                [state, _utc_now(), worker_id],
            )
            self._event("worker_safety_state_changed", {"worker_id": worker_id, "state": state, "reason": reason})

    def freeze_workers(
        self,
        source: str,
        affected_worker_ids: list[str],
        audit: dict[str, object],
    ) -> list[dict[str, Any]]:
        """Durably isolate active worker accounts without replacing their original freeze evidence."""

        with self._transaction():
            return self._freeze_workers(source, affected_worker_ids, audit)

    def _freeze_workers(
        self,
        source: str,
        affected_worker_ids: list[str],
        audit: dict[str, object],
    ) -> list[dict[str, Any]]:
        if not source or not affected_worker_ids:
            raise LedgerError("Worker freeze source and affected workers are required.")
        if any(not worker_id for worker_id in affected_worker_ids):
            raise LedgerError("Affected worker IDs are invalid.")
        if not isinstance(audit.get("reason"), str) or not audit["reason"]:
            raise LedgerError("Worker freeze audit reason is required.")
        worker_ids = list(dict.fromkeys(affected_worker_ids))
        frozen: list[dict[str, Any]] = []
        for worker_id in worker_ids:
            existing = self._connection.execute(
                """SELECT source, affected_worker_ids, audit, frozen_at, event_id
                   FROM worker_freezes WHERE worker_id = ? ORDER BY frozen_at DESC LIMIT 1""",
                [worker_id],
            ).fetchone()
            if existing is not None:
                frozen.append(
                    {
                        "worker_id": worker_id,
                        "source": existing[0],
                        "affected_worker_ids": json.loads(existing[1]),
                        "audit": json.loads(existing[2]),
                        "frozen_at": existing[3],
                        "event_id": existing[4],
                    }
                )
                continue
            status = self._connection.execute(
                "SELECT status FROM workers WHERE worker_id = ?", [worker_id]
            ).fetchone()
            if status is None:
                raise LedgerError("Worker is not active.")
            if status[0] != "active":
                continue
            frozen_at = _utc_now()
            payload = {
                "worker_id": worker_id,
                "source": source,
                "affected_worker_ids": worker_ids,
                "audit": audit,
            }
            event_id = self._event("worker_frozen", payload)
            self._connection.execute(
                """INSERT INTO worker_freezes
                   (freeze_id, worker_id, source, affected_worker_ids, audit, frozen_at, event_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                [
                    str(uuid4()), worker_id, source, json.dumps(worker_ids, sort_keys=True),
                    json.dumps(audit, sort_keys=True), frozen_at, event_id,
                ],
            )
            self._connection.execute(
                "UPDATE workers SET safety_state = 'frozen', last_seen_at = ? WHERE worker_id = ?",
                [frozen_at, worker_id],
            )
            self._alert(worker_id, "high", "worker_frozen", source)
            frozen.append(
                {
                    "worker_id": worker_id,
                    "source": source,
                    "affected_worker_ids": worker_ids,
                    "audit": audit,
                    "frozen_at": frozen_at,
                    "event_id": event_id,
                }
            )
        return frozen

    def freeze_worker_and_active_counterparts(
        self, worker_id: str, source: str, audit: dict[str, object]
    ) -> list[dict[str, Any]]:
        """Freeze a worker and every account sharing an active execution with it."""

        with self._transaction():
            if self._connection.execute(
                "SELECT 1 FROM workers WHERE worker_id = ? AND status = 'active'", [worker_id]
            ).fetchone() is None:
                raise LedgerError("Worker is not active.")
            return self._freeze_workers(source, self._active_counterpart_worker_ids(worker_id), audit)

    def _active_counterpart_worker_ids(self, worker_id: str) -> list[str]:
        counterparts = {worker_id}
        rows = self._connection.execute(
            """SELECT preflight FROM trader_intents
               WHERE status IN ('dispatching', 'working', 'cancelling', 'cancellation_executing', 'needs_human')"""
        ).fetchall()
        for (preflight_json,) in rows:
            preflight = json.loads(preflight_json)
            worker_ids = [
                item["worker_id"]
                for item in preflight
                if isinstance(item, dict) and isinstance(item.get("worker_id"), str)
            ]
            if worker_id in worker_ids:
                counterparts.update(worker_ids)
        return sorted(counterparts)

    def freeze_intent_workers(
        self, intent_id: str, source: str, audit: dict[str, object]
    ) -> list[dict[str, Any]]:
        """Freeze every valid participant retained in an intent's preflight evidence."""

        with self._transaction():
            worker_ids = list(dict.fromkeys(
                item["worker_id"]
                for item in self._intent_preflight(intent_id)
                if isinstance(item, dict) and isinstance(item.get("worker_id"), str) and item["worker_id"]
            ))
            if not worker_ids:
                raise LedgerError("Intent has no worker isolation evidence.")
            return self._freeze_workers(source, worker_ids, audit)

    def frozen_worker_ids(self, worker_ids: list[str]) -> set[str]:
        if not worker_ids:
            return set()
        placeholders = ", ".join("?" for _ in worker_ids)
        with self._lock:
            rows = self._connection.execute(
                f"SELECT worker_id FROM workers WHERE worker_id IN ({placeholders}) AND safety_state = 'frozen'",
                worker_ids,
            ).fetchall()
        return {str(row[0]) for row in rows}

    def request_worker_recovery(
        self, username: str, command_id: str, worker_id: str, operation: str
    ) -> tuple[dict[str, Any], bool]:
        if operation not in {"cleanup", "release"}:
            raise LedgerError("Worker recovery operation is invalid.")
        payload = {"worker_id": worker_id, "operation": operation}
        payload_hash = _hash(json.dumps(payload, sort_keys=True))
        with self._transaction():
            existing = self._connection.execute(
                """SELECT payload_hash, result FROM worker_recovery_commands
                   WHERE username = ? AND command_id = ?""",
                [username, command_id],
            ).fetchone()
            if existing is not None:
                if existing[0] != payload_hash:
                    raise LedgerError("Worker recovery command ID was already used with a different payload.")
                return json.loads(existing[1]), False
            state = self._connection.execute(
                "SELECT safety_state FROM workers WHERE worker_id = ? AND status = 'active'", [worker_id]
            ).fetchone()
            if state is None:
                raise LedgerError("Worker is not active.")
            if state[0] != "frozen":
                raise LedgerError("Worker recovery is available only for frozen workers.")
            result = {
                "worker_id": worker_id,
                "operation": operation,
                "operation_id": str(uuid4()),
                "status": f"{operation}_scheduled",
            }
            self._connection.execute(
                """INSERT INTO worker_recovery_commands
                   (username, command_id, payload_hash, worker_id, operation, result, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                [username, command_id, payload_hash, worker_id, operation, json.dumps(result, sort_keys=True), _utc_now()],
            )
            self._event(
                f"worker_{operation}_requested",
                {"worker_id": worker_id, "operation_id": result["operation_id"], "requested_by": username},
            )
            return result, True

    def record_worker_cleanup(
        self, worker_id: str, operation_id: str, requested_by: str, state: dict[str, object], succeeded: bool
    ) -> None:
        with self._transaction():
            self._event(
                "worker_cleanup_completed" if succeeded else "worker_cleanup_failed",
                {
                    "worker_id": worker_id,
                    "operation_id": operation_id,
                    "requested_by": requested_by,
                    "state": state,
                },
            )

    def release_frozen_worker(
        self, worker_id: str, operation_id: str, released_by: str, state: dict[str, object]
    ) -> None:
        if state.get("orders") or state.get("positions"):
            raise LedgerError("Worker release requires an empty broker-account reconciliation.")
        with self._transaction():
            changed = self._connection.execute(
                """UPDATE workers SET safety_state = 'connected', last_seen_at = ?
                   WHERE worker_id = ? AND status = 'active' AND safety_state = 'frozen'
                   RETURNING worker_id""",
                [_utc_now(), worker_id],
            ).fetchone()
            if changed is None:
                raise LedgerError("Worker recovery is available only for frozen workers.")
            self._event(
                "worker_released",
                {
                    "worker_id": worker_id,
                    "operation_id": operation_id,
                    "released_by": released_by,
                    "reconciled_at": _utc_now().isoformat(),
                },
            )

    def record_empty_account_reconciliation(
        self, worker_id: str, operation_id: str, requested_by: str, state: dict[str, object]
    ) -> None:
        if state.get("orders") or state.get("positions"):
            raise LedgerError("Empty-account reconciliation contains broker exposure.")
        with self._transaction():
            self._event(
                "worker_empty_account_reconciled",
                {
                    "worker_id": worker_id,
                    "operation_id": operation_id,
                    "requested_by": requested_by,
                    "orders": [],
                    "positions": [],
                    "reconciled_at": _utc_now().isoformat(),
                },
            )

    def record_worker_release_failure(
        self, worker_id: str, operation_id: str, requested_by: str, reason: str
    ) -> None:
        with self._transaction():
            self._event(
                "worker_release_failed",
                {
                    "worker_id": worker_id,
                    "operation_id": operation_id,
                    "requested_by": requested_by,
                    "reason": reason,
                },
            )

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
            execution_id = record.get("control_plane_command_id")
            if isinstance(execution_id, str) and execution_id:
                intents = self._connection.execute(
                    """SELECT intent_id, trader_id FROM trader_intents
                       WHERE status = 'working' AND CAST(preflight AS VARCHAR) LIKE ?""",
                    [f"%{execution_id}%"],
                ).fetchall()
                for intent_id, trader_id in intents:
                    event_id = self._event(
                        "intent_reconciliation_observed",
                        {
                            "intent_id": intent_id, "worker_id": worker_id, "cursor": cursor,
                            "entity": entity, "ticket": ticket, "change": change, "record": record,
                        },
                    )
                    self._connection.execute(
                        """INSERT INTO trader_intent_execution_records (intent_id, event_id, event_type, payload, recorded_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        [intent_id, event_id, "intent_reconciliation_observed",
                         json.dumps({"worker_id": worker_id, "record": record}, sort_keys=True), _utc_now()],
                    )
                    self._connection.execute(
                        "INSERT INTO trader_events (trader_id, event_id) VALUES (?, ?)", [trader_id, event_id]
                    )
            else:
                self._event(
                    "external_broker_change",
                    {"worker_id": worker_id, "cursor": cursor, "entity": entity, "ticket": ticket, "change": change},
                )
                affected = self._connection.execute(
                    """SELECT intent_id, trader_id FROM trader_intents
                       WHERE status IN ('dispatching', 'working')
                         AND CAST(preflight AS VARCHAR) LIKE ?""",
                    [f"%{worker_id}%"],
                ).fetchall()
                for intent_id, trader_id in affected:
                    payload = {
                        "worker_id": worker_id,
                        "cursor": cursor,
                        "entity": entity,
                        "ticket": ticket,
                        "change": change,
                        "reason": "Unattributed external broker change requires human recovery.",
                    }
                    execution_event_id = self._event(
                        "intent_execution_frozen",
                        {"intent_id": intent_id, **payload},
                    )
                    self._connection.execute(
                        """INSERT INTO trader_intent_execution_records
                           (intent_id, event_id, event_type, payload, recorded_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        [intent_id, execution_event_id, "intent_execution_frozen",
                         json.dumps(payload, separators=(",", ":"), sort_keys=True), _utc_now()],
                    )
                    self._connection.execute(
                        "INSERT INTO trader_events (trader_id, event_id) VALUES (?, ?)",
                        [trader_id, execution_event_id],
                    )
                    self._connection.execute(
                        "UPDATE trader_intents SET status = 'needs_human' WHERE intent_id = ?",
                        [intent_id],
                    )
                self._freeze_workers(
                    "external_broker_change",
                    self._active_counterpart_worker_ids(worker_id),
                    {
                        "reason": "Unattributed external broker change requires account isolation.",
                        "cursor": cursor,
                        "entity": entity,
                        "ticket": ticket,
                    },
                )

    def worker_reconciliation(self) -> list[dict[str, Any]]:
        now = _utc_now()
        with self._lock:
            workers = self._connection.execute(
                "SELECT worker_id, login, server, status, safety_state, last_seen_at FROM workers ORDER BY approved_at"
            ).fetchall()
            result = []
            for worker_id, login, server, status, safety_state, last_seen_at in workers:
                live_state = self._connection.execute(
                    """SELECT observed_at, connectivity, quotes, orders, positions, received_at
                       FROM worker_live_state WHERE worker_id = ?""",
                    [worker_id],
                ).fetchone()
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
                        "freeze": self._worker_freeze(worker_id),
                        "live_state": None if live_state is None else {
                            "observed_at": live_state[0],
                            "connectivity": bool(live_state[1]),
                            "quotes": json.loads(live_state[2]),
                            "orders": json.loads(live_state[3]),
                            "positions": json.loads(live_state[4]),
                            "received_at": live_state[5],
                        },
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

    def manual_trading_target(self) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                """SELECT pair_id, first_worker_id, second_worker_id, leg_order, interval_seconds,
                          revision, active_manual_trade_id, configured_by, configured_at
                   FROM manual_trading_target WHERE singleton = TRUE"""
            ).fetchone()
            if row is None:
                return None
            pair = self._product_pair_by_id(row[0])
            workers_by_id = {worker["worker_id"]: worker for worker in self.worker_reconciliation()}
            worker_ids = [row[1], row[2]]
            if any(worker_id not in workers_by_id for worker_id in worker_ids):
                raise LedgerError("Manual-trading target references a worker that no longer exists.")
            return {
                "pair": pair,
                "workers": [
                    {**workers_by_id[worker_id], "endpoint": pair["endpoints"][index]}
                    for index, worker_id in enumerate(worker_ids)
                ],
                "leg_order": row[3],
                "interval_seconds": row[4],
                "revision": row[5],
                "active_manual_trade_id": row[6],
                "configured_by": row[7],
                "configured_at": row[8],
            }

    def configure_manual_trading_target(
        self,
        username: str,
        *,
        pair_id: str,
        first_worker_id: str,
        second_worker_id: str,
        leg_order: str,
        interval_seconds: int,
        expected_revision: int,
    ) -> dict[str, Any]:
        if leg_order not in {"buy_to_sell", "sell_to_buy"}:
            raise LedgerError("Manual-trading leg order is invalid.")
        if isinstance(interval_seconds, bool) or interval_seconds < 0:
            raise LedgerError("Manual-trading interval must be a non-negative integer.")
        with self._transaction():
            current = self._connection.execute(
                """SELECT revision, active_manual_trade_id FROM manual_trading_target WHERE singleton = TRUE"""
            ).fetchone()
            actual_revision = 0 if current is None else current[0]
            if expected_revision != actual_revision:
                raise LedgerError("Manual-trading target revision does not match the current target.")
            if current is not None and current[1] is not None:
                raise LedgerError("Manual-trading target cannot be replaced while its manual trade is active.")
            if first_worker_id == second_worker_id:
                raise LedgerError("Manual-trading target requires two different workers.")
            pair = self._product_pair_by_id(pair_id)
            if pair["status"] != "active":
                raise LedgerError("Manual-trading target requires an active product pair.")
            applicability = {
                worker["worker_id"]: worker
                for worker in self._product_pair_with_worker_applicability(pair)["worker_applicability"]
            }
            workers_by_id = {worker["worker_id"]: worker for worker in self.worker_reconciliation()}
            for index, worker_id in enumerate((first_worker_id, second_worker_id)):
                worker = workers_by_id.get(worker_id)
                eligible = applicability.get(worker_id)
                if worker is None or eligible is None or eligible["applicability_status"] != "applicable":
                    raise LedgerError("Manual-trading target worker is not applicable to the selected product pair.")
                if worker["server"] != pair["endpoints"][index]["server"]:
                    raise LedgerError("Manual-trading target workers must match the selected endpoint order.")
                if worker["connectivity"] != "connected" or worker["live_state"] is None or not worker["live_state"]["connectivity"]:
                    raise LedgerError("Manual-trading target workers must be connected.")
                if worker["safety_state"] != "connected":
                    raise LedgerError("Frozen workers cannot be selected for manual trading.")
            revision = actual_revision + 1
            now = _utc_now()
            self._connection.execute(
                """INSERT INTO manual_trading_target
                       (singleton, pair_id, first_worker_id, second_worker_id, leg_order, interval_seconds,
                        revision, active_manual_trade_id, configured_by, configured_at)
                   VALUES (TRUE, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
                   ON CONFLICT (singleton) DO UPDATE SET
                       pair_id = excluded.pair_id, first_worker_id = excluded.first_worker_id,
                       second_worker_id = excluded.second_worker_id, leg_order = excluded.leg_order,
                       interval_seconds = excluded.interval_seconds, revision = excluded.revision,
                       configured_by = excluded.configured_by, configured_at = excluded.configured_at""",
                [pair_id, first_worker_id, second_worker_id, leg_order, interval_seconds, revision, username, now],
            )
            self._event(
                "manual_trading_target_configured",
                {
                    "pair_id": pair_id,
                    "first_worker_id": first_worker_id,
                    "second_worker_id": second_worker_id,
                    "leg_order": leg_order,
                    "interval_seconds": interval_seconds,
                    "revision": revision,
                    "configured_by": username,
                },
            )
        target = self.manual_trading_target()
        assert target is not None
        return target

    def _worker_freeze(self, worker_id: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            """SELECT source, affected_worker_ids, audit, frozen_at, event_id
               FROM worker_freezes WHERE worker_id = ? ORDER BY frozen_at DESC LIMIT 1""",
            [worker_id],
        ).fetchone()
        if row is None:
            return None
        return {
            "source": row[0],
            "affected_worker_ids": json.loads(row[1]),
            "audit": json.loads(row[2]),
            "frozen_at": row[3],
            "event_id": row[4],
        }

    def worker_snapshot_page(self, *, limit: int, cursor: str | None, query: str | None) -> dict[str, Any]:
        cursor_values = _decode_page_cursor(cursor, "worker_snapshots")
        clauses: list[str] = []
        parameters: list[Any] = []
        if query is not None:
            clauses.append(
                """lower(concat_ws(' ', s.worker_id, w.server, CAST(w.login AS VARCHAR),
                   CAST(s.account AS VARCHAR), CAST(s.terminal AS VARCHAR))) LIKE ?"""
            )
            parameters.append(f"%{query.lower()}%")
        if cursor_values is not None:
            clauses.append("(s.received_at < ? OR (s.received_at = ? AND s.snapshot_id < ?))")
            parameters.extend([cursor_values["timestamp"], cursor_values["timestamp"], cursor_values["id"]])
        where = "" if not clauses else f"WHERE {' AND '.join(clauses)}"
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT s.snapshot_id, s.worker_id, s.cursor, s.observed_at, s.account, s.terminal, s.received_at,
                       w.login, w.server
                FROM reconciliation_snapshots s
                JOIN workers w ON w.worker_id = s.worker_id
                {where}
                ORDER BY s.received_at DESC, s.snapshot_id DESC
                LIMIT ?
                """,
                [*parameters, limit + 1],
            ).fetchall()
        has_more = len(rows) > limit
        items = []
        for row in rows[:limit]:
            account = json.loads(row[4])
            terminal = json.loads(row[5])
            items.append(
                {
                    "snapshot_id": row[0],
                    "worker_id": row[1],
                    "cursor": row[2],
                    "timestamp": row[3],
                    "server": _summary_string(account.get("server")) or row[8],
                    "login": _summary_number(account.get("login")) or row[7],
                    "balance": _summary_number(account.get("balance")),
                    "equity": _summary_number(account.get("equity")),
                    "trade_allowed": _summary_boolean(account.get("trade_allowed")),
                    "trade_expert": _summary_boolean(account.get("trade_expert")),
                    "tradeapi_disabled": _summary_boolean(terminal.get("tradeapi_disabled")),
                    "received_at": row[6],
                }
            )
        return _page(items, has_more, "worker_snapshots", "received_at", "snapshot_id")

    def worker_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT snapshot_id, worker_id, cursor, observed_at, account, terminal, orders, positions, received_at
                FROM reconciliation_snapshots WHERE snapshot_id = ?
                """,
                [snapshot_id],
            ).fetchone()
        if row is None:
            raise LedgerError("Worker snapshot does not exist.")
        return {
            "snapshot_id": row[0],
            "worker_id": row[1],
            "cursor": row[2],
            "timestamp": row[3],
            "account": json.loads(row[4]),
            "terminal": json.loads(row[5]),
            "orders": json.loads(row[6]),
            "positions": json.loads(row[7]),
            "received_at": row[8],
        }

    def create_product_catalog_analysis(
        self,
        *,
        first_worker_id: str,
        second_worker_id: str,
        requested_by: str,
        policy: dict[str, object],
        analysis_period: dict[str, object],
    ) -> str:
        if first_worker_id == second_worker_id:
            raise LedgerError("Product catalog analysis requires two distinct workers.")
        now = _utc_now()
        with self._transaction():
            first = self._analysis_worker(first_worker_id, now)
            second = self._analysis_worker(second_worker_id, now)
            if first["server"] == second["server"]:
                raise LedgerError("Product catalog analysis requires workers on different exact MT5 servers.")
            analysis_id = str(uuid4())
            self._connection.execute(
                """
                INSERT INTO product_catalog_analyses (
                    analysis_id, requested_by, first_worker_id, first_login, first_server,
                    second_worker_id, second_login, second_server, policy, status, current_stage,
                    analysis_period, requested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', 'catalog', ?, ?)
                """,
                [
                    analysis_id,
                    requested_by,
                    first["worker_id"],
                    first["login"],
                    first["server"],
                    second["worker_id"],
                    second["login"],
                    second["server"],
                    json.dumps(policy, sort_keys=True),
                    json.dumps(analysis_period, sort_keys=True),
                    now,
                ],
            )
            self._event(
                "product_catalog_analysis_requested",
                {
                    "analysis_id": analysis_id,
                    "requested_by": requested_by,
                    "first_worker_id": first_worker_id,
                    "second_worker_id": second_worker_id,
                },
            )
            return analysis_id

    def _analysis_worker(self, worker_id: str, now: datetime) -> dict[str, Any]:
        row = self._connection.execute(
            """
            SELECT worker_id, login, server, status, safety_state, last_seen_at
            FROM workers
            WHERE worker_id = ?
            """,
            [worker_id],
        ).fetchone()
        if row is None or row[3] != "active":
            raise LedgerError("Selected worker must be approved, healthy, and connected.")
        if row[4] != "connected":
            raise LedgerError("Selected worker must be approved, healthy, and connected.")
        if row[5] is None or now - row[5] > timedelta(minutes=5):
            raise LedgerError("Selected worker must be approved, healthy, and connected.")
        return {"worker_id": row[0], "login": row[1], "server": row[2]}

    def record_product_catalog_analysis_catalog(
        self,
        analysis_id: str,
        *,
        first_evidence: dict[str, object],
        second_evidence: dict[str, object],
        eligible_candidates: list[dict[str, object]],
    ) -> None:
        with self._transaction():
            self._require_running_product_catalog_analysis(analysis_id)
            self._connection.execute(
                """
                UPDATE product_catalog_analyses
                SET                 first_catalog_evidence = ?,
                second_catalog_evidence = ?,
                eligible_candidates = ?,
                current_stage = 'm15_screening',
                    catalog_completed_at = ?
                WHERE analysis_id = ?
                """,
                [
                    json.dumps(first_evidence, sort_keys=True),
                    json.dumps(second_evidence, sort_keys=True),
                    json.dumps(eligible_candidates, sort_keys=True),
                    _utc_now(),
                    analysis_id,
                ],
            )
            self._event("product_catalog_analysis_catalog_completed", {"analysis_id": analysis_id})

    def complete_product_catalog_analysis(
        self,
        analysis_id: str,
        *,
        m15_screening_results: list[dict[str, object]],
        m1_verification_results: list[dict[str, object]],
        m1_verified: bool,
    ) -> None:
        with self._transaction():
            self._require_running_product_catalog_analysis(analysis_id)
            self._connection.execute(
                """
                UPDATE product_catalog_analyses
                SET status = 'succeeded',
                    current_stage = 'completed',
                    failure_reason = NULL,
                    m15_screening_results = ?,
                    m1_verification_results = ?,
                    m15_screened_at = ?,
                    m1_verified_at = ?,
                    completed_at = ?
                WHERE analysis_id = ?
                """,
                [
                    json.dumps(m15_screening_results, sort_keys=True),
                    json.dumps(m1_verification_results, sort_keys=True),
                    _utc_now(),
                    _utc_now() if m1_verified else None,
                    _utc_now(),
                    analysis_id,
                ],
            )
            self._event("product_catalog_analysis_m15_completed", {"analysis_id": analysis_id})
            if m1_verified:
                self._event("product_catalog_analysis_m1_completed", {"analysis_id": analysis_id})
            self._event("product_catalog_analysis_succeeded", {"analysis_id": analysis_id})

    def record_product_catalog_analysis_retry(self, analysis_id: str, stage: str, reason: str) -> None:
        with self._transaction():
            self._require_running_product_catalog_analysis(analysis_id)
            self._connection.execute(
                """
                UPDATE product_catalog_analyses
                SET retry_count = retry_count + 1
                WHERE analysis_id = ?
                """,
                [analysis_id],
            )
            self._event(
                "product_catalog_analysis_retry",
                {"analysis_id": analysis_id, "stage": stage, "reason": reason},
            )

    def fail_product_catalog_analysis(
        self,
        analysis_id: str,
        reason: str,
        *,
        stage: str,
        clear_m15_results: bool = True,
        m15_screening_results: list[dict[str, object]] | None = None,
    ) -> None:
        with self._transaction():
            self._require_running_product_catalog_analysis(analysis_id)
            persisted_m15_screening_results = (
                "[]"
                if clear_m15_results
                else json.dumps(m15_screening_results, sort_keys=True)
                if m15_screening_results is not None
                else self._connection.execute(
                    "SELECT m15_screening_results FROM product_catalog_analyses WHERE analysis_id = ?",
                    [analysis_id],
                ).fetchone()[0]
            )
            self._connection.execute(
                """
                UPDATE product_catalog_analyses
                SET status = 'failed',
                    current_stage = ?,
                    failure_reason = ?,
                    m15_screening_results = ?,
                    m1_verification_results = '[]',
                    completed_at = ?
                WHERE analysis_id = ?
                """,
                [stage, reason, persisted_m15_screening_results, _utc_now(), analysis_id],
            )
            self._event(
                "product_catalog_analysis_failed",
                {"analysis_id": analysis_id, "stage": stage, "reason": reason},
            )

    def _require_running_product_catalog_analysis(self, analysis_id: str) -> None:
        row = self._connection.execute(
            "SELECT status FROM product_catalog_analyses WHERE analysis_id = ?",
            [analysis_id],
        ).fetchone()
        if row is None:
            raise LedgerError("Product catalog analysis does not exist.")
        if row[0] != "running":
            raise LedgerError("Product catalog analysis is no longer running.")

    def product_catalog_analysis(self, analysis_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT analysis_id, requested_by, first_worker_id, first_login, first_server,
                       second_worker_id, second_login, second_server, policy, status, failure_reason,
                       current_stage, retry_count, analysis_period, first_catalog_evidence, second_catalog_evidence,
                       eligible_candidates, m15_screening_results, m1_verification_results, requested_at,
                       catalog_completed_at, m15_screened_at, m1_verified_at, completed_at
                FROM product_catalog_analyses
                WHERE analysis_id = ?
                """,
                [analysis_id],
            ).fetchone()
        if row is None:
            raise LedgerError("Product catalog analysis does not exist.")
        return {
            "analysis_id": row[0],
            "requested_by": row[1],
            "first_worker": {"worker_id": row[2], "login": row[3], "server": row[4]},
            "second_worker": {"worker_id": row[5], "login": row[6], "server": row[7]},
            "policy": json.loads(row[8]),
            "status": row[9],
            "failure_reason": row[10],
            "current_stage": row[11],
            "retry_count": row[12],
            "analysis_period": json.loads(row[13]),
            "first_catalog_evidence": None if row[14] is None else json.loads(row[14]),
            "second_catalog_evidence": None if row[15] is None else json.loads(row[15]),
            "eligible_candidates": json.loads(row[16]),
            "m15_screening_results": json.loads(row[17]),
            "m1_verification_results": json.loads(row[18]),
            "requested_at": row[19],
            "catalog_completed_at": row[20],
            "m15_screened_at": row[21],
            "m1_verified_at": row[22],
            "completed_at": row[23],
        }

    def product_catalog_analysis_page(
        self, *, limit: int, cursor: str | None, status: str | None, query: str | None
    ) -> dict[str, Any]:
        cursor_values = _decode_page_cursor(cursor, "product_catalog_analyses")
        clauses: list[str] = []
        parameters: list[Any] = []
        if status is not None:
            clauses.append("status = ?")
            parameters.append(status)
        if query is not None:
            normalized_date_query = "".join(character for character in query if character.isdigit())
            if normalized_date_query:
                clauses.append(
                    """(lower(concat_ws(' ', analysis_id, requested_by, first_worker_id, first_server,
                       second_worker_id, second_server, CAST(first_login AS VARCHAR), CAST(second_login AS VARCHAR),
                       CAST(policy AS VARCHAR))) LIKE ?
                       OR strftime(requested_at, '%Y%m%d') LIKE ?)"""
                )
                parameters.extend([f"%{query.lower()}%", f"%{normalized_date_query}%"])
            else:
                clauses.append(
                    """lower(concat_ws(' ', analysis_id, requested_by, first_worker_id, first_server,
                       second_worker_id, second_server, CAST(first_login AS VARCHAR), CAST(second_login AS VARCHAR),
                       CAST(policy AS VARCHAR))) LIKE ?"""
                )
                parameters.append(f"%{query.lower()}%")
        if cursor_values is not None:
            clauses.append("(requested_at < ? OR (requested_at = ? AND analysis_id < ?))")
            parameters.extend([cursor_values["timestamp"], cursor_values["timestamp"], cursor_values["id"]])
        where = "" if not clauses else f"WHERE {' AND '.join(clauses)}"
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT analysis_id, requested_by, first_worker_id, first_login, first_server,
                       second_worker_id, second_login, second_server, policy, status, current_stage,
                       retry_count, requested_at, completed_at
                FROM product_catalog_analyses {where}
                ORDER BY requested_at DESC, analysis_id DESC
                LIMIT ?
                """,
                [*parameters, limit + 1],
            ).fetchall()
        has_more = len(rows) > limit
        items = [
            {
                "analysis_id": row[0],
                "requested_by": row[1],
                "first_worker": {"worker_id": row[2], "login": row[3], "server": row[4]},
                "second_worker": {"worker_id": row[5], "login": row[6], "server": row[7]},
                "policy_label": json.loads(row[8]).get("label"),
                "status": row[9],
                "current_stage": row[10],
                "retry_count": row[11],
                "requested_at": row[12],
                "completed_at": row[13],
            }
            for row in rows[:limit]
        ]
        return _page(items, has_more, "product_catalog_analyses", "requested_at", "analysis_id")

    def create_product_pair_build_confirmation(
        self,
        analysis_id: str,
        *,
        first_symbol: str,
        second_symbol: str,
        requested_by: str,
    ) -> dict[str, Any]:
        with self._transaction():
            analysis = self._product_catalog_analysis_for_update(analysis_id)
            if analysis["status"] != "succeeded":
                raise LedgerError("Product catalog analysis is not ready for Build confirmation.")
            verification_result = next(
                (
                    result
                    for result in analysis["m1_verification_results"]
                    if result["first_symbol"] == first_symbol and result["second_symbol"] == second_symbol
                ),
                None,
            )
            if verification_result is None or verification_result["verification_status"] != "passed":
                raise LedgerError("Selected candidate is not a final passing candidate.")
            first_specification = self._analysis_symbol_specification(analysis["first_catalog_evidence"], first_symbol)
            second_specification = self._analysis_symbol_specification(analysis["second_catalog_evidence"], second_symbol)
            confirmation_id = str(uuid4())
            confirmation = self._product_pair_confirmation_payload(
                confirmation_id=confirmation_id,
                analysis=analysis,
                verification_result=verification_result,
                first_specification=first_specification,
                second_specification=second_specification,
                requested_by=requested_by,
            )
            self._connection.execute(
                """
                INSERT INTO product_pair_build_confirmations (
                    confirmation_id, analysis_id, requested_by, first_server, first_symbol,
                    second_server, second_symbol, confirmation, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    confirmation_id,
                    analysis_id,
                    requested_by,
                    str(confirmation["endpoints"][0]["server"]),
                    str(confirmation["endpoints"][0]["symbol"]),
                    str(confirmation["endpoints"][1]["server"]),
                    str(confirmation["endpoints"][1]["symbol"]),
                    json.dumps(confirmation, sort_keys=True),
                    _utc_now(),
                ],
            )
            self._event(
                "product_pair_build_confirmation_requested",
                {
                    "confirmation_id": confirmation_id,
                    "analysis_id": analysis_id,
                    "requested_by": requested_by,
                    "first_symbol": first_symbol,
                    "second_symbol": second_symbol,
                },
            )
            return confirmation

    def build_product_pair(self, confirmation_id: str, built_by: str) -> dict[str, Any]:
        with self._transaction():
            confirmation = self._product_pair_build_confirmation(confirmation_id)
            self._require_no_active_product_pair(confirmation["endpoints"])
            product_pair_id = self._insert_product_pair(
                confirmation=confirmation,
                confirmation_id=confirmation_id,
                built_by=built_by,
            )
            self._connection.execute(
                "UPDATE product_pair_build_confirmations SET used_at = ? WHERE confirmation_id = ?",
                [_utc_now(), confirmation_id],
            )
            self._clear_manual_trading_target_for_retired_pair(product_pair_id)
            self._event(
                "product_pair_built",
                {
                    "product_pair_id": product_pair_id,
                    "confirmation_id": confirmation_id,
                    "analysis_id": confirmation["analysis_id"],
                    "built_by": built_by,
                },
            )
            return self._product_pair_by_id(product_pair_id)

    def replace_product_pair(
        self,
        product_pair_id: str,
        *,
        confirmation_id: str,
        replaced_by: str,
    ) -> dict[str, Any]:
        with self._transaction():
            current = self._product_pair_by_id(product_pair_id)
            if current["status"] != "active":
                raise LedgerError("Active product pair does not exist.")
            confirmation = self._product_pair_build_confirmation(confirmation_id)
            if confirmation["endpoints"] != current["endpoints"]:
                raise LedgerError("Replacement confirmation must target the same unordered endpoint pair.")
            self._connection.execute(
                """
                UPDATE product_pairs
                SET status = 'retired',
                    retired_at = ?,
                    retired_by = ?,
                    retired_reason = 'replaced',
                    replaced_by_product_pair_id = ?,
                    active_pair_key = NULL
                WHERE product_pair_id = ? AND status = 'active'
                """,
                [_utc_now(), replaced_by, None, product_pair_id],
            )
            new_product_pair_id = self._insert_product_pair(
                confirmation=confirmation,
                confirmation_id=confirmation_id,
                built_by=replaced_by,
                replaces_product_pair_id=product_pair_id,
            )
            self._connection.execute(
                "UPDATE product_pairs SET replaced_by_product_pair_id = ? WHERE product_pair_id = ?",
                [new_product_pair_id, product_pair_id],
            )
            self._connection.execute(
                "UPDATE product_pair_build_confirmations SET used_at = ? WHERE confirmation_id = ?",
                [_utc_now(), confirmation_id],
            )
            self._event(
                "product_pair_built",
                {
                    "product_pair_id": new_product_pair_id,
                    "confirmation_id": confirmation_id,
                    "analysis_id": confirmation["analysis_id"],
                    "built_by": replaced_by,
                },
            )
            self._event(
                "product_pair_retired",
                {
                    "product_pair_id": product_pair_id,
                    "retired_by": replaced_by,
                    "reason": "replaced",
                },
            )
            self._event(
                "product_pair_replaced",
                {
                    "retired_product_pair_id": product_pair_id,
                    "replacement_product_pair_id": new_product_pair_id,
                    "confirmation_id": confirmation_id,
                    "analysis_id": confirmation["analysis_id"],
                    "replaced_by": replaced_by,
                },
            )
            return self._product_pair_by_id(new_product_pair_id)

    def retire_product_pair(self, product_pair_id: str, retired_by: str) -> dict[str, Any]:
        with self._transaction():
            current = self._product_pair_by_id(product_pair_id)
            if current["status"] != "active":
                raise LedgerError("Active product pair does not exist.")
            self._connection.execute(
                """
                UPDATE product_pairs
                SET status = 'retired',
                    retired_at = ?,
                    retired_by = ?,
                    retired_reason = 'manual_retirement',
                    active_pair_key = NULL
                WHERE product_pair_id = ? AND status = 'active'
                """,
                [_utc_now(), retired_by, product_pair_id],
            )
            self._clear_manual_trading_target_for_retired_pair(product_pair_id)
            self._event(
                "product_pair_retired",
                {
                    "product_pair_id": product_pair_id,
                    "retired_by": retired_by,
                    "reason": "manual_retirement",
                },
            )
            return self._product_pair_by_id(product_pair_id)

    def _clear_manual_trading_target_for_retired_pair(self, product_pair_id: str) -> None:
        target = self._connection.execute(
            """SELECT active_manual_trade_id FROM manual_trading_target
               WHERE singleton = TRUE AND pair_id = ?""",
            [product_pair_id],
        ).fetchone()
        if target is None:
            return
        if target[0] is not None:
            raise LedgerError("An active manual-trading target cannot be retired.")
        self._connection.execute(
            "DELETE FROM manual_trading_target WHERE singleton = TRUE AND pair_id = ?",
            [product_pair_id],
        )
        self._event("manual_trading_target_cleared", {"pair_id": product_pair_id, "reason": "product_pair_retired"})

    def product_pairs(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT product_pair_id, status, endpoint_a_server, endpoint_a_symbol, endpoint_b_server, endpoint_b_symbol,
                       lot_relationship, policy_snapshot, analysis_period, reference_specifications, approval_evidence,
                       source_workers, built_from_analysis_id, built_from_confirmation_id, built_by, created_at,
                       retired_at, retired_by, retired_reason, replaced_by_product_pair_id, replaces_product_pair_id
                FROM product_pairs
                ORDER BY created_at, product_pair_id
                """
            ).fetchall()
            return [self._product_pair_with_worker_applicability(self._product_pair_from_row(row)) for row in rows]

    def product_pairs_page(self, *, limit: int, cursor: str | None, status: str, query: str | None) -> dict[str, Any]:
        cursor_values = _decode_page_cursor(cursor, "product_pairs")
        clauses: list[str] = []
        parameters: list[Any] = []
        if status != "all":
            clauses.append("status = ?")
            parameters.append(status)
        if query is not None:
            clauses.append(
                """lower(concat_ws(' ', product_pair_id, status, endpoint_a_server, endpoint_a_symbol,
                   endpoint_b_server, endpoint_b_symbol, built_from_analysis_id)) LIKE ?"""
            )
            parameters.append(f"%{query.lower()}%")
        if cursor_values is not None:
            clauses.append("(created_at < ? OR (created_at = ? AND product_pair_id < ?))")
            parameters.extend([cursor_values["timestamp"], cursor_values["timestamp"], cursor_values["id"]])
        where = "" if not clauses else f"WHERE {' AND '.join(clauses)}"
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT product_pair_id, status, endpoint_a_server, endpoint_a_symbol, endpoint_b_server, endpoint_b_symbol,
                       lot_relationship, policy_snapshot, analysis_period, reference_specifications, approval_evidence,
                       source_workers, built_from_analysis_id, built_from_confirmation_id, built_by, created_at,
                       retired_at, retired_by, retired_reason, replaced_by_product_pair_id, replaces_product_pair_id
                FROM product_pairs {where}
                ORDER BY created_at DESC, product_pair_id DESC
                LIMIT ?
                """,
                [*parameters, limit + 1],
            ).fetchall()
            has_more = len(rows) > limit
            items = [
                self._product_pair_with_worker_applicability(self._product_pair_from_row(row))
                for row in rows[:limit]
            ]
        return _page(items, has_more, "product_pairs", "created_at", "product_pair_id")

    def product_pair_worker_reference(self, product_pair_id: str, worker_id: str) -> dict[str, Any]:
        with self._lock:
            pair = self._product_pair_by_id(product_pair_id)
            if pair["status"] != "active":
                raise LedgerError("Active product pair does not exist.")
            row = self._connection.execute(
                "SELECT worker_id, login, server FROM workers WHERE worker_id = ? AND status = 'active'",
                [worker_id],
            ).fetchone()
            if row is None:
                raise LedgerError("Worker is not active.")
            endpoint = next((item for item in pair["endpoints"] if item["server"] == row[2]), None)
            if endpoint is None:
                raise LedgerError("Worker does not belong to this product pair's endpoint servers.")
            reference = next(
                (
                    item
                    for item in pair["reference_specifications"]
                    if item["server"] == endpoint["server"] and item["symbol"] == endpoint["symbol"]
                ),
                None,
            )
            if reference is None:
                raise LedgerError("Product pair reference specification does not exist.")
            return {
                "product_pair_id": pair["product_pair_id"],
                "worker_id": row[0],
                "login": row[1],
                "server": row[2],
                "reference_symbol": endpoint["symbol"],
                "reference_specification": reference["specification"],
            }

    def create_product_pair_retest(
        self,
        product_pair_id: str,
        *,
        first_worker_id: str,
        second_worker_id: str,
        requested_by: str,
        analysis_period: dict[str, object],
    ) -> dict[str, Any]:
        if first_worker_id == second_worker_id:
            raise LedgerError("Product pair re-test requires two distinct workers.")
        now = _utc_now()
        with self._transaction():
            pair = self._product_pair_by_id(product_pair_id)
            if pair["status"] != "active":
                raise LedgerError("Active product pair does not exist.")
            first = self._analysis_worker(first_worker_id, now)
            second = self._analysis_worker(second_worker_id, now)
            workers_by_server = {
                str(first["server"]): first,
                str(second["server"]): second,
            }
            pair_servers = {str(endpoint["server"]) for endpoint in pair["endpoints"]}
            if len(workers_by_server) != 2 or set(workers_by_server) != pair_servers:
                raise LedgerError("Selected workers must belong to this product pair's exact MT5 servers.")
            source_workers = [
                workers_by_server[str(endpoint["server"])]
                for endpoint in pair["endpoints"]
            ]
            retest_id = str(uuid4())
            self._connection.execute(
                """
                INSERT INTO product_pair_retests (
                    retest_id, product_pair_id, requested_by,
                    first_worker_id, first_login, first_server,
                    second_worker_id, second_login, second_server,
                    policy_snapshot, analysis_period, reference_specifications,
                    status, current_stage, retry_count, requested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', 'catalog', 0, ?)
                """,
                [
                    retest_id,
                    product_pair_id,
                    requested_by,
                    source_workers[0]["worker_id"],
                    source_workers[0]["login"],
                    source_workers[0]["server"],
                    source_workers[1]["worker_id"],
                    source_workers[1]["login"],
                    source_workers[1]["server"],
                    json.dumps(pair["policy_snapshot"], sort_keys=True),
                    json.dumps(analysis_period, sort_keys=True),
                    json.dumps(pair["reference_specifications"], sort_keys=True),
                    now,
                ],
            )
            self._event(
                "product_pair_retest_requested",
                {
                    "retest_id": retest_id,
                    "product_pair_id": product_pair_id,
                    "requested_by": requested_by,
                    "first_worker_id": source_workers[0]["worker_id"],
                    "second_worker_id": source_workers[1]["worker_id"],
                },
            )
            return self.product_pair_retest(retest_id)

    def record_product_pair_retest_catalog(
        self,
        retest_id: str,
        *,
        first_evidence: dict[str, object],
        second_evidence: dict[str, object],
    ) -> None:
        with self._transaction():
            self._require_running_product_pair_retest(retest_id)
            self._connection.execute(
                """
                UPDATE product_pair_retests
                SET first_catalog_evidence = ?,
                    second_catalog_evidence = ?,
                    current_stage = 'm15_screening',
                    catalog_completed_at = ?
                WHERE retest_id = ?
                """,
                [
                    json.dumps(first_evidence, sort_keys=True),
                    json.dumps(second_evidence, sort_keys=True),
                    _utc_now(),
                    retest_id,
                ],
            )
            self._event("product_pair_retest_catalog_completed", {"retest_id": retest_id})

    def record_product_pair_retest_retry(self, retest_id: str, stage: str, reason: str) -> None:
        with self._transaction():
            self._require_running_product_pair_retest(retest_id)
            self._connection.execute(
                """
                UPDATE product_pair_retests
                SET retry_count = retry_count + 1
                WHERE retest_id = ?
                """,
                [retest_id],
            )
            self._event(
                "product_pair_retest_retry",
                {"retest_id": retest_id, "stage": stage, "reason": reason},
            )

    def complete_product_pair_retest(
        self,
        retest_id: str,
        *,
        m15_screening_results: list[dict[str, object]],
        m1_verification_results: list[dict[str, object]],
        passed: bool,
        failure_reason: str | None = None,
    ) -> dict[str, Any]:
        with self._transaction():
            self._require_running_product_pair_retest(retest_id)
            status = "passed" if passed else "failed"
            self._connection.execute(
                """
                UPDATE product_pair_retests
                SET status = ?,
                    current_stage = 'completed',
                    failure_reason = ?,
                    m15_screening_results = ?,
                    m1_verification_results = ?,
                    m15_screened_at = ?,
                    m1_verified_at = ?,
                    completed_at = ?
                WHERE retest_id = ?
                """,
                [
                    status,
                    None if passed else failure_reason,
                    json.dumps(m15_screening_results, sort_keys=True),
                    json.dumps(m1_verification_results, sort_keys=True),
                    _utc_now(),
                    _utc_now() if m1_verification_results else None,
                    _utc_now(),
                    retest_id,
                ],
            )
            self._event("product_pair_retest_m15_completed", {"retest_id": retest_id})
            if m1_verification_results:
                self._event("product_pair_retest_m1_completed", {"retest_id": retest_id})
            if passed:
                self._event("product_pair_retest_succeeded", {"retest_id": retest_id})
            else:
                retest = self.product_pair_retest(retest_id)
                self._alert(
                    str(retest["source_workers"]["first_worker"]["worker_id"]),
                    "high",
                    "product_pair_retest_failed",
                    "latest_retest_failed",
                    product_pair_id=str(retest["product_pair_id"]),
                )
                self._event(
                    "product_pair_retest_failed",
                    {
                        "retest_id": retest_id,
                        "product_pair_id": retest["product_pair_id"],
                        "reason": failure_reason,
                    },
                )
            return self.product_pair_retest(retest_id)

    def fail_product_pair_retest(
        self,
        retest_id: str,
        reason: str,
        *,
        stage: str,
        clear_m15_results: bool = True,
        m15_screening_results: list[dict[str, object]] | None = None,
    ) -> dict[str, Any]:
        with self._transaction():
            self._require_running_product_pair_retest(retest_id)
            persisted_m15_screening_results = (
                "[]"
                if clear_m15_results
                else json.dumps(m15_screening_results, sort_keys=True)
                if m15_screening_results is not None
                else self._connection.execute(
                    "SELECT m15_screening_results FROM product_pair_retests WHERE retest_id = ?",
                    [retest_id],
                ).fetchone()[0]
            )
            self._connection.execute(
                """
                UPDATE product_pair_retests
                SET status = 'failed',
                    current_stage = ?,
                    failure_reason = ?,
                    m15_screening_results = ?,
                    m1_verification_results = '[]',
                    completed_at = ?
                WHERE retest_id = ?
                """,
                [stage, reason, persisted_m15_screening_results, _utc_now(), retest_id],
            )
            retest = self.product_pair_retest(retest_id)
            self._alert(
                str(retest["source_workers"]["first_worker"]["worker_id"]),
                "high",
                "product_pair_retest_failed",
                "latest_retest_failed",
                product_pair_id=str(retest["product_pair_id"]),
            )
            self._event(
                "product_pair_retest_failed",
                {
                    "retest_id": retest_id,
                    "product_pair_id": retest["product_pair_id"],
                    "stage": stage,
                    "reason": reason,
                },
            )
            return retest

    def product_pair_retest(self, retest_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT retest_id, product_pair_id, requested_by,
                       first_worker_id, first_login, first_server,
                       second_worker_id, second_login, second_server,
                       policy_snapshot, analysis_period, reference_specifications,
                       status, current_stage, retry_count, failure_reason,
                       first_catalog_evidence, second_catalog_evidence,
                       m15_screening_results, m1_verification_results,
                       requested_at, catalog_completed_at, m15_screened_at, m1_verified_at, completed_at
                FROM product_pair_retests
                WHERE retest_id = ?
                """,
                [retest_id],
            ).fetchone()
        if row is None:
            raise LedgerError("Product pair re-test does not exist.")
        return self._product_pair_retest_from_row(row)

    def record_product_pair_worker_compatibility_check(
        self,
        product_pair_id: str,
        worker_id: str,
        *,
        checked_by: str,
        reference_symbol: str,
        reference_specification: dict[str, object],
        live_specification: dict[str, object] | None,
        hard_block_differences: list[dict[str, object]],
        warning_differences: list[dict[str, object]],
    ) -> dict[str, Any]:
        checked_at = _utc_now()
        with self._transaction():
            reference = self.product_pair_worker_reference(product_pair_id, worker_id)
            compatibility_check_id = str(uuid4())
            self._connection.execute(
                """
                INSERT INTO product_pair_worker_compatibility_checks (
                    compatibility_check_id, product_pair_id, worker_id, server, reference_symbol,
                    checked_by, reference_specification, live_specification,
                    hard_block_differences, warning_differences, checked_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    compatibility_check_id,
                    product_pair_id,
                    worker_id,
                    reference["server"],
                    reference_symbol,
                    checked_by,
                    json.dumps(reference_specification, sort_keys=True),
                    None if live_specification is None else json.dumps(live_specification, sort_keys=True),
                    json.dumps(hard_block_differences, sort_keys=True),
                    json.dumps(warning_differences, sort_keys=True),
                    checked_at,
                ],
            )
            self._event(
                "product_pair_worker_compatibility_checked",
                {
                    "compatibility_check_id": compatibility_check_id,
                    "product_pair_id": product_pair_id,
                    "worker_id": worker_id,
                    "checked_by": checked_by,
                    "reference_symbol": reference_symbol,
                    "hard_block_differences": hard_block_differences,
                    "warning_differences": warning_differences,
                    "applicability_status": "applicable",
                },
            )
            summary = self._product_pair_worker_applicability(
                self._product_pair_by_id(product_pair_id),
                {"worker_id": worker_id, "login": reference["login"], "server": reference["server"]},
            )
            return {
                "product_pair_id": product_pair_id,
                **summary,
                "compatibility_check_id": compatibility_check_id,
                "checked_by": checked_by,
                "checked_at": checked_at,
                "reference_symbol": reference_symbol,
                "reference_specification": reference_specification,
                "live_specification": live_specification,
                "hard_block_differences": hard_block_differences,
                "warning_differences": warning_differences,
            }

    def exclude_product_pair_worker(
        self,
        product_pair_id: str,
        worker_id: str,
        *,
        excluded_by: str,
    ) -> dict[str, Any]:
        excluded_at = _utc_now()
        with self._transaction():
            reference = self.product_pair_worker_reference(product_pair_id, worker_id)
            existing = self._product_pair_worker_exclusion(product_pair_id, worker_id)
            if existing is not None:
                raise LedgerError("Worker is already excluded for this product pair.")
            latest_check = self._latest_product_pair_worker_compatibility_check(product_pair_id, worker_id)
            self._connection.execute(
                """
                INSERT INTO product_pair_worker_exclusions (
                    product_pair_id, worker_id, excluded_by, compatibility_check_id, excluded_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    product_pair_id,
                    worker_id,
                    excluded_by,
                    None if latest_check is None else latest_check["compatibility_check_id"],
                    excluded_at,
                ],
            )
            self._event(
                "product_pair_worker_excluded",
                {
                    "product_pair_id": product_pair_id,
                    "worker_id": worker_id,
                    "excluded_by": excluded_by,
                    "compatibility_check_id": None if latest_check is None else latest_check["compatibility_check_id"],
                },
            )
            return self._product_pair_worker_applicability(
                self._product_pair_by_id(product_pair_id),
                {"worker_id": worker_id, "login": reference["login"], "server": reference["server"]},
            )

    def _product_pair_build_confirmation(self, confirmation_id: str) -> dict[str, Any]:
        row = self._connection.execute(
            "SELECT confirmation, used_at FROM product_pair_build_confirmations WHERE confirmation_id = ?",
            [confirmation_id],
        ).fetchone()
        if row is None:
            raise LedgerError("Product pair Build confirmation does not exist.")
        if row[1] is not None:
            raise LedgerError("Product pair Build confirmation has already been used.")
        return json.loads(row[0])

    def _product_catalog_analysis_for_update(self, analysis_id: str) -> dict[str, Any]:
        row = self._connection.execute(
            """
            SELECT analysis_id, requested_by, first_worker_id, first_login, first_server,
                   second_worker_id, second_login, second_server, policy, status, failure_reason,
                   current_stage, retry_count, analysis_period, first_catalog_evidence, second_catalog_evidence,
                   eligible_candidates, m15_screening_results, m1_verification_results, requested_at,
                   catalog_completed_at, m15_screened_at, m1_verified_at, completed_at
            FROM product_catalog_analyses
            WHERE analysis_id = ?
            """,
            [analysis_id],
        ).fetchone()
        if row is None:
            raise LedgerError("Product catalog analysis does not exist.")
        return {
            "analysis_id": row[0],
            "requested_by": row[1],
            "first_worker": {"worker_id": row[2], "login": row[3], "server": row[4]},
            "second_worker": {"worker_id": row[5], "login": row[6], "server": row[7]},
            "policy": json.loads(row[8]),
            "status": row[9],
            "failure_reason": row[10],
            "current_stage": row[11],
            "retry_count": row[12],
            "analysis_period": json.loads(row[13]),
            "first_catalog_evidence": None if row[14] is None else json.loads(row[14]),
            "second_catalog_evidence": None if row[15] is None else json.loads(row[15]),
            "eligible_candidates": json.loads(row[16]),
            "m15_screening_results": json.loads(row[17]),
            "m1_verification_results": json.loads(row[18]),
            "requested_at": row[19],
            "catalog_completed_at": row[20],
            "m15_screened_at": row[21],
            "m1_verified_at": row[22],
            "completed_at": row[23],
        }

    def _analysis_symbol_specification(
        self,
        catalog_evidence: dict[str, Any] | None,
        symbol: str,
    ) -> dict[str, Any]:
        if catalog_evidence is None:
            raise LedgerError("Product catalog analysis does not have immutable catalog evidence.")
        result = next(
            (specification for specification in catalog_evidence["symbols"] if specification["symbol"] == symbol),
            None,
        )
        if result is None:
            raise LedgerError("Selected candidate specification does not exist.")
        return result

    def _product_pair_confirmation_payload(
        self,
        *,
        confirmation_id: str,
        analysis: dict[str, Any],
        verification_result: dict[str, Any],
        first_specification: dict[str, Any],
        second_specification: dict[str, Any],
        requested_by: str,
    ) -> dict[str, Any]:
        canonical = sorted(
            (
                {
                    "server": analysis["first_worker"]["server"],
                    "symbol": verification_result["first_symbol"],
                    "specification": first_specification,
                },
                {
                    "server": analysis["second_worker"]["server"],
                    "symbol": verification_result["second_symbol"],
                    "specification": second_specification,
                },
            ),
            key=lambda item: (str(item["server"]), str(item["symbol"])),
        )
        return {
            "confirmation_id": confirmation_id,
            "analysis_id": analysis["analysis_id"],
            "requested_by": requested_by,
            "analysis_period": analysis["analysis_period"],
            "policy_snapshot": analysis["policy"],
            "lot_relationship": {"version": "FX_V1", "ratio": "1:1", "first_lots": 1, "second_lots": 1},
            "source_workers": {
                "first_worker": analysis["first_worker"],
                "second_worker": analysis["second_worker"],
            },
            "endpoints": [
                {"server": item["server"], "symbol": item["symbol"]}
                for item in canonical
            ],
            "reference_specifications": canonical,
            "approval_evidence": verification_result,
        }

    def _require_no_active_product_pair(self, endpoints: list[dict[str, Any]]) -> None:
        existing = self._connection.execute(
            """
            SELECT product_pair_id
            FROM product_pairs
            WHERE status = 'active'
              AND endpoint_a_server = ?
              AND endpoint_a_symbol = ?
              AND endpoint_b_server = ?
              AND endpoint_b_symbol = ?
            """,
            [
                str(endpoints[0]["server"]),
                str(endpoints[0]["symbol"]),
                str(endpoints[1]["server"]),
                str(endpoints[1]["symbol"]),
            ],
        ).fetchone()
        if existing is not None:
            raise LedgerError("An active product pair already exists for this unordered endpoint pair.")

    def _insert_product_pair(
        self,
        *,
        confirmation: dict[str, Any],
        confirmation_id: str,
        built_by: str,
        replaces_product_pair_id: str | None = None,
    ) -> str:
        product_pair_id = str(uuid4())
        endpoints = confirmation["endpoints"]
        try:
            self._connection.execute(
                """
                INSERT INTO product_pairs (
                    product_pair_id, status, endpoint_a_server, endpoint_a_symbol, endpoint_b_server, endpoint_b_symbol,
                    active_pair_key, lot_relationship, policy_snapshot, analysis_period, reference_specifications,
                    approval_evidence, source_workers, built_from_analysis_id, built_from_confirmation_id, built_by,
                    created_at, replaces_product_pair_id
                ) VALUES (?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    product_pair_id,
                    str(endpoints[0]["server"]),
                    str(endpoints[0]["symbol"]),
                    str(endpoints[1]["server"]),
                    str(endpoints[1]["symbol"]),
                    _active_product_pair_key(endpoints),
                    json.dumps(confirmation["lot_relationship"], sort_keys=True),
                    json.dumps(confirmation["policy_snapshot"], sort_keys=True),
                    json.dumps(confirmation["analysis_period"], sort_keys=True),
                    json.dumps(confirmation["reference_specifications"], sort_keys=True),
                    json.dumps(confirmation["approval_evidence"], sort_keys=True),
                    json.dumps(confirmation["source_workers"], sort_keys=True),
                    str(confirmation["analysis_id"]),
                    confirmation_id,
                    built_by,
                    _utc_now(),
                    replaces_product_pair_id,
                ],
            )
        except duckdb.ConstraintException as error:
            if "active_pair_key" in str(error):
                raise LedgerError("An active product pair already exists for this unordered endpoint pair.") from error
            raise
        return product_pair_id

    def _product_pair_by_id(self, product_pair_id: str) -> dict[str, Any]:
        row = self._connection.execute(
            """
            SELECT product_pair_id, status, endpoint_a_server, endpoint_a_symbol, endpoint_b_server, endpoint_b_symbol,
                   lot_relationship, policy_snapshot, analysis_period, reference_specifications, approval_evidence,
                   source_workers, built_from_analysis_id, built_from_confirmation_id, built_by, created_at,
                   retired_at, retired_by, retired_reason, replaced_by_product_pair_id, replaces_product_pair_id
            FROM product_pairs
            WHERE product_pair_id = ?
            """,
            [product_pair_id],
        ).fetchone()
        if row is None:
            raise LedgerError("Product pair does not exist.")
        return self._product_pair_from_row(row)

    def _product_pair_from_row(self, row: Any) -> dict[str, Any]:
        return {
            "product_pair_id": row[0],
            "status": row[1],
            "endpoints": [
                {"server": row[2], "symbol": row[3]},
                {"server": row[4], "symbol": row[5]},
            ],
            "lot_relationship": json.loads(row[6]),
            "policy_snapshot": json.loads(row[7]),
            "analysis_period": json.loads(row[8]),
            "reference_specifications": json.loads(row[9]),
            "approval_evidence": json.loads(row[10]),
            "source_workers": json.loads(row[11]),
            "built_from_analysis_id": row[12],
            "built_from_confirmation_id": row[13],
            "built_by": row[14],
            "created_at": row[15],
            "retired_at": row[16],
            "retired_by": row[17],
            "retired_reason": row[18],
            "replaced_by_product_pair_id": row[19],
            "replaces_product_pair_id": row[20],
        }

    def _product_pair_with_worker_applicability(self, pair: dict[str, Any]) -> dict[str, Any]:
        latest_retest = self._latest_product_pair_retest(pair["product_pair_id"])
        if pair["status"] != "active":
            return {**pair, "latest_retest": latest_retest, "worker_applicability": []}
        rows = self._connection.execute(
            """
            SELECT worker_id, login, server
            FROM workers
            WHERE status = 'active' AND server IN (?, ?)
            ORDER BY approved_at, worker_id
            """,
            [pair["endpoints"][0]["server"], pair["endpoints"][1]["server"]],
        ).fetchall()
        return {
            **pair,
            "latest_retest": latest_retest,
            "worker_applicability": [
                self._product_pair_worker_applicability(
                    pair,
                    {"worker_id": row[0], "login": row[1], "server": row[2]},
                )
                for row in rows
            ],
        }

    def _product_pair_worker_applicability(
        self,
        pair: dict[str, Any],
        worker: dict[str, Any],
    ) -> dict[str, Any]:
        latest_check = self._latest_product_pair_worker_compatibility_check(pair["product_pair_id"], worker["worker_id"])
        exclusion = self._product_pair_worker_exclusion(pair["product_pair_id"], worker["worker_id"])
        return {
            "worker_id": worker["worker_id"],
            "login": worker["login"],
            "server": worker["server"],
            "applicability_status": "excluded" if exclusion is not None else "applicable",
            "inspection_status": _product_pair_worker_inspection_status(latest_check),
            "latest_compatibility_check": latest_check,
            "exclusion": exclusion,
        }

    def _latest_product_pair_worker_compatibility_check(
        self,
        product_pair_id: str,
        worker_id: str,
    ) -> dict[str, Any] | None:
        row = self._connection.execute(
            """
            SELECT compatibility_check_id, checked_by, checked_at, reference_symbol, reference_specification,
                   live_specification, hard_block_differences, warning_differences
            FROM product_pair_worker_compatibility_checks
            WHERE product_pair_id = ? AND worker_id = ?
            ORDER BY checked_at DESC, compatibility_check_id DESC
            LIMIT 1
            """,
            [product_pair_id, worker_id],
        ).fetchone()
        if row is None:
            return None
        return {
            "compatibility_check_id": row[0],
            "checked_by": row[1],
            "checked_at": row[2],
            "reference_symbol": row[3],
            "reference_specification": json.loads(row[4]),
            "live_specification": None if row[5] is None else json.loads(row[5]),
            "hard_block_differences": json.loads(row[6]),
            "warning_differences": json.loads(row[7]),
        }

    def _product_pair_worker_exclusion(
        self,
        product_pair_id: str,
        worker_id: str,
    ) -> dict[str, Any] | None:
        row = self._connection.execute(
            """
            SELECT excluded_by, compatibility_check_id, excluded_at
            FROM product_pair_worker_exclusions
            WHERE product_pair_id = ? AND worker_id = ?
            """,
            [product_pair_id, worker_id],
        ).fetchone()
        if row is None:
            return None
        return {
            "excluded_by": row[0],
            "compatibility_check_id": row[1],
            "excluded_at": row[2],
        }

    def _require_running_product_pair_retest(self, retest_id: str) -> None:
        row = self._connection.execute(
            "SELECT status FROM product_pair_retests WHERE retest_id = ?",
            [retest_id],
        ).fetchone()
        if row is None:
            raise LedgerError("Product pair re-test does not exist.")
        if row[0] != "running":
            raise LedgerError("Product pair re-test is no longer running.")

    def _latest_product_pair_retest(self, product_pair_id: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            """
            SELECT retest_id, product_pair_id, requested_by,
                   first_worker_id, first_login, first_server,
                   second_worker_id, second_login, second_server,
                   policy_snapshot, analysis_period, reference_specifications,
                   status, current_stage, retry_count, failure_reason,
                   first_catalog_evidence, second_catalog_evidence,
                   m15_screening_results, m1_verification_results,
                   requested_at, catalog_completed_at, m15_screened_at, m1_verified_at, completed_at
            FROM product_pair_retests
            WHERE product_pair_id = ?
            ORDER BY requested_at DESC, retest_id DESC
            LIMIT 1
            """,
            [product_pair_id],
        ).fetchone()
        if row is None:
            return None
        return self._product_pair_retest_from_row(row)

    def _product_pair_retest_from_row(self, row: Any) -> dict[str, Any]:
        return {
            "retest_id": row[0],
            "product_pair_id": row[1],
            "requested_by": row[2],
            "source_workers": {
                "first_worker": {"worker_id": row[3], "login": row[4], "server": row[5]},
                "second_worker": {"worker_id": row[6], "login": row[7], "server": row[8]},
            },
            "policy_snapshot": json.loads(row[9]),
            "analysis_period": json.loads(row[10]),
            "reference_specifications": json.loads(row[11]),
            "status": row[12],
            "current_stage": row[13],
            "retry_count": row[14],
            "failure_reason": row[15],
            "first_catalog_evidence": None if row[16] is None else json.loads(row[16]),
            "second_catalog_evidence": None if row[17] is None else json.loads(row[17]),
            "m15_screening_results": json.loads(row[18]),
            "m1_verification_results": json.loads(row[19]),
            "requested_at": row[20],
            "catalog_completed_at": row[21],
            "m15_screened_at": row[22],
            "m1_verified_at": row[23],
            "completed_at": row[24],
        }

    def alerts(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT alert_id, worker_id, product_pair_id, enrollment_id, priority, alert_type, reason, occurred_at
                FROM alerts
                ORDER BY alert_id
                """
            ).fetchall()
        return [
            {"alert_id": row[0], "worker_id": row[1], "product_pair_id": row[2], "enrollment_id": row[3], "priority": row[4], "alert_type": row[5],
             "reason": row[6], "occurred_at": row[7]}
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

    def event_page(self, *, limit: int, cursor: str | None, event_type: str | None, query: str | None) -> dict[str, Any]:
        cursor_values = _decode_page_cursor(cursor, "events")
        clauses: list[str] = []
        parameters: list[Any] = []
        if event_type is not None:
            clauses.append("event_type = ?")
            parameters.append(event_type)
        if query is not None:
            clauses.append("lower(concat_ws(' ', event_type, CAST(payload AS VARCHAR))) LIKE ?")
            parameters.append(f"%{query.lower()}%")
        if cursor_values is not None:
            clauses.append("(occurred_at < ? OR (occurred_at = ? AND event_id < ?))")
            parameters.extend([cursor_values["timestamp"], cursor_values["timestamp"], cursor_values["id"]])
        where = "" if not clauses else f"WHERE {' AND '.join(clauses)}"
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT event_id, event_type, payload, occurred_at FROM events {where}
                ORDER BY occurred_at DESC, event_id DESC
                LIMIT ?
                """,
                [*parameters, limit + 1],
            ).fetchall()
        has_more = len(rows) > limit
        items = [
            {"event_id": row[0], "event_type": row[1], "payload": json.loads(row[2]), "occurred_at": row[3]}
            for row in rows[:limit]
        ]
        return _page(items, has_more, "events", "occurred_at", "event_id")

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
        product_pair_id: str | None = None,
        enrollment_id: str | None = None,
    ) -> None:
        self._connection.execute(
            """
            INSERT INTO alerts (worker_id, product_pair_id, enrollment_id, priority, alert_type, reason, occurred_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [worker_id, product_pair_id, enrollment_id, priority, alert_type, reason, _utc_now()],
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
                CREATE TABLE IF NOT EXISTS management_intent_commands (
                    username VARCHAR NOT NULL,
                    command_id VARCHAR NOT NULL,
                    payload_hash VARCHAR NOT NULL,
                    result JSON NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (username, command_id)
                );
                CREATE TABLE IF NOT EXISTS trader_intents (
                    intent_id VARCHAR PRIMARY KEY,
                    trader_id VARCHAR NOT NULL,
                    command_id VARCHAR NOT NULL,
                    pair_id VARCHAR NOT NULL,
                    intent JSON NOT NULL,
                    preflight JSON NOT NULL,
                    status VARCHAR NOT NULL,
                    accepted_at TIMESTAMPTZ NOT NULL,
                    UNIQUE (trader_id, command_id)
                );
                CREATE TABLE IF NOT EXISTS trader_intent_execution_records (
                    intent_id VARCHAR NOT NULL,
                    event_id BIGINT PRIMARY KEY,
                    event_type VARCHAR NOT NULL,
                    payload JSON NOT NULL,
                    recorded_at TIMESTAMPTZ NOT NULL
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
                    safety_state VARCHAR NOT NULL DEFAULT 'connected',
                    revoked_at TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS worker_freezes (
                    freeze_id VARCHAR PRIMARY KEY,
                    worker_id VARCHAR NOT NULL,
                    source VARCHAR NOT NULL,
                    affected_worker_ids JSON NOT NULL,
                    audit JSON NOT NULL,
                    frozen_at TIMESTAMPTZ NOT NULL,
                    event_id BIGINT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS certificate_overlaps (
                    role VARCHAR NOT NULL,
                    identity_id VARCHAR NOT NULL,
                    certificate VARCHAR NOT NULL,
                    public_key_pem VARCHAR NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (role, identity_id, certificate)
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
                CREATE TABLE IF NOT EXISTS worker_live_state (
                    worker_id VARCHAR PRIMARY KEY,
                    observed_at VARCHAR NOT NULL,
                    connectivity BOOLEAN NOT NULL,
                    quotes JSON NOT NULL,
                    orders JSON NOT NULL,
                    positions JSON NOT NULL,
                    received_at TIMESTAMPTZ NOT NULL
                );
                CREATE TABLE IF NOT EXISTS manual_trading_target (
                    singleton BOOLEAN PRIMARY KEY CHECK (singleton = TRUE),
                    pair_id VARCHAR NOT NULL,
                    first_worker_id VARCHAR NOT NULL,
                    second_worker_id VARCHAR NOT NULL,
                    leg_order VARCHAR NOT NULL,
                    interval_seconds BIGINT NOT NULL CHECK (interval_seconds >= 0),
                    revision BIGINT NOT NULL,
                    active_manual_trade_id VARCHAR,
                    configured_by VARCHAR NOT NULL,
                    configured_at TIMESTAMPTZ NOT NULL
                );
                CREATE TABLE IF NOT EXISTS worker_recovery_commands (
                    username VARCHAR NOT NULL,
                    command_id VARCHAR NOT NULL,
                    payload_hash VARCHAR NOT NULL,
                    worker_id VARCHAR NOT NULL,
                    operation VARCHAR NOT NULL,
                    result JSON NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (username, command_id)
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
                    product_pair_id VARCHAR,
                    enrollment_id VARCHAR,
                    priority VARCHAR NOT NULL,
                    alert_type VARCHAR NOT NULL,
                    reason VARCHAR NOT NULL,
                    occurred_at TIMESTAMPTZ NOT NULL
                );
                CREATE TABLE IF NOT EXISTS product_catalog_analyses (
                    analysis_id VARCHAR PRIMARY KEY,
                    requested_by VARCHAR NOT NULL,
                    first_worker_id VARCHAR NOT NULL,
                    first_login BIGINT NOT NULL,
                    first_server VARCHAR NOT NULL,
                    second_worker_id VARCHAR NOT NULL,
                    second_login BIGINT NOT NULL,
                    second_server VARCHAR NOT NULL,
                    policy JSON NOT NULL,
                    status VARCHAR NOT NULL,
                    current_stage VARCHAR NOT NULL DEFAULT 'catalog',
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    analysis_period JSON NOT NULL DEFAULT '{"ended_at_utc":"","started_at_utc":"","timeframe":"M15"}',
                    failure_reason VARCHAR,
                    first_catalog_evidence JSON,
                    second_catalog_evidence JSON,
                    eligible_candidates JSON NOT NULL DEFAULT '[]',
                    m15_screening_results JSON NOT NULL DEFAULT '[]',
                    m1_verification_results JSON NOT NULL DEFAULT '[]',
                    requested_at TIMESTAMPTZ NOT NULL,
                    catalog_completed_at TIMESTAMPTZ,
                    m15_screened_at TIMESTAMPTZ,
                    m1_verified_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS product_pair_build_confirmations (
                    confirmation_id VARCHAR PRIMARY KEY,
                    analysis_id VARCHAR NOT NULL,
                    requested_by VARCHAR NOT NULL,
                    first_server VARCHAR NOT NULL,
                    first_symbol VARCHAR NOT NULL,
                    second_server VARCHAR NOT NULL,
                    second_symbol VARCHAR NOT NULL,
                    confirmation JSON NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    used_at TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS product_pairs (
                    product_pair_id VARCHAR PRIMARY KEY,
                    status VARCHAR NOT NULL,
                    endpoint_a_server VARCHAR NOT NULL,
                    endpoint_a_symbol VARCHAR NOT NULL,
                    endpoint_b_server VARCHAR NOT NULL,
                    endpoint_b_symbol VARCHAR NOT NULL,
                    active_pair_key VARCHAR,
                    lot_relationship JSON NOT NULL,
                    policy_snapshot JSON NOT NULL,
                    analysis_period JSON NOT NULL,
                    reference_specifications JSON NOT NULL,
                    approval_evidence JSON NOT NULL,
                    source_workers JSON NOT NULL,
                    built_from_analysis_id VARCHAR NOT NULL,
                    built_from_confirmation_id VARCHAR NOT NULL,
                    built_by VARCHAR NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    retired_at TIMESTAMPTZ,
                    retired_by VARCHAR,
                    retired_reason VARCHAR,
                    replaced_by_product_pair_id VARCHAR,
                    replaces_product_pair_id VARCHAR
                );
                CREATE TABLE IF NOT EXISTS product_pair_worker_compatibility_checks (
                    compatibility_check_id VARCHAR PRIMARY KEY,
                    product_pair_id VARCHAR NOT NULL,
                    worker_id VARCHAR NOT NULL,
                    server VARCHAR NOT NULL,
                    reference_symbol VARCHAR NOT NULL,
                    checked_by VARCHAR NOT NULL,
                    reference_specification JSON NOT NULL,
                    live_specification JSON,
                    hard_block_differences JSON NOT NULL,
                    warning_differences JSON NOT NULL,
                    checked_at TIMESTAMPTZ NOT NULL
                );
                CREATE TABLE IF NOT EXISTS product_pair_worker_exclusions (
                    product_pair_id VARCHAR NOT NULL,
                    worker_id VARCHAR NOT NULL,
                    excluded_by VARCHAR NOT NULL,
                    compatibility_check_id VARCHAR,
                    excluded_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (product_pair_id, worker_id)
                );
                CREATE TABLE IF NOT EXISTS product_pair_retests (
                    retest_id VARCHAR PRIMARY KEY,
                    product_pair_id VARCHAR NOT NULL,
                    requested_by VARCHAR NOT NULL,
                    first_worker_id VARCHAR NOT NULL,
                    first_login BIGINT NOT NULL,
                    first_server VARCHAR NOT NULL,
                    second_worker_id VARCHAR NOT NULL,
                    second_login BIGINT NOT NULL,
                    second_server VARCHAR NOT NULL,
                    policy_snapshot JSON NOT NULL,
                    analysis_period JSON NOT NULL,
                    reference_specifications JSON NOT NULL,
                    status VARCHAR NOT NULL,
                    current_stage VARCHAR NOT NULL DEFAULT 'catalog',
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    failure_reason VARCHAR,
                    first_catalog_evidence JSON,
                    second_catalog_evidence JSON,
                    m15_screening_results JSON NOT NULL DEFAULT '[]',
                    m1_verification_results JSON NOT NULL DEFAULT '[]',
                    requested_at TIMESTAMPTZ NOT NULL,
                    catalog_completed_at TIMESTAMPTZ,
                    m15_screened_at TIMESTAMPTZ,
                    m1_verified_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ
                );
                """
            )
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
            self._connection.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS product_pair_id VARCHAR")
            self._connection.execute("ALTER TABLE enrollments DROP COLUMN IF EXISTS pairing_code")
            self._connection.execute(
                "ALTER TABLE enrollments ADD COLUMN IF NOT EXISTS password_secret_deleted_at TIMESTAMPTZ"
            )
            self._connection.execute("ALTER TABLE workers ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ")
            self._connection.execute("ALTER TABLE workers ADD COLUMN IF NOT EXISTS safety_state VARCHAR DEFAULT 'connected'")
            self._connection.execute("ALTER TABLE workers ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ")
            self._connection.execute("ALTER TABLE product_pairs ADD COLUMN IF NOT EXISTS active_pair_key VARCHAR")
            self._connection.execute(
                """
                UPDATE product_pairs
                SET active_pair_key = json_array(
                    json_array(endpoint_a_server, endpoint_a_symbol),
                    json_array(endpoint_b_server, endpoint_b_symbol)
                )
                WHERE status = 'active' AND active_pair_key IS NULL
                """
            )
            self._connection.execute(
                "UPDATE product_pairs SET active_pair_key = NULL WHERE status <> 'active' AND active_pair_key IS NOT NULL"
            )
            self._connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS product_pairs_active_pair_key_idx ON product_pairs(active_pair_key)"
            )
            self._connection.execute("UPDATE workers SET safety_state = 'connected' WHERE safety_state IS NULL")
            self._connection.execute("ALTER TABLE product_catalog_analyses ADD COLUMN IF NOT EXISTS current_stage VARCHAR DEFAULT 'catalog'")
            self._connection.execute("ALTER TABLE product_catalog_analyses ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0")
            self._connection.execute("ALTER TABLE product_catalog_analyses DROP COLUMN IF EXISTS exceptions")
            self._connection.execute(
                """ALTER TABLE product_catalog_analyses
                ADD COLUMN IF NOT EXISTS analysis_period JSON DEFAULT '{"ended_at_utc":"","started_at_utc":"","timeframe":"M15"}'"""
            )
            self._connection.execute(
                "ALTER TABLE product_catalog_analyses ADD COLUMN IF NOT EXISTS m15_screening_results JSON DEFAULT '[]'"
            )
            self._connection.execute(
                "ALTER TABLE product_catalog_analyses ADD COLUMN IF NOT EXISTS m1_verification_results JSON DEFAULT '[]'"
            )
            self._connection.execute(
                "ALTER TABLE product_catalog_analyses ADD COLUMN IF NOT EXISTS catalog_completed_at TIMESTAMPTZ"
            )
            self._connection.execute(
                "ALTER TABLE product_catalog_analyses ADD COLUMN IF NOT EXISTS m15_screened_at TIMESTAMPTZ"
            )
            self._connection.execute(
                "ALTER TABLE product_catalog_analyses ADD COLUMN IF NOT EXISTS m1_verified_at TIMESTAMPTZ"
            )
            self._connection.execute(
                "UPDATE product_catalog_analyses SET current_stage = 'catalog' WHERE current_stage IS NULL"
            )
            self._connection.execute("UPDATE product_catalog_analyses SET retry_count = 0 WHERE retry_count IS NULL")
            self._connection.execute(
                """UPDATE product_catalog_analyses
                SET analysis_period = '{"ended_at_utc":"","started_at_utc":"","timeframe":"M15"}'
                WHERE analysis_period IS NULL"""
            )
            self._connection.execute(
                "UPDATE product_catalog_analyses SET m15_screening_results = '[]' WHERE m15_screening_results IS NULL"
            )
            self._connection.execute(
                "ALTER TABLE trader_enrollments ADD COLUMN IF NOT EXISTS approved_by VARCHAR"
            )
            self._connection.execute(
                "ALTER TABLE trader_enrollments ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ"
            )
            self._connection.execute(
                "ALTER TABLE trader_enrollments ADD COLUMN IF NOT EXISTS attestation_provider VARCHAR DEFAULT 'unverified'"
            )
            self._connection.execute(
                "UPDATE product_catalog_analyses SET m1_verification_results = '[]' WHERE m1_verification_results IS NULL"
            )
            self._migrate_alert_enrollment_reference()
            self._migrate_reconciliation_snapshots()

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


def _product_pair_worker_inspection_status(latest_check: dict[str, Any] | None) -> str:
    if latest_check is None:
        return "uninspected"
    if latest_check["hard_block_differences"] or latest_check["warning_differences"]:
        return "differences_detected"
    return "compatible"


def _active_product_pair_key(endpoints: list[dict[str, Any]]) -> str:
    return json.dumps(
        [
            [str(endpoints[0]["server"]), str(endpoints[0]["symbol"])],
            [str(endpoints[1]["server"]), str(endpoints[1]["symbol"])],
        ],
        separators=(",", ":"),
    )


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


def _quote_with_receipt(quote: object, received_at: datetime) -> object:
    if not isinstance(quote, dict):
        return quote
    return {**quote, "controller_received_at": received_at.isoformat()}


def _summary_string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _summary_number(value: object) -> int | float | None:
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _summary_boolean(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _utc_now() -> datetime:
    return datetime.now(UTC)
