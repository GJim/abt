from __future__ import annotations

import asyncio
from anyio import BrokenResourceError, ClosedResourceError
from concurrent.futures import Future as ConcurrentFuture
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
import hashlib
import json
import logging
import math
import queue
from contextlib import asynccontextmanager
from pathlib import Path
import secrets
from statistics import median
from time import monotonic
from typing import Annotated, Any, Callable, Literal, cast
from uuid import uuid4

from fastapi import Cookie, FastAPI, Header, HTTPException, Query, Response, WebSocket, WebSocketDisconnect, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from ..trader_protocol import (
    PAIR_CELL_CONTROL_MESSAGE_TYPES,
    pair_cell_control_message_adapter,
    pair_cell_relay_envelope_adapter,
)

from .backup import BackupManager
from .crypto import (
    ProofError,
    parse_device_certificate,
    verify_enrollment_proof,
    verify_worker_proof,
    verify_worker_rotation_proof,
)
from .ledger import AuthenticationError, ControlLedger, LedgerError, _hash
from .secrets import DeviceCertificateIssuer, DeviceCertificateVerifier, SecretStore, SecretStoreError

_LOGGER = logging.getLogger(__name__)
_MARKET_DATA_REQUEST_TIMEOUT_SECONDS = 30


def _verify_worker_certificate(certificate_verifier: DeviceCertificateVerifier | None, worker: Any) -> None:
    claims = parse_device_certificate(worker.certificate)
    if certificate_verifier is None:
        raise ProofError("The device certificate verifier is unavailable.")
    certificate_verifier.verify(worker.certificate)
    if (
        claims["worker_id"] != worker.worker_id
        or claims["login"] != worker.login
        or claims["server"] != worker.server
        or claims["public_key_pem"] != worker.public_key_pem
    ):
        raise ProofError("The device certificate does not match this worker.")




class LoginRequest(BaseModel):
    username: str = Field(pattern=r"^[A-Z]{6}$")
    password: str = Field(min_length=20)


class PairQuarantineReleaseRequest(BaseModel):
    """One operator's explicit, audited release of one quarantined product.

    The pair is named by its ``route_id`` alone -- a Pair Execution Cell
    route binds no Trader identity -- and ``actor`` is intentionally not a
    client-supplied field: it is always the authenticated admin session's own
    username, so the audit trail cannot be forged by the request body.
    """

    model_config = ConfigDict(extra="forbid")

    route_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    reason: str = Field(min_length=1)


class EnrollmentRequest(BaseModel):
    registration_invite: str = Field(min_length=1, max_length=512)
    login: int = Field(gt=0)
    server: str = Field(min_length=1, max_length=128)
    account_info: dict[str, object]
    terminal_info: dict[str, object]
    mt5_password: str = Field(min_length=1)
    enrollment_challenge: str = Field(min_length=1, max_length=256)
    public_key_pem: str = Field(min_length=1)
    proof_signature: str = Field(min_length=1)

    @field_validator("server")
    @classmethod
    def reject_control_characters(cls, value: str) -> str:
        if any(character in value for character in "\r\n"):
            raise ValueError("server must not contain control characters")
        return value


class RegistrationInviteRequest(BaseModel):
    role: Literal["worker", "trader"]




class WorkerRotationChallengeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str = Field(min_length=1)
    public_key_pem: str = Field(min_length=1)


@dataclass(frozen=True)
class _WorkerRotationChallenge:
    nonce: str
    replacement_public_key_pem: str
    expires_at: datetime


class WorkerRotationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_config = ConfigDict(extra="forbid")

    worker_id: str = Field(min_length=1)
    public_key_pem: str = Field(min_length=1)
    old_key_signature: str = Field(min_length=1)
    replacement_key_signature: str = Field(min_length=1)


def _operations_dashboard(
    *,
    workers: list[dict[str, Any]],
    alerts: list[dict[str, Any]],
    pending_enrollments: list[dict[str, Any]],
) -> dict[str, object]:
    classified_alerts = [
        {
            **alert,
            "category": "intervention_required" if alert["priority"] in {"critical", "high"} else "informational",
            "classification_reason": alert["reason"],
        }
        for alert in alerts
    ]
    classified_enrollments = [
        {
            **enrollment,
            "category": "intervention_required",
            "classification_reason": "approval_required",
        }
        for enrollment in pending_enrollments
    ]
    classified_workers = [
        {
            **worker,
            **_worker_dashboard_classification(worker),
        }
        for worker in workers
    ]
    interventions = [
        {
            "item_type": "pending_enrollment",
            "item_id": enrollment["enrollment_id"],
            "category": enrollment["category"],
            "reason": enrollment["classification_reason"],
            "occurred_at": enrollment["created_at"],
        }
        for enrollment in classified_enrollments
    ]
    interventions.extend(
        {
            "item_type": "worker_alert",
            "item_id": alert["alert_id"],
            "category": alert["category"],
            "reason": alert["classification_reason"],
            "occurred_at": alert["occurred_at"],
            "worker_id": alert["worker_id"],
        }
        for alert in classified_alerts
        if alert["category"] == "intervention_required"
    )
    interventions.sort(key=lambda item: str(item["occurred_at"]), reverse=True)
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "interventions": interventions,
        "pending_enrollments": classified_enrollments,
        "workers": classified_workers,
        "alerts": classified_alerts,
    }


def _worker_dashboard_classification(worker: dict[str, Any]) -> dict[str, str]:
    if worker["connectivity"] == "stale":
        return {"category": "intervention_required", "classification_reason": "worker_stale"}
    if worker["connectivity"] == "revoked":
        return {"category": "informational", "classification_reason": "worker_revoked"}
    return {"category": "informational", "classification_reason": "worker_healthy"}


@dataclass
class _WorkerRelayRequest:
    request_id: str
    message: dict[str, object]
    future: ConcurrentFuture[dict[str, object]]
    started_at: float = field(default_factory=monotonic)
    expects_response: bool = True


@dataclass(eq=False)
class _WorkerSessionConnection:
    websocket: WebSocket
    outbound: queue.Queue[_WorkerRelayRequest | None] = field(default_factory=queue.Queue)
    pending: dict[str, _WorkerRelayRequest] = field(default_factory=dict)
    session_id: str = field(default_factory=lambda: str(uuid4()))
    superseded: bool = False


def create_app(
    ledger_path: Path,
    *,
    spa_directory: Path | None = None,
    secret_control_healthy: Callable[[], bool] = lambda: True,
    secret_store: SecretStore | None = None,
    certificate_issuer: DeviceCertificateIssuer | None = None,
    certificate_verifier: DeviceCertificateVerifier | None = None,
    backup_manager: BackupManager | None = None,
    backup_directory: Path | None = None,
    openbao_raft_directory: Path | None = None,
    softhsm_tokens_directory: Path | None = None,
) -> FastAPI:
    ledger = ControlLedger(ledger_path)
    backup_paths = (backup_directory, openbao_raft_directory, softhsm_tokens_directory)
    if backup_manager is None and all(backup_paths):
        backup_manager = BackupManager(
            ledger,
            backup_directory,
            openbao_raft_directory,
            softhsm_tokens_directory,
        )
    elif any(backup_paths):
        raise ValueError("All control-plane backup directories must be configured together.")
    if certificate_verifier is None and certificate_issuer is not None and callable(getattr(certificate_issuer, "verify", None)):
        certificate_verifier = cast(DeviceCertificateVerifier, certificate_issuer)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        cleanup_task = asyncio.create_task(_expire_pending_secrets(ledger, secret_store))
        backup_task = (
            asyncio.create_task(_create_hourly_backups(backup_manager)) if backup_manager is not None else None
        )
        try:
            yield
        finally:
            cleanup_task.cancel()
            tasks = [cleanup_task]
            if backup_task is not None:
                backup_task.cancel()
                tasks.append(backup_task)
            await asyncio.gather(*tasks, return_exceptions=True)
            ledger.close()

    app = FastAPI(title="abt control plane", version="0.1.0", lifespan=lifespan)
    app.state.ledger = ledger
    app.state.worker_rotation_challenges: dict[str, _WorkerRotationChallenge] = {}
    admin_notification_connections: set[WebSocket] = set()
    worker_connections: dict[str, set[_WorkerSessionConnection]] = {}

    @app.get("/health")
    def health() -> dict[str, str]:
        if not secret_control_healthy():
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Secret control plane is unavailable.")
        return {"status": "ok"}

    @app.post("/api/admin/login")
    def login(response: Response, body: LoginRequest) -> dict[str, str]:
        try:
            session = ledger.authenticate_admin(body.username, body.password)
        except AuthenticationError as error:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(error)) from error
        response.set_cookie(
            "abt_admin_session",
            session.token,
            httponly=True,
            secure=True,
            samesite="strict",
            max_age=8 * 60 * 60,
        )
        return {"csrf_token": session.csrf_token, "expires_at": session.expires_at.isoformat()}

    @app.get("/api/admin/session")
    def resume_session(abt_admin_session: Annotated[str | None, Cookie()] = None) -> dict[str, str]:
        if abt_admin_session is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Administrator login is required.")
        try:
            return {"csrf_token": ledger.resume_admin_session(abt_admin_session)}
        except AuthenticationError as error:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(error)) from error

    @app.post("/api/admin/registration-invites", status_code=status.HTTP_201_CREATED)
    def create_registration_invite(
        body: RegistrationInviteRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        invite = ledger.create_registration_invite(username, body.role)
        record = ledger.registration_invites()[0]
        return {**record, "invite": invite}

    @app.get("/api/admin/registration-invites")
    def list_registration_invites(
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> list[dict[str, object]]:
        _require_admin(ledger, abt_admin_session)
        return ledger.registration_invites()

    @app.post("/api/admin/registration-invites/{invite}/revoke", status_code=status.HTTP_204_NO_CONTENT)
    def revoke_registration_invite(
        invite: str,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> None:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            ledger.revoke_registration_invite(invite, username)
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error

    @app.post("/api/enrollments", status_code=status.HTTP_201_CREATED)
    async def create_enrollment(body: EnrollmentRequest) -> dict[str, str]:
        try:
            verify_enrollment_proof(
                body.public_key_pem,
                body.proof_signature,
                login=body.login,
                server=body.server,
                account_info=body.account_info,
                terminal_info=body.terminal_info,
                mt5_password=body.mt5_password,
                enrollment_challenge=body.enrollment_challenge,
            )
            if secret_store is None:
                raise SecretStoreError("The MT5 credential mediator is unavailable.")
            secret_ref = f"abt/data/mt5/pending/{uuid4()}"
            secret_store.write_password(secret_ref, body.mt5_password)
            try:
                enrollment = ledger.create_enrollment(
                    login=body.login,
                    server=body.server,
                    public_key_pem=body.public_key_pem,
                    account_info=body.account_info,
                    terminal_info=body.terminal_info,
                    password_secret_ref=secret_ref,
                    enrollment_challenge=body.enrollment_challenge,
                    registration_invite=body.registration_invite,
                )
            except LedgerError:
                secret_store.delete_password(secret_ref)
                raise
            enrollment_summary = next(
                item for item in ledger.pending_enrollments() if item["enrollment_id"] == enrollment.enrollment_id
            )
            await _broadcast_pending_enrollment(
                admin_notification_connections,
                cast(dict[str, object], jsonable_encoder(enrollment_summary)),
            )
            await _broadcast_management_snapshot(ledger, admin_notification_connections)
            return {"enrollment_id": enrollment.enrollment_id, "expires_at": enrollment.expires_at.isoformat()}
        except ProofError as error:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)) from error
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error
        except SecretStoreError as error:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)) from error

    @app.get("/api/enrollments/{enrollment_id}/status")
    def enrollment_status(enrollment_id: str) -> dict[str, str]:
        try:
            return {"status": ledger.enrollment_status(enrollment_id)}
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error)) from error


    @app.post("/api/workers/certificates/rotation-challenge")
    def issue_worker_rotation_challenge(body: WorkerRotationChallengeRequest) -> dict[str, str]:
        try:
            worker = ledger.active_worker(body.worker_id)
            _verify_worker_certificate(certificate_verifier, worker)
            nonce = secrets.token_urlsafe(32)
            app.state.worker_rotation_challenges[body.worker_id] = _WorkerRotationChallenge(
                nonce=nonce,
                replacement_public_key_pem=body.public_key_pem,
                expires_at=datetime.now(UTC) + timedelta(minutes=2),
            )
            return {"purpose": "worker_certificate_rotation", "worker_id": worker.worker_id, "nonce": nonce}
        except (LedgerError, ProofError) as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error

    @app.post("/api/workers/certificates/rotate")
    def rotate_worker_certificate(body: WorkerRotationRequest) -> dict[str, str]:
        try:
            worker = ledger.active_worker(body.worker_id)
            _verify_worker_certificate(certificate_verifier, worker)
            challenge = app.state.worker_rotation_challenges.pop(body.worker_id, None)
            if (
                challenge is None
                or challenge.replacement_public_key_pem != body.public_key_pem
                or datetime.now(UTC) >= challenge.expires_at
            ):
                raise ProofError("The Worker rotation challenge is invalid or expired.")
            verify_worker_rotation_proof(
                worker.public_key_pem, body.old_key_signature, worker_id=worker.worker_id,
                replacement_public_key_pem=challenge.replacement_public_key_pem, nonce=challenge.nonce,
            )
            verify_worker_rotation_proof(
                challenge.replacement_public_key_pem, body.replacement_key_signature, worker_id=worker.worker_id,
                replacement_public_key_pem=challenge.replacement_public_key_pem, nonce=challenge.nonce,
            )
            if certificate_issuer is None:
                raise SecretStoreError("The device certificate issuer is unavailable.")
            certificate = ledger.rotate_worker_certificate(
                worker.worker_id, challenge.replacement_public_key_pem,
                lambda worker_id, login, server, public_key_pem: certificate_issuer.issue(
                    worker_id=worker_id, login=login, server=server, public_key_pem=public_key_pem
                ),
            )
            return {"worker_id": worker.worker_id, "certificate": certificate}
        except (LedgerError, ProofError, SecretStoreError) as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error

    @app.get("/api/enrollment-challenge")
    def enrollment_challenge() -> dict[str, str]:
        challenge, expires_at = ledger.issue_enrollment_challenge()
        return {"challenge": challenge, "expires_at": expires_at.isoformat()}

    @app.websocket("/api/admin/notifications")
    async def admin_notifications(websocket: WebSocket) -> None:
        try:
            _require_admin(ledger, websocket.cookies.get("abt_admin_session"))
        except HTTPException:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        await websocket.accept()
        admin_notification_connections.add(websocket)
        try:
            await websocket.send_json(
                {"type": "pending_enrollments", "items": jsonable_encoder(ledger.pending_enrollments())}
            )
            while (await websocket.receive())["type"] != "websocket.disconnect":
                pass
        finally:
            admin_notification_connections.discard(websocket)

    @app.get("/api/admin/enrollments")
    def list_enrollments(
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> list[dict[str, object]]:
        _require_admin(ledger, abt_admin_session)
        if secret_store is None:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="The MT5 credential mediator is unavailable.")
        try:
            _delete_expired_pending_secrets(ledger, secret_store)
        except SecretStoreError as error:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)) from error
        return ledger.pending_enrollments()

    @app.get("/api/admin/events")
    def list_events(
        limit: Annotated[int, Query(ge=1, le=50)] = 50,
        cursor: Annotated[str | None, Query(min_length=1, max_length=512)] = None,
        page: Annotated[int | None, Query(ge=1)] = None,
        event_type: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
        q: Annotated[str | None, Query(min_length=1, max_length=256)] = None,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> dict[str, object]:
        _require_admin(ledger, abt_admin_session)
        try:
            return ledger.event_page(limit=limit, cursor=cursor, page=page, event_type=event_type, query=q)
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)) from error

    @app.get("/api/admin/workers")
    def list_workers(
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> list[dict[str, object]]:
        _require_admin(ledger, abt_admin_session)
        return ledger.worker_reconciliation()

    @app.get("/api/admin/alerts")
    def list_alerts(
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> list[dict[str, object]]:
        _require_admin(ledger, abt_admin_session)
        return ledger.alerts()

    @app.get("/api/admin/operations-dashboard")
    def operations_dashboard(
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> dict[str, object]:
        _require_admin(ledger, abt_admin_session)
        if secret_store is None:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="The MT5 credential mediator is unavailable.")
        try:
            _delete_expired_pending_secrets(ledger, secret_store)
        except SecretStoreError as error:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)) from error
        return _operations_dashboard(
            workers=ledger.worker_reconciliation(),
            alerts=ledger.alerts(),
            pending_enrollments=ledger.pending_enrollments(),
        )

    @app.post("/api/admin/workers/{worker_id}/revoke", status_code=status.HTTP_204_NO_CONTENT)
    async def revoke_worker(
        worker_id: str,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> Response:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            ledger.revoke_worker(worker_id, username)
            _create_pki_backup(backup_manager)
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error
        connections = tuple(worker_connections.pop(worker_id, set()))
        await asyncio.gather(
            *(connection.websocket.close(code=status.WS_1008_POLICY_VIOLATION) for connection in connections),
            return_exceptions=True,
        )
        return Response(status_code=status.HTTP_204_NO_CONTENT)


    @app.post("/api/admin/pairs/quarantine/release")
    async def release_pair_quarantine_route(
        payload: PairQuarantineReleaseRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        """Explicitly, audibly release one quarantined product for a Worker pair.

        This is an authenticated route-level operator action, not lifecycle
        ownership: it is authorized against the same live ``route_id`` as any
        relayed envelope, names no Trader, and the controller neither tracks
        quarantines nor decides whether a release may take effect.
        Quarantine is per-Worker durable state with no controller-side
        mirror, and ``PairExecutionCell`` silently rejects a release while an
        attempt is unresolved -- it never raises and never reports whether a
        release actually took effect. So this route never claims success on
        a fire-and-forget push alone: it sends a correlated request to both
        the leader and follower Worker session and awaits each one's own
        genuine applied/rejected outcome (a Worker that never had the symbol
        quarantined counts as applied -- there is nothing to release there),
        and only reports success if *both* applied. A rejection, a timeout,
        or either Worker being disconnected is reported as a failure with
        whatever per-worker outcome is known, and the request may always be
        safely retried once the blocking condition (unresolved attempt,
        disconnected Worker) is resolved -- releasing an already-released (or
        never-quarantined) product is itself always a safe, applied no-op.
        """

        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            release = ledger.release_pair_quarantine(
                route_id=payload.route_id,
                symbol=payload.symbol,
                actor=username,
                reason=payload.reason,
            )
            leader_connection = _connected_worker_session(
                worker_connections,
                cast(str, release["leader_worker_id"]),
                reason="The leader Worker must be connected to receive the quarantine release.",
            )
            follower_connection = _connected_worker_session(
                worker_connections,
                cast(str, release["follower_worker_id"]),
                reason="The follower Worker must be connected to receive the quarantine release.",
            )
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

        release = jsonable_encoder(release)
        leader_outcome, follower_outcome = await asyncio.gather(
            _request_pair_cell_quarantine_release(leader_connection, release, request_id=str(uuid4())),
            _request_pair_cell_quarantine_release(follower_connection, release, request_id=str(uuid4())),
        )
        applied = leader_outcome == "applied" and follower_outcome == "applied"
        ledger.record_pair_quarantine_release_outcome(
            route_id=payload.route_id,
            symbol=payload.symbol,
            actor=username,
            leader_outcome=leader_outcome,
            follower_outcome=follower_outcome,
            applied=applied,
        )
        body = {
            "status": "released" if applied else "rejected",
            "route_id": payload.route_id,
            "symbol": release["symbol"],
            "actor": release["actor"],
            "reason": release["reason"],
            "leader_outcome": leader_outcome,
            "follower_outcome": follower_outcome,
        }
        if not applied:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=body)
        return body

    @app.post("/api/admin/logout", status_code=status.HTTP_204_NO_CONTENT)
    def logout(
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> Response:
        _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        assert abt_admin_session is not None
        ledger.logout_admin(abt_admin_session)
        response = Response(status_code=status.HTTP_204_NO_CONTENT)
        response.delete_cookie("abt_admin_session", secure=True, samesite="strict")
        return response

    @app.post("/api/admin/enrollments/{enrollment_id}/approve")
    def approve_enrollment(
        enrollment_id: str,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, str]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            if certificate_issuer is None:
                raise SecretStoreError("The device certificate issuer is unavailable.")
            result = {
                "worker_id": ledger.approve_enrollment(
                    enrollment_id,
                    username,
                    lambda worker_id, login, server, public_key_pem: certificate_issuer.issue(
                        worker_id=worker_id,
                        login=login,
                        server=server,
                        public_key_pem=public_key_pem,
                    ),
                )
            }
            _create_pki_backup(backup_manager)
            return result
        except (LedgerError, SecretStoreError) as error:
            _LOGGER.warning("Worker enrollment approval failed for %s: %s", enrollment_id, error)
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error

    @app.post("/api/admin/enrollments/{enrollment_id}/reject", status_code=status.HTTP_204_NO_CONTENT)
    def reject_enrollment(
        enrollment_id: str,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> Response:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            if secret_store is None:
                raise SecretStoreError("The MT5 credential mediator is unavailable.")
            secret_ref = ledger.reject_enrollment(enrollment_id, username)
            secret_store.delete_password(secret_ref)
            ledger.mark_pending_password_deleted(secret_ref)
        except (LedgerError, SecretStoreError) as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @app.websocket("/api/worker/certificate")
    async def deliver_certificate(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            request = await _receive_exact_message(websocket, {"enrollment_id"})
            enrollment_id = _required_text(request, "enrollment_id")
            worker = ledger.active_worker_for_enrollment(enrollment_id)
            nonce = secrets.token_urlsafe(32)
            await websocket.send_json(
                {"purpose": "certificate_delivery", "worker_id": worker.worker_id, "nonce": nonce}
            )
            proof = await _receive_exact_message(websocket, {"signature"})
            verify_worker_proof(
                worker.public_key_pem,
                _required_text(proof, "signature"),
                purpose="certificate_delivery",
                worker_id=worker.worker_id,
                nonce=nonce,
            )
            await websocket.send_json({"worker_id": worker.worker_id, "certificate": worker.certificate})
        except WebSocketDisconnect as error:
            _LOGGER.info("Worker certificate delivery disconnected with code %s.", error.code)
            await _close_policy_violation(websocket)
        except (LedgerError, ProofError, ValueError) as error:
            _LOGGER.warning("Worker certificate delivery failed: %s", error)
            await _close_policy_violation(websocket)
        except Exception:
            _LOGGER.exception("Worker certificate delivery failed unexpectedly.")
            await _close_policy_violation(websocket)


    @app.websocket("/api/worker/credentials")
    async def mediate_password(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            request = await _receive_exact_message(websocket, {"worker_id", "certificate"})
            worker_id = _required_text(request, "worker_id")
            certificate = _required_text(request, "certificate")
            claims = parse_device_certificate(certificate)
            if certificate_verifier is None:
                raise ProofError("The device certificate verifier is unavailable.")
            certificate_verifier.verify(certificate)
            worker = ledger.worker_for_certificate(worker_id, certificate)
            if (
                certificate != worker.certificate
                or claims["worker_id"] != worker.worker_id
                or claims["login"] != worker.login
                or claims["server"] != worker.server
                or claims["public_key_pem"] != worker.public_key_pem
            ):
                raise ProofError("The device certificate does not match this worker.")
            nonce = secrets.token_urlsafe(32)
            await websocket.send_json(
                {"purpose": "password_request", "worker_id": worker.worker_id, "nonce": nonce}
            )
            proof = await _receive_exact_message(websocket, {"signature"})
            verify_worker_proof(
                worker.public_key_pem,
                _required_text(proof, "signature"),
                purpose="password_request",
                worker_id=worker.worker_id,
                nonce=nonce,
            )
            if secret_store is None:
                raise SecretStoreError("The MT5 credential mediator is unavailable.")
            await websocket.send_json({"password": secret_store.read_password(worker.password_secret_ref)})
        except WebSocketDisconnect as error:
            _LOGGER.info("Worker password mediation disconnected with code %s.", error.code)
            await _close_policy_violation(websocket)
        except (LedgerError, ProofError, SecretStoreError, ValueError) as error:
            _LOGGER.warning("Worker password mediation failed: %s", error)
            await _close_policy_violation(websocket)
        except Exception:
            _LOGGER.exception("Worker password mediation failed unexpectedly.")
            await _close_policy_violation(websocket)

    @app.websocket("/api/worker/session")
    async def worker_session(websocket: WebSocket) -> None:
        await websocket.accept()
        sender_task: asyncio.Task[None] | None = None
        try:
            request = await _receive_exact_message(websocket, {"worker_id", "certificate"})
            worker_id = _required_text(request, "worker_id")
            certificate = _required_text(request, "certificate")
            claims = parse_device_certificate(certificate)
            if certificate_verifier is None:
                raise ProofError("The device certificate verifier is unavailable.")
            certificate_verifier.verify(certificate)
            worker = ledger.worker_for_certificate(worker_id, certificate)
            if (
                certificate != worker.certificate
                or claims["worker_id"] != worker.worker_id
                or claims["login"] != worker.login
                or claims["server"] != worker.server
                or claims["public_key_pem"] != worker.public_key_pem
            ):
                raise ProofError("The device certificate does not match this worker.")
            nonce = secrets.token_urlsafe(32)
            await websocket.send_json({"purpose": "worker_session", "worker_id": worker.worker_id, "nonce": nonce})
            proof = await _receive_exact_message(websocket, {"signature"})
            verify_worker_proof(
                worker.public_key_pem,
                _required_text(proof, "signature"),
                purpose="worker_session",
                worker_id=worker.worker_id,
                nonce=nonce,
            )
            connection = _WorkerSessionConnection(websocket)
            ledger.record_worker_session(worker.worker_id, connection.session_id)
            previous_connections = tuple(worker_connections.get(worker.worker_id, set()))
            for previous_connection in previous_connections:
                previous_connection.superseded = True
                ledger.record_worker_session_lifecycle(
                    "worker_session_superseded",
                    worker.worker_id,
                    previous_connection.session_id,
                    replacement_session_id=connection.session_id,
                )
                _fail_pending_worker_relay_requests(
                    previous_connection,
                    "Worker session was superseded by a newer authenticated connection.",
                )
                try:
                    await previous_connection.websocket.close(
                        code=4001, reason="Superseded by newer worker session."
                    )
                except (BrokenResourceError, ClosedResourceError):
                    pass
            worker_connections[worker.worker_id] = {connection}
            sender_task = asyncio.create_task(_send_worker_relay_requests(connection))
            await websocket.send_json(
                {"type": "authenticated", "worker_id": worker.worker_id, "cursor": ledger.reconciliation_cursor(worker.worker_id)}
            )
            while True:
                try:
                    request = await asyncio.wait_for(websocket.receive_json(), timeout=30)
                except asyncio.TimeoutError:
                    ledger.worker_for_certificate(worker.worker_id, certificate)
                    continue
                ledger.worker_for_certificate(worker.worker_id, certificate)
                if not isinstance(request, dict):
                    raise ValueError("Invalid protocol message.")
                if not _is_authoritative_worker_session(worker_connections, worker.worker_id, connection):
                    _LOGGER.info(
                        "Ignoring message from superseded worker session %s for worker %s.",
                        connection.session_id,
                        worker.worker_id,
                    )
                    continue
                message_type = request.get("type")
                if message_type == "password_request" and set(request) == {"type"} and secret_store is not None:
                    await websocket.send_json(
                        {"type": "password", "password": secret_store.read_password(worker.password_secret_ref)}
                    )
                elif (
                    message_type == "recovery_sync"
                    and set(request) == {"type", "epoch", "journal"}
                    and isinstance(request.get("epoch"), str)
                    and request["epoch"]
                    and isinstance(request.get("journal"), list)
                    and all(isinstance(item, dict) for item in request["journal"])
                ):
                    ledger.record_worker_connection_audit(
                        worker.worker_id, "worker_recovery_sync", str(request["epoch"])
                    )
                    await websocket.send_json({"type": "recovery_sync_accepted", "epoch": request["epoch"]})
                elif message_type == "heartbeat" and set(request) == {"type"}:
                    ledger.record_worker_heartbeat(worker.worker_id)
                    await websocket.send_json({"type": "heartbeat_ack"})
                elif (
                    message_type == "recovery_state"
                    and set(request) == {"type", "state", "reason"}
                    and isinstance(request.get("state"), str)
                    and isinstance(request.get("reason"), str)
                    and request["reason"]
                ):
                    ledger.record_worker_connection_audit(
                        worker.worker_id, str(request["state"]), str(request["reason"])
                    )
                    await websocket.send_json({"type": "accepted", "state": request["state"]})
                    await _broadcast_management_snapshot(ledger, admin_notification_connections)
                elif message_type == "live_state_snapshot":
                    _validate_worker_stream_envelope(request)
                    await websocket.send_json({"type": "live_state_accepted"})
                elif message_type == "live_state_diff":
                    _validate_worker_stream_envelope(request)
                    await websocket.send_json({"type": "live_state_accepted"})
                elif message_type == "worker_fact":
                    envelope = _worker_fact_envelope(request, worker.worker_id)
                    ledger.record_worker_fact_audit(worker.worker_id, envelope)
                    await websocket.send_json({"type": "accepted", "cursor": request["cursor"]})
                elif (
                    message_type == "pair_relay"
                    and set(request) == {"type", "request_id", "envelope"}
                    and isinstance(request.get("request_id"), str)
                    and request["request_id"]
                    and isinstance(request.get("envelope"), dict)
                ):
                    try:
                        result = await _relay_pair_cell_envelope(
                            ledger, worker_connections, worker.worker_id, cast(dict[str, Any], request["envelope"])
                        )
                        await websocket.send_json(
                            {"type": "pair_relay_ack", "request_id": request["request_id"], "accepted": True, "result": result}
                        )
                    except LedgerError as error:
                        await websocket.send_json(
                            {
                                "type": "pair_relay_ack",
                                "request_id": request["request_id"],
                                "accepted": False,
                                "reason": str(error),
                            }
                        )
                elif message_type in PAIR_CELL_CONTROL_MESSAGE_TYPES:
                    _handle_pair_cell_control_message(
                        ledger, worker_connections, worker.worker_id, connection, request
                    )
                elif (
                    message_type == "pair_cell_quarantine_release_result"
                    and set(request) == {"type", "request_id", "symbol", "outcome"}
                    and isinstance(request.get("request_id"), str)
                    and request["request_id"]
                    and isinstance(request.get("symbol"), str)
                    and request["symbol"]
                    and request.get("outcome") in ("applied", "rejected")
                ):
                    _record_pair_cell_quarantine_release_result(connection, request)
                else:
                    raise ValueError("Invalid protocol message.")
        except WebSocketDisconnect as error:
            _LOGGER.info("Worker session disconnected with code %s.", error.code)
            if "worker" in locals() and "connection" in locals():
                ledger.record_worker_session_lifecycle(
                    "worker_session_disconnected",
                    worker.worker_id,
                    connection.session_id,
                    close_code=error.code,
                    disposition="superseded" if connection.superseded else "disconnected",
                )
            if "worker" in locals() and not ("connection" in locals() and connection.superseded):
                _handle_worker_disconnect(
                    ledger,
                    worker.worker_id,
                    "worker_session_disconnected",
                    f"Worker session disconnected with code {error.code}.",
                    worker_connections,
                )
            await _close_policy_violation(websocket)
        except (LedgerError, ProofError, SecretStoreError, ValueError) as error:
            _LOGGER.warning("Worker session authentication or request failed: %s", error)
            if "worker" in locals() and "connection" in locals():
                ledger.record_worker_session_lifecycle(
                    "worker_session_failed",
                    worker.worker_id,
                    connection.session_id,
                    reason=str(error),
                    disposition="superseded" if connection.superseded else "failed",
                )
            if "worker" in locals() and not ("connection" in locals() and connection.superseded):
                _handle_worker_disconnect(
                    ledger,
                    worker.worker_id,
                    "worker_session_failure",
                    f"Worker session request failed: {error}",
                    worker_connections,
                )
            await _close_policy_violation(websocket)
        except Exception:
            _LOGGER.exception("Worker session authentication or request failed unexpectedly.")
            if "worker" in locals() and "connection" in locals():
                ledger.record_worker_session_lifecycle(
                    "worker_session_failed",
                    worker.worker_id,
                    connection.session_id,
                    reason="Unexpected worker session failure.",
                    disposition="superseded" if connection.superseded else "failed",
                )
            if "worker" in locals() and not ("connection" in locals() and connection.superseded):
                _handle_worker_disconnect(
                    ledger,
                    worker.worker_id,
                    "worker_session_failure",
                    "Worker session request failed unexpectedly.",
                    worker_connections,
                )
            await _close_policy_violation(websocket)
        finally:
            if sender_task is not None:
                connection.outbound.put(None)
                await asyncio.gather(sender_task, return_exceptions=True)
            if "connection" in locals():
                _fail_pending_worker_relay_requests(connection, "Worker disconnected during relay delivery.")
            connections = worker_connections.get(worker.worker_id) if "worker" in locals() else None
            if connections is not None and "connection" in locals():
                connections.discard(connection)
                if not connections:
                    worker_connections.pop(worker.worker_id, None)

    if spa_directory is not None:
        def management_spa_route() -> FileResponse:
            return FileResponse(spa_directory / "index.html")

        for spa_route in (
            "/analysis", "/analysis/{analysis_path:path}", "/audit", "/workers", "/traders", "/intents",
            "/pairs/{pair_status:path}",
        ):
            app.add_api_route(spa_route, management_spa_route, methods=["GET"], include_in_schema=False)

        app.mount("/", StaticFiles(directory=spa_directory, html=True), name="management-spa")

    return app


async def _expire_pending_secrets(ledger: ControlLedger, secret_store: SecretStore | None) -> None:
    while True:
        await asyncio.sleep(60)
        if secret_store is None:
            continue
        try:
            _delete_expired_pending_secrets(ledger, secret_store)
        except SecretStoreError:
            _LOGGER.exception("Failed to delete expired pending worker credential.")


async def _create_hourly_backups(backup_manager: BackupManager) -> None:
    while True:
        await asyncio.sleep(60 * 60)
        try:
            backup_manager.create("hourly")
        except Exception:
            _LOGGER.exception("Failed to create hourly control-plane backup.")


def _create_pki_backup(backup_manager: BackupManager | None) -> None:
    if backup_manager is None:
        return
    backup_manager.create("pki-change")


def _delete_expired_pending_secrets(ledger: ControlLedger, secret_store: SecretStore) -> None:
    for secret_ref in ledger.expire_pending_enrollments():
        secret_store.delete_password(secret_ref)
        ledger.mark_pending_password_deleted(secret_ref)




def _ledger_error_status(error: LedgerError) -> int:
    return status.HTTP_404_NOT_FOUND if "does not exist" in str(error) else status.HTTP_409_CONFLICT


def _connected_worker_session(
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_id: str,
    *,
    reason: str = "Selected worker must remain connected during product catalog analysis.",
) -> _WorkerSessionConnection:
    connections = worker_connections.get(worker_id)
    if not connections or len(connections) != 1:
        raise LedgerError(reason)
    return next(iter(connections))


async def _send_worker_relay_requests(connection: _WorkerSessionConnection) -> None:
    while True:
        request = await asyncio.to_thread(connection.outbound.get)
        if request is None:
            return
        if request.expects_response:
            connection.pending[request.request_id] = request
        await connection.websocket.send_json(request.message)


async def _request_worker_relay(
    connection: _WorkerSessionConnection,
    *,
    timeout: int,
    message: dict[str, object],
) -> dict[str, object]:
    request = _WorkerRelayRequest(
        request_id=str(message["request_id"]),
        message=message,
        future=ConcurrentFuture(),
    )
    connection.outbound.put(request)
    try:
        return await asyncio.wait_for(asyncio.wrap_future(request.future), timeout=timeout)
    finally:
        connection.pending.pop(request.request_id, None)


async def _request_pair_cell_quarantine_release(
    connection: _WorkerSessionConnection,
    release: dict[str, object],
    *,
    request_id: str,
) -> str:
    """Ask one Worker to apply this quarantine release and await its own
    genuine applied/rejected outcome; a timeout or mid-flight disconnect
    never counts as applied -- it is reported as ``"unknown"`` so the
    caller never mistakes "we don't know" for success.
    """

    message = {
        "type": "pair_cell_quarantine_release_request",
        "request_id": request_id,
        "symbol": release["symbol"],
        "actor": release["actor"],
        "reason": release["reason"],
        "observed_at": release["observed_at"],
    }
    try:
        response = await _request_worker_relay(connection, timeout=15, message=message)
    except (asyncio.TimeoutError, LedgerError):
        return "unknown"
    outcome = response.get("outcome")
    return cast(str, outcome) if outcome in ("applied", "rejected") else "unknown"


def _push_worker_relay(connection: _WorkerSessionConnection, message: dict[str, object]) -> None:
    """Enqueue one fire-and-forget push through the connection's single writer."""

    connection.outbound.put(
        _WorkerRelayRequest(
            request_id=str(uuid4()), message=message, future=ConcurrentFuture(), expects_response=False
        )
    )






def _record_pair_cell_quarantine_release_result(
    connection: _WorkerSessionConnection,
    request: dict[str, object],
) -> None:
    """Resolve the pending admin quarantine-release request this Worker's
    reply correlates to, by ``request_id``.

    A reply with no matching pending request (already timed out, or a
    stray/duplicate resend) is safely ignored: whichever side is still
    waiting on the admin route either already gave up (timeout) or was
    already satisfied.
    """

    pending = connection.pending.pop(cast(str, request["request_id"]), None)
    if pending is None:
        return
    if not pending.future.done():
        pending.future.set_result(request)


async def _relay_pair_cell_envelope(
    ledger: ControlLedger,
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    from_worker_id: str,
    envelope: dict[str, Any],
) -> dict[str, Any]:
    """Authenticate, authorize, and forward one opaque Pair Execution Cell envelope.

    Only the sender's authenticated identity, the live route named by the
    envelope's ``route_id``, the sending role the controller's own route
    record assigns, the protocol version, and the payload size are checked (by
    pydantic and :meth:`ControlLedger.authorize_pair_relay`). The envelope is
    then forwarded byte-for-byte unchanged to the peer's single live session
    writer, which preserves emission order for that session.

    Nothing is queued, buffered, or persisted for a disconnected peer: an
    undeliverable envelope is rejected to its sender now, so a later
    reconnect starts a fresh ordered session and can never receive a replayed
    execution message from the previous one.
    """

    try:
        parsed = pair_cell_relay_envelope_adapter.validate_python(envelope)
    except ValidationError as error:
        raise LedgerError("Pair Execution Cell relay envelope is invalid.") from error
    if parsed.from_worker_id != from_worker_id:
        raise LedgerError("Pair Execution Cell relay envelope sender does not match the authenticated Worker.")
    result = ledger.authorize_pair_relay(envelope)
    connection = _connected_worker_session(
        worker_connections, parsed.to_worker_id, reason="Peer Worker is disconnected."
    )
    _push_worker_relay(connection, {"type": "pair_relay_deliver", "envelope": envelope})
    return result


# --------------------------------------------------------------------------- #
# Worker-initiated pairing control plane
# --------------------------------------------------------------------------- #

_PAIR_CELL_RESULT_TYPES = {
    "pair_cell_role": "pair_cell_role_result",
    "pair_cell_available_followers": "pair_cell_available_followers_result",
    "pair_cell_pairing_proposal": "pair_cell_pairing_proposal_result",
    "pair_cell_pairing_decision": "pair_cell_pairing_decision_result",
    "pair_cell_route_sync": "pair_cell_route_sync_result",
    "pair_cell_unpair": "pair_cell_unpair_result",
    "pair_cell_state_version": "pair_cell_state_version_result",
    "pair_cell_unpair_assertion": "pair_cell_unpair_assertion_result",
}


def _connected_worker_ids(worker_connections: dict[str, set[_WorkerSessionConnection]]) -> set[str]:
    """Exactly the Workers reachable through one live authenticated session now."""

    return {
        worker_id for worker_id, connections in worker_connections.items() if len(connections) == 1
    }


def _pair_cell_route_view(route: dict[str, Any] | None) -> dict[str, object] | None:
    """The controller's authoritative route record as the Workers see it."""

    if route is None:
        return None
    view = {
        "route_id": route["route_id"],
        "leader_worker_id": route["leader_worker_id"],
        "follower_worker_id": route["follower_worker_id"],
        "state": route["state"],
        "created_at": route["created_at"],
        "unpairing_at": route["unpairing_at"],
    }
    if "role" in route:
        view["role"] = route["role"]
    return cast(dict[str, object], jsonable_encoder(view))


def _push_pair_cell_message(
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_id: str,
    message: dict[str, object],
) -> None:
    """Best-effort notify one Worker through its single live session writer.

    A disconnected Worker is simply not notified: pairing control messages
    are never queued or replayed into a later session, and a reconnecting
    Worker re-reads the authoritative route record instead.
    """

    connections = worker_connections.get(worker_id)
    if connections and len(connections) == 1:
        _push_worker_relay(next(iter(connections)), message)


def _handle_pair_cell_control_message(
    ledger: ControlLedger,
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_id: str,
    connection: _WorkerSessionConnection,
    request: dict[str, Any],
) -> None:
    """Answer one authenticated Worker's pairing control message.

    Every reply and every peer notification is queued through the target
    session's single writer, so ordering within a live session is preserved.
    A malformed or refused control message is answered with an explicit
    reasoned refusal rather than dropping the authenticated session: pairing
    is a Worker-driven negotiation, and a refusal is a normal outcome.
    """

    result_type = _PAIR_CELL_RESULT_TYPES[str(request.get("type"))]
    request_id = request.get("request_id")
    try:
        message = pair_cell_control_message_adapter.validate_python(request)
    except ValidationError:
        _push_worker_relay(
            connection,
            {
                "type": result_type,
                "request_id": request_id if isinstance(request_id, str) else None,
                "accepted": False,
                "reason": "Pair Execution Cell control message is invalid.",
            },
        )
        return
    try:
        reply, notifications = _apply_pair_cell_control_message(
            ledger, worker_connections, worker_id, message
        )
    except LedgerError as error:
        _push_worker_relay(
            connection,
            {
                "type": result_type,
                "request_id": message.request_id,
                "accepted": False,
                "reason": str(error),
            },
        )
        return
    _push_worker_relay(connection, reply)
    for target_worker_id, notification in notifications:
        _push_pair_cell_message(worker_connections, target_worker_id, notification)


def _apply_pair_cell_control_message(
    ledger: ControlLedger,
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_id: str,
    message: Any,
) -> tuple[dict[str, object], list[tuple[str, dict[str, object]]]]:
    result_type = _PAIR_CELL_RESULT_TYPES[message.type]
    reply: dict[str, object] = {
        "type": result_type, "request_id": message.request_id, "accepted": True
    }
    notifications: list[tuple[str, dict[str, object]]] = []

    if message.type == "pair_cell_role":
        declaration = ledger.declare_pair_cell_role(worker_id, message.role)
        reply["role"] = declaration["role"]
        reply["declared_role"] = declaration["declared_role"]
        reply["route"] = _pair_cell_route_view(cast(dict[str, Any] | None, declaration["route"]))
        return reply, notifications

    if message.type == "pair_cell_available_followers":
        reply["followers"] = jsonable_encoder(
            ledger.available_pair_followers(
                requesting_worker_id=worker_id,
                connected_worker_ids=_connected_worker_ids(worker_connections),
            )
        )
        return reply, notifications

    if message.type == "pair_cell_route_sync":
        reply["route"] = _pair_cell_route_view(ledger.pair_route_for_worker(worker_id))
        return reply, notifications

    if message.type == "pair_cell_pairing_proposal":
        reservation = ledger.reserve_pair_route_proposal(
            leader_worker_id=worker_id,
            follower_worker_id=message.follower_worker_id,
            connected_worker_ids=_connected_worker_ids(worker_connections),
        )
        proposal_id = cast(str, reservation["proposal_id"])
        follower_connections = worker_connections.get(message.follower_worker_id)
        if not follower_connections or len(follower_connections) != 1:
            ledger.release_pair_route_reservation(
                proposal_id=proposal_id, reason="follower_disconnected"
            )
            raise LedgerError("The selected follower Worker is no longer connected.")
        reply["proposal_id"] = proposal_id
        reply["follower_worker_id"] = message.follower_worker_id
        reply["expires_at"] = jsonable_encoder(reservation["expires_at"])
        reply["timeout_seconds"] = reservation["timeout_seconds"]
        notifications.append(
            (
                message.follower_worker_id,
                {
                    "type": "pair_cell_pairing_proposed",
                    "proposal_id": proposal_id,
                    "leader_worker_id": worker_id,
                    "follower_worker_id": message.follower_worker_id,
                    "expires_at": jsonable_encoder(reservation["expires_at"]),
                    "timeout_seconds": reservation["timeout_seconds"],
                },
            )
        )
        return reply, notifications

    if message.type == "pair_cell_pairing_decision":
        reservation = ledger.pair_route_reservation(message.proposal_id)
        if reservation is None or reservation["follower_worker_id"] != worker_id:
            raise LedgerError("No live pairing reservation names this Worker as its follower.")
        leader_worker_id = cast(str, reservation["leader_worker_id"])
        if not message.accepted:
            ledger.release_pair_route_reservation(
                proposal_id=message.proposal_id, reason="follower_refused"
            )
            reply["outcome"] = "refused"
            reply["route"] = None
            notifications.append(
                (
                    leader_worker_id,
                    {
                        "type": "pair_cell_pairing_refused",
                        "proposal_id": message.proposal_id,
                        "follower_worker_id": worker_id,
                        "reason": message.reason,
                    },
                )
            )
            return reply, notifications
        try:
            authorized = ledger.authorize_pair_acceptance_payload(
                proposal_id=message.proposal_id,
                from_worker_id=worker_id,
                acceptance_payload=message.acceptance_payload,
                payload_hash=message.payload_hash,
            )
            route = ledger.create_pair_route(
                proposal_id=message.proposal_id,
                connected_worker_ids=_connected_worker_ids(worker_connections),
            )
        except LedgerError as error:
            ledger.release_pair_route_reservation(
                proposal_id=message.proposal_id, reason="pairing_acceptance_failed"
            )
            _push_pair_cell_message(
                worker_connections,
                leader_worker_id,
                {
                    "type": "pair_cell_pairing_failed",
                    "proposal_id": message.proposal_id,
                    "follower_worker_id": worker_id,
                    "reason": str(error),
                },
            )
            raise
        view = _pair_cell_route_view(route)
        reply["outcome"] = "paired"
        reply["route"] = view
        notifications.append(
            (
                cast(str, route["leader_worker_id"]),
                {
                    "type": "pair_cell_route_established",
                    "route": view,
                    "acceptance": authorized["acceptance"],
                },
            )
        )
        notifications.append(
            (
                cast(str, route["follower_worker_id"]),
                {"type": "pair_cell_route_established", "route": view},
            )
        )
        return reply, notifications

    if message.type == "pair_cell_unpair":
        if message.action == "enter":
            route = ledger.enter_pair_route_unpairing(route_id=message.route_id, worker_id=worker_id)
        else:
            route = ledger.cancel_pair_route_unpairing(route_id=message.route_id, worker_id=worker_id)
        view = _pair_cell_route_view(route)
        reply["action"] = message.action
        reply["route"] = view
        for target_worker_id in (route["leader_worker_id"], route["follower_worker_id"]):
            notifications.append(
                (
                    cast(str, target_worker_id),
                    {
                        "type": "pair_cell_route_state_changed",
                        "route": view,
                        "requested_by": worker_id,
                    },
                )
            )
        return reply, notifications

    if message.type == "pair_cell_state_version":
        recorded = ledger.record_pair_worker_state_version(
            route_id=message.route_id, worker_id=worker_id, state_version=message.state_version
        )
        reply["route_id"] = recorded["route_id"]
        reply["state_version"] = recorded["state_version"]
        reply["assertion_invalidated"] = recorded["assertion_invalidated"]
        return reply, notifications

    outcome = ledger.record_pair_unpair_assertion(
        route_id=message.route_id, worker_id=worker_id, state_version=message.state_version
    )
    reply["route_id"] = outcome["route_id"]
    reply["state_version"] = outcome["state_version"]
    reply["asserted_worker_ids"] = outcome["asserted_worker_ids"]
    reply["route_removed"] = outcome["route_removed"]
    if outcome["route_removed"]:
        for target_worker_id in (outcome["leader_worker_id"], outcome["follower_worker_id"]):
            notifications.append(
                (
                    cast(str, target_worker_id),
                    {
                        "type": "pair_cell_route_removed",
                        "route_id": outcome["route_id"],
                        "reason": "safe_unpair",
                    },
                )
            )
    return reply, notifications


def _release_pair_cell_reservations_for_worker(
    ledger: ControlLedger,
    worker_connections: dict[str, set[_WorkerSessionConnection]] | None,
    worker_id: str,
    *,
    reason: str,
) -> None:
    """Release every pairing reservation a vanished Worker was holding.

    A leader or follower that disconnects before the final route-creation
    commit must never strand its counterpart: the reservation, the role
    assignment it would have produced, and the execution-mode claim it took
    all go away together, and the surviving peer is told why.
    """

    live = ledger.pair_route_reservations_for_worker(worker_id)
    released = set(ledger.release_pair_route_reservations_for_worker(worker_id, reason))
    if worker_connections is None:
        return
    for reservation in live:
        if reservation["proposal_id"] not in released:
            continue
        for peer_worker_id in (reservation["leader_worker_id"], reservation["follower_worker_id"]):
            if peer_worker_id == worker_id:
                continue
            _push_pair_cell_message(
                worker_connections,
                cast(str, peer_worker_id),
                {
                    "type": "pair_cell_pairing_reservation_released",
                    "proposal_id": reservation["proposal_id"],
                    "reason": reason,
                },
            )


def _fail_pending_worker_relay_requests(connection: _WorkerSessionConnection, reason: str) -> None:
    for request in connection.pending.values():
        if not request.future.done():
            request.future.set_exception(LedgerError(reason))
    connection.pending.clear()


_HARD_BLOCK_FIELDS = (
    "trade_calc_mode",
    "digits",
    "point",
    "trade_tick_size",
    "contract_size",
    "volume_min",
    "volume_step",
    "allowed_directions",
)

_WARNING_FIELDS = (
    "volume_max",
    "trade_stops_level",
    "trade_freeze_level",
    "trade_tick_value",
    "currency_margin",
    "currency_profit",
    "swap_long",
    "swap_short",
    "swap_mode",
    "swap_rollover3days",
)

_COMPATIBILITY_CAPABILITY_FIELDS = ("filling_modes", "allowed_directions")
_SUPPORTED_ENTRY_FILLING_MODES = ("FOK", "IOC")


def _require_admin(
    ledger: ControlLedger,
    token: str | None,
    csrf_token: str | None = None,
    *,
    require_csrf: bool = False,
) -> str:
    if token is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Administrator login is required.")
    try:
        return ledger.validate_session(token, csrf_token, require_csrf=require_csrf)
    except AuthenticationError as error:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(error)) from error


async def _broadcast_pending_enrollment(
    connections: set[WebSocket],
    enrollment: dict[str, object],
) -> None:
    for connection in tuple(connections):
        try:
            await connection.send_json({"type": "pending_enrollment", "item": enrollment})
        except (RuntimeError, WebSocketDisconnect):
            connections.discard(connection)


async def _broadcast_management_snapshot(ledger: ControlLedger, connections: set[WebSocket]) -> None:
    snapshot = jsonable_encoder(
        {
            "type": "management_snapshot",
            "enrollments": ledger.pending_enrollments(),
            "workers": ledger.worker_reconciliation(),
            "alerts": ledger.alerts(),
        }
    )
    for connection in tuple(connections):
        try:
            await connection.send_json(snapshot)
        except (RuntimeError, WebSocketDisconnect):
            connections.discard(connection)


async def _receive_exact_message(websocket: WebSocket, expected_fields: set[str]) -> dict[str, object]:
    message = await websocket.receive_json()
    if not isinstance(message, dict) or set(message) != expected_fields:
        raise ValueError("Invalid protocol message.")
    return message


def _required_text(message: dict[str, object], field: str) -> str:
    value = message.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError("Invalid protocol message.")
    return value


def _is_authoritative_worker_session(
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_id: str,
    connection: _WorkerSessionConnection,
) -> bool:
    """Accept worker messages only from the session that owns the current cursor."""

    return not connection.superseded and worker_connections.get(worker_id) == {connection}


def _worker_fact_envelope(message: dict[str, object], worker_id: str) -> dict[str, object]:
    if (
        set(message) != {"type", "cursor", "envelope"}
        or isinstance(message.get("cursor"), bool)
        or not isinstance(message.get("cursor"), int)
        or not isinstance(message.get("envelope"), dict)
    ):
        raise ValueError("Invalid Worker fact envelope.")
    envelope = cast(dict[str, object], message["envelope"])
    if (
        envelope.get("type") not in {"worker_snapshot", "worker_broker_delta"}
        or envelope.get("protocol_version") != 1
        or envelope.get("worker_id") != worker_id
        or not isinstance(envelope.get("recovery_epoch"), str)
        or not envelope["recovery_epoch"]
    ):
        raise ValueError("Invalid Worker fact delivery metadata.")
    event_id = envelope.get("event_id", envelope.get("snapshot_baseline"))
    if event_id != message["cursor"]:
        raise ValueError("Worker fact cursor does not match its envelope.")
    if len(json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8")) > 1_000_000:
        raise ValueError("Worker fact envelope exceeds the relay size limit.")
    return envelope


def _validate_worker_stream_envelope(message: dict[str, object]) -> None:
    if message.get("type") not in {"live_state_snapshot", "live_state_diff"}:
        raise ValueError("Invalid Worker stream envelope.")
    if len(json.dumps(message, separators=(",", ":"), ensure_ascii=False).encode("utf-8")) > 1_000_000:
        raise ValueError("Worker stream envelope exceeds the relay size limit.")









def _handle_worker_disconnect(
    ledger: ControlLedger,
    worker_id: str,
    source: str,
    reason: str,
    worker_connections: dict[str, set[_WorkerSessionConnection]] | None = None,
) -> None:
    ledger.record_worker_connection_audit(worker_id, source, reason)
    _release_pair_cell_reservations_for_worker(
        ledger, worker_connections, worker_id, reason="worker_disconnected"
    )


async def _close_policy_violation(websocket: WebSocket) -> None:
    if websocket.client_state.name != "DISCONNECTED":
        try:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        except (RuntimeError, WebSocketDisconnect):
            pass
