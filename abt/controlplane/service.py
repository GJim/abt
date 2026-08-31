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

from ..pair_cell import PairContract
from ..trader_protocol import pair_cell_relay_envelope_adapter, trader_rpc_response_adapter

from .backup import BackupManager
from .crypto import (
    ProofError,
    parse_device_certificate,
    parse_trader_certificate,
    verify_enrollment_proof,
    verify_trader_enrollment_proof,
    verify_trader_proof,
    verify_trader_rotation_proof,
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


def _verify_trader_certificate(certificate_verifier: DeviceCertificateVerifier | None, trader: Any) -> None:
    claims = parse_trader_certificate(trader.certificate)
    if certificate_verifier is None:
        raise ProofError("The device certificate verifier is unavailable.")
    certificate_verifier.verify(trader.certificate)
    if (
        claims["trader_id"] != trader.trader_id
        or claims["strategy_name"] != trader.strategy_name
        or claims["public_key_pem"] != trader.public_key_pem
    ):
        raise ProofError("The Trader certificate does not match this Trader.")


class LoginRequest(BaseModel):
    username: str = Field(pattern=r"^[A-Z]{6}$")
    password: str = Field(min_length=20)


class PairExecutionModeClaimRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    leader_worker_id: str = Field(min_length=1)
    follower_worker_id: str = Field(min_length=1)
    trader_id: str = Field(min_length=1)
    mode: Literal["strategy_runtime", "pair_execution_cell", "shadow"]


class PairContractActivationRequest(BaseModel):
    """The complete, structurally-validated Pair Execution Cell contract.

    Only structural/identity/route/lease concerns are validated here (field
    types, presence, distinct Workers); strategy values -- ``edge_threshold``,
    ``risk_budget_usd``, volumes, quote/timing policy -- are opaque to the
    controller and stored/relayed as-is.
    """

    model_config = ConfigDict(extra="forbid")

    leader_worker_id: str = Field(min_length=1)
    follower_worker_id: str = Field(min_length=1)
    trader_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    leader_direction: Literal["LONG", "SHORT"]
    follower_direction: Literal["LONG", "SHORT"]
    leader_volume: str = Field(min_length=1)
    follower_volume: str = Field(min_length=1)
    filling_mode: Literal["FOK", "IOC"]
    edge_threshold: str = Field(min_length=1)
    risk_budget_usd: str = Field(min_length=1)
    quote_max_age_seconds: float = Field(gt=0)
    quote_max_skew_seconds: float = Field(gt=0)
    arm_timeout_seconds: float = Field(gt=0)
    commit_lead_seconds: float = Field(gt=0)
    execution_expiry_seconds: float = Field(gt=0)
    mode: Literal["live", "shadow"] = "live"
    maximum_holding_seconds: float | None = Field(
        gt=0, le=7 * 24 * 60 * 60, allow_inf_nan=False, default=None
    )
    flatten_at_ny: str | None = Field(
        pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$", default=None
    )
    ttl_seconds: float = Field(gt=0, default=3600.0)


class PairExecutionModeReleaseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    leader_worker_id: str = Field(min_length=1)
    follower_worker_id: str = Field(min_length=1)
    mode: Literal["strategy_runtime", "pair_execution_cell", "shadow"]


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


class TraderEnrollmentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    registration_invite: str = Field(min_length=1, max_length=512)
    strategy_name: str = Field(min_length=1, max_length=128)
    claimed_public_ip: str = Field(min_length=1, max_length=64)
    public_key_pem: str = Field(min_length=1)
    proof_signature: str = Field(min_length=1)


class TraderCertificateChallengeRequest(BaseModel):
    registration_id: str = Field(min_length=1)


class TraderCertificateRequest(BaseModel):
    registration_id: str = Field(min_length=1)
    signature: str = Field(min_length=1)


class TraderRotationChallengeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trader_id: str = Field(min_length=1)
    public_key_pem: str = Field(min_length=1)


@dataclass(frozen=True)
class _TraderRotationChallenge:
    nonce: str
    replacement_public_key_pem: str
    expires_at: datetime


class TraderRotationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trader_id: str = Field(min_length=1)
    public_key_pem: str = Field(min_length=1)
    old_key_signature: str = Field(min_length=1)
    replacement_key_signature: str = Field(min_length=1)


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


@dataclass(eq=False)
class _TraderSessionConnection:
    websocket: WebSocket
    worker_ids: set[str] | None = field(default_factory=set)
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def send_json(self, message: dict[str, object]) -> None:
        async with self.send_lock:
            await self.websocket.send_json(message)


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
        trader_session_sweep_task = asyncio.create_task(_sweep_stale_trader_sessions(ledger))
        backup_task = (
            asyncio.create_task(_create_hourly_backups(backup_manager)) if backup_manager is not None else None
        )
        try:
            yield
        finally:
            cleanup_task.cancel()
            trader_session_sweep_task.cancel()
            tasks = [cleanup_task, trader_session_sweep_task]
            if backup_task is not None:
                backup_task.cancel()
                tasks.append(backup_task)
            await asyncio.gather(*tasks, return_exceptions=True)
            ledger.close()

    app = FastAPI(title="abt control plane", version="0.1.0", lifespan=lifespan)
    app.state.ledger = ledger
    app.state.trader_certificate_challenges = {}
    app.state.trader_rotation_challenges: dict[str, _TraderRotationChallenge] = {}
    app.state.worker_rotation_challenges: dict[str, _WorkerRotationChallenge] = {}
    admin_notification_connections: set[WebSocket] = set()
    worker_connections: dict[str, set[_WorkerSessionConnection]] = {}
    trader_connections: dict[str, set[_TraderSessionConnection]] = {}

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

    @app.post("/api/traders/enrollments", status_code=status.HTTP_201_CREATED)
    def create_trader_enrollment(body: TraderEnrollmentRequest) -> dict[str, object]:
        try:
            verify_trader_enrollment_proof(
                body.public_key_pem,
                body.proof_signature,
                registration_invite=body.registration_invite,
                strategy_name=body.strategy_name,
                claimed_public_ip=body.claimed_public_ip,
            )
            ledger.consume_registration_invite(body.registration_invite, "trader")
            return ledger.create_trader_enrollment(
                strategy_name=body.strategy_name,
                claimed_public_ip=body.claimed_public_ip,
                public_key_pem=body.public_key_pem,
            )
        except ProofError as error:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)) from error
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error

    @app.get("/api/traders/enrollments/{registration_id}/status")
    def trader_enrollment_status(registration_id: str) -> dict[str, str]:
        try:
            return {"status": ledger.trader_enrollment_status(registration_id)}
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error)) from error

    @app.get("/api/admin/traders/enrollments")
    def list_trader_enrollments(
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> list[dict[str, object]]:
        _require_admin(ledger, abt_admin_session)
        return ledger.pending_trader_enrollments()

    @app.get("/api/admin/traders")
    def list_traders(
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> list[dict[str, object]]:
        _require_admin(ledger, abt_admin_session)
        return ledger.traders()

    @app.post("/api/admin/traders/enrollments/{registration_id}/approve")
    def approve_trader_enrollment(
        registration_id: str,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, str]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            if certificate_issuer is None:
                raise SecretStoreError("The device certificate issuer is unavailable.")
            trader_id = ledger.approve_trader_enrollment(
                registration_id,
                username,
                lambda trader_id, strategy_name, public_key_pem: certificate_issuer.issue_trader(
                    trader_id=trader_id,
                    strategy_name=strategy_name,
                    public_key_pem=public_key_pem,
                ),
            )
            _create_pki_backup(backup_manager)
            return {"trader_id": trader_id, "certificate": ledger.active_trader(trader_id).certificate}
        except (LedgerError, SecretStoreError) as error:
            _LOGGER.warning("Trader enrollment approval failed for %s: %s", registration_id, error)
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error

    @app.post("/api/admin/traders/enrollments/{registration_id}/reject", status_code=status.HTTP_204_NO_CONTENT)
    def reject_trader_enrollment(
        registration_id: str,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> Response:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            ledger.reject_trader_enrollment(registration_id, username)
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @app.post("/api/traders/certificates/challenge")
    def issue_trader_certificate_challenge(body: TraderCertificateChallengeRequest) -> dict[str, str]:
        try:
            trader = ledger.active_trader_for_enrollment(body.registration_id)
            nonce = secrets.token_urlsafe(32)
            app.state.trader_certificate_challenges[body.registration_id] = nonce
            return {"purpose": "certificate_delivery", "trader_id": trader.trader_id, "nonce": nonce}
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error

    @app.post("/api/traders/certificates")
    def deliver_trader_certificate(body: TraderCertificateRequest) -> dict[str, str]:
        try:
            trader = ledger.active_trader_for_enrollment(body.registration_id)
            nonce = app.state.trader_certificate_challenges.pop(body.registration_id, None)
            if nonce is None:
                raise ProofError("The Trader certificate challenge is invalid or expired.")
            verify_trader_proof(
                trader.public_key_pem,
                body.signature,
                purpose="certificate_delivery",
                trader_id=trader.trader_id,
                nonce=nonce,
            )
            return {"trader_id": trader.trader_id, "certificate": trader.certificate}
        except (LedgerError, ProofError) as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error

    @app.post("/api/traders/certificates/rotation-challenge")
    def issue_trader_rotation_challenge(body: TraderRotationChallengeRequest) -> dict[str, str]:
        try:
            trader = ledger.active_trader(body.trader_id)
            _verify_trader_certificate(certificate_verifier, trader)
            nonce = secrets.token_urlsafe(32)
            app.state.trader_rotation_challenges[body.trader_id] = _TraderRotationChallenge(
                nonce=nonce,
                replacement_public_key_pem=body.public_key_pem,
                expires_at=datetime.now(UTC) + timedelta(minutes=2),
            )
            return {"purpose": "trader_certificate_rotation", "trader_id": trader.trader_id, "nonce": nonce}
        except (LedgerError, ProofError) as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error

    @app.post("/api/traders/certificates/rotate")
    def rotate_trader_certificate(body: TraderRotationRequest) -> dict[str, str]:
        try:
            trader = ledger.active_trader(body.trader_id)
            _verify_trader_certificate(certificate_verifier, trader)
            challenge = app.state.trader_rotation_challenges.pop(body.trader_id, None)
            if (
                challenge is None
                or challenge.replacement_public_key_pem != body.public_key_pem
                or datetime.now(UTC) >= challenge.expires_at
            ):
                raise ProofError("The Trader rotation challenge is invalid or expired.")
            nonce = challenge.nonce
            replacement_public_key_pem = challenge.replacement_public_key_pem
            verify_trader_rotation_proof(
                trader.public_key_pem, body.old_key_signature, trader_id=trader.trader_id,
                replacement_public_key_pem=replacement_public_key_pem, nonce=nonce,
            )
            verify_trader_rotation_proof(
                replacement_public_key_pem, body.replacement_key_signature, trader_id=trader.trader_id,
                replacement_public_key_pem=replacement_public_key_pem, nonce=nonce,
            )
            if certificate_issuer is None:
                raise SecretStoreError("The device certificate issuer is unavailable.")
            certificate = ledger.rotate_trader_certificate(
                trader.trader_id, replacement_public_key_pem,
                lambda trader_id, strategy_name, public_key_pem: certificate_issuer.issue_trader(
                    trader_id=trader_id, strategy_name=strategy_name, public_key_pem=public_key_pem
                ),
            )
            return {"trader_id": trader.trader_id, "certificate": certificate}
        except (LedgerError, ProofError, SecretStoreError) as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error

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

    @app.post("/api/admin/traders/{trader_id}/revoke", status_code=status.HTTP_204_NO_CONTENT)
    async def revoke_trader(
        trader_id: str,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> Response:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            ledger.revoke_trader(trader_id, username)
            _create_pki_backup(backup_manager)
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error
        connections = tuple(trader_connections.pop(trader_id, set()))
        await asyncio.gather(
            *(connection.websocket.close(code=status.WS_1008_POLICY_VIOLATION) for connection in connections),
            return_exceptions=True,
        )
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @app.post("/api/admin/pairs/execution-mode")
    def claim_pair_execution_mode_route(
        payload: PairExecutionModeClaimRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        """Explicitly declare one Worker pair's execution-mode owner.

        This is the operator-controlled switch described by the Pair
        Execution Cell specification: it never runs automatically, and the
        legacy Strategy Runtime and a live Pair Execution Cell cannot both
        hold the same pair.
        """

        _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            ledger.claim_pair_execution_mode(
                payload.leader_worker_id, payload.follower_worker_id, payload.mode, payload.trader_id
            )
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error
        return {"status": "claimed", "mode": payload.mode}

    @app.post("/api/admin/pairs/activate")
    def activate_pair_contract_route(
        payload: PairContractActivationRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        """Create and distribute the complete immutable pair contract plus lease.

        This is the controller surface that activates a Pair Execution Cell
        pair end-to-end: it stores the full contract (not only an opaque
        lease hash), issues the epoch-fenced lease, and pushes both to every
        currently-connected leader/follower Worker session.  A Worker that is
        offline (or connects later) fetches the same activation itself via
        ``pair_cell_sync_request``, so delivery never depends on connection
        timing.  This is the narrowly-scoped explicit operator activation
        path: it never runs automatically, and execution-mode exclusivity
        with a live legacy Strategy Runtime is enforced by the ledger exactly
        as it is for the existing claim/release endpoints.
        """

        _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        fields = payload.model_dump(exclude={"ttl_seconds"})
        contract = PairContract(contract_id=str(uuid4()), **fields)
        try:
            activation = ledger.activate_pair_contract(
                contract=contract.canonical(),
                contract_hash=contract.hash,
                trader_id=payload.trader_id,
                ttl_seconds=payload.ttl_seconds,
            )
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error
        activation = jsonable_encoder(activation)
        for worker_id in (payload.leader_worker_id, payload.follower_worker_id):
            connections = worker_connections.get(worker_id)
            if connections and len(connections) == 1:
                _push_worker_relay(
                    next(iter(connections)),
                    {"type": "pair_cell_activate", "contract": activation["contract"], "lease": activation["lease"]},
                )
        return {"status": "activated", **activation}

    @app.post("/api/admin/pairs/execution-mode/release", status_code=status.HTTP_204_NO_CONTENT)
    def release_pair_execution_mode_route(
        payload: PairExecutionModeReleaseRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> Response:
        _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        ledger.release_pair_execution_mode(payload.leader_worker_id, payload.follower_worker_id, payload.mode)
        return Response(status_code=status.HTTP_204_NO_CONTENT)

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

    @app.websocket("/api/traders/session")
    async def trader_session(websocket: WebSocket) -> None:
        await websocket.accept()
        trader_id: str | None = None
        session_id: str | None = None
        relay_tasks: set[asyncio.Task[None]] = set()
        try:
            request = await _receive_exact_message(websocket, {"trader_id", "certificate"})
            trader_id = _required_text(request, "trader_id")
            certificate = _required_text(request, "certificate")
            claims = parse_trader_certificate(certificate)
            if certificate_verifier is None:
                raise ProofError("The device certificate verifier is unavailable.")
            certificate_verifier.verify(certificate)
            trader = ledger.trader_for_certificate(trader_id, certificate)
            if (
                certificate != trader.certificate
                or claims["trader_id"] != trader.trader_id
                or claims["strategy_name"] != trader.strategy_name
                or claims["public_key_pem"] != trader.public_key_pem
            ):
                raise ProofError("The Trader certificate does not match this Trader.")
            nonce = secrets.token_urlsafe(32)
            await websocket.send_json({"purpose": "trader_session", "trader_id": trader.trader_id, "nonce": nonce})
            proof = await _receive_exact_message(websocket, {"signature"})
            verify_trader_proof(
                trader.public_key_pem,
                _required_text(proof, "signature"),
                purpose="trader_session",
                trader_id=trader.trader_id,
                nonce=nonce,
            )
            ledger.active_trader(trader.trader_id)
            connection = _TraderSessionConnection(websocket)
            trader_connections.setdefault(trader.trader_id, set()).add(connection)
            session_id = ledger.open_trader_session(trader.trader_id)
            cursor = ledger.trader_event_cursor(trader.trader_id)
            await connection.send_json({"type": "authenticated", "trader_id": trader.trader_id, "cursor": cursor})
            delivered_event_ids: set[int] = set()
            last_delivered_event_id = cursor

            async def deliver_events_after(event_cursor: int) -> None:
                nonlocal last_delivered_event_id
                for event in ledger.trader_events_after(trader.trader_id, event_cursor):
                    await connection.send_json({"type": "event", **jsonable_encoder(event)})
                    event_id = int(event["event_id"])
                    delivered_event_ids.add(event_id)
                    last_delivered_event_id = max(last_delivered_event_id, event_id)

            await deliver_events_after(cursor)
            last_controller_heartbeat = monotonic()
            async def relay_and_reply(worker_id: str, envelope: dict[str, Any]) -> None:
                try:
                    result = await _relay_trader_worker_envelope(
                        ledger,
                        worker_connections,
                        trader.trader_id,
                        worker_id,
                        envelope,
                    )
                    await connection.send_json(
                        {"type": "relay", "worker_id": worker_id, "envelope": result}
                    )
                except Exception:
                    _LOGGER.exception("Concurrent Trader-to-Worker relay failed.")
                    await _close_policy_violation(websocket)

            while True:
                ledger.trader_for_certificate(trader.trader_id, certificate)
                try:
                    message = await asyncio.wait_for(websocket.receive_json(), timeout=30)
                except asyncio.TimeoutError:
                    await deliver_events_after(last_delivered_event_id)
                    await connection.send_json({"type": "heartbeat"})
                    last_controller_heartbeat = monotonic()
                    continue
                if not isinstance(message, dict) or not isinstance(message.get("type"), str):
                    raise ValueError("Invalid Trader message.")
                if monotonic() - last_controller_heartbeat >= 30:
                    await connection.send_json({"type": "heartbeat"})
                    last_controller_heartbeat = monotonic()
                message_type = message["type"]
                if message_type == "heartbeat" and set(message) == {"type"}:
                    ledger.record_trader_signal(session_id)
                    await connection.send_json({"type": "heartbeat_ack"})
                elif message_type == "ack" and set(message) == {"type", "cursor"} and isinstance(message["cursor"], int):
                    ledger.record_trader_signal(session_id)
                    ledger.acknowledge_trader_events(
                        trader.trader_id, message["cursor"], cursor, delivered_event_ids
                    )
                    await connection.send_json({"type": "acknowledged", "cursor": message["cursor"]})
                elif message_type == "resume" and set(message) == {"type", "cursor"} and isinstance(message["cursor"], int):
                    ledger.record_trader_signal(session_id)
                    await deliver_events_after(message["cursor"])
                elif message_type == "subscribe" and set(message) == {"type", "worker_ids"}:
                    worker_ids = _trader_market_subscription(message["worker_ids"])
                    connection.worker_ids = worker_ids
                    await connection.send_json(
                        {"type": "subscribed", "worker_ids": ["*"] if worker_ids is None else sorted(worker_ids)}
                    )
                elif message_type == "resume_workers" and set(message) == {"type", "cursors"}:
                    cursors = message["cursors"]
                    if not isinstance(cursors, list):
                        raise ValueError("Invalid Worker resume request.")
                    resumed_workers: list[str] = []
                    async with connection.send_lock:
                        for cursor_request in cursors:
                            if not isinstance(cursor_request, dict) or set(cursor_request) != {
                                "worker_id", "recovery_epoch", "event_id"
                            }:
                                raise ValueError("Invalid Worker resume request.")
                            worker_id = cursor_request["worker_id"]
                            epoch = cursor_request["recovery_epoch"]
                            event_id = cursor_request["event_id"]
                            if (
                                not isinstance(worker_id, str)
                                or not worker_id
                                or (epoch is not None and (not isinstance(epoch, str) or not epoch))
                                or isinstance(event_id, bool)
                                or not isinstance(event_id, int)
                                or (connection.worker_ids is not None and worker_id not in connection.worker_ids)
                            ):
                                raise ValueError("Invalid Worker resume request.")
                            replay = ledger.worker_relay_resume(worker_id, epoch, event_id)
                            replay_epoch = replay["recovery_epoch"] or epoch or f"unobserved:{worker_id}"
                            stream_envelope = {
                                "type": "worker_stream_gap" if replay["status"] == "gap" else "worker_stream_resumed",
                                "protocol_version": 1,
                                "worker_id": worker_id,
                                "recovery_epoch": replay_epoch,
                                **(
                                    {"expected_event_id": event_id + 1, "received_event_id": event_id}
                                    if replay["status"] == "gap"
                                    else {"after_event_id": event_id}
                                ),
                            }
                            await connection.websocket.send_json(
                                {"type": "worker_fact", "worker_id": worker_id, "envelope": stream_envelope}
                            )
                            for fact in replay["facts"]:
                                await connection.websocket.send_json(
                                    {"type": "worker_fact", "worker_id": worker_id, "envelope": fact}
                                )
                            resumed_workers.append(worker_id)
                        await connection.websocket.send_json(
                            {"type": "workers_resumed", "worker_ids": resumed_workers}
                        )
                elif message_type == "ack_worker_fact" and set(message) == {
                    "type", "worker_id", "recovery_epoch", "event_id"
                }:
                    worker_id = _required_text(message, "worker_id")
                    recovery_epoch = _required_text(message, "recovery_epoch")
                    event_id = message["event_id"]
                    if (
                        isinstance(event_id, bool)
                        or not isinstance(event_id, int)
                        or (connection.worker_ids is not None and worker_id not in connection.worker_ids)
                    ):
                        raise ValueError("Invalid Worker fact acknowledgement.")
                    ledger.acknowledge_worker_fact(
                        trader.trader_id, worker_id, recovery_epoch, event_id
                    )
                    await connection.send_json(
                        {
                            "type": "worker_fact_acknowledged",
                            "worker_id": worker_id,
                            "recovery_epoch": recovery_epoch,
                            "event_id": event_id,
                        }
                    )
                elif (
                    message_type == "pair_execution_mode_claim"
                    and set(message) == {"type", "request_id", "leader_worker_id", "follower_worker_id"}
                    and isinstance(message["request_id"], str)
                    and message["request_id"]
                    and isinstance(message["leader_worker_id"], str)
                    and message["leader_worker_id"]
                    and isinstance(message["follower_worker_id"], str)
                    and message["follower_worker_id"]
                ):
                    ledger.record_trader_signal(session_id)
                    try:
                        ledger.claim_pair_execution_mode(
                            message["leader_worker_id"], message["follower_worker_id"], "strategy_runtime", trader.trader_id
                        )
                        await connection.send_json(
                            {
                                "type": "pair_execution_mode_claim_result",
                                "request_id": message["request_id"],
                                "accepted": True,
                            }
                        )
                    except LedgerError as error:
                        await connection.send_json(
                            {
                                "type": "pair_execution_mode_claim_result",
                                "request_id": message["request_id"],
                                "accepted": False,
                                "reason": str(error),
                            }
                        )
                elif (
                    message_type == "query"
                    and set(message) == {"type", "request_id", "query"}
                    and isinstance(message["request_id"], str)
                    and message["request_id"]
                    and message["query"] == "active_workers"
                ):
                    query = cast(str, message["query"])
                    result = {"workers": ledger.trader_active_workers()}
                    await connection.send_json(
                        {
                            "type": "trader_query_result",
                            "request_id": message["request_id"],
                            "query": query,
                            "result": jsonable_encoder(result),
                        }
                    )
                elif (
                    message_type == "relay"
                    and set(message) == {"type", "worker_id", "envelope"}
                    and isinstance(message["worker_id"], str)
                    and isinstance(message["envelope"], dict)
                ):
                    ledger.record_trader_signal(session_id)
                    task = asyncio.create_task(
                        relay_and_reply(
                            message["worker_id"],
                            cast(dict[str, Any], message["envelope"]),
                        )
                    )
                    relay_tasks.add(task)
                    task.add_done_callback(relay_tasks.discard)
                else:
                    raise ValueError("Invalid Trader message.")
        except WebSocketDisconnect as error:
            _LOGGER.info("Trader session disconnected with code %s.", error.code)
        except (LedgerError, ProofError, SecretStoreError, ValueError) as error:
            _LOGGER.warning("Trader session authentication or request failed: %s", error)
            await _close_policy_violation(websocket)
        except Exception:
            _LOGGER.exception("Trader session authentication or request failed unexpectedly.")
            await _close_policy_violation(websocket)
        finally:
            for task in tuple(relay_tasks):
                task.cancel()
            if relay_tasks:
                await asyncio.gather(*relay_tasks, return_exceptions=True)
            if trader_id is not None:
                connections = trader_connections.get(trader_id)
                if connections is not None:
                    connections.discard(connection)
                    if not connections:
                        trader_connections.pop(trader_id, None)

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
                elif message_type == "pair_cell_sync_request" and set(request) == {"type"}:
                    activation = ledger.pair_cell_activation_for_worker(worker.worker_id)
                    await websocket.send_json(
                        {
                            "type": "pair_cell_sync",
                            "contract": None if activation is None else activation["contract"],
                            "lease": None if activation is None else jsonable_encoder(activation["lease"]),
                        }
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
                    await _broadcast_trader_worker_stream(
                        trader_connections, worker.worker_id, request
                    )
                elif message_type == "live_state_diff":
                    _validate_worker_stream_envelope(request)
                    await websocket.send_json({"type": "live_state_accepted"})
                    await _broadcast_trader_worker_stream(
                        trader_connections, worker.worker_id, request
                    )
                elif message_type == "worker_fact":
                    envelope = _worker_fact_envelope(request, worker.worker_id)
                    ledger.record_worker_fact_audit(worker.worker_id, envelope)
                    await websocket.send_json({"type": "accepted", "cursor": request["cursor"]})
                    await _broadcast_trader_worker_fact(trader_connections, worker.worker_id, envelope)
                elif message_type == "worker_relay":
                    _record_worker_relay_response(ledger, connection, worker.worker_id, request)
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
                    await _broadcast_trader_market_data_unavailable(
                        trader_connections,
                        worker.worker_id,
                        "worker_session_unavailable",
                    )

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


async def _sweep_stale_trader_sessions(ledger: ControlLedger) -> None:
    while True:
        ledger.sweep_stale_trader_sessions()
        await asyncio.sleep(30)


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


def _push_worker_relay(connection: _WorkerSessionConnection, message: dict[str, object]) -> None:
    """Enqueue one fire-and-forget push through the connection's single writer."""

    connection.outbound.put(
        _WorkerRelayRequest(
            request_id=str(uuid4()), message=message, future=ConcurrentFuture(), expects_response=False
        )
    )


async def _relay_trader_worker_envelope(
    ledger: ControlLedger,
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    trader_id: str,
    worker_id: str,
    envelope: dict[str, Any],
) -> dict[str, Any]:
    """Authorize and relay an opaque versioned envelope without trading interpretation."""

    required = {
        "type", "protocol_version", "trader_id", "worker_id", "runtime_command_id",
        "request_id", "payload_hash", "correlation", "payload",
    }
    if (
        not required.issubset(envelope)
        or envelope.get("protocol_version") != 1
        or envelope.get("trader_id") != trader_id
        or envelope.get("worker_id") != worker_id
        or not isinstance(envelope.get("request_id"), str)
        or not envelope["request_id"]
        or not isinstance(envelope.get("runtime_command_id"), str)
        or not envelope["runtime_command_id"]
        or not isinstance(envelope.get("payload"), dict)
        or not isinstance(envelope.get("correlation"), dict)
        or not isinstance(envelope.get("payload_hash"), str)
    ):
        raise LedgerError("Trader relay envelope is invalid.")
    if envelope["type"] == "worker_effect_requested":
        if not {"effect_id", "expires_at"}.issubset(envelope):
            raise LedgerError("Trader relay effect envelope is invalid.")
    elif envelope["type"] != "worker_read_requested":
        raise LedgerError("Trader relay envelope type is invalid.")
    payload_hash = hashlib.sha256(
        json.dumps(envelope["payload"], separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()
    if payload_hash != envelope["payload_hash"]:
        raise LedgerError("Trader relay payload hash is invalid.")
    if len(json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8")) > 1_000_000:
        raise LedgerError("Trader relay envelope exceeds the size limit.")
    request_id = cast(str, envelope["request_id"])
    recorded = ledger.record_trader_relay(trader_id, request_id, worker_id, envelope)
    if not recorded.pop("dispatch"):
        outcome = recorded.get("outcome")
        if isinstance(outcome, dict):
            return cast(dict[str, Any], outcome)
        raise LedgerError("Trader relay request is already awaiting a Worker outcome.")
    connection = _connected_worker_session(
        worker_connections, worker_id, reason="Selected worker is disconnected."
    )
    try:
        response = await _request_worker_relay(
            connection,
            timeout=30,
            message={"type": "worker_relay", "request_id": request_id, "envelope": envelope},
        )
    except asyncio.TimeoutError as error:
        raise LedgerError("Worker did not complete the relayed request.") from error
    outcome = response.get("envelope")
    if (
        response.get("type") != "worker_relay"
        or not isinstance(outcome, dict)
        or outcome.get("trader_id") != trader_id
        or outcome.get("worker_id") != worker_id
        or outcome.get("request_id") != request_id
        or outcome.get("payload_hash") != envelope["payload_hash"]
        or outcome.get("protocol_version") != 1
        or outcome.get("runtime_command_id") != envelope["runtime_command_id"]
        or outcome.get("type") not in {"worker_read_completed", "worker_effect_outcome"}
        or not isinstance(outcome.get("recovery_epoch"), str)
        or not outcome["recovery_epoch"]
    ):
        raise LedgerError("Worker returned an invalid relay outcome.")
    return ledger.complete_trader_relay(trader_id, request_id, cast(dict[str, Any], outcome))


def _record_worker_relay_response(
    ledger: ControlLedger,
    connection: _WorkerSessionConnection,
    worker_id: str,
    request: dict[str, object],
) -> None:
    if set(request) not in ({"type", "envelope"}, {"type", "request_id", "envelope"}) or not isinstance(request.get("envelope"), dict):
        raise ValueError("Invalid Worker relay response.")
    envelope = cast(dict[str, object], request["envelope"])
    request_id = _required_text(envelope, "request_id")
    trader_id = _required_text(envelope, "trader_id")
    if _required_text(envelope, "worker_id") != worker_id:
        raise ValueError("Worker relay response identity does not match its session.")
    ledger.complete_trader_relay(trader_id, request_id, cast(dict[str, Any], envelope))
    pending = connection.pending.pop(request_id, None)
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
    """Authorize, audit, and forward one opaque Pair Execution Cell envelope.

    Only identity, route, lease epoch/expiry, protocol version, and size are
    validated (by pydantic and :meth:`ControlLedger.record_pair_relay`); the
    opaque ``payload`` (quotes, arm/commit, leg status) is never inspected and
    no edge or pair-lifecycle state is calculated here.
    """

    try:
        parsed = pair_cell_relay_envelope_adapter.validate_python(envelope)
    except ValidationError as error:
        raise LedgerError("Pair Execution Cell relay envelope is invalid.") from error
    if parsed.from_worker_id != from_worker_id:
        raise LedgerError("Pair Execution Cell relay envelope sender does not match the authenticated Worker.")
    result = ledger.record_pair_relay(envelope)
    connection = _connected_worker_session(
        worker_connections, parsed.to_worker_id, reason="Peer Worker is disconnected."
    )
    _push_worker_relay(connection, {"type": "pair_relay_deliver", "envelope": envelope})
    return result


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


async def _broadcast_trader_worker_stream(
    trader_connections: dict[str, set[_TraderSessionConnection]],
    worker_id: str,
    envelope: dict[str, object],
) -> None:
    message = {"type": "worker_stream", "worker_id": worker_id, "envelope": envelope}
    connections = tuple(
        connection
        for trader_connections_for_identity in trader_connections.values()
        for connection in trader_connections_for_identity
        if connection.worker_ids is None or worker_id in connection.worker_ids
    )
    for connection in connections:
        try:
            await connection.send_json(message)
        except (RuntimeError, WebSocketDisconnect):
            pass


async def _broadcast_trader_worker_fact(
    trader_connections: dict[str, set[_TraderSessionConnection]],
    worker_id: str,
    envelope: dict[str, object],
) -> None:
    message = {"type": "worker_fact", "worker_id": worker_id, "envelope": envelope}
    for connections in tuple(trader_connections.values()):
        for connection in tuple(connections):
            if connection.worker_ids is None or worker_id in connection.worker_ids:
                try:
                    await connection.send_json(message)
                except (RuntimeError, WebSocketDisconnect):
                    pass


async def _broadcast_trader_market_data_unavailable(
    trader_connections: dict[str, set[_TraderSessionConnection]],
    worker_id: str,
    reason: str,
) -> None:
    message = {"type": "market_data_unavailable", "worker_id": worker_id, "reason": reason}
    connections = tuple(
        connection
        for trader_connections_for_identity in trader_connections.values()
        for connection in trader_connections_for_identity
        if connection.worker_ids is None or worker_id in connection.worker_ids
    )
    for connection in connections:
        try:
            await connection.send_json(message)
        except (RuntimeError, WebSocketDisconnect):
            pass


def _trader_market_subscription(value: object) -> set[str] | None:
    if not isinstance(value, list) or not value or not all(isinstance(worker_id, str) and worker_id for worker_id in value):
        raise ValueError("Invalid Trader market-data subscription.")
    worker_ids = set(value)
    if "*" in worker_ids:
        if worker_ids != {"*"}:
            raise ValueError("Invalid Trader market-data subscription.")
        return None
    return worker_ids


def _handle_worker_disconnect(ledger: ControlLedger, worker_id: str, source: str, reason: str) -> None:
    ledger.record_worker_connection_audit(worker_id, source, reason)


async def _close_policy_violation(websocket: WebSocket) -> None:
    if websocket.client_state.name != "DISCONNECTED":
        try:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        except (RuntimeError, WebSocketDisconnect):
            pass
