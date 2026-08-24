from __future__ import annotations

import asyncio
from concurrent.futures import Future as ConcurrentFuture
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
import hashlib
import json
import logging
import math
import queue
from contextlib import asynccontextmanager
from decimal import Decimal
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


class TraderIntentPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["intent"]
    pair_id: str = Field(min_length=1)
    primary_direction: Literal["LONG", "SHORT"]
    lots: Decimal = Field(gt=0)
    entry_price: Decimal = Field(gt=0)
    stop_loss_pips: Decimal = Field(gt=0)
    take_profit_pips: Decimal = Field(gt=0)
    filling_mode: Literal["FOK", "IOC"]
    expires_at: datetime

    @field_validator("expires_at")
    @classmethod
    def require_absolute_expiry(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("expires_at must include a timezone.")
        return value.astimezone(UTC)


class TraderIntentCancellationPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["cancel"]
    intent_id: str = Field(min_length=1)


class ManagementIntentCommandRequest(BaseModel):
    command_id: str = Field(min_length=1, max_length=128)


class WorkerRecoveryCommandRequest(BaseModel):
    command_id: str = Field(min_length=1, max_length=128)


class ManualTradingTargetRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pair_id: str = Field(min_length=1)
    first_worker_id: str = Field(min_length=1)
    second_worker_id: str = Field(min_length=1)
    leg_order: Literal["buy_to_sell", "sell_to_buy"]
    interval_seconds: int = Field(ge=0)
    expected_revision: int = Field(ge=0)


class ManualTradeEntryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command_id: str = Field(min_length=1, max_length=128)
    target_revision: int = Field(ge=1)
    buy_worker_id: str = Field(min_length=1)
    sell_worker_id: str = Field(min_length=1)
    base_lots: Decimal = Field(gt=0)
    stop_loss_pips: Decimal = Field(gt=0)
    take_profit_pips: Decimal = Field(gt=0)


class ManualTradeExitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command_id: str = Field(min_length=1, max_length=128)


class ManualTradeProtectionRequest(ManualTradeExitRequest):
    stop_loss_pips: Decimal = Field(gt=0)
    take_profit_pips: Decimal = Field(gt=0)


class ManagementIntentCreateRequest(TraderIntentPayload):
    command_id: str = Field(min_length=1, max_length=128)


class ProductCatalogAnalysisPolicy(BaseModel):
    label: str = Field(min_length=1, max_length=128)
    require_equal_base_currency: bool = True
    require_equal_profit_currency: bool = True
    minimum_m15_common_coverage: float = Field(default=1.0, ge=0, le=1)
    minimum_m1_common_coverage: float = Field(default=0.98, ge=0, le=1)
    minimum_m15_return_correlation: float = Field(default=0.97, ge=-1, le=1)
    minimum_m1_return_correlation: float = Field(default=0.95, ge=-1, le=1)
    maximum_m1_median_price_difference_points: float = Field(default=2.0, ge=0)


class ProductCatalogAnalysisRequest(BaseModel):
    first_worker_id: str = Field(min_length=1)
    second_worker_id: str = Field(min_length=1)
    policy: ProductCatalogAnalysisPolicy


class ProductPairBuildConfirmationRequest(BaseModel):
    first_symbol: str = Field(min_length=1)
    second_symbol: str = Field(min_length=1)


class ProductPairConfirmationUseRequest(BaseModel):
    confirmation_id: str = Field(min_length=1)


class ProductPairRetestRequest(BaseModel):
    first_worker_id: str = Field(min_length=1)
    second_worker_id: str = Field(min_length=1)


def _operations_dashboard(
    *,
    workers: list[dict[str, Any]],
    alerts: list[dict[str, Any]],
    pending_enrollments: list[dict[str, Any]],
    product_pairs: list[dict[str, Any]],
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
    classified_product_pairs = [
        {
            **product_pair,
            "category": "informational",
            "classification_reason": f"product_pair_{product_pair['status']}",
        }
        for product_pair in product_pairs
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
            "product_pair_id": alert["product_pair_id"],
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
        "product_pairs": classified_product_pairs,
        "paired_trade_lifecycle": {
            "availability": "unavailable",
            "reason": "Paired-trade lifecycle records are not available in the control-plane ledger.",
        },
    }


def _worker_dashboard_classification(worker: dict[str, Any]) -> dict[str, str]:
    if worker["safety_state"] in {"frozen", "lost_link_safety", "needs_human"}:
        return {"category": "intervention_required", "classification_reason": worker["safety_state"]}
    if worker["connectivity"] == "stale":
        return {"category": "intervention_required", "classification_reason": "worker_stale"}
    if worker["connectivity"] == "revoked":
        return {"category": "informational", "classification_reason": "worker_revoked"}
    return {"category": "informational", "classification_reason": "worker_healthy"}


@dataclass
class _WorkerAnalysisRequest:
    analysis_id: str
    request_id: str
    stage: str
    message: dict[str, object]
    future: ConcurrentFuture[dict[str, object]]
    started_at: float = field(default_factory=monotonic)


@dataclass(eq=False)
class _WorkerSessionConnection:
    websocket: WebSocket
    outbound: queue.Queue[_WorkerAnalysisRequest | None] = field(default_factory=queue.Queue)
    pending: dict[str, _WorkerAnalysisRequest] = field(default_factory=dict)


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
    trader_connections: dict[str, set[WebSocket]] = {}
    worker_execution_locks: dict[str, asyncio.Lock] = {}
    startup_reconciliation_started_at = datetime.now(UTC)

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

    @app.get("/api/admin/intents/{intent_id}")
    def get_intent_record(
        intent_id: str,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> dict[str, object]:
        """Expose the original request, preflight, and immutable broker lifecycle."""

        _require_admin(ledger, abt_admin_session)
        try:
            return ledger.intent_record(intent_id)
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error)) from error

    @app.get("/api/admin/intents")
    def list_intents(
        active_only: bool = True,
        status_filter: Annotated[str | None, Query(alias="status", min_length=1, max_length=64)] = None,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> list[dict[str, object]]:
        _require_admin(ledger, abt_admin_session)
        return ledger.intents(active_only=active_only, status_filter=status_filter)

    @app.post("/api/admin/intents", status_code=status.HTTP_201_CREATED)
    async def create_management_intent(
        body: ManagementIntentCreateRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        return await _preflight_management_intent(
            ledger, worker_connections, worker_execution_locks, username, body.command_id, body.model_dump(mode="json", exclude={"command_id"})
        )

    @app.post("/api/admin/intents/{intent_id}/cancel")
    async def cancel_intent(
        intent_id: str,
        body: ManagementIntentCommandRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            result, preflight, scheduled = ledger.request_management_intent_operation(
                username, body.command_id, intent_id, "cancel"
            )
            if scheduled:
                asyncio.create_task(
                    _cancel_intent(ledger, worker_connections, worker_execution_locks, intent_id, preflight)
                )
            return result
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error

    @app.get("/api/admin/worker-snapshots")
    def list_worker_snapshots(
        limit: Annotated[int, Query(ge=1, le=50)] = 50,
        cursor: Annotated[str | None, Query(min_length=1, max_length=512)] = None,
        page: Annotated[int | None, Query(ge=1)] = None,
        q: Annotated[str | None, Query(min_length=1, max_length=256)] = None,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> dict[str, object]:
        _require_admin(ledger, abt_admin_session)
        try:
            return ledger.worker_snapshot_page(limit=limit, cursor=cursor, page=page, query=q)
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)) from error

    @app.get("/api/admin/worker-snapshots/{snapshot_id}")
    def get_worker_snapshot(
        snapshot_id: str,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> dict[str, object]:
        _require_admin(ledger, abt_admin_session)
        try:
            return ledger.worker_snapshot(snapshot_id)
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error)) from error

    @app.get("/api/admin/workers")
    def list_workers(
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> list[dict[str, object]]:
        _require_admin(ledger, abt_admin_session)
        return ledger.worker_reconciliation()

    @app.get("/api/admin/manual-trading-target")
    def get_manual_trading_target(
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> dict[str, object] | None:
        _require_admin(ledger, abt_admin_session)
        return ledger.manual_trading_target()

    @app.put("/api/admin/manual-trading-target")
    def configure_manual_trading_target(
        body: ManualTradingTargetRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            return ledger.configure_manual_trading_target(username, **body.model_dump())
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

    @app.post("/api/admin/manual-trades/preview")
    def preview_manual_trade(
        body: ManualTradeEntryRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            return ledger.preview_manual_trade(body.model_dump(mode="json", exclude={"command_id"}))
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

    @app.post("/api/admin/manual-trades", status_code=status.HTTP_201_CREATED)
    async def create_manual_trade(
        body: ManualTradeEntryRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            result = ledger.request_manual_trade(
                username, body.command_id, body.model_dump(mode="json", exclude={"command_id"})
            )
            if result["status"] == "scheduled":
                asyncio.create_task(
                    _dispatch_manual_trade(
                        ledger, worker_connections, worker_execution_locks, str(result["manual_trade_id"])
                    )
                )
            return result
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

    @app.post("/api/admin/manual-trades/active/exit/preview")
    def preview_active_manual_trade_exit(
        body: ManualTradeExitRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            return ledger.preview_active_manual_trade_operation("exit", body.model_dump(exclude={"command_id"}))
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

    @app.post("/api/admin/manual-trades/active/exit", status_code=status.HTTP_201_CREATED)
    async def exit_active_manual_trade(
        body: ManualTradeExitRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            result = ledger.request_active_manual_trade_operation(
                username, body.command_id, "exit", body.model_dump(exclude={"command_id"})
            )
            if result["status"] == "scheduled":
                asyncio.create_task(
                    _dispatch_manual_trade_operation(
                        ledger, worker_connections, worker_execution_locks, str(result["operation_id"])
                    )
                )
            return result
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

    @app.post("/api/admin/manual-trades/active/protection/preview")
    def preview_active_manual_trade_protection(
        body: ManualTradeProtectionRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            return ledger.preview_active_manual_trade_operation(
                "protection", body.model_dump(mode="json", exclude={"command_id"})
            )
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

    @app.post("/api/admin/manual-trades/active/protection", status_code=status.HTTP_201_CREATED)
    async def update_active_manual_trade_protection(
        body: ManualTradeProtectionRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            result = ledger.request_active_manual_trade_operation(
                username, body.command_id, "protection", body.model_dump(mode="json", exclude={"command_id"})
            )
            if result["status"] == "scheduled":
                asyncio.create_task(
                    _dispatch_manual_trade_operation(
                        ledger, worker_connections, worker_execution_locks, str(result["operation_id"])
                    )
                )
            return result
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

    @app.post("/api/admin/workers/{worker_id}/cleanup")
    async def cleanup_worker(
        worker_id: str,
        body: WorkerRecoveryCommandRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            result, scheduled = ledger.request_worker_recovery(username, body.command_id, worker_id, "cleanup")
            if scheduled:
                asyncio.create_task(
                    _cleanup_frozen_worker(
                        ledger, worker_connections, worker_execution_locks, worker_id, result, username,
                        admin_notification_connections,
                    )
                )
            return result
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

    @app.post("/api/admin/workers/{worker_id}/release")
    async def release_worker(
        worker_id: str,
        body: WorkerRecoveryCommandRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            result, scheduled = ledger.request_worker_recovery(username, body.command_id, worker_id, "release")
            if scheduled:
                asyncio.create_task(
                    _release_frozen_worker(
                        ledger, worker_connections, worker_execution_locks, worker_id, result, username,
                        admin_notification_connections,
                    )
                )
            return result
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

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
            product_pairs=ledger.product_pairs(),
        )

    @app.post("/api/admin/product-catalog-analyses", status_code=status.HTTP_201_CREATED)
    async def create_product_catalog_analysis(
        body: ProductCatalogAnalysisRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        policy = cast(dict[str, object], body.policy.model_dump(mode="json"))
        try:
            first_connection = _connected_worker_session(worker_connections, body.first_worker_id)
            second_connection = _connected_worker_session(worker_connections, body.second_worker_id)
            analysis_period = _previous_complete_utc_week()
            async with _pair_worker_execution(worker_execution_locks, body.first_worker_id, body.second_worker_id):
                analysis_id = ledger.create_product_catalog_analysis(
                    first_worker_id=body.first_worker_id,
                    second_worker_id=body.second_worker_id,
                    requested_by=username,
                    policy=policy,
                    analysis_period=analysis_period,
                )
                current_stage = "catalog"
                m15_screening_results: list[dict[str, object]] = []
                try:
                    first_result, second_result = await asyncio.gather(
                        _request_product_catalog_evidence(first_connection, analysis_id, policy),
                        _request_product_catalog_evidence(second_connection, analysis_id, policy),
                    )
                    first_evidence = _validated_product_catalog_response(first_result, analysis_id)
                    second_evidence = _validated_product_catalog_response(second_result, analysis_id)
                    eligible_candidates = _analyze_product_catalogs(
                        first_evidence["symbols"], second_evidence["symbols"]
                    )
                    ledger.record_product_catalog_analysis_catalog(
                        analysis_id,
                        first_evidence=first_evidence,
                        second_evidence=second_evidence,
                        eligible_candidates=eligible_candidates,
                    )
                    m1_verification_results: list[dict[str, object]] = []
                    if eligible_candidates:
                        current_stage = "m15_screening"
                        first_market_data, second_market_data = await _request_market_data_with_retry(
                            analysis_id,
                            first_connection=first_connection,
                            second_connection=second_connection,
                            first_symbols=_candidate_symbols(eligible_candidates, "first_symbol"),
                            second_symbols=_candidate_symbols(eligible_candidates, "second_symbol"),
                            analysis_period=analysis_period,
                            policy=policy,
                            stage="m15_screening",
                            timeframe="M15",
                            record_retry=lambda reason: ledger.record_product_catalog_analysis_retry(
                                analysis_id,
                                "m15_screening",
                                reason,
                            ),
                        )
                        m15_screening_results = _screen_m15_candidates(
                            eligible_candidates,
                            first_market_data,
                            second_market_data,
                            policy,
                        )
                        m15_passing_candidates = [
                            result for result in m15_screening_results if result["screening_status"] == "passed"
                        ]
                        if m15_passing_candidates:
                            current_stage = "m1_verification"
                            first_m1_market_data, second_m1_market_data = await _request_market_data_with_retry(
                                analysis_id,
                                first_connection=first_connection,
                                second_connection=second_connection,
                                first_symbols=_candidate_symbols(m15_passing_candidates, "first_symbol"),
                                second_symbols=_candidate_symbols(m15_passing_candidates, "second_symbol"),
                                analysis_period=analysis_period,
                                policy=policy,
                                stage="m1_verification",
                                timeframe="M1",
                                record_retry=lambda reason: ledger.record_product_catalog_analysis_retry(
                                    analysis_id,
                                    "m1_verification",
                                    reason,
                                ),
                            )
                            m1_verification_results = _verify_m1_candidates(
                                m15_passing_candidates,
                                first_m1_market_data,
                                second_m1_market_data,
                                first_evidence["symbols"],
                                second_evidence["symbols"],
                                policy,
                            )
                    ledger.complete_product_catalog_analysis(
                        analysis_id,
                        m15_screening_results=m15_screening_results,
                        m1_verification_results=m1_verification_results,
                        m1_verified=bool(m1_verification_results),
                    )
                except (asyncio.TimeoutError, LedgerError, ValueError) as error:
                    stage = {
                        "catalog": "catalog_failed",
                        "m15_screening": "m15_failed",
                        "m1_verification": "m1_failed",
                    }[current_stage]
                    ledger.fail_product_catalog_analysis(
                        analysis_id,
                        str(error),
                        stage=stage,
                        clear_m15_results=stage != "m1_failed",
                        m15_screening_results=m15_screening_results,
                    )
                return ledger.product_catalog_analysis(analysis_id)
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error

    @app.get("/api/admin/product-catalog-analyses")
    def list_product_catalog_analyses(
        limit: Annotated[int, Query(ge=1, le=50)] = 50,
        cursor: Annotated[str | None, Query(min_length=1, max_length=512)] = None,
        page: Annotated[int | None, Query(ge=1)] = None,
        status_filter: Annotated[str | None, Query(alias="status", min_length=1, max_length=32)] = None,
        q: Annotated[str | None, Query(min_length=1, max_length=256)] = None,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> dict[str, object]:
        _require_admin(ledger, abt_admin_session)
        try:
            return ledger.product_catalog_analysis_page(
                limit=limit,
                cursor=cursor,
                page=page,
                status=status_filter,
                query=q,
            )
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)) from error

    @app.get("/api/admin/product-catalog-analyses/{analysis_id}")
    def get_product_catalog_analysis(
        analysis_id: str,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> dict[str, object]:
        _require_admin(ledger, abt_admin_session)
        try:
            return ledger.product_catalog_analysis(analysis_id)
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error)) from error

    @app.post("/api/admin/product-catalog-analyses/{analysis_id}/product-pair-build-confirmations", status_code=status.HTTP_201_CREATED)
    def create_product_pair_build_confirmation(
        analysis_id: str,
        body: ProductPairBuildConfirmationRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            return ledger.create_product_pair_build_confirmation(
                analysis_id,
                first_symbol=body.first_symbol,
                second_symbol=body.second_symbol,
                requested_by=username,
            )
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

    @app.get("/api/admin/product-pairs")
    def list_product_pairs(
        status_filter: Annotated[Literal["active", "retired", "all"], Query(alias="status")] = "all",
        limit: Annotated[int, Query(ge=1, le=50)] = 50,
        cursor: Annotated[str | None, Query(min_length=1, max_length=512)] = None,
        page: Annotated[int | None, Query(ge=1)] = None,
        q: Annotated[str | None, Query(min_length=1, max_length=256)] = None,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> dict[str, object]:
        _require_admin(ledger, abt_admin_session)
        try:
            return ledger.product_pairs_page(limit=limit, cursor=cursor, page=page, status=status_filter, query=q)
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)) from error

    @app.post("/api/admin/product-pairs/{product_pair_id}/workers/{worker_id}/compatibility-check")
    async def check_product_pair_worker_compatibility(
        product_pair_id: str,
        worker_id: str,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            reference = ledger.product_pair_worker_reference(product_pair_id, worker_id)
            connection = _connected_worker_session(
                worker_connections,
                worker_id,
                reason="Selected worker must remain connected during product pair compatibility checks.",
            )
            request_analysis_id = f"compatibility-check:{uuid4()}"
            response = await _request_product_catalog_evidence(connection, request_analysis_id, {})
            live_catalog = _validated_product_catalog_response(response, request_analysis_id)
            live_specification = next(
                (
                    symbol
                    for symbol in cast(list[dict[str, object]], live_catalog["symbols"])
                    if symbol["symbol"] == reference["reference_symbol"]
                ),
                None,
            )
            hard_block_differences, warning_differences = _product_pair_compatibility_differences(
                cast(dict[str, object], reference["reference_specification"]),
                live_specification,
            )
            return ledger.record_product_pair_worker_compatibility_check(
                product_pair_id,
                worker_id,
                checked_by=username,
                reference_symbol=str(reference["reference_symbol"]),
                reference_specification=cast(dict[str, object], reference["reference_specification"]),
                live_specification=live_specification,
                hard_block_differences=hard_block_differences,
                warning_differences=warning_differences,
            )
        except (LedgerError, ValueError) as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

    @app.post("/api/admin/product-pairs/{product_pair_id}/workers/{worker_id}/exclude")
    def exclude_product_pair_worker(
        product_pair_id: str,
        worker_id: str,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            return ledger.exclude_product_pair_worker(product_pair_id, worker_id, excluded_by=username)
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

    @app.post("/api/admin/product-pairs/{product_pair_id}/retests", status_code=status.HTTP_201_CREATED)
    async def retest_product_pair(
        product_pair_id: str,
        body: ProductPairRetestRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            analysis_period = _previous_complete_utc_week()
            retest = ledger.create_product_pair_retest(
                product_pair_id,
                first_worker_id=body.first_worker_id,
                second_worker_id=body.second_worker_id,
                requested_by=username,
                analysis_period=analysis_period,
            )
            first_worker = cast(dict[str, object], retest["source_workers"])["first_worker"]
            second_worker = cast(dict[str, object], retest["source_workers"])["second_worker"]
            first_connection = _connected_worker_session(
                worker_connections,
                str(cast(dict[str, object], first_worker)["worker_id"]),
                reason="Selected worker must remain connected during product pair re-tests.",
            )
            second_connection = _connected_worker_session(
                worker_connections,
                str(cast(dict[str, object], second_worker)["worker_id"]),
                reason="Selected worker must remain connected during product pair re-tests.",
            )
            policy = cast(dict[str, object], retest["policy_snapshot"])
            current_stage = "catalog"
            m15_screening_results: list[dict[str, object]] = []
            async with _pair_worker_execution(
                worker_execution_locks,
                str(cast(dict[str, object], first_worker)["worker_id"]),
                str(cast(dict[str, object], second_worker)["worker_id"]),
            ):
                try:
                    first_result, second_result = await asyncio.gather(
                        _request_product_catalog_evidence(first_connection, str(retest["retest_id"]), policy),
                        _request_product_catalog_evidence(second_connection, str(retest["retest_id"]), policy),
                    )
                    first_evidence = _validated_product_catalog_response(first_result, str(retest["retest_id"]))
                    second_evidence = _validated_product_catalog_response(second_result, str(retest["retest_id"]))
                    ledger.record_product_pair_retest_catalog(
                        str(retest["retest_id"]),
                        first_evidence=first_evidence,
                        second_evidence=second_evidence,
                    )
                    candidate = _product_pair_retest_candidate(
                        cast(list[dict[str, object]], retest["reference_specifications"]),
                        first_evidence,
                        second_evidence,
                    )
                    current_stage = "m15_screening"
                    first_market_data, second_market_data = await _request_market_data_with_retry(
                        str(retest["retest_id"]),
                        first_connection=first_connection,
                        second_connection=second_connection,
                        first_symbols=[str(candidate["first_symbol"])],
                        second_symbols=[str(candidate["second_symbol"])],
                        analysis_period=analysis_period,
                        policy=policy,
                        stage="m15_screening",
                        timeframe="M15",
                        record_retry=lambda reason: ledger.record_product_pair_retest_retry(
                            str(retest["retest_id"]),
                            "m15_screening",
                            reason,
                        ),
                    )
                    m15_screening_results = _screen_m15_candidates(
                        [candidate],
                        first_market_data,
                        second_market_data,
                        policy,
                    )
                    m1_verification_results: list[dict[str, object]] = []
                    if m15_screening_results[0]["screening_status"] == "passed":
                        current_stage = "m1_verification"
                        first_m1_market_data, second_m1_market_data = await _request_market_data_with_retry(
                            str(retest["retest_id"]),
                            first_connection=first_connection,
                            second_connection=second_connection,
                            first_symbols=[str(candidate["first_symbol"])],
                            second_symbols=[str(candidate["second_symbol"])],
                            analysis_period=analysis_period,
                            policy=policy,
                            stage="m1_verification",
                            timeframe="M1",
                            record_retry=lambda reason: ledger.record_product_pair_retest_retry(
                                str(retest["retest_id"]),
                                "m1_verification",
                                reason,
                            ),
                        )
                        m1_verification_results = _verify_m1_candidates(
                            [candidate],
                            first_m1_market_data,
                            second_m1_market_data,
                            cast(list[dict[str, object]], first_evidence["symbols"]),
                            cast(list[dict[str, object]], second_evidence["symbols"]),
                            policy,
                        )
                    passed = (
                        bool(m1_verification_results)
                        and m1_verification_results[0]["verification_status"] == "passed"
                    )
                    failure_reason = None if passed else "Re-test failed the original analysis policy."
                    return ledger.complete_product_pair_retest(
                        str(retest["retest_id"]),
                        m15_screening_results=m15_screening_results,
                        m1_verification_results=m1_verification_results,
                        passed=passed,
                        failure_reason=failure_reason,
                    )
                except (asyncio.TimeoutError, LedgerError, ValueError) as error:
                    stage = {
                        "catalog": "catalog_failed",
                        "m15_screening": "m15_failed",
                        "m1_verification": "m1_failed",
                    }[current_stage]
                    return ledger.fail_product_pair_retest(
                        str(retest["retest_id"]),
                        str(error),
                        stage=stage,
                        clear_m15_results=stage != "m1_failed",
                        m15_screening_results=m15_screening_results,
                    )
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

    @app.post("/api/admin/product-pairs", status_code=status.HTTP_201_CREATED)
    async def build_product_pair(
        body: ProductPairConfirmationUseRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            result = ledger.build_product_pair(body.confirmation_id, username)
            await _broadcast_management_snapshot(ledger, admin_notification_connections)
            return result
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

    @app.post("/api/admin/product-pairs/{product_pair_id}/replace", status_code=status.HTTP_201_CREATED)
    async def replace_product_pair(
        product_pair_id: str,
        body: ProductPairConfirmationUseRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            result = ledger.replace_product_pair(
                product_pair_id,
                confirmation_id=body.confirmation_id,
                replaced_by=username,
            )
            await _broadcast_management_snapshot(ledger, admin_notification_connections)
            return result
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

    @app.post("/api/admin/product-pairs/{product_pair_id}/retire")
    async def retire_product_pair(
        product_pair_id: str,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            result = ledger.retire_product_pair(product_pair_id, username)
            await _broadcast_management_snapshot(ledger, admin_notification_connections)
            return result
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

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
            *(connection.close(code=status.WS_1008_POLICY_VIOLATION) for connection in connections),
            return_exceptions=True,
        )
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
            trader_connections.setdefault(trader.trader_id, set()).add(websocket)
            session_id = ledger.open_trader_session(trader.trader_id)
            cursor = ledger.trader_event_cursor(trader.trader_id)
            await websocket.send_json({"type": "authenticated", "trader_id": trader.trader_id, "cursor": cursor})
            delivered_event_ids: set[int] = set()
            last_delivered_event_id = cursor

            async def deliver_events_after(event_cursor: int) -> None:
                nonlocal last_delivered_event_id
                for event in ledger.trader_events_after(trader.trader_id, event_cursor):
                    await websocket.send_json({"type": "event", **jsonable_encoder(event)})
                    event_id = int(event["event_id"])
                    delivered_event_ids.add(event_id)
                    last_delivered_event_id = max(last_delivered_event_id, event_id)

            await deliver_events_after(cursor)
            last_controller_heartbeat = monotonic()
            while True:
                ledger.trader_for_certificate(trader.trader_id, certificate)
                try:
                    message = await asyncio.wait_for(websocket.receive_json(), timeout=30)
                except asyncio.TimeoutError:
                    await websocket.send_json({"type": "heartbeat"})
                    last_controller_heartbeat = monotonic()
                    continue
                if not isinstance(message, dict) or not isinstance(message.get("type"), str):
                    raise ValueError("Invalid Trader message.")
                if monotonic() - last_controller_heartbeat >= 30:
                    await websocket.send_json({"type": "heartbeat"})
                    last_controller_heartbeat = monotonic()
                message_type = message["type"]
                if message_type == "heartbeat" and set(message) == {"type"}:
                    ledger.record_trader_signal(session_id)
                    await websocket.send_json({"type": "heartbeat_ack"})
                elif message_type == "ack" and set(message) == {"type", "cursor"} and isinstance(message["cursor"], int):
                    ledger.record_trader_signal(session_id)
                    ledger.acknowledge_trader_events(
                        trader.trader_id, message["cursor"], cursor, delivered_event_ids
                    )
                    await websocket.send_json({"type": "acknowledged", "cursor": message["cursor"]})
                elif message_type == "resume" and set(message) == {"type", "cursor"} and isinstance(message["cursor"], int):
                    ledger.record_trader_signal(session_id)
                    await deliver_events_after(message["cursor"])
                elif (
                    message_type == "command"
                    and set(message) == {"type", "command_id", "payload"}
                    and isinstance(message["command_id"], str)
                    and isinstance(message["payload"], dict)
                ):
                    ledger.active_trader(trader.trader_id)
                    ledger.record_trader_signal(session_id)
                    payload = cast(dict[str, Any], message["payload"])
                    if payload.get("type") == "intent":
                        result = await _preflight_trader_intent(
                            ledger,
                            worker_connections,
                            worker_execution_locks,
                            trader.trader_id,
                            message["command_id"],
                            payload,
                        )
                    elif payload.get("type") == "cancel":
                        cancellation = TraderIntentCancellationPayload.model_validate(payload)
                        result, preflight, scheduled = ledger.request_trader_intent_cancellation(
                            trader.trader_id, message["command_id"], cancellation.model_dump(mode="json")
                        )
                        if scheduled:
                            asyncio.create_task(
                                _cancel_intent(
                                    ledger, worker_connections, worker_execution_locks,
                                    cancellation.intent_id, preflight,
                                )
                            )
                    else:
                        raise ValueError("Invalid Trader command.")
                    await websocket.send_json(result)
                    await deliver_events_after(last_delivered_event_id)
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
            if trader_id is not None:
                connections = trader_connections.get(trader_id)
                if connections is not None:
                    connections.discard(websocket)
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
            ledger.record_worker_session(worker.worker_id)
            connection = _WorkerSessionConnection(websocket)
            worker_connections.setdefault(worker.worker_id, set()).add(connection)
            sender_task = asyncio.create_task(_send_worker_analysis_requests(connection))
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
                message_type = request.get("type")
                if message_type == "password_request" and set(request) == {"type"} and secret_store is not None:
                    await websocket.send_json(
                        {"type": "password", "password": secret_store.read_password(worker.password_secret_ref)}
                    )
                elif message_type == "heartbeat" and set(request) == {"type"}:
                    ledger.record_worker_heartbeat(worker.worker_id)
                    await websocket.send_json({"type": "heartbeat_ack"})
                elif (
                    message_type == "safety_state"
                    and set(request) == {"type", "state", "reason"}
                    and isinstance(request.get("state"), str)
                    and isinstance(request.get("reason"), str)
                    and request["reason"]
                ):
                    if request["state"] == "frozen":
                        ledger.freeze_worker_and_active_counterparts(
                            worker.worker_id,
                            "worker_reported_safety_state",
                            {"reason": request["reason"]},
                        )
                    else:
                        ledger.record_worker_safety_state(worker.worker_id, request["state"], request["reason"])
                    await websocket.send_json({"type": "accepted", "state": request["state"]})
                    await _broadcast_management_snapshot(ledger, admin_notification_connections)
                elif message_type == "live_state_snapshot":
                    _record_live_state_snapshot(ledger, worker.worker_id, request)
                    await websocket.send_json({"type": "live_state_accepted"})
                    await _broadcast_management_snapshot(ledger, admin_notification_connections)
                elif message_type == "live_state_diff":
                    _record_live_state_diff(ledger, worker.worker_id, request)
                    await websocket.send_json({"type": "live_state_accepted"})
                    await _broadcast_management_snapshot(ledger, admin_notification_connections)
                elif message_type == "snapshot":
                    _record_snapshot(ledger, worker.worker_id, request)
                    await websocket.send_json({"type": "accepted", "cursor": request["cursor"]})
                    await _broadcast_management_snapshot(ledger, admin_notification_connections)
                    asyncio.create_task(
                        _fail_undispatched_accepted_intents_after_startup_reconciliation(
                            ledger, startup_reconciliation_started_at, worker_connections, worker_execution_locks
                        )
                    )
                    asyncio.create_task(
                        _recover_manual_trades_after_startup_reconciliation(
                            ledger, startup_reconciliation_started_at, worker_connections, worker_execution_locks
                        )
                    )
                elif message_type == "delta":
                    _record_delta(ledger, worker.worker_id, request)
                    await websocket.send_json({"type": "accepted", "cursor": request["cursor"]})
                    await _broadcast_management_snapshot(ledger, admin_notification_connections)
                elif message_type == "product_catalog_analysis_response":
                    _record_product_catalog_analysis_response(connection, request)
                elif message_type == "product_catalog_analysis_error":
                    _record_product_catalog_analysis_error(connection, request)
                elif message_type == "order_check_response":
                    _record_order_check_response(connection, request)
                elif message_type == "order_check_error":
                    _record_order_check_error(connection, request)
                elif message_type == "order_execute_response":
                    _record_order_execute_response(connection, request)
                elif message_type == "order_execute_error":
                    _record_order_execute_error(connection, request)
                elif message_type == "execution_recovery_response":
                    _record_execution_recovery_response(connection, request)
                elif message_type == "execution_recovery_error":
                    _record_execution_recovery_error(connection, request)
                else:
                    raise ValueError("Invalid protocol message.")
        except WebSocketDisconnect as error:
            _LOGGER.info("Worker session disconnected with code %s.", error.code)
            if "worker" in locals():
                _freeze_worker_session(
                    ledger,
                    worker.worker_id,
                    "worker_session_disconnected",
                    f"Worker session disconnected with code {error.code}.",
                )
            await _close_policy_violation(websocket)
        except (LedgerError, ProofError, SecretStoreError, ValueError) as error:
            _LOGGER.warning("Worker session authentication or request failed: %s", error)
            if "worker" in locals():
                _freeze_worker_session(
                    ledger,
                    worker.worker_id,
                    "worker_session_failure",
                    f"Worker session request failed: {error}",
                )
            await _close_policy_violation(websocket)
        except Exception:
            _LOGGER.exception("Worker session authentication or request failed unexpectedly.")
            if "worker" in locals():
                _freeze_worker_session(
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
                _fail_pending_worker_analysis_requests(connection, "Worker disconnected during product catalog analysis.")
            connections = worker_connections.get(worker.worker_id) if "worker" in locals() else None
            if connections is not None and "connection" in locals():
                connections.discard(connection)
                if not connections:
                    worker_connections.pop(worker.worker_id, None)

    if spa_directory is not None:
        def management_spa_route() -> FileResponse:
            return FileResponse(spa_directory / "index.html")

        for spa_route in (
            "/analysis", "/analysis/{analysis_path:path}", "/audit", "/workers", "/worker-recovery", "/traders", "/intents",
            "/manual-trading", "/pairs/{pair_status:path}",
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
    if not connections:
        raise LedgerError(reason)
    return next(iter(connections))


async def _send_worker_analysis_requests(connection: _WorkerSessionConnection) -> None:
    while True:
        request = await asyncio.to_thread(connection.outbound.get)
        if request is None:
            return
        connection.pending[request.request_id] = request
        await connection.websocket.send_json(request.message)


async def _request_product_catalog_evidence(
    connection: _WorkerSessionConnection,
    analysis_id: str,
    policy: dict[str, object],
) -> dict[str, object]:
    return await _request_worker_analysis(
        connection,
        analysis_id=analysis_id,
        stage="catalog",
        timeout=5,
        message={
            "type": "product_catalog_analysis_request",
            "stage": "catalog",
            "analysis_id": analysis_id,
            "request_id": str(uuid4()),
            "policy": policy,
        },
    )


async def _request_market_data_with_retry(
    analysis_id: str,
    *,
    first_connection: _WorkerSessionConnection,
    second_connection: _WorkerSessionConnection,
    first_symbols: list[str],
    second_symbols: list[str],
    analysis_period: dict[str, object],
    policy: dict[str, object],
    stage: str,
    timeframe: str,
    record_retry: Callable[[str], None],
) -> tuple[dict[str, object], dict[str, object]]:
    last_error: asyncio.TimeoutError | LedgerError | ValueError | None = None
    for attempt in range(2):
        try:
            first_response, second_response = await asyncio.gather(
                _request_worker_analysis(
                    first_connection,
                    analysis_id=analysis_id,
                    stage=stage,
                    timeout=_MARKET_DATA_REQUEST_TIMEOUT_SECONDS,
                    message=_market_data_request(analysis_id, stage, timeframe, first_symbols, analysis_period, policy),
                ),
                _request_worker_analysis(
                    second_connection,
                    analysis_id=analysis_id,
                    stage=stage,
                    timeout=_MARKET_DATA_REQUEST_TIMEOUT_SECONDS,
                    message=_market_data_request(analysis_id, stage, timeframe, second_symbols, analysis_period, policy),
                ),
            )
            return (
                _validated_market_data_response(first_response, analysis_id, stage, timeframe, first_symbols, analysis_period),
                _validated_market_data_response(second_response, analysis_id, stage, timeframe, second_symbols, analysis_period),
            )
        except (asyncio.TimeoutError, LedgerError, ValueError) as error:
            last_error = error
            _LOGGER.warning(
                "Market-data analysis %s failed at %s (%s), attempt %s/2: %r",
                analysis_id,
                stage,
                timeframe,
                attempt + 1,
                error,
            )
            if attempt == 1:
                break
            record_retry(_market_data_request_error_reason(error))
    assert last_error is not None
    if isinstance(last_error, asyncio.TimeoutError):
        raise LedgerError(_market_data_request_error_reason(last_error)) from last_error
    raise last_error


def _market_data_request_error_reason(error: asyncio.TimeoutError | LedgerError | ValueError) -> str:
    if isinstance(error, asyncio.TimeoutError):
        return f"Worker did not respond within {_MARKET_DATA_REQUEST_TIMEOUT_SECONDS} seconds."
    return str(error)


async def _request_worker_analysis(
    connection: _WorkerSessionConnection,
    *,
    analysis_id: str,
    stage: str,
    timeout: int,
    message: dict[str, object],
) -> dict[str, object]:
    request = _WorkerAnalysisRequest(
        analysis_id=analysis_id,
        request_id=str(message["request_id"]),
        stage=stage,
        message=message,
        future=ConcurrentFuture(),
    )
    connection.outbound.put(request)
    try:
        return await asyncio.wait_for(asyncio.wrap_future(request.future), timeout=timeout)
    finally:
        connection.pending.pop(request.request_id, None)


def _record_product_catalog_analysis_response(
    connection: _WorkerSessionConnection,
    request: dict[str, object],
) -> None:
    analysis_id = _required_text(request, "analysis_id")
    request_id = _required_text(request, "request_id")
    pending = connection.pending.pop(request_id, None)
    if pending is None:
        return
    if pending.analysis_id != analysis_id or request.get("stage", "catalog") != pending.stage:
        raise ValueError("Invalid worker product catalog analysis response.")
    if not pending.future.done():
        _LOGGER.info(
            "Worker completed %s analysis request %s in %.3fs.",
            pending.stage,
            request_id,
            monotonic() - pending.started_at,
        )
        pending.future.set_result(request)


def _record_product_catalog_analysis_error(
    connection: _WorkerSessionConnection,
    request: dict[str, object],
) -> None:
    analysis_id = _required_text(request, "analysis_id")
    request_id = _required_text(request, "request_id")
    reason = _required_text(request, "reason")
    pending = connection.pending.get(request_id)
    if pending is None:
        return
    stage = request.get("stage", "catalog")
    if pending.analysis_id != analysis_id or stage != pending.stage:
        raise ValueError("Invalid worker product catalog analysis error.")
    expected_fields = {"type", "analysis_id", "request_id", "stage", "reason"}
    if stage in {"m15_screening", "m1_verification"}:
        if _required_text(request, "timeframe") not in {"M15", "M1"}:
            raise ValueError("Invalid worker product catalog analysis error.")
        expected_fields.add("timeframe")
    elif stage != "catalog":
        raise ValueError("Invalid worker product catalog analysis error.")
    if set(request) != expected_fields:
        raise ValueError("Invalid worker product catalog analysis error.")
    connection.pending.pop(request_id, None)
    if not pending.future.done():
        _LOGGER.warning(
            "Worker failed %s analysis request %s after %.3fs: %s",
            pending.stage,
            request_id,
            monotonic() - pending.started_at,
            reason,
        )
        pending.future.set_exception(LedgerError(reason))


def _record_order_check_response(connection: _WorkerSessionConnection, request: dict[str, object]) -> None:
    request_id = _required_text(request, "request_id")
    pending = connection.pending.pop(request_id, None)
    if pending is None:
        return
    if pending.stage != "order_check" or request.get("analysis_id") != "order_check":
        raise ValueError("Invalid worker order-check response.")
    if not pending.future.done():
        pending.future.set_result(request)


def _record_order_check_error(connection: _WorkerSessionConnection, request: dict[str, object]) -> None:
    request_id = _required_text(request, "request_id")
    pending = connection.pending.pop(request_id, None)
    if pending is None:
        return
    if pending.stage != "order_check" or request.get("analysis_id") != "order_check":
        raise ValueError("Invalid worker order-check error.")
    if set(request) != {"type", "analysis_id", "request_id", "reason"}:
        raise ValueError("Invalid worker order-check error.")
    pending.future.set_exception(LedgerError(_required_text(request, "reason")))


def _record_order_execute_response(connection: _WorkerSessionConnection, request: dict[str, object]) -> None:
    request_id = _required_text(request, "request_id")
    pending = connection.pending.pop(request_id, None)
    if pending is None:
        return
    if pending.stage != "order_execute" or set(request) != {"type", "request_id", "accepted", "order", "result"}:
        raise ValueError("Invalid worker order execution response.")
    if not pending.future.done():
        pending.future.set_result(request)


def _record_order_execute_error(connection: _WorkerSessionConnection, request: dict[str, object]) -> None:
    request_id = _required_text(request, "request_id")
    pending = connection.pending.pop(request_id, None)
    if pending is None:
        return
    if pending.stage != "order_execute" or set(request) != {"type", "request_id", "reason"}:
        raise ValueError("Invalid worker order execution error.")
    pending.future.set_exception(LedgerError(_required_text(request, "reason")))


def _record_execution_recovery_response(connection: _WorkerSessionConnection, request: dict[str, object]) -> None:
    request_id = _required_text(request, "request_id")
    pending = connection.pending.pop(request_id, None)
    if pending is None:
        return
    if pending.stage not in {"execution_reconcile", "execution_cancel", "execution_close", "worker_cleanup_reconcile",
                             "worker_cleanup_cancel", "worker_cleanup_close"} or set(request) != {
        "type", "request_id", "operation", "accepted", "result"
    } or request["operation"] != pending.stage or not isinstance(request["accepted"], bool) or not isinstance(request["result"], dict):
        raise ValueError("Invalid worker execution recovery response.")
    if not pending.future.done():
        pending.future.set_result(request)


def _record_execution_recovery_error(connection: _WorkerSessionConnection, request: dict[str, object]) -> None:
    request_id = _required_text(request, "request_id")
    pending = connection.pending.pop(request_id, None)
    if pending is None:
        return
    if pending.stage not in {"execution_reconcile", "execution_cancel", "execution_close", "worker_cleanup_reconcile",
                             "worker_cleanup_cancel", "worker_cleanup_close"} or set(request) != {
        "type", "request_id", "operation", "reason"
    } or request["operation"] != pending.stage:
        raise ValueError("Invalid worker execution recovery error.")
    pending.future.set_exception(LedgerError(_required_text(request, "reason")))


def _fail_pending_worker_analysis_requests(connection: _WorkerSessionConnection, reason: str) -> None:
    for request in connection.pending.values():
        if not request.future.done():
            request.future.set_exception(LedgerError(reason))
    connection.pending.clear()


def _validated_product_catalog_response(response: dict[str, object], analysis_id: str) -> dict[str, object]:
    if response.get("type") != "product_catalog_analysis_response":
        raise ValueError("Worker returned an invalid product catalog analysis response.")
    if response.get("stage", "catalog") != "catalog":
        raise ValueError("Worker returned an invalid product catalog analysis response.")
    if _required_text(response, "analysis_id") != analysis_id:
        raise ValueError("Worker returned a mismatched product catalog analysis response.")
    collected_at = _required_text(response, "collected_at")
    raw_symbols = response.get("symbols")
    if not isinstance(raw_symbols, list):
        raise ValueError("Worker returned incomplete product catalog evidence.")
    return {
        "collected_at": collected_at,
        "symbols": [_validated_symbol_specification(symbol) for symbol in raw_symbols],
    }


def _validated_symbol_specification(symbol: object) -> dict[str, object]:
    if not isinstance(symbol, dict):
        raise ValueError("Worker returned incomplete product catalog evidence.")
    normalized = {
        "symbol": symbol.get("symbol"),
        "trade_calc_mode": symbol.get("trade_calc_mode"),
        "currency_base": symbol.get("currency_base"),
        "currency_profit": symbol.get("currency_profit"),
        "digits": symbol.get("digits"),
        "point": symbol.get("point"),
    }
    if not all(isinstance(normalized[field], str) and normalized[field] for field in ("symbol", "currency_base", "currency_profit")):
        raise ValueError("Worker returned incomplete product catalog evidence.")
    trade_calc_mode = normalized["trade_calc_mode"]
    if isinstance(trade_calc_mode, bool) or not isinstance(trade_calc_mode, (int, str)):
        raise ValueError("Worker returned incomplete product catalog evidence.")
    if isinstance(trade_calc_mode, str) and not trade_calc_mode:
        raise ValueError("Worker returned incomplete product catalog evidence.")
    if not isinstance(normalized["digits"], int) or isinstance(normalized["digits"], bool):
        raise ValueError("Worker returned incomplete product catalog evidence.")
    if not isinstance(normalized["point"], (int, float)) or isinstance(normalized["point"], bool):
        raise ValueError("Worker returned incomplete product catalog evidence.")
    for field in ("trade_tick_size", "contract_size", "volume_min", "volume_step", "volume_max", "trade_tick_value", "swap_long", "swap_short"):
        value = symbol.get(field)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ValueError("Worker returned incomplete product catalog evidence.")
        normalized[field] = float(value)
    for field in ("trade_stops_level", "trade_freeze_level", "swap_rollover3days"):
        value = symbol.get(field)
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError("Worker returned incomplete product catalog evidence.")
        normalized[field] = value
    swap_mode = symbol.get("swap_mode")
    if swap_mode is not None and (not isinstance(swap_mode, int) or isinstance(swap_mode, bool)):
        raise ValueError("Worker returned incomplete product catalog evidence.")
    normalized["swap_mode"] = swap_mode
    for field in ("currency_margin",):
        value = symbol.get(field)
        if not isinstance(value, str) or not value:
            raise ValueError("Worker returned incomplete product catalog evidence.")
        normalized[field] = value
    for field in ("filling_modes", "allowed_directions"):
        value = symbol.get(field)
        if not isinstance(value, list) or not value or not all(isinstance(item, str) and item for item in value):
            raise ValueError("Worker returned incomplete product catalog evidence.")
        normalized[field] = list(value)
    return normalized


def _validated_market_data_response(
    response: dict[str, object],
    analysis_id: str,
    stage: str,
    timeframe: str,
    requested_symbols: list[str],
    analysis_period: dict[str, object],
) -> dict[str, object]:
    if response.get("type") != "product_catalog_analysis_response":
        raise ValueError("Worker returned an invalid market-data analysis response.")
    if response.get("stage") != stage:
        raise ValueError("Worker returned an invalid market-data analysis response.")
    if _required_text(response, "analysis_id") != analysis_id:
        raise ValueError("Worker returned a mismatched market-data analysis response.")
    if _required_text(response, "timeframe") != timeframe:
        raise ValueError("Worker returned incomplete market-data evidence.")
    if _required_text(response, "period_start_utc") != str(analysis_period["started_at_utc"]):
        raise ValueError("Worker returned incomplete market-data evidence.")
    if _required_text(response, "period_end_utc") != str(analysis_period["ended_at_utc"]):
        raise ValueError("Worker returned incomplete market-data evidence.")
    raw_symbols = response.get("symbols")
    if not isinstance(raw_symbols, list):
        raise ValueError("Worker returned incomplete market-data evidence.")
    symbols: dict[str, dict[str, object]] = {}
    for symbol_payload in raw_symbols:
        symbol = _validated_market_data_symbol(symbol_payload)
        _validate_weekday_market_data_completeness(symbol, analysis_period)
        symbols[str(symbol["symbol"])] = symbol
    if set(symbols) != set(requested_symbols):
        raise ValueError("Worker returned incomplete market-data evidence.")
    return {
        "collected_at": _required_text(response, "collected_at"),
        "timeframe": timeframe,
        "period_start_utc": _required_text(response, "period_start_utc"),
        "period_end_utc": _required_text(response, "period_end_utc"),
        "symbols": symbols,
    }


def _validate_weekday_market_data_completeness(
    symbol: dict[str, object],
    analysis_period: dict[str, object],
) -> None:
    """Require one UTC bar per weekday, without assuming broker intraday sessions."""

    start = _parse_utc_timestamp(str(analysis_period["started_at_utc"]))
    end = _parse_utc_timestamp(str(analysis_period["ended_at_utc"]))
    expected_days = {
        (start + timedelta(days=offset)).date()
        for offset in range((end.date() - start.date()).days)
        if (start + timedelta(days=offset)).weekday() < 5
    }
    observed_days: set[date] = set()
    for bar in cast(list[dict[str, object]], symbol["bars"]):
        observed_at = _parse_utc_timestamp(str(bar["time_utc"]))
        if start <= observed_at < end and observed_at.weekday() < 5:
            observed_days.add(observed_at.date())
    if not expected_days.issubset(observed_days):
        raise ValueError("Worker market-data evidence must include every UTC weekday in the requested period.")


def _parse_utc_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("Worker returned incomplete market-data evidence.") from error
    if parsed.tzinfo is None:
        raise ValueError("Worker returned incomplete market-data evidence.")
    return parsed.astimezone(UTC)


def _validated_market_data_symbol(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("Worker returned incomplete market-data evidence.")
    symbol = _required_text(value, "symbol")
    bars = value.get("bars")
    time_metadata = value.get("time_metadata")
    if not isinstance(bars, list) or not bars or not isinstance(time_metadata, dict):
        raise ValueError("Worker returned incomplete market-data evidence.")
    normalized_bars: list[dict[str, object]] = []
    seen_times: set[str] = set()
    for bar in bars:
        if not isinstance(bar, dict):
            raise ValueError("Worker returned incomplete market-data evidence.")
        time_utc = _required_text(bar, "time_utc")
        if time_utc in seen_times:
            raise ValueError("Worker returned incomplete market-data evidence.")
        seen_times.add(time_utc)
        raw_time = bar.get("time")
        close = bar.get("close")
        if not isinstance(raw_time, (int, float)) or isinstance(raw_time, bool) or raw_time <= 0:
            raise ValueError("Worker returned incomplete market-data evidence.")
        if not isinstance(close, (int, float)) or isinstance(close, bool):
            raise ValueError("Worker returned incomplete market-data evidence.")
        normalized_bars.append({"time": int(raw_time), "time_utc": time_utc, "close": float(close)})
    return {"symbol": symbol, "bars": normalized_bars, "time_metadata": dict(time_metadata)}


def _analyze_product_catalogs(
    first_symbols: list[dict[str, object]],
    second_symbols: list[dict[str, object]],
) -> list[dict[str, object]]:
    first_index: dict[tuple[str, str], list[dict[str, object]]] = {}
    second_index: dict[tuple[str, str], list[dict[str, object]]] = {}
    for symbol in first_symbols:
        first_index.setdefault((str(symbol["currency_base"]), str(symbol["currency_profit"])), []).append(symbol)
    for symbol in second_symbols:
        second_index.setdefault((str(symbol["currency_base"]), str(symbol["currency_profit"])), []).append(symbol)
    candidates: list[dict[str, object]] = []
    for currencies in sorted(set(first_index) & set(second_index)):
        for first in first_index[currencies]:
            for second in second_index[currencies]:
                if first["trade_calc_mode"] == second["trade_calc_mode"]:
                    candidates.append(
                        {
                            "first_symbol": first["symbol"],
                            "second_symbol": second["symbol"],
                            "currency_base": first["currency_base"],
                            "currency_profit": first["currency_profit"],
                            "first_point": first["point"],
                            "second_point": second["point"],
                        }
                    )
    return candidates


def _candidate_symbols(candidates: list[dict[str, object]], field: str) -> list[str]:
    return list(dict.fromkeys(str(candidate[field]) for candidate in candidates))


def _product_pair_retest_candidate(
    reference_specifications: list[dict[str, object]],
    first_evidence: dict[str, object],
    second_evidence: dict[str, object],
) -> dict[str, object]:
    first_server = str(reference_specifications[0]["server"])
    second_server = str(reference_specifications[1]["server"])
    first_symbol_name = str(reference_specifications[0]["symbol"])
    second_symbol_name = str(reference_specifications[1]["symbol"])
    first_specification = next(
        (
            symbol
            for symbol in cast(list[dict[str, object]], first_evidence["symbols"])
            if symbol["symbol"] == first_symbol_name
        ),
        None,
    )
    second_specification = next(
        (
            symbol
            for symbol in cast(list[dict[str, object]], second_evidence["symbols"])
            if symbol["symbol"] == second_symbol_name
        ),
        None,
    )
    if first_specification is None or second_specification is None:
        raise ValueError("Worker returned incomplete product catalog evidence for the active product pair.")
    eligible_candidates = _analyze_product_catalogs([first_specification], [second_specification])
    candidate = next(
        (
            item
            for item in eligible_candidates
            if item["first_symbol"] == first_symbol_name and item["second_symbol"] == second_symbol_name
        ),
        None,
    )
    if candidate is None:
        raise ValueError(
            f"Active product pair symbols {first_server}:{first_symbol_name} and "
            f"{second_server}:{second_symbol_name} no longer satisfy the original catalog requirements."
        )
    return candidate


def _market_data_request(
    analysis_id: str,
    stage: str,
    timeframe: str,
    symbols: list[str],
    analysis_period: dict[str, object],
    policy: dict[str, object],
) -> dict[str, object]:
    return {
        "type": "product_catalog_analysis_request",
        "stage": stage,
        "analysis_id": analysis_id,
        "request_id": str(uuid4()),
        "policy": policy,
        "timeframe": timeframe,
        "period_start_utc": analysis_period["started_at_utc"],
        "period_end_utc": analysis_period["ended_at_utc"],
        "symbols": symbols,
    }


@asynccontextmanager
async def _pair_worker_execution(
    worker_execution_locks: dict[str, asyncio.Lock],
    first_worker_id: str,
    second_worker_id: str,
):
    first_lock = worker_execution_locks.setdefault(first_worker_id, asyncio.Lock())
    second_lock = worker_execution_locks.setdefault(second_worker_id, asyncio.Lock())
    ordered = sorted(
        ((first_worker_id, first_lock), (second_worker_id, second_lock)),
        key=lambda item: item[0],
    )
    await ordered[0][1].acquire()
    if ordered[1][1] is ordered[0][1]:
        try:
            yield
        finally:
            ordered[0][1].release()
        return
    await ordered[1][1].acquire()
    try:
        yield
    finally:
        ordered[1][1].release()
        ordered[0][1].release()


async def _preflight_trader_intent(
    ledger: ControlLedger,
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_execution_locks: dict[str, asyncio.Lock],
    trader_id: str,
    command_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return await _preflight_intent(
        ledger, worker_connections, worker_execution_locks, trader_id, command_id, payload, management=False
    )


async def _preflight_management_intent(
    ledger: ControlLedger,
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_execution_locks: dict[str, asyncio.Lock],
    username: str,
    command_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return await _preflight_intent(
        ledger, worker_connections, worker_execution_locks, username, command_id, payload, management=True
    )


async def _preflight_intent(
    ledger: ControlLedger,
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_execution_locks: dict[str, asyncio.Lock],
    originator: str,
    command_id: str,
    payload: dict[str, Any],
    *,
    management: bool,
) -> dict[str, Any]:
    """Accept an intent only after both selected brokers validate its exact orders."""

    existing = (
        ledger.management_command_result(originator, command_id, payload)
        if management
        else ledger.trader_command_result(originator, command_id, payload)
    )
    if existing is not None:
        return existing
    outcomes: list[dict[str, object]] = []
    try:
        intent = TraderIntentPayload.model_validate(payload)
        remaining_seconds = (intent.expires_at - datetime.now(UTC)).total_seconds()
        if remaining_seconds <= 0:
            raise LedgerError("Intent expiry has passed.")
        deadline = monotonic() + remaining_seconds
        pair = next((item for item in ledger.product_pairs() if item["product_pair_id"] == intent.pair_id), None)
        if pair is None or pair["status"] != "active":
            raise LedgerError("Active product pair does not exist.")
        specifications = cast(list[dict[str, Any]], pair["reference_specifications"])
        if len(specifications) != 2 or any(intent.filling_mode not in item["specification"]["filling_modes"] for item in specifications):
            raise LedgerError("Intent filling mode is not supported by both endpoints.")
        sources = cast(dict[str, dict[str, Any]], pair["source_workers"])
        workers = [sources["first_worker"], sources["second_worker"]]
        frozen_worker_ids = ledger.frozen_worker_ids([str(worker["worker_id"]) for worker in workers])
        if frozen_worker_ids:
            raise LedgerError(
                f"Frozen worker cannot be selected for a new trade: {', '.join(sorted(frozen_worker_ids))}."
            )
        endpoint_by_server = {str(item["server"]): item for item in specifications}
        if set(endpoint_by_server) != {str(worker["server"]) for worker in workers}:
            raise LedgerError("Product pair source workers do not match its endpoints.")
        references = [endpoint_by_server[str(worker["server"])] for worker in workers]
        directions = [intent.primary_direction, "SHORT" if intent.primary_direction == "LONG" else "LONG"]
        if any(
            direction not in reference["specification"]["allowed_directions"]
            or intent.stop_loss_pips * _intent_pip_size(reference["specification"])
            < Decimal(str(reference["specification"]["trade_stops_level"]))
            * Decimal(str(reference["specification"]["point"]))
            or intent.take_profit_pips * _intent_pip_size(reference["specification"])
            < Decimal(str(reference["specification"]["trade_stops_level"]))
            * Decimal(str(reference["specification"]["point"]))
            for direction, reference in zip(directions, references, strict=True)
        ):
            raise LedgerError("Intent direction or protective prices violate endpoint constraints.")
        if any(not _valid_intent_volume(intent.lots, reference["specification"]) for reference in references):
            raise LedgerError("Intent lots are not exactly valid for both endpoints.")
        execution_id = f"abt:{hashlib.sha256(f'{originator}:{command_id}'.encode()).hexdigest()[:24]}"
        orders = [
            _intent_order(intent, reference, primary=index == 0, execution_id=execution_id)
            for index, reference in enumerate(references)
        ]
        if any(not _valid_intent_order_prices(order, reference["specification"]) for order, reference in zip(orders, references, strict=True)):
            raise LedgerError("Intent entry or protective prices are not valid for both endpoint price increments.")
        outcomes = [
            {
                "worker_id": str(worker["worker_id"]),
                "server": str(worker["server"]),
                "status": "not_started",
                "order": order,
            }
            for worker, order in zip(workers, orders, strict=True)
        ]
        connections: list[_WorkerSessionConnection] = []
        for worker, outcome in zip(workers, outcomes, strict=True):
            try:
                connections.append(
                    _connected_worker_session(
                        worker_connections, str(worker["worker_id"]), reason="Selected worker is disconnected."
                    )
                )
            except LedgerError as error:
                outcome.update(status="rejected", reason=str(error))
        if len(connections) != len(workers):
            raise LedgerError("Selected worker is disconnected.")
        if monotonic() >= deadline:
            raise LedgerError("Intent expiry passed before broker preflight.")
        async with _pair_worker_execution(worker_execution_locks, str(workers[0]["worker_id"]), str(workers[1]["worker_id"])):
            responses = await asyncio.gather(
                *(
                    _request_order_check(connection, order, timeout=max(0.001, deadline - monotonic()))
                    for connection, order in zip(connections, orders, strict=True)
                ),
                return_exceptions=True,
            )
        for outcome, response in zip(outcomes, responses, strict=True):
            if isinstance(response, Exception):
                outcome.update(status="rejected", reason=str(response))
            elif _valid_order_check_response(response, cast(dict[str, object], outcome["order"])):
                outcome.update(status="accepted" if response["accepted"] else "rejected", response=response)
            else:
                outcome.update(status="malformed", response=response)
        if monotonic() >= deadline:
            raise LedgerError("Intent expiry passed during broker preflight.")
        if any(outcome["status"] != "accepted" for outcome in outcomes):
            raise LedgerError("A broker rejected the intent order check.")
    except (KeyError, LedgerError, ValidationError, ValueError, asyncio.TimeoutError) as error:
        if not outcomes:
            outcomes = [{"status": "rejected", "reason": str(error)}]
        return (
            ledger.reject_management_intent(originator, command_id, payload, str(error), outcomes)
            if management
            else ledger.reject_trader_command(originator, command_id, payload, str(error), outcomes)
        )
    accepted = (
        ledger.accept_management_intent(originator, command_id, payload, outcomes)
        if management
        else ledger.accept_trader_intent(originator, command_id, payload, outcomes)
    )
    asyncio.create_task(
        _dispatch_accepted_trader_intent(
            ledger,
            worker_connections,
            worker_execution_locks,
            str(accepted["intent_id"]),
            str(payload["filling_mode"]),
            outcomes,
        )
    )
    return accepted


async def _dispatch_accepted_trader_intent(
    ledger: ControlLedger,
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_execution_locks: dict[str, asyncio.Lock],
    intent_id: str,
    filling_mode: str,
    outcomes: list[dict[str, object]],
) -> None:
    """Send only preflighted orders and make every broker result durable."""

    if filling_mode not in {"FOK", "IOC"}:
        raise LedgerError("Intent filling mode is invalid.")
    worker_ids = [str(outcome["worker_id"]) for outcome in outcomes]
    if len(worker_ids) != 2 or len(set(worker_ids)) != 2:
        raise LedgerError("Intent execution requires two distinct worker outcomes.")
    if not ledger.claim_accepted_intent_dispatch(intent_id):
        return
    ledger.record_intent_execution(intent_id, "intent_dispatch_started", {"filling_mode": filling_mode})
    try:
        connections = [
            _connected_worker_session(worker_connections, worker_id, reason="Selected worker disconnected before dispatch.")
            for worker_id in worker_ids
        ]
        async with _pair_worker_execution(worker_execution_locks, *worker_ids):
            frozen_worker_ids = ledger.frozen_worker_ids(worker_ids)
            if frozen_worker_ids:
                raise LedgerError(
                    f"Frozen worker cannot receive a new trade: {', '.join(sorted(frozen_worker_ids))}."
                )
            responses = await asyncio.gather(
                *(
                    _request_order_execute(connection, cast(dict[str, object], outcome["order"]))
                    for connection, outcome in zip(connections, outcomes, strict=True)
                ),
                return_exceptions=True,
            )
    except LedgerError as error:
        _freeze_intent_execution(ledger, intent_id, str(error), worker_ids)
        return

    accepted = True
    for outcome, response in zip(outcomes, responses, strict=True):
        if isinstance(response, Exception):
            accepted = False
            record = {"worker_id": outcome["worker_id"], "order": outcome["order"], "error": str(response)}
        else:
            response = cast(dict[str, object], response)
            valid = response.get("accepted") is True and response.get("order") == outcome["order"]
            accepted = accepted and valid
            record = {
                "worker_id": outcome["worker_id"],
                "order": outcome["order"],
                "response": response,
            }
        ledger.record_intent_execution(intent_id, "intent_leg_dispatched", record)

    if not accepted:
        ledger.record_intent_execution(intent_id, "incomplete_entry_detected", {}, status="working")
        _freeze_intent_execution(
            ledger,
            intent_id,
            f"Incomplete {filling_mode} entry requires worker isolation.",
            worker_ids,
        )
    elif filling_mode == "FOK":
        ledger.record_intent_execution(intent_id, "fok_orders_placed", {}, status="working")
    else:
        ledger.record_intent_execution(
            intent_id,
            "ioc_orders_placed",
            {"notice": "Matched volume is established only by reconciliation; placement acknowledgements are not fills."},
            status="working",
        )


async def _dispatch_manual_trade(
    ledger: ControlLedger,
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_execution_locks: dict[str, asyncio.Lock],
    manual_trade_id: str,
) -> None:
    """Dispatch a manual market pair sequentially, protecting every fill before waiting."""

    if not ledger.claim_manual_trade_dispatch(manual_trade_id):
        return
    plan = ledger.manual_trade_plan(manual_trade_id)
    legs = cast(list[dict[str, object]], plan["legs"])
    worker_ids = [str(leg["worker_id"]) for leg in legs]
    try:
        if len(legs) != 2 or len(set(worker_ids)) != 2:
            raise LedgerError("Manual trade has invalid leg evidence.")
        async with _pair_worker_execution(worker_execution_locks, *worker_ids):
            if ledger.frozen_worker_ids(worker_ids):
                raise LedgerError("A selected worker is frozen before manual-trade dispatch.")
            connections = [
                _connected_worker_session(worker_connections, worker_id, reason="Selected worker disconnected before dispatch.")
                for worker_id in worker_ids
            ]
            execution_id = f"abt:m:{hashlib.sha256(manual_trade_id.encode()).hexdigest()[:24]}"
            first_order = _manual_market_order(legs[0], execution_id)
            first_response = await _request_order_execute(connections[0], first_order)
            first_fill = _manual_fill_price(first_response, first_order)
            first_protection = _manual_protection_order(legs[0], first_response, first_fill)
            protection_response = await _request_order_execute(connections[0], first_protection)
            if protection_response.get("accepted") is not True or protection_response.get("order") != first_protection:
                raise LedgerError("First manual-trade leg protection was not confirmed.")
            ledger.record_manual_trade_event(
                manual_trade_id,
                "manual_trade_first_leg_protected",
                {"leg": legs[0], "fill_price": str(first_fill), "protection": first_protection},
            )
            await asyncio.sleep(int(plan["interval_seconds"]))
            observed = await _execution_recovery_request(
                connections[0], "execution_reconcile", {"execution_id": execution_id}
            )
            state = _cleanup_state(observed)
            if not state["positions"]:
                raise LedgerError("First manual-trade protection exited before second-leg dispatch.")
            second_order = _manual_market_order(legs[1], execution_id)
            second_response = await _request_order_execute(connections[1], second_order)
            second_fill = _manual_fill_price(second_response, second_order)
            second_protection = _manual_protection_order(legs[1], second_response, second_fill)
            second_protection_response = await _request_order_execute(connections[1], second_protection)
            if second_protection_response.get("accepted") is not True or second_protection_response.get("order") != second_protection:
                raise LedgerError("Second manual-trade leg protection was not confirmed.")
            ledger.activate_manual_trade(
                manual_trade_id,
                [
                    {"worker_id": str(legs[0]["worker_id"]), "position": str(first_protection["position"]), "fill_price": str(first_fill)},
                    {"worker_id": str(legs[1]["worker_id"]), "position": str(second_protection["position"]), "fill_price": str(second_fill)},
                ],
            )
            ledger.record_manual_trade_event(
                manual_trade_id,
                "manual_trade_active",
                {
                    "first_leg": {"fill_price": str(first_fill), "protection": first_protection},
                    "second_leg": {"fill_price": str(second_fill), "protection": second_protection},
                },
            )
    except (KeyError, LedgerError, ValueError, asyncio.TimeoutError) as error:
        _freeze_manual_trade(ledger, manual_trade_id, worker_ids, str(error))


def _manual_market_order(leg: dict[str, object], execution_id: str) -> dict[str, object]:
    return {
        "action": "market",
        "symbol": str(leg["symbol"]),
        "volume": str(leg["lots"]),
        "direction": "LONG" if leg["direction"] == "BUY" else "SHORT",
        "filling_mode": "FOK",
        "control_plane_command_id": execution_id,
    }


def _manual_fill_price(response: dict[str, object], order: dict[str, object]) -> Decimal:
    if response.get("accepted") is not True or response.get("order") != order or not isinstance(response.get("result"), dict):
        raise LedgerError("Manual-trade market order was not confirmed.")
    try:
        fill_price = Decimal(str(response["result"]["price"]))
    except (KeyError, ValueError, ArithmeticError) as error:
        raise LedgerError("Manual-trade market order did not return an actual fill price.") from error
    if fill_price <= 0:
        raise LedgerError("Manual-trade market order did not return an actual fill price.")
    return fill_price


def _manual_protection_order(
    leg: dict[str, object], market_response: dict[str, object], fill_price: Decimal
) -> dict[str, object]:
    result = cast(dict[str, object], market_response["result"])
    ticket = result.get("position", result.get("order"))
    if isinstance(ticket, bool) or not isinstance(ticket, (str, int)) or not str(ticket):
        raise LedgerError("Manual-trade market order did not return a position reference.")
    pip_size = Decimal(str(leg["pip_size"]))
    stop_loss_pips = Decimal(str(cast(dict[str, object], leg).get("stop_loss_pips", "0")))
    take_profit_pips = Decimal(str(cast(dict[str, object], leg).get("take_profit_pips", "0")))
    if stop_loss_pips <= 0 or take_profit_pips <= 0:
        raise LedgerError("Manual-trade protection distances are invalid.")
    buy = leg["direction"] == "BUY"
    return {
        "action": "protect",
        "symbol": str(leg["symbol"]),
        "position": str(ticket),
        "sl": str(fill_price - stop_loss_pips * pip_size if buy else fill_price + stop_loss_pips * pip_size),
        "tp": str(fill_price + take_profit_pips * pip_size if buy else fill_price - take_profit_pips * pip_size),
    }


def _freeze_manual_trade(ledger: ControlLedger, manual_trade_id: str, worker_ids: list[str], reason: str) -> None:
    _LOGGER.warning("Manual trade %s froze its participating workers: %s", manual_trade_id, reason)
    ledger.record_manual_trade_event(
        manual_trade_id, "manual_trade_execution_frozen", {"reason": reason}, status="needs_human"
    )
    if worker_ids:
        ledger.freeze_workers(
            "manual_trade_execution_anomaly", worker_ids, {"manual_trade_id": manual_trade_id, "reason": reason}
        )


async def _dispatch_manual_trade_operation(
    ledger: ControlLedger,
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_execution_locks: dict[str, asyncio.Lock],
    operation_id: str,
) -> None:
    if not ledger.claim_manual_trade_operation_dispatch(operation_id):
        return
    record = ledger.manual_trade_operation(operation_id)
    plan = cast(dict[str, object], record["plan"])
    legs = cast(list[dict[str, object]], plan["legs"])
    worker_ids = [str(leg["worker_id"]) for leg in legs]
    try:
        if len(legs) != 2 or len(set(worker_ids)) != 2:
            raise LedgerError("Manual-trade operation has invalid leg evidence.")
        async with _pair_worker_execution(worker_execution_locks, *worker_ids):
            if ledger.frozen_worker_ids(worker_ids):
                raise LedgerError("A participating worker is frozen.")
            connections = [
                _connected_worker_session(worker_connections, worker_id, reason="Participating worker disconnected.")
                for worker_id in worker_ids
            ]
            execution_id = f"abt:m:{hashlib.sha256(str(record['manual_trade_id']).encode()).hexdigest()[:24]}"
            observed = await _reconcile_execution(connections, execution_id)
            positions = []
            for leg, observed_leg in zip(legs, observed, strict=True):
                position = next((item for item in observed_leg["positions"] if item["ticket"] == leg["position"]), None)
                if position is None:
                    raise LedgerError("Active manual-trade position could not be reconciled.")
                positions.append(position)
            if record["operation"] == "exit":
                responses = await asyncio.gather(*(
                    _execution_recovery_request(
                        connection, "execution_close", {"ticket": position["ticket"], "volume": position["volume"]}
                    )
                    for connection, position in zip(connections, positions, strict=True)
                ))
                if any(response.get("accepted") is not True for response in responses):
                    raise LedgerError("Manual-trade full exit was not confirmed for both legs.")
                final = await _reconcile_execution(connections, execution_id)
                if any(leg["orders"] or leg["positions"] for leg in final):
                    raise LedgerError("Manual-trade full exit did not prove zero exposure.")
            else:
                orders = [
                    {
                        "action": "protect", "symbol": str(leg["symbol"]), "position": str(leg["position"]),
                        "sl": str(leg["stop_loss"]), "tp": str(leg["take_profit"]),
                    }
                    for leg in legs
                ]
                responses = await asyncio.gather(*(
                    _request_order_execute(connection, order) for connection, order in zip(connections, orders, strict=True)
                ))
                if any(response.get("accepted") is not True or response.get("order") != order
                       for response, order in zip(responses, orders, strict=True)):
                    raise LedgerError("Manual-trade protection update was not confirmed for both legs.")
            ledger.record_manual_trade_operation(
                operation_id, "manual_trade_operation_completed", {"plan": plan}, status="completed"
            )
    except (KeyError, LedgerError, ValueError, asyncio.TimeoutError) as error:
        ledger.record_manual_trade_operation(
            operation_id, "manual_trade_operation_frozen", {"reason": str(error)}, status="needs_human"
        )
        ledger.freeze_workers(
            "manual_trade_operation_anomaly", worker_ids,
            {"manual_trade_id": record["manual_trade_id"], "operation_id": operation_id, "reason": str(error)},
        )


async def _cancel_intent(
    ledger: ControlLedger,
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_execution_locks: dict[str, asyncio.Lock],
    intent_id: str,
    outcomes: list[dict[str, object]],
) -> None:
    """Cancel zero-fill orders and prove both legs remain unfilled before terminalizing."""

    await _operate_on_intent_exposure(ledger, worker_connections, worker_execution_locks, intent_id, outcomes)


async def _operate_on_intent_exposure(
    ledger: ControlLedger,
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_execution_locks: dict[str, asyncio.Lock],
    intent_id: str,
    outcomes: list[dict[str, object]],
) -> None:
    operation = "Cancellation"
    claim = getattr(ledger, "claim_scheduled_intent_operation", None)
    if callable(claim) and not claim(intent_id):
        return
    worker_ids: list[str] = []
    try:
        worker_ids = [str(outcome["worker_id"]) for outcome in outcomes]
        execution_ids = {
            str(cast(dict[str, object], outcome["order"])["control_plane_command_id"]) for outcome in outcomes
        }
        if len(worker_ids) != 2 or len(set(worker_ids)) != 2 or len(execution_ids) != 1:
            raise LedgerError(f"{operation} has malformed execution evidence.")
        execution_id = execution_ids.pop()
        connections = [_connected_worker_session(worker_connections, worker_id) for worker_id in worker_ids]
        async with _pair_worker_execution(worker_execution_locks, *worker_ids):
            observed = await _reconcile_execution(connections, execution_id)
            ledger.record_intent_execution(
                intent_id,
                "cancellation_reconciled",
                {"execution_id": execution_id, "legs": observed},
            )
            if any(leg["positions"] for leg in observed):
                ledger.record_intent_execution(
                    intent_id, "rejected_due_to_fill", {"execution_id": execution_id, "legs": observed}, status="working"
                )
                _freeze_intent_execution(
                    ledger,
                    intent_id,
                    "Cancellation observed broker exposure.",
                    worker_ids,
                )
                return
            for connection, leg in zip(connections, observed, strict=True):
                for order in leg["orders"]:
                    await _execution_recovery_request(
                        connection, "execution_cancel", {"ticket": order["ticket"], "volume": order["volume"]}
                    )
            final = await _reconcile_execution(connections, execution_id)
            if any(leg["orders"] or leg["positions"] for leg in final):
                raise LedgerError(f"{operation} final reconciliation did not prove zero exposure.")
            ledger.record_intent_execution(
                intent_id,
                "cancelled",
                {"execution_id": execution_id, "legs": final},
                status="cancelled",
            )
    except (KeyError, LedgerError, ValueError, asyncio.TimeoutError) as error:
        _freeze_intent_execution(ledger, intent_id, f"{operation} could not be proven safe: {error}", worker_ids)
        return
def _freeze_intent_execution(
    ledger: ControlLedger, intent_id: str, reason: str, worker_ids: list[str] | None = None
) -> None:
    ledger.record_intent_execution(intent_id, "intent_execution_frozen", {"reason": reason}, status="needs_human")
    if worker_ids:
        freeze_workers = getattr(ledger, "freeze_workers", None)
        if callable(freeze_workers):
            freeze_workers(
                "intent_execution_anomaly",
                worker_ids,
                {"intent_id": intent_id, "reason": reason},
            )
        return
    freeze_intent_workers = getattr(ledger, "freeze_intent_workers", None)
    if callable(freeze_intent_workers):
        freeze_intent_workers(
            intent_id,
            "intent_execution_anomaly",
            {"intent_id": intent_id, "reason": reason},
        )


async def _reconcile_execution(
    connections: list[_WorkerSessionConnection], execution_id: str
) -> list[dict[str, list[dict[str, str]]]]:
    responses = await asyncio.gather(*(
        _execution_recovery_request(connection, "execution_reconcile", {"execution_id": execution_id})
        for connection in connections
    ))
    legs: list[dict[str, list[dict[str, str]]]] = []
    for response in responses:
        result = response["result"]
        orders = _execution_records(result.get("orders"))
        positions = _execution_records(result.get("positions"))
        legs.append({"orders": orders, "positions": positions})
    return legs


async def _execution_recovery_request(
    connection: _WorkerSessionConnection, operation: str, payload: dict[str, str]
) -> dict[str, object]:
    request_id = str(uuid4())
    response = await _request_worker_analysis(
        connection, analysis_id=operation, stage=operation, timeout=30,
        message={"type": f"{operation}_request", "request_id": request_id, **payload},
    )
    if response.get("accepted") is not True or response.get("operation") != operation:
        raise LedgerError(f"Worker did not confirm {operation}.")
    return response


async def _worker_cleanup_request(
    connection: _WorkerSessionConnection, operation: str, payload: dict[str, str] | None = None
) -> dict[str, object]:
    request_id = str(uuid4())
    response = await _request_worker_analysis(
        connection,
        analysis_id=operation,
        stage=operation,
        timeout=30,
        message={"type": f"{operation}_request", "request_id": request_id, **(payload or {})},
    )
    if response.get("accepted") is not True or response.get("operation") != operation:
        raise LedgerError(f"Worker did not confirm {operation}.")
    return response


def _cleanup_state(response: dict[str, object]) -> dict[str, list[dict[str, str]]]:
    result = response.get("result")
    if not isinstance(result, dict):
        raise LedgerError("Worker cleanup reconciliation returned an unknown broker state.")
    return {"orders": _execution_records(result.get("orders")), "positions": _execution_records(result.get("positions"))}


async def _cleanup_frozen_worker(
    ledger: ControlLedger,
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_execution_locks: dict[str, asyncio.Lock],
    worker_id: str,
    command: dict[str, object],
    username: str,
    admin_notification_connections: set[WebSocket] | None = None,
) -> None:
    operation_id = str(command["operation_id"])
    state: dict[str, object] = {"orders": [], "positions": []}
    try:
        async with _pair_worker_execution(worker_execution_locks, worker_id, worker_id):
            connection = _connected_worker_session(
                worker_connections, worker_id, reason="Frozen worker must be connected for account cleanup."
            )
            state = _cleanup_state(await _worker_cleanup_request(connection, "worker_cleanup_reconcile"))
            for order in state["orders"]:
                await _worker_cleanup_request(connection, "worker_cleanup_cancel", order)
            state = _cleanup_state(await _worker_cleanup_request(connection, "worker_cleanup_reconcile"))
            if state["orders"]:
                raise LedgerError("Broker did not confirm cancellation of every pending order.")
            for position in state["positions"]:
                await _worker_cleanup_request(connection, "worker_cleanup_close", position)
            state = _cleanup_state(await _worker_cleanup_request(connection, "worker_cleanup_reconcile"))
            if state["orders"] or state["positions"]:
                raise LedgerError("Broker cleanup final reconciliation did not prove an empty account.")
    except (KeyError, LedgerError, ValueError, asyncio.TimeoutError) as error:
        _LOGGER.warning("Worker cleanup %s failed for %s: %s", operation_id, worker_id, error)
        ledger.record_worker_cleanup(worker_id, operation_id, username, state, False)
    else:
        ledger.record_worker_cleanup(worker_id, operation_id, username, state, True)
    if admin_notification_connections is not None:
        await _broadcast_management_snapshot(ledger, admin_notification_connections)


async def _release_frozen_worker(
    ledger: ControlLedger,
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_execution_locks: dict[str, asyncio.Lock],
    worker_id: str,
    command: dict[str, object],
    username: str,
    admin_notification_connections: set[WebSocket] | None = None,
) -> None:
    operation_id = str(command["operation_id"])
    try:
        async with _pair_worker_execution(worker_execution_locks, worker_id, worker_id):
            connection = _connected_worker_session(
                worker_connections, worker_id, reason="Frozen worker must be connected for independent release."
            )
            state = _cleanup_state(await _worker_cleanup_request(connection, "worker_cleanup_reconcile"))
            ledger.record_empty_account_reconciliation(worker_id, operation_id, username, state)
            ledger.release_frozen_worker(worker_id, operation_id, username, state)
    except (KeyError, LedgerError, ValueError, asyncio.TimeoutError) as error:
        _LOGGER.warning("Worker release %s failed for %s: %s", operation_id, worker_id, error)
        ledger.record_worker_release_failure(worker_id, operation_id, username, str(error))
    if admin_notification_connections is not None:
        await _broadcast_management_snapshot(ledger, admin_notification_connections)


def _execution_records(records: object) -> list[dict[str, str]]:
    if not isinstance(records, list):
        raise ValueError("Execution reconciliation returned an unknown broker state.")
    normalized: list[dict[str, str]] = []
    for record in records:
        if not isinstance(record, dict) or not isinstance(record.get("ticket"), (str, int)) or isinstance(record.get("ticket"), bool):
            raise ValueError("Execution reconciliation returned an unknown broker record.")
        volume_value = record.get("volume", record.get("volume_current", record.get("volume_initial")))
        try:
            volume = Decimal(str(volume_value))
        except (ValueError, ArithmeticError) as error:
            raise ValueError("Execution reconciliation returned an unknown broker volume.") from error
        if volume <= 0:
            raise ValueError("Execution reconciliation returned an unknown broker volume.")
        normalized.append({"ticket": str(record["ticket"]), "volume": str(volume)})
    return normalized


async def _fail_undispatched_accepted_intents_after_startup_reconciliation(
    ledger: ControlLedger,
    startup_reconciliation_started_at: datetime,
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_execution_locks: dict[str, asyncio.Lock],
) -> None:
    """Terminally fail restart-surviving intents once both workers are freshly reconciled."""

    for record in ledger.accepted_intents_for_dispatch():
        outcomes = cast(list[dict[str, object]], record["preflight"])
        if len(outcomes) != 2 or not all(isinstance(item.get("worker_id"), str) and isinstance(item.get("order"), dict) for item in outcomes):
            _freeze_intent_execution(ledger, str(record["intent_id"]), "Interrupted dispatch has malformed preflight evidence.")
            continue
        worker_ids = [str(item["worker_id"]) for item in outcomes]
        if not ledger.workers_reconciled_since(worker_ids, startup_reconciliation_started_at):
            continue
        if record["status"] == "cancelling":
            await _cancel_intent(
                ledger, worker_connections, worker_execution_locks, str(record["intent_id"]), outcomes
            )
            continue
        if record["status"] == "dispatching":
            _freeze_intent_execution(
                ledger,
                str(record["intent_id"]),
                "Controller restarted during broker dispatch; broker state is uncertain.",
                worker_ids,
            )
        else:
            ledger.record_intent_execution(
                str(record["intent_id"]),
                "intent_dispatch_abandoned",
                {"reason": "Controller restarted before dispatch; preflight does not reserve broker liquidity."},
                status="failed_before_dispatch",
            )


async def _recover_manual_trades_after_startup_reconciliation(
    ledger: ControlLedger,
    startup_reconciliation_started_at: datetime,
    worker_connections: dict[str, set[_WorkerSessionConnection]],
    worker_execution_locks: dict[str, asyncio.Lock],
) -> None:
    """Resume definitely undispatched manual work and isolate interrupted broker dispatches."""

    for record in ledger.manual_trades_for_recovery():
        plan = cast(dict[str, object], record["plan"])
        legs = cast(list[dict[str, object]], plan.get("legs", []))
        worker_ids = [str(leg["worker_id"]) for leg in legs if isinstance(leg.get("worker_id"), str)]
        if len(worker_ids) != 2 or len(set(worker_ids)) != 2:
            _freeze_manual_trade(
                ledger, str(record["manual_trade_id"]), worker_ids, "Interrupted manual trade has malformed leg evidence."
            )
            continue
        if not ledger.workers_reconciled_since(worker_ids, startup_reconciliation_started_at):
            continue
        if record["status"] == "scheduled":
            await _dispatch_manual_trade(
                ledger, worker_connections, worker_execution_locks, str(record["manual_trade_id"])
            )
        else:
            _freeze_manual_trade(
                ledger, str(record["manual_trade_id"]), worker_ids,
                "Controller restarted during manual broker dispatch; broker state is uncertain.",
            )
    for record in ledger.manual_trade_operations_for_recovery():
        plan = cast(dict[str, object], record["plan"])
        legs = cast(list[dict[str, object]], plan.get("legs", []))
        worker_ids = [str(leg["worker_id"]) for leg in legs if isinstance(leg.get("worker_id"), str)]
        if len(worker_ids) != 2 or len(set(worker_ids)) != 2:
            ledger.record_manual_trade_operation(
                str(record["operation_id"]), "manual_trade_operation_frozen",
                {"reason": "Interrupted operation has malformed leg evidence."}, status="needs_human",
            )
            continue
        if not ledger.workers_reconciled_since(worker_ids, startup_reconciliation_started_at):
            continue
        if record["status"] == "scheduled":
            await _dispatch_manual_trade_operation(
                ledger, worker_connections, worker_execution_locks, str(record["operation_id"])
            )
        else:
            reason = "Controller restarted during manual-trade operation; broker state is uncertain."
            ledger.record_manual_trade_operation(
                str(record["operation_id"]), "manual_trade_operation_frozen", {"reason": reason}, status="needs_human"
            )
            ledger.freeze_workers(
                "manual_trade_operation_anomaly", worker_ids,
                {"manual_trade_id": record["manual_trade_id"], "operation_id": record["operation_id"], "reason": reason},
            )


def _valid_intent_volume(lots: Decimal, specification: dict[str, Any]) -> bool:
    minimum, maximum, step = (Decimal(str(specification[name])) for name in ("volume_min", "volume_max", "volume_step"))
    return minimum <= lots <= maximum and (lots - minimum) % step == 0


def _valid_intent_order_prices(order: dict[str, object], specification: dict[str, Any]) -> bool:
    """Require the shared entry and mapped protections to lie on each symbol's price grid."""

    try:
        point = Decimal(str(specification["point"]))
        prices = [Decimal(str(order[field])) for field in ("price", "sl", "tp")]
    except (KeyError, ValueError, ArithmeticError):
        return False
    return point > 0 and all(price > 0 and price % point == 0 for price in prices)


def _intent_order(
    intent: TraderIntentPayload, reference: dict[str, Any], *, primary: bool, execution_id: str
) -> dict[str, object]:
    specification = cast(dict[str, Any], reference["specification"])
    pip_size = _intent_pip_size(specification)
    direction = intent.primary_direction if primary else ("SHORT" if intent.primary_direction == "LONG" else "LONG")
    entry = intent.entry_price
    stop_loss = entry - intent.stop_loss_pips * pip_size if direction == "LONG" else entry + intent.stop_loss_pips * pip_size
    take_profit = entry + intent.take_profit_pips * pip_size if direction == "LONG" else entry - intent.take_profit_pips * pip_size
    return {
        "action": "pending_limit",
        "symbol": reference["symbol"],
        "volume": str(intent.lots),
        "direction": direction,
        "price": str(entry),
        "sl": str(stop_loss),
        "tp": str(take_profit),
        "filling_mode": intent.filling_mode,
        "expires_at": intent.expires_at.isoformat(),
        "control_plane_command_id": execution_id,
    }


def _intent_pip_size(specification: dict[str, Any]) -> Decimal:
    point = Decimal(str(specification["point"]))
    digits = int(specification["digits"])
    pip_digits = 2 if digits in {2, 3} else 4
    return point * Decimal(10) ** max(0, digits - pip_digits)


async def _request_order_check(
    connection: _WorkerSessionConnection, order: dict[str, object], *, timeout: float
) -> dict[str, object]:
    request_id = str(uuid4())
    return await _request_worker_analysis(
        connection,
        analysis_id="order_check",
        stage="order_check",
        timeout=timeout,
        message={"type": "order_check_request", "analysis_id": "order_check", "request_id": request_id, "order": order},
    )


async def _request_order_execute(
    connection: _WorkerSessionConnection, order: dict[str, object], *, timeout: float = 30
) -> dict[str, object]:
    request_id = str(uuid4())
    return await _request_worker_analysis(
        connection,
        analysis_id="order_execute",
        stage="order_execute",
        timeout=timeout,
        message={"type": "order_execute_request", "request_id": request_id, "order": order},
    )


def _valid_order_check_response(response: dict[str, object], order: dict[str, object]) -> bool:
    required = {"type", "analysis_id", "request_id", "accepted", "order"}
    response_fields = set(response)
    if response_fields != required and response_fields != required | {"diagnostics"}:
        return False
    if (
        response["type"] != "order_check_response"
        or response["analysis_id"] != "order_check"
        or not isinstance(response["request_id"], str)
        or not response["request_id"]
        or not isinstance(response["accepted"], bool)
        or response["order"] != order
    ):
        return False
    return (
        "diagnostics" not in response
        or (
            _valid_order_check_diagnostics(response["diagnostics"])
            and response["accepted"] == (response["diagnostics"]["retcode"] == 0)
        )
    )


def _valid_order_check_diagnostics(value: object) -> bool:
    if not isinstance(value, dict) or not {"retcode"} <= set(value) <= {"retcode", "comment", "quote"}:
        return False
    if isinstance(value["retcode"], bool) or not isinstance(value["retcode"], int):
        return False
    if "comment" in value and (not isinstance(value["comment"], str) or not value["comment"]):
        return False
    if "quote" not in value:
        return True
    quote = value["quote"]
    return (
        isinstance(quote, dict)
        and set(quote) <= {"bid", "ask", "time"}
        and bool(quote)
        and all(isinstance(item, (int, float)) and not isinstance(item, bool) for item in quote.values())
    )


def _previous_complete_utc_week(now: datetime | None = None) -> dict[str, str]:
    now = datetime.now(UTC) if now is None else now.astimezone(UTC)
    current_week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    previous_week_start = current_week_start - timedelta(days=7)
    return {
        "timeframe": "M15",
        "started_at_utc": previous_week_start.isoformat().replace("+00:00", "Z"),
        "ended_at_utc": current_week_start.isoformat().replace("+00:00", "Z"),
    }


def _screen_m15_candidates(
    candidates: list[dict[str, object]],
    first_market_data: dict[str, object],
    second_market_data: dict[str, object],
    policy: dict[str, object],
) -> list[dict[str, object]]:
    first_symbols = cast(dict[str, dict[str, object]], first_market_data["symbols"])
    second_symbols = cast(dict[str, dict[str, object]], second_market_data["symbols"])
    results: list[dict[str, object]] = []
    coverage_minimum = float(policy.get("minimum_m15_common_coverage", policy.get("minimum_common_coverage", 1.0)))
    correlation_minimum = float(policy.get("minimum_m15_return_correlation", 0.97))
    for candidate in candidates:
        first_symbol = first_symbols[str(candidate["first_symbol"])]
        second_symbol = second_symbols[str(candidate["second_symbol"])]
        statistics = _market_data_statistics(candidate, first_symbol, second_symbol)
        coverage_ratio = float(statistics["coverage_ratio"])
        return_correlation = cast(float | None, statistics["return_correlation"])
        coverage_passed = coverage_ratio >= coverage_minimum
        correlation_passed = return_correlation is not None and return_correlation >= correlation_minimum
        results.append(
            {
                **candidate,
                "screening_status": "passed" if coverage_passed and correlation_passed else "failed",
                "statistics": statistics,
                "policy_evaluation": {
                    "minimum_m15_common_coverage": coverage_minimum,
                    "minimum_m15_return_correlation": correlation_minimum,
                    "coverage_passed": coverage_passed,
                    "return_correlation_passed": correlation_passed,
                },
                "first_market_data": _market_data_summary(first_symbol),
                "second_market_data": _market_data_summary(second_symbol),
            }
        )
    return results


def _verify_m1_candidates(
    candidates: list[dict[str, object]],
    first_market_data: dict[str, object],
    second_market_data: dict[str, object],
    first_specifications: list[dict[str, object]],
    second_specifications: list[dict[str, object]],
    policy: dict[str, object],
) -> list[dict[str, object]]:
    first_symbols = cast(dict[str, dict[str, object]], first_market_data["symbols"])
    second_symbols = cast(dict[str, dict[str, object]], second_market_data["symbols"])
    first_index = {str(symbol["symbol"]): symbol for symbol in first_specifications}
    second_index = {str(symbol["symbol"]): symbol for symbol in second_specifications}
    coverage_minimum = float(policy.get("minimum_m1_common_coverage", policy.get("minimum_common_coverage", 0.98)))
    correlation_minimum = float(policy.get("minimum_m1_return_correlation", 0.95))
    median_maximum = float(policy.get("maximum_m1_median_price_difference_points", 2.0))
    results: list[dict[str, object]] = []
    for candidate in candidates:
        first_symbol_name = str(candidate["first_symbol"])
        second_symbol_name = str(candidate["second_symbol"])
        statistics = _market_data_statistics(
            candidate,
            first_symbols[first_symbol_name],
            second_symbols[second_symbol_name],
        )
        hard_block_differences = _specification_differences(
            first_index[first_symbol_name],
            second_index[second_symbol_name],
            _HARD_BLOCK_FIELDS,
        )
        supported_filling_modes = _shared_supported_filling_modes(
            first_index[first_symbol_name]["filling_modes"],
            second_index[second_symbol_name]["filling_modes"],
        )
        if not supported_filling_modes:
            hard_block_differences.append(
                {
                    "field": "filling_modes",
                    "first_value": first_index[first_symbol_name]["filling_modes"],
                    "second_value": second_index[second_symbol_name]["filling_modes"],
                    "reason": "no_common_supported_filling_mode",
                }
            )
        warning_differences = _specification_differences(
            first_index[first_symbol_name],
            second_index[second_symbol_name],
            _WARNING_FIELDS,
        )
        coverage_ratio = float(statistics["coverage_ratio"])
        return_correlation = cast(float | None, statistics["return_correlation"])
        median_difference = cast(float | None, statistics["median_price_difference_points"])
        policy_evaluation = {
            "minimum_m1_common_coverage": coverage_minimum,
            "minimum_m1_return_correlation": correlation_minimum,
            "maximum_m1_median_price_difference_points": median_maximum,
            "coverage_passed": coverage_ratio >= coverage_minimum,
            "return_correlation_passed": return_correlation is not None and return_correlation >= correlation_minimum,
            "median_price_difference_passed": median_difference is not None and median_difference <= median_maximum,
            "hard_block_differences_passed": not hard_block_differences,
        }
        results.append(
            {
                **candidate,
                "verification_status": "passed" if all(
                    isinstance(value, bool) and value for key, value in policy_evaluation.items() if key.endswith("_passed")
                ) else "failed",
                "statistics": statistics,
                "policy_evaluation": policy_evaluation,
                "hard_block_differences": hard_block_differences,
                "warning_differences": warning_differences,
                "supported_filling_modes": supported_filling_modes,
                "first_market_data": _market_data_summary(first_symbols[first_symbol_name]),
                "second_market_data": _market_data_summary(second_symbols[second_symbol_name]),
            }
        )
    return results


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


def _shared_supported_filling_modes(first_modes: object, second_modes: object) -> list[str]:
    first = _normalized_capability_set(first_modes)
    second = _normalized_capability_set(second_modes)
    return [mode for mode in _SUPPORTED_ENTRY_FILLING_MODES if mode in first and mode in second]


def _specification_differences(
    first_specification: dict[str, object],
    second_specification: dict[str, object],
    fields: tuple[str, ...],
) -> list[dict[str, object]]:
    differences: list[dict[str, object]] = []
    for field in fields:
        if field not in first_specification or field not in second_specification:
            continue
        if _normalized_comparison_value(first_specification[field]) == _normalized_comparison_value(second_specification[field]):
            continue
        differences.append(
            {
                "field": field,
                "first_value": first_specification[field],
                "second_value": second_specification[field],
            }
        )
    return differences


def _product_pair_compatibility_differences(
    reference_specification: dict[str, object],
    live_specification: dict[str, object] | None,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    if live_specification is None:
        return (
            [{
                "field": "symbol",
                "reference_value": reference_specification["symbol"],
                "worker_value": None,
                "reason": "missing_symbol",
            }],
            [],
        )
    return (
        _compatibility_differences(reference_specification, live_specification, _HARD_BLOCK_FIELDS),
        _compatibility_differences(reference_specification, live_specification, _WARNING_FIELDS),
    )


def _compatibility_differences(
    reference_specification: dict[str, object],
    live_specification: dict[str, object],
    fields: tuple[str, ...],
) -> list[dict[str, object]]:
    differences: list[dict[str, object]] = []
    for field in fields:
        if field not in reference_specification:
            continue
        reference_value = reference_specification[field]
        worker_value = live_specification.get(field)
        if field in _COMPATIBILITY_CAPABILITY_FIELDS:
            required = _normalized_capability_set(reference_value)
            available = _normalized_capability_set(worker_value)
            if required and required.issubset(available):
                continue
        elif worker_value is not None and _normalized_comparison_value(reference_value) == _normalized_comparison_value(worker_value):
            continue
        differences.append(
            {
                "field": field,
                "reference_value": reference_value,
                "worker_value": worker_value,
            }
        )
    return differences


def _normalized_capability_set(value: object) -> set[object]:
    if not isinstance(value, list):
        return set()
    return { _normalized_comparison_value(item) for item in value }


def _normalized_comparison_value(value: object) -> object:
    if isinstance(value, list):
        return tuple(sorted(_normalized_comparison_value(item) for item in value))
    return value


def _aligned_closes(
    first_symbol: dict[str, object],
    second_symbol: dict[str, object],
) -> list[tuple[str, float, float]]:
    first_index = {
        _bar_minute_key(str(bar["time_utc"])): float(bar["close"])
        for bar in cast(list[dict[str, object]], first_symbol["bars"])
    }
    second_index = {
        _bar_minute_key(str(bar["time_utc"])): float(bar["close"])
        for bar in cast(list[dict[str, object]], second_symbol["bars"])
    }
    return [
        (time_utc, first_index[time_utc], second_index[time_utc])
        for time_utc in sorted(first_index.keys() & second_index.keys())
    ]


def _bar_minute_key(value: str) -> str:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC).replace(second=0, microsecond=0).isoformat()


def _market_data_statistics(
    candidate: dict[str, object],
    first_symbol: dict[str, object],
    second_symbol: dict[str, object],
) -> dict[str, object]:
    aligned = _aligned_closes(first_symbol, second_symbol)
    first_bars = cast(list[dict[str, object]], first_symbol["bars"])
    second_bars = cast(list[dict[str, object]], second_symbol["bars"])
    denominator = max(len(first_bars), len(second_bars))
    coverage_ratio = 0.0 if denominator == 0 else len(aligned) / denominator
    first_returns = _returns([pair[1] for pair in aligned])
    second_returns = _returns([pair[2] for pair in aligned])
    return_correlation = _correlation(first_returns, second_returns)
    target_point = max(float(candidate.get("first_point", 0) or 0), float(candidate.get("second_point", 0) or 0), 0.00001)
    price_differences = [abs(first_close - second_close) / target_point for _, first_close, second_close in aligned]
    return {
        "aligned_bar_count": len(aligned),
        "first_bar_count": len(first_bars),
        "second_bar_count": len(second_bars),
        "coverage_ratio": round(coverage_ratio, 6),
        "return_correlation": None if return_correlation is None else round(return_correlation, 6),
        "median_price_difference_points": None if not price_differences else round(float(median(price_differences)), 6),
        "p99_price_difference_points": None if not price_differences else round(_p99(price_differences), 6),
        "target_point": target_point,
    }


def _returns(closes: list[float]) -> list[float]:
    if len(closes) < 2:
        return []
    return [closes[index] - closes[index - 1] for index in range(1, len(closes))]


def _correlation(first: list[float], second: list[float]) -> float | None:
    if len(first) != len(second) or len(first) < 2:
        return None
    first_mean = sum(first) / len(first)
    second_mean = sum(second) / len(second)
    numerator = sum((left - first_mean) * (right - second_mean) for left, right in zip(first, second, strict=False))
    first_variance = sum((value - first_mean) ** 2 for value in first)
    second_variance = sum((value - second_mean) ** 2 for value in second)
    if first_variance <= 0 or second_variance <= 0:
        return None
    return numerator / math.sqrt(first_variance * second_variance)


def _p99(values: list[float]) -> float:
    ordered = sorted(values)
    return float(ordered[max(0, math.ceil(len(ordered) * 0.99) - 1)])


def _market_data_summary(symbol: dict[str, object]) -> dict[str, object]:
    bars = cast(list[dict[str, object]], symbol["bars"])
    return {
        "symbol": symbol["symbol"],
        "bar_count": len(bars),
        "first_raw_epoch": bars[0]["time"],
        "last_raw_epoch": bars[-1]["time"],
        "first_utc": bars[0]["time_utc"],
        "last_utc": bars[-1]["time_utc"],
        "content_hash": hashlib.sha256(
            json.dumps(bars, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        ).hexdigest(),
        "time_metadata": symbol["time_metadata"],
    }


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
            "product_pairs": ledger.product_pairs(),
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


def _record_snapshot(ledger: ControlLedger, worker_id: str, message: dict[str, object]) -> None:
    required = {"type", "cursor", "observed_at", "account", "terminal", "orders", "positions"}
    if set(message) != required or not isinstance(message["cursor"], int) or isinstance(message["cursor"], bool):
        raise ValueError("Invalid protocol message.")
    if not isinstance(message["observed_at"], str) or not isinstance(message["account"], dict) or not isinstance(message["terminal"], dict):
        raise ValueError("Invalid protocol message.")
    if not isinstance(message["orders"], list) or not isinstance(message["positions"], list):
        raise ValueError("Invalid protocol message.")
    ledger.record_snapshot(
        worker_id, message["cursor"], message["observed_at"], message["account"], message["terminal"],
        message["orders"], message["positions"],
    )


def _record_live_state_snapshot(ledger: ControlLedger, worker_id: str, message: dict[str, object]) -> None:
    required = {"type", "observed_at", "connectivity", "quotes", "orders", "positions"}
    if set(message) != required or not isinstance(message["observed_at"], str) or not isinstance(message["connectivity"], bool):
        raise ValueError("Invalid protocol message.")
    if not all(isinstance(message[field], list) for field in ("quotes", "orders", "positions")):
        raise ValueError("Invalid protocol message.")
    if not all(isinstance(quote, dict) for quote in message["quotes"]):
        raise ValueError("Invalid protocol message.")
    ledger.record_live_state(
        worker_id, message["observed_at"], message["connectivity"], message["quotes"], message["orders"], message["positions"]
    )


def _record_live_state_diff(ledger: ControlLedger, worker_id: str, message: dict[str, object]) -> None:
    if set(message) != {"type", "observed_at", "entity", "value"} or not isinstance(message["observed_at"], str):
        raise ValueError("Invalid protocol message.")
    entity = message["entity"]
    if entity not in {"connectivity", "quotes", "orders", "positions"}:
        raise ValueError("Invalid protocol message.")
    if entity == "connectivity":
        if not isinstance(message["value"], bool):
            raise ValueError("Invalid protocol message.")
    elif not isinstance(message["value"], list):
        raise ValueError("Invalid protocol message.")
    ledger.record_live_state_diff(worker_id, message["observed_at"], entity, message["value"])


def _record_delta(ledger: ControlLedger, worker_id: str, message: dict[str, object]) -> None:
    required = {"type", "cursor", "observed_at", "entity", "ticket", "change", "record"}
    if set(message) != required or not isinstance(message["cursor"], int) or isinstance(message["cursor"], bool):
        raise ValueError("Invalid protocol message.")
    if not all(isinstance(message[field], str) and message[field] for field in ("observed_at", "entity", "ticket", "change")):
        raise ValueError("Invalid protocol message.")
    if message["entity"] not in {"order", "position"} or message["change"] not in {
        "created", "state_changed", "volume_changed", "modified", "closed"
    }:
        raise ValueError("Invalid protocol message.")
    if not isinstance(message["record"], dict):
        raise ValueError("Invalid protocol message.")
    ledger.record_delta(
        worker_id, message["cursor"], message["observed_at"], message["entity"], message["ticket"],
        message["change"], message["record"],
    )


def _freeze_worker_session(ledger: ControlLedger, worker_id: str, source: str, reason: str) -> None:
    try:
        ledger.freeze_worker_and_active_counterparts(worker_id, source, {"reason": reason})
    except LedgerError as error:
        _LOGGER.warning("Could not isolate worker %s after %s: %s", worker_id, source, error)


async def _close_policy_violation(websocket: WebSocket) -> None:
    if websocket.client_state.name != "DISCONNECTED":
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
