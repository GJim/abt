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
from pathlib import Path
import secrets
from statistics import median
from typing import Annotated, Any, Callable, cast
from uuid import uuid4

from fastapi import Cookie, FastAPI, Header, HTTPException, Response, WebSocket, WebSocketDisconnect, status
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from .backup import BackupManager
from .crypto import ProofError, parse_device_certificate, verify_enrollment_proof, verify_worker_proof
from .ledger import AuthenticationError, ControlLedger, LedgerError
from .secrets import DeviceCertificateIssuer, DeviceCertificateVerifier, SecretStore, SecretStoreError

_LOGGER = logging.getLogger(__name__)


class LoginRequest(BaseModel):
    username: str = Field(pattern=r"^[A-Z]{6}$")
    password: str = Field(min_length=20)


class EnrollmentRequest(BaseModel):
    login: int = Field(gt=0)
    server: str = Field(min_length=1, max_length=128)
    pairing_code: str = Field(pattern=r"^\d{8}$")
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


@dataclass
class _WorkerAnalysisRequest:
    analysis_id: str
    request_id: str
    stage: str
    message: dict[str, object]
    future: ConcurrentFuture[dict[str, object]]


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
    worker_connections: dict[str, set[_WorkerSessionConnection]] = {}
    worker_execution_locks: dict[str, asyncio.Lock] = {}

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

    @app.post("/api/enrollments", status_code=status.HTTP_201_CREATED)
    def create_enrollment(body: EnrollmentRequest) -> dict[str, str]:
        try:
            verify_enrollment_proof(
                body.public_key_pem,
                body.proof_signature,
                login=body.login,
                server=body.server,
                pairing_code=body.pairing_code,
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
                    pairing_code=body.pairing_code,
                    public_key_pem=body.public_key_pem,
                    account_info=body.account_info,
                    terminal_info=body.terminal_info,
                    password_secret_ref=secret_ref,
                    enrollment_challenge=body.enrollment_challenge,
                )
            except LedgerError:
                secret_store.delete_password(secret_ref)
                raise
            return {"enrollment_id": enrollment.enrollment_id, "expires_at": enrollment.expires_at.isoformat()}
        except ProofError as error:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)) from error
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error
        except SecretStoreError as error:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)) from error

    @app.get("/api/enrollment-challenge")
    def enrollment_challenge() -> dict[str, str]:
        challenge, expires_at = ledger.issue_enrollment_challenge()
        return {"challenge": challenge, "expires_at": expires_at.isoformat()}

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
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> list[dict[str, object]]:
        _require_admin(ledger, abt_admin_session)
        return ledger.events()

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
                    eligible_candidates, exceptions = _analyze_product_catalogs(
                        first_evidence["symbols"], second_evidence["symbols"]
                    )
                    ledger.record_product_catalog_analysis_catalog(
                        analysis_id,
                        first_evidence=first_evidence,
                        second_evidence=second_evidence,
                        eligible_candidates=eligible_candidates,
                        exceptions=exceptions,
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
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> list[dict[str, object]]:
        _require_admin(ledger, abt_admin_session)
        return ledger.product_pairs()

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
    def build_product_pair(
        body: ProductPairConfirmationUseRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            return ledger.build_product_pair(body.confirmation_id, username)
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

    @app.post("/api/admin/product-pairs/{product_pair_id}/replace", status_code=status.HTTP_201_CREATED)
    def replace_product_pair(
        product_pair_id: str,
        body: ProductPairConfirmationUseRequest,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            return ledger.replace_product_pair(
                product_pair_id,
                confirmation_id=body.confirmation_id,
                replaced_by=username,
            )
        except LedgerError as error:
            raise HTTPException(status_code=_ledger_error_status(error), detail=str(error)) from error

    @app.post("/api/admin/product-pairs/{product_pair_id}/retire")
    def retire_product_pair(
        product_pair_id: str,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            return ledger.retire_product_pair(product_pair_id, username)
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
            request = await _worker_message(websocket, {"enrollment_id"})
            enrollment_id = _required_text(request, "enrollment_id")
            worker = ledger.active_worker_for_enrollment(enrollment_id)
            nonce = secrets.token_urlsafe(32)
            await websocket.send_json(
                {"purpose": "certificate_delivery", "worker_id": worker.worker_id, "nonce": nonce}
            )
            proof = await _worker_message(websocket, {"signature"})
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
            await _reject_worker(websocket)
        except (LedgerError, ProofError, ValueError) as error:
            _LOGGER.warning("Worker certificate delivery failed: %s", error)
            await _reject_worker(websocket)
        except Exception:
            _LOGGER.exception("Worker certificate delivery failed unexpectedly.")
            await _reject_worker(websocket)

    @app.websocket("/api/worker/credentials")
    async def mediate_password(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            request = await _worker_message(websocket, {"worker_id", "certificate"})
            worker = ledger.active_worker(_required_text(request, "worker_id"))
            certificate = _required_text(request, "certificate")
            claims = parse_device_certificate(certificate)
            if certificate_verifier is None:
                raise ProofError("The device certificate verifier is unavailable.")
            certificate_verifier.verify(certificate)
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
            proof = await _worker_message(websocket, {"signature"})
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
            await _reject_worker(websocket)
        except (LedgerError, ProofError, SecretStoreError, ValueError) as error:
            _LOGGER.warning("Worker password mediation failed: %s", error)
            await _reject_worker(websocket)
        except Exception:
            _LOGGER.exception("Worker password mediation failed unexpectedly.")
            await _reject_worker(websocket)

    @app.websocket("/api/worker/session")
    async def worker_session(websocket: WebSocket) -> None:
        await websocket.accept()
        sender_task: asyncio.Task[None] | None = None
        try:
            request = await _worker_message(websocket, {"worker_id", "certificate"})
            worker = ledger.active_worker(_required_text(request, "worker_id"))
            certificate = _required_text(request, "certificate")
            claims = parse_device_certificate(certificate)
            if certificate_verifier is None:
                raise ProofError("The device certificate verifier is unavailable.")
            certificate_verifier.verify(certificate)
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
            proof = await _worker_message(websocket, {"signature"})
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
                request = await websocket.receive_json()
                if not isinstance(request, dict):
                    raise ValueError("Invalid worker message.")
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
                    ledger.record_worker_safety_state(worker.worker_id, request["state"], request["reason"])
                    await websocket.send_json({"type": "accepted", "state": request["state"]})
                elif message_type == "snapshot":
                    _record_snapshot(ledger, worker.worker_id, request)
                    await websocket.send_json({"type": "accepted", "cursor": request["cursor"]})
                elif message_type == "delta":
                    _record_delta(ledger, worker.worker_id, request)
                    await websocket.send_json({"type": "accepted", "cursor": request["cursor"]})
                elif message_type == "product_catalog_analysis_response":
                    _record_product_catalog_analysis_response(connection, request)
                else:
                    raise ValueError("Invalid worker message.")
        except WebSocketDisconnect as error:
            _LOGGER.info("Worker session disconnected with code %s.", error.code)
            await _reject_worker(websocket)
        except (LedgerError, ProofError, SecretStoreError, ValueError) as error:
            _LOGGER.warning("Worker session authentication or request failed: %s", error)
            await _reject_worker(websocket)
        except Exception:
            _LOGGER.exception("Worker session authentication or request failed unexpectedly.")
            await _reject_worker(websocket)
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
                    timeout=5,
                    message=_market_data_request(analysis_id, stage, timeframe, first_symbols, analysis_period, policy),
                ),
                _request_worker_analysis(
                    second_connection,
                    analysis_id=analysis_id,
                    stage=stage,
                    timeout=5,
                    message=_market_data_request(analysis_id, stage, timeframe, second_symbols, analysis_period, policy),
                ),
            )
            return (
                _validated_market_data_response(first_response, analysis_id, stage, timeframe, first_symbols, analysis_period),
                _validated_market_data_response(second_response, analysis_id, stage, timeframe, second_symbols, analysis_period),
            )
        except (asyncio.TimeoutError, LedgerError, ValueError) as error:
            last_error = error
            if attempt == 1:
                break
            record_retry(str(error))
    assert last_error is not None
    raise last_error


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
        pending.future.set_result(request)


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
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    first_index: dict[tuple[str, str], list[dict[str, object]]] = {}
    second_index: dict[tuple[str, str], list[dict[str, object]]] = {}
    for symbol in first_symbols:
        first_index.setdefault((str(symbol["currency_base"]), str(symbol["currency_profit"])), []).append(symbol)
    for symbol in second_symbols:
        second_index.setdefault((str(symbol["currency_base"]), str(symbol["currency_profit"])), []).append(symbol)
    candidates: list[dict[str, object]] = []
    exceptions: list[dict[str, object]] = []
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
                else:
                    exceptions.append(
                        {
                            "first_symbol": first["symbol"],
                            "second_symbol": second["symbol"],
                            "currency_base": first["currency_base"],
                            "currency_profit": first["currency_profit"],
                            "reason": "trade_calc_mode_mismatch",
                            "first_trade_calc_mode": first["trade_calc_mode"],
                            "second_trade_calc_mode": second["trade_calc_mode"],
                        }
                    )
    return candidates, exceptions


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
    eligible_candidates, _exceptions = _analyze_product_catalogs([first_specification], [second_specification])
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
    first_index = {str(bar["time_utc"]): float(bar["close"]) for bar in cast(list[dict[str, object]], first_symbol["bars"])}
    second_index = {str(bar["time_utc"]): float(bar["close"]) for bar in cast(list[dict[str, object]], second_symbol["bars"])}
    return [
        (time_utc, first_index[time_utc], second_index[time_utc])
        for time_utc in sorted(first_index.keys() & second_index.keys())
    ]


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


async def _worker_message(websocket: WebSocket, expected_fields: set[str]) -> dict[str, object]:
    message = await websocket.receive_json()
    if not isinstance(message, dict) or set(message) != expected_fields:
        raise ValueError("Invalid worker message.")
    return message


def _required_text(message: dict[str, object], field: str) -> str:
    value = message.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError("Invalid worker message.")
    return value


def _record_snapshot(ledger: ControlLedger, worker_id: str, message: dict[str, object]) -> None:
    required = {"type", "cursor", "observed_at", "account", "terminal", "orders", "positions"}
    if set(message) != required or not isinstance(message["cursor"], int) or isinstance(message["cursor"], bool):
        raise ValueError("Invalid worker message.")
    if not isinstance(message["observed_at"], str) or not isinstance(message["account"], dict) or not isinstance(message["terminal"], dict):
        raise ValueError("Invalid worker message.")
    if not isinstance(message["orders"], list) or not isinstance(message["positions"], list):
        raise ValueError("Invalid worker message.")
    ledger.record_snapshot(
        worker_id, message["cursor"], message["observed_at"], message["account"], message["terminal"],
        message["orders"], message["positions"],
    )


def _record_delta(ledger: ControlLedger, worker_id: str, message: dict[str, object]) -> None:
    required = {"type", "cursor", "observed_at", "entity", "ticket", "change", "record"}
    if set(message) != required or not isinstance(message["cursor"], int) or isinstance(message["cursor"], bool):
        raise ValueError("Invalid worker message.")
    if not all(isinstance(message[field], str) and message[field] for field in ("observed_at", "entity", "ticket", "change")):
        raise ValueError("Invalid worker message.")
    if message["entity"] not in {"order", "position"} or message["change"] not in {
        "created", "state_changed", "volume_changed", "modified", "closed"
    }:
        raise ValueError("Invalid worker message.")
    if not isinstance(message["record"], dict):
        raise ValueError("Invalid worker message.")
    ledger.record_delta(
        worker_id, message["cursor"], message["observed_at"], message["entity"], message["ticket"],
        message["change"], message["record"],
    )


async def _reject_worker(websocket: WebSocket) -> None:
    if websocket.client_state.name != "DISCONNECTED":
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
