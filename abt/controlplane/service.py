from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
import secrets
from typing import Annotated, Callable, cast
from uuid import uuid4

from fastapi import Cookie, FastAPI, Header, HTTPException, Request, Response, WebSocket, WebSocketDisconnect, status
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from .backup import BackupManager
from .crypto import ProofError, parse_device_certificate, verify_enrollment_proof, verify_worker_proof
from .ledger import AuthenticationError, ControlLedger, IpLockedError, LedgerError
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


def create_app(
    ledger_path: Path,
    *,
    trusted_proxy_ips: frozenset[str] = frozenset(),
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
    worker_connections: dict[str, set[WebSocket]] = {}

    @app.get("/health")
    def health() -> dict[str, str]:
        if not secret_control_healthy():
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Secret control plane is unavailable.")
        return {"status": "ok"}

    @app.post("/api/admin/login")
    def login(request: Request, response: Response, body: LoginRequest) -> dict[str, str]:
        try:
            session = ledger.authenticate_admin(body.username, body.password, _source_ip(request, trusted_proxy_ips))
        except IpLockedError as error:
            raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=str(error)) from error
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

    @app.post("/api/enrollments", status_code=status.HTTP_201_CREATED)
    def create_enrollment(request: Request, body: EnrollmentRequest) -> dict[str, str]:
        source_ip = _source_ip(request, trusted_proxy_ips)
        try:
            ledger.record_registration_attempt(source_ip)
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
                    source_ip=source_ip,
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
            status_code = (
                status.HTTP_429_TOO_MANY_REQUESTS
                if str(error) == "Worker registration rate limit exceeded."
                else status.HTTP_409_CONFLICT
            )
            raise HTTPException(status_code=status_code, detail=str(error)) from error
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
        except (LedgerError, ProofError, ValueError, WebSocketDisconnect):
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
        except (LedgerError, ProofError, SecretStoreError, ValueError, WebSocketDisconnect):
            await _reject_worker(websocket)

    @app.websocket("/api/worker/session")
    async def worker_session(websocket: WebSocket) -> None:
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
            worker_connections.setdefault(worker.worker_id, set()).add(websocket)
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
                else:
                    raise ValueError("Invalid worker message.")
        except (LedgerError, ProofError, SecretStoreError, ValueError, WebSocketDisconnect):
            await _reject_worker(websocket)
        finally:
            connections = worker_connections.get(worker.worker_id) if "worker" in locals() else None
            if connections is not None:
                connections.discard(websocket)
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


def _source_ip(request: Request, trusted_proxy_ips: frozenset[str]) -> str:
    cloudflare_ip = request.headers.get("cf-connecting-ip")
    if cloudflare_ip and request.client is not None and request.client.host in trusted_proxy_ips:
        return cloudflare_ip
    if request.client is None:
        return "unknown"
    return request.client.host


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
