from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

from fastapi import Cookie, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from .crypto import ProofError, verify_enrollment_proof
from .ledger import AuthenticationError, ControlLedger, IpLockedError, LedgerError


class LoginRequest(BaseModel):
    username: str = Field(pattern=r"^[A-Z]{6}$")
    password: str = Field(min_length=20)


class EnrollmentRequest(BaseModel):
    login: int = Field(gt=0)
    server: str = Field(min_length=1, max_length=128)
    pairing_code: str = Field(pattern=r"^\d{8}$")
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
) -> FastAPI:
    ledger = ControlLedger(ledger_path)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        ledger.close()

    app = FastAPI(title="abt control plane", version="0.1.0", lifespan=lifespan)
    app.state.ledger = ledger

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
        try:
            verify_enrollment_proof(
                body.public_key_pem,
                body.proof_signature,
                login=body.login,
                server=body.server,
                pairing_code=body.pairing_code,
            )
        except ProofError as error:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)) from error
        enrollment = ledger.create_enrollment(
            login=body.login,
            server=body.server,
            pairing_code=body.pairing_code,
            public_key_pem=body.public_key_pem,
            source_ip=_source_ip(request, trusted_proxy_ips),
        )
        return {"enrollment_id": enrollment.enrollment_id, "expires_at": enrollment.expires_at.isoformat()}

    @app.get("/api/admin/enrollments")
    def list_enrollments(
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> list[dict[str, object]]:
        _require_admin(ledger, abt_admin_session)
        return ledger.pending_enrollments()

    @app.get("/api/admin/events")
    def list_events(
        abt_admin_session: Annotated[str | None, Cookie()] = None,
    ) -> list[dict[str, object]]:
        _require_admin(ledger, abt_admin_session)
        return ledger.events()

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
            return {"worker_id": ledger.approve_enrollment(enrollment_id, username)}
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error

    @app.post("/api/admin/enrollments/{enrollment_id}/reject", status_code=status.HTTP_204_NO_CONTENT)
    def reject_enrollment(
        enrollment_id: str,
        abt_admin_session: Annotated[str | None, Cookie()] = None,
        x_csrf_token: Annotated[str | None, Header()] = None,
    ) -> Response:
        username = _require_admin(ledger, abt_admin_session, x_csrf_token, require_csrf=True)
        try:
            ledger.reject_enrollment(enrollment_id, username)
        except LedgerError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    if spa_directory is not None:
        app.mount("/", StaticFiles(directory=spa_directory, html=True), name="management-spa")

    return app


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
