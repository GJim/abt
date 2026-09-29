from __future__ import annotations

import json
from base64 import b64encode
from collections.abc import Callable
from typing import Protocol
from urllib.parse import urlsplit, urlunsplit

from abt.controlplane.crypto import worker_proof_payload

from .enrollment import WorkerEnrollmentError
from .keystore import HardwareKeyStore


class WorkerWebSocket(Protocol):
    def send(self, message: str) -> None: ...

    def recv(self, timeout: float | None = None) -> str: ...

    def __enter__(self) -> WorkerWebSocket: ...

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None: ...


WebSocketConnector = Callable[[str], WorkerWebSocket]

#: Keepalive ping cadence for worker WSS channels. The interval stays at the
#: library default; the timeout is relaxed so transient stalls on the
#: Cloudflare path (observed 8s+ bursts, occasionally upstream-only) do not
#: kill the session. Genuine liveness is still enforced by the 30-second
#: application heartbeat (15s response timeout) and the five-minute
#: lost-link safety state.
WORKER_PING_INTERVAL_SECONDS = 20.0
WORKER_PING_TIMEOUT_SECONDS = 60.0


def default_worker_connector(url: str) -> WorkerWebSocket:
    """Open a worker WSS channel with stall-tolerant keepalive settings."""

    from websockets.sync.client import connect as websocket_connect

    return websocket_connect(
        url,
        ping_interval=WORKER_PING_INTERVAL_SECONDS,
        ping_timeout=WORKER_PING_TIMEOUT_SECONDS,
    )


def retrieve_mt5_password(
    *,
    controller_url: str,
    enrollment_id: str,
    key_store: HardwareKeyStore,
    connect: WebSocketConnector | None = None,
) -> str:
    """Retrieve an approved worker's password without persisting or displaying it."""

    if connect is None:
        connect = default_worker_connector
    certificate_url = _worker_endpoint(controller_url, "/api/worker/certificate")
    with connect(certificate_url) as socket:
        _send(socket, {"enrollment_id": enrollment_id})
        challenge = _message(socket, timeout=30.0)
        worker_id = _required_text(challenge, "worker_id")
        _send_proof(socket, key_store, challenge, "certificate_delivery", worker_id)
        delivery = _message(socket, timeout=30.0)
        if _required_text(delivery, "worker_id") != worker_id:
            raise WorkerEnrollmentError("The controller returned an invalid device certificate.")
        certificate = _required_text(delivery, "certificate")

    credentials_url = _worker_endpoint(controller_url, "/api/worker/credentials")
    with connect(credentials_url) as socket:
        _send(socket, {"worker_id": worker_id, "certificate": certificate})
        challenge = _message(socket, timeout=30.0)
        _send_proof(socket, key_store, challenge, "password_request", worker_id)
        return _required_text(_message(socket, timeout=30.0), "password")


def _worker_endpoint(controller_url: str, endpoint: str) -> str:
    parsed = urlsplit(controller_url)
    if parsed.scheme != "https" or not parsed.netloc or parsed.query or parsed.fragment:
        raise WorkerEnrollmentError("The controller URL must be an HTTPS origin.")
    return urlunsplit(("wss", parsed.netloc, endpoint, "", ""))


def _send_proof(
    socket: WorkerWebSocket,
    key_store: HardwareKeyStore,
    challenge: dict[str, object],
    purpose: str,
    worker_id: str,
) -> None:
    if _required_text(challenge, "purpose") != purpose or _required_text(challenge, "worker_id") != worker_id:
        raise WorkerEnrollmentError("The controller returned an invalid worker challenge.")
    nonce = _required_text(challenge, "nonce")
    signature = key_store.sign(worker_proof_payload(purpose=purpose, worker_id=worker_id, nonce=nonce))
    if not isinstance(signature, bytes):
        raise WorkerEnrollmentError("The device key returned an invalid signature.")
    _send(socket, {"signature": b64encode(signature).decode("ascii")})


def _send(socket: WorkerWebSocket, message: dict[str, object]) -> None:
    socket.send(json.dumps(message, separators=(",", ":"), sort_keys=True))


def _message(socket: WorkerWebSocket, *, timeout: float | None = None) -> dict[str, object]:
    try:
        message = json.loads(socket.recv(timeout=timeout))
    except (TypeError, ValueError) as error:
        raise WorkerEnrollmentError("The controller returned an invalid worker response.") from error
    if not isinstance(message, dict):
        raise WorkerEnrollmentError("The controller returned an invalid worker response.")
    return message


def _required_text(message: dict[str, object], field: str) -> str:
    value = message.get(field)
    if not isinstance(value, str) or not value:
        raise WorkerEnrollmentError("The controller returned an invalid worker response.")
    return value
