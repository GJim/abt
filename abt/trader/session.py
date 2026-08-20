from __future__ import annotations

import json
from base64 import b64encode
from collections import deque
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Protocol, TextIO
from urllib.parse import urlsplit, urlunsplit

from abt.controlplane.crypto import trader_proof_payload
from abt.worker.keystore import HardwareKeyStore

from .identity import TraderEnrollmentError


class TraderWebSocket(Protocol):
    def send(self, message: str) -> None: ...
    def recv(self, timeout: float | None = None) -> str: ...
    def __enter__(self) -> TraderWebSocket: ...
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None: ...


WebSocketConnector = Callable[[str], TraderWebSocket]


def deliver_trader_events(
    messages: Iterable[dict[str, object]],
    *,
    output: TextIO,
    acknowledge: Callable[[int], None],
    save_cursor: Callable[[int], None],
) -> None:
    """Emit each event before acknowledging it and mirroring the acknowledged cursor."""

    for message in messages:
        if message.get("type") != "event" or not isinstance(message.get("event_id"), int):
            continue
        output.write(json.dumps(message, separators=(",", ":"), sort_keys=True) + "\n")
        output.flush()
        cursor = message["event_id"]
        acknowledge(cursor)
        save_cursor(cursor)


def open_authenticated_trader_session(
    *,
    controller_url: str,
    enrollment_id: str,
    key_store: HardwareKeyStore,
    certificate: tuple[str, str],
    connect: WebSocketConnector | None = None,
) -> tuple[TraderWebSocket, str]:
    """Open an authenticated Trader WSS session using an in-memory certificate."""

    if connect is None:
        from websockets.sync.client import connect as websocket_connect
        connect = websocket_connect
    trader_id, certificate_value = certificate
    socket = connect(_trader_endpoint(controller_url, "/api/traders/session"))
    try:
        socket.__enter__()
        _send(socket, {"trader_id": trader_id, "certificate": certificate_value})
        challenge = _message(socket)
        _send_proof(socket, key_store, challenge, "trader_session", trader_id)
        authenticated = _message(socket)
        if authenticated.get("type") != "authenticated" or authenticated.get("trader_id") != trader_id:
            raise TraderEnrollmentError("The controller returned an invalid Trader session response.")
        return socket, trader_id
    except Exception:
        socket.__exit__(None, None, None)
        raise


def run_trader_session(
    socket: TraderWebSocket,
    *,
    output: TextIO,
    save_cursor: Callable[[int], None],
    on_heartbeat: Callable[[], None] | None = None,
) -> None:
    """Run one foreground connection until it disconnects."""

    pending_events: deque[dict[str, object]] = deque()
    while True:
        if pending_events:
            message = pending_events.popleft()
        else:
            try:
                message = _message(socket, timeout=30)
            except TimeoutError:
                if on_heartbeat is not None:
                    on_heartbeat()
                _send(socket, {"type": "heartbeat"})
                continue
        message_type = message.get("type")
        if message_type == "heartbeat":
            _send(socket, {"type": "heartbeat"})
            continue
        if message_type == "heartbeat_ack":
            continue
        if message_type != "event" or not isinstance(message.get("event_id"), int):
            raise TraderEnrollmentError("The controller returned an invalid Trader event.")
        output.write(json.dumps(message, separators=(",", ":"), sort_keys=True) + "\n")
        output.flush()
        cursor = message["event_id"]
        _send(socket, {"type": "ack", "cursor": cursor})
        while True:
            acknowledged = _message(socket)
            if acknowledged == {"type": "acknowledged", "cursor": cursor}:
                break
            if acknowledged.get("type") == "event" and isinstance(acknowledged.get("event_id"), int):
                pending_events.append(acknowledged)
                continue
            if acknowledged.get("type") == "heartbeat":
                _send(socket, {"type": "heartbeat"})
                continue
            raise TraderEnrollmentError("The controller rejected Trader event acknowledgement.")
        save_cursor(cursor)


def _send_proof(
    socket: TraderWebSocket, key_store: HardwareKeyStore, challenge: dict[str, object], purpose: str, trader_id: str
) -> None:
    if challenge.get("purpose") != purpose or challenge.get("trader_id") != trader_id:
        raise TraderEnrollmentError("The controller returned an invalid Trader challenge.")
    nonce = _required_text(challenge, "nonce")
    signature = key_store.sign(trader_proof_payload(purpose=purpose, trader_id=trader_id, nonce=nonce))
    if not isinstance(signature, bytes):
        raise TraderEnrollmentError("The Windows CNG key returned an invalid signature.")
    _send(socket, {"signature": b64encode(signature).decode("ascii")})


def _trader_endpoint(controller_url: str, endpoint: str) -> str:
    parsed = urlsplit(controller_url)
    if parsed.scheme != "https" or not parsed.netloc or parsed.query or parsed.fragment:
        raise TraderEnrollmentError("The controller URL must be an HTTPS origin.")
    return urlunsplit(("wss", parsed.netloc, endpoint, "", ""))


def _send(socket: TraderWebSocket, message: dict[str, object]) -> None:
    socket.send(json.dumps(message, separators=(",", ":"), sort_keys=True))


def _message(socket: TraderWebSocket, *, timeout: float | None = None) -> dict[str, object]:
    try:
        message = json.loads(socket.recv(timeout=timeout))
    except TimeoutError:
        raise
    except (TypeError, ValueError) as error:
        raise TraderEnrollmentError("The controller returned an invalid Trader response.") from error
    if not isinstance(message, dict):
        raise TraderEnrollmentError("The controller returned an invalid Trader response.")
    return message


def _required_text(message: dict[str, object], field: str) -> str:
    value = message.get(field)
    if not isinstance(value, str) or not value:
        raise TraderEnrollmentError("The controller returned an invalid Trader response.")
    return value
