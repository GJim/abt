from __future__ import annotations

import base64
import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol, TextIO
from uuid import uuid4

from abt.controlplane.crypto import trader_rotation_payload
from abt.worker.keystore import HardwareKeyStore

from .identity import TraderEnrollmentError, TraderIdentity, atomic_write_json, load_identity, save_identity, state_path

_HALF_LIFE = timedelta(days=15)
_OVERLAP = timedelta(hours=1)
_RETRY_DELAY = timedelta(days=1)


class TraderRotationError(TraderEnrollmentError):
    """Raised when Trader device-certificate maintenance cannot safely continue."""


class TraderCertificateRotated(TraderRotationError):
    """Signals that the foreground session must reconnect with a replacement key."""


class TraderCertificateExpired(TraderRotationError):
    """Signals that an expired Trader certificate cannot resume a session."""


class TraderRotationTransport(Protocol):
    def rotation_challenge(self, controller_url: str, trader_id: str, public_key_pem: str) -> Mapping[str, object]: ...

    def rotate(
        self,
        controller_url: str,
        trader_id: str,
        public_key_pem: str,
        old_signature: str,
        replacement_signature: str,
    ) -> Mapping[str, object]: ...


def maintain_trader_certificate(
    *,
    identity_path: Path,
    identity: TraderIdentity,
    trader_id: str,
    certificate: str,
    current_key: HardwareKeyStore,
    key_store_factory: Callable[[str], HardwareKeyStore],
    transport: TraderRotationTransport,
    now: Callable[[], datetime],
    error_output: TextIO,
) -> None:
    """Clean retired keys and rotate an eligible Trader certificate."""

    observed_at = _utc(now())
    current_state_path = state_path(identity_path)
    if _recover_pending_rotation(identity_path, identity, current_state_path):
        raise TraderCertificateRotated("Trader rotation recovery completed; reconnecting with the replacement identity.")
    _clean_retired_keys(current_state_path, identity.key_name, key_store_factory, observed_at, error_output)
    issued_at, expires_at = _certificate_lifetime(certificate)
    if observed_at >= expires_at:
        raise TraderCertificateExpired("The Trader device certificate has expired; operator action is required.")
    if observed_at - issued_at < _HALF_LIFE:
        return

    replacement_name = f"{identity.key_name}-rotation-{uuid4()}"
    journal_path = _rotation_journal_path(identity_path)
    replacement_key: HardwareKeyStore | None = None
    became_current = False
    try:
        replacement_key = key_store_factory(replacement_name)
        replacement_public_key = replacement_key.public_key_pem()
        challenge = transport.rotation_challenge(identity.controller_url, trader_id, replacement_public_key)
        nonce = _required(challenge, "nonce")
        if _required(challenge, "purpose") != "trader_certificate_rotation" or _required(challenge, "trader_id") != trader_id:
            raise TraderRotationError("The controller returned an invalid rotation challenge.")
        payload = trader_rotation_payload(
            trader_id=trader_id, replacement_public_key_pem=replacement_public_key, nonce=nonce
        )
        result = transport.rotate(
            identity.controller_url,
            trader_id,
            replacement_public_key,
            _signature(current_key, payload),
            _signature(replacement_key, payload),
        )
        if _required(result, "trader_id") != trader_id or not _required(result, "certificate"):
            raise TraderRotationError("The controller returned an invalid rotation response.")
        replacement = TraderIdentity(
            controller_url=identity.controller_url,
            enrollment_id=identity.enrollment_id,
            strategy_name=identity.strategy_name,
            public_ip=identity.public_ip,
            key_name=replacement_name,
        )
        _write_journal(journal_path, identity.key_name, replacement_name, observed_at + _OVERLAP)
        save_identity(identity_path, replacement, replace=True)
        became_current = True
        try:
            _add_retired_key(current_state_path, identity.key_name, observed_at + _OVERLAP)
        except TraderRotationError as error:
            raise TraderCertificateRotated(
                "Trader device certificate rotated; reconnecting with the replacement identity."
            ) from error
        journal_path.unlink()
    except TraderRotationError:
        raise
    except Exception as error:
        raise TraderRotationError("Trader device certificate rotation failed safely.") from error
    finally:
        if replacement_key is not None and not became_current:
            _delete_key(replacement_key)
        if replacement_key is not None:
            _close_key(replacement_key)
    raise TraderCertificateRotated("Trader device certificate rotated; reconnecting with the replacement identity.")


def save_acknowledged_cursor(path: Path, cursor: int) -> None:
    """Mirror only a control-plane-acknowledged Trader event cursor."""

    if not isinstance(cursor, int):
        raise TraderRotationError("The Trader acknowledgement cursor is invalid.")
    state = _load_state(path)
    state["acknowledged_cursor"] = cursor
    _write_state(path, state)


def validate_or_warn_state(path: Path, error_output: TextIO) -> None:
    if not path.exists():
        print("Trader cursor state is unavailable; the controller will replay from its acknowledged cursor.", file=error_output)
        return
    try:
        _load_state(path)
    except TraderRotationError:
        print("Trader cursor state is invalid; the controller will replay from its acknowledged cursor.", file=error_output)


def _certificate_lifetime(certificate: str) -> tuple[datetime, datetime]:
    try:
        envelope = json.loads(certificate)
        claims = json.loads(base64.b64decode(envelope["payload"], validate=True))
        issued_at = datetime.fromisoformat(claims["issued_at"])
        expires_at = datetime.fromisoformat(claims["expires_at"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise TraderRotationError("The controller returned an invalid Trader device certificate.") from error
    if issued_at.tzinfo is None or expires_at.tzinfo is None or expires_at <= issued_at:
        raise TraderRotationError("The controller returned an invalid Trader device certificate.")
    return issued_at.astimezone(UTC), expires_at.astimezone(UTC)


def _clean_retired_keys(
    path: Path,
    active_key_name: str,
    key_store_factory: Callable[[str], HardwareKeyStore],
    observed_at: datetime,
    error_output: TextIO,
) -> None:
    state = _load_state(path)
    retained: list[dict[str, str]] = []
    changed = False
    for retired in state["retired_keys"]:
        key_name = retired["key_name"]
        eligible_at = _parse_timestamp(retired["eligible_at"])
        retry_at = _parse_timestamp(retired.get("retry_at", retired["eligible_at"]))
        if key_name == active_key_name or observed_at < eligible_at or observed_at < retry_at:
            retained.append(retired)
            continue
        key = key_store_factory(key_name)
        try:
            _delete_key(key)
        except Exception:
            retained.append(
                {"key_name": key_name, "eligible_at": eligible_at.isoformat(), "retry_at": (observed_at + _RETRY_DELAY).isoformat()}
            )
            changed = True
            print(f"Trader retired CNG key cleanup failed for {key_name}; retrying in 24 hours.", file=error_output)
        else:
            changed = True
        finally:
            _close_key(key)
    if changed:
        state["retired_keys"] = retained
        _write_state(path, state)


def _add_retired_key(path: Path, key_name: str, eligible_at: datetime) -> None:
    state = _load_state(path)
    state["retired_keys"] = [entry for entry in state["retired_keys"] if entry["key_name"] != key_name]
    state["retired_keys"].append({"key_name": key_name, "eligible_at": eligible_at.isoformat()})
    _write_state(path, state)


def _rotation_journal_path(identity_path: Path) -> Path:
    return identity_path.with_name(f"{identity_path.stem}.rotation.pending{identity_path.suffix}")


def _write_journal(path: Path, retired_key_name: str, replacement_key_name: str, eligible_at: datetime) -> None:
    atomic_write_json(
        path,
        {
            "version": 1,
            "retired_key_name": retired_key_name,
            "replacement_key_name": replacement_key_name,
            "eligible_at": eligible_at.isoformat(),
        },
        "Trader rotation recovery journal",
    )


def _recover_pending_rotation(identity_path: Path, identity: TraderIdentity, current_state_path: Path) -> bool:
    journal_path = _rotation_journal_path(identity_path)
    if not journal_path.exists():
        return False
    try:
        journal = json.loads(journal_path.read_text(encoding="utf-8"))
        if (
            not isinstance(journal, dict)
            or set(journal) != {"version", "retired_key_name", "replacement_key_name", "eligible_at"}
            or journal.get("version") != 1
            or not isinstance(journal["retired_key_name"], str)
            or not isinstance(journal["replacement_key_name"], str)
            or not isinstance(journal["eligible_at"], str)
        ):
            raise ValueError
        eligible_at = _parse_timestamp(journal["eligible_at"])
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
        raise TraderRotationError(f"Trader rotation recovery journal is invalid: {journal_path}.") from error
    switched_identity = identity.key_name == journal["retired_key_name"]
    if switched_identity:
        identity = TraderIdentity(
            controller_url=identity.controller_url,
            enrollment_id=identity.enrollment_id,
            strategy_name=identity.strategy_name,
            public_ip=identity.public_ip,
            key_name=journal["replacement_key_name"],
        )
        save_identity(identity_path, identity, replace=True)
    elif identity.key_name != journal["replacement_key_name"]:
        raise TraderRotationError("Trader rotation recovery journal does not match the active identity.")
    _add_retired_key(current_state_path, journal["retired_key_name"], eligible_at)
    journal_path.unlink()
    return switched_identity


def _load_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"acknowledged_cursor": None, "retired_keys": []}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError
        if set(value) == {"version", "acknowledged_cursor"} and value.get("version") == 1:
            value = {"version": 2, "acknowledged_cursor": value["acknowledged_cursor"], "retired_keys": []}
        if set(value) != {"version", "acknowledged_cursor", "retired_keys"} or value.get("version") != 2:
            raise ValueError
        cursor = value["acknowledged_cursor"]
        retired = value["retired_keys"]
        if cursor is not None and not isinstance(cursor, int) or not isinstance(retired, list):
            raise ValueError
        normalized: list[dict[str, str]] = []
        for entry in retired:
            if not isinstance(entry, dict) or set(entry) - {"key_name", "eligible_at", "retry_at"}:
                raise ValueError
            if not isinstance(entry.get("key_name"), str) or not entry["key_name"] or not isinstance(entry.get("eligible_at"), str):
                raise ValueError
            _parse_timestamp(entry["eligible_at"])
            if "retry_at" in entry:
                if not isinstance(entry["retry_at"], str):
                    raise ValueError
                _parse_timestamp(entry["retry_at"])
            normalized.append(dict(entry))
        return {"acknowledged_cursor": cursor, "retired_keys": normalized}
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
        raise TraderRotationError(f"Trader rotation state is invalid: {path}.") from error


def _write_state(path: Path, state: dict[str, object]) -> None:
    atomic_write_json(path, {"version": 2, **state}, "Trader state")


def _required(response: Mapping[str, object], field: str) -> str:
    value = response.get(field)
    if not isinstance(value, str) or not value:
        raise TraderRotationError("The controller returned an invalid rotation response.")
    return value


def _signature(key: HardwareKeyStore, payload: bytes) -> str:
    signature = key.sign(payload)
    if not isinstance(signature, bytes):
        raise TraderRotationError("The Windows CNG key returned an invalid signature.")
    return base64.b64encode(signature).decode("ascii")


def _parse_timestamp(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value)
    if timestamp.tzinfo is None:
        raise ValueError
    return timestamp.astimezone(UTC)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise TraderRotationError("The Trader rotation clock must include a timezone.")
    return value.astimezone(UTC)


def _delete_key(key: HardwareKeyStore) -> None:
    delete = getattr(key, "delete", None)
    if not callable(delete):
        raise TraderRotationError("The Windows CNG key store cannot delete retired keys.")
    delete()


def _close_key(key: HardwareKeyStore) -> None:
    close = getattr(key, "close", None)
    if callable(close):
        close()
