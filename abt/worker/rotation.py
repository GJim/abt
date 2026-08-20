from __future__ import annotations

import base64
import json
import os
import tempfile
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol, TextIO
from uuid import uuid4

from abt.controlplane.crypto import worker_rotation_payload

from .enrollment import WorkerEnrollmentError
from .identity import WorkerIdentity, save_identity
from .keystore import HardwareKeyStore


_HALF_LIFE = timedelta(days=15)
_OVERLAP = timedelta(hours=1)
_RETRY_DELAY = timedelta(days=1)


class WorkerRotationError(WorkerEnrollmentError):
    """Raised when automatic device-certificate maintenance cannot proceed safely."""


class WorkerCertificateRotated(WorkerRotationError):
    """Signals the caller to reconnect using the atomically saved replacement identity."""


class WorkerCertificateExpired(WorkerRotationError):
    """Signals that an expired identity cannot safely resume a Worker session."""


class WorkerRotationTransport(Protocol):
    def rotation_challenge(self, controller_url: str, worker_id: str, public_key_pem: str) -> Mapping[str, object]: ...

    def rotate(
        self,
        controller_url: str,
        worker_id: str,
        public_key_pem: str,
        old_signature: str,
        replacement_signature: str,
    ) -> Mapping[str, object]: ...


def worker_state_path(identity_path: Path) -> Path:
    return identity_path.with_name(f"{identity_path.stem}.state{identity_path.suffix}")


def ensure_worker_certificate_current(certificate: str, *, now: Callable[[], datetime]) -> None:
    """Reject an expired certificate before it is used to resume a Worker session."""

    _, expires_at = _certificate_lifetime(certificate)
    if _utc(now()) >= expires_at:
        raise WorkerCertificateExpired("The Worker device certificate has expired; operator action is required.")


def maintain_worker_certificate(
    *,
    identity_path: Path,
    identity: WorkerIdentity,
    worker_id: str,
    certificate: str,
    current_key: HardwareKeyStore,
    key_store_factory: Callable[[str], HardwareKeyStore],
    transport: WorkerRotationTransport,
    now: Callable[[], datetime],
    error_output: TextIO,
) -> None:
    """Clean retired keys and rotate an eligible active device certificate."""

    observed_at = _utc(now())
    state_path = worker_state_path(identity_path)
    if _recover_pending_rotation(identity_path, identity, state_path):
        raise WorkerCertificateRotated("Worker rotation recovery completed; reconnecting with the replacement identity.")
    _clean_retired_keys(state_path, identity.key_name, key_store_factory, observed_at, error_output)
    issued_at, expires_at = _certificate_lifetime(certificate)
    ensure_worker_certificate_current(certificate, now=lambda: observed_at)
    if observed_at - issued_at < _HALF_LIFE:
        return

    replacement_name = f"{identity.key_name}-rotation-{uuid4()}"
    journal_path = _rotation_journal_path(identity_path)
    replacement_key: HardwareKeyStore | None = None
    became_current = False
    try:
        replacement_key = key_store_factory(replacement_name)
        replacement_public_key = replacement_key.public_key_pem()
        challenge = transport.rotation_challenge(identity.controller_url, worker_id, replacement_public_key)
        nonce = _required(challenge, "nonce")
        if _required(challenge, "purpose") != "worker_certificate_rotation" or _required(challenge, "worker_id") != worker_id:
            raise WorkerRotationError("The controller returned an invalid rotation challenge.")
        payload = worker_rotation_payload(
            worker_id=worker_id, replacement_public_key_pem=replacement_public_key, nonce=nonce
        )
        result = transport.rotate(
            identity.controller_url,
            worker_id,
            replacement_public_key,
            _signature(current_key, payload),
            _signature(replacement_key, payload),
        )
        if _required(result, "worker_id") != worker_id or not _required(result, "certificate"):
            raise WorkerRotationError("The controller returned an invalid rotation response.")
        replacement = WorkerIdentity(
            controller_url=identity.controller_url,
            enrollment_id=identity.enrollment_id,
            login=identity.login,
            server=identity.server,
            key_name=replacement_name,
        )
        _write_journal(journal_path, identity.key_name, replacement_name, observed_at + _OVERLAP)
        save_identity(identity_path, replacement, replace=True)
        became_current = True
        try:
            _add_retired_key(state_path, identity.key_name, observed_at + _OVERLAP)
        except WorkerRotationError as error:
            raise WorkerCertificateRotated(
                "Worker device certificate rotated; reconnecting with the replacement identity."
            ) from error
        journal_path.unlink()
    except WorkerRotationError:
        raise
    except Exception as error:
        raise WorkerRotationError("Worker device certificate rotation failed safely.") from error
    finally:
        if replacement_key is not None and not became_current:
            _delete_key(replacement_key)
        if replacement_key is not None:
            _close_key(replacement_key)
    raise WorkerCertificateRotated("Worker device certificate rotated; reconnecting with the replacement identity.")


def _certificate_lifetime(certificate: str) -> tuple[datetime, datetime]:
    try:
        envelope = json.loads(certificate)
        claims = json.loads(base64.b64decode(envelope["payload"], validate=True))
        issued_at = datetime.fromisoformat(claims["issued_at"])
        expires_at = datetime.fromisoformat(claims["expires_at"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise WorkerRotationError("The controller returned an invalid Worker device certificate.") from error
    if issued_at.tzinfo is None or expires_at.tzinfo is None or expires_at <= issued_at:
        raise WorkerRotationError("The controller returned an invalid Worker device certificate.")
    return issued_at.astimezone(UTC), expires_at.astimezone(UTC)


def _clean_retired_keys(
    state_path: Path,
    active_key_name: str,
    key_store_factory: Callable[[str], HardwareKeyStore],
    observed_at: datetime,
    error_output: TextIO,
) -> None:
    state = _load_state(state_path)
    retained: list[dict[str, str]] = []
    changed = False
    for retired in state["retired_keys"]:
        key_name = retired["key_name"]
        eligible_at = _parse_timestamp(retired["eligible_at"])
        retry_at = _parse_timestamp(retired["retry_at"]) if "retry_at" in retired else eligible_at
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
            print(f"Worker retired CNG key cleanup failed for {key_name}; retrying in 24 hours.", file=error_output)
        else:
            changed = True
        finally:
            _close_key(key)
    if changed:
        _write_state(state_path, retained)


def _add_retired_key(state_path: Path, key_name: str, eligible_at: datetime) -> None:
    state = _load_state(state_path)
    retired = [entry for entry in state["retired_keys"] if entry["key_name"] != key_name]
    retired.append({"key_name": key_name, "eligible_at": eligible_at.isoformat()})
    _write_state(state_path, retired)


def _rotation_journal_path(identity_path: Path) -> Path:
    return identity_path.with_name(f"{identity_path.stem}.rotation.pending{identity_path.suffix}")


def _write_journal(path: Path, retired_key_name: str, replacement_key_name: str, eligible_at: datetime) -> None:
    _atomic_write(
        path,
        {
            "version": 1,
            "retired_key_name": retired_key_name,
            "replacement_key_name": replacement_key_name,
            "eligible_at": eligible_at.isoformat(),
        },
        "Worker rotation recovery journal",
    )


def _recover_pending_rotation(identity_path: Path, identity: WorkerIdentity, state_path: Path) -> bool:
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
        raise WorkerRotationError(f"Worker rotation recovery journal is invalid: {journal_path}.") from error
    switched_identity = identity.key_name == journal["retired_key_name"]
    if switched_identity:
        identity = WorkerIdentity(
            controller_url=identity.controller_url,
            enrollment_id=identity.enrollment_id,
            login=identity.login,
            server=identity.server,
            key_name=journal["replacement_key_name"],
        )
        save_identity(identity_path, identity, replace=True)
    elif identity.key_name != journal["replacement_key_name"]:
        raise WorkerRotationError("Worker rotation recovery journal does not match the active identity.")
    _add_retired_key(state_path, journal["retired_key_name"], eligible_at)
    journal_path.unlink()
    return switched_identity


def _load_state(path: Path) -> dict[str, list[dict[str, str]]]:
    if not path.exists():
        return {"retired_keys": []}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        retired = value["retired_keys"]
        if value.get("version") != 1 or set(value) != {"version", "retired_keys"} or not isinstance(retired, list):
            raise ValueError
        normalized = []
        for entry in retired:
            if not isinstance(entry, dict) or set(entry) - {"key_name", "eligible_at", "retry_at"}:
                raise ValueError
            key_name = entry.get("key_name")
            eligible_at = entry.get("eligible_at")
            if not isinstance(key_name, str) or not key_name or not isinstance(eligible_at, str):
                raise ValueError
            _parse_timestamp(eligible_at)
            if "retry_at" in entry:
                if not isinstance(entry["retry_at"], str):
                    raise ValueError
                _parse_timestamp(entry["retry_at"])
            normalized.append(dict(entry))
        return {"retired_keys": normalized}
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
        raise WorkerRotationError(f"Worker rotation state is invalid: {path}.") from error


def _write_state(path: Path, retired_keys: list[dict[str, str]]) -> None:
    _atomic_write(path, {"version": 1, "retired_keys": retired_keys}, "Worker rotation state")


def _atomic_write(path: Path, value: dict[str, object], label: str) -> None:
    temporary_name: str | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent, text=True)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as temporary:
            json.dump(value, temporary, sort_keys=True, separators=(",", ":"))
            temporary.write("\n")
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, path)
        temporary_name = None
    except OSError as error:
        raise WorkerRotationError(f"{label} cannot be written: {path}.") from error
    finally:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name)
            except OSError:
                pass


def _required(response: Mapping[str, object], field: str) -> str:
    value = response.get(field)
    if not isinstance(value, str) or not value:
        raise WorkerRotationError("The controller returned an invalid rotation response.")
    return value


def _signature(key: HardwareKeyStore, payload: bytes) -> str:
    signature = key.sign(payload)
    if not isinstance(signature, bytes):
        raise WorkerRotationError("The Windows CNG key returned an invalid signature.")
    return base64.b64encode(signature).decode("ascii")


def _parse_timestamp(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value)
    if timestamp.tzinfo is None:
        raise ValueError
    return timestamp.astimezone(UTC)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise WorkerRotationError("The Worker rotation clock must include a timezone.")
    return value.astimezone(UTC)


def _delete_key(key: HardwareKeyStore) -> None:
    delete = getattr(key, "delete", None)
    if not callable(delete):
        raise WorkerRotationError("The Windows CNG key store cannot delete retired keys.")
    delete()


def _close_key(key: HardwareKeyStore) -> None:
    close = getattr(key, "close", None)
    if callable(close):
        close()
