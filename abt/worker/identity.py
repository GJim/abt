from __future__ import annotations

import json
import os
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urlsplit

from .enrollment import WorkerEnrollmentError


_VERSION = 1
_FIELDS = {"version", "controller_url", "enrollment_id", "login", "server", "key_name"}


@dataclass(frozen=True)
class WorkerIdentity:
    controller_url: str
    enrollment_id: str
    login: int
    server: str
    key_name: str


def default_identity_path() -> Path:
    return Path(sys.argv[0]).resolve().parent / "worker.json"


def pending_identity_path(path: Path) -> Path:
    return path.with_name(f"{path.stem}.pending{path.suffix}")


def load_identity(path: Path) -> WorkerIdentity:
    if not path.exists():
        raise WorkerEnrollmentError(f"Worker identity configuration does not exist: {path}.")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise WorkerEnrollmentError(f"Worker identity configuration is invalid: {path}.") from error
    if not isinstance(raw, dict) or set(raw) != _FIELDS or raw.get("version") != _VERSION:
        raise WorkerEnrollmentError(f"Worker identity configuration is invalid: {path}.")
    identity = WorkerIdentity(
        controller_url=raw["controller_url"],
        enrollment_id=raw["enrollment_id"],
        login=raw["login"],
        server=raw["server"],
        key_name=raw["key_name"],
    )
    _validate_identity(identity, path)
    return identity


def ensure_identity_can_be_saved(path: Path, *, replace: bool) -> None:
    if path.exists() and not replace:
        raise WorkerEnrollmentError(
            f"Worker identity configuration already exists: {path}. Use --replace-config to enroll a different worker."
        )
    if not path.parent.is_dir():
        raise WorkerEnrollmentError(f"Worker identity configuration directory does not exist: {path.parent}.")


def save_identity(path: Path, identity: WorkerIdentity, *, replace: bool) -> None:
    ensure_identity_can_be_saved(path, replace=replace)
    _validate_identity(identity, path)
    temporary_name: str | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent, text=True)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as temporary:
            json.dump({"version": _VERSION, **asdict(identity)}, temporary, sort_keys=True, separators=(",", ":"))
            temporary.write("\n")
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, path)
        temporary_name = None
    except OSError as error:
        raise WorkerEnrollmentError(f"Worker identity configuration cannot be written: {path}.") from error
    finally:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name)
            except OSError:
                pass


def _validate_identity(identity: WorkerIdentity, path: Path) -> None:
    parsed = urlsplit(identity.controller_url) if isinstance(identity.controller_url, str) else None
    if (
        parsed is None
        or parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise WorkerEnrollmentError(f"Worker identity configuration is invalid: {path}.")
    if (
        not isinstance(identity.enrollment_id, str)
        or not identity.enrollment_id
        or not isinstance(identity.login, int)
        or isinstance(identity.login, bool)
        or identity.login <= 0
        or not isinstance(identity.server, str)
        or not identity.server
        or not isinstance(identity.key_name, str)
        or not identity.key_name
    ):
        raise WorkerEnrollmentError(f"Worker identity configuration is invalid: {path}.")
