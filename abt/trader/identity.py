from __future__ import annotations

import json
import os
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urlsplit


class TraderEnrollmentError(RuntimeError):
    """Raised when Trader enrollment or connection cannot safely continue."""


_VERSION = 1
_FIELDS = {"version", "controller_url", "enrollment_id", "strategy_name", "public_ip", "key_name"}


@dataclass(frozen=True)
class TraderIdentity:
    controller_url: str
    enrollment_id: str
    strategy_name: str
    public_ip: str
    key_name: str


def default_identity_path() -> Path:
    return Path(sys.argv[0]).resolve().parent / "trader.json"


def pending_identity_path(path: Path) -> Path:
    return path.with_name(f"{path.stem}.pending{path.suffix}")


def state_path(path: Path) -> Path:
    return path.with_name(f"{path.stem}.state{path.suffix}")


def load_identity(path: Path) -> TraderIdentity:
    if not path.exists():
        raise TraderEnrollmentError(f"Trader identity configuration does not exist: {path}.")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise TraderEnrollmentError(f"Trader identity configuration is invalid: {path}.") from error
    if not isinstance(raw, dict) or set(raw) != _FIELDS or raw.get("version") != _VERSION:
        raise TraderEnrollmentError(f"Trader identity configuration is invalid: {path}.")
    identity = TraderIdentity(
        controller_url=raw["controller_url"],
        enrollment_id=raw["enrollment_id"],
        strategy_name=raw["strategy_name"],
        public_ip=raw["public_ip"],
        key_name=raw["key_name"],
    )
    _validate(identity, path)
    return identity


def ensure_identity_can_be_saved(path: Path, *, replace: bool) -> None:
    if path.exists() and not replace:
        raise TraderEnrollmentError(
            f"Trader identity configuration already exists: {path}. Use --replace-config to enroll a different Trader."
        )
    if not path.parent.is_dir():
        raise TraderEnrollmentError(f"Trader identity configuration directory does not exist: {path.parent}.")


def save_identity(path: Path, identity: TraderIdentity, *, replace: bool) -> None:
    ensure_identity_can_be_saved(path, replace=replace)
    _validate(identity, path)
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
        raise TraderEnrollmentError(f"Trader identity configuration cannot be written: {path}.") from error
    finally:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name)
            except OSError:
                pass


def _validate(identity: TraderIdentity, path: Path) -> None:
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
        or not all(isinstance(value, str) and value for value in (
            identity.enrollment_id, identity.strategy_name, identity.public_ip, identity.key_name
        ))
    ):
        raise TraderEnrollmentError(f"Trader identity configuration is invalid: {path}.")
