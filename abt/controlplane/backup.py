from __future__ import annotations

import hashlib
import json
import shutil
import tarfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .ledger import ControlLedger


class BackupError(RuntimeError):
    """Raised when a control-plane backup cannot be created or verified."""


class BackupManager:
    """Creates complete, locally retained control-plane restore sets."""

    _ARTIFACTS = ("ledger-export.tar.gz", "openbao-raft.tar.gz", "softhsm-tokens.tar.gz")

    def __init__(
        self,
        ledger: ControlLedger,
        backup_directory: Path,
        openbao_raft_directory: Path,
        softhsm_tokens_directory: Path,
        *,
        rotations: int = 24,
    ) -> None:
        if rotations < 1:
            raise ValueError("At least one backup rotation is required.")
        self._ledger = ledger
        self._backup_directory = backup_directory
        self._openbao_raft_directory = openbao_raft_directory
        self._softhsm_tokens_directory = softhsm_tokens_directory
        self._rotations = rotations

    def create(self, reason: str) -> Path:
        if not reason or any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for character in reason):
            raise BackupError("Backup reason must contain only letters, digits, hyphens, or underscores.")
        self._backup_directory.mkdir(parents=True, exist_ok=True)
        name = f"{datetime.now(UTC):%Y%m%dT%H%M%S%fZ}-{reason}"
        destination = self._backup_directory / name
        destination.mkdir()
        try:
            self._ledger.snapshot(destination / "ledger-export.tar.gz")
            self._archive(self._openbao_raft_directory, destination / "openbao-raft.tar.gz")
            self._archive(self._softhsm_tokens_directory, destination / "softhsm-tokens.tar.gz")
            artifacts = {
                artifact: {"sha256": _sha256(destination / artifact), "bytes": (destination / artifact).stat().st_size}
                for artifact in self._ARTIFACTS
            }
            (destination / "manifest.json").write_text(
                json.dumps(
                    {
                        "artifacts": artifacts,
                        "created_at": datetime.now(UTC).isoformat(),
                        "reason": reason,
                        "schema": 1,
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            self._rotate()
            return destination
        except BaseException:
            shutil.rmtree(destination, ignore_errors=True)
            raise

    def verify_restore_set(self, backup_set: Path) -> dict[str, Any]:
        try:
            resolved_set = backup_set.resolve(strict=True)
            root = self._backup_directory.resolve(strict=True)
        except OSError as error:
            raise BackupError("Backup set does not exist.") from error
        if resolved_set.parent != root:
            raise BackupError("Backup set is not in the allowed backup directory.")
        try:
            manifest = json.loads((resolved_set / "manifest.json").read_text(encoding="utf-8"))
            artifacts = manifest["artifacts"]
        except (OSError, json.JSONDecodeError, KeyError, TypeError) as error:
            raise BackupError("Backup manifest is invalid.") from error
        if set(artifacts) != set(self._ARTIFACTS):
            raise BackupError("Backup manifest does not define the required restore set.")
        for name in self._ARTIFACTS:
            entry = artifacts[name]
            artifact = resolved_set / name
            if not isinstance(entry, dict) or entry.get("sha256") != _sha256(artifact):
                raise BackupError(f"Backup artifact {name!r} failed integrity verification.")
            self._verify_archive(artifact)
        return manifest

    @staticmethod
    def _archive(source: Path, destination: Path) -> None:
        if not source.is_dir():
            raise BackupError(f"Required persistent volume {source} is unavailable.")
        with tarfile.open(destination, "w:gz") as archive:
            for path in sorted(source.rglob("*")):
                archive.add(path, arcname=path.relative_to(source), recursive=False)

    @staticmethod
    def _verify_archive(artifact: Path) -> None:
        try:
            with tarfile.open(artifact, "r:gz") as archive:
                members = archive.getmembers()
                if any(member.name.startswith("/") or ".." in Path(member.name).parts for member in members):
                    raise BackupError(f"Backup artifact {artifact.name!r} contains an unsafe path.")
                if artifact.name == "ledger-export.tar.gz" and not {"schema.sql", "load.sql"}.issubset(
                    {member.name for member in members}
                ):
                    raise BackupError("DuckDB backup is not a restorable export.")
        except (OSError, tarfile.TarError) as error:
            raise BackupError(f"Backup artifact {artifact.name!r} is not a valid archive.") from error

    def _rotate(self) -> None:
        backups = sorted(path for path in self._backup_directory.iterdir() if path.is_dir())
        for expired in backups[:-self._rotations]:
            shutil.rmtree(expired)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as artifact:
            for chunk in iter(lambda: artifact.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise BackupError(f"Backup artifact {path.name!r} is unavailable.") from error
    return digest.hexdigest()
