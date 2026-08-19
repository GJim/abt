from __future__ import annotations

import re
import sys
import tomllib
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from math import isfinite
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


CONTEXT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_CALIBRATION_STATUSES = {"unavailable", "calibrated"}


class ConfigError(RuntimeError):
    """Raised when the local CLI configuration is invalid."""


@dataclass(frozen=True)
class CalibrationSample:
    source: str
    calibrated_at_utc: str
    offset_seconds: int
    error_seconds: float
    ticket: int | None = None
    symbol: str | None = None


@dataclass(frozen=True)
class TimeCalibrationFamily:
    offset_seconds: int | None = None
    calibrated_local_date: str | None = None
    calibrated_at_utc: str | None = None
    samples: tuple[CalibrationSample, ...] = ()
    status: str = "unavailable"
    calibration_symbol: str | None = None


@dataclass(frozen=True)
class TimeCalibration:
    market_data: TimeCalibrationFamily = field(default_factory=TimeCalibrationFamily)
    trade_records: TimeCalibrationFamily = field(default_factory=TimeCalibrationFamily)


@dataclass(frozen=True)
class Context:
    name: str
    terminal_path: Path
    login: int
    server: str
    user_timezone: str | None = None
    time_calibration: TimeCalibration = field(default_factory=TimeCalibration)


@dataclass(frozen=True)
class Config:
    path: Path
    current_context: str | None
    contexts: dict[str, Context]
    version: int = 1


def default_config_path() -> Path:
    return Path(sys.argv[0]).resolve().parent / "mt5.toml"


def load(path: Path) -> Config:
    if not path.exists():
        return Config(path=path, current_context=None, contexts={})
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ConfigError(f"Cannot read configuration {path}: {error}") from error

    version = raw.get("version", 1)
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise ConfigError("version must be a positive integer.")
    raw_contexts = raw.get("contexts", {})
    if not isinstance(raw_contexts, dict):
        raise ConfigError("Configuration field [contexts] must be a table.")

    contexts: dict[str, Context] = {}
    for name, values in raw_contexts.items():
        if not isinstance(name, str) or not CONTEXT_NAME.fullmatch(name):
            raise ConfigError(f"Invalid context name: {name!r}")
        if not isinstance(values, dict):
            raise ConfigError(f"Context {name!r} must be a table.")
        try:
            terminal_path = Path(str(values["terminal_path"]))
            login = int(values["login"])
            server = str(values["server"])
        except (KeyError, TypeError, ValueError) as error:
            raise ConfigError(f"Context {name!r} requires terminal_path, login, and server.") from error
        user_timezone = _load_timezone(name, values.get("user_timezone"))
        calibration = _load_time_calibration(name, values.get("time_calibration"))
        contexts[name] = Context(name, terminal_path, login, server, user_timezone, calibration)

    current_context = raw.get("current_context")
    if current_context is not None and (not isinstance(current_context, str) or current_context not in contexts):
        raise ConfigError("current_context must name an existing context.")
    return Config(path=path, current_context=current_context, contexts=contexts, version=version)


def save(config: Config) -> None:
    lines = ["version = 2"]
    if config.current_context is not None:
        lines.append(f'current_context = "{_escape(config.current_context)}"')
    for name in sorted(config.contexts):
        context = config.contexts[name]
        lines.extend(
            [
                "",
                f'[contexts."{_escape(name)}"]',
                f'terminal_path = "{_escape(str(context.terminal_path))}"',
                f"login = {context.login}",
                f'server = "{_escape(context.server)}"',
                *(
                    [f'user_timezone = "{_escape(context.user_timezone)}"']
                    if context.user_timezone is not None
                    else []
                ),
            ]
        )
        _save_family(lines, name, "market_data", context.time_calibration.market_data)
        _save_family(lines, name, "trade_records", context.time_calibration.trade_records)
    config.path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = config.path.with_suffix(".tmp")
    temporary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    temporary_path.replace(config.path)


def _load_timezone(name: str, value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigError(f"Context {name!r} user_timezone must be an IANA timezone name.")
    try:
        ZoneInfo(value)
    except (ValueError, ZoneInfoNotFoundError) as error:
        raise ConfigError(f"Context {name!r} has an unknown IANA timezone: {value!r}.") from error
    return value


def _load_time_calibration(name: str, value: object) -> TimeCalibration:
    if value is None:
        return TimeCalibration()
    if not isinstance(value, dict):
        raise ConfigError(f"Context {name!r} time_calibration must be a table.")
    return TimeCalibration(
        market_data=_load_family(name, "market_data", value.get("market_data")),
        trade_records=_load_family(name, "trade_records", value.get("trade_records")),
    )


def _load_family(context_name: str, family_name: str, value: object) -> TimeCalibrationFamily:
    if value is None:
        return TimeCalibrationFamily()
    if not isinstance(value, dict):
        raise ConfigError(f"Context {context_name!r} {family_name} calibration must be a table.")
    offset = value.get("offset_seconds")
    if offset is not None and (not isinstance(offset, int) or isinstance(offset, bool)):
        raise ConfigError(f"Context {context_name!r} {family_name} offset_seconds must be an integer.")
    local_date = _optional_date(context_name, family_name, value.get("calibrated_local_date"))
    calibrated_at = _optional_utc_datetime(context_name, family_name, value.get("calibrated_at_utc"))
    status = value.get("status", "unavailable")
    if not isinstance(status, str) or status not in _CALIBRATION_STATUSES:
        raise ConfigError(f"Context {context_name!r} {family_name} calibration status is invalid.")
    if status == "calibrated" and (
        offset is None or local_date is None or calibrated_at is None
    ):
        raise ConfigError(f"Context {context_name!r} {family_name} calibrated status requires an offset and timestamps.")
    symbol = value.get("calibration_symbol")
    if symbol is not None and not isinstance(symbol, str):
        raise ConfigError(f"Context {context_name!r} {family_name} calibration_symbol must be a string.")
    samples_value = value.get("samples", [])
    if not isinstance(samples_value, list):
        raise ConfigError(f"Context {context_name!r} {family_name} samples must be an array.")
    samples = tuple(_load_sample(context_name, family_name, sample) for sample in samples_value)
    if len(samples) > 20:
        raise ConfigError(f"Context {context_name!r} {family_name} samples may contain at most 20 entries.")
    return TimeCalibrationFamily(offset, local_date, calibrated_at, samples, status, symbol)


def _optional_date(context_name: str, family_name: str, value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigError(f"Context {context_name!r} {family_name} calibrated_local_date must be YYYY-MM-DD.")
    try:
        date.fromisoformat(value)
    except ValueError as error:
        raise ConfigError(f"Context {context_name!r} {family_name} calibrated_local_date must be YYYY-MM-DD.") from error
    return value


def _optional_utc_datetime(context_name: str, family_name: str, value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigError(f"Context {context_name!r} {family_name} calibrated_at_utc must be ISO-8601.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ConfigError(f"Context {context_name!r} {family_name} calibrated_at_utc must be ISO-8601.") from error
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ConfigError(f"Context {context_name!r} {family_name} calibrated_at_utc must use UTC.")
    return value


def _load_sample(context_name: str, family_name: str, value: object) -> CalibrationSample:
    if not isinstance(value, dict):
        raise ConfigError(f"Context {context_name!r} {family_name} calibration sample must be a table.")
    source = value.get("source")
    calibrated_at = value.get("calibrated_at_utc")
    offset = value.get("offset_seconds")
    error = value.get("error_seconds")
    ticket = value.get("ticket")
    symbol = value.get("symbol")
    if not isinstance(source, str) or not source:
        raise ConfigError(f"Context {context_name!r} {family_name} calibration sample source is required.")
    _optional_utc_datetime(context_name, family_name, calibrated_at)
    if not isinstance(calibrated_at, str):
        raise ConfigError(f"Context {context_name!r} {family_name} calibration sample calibrated_at_utc is required.")
    if not isinstance(offset, int) or isinstance(offset, bool):
        raise ConfigError(f"Context {context_name!r} {family_name} calibration sample offset_seconds must be an integer.")
    if not isinstance(error, (int, float)) or isinstance(error, bool) or not isfinite(error) or error < 0:
        raise ConfigError(f"Context {context_name!r} {family_name} calibration sample error_seconds must be non-negative.")
    if ticket is not None and (not isinstance(ticket, int) or isinstance(ticket, bool) or ticket <= 0):
        raise ConfigError(f"Context {context_name!r} {family_name} calibration sample ticket must be positive.")
    if symbol is not None and not isinstance(symbol, str):
        raise ConfigError(f"Context {context_name!r} {family_name} calibration sample symbol must be a string.")
    return CalibrationSample(source, calibrated_at, offset, float(error), ticket, symbol)


def _save_family(lines: list[str], context_name: str, family_name: str, family: TimeCalibrationFamily) -> None:
    if (
        family.status == "unavailable"
        and family.offset_seconds is None
        and family.calibrated_local_date is None
        and family.calibrated_at_utc is None
        and not family.samples
        and family.calibration_symbol is None
    ):
        return
    lines.extend(["", f'[contexts."{_escape(context_name)}".time_calibration.{family_name}]'])
    lines.append(f'status = "{_escape(family.status)}"')
    if family.offset_seconds is not None:
        lines.append(f"offset_seconds = {family.offset_seconds}")
    if family.calibrated_local_date is not None:
        lines.append(f'calibrated_local_date = "{_escape(family.calibrated_local_date)}"')
    if family.calibrated_at_utc is not None:
        lines.append(f'calibrated_at_utc = "{_escape(family.calibrated_at_utc)}"')
    if family.calibration_symbol is not None:
        lines.append(f'calibration_symbol = "{_escape(family.calibration_symbol)}"')
    if family.samples:
        lines.append("samples = [")
        for sample in family.samples:
            fields = [
                f'source = "{_escape(sample.source)}"',
                f'calibrated_at_utc = "{_escape(sample.calibrated_at_utc)}"',
                f"offset_seconds = {sample.offset_seconds}",
                f"error_seconds = {sample.error_seconds}",
            ]
            if sample.ticket is not None:
                fields.append(f"ticket = {sample.ticket}")
            if sample.symbol is not None:
                fields.append(f'symbol = "{_escape(sample.symbol)}"')
            lines.append(f"  {{ {', '.join(fields)} }},")
        lines.append("]")


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def user_timezone(value: str) -> str:
    try:
        ZoneInfo(value)
    except (ValueError, ZoneInfoNotFoundError) as error:
        raise ValueError(f"Unknown IANA timezone: {value!r}.") from error
    return value
