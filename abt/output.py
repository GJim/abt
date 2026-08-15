from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import UTC, date, datetime
from enum import Enum
from math import isfinite
from typing import Any
from zoneinfo import ZoneInfo


def normalize(value: Any) -> Any:
    if hasattr(value, "_asdict"):
        return {key: normalize(item) for key, item in value._asdict().items()}
    if isinstance(value, Mapping):
        return {str(key): normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


def render(
    value: Any,
    output: str,
    *,
    user_timezone: ZoneInfo | None = None,
    source_family: str | None = None,
    calibration: Mapping[str, Any] | object | None = None,
    field_calibrations: Mapping[str, Mapping[str, Any] | object] | None = None,
    broker_offset_seconds: int | None = None,
) -> str:
    normalized = normalize(value)
    calibration = _coerce_calibration(calibration, source_family or "utc", user_timezone)
    field_calibrations = _coerce_field_calibrations(field_calibrations, user_timezone)
    offset = _offset_seconds(calibration, broker_offset_seconds)
    metadata = (
        _time_metadata(user_timezone, source_family or "utc", calibration, offset, field_calibrations)
        if user_timezone is not None
        else None
    )
    if output == "json":
        normalized = _add_utc_epoch_fields(normalized, offset, field_calibrations)
        if metadata is not None:
            normalized = _add_time_metadata(normalized, metadata)
        return json.dumps(normalized, ensure_ascii=False, indent=2, default=str)
    if user_timezone is not None:
        normalized = _format_epoch_fields(
            normalized,
            user_timezone,
            offset_seconds=offset,
            field_calibrations=field_calibrations,
        )
    if metadata is not None:
        normalized = _add_time_metadata(normalized, metadata)
    return render_table(normalized)


def render_table(value: Any) -> str:
    if isinstance(value, Mapping):
        nested_lists = [(key, item) for key, item in value.items() if isinstance(item, list)]
        scalar_items = {key: item for key, item in value.items() if not isinstance(item, list)}
        if len(nested_lists) == 1:
            name, records = nested_lists[0]
            metadata = render_table(scalar_items) if scalar_items else ""
            rendered_records = render_table(records)
            return "\n\n".join(part for part in (metadata, name, rendered_records) if part)
        return _table([{"field": key, "value": _scalar(item)} for key, item in value.items()])
    if isinstance(value, list):
        if not value:
            return "(no results)"
        if all(isinstance(item, Mapping) for item in value):
            columns = list(dict.fromkeys(key for item in value for key in item))
            return _table([{column: _scalar(item.get(column, "")) for column in columns} for item in value], columns)
        return _table([{"value": _scalar(item)} for item in value])
    return _scalar(value)


def _scalar(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, default=str)
    if value is None:
        return ""
    return str(value)


def _add_utc_epoch_fields(
    value: Any,
    offset_seconds: int = 0,
    field_calibrations: Mapping[str, Mapping[str, Any]] | None = None,
) -> Any:
    if isinstance(value, Mapping):
        normalized = {
            str(key): _add_utc_epoch_fields(item, offset_seconds, field_calibrations)
            for key, item in value.items()
        }
        for key, item in list(normalized.items()):
            epoch = _epoch_value(key, item)
            utc_key = f"{key}_utc"
            if epoch is not None and utc_key not in normalized:
                normalized[utc_key] = _utc_iso(epoch - _field_offset(key, offset_seconds, field_calibrations))
        return normalized
    if isinstance(value, list):
        return [_add_utc_epoch_fields(item, offset_seconds, field_calibrations) for item in value]
    return value


def _add_time_metadata(value: Any, metadata: Mapping[str, Any]) -> Any:
    if isinstance(value, Mapping):
        result = dict(value)
        result.setdefault("time_metadata", dict(metadata))
        return result
    return {"records": value, "time_metadata": dict(metadata)}


def _format_epoch_fields(
    value: Any,
    user_timezone: ZoneInfo,
    key: str | None = None,
    offset_seconds: int = 0,
    field_calibrations: Mapping[str, Mapping[str, Any]] | None = None,
) -> Any:
    epoch = _epoch_value(key, value) if key is not None else None
    if epoch is not None:
        offset = _field_offset(key, offset_seconds, field_calibrations)
        return datetime.fromtimestamp(epoch - offset, UTC).astimezone(user_timezone).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, Mapping):
        return {
            str(item_key): _format_epoch_fields(
                item,
                user_timezone,
                key=str(item_key),
                offset_seconds=offset_seconds,
                field_calibrations=field_calibrations,
            )
            for item_key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _format_epoch_fields(
                item,
                user_timezone,
                offset_seconds=offset_seconds,
                field_calibrations=field_calibrations,
            )
            for item in value
        ]
    return value


def _offset_seconds(calibration: Mapping[str, Any] | None, broker_offset_seconds: int | None) -> int:
    if broker_offset_seconds is not None:
        return broker_offset_seconds
    if calibration is None:
        return 0
    offset = calibration.get("offset_seconds")
    return offset if isinstance(offset, int) and not isinstance(offset, bool) else 0


def _time_metadata(
    user_timezone: ZoneInfo,
    source_family: str,
    calibration: Mapping[str, Any] | None,
    offset_seconds: int,
    field_calibrations: Mapping[str, Mapping[str, Any]] | None,
) -> dict[str, Any]:
    family = calibration.get("family", source_family) if calibration is not None else source_family
    status = calibration.get("status", "utc") if calibration is not None else "utc"
    layer = calibration.get("offset_layer", "utc") if calibration is not None else "utc"
    metadata: dict[str, Any] = {
        "source_family": family,
        "raw_epoch_clock": (
            "broker market-data clock"
            if family == "market_data"
            else "broker trade-record clock"
            if family == "trade_records"
            else "UTC"
        ),
        "offset_layer": layer,
        "offset_seconds_used": offset_seconds,
        "calibration_status": status,
        "epoch_utc_semantics": "raw MT5 epoch minus offset_seconds_used",
        "user_timezone": user_timezone.key,
        "rendered_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    if calibration is not None:
        metadata["calibration"] = dict(calibration)
    if field_calibrations:
        metadata["field_source_overrides"] = {
            field: {
                "source_family": value.get("family"),
                "offset_layer": value.get("offset_layer"),
                "offset_seconds": value.get("offset_seconds"),
            }
            for field, value in field_calibrations.items()
        }
    return metadata


def _coerce_calibration(
    calibration: Mapping[str, Any] | object | None,
    source_family: str,
    user_timezone: ZoneInfo | None,
) -> Mapping[str, Any] | None:
    if calibration is None:
        return None
    values = dict(calibration) if isinstance(calibration, Mapping) else asdict(calibration) if is_dataclass(calibration) else None
    if values is None:
        raise TypeError("calibration must be a mapping or dataclass.")
    if "offset_layer" in values:
        return values
    offset = values.get("offset_seconds")
    has_offset = isinstance(offset, int) and not isinstance(offset, bool)
    current = (
        has_offset
        and values.get("status") == "calibrated"
        and user_timezone is not None
        and values.get("calibrated_local_date") == datetime.now(UTC).astimezone(user_timezone).date().isoformat()
    )
    trade_records = source_family == "trade_records"
    values["family"] = source_family
    values["status"] = "calibrated" if current else "expired" if has_offset else "unavailable"
    values["offset_layer"] = (
        "trade_record_calibration"
        if trade_records and current
        else "expired_trade_record_calibration"
        if trade_records and has_offset
        else "market_data_calibration"
        if source_family == "market_data" and current
        else "latest_market_data_calibration"
        if source_family == "market_data" and has_offset
        else "utc_fallback"
    )
    return values


def _coerce_field_calibrations(
    field_calibrations: Mapping[str, Mapping[str, Any] | object] | None,
    user_timezone: ZoneInfo | None,
) -> Mapping[str, Mapping[str, Any]] | None:
    if field_calibrations is None:
        return None
    result: dict[str, Mapping[str, Any]] = {}
    for field, calibration in field_calibrations.items():
        if not isinstance(field, str):
            raise TypeError("field calibration names must be strings.")
        converted = _coerce_calibration(calibration, "market_data", user_timezone)
        if converted is not None:
            result[field] = converted
    return result


def _field_offset(
    field: str | None,
    default: int,
    field_calibrations: Mapping[str, Mapping[str, Any]] | None,
) -> int:
    if field is None or field_calibrations is None:
        return default
    calibration = field_calibrations.get(field)
    return _offset_seconds(calibration, None) if calibration is not None else default


def _epoch_value(name: str | None, value: Any) -> float | None:
    if name is None or isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value) or value <= 0:
        return None
    if name == "time_msc" or name.endswith("_time_msc") or (name.startswith("time_") and name.endswith("_msc")):
        return value / 1000
    if (
        name == "time"
        or name.endswith("_time")
        or name.startswith("time_")
        or name.endswith("_expiration")
        or name == "expiration"
    ):
        if name == "type_time":
            return None
        return value
    return None


def _utc_iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat().replace("+00:00", "Z")


def _table(rows: list[dict[str, str]], columns: list[str] | None = None) -> str:
    columns = columns or list(rows[0])
    widths = {column: max(len(column), *(len(row.get(column, "")) for row in rows)) for column in columns}
    header = " | ".join(column.ljust(widths[column]) for column in columns)
    separator = "-+-".join("-" * widths[column] for column in columns)
    body = [" | ".join(row.get(column, "").ljust(widths[column]) for column in columns) for row in rows]
    return "\n".join([header, separator, *body])
