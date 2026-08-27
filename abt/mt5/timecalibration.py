from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from numbers import Real
from statistics import median
from time import sleep as _sleep
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from .config import CalibrationSample, Config, Context, TimeCalibration, TimeCalibrationFamily


MARKET_DATA = "market_data"
TRADE_RECORDS = "trade_records"
_MAX_SAMPLES = 20
_FULL_MARKET_SAMPLE_COUNT = 3
_MARKET_SAMPLE_INTERVAL_SECONDS = 1


def prepare_market_data(
    api: object,
    config: Config,
    context: Context,
    symbol: str,
    user_timezone: ZoneInfo,
) -> tuple[Context, TimeCalibrationFamily]:
    """Refresh or verify market-data calibration without blocking a read on failure."""
    family = context.time_calibration.market_data
    now = _utc_now()
    if not _is_current(family, user_timezone, now):
        samples = _market_samples(api, symbol)
        if samples:
            family = _calibrated_market_family(family, samples, symbol, user_timezone)
            context = _with_family(context, MARKET_DATA, family)
            _save_context(config, context)
        return context, family

    probe_symbol = family.calibration_symbol or symbol
    probe = _market_sample(api, probe_symbol)
    if probe is None:
        return context, family
    if probe.offset_seconds != family.offset_seconds:
        samples = _market_samples(api, probe_symbol)
        if samples:
            family = _calibrated_market_family(family, samples, probe_symbol, user_timezone)
            context = _with_family(context, MARKET_DATA, family)
            _save_context(config, context)
        return context, family

    family = replace(family, samples=_append_samples(family.samples, (probe,)))
    context = _with_family(context, MARKET_DATA, family)
    _save_context(config, context)
    return context, family


def record_successful_write(
    api: object,
    config: Config,
    context: Context,
    request: Mapping[str, Any],
    sent: object,
    sent_before_utc: datetime,
    sent_after_utc: datetime,
    user_timezone: ZoneInfo,
) -> tuple[Context, TimeCalibrationFamily]:
    """Persist a trade-record offset only when a record can be verified by ticket."""
    record = _find_trade_record(api, request, sent)
    family = context.time_calibration.trade_records
    if record is None:
        return context, family
    ticket, source, epoch = record
    midpoint = sent_before_utc + (sent_after_utc - sent_before_utc) / 2
    difference = epoch - midpoint.timestamp()
    offset = int(round(difference))
    error = abs(difference - offset) + (sent_after_utc - sent_before_utc).total_seconds() / 2
    calibrated_at = _utc_iso(sent_after_utc)
    sample = CalibrationSample(
        source=source,
        calibrated_at_utc=calibrated_at,
        offset_seconds=offset,
        error_seconds=round(error, 6),
        ticket=ticket,
    )
    family = TimeCalibrationFamily(
        offset_seconds=offset,
        calibrated_local_date=_local_date(sent_after_utc, user_timezone),
        calibrated_at_utc=calibrated_at,
        samples=_append_samples(family.samples, (sample,)),
        status="calibrated",
    )
    context = _with_family(context, TRADE_RECORDS, family)
    _save_context(config, context)
    return context, family


def render_calibration(
    family: TimeCalibrationFamily,
    family_name: str,
    user_timezone: ZoneInfo,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or _utc_now()
    current = _is_current(family, user_timezone, now)
    has_offset = family.offset_seconds is not None
    if family_name == TRADE_RECORDS:
        layer = (
            "trade_record_calibration"
            if current and has_offset
            else "expired_trade_record_calibration"
            if has_offset
            else "utc_fallback"
        )
    else:
        layer = (
            "market_data_calibration"
            if current and has_offset
            else "latest_market_data_calibration"
            if has_offset
            else "utc_fallback"
        )
    status = "calibrated" if current and has_offset else "expired" if has_offset else "unavailable"
    return {
        "family": family_name,
        "status": status,
        "offset_seconds": family.offset_seconds,
        "offset_layer": layer,
        "calibrated_local_date": family.calibrated_local_date,
        "calibrated_at_utc": family.calibrated_at_utc,
        "calibration_symbol": family.calibration_symbol,
        "sample_count": len(family.samples),
    }


def time_status(context: Context, user_timezone: ZoneInfo) -> dict[str, Any]:
    now = _utc_now()
    market = render_calibration(context.time_calibration.market_data, MARKET_DATA, user_timezone, now=now)
    trade = render_calibration(context.time_calibration.trade_records, TRADE_RECORDS, user_timezone, now=now)
    return {
        "context": context.name,
        "user_timezone": user_timezone.key,
        "now_local_date": _local_date(now, user_timezone),
        "families": [market, trade],
        "fallback_priority": {
            "market_data": ["current market-data calibration", "latest market-data calibration", "UTC"],
            "trade_records": ["current trade-record calibration", "expired trade-record calibration", "UTC"],
        },
    }


def _market_samples(api: object, symbol: str) -> tuple[CalibrationSample, ...]:
    samples: list[CalibrationSample] = []
    previous_epoch: int | None = None
    for index in range(_FULL_MARKET_SAMPLE_COUNT):
        sample = _market_sample(api, symbol)
        if sample is not None:
            epoch = int(round(_parse_utc(sample.calibrated_at_utc).timestamp())) + sample.offset_seconds
            if previous_epoch is None or epoch > previous_epoch:
                samples.append(sample)
                previous_epoch = epoch
        if index < _FULL_MARKET_SAMPLE_COUNT - 1:
            _sleep(_MARKET_SAMPLE_INTERVAL_SECONDS)
    return tuple(samples) if len(samples) == _FULL_MARKET_SAMPLE_COUNT else ()


def _market_sample(api: object, symbol: str) -> CalibrationSample | None:
    before = _utc_now()
    getter = getattr(api, "symbol_info_tick", None)
    if not callable(getter):
        return None
    tick = getter(symbol)
    after = _utc_now()
    epoch_milliseconds = _field(tick, "time_msc")
    if _valid_epoch(epoch_milliseconds):
        epoch = float(epoch_milliseconds) / 1000
        source = "symbol_info_tick.time_msc"
    else:
        epoch = _field(tick, "time")
        source = "symbol_info_tick.time"
    if not _valid_epoch(epoch):
        return None
    midpoint = before + (after - before) / 2
    difference = float(epoch) - midpoint.timestamp()
    offset = int(round(difference))
    error = abs(difference - offset) + (after - before).total_seconds() / 2
    return CalibrationSample(
        source=source,
        calibrated_at_utc=_utc_iso(after),
        offset_seconds=offset,
        error_seconds=round(error, 6),
        symbol=symbol,
    )


def _calibrated_market_family(
    existing: TimeCalibrationFamily,
    samples: tuple[CalibrationSample, ...],
    symbol: str,
    user_timezone: ZoneInfo,
) -> TimeCalibrationFamily:
    selected = samples[-1]
    offset = int(round(median(sample.offset_seconds for sample in samples)))
    return TimeCalibrationFamily(
        offset_seconds=offset,
        calibrated_local_date=_local_date(_parse_utc(selected.calibrated_at_utc), user_timezone),
        calibrated_at_utc=selected.calibrated_at_utc,
        samples=_append_samples(existing.samples, samples),
        status="calibrated",
        calibration_symbol=symbol,
    )


def _find_trade_record(api: object, request: Mapping[str, Any], sent: object) -> tuple[int, str, float] | None:
    candidates = _candidate_tickets(request, sent)
    for ticket in candidates:
        for source, method_name in (
            ("order", "orders_get"),
            ("order", "history_orders_get"),
            ("deal", "history_deals_get"),
            ("position", "positions_get"),
        ):
            record = _record_by_ticket(api, method_name, ticket)
            epoch = _record_epoch(record, source)
            if epoch is not None:
                return ticket, source, epoch
    return None


def _candidate_tickets(request: Mapping[str, Any], sent: object) -> tuple[int, ...]:
    existing = {
        value
        for value in (request.get("order"), request.get("position"), request.get("position_by"))
        if isinstance(value, int) and not isinstance(value, bool) and value > 0
    }
    values = (
        _field(sent, "order"),
        _field(sent, "deal"),
        _field(sent, "position"),
    )
    seen: set[int] = set()
    tickets: list[int] = []
    for value in values:
        if (
            isinstance(value, int)
            and not isinstance(value, bool)
            and value > 0
            and value not in existing
            and value not in seen
        ):
            seen.add(value)
            tickets.append(value)
    return tuple(tickets)


def _record_by_ticket(api: object, method_name: str, ticket: int) -> object | None:
    getter = getattr(api, method_name, None)
    if not callable(getter):
        return None
    try:
        records = getter(ticket=ticket)
    except TypeError:
        if not method_name.startswith("history_"):
            return None
        now = _utc_now()
        try:
            records = getter(now - timedelta(days=1), now + timedelta(seconds=1), ticket=ticket)
        except (TypeError, ValueError):
            return None
    except ValueError:
        return None
    if records is None and method_name.startswith("history_"):
        now = _utc_now()
        try:
            records = getter(now - timedelta(days=1), now + timedelta(seconds=1), ticket=ticket)
        except (TypeError, ValueError):
            return None
    if records is None:
        return None
    try:
        return next((record for record in records if _field(record, "ticket") == ticket), None)
    except TypeError:
        return None


def _record_epoch(record: object | None, source: str) -> float | None:
    names = (
        ("time_update", "time", "time_setup", "time_done")
        if source == "position"
        else ("time_done", "time", "time_update", "time_setup")
    )
    for name in names:
        value = _field(record, name)
        if _valid_epoch(value):
            return float(value)
    msc_names = (
        ("time_update_msc", "time_msc", "time_setup_msc", "time_done_msc")
        if source == "position"
        else ("time_done_msc", "time_msc", "time_update_msc", "time_setup_msc")
    )
    for name in msc_names:
        value = _field(record, name)
        if _valid_epoch(value):
            return float(value) / 1000
    return None


def _with_family(context: Context, family_name: str, family: TimeCalibrationFamily) -> Context:
    calibration = (
        replace(context.time_calibration, market_data=family)
        if family_name == MARKET_DATA
        else replace(context.time_calibration, trade_records=family)
    )
    return replace(context, time_calibration=calibration)


def _save_context(config: Config, context: Context) -> None:
    from .config import save

    save(replace(config, contexts={**config.contexts, context.name: context}, version=2))


def _append_samples(
    existing: tuple[CalibrationSample, ...], added: tuple[CalibrationSample, ...]
) -> tuple[CalibrationSample, ...]:
    return (*existing, *added)[-_MAX_SAMPLES:]


def _is_current(family: TimeCalibrationFamily, user_timezone: ZoneInfo, now: datetime) -> bool:
    return (
        family.status == "calibrated"
        and family.offset_seconds is not None
        and family.calibrated_local_date == _local_date(now, user_timezone)
    )


def _local_date(value: datetime, user_timezone: ZoneInfo) -> str:
    return value.astimezone(user_timezone).date().isoformat()


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _utc_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _valid_epoch(value: object) -> bool:
    return isinstance(value, Real) and not isinstance(value, bool) and value > 0


def _field(value: object, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _utc_now() -> datetime:
    return datetime.now(UTC)
