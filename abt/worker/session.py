from __future__ import annotations

from collections import deque
from datetime import timedelta
import json
import logging
from math import floor
from statistics import median
from collections.abc import Callable
from datetime import UTC, datetime
from dataclasses import dataclass, field
from typing import Protocol, Self
from zoneinfo import ZoneInfo

from websockets.exceptions import ConnectionClosed

from ..mt5.config import TimeCalibrationFamily
from ..mt5.output import render
from ..mt5.timecalibration import MARKET_DATA, render_calibration
from .credentials import (
    WebSocketConnector,
    WorkerWebSocket,
    _message,
    _required_text,
    _send,
    _send_proof,
    _worker_endpoint,
)
from .enrollment import WorkerEnrollmentError, WorkerSessionDisconnected


_LOGGER = logging.getLogger(__name__)
from .keystore import HardwareKeyStore


class ProductCatalogReadOnlyMT5(Protocol):
    def symbols_get(self) -> object: ...


class MarketDataReadOnlyMT5(Protocol):
    def copy_rates_range(self, symbol: str, timeframe: object, from_time: datetime, to_time: datetime) -> object: ...

    def symbol_info_tick(self, symbol: str) -> object: ...


@dataclass
class AuthenticatedWorkerSession:
    """A proved WSS channel for one approved 帳戶工作者."""

    socket: WorkerWebSocket
    reconciliation_cursor: int
    worker_id: str = ""
    certificate: str = ""
    _analysis_requests: deque[dict[str, object]] = field(default_factory=deque, init=False, repr=False)
    _order_check_requests: deque[dict[str, object]] = field(default_factory=deque, init=False, repr=False)
    _order_execute_requests: deque[dict[str, object]] = field(default_factory=deque, init=False, repr=False)
    _execution_recovery_requests: deque[dict[str, object]] = field(default_factory=deque, init=False, repr=False)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        try:
            self.socket.__exit__(exc_type, exc_value, traceback)
        except Exception as error:
            _raise_closed_connection(error, "authenticated worker session")

    def request_password(self) -> str:
        try:
            _send(self.socket, {"type": "password_request"})
            response = self._response()
            if response.get("type") != "password":
                raise WorkerEnrollmentError("The controller returned an invalid worker response.")
            return _required_text(response, "password")
        except Exception as error:
            _raise_closed_connection(error, "password request")

    def send_reconciliation(self, message: dict[str, object]) -> None:
        try:
            _send(self.socket, message)
            response = self._response()
            if response.get("type") != "accepted" or response.get("cursor") != message.get("cursor"):
                raise WorkerEnrollmentError("The controller rejected worker reconciliation.")
        except Exception as error:
            _raise_closed_connection(error, "reconciliation")

    def heartbeat(self) -> bool:
        try:
            _send(self.socket, {"type": "heartbeat"})
            response = self._response()
            return response == {"type": "heartbeat_ack"}
        except Exception as error:
            _raise_closed_connection(error, "heartbeat")

    def send_safety_state(self, state: str, reason: str) -> None:
        try:
            _send(self.socket, {"type": "safety_state", "state": state, "reason": reason})
            response = self._response()
            if response != {"type": "accepted", "state": state}:
                raise WorkerEnrollmentError("The controller rejected the worker safety state.")
        except Exception as error:
            _raise_closed_connection(error, "safety-state update")

    def receive_product_catalog_analysis(self, timeout: float | None = None) -> dict[str, object] | None:
        if self._analysis_requests:
            return self._parse_product_catalog_analysis(self._analysis_requests.popleft())
        while True:
            try:
                response = _message(self.socket, timeout=timeout)
            except TimeoutError:
                return None
            except Exception as error:
                _raise_closed_connection(error, "analysis request")
            if response.get("type") == "order_check_request":
                self._order_check_requests.append(response)
                continue
            if response.get("type") == "order_execute_request":
                self._order_execute_requests.append(response)
                continue
            if response.get("type", "").startswith("execution_"):
                self._execution_recovery_requests.append(response)
                continue
            return self._parse_product_catalog_analysis(response)

    def receive_order_check(self, timeout: float | None = None) -> dict[str, object] | None:
        if self._order_check_requests:
            return self._parse_order_check(self._order_check_requests.popleft())
        try:
            response = _message(self.socket, timeout=timeout)
        except TimeoutError:
            return None
        except Exception as error:
            _raise_closed_connection(error, "order-check request")
        if response.get("type") != "order_check_request":
            if response.get("type") == "order_execute_request":
                self._order_execute_requests.append(response)
                return None
            if response.get("type", "").startswith("execution_"):
                self._execution_recovery_requests.append(response)
                return None
            self._analysis_requests.append(response)
            return None
        return self._parse_order_check(response)

    def _response(self) -> dict[str, object]:
        while True:
            response = _message(self.socket)
            if response.get("type") == "product_catalog_analysis_request":
                self._analysis_requests.append(response)
                continue
            if response.get("type") == "order_check_request":
                self._order_check_requests.append(response)
                continue
            if response.get("type") == "order_execute_request":
                self._order_execute_requests.append(response)
                continue
            if response.get("type", "").startswith("execution_"):
                self._execution_recovery_requests.append(response)
                continue
            return response

    def _parse_product_catalog_analysis(self, response: dict[str, object]) -> dict[str, object]:
        if response.get("type") != "product_catalog_analysis_request":
            raise WorkerEnrollmentError("The controller returned an invalid worker response.")
        stage = response.get("stage", "catalog")
        if stage not in {"catalog", "m15_screening", "m1_verification"}:
            raise WorkerEnrollmentError("The controller returned an invalid worker response.")
        result = {
            "analysis_id": _required_text(response, "analysis_id"),
            "request_id": _required_text(response, "request_id"),
            "stage": stage,
            "policy": response.get("policy") if isinstance(response.get("policy"), dict) else {},
        }
        if stage in {"m15_screening", "m1_verification"}:
            raw_symbols = response.get("symbols")
            result.update(
                {
                    "timeframe": _required_text(response, "timeframe"),
                    "period_start_utc": _required_text(response, "period_start_utc"),
                    "period_end_utc": _required_text(response, "period_end_utc"),
                    "symbols": [symbol for symbol in raw_symbols] if isinstance(raw_symbols, list) else [],
                }
            )
        return result

    def _parse_order_check(self, response: dict[str, object]) -> dict[str, object]:
        if set(response) != {"type", "analysis_id", "request_id", "order"} or response.get("type") != "order_check_request":
            raise WorkerEnrollmentError("The controller returned an invalid order-check request.")
        if response.get("analysis_id") != "order_check":
            raise WorkerEnrollmentError("The controller returned an invalid order-check request.")
        return {"request_id": _required_text(response, "request_id"), "order": response["order"]}

    def send_order_check(
        self, *, request_id: str, order: dict[str, object], accepted: bool, diagnostics: dict[str, object]
    ) -> None:
        try:
            _send(
                self.socket,
                {
                    "type": "order_check_response",
                    "analysis_id": "order_check",
                    "request_id": request_id,
                    "accepted": accepted,
                    "order": order,
                    "diagnostics": diagnostics,
                },
            )
        except Exception as error:
            _raise_closed_connection(error, "order-check response")

    def send_order_check_error(self, *, request_id: str, reason: str) -> None:
        try:
            _send(
                self.socket,
                {"type": "order_check_error", "analysis_id": "order_check", "request_id": request_id, "reason": reason},
            )
        except Exception as error:
            _raise_closed_connection(error, "order-check error response")

    def receive_order_execute(self, timeout: float | None = None) -> dict[str, object] | None:
        if self._order_execute_requests:
            return self._parse_order_execute(self._order_execute_requests.popleft())
        try:
            response = _message(self.socket, timeout=timeout)
        except TimeoutError:
            return None
        except Exception as error:
            _raise_closed_connection(error, "order execution request")
        if response.get("type") != "order_execute_request":
            if response.get("type") == "order_check_request":
                self._order_check_requests.append(response)
            elif response.get("type", "").startswith("execution_"):
                self._execution_recovery_requests.append(response)
            else:
                self._analysis_requests.append(response)
            return None
        return self._parse_order_execute(response)

    def _parse_order_execute(self, response: dict[str, object]) -> dict[str, object]:
        if set(response) != {"type", "request_id", "order"} or response.get("type") != "order_execute_request":
            raise WorkerEnrollmentError("The controller returned an invalid order execution request.")
        order = response.get("order")
        if not isinstance(order, dict):
            raise WorkerEnrollmentError("The controller returned an invalid order execution request.")
        return {"request_id": _required_text(response, "request_id"), "order": order}

    def send_order_execute(
        self, *, request_id: str, order: dict[str, object], accepted: bool, result: dict[str, object]
    ) -> None:
        try:
            _send(
                self.socket,
                {
                    "type": "order_execute_response",
                    "request_id": request_id,
                    "accepted": accepted,
                    "order": order,
                    "result": result,
                },
            )
        except Exception as error:
            _raise_closed_connection(error, "order execution response")

    def send_order_execute_error(self, *, request_id: str, reason: str) -> None:
        try:
            _send(self.socket, {"type": "order_execute_error", "request_id": request_id, "reason": reason})
        except Exception as error:
            _raise_closed_connection(error, "order execution error response")

    def receive_execution_recovery(self, timeout: float | None = None) -> dict[str, object] | None:
        if self._execution_recovery_requests:
            return self._parse_execution_recovery(self._execution_recovery_requests.popleft())
        try:
            response = _message(self.socket, timeout=timeout)
        except TimeoutError:
            return None
        except Exception as error:
            _raise_closed_connection(error, "execution recovery request")
        if response.get("type", "").startswith("execution_"):
            return self._parse_execution_recovery(response)
        if response.get("type") == "order_check_request":
            self._order_check_requests.append(response)
        elif response.get("type") == "order_execute_request":
            self._order_execute_requests.append(response)
        else:
            self._analysis_requests.append(response)
        return None

    def _parse_execution_recovery(self, response: dict[str, object]) -> dict[str, object]:
        request_type = response.get("type")
        if request_type == "execution_reconcile_request" and set(response) == {"type", "request_id", "execution_id"}:
            return {"type": request_type, "request_id": _required_text(response, "request_id"),
                    "execution_id": _required_text(response, "execution_id")}
        if request_type in {"execution_cancel_request", "execution_close_request"} and set(response) == {
            "type", "request_id", "ticket", "volume"
        }:
            return {"type": request_type, "request_id": _required_text(response, "request_id"),
                    "ticket": _required_text(response, "ticket"), "volume": _required_text(response, "volume")}
        raise WorkerEnrollmentError("The controller returned an invalid execution recovery request.")

    def send_execution_recovery(self, *, request_id: str, operation: str, accepted: bool,
                                result: dict[str, object]) -> None:
        try:
            _send(self.socket, {"type": "execution_recovery_response", "request_id": request_id,
                                "operation": operation, "accepted": accepted, "result": result})
        except Exception as error:
            _raise_closed_connection(error, "execution recovery response")

    def send_execution_recovery_error(self, *, request_id: str, operation: str, reason: str) -> None:
        try:
            _send(self.socket, {"type": "execution_recovery_error", "request_id": request_id,
                                "operation": operation, "reason": reason})
        except Exception as error:
            _raise_closed_connection(error, "execution recovery error response")

    def send_product_catalog_analysis(
        self,
        *,
        analysis_id: str,
        request_id: str,
        collected_at: str,
        symbols: list[dict[str, object]],
        stage: str = "catalog",
        timeframe: str | None = None,
        period_start_utc: str | None = None,
        period_end_utc: str | None = None,
    ) -> None:
        response = {
            "type": "product_catalog_analysis_response",
            "stage": stage,
            "analysis_id": analysis_id,
            "request_id": request_id,
            "collected_at": collected_at,
            "symbols": symbols,
        }
        if stage in {"m15_screening", "m1_verification"}:
            if not all(isinstance(value, str) and value for value in (timeframe, period_start_utc, period_end_utc)):
                raise WorkerEnrollmentError("Market-data analysis responses must include timeframe and UTC period.")
            response.update(
                {
                    "timeframe": timeframe,
                    "period_start_utc": period_start_utc,
                    "period_end_utc": period_end_utc,
                }
            )
        try:
            _send(self.socket, response)
        except Exception as error:
            _raise_closed_connection(error, "analysis response")

    def send_product_catalog_analysis_error(
        self,
        *,
        analysis_id: str,
        request_id: str,
        stage: str,
        reason: str,
        timeframe: str | None = None,
    ) -> None:
        if stage not in {"catalog", "m15_screening", "m1_verification"} or not reason:
            raise WorkerEnrollmentError("The worker cannot send an invalid product catalog analysis error.")
        response = {
            "type": "product_catalog_analysis_error",
            "analysis_id": analysis_id,
            "request_id": request_id,
            "stage": stage,
            "reason": reason,
        }
        if stage in {"m15_screening", "m1_verification"}:
            if not isinstance(timeframe, str) or not timeframe:
                raise WorkerEnrollmentError("Market-data analysis errors must include a timeframe.")
            response["timeframe"] = timeframe
        try:
            _send(self.socket, response)
        except Exception as error:
            _raise_closed_connection(error, "analysis error response")


def collect_product_catalog_evidence(
    mt5: ProductCatalogReadOnlyMT5,
    *,
    collected_at: datetime | None = None,
) -> dict[str, object]:
    when = (datetime.now(UTC) if collected_at is None else collected_at).isoformat()
    raw_symbols = mt5.symbols_get()
    if not isinstance(raw_symbols, (list, tuple)):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    return {"collected_at": when, "symbols": [_symbol_specification(symbol) for symbol in raw_symbols]}


def collect_market_data_evidence(
    mt5: MarketDataReadOnlyMT5,
    *,
    symbols: list[str],
    timeframe: str,
    period_start_utc: str,
    period_end_utc: str,
    collected_at: datetime | None = None,
) -> dict[str, object]:
    when = (datetime.now(UTC) if collected_at is None else collected_at).isoformat()
    start = _parse_utc(period_start_utc)
    end = _parse_utc(period_end_utc)
    if start >= end:
        raise WorkerEnrollmentError("The controller requested an invalid market-data interval.")
    if not symbols or not all(isinstance(symbol, str) and symbol for symbol in symbols):
        raise WorkerEnrollmentError("The controller requested invalid market-data symbols.")
    calibration = _market_data_calibration(mt5, symbols)
    return {
        "collected_at": when,
        "timeframe": timeframe,
        "period_start_utc": period_start_utc,
        "period_end_utc": period_end_utc,
        "symbols": [
            _market_data_symbol_evidence_with_retry(mt5, symbol, timeframe, start, end, calibration)
            for symbol in symbols
        ],
    }


def _market_data_symbol_evidence_with_retry(
    mt5: MarketDataReadOnlyMT5,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    calibration: dict[str, object],
) -> dict[str, object]:
    last_error: WorkerEnrollmentError | None = None
    for attempt in range(1, 4):
        try:
            return _market_data_symbol_evidence(mt5, symbol, timeframe, start, end, calibration)
        except WorkerEnrollmentError as error:
            last_error = error
            _LOGGER.debug(
                "Market-data evidence failed for symbol %s, timeframe %s, attempt %s/3: %s",
                symbol,
                timeframe,
                attempt,
                error,
            )
    assert last_error is not None
    raise WorkerEnrollmentError(
        f"Unable to collect {timeframe} market-data evidence for {symbol} after 3 attempts: {last_error}"
    ) from last_error


def _symbol_specification(symbol: object) -> dict[str, object]:
    source = _symbol_source(symbol)
    name = _symbol_field(source, "symbol", "name")
    trade_calc_mode = _symbol_field(source, "trade_calc_mode")
    currency_base = _symbol_field(source, "currency_base")
    currency_profit = _symbol_field(source, "currency_profit")
    digits = _symbol_field(source, "digits")
    point = _symbol_field(source, "point")
    if not isinstance(name, str) or not name:
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    if isinstance(trade_calc_mode, bool) or not isinstance(trade_calc_mode, (int, str)):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    if isinstance(trade_calc_mode, str) and not trade_calc_mode:
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    if not isinstance(currency_base, str) or not currency_base:
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    if not isinstance(currency_profit, str) or not currency_profit:
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    if not isinstance(digits, int) or isinstance(digits, bool):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    if not isinstance(point, (int, float)) or isinstance(point, bool):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    return {
        "symbol": name,
        "trade_calc_mode": trade_calc_mode,
        "currency_base": currency_base,
        "currency_profit": currency_profit,
        "digits": digits,
        "point": point,
        "trade_tick_size": _required_symbol_float(source, "trade_tick_size"),
        "contract_size": _required_symbol_float(source, "contract_size", "trade_contract_size"),
        "volume_min": _required_symbol_float(source, "volume_min"),
        "volume_step": _required_symbol_float(source, "volume_step"),
        "filling_modes": _symbol_filling_modes(source),
        "allowed_directions": _symbol_allowed_directions(source),
        "volume_max": _required_symbol_float(source, "volume_max"),
        "trade_stops_level": _required_symbol_int(source, "trade_stops_level"),
        "trade_freeze_level": _required_symbol_int(source, "trade_freeze_level"),
        "trade_tick_value": _required_symbol_float(source, "trade_tick_value"),
        "currency_margin": _required_symbol_text(source, "currency_margin"),
        "swap_long": _required_symbol_float(source, "swap_long"),
        "swap_short": _required_symbol_float(source, "swap_short"),
        "swap_mode": _required_symbol_int(source, "swap_mode"),
        "swap_rollover3days": _required_symbol_int(source, "swap_rollover3days"),
    }


def _symbol_source(symbol: object) -> dict[str, object]:
    if isinstance(symbol, dict):
        return symbol
    as_dict = getattr(symbol, "_asdict", None)
    if callable(as_dict):
        source = as_dict()
        if isinstance(source, dict):
            return source
    try:
        source = vars(symbol)
    except TypeError as error:
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.") from error
    if not isinstance(source, dict):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    return source


def _symbol_field(source: dict[str, object], *names: str) -> object:
    for name in names:
        if name in source:
            return source[name]
    return None


def _required_symbol_float(source: dict[str, object], *names: str) -> float:
    value = _symbol_field(source, *names)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    return float(value)


def _required_symbol_int(source: dict[str, object], *names: str) -> int:
    value = _symbol_field(source, *names)
    if not isinstance(value, int) or isinstance(value, bool):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    return value


def _required_symbol_text(source: dict[str, object], *names: str) -> str:
    value = _symbol_field(source, *names)
    if not isinstance(value, str) or not value:
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    return value


def _required_symbol_text_list(source: dict[str, object], *names: str) -> list[str]:
    value = _symbol_field(source, *names)
    if not isinstance(value, list) or not value or not all(isinstance(item, str) and item for item in value):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    return list(value)


def _symbol_filling_modes(source: dict[str, object]) -> list[str]:
    if isinstance(_symbol_field(source, "filling_modes"), list):
        return _required_symbol_text_list(source, "filling_modes")
    filling_mode = _symbol_field(source, "filling_mode")
    if isinstance(filling_mode, int) and not isinstance(filling_mode, bool):
        modes = [name for bit, name in ((1, "FOK"), (2, "IOC"), (4, "BOC")) if filling_mode & bit]
        if modes:
            return modes
        legacy_mode = {0: ["FOK"], 1: ["IOC"], 2: ["RETURN"], 3: ["BOC"]}.get(filling_mode)
        if legacy_mode is not None:
            return legacy_mode
    raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")


def _symbol_allowed_directions(source: dict[str, object]) -> list[str]:
    if isinstance(_symbol_field(source, "allowed_directions"), list):
        return _required_symbol_text_list(source, "allowed_directions")
    order_mode = _symbol_field(source, "order_mode")
    if isinstance(order_mode, int) and not isinstance(order_mode, bool):
        directions = []
        if order_mode & 1:
            directions.append("LONG")
        if order_mode & 2:
            directions.append("SHORT")
        if directions:
            return directions
    trade_mode = _symbol_field(source, "trade_mode")
    if isinstance(trade_mode, int) and not isinstance(trade_mode, bool):
        directions = {
            1: ["LONG"],
            2: ["SHORT"],
            4: ["LONG", "SHORT"],
        }.get(trade_mode)
        if directions is not None:
            return directions
    raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")


def _market_data_symbol_evidence(
    mt5: MarketDataReadOnlyMT5,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    calibration: dict[str, object],
) -> dict[str, object]:
    raw_rates = mt5.copy_rates_range(symbol, _timeframe_value(mt5, timeframe), start, end - timedelta(seconds=1))
    bars = _structured_market_data(raw_rates)
    if not bars:
        raise WorkerEnrollmentError("The local MT5 terminal returned incomplete market-data evidence.")
    rendered = json.loads(
        render(
            bars,
            "json",
            user_timezone=ZoneInfo("UTC"),
            source_family=MARKET_DATA,
            calibration=calibration,
        )
    )
    if not isinstance(rendered, dict):
        raise WorkerEnrollmentError("The local MT5 terminal returned incomplete market-data evidence.")
    records = rendered.get("records")
    time_metadata = rendered.get("time_metadata")
    if not isinstance(records, list) or not isinstance(time_metadata, dict):
        raise WorkerEnrollmentError("The local MT5 terminal returned incomplete market-data evidence.")
    return {"symbol": symbol, "bars": records, "time_metadata": time_metadata}


def _market_data_calibration(mt5: MarketDataReadOnlyMT5, symbols: list[str]) -> dict[str, object]:
    for symbol in symbols:
        samples = tuple(sample for _ in range(3) if (sample := _market_sample(mt5, symbol)) is not None)
        if not samples:
            continue
        offset = int(round(median(sample["offset_seconds"] for sample in samples)))
        selected = samples[-1]
        calibration = render_calibration(
            TimeCalibrationFamily(
                offset_seconds=offset,
                calibrated_local_date=_parse_utc(str(selected["calibrated_at_utc"])).date().isoformat(),
                calibrated_at_utc=str(selected["calibrated_at_utc"]),
                status="calibrated",
                calibration_symbol=symbol,
            ),
            MARKET_DATA,
            ZoneInfo("UTC"),
            now=_parse_utc(str(selected["calibrated_at_utc"])),
        )
        calibration["samples"] = list(samples)
        calibration["sample_count"] = len(samples)
        _LOGGER.debug("Using %s for shared market-data calibration.", symbol)
        return calibration
    raise WorkerEnrollmentError(
        "No valid symbol_info_tick.time was available across 3 calibration samples for any requested symbol."
    )


def _market_sample(mt5: MarketDataReadOnlyMT5, symbol: str) -> dict[str, object] | None:
    before = datetime.now(UTC)
    tick = mt5.symbol_info_tick(symbol)
    after = datetime.now(UTC)
    epoch = _field(tick, "time")
    if not isinstance(epoch, (int, float)) or isinstance(epoch, bool) or epoch <= 0:
        return None
    midpoint = before + (after - before) / 2
    difference = float(epoch) - midpoint.timestamp()
    offset = int(round(difference))
    error = abs(difference - offset) + (after - before).total_seconds() / 2
    return {
        "source": "symbol_info_tick.time",
        "calibrated_at_utc": after.isoformat().replace("+00:00", "Z"),
        "offset_seconds": offset,
        "error_seconds": round(error, 6),
        "symbol": symbol,
    }


def _timeframe_value(mt5: object, timeframe: str) -> object:
    value = getattr(mt5, f"TIMEFRAME_{timeframe}", None)
    return timeframe if value is None else value


def _structured_market_data(values: object) -> list[dict[str, object]]:
    if values is None:
        raise WorkerEnrollmentError("The local MT5 terminal returned incomplete market-data evidence.")
    if isinstance(values, (list, tuple)):
        return [_market_bar(value) for value in values]
    names = getattr(getattr(values, "dtype", None), "names", None)
    if names is None:
        raise WorkerEnrollmentError("The local MT5 terminal returned incomplete market-data evidence.")
    return [
        _market_bar({name: row[name].item() if hasattr(row[name], "item") else row[name] for name in names})
        for row in values
    ]


def _market_bar(value: object) -> dict[str, object]:
    source = value if isinstance(value, dict) else vars(value) if hasattr(value, "__dict__") else value
    if not isinstance(source, dict):
        raise WorkerEnrollmentError("The local MT5 terminal returned incomplete market-data evidence.")
    time = source.get("time")
    if not isinstance(time, (int, float)) or isinstance(time, bool) or time <= 0:
        raise WorkerEnrollmentError("The local MT5 terminal returned incomplete market-data evidence.")
    bar = {"time": floor(float(time))}
    for field in ("open", "high", "low", "close"):
        numeric = source.get(field)
        if not isinstance(numeric, (int, float)) or isinstance(numeric, bool):
            raise WorkerEnrollmentError("The local MT5 terminal returned incomplete market-data evidence.")
        bar[field] = float(numeric)
    return bar


def _field(value: object, name: str) -> object:
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def open_authenticated_worker_session(
    *,
    controller_url: str,
    enrollment_id: str,
    key_store: HardwareKeyStore,
    connect: WebSocketConnector | None = None,
    certificate_received: Callable[[str], None] | None = None,
) -> AuthenticatedWorkerSession:
    """Deliver the approved certificate, then prove the device key on one persistent WSS channel."""

    if connect is None:
        from websockets.sync.client import connect as websocket_connect

        connect = websocket_connect
    try:
        with connect(_worker_endpoint(controller_url, "/api/worker/certificate")) as certificate_socket:
            _send(certificate_socket, {"enrollment_id": enrollment_id})
            challenge = _message(certificate_socket)
            worker_id = _required_text(challenge, "worker_id")
            _send_proof(certificate_socket, key_store, challenge, "certificate_delivery", worker_id)
            delivery = _message(certificate_socket)
            if _required_text(delivery, "worker_id") != worker_id:
                raise WorkerEnrollmentError("The controller returned an invalid device certificate.")
            certificate = _required_text(delivery, "certificate")
            if certificate_received is not None:
                certificate_received(certificate)
    except Exception as error:
        _raise_closed_connection(error, "certificate delivery")

    socket = connect(_worker_endpoint(controller_url, "/api/worker/session"))
    try:
        socket.__enter__()
        _send(socket, {"worker_id": worker_id, "certificate": certificate})
        challenge = _message(socket)
        _send_proof(socket, key_store, challenge, "worker_session", worker_id)
        authenticated = _message(socket)
        cursor = authenticated.get("cursor") if isinstance(authenticated, dict) else None
        if (
            not isinstance(cursor, int)
            or isinstance(cursor, bool)
            or cursor < 0
            or authenticated != {"type": "authenticated", "worker_id": worker_id, "cursor": cursor}
        ):
            raise WorkerEnrollmentError("The controller returned an invalid worker response.")
    except BaseException as error:
        try:
            socket.__exit__(type(error), error, error.__traceback__)
        except Exception:
            pass
        if isinstance(error, Exception):
            _raise_closed_connection(error, "authenticated worker session")
        raise
    return AuthenticatedWorkerSession(socket, reconciliation_cursor=cursor, worker_id=worker_id, certificate=certificate)


def _raise_closed_connection(error: Exception, phase: str) -> None:
    if isinstance(error, (ConnectionClosed, OSError, TimeoutError)):
        raise WorkerSessionDisconnected(f"The controller closed the {phase} WebSocket.") from error
    raise error
