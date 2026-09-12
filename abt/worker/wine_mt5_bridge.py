from __future__ import annotations

import argparse
import math
import sys
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

try:
    from .wine_mt5_protocol import PROTOCOL_VERSION, ProtocolError, read_frame, write_frame
except ImportError:  # Executed directly by Windows Python under Wine.
    from wine_mt5_protocol import PROTOCOL_VERSION, ProtocolError, read_frame, write_frame
    try:  # Protocol is already imported; drop the script dir so siblings never shadow stdlib.
        import os as _os

        _bridge_dir = _os.path.normcase(_os.path.abspath(_os.path.dirname(__file__)))
        sys.path = [
            _entry
            for _entry in sys.path
            if _os.path.normcase(_os.path.abspath(_entry or ".")) != _bridge_dir
        ]
        del _os, _bridge_dir
    except Exception:
        pass


REQUEST_FIELDS = {"version", "id", "operation", "params"}
NO_PARAM_OPERATIONS = {
    "health",
    "constants",
    "initialize",
    "shutdown",
    "account_info",
    "terminal_info",
    "symbols_get",
    "orders_get",
    "positions_get",
    "last_error",
}
PARAM_FIELDS = {
    "login": {"login", "password", "server"},
    "symbol_info": {"symbol"},
    "symbol_info_tick": {"symbol"},
    "symbol_select": {"symbol", "enable"},
    "copy_rates_range": {"symbol", "timeframe", "from", "to"},
    "copy_rates_from_pos": {"symbol", "timeframe", "start_pos", "count"},
    "copy_ticks_range": {"symbol", "from", "to", "flags"},
    "order_calc_margin": {"action", "symbol", "volume", "price"},
    "order_calc_profit": {"action", "symbol", "volume", "open_price", "close_price"},
    "order_check": {"request"},
    "order_send": {"request", "expected_login", "expected_server"},
}
CONSTANT_PREFIXES = ("COPY_TICKS_", "TIMEFRAME_", "TRADE_", "ORDER_", "POSITION_", "DEAL_", "ACCOUNT_", "SYMBOL_")
ORDER_REQUEST_FIELDS = {
    "action",
    "magic",
    "order",
    "symbol",
    "volume",
    "price",
    "stoplimit",
    "sl",
    "tp",
    "deviation",
    "type",
    "type_filling",
    "type_time",
    "expiration",
    "comment",
    "position",
    "position_by",
}


class BridgeError(RuntimeError):
    pass


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise BridgeError("MT5 returned a non-finite float")
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    as_dict = getattr(value, "_asdict", None)
    if callable(as_dict):
        return _json_value(as_dict())
    to_list = getattr(value, "tolist", None)
    if callable(to_list):
        return _json_value(to_list())
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    raise BridgeError(f"MT5 returned unsupported value type {type(value).__name__}")


def _required_text(params: Mapping[str, Any], field: str) -> str:
    value = params.get(field)
    if not isinstance(value, str) or not value:
        raise BridgeError(f"{field} must be a non-empty string")
    return value


def _required_int(params: Mapping[str, Any], field: str) -> int:
    value = params.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise BridgeError(f"{field} must be an integer")
    return value


def _required_positive_int(params: Mapping[str, Any], field: str) -> int:
    value = _required_int(params, field)
    if value <= 0:
        raise BridgeError(f"{field} must be a positive integer")
    return value


def _constant(mt5: object, name: object, allowed_names: set[str]) -> int:
    if not isinstance(name, str) or name not in allowed_names:
        raise BridgeError(f"unsupported MT5 constant {name!r}")
    value = getattr(mt5, name, None)
    if isinstance(value, bool) or not isinstance(value, int):
        raise BridgeError(f"unknown MT5 constant {name!r}")
    return value


def _required_number(params: Mapping[str, Any], field: str) -> int | float:
    value = params.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise BridgeError(f"{field} must be a finite number")
    return value


def _timestamp(params: Mapping[str, Any], field: str) -> datetime:
    value = _required_text(params, field)
    try:
        result = datetime.fromisoformat(value)
    except ValueError as error:
        raise BridgeError(f"{field} must be an ISO-8601 timestamp") from error
    if result.tzinfo is None:
        raise BridgeError(f"{field} must include a timezone")
    return result


def _order_request(params: Mapping[str, Any]) -> dict[str, Any]:
    request = params.get("request")
    if not isinstance(request, dict):
        raise BridgeError("order request must be an object")
    unknown = set(request) - ORDER_REQUEST_FIELDS
    if unknown:
        raise BridgeError(f"unknown order request fields: {sorted(unknown)}")
    if "action" not in request:
        raise BridgeError("order request requires action")
    if request.get("comment") or request.get("magic") is not None:
        raise BridgeError("MT5 orders must not set comment or magic number fields")
    for key, value in request.items():
        if value is None or isinstance(value, (str, bool, int)):
            continue
        if isinstance(value, float) and math.isfinite(value):
            continue
        raise BridgeError(f"order request field {key} has an unsupported value")
    return request


def _validate_request(request: Mapping[str, Any], *, allow_mutation: bool) -> tuple[str, str, dict[str, Any]]:
    unknown = set(request) - REQUEST_FIELDS
    if unknown:
        raise BridgeError(f"unknown request fields: {sorted(unknown)}")
    version = request.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version != PROTOCOL_VERSION:
        raise BridgeError("unsupported protocol version")
    request_id = request.get("id")
    operation = request.get("operation")
    params = request.get("params")
    if not isinstance(request_id, str) or not request_id or len(request_id) > 128:
        raise BridgeError("request id must be a non-empty string of at most 128 characters")
    if not isinstance(operation, str):
        raise BridgeError("operation must be a string")
    if not isinstance(params, dict):
        raise BridgeError("params must be an object")
    if operation == "order_send" and not allow_mutation:
        raise BridgeError("mutation operation requires one-shot mode")
    if operation == "history_deals_get":
        fields = set(params)
        if fields not in ({"position"}, {"from", "to"}):
            raise BridgeError("history_deals_get requires position or an exact date range")
        return request_id, operation, params
    if operation in NO_PARAM_OPERATIONS:
        allowed: set[str] = set()
    elif operation in PARAM_FIELDS:
        allowed = PARAM_FIELDS[operation]
    else:
        raise BridgeError(f"unsupported operation {operation!r}")
    unknown_params = set(params) - allowed
    if unknown_params:
        raise BridgeError(f"unknown parameters for {operation}: {sorted(unknown_params)}")
    missing_params = allowed - set(params)
    if missing_params:
        raise BridgeError(f"missing parameters for {operation}: {sorted(missing_params)}")
    if operation in {"order_check", "order_send"}:
        _order_request(params)
    return request_id, operation, params


def _constants(mt5: object) -> dict[str, int]:
    result: dict[str, int] = {}
    for name in dir(mt5):
        if not name.startswith(CONSTANT_PREFIXES):
            continue
        value = getattr(mt5, name, None)
        if isinstance(value, int) and not isinstance(value, bool):
            result[name] = value
    return result


def _call(mt5: object, operation: str, params: dict[str, Any]) -> Any:
    if operation == "health":
        return {"bridge": "ready"}
    if operation == "constants":
        return _constants(mt5)
    if operation == "initialize":
        return {"initialized": bool(mt5.initialize()), "last_error": _json_value(mt5.last_error())}
    if operation == "login":
        logged_in = bool(
            mt5.login(
                _required_positive_int(params, "login"),
                password=_required_text(params, "password"),
                server=_required_text(params, "server"),
            )
        )
        return {"logged_in": logged_in, "last_error": _json_value(mt5.last_error())}
    if operation == "shutdown":
        mt5.shutdown()
        return {"shutdown": True}
    if operation == "last_error":
        return mt5.last_error()
    if operation in {"account_info", "terminal_info", "symbols_get", "orders_get", "positions_get"}:
        return getattr(mt5, operation)()
    if operation in {"symbol_info", "symbol_info_tick"}:
        return getattr(mt5, operation)(_required_text(params, "symbol"))
    if operation == "symbol_select":
        enable = params.get("enable")
        if not isinstance(enable, bool):
            raise BridgeError("enable must be a boolean")
        return {"selected": bool(mt5.symbol_select(_required_text(params, "symbol"), enable))}
    if operation == "copy_rates_range":
        return mt5.copy_rates_range(
            _required_text(params, "symbol"),
            _required_int(params, "timeframe"),
            _timestamp(params, "from"),
            _timestamp(params, "to"),
        )
    if operation == "copy_rates_from_pos":
        timeframe = params.get("timeframe")
        if isinstance(timeframe, str):
            timeframe = _constant(
                mt5,
                timeframe,
                {
                    "TIMEFRAME_M1",
                    "TIMEFRAME_M2",
                    "TIMEFRAME_M3",
                    "TIMEFRAME_M4",
                    "TIMEFRAME_M5",
                    "TIMEFRAME_M6",
                    "TIMEFRAME_M10",
                    "TIMEFRAME_M12",
                    "TIMEFRAME_M15",
                    "TIMEFRAME_M20",
                    "TIMEFRAME_M30",
                    "TIMEFRAME_H1",
                    "TIMEFRAME_H2",
                    "TIMEFRAME_H3",
                    "TIMEFRAME_H4",
                    "TIMEFRAME_H6",
                    "TIMEFRAME_H8",
                    "TIMEFRAME_H12",
                    "TIMEFRAME_D1",
                    "TIMEFRAME_W1",
                    "TIMEFRAME_MN1",
                },
            )
        elif not isinstance(timeframe, int) or isinstance(timeframe, bool):
            raise BridgeError("timeframe must be an integer or MT5 timeframe constant name")
        return mt5.copy_rates_from_pos(
            _required_text(params, "symbol"),
            timeframe,
            _required_int(params, "start_pos"),
            _required_int(params, "count"),
        )
    if operation == "copy_ticks_range":
        return mt5.copy_ticks_range(
            _required_text(params, "symbol"),
            _timestamp(params, "from"),
            _timestamp(params, "to"),
            _required_int(params, "flags"),
        )
    if operation == "order_calc_margin":
        return mt5.order_calc_margin(
            _required_int(params, "action"),
            _required_text(params, "symbol"),
            _required_number(params, "volume"),
            _required_number(params, "price"),
        )
    if operation == "order_calc_profit":
        return mt5.order_calc_profit(
            _required_int(params, "action"),
            _required_text(params, "symbol"),
            _required_number(params, "volume"),
            _required_number(params, "open_price"),
            _required_number(params, "close_price"),
        )
    if operation == "order_check":
        return mt5.order_check(_order_request(params))
    if operation == "order_send":
        if not mt5.initialize():
            raise BridgeError("one-shot MT5 initialization failed")
        account = mt5.account_info()
        as_dict = account._asdict() if hasattr(account, "_asdict") else account
        if not isinstance(as_dict, Mapping):
            raise BridgeError("one-shot MT5 account evidence is unavailable")
        if as_dict.get("login") != _required_positive_int(params, "expected_login") or as_dict.get("server") != _required_text(params, "expected_server"):
            raise BridgeError("one-shot MT5 account does not match the Worker identity")
        try:
            return mt5.order_send(_order_request(params))
        finally:
            mt5.shutdown()
    if operation == "history_deals_get":
        if "position" in params:
            return mt5.history_deals_get(position=_required_int(params, "position"))
        return mt5.history_deals_get(_timestamp(params, "from"), _timestamp(params, "to"))
    raise AssertionError(f"validated operation not dispatched: {operation}")


def handle_request(
    mt5: object,
    request: Mapping[str, Any],
    *,
    allow_mutation: bool = False,
) -> dict[str, Any]:
    request_id, operation, params = _validate_request(request, allow_mutation=allow_mutation)
    return {
        "version": PROTOCOL_VERSION,
        "id": request_id,
        "ok": True,
        "result": _json_value(_call(mt5, operation, params)),
    }


def _serve_one(mt5: object, *, allow_mutation: bool) -> bool:
    request = read_frame(sys.stdin.buffer)
    request_id = request.get("id") if isinstance(request.get("id"), str) else "invalid"
    try:
        response = handle_request(mt5, request, allow_mutation=allow_mutation)
    except Exception as error:
        response = {
            "version": PROTOCOL_VERSION,
            "id": request_id,
            "ok": False,
            "error": {"type": type(error).__name__, "message": str(error)},
        }
    write_frame(sys.stdout.buffer, response)
    return request.get("operation") == "shutdown" and response.get("ok") is True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--one-shot-mutation", action="store_true")
    arguments = parser.parse_args(argv)
    try:
        import MetaTrader5 as mt5
    except ImportError:
        print("bridge startup failed: MetaTrader5 package unavailable", file=sys.stderr, flush=True)
        return 2

    if arguments.one_shot_mutation:
        try:
            _serve_one(mt5, allow_mutation=True)
            return 0
        except ProtocolError as error:
            print(f"bridge protocol failure: {error}", file=sys.stderr, flush=True)
            return 3

    while True:
        try:
            if _serve_one(mt5, allow_mutation=False):
                return 0
        except ProtocolError as error:
            print(f"bridge protocol failure: {error}", file=sys.stderr, flush=True)
            return 3


if __name__ == "__main__":
    raise SystemExit(main())
