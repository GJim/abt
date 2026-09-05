from __future__ import annotations

import math
import sys
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

from protocol import PROTOCOL_VERSION, ProtocolError, read_frame, write_frame

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
    "symbol_info": {"symbol"},
    "symbol_info_tick": {"symbol"},
    "copy_rates_from_pos": {"symbol", "timeframe", "start_pos", "count"},
    "order_calc_margin": {"action", "symbol", "volume", "price"},
    "order_calc_profit": {"action", "symbol", "volume", "open_price", "close_price"},
    "order_check": {"request"},
}
EXPOSED_CONSTANTS = (
    "ORDER_FILLING_FOK",
    "ORDER_TIME_GTC",
    "ORDER_TYPE_BUY",
    "TRADE_ACTION_DEAL",
)
ORDER_CHECK_FIELDS = {
    "action",
    "symbol",
    "volume",
    "type",
    "price",
    "type_time",
    "type_filling",
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
    if hasattr(value, "_asdict"):
        return _json_value(value._asdict())
    if hasattr(value, "tolist"):
        return _json_value(value.tolist())
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    raise BridgeError(f"MT5 returned unsupported value type {type(value).__name__}")


def _constant(mt5: object, name: object, allowed_names: set[str]) -> int:
    if not isinstance(name, str) or name not in allowed_names:
        raise BridgeError(f"unsupported MT5 constant {name!r}")
    value = getattr(mt5, name, None)
    if isinstance(value, bool) or not isinstance(value, int):
        raise BridgeError(f"unknown MT5 constant {name!r}")
    return value


def _validate_request(request: Mapping[str, Any]) -> tuple[str, str, dict[str, Any]]:
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
    if operation == "order_check":
        order_request = params.get("request")
        if not isinstance(order_request, dict):
            raise BridgeError("order_check request must be an object")
        unknown_order_fields = set(order_request) - ORDER_CHECK_FIELDS
        if unknown_order_fields:
            raise BridgeError(
                f"unknown order_check request fields: {sorted(unknown_order_fields)}"
            )
        missing_order_fields = ORDER_CHECK_FIELDS - set(order_request)
        if missing_order_fields:
            raise BridgeError(
                f"missing order_check request fields: {sorted(missing_order_fields)}"
            )
        if not isinstance(order_request["symbol"], str) or not order_request["symbol"]:
            raise BridgeError("order_check symbol must be a non-empty string")
        for field in ("volume", "price"):
            value = order_request[field]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
                raise BridgeError(f"order_check {field} must be positive")
        for field in ("action", "type", "type_time", "type_filling"):
            if isinstance(order_request[field], bool) or not isinstance(order_request[field], int):
                raise BridgeError(f"order_check {field} must be an integer")
    return request_id, operation, params


def _call(mt5: object, operation: str, params: dict[str, Any]) -> Any:
    if operation == "health":
        return {"bridge": "ready"}
    if operation == "constants":
        return {name: getattr(mt5, name) for name in EXPOSED_CONSTANTS}
    if operation == "initialize":
        return {"initialized": bool(mt5.initialize()), "last_error": _json_value(mt5.last_error())}
    if operation == "shutdown":
        mt5.shutdown()
        return {"shutdown": True}
    if operation == "last_error":
        return mt5.last_error()
    if operation in {"account_info", "terminal_info", "symbols_get", "orders_get", "positions_get"}:
        return getattr(mt5, operation)()
    if operation in {"symbol_info", "symbol_info_tick"}:
        return getattr(mt5, operation)(params["symbol"])
    if operation == "copy_rates_from_pos":
        timeframe = _constant(mt5, params["timeframe"], {"TIMEFRAME_M1"})
        return mt5.copy_rates_from_pos(
            params["symbol"],
            timeframe,
            params["start_pos"],
            params["count"],
        )
    if operation == "order_calc_margin":
        return mt5.order_calc_margin(
            _constant(mt5, params["action"], {"ORDER_TYPE_BUY"}),
            params["symbol"],
            params["volume"],
            params["price"],
        )
    if operation == "order_calc_profit":
        return mt5.order_calc_profit(
            _constant(mt5, params["action"], {"ORDER_TYPE_BUY"}),
            params["symbol"],
            params["volume"],
            params["open_price"],
            params["close_price"],
        )
    if operation == "order_check":
        order_request = params["request"]
        required_constants = {
            "action": mt5.TRADE_ACTION_DEAL,
            "type": mt5.ORDER_TYPE_BUY,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_FOK,
        }
        for field, expected in required_constants.items():
            if order_request[field] != expected:
                raise BridgeError(f"unsupported order_check {field}")
        return mt5.order_check(order_request)
    raise AssertionError(f"validated operation not dispatched: {operation}")


def handle_request(mt5: object, request: Mapping[str, Any]) -> dict[str, Any]:
    request_id, operation, params = _validate_request(request)
    return {
        "version": PROTOCOL_VERSION,
        "id": request_id,
        "ok": True,
        "result": _json_value(_call(mt5, operation, params)),
    }


def main() -> int:
    try:
        import MetaTrader5 as mt5
    except ImportError:
        print("bridge startup failed: MetaTrader5 package unavailable", file=sys.stderr, flush=True)
        return 2

    while True:
        try:
            request = read_frame(sys.stdin.buffer)
        except ProtocolError as error:
            print(f"bridge protocol failure: {error}", file=sys.stderr, flush=True)
            return 3
        request_id = request.get("id") if isinstance(request.get("id"), str) else "invalid"
        try:
            response = handle_request(mt5, request)
        except Exception as error:
            response = {
                "version": PROTOCOL_VERSION,
                "id": request_id,
                "ok": False,
                "error": {"type": type(error).__name__, "message": str(error)},
            }
        try:
            write_frame(sys.stdout.buffer, response)
        except ProtocolError as error:
            print(f"bridge response failure: {error}", file=sys.stderr, flush=True)
            return 4
        if request.get("operation") == "shutdown" and response.get("ok") is True:
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
