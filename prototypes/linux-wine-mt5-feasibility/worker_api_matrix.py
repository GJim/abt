from __future__ import annotations

import argparse
import json
import math
from datetime import UTC, datetime, timedelta
from typing import Any, Callable


def _count(value: object, name: str) -> int:
    if value is None:
        raise RuntimeError(f"{name} returned None")
    return len(value)  # type: ignore[arg-type]


def _finite(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeError(f"{name} returned a non-number")
    number = float(value)
    if not math.isfinite(number):
        raise RuntimeError(f"{name} returned a non-finite number")
    return number


def _market_watch_state(symbols: object) -> dict[str, tuple[object, object]]:
    if symbols is None:
        raise RuntimeError("symbols_get returned None")
    return {
        item.name: (getattr(item, "select", None), getattr(item, "visible", None))
        for item in symbols  # type: ignore[union-attr]
    }


def run_matrix(
    mt5: object,
    symbol_name: str,
    *,
    include_idempotent_symbol_select: bool,
    include_same_account_login: bool,
) -> dict[str, Any]:
    results: dict[str, Any] = {}

    def check(name: str, operation: Callable[[], Any], validate: Callable[[Any], Any]) -> Any:
        value = operation()
        evidence = validate(value)
        results[name] = {"status": "pass", "evidence": evidence}
        return value

    initialized = mt5.initialize()
    if initialized is not True:
        raise RuntimeError(f"initialize failed: {mt5.last_error()}")
    results["initialize"] = {"status": "pass"}
    try:
        terminal = check(
            "terminal_info",
            mt5.terminal_info,
            lambda value: {
                "connected": value.connected,
                "trade_allowed": value.trade_allowed,
                "build": value.build,
            }
            if value is not None and value.connected is True
            else (_ for _ in ()).throw(RuntimeError("terminal is not connected")),
        )
        account = check(
            "account_info",
            mt5.account_info,
            lambda value: {
                "server": value.server,
                "trade_mode": value.trade_mode,
                "trade_allowed": value.trade_allowed,
            }
            if value is not None
            else (_ for _ in ()).throw(RuntimeError("account_info returned None")),
        )
        fingerprint = (account.login, account.server, account.trade_mode)

        if include_same_account_login:
            login_ok = mt5.login(account.login, server=account.server)
            current = mt5.account_info()
            if login_ok is not True or current is None or (current.login, current.server, current.trade_mode) != fingerprint:
                raise RuntimeError(f"same-account login failed or changed account: {mt5.last_error()}")
            results["login"] = {
                "status": "pass",
                "evidence": "same stored-credential account; fingerprint unchanged",
                "limitation": "password argument was not supplied or exposed",
            }
        else:
            results["login"] = {"status": "not_run", "reason": "requires a separate same-account session gate"}

        symbols = check(
            "symbols_get",
            mt5.symbols_get,
            lambda value: {"count": _count(value, "symbols_get")},
        )
        watch_before = _market_watch_state(symbols)
        if symbol_name not in watch_before:
            raise RuntimeError(f"{symbol_name} is unavailable")

        symbol = check(
            "symbol_info",
            lambda: mt5.symbol_info(symbol_name),
            lambda value: {
                "name": value.name,
                "trade_mode": value.trade_mode,
                "visible": value.visible,
                "selected": value.select,
                "volume_min": value.volume_min,
                "point": value.point,
            }
            if value is not None
            else (_ for _ in ()).throw(RuntimeError("symbol_info returned None")),
        )
        tick = check(
            "symbol_info_tick",
            lambda: mt5.symbol_info_tick(symbol_name),
            lambda value: {"time": value.time, "bid_positive": value.bid > 0, "ask_positive": value.ask > 0}
            if value is not None and value.bid > 0 and value.ask > 0
            else (_ for _ in ()).throw(RuntimeError("symbol_info_tick returned no positive market")),
        )

        now = datetime.now(UTC)
        check(
            "copy_rates_range",
            lambda: mt5.copy_rates_range(symbol_name, mt5.TIMEFRAME_M1, now - timedelta(hours=1), now),
            lambda value: {"count": _count(value, "copy_rates_range")}
            if _count(value, "copy_rates_range") > 0
            else (_ for _ in ()).throw(RuntimeError("copy_rates_range returned zero rows")),
        )
        check(
            "copy_ticks_range",
            lambda: mt5.copy_ticks_range(symbol_name, now - timedelta(minutes=5), now, mt5.COPY_TICKS_ALL),
            lambda value: {"count": _count(value, "copy_ticks_range")}
            if _count(value, "copy_ticks_range") > 0
            else (_ for _ in ()).throw(RuntimeError("copy_ticks_range returned zero rows")),
        )

        volume = _finite(symbol.volume_min, "volume_min")
        ask = _finite(tick.ask, "tick.ask")
        point = _finite(symbol.point, "symbol.point")
        check(
            "order_calc_margin",
            lambda: mt5.order_calc_margin(mt5.ORDER_TYPE_BUY, symbol_name, volume, ask),
            lambda value: {"finite": math.isfinite(_finite(value, "order_calc_margin"))},
        )
        check(
            "order_calc_profit",
            lambda: mt5.order_calc_profit(mt5.ORDER_TYPE_BUY, symbol_name, volume, ask, ask + 10 * point),
            lambda value: {"finite": math.isfinite(_finite(value, "order_calc_profit"))},
        )
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol_name,
            "volume": volume,
            "type": mt5.ORDER_TYPE_BUY,
            "price": ask,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_FOK,
        }
        check(
            "order_check",
            lambda: mt5.order_check(request),
            lambda value: {"retcode": value.retcode, "comment": value.comment}
            if value is not None and value.retcode == 0
            else (_ for _ in ()).throw(RuntimeError("order_check did not pass")),
        )
        orders = check("orders_get", mt5.orders_get, lambda value: {"count": _count(value, "orders_get")})
        positions = check("positions_get", mt5.positions_get, lambda value: {"count": _count(value, "positions_get")})

        if include_idempotent_symbol_select:
            if watch_before[symbol_name] != (True, True):
                raise RuntimeError("symbol_select no-op proof requires symbol already selected and visible")
            if mt5.symbol_select(symbol_name, True) is not True:
                raise RuntimeError(f"idempotent symbol_select failed: {mt5.last_error()}")
            watch_after = _market_watch_state(mt5.symbols_get())
            if watch_after != watch_before:
                raise RuntimeError("idempotent symbol_select changed Market Watch")
            results["symbol_select"] = {"status": "pass", "evidence": "already selected; full catalog unchanged"}
        else:
            results["symbol_select"] = {"status": "not_run", "reason": "Market Watch mutation excluded from read-only phase"}

        if _count(mt5.orders_get(), "final orders_get") != _count(orders, "initial orders"):
            raise RuntimeError("matrix changed order count")
        if _count(mt5.positions_get(), "final positions_get") != _count(positions, "initial positions"):
            raise RuntimeError("matrix changed position count")
        if mt5.terminal_info().connected is not True or mt5.account_info() is None:
            raise RuntimeError("session evidence unavailable after matrix")
        results["order_send"] = {"status": "not_run", "reason": "reserved for separately authorized mutation roundtrip"}
        results["shutdown"] = {"status": "deferred", "reason": "executed in finally after report construction"}
        return {
            "verdict": "worker_python_mt5_api_matrix_passed",
            "symbol": symbol_name,
            "terminal_build": terminal.build,
            "api_results": results,
        }
    finally:
        mt5.shutdown()
        if "shutdown" in results:
            results["shutdown"] = {"status": "pass"}


def main() -> int:
    parser = argparse.ArgumentParser(description="Exercise the Python MT5 APIs currently used by ABT Worker")
    parser.add_argument("--symbol", default="EURUSD")
    parser.add_argument("--include-idempotent-symbol-select", action="store_true")
    parser.add_argument("--include-same-account-login", action="store_true")
    args = parser.parse_args()
    try:
        import MetaTrader5 as mt5
        result = run_matrix(
            mt5,
            args.symbol,
            include_idempotent_symbol_select=args.include_idempotent_symbol_select,
            include_same_account_login=args.include_same_account_login,
        )
    except Exception as error:
        print(json.dumps({"verdict": "failed", "error": str(error)}, indent=2))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
