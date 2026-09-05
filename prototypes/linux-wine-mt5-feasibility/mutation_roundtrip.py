from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
from typing import Any

CONFIRMATION_PHRASE = "DEMO_OPEN_CLOSE"


class MutationError(RuntimeError):
    pass


def _required(value: object, message: str) -> object:
    if value is None:
        raise MutationError(message)
    return value


def _positive_number(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MutationError(f"{name} must be numeric")
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise MutationError(f"{name} must be finite and positive")
    return number


def _receipt(value: object) -> dict[str, object]:
    return {
        "retcode": getattr(value, "retcode", None),
        "comment": getattr(value, "comment", None),
        "order": getattr(value, "order", None),
        "deal": getattr(value, "deal", None),
    }


def inspect_demo_preflight(mt5: object, symbol_name: str) -> dict[str, Any]:
    if not mt5.initialize():
        raise MutationError(f"MT5 initialization failed: {mt5.last_error()}")
    terminal = _required(mt5.terminal_info(), "terminal evidence unavailable")
    account = _required(mt5.account_info(), "account evidence unavailable")
    orders = _required(mt5.orders_get(), "starting orders unavailable")
    positions = _required(mt5.positions_get(), "starting positions unavailable")
    symbol = _required(mt5.symbol_info(symbol_name), "symbol evidence unavailable")
    tick = _required(mt5.symbol_info_tick(symbol_name), "tick unavailable")
    checks = {
        "terminal_connected": getattr(terminal, "connected", None) is True,
        "terminal_trading_enabled": getattr(terminal, "trade_allowed", None) is True,
        "demo_account": getattr(account, "trade_mode", None) == mt5.ACCOUNT_TRADE_MODE_DEMO,
        "account_trading_allowed": getattr(account, "trade_allowed", None) is True,
        "zero_starting_orders": len(orders) == 0,
        "zero_starting_positions": len(positions) == 0,
        "symbol_full_trading": getattr(symbol, "trade_mode", None) == mt5.SYMBOL_TRADE_MODE_FULL,
        "symbol_already_selected": getattr(symbol, "select", None) is True,
        "symbol_already_visible": getattr(symbol, "visible", None) is True,
        "positive_minimum_volume": _positive_number(getattr(symbol, "volume_min", None), "minimum volume") > 0,
        "positive_bid": _positive_number(getattr(tick, "bid", None), "bid") > 0,
        "positive_ask": _positive_number(getattr(tick, "ask", None), "ask") > 0,
    }
    return {
        "verdict": "preflight_ready" if all(checks.values()) else "preflight_blocked",
        "ready_for_confirmed_roundtrip": all(checks.values()),
        "server": getattr(account, "server", None),
        "symbol": symbol_name,
        "minimum_volume": getattr(symbol, "volume_min", None),
        "checks": checks,
    }


def run_demo_roundtrip(
    mt5: object,
    symbol_name: str,
    *,
    confirmation_phrase: str,
    authorization_reference: str,
) -> dict[str, Any]:
    if confirmation_phrase != CONFIRMATION_PHRASE:
        raise MutationError("explicit confirmation is required")
    if not re.fullmatch(r"[A-Za-z0-9._:/-]{1,128}", authorization_reference):
        raise MutationError("a bounded operator authorization reference is required")
    if not mt5.initialize():
        raise MutationError(f"MT5 initialization failed: {mt5.last_error()}")

    terminal = _required(mt5.terminal_info(), "terminal evidence unavailable")
    if getattr(terminal, "connected", None) is not True:
        raise MutationError("terminal is not connected")
    if getattr(terminal, "trade_allowed", None) is not True:
        raise MutationError("terminal trading is not enabled")

    account = _required(mt5.account_info(), "account evidence unavailable")
    if getattr(account, "trade_mode", None) != mt5.ACCOUNT_TRADE_MODE_DEMO:
        raise MutationError("account is not a demo account")
    if getattr(account, "trade_allowed", None) is not True:
        raise MutationError("demo account does not allow trading")
    account_fingerprint = (
        getattr(account, "login", None),
        getattr(account, "server", None),
        getattr(account, "trade_mode", None),
    )

    starting_orders = _required(mt5.orders_get(), "starting orders unavailable")
    starting_positions = _required(mt5.positions_get(), "starting positions unavailable")
    if len(starting_orders) != 0 or len(starting_positions) != 0:
        raise MutationError("demo roundtrip requires zero starting orders and positions")

    symbol = _required(mt5.symbol_info(symbol_name), "symbol evidence unavailable")
    if getattr(symbol, "trade_mode", None) != mt5.SYMBOL_TRADE_MODE_FULL:
        raise MutationError("symbol is not in full trading mode")
    if getattr(symbol, "visible", None) is not True or getattr(symbol, "select", None) is not True:
        raise MutationError("symbol must already be selected and visible")
    volume = _positive_number(getattr(symbol, "volume_min", None), "minimum volume")

    opening_tick = _required(mt5.symbol_info_tick(symbol_name), "opening tick unavailable")
    opening_price = _positive_number(getattr(opening_tick, "ask", None), "opening ask")
    open_request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol_name,
        "volume": volume,
        "type": mt5.ORDER_TYPE_BUY,
        "price": opening_price,
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_FOK,
    }
    check = _required(mt5.order_check(open_request), "order_check returned no result")
    if getattr(check, "retcode", None) != 0:
        raise MutationError(
            f"order_check rejected the demo order: {getattr(check, 'retcode', None)} "
            f"{getattr(check, 'comment', None)}"
        )

    current_terminal = _required(mt5.terminal_info(), "terminal evidence unavailable after order_check")
    current_account = _required(mt5.account_info(), "account evidence unavailable after order_check")
    current_orders = _required(mt5.orders_get(), "orders unavailable after order_check")
    current_positions = _required(mt5.positions_get(), "positions unavailable after order_check")
    current_symbol = _required(mt5.symbol_info(symbol_name), "symbol unavailable after order_check")
    current_fingerprint = (
        getattr(current_account, "login", None),
        getattr(current_account, "server", None),
        getattr(current_account, "trade_mode", None),
    )
    state_unchanged = (
        getattr(current_terminal, "connected", None) is True
        and getattr(current_terminal, "trade_allowed", None) is True
        and current_fingerprint == account_fingerprint
        and getattr(current_account, "trade_allowed", None) is True
        and len(current_orders) == 0
        and len(current_positions) == 0
        and getattr(current_symbol, "name", None) == symbol_name
        and getattr(current_symbol, "trade_mode", None) == mt5.SYMBOL_TRADE_MODE_FULL
        and getattr(current_symbol, "visible", None) is True
        and getattr(current_symbol, "select", None) is True
        and getattr(current_symbol, "volume_min", None) == volume
    )
    if not state_unchanged:
        raise MutationError("safety state changed after order_check; refusing order_send")

    open_result = mt5.order_send(open_request)
    if open_result is None:
        raise MutationError("unknown open outcome; never retry; inspect MT5 manually")
    if getattr(open_result, "retcode", None) != mt5.TRADE_RETCODE_DONE:
        raise MutationError(
            f"open was not completed: {getattr(open_result, 'retcode', None)} "
            f"{getattr(open_result, 'comment', None)}; inspect MT5 manually"
        )

    observed_positions = _required(mt5.positions_get(), "positions unavailable after open")
    matching = [
        position
        for position in observed_positions
        if getattr(position, "symbol", None) == symbol_name
        and getattr(position, "type", None) == mt5.ORDER_TYPE_BUY
    ]
    if len(observed_positions) != 1 or len(matching) != 1:
        raise MutationError("open receipt did not converge to one exact observed position; inspect MT5 manually")
    position = matching[0]
    ticket = getattr(position, "ticket", None)
    if isinstance(ticket, bool) or not isinstance(ticket, int) or ticket <= 0:
        raise MutationError("observed position has no valid ticket; inspect MT5 manually")
    observed_volume = _positive_number(getattr(position, "volume", None), "observed volume")
    if not math.isclose(observed_volume, volume, rel_tol=0.0, abs_tol=1e-12):
        raise MutationError("observed position volume differs from minimum volume; inspect MT5 manually")

    close_terminal = _required(mt5.terminal_info(), "terminal evidence unavailable before close")
    close_account = _required(mt5.account_info(), "account evidence unavailable before close")
    close_orders = _required(mt5.orders_get(), "orders unavailable before close")
    close_positions = _required(mt5.positions_get(), "positions unavailable before close")
    close_symbol = _required(mt5.symbol_info(symbol_name), "symbol unavailable before close")
    close_matching = [
        candidate
        for candidate in close_positions
        if getattr(candidate, "ticket", None) == ticket
        and getattr(candidate, "symbol", None) == symbol_name
        and getattr(candidate, "type", None) == mt5.ORDER_TYPE_BUY
        and math.isclose(
            _positive_number(getattr(candidate, "volume", None), "close-time position volume"),
            observed_volume,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
    ]
    close_gate_valid = (
        getattr(close_terminal, "connected", None) is True
        and getattr(close_terminal, "trade_allowed", None) is True
        and (
            getattr(close_account, "login", None),
            getattr(close_account, "server", None),
            getattr(close_account, "trade_mode", None),
        )
        == account_fingerprint
        and getattr(close_account, "trade_allowed", None) is True
        and len(close_orders) == 0
        and len(close_positions) == 1
        and len(close_matching) == 1
        and getattr(close_symbol, "name", None) == symbol_name
        and getattr(close_symbol, "trade_mode", None) == mt5.SYMBOL_TRADE_MODE_FULL
        and getattr(close_symbol, "visible", None) is True
        and getattr(close_symbol, "select", None) is True
        and getattr(close_symbol, "volume_min", None) == volume
    )
    if not close_gate_valid:
        raise MutationError(
            f"safety state changed before close for ticket {ticket}; do not retry; inspect MT5 manually"
        )

    closing_tick = _required(mt5.symbol_info_tick(symbol_name), "closing tick unavailable")
    closing_price = _positive_number(getattr(closing_tick, "bid", None), "closing bid")
    close_request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol_name,
        "volume": observed_volume,
        "type": mt5.ORDER_TYPE_SELL,
        "position": ticket,
        "price": closing_price,
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_FOK,
    }
    close_result = mt5.order_send(close_request)
    if close_result is None:
        raise MutationError(f"unknown close outcome for ticket {ticket}; never retry; inspect MT5 manually")
    if getattr(close_result, "retcode", None) != mt5.TRADE_RETCODE_DONE:
        raise MutationError(
            f"close was not completed for ticket {ticket}: {getattr(close_result, 'retcode', None)} "
            f"{getattr(close_result, 'comment', None)}; inspect MT5 manually"
        )

    deadline = time.monotonic() + 5.0
    while True:
        final_orders = _required(mt5.orders_get(), "final orders unavailable")
        final_positions = _required(mt5.positions_get(), "final positions unavailable")
        if len(final_orders) == 0 and len(final_positions) == 0:
            break
        if time.monotonic() >= deadline:
            raise MutationError(f"ticket {ticket} did not converge to zero exposure; inspect MT5 manually")
        time.sleep(0.05)

    return {
        "verdict": "demo_roundtrip_validated",
        "authorization_reference": authorization_reference,
        "server": getattr(account, "server", None),
        "symbol": symbol_name,
        "volume": volume,
        "ticket": ticket,
        "safety_evidence": {
            "confirmation_validated": True,
            "demo_account": True,
            "terminal_connected": True,
            "terminal_trading_enabled": True,
            "account_trading_allowed": True,
            "initial_order_count": 0,
            "initial_position_count": 0,
            "symbol_full_trading": True,
            "symbol_already_selected": True,
            "symbol_already_visible": True,
            "order_check_passed": True,
            "order_check_retcode": getattr(check, "retcode", None),
            "order_check_comment": getattr(check, "comment", None),
        },
        "open_request": {
            "symbol": symbol_name,
            "volume": volume,
            "type": "BUY",
            "price": opening_price,
        },
        "open_receipt": _receipt(open_result),
        "close_request": {
            "symbol": symbol_name,
            "volume": observed_volume,
            "type": "SELL",
            "position": ticket,
            "price": closing_price,
        },
        "close_receipt": _receipt(close_result),
        "final_order_count": 0,
        "final_position_count": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="One-shot demo-only MT5 minimum-volume open/close proof")
    parser.add_argument("--symbol", default="EURUSD")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--confirm-demo-roundtrip")
    parser.add_argument("--operator-authorization")
    args = parser.parse_args()
    if not args.preflight and args.confirm_demo_roundtrip != CONFIRMATION_PHRASE:
        print("Refusing mutation: confirmation phrase does not match.", file=sys.stderr)
        return 2
    if not args.preflight and not args.operator_authorization:
        print("Refusing mutation: operator authorization reference is required.", file=sys.stderr)
        return 2
    try:
        import MetaTrader5 as mt5
    except ImportError:
        print("Mutation probe failed: MetaTrader5 package unavailable.", file=sys.stderr)
        return 2
    try:
        if args.preflight:
            result = inspect_demo_preflight(mt5, args.symbol)
        else:
            result = run_demo_roundtrip(
                mt5,
                args.symbol,
                confirmation_phrase=args.confirm_demo_roundtrip,
                authorization_reference=args.operator_authorization,
            )
    except MutationError as error:
        print(json.dumps({"verdict": "failed", "error": str(error)}, indent=2), file=sys.stderr)
        return 1
    finally:
        mt5.shutdown()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
