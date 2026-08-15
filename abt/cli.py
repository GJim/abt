from __future__ import annotations

import argparse
import getpass
import json
import math
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

import MetaTrader5 as mt5

from . import config
from .config import Config, Context
from .mt5 import SessionError, connected, delete_password, save_password
from .output import normalize, render
from .timecalibration import (
    MARKET_DATA,
    TRADE_RECORDS,
    prepare_market_data,
    record_successful_write,
    render_calibration,
    time_status,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="abt", description="Manual MetaTrader 5 CLI")
    parser.add_argument("--config", type=Path, default=config.default_config_path())
    parser.add_argument("--context", help="Override the configured current context.")
    parser.add_argument("--output", choices=("table", "json"), default="table")
    subparsers = parser.add_subparsers(dest="command", required=True)

    context_parser = subparsers.add_parser("context", help="Manage named MT5 contexts.")
    context_subparsers = context_parser.add_subparsers(dest="context_command", required=True)
    context_subparsers.add_parser("list")
    add = context_subparsers.add_parser("add")
    add.add_argument("name")
    add.add_argument("--terminal-path", required=True, type=Path)
    add.add_argument("--login", required=True, type=int)
    add.add_argument("--server", required=True)
    add.add_argument("--timezone", required=True, type=config.user_timezone, metavar="IANA_TIMEZONE")
    login = context_subparsers.add_parser("login")
    login.add_argument("name")
    use = context_subparsers.add_parser("use")
    use.add_argument("name")
    use.add_argument("--yes", action="store_true")
    remove = context_subparsers.add_parser("remove")
    remove.add_argument("name")
    remove.add_argument("--yes", action="store_true")
    set_timezone = context_subparsers.add_parser("set-timezone")
    set_timezone.add_argument("name")
    set_timezone.add_argument("timezone", type=config.user_timezone, metavar="IANA_TIMEZONE")
    context_subparsers.add_parser("status")
    context_subparsers.add_parser("time-status", help="Show persisted time-calibration status.")

    subparsers.add_parser("account")
    subparsers.add_parser("terminal")
    subparsers.add_parser("version")
    symbols = subparsers.add_parser("symbols")
    symbols.add_argument("--all", action="store_true", help="Include symbols not in Market Watch.")
    symbol = subparsers.add_parser("symbol")
    symbol.add_argument("symbol")
    symbol_select = subparsers.add_parser("symbol-select", help="Add a symbol to Market Watch.")
    symbol_select.add_argument("symbol")
    symbol_hide = subparsers.add_parser("symbol-hide", help="Remove a symbol from Market Watch.")
    symbol_hide.add_argument("symbol")
    tick = subparsers.add_parser("tick")
    tick.add_argument("symbol")
    positions = subparsers.add_parser("positions")
    positions.add_argument("--symbol")
    positions.add_argument("--ticket", type=int)
    subparsers.add_parser("positions-total")
    orders = subparsers.add_parser("orders")
    orders.add_argument("--symbol")
    orders.add_argument("--ticket", type=int)
    orders.add_argument("--group")
    subparsers.add_parser("orders-total")
    subparsers.add_parser("symbols-total")
    for name in ("history-orders", "history-deals"):
        history = subparsers.add_parser(name)
        history.add_argument("--from", dest="from_date", type=_date)
        history.add_argument("--to", dest="to_date", type=_date)
        history.add_argument("--since", type=_duration)
        history.add_argument("--group")
        history.add_argument("--ticket", type=int)
        history.add_argument("--position", type=int)
    for name in ("history-orders-total", "history-deals-total"):
        history_total = subparsers.add_parser(name)
        history_total.add_argument("--from", dest="from_date", type=_date)
        history_total.add_argument("--to", dest="to_date", type=_date)
        history_total.add_argument("--since", type=_duration)
    for name in ("rates-from", "rates-from-pos", "rates-range"):
        rates = subparsers.add_parser(name)
        rates.add_argument("symbol")
        rates.add_argument("timeframe", type=_timeframe)
        rates.add_argument("--count", required=name != "rates-range", type=_positive_count)
        if name == "rates-from":
            rates.add_argument("--from", dest="from_time", required=True, type=_datetime_input)
        elif name == "rates-from-pos":
            rates.add_argument("--start-pos", required=True, type=int)
        else:
            rates.add_argument("--from", dest="from_time", required=True, type=_datetime_input)
            rates.add_argument("--to", dest="to_time", required=True, type=_datetime_input)
    for name in ("ticks-from", "ticks-range"):
        ticks = subparsers.add_parser(name)
        ticks.add_argument("symbol")
        ticks.add_argument("--count", required=name == "ticks-from", type=_positive_count)
        ticks.add_argument("--flags", choices=("all", "info", "trade"), default="all")
        ticks.add_argument("--from", dest="from_time", required=True, type=_datetime_input)
        if name == "ticks-range":
            ticks.add_argument("--to", dest="to_time", required=True, type=_datetime_input)
    book = subparsers.add_parser("book")
    book.add_argument("symbol")
    book.add_argument("--watch", type=_positive_seconds, default=0)
    margin = subparsers.add_parser("calc-margin")
    margin.add_argument("side", choices=("buy", "sell"))
    margin.add_argument("symbol")
    margin.add_argument("volume", type=float)
    margin.add_argument("price", type=float)
    profit = subparsers.add_parser("calc-profit")
    profit.add_argument("side", choices=("buy", "sell"))
    profit.add_argument("symbol")
    profit.add_argument("volume", type=float)
    profit.add_argument("open_price", type=float)
    profit.add_argument("close_price", type=float)
    check = subparsers.add_parser("order-check", help="Validate an MT5 trade request without sending it.")
    check.add_argument("--request-json", required=True, type=_json_object)

    for side in ("buy", "sell"):
        market = subparsers.add_parser(side, help=f"Submit a market {side} order.")
        _add_market_arguments(market, side)
    market = subparsers.add_parser("market", help="Submit a market buy or sell order.")
    market_subparsers = market.add_subparsers(dest="market_side", required=True)
    for side in ("buy", "sell"):
        market_side = market_subparsers.add_parser(side)
        _add_market_arguments(market_side, side)

    for name, side in _PENDING_ORDER_SIDES.items():
        pending = subparsers.add_parser(name, help=f"Submit a {name} pending order.")
        _add_pending_arguments(pending, name, side)
    pending = subparsers.add_parser("pending", help="Submit a typed pending order.")
    pending_subparsers = pending.add_subparsers(dest="pending_kind", required=True)
    for name, side in _PENDING_ORDER_SIDES.items():
        pending_kind = pending_subparsers.add_parser(name)
        _add_pending_arguments(pending_kind, name, side)

    cancel = subparsers.add_parser("cancel", help="Cancel a pending order by ticket.")
    cancel.add_argument("ticket", type=_positive_ticket)
    _add_confirmation_argument(cancel)

    pending_modify = subparsers.add_parser("pending-modify", help="Modify a pending order.")
    pending_modify.add_argument("ticket", type=_positive_ticket)
    pending_modify.add_argument("--price", type=_positive_float)
    pending_modify.add_argument("--stop-limit-price", type=_positive_float)
    _add_time_arguments(pending_modify, required=False)
    _add_protection_arguments(pending_modify, allow_clear=True)
    _add_confirmation_argument(pending_modify)

    position_modify = subparsers.add_parser("position-modify", help="Modify a position's SL or TP.")
    _add_position_selector_arguments(position_modify)
    _add_protection_arguments(position_modify, allow_clear=True)
    _add_confirmation_argument(position_modify)

    position_close = subparsers.add_parser("position-close", help="Close all or part of a position.")
    _add_position_selector_arguments(position_close)
    position_close.add_argument("--volume", type=_positive_float)
    position_close.add_argument("--fill", required=True, choices=_FILLING_MODES)
    position_close.add_argument("--deviation-points", required=True, type=_nonnegative_int)
    _add_magic_comment_arguments(position_close)
    _add_confirmation_argument(position_close)

    close_by = subparsers.add_parser("close-by", help="Close two opposing hedging positions against each other.")
    close_by.add_argument("ticket", type=_positive_ticket)
    close_by.add_argument("position_by", type=_positive_ticket)
    _add_confirmation_argument(close_by)

    send = subparsers.add_parser(
        "order-send",
        help="Advanced: check and send an explicit raw MT5 request JSON object.",
    )
    send.add_argument("--request-json", required=True, type=_json_object)
    _add_confirmation_argument(send)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        loaded_config = config.load(args.config)
        result = dispatch(args, loaded_config)
        if result is not None:
            print(
                render(
                    result,
                    args.output,
                    user_timezone=getattr(args, "user_timezone", None),
                    source_family=getattr(args, "time_source_family", None),
                    calibration=getattr(args, "time_calibration", None),
                    field_calibrations=getattr(args, "time_field_calibrations", None),
                )
            )
        return 0
    except (config.ConfigError, SessionError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


def dispatch(args: argparse.Namespace, loaded_config: Config) -> Any:
    if args.command == "context":
        return _context_command(args, loaded_config)
    context = _selected_context(args, loaded_config)
    args.user_timezone = _required_user_timezone(context)
    with connected(context) as api:
        args.time_source_family = _time_source_family(args.command)
        if args.time_source_family == MARKET_DATA:
            symbol = _market_data_symbol(args)
            context, family = prepare_market_data(api, loaded_config, context, symbol, args.user_timezone)
            args.time_calibration = render_calibration(family, MARKET_DATA, args.user_timezone)
        elif args.time_source_family == TRADE_RECORDS:
            args.time_calibration = render_calibration(
                context.time_calibration.trade_records, TRADE_RECORDS, args.user_timezone
            )
            market_calibration = render_calibration(
                context.time_calibration.market_data, MARKET_DATA, args.user_timezone
            )
            args.time_field_calibrations = {
                "expiration": market_calibration,
                "broker_expiration": market_calibration,
                "actual_broker_expiration": market_calibration,
            }
        else:
            args.time_calibration = {
                "family": "host_utc",
                "status": "utc",
                "offset_seconds": 0,
                "offset_layer": "utc",
            }
        if _is_write_command(args):
            return _write_command(args, api, loaded_config, context)
        return _read_command(args, api)


def _context_command(args: argparse.Namespace, loaded_config: Config) -> Any:
    if args.context_command == "list":
        return [
            {
                "name": context.name,
                "current": context.name == loaded_config.current_context,
                "login": context.login,
                "server": context.server,
                "terminal_path": str(context.terminal_path),
                "user_timezone": context.user_timezone,
            }
            for context in loaded_config.contexts.values()
        ]
    if args.context_command == "status":
        if loaded_config.current_context is None:
            return {"current_context": None}
        context = loaded_config.contexts[loaded_config.current_context]
        args.user_timezone = _required_user_timezone(context)
        with connected(context) as api:
            return {"context": context.name, "account": api.account_info(), "terminal": api.terminal_info()}
    if args.context_command == "time-status":
        context = _selected_context(args, loaded_config)
        args.user_timezone = _required_user_timezone(context)
        args.time_source_family = "host_utc"
        args.time_calibration = {
            "family": "host_utc",
            "status": "utc",
            "offset_seconds": 0,
            "offset_layer": "utc",
        }
        return time_status(context, args.user_timezone)
    if args.context_command == "add":
        if not config.CONTEXT_NAME.fullmatch(args.name):
            raise ValueError("Context names may contain only letters, digits, dot, dash, and underscore.")
        if args.name in loaded_config.contexts:
            raise ValueError(f"Context {args.name!r} already exists.")
        context = Context(args.name, args.terminal_path.resolve(), args.login, args.server, args.timezone)
        _login_interactively(context)
        contexts = {**loaded_config.contexts, context.name: context}
        config.save(Config(loaded_config.path, loaded_config.current_context or context.name, contexts))
        return {
            "context": context.name,
            "login": context.login,
            "server": context.server,
            "user_timezone": context.user_timezone,
            "saved": True,
        }
    if args.context_command == "login":
        context = _named_context(args.name, loaded_config)
        _login_interactively(context)
        return {"context": context.name, "login": context.login, "server": context.server, "saved": True}
    if args.context_command == "use":
        target = _named_context(args.name, loaded_config)
        _confirm_context_switch(loaded_config, target, args.yes)
        with connected(target) as api:
            account = api.account_info()
        config.save(Config(loaded_config.path, target.name, loaded_config.contexts))
        return {"current_context": target.name, "login": account.login, "server": account.server}
    if args.context_command == "remove":
        context = _named_context(args.name, loaded_config)
        if not args.yes and not _confirm(f"Remove context {context.name!r} and its saved password?"):
            raise ValueError("Context removal cancelled.")
        delete_password(context)
        contexts = {name: item for name, item in loaded_config.contexts.items() if name != context.name}
        current = None if loaded_config.current_context == context.name else loaded_config.current_context
        config.save(Config(loaded_config.path, current, contexts))
        return {"removed": context.name}
    if args.context_command == "set-timezone":
        context = _named_context(args.name, loaded_config)
        updated = Context(
            context.name,
            context.terminal_path,
            context.login,
            context.server,
            args.timezone,
            context.time_calibration,
        )
        contexts = {**loaded_config.contexts, context.name: updated}
        config.save(Config(loaded_config.path, loaded_config.current_context, contexts))
        return {"context": updated.name, "user_timezone": updated.user_timezone, "saved": True}
    raise ValueError("Unknown context command.")


def _login_interactively(context: Context) -> None:
    password = getpass.getpass(f"MT5 password for {context.login}@{context.server}: ")
    if not password:
        raise ValueError("Password cannot be empty.")
    if not context.terminal_path.is_file():
        raise ValueError(f"Terminal executable does not exist: {context.terminal_path}")
    if not mt5.initialize(
        path=str(context.terminal_path), login=context.login, password=password, server=context.server, timeout=10_000
    ):
        raise SessionError(f"MT5 login failed: {mt5.last_error()}")
    try:
        account = mt5.account_info()
        if account is None or account.login != context.login or account.server != context.server:
            actual = "unavailable" if account is None else f"{account.login}@{account.server}"
            raise SessionError(f"Terminal logged into {actual}, not {context.login}@{context.server}.")
    finally:
        mt5.shutdown()
    save_password(context, password)


def _confirm_context_switch(loaded_config: Config, target: Context, yes: bool) -> None:
    if yes or loaded_config.current_context is None or loaded_config.current_context == target.name:
        return
    current = loaded_config.contexts[loaded_config.current_context]
    try:
        with connected(current) as api:
            positions = api.positions_total()
            orders = api.orders_total()
    except SessionError:
        positions = orders = 0
    if positions or orders:
        prompt = f"{current.name!r} has {positions} position(s) and {orders} order(s). Switch to {target.name!r}?"
        if not _confirm(prompt):
            raise ValueError("Context switch cancelled.")


def _read_command(args: argparse.Namespace, api: object) -> Any:
    if args.command == "account":
        return api.account_info()
    if args.command == "terminal":
        return api.terminal_info()
    if args.command == "version":
        return {"version": api.version(), "last_error": api.last_error()}
    if args.command == "symbols":
        symbols = api.symbols_get()
        if symbols is None:
            raise SessionError(f"Unable to read symbols: {api.last_error()}")
        filtered = [symbol for symbol in symbols if args.all or symbol.visible]
        return [
            {
                "name": symbol.name,
                "visible": symbol.visible,
                "trade_mode": symbol.trade_mode,
                "digits": symbol.digits,
                "point": symbol.point,
                "volume_min": symbol.volume_min,
                "volume_step": symbol.volume_step,
                "volume_max": symbol.volume_max,
                "filling_mode": symbol.filling_mode,
                "currency_base": symbol.currency_base,
                "currency_profit": symbol.currency_profit,
            }
            for symbol in filtered
        ]
    if args.command == "symbol":
        return _required(api.symbol_info(args.symbol), f"Unknown symbol {args.symbol!r}", api)
    if args.command in {"symbol-select", "symbol-hide"}:
        visible = args.command == "symbol-select"
        action = "add to" if visible else "remove from"
        if not api.symbol_select(args.symbol, visible):
            raise SessionError(f"Unable to {action} Market Watch: {api.last_error()}")
        symbol = _required(api.symbol_info(args.symbol), f"Unknown symbol {args.symbol!r}", api)
        if symbol.visible != visible:
            raise SessionError(f"MT5 did not {action} Market Watch for {args.symbol!r}.")
        return {"symbol": symbol.name, "visible": symbol.visible}
    if args.command == "tick":
        return _required(api.symbol_info_tick(args.symbol), f"No tick for {args.symbol!r}", api)
    if args.command == "positions":
        return _get_records(api.positions_get, args, api)
    if args.command == "positions-total":
        return api.positions_total()
    if args.command == "orders":
        return _get_records(api.orders_get, args, api)
    if args.command == "orders-total":
        return api.orders_total()
    if args.command == "symbols-total":
        return api.symbols_total()
    if args.command in {"history-orders", "history-deals"}:
        start, end = _history_window(args, args.user_timezone)
        getter: Callable[..., Any] = api.history_orders_get if args.command == "history-orders" else api.history_deals_get
        records = _get_history(getter, start, end, args)
        return {"from": start, "to": end, "records": records}
    if args.command in {"history-orders-total", "history-deals-total"}:
        start, end = _history_window(args, args.user_timezone)
        getter = api.history_orders_total if args.command == "history-orders-total" else api.history_deals_total
        return {"from": start, "to": end, "total": getter(start, end)}
    if args.command.startswith("rates-"):
        return _rates(api, args)
    if args.command.startswith("ticks-"):
        return _ticks(api, args)
    if args.command == "book":
        return _market_book(api, args.symbol, args.watch)
    if args.command == "calc-margin":
        action = api.ORDER_TYPE_BUY if args.side == "buy" else api.ORDER_TYPE_SELL
        return _required(
            api.order_calc_margin(action, args.symbol, args.volume, args.price),
            "Broker rejected margin calculation",
            api,
        )
    if args.command == "calc-profit":
        action = api.ORDER_TYPE_BUY if args.side == "buy" else api.ORDER_TYPE_SELL
        return _required(
            api.order_calc_profit(action, args.symbol, args.volume, args.open_price, args.close_price),
            "Broker rejected profit calculation",
            api,
        )
    if args.command == "order-check":
        result = api.order_check(args.request_json)
        return _required(result, "Broker rejected order check", api)
    raise ValueError("Unknown command.")


_FILLING_MODES = ("fok", "ioc", "return")
_PENDING_ORDER_SIDES = {
    "buy-limit": "buy",
    "sell-limit": "sell",
    "buy-stop": "buy",
    "sell-stop": "sell",
    "buy-stop-limit": "buy",
    "sell-stop-limit": "sell",
}
_WRITE_COMMANDS = {
    "buy",
    "sell",
    "market",
    "buy-limit",
    "sell-limit",
    "buy-stop",
    "sell-stop",
    "buy-stop-limit",
    "sell-stop-limit",
    "pending",
    "cancel",
    "pending-modify",
    "position-modify",
    "position-close",
    "close-by",
    "order-send",
}


def _add_market_arguments(parser: argparse.ArgumentParser, side: str) -> None:
    parser.set_defaults(write_kind="market", side=side)
    parser.add_argument("symbol")
    parser.add_argument("volume", type=_positive_float)
    parser.add_argument("--fill", required=True, choices=_FILLING_MODES)
    parser.add_argument("--deviation-points", required=True, type=_nonnegative_int)
    _add_protection_arguments(parser)
    parser.add_argument(
        "--protection-from-fill",
        action="store_true",
        help="Reserved for post-fill protection; not supported yet.",
    )
    _add_magic_comment_arguments(parser)
    _add_confirmation_argument(parser)


def _add_pending_arguments(parser: argparse.ArgumentParser, name: str, side: str) -> None:
    parser.set_defaults(write_kind="pending", pending_kind=name, side=side)
    parser.add_argument("symbol")
    parser.add_argument("volume", type=_positive_float)
    parser.add_argument("--price", required=True, type=_positive_float)
    parser.add_argument("--stop-limit-price", type=_positive_float)
    parser.add_argument("--fill", required=True, choices=_FILLING_MODES)
    _add_time_arguments(parser, required=True)
    _add_protection_arguments(parser)
    _add_magic_comment_arguments(parser)
    _add_confirmation_argument(parser)


def _add_time_arguments(parser: argparse.ArgumentParser, *, required: bool) -> None:
    parser.add_argument("--time", dest="time_in_force", choices=("gtc", "day", "specified"), required=required)
    expiration = parser.add_mutually_exclusive_group()
    expiration.add_argument("--expires-in", type=_expiration_duration)
    expiration.add_argument("--expires-at", type=_datetime_input)


def _add_protection_arguments(parser: argparse.ArgumentParser, *, allow_clear: bool = False) -> None:
    for name in ("sl", "tp"):
        group = parser.add_mutually_exclusive_group()
        group.add_argument(f"--{name}", type=_positive_float, help=f"Absolute {name.upper()} price.")
        group.add_argument(f"--{name}-points", type=_positive_float, help=f"{name.upper()} distance in symbol points.")
        group.add_argument(f"--{name}-pips", type=_positive_float, help=f"{name.upper()} distance in Forex pips.")
        group.add_argument(f"--{name}-percent", type=_positive_float, help=f"{name.upper()} distance as a percent.")
        if allow_clear:
            group.add_argument(f"--clear-{name}", action="store_true", help=f"Clear the existing {name.upper()}.")


def _add_magic_comment_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--magic", type=_nonnegative_int)
    parser.add_argument("--comment", type=_comment)


def _add_confirmation_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--yes", action="store_true", help="Skip the interactive request confirmation.")


def _add_position_selector_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--symbol")
    parser.add_argument("--ticket", type=_positive_ticket)


def _is_write_command(args: argparse.Namespace) -> bool:
    return args.command in _WRITE_COMMANDS


def _time_source_family(command: str) -> str:
    if command == "tick" or command.startswith("rates-") or command.startswith("ticks-"):
        return MARKET_DATA
    if command in {"orders", "positions", "history-orders", "history-deals"} or command in _WRITE_COMMANDS:
        return TRADE_RECORDS
    return "host_utc"


def _market_data_symbol(args: argparse.Namespace) -> str:
    symbol = getattr(args, "symbol", None)
    if not isinstance(symbol, str) or not symbol:
        raise ValueError("Market-data command requires a symbol for time calibration.")
    return symbol


def _write_command(
    args: argparse.Namespace,
    api: object,
    loaded_config: Config | None = None,
    context: Context | None = None,
) -> dict[str, Any]:
    request = _build_write_request(args, api)
    preview_expiration = _expiration_details(getattr(args, "_expiration_mapping", None))
    if not args.yes:
        print("Broker request preview:")
        preview: dict[str, Any] = {"request": request}
        if preview_expiration is not None:
            preview["expiration"] = preview_expiration
        print(
            render(
                preview,
                args.output,
                user_timezone=getattr(args, "user_timezone", None),
                source_family=getattr(args, "time_source_family", None),
                calibration=getattr(args, "time_calibration", None),
                field_calibrations=getattr(args, "time_field_calibrations", None),
            )
        )
        if not _confirm("Check this request and send it?"):
            result: dict[str, Any] = {"request": request, "cancelled": True}
            if preview_expiration is not None:
                result["expiration"] = preview_expiration
            return result
    def record_calibration(request: Mapping[str, Any], sent: object, before: datetime, after: datetime) -> None:
        if loaded_config is None or context is None:
            return
        _, family = record_successful_write(
            api,
            loaded_config,
            context,
            request,
            sent,
            before,
            after,
            args.user_timezone,
        )
        args.time_calibration = render_calibration(family, TRADE_RECORDS, args.user_timezone)

    return _check_then_send(
        request,
        api,
        expiration_plan=getattr(args, "_expiration_plan", None),
        expiration_symbol=getattr(args, "_expiration_symbol", None),
        successful_send_callback=record_calibration,
    )


def _build_write_request(args: argparse.Namespace, api: object) -> dict[str, Any]:
    if args.command == "order-send":
        return args.request_json
    if getattr(args, "write_kind", None) == "market":
        return _market_request(args, api)
    if getattr(args, "write_kind", None) == "pending":
        return _pending_request(args, api)
    if args.command == "cancel":
        return {"action": _constant(api, "TRADE_ACTION_REMOVE"), "order": args.ticket}
    if args.command == "pending-modify":
        return _pending_modify_request(args, api)
    if args.command == "position-modify":
        return _position_modify_request(args, api)
    if args.command == "position-close":
        return _position_close_request(args, api)
    if args.command == "close-by":
        return _close_by_request(args, api)
    raise ValueError("Unknown write command.")


def _market_request(args: argparse.Namespace, api: object) -> dict[str, Any]:
    if args.protection_from_fill:
        raise ValueError("--protection-from-fill is not supported; protections are calculated from the executable quote.")
    price = _executable_price(api, args.symbol, args.side)
    request = {
        "action": _constant(api, "TRADE_ACTION_DEAL"),
        "symbol": args.symbol,
        "volume": args.volume,
        "type": _order_type(api, args.side),
        "price": price,
        "deviation": args.deviation_points,
        "type_filling": _filling_type(api, args.fill),
    }
    request.update(_new_protections(args, api, args.symbol, price, args.side))
    request.update(_metadata(args))
    return request


def _pending_request(args: argparse.Namespace, api: object) -> dict[str, Any]:
    if args.pending_kind.endswith("stop-limit"):
        if args.stop_limit_price is None:
            raise ValueError(f"{args.pending_kind} requires --stop-limit-price.")
    elif args.stop_limit_price is not None:
        raise ValueError("--stop-limit-price is valid only for buy-stop-limit and sell-stop-limit.")
    expiration = _time_request(args, api=api, symbol=args.symbol)
    request = {
        "action": _constant(api, "TRADE_ACTION_PENDING"),
        "symbol": args.symbol,
        "volume": args.volume,
        "type": _constant(api, f"ORDER_TYPE_{args.pending_kind.upper().replace('-', '_')}"),
        "price": args.price,
        "type_filling": _filling_type(api, args.fill),
        "type_time": _time_type(api, args.time_in_force),
    }
    if args.stop_limit_price is not None:
        request["stoplimit"] = args.stop_limit_price
    if expiration is not None:
        request["expiration"] = expiration
    request.update(_new_protections(args, api, args.symbol, args.price, args.side))
    request.update(_metadata(args))
    return request


def _pending_modify_request(args: argparse.Namespace, api: object) -> dict[str, Any]:
    order = _pending_order(api, args.ticket)
    order_type = _field(order, "type")
    stop_limit_types = {
        _constant(api, "ORDER_TYPE_BUY_STOP_LIMIT"),
        _constant(api, "ORDER_TYPE_SELL_STOP_LIMIT"),
    }
    if args.stop_limit_price is not None and order_type not in stop_limit_types:
        raise ValueError("--stop-limit-price is valid only for a stop-limit order.")
    type_time, time_name = _existing_or_requested_time(args, api, order)
    expiration = _time_request(
        args,
        api=api,
        symbol=_field(order, "symbol"),
        time_name=time_name,
        existing=_field(order, "time_expiration"),
    )
    price = args.price if args.price is not None else _positive_existing_price(order, "price_open")
    request = {
        "action": _constant(api, "TRADE_ACTION_MODIFY"),
        "order": args.ticket,
        "price": price,
        "sl": _modified_protection(args, "sl", api, _field(order, "symbol"), price, _order_side(api, order_type), order),
        "tp": _modified_protection(args, "tp", api, _field(order, "symbol"), price, _order_side(api, order_type), order),
        "type_time": type_time,
    }
    if expiration is not None:
        request["expiration"] = expiration
    if order_type in stop_limit_types:
        stoplimit = args.stop_limit_price
        if stoplimit is None:
            stoplimit = _positive_existing_price(order, "price_stoplimit")
        request["stoplimit"] = stoplimit
    return request


def _position_modify_request(args: argparse.Namespace, api: object) -> dict[str, Any]:
    if not _has_protection_change(args):
        raise ValueError("Specify an SL or TP value, or --clear-sl/--clear-tp.")
    position, hedging = _selected_position(args, api)
    side = _position_side(api, position)
    base_price = _positive_existing_price(position, "price_open")
    request = {
        "action": _constant(api, "TRADE_ACTION_SLTP"),
        "sl": _modified_protection(args, "sl", api, _field(position, "symbol"), base_price, side, position),
        "tp": _modified_protection(args, "tp", api, _field(position, "symbol"), base_price, side, position),
    }
    if hedging:
        request["position"] = _field(position, "ticket")
    else:
        request["symbol"] = _field(position, "symbol")
    return request


def _position_close_request(args: argparse.Namespace, api: object) -> dict[str, Any]:
    position, hedging = _selected_position(args, api)
    current_volume = _positive_existing_price(position, "volume")
    volume = args.volume if args.volume is not None else current_volume
    if volume > current_volume:
        raise ValueError(f"--volume cannot exceed the open position volume ({current_volume}).")
    close_side = "sell" if _position_side(api, position) == "buy" else "buy"
    request = {
        "action": _constant(api, "TRADE_ACTION_DEAL"),
        "symbol": _field(position, "symbol"),
        "volume": volume,
        "type": _order_type(api, close_side),
        "price": _executable_price(api, _field(position, "symbol"), close_side),
        "deviation": args.deviation_points,
        "type_filling": _filling_type(api, args.fill),
    }
    if hedging:
        request["position"] = _field(position, "ticket")
    request.update(_metadata(args))
    return request


def _close_by_request(args: argparse.Namespace, api: object) -> dict[str, Any]:
    if not _is_hedging_account(api):
        raise ValueError("close-by is available only for hedging accounts.")
    position = _position_for_ticket(api, args.ticket)
    position_by = _position_for_ticket(api, args.position_by)
    if _field(position, "symbol") != _field(position_by, "symbol"):
        raise ValueError("close-by positions must use the same symbol.")
    if _position_side(api, position) == _position_side(api, position_by):
        raise ValueError("close-by positions must be in opposite directions.")
    return {
        "action": _constant(api, "TRADE_ACTION_CLOSE_BY"),
        "position": _field(position, "ticket"),
        "position_by": _field(position_by, "ticket"),
    }


def _check_then_send(
    request: dict[str, Any],
    api: object,
    *,
    expiration_plan: ExpirationPlan | None = None,
    expiration_symbol: str | None = None,
    successful_send_callback: Callable[[Mapping[str, Any], object, datetime, datetime], None] | None = None,
) -> dict[str, Any]:
    mapping = (
        _map_expiration(expiration_plan, api, expiration_symbol)
        if expiration_plan is not None and expiration_symbol is not None
        else None
    )
    if mapping is not None:
        request = {**request, "expiration": mapping.broker_expiration}
    expiration = _expiration_details(mapping)

    def result(**values: Any) -> dict[str, Any]:
        if expiration is not None:
            values["expiration"] = expiration
        return values

    check = api.order_check(request)
    if check is None:
        return result(
            request=request,
            check=None,
            sent=False,
            error=f"MT5 order check failed: {_last_error(api)}",
        )
    retcode = _field(check, "retcode")
    if retcode != 0:
        return result(
            request=request,
            check=check,
            sent=False,
            error="MT5 order check rejected the request; it was not sent.",
        )
    sent_before_utc = datetime.now(UTC)
    sent = api.order_send(request)
    sent_after_utc = datetime.now(UTC)
    if sent is None:
        return result(
            request=request,
            check=check,
            send=None,
            sent=False,
            error=f"MT5 order send failed: {_last_error(api)}",
        )
    if _field(sent, "retcode") != _constant(api, "TRADE_RETCODE_DONE"):
        return result(
            request=request,
            check=check,
            send=sent,
            sent=False,
            error="MT5 broker rejected the request.",
        )
    if successful_send_callback is not None:
        successful_send_callback(request, sent, sent_before_utc, sent_after_utc)
    actual_expiration = _actual_expiration(api, sent)
    if expiration is not None and actual_expiration is not None:
        expiration["actual_broker_expiration"] = actual_expiration
    if (
        mapping is not None
        and expiration_plan is not None
        and expiration_plan.duration is not None
        and (
            actual_expiration is None
            or actual_expiration < mapping.minimum_broker_expiration
        )
    ):
        cancellation = _cancel_pending_order(api, sent)
        reason = (
            "Broker expiration could not be verified"
            if actual_expiration is None
            else "Broker normalized expiration earlier than the requested duration"
        )
        return result(
            request=request,
            check=check,
            send=sent,
            sent=False,
            cancellation=cancellation,
            error=f"{reason}; the pending order was cancelled.",
        )
    return result(request=request, check=check, send=sent, sent=True)


def _new_protections(
    args: argparse.Namespace, api: object, symbol: str, base_price: float, side: str
) -> dict[str, float]:
    values: dict[str, float] = {}
    for name in ("sl", "tp"):
        value = _protection_value(args, name, api, symbol, base_price, side)
        if value is not None:
            values[name] = value
    return values


def _modified_protection(
    args: argparse.Namespace,
    name: str,
    api: object,
    symbol: str,
    base_price: float,
    side: str,
    existing: object,
) -> float:
    if getattr(args, f"clear_{name}", False):
        return 0.0
    value = _protection_value(args, name, api, symbol, base_price, side)
    if value is not None:
        return value
    return float(_field(existing, name, 0.0) or 0.0)


def _protection_value(
    args: argparse.Namespace, name: str, api: object, symbol: str, base_price: float, side: str
) -> float | None:
    absolute = getattr(args, name, None)
    if absolute is not None:
        return absolute
    points = getattr(args, f"{name}_points", None)
    pips = getattr(args, f"{name}_pips", None)
    percent = getattr(args, f"{name}_percent", None)
    if points is None and pips is None and percent is None:
        return None
    info = _symbol_info(api, symbol)
    if points is not None:
        distance = points * _positive_existing_price(info, "point")
        value = base_price + _protection_sign(name, side) * distance
    elif pips is not None:
        value = base_price + _protection_sign(name, side) * pips * _forex_pip_size(api, info, symbol)
    else:
        value = base_price * (1 + _protection_sign(name, side) * percent / 100)
    if value <= 0:
        raise ValueError(f"Calculated {name.upper()} must be greater than zero.")
    digits = int(_field(info, "digits"))
    return float(Decimal(str(value)).quantize(Decimal(1).scaleb(-digits), rounding=ROUND_HALF_UP))


def _protection_sign(name: str, side: str) -> int:
    if name == "sl":
        return -1 if side == "buy" else 1
    return 1 if side == "buy" else -1


def _forex_pip_size(api: object, info: object, symbol: str) -> float:
    calc_mode = _field(info, "trade_calc_mode")
    forex_modes = (
        getattr(api, "SYMBOL_CALC_MODE_FOREX", None),
        getattr(api, "SYMBOL_CALC_MODE_FOREX_NO_LEVERAGE", None),
    )
    if calc_mode is None or not any(mode is not None and calc_mode == mode for mode in forex_modes):
        raise ValueError(f"--sl-pips/--tp-pips require a Forex symbol; {symbol!r} is not Forex.")
    return 0.01 if _field(info, "currency_profit") == "JPY" else 0.0001


@dataclass(frozen=True)
class ExpirationPlan:
    intent_utc: datetime | None
    duration: timedelta | None
    existing_broker_expiration: int | None


@dataclass(frozen=True)
class ExpirationMapping:
    intent_utc: datetime | None
    broker_expiration: int
    minimum_broker_expiration: int
    source: str


def _time_request(
    args: argparse.Namespace,
    *,
    api: object,
    symbol: str,
    time_name: str | None = None,
    existing: object | None = None,
) -> int | None:
    plan = _expiration_plan(args, time_name=time_name, existing=existing)
    if plan is None:
        return None
    mapping = _map_expiration(plan, api, symbol)
    args._expiration_plan = plan
    args._expiration_mapping = mapping
    args._expiration_symbol = symbol
    return mapping.broker_expiration


def _expiration_plan(
    args: argparse.Namespace, *, time_name: str | None = None, existing: object | None = None
) -> ExpirationPlan | None:
    time_name = time_name or args.time_in_force
    expires_in = args.expires_in
    expires_at = args.expires_at
    if (expires_in is not None or expires_at is not None) and time_name != "specified":
        raise ValueError("--expires-in and --expires-at require --time specified.")
    if time_name != "specified":
        return None
    if expires_in is not None:
        return ExpirationPlan(intent_utc=None, duration=expires_in, existing_broker_expiration=None)
    if expires_at is not None:
        return ExpirationPlan(
            intent_utc=_utc_timestamp(expires_at, getattr(args, "user_timezone", None)),
            duration=None,
            existing_broker_expiration=None,
        )
    if existing is not None:
        return ExpirationPlan(
            intent_utc=None,
            duration=None,
            existing_broker_expiration=_existing_broker_expiration(existing),
        )
    raise ValueError("--time specified requires exactly one of --expires-in or --expires-at.")


def _map_expiration(plan: ExpirationPlan, api: object, symbol: str) -> ExpirationMapping:
    if plan.existing_broker_expiration is not None:
        return ExpirationMapping(
            intent_utc=None,
            broker_expiration=plan.existing_broker_expiration,
            minimum_broker_expiration=plan.existing_broker_expiration,
            source="existing pending order",
        )
    broker_now = _broker_epoch(api, symbol)
    if plan.duration is not None:
        minimum = broker_now + math.ceil(plan.duration.total_seconds())
        return ExpirationMapping(
            intent_utc=datetime.now(UTC) + plan.duration,
            broker_expiration=_round_expiration_up_to_minute(minimum),
            minimum_broker_expiration=minimum,
            source=f"symbol_info_tick({symbol}).time",
        )
    if plan.intent_utc is None:
        raise RuntimeError("Expiration plan has no expiration value.")
    minimum = broker_now + math.ceil((plan.intent_utc - datetime.now(UTC)).total_seconds())
    return ExpirationMapping(
        intent_utc=plan.intent_utc,
        broker_expiration=_round_expiration_up_to_minute(minimum),
        minimum_broker_expiration=minimum,
        source=f"symbol_info_tick({symbol}).time",
    )


def _round_expiration_up_to_minute(epoch: int) -> int:
    return ((epoch + 59) // 60) * 60


def _expiration_details(mapping: ExpirationMapping | None) -> dict[str, Any] | None:
    if mapping is None:
        return None
    details: dict[str, Any] = {
        "broker_expiration": mapping.broker_expiration,
        "broker_time_source": mapping.source,
    }
    if mapping.intent_utc is not None:
        details["intent_utc"] = mapping.intent_utc.isoformat().replace("+00:00", "Z")
    return details


def _broker_epoch(api: object, symbol: str) -> int:
    tick = api.symbol_info_tick(symbol)
    if tick is None:
        raise SessionError(f"No server-time tick for {symbol!r}: {_last_error(api)}")
    epoch = _field(tick, "time")
    if not isinstance(epoch, int) or epoch <= 0:
        raise SessionError(f"MT5 returned no valid server-time epoch for {symbol!r}.")
    return epoch


def _existing_broker_expiration(value: object) -> int:
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    return int(_expiration_datetime(value).timestamp())


def _actual_expiration(api: object, sent: object) -> int | None:
    ticket = _field(sent, "order")
    if not isinstance(ticket, int) or ticket <= 0:
        return None
    getter = getattr(api, "orders_get", None)
    if not callable(getter):
        return None
    orders = getter(ticket=ticket)
    if orders is None:
        return None
    order = next((item for item in orders if _field(item, "ticket") == ticket), None)
    if order is None:
        return None
    value = _field(order, "time_expiration")
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    if isinstance(value, datetime) and value.tzinfo is not None:
        return int(value.timestamp())
    return None


def _cancel_pending_order(api: object, sent: object) -> object | None:
    ticket = _field(sent, "order")
    if not isinstance(ticket, int) or ticket <= 0:
        return None
    return api.order_send({"action": _constant(api, "TRADE_ACTION_REMOVE"), "order": ticket})


def _existing_or_requested_time(args: argparse.Namespace, api: object, order: object) -> tuple[int, str]:
    if args.time_in_force is not None:
        return _time_type(api, args.time_in_force), args.time_in_force
    current = _field(order, "type_time")
    for name in ("gtc", "day", "specified"):
        if current == _time_type(api, name):
            return current, name
    raise ValueError("The existing pending order has an unsupported time-in-force mode.")


def _symbol_info(api: object, symbol: str) -> object:
    info = api.symbol_info(symbol)
    if info is None:
        raise SessionError(f"Unknown symbol {symbol!r}: {_last_error(api)}")
    return info


def _executable_price(api: object, symbol: str, side: str) -> float:
    tick = api.symbol_info_tick(symbol)
    if tick is None:
        raise SessionError(f"No executable quote for {symbol!r}: {_last_error(api)}")
    field = "ask" if side == "buy" else "bid"
    price = _field(tick, field)
    if not isinstance(price, (int, float)) or not math.isfinite(price) or price <= 0:
        raise SessionError(f"MT5 returned an invalid {field} quote for {symbol!r}.")
    return float(price)


def _pending_order(api: object, ticket: int) -> object:
    records = api.orders_get(ticket=ticket)
    if records is None:
        raise SessionError(f"Unable to read pending order {ticket}: {_last_error(api)}")
    values = list(records)
    if len(values) != 1:
        raise ValueError(f"No unique pending order exists for ticket {ticket}.")
    order = values[0]
    pending_types = {_constant(api, f"ORDER_TYPE_{name.upper().replace('-', '_')}") for name in _PENDING_ORDER_SIDES}
    if _field(order, "type") not in pending_types:
        raise ValueError(f"Ticket {ticket} is not a pending order.")
    return order


def _selected_position(args: argparse.Namespace, api: object) -> tuple[object, bool]:
    hedging = _is_hedging_account(api)
    if hedging:
        if args.ticket is None:
            raise ValueError("Hedging account position operations require --ticket.")
        if args.symbol is not None:
            raise ValueError("Hedging account position operations identify the position by --ticket, not --symbol.")
        return _position_for_ticket(api, args.ticket), True
    if args.symbol is None:
        raise ValueError("Netting account position operations require --symbol.")
    if args.ticket is not None:
        raise ValueError("Netting account position operations identify the position by --symbol, not --ticket.")
    records = api.positions_get(symbol=args.symbol)
    if records is None:
        raise SessionError(f"Unable to read position for {args.symbol!r}: {_last_error(api)}")
    values = list(records)
    if len(values) != 1:
        raise ValueError(f"No unique netting position exists for symbol {args.symbol!r}.")
    return values[0], False


def _position_for_ticket(api: object, ticket: int) -> object:
    records = api.positions_get(ticket=ticket)
    if records is None:
        raise SessionError(f"Unable to read position {ticket}: {_last_error(api)}")
    values = list(records)
    if len(values) != 1:
        raise ValueError(f"No unique position exists for ticket {ticket}.")
    return values[0]


def _is_hedging_account(api: object) -> bool:
    account = api.account_info()
    if account is None:
        raise SessionError(f"Unable to read account mode: {_last_error(api)}")
    return _field(account, "margin_mode") == _constant(api, "ACCOUNT_MARGIN_MODE_RETAIL_HEDGING")


def _position_side(api: object, position: object) -> str:
    return _order_side(api, _field(position, "type"), position=True)


def _order_side(api: object, order_type: object, *, position: bool = False) -> str:
    buy = _constant(api, "POSITION_TYPE_BUY" if position else "ORDER_TYPE_BUY")
    sell = _constant(api, "POSITION_TYPE_SELL" if position else "ORDER_TYPE_SELL")
    if order_type == buy:
        return "buy"
    if order_type == sell:
        return "sell"
    if not position:
        pending_buy_types = {
            _constant(api, "ORDER_TYPE_BUY_LIMIT"),
            _constant(api, "ORDER_TYPE_BUY_STOP"),
            _constant(api, "ORDER_TYPE_BUY_STOP_LIMIT"),
        }
        if order_type in pending_buy_types:
            return "buy"
        return "sell"
    raise ValueError("MT5 returned an unknown position direction.")


def _order_type(api: object, side: str) -> int:
    return _constant(api, f"ORDER_TYPE_{side.upper()}")


def _filling_type(api: object, name: str) -> int:
    return _constant(api, f"ORDER_FILLING_{name.upper()}")


def _time_type(api: object, name: str) -> int:
    return _constant(api, f"ORDER_TIME_{name.upper()}")


def _metadata(args: argparse.Namespace) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    if getattr(args, "magic", None) is not None:
        metadata["magic"] = args.magic
    if getattr(args, "comment", None) is not None:
        metadata["comment"] = args.comment
    return metadata


def _has_protection_change(args: argparse.Namespace) -> bool:
    return any(
        getattr(args, name, None) is not None or getattr(args, f"clear_{name}", False)
        for name in ("sl", "tp", "sl_points", "tp_points", "sl_pips", "tp_pips", "sl_percent", "tp_percent")
    )


def _positive_existing_price(value: object, field: str) -> float:
    raw = _field(value, field)
    if not isinstance(raw, (int, float)) or not math.isfinite(raw) or raw <= 0:
        raise ValueError(f"MT5 returned an invalid {field} value.")
    return float(raw)


def _expiration_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("MT5 returned an expiration without a UTC offset.")
        return value.astimezone(UTC)
    if isinstance(value, (int, float)) and value > 0:
        return datetime.fromtimestamp(value, UTC)
    raise ValueError("MT5 returned no valid expiration for the specified pending order.")


def _field(value: object, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _constant(api: object, name: str) -> int:
    value = getattr(api, name, None)
    if not isinstance(value, int):
        raise RuntimeError(f"MT5 does not expose required constant {name}.")
    return value


def _last_error(api: object) -> Any:
    getter = getattr(api, "last_error", None)
    return getter() if callable(getter) else "unknown MT5 error"


def _get_records(getter: Callable[..., Any], args: argparse.Namespace, api: object) -> Any:
    filters = [name for name in ("symbol", "ticket", "group") if getattr(args, name, None) is not None]
    if len(filters) > 1:
        raise ValueError(f"Only one of {', '.join('--' + name for name in filters)} may be used.")
    keyword = {filters[0]: getattr(args, filters[0])} if filters else {}
    records = getter(**keyword)
    if records is None:
        raise SessionError(f"MT5 query failed: {api.last_error()}")
    return records


def _get_history(getter: Callable[..., Any], start: datetime, end: datetime, args: argparse.Namespace) -> Any:
    filters = [name for name in ("group", "ticket", "position") if getattr(args, name, None) is not None]
    if len(filters) > 1:
        raise ValueError(f"Only one of {', '.join('--' + name for name in filters)} may be used.")
    keyword = {filters[0]: getattr(args, filters[0])} if filters else {}
    records = getter(start, end, **keyword)
    if records is None:
        raise SessionError("MT5 history query failed.")
    return records


def _market_book(api: object, symbol: str, watch_seconds: int) -> Any:
    if not api.market_book_add(symbol):
        raise SessionError(f"Market depth subscription failed: {api.last_error()}")
    try:
        snapshots = []
        deadline = time.monotonic() + watch_seconds
        while True:
            book = api.market_book_get(symbol)
            if book is None:
                raise SessionError(f"Market depth query failed: {api.last_error()}")
            snapshots.append({"received_at": datetime.now(UTC), "levels": book})
            if time.monotonic() >= deadline:
                return snapshots[0] if watch_seconds == 0 else snapshots
            time.sleep(1)
    finally:
        api.market_book_release(symbol)


def _rates(api: object, args: argparse.Namespace) -> list[dict[str, Any]]:
    if args.command == "rates-from":
        values = api.copy_rates_from(
            args.symbol,
            args.timeframe,
            _market_data_query_time(args.from_time, args),
            args.count,
        )
    elif args.command == "rates-from-pos":
        if args.start_pos < 0:
            raise ValueError("--start-pos cannot be negative.")
        values = api.copy_rates_from_pos(args.symbol, args.timeframe, args.start_pos, args.count)
    else:
        from_time = _market_data_query_time(args.from_time, args)
        to_time = _market_data_query_time(args.to_time, args)
        if from_time > to_time:
            raise ValueError("--from must not be after --to.")
        values = api.copy_rates_range(args.symbol, args.timeframe, from_time, to_time)
    if values is None:
        raise SessionError(f"MT5 rates query failed: {api.last_error()}")
    return _structured_records(values)


def _ticks(api: object, args: argparse.Namespace) -> list[dict[str, Any]]:
    flags = {"all": api.COPY_TICKS_ALL, "info": api.COPY_TICKS_INFO, "trade": api.COPY_TICKS_TRADE}[args.flags]
    if args.command == "ticks-from":
        values = api.copy_ticks_from(
            args.symbol,
            _market_data_query_time(args.from_time, args),
            args.count,
            flags,
        )
    else:
        from_time = _market_data_query_time(args.from_time, args)
        to_time = _market_data_query_time(args.to_time, args)
        if from_time > to_time:
            raise ValueError("--from must not be after --to.")
        values = api.copy_ticks_range(args.symbol, from_time, to_time, flags)
    if values is None:
        raise SessionError(f"MT5 ticks query failed: {api.last_error()}")
    return _structured_records(values)


def _structured_records(values: Any) -> list[dict[str, Any]]:
    names = values.dtype.names
    if names is None:
        raise ValueError("MT5 returned an unexpected non-structured market-data result.")
    return [{name: row[name].item() if hasattr(row[name], "item") else row[name] for name in names} for row in values]


def _selected_context(args: argparse.Namespace, loaded_config: Config) -> Context:
    name = args.context or loaded_config.current_context
    if name is None:
        raise ValueError("No current context. Run `abt context add` and `abt context use` first.")
    return _named_context(name, loaded_config)


def _named_context(name: str, loaded_config: Config) -> Context:
    try:
        return loaded_config.contexts[name]
    except KeyError as error:
        raise ValueError(f"Unknown context {name!r}.") from error


def _required_user_timezone(context: Context) -> ZoneInfo:
    if context.user_timezone is None:
        raise ValueError(
            f"Context {context.name!r} has no user_timezone. "
            f"Run `abt context set-timezone {context.name} <IANA_TIMEZONE>` first."
        )
    return ZoneInfo(context.user_timezone)


def _required(value: Any, message: str, api: object) -> Any:
    if value is None:
        raise SessionError(f"{message}: {api.last_error()}")
    return value


def _history_window(args: argparse.Namespace, user_timezone: ZoneInfo | None = None) -> tuple[datetime, datetime]:
    if args.since is not None and (args.from_date is not None or args.to_date is not None):
        raise ValueError("--since cannot be combined with --from or --to.")
    now = datetime.now(UTC)
    if args.since is not None:
        return now - args.since, now
    if (args.from_date is not None or args.to_date is not None) and user_timezone is None:
        raise ValueError("A context user_timezone is required for history date ranges.")
    start = (
        datetime.combine(args.from_date, datetime.min.time(), user_timezone).astimezone(UTC)
        if args.from_date
        else now - timedelta(days=7)
    )
    end = (
        datetime.combine(args.to_date, datetime.max.time(), user_timezone).astimezone(UTC)
        if args.to_date
        else now
    )
    if start > end:
        raise ValueError("--from must not be after --to.")
    return start, end


def _date(value: str) -> datetime.date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as error:
        raise argparse.ArgumentTypeError("Dates must use YYYY-MM-DD.") from error


def _datetime_input(value: str) -> str:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise argparse.ArgumentTypeError("Timestamps must use ISO-8601.") from error
    return value


def _timestamp(value: str, user_timezone: ZoneInfo | None = None) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise argparse.ArgumentTypeError("Timestamps must use ISO-8601.") from error
    if parsed.tzinfo is None:
        if user_timezone is None:
            raise ValueError("An unoffset datetime requires the context user_timezone.")
        return _local_datetime_to_utc(parsed, user_timezone)
    return parsed.astimezone(UTC)


def _utc_timestamp(value: str, user_timezone: ZoneInfo | None = None) -> datetime:
    return _timestamp(value, user_timezone)


def _market_data_query_time(value: str, args: argparse.Namespace) -> datetime:
    utc_time = _timestamp(value, args.user_timezone)
    calibration = getattr(args, "time_calibration", None)
    offset = calibration.get("offset_seconds") if isinstance(calibration, Mapping) else None
    if not isinstance(offset, int) or isinstance(offset, bool):
        return utc_time
    return utc_time + timedelta(seconds=offset)


def _local_datetime_to_utc(value: datetime, user_timezone: ZoneInfo) -> datetime:
    candidates = [
        value.replace(tzinfo=user_timezone, fold=fold).astimezone(UTC)
        for fold in (0, 1)
    ]
    valid = [candidate for candidate in candidates if candidate.astimezone(user_timezone).replace(tzinfo=None) == value]
    if not valid:
        raise argparse.ArgumentTypeError(
            f"{value.isoformat()} does not exist in {user_timezone.key} because of a DST transition."
        )
    if len(set(valid)) != 1:
        raise argparse.ArgumentTypeError(
            f"{value.isoformat()} is ambiguous in {user_timezone.key} because of a DST transition; include an offset."
        )
    return valid[0]


def _timeframe(value: str) -> int:
    name = value.upper()
    if not name.startswith("TIMEFRAME_"):
        name = f"TIMEFRAME_{name}"
    try:
        return int(getattr(mt5, name))
    except (AttributeError, TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError(f"Unknown MT5 timeframe: {value}") from error


def _duration(value: str) -> timedelta:
    if len(value) < 2 or value[-1] not in {"h", "d"} or not value[:-1].isdigit() or int(value[:-1]) <= 0:
        raise argparse.ArgumentTypeError("--since must be a positive integer followed by h or d.")
    return timedelta(**({"hours": int(value[:-1])} if value[-1] == "h" else {"days": int(value[:-1])}))


def _positive_seconds(value: str) -> int:
    try:
        seconds = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("--watch must be a whole number of seconds.") from error
    if seconds < 0:
        raise argparse.ArgumentTypeError("--watch cannot be negative.")
    return seconds


def _positive_count(value: str) -> int:
    try:
        count = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("--count must be a whole number.") from error
    if count <= 0:
        raise argparse.ArgumentTypeError("--count must be greater than zero.")
    return count


def _positive_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("Value must be a number.") from error
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("Value must be a finite number greater than zero.")
    return number


def _nonnegative_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("Value must be a whole number.") from error
    if number < 0:
        raise argparse.ArgumentTypeError("Value cannot be negative.")
    return number


def _positive_ticket(value: str) -> int:
    ticket = _nonnegative_int(value)
    if ticket == 0:
        raise argparse.ArgumentTypeError("Ticket must be greater than zero.")
    return ticket


def _expiration_duration(value: str) -> timedelta:
    if len(value) < 2 or value[-1] not in {"m", "h", "d"} or not value[:-1].isdigit() or int(value[:-1]) <= 0:
        raise argparse.ArgumentTypeError("--expires-in must be a positive integer followed by m, h, or d.")
    amount = int(value[:-1])
    unit = {"m": "minutes", "h": "hours", "d": "days"}[value[-1]]
    return timedelta(**{unit: amount})


def _comment(value: str) -> str:
    if len(value) > 31:
        raise argparse.ArgumentTypeError("--comment cannot be longer than 31 characters.")
    return value


def _json_object(value: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as error:
        raise argparse.ArgumentTypeError(f"--request-json is not valid JSON: {error.msg}") from error
    if not isinstance(parsed, dict):
        raise argparse.ArgumentTypeError("--request-json must be a JSON object.")
    return parsed


def _confirm(prompt: str) -> bool:
    while True:
        answer = input(f"{prompt} [yes/no]: ").strip().lower()
        if answer == "yes":
            return True
        if answer == "no":
            return False
        print("Please answer literal yes or no.")


if __name__ == "__main__":
    raise SystemExit(main())
