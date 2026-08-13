"""THROWAWAY PROTOTYPE: verify read-only Python-to-MT5 connectivity."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import MetaTrader5 as mt5


DEFAULT_TERMINAL_PATH = Path(r"C:\Program Files\MetaTrader 5\terminal64.exe")
DEFAULT_SYMBOLS = ("EURUSD", "GBPUSD", "USDCHF", "USDJPY")


def value(record: object, name: str) -> object:
    return getattr(record, name, None)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read account, terminal, and quote data without sending trade requests."
    )
    parser.add_argument("symbols", nargs="*", default=DEFAULT_SYMBOLS)
    parser.add_argument(
        "--terminal-path",
        type=Path,
        default=Path(os.environ.get("MT5_TERMINAL_PATH", DEFAULT_TERMINAL_PATH)),
    )
    args = parser.parse_args()

    if not args.terminal_path.is_file():
        print(f"Terminal not found: {args.terminal_path}", file=sys.stderr)
        return 2

    if not mt5.initialize(path=str(args.terminal_path), timeout=10_000):
        print(f"MT5 initialization failed: {mt5.last_error()}", file=sys.stderr)
        return 1

    try:
        terminal = mt5.terminal_info()
        account = mt5.account_info()
        if terminal is None or account is None:
            print(f"MT5 returned no terminal/account information: {mt5.last_error()}", file=sys.stderr)
            return 1

        print("READ-ONLY PROTOTYPE: no trade request has been sent.")
        print(
            "terminal:",
            {
                "connected": value(terminal, "connected"),
                "trade_allowed": value(terminal, "trade_allowed"),
                "tradeapi_disabled": value(terminal, "tradeapi_disabled"),
                "ping_last_us": value(terminal, "ping_last"),
            },
        )
        print(
            "account:",
            {
                "server": value(account, "server"),
                "company": value(account, "company"),
                "currency": value(account, "currency"),
                "trade_mode": value(account, "trade_mode"),
                "trade_allowed": value(account, "trade_allowed"),
                "balance": value(account, "balance"),
                "equity": value(account, "equity"),
                "margin_free": value(account, "margin_free"),
            },
        )

        for symbol in args.symbols:
            info = mt5.symbol_info(symbol)
            tick = mt5.symbol_info_tick(symbol)
            if info is None:
                print(f"{symbol}: unavailable ({mt5.last_error()})")
                continue
            print(
                f"{symbol}:",
                {
                    "visible": value(info, "visible"),
                    "trade_mode": value(info, "trade_mode"),
                    "filling_mode": value(info, "filling_mode"),
                    "volume_min": value(info, "volume_min"),
                    "volume_max": value(info, "volume_max"),
                    "volume_step": value(info, "volume_step"),
                    "digits": value(info, "digits"),
                    "point": value(info, "point"),
                    "bid": value(tick, "bid"),
                    "ask": value(tick, "ask"),
                    "tick_time_msc": value(tick, "time_msc"),
                },
            )
    finally:
        mt5.shutdown()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
