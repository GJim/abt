from __future__ import annotations

import argparse
import json
import math
import os
import selectors
import struct
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from protocol import MAX_FRAME_BYTES, PROTOCOL_VERSION, ProtocolError, decode_json_object, encode_frame

HERE = Path(__file__).resolve().parent
DEFAULT_WINE_PREFIX = Path.home() / ".mt5"
DEFAULT_WINDOWS_PYTHON = r"C:\abt-python313\python.exe"
MAX_DIAGNOSTIC_BYTES = 64 * 1024
SAFE_ENV_KEYS = {
    "DISPLAY",
    "HOME",
    "LANG",
    "LC_ALL",
    "PATH",
    "USER",
    "WAYLAND_DISPLAY",
    "XAUTHORITY",
    "XDG_RUNTIME_DIR",
}


class BridgeClientError(RuntimeError):
    pass


def _wine_path(path: Path) -> str:
    return "Z:" + str(path.resolve()).replace("/", "\\")


def _write_all_before(stream: object, data: bytes, deadline: float) -> None:
    fileno = getattr(stream, "fileno", None)
    if not callable(fileno):
        raise BridgeClientError("bridge stdin has no file descriptor")
    raw_fd = fileno()
    if not isinstance(raw_fd, int):
        raise BridgeClientError("bridge stdin file descriptor is invalid")
    fd = raw_fd
    offset = 0
    selector = selectors.DefaultSelector()
    selector.register(fd, selectors.EVENT_WRITE)
    os.set_blocking(fd, False)
    try:
        while offset < len(data):
            timeout = deadline - time.monotonic()
            if timeout <= 0 or not selector.select(timeout):
                raise BridgeClientError("bridge request timed out while writing a frame")
            try:
                written = os.write(fd, data[offset:])
            except BlockingIOError:
                continue
            if written <= 0:
                raise BridgeClientError("bridge request pipe closed while writing a frame")
            offset += written
    except BrokenPipeError as error:
        raise BridgeClientError("bridge request pipe closed while writing a frame") from error
    finally:
        os.set_blocking(fd, True)
        selector.close()


def _read_exact_before(stream: object, size: int, deadline: float) -> bytes:
    fileno = getattr(stream, "fileno", None)
    if not callable(fileno):
        raise BridgeClientError("bridge stdout has no file descriptor")
    raw_fd = fileno()
    if not isinstance(raw_fd, int):
        raise BridgeClientError("bridge stdout file descriptor is invalid")
    fd = raw_fd
    chunks: list[bytes] = []
    remaining = size
    selector = selectors.DefaultSelector()
    selector.register(fd, selectors.EVENT_READ)
    try:
        while remaining:
            timeout = deadline - time.monotonic()
            if timeout <= 0 or not selector.select(timeout):
                raise BridgeClientError("bridge response timed out while reading a frame")
            chunk = os.read(fd, remaining)
            if not chunk:
                raise ProtocolError("unexpected EOF while reading frame")
            chunks.append(chunk)
            remaining -= len(chunk)
    finally:
        selector.close()
    return b"".join(chunks)


def _read_response_before(stream: object, deadline: float) -> dict[str, Any]:
    size = struct.unpack(">I", _read_exact_before(stream, 4, deadline))[0]
    if size > MAX_FRAME_BYTES:
        raise ProtocolError(f"frame size {size} exceeds maximum {MAX_FRAME_BYTES}")
    return decode_json_object(_read_exact_before(stream, size, deadline))


def _validate_response(response: dict[str, Any]) -> None:
    if response.get("ok") is True:
        expected = {"version", "id", "ok", "result"}
        if set(response) != expected:
            raise BridgeClientError("malformed bridge response: success schema mismatch")
        return
    if response.get("ok") is False:
        expected = {"version", "id", "ok", "error"}
        error = response.get("error")
        if set(response) != expected or not isinstance(error, dict) or set(error) != {"type", "message"}:
            raise BridgeClientError("malformed bridge response: error schema mismatch")
        if not isinstance(error.get("type"), str) or not isinstance(error.get("message"), str):
            raise BridgeClientError("malformed bridge response: invalid error fields")
        return
    raise BridgeClientError("malformed bridge response: ok must be boolean")


class BridgeClient:
    def __init__(
        self,
        *,
        wine_prefix: Path,
        windows_python: str,
        timeout_seconds: float = 15.0,
    ) -> None:
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(float(timeout_seconds))
            or timeout_seconds <= 0
            or timeout_seconds > 300
        ):
            raise BridgeClientError("timeout_seconds must be finite and between 0 and 300")
        env = {key: value for key, value in os.environ.items() if key in SAFE_ENV_KEYS}
        env["WINEPREFIX"] = str(wine_prefix.resolve())
        env["WINEDEBUG"] = "-all"
        self.timeout_seconds = timeout_seconds
        self.process = subprocess.Popen(
            ["wine", windows_python, _wine_path(HERE / "bridge.py")],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            shell=False,
        )
        self._stderr_buffer = bytearray()
        self._stderr_thread = threading.Thread(
            target=self._drain_stderr,
            name="wine-mt5-bridge-stderr",
            daemon=True,
        )
        self._stderr_thread.start()

    def _drain_stderr(self) -> None:
        stream = self.process.stderr
        if stream is None:
            return
        while True:
            chunk = stream.read(4096)
            if not chunk:
                return
            self._stderr_buffer.extend(chunk)
            overflow = len(self._stderr_buffer) - MAX_DIAGNOSTIC_BYTES
            if overflow > 0:
                del self._stderr_buffer[:overflow]

    def request(self, operation: str, params: dict[str, Any] | None = None) -> Any:
        if self.process.stdin is None or self.process.stdout is None:
            raise BridgeClientError("bridge pipes are unavailable")
        request_id = str(uuid.uuid4())
        deadline = time.monotonic() + self.timeout_seconds
        try:
            frame = encode_frame(
                {
                    "version": PROTOCOL_VERSION,
                    "id": request_id,
                    "operation": operation,
                    "params": {} if params is None else params,
                }
            )
            _write_all_before(self.process.stdin, frame, deadline)
        except ProtocolError as error:
            raise BridgeClientError("bridge request could not be encoded") from error

        try:
            response = _read_response_before(self.process.stdout, deadline)
        except ProtocolError as error:
            raise BridgeClientError(f"invalid bridge response: {error}") from error
        _validate_response(response)
        version = response.get("version")
        if isinstance(version, bool) or not isinstance(version, int) or version != PROTOCOL_VERSION:
            raise BridgeClientError("bridge returned an unsupported protocol version")
        if response.get("id") != request_id:
            raise BridgeClientError("bridge returned a mismatched response id")
        if response.get("ok") is not True:
            error = response.get("error")
            message = error.get("message") if isinstance(error, dict) else "unknown bridge error"
            raise BridgeClientError(str(message))
        return response.get("result")

    def close(self) -> None:
        if self.process.poll() is None:
            try:
                self.request("shutdown")
            except BridgeClientError:
                pass
        if self.process.stdin is not None:
            try:
                self.process.stdin.close()
            except BrokenPipeError:
                pass
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        self._stderr_thread.join(timeout=1)
        if self.process.returncode not in {0, None} and self._stderr_buffer:
            diagnostic = bytes(self._stderr_buffer).decode("utf-8", errors="replace").strip()
            if diagnostic:
                print(f"bridge diagnostic: {diagnostic[:1000]}", file=sys.stderr)

    def __enter__(self) -> BridgeClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _require_mapping(value: Any, operation: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BridgeClientError(f"{operation} returned no object")
    return value


def _require_list(value: Any, operation: str) -> list[Any]:
    if not isinstance(value, list):
        raise BridgeClientError(f"{operation} returned no list")
    return value


def _require_finite_number(value: Any, operation: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BridgeClientError(f"{operation} returned no number")
    number = float(value)
    if not math.isfinite(number):
        raise BridgeClientError(f"{operation} returned a non-finite number")
    return number


def _market_watch_state(symbols: list[Any]) -> dict[str, tuple[Any, Any]]:
    state: dict[str, tuple[Any, Any]] = {}
    for item in symbols:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str):
            raise BridgeClientError("symbols_get returned a malformed symbol")
        state[item["name"]] = (item.get("select"), item.get("visible"))
    return state


def run_read_only_smoke(client: BridgeClient, requested_symbol: str) -> dict[str, Any]:
    started = time.monotonic()
    health = client.request("health")
    initialized = _require_mapping(client.request("initialize"), "initialize")
    if initialized.get("initialized") is not True:
        raise BridgeClientError(f"MT5 initialization failed: {initialized.get('last_error')}")

    terminal = _require_mapping(client.request("terminal_info"), "terminal_info")
    if terminal.get("connected") is not True:
        raise BridgeClientError("MT5 terminal is not connected")
    account = _require_mapping(client.request("account_info"), "account_info")
    orders = _require_list(client.request("orders_get"), "orders_get")
    positions = _require_list(client.request("positions_get"), "positions_get")
    if orders or positions:
        raise BridgeClientError(
            "read-only probe requires zero starting orders and positions"
        )
    symbols = _require_list(client.request("symbols_get"), "symbols_get")
    if not symbols:
        raise BridgeClientError("symbols_get returned no symbols")
    initial_market_watch = _market_watch_state(symbols)

    symbol_names = set(initial_market_watch)
    if requested_symbol not in symbol_names:
        raise BridgeClientError(f"requested symbol {requested_symbol!r} is unavailable")
    symbol = _require_mapping(
        client.request("symbol_info", {"symbol": requested_symbol}), "symbol_info"
    )
    tick = _require_mapping(
        client.request("symbol_info_tick", {"symbol": requested_symbol}), "symbol_info_tick"
    )
    point = _require_finite_number(symbol.get("point"), "symbol_info.point")
    volume = _require_finite_number(symbol.get("volume_min"), "symbol_info.volume_min")
    ask = _require_finite_number(tick.get("ask"), "symbol_info_tick.ask")
    if point <= 0 or volume <= 0 or ask <= 0:
        raise BridgeClientError("symbol did not provide positive point, minimum volume, and ask")

    rates = _require_list(
        client.request(
            "copy_rates_from_pos",
            {"symbol": requested_symbol, "timeframe": "TIMEFRAME_M1", "start_pos": 0, "count": 10},
        ),
        "copy_rates_from_pos",
    )
    if len(rates) != 10:
        raise BridgeClientError(f"copy_rates_from_pos returned {len(rates)} rates instead of 10")
    margin = _require_finite_number(
        client.request(
            "order_calc_margin",
            {"action": "ORDER_TYPE_BUY", "symbol": requested_symbol, "volume": volume, "price": ask},
        ),
        "order_calc_margin",
    )
    profit = _require_finite_number(
        client.request(
            "order_calc_profit",
            {
                "action": "ORDER_TYPE_BUY",
                "symbol": requested_symbol,
                "volume": volume,
                "open_price": ask,
                "close_price": ask + 10 * point,
            },
        ),
        "order_calc_profit",
    )
    constants = _require_mapping(client.request("constants"), "constants")
    order_check = _require_mapping(
        client.request(
            "order_check",
            {
                "request": {
                    "action": constants["TRADE_ACTION_DEAL"],
                    "symbol": requested_symbol,
                    "volume": volume,
                    "type": constants["ORDER_TYPE_BUY"],
                    "price": ask,
                    "type_time": constants["ORDER_TIME_GTC"],
                    "type_filling": constants["ORDER_FILLING_FOK"],
                }
            },
        ),
        "order_check",
    )
    if order_check.get("retcode") != 0 or order_check.get("comment") != "Done":
        raise BridgeClientError(
            f"order_check was unsuccessful: {order_check.get('retcode')} {order_check.get('comment')}"
        )
    final_orders = _require_list(client.request("orders_get"), "orders_get")
    final_positions = _require_list(client.request("positions_get"), "positions_get")
    final_symbols = _require_list(client.request("symbols_get"), "symbols_get")
    if final_orders or final_positions:
        raise BridgeClientError("read-only probe left broker orders or positions")
    if _market_watch_state(final_symbols) != initial_market_watch:
        raise BridgeClientError("read-only probe changed Market Watch state")

    return {
        "verdict": "read_only_path_validated",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "health": health,
        "terminal": {
            "connected": terminal.get("connected"),
            "build": terminal.get("build"),
            "name": terminal.get("name"),
            "company": terminal.get("company"),
        },
        "account": {
            "server": account.get("server"),
            "currency": account.get("currency"),
            "trade_mode": account.get("trade_mode"),
            "margin_mode": account.get("margin_mode"),
        },
        "inventory": {
            "symbol_count": len(symbols),
            "order_count": len(orders) if isinstance(orders, list) else None,
            "position_count": len(positions) if isinstance(positions, list) else None,
        },
        "symbol": {
            "name": symbol.get("name"),
            "trade_mode": symbol.get("trade_mode"),
            "visible": symbol.get("visible"),
            "volume_min": volume,
            "point": point,
            "tick_bid": tick.get("bid"),
            "tick_ask": tick.get("ask"),
            "m1_rate_count": len(rates) if isinstance(rates, list) else None,
        },
        "calculations": {"margin": margin, "profit_for_10_points": profit},
        "order_check": order_check,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Native Linux to Wine MT5 feasibility smoke test")
    parser.add_argument("--wine-prefix", type=Path, default=DEFAULT_WINE_PREFIX)
    parser.add_argument("--windows-python", default=DEFAULT_WINDOWS_PYTHON)
    parser.add_argument("--symbol", default="EURUSD")
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    args = parser.parse_args()
    try:
        with BridgeClient(
            wine_prefix=args.wine_prefix,
            windows_python=args.windows_python,
            timeout_seconds=args.timeout_seconds,
        ) as client:
            result = run_read_only_smoke(client, args.symbol)
    except (BridgeClientError, OSError, KeyError, TypeError, ValueError) as error:
        print(json.dumps({"verdict": "failed", "error": str(error)}, indent=2), file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
