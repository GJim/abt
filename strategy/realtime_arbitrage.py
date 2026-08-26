"""Risk-gated realtime arbitrage across any two approved account Workers."""

from __future__ import annotations

import argparse
import json
import signal
import subprocess
import sys
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from queue import Empty, Queue
from threading import Event, Thread
from uuid import uuid4
from zoneinfo import ZoneInfo


NY = ZoneInfo("America/New_York")
DEFAULT_TRADER_CONFIG = str(Path(__file__).with_name("trader.json"))


class StrategyError(RuntimeError):
    """Raised when the strategy cannot safely retain or create exposure."""


@dataclass(frozen=True, slots=True)
class Endpoint:
    worker_id: str


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    entry_edge_pips: float
    lots: float
    maximum_trades: int
    daily_loss_fraction: float
    trade_loss_fraction: float
    quote_max_age_seconds: float
    execute: bool


@dataclass(slots=True)
class Account:
    balance: float
    equity: float
    day_start_equity: float


@dataclass(slots=True)
class Pair:
    direction: str
    first_entry: float
    second_entry: float
    first_equity_at_entry: float
    second_equity_at_entry: float


class TraderGateway:
    """Hide JSONL process correlation behind one request/event interface."""

    def __init__(self, command: list[str]) -> None:
        self._process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None, text=True, encoding="utf-8", bufsize=1)
        if self._process.stdin is None or self._process.stdout is None:
            self.close()
            raise StrategyError("Could not open Trader JSONL streams.")
        self._messages: Queue[dict[str, object] | None] = Queue()
        self._deferred: deque[dict[str, object]] = deque()
        Thread(target=self._read, daemon=True).start()

    def request(self, worker_id: str, *, kind: str, request: dict[str, object]) -> dict[str, object]:
        request_id = str(uuid4())
        self._send({"request_id": request_id, "worker_id": worker_id, "payload": {"kind": kind, "request": request}})
        while True:
            message = self._next()
            if message.get("type") == "trader_rpc_result" and message.get("request_id") == request_id:
                if message.get("status") != "completed" or not isinstance(message.get("result"), dict):
                    raise StrategyError(f"Worker {worker_id} rejected {request['type']}.")
                return message["result"]
            self._deferred.append(message)

    def query(self, query: str) -> dict[str, object]:
        request_id = str(uuid4())
        self._send({"request_id": request_id, "query": query})
        while True:
            message = self._next()
            if message.get("type") == "trader_query_result" and message.get("request_id") == request_id:
                if message.get("query") != query or not isinstance(message.get("result"), dict):
                    raise StrategyError(f"Controller rejected {query}.")
                return message["result"]
            self._deferred.append(message)

    def market_data(self) -> dict[str, object]:
        while True:
            message = self._deferred.popleft() if self._deferred else self._next()
            if message.get("type") == "market_data":
                return message
            if message.get("type") == "market_data_unavailable":
                raise StrategyError("Worker market data became unavailable.")

    def close(self) -> None:
        if hasattr(self, "_process") and self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()

    def _send(self, message: dict[str, object]) -> None:
        if self._process.poll() is not None or self._process.stdin is None:
            raise StrategyError("Trader process is not running.")
        self._process.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
        self._process.stdin.flush()

    def _next(self) -> dict[str, object]:
        try:
            value = self._messages.get(timeout=30)
        except Empty as error:
            raise StrategyError("Timed out waiting for Trader data.") from error
        if value is None:
            raise StrategyError("Trader process exited.")
        return value

    def _read(self) -> None:
        assert self._process.stdout is not None
        for line in self._process.stdout:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                self._messages.put(value)
        self._messages.put(None)


class RealtimeArbitrage:
    """Own one pair lifecycle; the caller only supplies endpoints and configuration."""

    def __init__(
        self, gateway: TraderGateway, *, first: Endpoint, second: Endpoint, symbol: str, config: StrategyConfig
    ) -> None:
        if first.worker_id == second.worker_id:
            raise StrategyError("Arbitrage endpoints must use different Workers.")
        self.gateway, self.endpoints, self.symbol, self.config = gateway, {"first": first, "second": second}, symbol, config
        self._verify_active_workers_and_symbol()
        self.accounts = {name: self._account(endpoint) for name, endpoint in self.endpoints.items()}
        self.quotes: dict[str, tuple[datetime, float, float]] = {}
        self.margin_per_lot: dict[tuple[str, str], float] = {}
        self.margin_refresh_at: datetime | None = None
        self.pair: Pair | None = None
        self.awaiting_clear = self.stopped = False
        self.completed_trades = 0
        self._ny_date: object | None = None

    def run(self, stop: Event) -> None:
        while not stop.is_set() and not self.stopped:
            self._reset_daily_equity()
            self._consume(self.gateway.market_data())
            if not self._quotes_fresh():
                continue
            self._refresh_margin_if_due()
            if self.pair is not None and self._risk_breached():
                self._halt("risk limit reached")
            direction = self._direction()
            if self.pair is not None:
                if direction is not None and direction != self.pair.direction:
                    self._flatten_all()
                    self.completed_trades += 1
                    self.pair, self.awaiting_clear = None, True
                continue
            if self.awaiting_clear:
                self.awaiting_clear = direction is not None
            elif direction is not None and self.completed_trades < self.config.maximum_trades:
                self._open(direction)

    def shutdown(self) -> None:
        if self.config.execute:
            self._flatten_all()

    def _account(self, endpoint: Endpoint) -> Account:
        result = self.gateway.request(endpoint.worker_id, kind="read", request={"type": "account_info"})
        value = result.get("account")
        if not isinstance(value, dict):
            raise StrategyError(f"Worker {endpoint.worker_id} returned no account.")
        equity = _positive(value.get("equity"), "equity")
        return Account(_positive(value.get("balance"), "balance"), equity, equity)

    def _verify_active_workers_and_symbol(self) -> None:
        result = self.gateway.query("active_workers")
        workers = result.get("workers")
        if not isinstance(workers, list):
            raise StrategyError("Controller returned invalid active Worker inventory.")
        active = {
            worker.get("worker_id"): worker
            for worker in workers
            if isinstance(worker, dict) and isinstance(worker.get("worker_id"), str)
        }
        for endpoint in self.endpoints.values():
            worker = active.get(endpoint.worker_id)
            if not isinstance(worker, dict) or worker.get("connectivity") != "connected" or worker.get("safety_state") != "connected":
                raise StrategyError(f"Worker {endpoint.worker_id} is not an active, safe Worker.")
            result = self.gateway.request(endpoint.worker_id, kind="read", request={"type": "symbol_info", "symbol": self.symbol})
            symbol = result.get("symbol")
            if not isinstance(symbol, dict) or symbol.get("name") != self.symbol or symbol.get("trade_mode") != 4:
                raise StrategyError(f"Worker {endpoint.worker_id} cannot trade {self.symbol}.")

    def _consume(self, message: dict[str, object]) -> None:
        worker, observed_at, quotes = message.get("worker_id"), message.get("observed_at"), message.get("quotes")
        name = next((key for key, endpoint in self.endpoints.items() if endpoint.worker_id == worker), None)
        if name is None or not isinstance(observed_at, str) or not isinstance(quotes, list):
            return
        quote = next((item for item in quotes if isinstance(item, dict) and item.get("symbol") == self.symbol), None)
        if not isinstance(quote, dict):
            return
        self.quotes[name] = (_utc(observed_at), _positive(quote.get("bid"), "bid"), _positive(quote.get("ask"), "ask"))

    def _quotes_fresh(self) -> bool:
        return set(self.quotes) == {"first", "second"} and all((datetime.now(UTC) - value[0]).total_seconds() <= self.config.quote_max_age_seconds for value in self.quotes.values())

    def _refresh_margin_if_due(self) -> None:
        if self.margin_refresh_at is not None and datetime.now(UTC) < self.margin_refresh_at:
            return
        for name, endpoint in self.endpoints.items():
            _, bid, ask = self.quotes[name]
            for direction, price in (("LONG", ask), ("SHORT", bid)):
                result = self.gateway.request(endpoint.worker_id, kind="read", request={"type": "calc_margin", "symbol": self.symbol, "volume": "1.00", "direction": direction, "price": f"{price:.10f}"})
                self.margin_per_lot[name, direction] = _positive(result.get("margin"), "calculated margin")
        self.margin_refresh_at = datetime.now(UTC) + timedelta(hours=1)

    def _direction(self) -> str | None:
        _, first_bid, first_ask = self.quotes["first"]
        _, second_bid, second_ask = self.quotes["second"]
        edge = self.config.entry_edge_pips * 0.0001
        if first_bid - second_ask + 1e-12 >= edge:
            return "short_first_long_second"
        if second_bid - first_ask + 1e-12 >= edge:
            return "long_first_short_second"
        return None

    def _open(self, direction: str) -> None:
        first_direction, second_direction = ("SHORT", "LONG") if direction == "short_first_long_second" else ("LONG", "SHORT")
        for name, side in (("first", first_direction), ("second", second_direction)):
            if self.config.lots * self.margin_per_lot[name, side] > self.accounts[name].equity * 0.5:
                self._halt(f"warning: {name} margin exceeds 50% of equity")
        try:
            self._market("first", first_direction)
            self._market("second", second_direction)
        except Exception as error:
            self._halt(f"unhedged entry: {error}")
        _, first_bid, first_ask = self.quotes["first"]
        _, second_bid, second_ask = self.quotes["second"]
        self.pair = Pair(direction, first_bid if first_direction == "SHORT" else first_ask, second_ask if second_direction == "LONG" else second_bid, self.accounts["first"].equity, self.accounts["second"].equity)

    def _market(self, name: str, direction: str) -> None:
        if self.config.execute:
            endpoint = self.endpoints[name]
            self.gateway.request(endpoint.worker_id, kind="operation", request={"type": "market", "symbol": self.symbol, "volume": f"{self.config.lots:.2f}", "direction": direction, "filling_mode": "IOC"})

    def _risk_breached(self) -> bool:
        assert self.pair is not None
        for name, pnl in self._pnl().items():
            account = self.accounts[name]
            entry = self.pair.first_equity_at_entry if name == "first" else self.pair.second_equity_at_entry
            if pnl <= -entry * self.config.trade_loss_fraction or account.equity + pnl <= account.day_start_equity * (1 - self.config.daily_loss_fraction):
                return True
        return False

    def _pnl(self) -> dict[str, float]:
        assert self.pair is not None
        _, first_bid, first_ask = self.quotes["first"]
        _, second_bid, second_ask = self.quotes["second"]
        units = self.config.lots * 100_000
        if self.pair.direction == "short_first_long_second":
            return {"first": (self.pair.first_entry - first_ask) * units, "second": (second_bid - self.pair.second_entry) * units}
        return {"first": (first_bid - self.pair.first_entry) * units, "second": (self.pair.second_entry - second_ask) * units}

    def _flatten_all(self) -> None:
        for name, endpoint in self.endpoints.items():
            if not self.config.execute:
                continue
            result = self.gateway.request(endpoint.worker_id, kind="read", request={"type": "current_positions"})
            positions = result.get("positions")
            if not isinstance(positions, list):
                raise StrategyError("Worker returned invalid positions.")
            for position in positions:
                if not isinstance(position, dict) or not isinstance(position.get("ticket"), int) or not isinstance(position.get("volume"), (int, float)):
                    raise StrategyError("Worker returned an unclsoable position.")
                self.gateway.request(endpoint.worker_id, kind="operation", request={"type": "close", "ticket": str(position["ticket"]), "volume": f"{float(position['volume']):.2f}"})
            self.accounts[name] = self._account(endpoint)

    def _halt(self, reason: str) -> None:
        self.stopped = True
        self._flatten_all()
        raise StrategyError(reason)

    def _reset_daily_equity(self) -> None:
        today = datetime.now(NY).date()
        if today != self._ny_date:
            for name, endpoint in self.endpoints.items():
                self.accounts[name] = self._account(endpoint)
                self.accounts[name].day_start_equity = self.accounts[name].equity
            self._ny_date = today


def _positive(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise StrategyError(f"Invalid {field}.")
    return float(value)


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise StrategyError("Quote timestamp must include an offset.")
    return parsed.astimezone(UTC)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--first-worker")
    parser.add_argument("--second-worker")
    parser.add_argument("--symbol", default="NZDUSD")
    parser.add_argument("--entry-edge-pips", type=float, default=0.4)
    parser.add_argument("--lots", type=float, default=0.1)
    parser.add_argument("--max-trades", type=int, default=100)
    parser.add_argument("--daily-loss-percent", type=float, default=3)
    parser.add_argument("--trade-loss-percent", type=float, default=2)
    parser.add_argument("--quote-max-age-seconds", type=float, default=1)
    parser.add_argument("--trader-executable", default="abt-trader")
    parser.add_argument("--trader-config", default=DEFAULT_TRADER_CONFIG)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if (args.first_worker is None) != (args.second_worker is None):
        parser.error("--first-worker and --second-worker must be specified together.")
    if args.first_worker == args.second_worker and args.first_worker is not None:
        parser.error("Workers must differ.")
    if min(args.entry_edge_pips, args.lots, args.max_trades, args.quote_max_age_seconds, args.daily_loss_percent, args.trade_loss_percent) <= 0:
        parser.error("All limits must be positive.")
    config = StrategyConfig(args.entry_edge_pips, args.lots, args.max_trades, args.daily_loss_percent / 100, args.trade_loss_percent / 100, args.quote_max_age_seconds, args.execute)
    command = [args.trader_executable, "connect", "--jsonl", "--config", args.trader_config]
    if args.first_worker is not None:
        command.extend(["--worker-id", args.first_worker, "--worker-id", args.second_worker])
    gateway, stop = TraderGateway(command), Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    strategy: RealtimeArbitrage | None = None
    try:
        first_worker, second_worker = (
            (args.first_worker, args.second_worker)
            if args.first_worker is not None
            else _select_workers(gateway)
        )
        strategy = RealtimeArbitrage(
            gateway, first=Endpoint(first_worker), second=Endpoint(second_worker), symbol=args.symbol, config=config
        )
        strategy.run(stop)
        return 0
    except StrategyError as error:
        print(f"Strategy stopped: {error}", file=sys.stderr)
        return 1
    finally:
        if strategy is not None:
            try:
                strategy.shutdown()
            except StrategyError as error:
                print(f"Emergency flatten failed: {error}", file=sys.stderr)
        gateway.close()


def _select_workers(gateway: TraderGateway) -> tuple[str, str]:
    result = gateway.query("active_workers")
    workers = result.get("workers")
    if not isinstance(workers, list):
        raise StrategyError("Controller returned invalid active Worker inventory.")
    eligible = [
        worker
        for worker in workers
        if isinstance(worker, dict)
        and isinstance(worker.get("worker_id"), str)
        and worker.get("connectivity") == "connected"
        and worker.get("safety_state") == "connected"
    ]
    if len(eligible) < 2:
        raise StrategyError("At least two connected, safe active Workers are required.")
    print("Select two active Workers:")
    for index, worker in enumerate(eligible, start=1):
        print(f"  {index}. {worker['worker_id']} ({worker.get('server', 'unknown server')})")
    first = _selected_worker(eligible, "First Worker")
    second = _selected_worker(eligible, "Second Worker", excluded=first)
    return first, second


def _selected_worker(workers: list[dict[str, object]], label: str, *, excluded: str | None = None) -> str:
    while True:
        try:
            selected = int(input(f"{label} number: "))
        except ValueError:
            print("Enter a number from the list.")
            continue
        if not 1 <= selected <= len(workers):
            print("Enter a number from the list.")
            continue
        worker_id = workers[selected - 1]["worker_id"]
        assert isinstance(worker_id, str)
        if worker_id == excluded:
            print("Select a different Worker.")
            continue
        return worker_id


if __name__ == "__main__":
    raise SystemExit(main())
