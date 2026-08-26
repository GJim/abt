"""Risk-gated realtime arbitrage across any two approved account Workers."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import subprocess
import sys
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from queue import Empty, Queue
from threading import Event, Thread
from uuid import uuid4
from zoneinfo import ZoneInfo


NY = ZoneInfo("America/New_York")
DEFAULT_TRADER_CONFIG = str(Path(__file__).with_name("trader.json"))
_LOGGER = logging.getLogger(__name__)


class StrategyError(RuntimeError):
    """Raised when the strategy cannot safely retain or create exposure."""


class WorkerMarketDataUnavailable(StrategyError):
    """Raised when the controller reports that a selected Worker's feed is unavailable."""

    def __init__(self, worker_id: str, reason: str) -> None:
        self.worker_id, self.reason = worker_id, reason
        super().__init__(f"Worker {worker_id} market data became unavailable: {reason}")


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
    emergency_stop_loss_usd: float
    integrity_check_seconds: float
    minimum_hold_seconds: float
    flatten_at_ny: time
    worker_disconnect_grace_seconds: float
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
    first_ticket: int
    second_ticket: int
    first_direction: str
    second_direction: str
    opened_at: datetime


class TraderGateway:
    """Hide JSONL process correlation behind one request/event interface."""

    def __init__(self, command: list[str]) -> None:
        _LOGGER.info("trader_gateway_start")
        self._process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None, text=True, encoding="utf-8", bufsize=1)
        if self._process.stdin is None or self._process.stdout is None:
            self.close()
            raise StrategyError("Could not open Trader JSONL streams.")
        self._messages: Queue[dict[str, object] | None] = Queue()
        self._deferred: deque[dict[str, object]] = deque()
        Thread(target=self._read, daemon=True).start()

    def request(self, worker_id: str, *, kind: str, request: dict[str, object]) -> dict[str, object]:
        request_id = str(uuid4())
        _LOGGER.debug("rpc_request worker=%s kind=%s type=%s request_id=%s", worker_id, kind, request["type"], request_id)
        self._send({"request_id": request_id, "worker_id": worker_id, "payload": {"kind": kind, "request": request}})
        while True:
            message = self._next()
            if message.get("type") == "trader_rpc_result" and message.get("request_id") == request_id:
                if message.get("status") != "completed" or not isinstance(message.get("result"), dict):
                    _LOGGER.error("rpc_rejected worker=%s kind=%s type=%s request_id=%s", worker_id, kind, request["type"], request_id)
                    raise StrategyError(f"Worker {worker_id} rejected {request['type']}.")
                _LOGGER.debug("rpc_completed worker=%s kind=%s type=%s request_id=%s", worker_id, kind, request["type"], request_id)
                return message["result"]
            self._deferred.append(message)

    def query(self, query: str) -> dict[str, object]:
        request_id = str(uuid4())
        _LOGGER.debug("controller_query query=%s request_id=%s", query, request_id)
        self._send({"request_id": request_id, "query": query})
        while True:
            message = self._next()
            if message.get("type") == "trader_query_result" and message.get("request_id") == request_id:
                if message.get("query") != query or not isinstance(message.get("result"), dict):
                    raise StrategyError(f"Controller rejected {query}.")
                return message["result"]
            self._deferred.append(message)

    def market_data(self, *, timeout_seconds: float = 1) -> dict[str, object] | None:
        """Return the next market-data event, or None while the healthy stream is quiet."""

        while True:
            if self._deferred:
                message = self._deferred.popleft()
            else:
                try:
                    message = self._messages.get(timeout=timeout_seconds)
                except Empty:
                    return None
                if message is None:
                    raise StrategyError("Trader process exited.")
            if message.get("type") == "market_data":
                return message
            if message.get("type") == "market_data_unavailable":
                worker_id, reason = message.get("worker_id"), message.get("reason")
                if not isinstance(worker_id, str) or not worker_id or not isinstance(reason, str) or not reason:
                    raise StrategyError("Controller returned invalid market-data availability.")
                raise WorkerMarketDataUnavailable(worker_id, reason)

    def close(self) -> None:
        if hasattr(self, "_process") and self._process.poll() is None:
            _LOGGER.info("trader_gateway_stop")
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
        _LOGGER.info(
            "strategy_initializing symbol=%s first_worker=%s second_worker=%s lots=%.2f edge_pips=%.2f max_trades=%d execute=%s",
            symbol, first.worker_id, second.worker_id, config.lots, config.entry_edge_pips, config.maximum_trades, config.execute,
        )
        self._verify_active_workers_and_symbol()
        self.accounts = {name: self._account(endpoint) for name, endpoint in self.endpoints.items()}
        self._assert_empty_accounts()
        self.quotes: dict[str, tuple[datetime, float, float]] = {}
        self.margin_per_lot: dict[tuple[str, str], float] = {}
        self.margin_refresh_at: datetime | None = None
        self.integrity_check_at: datetime | None = None
        self.pair: Pair | None = None
        self.unavailable_endpoints: set[str] = set()
        self.worker_disconnect_deadline: datetime | None = None
        self.awaiting_clear = self.stopped = False
        self.completed_trades = 0
        self._ny_date: object | None = None
        _LOGGER.info("strategy_initialized")

    def run(self, stop: Event) -> None:
        _LOGGER.info("strategy_running")
        while not stop.is_set() and not self.stopped:
            self._reset_daily_equity()
            if self._cutoff_reached():
                self._halt(f"New York daily cutoff reached at {self.config.flatten_at_ny:%H:%M}")
            if self.worker_disconnect_deadline is not None and datetime.now(UTC) >= self.worker_disconnect_deadline:
                self._halt("worker market data did not recover before disconnect grace deadline")
            try:
                message = self.gateway.market_data()
            except WorkerMarketDataUnavailable as error:
                self._suspend_for_worker_disconnect(error)
                continue
            if message is None:
                continue
            if self._consume(message):
                self._recover_worker_market_data(message)
            if self.unavailable_endpoints:
                continue
            self._check_active_pair_integrity()
            if not self._quotes_fresh():
                continue
            self._refresh_margin_if_due()
            self._check_integrity_if_due()
            if self.pair is not None and self._risk_breached():
                _LOGGER.warning("risk_limit_triggered completed_trades=%d", self.completed_trades)
                self._halt("risk limit reached")
            direction = self._direction()
            if self.pair is not None:
                if direction is not None and direction != self.pair.direction:
                    if not self._minimum_hold_elapsed():
                        remaining = self.config.minimum_hold_seconds - (datetime.now(UTC) - self.pair.opened_at).total_seconds()
                        _LOGGER.info(
                            "reverse_signal_deferred_minimum_hold old_direction=%s new_direction=%s remaining_seconds=%.1f",
                            self.pair.direction,
                            direction,
                            max(0, remaining),
                        )
                        continue
                    _LOGGER.info("reverse_signal_close old_direction=%s new_direction=%s", self.pair.direction, direction)
                    self._flatten_all()
                    self.completed_trades += 1
                    self.pair, self.awaiting_clear = None, True
                continue
            if self.awaiting_clear:
                self.awaiting_clear = direction is not None
                if not self.awaiting_clear:
                    _LOGGER.info("reentry_rearmed")
            elif direction is not None and self.completed_trades < self.config.maximum_trades:
                self._open(direction)
            elif direction is not None:
                _LOGGER.info("entry_skipped_trade_limit completed_trades=%d maximum_trades=%d", self.completed_trades, self.config.maximum_trades)

    def shutdown(self) -> None:
        _LOGGER.info("strategy_shutdown_requested execute=%s", self.config.execute)
        if self.config.execute:
            self._flatten_all()
        _LOGGER.info("strategy_shutdown_complete")

    def _account(self, endpoint: Endpoint) -> Account:
        result = self.gateway.request(endpoint.worker_id, kind="read", request={"type": "account_info"})
        value = result.get("account")
        if not isinstance(value, dict):
            raise StrategyError(f"Worker {endpoint.worker_id} returned no account.")
        equity = _positive(value.get("equity"), "equity")
        balance = _positive(value.get("balance"), "balance")
        _LOGGER.info("account_loaded worker=%s balance=%.2f equity=%.2f", endpoint.worker_id, balance, equity)
        return Account(balance, equity, equity)

    def _verify_active_workers_and_symbol(self) -> None:
        result = self.gateway.query("active_workers")
        workers = result.get("workers")
        if not isinstance(workers, list):
            raise StrategyError("Controller returned invalid active Worker inventory.")
        _LOGGER.info("active_workers_loaded count=%d", len(workers))
        active = {
            worker.get("worker_id"): worker
            for worker in workers
            if isinstance(worker, dict) and isinstance(worker.get("worker_id"), str)
        }
        for endpoint in self.endpoints.values():
            worker = active.get(endpoint.worker_id)
            if not isinstance(worker, dict) or worker.get("connectivity") != "connected" or worker.get("safety_state") != "connected":
                raise StrategyError(f"Worker {endpoint.worker_id} is not an active, safe Worker.")
            _LOGGER.info("worker_verified worker=%s server=%s", endpoint.worker_id, worker.get("server"))
            result = self.gateway.request(endpoint.worker_id, kind="read", request={"type": "symbol_info", "symbol": self.symbol})
            symbol = result.get("symbol")
            if not isinstance(symbol, dict) or symbol.get("name") != self.symbol or symbol.get("trade_mode") != 4:
                raise StrategyError(f"Worker {endpoint.worker_id} cannot trade {self.symbol}.")
            _LOGGER.info("symbol_verified worker=%s symbol=%s", endpoint.worker_id, self.symbol)

    def _consume(self, message: dict[str, object]) -> bool:
        worker, observed_at, quotes = message.get("worker_id"), message.get("observed_at"), message.get("quotes")
        name = next((key for key, endpoint in self.endpoints.items() if endpoint.worker_id == worker), None)
        if name is None or not isinstance(observed_at, str) or not isinstance(quotes, list):
            return False
        quote = next((item for item in quotes if isinstance(item, dict) and item.get("symbol") == self.symbol), None)
        if not isinstance(quote, dict):
            return False
        self.quotes[name] = (_utc(observed_at), _positive(quote.get("bid"), "bid"), _positive(quote.get("ask"), "ask"))
        _LOGGER.debug("quote_updated endpoint=%s bid=%s ask=%s observed_at=%s", name, quote["bid"], quote["ask"], observed_at)
        return True

    def _suspend_for_worker_disconnect(self, error: WorkerMarketDataUnavailable) -> None:
        name = next((key for key, endpoint in self.endpoints.items() if endpoint.worker_id == error.worker_id), None)
        if name is None:
            raise StrategyError(f"Controller reported market-data loss for unselected Worker {error.worker_id}.")
        self.quotes.pop(name, None)
        self.unavailable_endpoints.add(name)
        deadline = datetime.now(UTC) + timedelta(seconds=self.config.worker_disconnect_grace_seconds)
        if self.worker_disconnect_deadline is None:
            self.worker_disconnect_deadline = deadline
            _LOGGER.warning(
                "worker_market_data_suspended endpoint=%s worker=%s reason=%s grace_seconds=%.0f deadline=%s",
                name,
                error.worker_id,
                error.reason,
                self.config.worker_disconnect_grace_seconds,
                deadline.isoformat(),
            )
        else:
            _LOGGER.warning(
                "worker_market_data_still_unavailable endpoint=%s worker=%s reason=%s deadline=%s",
                name,
                error.worker_id,
                error.reason,
                self.worker_disconnect_deadline.isoformat(),
            )

    def _recover_worker_market_data(self, message: dict[str, object]) -> None:
        worker_id = message.get("worker_id")
        name = next((key for key, endpoint in self.endpoints.items() if endpoint.worker_id == worker_id), None)
        if name is None or name not in self.unavailable_endpoints:
            return
        self.unavailable_endpoints.remove(name)
        if not self.unavailable_endpoints:
            _LOGGER.info("worker_market_data_recovered endpoint=%s worker=%s", name, worker_id)
            self.worker_disconnect_deadline = None

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
                _LOGGER.info("margin_refreshed endpoint=%s direction=%s per_lot=%.2f", name, direction, self.margin_per_lot[name, direction])
        self.margin_refresh_at = datetime.now(UTC) + timedelta(hours=1)
        _LOGGER.info("margin_refresh_complete next_at=%s", self.margin_refresh_at.isoformat())

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
        _LOGGER.info("entry_signal direction=%s first_side=%s second_side=%s", direction, first_direction, second_direction)
        for name, side in (("first", first_direction), ("second", second_direction)):
            required = self.config.lots * self.margin_per_lot[name, side]
            limit = self.accounts[name].equity * 0.5
            if required > limit:
                _LOGGER.warning("margin_limit_triggered endpoint=%s required=%.2f limit=%.2f", name, required, limit)
                self._halt(f"warning: {name} margin exceeds 50% of equity")
        try:
            self._market("first", first_direction)
            self._market("second", second_direction)
            if not self.config.execute:
                _, first_bid, first_ask = self.quotes["first"]
                _, second_bid, second_ask = self.quotes["second"]
                self.pair = Pair(
                    direction, first_bid if first_direction == "SHORT" else first_ask,
                    second_ask if second_direction == "LONG" else second_bid,
                    self.accounts["first"].equity, self.accounts["second"].equity, 0, 0, first_direction, second_direction,
                    datetime.now(UTC),
                )
                return
            first_position = self._expected_position("first", first_direction)
            second_position = self._expected_position("second", second_direction)
            self._set_emergency_protection("first", first_position, first_direction)
            self._set_emergency_protection("second", second_position, second_direction)
        except Exception as error:
            self._halt(f"unhedged entry: {error}")
        self.pair = Pair(
            direction,
            _positive(first_position.get("price_open"), "first position price_open"),
            _positive(second_position.get("price_open"), "second position price_open"),
            self.accounts["first"].equity,
            self.accounts["second"].equity,
            _ticket(first_position),
            _ticket(second_position),
            first_direction,
            second_direction,
            datetime.now(UTC),
        )
        _LOGGER.info("pair_opened direction=%s first_entry=%.5f second_entry=%.5f", direction, self.pair.first_entry, self.pair.second_entry)

    def _market(self, name: str, direction: str) -> None:
        if self.config.execute:
            endpoint = self.endpoints[name]
            _LOGGER.info("market_order_submit endpoint=%s worker=%s direction=%s symbol=%s lots=%.2f", name, endpoint.worker_id, direction, self.symbol, self.config.lots)
            self.gateway.request(endpoint.worker_id, kind="operation", request={"type": "market", "symbol": self.symbol, "volume": f"{self.config.lots:.2f}", "direction": direction, "filling_mode": "IOC"})
            _LOGGER.info("market_order_completed endpoint=%s worker=%s", name, endpoint.worker_id)
        else:
            _LOGGER.info("market_order_dry_run endpoint=%s direction=%s", name, direction)

    def _expected_position(self, name: str, direction: str) -> dict[str, object]:
        endpoint = self.endpoints[name]
        result = self.gateway.request(endpoint.worker_id, kind="read", request={"type": "current_positions"})
        positions = result.get("positions")
        expected_type = 0 if direction == "LONG" else 1
        matching = [
            position
            for position in positions if isinstance(position, dict)
            and position.get("symbol") == self.symbol
            and position.get("type") == expected_type
            and position.get("volume") == self.config.lots
        ] if isinstance(positions, list) else []
        if len(matching) != 1:
            raise StrategyError(f"Worker {endpoint.worker_id} did not confirm one expected position.")
        return matching[0]

    def _set_emergency_protection(self, name: str, position: dict[str, object], direction: str) -> None:
        if not self.config.execute:
            return
        endpoint = self.endpoints[name]
        entry = _positive(position.get("price_open"), "position price_open")
        sl = self._profit_target_price(endpoint, direction, entry, -self.config.emergency_stop_loss_usd)
        tp = self._profit_target_price(endpoint, direction, entry, self.config.emergency_stop_loss_usd)
        self.gateway.request(
            endpoint.worker_id,
            kind="operation",
            request={
                "type": "modify_sl_tp",
                "symbol": self.symbol,
                "position": str(_ticket(position)),
                "sl": f"{sl:.10f}",
                "tp": f"{tp:.10f}",
            },
        )
        _LOGGER.info(
            "emergency_protection_set endpoint=%s ticket=%s sl=%.10f tp=%.10f magnitude_usd=%.2f",
            name,
            _ticket(position),
            sl,
            tp,
            self.config.emergency_stop_loss_usd,
        )

    def _profit_target_price(self, endpoint: Endpoint, direction: str, entry: float, target_profit: float) -> float:
        lower, upper = entry * 0.5, entry * 1.5
        for _ in range(32):
            candidate = (lower + upper) / 2
            result = self.gateway.request(
                endpoint.worker_id,
                kind="read",
                request={
                    "type": "calc_profit", "symbol": self.symbol, "volume": f"{self.config.lots:.2f}",
                    "direction": direction, "open_price": f"{entry:.10f}", "close_price": f"{candidate:.10f}",
                },
            )
            profit = result.get("profit")
            if isinstance(profit, bool) or not isinstance(profit, (int, float)):
                raise StrategyError("Worker returned invalid calculated profit.")
            profit_increases_with_price = direction == "LONG"
            if (profit < target_profit) == profit_increases_with_price:
                lower = candidate
            else:
                upper = candidate
        return (lower + upper) / 2

    def _risk_breached(self) -> bool:
        assert self.pair is not None
        for name, pnl in self._pnl().items():
            account = self.accounts[name]
            entry = self.pair.first_equity_at_entry if name == "first" else self.pair.second_equity_at_entry
            if pnl <= -entry * self.config.trade_loss_fraction or account.equity + pnl <= account.day_start_equity * (1 - self.config.daily_loss_fraction):
                _LOGGER.warning("risk_breach endpoint=%s pnl=%.2f marked_equity=%.2f day_start_equity=%.2f", name, pnl, account.equity + pnl, account.day_start_equity)
                return True
        return False

    def _minimum_hold_elapsed(self) -> bool:
        assert self.pair is not None
        return (datetime.now(UTC) - self.pair.opened_at).total_seconds() >= self.config.minimum_hold_seconds

    def _cutoff_reached(self, now: datetime | None = None) -> bool:
        return (now or datetime.now(NY)).astimezone(NY).time() >= self.config.flatten_at_ny

    def _assert_empty_accounts(self) -> None:
        for name, endpoint in self.endpoints.items():
            positions = self._records(endpoint, "current_positions", "positions")
            orders = self._records(endpoint, "current_orders", "orders")
            if positions or orders:
                raise StrategyError(f"Worker {endpoint.worker_id} has existing positions or orders at startup.")
            _LOGGER.info("account_integrity_verified endpoint=%s worker=%s", name, endpoint.worker_id)

    def _check_integrity_if_due(self) -> None:
        if self.integrity_check_at is not None and datetime.now(UTC) < self.integrity_check_at:
            return
        self._check_integrity()

    def _check_active_pair_integrity(self) -> None:
        if self.pair is not None and self.config.execute:
            self._check_integrity()

    def _check_integrity(self) -> None:
        for name, endpoint in self.endpoints.items():
            positions = self._records(endpoint, "current_positions", "positions")
            orders = self._records(endpoint, "current_orders", "orders")
            expected_ticket = None if self.pair is None or not self.config.execute else (
                self.pair.first_ticket if name == "first" else self.pair.second_ticket
            )
            expected_direction = None if self.pair is None else (
                self.pair.first_direction if name == "first" else self.pair.second_direction
            )
            expected_type = 0 if expected_direction == "LONG" else 1
            expected_position = (
                len(positions) == 1
                and _ticket(positions[0]) == expected_ticket
                and positions[0].get("symbol") == self.symbol
                and positions[0].get("type") == expected_type
                and positions[0].get("volume") == self.config.lots
            )
            if orders or (expected_ticket is None and positions) or (expected_ticket is not None and not expected_position):
                _LOGGER.warning(
                    "external_account_change endpoint=%s worker=%s positions=%d orders=%d expected_ticket=%s",
                    name, endpoint.worker_id, len(positions), len(orders), expected_ticket,
                )
                self._halt("external position or order detected")
        self.integrity_check_at = datetime.now(UTC) + timedelta(seconds=self.config.integrity_check_seconds)

    def _records(self, endpoint: Endpoint, request_type: str, field: str) -> list[dict[str, object]]:
        result = self.gateway.request(endpoint.worker_id, kind="read", request={"type": request_type})
        values = result.get(field)
        if not isinstance(values, list) or not all(isinstance(value, dict) for value in values):
            raise StrategyError(f"Worker {endpoint.worker_id} returned invalid {field}.")
        return values

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
            _LOGGER.info("flatten_positions endpoint=%s worker=%s count=%d", name, endpoint.worker_id, len(positions))
            for position in positions:
                if not isinstance(position, dict) or not isinstance(position.get("ticket"), int) or not isinstance(position.get("volume"), (int, float)):
                    raise StrategyError("Worker returned an unclsoable position.")
                self.gateway.request(endpoint.worker_id, kind="operation", request={"type": "close", "ticket": str(position["ticket"]), "volume": f"{float(position['volume']):.2f}"})
                _LOGGER.info("position_closed endpoint=%s ticket=%s volume=%.2f", name, position["ticket"], float(position["volume"]))
            self.accounts[name] = self._account(endpoint)

    def _halt(self, reason: str) -> None:
        self.stopped = True
        _LOGGER.warning("strategy_halt reason=%s", reason)
        self._flatten_all()
        raise StrategyError(reason)

    def _reset_daily_equity(self) -> None:
        today = datetime.now(NY).date()
        if today != self._ny_date:
            _LOGGER.info("daily_equity_reset ny_date=%s", today)
            for name, endpoint in self.endpoints.items():
                self.accounts[name] = self._account(endpoint)
                self.accounts[name].day_start_equity = self.accounts[name].equity
            self._ny_date = today


def _positive(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise StrategyError(f"Invalid {field}.")
    return float(value)


def _ticket(position: dict[str, object]) -> int:
    ticket = position.get("ticket")
    if isinstance(ticket, bool) or not isinstance(ticket, int):
        raise StrategyError("Worker returned a position without a ticket.")
    return ticket


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
    parser.add_argument("--emergency-stop-loss-usd", type=float, default=40)
    parser.add_argument("--integrity-check-seconds", type=float, default=5)
    parser.add_argument("--minimum-hold-seconds", type=float, default=180)
    parser.add_argument("--flatten-at-ny", type=_ny_time, default=time(16, 0), metavar="HH:MM")
    parser.add_argument("--worker-disconnect-grace-seconds", type=float, default=300)
    parser.add_argument("--trader-executable", default="abt-trader")
    parser.add_argument("--trader-config", default=DEFAULT_TRADER_CONFIG)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true", help="emit DEBUG diagnostics")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if (args.first_worker is None) != (args.second_worker is None):
        parser.error("--first-worker and --second-worker must be specified together.")
    if args.first_worker == args.second_worker and args.first_worker is not None:
        parser.error("Workers must differ.")
    if min(args.entry_edge_pips, args.lots, args.max_trades, args.quote_max_age_seconds, args.daily_loss_percent, args.trade_loss_percent, args.emergency_stop_loss_usd, args.integrity_check_seconds, args.minimum_hold_seconds, args.worker_disconnect_grace_seconds) <= 0:
        parser.error("All limits must be positive.")
    config = StrategyConfig(
        args.entry_edge_pips, args.lots, args.max_trades, args.daily_loss_percent / 100, args.trade_loss_percent / 100,
        args.quote_max_age_seconds, args.emergency_stop_loss_usd, args.integrity_check_seconds, args.minimum_hold_seconds,
        args.flatten_at_ny, args.worker_disconnect_grace_seconds, args.execute,
    )
    command = [args.trader_executable, "connect", "--jsonl", "--config", args.trader_config]
    if args.first_worker is not None:
        command.extend(["--worker-id", args.first_worker, "--worker-id", args.second_worker])
    gateway, stop = TraderGateway(command), Event()
    signal.signal(signal.SIGINT, lambda *_: (_LOGGER.info("interrupt_received"), stop.set()))
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
        _LOGGER.error("strategy_stopped reason=%s", error)
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


def _ny_time(value: str) -> time:
    try:
        parsed = datetime.strptime(value, "%H:%M").time()
    except ValueError as error:
        raise argparse.ArgumentTypeError("must use 24-hour HH:MM format") from error
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
