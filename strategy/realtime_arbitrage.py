"""Risk-gated realtime arbitrage across any two approved account Workers."""

from __future__ import annotations

import argparse
import json
import logging
import math
import math
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
    entry_edge_points: float
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
    symbol: str
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


@dataclass(frozen=True, slots=True)
class TradeCandidate:
    symbol: str
    direction: str
    edge: float


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

    def __init__(self, gateway: TraderGateway, *, first: Endpoint, second: Endpoint, config: StrategyConfig) -> None:
        if first.worker_id == second.worker_id:
            raise StrategyError("Arbitrage endpoints must use different Workers.")
        self.gateway, self.endpoints, self.config = gateway, {"first": first, "second": second}, config
        _LOGGER.info(
            "strategy_initializing first_worker=%s second_worker=%s lots=%.2f edge_points=%.2f max_trades=%d execute=%s",
            first.worker_id, second.worker_id, config.lots, config.entry_edge_points, config.maximum_trades, config.execute,
        )
        self._verify_active_workers()
        self.shared_symbols = self._load_shared_symbols()
        self._configure_live_symbols()
        self.accounts = {name: self._account(endpoint) for name, endpoint in self.endpoints.items()}
        self._assert_empty_accounts()
        self.quotes: dict[str, dict[str, tuple[datetime, float, float]]] = {"first": {}, "second": {}}
        self.margin_per_lot: dict[tuple[str, str, str], tuple[datetime, float]] = {}
        self.integrity_check_at: datetime | None = None
        self.pair: Pair | None = None
        self.unavailable_endpoints: set[str] = set()
        self.worker_disconnect_deadline: datetime | None = None
        self.awaiting_clear_symbol: str | None = None
        self.stopped = False
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
            if self.pair is not None and self.config.execute:
                self._check_active_pair_integrity()
            else:
                self._check_integrity_if_due()
            if self.pair is not None and self._quotes_fresh(self.pair.symbol) and self._risk_breached():
                _LOGGER.warning("risk_limit_triggered completed_trades=%d", self.completed_trades)
                self._halt("risk limit reached")
            if self.pair is not None:
                candidate = self._candidate_for_symbol(self.pair.symbol)
                if candidate is not None and candidate.direction != self.pair.direction:
                    if not self._minimum_hold_elapsed():
                        remaining = self.config.minimum_hold_seconds - (datetime.now(UTC) - self.pair.opened_at).total_seconds()
                        _LOGGER.info(
                            "reverse_signal_deferred_minimum_hold old_direction=%s new_direction=%s remaining_seconds=%.1f",
                            self.pair.direction,
                            candidate.direction,
                            max(0, remaining),
                        )
                        continue
                    _LOGGER.info("reverse_signal_close symbol=%s old_direction=%s new_direction=%s", self.pair.symbol, self.pair.direction, candidate.direction)
                    self._flatten_all()
                    self.completed_trades += 1
                    self.awaiting_clear_symbol, self.pair = self.pair.symbol, None
                continue
            candidate = self._best_candidate()
            if self.awaiting_clear_symbol is not None:
                if self._candidate_for_symbol(self.awaiting_clear_symbol) is None:
                    _LOGGER.info("reentry_rearmed")
                    self.awaiting_clear_symbol = None
                elif candidate is None or candidate.symbol == self.awaiting_clear_symbol:
                    continue
            if candidate is not None and self.completed_trades < self.config.maximum_trades:
                self._open(candidate)
            elif candidate is not None:
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

    def _verify_active_workers(self) -> None:
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

    def _load_shared_symbols(self) -> dict[str, float]:
        catalogs = {name: self._catalog_points(endpoint) for name, endpoint in self.endpoints.items()}
        shared = {
            symbol: max(catalogs["first"][symbol], catalogs["second"][symbol])
            for symbol in catalogs["first"].keys() & catalogs["second"].keys()
        }
        if not shared:
            raise StrategyError("Selected Workers have no shared tradable symbols.")
        _LOGGER.info("shared_symbols_loaded count=%d", len(shared))
        return shared

    def _configure_live_symbols(self) -> None:
        symbols = sorted(self.shared_symbols)
        for endpoint in self.endpoints.values():
            try:
                result = self.gateway.request(
                    endpoint.worker_id,
                    kind="operation",
                    request={"type": "set_live_symbols", "symbols": symbols},
                )
            except StrategyError as error:
                raise StrategyError(f"Worker {endpoint.worker_id} rejected live symbol configuration.") from error
            if result.get("symbols") != symbols:
                raise StrategyError(f"Worker {endpoint.worker_id} returned an invalid live symbol configuration.")
            _LOGGER.info("live_symbols_configured worker=%s count=%d", endpoint.worker_id, len(symbols))

    def _catalog_points(self, endpoint: Endpoint) -> dict[str, float]:
        try:
            result = self.gateway.request(endpoint.worker_id, kind="read", request={"type": "symbols"})
        except StrategyError as error:
            raise StrategyError(f"Worker {endpoint.worker_id} returned no symbol catalog.") from error
        symbols = result.get("symbols")
        if not isinstance(symbols, list):
            raise StrategyError(f"Worker {endpoint.worker_id} returned an invalid symbol catalog.")
        points: dict[str, float] = {}
        for symbol in symbols:
            if not isinstance(symbol, dict):
                raise StrategyError(f"Worker {endpoint.worker_id} returned an invalid symbol catalog.")
            symbol_name, trade_mode, point = symbol.get("name"), symbol.get("trade_mode"), symbol.get("point")
            if not isinstance(symbol_name, str) or not symbol_name or symbol_name in points:
                raise StrategyError(f"Worker {endpoint.worker_id} returned an invalid symbol catalog.")
            if isinstance(trade_mode, bool) or not isinstance(trade_mode, int):
                raise StrategyError(f"Worker {endpoint.worker_id} returned an invalid symbol catalog.")
            if trade_mode != 4:
                continue
            if not _positive_finite(point):
                raise StrategyError(f"Worker {endpoint.worker_id} returned an invalid symbol catalog.")
            points[symbol_name] = float(point)
        return points

    def _consume(self, message: dict[str, object]) -> bool:
        worker, observed_at, quotes = message.get("worker_id"), message.get("observed_at"), message.get("quotes")
        name = next((key for key, endpoint in self.endpoints.items() if endpoint.worker_id == worker), None)
        if name is None or not isinstance(observed_at, str) or not isinstance(quotes, list):
            return False
        try:
            timestamp = _utc(observed_at)
        except (StrategyError, ValueError):
            _LOGGER.warning("quote_ignored endpoint=%s reason=invalid_observed_at", name)
            return False
        consumed = False
        for quote in quotes:
            if not isinstance(quote, dict) or not isinstance(quote.get("symbol"), str) or not quote["symbol"]:
                continue
            try:
                bid, ask = _positive(quote.get("bid"), "bid"), _positive(quote.get("ask"), "ask")
            except StrategyError:
                _LOGGER.warning("quote_ignored endpoint=%s symbol=%s reason=invalid_price", name, quote["symbol"])
                continue
            self.quotes[name][quote["symbol"]] = (timestamp, bid, ask)
            _LOGGER.debug("quote_updated endpoint=%s symbol=%s bid=%s ask=%s observed_at=%s", name, quote["symbol"], bid, ask, observed_at)
            consumed = True
        return consumed

    def _suspend_for_worker_disconnect(self, error: WorkerMarketDataUnavailable) -> None:
        name = next((key for key, endpoint in self.endpoints.items() if endpoint.worker_id == error.worker_id), None)
        if name is None:
            raise StrategyError(f"Controller reported market-data loss for unselected Worker {error.worker_id}.")
        self.quotes[name].clear()
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

    def _quotes_fresh(self, symbol: str) -> bool:
        values = [self.quotes[name].get(symbol) for name in ("first", "second")]
        return all(value is not None and (datetime.now(UTC) - value[0]).total_seconds() <= self.config.quote_max_age_seconds for value in values)

    def _candidate_for_symbol(self, symbol: str) -> TradeCandidate | None:
        point = self.shared_symbols.get(symbol)
        if point is None or not self._quotes_fresh(symbol):
            return None
        _, first_bid, first_ask = self.quotes["first"][symbol]
        _, second_bid, second_ask = self.quotes["second"][symbol]
        threshold = self.config.entry_edge_points * point
        candidates = (
            TradeCandidate(symbol, "short_first_long_second", first_bid - second_ask),
            TradeCandidate(symbol, "long_first_short_second", second_bid - first_ask),
        )
        eligible = [candidate for candidate in candidates if candidate.edge + 1e-12 >= threshold]
        return min(eligible, key=lambda candidate: (-candidate.edge, candidate.direction)) if eligible else None

    def _best_candidate(self) -> TradeCandidate | None:
        symbols = sorted(self.shared_symbols.keys() & self.quotes["first"].keys() & self.quotes["second"].keys())
        candidates = [candidate for symbol in symbols if (candidate := self._candidate_for_symbol(symbol)) is not None]
        return min(candidates, key=lambda candidate: (-candidate.edge, candidate.symbol, candidate.direction)) if candidates else None

    def _refresh_margin(self, symbol: str, name: str, direction: str) -> float:
        key = symbol, name, direction
        cached = self.margin_per_lot.get(key)
        now = datetime.now(UTC)
        if cached is not None and now - cached[0] < timedelta(hours=1):
            return cached[1]
        _, bid, ask = self.quotes[name][symbol]
        endpoint = self.endpoints[name]
        price = ask if direction == "LONG" else bid
        result = self.gateway.request(endpoint.worker_id, kind="read", request={"type": "calc_margin", "symbol": symbol, "volume": "1.00", "direction": direction, "price": f"{price:.10f}"})
        margin = _positive(result.get("margin"), "calculated margin")
        self.margin_per_lot[key] = now, margin
        _LOGGER.info("margin_refreshed endpoint=%s symbol=%s direction=%s per_lot=%.2f", name, symbol, direction, margin)
        return margin

    def _open(self, candidate: TradeCandidate) -> None:
        symbol, direction = candidate.symbol, candidate.direction
        first_direction, second_direction = ("SHORT", "LONG") if direction == "short_first_long_second" else ("LONG", "SHORT")
        _LOGGER.info("entry_signal symbol=%s direction=%s edge=%s first_side=%s second_side=%s", symbol, direction, candidate.edge, first_direction, second_direction)
        for name, side in (("first", first_direction), ("second", second_direction)):
            required = self.config.lots * self._refresh_margin(symbol, name, side)
            limit = self.accounts[name].equity * 0.5
            if required > limit:
                _LOGGER.warning("margin_limit_triggered endpoint=%s required=%.2f limit=%.2f", name, required, limit)
                self._halt(f"warning: {name} margin exceeds 50% of equity")
        try:
            self._market("first", symbol, first_direction)
            self._market("second", symbol, second_direction)
            if not self.config.execute:
                _, first_bid, first_ask = self.quotes["first"][symbol]
                _, second_bid, second_ask = self.quotes["second"][symbol]
                self.pair = Pair(
                    symbol, direction, first_bid if first_direction == "SHORT" else first_ask,
                    second_ask if second_direction == "LONG" else second_bid,
                    self.accounts["first"].equity, self.accounts["second"].equity, 0, 0, first_direction, second_direction,
                    datetime.now(UTC),
                )
                return
            first_position = self._expected_position("first", symbol, first_direction)
            second_position = self._expected_position("second", symbol, second_direction)
            self._set_emergency_protection("first", symbol, first_position, first_direction)
            self._set_emergency_protection("second", symbol, second_position, second_direction)
        except Exception as error:
            self._halt(f"unhedged entry: {error}")
        self.pair = Pair(
            symbol,
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
        _LOGGER.info("pair_opened symbol=%s direction=%s first_entry=%.5f second_entry=%.5f", symbol, direction, self.pair.first_entry, self.pair.second_entry)

    def _market(self, name: str, symbol: str, direction: str) -> None:
        if self.config.execute:
            endpoint = self.endpoints[name]
            _LOGGER.info("market_order_submit endpoint=%s worker=%s direction=%s symbol=%s lots=%.2f", name, endpoint.worker_id, direction, symbol, self.config.lots)
            self.gateway.request(endpoint.worker_id, kind="operation", request={"type": "market", "symbol": symbol, "volume": f"{self.config.lots:.2f}", "direction": direction, "filling_mode": "IOC"})
            _LOGGER.info("market_order_completed endpoint=%s worker=%s", name, endpoint.worker_id)
        else:
            _LOGGER.info("market_order_dry_run endpoint=%s direction=%s", name, direction)

    def _expected_position(self, name: str, symbol: str, direction: str) -> dict[str, object]:
        endpoint = self.endpoints[name]
        result = self.gateway.request(endpoint.worker_id, kind="read", request={"type": "current_positions"})
        positions = result.get("positions")
        expected_type = 0 if direction == "LONG" else 1
        matching = [
            position
            for position in positions if isinstance(position, dict)
            and position.get("symbol") == symbol
            and position.get("type") == expected_type
            and position.get("volume") == self.config.lots
        ] if isinstance(positions, list) else []
        if len(matching) != 1:
            raise StrategyError(f"Worker {endpoint.worker_id} did not confirm one expected position.")
        return matching[0]

    def _set_emergency_protection(self, name: str, symbol: str, position: dict[str, object], direction: str) -> None:
        if not self.config.execute:
            return
        endpoint = self.endpoints[name]
        entry = _positive(position.get("price_open"), "position price_open")
        sl = self._profit_target_price(endpoint, symbol, direction, entry, -self.config.emergency_stop_loss_usd)
        tp = self._profit_target_price(endpoint, symbol, direction, entry, self.config.emergency_stop_loss_usd)
        self.gateway.request(
            endpoint.worker_id,
            kind="operation",
            request={
                "type": "modify_sl_tp",
                "symbol": symbol,
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

    def _profit_target_price(self, endpoint: Endpoint, symbol: str, direction: str, entry: float, target_profit: float) -> float:
        lower, upper = entry * 0.5, entry * 1.5
        for _ in range(32):
            candidate = (lower + upper) / 2
            result = self.gateway.request(
                endpoint.worker_id,
                kind="read",
                request={
                    "type": "calc_profit", "symbol": symbol, "volume": f"{self.config.lots:.2f}",
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
                and positions[0].get("symbol") == self.pair.symbol
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
        _, first_bid, first_ask = self.quotes["first"][self.pair.symbol]
        _, second_bid, second_ask = self.quotes["second"][self.pair.symbol]
        return {
            "first": self._calculated_profit(
                "first",
                self.pair.first_direction,
                self.pair.first_entry,
                first_ask if self.pair.first_direction == "SHORT" else first_bid,
            ),
            "second": self._calculated_profit(
                "second",
                self.pair.second_direction,
                self.pair.second_entry,
                second_ask if self.pair.second_direction == "SHORT" else second_bid,
            ),
        }

    def _calculated_profit(self, name: str, direction: str, open_price: float, close_price: float) -> float:
        assert self.pair is not None
        endpoint = self.endpoints[name]
        result = self.gateway.request(
            endpoint.worker_id,
            kind="read",
            request={
                "type": "calc_profit",
                "symbol": self.pair.symbol,
                "volume": f"{self.config.lots:.2f}",
                "direction": direction,
                "open_price": f"{open_price:.10f}",
                "close_price": f"{close_price:.10f}",
            },
        )
        profit = result.get("profit")
        if isinstance(profit, bool) or not isinstance(profit, (int, float)) or not math.isfinite(profit):
            raise StrategyError("Worker returned invalid calculated profit.")
        return float(profit)

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


def _positive_finite(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value) and value > 0


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
    parser.add_argument("--entry-edge-points", type=float, default=4)
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
    if min(args.entry_edge_points, args.lots, args.max_trades, args.quote_max_age_seconds, args.daily_loss_percent, args.trade_loss_percent, args.emergency_stop_loss_usd, args.integrity_check_seconds, args.minimum_hold_seconds, args.worker_disconnect_grace_seconds) <= 0:
        parser.error("All limits must be positive.")
    config = StrategyConfig(
        args.entry_edge_points, args.lots, args.max_trades, args.daily_loss_percent / 100, args.trade_loss_percent / 100,
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
        strategy = RealtimeArbitrage(gateway, first=Endpoint(first_worker), second=Endpoint(second_worker), config=config)
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
