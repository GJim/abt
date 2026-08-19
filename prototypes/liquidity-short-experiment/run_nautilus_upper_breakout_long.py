"""PROTOTYPE — tick-driven NautilusTrader EURUSDC upper-breakout-long control.

This is deliberately research code.  It reuses the agreed H1/M15 zone rules
from ``run_upper_breakout_long.py`` and asks NautilusTrader's simulated
exchange to execute each triggered order against City's recorded bid/ask ticks.
"""

from __future__ import annotations

import argparse
import json
from collections import deque
from dataclasses import asdict, dataclass
from decimal import Decimal
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator

import numpy as np
import pyarrow.parquet as pq
from nautilus_trader.trading.strategy import Strategy

from run import H1, M15, POSITION_LIFETIME_H1_BARS, PIP, _trading_expiry, _write_json
from run_upper_breakout_long import CANCELLATION_PIPS, _build_zones


DEFAULT_DATA_ROOT = Path(
    r"C:\Users\gjim\.copilot\session-state\6e8ac916-d1a8-4af7-806e-62fc6bf0d748"
    r"\files\city-realistic-backtest"
)
NANOSECONDS = 1_000_000_000


@dataclass
class OrderLedger:
    zone_id: str
    activated_at: int
    planned_entry: float
    expires_at: int
    status: str = "not_reached"
    status_time: int | None = None
    reason: str | None = None
    client_order_id: str | None = None


@dataclass
class TradeLedger:
    zone_id: str
    entry_time: int
    entry_price: float
    stop_loss: float
    take_profit: float
    entry_order_id: str
    exit_time: int | None = None
    exit_price: float | None = None
    outcome: str = "open"
    exit_order_id: str | None = None
    position_expires_at: int | None = None
    exit_requested: str | None = None


@dataclass(frozen=True)
class NativeClosedPosition:
    opening_order_id: str
    closing_order_id: str
    realized_pnl: float
    currency: str
    realized_return: float


def main() -> int:
    args = _parse_args()
    _require_nautilus()
    from nautilus_trader.backtest.engine import BacktestEngine
    from nautilus_trader.model.currencies import EUR, USD
    from nautilus_trader.model.enums import AccountType, OmsType
    from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
    from nautilus_trader.model.instruments import CurrencyPair
    from nautilus_trader.model.objects import Money, Price, Quantity

    h1_bars = _load_bars(args.h1_rates, H1)
    m15_bars = _load_bars(args.m15_rates, M15)
    symbol = json.loads(args.symbol_info.read_text(encoding="utf-8"))
    quantity_base_units = _quantity_base_units(args.volume_lots, symbol)
    zones = _build_zones(h1_bars, m15_bars)
    start_ns = _to_ns(args.start)
    end_ns = _to_ns(args.end) if args.end else None
    # A slice must retain orders activated before its first tick when they have
    # not yet reached their 24-H1-bar expiry.
    active_zones = [zone for zone in zones if zone.order_expires_at * NANOSECONDS > start_ns]

    venue = Venue("CITY")
    instrument_id = InstrumentId(Symbol("EURUSDC"), venue)
    instrument = CurrencyPair(
        instrument_id=instrument_id,
        raw_symbol=Symbol("EURUSDC"),
        base_currency=EUR,
        quote_currency=USD,
        price_precision=5,
        size_precision=0,
        price_increment=Price.from_str("0.00001"),
        size_increment=Quantity.from_int(_base_units(symbol["volume_step"], symbol)),
        ts_event=start_ns,
        ts_init=start_ns,
        lot_size=Quantity.from_int(int(symbol["trade_contract_size"])),
        min_quantity=Quantity.from_int(_base_units(symbol["volume_min"], symbol)),
        maker_fee=Decimal("0"),
        taker_fee=Decimal("0"),
    )
    strategy = UpperBreakoutLongStrategy(
        instrument_id=instrument_id,
        zones=active_zones,
        h1_bars=h1_bars,
        quantity_base_units=quantity_base_units,
        take_profit_pips=args.take_profit_pips,
        stop_loss_pips=args.stop_loss_pips,
    )
    engine = BacktestEngine()
    engine.add_venue(
        venue=venue,
        oms_type=OmsType.HEDGING,
        account_type=AccountType.MARGIN,
        starting_balances=[Money(args.starting_balance, USD)],
        base_currency=USD,
        default_leverage=Decimal(str(args.leverage)),
        reject_stop_orders=False,
    )
    engine.add_instrument(instrument)
    engine.add_strategy(strategy)
    stream = TickStream(
        catalog=args.tick_catalog,
        instrument_id=instrument_id,
        start_ns=start_ns,
        end_ns=end_ns,
        batch_size=args.batch_size,
    )
    engine.add_data_iterator("city-eurusdc-parquet", stream.iter_data())
    try:
        engine.run()
        strategy.assert_native_reconciliation()
    finally:
        engine.dispose()

    report = _report(args, symbol, quantity_base_units, zones, active_zones, strategy, stream)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(args.output_dir / "zones.json", [asdict(zone) for zone in active_zones])
    _write_json(args.output_dir / "orders.json", [asdict(value) for value in strategy.orders.values()])
    _write_json(args.output_dir / "trades.json", [asdict(value) for value in strategy.trades.values()])
    _write_json(args.output_dir / "report.json", report)
    print(json.dumps(report, indent=2))
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PROTOTYPE: stream City EURUSDC ticks through NautilusTrader's BacktestEngine.",
    )
    parser.add_argument("--h1-rates", type=Path, default=DEFAULT_DATA_ROOT / "eurusdc-h1-20k.json")
    parser.add_argument("--m15-rates", type=Path, default=DEFAULT_DATA_ROOT / "eurusdc-m15-20k.json")
    parser.add_argument("--tick-catalog", type=Path, default=DEFAULT_DATA_ROOT / "tick-catalog")
    parser.add_argument("--symbol-info", type=Path, default=DEFAULT_DATA_ROOT / "eurusdc-symbol.json")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--start", default="2026-01-01T00:00:00Z")
    parser.add_argument("--end", help="Exclusive UTC ISO-8601 timestamp; omit for all catalog data.")
    parser.add_argument("--batch-size", type=_positive_int, default=100_000)
    parser.add_argument(
        "--volume-lots",
        type=Decimal,
        default=Decimal("0.01"),
        help="City volume in lots; defaults to its observed 0.01 minimum.",
    )
    parser.add_argument("--starting-balance", type=float, default=100_000.0)
    parser.add_argument("--leverage", type=float, default=100.0)
    parser.add_argument("--take-profit-pips", type=float, default=15.0)
    parser.add_argument("--stop-loss-pips", type=float, default=10.0)
    args = parser.parse_args()
    if args.take_profit_pips <= 0 or args.stop_loss_pips <= 0 or args.volume_lots <= 0:
        parser.error("Pip distances and volume must be greater than zero.")
    if args.end and _to_ns(args.end) <= _to_ns(args.start):
        parser.error("--end must be after --start.")
    return args


def _require_nautilus() -> None:
    try:
        import nautilus_trader  # noqa: F401
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Missing dependencies. Run with: uv run --with nautilus_trader --with pyarrow "
            "python prototypes\\liquidity-short-experiment\\run_nautilus_upper_breakout_long.py ..."
        ) from exc


def _load_bars(path: Path, duration: int) -> list:
    from run import _load_bars as load_bars

    return load_bars(path, duration)


def _to_ns(value: str) -> int:
    normalized = value.replace("Z", "+00:00")
    timestamp = datetime.fromisoformat(normalized)
    if timestamp.tzinfo is None:
        raise ValueError(f"Timestamp must include a UTC offset: {value}")
    return int(timestamp.timestamp() * NANOSECONDS)


def _base_units(volume_lots: object, symbol: dict[str, object]) -> int:
    units = Decimal(str(volume_lots)) * Decimal(str(symbol["trade_contract_size"]))
    if units != units.to_integral_value():
        raise ValueError("City volume must convert to a whole base-currency unit quantity.")
    return int(units)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("Value must be a positive integer.")
    return parsed


def _quantity_base_units(volume_lots: Decimal, symbol: dict[str, object]) -> int:
    minimum = Decimal(str(symbol["volume_min"]))
    step = Decimal(str(symbol["volume_step"]))
    if volume_lots < minimum or (volume_lots - minimum) % step != 0:
        raise ValueError(f"--volume-lots must be at least {minimum} and increase by {step}.")
    return _base_units(volume_lots, symbol)


class TickStream:
    """Convert Parquet record batches to bounded QuoteTick lists for engine streaming."""

    def __init__(self, catalog: Path, instrument_id: object, start_ns: int, end_ns: int | None, batch_size: int) -> None:
        self.catalog = catalog
        self.instrument_id = instrument_id
        self.start_ns = start_ns
        self.end_ns = end_ns
        self.batch_size = batch_size
        self.files_read = 0
        self.ticks_streamed = 0
        self.first_tick_ns: int | None = None
        self.last_tick_ns: int | None = None

    def iter_data(self) -> Iterator[list]:
        from nautilus_trader.model.data import QuoteTick

        previous_ns: int | None = None
        for path in sorted(self.catalog.glob("*.parquet")):
            self.files_read += 1
            for batch in pq.ParquetFile(path).iter_batches(
                batch_size=self.batch_size,
                columns=["time_ns", "bid", "ask"],
            ):
                columns = batch.to_pydict()
                times = np.asarray(columns["time_ns"], dtype=np.int64)
                mask = times >= self.start_ns
                if self.end_ns is not None:
                    mask &= times < self.end_ns
                if not mask.any():
                    continue
                times, bids, asks = times[mask], np.asarray(columns["bid"], dtype=float)[mask], np.asarray(
                    columns["ask"], dtype=float,
                )[mask]
                if np.any(bids <= 0) or np.any(asks <= 0) or np.any(asks < bids):
                    raise ValueError(f"{path} contains an invalid bid/ask quote.")
                if np.any(np.diff(times) < 0) or (previous_ns is not None and times[0] < previous_ns):
                    raise ValueError(f"{path} is not globally timestamp ordered.")
                previous_ns = int(times[-1])
                # Despite its historical method name, this bulk API receives
                # floating-point prices/sizes and applies Nautilus fixed-point
                # conversion using the supplied precisions.
                bid_raw = bids.astype(np.float64)
                ask_raw = asks.astype(np.float64)
                sizes = np.ones(len(times), dtype=np.float64)
                ticks = QuoteTick.from_raw_arrays_to_list(
                    self.instrument_id,
                    5,
                    0,
                    bid_raw,
                    ask_raw,
                    sizes,
                    sizes,
                    times.astype(np.uint64),
                    times.astype(np.uint64),
                )
                self.ticks_streamed += len(ticks)
                self.first_tick_ns = self.first_tick_ns or int(times[0])
                self.last_tick_ns = int(times[-1])
                yield ticks


class UpperBreakoutLongStrategy(Strategy):
    """The agreed zone rules, with order fills delegated to NautilusTrader."""

    def __init__(
        self,
        instrument_id: object,
        zones: list,
        h1_bars: list,
        quantity_base_units: int,
        take_profit_pips: float,
        stop_loss_pips: float,
    ) -> None:
        super().__init__()
        self.instrument_id = instrument_id
        self.pending = deque(sorted(zones, key=lambda zone: zone.activated_at))
        self.h1_bars = h1_bars
        self.h1_closes_by_end = {bar.end: bar.close for bar in h1_bars}
        self.quantity_base_units = quantity_base_units
        self.take_profit_pips = take_profit_pips
        self.stop_loss_pips = stop_loss_pips
        self.orders: dict[str, OrderLedger] = {
            zone.zone_id: OrderLedger(zone.zone_id, zone.activated_at, zone.lower, zone.order_expires_at) for zone in zones
        }
        self.trades: dict[str, TradeLedger] = {}
        self._entry_orders: dict[str, tuple[object, object]] = {}
        self._zone_by_entry_order_id: dict[str, str] = {}
        self._positions_by_zone: dict[str, object] = {}
        self.native_closed_positions: dict[str, NativeClosedPosition] = {}

    def on_start(self) -> None:
        self.subscribe_quote_ticks(self.instrument_id)

    def on_quote_tick(self, tick: object) -> None:
        now_ns = int(tick.ts_event)
        now = now_ns // NANOSECONDS
        while self.pending and self.pending[0].activated_at <= now:
            zone = self.pending.popleft()
            self._submit_entry(zone)
        for client_id, (zone, order) in list(self._entry_orders.items()):
            ledger = self.orders[zone.zone_id]
            if now >= zone.order_expires_at:
                self._cancel_entry(client_id, order, now, "order_timeout")
            elif tick.bid_price.as_double() <= zone.lower - CANCELLATION_PIPS * PIP:
                self._cancel_entry(client_id, order, now, "cancellation_before_fill")
            elif self.h1_closes_by_end.get(now, float("-inf")) > zone.pivot_price:
                self._cancel_entry(client_id, order, now, "pivot_breakout_before_fill")

        for zone_id, trade in list(self.trades.items()):
            if trade.outcome != "open" or trade.exit_requested is not None:
                continue
            if tick.bid_price.as_double() <= trade.stop_loss:
                self._request_native_exit(zone_id, "sl")
            elif tick.bid_price.as_double() >= trade.take_profit:
                self._request_native_exit(zone_id, "tp")
            elif trade.position_expires_at is not None and now >= trade.position_expires_at:
                self._request_native_exit(zone_id, "timeout")

    def _submit_entry(self, zone: object) -> None:
        from nautilus_trader.model.enums import OrderSide, TimeInForce
        from nautilus_trader.model.objects import Price, Quantity

        order = self.order_factory.stop_market(
            self.instrument_id,
            OrderSide.BUY,
            Quantity.from_int(self.quantity_base_units),
            Price.from_str(f"{zone.lower:.5f}"),
            time_in_force=TimeInForce.GTD,
            expire_time=datetime.fromtimestamp(zone.order_expires_at, UTC),
        )
        client_id = str(order.client_order_id)
        self.orders[zone.zone_id].status = "submitted"
        self.orders[zone.zone_id].client_order_id = client_id
        self._entry_orders[client_id] = (zone, order)
        self.submit_order(order)

    def _cancel_entry(self, client_id: str, order: object, now: int, reason: str) -> None:
        ledger = self.orders[self._entry_orders[client_id][0].zone_id]
        ledger.status, ledger.status_time, ledger.reason = "cancel_requested", now, reason
        self.cancel_order(order)

    def on_order_filled(self, event: object) -> None:
        client_id = str(event.client_order_id)
        timestamp = int(event.ts_event) // NANOSECONDS
        price = event.last_px.as_double()
        if client_id in self._entry_orders:
            zone, _ = self._entry_orders.pop(client_id)
            ledger = self.orders[zone.zone_id]
            ledger.status, ledger.status_time, ledger.reason = "filled", timestamp, None
            self._open_trade(zone, client_id, timestamp, price)
            return

    def on_order_canceled(self, event: object) -> None:
        client_id = str(event.client_order_id)
        if client_id in self._entry_orders:
            zone, _ = self._entry_orders.pop(client_id)
            ledger = self.orders[zone.zone_id]
            if ledger.status != "cancel_requested":
                ledger.status, ledger.status_time, ledger.reason = "cancelled", int(event.ts_event) // NANOSECONDS, "engine_cancelled"
            else:
                ledger.status = "cancelled"

    def on_order_expired(self, event: object) -> None:
        client_id = str(event.client_order_id)
        if client_id in self._entry_orders:
            zone, _ = self._entry_orders.pop(client_id)
            ledger = self.orders[zone.zone_id]
            ledger.status, ledger.status_time, ledger.reason = "cancelled", int(event.ts_event) // NANOSECONDS, "order_timeout"

    def on_position_opened(self, event: object) -> None:
        entry_order_id = str(event.opening_order_id)
        zone_id = self._zone_by_entry_order_id.get(entry_order_id)
        if zone_id is not None:
            self._positions_by_zone[zone_id] = self.cache.position(event.position_id)

    def on_position_closed(self, event: object) -> None:
        entry_order_id = str(event.opening_order_id)
        closing_order_id = str(event.closing_order_id)
        pnl = event.realized_pnl
        self.native_closed_positions[closing_order_id] = NativeClosedPosition(
            opening_order_id=entry_order_id,
            closing_order_id=closing_order_id,
            realized_pnl=pnl.as_double(),
            currency=str(pnl.currency),
            realized_return=float(event.realized_return),
        )
        zone_id = self._zone_by_entry_order_id.get(entry_order_id)
        if zone_id is None:
            return
        trade = self.trades[zone_id]
        if trade.exit_requested is None:
            raise RuntimeError(f"Native position {entry_order_id} closed without a strategy exit request.")
        trade.exit_time = int(event.ts_event) // NANOSECONDS
        trade.exit_price = float(event.avg_px_close)
        trade.outcome = trade.exit_requested
        trade.exit_order_id = closing_order_id
        self._positions_by_zone.pop(zone_id, None)

    def assert_native_reconciliation(self) -> None:
        completed = [trade for trade in self.trades.values() if trade.outcome != "open"]
        ledger_entry_ids = {trade.entry_order_id for trade in completed}
        native_entry_ids = {position.opening_order_id for position in self.native_closed_positions.values()}
        if ledger_entry_ids != native_entry_ids:
            raise RuntimeError(
                "Native position closures do not match the strategy trade ledger: "
                f"ledger={sorted(ledger_entry_ids)!r}, native={sorted(native_entry_ids)!r}",
            )

    def _open_trade(self, zone: object, entry_order_id: str, entry_time: int, entry_price: float) -> None:
        stop_loss = entry_price - self.stop_loss_pips * PIP
        take_profit = entry_price + self.take_profit_pips * PIP
        self.trades[zone.zone_id] = TradeLedger(
            zone.zone_id,
            entry_time,
            entry_price,
            stop_loss,
            take_profit,
            entry_order_id,
            position_expires_at=_trading_expiry(self.h1_bars, entry_time, POSITION_LIFETIME_H1_BARS),
        )

        self._zone_by_entry_order_id[entry_order_id] = zone.zone_id

    def _request_native_exit(self, zone_id: str, outcome: str) -> None:
        trade = self.trades[zone_id]
        position = self._positions_by_zone.get(zone_id)
        if position is None:
            raise RuntimeError(f"Filled {zone_id} has no native position to close.")
        trade.exit_requested = outcome
        self.close_position(position)


def _report(
    args: argparse.Namespace,
    symbol: dict[str, object],
    quantity_base_units: int,
    all_zones: list,
    active_zones: list,
    strategy: UpperBreakoutLongStrategy,
    stream: TickStream,
) -> dict:
    completed = [trade for trade in strategy.trades.values() if trade.outcome != "open"]
    native = list(strategy.native_closed_positions.values())
    return {
        "prototype": True,
        "implementation": {
            "engine": "NautilusTrader BacktestEngine",
            "market_data": "BacktestEngine.add_data_iterator over bounded Parquet batches",
            "strategy_rules": (
                "run_upper_breakout_long.py zone construction; native stop-market entry; "
                "strategy TP/SL/timeout trigger followed by native market position close"
            ),
            "oms": "HEDGING (each filled entry has an independent native position)",
            "commission": {"maker_fee": 0, "taker_fee": 0, "explicitly_configured": True},
            "volume_lots": str(args.volume_lots),
            "quantity_base_units": quantity_base_units,
        },
        "input": {
            "catalog": str(args.tick_catalog),
            "start": args.start,
            "end_exclusive": args.end,
            "catalog_files_scanned": stream.files_read,
            "ticks_streamed": stream.ticks_streamed,
            "first_tick_ns": stream.first_tick_ns,
            "last_tick_ns": stream.last_tick_ns,
            "zones_constructed_from_bars": len(all_zones),
            "zones_present_in_window": len(active_zones),
        },
        "summary": {
            "orders": len(strategy.orders),
            "orders_filled": sum(order.status == "filled" for order in strategy.orders.values()),
            "completed_trades": len(completed),
            "tp": sum(trade.outcome == "tp" for trade in completed),
            "sl": sum(trade.outcome == "sl" for trade in completed),
            "timeout": sum(trade.outcome == "timeout" for trade in completed),
            "open_at_end": sum(trade.outcome == "open" for trade in strategy.trades.values()),
        },
        "native_engine": {
            "closed_positions": len(native),
            "winning_positions": sum(position.realized_pnl > 0 for position in native),
            "losing_positions": sum(position.realized_pnl < 0 for position in native),
            "breakeven_positions": sum(position.realized_pnl == 0 for position in native),
            "realized_pnl": sum(position.realized_pnl for position in native),
            "currency": native[0].currency if native else "USD",
            "reconciled_to_trade_ledger": True,
        },
        "swap": {
            "modeled": False,
            "limitation": "NautilusTrader's simulated venue is not configured with City rollover financing. Results exclude swap.",
            "observed_city_symbol_metadata": {
                "swap_mode": symbol.get("swap_mode"),
                "swap_long": symbol.get("swap_long"),
                "swap_short": symbol.get("swap_short"),
                "swap_rollover3days": symbol.get("swap_rollover3days"),
                "captured_at_utc": symbol.get("time_utc"),
            },
        },
        "limitations": [
            "City Parquet files are not a Nautilus native catalog layout; this runner streams their rows directly into the actual engine.",
            "Entry and exit prices are the simulated bid/ask tick fills, not the M15-bar assumed boundary prices.",
            "Nautilus OrderFactory cannot attach conditional reduce-only exits to a HEDGING position without a position ID. "
            "The strategy therefore requests a native market close on the first tick meeting TP, SL, or timeout.",
            "Zone activation, cancellation, and timeout checks run when the next quote arrives; no quote means no action until that quote.",
            "Only ticks inside the requested window are executed. A slice is not a full-year result.",
        ],
    }


if __name__ == "__main__":
    raise SystemExit(main())
