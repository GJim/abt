"""PROTOTYPE — EURUSD upper-liquidity short experiment.

This is a research harness, not a live trading system. It implements the
confirmed H1/M15 strategy specification and deliberately ignores execution
costs. It writes its complete zone, order, trade, and group ledgers to JSON.
"""

from __future__ import annotations

import argparse
import json
from bisect import bisect_left, bisect_right
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from math import sqrt
from pathlib import Path
from statistics import median
from typing import Iterable


H1 = 60 * 60
M15 = 15 * 60
PIP = 0.0001
SWING_LENGTH = 3
ATR_PERIOD = 14
ZONE_ATR_MULTIPLIER = 0.10
ORDER_LIFETIME_H1_BARS = 24
POSITION_LIFETIME_H1_BARS = 24
_BAR_TIMES: dict[int, list[int]] = {}


@dataclass(frozen=True)
class Bar:
    time: int
    open: float
    high: float
    low: float
    close: float

    @property
    def end(self) -> int:
        return self.time + (H1 if self.duration == H1 else M15)

    duration: int


@dataclass(frozen=True)
class Zone:
    zone_id: str
    pivot_time: int
    pivot_price: float
    lower: float
    upper: float
    atr: float
    activated_at: int
    order_expires_at: int


@dataclass(frozen=True)
class OrderResult:
    zone: Zone
    status: str
    status_time: int
    entry_time: int | None = None
    entry_price: float | None = None
    cancel_reason: str | None = None
    immediate_outcome: str | None = None


@dataclass(frozen=True)
class Trade:
    zone_id: str
    entry_time: int
    entry_price: float
    stop_loss: float
    take_profit: float
    exit_time: int
    exit_price: float
    outcome: str
    risk_pips: float
    zone_freshness_h1_bars: int
    atr: float
    overlapping_zone: bool


def main() -> int:
    parser = argparse.ArgumentParser(description="PROTOTYPE: EURUSD upper-liquidity short experiment")
    parser.add_argument("--h1-rates", type=Path, required=True, help="`mt5 --output json rates-* EURUSD H1` export.")
    parser.add_argument("--m15-rates", type=Path, required=True, help="`mt5 --output json rates-* EURUSD M15` export.")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    h1_bars = _load_bars(args.h1_rates, H1)
    m15_bars = _load_bars(args.m15_rates, M15)
    _validate_inputs(h1_bars, m15_bars)

    zones = _build_zones(h1_bars, m15_bars)
    order_results = [_simulate_order(zone, h1_bars, m15_bars) for zone in zones]
    trades = _simulate_trades(order_results, h1_bars, m15_bars)
    _write_report(args.output_dir, zones, order_results, trades)
    return 0


def _load_bars(path: Path, duration: int) -> list[Bar]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    records = raw["records"] if isinstance(raw, dict) else raw
    bars = [
        Bar(
            time=_record_time(record),
            open=float(record["open"]),
            high=float(record["high"]),
            low=float(record["low"]),
            close=float(record["close"]),
            duration=duration,
        )
        for record in records
    ]
    return sorted(bars, key=lambda bar: bar.time)


def _record_time(record: dict[str, object]) -> int:
    value = record.get("time")
    if not isinstance(value, int):
        raise ValueError("The mt5 JSON export must include raw MT5 market-data time values.")
    return value


def _validate_inputs(h1_bars: list[Bar], m15_bars: list[Bar]) -> None:
    if len(h1_bars) < ATR_PERIOD + SWING_LENGTH * 2 + 2:
        raise ValueError("H1 input does not contain enough completed bars for ATR and swing confirmation.")
    if not m15_bars:
        raise ValueError("M15 input is empty.")
    for bars, label in ((h1_bars, "H1"), (m15_bars, "M15")):
        times = [bar.time for bar in bars]
        if len(times) != len(set(times)):
            raise ValueError(f"{label} input contains duplicate calibrated timestamps.")


def _build_zones(h1_bars: list[Bar], m15_bars: list[Bar]) -> list[Zone]:
    atrs = _wilder_atr(h1_bars)
    zones: list[Zone] = []
    for pivot_index in range(SWING_LENGTH, len(h1_bars) - SWING_LENGTH):
        atr = atrs[pivot_index]
        if atr is None or not _is_swing_high(h1_bars, pivot_index):
            continue
        confirmation_index = pivot_index + SWING_LENGTH
        pivot = h1_bars[pivot_index]
        lower = pivot.high - atr * ZONE_ATR_MULTIPLIER
        upper = pivot.high + atr * ZONE_ATR_MULTIPLIER
        if (upper - lower) / PIP >= 5:
            continue

        second_touch_index = _second_touch_index(
            h1_bars,
            start_index=confirmation_index + 1,
            lower=lower,
            upper=upper,
            pivot_price=pivot.high,
        )
        if second_touch_index is None:
            continue
        activated_at = h1_bars[second_touch_index].end
        last_m15 = _last_completed_bar(m15_bars, activated_at)
        if last_m15 is None or last_m15.close >= lower:
            continue
        expiry_index = _future_h1_index(h1_bars, second_touch_index, ORDER_LIFETIME_H1_BARS)
        if expiry_index is None:
            continue
        zones.append(
            Zone(
                zone_id=f"upper-{pivot.time}",
                pivot_time=pivot.time,
                pivot_price=pivot.high,
                lower=lower,
                upper=upper,
                atr=atr,
                activated_at=activated_at,
                order_expires_at=h1_bars[expiry_index].end,
            )
        )
    return zones


def _wilder_atr(bars: list[Bar]) -> list[float | None]:
    true_ranges: list[float] = [0.0]
    for previous, current in zip(bars, bars[1:]):
        true_ranges.append(max(current.high - current.low, abs(current.high - previous.close), abs(current.low - previous.close)))
    atrs: list[float | None] = [None] * len(bars)
    if len(bars) <= ATR_PERIOD:
        return atrs
    value = sum(true_ranges[1 : ATR_PERIOD + 1]) / ATR_PERIOD
    atrs[ATR_PERIOD] = value
    for index in range(ATR_PERIOD + 1, len(bars)):
        value = (value * (ATR_PERIOD - 1) + true_ranges[index]) / ATR_PERIOD
        atrs[index] = value
    return atrs


def _is_swing_high(bars: list[Bar], index: int) -> bool:
    high = bars[index].high
    neighborhood = bars[index - SWING_LENGTH : index] + bars[index + 1 : index + SWING_LENGTH + 1]
    return all(high > bar.high for bar in neighborhood)


def _second_touch_index(
    bars: list[Bar],
    *,
    start_index: int,
    lower: float,
    upper: float,
    pivot_price: float,
) -> int | None:
    for index in range(start_index, len(bars)):
        bar = bars[index]
        if bar.high >= lower and bar.low <= upper:
            if bar.close <= pivot_price:
                return index
            return None
    return None


def _future_h1_index(bars: list[Bar], start_index: int, count: int) -> int | None:
    index = start_index + count
    return index if index < len(bars) else None


def _last_completed_bar(bars: list[Bar], at: int) -> Bar | None:
    index = bisect_right(_times(bars), at - bars[0].duration)
    return bars[index - 1] if index else None


def _simulate_order(zone: Zone, h1_bars: list[Bar], m15_bars: list[Bar]) -> OrderResult:
    for bar in _bars_between(m15_bars, zone.activated_at, zone.order_expires_at):
        hit_entry = bar.high >= zone.lower
        hit_stop = bar.high >= zone.upper
        hit_target = bar.low <= zone.lower - 10 * PIP
        if hit_entry and hit_stop:
            return OrderResult(
                zone,
                "filled",
                bar.end,
                bar.end,
                zone.lower,
                immediate_outcome="sl",
            )
        if hit_entry and hit_target:
            return OrderResult(zone, "cancelled", bar.end, cancel_reason="ambiguous_entry_and_tp")
        if hit_target:
            return OrderResult(zone, "cancelled", bar.end, cancel_reason="theoretical_tp_before_fill")
        if hit_entry:
            return OrderResult(zone, "filled", bar.end, bar.end, zone.lower)
        if _h1_close_at(h1_bars, bar.end) is not None and _h1_close_at(h1_bars, bar.end) > zone.pivot_price:
            return OrderResult(zone, "cancelled", bar.end, cancel_reason="pivot_breakout_before_fill")
    return OrderResult(zone, "cancelled", zone.order_expires_at, cancel_reason="order_timeout")


def _simulate_trades(order_results: list[OrderResult], h1_bars: list[Bar], m15_bars: list[Bar]) -> list[Trade]:
    filled = [result for result in order_results if result.status == "filled"]
    trades: list[Trade] = []
    for result in filled:
        assert result.entry_time is not None and result.entry_price is not None
        expiry = _trading_expiry(h1_bars, result.entry_time, POSITION_LIFETIME_H1_BARS)
        if expiry is None:
            continue
        freshness = _freshness_h1_bars(h1_bars, result)
        trade = (
            _trade(result, result.status_time, result.zone.upper, result.immediate_outcome, freshness, False)
            if result.immediate_outcome is not None
            else _simulate_trade(result, m15_bars, expiry, freshness)
        )
        if trade is not None:
            trades.append(trade)
    return _mark_overlaps(trades)


def _trading_expiry(h1_bars: list[Bar], entry_time: int, count: int) -> int | None:
    start = bisect_right(_times(h1_bars), entry_time - H1)
    index = start + count - 1
    return h1_bars[index].end if index < len(h1_bars) else None


def _simulate_trade(result: OrderResult, m15_bars: list[Bar], expiry: int, freshness: int) -> Trade | None:
    assert result.entry_time is not None and result.entry_price is not None
    zone = result.zone
    for bar in _bars_between(m15_bars, result.entry_time, expiry):
        hit_stop = bar.high >= zone.upper
        hit_target = bar.low <= result.entry_price - 10 * PIP
        if hit_stop:
            return _trade(result, bar.end, zone.upper, "sl", freshness, False)
        if hit_target:
            return _trade(result, bar.end, result.entry_price - 10 * PIP, "tp", freshness, False)
    timeout_bar = next((bar for bar in m15_bars if bar.end >= expiry), None)
    if timeout_bar is None:
        return None
    return _trade(result, timeout_bar.end, timeout_bar.close, "timeout", freshness, False)


def _freshness_h1_bars(h1_bars: list[Bar], result: OrderResult) -> int:
    assert result.entry_time is not None
    return sum(result.zone.activated_at < bar.end <= result.entry_time for bar in h1_bars)


def _trade(
    result: OrderResult,
    exit_time: int,
    exit_price: float,
    outcome: str,
    freshness: int,
    overlapping: bool,
) -> Trade:
    assert result.entry_time is not None and result.entry_price is not None
    return Trade(
        zone_id=result.zone.zone_id,
        entry_time=result.entry_time,
        entry_price=result.entry_price,
        stop_loss=result.zone.upper,
        take_profit=result.entry_price - 10 * PIP,
        exit_time=exit_time,
        exit_price=exit_price,
        outcome=outcome,
        risk_pips=(result.zone.upper - result.entry_price) / PIP,
        zone_freshness_h1_bars=freshness,
        atr=result.zone.atr,
        overlapping_zone=overlapping,
    )


def _mark_overlaps(trades: list[Trade]) -> list[Trade]:
    marked: list[Trade] = []
    for trade in trades:
        overlapping = sum(
            other.zone_id != trade.zone_id and other.entry_time == trade.entry_time for other in trades
        ) > 0
        marked.append(
            Trade(
                **{**asdict(trade), "overlapping_zone": overlapping},
            )
        )
    return marked


def _bars_between(bars: list[Bar], start: int, end: int) -> Iterable[Bar]:
    times = _times(bars)
    first = bisect_left(times, start)
    last = bisect_right(times, end - bars[0].duration)
    return iter(bars[first:last])


def _h1_close_at(bars: list[Bar], end: int) -> float | None:
    index = bisect_left(_times(bars), end - H1)
    return bars[index].close if index < len(bars) and bars[index].end == end else None


def _times(bars: list[Bar]) -> list[int]:
    return _BAR_TIMES.setdefault(id(bars), [bar.time for bar in bars])


def _write_report(output_dir: Path, zones: list[Zone], orders: list[OrderResult], trades: list[Trade]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    eligible = [trade for trade in trades if not trade.overlapping_zone]
    reports = _reports(eligible)
    reports["summary"]["zones"] = len(zones)
    reports["summary"]["orders_filled"] = sum(order.status == "filled" for order in orders)
    _write_json(output_dir / "zones.json", [_serialize(zone) for zone in zones])
    _write_json(output_dir / "orders.json", [_serialize(order) for order in orders])
    _write_json(output_dir / "trades.json", [_serialize(trade) for trade in trades])
    _write_json(output_dir / "report.json", reports)
    print(json.dumps(reports["summary"], indent=2))


def _reports(trades: list[Trade]) -> dict[str, object]:
    train = [trade for trade in trades if _year(trade.entry_time) in (2024, 2025)]
    validation = [trade for trade in trades if _year(trade.entry_time) == 2026]
    atr_median = median([trade.atr for trade in train]) if train else None
    control_train = _stats(train)
    control_validation = _stats(validation)
    groups = {
        "risk_0_to_2_5_pips": lambda trade: trade.risk_pips < 2.5,
        "risk_2_5_to_5_pips": lambda trade: 2.5 <= trade.risk_pips < 5,
        "freshness_0_to_8_h1": lambda trade: trade.zone_freshness_h1_bars <= 8,
        "freshness_9_to_16_h1": lambda trade: 9 <= trade.zone_freshness_h1_bars <= 16,
        "freshness_17_to_24_h1": lambda trade: 17 <= trade.zone_freshness_h1_bars <= 24,
    }
    if atr_median is not None:
        groups["atr_below_train_median"] = lambda trade: trade.atr < atr_median
        groups["atr_at_or_above_train_median"] = lambda trade: trade.atr >= atr_median
    group_reports = {
        name: _group_report(
            [trade for trade in train if predicate(trade)],
            [trade for trade in validation if predicate(trade)],
            control_train,
            control_validation,
        )
        for name, predicate in groups.items()
    }
    return {
        "summary": {
            "zones": None,
            "eligible_trades": len(trades),
            "train_trades": len(train),
            "validation_trades": len(validation),
            "atr_train_median": atr_median,
        },
        "control": {"train": control_train, "validation": control_validation},
        "groups": group_reports,
    }


def _group_report(
    train: list[Trade],
    validation: list[Trade],
    control_train: dict[str, object],
    control_validation: dict[str, object],
) -> dict[str, object]:
    train_stats = _stats(train)
    validation_stats = _stats(validation)
    return {
        "train": train_stats,
        "validation": validation_stats,
        "selected": _passes(train_stats, control_train) and _passes(validation_stats, control_validation),
    }


def _stats(trades: list[Trade]) -> dict[str, object]:
    total = len(trades)
    successes = sum(trade.outcome == "tp" for trade in trades)
    rate = successes / total if total else None
    return {
        "trades": total,
        "tp_first": successes,
        "sl_first": sum(trade.outcome == "sl" for trade in trades),
        "timeout": sum(trade.outcome == "timeout" for trade in trades),
        "tp_first_rate": rate,
        "wilson_lower_95": _wilson_lower(successes, total) if total else None,
    }


def _passes(group: dict[str, object], control: dict[str, object]) -> bool:
    rate = group["tp_first_rate"]
    lower = group["wilson_lower_95"]
    control_rate = control["tp_first_rate"]
    return (
        isinstance(rate, float)
        and isinstance(lower, float)
        and isinstance(control_rate, float)
        and group["trades"] >= 30
        and rate > 0.5
        and lower > 0.5
        and rate - control_rate >= 0.05
    )


def _wilson_lower(successes: int, total: int) -> float:
    z = 1.959963984540054
    proportion = successes / total
    denominator = 1 + z * z / total
    centre = proportion + z * z / (2 * total)
    spread = z * sqrt(proportion * (1 - proportion) / total + z * z / (4 * total * total))
    return (centre - spread) / denominator


def _year(timestamp: int) -> int:
    return datetime.fromtimestamp(timestamp, UTC).year


def _serialize(value: object) -> dict[str, object]:
    return asdict(value)


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
