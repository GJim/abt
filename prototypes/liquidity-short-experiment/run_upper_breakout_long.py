"""PROTOTYPE — EURUSD upper-liquidity breakout long control experiment."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from run import (
    ATR_PERIOD,
    H1,
    M15,
    ORDER_LIFETIME_H1_BARS,
    PIP,
    POSITION_LIFETIME_H1_BARS,
    SWING_LENGTH,
    ZONE_ATR_MULTIPLIER,
    Bar,
    OrderResult,
    Trade,
    Zone,
    _bars_between,
    _future_h1_index,
    _h1_close_at,
    _is_swing_high,
    _last_completed_bar,
    _load_bars,
    _mark_overlaps,
    _second_touch_index,
    _stats,
    _trading_expiry,
    _validate_inputs,
    _wilder_atr,
    _write_json,
)

CANCELLATION_PIPS = 10


def main() -> int:
    parser = argparse.ArgumentParser(description="PROTOTYPE: EURUSD upper-liquidity breakout long control experiment")
    parser.add_argument("--h1-rates", type=Path, required=True)
    parser.add_argument("--m15-rates", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--take-profit-pips", type=_positive_pips, default=15)
    parser.add_argument("--stop-loss-pips", type=_positive_pips, default=10)
    args = parser.parse_args()

    h1_bars = _load_bars(args.h1_rates, H1)
    m15_bars = _load_bars(args.m15_rates, M15)
    _validate_inputs(h1_bars, m15_bars)
    zones = _build_zones(h1_bars, m15_bars)
    orders = [_simulate_order(zone, h1_bars, m15_bars, args.take_profit_pips) for zone in zones]
    trades = _simulate_trades(orders, h1_bars, m15_bars, args.take_profit_pips, args.stop_loss_pips)
    _write_report(args.output_dir, zones, orders, trades, args.take_profit_pips, args.stop_loss_pips)
    return 0


def _build_zones(h1_bars: list[Bar], m15_bars: list[Bar]) -> list[Zone]:
    atrs = _wilder_atr(h1_bars)
    zones: list[Zone] = []
    for pivot_index in range(SWING_LENGTH, len(h1_bars) - SWING_LENGTH):
        atr = atrs[pivot_index]
        if atr is None or not _is_swing_high(h1_bars, pivot_index):
            continue
        pivot = h1_bars[pivot_index]
        lower = pivot.high - atr * ZONE_ATR_MULTIPLIER
        upper = pivot.high + atr * ZONE_ATR_MULTIPLIER
        if (upper - lower) / PIP >= 5:
            continue
        second_touch_index = _second_touch_index(
            h1_bars,
            start_index=pivot_index + SWING_LENGTH + 1,
            lower=lower,
            upper=upper,
            pivot_price=pivot.high,
        )
        if second_touch_index is None:
            continue
        activated_at = h1_bars[second_touch_index].end
        last_m15 = _last_completed_bar(m15_bars, activated_at)
        if last_m15 is None or not lower - 10 * PIP < last_m15.close < lower:
            continue
        expiry_index = _future_h1_index(h1_bars, second_touch_index, ORDER_LIFETIME_H1_BARS)
        if expiry_index is None:
            continue
        zones.append(
            Zone(
                zone_id=f"upper-long-{pivot.time}",
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


def _simulate_order(
    zone: Zone,
    h1_bars: list[Bar],
    m15_bars: list[Bar],
    take_profit_pips: float,
) -> OrderResult:
    cancellation_price = zone.lower - CANCELLATION_PIPS * PIP
    take_profit = zone.lower + take_profit_pips * PIP
    for bar in _bars_between(m15_bars, zone.activated_at, zone.order_expires_at):
        hit_entry = bar.high >= zone.lower
        hit_cancellation = bar.low <= cancellation_price
        hit_target = bar.high >= take_profit
        if hit_entry and hit_cancellation:
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
        if hit_cancellation:
            return OrderResult(zone, "cancelled", bar.end, cancel_reason="cancellation_before_fill")
        if hit_entry:
            return OrderResult(zone, "filled", bar.end, bar.end, zone.lower)
        close = _h1_close_at(h1_bars, bar.end)
        if close is not None and close > zone.pivot_price:
            return OrderResult(zone, "cancelled", bar.end, cancel_reason="pivot_breakout_before_fill")
    return OrderResult(zone, "cancelled", zone.order_expires_at, cancel_reason="order_timeout")


def _simulate_trades(
    orders: list[OrderResult],
    h1_bars: list[Bar],
    m15_bars: list[Bar],
    take_profit_pips: float,
    stop_loss_pips: float,
) -> list[Trade]:
    trades: list[Trade] = []
    for order in orders:
        if order.status != "filled":
            continue
        assert order.entry_time is not None and order.entry_price is not None
        expiry = _trading_expiry(h1_bars, order.entry_time, POSITION_LIFETIME_H1_BARS)
        if expiry is None:
            continue
        freshness = sum(order.zone.activated_at < bar.end <= order.entry_time for bar in h1_bars)
        if order.immediate_outcome is not None:
            trades.append(
                _trade(
                    order,
                    order.status_time,
                    order.entry_price - stop_loss_pips * PIP,
                    "sl",
                    freshness,
                    False,
                    take_profit_pips,
                    stop_loss_pips,
                )
            )
            continue
        trade = _simulate_trade(order, m15_bars, expiry, freshness, take_profit_pips, stop_loss_pips)
        if trade is not None:
            trades.append(trade)
    return _mark_overlaps(trades)


def _simulate_trade(
    order: OrderResult,
    m15_bars: list[Bar],
    expiry: int,
    freshness: int,
    take_profit_pips: float,
    stop_loss_pips: float,
) -> Trade | None:
    assert order.entry_time is not None and order.entry_price is not None
    stop_loss = order.entry_price - stop_loss_pips * PIP
    take_profit = order.entry_price + take_profit_pips * PIP
    for bar in _bars_between(m15_bars, order.entry_time, expiry):
        if bar.low <= stop_loss:
            return _trade(order, bar.end, stop_loss, "sl", freshness, False, take_profit_pips, stop_loss_pips)
        if bar.high >= take_profit:
            return _trade(order, bar.end, take_profit, "tp", freshness, False, take_profit_pips, stop_loss_pips)
    timeout_bar = next((bar for bar in m15_bars if bar.end >= expiry), None)
    if timeout_bar is None:
        return None
    return _trade(
        order,
        timeout_bar.end,
        timeout_bar.close,
        "timeout",
        freshness,
        False,
        take_profit_pips,
        stop_loss_pips,
    )


def _trade(
    order: OrderResult,
    exit_time: int,
    exit_price: float,
    outcome: str,
    freshness: int,
    overlapping: bool,
    take_profit_pips: float,
    stop_loss_pips: float,
) -> Trade:
    assert order.entry_time is not None and order.entry_price is not None
    return Trade(
        zone_id=order.zone.zone_id,
        entry_time=order.entry_time,
        entry_price=order.entry_price,
        stop_loss=order.entry_price - stop_loss_pips * PIP,
        take_profit=order.entry_price + take_profit_pips * PIP,
        exit_time=exit_time,
        exit_price=exit_price,
        outcome=outcome,
        risk_pips=stop_loss_pips,
        zone_freshness_h1_bars=freshness,
        atr=order.zone.atr,
        overlapping_zone=overlapping,
    )


def _write_report(
    output_dir: Path,
    zones: list[Zone],
    orders: list[OrderResult],
    trades: list[Trade],
    take_profit_pips: float,
    stop_loss_pips: float,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    eligible = [trade for trade in trades if not trade.overlapping_zone]
    train = [trade for trade in eligible if _year(trade.entry_time) in (2024, 2025)]
    validation = [trade for trade in eligible if _year(trade.entry_time) == 2026]
    report = {
        "summary": {
            "take_profit_pips": take_profit_pips,
            "stop_loss_pips": stop_loss_pips,
            "cancellation_pips": CANCELLATION_PIPS,
            "zones": len(zones),
            "orders_filled": sum(order.status == "filled" for order in orders),
            "eligible_trades": len(eligible),
            "train_trades": len(train),
            "validation_trades": len(validation),
        },
        "control": {"train": _stats(train), "validation": _stats(validation)},
    }
    _write_json(output_dir / "zones.json", [asdict(zone) for zone in zones])
    _write_json(output_dir / "orders.json", [asdict(order) for order in orders])
    _write_json(output_dir / "trades.json", [asdict(trade) for trade in trades])
    _write_json(output_dir / "report.json", report)
    print(json.dumps(report, indent=2))


def _year(timestamp: int) -> int:
    return datetime.fromtimestamp(timestamp, UTC).year


def _positive_pips(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("Pip distances must be greater than zero.")
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
