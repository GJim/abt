"""PROTOTYPE — screen fixed London-range-breakout rules on City EURUSDC ticks.

This is an exploratory screen, not a tradable strategy.  It tests the fixed
candidate grid below using 2026-01 through 2026-05 only, then reports the
untouched 2026-06 onward period separately.  It uses recorded bid/ask quotes,
one position per UTC day, and no commission or financing.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq


DEFAULT_CATALOG = Path(
    r"C:\Users\gjim\.copilot\session-state\6e8ac916-d1a8-4af7-806e-62fc6bf0d748"
    r"\files\city-realistic-backtest\tick-catalog"
)
PIP = 0.0001
TRAIN_END_NS = int(datetime(2026, 6, 1, tzinfo=UTC).timestamp() * 1_000_000_000)
SCREEN_CONFIGS = tuple(
    (direction, stop_pips, target_pips)
    for direction in ("long", "short")
    for stop_pips in (5, 10)
    for target_pips in (5, 10, 15)
)


@dataclass(frozen=True)
class Trade:
    direction: str
    entry_ns: int
    exit_ns: int
    entry: float
    exit: float
    outcome: str
    pnl_pips: float


def main() -> int:
    args = _parse_args()
    ticks = _load_ticks(args.tick_catalog)
    reports = [_simulate(ticks, *config) for config in SCREEN_CONFIGS]
    output = {
        "method": {
            "range": "00:00-06:00 UTC; use ask high and bid low",
            "entry_window": "07:00-12:00 UTC; first breakout only",
            "exit": "bid/ask tick-level stop or target; otherwise 21:00 UTC market close",
            "costs": "recorded bid/ask spread included; commission and financing excluded",
            "selection_period": "2026-01-01 through 2026-05-31",
            "holdout_period": "2026-06-01 onward",
        },
        "candidates": reports,
    }
    args.output.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(json.dumps(output, indent=2))
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PROTOTYPE: screen London range breakouts with real City bid/ask ticks.")
    parser.add_argument("--tick-catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def _load_ticks(catalog: Path) -> dict[str, np.ndarray]:
    pieces: list[dict[str, np.ndarray]] = []
    for path in sorted(catalog.glob("*.parquet")):
        table = pq.read_table(path, columns=["time_ns", "bid", "ask"])
        pieces.append({name: np.asarray(table[name]) for name in table.column_names})
    return {name: np.concatenate([piece[name] for piece in pieces]) for name in ("time_ns", "bid", "ask")}


def _simulate(ticks: dict[str, np.ndarray], direction: str, stop_pips: int, target_pips: int) -> dict[str, object]:
    times, bids, asks = ticks["time_ns"], ticks["bid"], ticks["ask"]
    dates = np.array([datetime.fromtimestamp(value / 1_000_000_000, UTC).date() for value in times])
    boundaries = np.r_[0, np.flatnonzero(dates[1:] != dates[:-1]) + 1, len(times)]
    trades: list[Trade] = []
    for first, last in zip(boundaries[:-1], boundaries[1:]):
        day_times, day_bids, day_asks = times[first:last], bids[first:last], asks[first:last]
        hours = np.array([datetime.fromtimestamp(value / 1_000_000_000, UTC).hour for value in day_times])
        range_mask = hours < 6
        entry_mask = (hours >= 7) & (hours < 12)
        if not range_mask.any() or not entry_mask.any():
            continue
        upper, lower = day_asks[range_mask].max(), day_bids[range_mask].min()
        entry_indices = np.flatnonzero(entry_mask)
        if direction == "long":
            crossed = entry_indices[day_asks[entry_indices] >= upper]
        else:
            crossed = entry_indices[day_bids[entry_indices] <= lower]
        if not len(crossed):
            continue
        entry_index = int(crossed[0])
        entry = float(day_asks[entry_index] if direction == "long" else day_bids[entry_index])
        exit_index, exit_price, outcome = _exit(
            day_times,
            day_bids,
            day_asks,
            entry_index,
            direction,
            entry,
            stop_pips,
            target_pips,
        )
        pnl = (exit_price - entry) / PIP if direction == "long" else (entry - exit_price) / PIP
        trades.append(Trade(direction, int(day_times[entry_index]), int(day_times[exit_index]), entry, exit_price, outcome, pnl))
    return {
        "direction": direction,
        "stop_pips": stop_pips,
        "target_pips": target_pips,
        "train": _stats([trade for trade in trades if trade.entry_ns < TRAIN_END_NS]),
        "holdout": _stats([trade for trade in trades if trade.entry_ns >= TRAIN_END_NS]),
    }


def _exit(
    times: np.ndarray,
    bids: np.ndarray,
    asks: np.ndarray,
    entry_index: int,
    direction: str,
    entry: float,
    stop_pips: int,
    target_pips: int,
) -> tuple[int, float, str]:
    cutoff = datetime.fromtimestamp(times[entry_index] / 1_000_000_000, UTC).replace(hour=21, minute=0, second=0)
    cutoff_ns = int(cutoff.timestamp() * 1_000_000_000)
    indices = np.arange(entry_index + 1, len(times))
    indices = indices[times[indices] < cutoff_ns]
    stop = entry - stop_pips * PIP if direction == "long" else entry + stop_pips * PIP
    target = entry + target_pips * PIP if direction == "long" else entry - target_pips * PIP
    for index in indices:
        if direction == "long":
            if bids[index] <= stop:
                return int(index), float(bids[index]), "stop"
            if bids[index] >= target:
                return int(index), float(bids[index]), "target"
        else:
            if asks[index] >= stop:
                return int(index), float(asks[index]), "stop"
            if asks[index] <= target:
                return int(index), float(asks[index]), "target"
    exit_index = int(indices[-1]) if len(indices) else entry_index
    exit_price = float(bids[exit_index] if direction == "long" else asks[exit_index])
    return exit_index, exit_price, "time"


def _stats(trades: list[Trade]) -> dict[str, object]:
    pnl = [trade.pnl_pips for trade in trades]
    return {
        "trades": len(trades),
        "targets": sum(trade.outcome == "target" for trade in trades),
        "stops": sum(trade.outcome == "stop" for trade in trades),
        "time_exits": sum(trade.outcome == "time" for trade in trades),
        "win_rate_percent": round(100 * sum(value > 0 for value in pnl) / len(pnl), 2) if pnl else None,
        "net_pips": round(sum(pnl), 2),
        "mean_pips": round(float(np.mean(pnl)), 3) if pnl else None,
        "trades_ledger": [asdict(trade) for trade in trades],
    }


if __name__ == "__main__":
    raise SystemExit(main())
