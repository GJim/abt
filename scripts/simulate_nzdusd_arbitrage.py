"""Simulate one risk-gated NZDUSD cross-broker pair from exported MT5 CSV files."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path

from abt.trader.arbitrage_simulation import (
    AccountSpec,
    Quote,
    RiskPolicy,
    SimulationConfig,
    simulate_single_pair,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audacity-csv", type=Path, required=True)
    parser.add_argument("--ftmo-csv", type=Path, required=True)
    parser.add_argument("--entry-edge-pips", type=float, default=0.2)
    parser.add_argument("--volume", type=float, default=0.1)
    arguments = parser.parse_args(argv)

    result = simulate_single_pair(
        _quotes(arguments.audacity_csv),
        _quotes(arguments.ftmo_csv),
        audacity=AccountSpec("audacity", 5_000, margin_per_lot=595.78, minimum_volume=0.01, volume_step=0.01),
        ftmo=AccountSpec("ftmo", 5_000, margin_per_lot=1_985.90, minimum_volume=0.01, volume_step=0.01),
        policy=RiskPolicy(),
        config=SimulationConfig(entry_edge=arguments.entry_edge_pips * 0.0001, requested_volume=arguments.volume),
    )
    print(f"completed_trades={len(result.trades)}")
    print(f"audacity_balance_usd={result.audacity.balance:.2f}")
    print(f"audacity_equity_usd={result.audacity.equity:.2f}")
    print(f"ftmo_balance_usd={result.ftmo.balance:.2f}")
    print(f"ftmo_equity_usd={result.ftmo.equity:.2f}")
    print(f"stopped_reason={result.stopped_reason or 'none'}")
    return 0


def _quotes(path: Path) -> list[Quote]:
    with path.open(encoding="utf-8", newline="") as source:
        return [
            Quote(
                observed_at=datetime.fromisoformat(row["time_msc_utc"].replace("Z", "+00:00")),
                bid=float(row["bid"]),
                ask=float(row["ask"]),
            )
            for row in csv.DictReader(source)
        ]


if __name__ == "__main__":
    raise SystemExit(main())
