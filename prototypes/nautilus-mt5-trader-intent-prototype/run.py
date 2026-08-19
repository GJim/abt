"""PROTOTYPE — do not promote this code to production.

Question: Can MT5 rate exports drive a NautilusTrader backtest and produce a
shared-trade-intent request at the console's future Trader boundary?

Run:
    uv run --with nautilus_trader --with pandas python prototypes\nautilus-mt5-trader-intent-prototype\run.py

Use actual data exported by:
    uv run mt5 --output json rates-range EURUSD M1 --from ... --to ... > rates.json
then append:
    --mt5-rates rates.json

The current console intentionally has no Trader registration or intent API. By
default this prototype prints the exact request that a future authenticated
Trader client would submit; --console-url attempts that request so the missing
boundary is visible as an HTTP response rather than hidden.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import httpx
import pandas as pd
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.model import TraderId
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.trading.strategy import Strategy


@dataclass(frozen=True)
class StrategyOutcome:
    bars_processed: int
    direction: str
    reference_price: float


class MovingAverageIntentStrategy(Strategy):
    """Tiny strategy whose only product is a post-backtest intent candidate."""

    def __init__(self, primary_bar_type: BarType) -> None:
        super().__init__()
        self.primary_bar_type = primary_bar_type
        self.closes: list[float] = []

    def on_start(self) -> None:
        self.subscribe_bars(self.primary_bar_type)

    def on_bar(self, bar: Bar) -> None:
        self.closes.append(bar.close.as_double())

    def outcome(self) -> StrategyOutcome:
        slow = sum(self.closes[-5:]) / min(len(self.closes), 5)
        direction = "buy" if self.closes[-1] >= slow else "sell"
        return StrategyOutcome(len(self.closes), direction, self.closes[-1])


def main() -> int:
    parser = argparse.ArgumentParser(description="PROTOTYPE: MT5 rates -> NautilusTrader -> Trader intent")
    parser.add_argument("--mt5-rates", type=Path, help="JSON from `mt5 --output json rates-range ...`.")
    parser.add_argument("--console-url", help="Optional console base URL; sends the candidate request.")
    args = parser.parse_args()

    rates = _load_rates(args.mt5_rates)
    outcome = _run_backtest(rates)
    intent = _intent_from_outcome(outcome)

    print("BACKTEST OUTCOME")
    print(json.dumps(asdict(outcome), indent=2))
    print("\nFUTURE TRADER INTENT REQUEST")
    print(json.dumps(intent, indent=2))

    if args.console_url:
        response = httpx.post(
            f"{args.console_url.rstrip('/')}/api/trader/intents",
            headers={
                "X-Trader-Role": "trader",
                "X-Trader-Command-Id": intent["trader_command_id"],
            },
            json=intent,
            timeout=5.0,
        )
        print(f"\nCONSOLE RESPONSE: {response.status_code} {response.text}")
    else:
        print(
            "\nConsole delivery skipped. The current read-only console has no Trader "
            "identity or intent endpoint; run with --console-url to expose that gap."
        )
    return 0


def _load_rates(path: Path | None) -> list[dict[str, object]]:
    if path is None:
        start = datetime(2026, 1, 2, 8, tzinfo=UTC)
        return [
            {
                "time": int((start + timedelta(minutes=index)).timestamp()),
                "open": 1.1000 + index * 0.0001,
                "high": 1.1003 + index * 0.0001,
                "low": 1.0998 + index * 0.0001,
                "close": 1.1001 + index * 0.0001,
                "tick_volume": 100 + index,
            }
            for index in range(10)
        ]
    raw = json.loads(path.read_text(encoding="utf-8"))
    return raw["records"] if isinstance(raw, dict) and "records" in raw else raw


def _run_backtest(rates: list[dict[str, object]]) -> StrategyOutcome:
    dataframe = pd.DataFrame(rates).rename(columns={"time": "timestamp", "tick_volume": "volume"})
    dataframe["timestamp"] = pd.to_datetime(dataframe["timestamp"], unit="s", utc=True)
    dataframe = dataframe.set_index("timestamp")[["open", "high", "low", "close", "volume"]]

    engine = BacktestEngine(config=BacktestEngineConfig(trader_id=TraderId("MT5_PROTOTYPE-001")))
    venue = Venue("XCME")
    engine.add_venue(
        venue=venue,
        oms_type=OmsType.NETTING,
        account_type=AccountType.MARGIN,
        starting_balances=[Money(1_000_000, USD)],
        base_currency=USD,
        default_leverage=Decimal(1),
    )
    instrument = TestInstrumentProvider.eurusd_future(expiry_year=2026, expiry_month=3, venue_name="XCME")
    engine.add_instrument(instrument)
    bar_type = BarType.from_str(f"{instrument.id}-1-MINUTE-LAST-EXTERNAL")
    bars = BarDataWrangler(bar_type, instrument).process(dataframe)
    strategy = MovingAverageIntentStrategy(bar_type)
    engine.add_data(bars)
    engine.add_strategy(strategy)
    engine.run()
    outcome = strategy.outcome()
    engine.dispose()
    return outcome


def _intent_from_outcome(outcome: StrategyOutcome) -> dict[str, object]:
    return {
        "trader_command_id": str(uuid4()),
        "strategy_name": "nautilus-mt5-moving-average-prototype",
        "approved_symbol_pair_id": "EURUSD-MT5-PAIR-TO-BE-APPROVED",
        "primary_direction": outcome.direction,
        "equivalent_volume": 0.01,
        "reference_price": outcome.reference_price,
        "entry_expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        "stop_loss_pips": 20,
        "take_profit_pips": 40,
        "evidence": {"bars_processed": outcome.bars_processed},
    }


if __name__ == "__main__":
    raise SystemExit(main())
