from __future__ import annotations

from datetime import UTC, datetime, timedelta
import unittest

from abt.trader.arbitrage_simulation import (
    AccountSpec,
    Quote,
    RiskPolicy,
    SimulationConfig,
    simulate_single_pair,
)


def quote(second: int, bid: float, ask: float) -> Quote:
    return Quote(datetime(2026, 8, 26, tzinfo=UTC) + timedelta(seconds=second), bid, ask)


class ArbitrageSimulationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.audacity = AccountSpec("audacity", 5_000, 600, 0.01, 0.01)
        self.ftmo = AccountSpec("ftmo", 5_000, 2_000, 0.01, 0.01)
        self.policy = RiskPolicy()
        self.config = SimulationConfig(entry_edge=0.00002, requested_volume=0.1)

    def test_closes_one_pair_on_reverse_and_waits_for_clear_before_reentry(self) -> None:
        result = simulate_single_pair(
            [
                quote(0, 1.0002, 1.0003),
                quote(1, 1.0002, 1.0003),
                quote(2, 1.0000, 1.0001),
                quote(3, 1.0002, 1.0003),
            ],
            [
                quote(0, 0.9998, 1.0000),
                quote(1, 1.0005, 1.0007),
                quote(2, 1.0000, 1.0002),
                quote(3, 0.9998, 1.0000),
            ],
            audacity=self.audacity,
            ftmo=self.ftmo,
            policy=self.policy,
            config=self.config,
        )

        self.assertEqual(1, len(result.trades))
        self.assertEqual("reverse_edge", result.trades[0].close_reason)
        self.assertEqual("short_audacity_long_ftmo", result.trades[0].direction)
        self.assertEqual(0.1, result.trades[0].volume)
        self.assertIsNone(result.stopped_reason)

    def test_stops_all_new_entries_when_one_leg_hits_trade_loss_limit(self) -> None:
        result = simulate_single_pair(
            [quote(0, 1.0002, 1.0003), quote(1, 1.0001, 1.0104)],
            [quote(0, 0.9998, 1.0000), quote(1, 0.9998, 1.0000)],
            audacity=self.audacity,
            ftmo=self.ftmo,
            policy=self.policy,
            config=self.config,
        )

        self.assertEqual(1, len(result.trades))
        self.assertEqual("trade_loss_stop", result.trades[0].close_reason)
        self.assertEqual("trade_loss_stop", result.stopped_reason)
        self.assertTrue(result.audacity.stopped)
        self.assertTrue(result.ftmo.stopped)


if __name__ == "__main__":
    unittest.main()
