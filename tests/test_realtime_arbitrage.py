from __future__ import annotations

import unittest

from strategy.realtime_arbitrage import Endpoint, RealtimeArbitrage, StrategyConfig, StrategyError


class FakeGateway:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, object]]] = []

    def request(self, worker_id: str, *, kind: str, request: dict[str, object]) -> dict[str, object]:
        self.calls.append((worker_id, kind, request))
        if request["type"] == "account_info":
            return {"account": {"balance": 5_000.0, "equity": 5_000.0}}
        if request["type"] == "symbol_info":
            return {"symbol": {"name": request["symbol"], "trade_mode": 4}}
        if request["type"] == "current_positions":
            return {"positions": [{"ticket": 11 if worker_id == "audacity" else 22, "volume": 0.1}]}
        return {"operation": request["type"], "result": {}}

    def query(self, query: str) -> dict[str, object]:
        self.calls.append(("", "query", {"query": query}))
        return {
            "workers": [
                {"worker_id": "audacity", "connectivity": "connected", "safety_state": "connected"},
                {"worker_id": "ftmo", "connectivity": "connected", "safety_state": "connected"},
                {"worker_id": "worker-a", "connectivity": "connected", "safety_state": "connected"},
                {"worker_id": "worker-b", "connectivity": "connected", "safety_state": "connected"},
            ]
        }


def configuration(*, execute: bool = True) -> StrategyConfig:
    return StrategyConfig(0.4, 0.1, 100, 0.03, 0.02, 1, execute)


class RealtimeArbitrageTests(unittest.TestCase):
    def test_shutdown_closes_every_position_on_each_worker_when_execution_is_enabled(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway, first=Endpoint("audacity"), second=Endpoint("ftmo"), symbol="NZDUSD", config=configuration()
        )

        strategy.shutdown()

        closes = [(worker, request) for worker, kind, request in gateway.calls if kind == "operation" and request["type"] == "close"]
        self.assertEqual(
            [
                ("audacity", {"type": "close", "ticket": "11", "volume": "0.10"}),
                ("ftmo", {"type": "close", "ticket": "22", "volume": "0.10"}),
            ],
            closes,
        )

    def test_halts_before_any_order_when_calculated_margin_exceeds_half_equity(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), symbol="EURUSD", config=configuration(execute=False)
        )
        strategy.margin_per_lot = {("first", "SHORT"): 30_000, ("second", "LONG"): 1_000}

        with self.assertRaisesRegex(StrategyError, "exceeds 50%"):
            strategy._open("short_first_long_second")

        self.assertFalse(any(kind == "operation" for _, kind, _ in gateway.calls))


if __name__ == "__main__":
    unittest.main()
