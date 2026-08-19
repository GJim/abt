from __future__ import annotations

import json
import unittest
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from abt.mt5.cli import _check_then_send, _context_command, build_parser
from abt.mt5.config import CalibrationSample, Config, Context, TimeCalibration, TimeCalibrationFamily
from abt.mt5.output import render
from abt.mt5.timecalibration import (
    MARKET_DATA,
    TRADE_RECORDS,
    prepare_market_data,
    record_successful_write,
    render_calibration,
    time_status,
)


class TimeCalibrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.timezone = ZoneInfo("Asia/Taipei")
        self.now = datetime(2026, 8, 14, 12, tzinfo=UTC)
        self.context = Context("demo", Path(r"C:\MT5\terminal64.exe"), 123456, "Broker-Demo", self.timezone.key)
        self.config = Config(Path("unused.toml"), "demo", {"demo": self.context})

    def test_missing_or_new_day_market_calibration_collects_full_samples(self) -> None:
        api = CalibrationApi(int(self.now.timestamp()) + 10_800)
        with (
            patch("abt.mt5.timecalibration._utc_now", return_value=self.now),
            patch("abt.mt5.timecalibration._save_context") as save_context,
        ):
            updated, family = prepare_market_data(api, self.config, self.context, "EURUSD", self.timezone)

        self.assertEqual(api.tick_calls, 3)
        self.assertEqual(family.offset_seconds, 10_800)
        self.assertEqual(family.status, "calibrated")
        self.assertEqual(family.calibrated_local_date, "2026-08-14")
        self.assertEqual(family.calibration_symbol, "EURUSD")
        self.assertEqual(len(family.samples), 3)
        self.assertEqual(updated.time_calibration.market_data, family)
        save_context.assert_called_once()

    def test_same_day_probe_only_recalibrates_when_drift_changes(self) -> None:
        family = TimeCalibrationFamily(
            offset_seconds=10_800,
            calibrated_local_date="2026-08-14",
            calibrated_at_utc="2026-08-14T12:00:00Z",
            status="calibrated",
            calibration_symbol="EURUSD",
        )
        context = Context(
            self.context.name,
            self.context.terminal_path,
            self.context.login,
            self.context.server,
            self.context.user_timezone,
            TimeCalibration(market_data=family),
        )
        config = Config(Path("unused.toml"), "demo", {"demo": context})
        unchanged_api = CalibrationApi(int(self.now.timestamp()) + 10_800)
        changed_api = CalibrationApi(int(self.now.timestamp()) + 14_400)
        with (
            patch("abt.mt5.timecalibration._utc_now", return_value=self.now),
            patch("abt.mt5.timecalibration._save_context") as save_context,
        ):
            _, unchanged = prepare_market_data(
                unchanged_api, config, context, "GBPUSD", self.timezone
            )
            _, changed = prepare_market_data(
                changed_api, config, context, "GBPUSD", self.timezone
            )

        self.assertEqual(unchanged.offset_seconds, 10_800)
        self.assertEqual(len(unchanged.samples), 1)
        self.assertEqual(unchanged_api.tick_calls, 1)
        self.assertEqual(unchanged_api.symbols, ["EURUSD"])
        self.assertEqual(changed.offset_seconds, 14_400)
        self.assertEqual(len(changed.samples), 3)
        self.assertEqual(changed_api.tick_calls, 4)
        self.assertEqual(changed_api.symbols, ["EURUSD"] * 4)
        self.assertEqual(save_context.call_count, 2)

    def test_market_uses_latest_then_utc_fallback_when_calibration_cannot_run(self) -> None:
        stale = TimeCalibrationFamily(
            offset_seconds=10_800,
            calibrated_local_date="2026-08-13",
            calibrated_at_utc="2026-08-13T12:00:00Z",
            status="calibrated",
        )
        context = Context(
            self.context.name,
            self.context.terminal_path,
            self.context.login,
            self.context.server,
            self.context.user_timezone,
            TimeCalibration(market_data=stale),
        )
        with (
            patch("abt.mt5.timecalibration._utc_now", return_value=self.now),
            patch("abt.mt5.timecalibration._save_context") as save_context,
        ):
            _, retained = prepare_market_data(
                CalibrationApi(None), self.config, context, "EURUSD", self.timezone
            )

        self.assertEqual(render_calibration(retained, MARKET_DATA, self.timezone)["offset_layer"], "latest_market_data_calibration")
        self.assertEqual(
            render_calibration(TimeCalibrationFamily(), MARKET_DATA, self.timezone)["offset_layer"], "utc_fallback"
        )
        save_context.assert_not_called()

    def test_successful_write_calibrates_from_matching_order_record(self) -> None:
        record = SimpleNamespace(ticket=55, time_done=int(self.now.timestamp()) + 7_200)
        api = TradeRecordApi(record)
        with (
            patch("abt.mt5.timecalibration._save_context") as save_context,
            patch("abt.mt5.timecalibration._utc_now", return_value=self.now),
        ):
            updated, family = record_successful_write(
                api,
                self.config,
                self.context,
                {"action": 1},
                SimpleNamespace(order=55, deal=0),
                self.now,
                self.now,
                self.timezone,
            )

        self.assertEqual(family.offset_seconds, 7_200)
        self.assertEqual(family.calibrated_local_date, "2026-08-14")
        self.assertEqual(family.samples[-1].ticket, 55)
        self.assertEqual(family.samples[-1].source, "order")
        self.assertEqual(family.samples[-1].error_seconds, 0)
        self.assertEqual(updated.time_calibration.trade_records, family)
        save_context.assert_called_once()

    def test_successful_write_finds_deal_and_position_records_by_ticket(self) -> None:
        cases = (
            (
                "deal",
                DealRecordApi(SimpleNamespace(ticket=56, time=int(self.now.timestamp()) + 3_600)),
                SimpleNamespace(order=0, deal=56),
                3_600,
            ),
            (
                "position",
                PositionRecordApi(SimpleNamespace(ticket=57, time=1, time_update=int(self.now.timestamp()) + 1_800)),
                SimpleNamespace(order=0, deal=0, position=57),
                1_800,
            ),
        )
        with patch("abt.mt5.timecalibration._save_context"):
            for source, api, sent, expected_offset in cases:
                with self.subTest(source=source):
                    _, family = record_successful_write(
                        api,
                        self.config,
                        self.context,
                        {"action": 1},
                        sent,
                        self.now,
                        self.now,
                        self.timezone,
                    )
                    self.assertEqual(family.samples[-1].source, source)
                    self.assertEqual(family.offset_seconds, expected_offset)

    def test_failed_record_lookup_leaves_existing_trade_offset_unchanged(self) -> None:
        existing = TimeCalibrationFamily(
            offset_seconds=7_200,
            calibrated_local_date="2026-08-14",
            calibrated_at_utc="2026-08-14T12:00:00Z",
            status="calibrated",
        )
        context = Context(
            self.context.name,
            self.context.terminal_path,
            self.context.login,
            self.context.server,
            self.context.user_timezone,
            TimeCalibration(trade_records=existing),
        )
        with patch("abt.mt5.timecalibration._save_context") as save_context:
            updated, actual = record_successful_write(
                TradeRecordApi(None),
                self.config,
                context,
                {"action": 1},
                SimpleNamespace(order=99),
                self.now,
                self.now,
                self.timezone,
            )

        self.assertEqual(updated, context)
        self.assertEqual(actual, existing)
        save_context.assert_not_called()

    def test_stale_trade_record_does_not_replace_current_calibration(self) -> None:
        existing = TimeCalibrationFamily(
            offset_seconds=10_800,
            calibrated_local_date="2026-08-14",
            calibrated_at_utc="2026-08-14T12:00:00Z",
            status="calibrated",
        )
        context = Context(
            self.context.name,
            self.context.terminal_path,
            self.context.login,
            self.context.server,
            self.context.user_timezone,
            TimeCalibration(trade_records=existing),
        )
        record = SimpleNamespace(ticket=55, time_done=int(self.now.timestamp()) + 10_792)
        api = TradeRecordApi(record)

        with patch("abt.mt5.timecalibration._save_context") as save_context:
            updated, actual = record_successful_write(
                api,
                self.config,
                context,
                {"action": 7, "order": 55},
                SimpleNamespace(order=55, deal=0),
                self.now,
                self.now,
                self.timezone,
            )

        self.assertEqual(updated, context)
        self.assertEqual(actual, existing)
        save_context.assert_not_called()

    def test_trade_status_marks_offset_expired_on_the_next_local_day(self) -> None:
        family = TimeCalibrationFamily(
            offset_seconds=7_200,
            calibrated_local_date="2026-08-13",
            calibrated_at_utc="2026-08-13T12:00:00Z",
            status="calibrated",
            samples=(
                CalibrationSample("deal", "2026-08-13T12:00:00Z", 7_200, 0, ticket=1),
            ),
        )
        context = Context(
            self.context.name,
            self.context.terminal_path,
            self.context.login,
            self.context.server,
            self.context.user_timezone,
            TimeCalibration(trade_records=family),
        )
        with patch("abt.mt5.timecalibration._utc_now", return_value=self.now):
            calibration = render_calibration(family, TRADE_RECORDS, self.timezone)
            status = time_status(context, self.timezone)

        self.assertEqual(calibration["status"], "expired")
        self.assertEqual(calibration["offset_layer"], "expired_trade_record_calibration")
        self.assertEqual(status["families"][1]["status"], "expired")
        self.assertEqual(status["fallback_priority"]["trade_records"][-1], "UTC")


class TimeOutputTests(unittest.TestCase):
    def test_market_and_expired_trade_metadata_preserve_raw_epochs(self) -> None:
        timezone = ZoneInfo("Asia/Taipei")
        market = {
            "family": MARKET_DATA,
            "status": "calibrated",
            "offset_seconds": 10_800,
            "offset_layer": "market_data_calibration",
        }
        rendered_market = render(
            {"time": 1786716000},
            "json",
            user_timezone=timezone,
            source_family=MARKET_DATA,
            calibration=market,
        )
        parsed_market = json.loads(rendered_market)
        self.assertEqual(parsed_market["time"], 1786716000)
        self.assertEqual(parsed_market["time_utc"], "2026-08-14T11:00:00Z")
        self.assertEqual(parsed_market["time_metadata"]["source_family"], MARKET_DATA)
        self.assertEqual(parsed_market["time_metadata"]["offset_layer"], "market_data_calibration")

        expired_trade = {
            "family": TRADE_RECORDS,
            "status": "expired",
            "offset_seconds": 7_200,
            "offset_layer": "expired_trade_record_calibration",
        }
        table = render(
            {"time": 1786716000},
            "table",
            user_timezone=timezone,
            source_family=TRADE_RECORDS,
            calibration=expired_trade,
        )
        self.assertIn("expired_trade_record_calibration", table)
        self.assertIn("2026-08-14 20:00:00", table)

    def test_expiration_fields_use_market_data_override_for_trade_write_output(self) -> None:
        rendered = render(
            {"time": 1786716000, "expiration": 1786716000},
            "json",
            user_timezone=ZoneInfo("Asia/Taipei"),
            source_family=TRADE_RECORDS,
            calibration={
                "family": TRADE_RECORDS,
                "status": "calibrated",
                "offset_seconds": 7_200,
                "offset_layer": "trade_record_calibration",
            },
            field_calibrations={
                "expiration": {
                    "family": MARKET_DATA,
                    "status": "calibrated",
                    "offset_seconds": 10_800,
                    "offset_layer": "market_data_calibration",
                },
            },
        )
        parsed = json.loads(rendered)
        self.assertEqual(parsed["time_utc"], "2026-08-14T12:00:00Z")
        self.assertEqual(parsed["expiration_utc"], "2026-08-14T11:00:00Z")
        self.assertEqual(
            parsed["time_metadata"]["field_source_overrides"]["expiration"]["source_family"], MARKET_DATA
        )


class CliTimeCalibrationTests(unittest.TestCase):
    def test_time_status_never_opens_a_broker_session(self) -> None:
        context = Context("demo", Path(r"C:\MT5\terminal64.exe"), 123456, "Broker-Demo", "Asia/Taipei")
        config = Config(Path("unused.toml"), "demo", {"demo": context})
        args = build_parser().parse_args(["context", "time-status"])
        with patch("abt.mt5.cli.connected") as connected:
            result = _context_command(args, config)

        self.assertEqual(result["context"], "demo")
        self.assertEqual(args.time_source_family, "host_utc")
        connected.assert_not_called()

    def test_send_callback_receives_host_timestamps_only_after_success(self) -> None:
        api = SendApi()
        received: list[tuple[object, ...]] = []
        result = _check_then_send(
            {"action": 1},
            api,
            successful_send_callback=lambda request, sent, before, after: received.append(
                (request, sent, before, after)
            ),
        )

        self.assertTrue(result["sent"])
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0][0], {"action": 1})
        self.assertLessEqual(received[0][2], received[0][3])
        self.assertEqual(api.events, ["check", "send"])


class CalibrationApi:
    def __init__(self, epoch: int | None) -> None:
        self.epoch = epoch
        self.tick_calls = 0
        self.symbols: list[str] = []

    def symbol_info_tick(self, symbol: str) -> SimpleNamespace | None:
        self.tick_calls += 1
        self.symbols.append(symbol)
        return None if self.epoch is None else SimpleNamespace(time=self.epoch)


class TradeRecordApi:
    def __init__(self, record: SimpleNamespace | None) -> None:
        self.record = record

    def orders_get(self, *, ticket: int) -> tuple[SimpleNamespace, ...]:
        return (self.record,) if self.record is not None and self.record.ticket == ticket else ()


class DealRecordApi:
    def __init__(self, record: SimpleNamespace) -> None:
        self.record = record

    def history_deals_get(self, *, ticket: int) -> tuple[SimpleNamespace, ...]:
        return (self.record,) if self.record.ticket == ticket else ()


class PositionRecordApi:
    def __init__(self, record: SimpleNamespace) -> None:
        self.record = record

    def positions_get(self, *, ticket: int) -> tuple[SimpleNamespace, ...]:
        return (self.record,) if self.record.ticket == ticket else ()


class SendApi:
    TRADE_RETCODE_DONE = 10009

    def __init__(self) -> None:
        self.events: list[str] = []

    def order_check(self, request: dict[str, object]) -> SimpleNamespace:
        self.events.append("check")
        return SimpleNamespace(retcode=0)

    def order_send(self, request: dict[str, object]) -> SimpleNamespace:
        self.events.append("send")
        return SimpleNamespace(retcode=self.TRADE_RETCODE_DONE)


if __name__ == "__main__":
    unittest.main()
