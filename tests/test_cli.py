from __future__ import annotations

import json
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from datetime import UTC, date, datetime, timedelta
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from abt.cli import (
    ExpirationPlan,
    _build_write_request,
    _check_then_send,
    _context_command,
    dispatch,
    _duration,
    _history_window,
    _rates,
    _required_user_timezone,
    _timestamp,
    _ticks,
    _utc_timestamp,
    _write_command,
    build_parser,
)
from abt.config import CalibrationSample, Config, ConfigError, Context, TimeCalibration, TimeCalibrationFamily, load, save
from abt.output import render


class ConfigTests(unittest.TestCase):
    def test_round_trip_preserves_context_and_windows_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "abt.toml"
            expected = Context(
                "demo",
                Path(r"C:\MT5\terminal64.exe"),
                123456,
                "Broker-Demo",
                "Asia/Taipei",
                TimeCalibration(
                    market_data=TimeCalibrationFamily(
                        offset_seconds=10_800,
                        calibrated_local_date="2026-08-14",
                        calibrated_at_utc="2026-08-14T12:00:00Z",
                        status="calibrated",
                        calibration_symbol="EURUSD",
                        samples=(
                            CalibrationSample(
                                "symbol_info_tick.time",
                                "2026-08-14T12:00:00Z",
                                10_800,
                                0.1,
                                symbol="EURUSD",
                            ),
                        ),
                    ),
                ),
            )
            save(Config(path, "demo", {"demo": expected}))
            actual = load(path)
        self.assertEqual(actual.current_context, "demo")
        self.assertEqual(actual.version, 2)
        self.assertEqual(actual.contexts["demo"], expected)

    def test_invalid_timezone_is_rejected_and_legacy_context_stays_editable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "abt.toml"
            path.write_text(
                """
current_context = "legacy"

[contexts.legacy]
terminal_path = "C:\\\\MT5\\\\terminal64.exe"
login = 123456
server = "Broker-Demo"
""",
                encoding="utf-8",
            )
            legacy = load(path)
            self.assertEqual(legacy.version, 1)
            self.assertIsNone(legacy.contexts["legacy"].user_timezone)
            self.assertIsNone(legacy.contexts["legacy"].time_calibration.market_data.offset_seconds)
            with self.assertRaises(ValueError):
                _required_user_timezone(legacy.contexts["legacy"])
            listed = _context_command(build_parser().parse_args(["context", "list"]), legacy)
            self.assertEqual(listed[0]["user_timezone"], None)
            with patch("abt.cli.connected") as connected_mock:
                with self.assertRaises(ValueError):
                    dispatch(build_parser().parse_args(["account"]), legacy)
                connected_mock.assert_not_called()

            path.write_text(
                """
[contexts.invalid]
terminal_path = "C:\\\\MT5\\\\terminal64.exe"
login = 123456
server = "Broker-Demo"
user_timezone = "Not/A_Timezone"
""",
                encoding="utf-8",
            )
            with self.assertRaises(ConfigError):
                load(path)

    def test_context_set_timezone_updates_legacy_context_without_a_broker_session(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "abt.toml"
            legacy = Context("legacy", Path(r"C:\MT5\terminal64.exe"), 123456, "Broker-Demo")
            loaded = Config(path, "legacy", {"legacy": legacy})
            args = build_parser().parse_args(["--config", str(path), "context", "set-timezone", "legacy", "America/New_York"])
            result = _context_command(args, loaded)
            self.assertEqual(result["user_timezone"], "America/New_York")
            self.assertEqual(load(path).contexts["legacy"].user_timezone, "America/New_York")


class HistoryWindowTests(unittest.TestCase):
    def test_defaults_to_last_seven_days(self) -> None:
        start, end = _history_window(Namespace(since=None, from_date=None, to_date=None))
        self.assertEqual(start.tzinfo, UTC)
        self.assertEqual(end.tzinfo, UTC)
        self.assertAlmostEqual((end - start).total_seconds(), timedelta(days=7).total_seconds(), delta=1)

    def test_date_boundaries_cover_full_local_days(self) -> None:
        start, end = _history_window(
            Namespace(since=None, from_date=date(2026, 8, 1), to_date=date(2026, 8, 2)),
            ZoneInfo("America/New_York"),
        )
        self.assertEqual(start, datetime(2026, 8, 1, 4, tzinfo=UTC))
        self.assertEqual(end, datetime(2026, 8, 3, 3, 59, 59, 999999, tzinfo=UTC))

    def test_date_boundaries_follow_dst_transition(self) -> None:
        start, end = _history_window(
            Namespace(since=None, from_date=date(2026, 3, 8), to_date=date(2026, 3, 8)),
            ZoneInfo("America/New_York"),
        )
        self.assertEqual(start, datetime(2026, 3, 8, 5, tzinfo=UTC))
        self.assertEqual(end, datetime(2026, 3, 9, 3, 59, 59, 999999, tzinfo=UTC))

    def test_since_rejects_explicit_range(self) -> None:
        with self.assertRaises(ValueError):
            _history_window(Namespace(since=timedelta(hours=1), from_date=date(2026, 8, 1), to_date=None))


class ParsingAndOutputTests(unittest.TestCase):
    def test_since_accepts_only_hours_or_days(self) -> None:
        self.assertEqual(_duration("12h"), timedelta(hours=12))
        self.assertEqual(_duration("7d"), timedelta(days=7))
        with self.assertRaises(Exception):
            _duration("30m")

    def test_timestamps_use_context_timezone_when_unoffset(self) -> None:
        self.assertEqual(_timestamp("2026-08-14T12:00:00Z"), datetime(2026, 8, 14, 12, tzinfo=UTC))
        self.assertEqual(
            _timestamp("2026-08-14T12:00:00", ZoneInfo("Asia/Taipei")),
            datetime(2026, 8, 14, 4, tzinfo=UTC),
        )
        self.assertEqual(
            _utc_timestamp("2026-08-14T12:00:00+02:00", ZoneInfo("Asia/Taipei")),
            datetime(2026, 8, 14, 10, tzinfo=UTC),
        )
        with self.assertRaises(Exception):
            _timestamp("2026-08-14T12:00:00")

    def test_timestamps_reject_ambiguous_and_nonexistent_dst_local_times(self) -> None:
        timezone = ZoneInfo("America/New_York")
        with self.assertRaises(Exception):
            _timestamp("2026-11-01T01:30:00", timezone)
        with self.assertRaises(Exception):
            _timestamp("2026-03-08T02:30:00", timezone)

    def test_json_output_is_machine_readable(self) -> None:
        self.assertEqual(json.loads(render({"login": 42}, "json")), {"login": 42})

    def test_epoch_output_is_local_in_tables_and_preserved_with_utc_json_fields(self) -> None:
        value = {"time": 1786716000, "time_setup_msc": 1786716000123, "type_time": 2}
        timezone = ZoneInfo("Asia/Taipei")
        table = render(value, "table", user_timezone=timezone)
        self.assertIn("2026-08-14 22:00:00", table)
        rendered_json = json.loads(render(value, "json", user_timezone=timezone))
        self.assertEqual(rendered_json["time"], 1786716000)
        self.assertEqual(rendered_json["time_utc"], "2026-08-14T14:00:00Z")
        self.assertEqual(rendered_json["time_setup_msc_utc"], "2026-08-14T14:00:00.123000Z")
        self.assertNotIn("type_time_utc", rendered_json)
        self.assertEqual(rendered_json["time_metadata"]["user_timezone"], "Asia/Taipei")

    def test_broker_clock_epoch_is_adjusted_before_user_timezone_rendering(self) -> None:
        output = render(
            {"time": 1786711898},
            "table",
            user_timezone=ZoneInfo("Asia/Taipei"),
            broker_offset_seconds=10_800,
        )
        self.assertIn("2026-08-14 17:51:38", output)

    def test_table_renders_history_records_as_rows(self) -> None:
        output = render({"from": "2026-08-14T00:00:00Z", "records": [{"ticket": 42}]}, "table")
        self.assertIn("records", output)
        self.assertIn("ticket", output)
        self.assertNotIn('[{"ticket"', output)

    def test_pending_order_parser_requires_timeframe_and_count(self) -> None:
        parsed = build_parser().parse_args(
            ["rates-from", "EURUSD", "M1", "--from", "2026-08-14T00:00:00Z", "--count", "10"]
        )
        self.assertEqual(parsed.symbol, "EURUSD")
        self.assertEqual(parsed.count, 10)

    def test_market_data_queries_send_user_time_in_the_calibrated_broker_clock(self) -> None:
        captured: list[tuple[object, ...]] = []
        api = SimpleNamespace(
            COPY_TICKS_ALL=0,
            COPY_TICKS_INFO=1,
            COPY_TICKS_TRADE=2,
            copy_rates_from=lambda *values: captured.append(values) or object(),
            copy_rates_range=lambda *values: captured.append(values) or object(),
            copy_ticks_from=lambda *values: captured.append(values) or object(),
            copy_ticks_range=lambda *values: captured.append(values) or object(),
        )
        timezone = ZoneInfo("Asia/Taipei")
        calibration = {"offset_seconds": 10_800}
        expected = datetime(2026, 8, 14, 12, tzinfo=UTC)
        cases = (
            (
                _rates,
                Namespace(
                    command="rates-from",
                    symbol="EURUSD",
                    timeframe=1,
                    from_time="2026-08-14T17:00:00",
                    count=3,
                    user_timezone=timezone,
                    time_calibration=calibration,
                ),
                2,
            ),
            (
                _rates,
                Namespace(
                    command="rates-range",
                    symbol="EURUSD",
                    timeframe=1,
                    from_time="2026-08-14T17:00:00",
                    to_time="2026-08-14T18:00:00",
                    user_timezone=timezone,
                    time_calibration=calibration,
                ),
                2,
            ),
            (
                _ticks,
                Namespace(
                    command="ticks-from",
                    symbol="EURUSD",
                    from_time="2026-08-14T17:00:00",
                    count=3,
                    flags="all",
                    user_timezone=timezone,
                    time_calibration=calibration,
                ),
                1,
            ),
            (
                _ticks,
                Namespace(
                    command="ticks-range",
                    symbol="EURUSD",
                    from_time="2026-08-14T17:00:00",
                    to_time="2026-08-14T18:00:00",
                    flags="all",
                    user_timezone=timezone,
                    time_calibration=calibration,
                ),
                1,
            ),
        )

        with patch("abt.cli._structured_records", return_value=[]):
            for function, args, start_index in cases:
                with self.subTest(command=args.command):
                    function(api, args)
                    self.assertEqual(captured[-1][start_index], expected)

    def test_order_check_requires_json_object(self) -> None:
        parsed = build_parser().parse_args(
            ["order-check", "--request-json", '{"action": 1, "symbol": "EURUSD"}']
        )
        self.assertEqual(parsed.request_json["action"], 1)
        with self.assertRaises(SystemExit):
            build_parser().parse_args(["order-check", "--request-json", "[]"])


class WriteRequestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.api = FakeMt5()

    def test_market_request_uses_executable_price_and_explicit_metadata(self) -> None:
        args = build_parser().parse_args(
            [
                "buy",
                "EURUSD",
                "0.10",
                "--fill",
                "fok",
                "--deviation-points",
                "8",
                "--sl-points",
                "10",
                "--tp-percent",
                "1",
                "--magic",
                "42",
                "--comment",
                "manual-entry",
                "--yes",
            ]
        )
        request = _build_write_request(args, self.api)
        self.assertEqual(request["action"], self.api.TRADE_ACTION_DEAL)
        self.assertEqual(request["type"], self.api.ORDER_TYPE_BUY)
        self.assertEqual(request["price"], 1.2345)
        self.assertEqual(request["sl"], 1.2344)
        self.assertEqual(request["tp"], 1.24685)
        self.assertEqual(request["magic"], 42)
        self.assertEqual(request["comment"], "manual-entry")

    def test_market_request_does_not_invent_metadata(self) -> None:
        args = build_parser().parse_args(
            ["sell", "EURUSD", "0.10", "--fill", "ioc", "--deviation-points", "0", "--yes"]
        )
        request = _build_write_request(args, self.api)
        self.assertNotIn("magic", request)
        self.assertNotIn("comment", request)

    def test_pending_request_maps_explicit_offset_to_utc_intent_and_broker_epoch(self) -> None:
        args = build_parser().parse_args(
            [
                "buy-limit",
                "EURUSD",
                "0.10",
                "--price",
                "1.20000",
                "--fill",
                "return",
                "--time",
                "specified",
                "--expires-at",
                "2026-08-15T20:00:00+08:00",
                "--yes",
            ]
        )
        args.user_timezone = ZoneInfo("America/New_York")
        request = _build_write_request(args, self.api)
        self.assertEqual(request["type_time"], self.api.ORDER_TIME_SPECIFIED)
        mapping = args._expiration_mapping
        self.assertEqual(mapping.intent_utc, datetime(2026, 8, 15, 12, tzinfo=UTC))
        self.assertEqual(request["expiration"], mapping.broker_expiration)
        self.assertEqual(request["expiration"] % 60, 0)
        invalid = build_parser().parse_args(
            [
                "buy-limit",
                "EURUSD",
                "0.10",
                "--price",
                "1.2",
                "--fill",
                "fok",
                "--time",
                "gtc",
                "--expires-in",
                "1h",
            ]
        )
        with self.assertRaises(ValueError):
            _build_write_request(invalid, self.api)
        minute_expiry = build_parser().parse_args(
            [
                "buy-limit",
                "EURUSD",
                "0.10",
                "--price",
                "1.2",
                "--fill",
                "fok",
                "--time",
                "specified",
                "--expires-in",
                "5m",
            ]
        )
        self.assertEqual(minute_expiry.expires_in, timedelta(minutes=5))

    def test_relative_expiration_rounds_up_and_cancels_if_broker_shortens_it(self) -> None:
        plan = ExpirationPlan(intent_utc=None, duration=timedelta(minutes=5), existing_broker_expiration=None)
        minimum = self.api.symbol_info_tick("EURUSD").time + 300
        self.api.send_result = SimpleNamespace(retcode=self.api.TRADE_RETCODE_DONE, order=88)
        self.api.orders = [SimpleNamespace(ticket=88, time_expiration=minimum - 1)]

        result = _check_then_send(
            {"action": self.api.TRADE_ACTION_PENDING, "symbol": "EURUSD"},
            self.api,
            expiration_plan=plan,
            expiration_symbol="EURUSD",
        )

        self.assertFalse(result["sent"])
        self.assertEqual(result["request"]["expiration"], ((minimum + 59) // 60) * 60)
        self.assertEqual(result["expiration"]["broker_expiration"], result["request"]["expiration"])
        self.assertEqual(result["expiration"]["actual_broker_expiration"], minimum - 1)
        self.assertEqual(self.api.sent_requests[-1], {"action": self.api.TRADE_ACTION_REMOVE, "order": 88})
        self.assertIn("cancelled", result["error"])

    def test_stop_limit_and_comment_validation_reject_invalid_input(self) -> None:
        missing_stop_limit = build_parser().parse_args(
            [
                "buy-stop-limit",
                "EURUSD",
                "0.10",
                "--price",
                "1.24",
                "--fill",
                "fok",
                "--time",
                "gtc",
            ]
        )
        with self.assertRaises(ValueError):
            _build_write_request(missing_stop_limit, self.api)
        with self.assertRaises(SystemExit):
            build_parser().parse_args(
                [
                    "buy",
                    "EURUSD",
                    "0.1",
                    "--fill",
                    "fok",
                    "--deviation-points",
                    "1",
                    "--comment",
                    "x" * 32,
                ]
            )

    def test_pending_and_position_modification_preserve_omitted_protections(self) -> None:
        self.api.orders = [
            SimpleNamespace(
                ticket=51,
                type=self.api.ORDER_TYPE_BUY_LIMIT,
                symbol="EURUSD",
                price_open=1.2,
                sl=1.1,
                tp=1.3,
                type_time=self.api.ORDER_TIME_GTC,
                time_expiration=0,
                price_stoplimit=0,
            )
        ]
        pending_args = build_parser().parse_args(["pending-modify", "51", "--price", "1.21", "--yes"])
        pending_request = _build_write_request(pending_args, self.api)
        self.assertEqual(pending_request["sl"], 1.1)
        self.assertEqual(pending_request["tp"], 1.3)

        position_args = build_parser().parse_args(["position-modify", "--symbol", "EURUSD", "--clear-sl", "--yes"])
        position_request = _build_write_request(position_args, self.api)
        self.assertEqual(position_request["symbol"], "EURUSD")
        self.assertEqual(position_request["sl"], 0.0)
        self.assertEqual(position_request["tp"], 1.3)

    def test_position_close_uses_netting_symbol_and_full_volume_by_default(self) -> None:
        args = build_parser().parse_args(
            [
                "position-close",
                "--symbol",
                "EURUSD",
                "--fill",
                "ioc",
                "--deviation-points",
                "5",
                "--yes",
            ]
        )
        request = _build_write_request(args, self.api)
        self.assertEqual(request["symbol"], "EURUSD")
        self.assertEqual(request["volume"], 0.3)
        self.assertEqual(request["type"], self.api.ORDER_TYPE_SELL)
        self.assertNotIn("position", request)

    def test_hedging_position_operations_require_ticket(self) -> None:
        self.api.margin_mode = self.api.ACCOUNT_MARGIN_MODE_RETAIL_HEDGING
        missing_ticket = build_parser().parse_args(
            ["position-close", "--symbol", "EURUSD", "--fill", "ioc", "--deviation-points", "5", "--yes"]
        )
        with self.assertRaises(ValueError):
            _build_write_request(missing_ticket, self.api)
        args = build_parser().parse_args(
            ["position-close", "--ticket", "71", "--fill", "ioc", "--deviation-points", "5", "--yes"]
        )
        request = _build_write_request(args, self.api)
        self.assertEqual(request["position"], 71)

    def test_pips_are_rejected_for_non_forex_symbols(self) -> None:
        self.api.trade_calc_mode = 99
        args = build_parser().parse_args(
            [
                "buy",
                "EURUSD",
                "0.10",
                "--fill",
                "fok",
                "--deviation-points",
                "8",
                "--sl-pips",
                "10",
                "--yes",
            ]
        )
        with self.assertRaises(ValueError):
            _build_write_request(args, self.api)

    def test_successful_send_checks_before_sending(self) -> None:
        result = _check_then_send({"action": self.api.TRADE_ACTION_DEAL}, self.api)
        self.assertTrue(result["sent"])
        self.assertEqual(self.api.events, ["check", "send"])

    def test_interactive_write_requires_literal_confirmation(self) -> None:
        args = build_parser().parse_args(["buy", "EURUSD", "0.10", "--fill", "fok", "--deviation-points", "8"])
        stdout = StringIO()
        with patch("builtins.input", side_effect=["okay", "no"]), redirect_stdout(stdout):
            result = _write_command(args, self.api)
        self.assertTrue(result["cancelled"])
        self.assertEqual(self.api.events, [])
        self.assertIn("Please answer literal yes or no.", stdout.getvalue())

    def test_rejected_order_check_never_sends(self) -> None:
        self.api.check_result = SimpleNamespace(retcode=10016, comment="invalid stops")
        result = _check_then_send({"action": self.api.TRADE_ACTION_DEAL}, self.api)
        self.assertFalse(result["sent"])
        self.assertEqual(self.api.sent_requests, [])
        self.assertEqual(self.api.checked_requests, [{"action": self.api.TRADE_ACTION_DEAL}])

    def test_rejected_order_send_is_not_reported_as_sent(self) -> None:
        self.api.send_result = SimpleNamespace(retcode=10022, comment="Invalid expiration")
        result = _check_then_send({"action": self.api.TRADE_ACTION_PENDING}, self.api)
        self.assertFalse(result["sent"])
        self.assertEqual(result["send"].retcode, 10022)


class FakeMt5:
    TRADE_RETCODE_DONE = 10009
    TRADE_ACTION_DEAL = 1
    TRADE_ACTION_PENDING = 5
    TRADE_ACTION_SLTP = 6
    TRADE_ACTION_MODIFY = 7
    TRADE_ACTION_REMOVE = 8
    TRADE_ACTION_CLOSE_BY = 10
    ORDER_TYPE_BUY = 0
    ORDER_TYPE_SELL = 1
    ORDER_TYPE_BUY_LIMIT = 2
    ORDER_TYPE_SELL_LIMIT = 3
    ORDER_TYPE_BUY_STOP = 4
    ORDER_TYPE_SELL_STOP = 5
    ORDER_TYPE_BUY_STOP_LIMIT = 6
    ORDER_TYPE_SELL_STOP_LIMIT = 7
    ORDER_FILLING_FOK = 0
    ORDER_FILLING_IOC = 1
    ORDER_FILLING_RETURN = 2
    ORDER_TIME_GTC = 0
    ORDER_TIME_DAY = 1
    ORDER_TIME_SPECIFIED = 2
    ACCOUNT_MARGIN_MODE_RETAIL_HEDGING = 2
    POSITION_TYPE_BUY = 0
    POSITION_TYPE_SELL = 1
    SYMBOL_CALC_MODE_FOREX = 0
    SYMBOL_CALC_MODE_FOREX_NO_LEVERAGE = 5

    def __init__(self) -> None:
        self.orders: list[SimpleNamespace] = []
        self.checked_requests: list[dict[str, object]] = []
        self.sent_requests: list[dict[str, object]] = []
        self.events: list[str] = []
        self.margin_mode = 0
        self.trade_calc_mode = self.SYMBOL_CALC_MODE_FOREX
        self.check_result = SimpleNamespace(retcode=0, comment="Done")
        self.position = SimpleNamespace(
            ticket=71,
            symbol="EURUSD",
            type=self.POSITION_TYPE_BUY,
            volume=0.3,
            price_open=1.2,
            sl=1.1,
            tp=1.3,
        )

    def symbol_info_tick(self, symbol: str) -> SimpleNamespace:
        return SimpleNamespace(ask=1.2345, bid=1.2343, time=1786716000)

    def symbol_info(self, symbol: str) -> SimpleNamespace:
        return SimpleNamespace(point=0.00001, digits=5, trade_calc_mode=self.trade_calc_mode, currency_profit="USD")

    def account_info(self) -> SimpleNamespace:
        return SimpleNamespace(margin_mode=self.margin_mode)

    def positions_get(self, **kwargs: object) -> tuple[SimpleNamespace, ...]:
        if kwargs.get("symbol") == self.position.symbol or kwargs.get("ticket") == self.position.ticket:
            return (self.position,)
        return ()

    def orders_get(self, **kwargs: object) -> tuple[SimpleNamespace, ...]:
        return tuple(order for order in self.orders if order.ticket == kwargs.get("ticket"))

    def order_check(self, request: dict[str, object]) -> SimpleNamespace:
        self.events.append("check")
        self.checked_requests.append(request)
        return self.check_result

    def order_send(self, request: dict[str, object]) -> SimpleNamespace:
        self.events.append("send")
        self.sent_requests.append(request)
        return getattr(self, "send_result", SimpleNamespace(retcode=10009))

    def last_error(self) -> tuple[int, str]:
        return (0, "ok")


if __name__ == "__main__":
    unittest.main()
