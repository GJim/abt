"""Analyze 24 hours of one active pair's Trader-mediated historical quotes.

Run from this Windows repository, for example:

    uv run python scripts\analyze_active_pair_ticks.py --pair-id <product-pair-id>

The script launches ``abt-trader connect --jsonl`` as a long-lived child process.
It performs only read requests, first queries active Workers and pairs, and refuses
to use a source Worker that is disconnected, frozen, or needs human intervention.
Each historical request is capped at 1,000 ticks, so the script adaptively bisects
full responses and removes duplicate boundary ticks.  Output is one JSON document.

Trend tolerance is a percent of the immediately preceding quote.  A rising run
ends only when an adjacent quote declines by *more* than its tolerance; a falling
run analogously ends only on a rise of more than its tolerance.  Crosses use the
latest quote from each Worker only while both are no older than
``--quote-staleness-seconds``.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
from queue import Empty, Queue
import subprocess
import sys
from threading import Thread
from typing import TextIO
from uuid import uuid4

from abt.trader.tick_analysis import (
    Tick,
    TickAnalysisError,
    analyze_trends,
    fetch_adaptive_ticks,
    find_cross_opportunities,
    parse_ticks,
    select_active_pair,
    select_safe_pair_workers,
    utc_timestamp,
)


class TraderJsonlClient:
    """A correlated JSONL client for one long-lived ``abt-trader connect`` process."""

    def __init__(self, command: Sequence[str], *, timeout_seconds: float) -> None:
        self._timeout_seconds = timeout_seconds
        try:
            self._process = subprocess.Popen(
                list(command),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=None,
                text=True,
                encoding="utf-8",
                bufsize=1,
            )
        except OSError as error:
            raise TickAnalysisError(f"Could not start abt-trader: {error}") from error
        if self._process.stdin is None or self._process.stdout is None:
            self.close()
            raise TickAnalysisError("Could not open abt-trader JSONL streams.")
        self._responses: Queue[str | None] = Queue()
        Thread(target=self._read_stdout, name="abt-trader-jsonl-output", daemon=True).start()

    def query(self, query: str) -> dict[str, object]:
        request_id = f"tick-analysis-{uuid4()}"
        response = self._request({"request_id": request_id, "query": query}, request_id, "trader_query_result")
        if response.get("query") != query:
            raise TickAnalysisError("abt-trader returned a mismatched inventory query response.")
        result = response.get("result")
        if not isinstance(result, dict):
            raise TickAnalysisError("abt-trader returned an invalid inventory query result.")
        return result

    def historical_ticks(self, worker_id: str, symbol: str, start: datetime, end: datetime) -> list[dict[str, object]]:
        request_id = f"tick-analysis-{uuid4()}"
        response = self._request(
            {
                "request_id": request_id,
                "worker_id": worker_id,
                "payload": {
                    "kind": "read",
                    "request": {
                        "type": "historical_ticks",
                        "symbol": symbol,
                        "from_utc": utc_timestamp(start),
                        "to_utc": utc_timestamp(end),
                        "flags": "info",
                    },
                },
            },
            request_id,
            "trader_rpc_result",
        )
        if response.get("status") != "completed":
            reason = _reason(response)
            raise TickAnalysisError(f"Historical tick request for Worker '{worker_id}' was rejected: {reason}")
        result = response.get("result")
        if not isinstance(result, Mapping) or not isinstance(result.get("ticks"), list):
            raise TickAnalysisError("abt-trader returned an invalid historical tick result.")
        ticks = result["ticks"]
        if not all(isinstance(tick, Mapping) for tick in ticks):
            raise TickAnalysisError("abt-trader returned an invalid historical tick record.")
        return [dict(tick) for tick in ticks]

    def close(self) -> None:
        if not hasattr(self, "_process"):
            return
        if self._process.stdin is not None:
            try:
                self._process.stdin.close()
            except (OSError, ValueError):
                pass
        if self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5)

    def _request(self, command: dict[str, object], request_id: str, response_type: str) -> dict[str, object]:
        if self._process.poll() is not None:
            raise TickAnalysisError(f"abt-trader exited with code {self._process.returncode}.")
        assert self._process.stdin is not None
        try:
            self._process.stdin.write(json.dumps(command, separators=(",", ":"), sort_keys=True) + "\n")
            self._process.stdin.flush()
        except (OSError, ValueError) as error:
            raise TickAnalysisError("Could not send the request to abt-trader.") from error
        while True:
            try:
                line = self._responses.get(timeout=self._timeout_seconds)
            except Empty as error:
                raise TickAnalysisError(f"Timed out waiting for abt-trader request '{request_id}'.") from error
            if line is None:
                raise TickAnalysisError(f"abt-trader exited with code {self._process.poll()}.")
            try:
                response = json.loads(line)
            except json.JSONDecodeError as error:
                raise TickAnalysisError("abt-trader emitted invalid JSONL output.") from error
            if not isinstance(response, dict):
                raise TickAnalysisError("abt-trader emitted a non-object JSONL response.")
            if (
                response.get("request_id") == request_id
                and response.get("type") == response_type
                and not (response_type == "trader_rpc_result" and response.get("status") == "recorded")
            ):
                return response

    def _read_stdout(self) -> None:
        assert self._process.stdout is not None
        for line in self._process.stdout:
            self._responses.put(line)
        self._responses.put(None)


def main(argv: list[str] | None = None, *, output: TextIO | None = None, error_output: TextIO | None = None) -> int:
    """Run the selected active-pair historical tick analysis."""

    output = output or sys.stdout
    error_output = error_output or sys.stderr
    arguments = _parser().parse_args(argv)
    try:
        end = _parse_utc(arguments.to_utc) if arguments.to_utc else datetime.now(UTC)
        start = end - timedelta(hours=arguments.hours)
        if arguments.hours <= 0:
            raise TickAnalysisError("--hours must be positive.")
        if (
            arguments.quote_staleness_seconds <= 0
            or arguments.max_slice_minutes <= 0
            or arguments.min_slice_seconds <= 0
            or arguments.rpc_timeout_seconds <= 0
        ):
            raise TickAnalysisError("Staleness, maximum/minimum slice, and RPC timeout values must be positive.")
        tolerances = arguments.tolerance_percent or [1.0, 0.1, 0.01]
        command = [arguments.trader_executable, "connect"]
        if arguments.trader_config is not None:
            command.extend(["--config", str(arguments.trader_config)])
        command.append("--jsonl")
        client = TraderJsonlClient(command, timeout_seconds=arguments.rpc_timeout_seconds)
        try:
            worker_result = client.query("active_workers")
            pair_result = client.query("active_pairs")
            workers = _object_list(worker_result.get("workers"), "active Workers")
            pair = select_active_pair(_object_list(pair_result.get("pairs"), "active pairs"), pair_id=arguments.pair_id)
            selected_workers = select_safe_pair_workers(pair, workers)
            tick_sets: dict[str, list[Tick]] = {}
            endpoint_summary: list[dict[str, object]] = []
            for worker_id, symbol in selected_workers:
                records = fetch_adaptive_ticks(
                    lambda slice_start, slice_end, worker_id=worker_id, symbol=symbol: client.historical_ticks(
                        worker_id, symbol, slice_start, slice_end
                    ),
                    start=start,
                    end=end,
                    maximum_slice=timedelta(minutes=arguments.max_slice_minutes),
                    minimum_slice=timedelta(seconds=arguments.min_slice_seconds),
                )
                ticks = parse_ticks(records)
                if not ticks:
                    raise TickAnalysisError(f"No historical ticks were returned for Worker '{worker_id}' symbol '{symbol}'.")
                tick_sets[worker_id] = ticks
                endpoint_summary.append({"worker_id": worker_id, "symbol": symbol, "tick_count": len(ticks)})
            first_worker_id, _ = selected_workers[0]
            second_worker_id, _ = selected_workers[1]
            report = {
                "product_pair_id": pair["product_pair_id"],
                "from_utc": utc_timestamp(start),
                "to_utc": utc_timestamp(end),
                "quote_staleness_seconds": arguments.quote_staleness_seconds,
                "tolerance_percents": tolerances,
                "endpoints": endpoint_summary,
                "trends": {
                    worker_id: {
                        side: {tolerance: extremes.json() for tolerance, extremes in values.items()}
                        for side, values in analyze_trends(tick_sets[worker_id], tolerances).items()
                    }
                    for worker_id, _ in selected_workers
                },
                "cross_opportunities": [
                    opportunity.json()
                    for opportunity in find_cross_opportunities(
                        tick_sets[first_worker_id],
                        tick_sets[second_worker_id],
                        quote_staleness=timedelta(seconds=arguments.quote_staleness_seconds),
                    )
                ],
            }
        finally:
            client.close()
        print(json.dumps(report, allow_nan=False, indent=2, sort_keys=True), file=output)
        return 0
    except TickAnalysisError as error:
        print(f"Historical tick analysis failed: {error}", file=error_output)
        return 1
    except KeyboardInterrupt:
        print("Historical tick analysis stopped.", file=error_output)
        return 130


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read and analyze one active pair's Trader-mediated historical ticks; never sends broker operations."
    )
    parser.add_argument("--pair-id", help="the one active product-pair ID to analyze; required if multiple pairs are active")
    parser.add_argument("--hours", type=float, default=24, help="UTC history window ending now or --to-utc (default: 24)")
    parser.add_argument("--to-utc", help="UTC window end as ISO-8601, for example 2026-08-26T01:00:00Z")
    parser.add_argument(
        "--tolerance-percent",
        type=float,
        action="append",
        help="trend reversal tolerance percent; repeat as needed (default: 1, 0.1, 0.01)",
    )
    parser.add_argument("--quote-staleness-seconds", type=float, default=5, help="maximum age of either cross quote (default: 5)")
    parser.add_argument("--max-slice-minutes", type=float, default=60, help="initial tick request interval (default: 60)")
    parser.add_argument("--min-slice-seconds", type=float, default=1, help="smallest adaptive tick request interval (default: 1)")
    parser.add_argument("--rpc-timeout-seconds", type=float, default=45, help="per-JSONL-request timeout (default: 45)")
    parser.add_argument(
        "--trader-executable",
        default="abt-trader",
        help=r"abt-trader executable (default: abt-trader; example: C:\path\to\abt-trader.exe)",
    )
    parser.add_argument("--trader-config", type=Path, help=r"optional Trader identity config, for example C:\Users\you\abt\trader.json")
    return parser


def _parse_utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise TickAnalysisError("--to-utc must be an ISO-8601 timestamp with an offset or Z.") from error
    if parsed.tzinfo is None:
        raise TickAnalysisError("--to-utc must include an offset or Z.")
    return parsed.astimezone(UTC)


def _object_list(value: object, label: str) -> list[Mapping[str, object]]:
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise TickAnalysisError(f"abt-trader returned invalid {label}.")
    return list(value)


def _reason(response: Mapping[str, object]) -> str:
    result = response.get("result")
    if isinstance(result, Mapping) and isinstance(result.get("reason"), str):
        return result["reason"]
    return "no reason was supplied"


if __name__ == "__main__":
    raise SystemExit(main())
