"""Reusable, read-only analysis primitives for Trader-mediated tick history."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
import json
import math


class TickAnalysisError(ValueError):
    """Raised when tick-analysis input cannot produce an unambiguous result."""


@dataclass(frozen=True, slots=True)
class Tick:
    """One broker quote, normalized to a UTC timestamp."""

    observed_at: datetime
    bid: float
    ask: float


@dataclass(frozen=True, slots=True)
class PriceRun:
    """The largest directional movement in one tolerance-delimited run."""

    side: str
    direction: str
    tolerance_percent: float
    started_at: datetime
    ended_at: datetime
    start_price: float
    end_price: float
    change: float
    change_percent: float

    def json(self) -> dict[str, object]:
        result = asdict(self)
        result["started_at"] = utc_timestamp(self.started_at)
        result["ended_at"] = utc_timestamp(self.ended_at)
        return result


@dataclass(frozen=True, slots=True)
class TrendExtremes:
    """Largest rise and fall for one price side and tolerance."""

    rise: PriceRun
    fall: PriceRun

    def json(self) -> dict[str, object]:
        return {"rise": self.rise.json(), "fall": self.fall.json()}


@dataclass(frozen=True, slots=True)
class CrossOpportunity:
    """A continuous interval where an executable cross was observable."""

    direction: str
    started_at: datetime
    ended_at: datetime
    maximum_edge: float

    def json(self) -> dict[str, object]:
        return {
            "direction": self.direction,
            "started_at": utc_timestamp(self.started_at),
            "ended_at": utc_timestamp(self.ended_at),
            "duration_seconds": (self.ended_at - self.started_at).total_seconds(),
            "maximum_edge": self.maximum_edge,
        }


def utc_timestamp(value: datetime) -> str:
    """Format an aware timestamp in the RPC's unambiguous UTC form."""

    if value.tzinfo is None:
        raise TickAnalysisError("Tick timestamps must include a UTC offset.")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_ticks(records: Iterable[Mapping[str, object]]) -> list[Tick]:
    """Validate, normalize, and timestamp-sort MT5 historical tick records."""

    ticks: list[Tick] = []
    for record in records:
        milliseconds = record.get("time_msc")
        seconds = record.get("time")
        if _is_number(milliseconds):
            observed_at = datetime.fromtimestamp(float(milliseconds) / 1_000, tz=UTC)
        elif _is_number(seconds):
            observed_at = datetime.fromtimestamp(float(seconds), tz=UTC)
        else:
            raise TickAnalysisError("A historical tick has no numeric time_msc or time field.")
        bid = _positive_number(record.get("bid"), "bid")
        ask = _positive_number(record.get("ask"), "ask")
        ticks.append(Tick(observed_at=observed_at, bid=bid, ask=ask))
    return sorted(ticks, key=lambda tick: tick.observed_at)


def deduplicate_tick_records(records: Iterable[Mapping[str, object]]) -> list[dict[str, object]]:
    """De-duplicate overlapping adaptive-slice responses without changing their fields."""

    unique: dict[str, dict[str, object]] = {}
    for record in records:
        copied = dict(record)
        try:
            key = json.dumps(copied, allow_nan=False, separators=(",", ":"), sort_keys=True)
        except (TypeError, ValueError) as error:
            raise TickAnalysisError("A historical tick record is not JSON-compatible.") from error
        unique.setdefault(key, copied)
    return list(unique.values())


def fetch_adaptive_ticks(
    fetch: Callable[[datetime, datetime], Sequence[Mapping[str, object]]],
    *,
    start: datetime,
    end: datetime,
    max_records: int = 1_000,
    maximum_slice: timedelta = timedelta(hours=1),
    minimum_slice: timedelta = timedelta(seconds=1),
) -> list[dict[str, object]]:
    """Fetch bounded tick history, splitting any response that reaches its record cap.

    Adjacent slices intentionally share their boundary; exact duplicate records are
    removed after all requests finish so no tick is lost at a boundary.
    """

    _validate_interval(start, end)
    if max_records < 1:
        raise TickAnalysisError("The historical tick record limit must be positive.")
    if maximum_slice <= timedelta(0):
        raise TickAnalysisError("The maximum historical tick slice must be positive.")
    if minimum_slice <= timedelta(0):
        raise TickAnalysisError("The minimum historical tick slice must be positive.")

    collected: list[Mapping[str, object]] = []

    def collect(slice_start: datetime, slice_end: datetime) -> None:
        response = fetch(slice_start, slice_end)
        if len(response) > max_records:
            raise TickAnalysisError("A historical tick response exceeded the documented record limit.")
        if len(response) < max_records:
            collected.extend(response)
            return
        duration = slice_end - slice_start
        if duration <= minimum_slice:
            raise TickAnalysisError(
                "Historical tick density reached the record limit at the minimum slice; use a smaller --min-slice-seconds."
            )
        midpoint = slice_start + duration / 2
        collect(slice_start, midpoint)
        collect(midpoint, slice_end)

    slice_start = start.astimezone(UTC)
    final_end = end.astimezone(UTC)
    while slice_start < final_end:
        slice_end = min(slice_start + maximum_slice, final_end)
        collect(slice_start, slice_end)
        slice_start = slice_end
    return deduplicate_tick_records(collected)


def analyze_trends(ticks: Sequence[Tick], tolerance_percents: Iterable[float]) -> dict[str, dict[str, TrendExtremes]]:
    """Find largest tolerance-delimited rises and falls independently for bid and ask."""

    if not ticks:
        raise TickAnalysisError("At least one historical tick is required for trend analysis.")
    ordered = sorted(ticks, key=lambda tick: tick.observed_at)
    result: dict[str, dict[str, TrendExtremes]] = {"bid": {}, "ask": {}}
    for tolerance_percent in tolerance_percents:
        tolerance = _tolerance_ratio(tolerance_percent)
        label = _tolerance_label(tolerance_percent)
        for side in ("bid", "ask"):
            result[side][label] = TrendExtremes(
                rise=_largest_directional_run(ordered, side, tolerance_percent, tolerance, rising=True),
                fall=_largest_directional_run(ordered, side, tolerance_percent, tolerance, rising=False),
            )
    return result


def find_cross_opportunities(
    first_ticks: Sequence[Tick],
    second_ticks: Sequence[Tick],
    *,
    quote_staleness: timedelta,
) -> list[CrossOpportunity]:
    """Find bid-to-opposite-ask crosses while both latest quotes remain fresh."""

    if quote_staleness <= timedelta(0):
        raise TickAnalysisError("Quote staleness must be positive.")
    events = sorted(
        [(tick.observed_at, 0, tick) for tick in first_ticks] + [(tick.observed_at, 1, tick) for tick in second_ticks],
        key=lambda event: event[0],
    )
    opportunities: list[CrossOpportunity] = []
    first_quote: Tick | None = None
    second_quote: Tick | None = None
    index = 0
    while index < len(events):
        observed_at = events[index][0]
        while index < len(events) and events[index][0] == observed_at:
            _, worker_index, tick = events[index]
            if worker_index == 0:
                first_quote = tick
            else:
                second_quote = tick
            index += 1
        next_observed_at = events[index][0] if index < len(events) else None
        if first_quote is None or second_quote is None:
            continue
        expiration = min(first_quote.observed_at + quote_staleness, second_quote.observed_at + quote_staleness)
        interval_end = expiration if next_observed_at is None else min(expiration, next_observed_at)
        if interval_end <= observed_at:
            continue
        for direction, edge in (
            ("first_bid_ge_second_ask", first_quote.bid - second_quote.ask),
            ("second_bid_ge_first_ask", second_quote.bid - first_quote.ask),
        ):
            if edge >= 0:
                _append_opportunity(opportunities, direction, observed_at, interval_end, edge)
    return sorted(opportunities, key=lambda item: (item.started_at, item.direction))


def select_active_pair(
    pairs: Iterable[Mapping[str, object]], *, pair_id: str | None = None
) -> dict[str, object]:
    """Select exactly one active-pair response, rejecting ambiguous inventory."""

    active = [dict(pair) for pair in pairs if isinstance(pair.get("product_pair_id"), str)]
    if pair_id is not None:
        selected = [pair for pair in active if pair["product_pair_id"] == pair_id]
        if len(selected) != 1:
            raise TickAnalysisError(f"Active product pair '{pair_id}' was not found.")
        return selected[0]
    if len(active) != 1:
        raise TickAnalysisError(
            f"Expected exactly one active product pair, but the controller returned {len(active)}; specify --pair-id."
        )
    return active[0]


def select_safe_pair_workers(
    pair: Mapping[str, object], workers: Iterable[Mapping[str, object]]
) -> list[tuple[str, str]]:
    """Map each pair endpoint to a connected, safe Worker and its symbol.

    Prefer the pair's original source Worker. If it is unavailable, an endpoint
    can use its sole connected, safe Worker on the same MT5 server.
    """

    endpoints = pair.get("endpoints")
    sources = pair.get("source_workers")
    if not isinstance(endpoints, list) or len(endpoints) != 2 or not isinstance(sources, Mapping):
        raise TickAnalysisError("The active product pair has invalid endpoints or source Workers.")
    source_by_server: dict[str, str] = {}
    for source in sources.values():
        if not isinstance(source, Mapping):
            raise TickAnalysisError("The active product pair has an invalid source Worker.")
        server = source.get("server")
        worker_id = source.get("worker_id")
        if not isinstance(server, str) or not server or not isinstance(worker_id, str) or not worker_id:
            raise TickAnalysisError("The active product pair has an invalid source Worker.")
        if server in source_by_server:
            raise TickAnalysisError("The active product pair must use source Workers on different servers.")
        source_by_server[server] = worker_id
    active_workers = {
        worker.get("worker_id"): worker
        for worker in workers
        if isinstance(worker.get("worker_id"), str)
    }
    selection: list[tuple[str, str]] = []
    for endpoint in endpoints:
        if not isinstance(endpoint, Mapping):
            raise TickAnalysisError("The active product pair has an invalid endpoint.")
        server = endpoint.get("server")
        symbol = endpoint.get("symbol")
        if not isinstance(server, str) or not server or not isinstance(symbol, str) or not symbol:
            raise TickAnalysisError("The active product pair has an invalid endpoint.")
        preferred_worker_id = source_by_server.get(server)
        safe_workers = [
            (worker_id, worker)
            for worker_id, worker in active_workers.items()
            if worker.get("server") == server
            and worker.get("connectivity") == "connected"
            and worker.get("safety_state") == "connected"
        ]
        preferred = next((candidate for candidate in safe_workers if candidate[0] == preferred_worker_id), None)
        if preferred is not None:
            worker_id, worker = preferred
        elif len(safe_workers) == 1:
            worker_id, worker = safe_workers[0]
        elif not safe_workers:
            raise TickAnalysisError(f"No connected, safe Worker is available for endpoint {server}:{symbol}.")
        else:
            raise TickAnalysisError(f"Multiple connected, safe Workers are available for endpoint {server}:{symbol}.")
        selection.append((worker_id, symbol))
    return selection


def _largest_directional_run(
    ticks: Sequence[Tick], side: str, tolerance_percent: float, tolerance: float, *, rising: bool
) -> PriceRun:
    start = ticks[0]
    extreme = start
    previous = start
    largest = _run(start, extreme, side, tolerance_percent, rising)
    for current in ticks[1:]:
        previous_price = getattr(previous, side)
        current_price = getattr(current, side)
        reversal = (
            current_price < previous_price * (1 - tolerance)
            if rising
            else current_price > previous_price * (1 + tolerance)
        )
        if reversal:
            candidate = _run(start, extreme, side, tolerance_percent, rising)
            if candidate.change > largest.change:
                largest = candidate
            start = current
            extreme = current
        elif (current_price > getattr(extreme, side)) if rising else (current_price < getattr(extreme, side)):
            extreme = current
        previous = current
    candidate = _run(start, extreme, side, tolerance_percent, rising)
    return candidate if candidate.change > largest.change else largest


def _run(start: Tick, end: Tick, side: str, tolerance_percent: float, rising: bool) -> PriceRun:
    start_price = getattr(start, side)
    end_price = getattr(end, side)
    change = end_price - start_price if rising else start_price - end_price
    return PriceRun(
        side=side,
        direction="rise" if rising else "fall",
        tolerance_percent=tolerance_percent,
        started_at=start.observed_at,
        ended_at=end.observed_at,
        start_price=start_price,
        end_price=end_price,
        change=change,
        change_percent=change / start_price * 100,
    )


def _append_opportunity(
    opportunities: list[CrossOpportunity], direction: str, started_at: datetime, ended_at: datetime, edge: float
) -> None:
    for index in range(len(opportunities) - 1, -1, -1):
        previous = opportunities[index]
        if previous.direction != direction:
            continue
        if previous.ended_at == started_at:
            opportunities[index] = CrossOpportunity(
                direction=direction,
                started_at=previous.started_at,
                ended_at=ended_at,
                maximum_edge=max(previous.maximum_edge, edge),
            )
            return
        break
    opportunities.append(CrossOpportunity(direction, started_at, ended_at, edge))


def _tolerance_ratio(value: float) -> float:
    if not _is_number(value) or value < 0 or value >= 100:
        raise TickAnalysisError("Tolerance percentages must be numeric values from 0 up to (but not including) 100.")
    return value / 100


def _tolerance_label(value: float) -> str:
    return f"{value:g}%"


def _validate_interval(start: datetime, end: datetime) -> None:
    if start.tzinfo is None or end.tzinfo is None or start >= end:
        raise TickAnalysisError("Historical tick bounds must be ordered, offset-aware timestamps.")


def _positive_number(value: object, field: str) -> float:
    if not _is_number(value) or value <= 0:
        raise TickAnalysisError(f"A historical tick has an invalid {field} quote.")
    return float(value)


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
