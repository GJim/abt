"""Risk-gated, single-pair cross-broker arbitrage simulation."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, time
import math
from zoneinfo import ZoneInfo


class ArbitrageSimulationError(ValueError):
    """Raised when arbitrage simulation inputs are unsafe or inconsistent."""


@dataclass(frozen=True, slots=True)
class Quote:
    """One executable broker quote at an offset-aware timestamp."""

    observed_at: datetime
    bid: float
    ask: float


@dataclass(frozen=True, slots=True)
class AccountSpec:
    """The account facts needed to size and risk-check one broker leg."""

    name: str
    starting_equity: float
    margin_per_lot: float
    minimum_volume: float
    volume_step: float


@dataclass(frozen=True, slots=True)
class RiskPolicy:
    """Per-account loss limits, expressed as fractions of equity."""

    daily_loss_fraction: float = 0.03
    trade_loss_fraction: float = 0.02


@dataclass(frozen=True, slots=True)
class SimulationConfig:
    """The strategy inputs shared by historical and live quote adapters."""

    entry_edge: float
    requested_volume: float
    contract_size: float = 100_000
    timezone: ZoneInfo = ZoneInfo("America/New_York")
    minimum_hold_seconds: float = 0
    emergency_protection_usd: float | None = None
    flatten_at_local: time | None = None
    maximum_trades: int = 100
    maximum_margin_fraction: float = 0.5


@dataclass(frozen=True, slots=True)
class CompletedTrade:
    """A closed, hedged pair and its per-account realised outcome."""

    direction: str
    opened_at: datetime
    closed_at: datetime
    volume: float
    audacity_pnl: float
    ftmo_pnl: float
    close_reason: str


@dataclass(frozen=True, slots=True)
class AccountResult:
    """The end-of-window cash balance and marked equity for one account."""

    balance: float
    equity: float
    stopped: bool


@dataclass(frozen=True, slots=True)
class SimulationResult:
    """A complete result behind the simulator's one-call interface."""

    trades: tuple[CompletedTrade, ...]
    audacity: AccountResult
    ftmo: AccountResult
    rejected_entries: int
    stopped_reason: str | None


@dataclass(slots=True)
class _OpenPair:
    direction: str
    opened_at: datetime
    volume: float
    audacity_entry: float
    ftmo_entry: float
    audacity_equity_at_entry: float
    ftmo_equity_at_entry: float


def simulate_single_pair(
    audacity_quotes: Sequence[Quote],
    ftmo_quotes: Sequence[Quote],
    *,
    audacity: AccountSpec,
    ftmo: AccountSpec,
    policy: RiskPolicy,
    config: SimulationConfig,
) -> SimulationResult:
    """Simulate one cross-broker pair, stopping before either account breaches a limit.

    A trade opens only after both quotes exist and its directional edge reaches
    ``entry_edge``. It closes on an opposite qualifying edge only after the
    configured minimum hold, broker emergency protection, a per-trade loss
    stop, a daily account loss stop, or the configured daily cutoff. After a
    normal reversal close, the simulator waits for both edges to disappear
    before accepting another entry.
    """

    _validate(audacity, ftmo, policy, config)
    events = sorted(
        [(quote.observed_at, "audacity", quote) for quote in audacity_quotes]
        + [(quote.observed_at, "ftmo", quote) for quote in ftmo_quotes],
        key=lambda item: item[0],
    )
    if not events:
        raise ArbitrageSimulationError("At least one quote is required.")

    balances = {"audacity": audacity.starting_equity, "ftmo": ftmo.starting_equity}
    daily_start = balances.copy()
    daily_date: object | None = None
    latest: dict[str, Quote] = {}
    open_pair: _OpenPair | None = None
    awaiting_clear = False
    stopped_reason: str | None = None
    rejected_entries = 0
    trades: list[CompletedTrade] = []
    index = 0
    while index < len(events):
        observed_at = events[index][0]
        while index < len(events) and events[index][0] == observed_at:
            _, name, quote = events[index]
            latest[name] = quote
            index += 1
        if set(latest) != {"audacity", "ftmo"}:
            continue

        current_date = observed_at.astimezone(config.timezone).date()
        if current_date != daily_date:
            equity = _equity(balances, open_pair, latest, config.contract_size)
            daily_start = equity.copy()
            daily_date = current_date

        if config.flatten_at_local is not None and observed_at.astimezone(config.timezone).time() >= config.flatten_at_local:
            if open_pair is not None:
                trades.append(_close(open_pair, latest, observed_at, config.contract_size, "daily_cutoff"))
                _apply_trade(balances, trades[-1])
                open_pair = None
            stopped_reason = "daily_cutoff"
            break

        equity = _equity(balances, open_pair, latest, config.contract_size)
        if stopped_reason is None and _daily_stop(equity, daily_start, policy):
            if open_pair is not None:
                trades.append(_close(open_pair, latest, observed_at, config.contract_size, "daily_loss_stop"))
                _apply_trade(balances, trades[-1])
                open_pair = None
            stopped_reason = "daily_loss_stop"
            continue
        if stopped_reason is not None:
            continue

        if open_pair is not None and config.emergency_protection_usd is not None:
            protection_reason = _emergency_protection_reason(
                open_pair, latest, config.emergency_protection_usd, config.contract_size
            )
            if protection_reason is not None:
                trades.append(_close(open_pair, latest, observed_at, config.contract_size, protection_reason))
                _apply_trade(balances, trades[-1])
                open_pair = None
                stopped_reason = protection_reason
                continue

        if open_pair is not None and _trade_stop(open_pair, latest, policy, config.contract_size):
            trades.append(_close(open_pair, latest, observed_at, config.contract_size, "trade_loss_stop"))
            _apply_trade(balances, trades[-1])
            open_pair = None
            stopped_reason = "trade_loss_stop"
            continue

        direction = _direction(latest, config.entry_edge)
        if open_pair is not None:
            held_seconds = (observed_at - open_pair.opened_at).total_seconds()
            if direction is not None and direction != open_pair.direction and held_seconds >= config.minimum_hold_seconds:
                trades.append(_close(open_pair, latest, observed_at, config.contract_size, "reverse_edge"))
                _apply_trade(balances, trades[-1])
                open_pair = None
                awaiting_clear = True
            continue
        if awaiting_clear:
            if direction is None:
                awaiting_clear = False
            continue
        if direction is not None and len(trades) < config.maximum_trades:
            volume = _volume(config.requested_volume, audacity, ftmo, balances, config.maximum_margin_fraction)
            if volume is None:
                rejected_entries += 1
                continue
            open_pair = _open(direction, latest, observed_at, volume, balances)

    equity = _equity(balances, open_pair, latest, config.contract_size)
    return SimulationResult(
        trades=tuple(trades),
        audacity=AccountResult(balances["audacity"], equity["audacity"], stopped_reason is not None),
        ftmo=AccountResult(balances["ftmo"], equity["ftmo"], stopped_reason is not None),
        rejected_entries=rejected_entries,
        stopped_reason=stopped_reason,
    )


def _direction(quotes: dict[str, Quote], edge: float) -> str | None:
    audacity, ftmo = quotes["audacity"], quotes["ftmo"]
    if audacity.bid - ftmo.ask + 1e-12 >= edge:
        return "short_audacity_long_ftmo"
    if ftmo.bid - audacity.ask + 1e-12 >= edge:
        return "long_audacity_short_ftmo"
    return None


def _open(
    direction: str, quotes: dict[str, Quote], observed_at: datetime, volume: float, balances: dict[str, float]
) -> _OpenPair:
    audacity, ftmo = quotes["audacity"], quotes["ftmo"]
    if direction == "short_audacity_long_ftmo":
        return _OpenPair(direction, observed_at, volume, audacity.bid, ftmo.ask, balances["audacity"], balances["ftmo"])
    return _OpenPair(direction, observed_at, volume, audacity.ask, ftmo.bid, balances["audacity"], balances["ftmo"])


def _close(
    pair: _OpenPair, quotes: dict[str, Quote], observed_at: datetime, contract_size: float, reason: str
) -> CompletedTrade:
    audacity, ftmo = quotes["audacity"], quotes["ftmo"]
    if pair.direction == "short_audacity_long_ftmo":
        audacity_pnl = (pair.audacity_entry - audacity.ask) * contract_size * pair.volume
        ftmo_pnl = (ftmo.bid - pair.ftmo_entry) * contract_size * pair.volume
    else:
        audacity_pnl = (audacity.bid - pair.audacity_entry) * contract_size * pair.volume
        ftmo_pnl = (pair.ftmo_entry - ftmo.ask) * contract_size * pair.volume
    return CompletedTrade(pair.direction, pair.opened_at, observed_at, pair.volume, audacity_pnl, ftmo_pnl, reason)


def _equity(
    balances: dict[str, float], pair: _OpenPair | None, quotes: dict[str, Quote], contract_size: float
) -> dict[str, float]:
    if pair is None:
        return balances.copy()
    marked = _close(pair, quotes, quotes["audacity"].observed_at, contract_size, "mark")
    return {"audacity": balances["audacity"] + marked.audacity_pnl, "ftmo": balances["ftmo"] + marked.ftmo_pnl}


def _daily_stop(equity: dict[str, float], daily_start: dict[str, float], policy: RiskPolicy) -> bool:
    return any(equity[name] <= daily_start[name] * (1 - policy.daily_loss_fraction) for name in equity)


def _trade_stop(pair: _OpenPair, quotes: dict[str, Quote], policy: RiskPolicy, contract_size: float) -> bool:
    marked = _close(pair, quotes, quotes["audacity"].observed_at, contract_size, "mark")
    return (
        marked.audacity_pnl <= -pair.audacity_equity_at_entry * policy.trade_loss_fraction
        or marked.ftmo_pnl <= -pair.ftmo_equity_at_entry * policy.trade_loss_fraction
    )


def _emergency_protection_reason(
    pair: _OpenPair, quotes: dict[str, Quote], protection_usd: float, contract_size: float
) -> str | None:
    marked = _close(pair, quotes, quotes["audacity"].observed_at, contract_size, "mark")
    pnl = (marked.audacity_pnl, marked.ftmo_pnl)
    if any(value <= -protection_usd for value in pnl):
        return "emergency_stop_loss"
    if any(value >= protection_usd for value in pnl):
        return "emergency_take_profit"
    return None


def _apply_trade(balances: dict[str, float], trade: CompletedTrade) -> None:
    balances["audacity"] += trade.audacity_pnl
    balances["ftmo"] += trade.ftmo_pnl


def _volume(
    requested: float,
    audacity: AccountSpec,
    ftmo: AccountSpec,
    balances: dict[str, float],
    maximum_margin_fraction: float,
) -> float | None:
    upper = min(
        requested,
        balances["audacity"] * maximum_margin_fraction / audacity.margin_per_lot,
        balances["ftmo"] * maximum_margin_fraction / ftmo.margin_per_lot,
    )
    step = max(audacity.volume_step, ftmo.volume_step)
    volume = math.floor((upper + 1e-12) / step) * step
    minimum = max(audacity.minimum_volume, ftmo.minimum_volume)
    return round(volume, 8) if volume + 1e-12 >= minimum else None


def _validate(audacity: AccountSpec, ftmo: AccountSpec, policy: RiskPolicy, config: SimulationConfig) -> None:
    for account in (audacity, ftmo):
        if (
            not account.name
            or account.starting_equity <= 0
            or account.margin_per_lot <= 0
            or account.minimum_volume <= 0
            or account.volume_step <= 0
        ):
            raise ArbitrageSimulationError("Account specifications must contain positive funding, margin, and volume values.")
    if not 0 < policy.daily_loss_fraction < 1 or not 0 < policy.trade_loss_fraction < 1:
        raise ArbitrageSimulationError("Loss fractions must be between zero and one.")
    if (
        config.entry_edge <= 0
        or config.requested_volume <= 0
        or config.contract_size <= 0
        or config.minimum_hold_seconds < 0
        or config.emergency_protection_usd is not None and config.emergency_protection_usd <= 0
        or config.maximum_trades <= 0
        or not 0 < config.maximum_margin_fraction <= 1
    ):
        raise ArbitrageSimulationError("Entry edge, requested volume, and contract size must be positive.")
