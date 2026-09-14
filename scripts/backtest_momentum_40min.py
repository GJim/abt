"""Backtest momentum entry opportunities per symbol over recent 40 minutes.

Uses the *given* pair config (embedded as CONFIG below, entry_mode=momentum)
and replays leader-leg ticks through the production ``momentum_bias`` pure
function (``abt.pair_cell``), with production merge semantics:

* 1-second resampled mid buffer, last tick wins per second bucket;
* spread gate: ``(ask-bid)/point <= trend_max_spread_points`` (leader-only);
* ``momentum_bias`` with T/vol/k/min_mom/coverage from CONFIG;
* ``min_mom_points`` is a *point count* (x point -> price units), mirroring
  ``PairExecutionCell._trend_bias_for_product``.

Window: fetches EVAL_MINUTES (40) + WARMUP (= vol window, 600s) of ticks so
the first evaluated second already has full volatility history; signals are
only counted inside the trailing 40-minute evaluation window.

Opportunity clustering: raw per-second signals are grouped into one entry
opportunity while the same direction persists with gaps <= CLUSTER_GAP_S;
a direction flip or a longer silence starts a new opportunity. The reported
entry point is the first signal second of each cluster.

Usage (Windows, MT5 terminal already running):
    .\\.venv\\Scripts\\python.exe scripts\\backtest_momentum_40min.py [--minutes 40] [--symbols EURUSD,USDJPY,...]
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from abt.pair_cell import momentum_bias  # noqa: E402  pure function, no side effects

# --------------------------------------------------------------------------
# Given pair config (verbatim from the request).
# --------------------------------------------------------------------------
CONFIG = {
    "daily_loss_fraction": "0.02",
    "daily_loss_warning_threshold_usd": "20",
    "entry_edge_points": "4",
    "entry_mode": "momentum",
    "follower_confirmation_timeout_seconds": 5.0,
    "maximum_holding_seconds": None,
    "maximum_loss_per_trade_usd": "40",
    "maximum_margin_fraction": "0.02",
    "mode": "live",
    "quote_max_age_seconds": 10.0,
    "quote_max_skew_seconds": 10.0,
    "relay_handling_timeout_seconds": 5.0,
    "sizing_refresh_seconds": 3600.0,
    "strategy_budget_usd": "5000",
    "trade_loss_fraction": "0.01",
    "trading_blackout_end_ny": "18:30",
    "trading_blackout_start_ny": "16:30",
    "trend_breakout_buffer_points": "2",
    "trend_lookback_seconds": 1800.0,
    "trend_max_spread_points": "20",
    "trend_min_coverage": 0.8,
    "trend_min_mom_points": "5",
    "trend_min_range_points": "12",
    "trend_momentum_T_seconds": 120.0,
    "trend_momentum_k": "2.0",
    "trend_vol_window_seconds": 600.0,
}

# product_id map from the request (derived product identity per symbol).
PRODUCTS = {
    "AUDCAD": "AUDCAD:019687aa605c111a",
    "AUDCHF": "AUDCHF:29723c5703fc735b",
    "AUDJPY": "AUDJPY:d0ef412368925244",
    "AUDNZD": "AUDNZD:a0761b1cffc71ac7",
    "AUDUSD": "AUDUSD:da92ec32ad2e23df",
    "CADCHF": "CADCHF:46d656621b8e2639",
    "CADJPY": "CADJPY:57851048a2603b36",
    "CHFJPY": "CHFJPY:2402c7a46ae20ed7",
    "EURAUD": "EURAUD:6a8aa4c994904fa9",
    "EURCAD": "EURCAD:96f1c5d66b1bfe57",
    "EURCHF": "EURCHF:842cefcb620073de",
    "EURGBP": "EURGBP:f4b7a22b47ecf173",
    "EURJPY": "EURJPY:9e2bb669a089e078",
    "EURNZD": "EURNZD:f85674367794454d",
    "EURUSD": "EURUSD:5c67e6326ec43040",
    "GBPAUD": "GBPAUD:e9247b78578cc56d",
    "GBPCAD": "GBPCAD:ead8e5e917ab21a6",
    "GBPCHF": "GBPCHF:2d03d4b9e6c22a11",
    "GBPJPY": "GBPJPY:4970b6befbfab336",
    "GBPNZD": "GBPNZD:2eee334a9604aa00",
    "GBPUSD": "GBPUSD:919ca73614cff1fa",
    "NZDCAD": "NZDCAD:1a9b2c14f4024ed7",
    "NZDCHF": "NZDCHF:2c3ece693c2203ef",
    "NZDJPY": "NZDJPY:abcde99b3c336d91",
    "NZDUSD": "NZDUSD:ac5eca294b47f896",
    "USDCAD": "USDCAD:ecd0cd7158acbd96",
    "USDCHF": "USDCHF:758e666ae02d9cd1",
    "USDJPY": "USDJPY:1599629db933e22b",
    "XAGUSD": "XAGUSD:c361b3ab4b689c74",
    "XAUUSD": "XAUUSD:88a2fe647262edcb",
}

NY = ZoneInfo("America/New_York")
CLUSTER_GAP_S = 30  # same-direction signals bridged across <=30s silence


def get(item, key: str, default=None):
    try:
        return item[key]
    except (KeyError, IndexError, TypeError):
        return getattr(item, key, default)


def in_blackout(now_utc: datetime) -> bool:
    """NY Mon-Fri [start,end) plus all weekend: no entries (canonical policy)."""
    ny = now_utc.astimezone(NY)
    if ny.weekday() >= 5:
        return True
    sh, sm = map(int, CONFIG["trading_blackout_start_ny"].split(":"))
    eh, em = map(int, CONFIG["trading_blackout_end_ny"].split(":"))
    hm = ny.hour * 60 + ny.minute
    return sh * 60 + sm <= hm < eh * 60 + em


def fetch_ticks(mt5, symbol: str, start, end):
    ticks = mt5.copy_ticks_range(symbol, start, end, mt5.COPY_TICKS_ALL)
    if ticks is None:
        return []
    return list(ticks)


def build_second_series(ticks):
    """sec -> (mid, spread, last_bid, last_ask); last tick wins per second."""
    per_sec: dict[int, list] = defaultdict(list)
    for t in ticks:
        ms = get(t, "time_msc") or int(float(get(t, "time", 0)) * 1000)
        bid, ask = float(get(t, "bid")), float(get(t, "ask"))
        if bid <= 0 or ask <= 0 or ask < bid:
            continue
        per_sec[int(ms / 1000)].append((ms, bid, ask))
    out: dict[int, tuple[float, float, float, float]] = {}
    for sec, lst in per_sec.items():
        _, bid, ask = max(lst)
        out[sec] = ((bid + ask) / 2.0, ask - bid, bid, ask)
    return out


def backtest_symbol(mt5, symbol: str, minutes: float, end_utc: datetime) -> dict:
    info = mt5.symbol_info(symbol)
    if info is None:
        return {"symbol": symbol, "error": "symbol_info returned None"}
    point = float(info.point)
    if not point or point <= 0:
        return {"symbol": symbol, "error": f"bad point={info.point}"}
    digits = int(get(info, "digits", 5) or 5)

    vol_s = float(CONFIG["trend_vol_window_seconds"])
    warmup_s = vol_s + 60.0  # full vol window + margin so t=0 is evaluable
    start = end_utc - timedelta(seconds=minutes * 60 + warmup_s)
    eval_from = end_utc - timedelta(seconds=minutes * 60)

    ticks = fetch_ticks(mt5, symbol, start, end_utc)
    series = build_second_series(ticks)
    secs = sorted(series)
    if len(secs) < 10:
        return {
            "symbol": symbol, "point": point, "n_ticks": len(ticks),
            "n_seconds": len(secs), "error": "too few ticks (feed/bridge or market closed?)",
        }

    t_s = float(CONFIG["trend_momentum_T_seconds"])
    k = Decimal(str(CONFIG["trend_momentum_k"]))
    min_mom = Decimal(str(CONFIG["trend_min_mom_points"])) * Decimal(str(point))
    coverage = float(CONFIG["trend_min_coverage"])
    max_spread = float(CONFIG["trend_max_spread_points"])

    history = [(s, Decimal(str(series[s][0]))) for s in secs]
    raw_signals: list[dict] = []
    blocked_spread = 0
    best_score = 0.0
    best_mom_pts = 0.0
    eval_seconds = 0
    for i, s in enumerate(secs):
        if s < int(eval_from.timestamp()):
            continue
        eval_seconds += 1
        mid = Decimal(str(series[s][0]))
        spread = series[s][1]
        if spread / point > max_spread:
            blocked_spread += 1
            continue
        hist = history[: i + 1]
        bias, strength, _reason = momentum_bias(
            hist, mid, now_epoch=float(s),
            t_seconds=t_s, vol_window_seconds=vol_s,
            k=k, min_mom_points=min_mom, min_coverage=coverage,
        )
        ref = [(x, m) for x, m in hist if x <= s - t_s]
        mom_pts = abs(float(mid - ref[-1][1])) / point if ref else 0.0
        sc = abs(float(strength))
        best_score = max(best_score, sc)
        best_mom_pts = max(best_mom_pts, mom_pts)
        if bias is not None:
            if in_blackout(datetime.fromtimestamp(s, timezone.utc)):
                continue
            raw_signals.append({
                "epoch": s,
                "time_utc": datetime.fromtimestamp(s, timezone.utc).strftime("%H:%M:%S"),
                "dir": bias,
                "score": round(sc, 2),
                "mom_pts": round(mom_pts, 1),
                "mid": round(series[s][0], digits),
            })

    # Cluster raw signals into entry opportunities.
    opportunities: list[dict] = []
    for sig in raw_signals:
        if (opportunities and opportunities[-1]["dir"] == sig["dir"]
                and sig["epoch"] - opportunities[-1]["last_epoch"] <= CLUSTER_GAP_S):
            opportunities[-1]["last_epoch"] = sig["epoch"]
            opportunities[-1]["last_time"] = sig["time_utc"]
            opportunities[-1]["n_signals"] += 1
            opportunities[-1]["peak_score"] = max(opportunities[-1]["peak_score"], sig["score"])
            opportunities[-1]["peak_mom_pts"] = max(opportunities[-1]["peak_mom_pts"], sig["mom_pts"])
        else:
            opportunities.append({
                "dir": sig["dir"], "first_time": sig["time_utc"], "last_time": sig["time_utc"],
                "first_epoch": sig["epoch"], "last_epoch": sig["epoch"],
                "entry_mid": sig["mid"], "first_score": sig["score"],
                "first_mom_pts": sig["mom_pts"], "peak_score": sig["score"],
                "peak_mom_pts": sig["mom_pts"], "n_signals": 1,
            })

    hi = max(v[0] for v in series.values())
    lo = min(v[0] for v in series.values())
    return {
        "symbol": symbol,
        "product_id": PRODUCTS.get(symbol, "?"),
        "point": point,
        "n_ticks": len(ticks),
        "n_seconds": len(secs),
        "eval_seconds": eval_seconds,
        "range_pct": round((hi - lo) / lo * 100, 3) if lo else 0.0,
        "best_score": round(best_score, 2),
        "best_mom_pts": round(best_mom_pts, 1),
        "blocked_spread": blocked_spread,
        "raw_signals": len(raw_signals),
        "opportunities": opportunities,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--minutes", type=float, default=40.0)
    ap.add_argument("--symbols", default=",".join(PRODUCTS),
                    help="comma-separated MT5 symbols (default: all 30)")
    args = ap.parse_args()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    assert CONFIG["entry_mode"] == "momentum", CONFIG["entry_mode"]
    print(f"config: entry_mode=momentum T={CONFIG['trend_momentum_T_seconds']}s "
          f"vol={CONFIG['trend_vol_window_seconds']}s k={CONFIG['trend_momentum_k']} "
          f"min_mom={CONFIG['trend_min_mom_points']}pts max_spread={CONFIG['trend_max_spread_points']}pts "
          f"coverage={CONFIG['trend_min_coverage']}")
    print(f"window: last {args.minutes:g}min evaluated (+{float(CONFIG['trend_vol_window_seconds']) + 60:.0f}s warmup, not counted)")

    import MetaTrader5 as mt5

    if not mt5.initialize():
        print(f"MT5 initialize() failed: {mt5.last_error()} (is the terminal running?)")
        return 2
    try:
        end_utc = datetime.now(timezone.utc)
        eval_from = end_utc - timedelta(seconds=args.minutes * 60)
        print(f"eval window (UTC): {eval_from:%Y-%m-%d %H:%M:%S} .. {end_utc:%Y-%m-%d %H:%M:%S}")
        total_opp = 0
        total_raw = 0
        for symbol in symbols:
            try:
                r = backtest_symbol(mt5, symbol, args.minutes, end_utc)
            except Exception as exc:  # noqa: BLE001  per-symbol isolation
                print(f"\n== {symbol}: ERROR {exc}")
                continue
            if "error" in r:
                print(f"\n== {symbol} ({r.get('product_id', '?')}): ERROR {r['error']} "
                      f"(ticks={r.get('n_ticks', 0)}, secs={r.get('n_seconds', 0)})")
                continue
            opps = r["opportunities"]
            total_opp += len(opps)
            total_raw += r["raw_signals"]
            print(f"\n== {symbol} ({r['product_id']}): "
                  f"opportunities={len(opps)} raw_signals={r['raw_signals']} "
                  f"[ticks={r['n_ticks']} secs={r['n_seconds']} eval_secs={r['eval_seconds']} "
                  f"range={r['range_pct']}% best_score={r['best_score']} "
                  f"best_mom={r['best_mom_pts']}pts spread_blocks={r['blocked_spread']}]")
            for n, o in enumerate(opps, 1):
                print(f"    #{n} {o['dir']} entry@{o['first_time']}UTC mid={o['entry_mid']} "
                      f"score={o['first_score']} mom={o['first_mom_pts']}pts "
                      f"(peak score={o['peak_score']} peak_mom={o['peak_mom_pts']}pts, "
                      f"holds_to={o['last_time']} n={o['n_signals']})")
            if not opps:
                print("    (no entry)")
        print(f"\nTOTAL: opportunities={total_opp} raw_signals={total_raw} across {len(symbols)} symbols")
        return 0
    finally:
        mt5.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
