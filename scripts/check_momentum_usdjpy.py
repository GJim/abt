"""Momentum entry audit: replay USDJPY ticks through production momentum_bias.

Compares three configurations over the same 2h tick history:
  (a) live  -- verbatim adopted policy from worker.paircell.sqlite (cell_policy)
  (b) fixed -- same k/T but min_mom interpreted as point counts (x point)
  (c) relaxed -- k=1.0, min 2pts, T=300s

Also runs feed-health checks (tick count, flat feed, gaps) so a data/bridge
problem is distinguishable from a too-strict threshold.

Usage (Windows, MT5 terminal already running):
    .\\.venv\\Scripts\\python.exe scripts/check_momentum_usdjpy.py [--symbol USDJPY] [--hours 2]
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from abt.pair_cell import momentum_bias  # noqa: E402  (pure function, no side effects)

REPO = Path(__file__).resolve().parents[1]
SQLITE = REPO / ".venv" / "Scripts" / "worker.paircell.sqlite"
JSON_CFG = REPO / ".venv" / "Scripts" / "worker.paircell.json"


def load_live_policy() -> dict:
    """Canonical adopted policy from sqlite; fallback to the leader json file."""
    try:
        con = sqlite3.connect("file:" + str(SQLITE) + "?mode=ro", uri=True)
        row = con.execute("SELECT payload FROM cell_policy").fetchone()
        con.close()
        if row:
            return json.loads(row[0])
    except Exception as exc:
        print(f"[warn] sqlite policy unreadable ({exc}), falling back to json file")
    return json.loads(JSON_CFG.read_text(encoding="utf-8"))


def fetch_ticks(symbol: str, hours: float):
    import MetaTrader5 as mt5

    if not mt5.initialize():
        raise SystemExit(f"MT5 initialize() failed: {mt5.last_error()} (is the terminal running?)")
    try:
        info = mt5.symbol_info(symbol)
        if info is None:
            raise SystemExit(f"symbol_info({symbol}) returned None")
        point = float(info.point)
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=hours, minutes=5)
        ticks = mt5.copy_ticks_range(symbol, start, end, mt5.COPY_TICKS_ALL)
        if ticks is None or len(ticks) == 0:
            # degraded fallback mirroring production: M1 bars
            bars = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M1, 0, int(hours * 60) + 5)
            return point, [], bars, True
        return point, ticks, None, False
    finally:
        mt5.shutdown()


def get(item, key: str, default=None):
    try:
        return item[key]
    except (KeyError, IndexError, TypeError):
        return getattr(item, key, default)


def build_second_bars(ticks, degraded_bars):
    """bucket -> (mid, spread); last tick wins per second (production merge semantics)."""
    bars: dict[int, tuple[float, float]] = {}
    if degraded_bars is not None:
        for b in degraded_bars:
            sec = int(get(b, "time"))
            mid = (float(get(b, "high")) + float(get(b, "low"))) / 2.0
            bars[sec] = (mid, 0.0)
        return bars, True
    per_sec: dict[int, list] = defaultdict(list)
    for t in ticks:
        ms = get(t, "time_msc") or int(float(get(t, "time", 0)) * 1000)
        bid, ask = float(get(t, "bid")), float(get(t, "ask"))
        if bid <= 0 or ask <= 0 or ask < bid:
            continue
        per_sec[int(ms / 1000)].append((ms, bid, ask))
    for sec, lst in per_sec.items():
        _, bid, ask = max(lst)  # last tick wins, like production seed merge
        bars[sec] = ((bid + ask) / 2.0, ask - bid)
    return bars, False


def evaluate(bars, secs, point, cfg, label):
    t_s = float(cfg["T"])
    vol_s = float(cfg["vol"])
    k = Decimal(str(cfg["k"]))
    min_mom = Decimal(str(cfg["min_mom_price"]))
    coverage = float(cfg["coverage"])
    max_spread = float(cfg.get("max_spread_pts", 1e9))
    history = [(s, Decimal(str(bars[s][0]))) for s in secs]  # ascending
    sigs = []
    best_score = 0.0
    best_mom_pts = 0.0
    blocked_spread = 0
    for i, s in enumerate(secs):
        if s < secs[0] + vol_s:
            continue  # warming: need full vol window, mirrors min_coverage gate
        if i % 5:
            continue  # 5s step is plenty for T>=120s signals
        mid = Decimal(str(bars[s][0]))
        if bars[s][1] / point > max_spread:
            blocked_spread += 1
            continue
        hist = history[: i + 1]
        bias, strength, _ = momentum_bias(
            hist, mid, now_epoch=float(s),
            t_seconds=t_s, vol_window_seconds=vol_s,
            k=k, min_mom_points=min_mom, min_coverage=coverage,
        )
        sc = abs(float(strength))
        ref = [(x, m) for x, m in hist if x <= s - t_s]
        mom_pts = abs(float(mid - ref[-1][1])) / point if ref else 0.0
        best_score = max(best_score, sc)
        best_mom_pts = max(best_mom_pts, mom_pts)
        if bias is not None:
            sigs.append((s, bias, sc, mom_pts))
    print(f"[{label}] T={t_s:g}s k={cfg['k']} min_mom={cfg['min_mom_price']} "
          f"-> signals={len(sigs)} best_|score|={best_score:.2f} best_|mom|={best_mom_pts:.1f}pts "
          f"spread_blocks={blocked_spread}")
    for s, d, sc, mp in sigs[:10]:
        print(f"    {datetime.fromtimestamp(s, timezone.utc):%H:%M:%S} {d} score={sc:.2f} mom={mp:.1f}pts")
    return sigs, best_score, best_mom_pts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="USDJPY")
    ap.add_argument("--hours", type=float, default=2.0)
    args = ap.parse_args()

    policy = load_live_policy()
    print(f"live policy: entry_mode={policy.get('entry_mode')} "
          f"T={policy.get('trend_momentum_T_seconds')} vol={policy.get('trend_vol_window_seconds')} "
          f"k={policy.get('trend_momentum_k')} min_mom={policy.get('trend_min_mom_points')} "
          f"coverage={policy.get('trend_min_coverage')} max_spread={policy.get('trend_max_spread_points')}")

    point, ticks, bars_fb, degraded = fetch_ticks(args.symbol, args.hours)
    print(f"point({args.symbol})={point} degraded={'YES(M1 fallback)' if degraded else 'no(ticks)'}")
    if not degraded and (ticks is None or len(ticks) < 10):
        print(f"FEED BUG: only {0 if ticks is None else len(ticks)} ticks in {args.hours}h "
              f"-> bridge/feed problem, not a threshold problem.")
        return 2
    bars, used_fb = build_second_bars(ticks, bars_fb)
    secs = sorted(bars)
    print(f"seconds covered: {len(secs)} (span {(secs[-1]-secs[0])/3600:.2f}h), n_ticks={0 if degraded else len(ticks)}")
    if len(secs) >= 2:
        hi = max(m for m, _ in bars.values())
        lo = min(m for m, _ in bars.values())
        print(f"window range: {lo} -> {hi} = {(hi-lo)/lo*100:.3f}%")
    if not degraded:
        mids = [m for m, _ in bars.values()]
        if max(mids) == min(mids):
            print("FEED BUG: mid perfectly flat for 2h -> frozen feed, not a threshold problem.")
            return 2

    cov = float(policy.get("trend_min_coverage", 0.8))
    live = {"T": float(policy["trend_momentum_T_seconds"]), "vol": float(policy["trend_vol_window_seconds"]),
            "k": str(policy["trend_momentum_k"]), "min_mom_price": str(policy["trend_min_mom_points"]),
            "coverage": cov, "max_spread_pts": float(policy.get("trend_max_spread_points", 1e9))}
    fixed = dict(live, min_mom_price=str(Decimal(str(policy["trend_min_mom_points"])) * Decimal(str(point))))
    relaxed = dict(live, T=300.0, k="1.0",
                   min_mom_price=str(Decimal("2") * Decimal(str(point))))

    print(f"unit check: live min_mom={live['min_mom_price']} price-units "
          f"= {Decimal(live['min_mom_price'])/Decimal(str(point)):,.0f} points @ point={point}")
    evaluate(bars, secs, point, live, "live-verbatim")
    evaluate(bars, secs, point, fixed, "unit-fixed (5pts)")
    evaluate(bars, secs, point, relaxed, "relaxed (k=1,min2pts,T=300)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
