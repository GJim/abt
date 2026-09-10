"""Compare pair-cell trading performance across strategy regimes from worker logs.

Parses leader/follower debug logs (plain UTF-8 leader logs and PowerShell
Tee-Object UTF-16 follower logs, including mid-line console wraps) plus the
leader's local pair-cell SQLite, and prints:
  * per-log entry/realized/trail/solo/human counts,
  * leader legs split by a cutoff timestamp (old vs new protection model),
  * pair nets matched by attempt ID across both sides.

Example:
    uv run python scripts/analyze_pair_performance.py `
      --leader-log leader.log `
      --follower-log follower.log follwer-1.log follwer-2.log `
      --sqlite .venv/Scripts/worker.paircell.sqlite `
      --cutoff 2026-09-09T01:17
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

TS = re.compile(r"^\s*(uv\s*:\s*)?(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})")


def read_records(path: Path) -> list[str]:
    """Split a log into logical records, rejoining console-wrapped lines."""
    try:
        raw = path.read_text(encoding="utf-8")
    except (UnicodeError, ValueError):
        raw = path.read_text(encoding="utf-16", errors="replace")
    out: list[str] = []
    current: str | None = None
    for line in raw.splitlines():
        match = TS.match(line)
        if match:
            if current is not None:
                out.append(current)
            current = line[match.start(2):]
        elif current is not None:
            current += line.strip()
    if current is not None:
        out.append(current)
    return out


def parse_log(path: Path) -> dict:
    entries: dict[str, dict] = {}
    observed: dict[str, dict] = {}
    realized: list[dict] = []
    closes: list[dict] = []
    trails = asym = solos = 0
    humans: list[str] = []
    lines = read_records(path)
    for line in lines:
        # Attempt IDs are 8-char prefixes in the compact vocabulary; they
        # match across both sides because both truncate the same UUID.
        match = re.search(
            r"evt=entry_selected att=(\S+) sym=(\S+) dir=(\S+) lots=(\S+) edge=(\S+)",
            line,
        )
        if match:
            aid, symbol, direction, lots, edge = match.groups()
            entries[aid] = {
                "symbol": symbol, "direction": direction, "lots": lots,
                "edge": edge, "at": line[:19],
            }
            continue
        match = re.search(
            r"evt=entry_filled att=(\S+) tkt=(\S+) sym=(\S+) side=(\S+) vol=(\S+) px=(\S+)",
            line,
        )
        if match:
            aid = match.group(1)
            observed[aid] = {"ticket": match.group(2), "side": match.group(4)}
            if aid in entries:
                entries[aid].update({"ticket": match.group(2), "side": match.group(4)})
            continue
        match = re.search(r"evt=realized_pnl_recorded .*position:([^=\s]+)=(-?[\d.]+)", line)
        if match:
            realized.append({"ticket": match.group(1), "pnl": float(match.group(2).rstrip(".")), "at": line[:19]})
        match = re.search(r"evt=close_requested \S+ att=(\S+) (.+)$", line)
        if match:
            closes.append({"attempt": match.group(1), "reason": match.group(2).strip()})
        if "profit_trail_applied" in line:
            trails += 1
        if "asymmetric_protection_applied" in line:
            asym += 1
        if "peer_leg_empty_leader_continues_solo" in line:
            solos += 1
        if "needs_human=True" in line:
            humans.append(line[:19])
    return {
        "entries": entries, "observed": observed, "realized": realized, "closes": closes,
        "trails": trails, "asym": asym, "solos": solos, "humans": humans,
        "first": lines[0][:19] if lines else None, "last": lines[-1][:19] if lines else None,
    }


def summarize(name: str, parsed: dict) -> None:
    realized = parsed["realized"]
    total = sum(item["pnl"] for item in realized)
    wins = sum(1 for item in realized if item["pnl"] > 0)
    print(f"== {name} [{parsed['first']}..{parsed['last']}]")
    print(f"   entries={len(parsed['entries'])} realized={len(realized)} "
          f"total={total:.2f} wins={wins}/{len(realized)}")
    print(f"   trails={parsed['trails']} asym_init={parsed['asym']} solos={parsed['solos']} "
          f"human_lines={len(parsed['humans'])}")
    symbols = Counter(entry["symbol"] for entry in parsed["entries"].values())
    if symbols:
        print(f"   symbols={dict(symbols)}")
    reasons = Counter(close["reason"] for close in parsed["closes"])
    if reasons:
        print(f"   close_reasons={dict(reasons)}")


def sqlite_legs(path: Path, cutoff: str) -> tuple[list[float], list[float]]:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            "SELECT amount_usd, recorded_at FROM cell_realized_pnl"
        ).fetchall()
    finally:
        connection.close()
    old = [float(amount) for amount, at in rows if at < cutoff]
    new = [float(amount) for amount, at in rows if at >= cutoff]
    return old, new


def describe(tag: str, values: list[float]) -> None:
    wins = sum(1 for value in values if value > 0)
    extra = (f" wr={wins / len(values):.0%} avg={sum(values) / len(values):.2f}"
             f" best={max(values):.2f} worst={min(values):.2f}") if values else ""
    print(f"{tag}: n={len(values)} total={sum(values):.2f} wins={wins}/{len(values)}{extra}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leader-log", type=Path, required=True)
    parser.add_argument("--follower-log", type=Path, nargs="*", default=[])
    parser.add_argument("--sqlite", type=Path, default=None)
    parser.add_argument("--cutoff", default="2026-09-09T01:17",
                        help="ISO prefix separating old/new regimes in SQLite")
    arguments = parser.parse_args(argv)

    leader = parse_log(arguments.leader_log)
    summarize(f"leader {arguments.leader_log.name}", leader)

    follower_realized: dict[str, float] = {}
    follower_observed: dict[str, str] = {}
    for path in arguments.follower_log:
        if not path.exists():
            print(f"== {path.name} MISSING")
            continue
        parsed = parse_log(path)
        summarize(f"follower {path.name}", parsed)
        for item in parsed["realized"]:
            follower_realized[item["ticket"]] = item["pnl"]
        for aid, obs in parsed["observed"].items():
            follower_observed[aid] = obs["ticket"]

    if arguments.sqlite is not None and arguments.sqlite.exists():
        old, new = sqlite_legs(arguments.sqlite, arguments.cutoff)
        describe(f"sqlite leader-legs OLD (<{arguments.cutoff})", old)
        describe(f"sqlite leader-legs NEW (>={arguments.cutoff})", new)

    leader_realized = {item["ticket"]: item["pnl"] for item in leader["realized"]}
    print("\n== pair nets (attempt-matched)")
    matched = 0
    net = 0.0
    def _fmt(value: float | None) -> str:
        return f"{value:.2f}" if value is not None else "None"

    for aid, entry in leader["entries"].items():
        ours = leader_realized.get(entry.get("ticket", ""), None)
        ticket = follower_observed.get(aid, "")
        theirs = follower_realized.get(ticket, None)
        if ours is not None and theirs is not None:
            matched += 1
            net += ours + theirs
        print(f"  {entry['symbol']} {entry['direction']} L={_fmt(ours)} F={_fmt(theirs)} at={entry['at']}")
    print(f"matched={matched} pair_net={net:.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
