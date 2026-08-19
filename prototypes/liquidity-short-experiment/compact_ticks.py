"""Compact one `mt5 ticks-range --output json` export into a Parquet tick chunk."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()

    source = json.loads(args.input.read_text(encoding="utf-8"))
    rows = source["records"]
    ticks = {
        "time_ns": [int(row["time_msc"]) * 1_000_000 for row in rows if row["bid"] > 0 and row["ask"] > 0],
        "bid": [row["bid"] for row in rows if row["bid"] > 0 and row["ask"] > 0],
        "ask": [row["ask"] for row in rows if row["bid"] > 0 and row["ask"] > 0],
    }
    if not ticks["time_ns"]:
        raise ValueError("The tick export has no valid bid/ask rows.")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(ticks), args.output, compression="zstd")
    print(json.dumps({"ticks": len(ticks["time_ns"]), "output": str(args.output)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
