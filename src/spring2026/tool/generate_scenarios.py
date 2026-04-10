#!/usr/bin/env python3
"""Generate scenarios.csv as a cross product of trace files and params.toml files.

Usage:
    python generate_scenarios.py --traces traces/ --params params/ --output scenarios.csv
"""
from __future__ import annotations

import argparse
import csv
import itertools
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--traces", type=Path, required=True, help="Directory of trace .csv files.")
    parser.add_argument("--params", type=Path, required=True, help="Directory of params .toml files.")
    parser.add_argument("--output", type=Path, default=Path("scenarios.csv"), help="Output CSV path (default: scenarios.csv).")
    args = parser.parse_args()

    assert args.traces.is_dir(), f"Not a directory: {args.traces}"
    assert args.params.is_dir(), f"Not a directory: {args.params}"

    trace_files = sorted(args.traces.glob("*.csv"))
    params_files = sorted(args.params.glob("*.toml"))

    assert trace_files, f"No .csv files found in {args.traces}"
    assert params_files, f"No .toml files found in {args.params}"

    with open(args.output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["trace", "params"])
        writer.writerows(itertools.product(trace_files, params_files))

    print(f"Wrote {len(trace_files) * len(params_files)} scenarios ({len(trace_files)} traces x {len(params_files)} params) to {args.output}")


if __name__ == "__main__":
    main()
