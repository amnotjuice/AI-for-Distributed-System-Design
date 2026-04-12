#!/usr/bin/env python3
"""Generate scenarios.csv as a cross product of trace pairs, params.toml files, and objectives.

Trace files are expected in pairs: one with seed 0 (train) and one with seed 1 (test),
distinguished by a trailing _0 / _1 before the .csv extension. Each pair is matched by
their shared base name (e.g. trace_scale4x_600s_0.csv and trace_scale4x_600s_1.csv).

The output cross product is: trace_pairs × params × objectives, where objectives cycles
through ["weight_1", "weight_2"].

Usage:
    python generate_scenarios.py --traces traces/ --params params/ --output scenarios.csv
"""
from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from pathlib import Path

OBJECTIVES = ["basic", "prio"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--traces", type=Path, required=True, help="Directory of trace .csv files.")
    parser.add_argument("--params", type=Path, required=True, help="Directory of params .toml files.")
    parser.add_argument("--output", type=Path, default=Path("../scenarios/scenarios.csv"), help="Output CSV path (default: ../scenarios/scenarios.csv).")
    args = parser.parse_args()

    assert args.traces.is_dir(), f"Not a directory: {args.traces}"
    assert args.params.is_dir(), f"Not a directory: {args.params}"

    # Group trace files by base name (strip trailing _train / _test)
    groups: dict[str, dict[str, Path]] = defaultdict(dict)
    for t in sorted(args.traces.glob("*.csv")):
        m = re.match(r"^(.+)_(train|test)\.csv$", t.name)
        if m:
            groups[m.group(1)][m.group(2)] = t

    pairs = [(g["train"], g["test"]) for g in (groups[b] for b in sorted(groups)) if "train" in g and "test" in g]
    assert pairs, f"No train/test trace pairs found in {args.traces}"

    params_files = sorted(args.params.glob("*.toml"))
    assert params_files, f"No .toml files found in {args.params}"

    rows = [
        [train, test, params, obj]
        for train, test in pairs
        for params in params_files
        for obj in OBJECTIVES
    ]

    with open(args.output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["train_trace", "test_trace", "params", "objective"])
        writer.writerows(rows)

    print(f"Wrote {len(rows)} scenarios ({len(pairs)} trace pairs × {len(params_files)} params × {len(OBJECTIVES)} objectives) to {args.output}")


if __name__ == "__main__":
    main()
