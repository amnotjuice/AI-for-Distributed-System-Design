#!/usr/bin/env python3
"""Re-analyze two-shot-avg-low and two-shot-avg-rich-low with the updated analyze.py.

Clears existing analysis.jsonl for both groups and re-runs evaluation,
capturing OOM counts, completion rates, and per-priority latency.

Usage:
    python reanalyze_avg.py
    python reanalyze_avg.py --groups two-shot-avg-low          # single group
    python reanalyze_avg.py --dry-run                          # list what would run
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ITERATE_DIR = Path(__file__).resolve().parent
ONE_SHOT_DIR = ITERATE_DIR.parent / "one-shot"

GROUPS = [
    "two-shot-avg-low",
    "two-shot-avg-rich-low",
]


def reanalyze(group: str, dry_run: bool = False) -> None:
    scheduler_dir = ITERATE_DIR / group / "schedulers"
    output_dir = ITERATE_DIR / group / "output"
    output_path = output_dir / "analysis.jsonl"

    if not scheduler_dir.exists():
        print(f"[{group}] scheduler dir not found: {scheduler_dir}")
        return

    print(f"\n=== {group} ===")
    print(f"  schedulers: {scheduler_dir}")
    print(f"  output:     {output_path}")

    if dry_run:
        n = len(list(scheduler_dir.glob("scheduler_*.py")))
        print(f"  [dry-run] would delete {output_path} and re-evaluate {n} schedulers")
        return

    # Remove old results so analyze.py re-runs all schedulers fresh
    if output_path.exists():
        output_path.unlink()
        print(f"  Removed old {output_path.name}")

    cmd = [
        sys.executable,
        str(ONE_SHOT_DIR / "analyze.py"),
        str(scheduler_dir),
        "--output-dir", str(output_dir),
    ]
    print(f"  Running: {' '.join(str(c) for c in cmd)}")
    result = subprocess.run(cmd, cwd=str(ONE_SHOT_DIR))
    if result.returncode != 0:
        print(f"  [ERROR] analyze.py exited with code {result.returncode}")
    else:
        print(f"  Done -> {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--groups", nargs="+", default=GROUPS,
                        help="Which groups to re-analyze (default: both avg groups)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would happen without running")
    args = parser.parse_args()

    for group in args.groups:
        reanalyze(group, dry_run=args.dry_run)

    print("\nDone. Run the comparison script after this completes.")


if __name__ == "__main__":
    main()
