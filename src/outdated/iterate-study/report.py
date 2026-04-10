#!/usr/bin/env python3
"""Report: what % of iterate-study schedulers beat the original one-shot scheduler?

Usage:
    python report.py --effort none
    python report.py --effort high --metric throughput
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ITERATE_DIR = Path(__file__).resolve().parent
SRC_DIR = ITERATE_DIR.parent
ONE_SHOT_DIR = SRC_DIR / "one-shot"


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--effort", required=True, choices=["none", "low", "medium", "high"])
    parser.add_argument("--metric", default="latency", choices=["latency", "throughput"])
    args = parser.parse_args()

    # Load generation metadata
    meta_path = ITERATE_DIR / f"schedulers-{args.effort}" / "meta.json"
    assert meta_path.exists(), f"No metadata at {meta_path}. Run generate.py first."
    meta = json.loads(meta_path.read_text())
    original_median = meta[f"source_median_{args.metric}"]

    # Load analysis results (written by one-shot/analyze.py --experiment iterate)
    analysis_path = (
        ONE_SHOT_DIR / "experiments" / "iterate" / "output"
        / f"schedulers-{args.effort}" / "analysis.jsonl"
    )
    assert analysis_path.exists(), (
        f"No analysis at {analysis_path}\n"
        f"Run: cd {ONE_SHOT_DIR} && python analyze.py "
        f"{ITERATE_DIR / f'schedulers-{args.effort}'} --experiment iterate"
    )

    records = []
    with open(analysis_path) as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    total = len(records)
    functional = [r for r in records if r["functional"]]

    if args.metric == "latency":
        better = [r for r in functional if r[f"median_{args.metric}"] < original_median]
    else:
        better = [r for r in functional if r[f"median_{args.metric}"] > original_median]

    print(f"Source: {meta['source_scheduler']} (median {args.metric}: {original_median:.4f})")
    print(f"Model: {meta['model']}")
    print(f"Total attempts: {total}")
    print(f"Functional: {len(functional)}/{total} ({100*len(functional)/total:.0f}%)")
    print(f"Beat original: {len(better)}/{total} ({100*len(better)/total:.0f}%)")

    if better:
        if args.metric == "latency":
            best_improved = min(better, key=lambda r: r[f"median_{args.metric}"])
        else:
            best_improved = max(better, key=lambda r: r[f"median_{args.metric}"])
        imp = (best_improved[f"median_{args.metric}"] - original_median) / original_median * 100
        print(f"Best improvement: {best_improved['filename']} ({imp:+.1f}%)")

    if functional:
        medians = sorted(r[f"median_{args.metric}"] for r in functional)
        print(f"\nAll functional median {args.metric} values:")
        for r in sorted(functional, key=lambda r: r[f"median_{args.metric}"]):
            vs_orig = (r[f"median_{args.metric}"] - original_median) / original_median * 100
            marker = " <-- better" if r in better else ""
            print(f"  {r['filename']}: {r[f'median_{args.metric}']:.4f} ({vs_orig:+.1f}%){marker}")


if __name__ == "__main__":
    main()
