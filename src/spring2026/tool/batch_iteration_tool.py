#!/usr/bin/env python3
"""Batch improvement: runs one_iteration_tool over a directory of schedulers.

Usage:
    python batch_iteration_tool.py --schedulers schedulers-high/ \\
        --trace traces/trace_scale1x_600s_0.csv --output out/
"""
from __future__ import annotations

import argparse
import csv
import os
import random
import sys
from pathlib import Path

TOOLS_DIR = Path(__file__).resolve().parent
ONE_SHOT_DIR = TOOLS_DIR.parent
sys.path.insert(0, str(TOOLS_DIR))
sys.path.insert(0, str(ONE_SHOT_DIR))
sys.path.insert(0, str(ONE_SHOT_DIR.parent))

from dotenv import load_dotenv
from config import get_canonical_base_params
from one_iteration_tool import (
    MOCK_DATA_DIR, build_improvement_prompt, call_llm,
    evaluate_across_scales, extract_code, geometric_mean,
)


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--schedulers", type=Path, required=True)
    parser.add_argument("--trace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scales", default="1,2,4,8,16")
    parser.add_argument("--model", default="gpt-5.2-2025-12-11")
    parser.add_argument("--effort", default="high", choices=["none", "low", "medium", "high"])
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    assert args.schedulers.is_dir(), f"Not a directory: {args.schedulers}"
    assert args.trace.exists(), f"Trace not found: {args.trace}"
    if not args.dry_run:
        assert os.environ.get("OPENAI_API_KEY"), "OPENAI_API_KEY not set"
    scheduler_files = sorted(args.schedulers.glob("scheduler_*.py"))
    assert scheduler_files, f"No scheduler_*.py found in {args.schedulers}"

    scales = [int(s.strip()) for s in args.scales.split(",")]
    args.output.mkdir(parents=True, exist_ok=True)
    base_params = get_canonical_base_params()
    csv_path = args.output / "results.csv"
    csv_columns = ["scheduler", "geomean_latency"] + [f"latency_{s}x" for s in scales]

    print(f"Schedulers : {len(scheduler_files)} in {args.schedulers}")
    print(f"Trace      : {args.trace}")
    print(f"Output     : {args.output}\n")

    with open(csv_path, "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_columns, extrasaction="ignore")
        writer.writeheader()

        for i, sched_path in enumerate(scheduler_files, 1):
            print(f"[{i}/{len(scheduler_files)}] {sched_path.name}")
            try:
                if args.dry_run:
                    scale_results = {s: {"ok": True, "latency": round(random.Random(i).uniform(0.05, 2.0), 4)} for s in scales}
                    for s, r in scale_results.items():
                        print(f"  scale={s}x  {r['latency']:.4f}s  (dry-run)")
                else:
                    scale_results = evaluate_across_scales(
                        sched_path.resolve(), str(args.trace.resolve()), scales, base_params,
                    )

                valid = [r["latency"] for r in scale_results.values() if r.get("ok")]
                gm = geometric_mean(valid) if valid else float("nan")
                print(f"  geomean: {gm:.4f}s")
                row = {"scheduler": sched_path.name, "geomean_latency": round(gm, 4)}
                for s in scales:
                    r = scale_results.get(s, {})
                    row[f"latency_{s}x"] = round(r["latency"], 4) if r.get("ok") else "FAILED"

                if args.dry_run:
                    mock_schedulers = sorted(MOCK_DATA_DIR.glob("scheduler_*.py"))
                    assert mock_schedulers, f"No scheduler_*.py in {MOCK_DATA_DIR}"
                    improved_code = mock_schedulers[0].read_text()
                else:
                    prompt = build_improvement_prompt(sched_path.read_text(), scale_results, base_params)
                    improved_code = extract_code(call_llm(prompt, args.model, args.effort, args.verbose))

                out_path = args.output / f"{sched_path.stem}_iter1.py"
                out_path.write_text(improved_code + "\n")
                print(f"  -> {out_path.name}")
            except Exception as exc:
                print(f"  ERROR: {exc}")
                row = {"scheduler": sched_path.name, "geomean_latency": "FAILED"}
                for s in scales:
                    row[f"latency_{s}x"] = "FAILED"

            writer.writerow(row)
            csv_file.flush()

    print(f"\nResults written to {csv_path}")


if __name__ == "__main__":
    main()
