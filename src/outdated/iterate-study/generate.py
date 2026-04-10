#!/usr/bin/env python3
"""Generate N improved schedulers from the best one-shot result.

Research question: If you take the best scheduler from iteration 1,
then try N times to improve it with AI, what % of the time do you
get a better scheduler?

Usage:
    python generate.py --effort none --n 10
    python generate.py --effort high --n 20 --model claude-opus-4-20250514

Then evaluate:
    cd ../one-shot
    python analyze.py ../iterate-study/schedulers-none/ --experiment iterate
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Paths
ITERATE_DIR = Path(__file__).resolve().parent
SRC_DIR = ITERATE_DIR.parent
PROJECT_ROOT = SRC_DIR.parent
ONE_SHOT_DIR = SRC_DIR / "one-shot"

# CWD must be project root for build_system_context (reads src/markdown/)
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(SRC_DIR))
# Load API keys from src/.env
from dotenv import load_dotenv
load_dotenv(SRC_DIR / ".env")

os.environ["LITELLM_LOG"] = "ERROR"
import logging
logging.getLogger("LiteLLM").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

from tool.iterate_once import iterate_once
sys.path.insert(0, str(ONE_SHOT_DIR))
from simulation_utils import get_raw_stats_for_policy
from config import get_canonical_base_params


def find_best_scheduler(effort: str, metric: str = "latency") -> dict:
    """Find the best functional scheduler from one-shot analysis results."""
    return _load_schedulers(effort, metric, selection="best")


def find_avg_scheduler(effort: str, metric: str = "latency") -> dict:
    """Find the median functional scheduler from one-shot analysis results."""
    return _load_schedulers(effort, metric, selection="avg")


def _load_schedulers(effort: str, metric: str, selection: str) -> dict:
    analysis_path = ONE_SHOT_DIR / "output" / f"schedulers-{effort}" / "analysis.jsonl"
    assert analysis_path.exists(), f"No one-shot results at {analysis_path}"

    records = []
    with open(analysis_path) as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    functional = [r for r in records if r["functional"]]
    assert functional, f"No functional schedulers for effort={effort}"

    if selection == "best":
        if metric == "latency":
            return min(functional, key=lambda r: r[f"median_{metric}"])
        else:
            return max(functional, key=lambda r: r[f"median_{metric}"])
    elif selection == "avg":
        import statistics
        sorted_func = sorted(functional, key=lambda r: r[f"median_{metric}"])
        med_val = statistics.median(r[f"median_{metric}"] for r in sorted_func)
        return min(sorted_func, key=lambda r: abs(r[f"median_{metric}"] - med_val))


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--effort", required=True, choices=["none", "low", "medium", "high"])
    parser.add_argument("--n", type=int, default=10, help="Number of improvement attempts")
    parser.add_argument("--metric", default="latency", choices=["latency", "throughput"])
    parser.add_argument("--model", default="gpt-5.2-2025-12-11")
    parser.add_argument("--feedback", default="minimal", choices=["minimal", "rich"],
                        help="Feedback mode: minimal (main.py style) or rich (per-trace + baseline)")
    parser.add_argument("--subdir", default=None,
                        help="Output subdirectory name (default: two-shot or two-shot-rich)")
    parser.add_argument("--selection", default="best", choices=["best", "avg"],
                        help="Which one-shot scheduler to use as starting point: best or median (avg)")
    args = parser.parse_args()

    # Find starting one-shot scheduler
    if args.selection == "avg":
        best = find_avg_scheduler(args.effort, args.metric)
    else:
        best = find_best_scheduler(args.effort, args.metric)
    scheduler_path = ONE_SHOT_DIR / f"schedulers-{args.effort}" / best["filename"]
    scheduler_code = scheduler_path.read_text()

    print(f"Best one-shot scheduler: {best['filename']}")
    print(f"  median_{args.metric}: {best[f'median_{args.metric}']:.4f}")
    print(f"  beats_baseline: {best.get('beats_baseline')}")
    print(f"  model: {args.model}")
    print(f"  feedback: {args.feedback}")
    print(f"  attempts: {args.n}")

    # For rich feedback, re-run simulation to get per-trace SimulatorStats
    raw_stats_dicts = None
    if args.feedback == "rich":
        import re
        base_params = get_canonical_base_params()
        traces_dir = ONE_SHOT_DIR / "traces"
        trace_files = sorted(str(p) for p in traces_dir.glob("*.csv"))
        key_match = re.search(r"""@register_scheduler\((?:key=)?['"]([^'"]+)['"]\)""", scheduler_code)
        assert key_match, "No scheduler key found in source code"

        # exec the scheduler so it's registered
        from typing import List, Tuple
        from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
        from eudoxia.scheduler.decorators import register_scheduler, register_scheduler_init
        from eudoxia.utils import Priority
        from eudoxia.workload import OperatorState, Pipeline
        from eudoxia.workload.runtime_status import ASSIGNABLE_STATES
        exec(scheduler_code, {
            "__builtins__": __builtins__, "List": List, "Tuple": Tuple,
            "Pipeline": Pipeline, "OperatorState": OperatorState,
            "ASSIGNABLE_STATES": ASSIGNABLE_STATES,
            "Assignment": Assignment, "ExecutionResult": ExecutionResult,
            "Suspend": Suspend, "register_scheduler_init": register_scheduler_init,
            "register_scheduler": register_scheduler, "Priority": Priority,
        })

        print("Running simulation to collect raw stats...")
        raw_stats = get_raw_stats_for_policy(base_params, trace_files, key_match.group(1))
        raw_stats_dicts = [s.to_dict() for s in raw_stats]
        print(f"  Got stats for {len(raw_stats_dicts)} traces")

    # Output directory
    subdir = args.subdir or ("two-shot-rich" if args.feedback == "rich" else "two-shot")
    output_dir = ITERATE_DIR / subdir / "schedulers"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save metadata for report.py
    meta = {
        "source_effort": args.effort,
        "source_scheduler": best["filename"],
        f"source_median_{args.metric}": best[f"median_{args.metric}"],
        "source_results": best,
        "model": args.model,
        "metric": args.metric,
        "feedback_mode": args.feedback,
        "n": args.n,
    }
    (output_dir / "meta.json").write_text(json.dumps(meta, indent=2))

    # Generate N improved versions
    orig_num = best["filename"].replace(f"scheduler_{args.effort}_", "").replace(".py", "")
    for i in range(1, args.n + 1):
        policy_key = f"scheduler_{args.effort}_{orig_num}_r{i}"
        out_file = output_dir / f"{policy_key}.py"

        if out_file.exists():
            print(f"[{i}/{args.n}] {policy_key}  already exists, skipping")
            continue

        print(f"[{i}/{args.n}] Generating {policy_key}...")
        try:
            new_code = iterate_once(
                scheduler_code=scheduler_code,
                simulation_results=best,
                policy_key=policy_key,
                metric=args.metric,
                model=args.model,
                feedback_mode=args.feedback,
                raw_stats=raw_stats_dicts,
            )

            # Add header metadata
            header = (
                f"# source: {best['filename']}\n"
                f"# model: {args.model}\n"
                f"# effort: {args.effort}\n"
                f"# feedback: {args.feedback}\n"
                f"# iteration: r{i}\n"
            )
            if not new_code.startswith("#"):
                new_code = header + new_code

            out_file.write_text(new_code)
            print(f"  Saved: {out_file.name}")
        except Exception as e:
            print(f"  FAILED: {e}")

    eval_output = ITERATE_DIR / subdir / "output"
    print(f"\nGenerated schedulers in {output_dir}")
    print(f"Evaluate with:")
    print(f"  cd {ONE_SHOT_DIR}")
    print(f"  python analyze.py {output_dir} --output-dir {eval_output}")


if __name__ == "__main__":
    main()
