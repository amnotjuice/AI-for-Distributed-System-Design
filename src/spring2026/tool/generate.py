#!/usr/bin/env python3
"""Generate one-shot schedulers for spring2026 experiments.

Usage:
    python generate.py --effort low --n 50
    python generate.py --effort high --n 50 --model gpt-5.2-2025-12-11
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics as _statistics
import sys
import time
from datetime import datetime
from pathlib import Path

_SRC = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_SRC))

from dotenv import load_dotenv
load_dotenv(_SRC / ".env")

os.environ.setdefault("LITELLM_LOG", "ERROR")
import logging
for name in ["eudoxia", "LiteLLM", "httpcore", "httpx", "openai._base_client"]:
    logging.getLogger(name).setLevel(logging.WARNING)

# CWD must be project root for build_system_context (reads src/markdown/)
os.chdir(_SRC.parent)

from llm import generate_policy, setup_cost_tracking, reset_cost_tracking, get_cost_statistics, get_last_request_cost
from prompts import get_user_request_v2, get_user_request_v2_est
from spring2026.tool.config import DEFAULT_MODEL, RESULTS_DIR, SCHEDULERS_DIR, SUPPORTED_EFFORTS


def generate_one_scheduler(
    scheduler_dir: Path,
    index: int,
    effort: str,
    model: str,
    verbose: bool,
    exp: str = "reasoning",
) -> Path | None:
    """Generate a single scheduler .py file via LLM."""
    policy_key = f"scheduler_{effort}_{index:03d}" if exp == "reasoning" else f"scheduler_est_{index:03d}"
    output_path = scheduler_dir / f"{policy_key}.py"
    if output_path.exists():
        print(f"  {output_path.name} exists, skipping")
        return output_path

    if exp == "estimation":
        user_request = get_user_request_v2_est(policy_key)
        context_files = ["eudoxia_bauplan_est.md"]
    else:
        user_request = get_user_request_v2(policy_key)
        context_files = ["eudoxia_bauplan.md"]

    gen_start = time.time()
    try:
        result = generate_policy(
            user_request=user_request,
            feedback_history=[],
            model=model,
            temperature=1.0,
            policy_key=policy_key,
            verbose=verbose,
            reasoning_effort_override=effort,
            context_files=context_files,
        )
        code = result["policy_code"]
        if not code or not code.strip():
            print(f"  FAIL {policy_key}: empty code")
            _save_failure(scheduler_dir, policy_key, "empty_code")
            return None
    except Exception as exc:
        print(f"  FAIL {policy_key}: {exc}")
        _save_failure(scheduler_dir, policy_key, str(exc))
        return None

    cost = get_last_request_cost()
    secs = time.time() - gen_start
    header = "\n".join([
        f"# policy_key: {policy_key}",
        f"# reasoning_effort: {effort}",
        f"# exp: {exp}",
        f"# model: {model}",
        f"# llm_cost: {cost:.6f}",
        f"# generation_seconds: {secs:.2f}",
        f"# generated_at: {datetime.now().isoformat()}",
        "",
    ])
    output_path.write_text(header + code + "\n")
    print(f"  OK {output_path.name}  (${cost:.4f}, {secs:.1f}s)")
    return output_path


def _save_failure(scheduler_dir: Path, policy_key: str, error: str) -> None:
    fail_dir = scheduler_dir / "failures"
    fail_dir.mkdir(exist_ok=True)
    record = {"policy_key": policy_key, "error": error, "timestamp": datetime.now().isoformat()}
    (fail_dir / f"{policy_key}.json").write_text(json.dumps(record, indent=2))


# ---------------------------------------------------------------------------
# Two-iteration experiment helpers
# ---------------------------------------------------------------------------

def _select_source_scheduler(source: str) -> tuple:
    """Select best/worst/median scheduler from 01_reasoning/low results.

    Returns (record_dict, scheduler_path).
    """
    analysis_path = RESULTS_DIR / "01_reasoning" / "low" / "analysis.jsonl"
    assert analysis_path.exists(), (
        f"No results at {analysis_path}. Run: python analyze.py 01_reasoning"
    )
    records = []
    with open(analysis_path) as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    functional = [r for r in records if r.get("functional") and r.get("median_latency") is not None]
    assert functional, "No functional schedulers in 01_reasoning/low results"

    if source == "best":
        chosen = min(functional, key=lambda r: r["median_latency"])
    elif source == "worst":
        chosen = max(functional, key=lambda r: r["median_latency"])
    else:  # median
        med_val = _statistics.median(r["median_latency"] for r in functional)
        chosen = min(functional, key=lambda r: abs(r["median_latency"] - med_val))

    sched_path = SCHEDULERS_DIR / "reasoning" / "low" / chosen["filename"]
    assert sched_path.exists(), f"Scheduler file not found: {sched_path}"
    return chosen, sched_path


def _get_rich_stats(scheduler_code: str, policy_key: str) -> list:
    """Run source scheduler on canonical_train trace and return stats as dicts."""
    from typing import List, Tuple
    from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
    from eudoxia.scheduler.decorators import register_scheduler, register_scheduler_init
    from eudoxia.utils import Priority
    from eudoxia.workload import OperatorState, Pipeline
    from eudoxia.workload.runtime_status import ASSIGNABLE_STATES
    from spring2026.tool.config import TRACES_DIR, get_canonical_base_params
    from simulation_utils import get_raw_stats_for_policy

    exec(scheduler_code, {
        "__builtins__": __builtins__, "List": List, "Tuple": Tuple,
        "Pipeline": Pipeline, "OperatorState": OperatorState,
        "ASSIGNABLE_STATES": ASSIGNABLE_STATES, "Assignment": Assignment,
        "ExecutionResult": ExecutionResult, "Suspend": Suspend,
        "register_scheduler_init": register_scheduler_init,
        "register_scheduler": register_scheduler, "Priority": Priority,
    })

    base_params = get_canonical_base_params()
    canonical = str(TRACES_DIR / "bench_canonical_train.csv")
    raw = get_raw_stats_for_policy(base_params, [canonical], policy_key)
    return [s.to_dict() for s in raw]


def generate_two_iter_scheduler(
    source_record: dict,
    source_code: str,
    context: str,
    policy_key: str,
    output_dir: Path,
    model: str,
    verbose: bool,
    raw_stats_dicts: list | None,
) -> Path | None:
    """Generate one two-iteration scheduler from the source + feedback."""
    output_path = output_dir / f"{policy_key}.py"
    if output_path.exists():
        print(f"  {output_path.name} exists, skipping")
        return output_path

    source_latency = source_record.get("median_latency", 0)
    if context == "simple":
        feedback_text = (
            f"This scheduler achieved a median weighted latency of {source_latency:.2f}s. "
            f"Please improve it to reduce latency further."
        )
    else:  # rich
        stats_json = json.dumps(raw_stats_dicts, indent=2) if raw_stats_dicts else "[]"
        feedback_text = (
            f"This scheduler achieved a median weighted latency of {source_latency:.2f}s.\n\n"
            f"Here are the per-trace simulation statistics:\n{stats_json}\n\n"
            f"Please improve it to reduce latency further."
        )

    feedback_history = [{"policy_code": source_code, "feedback": feedback_text}]

    gen_start = time.time()
    try:
        result = generate_policy(
            user_request=get_user_request_v2(policy_key),
            feedback_history=feedback_history,
            model=model,
            temperature=1.0,
            policy_key=policy_key,
            verbose=verbose,
            reasoning_effort_override="low",
        )
        code = result["policy_code"]
        if not code or not code.strip():
            print(f"  FAIL {policy_key}: empty code")
            _save_failure(output_dir, policy_key, "empty_code")
            return None
    except Exception as exc:
        print(f"  FAIL {policy_key}: {exc}")
        _save_failure(output_dir, policy_key, str(exc))
        return None

    cost = get_last_request_cost()
    secs = time.time() - gen_start
    header = "\n".join([
        f"# policy_key: {policy_key}",
        f"# context: {context}",
        f"# model: {model}",
        f"# llm_cost: {cost:.6f}",
        f"# generation_seconds: {secs:.2f}",
        f"# generated_at: {datetime.now().isoformat()}",
        "",
    ])
    output_path.write_text(header + code + "\n")
    print(f"  OK {output_path.name}  (${cost:.4f}, {secs:.1f}s)")
    return output_path


def _run_two_iter(args) -> None:
    """Run two-iteration scheduler generation."""
    source_record, source_path = _select_source_scheduler(args.source)
    source_code = source_path.read_text()
    source_latency = source_record["median_latency"]

    print(f"\nSource: {source_path.name}  |  median_latency={source_latency:.4f}")
    print(f"context={args.context}  n={args.n}  model={args.model}")

    raw_stats_dicts = None
    if args.context == "rich":
        key_match = re.search(r"""@register_scheduler\((?:key=)?['"]([^'"]+)['"]\)""", source_code)
        assert key_match, "No scheduler key found in source code"
        print("Running simulation for rich context stats...")
        raw_stats_dicts = _get_rich_stats(source_code, key_match.group(1))
        print(f"  Got {len(raw_stats_dicts)} trace stats")

    combo = f"{args.source}_{args.context}"
    output_dir = SCHEDULERS_DIR / "two_iter" / combo
    output_dir.mkdir(parents=True, exist_ok=True)

    meta = {
        "source": args.source,
        "context": args.context,
        "source_filename": source_path.name,
        "source_median_latency": source_latency,
        "model": args.model,
        "n": args.n,
    }
    (output_dir / "meta.json").write_text(json.dumps(meta, indent=2))

    setup_cost_tracking()
    reset_cost_tracking()

    generated = []
    for i in range(1, args.n + 1):
        policy_key = f"scheduler_iter_{args.source}_{args.context}_{i:03d}"
        print(f"\n[{i}/{args.n}]")
        p = generate_two_iter_scheduler(
            source_record=source_record,
            source_code=source_code,
            context=args.context,
            policy_key=policy_key,
            output_dir=output_dir,
            model=args.model,
            verbose=args.verbose,
            raw_stats_dicts=raw_stats_dicts,
        )
        if p:
            generated.append(p)

    stats = get_cost_statistics()
    print(f"\n{'='*50}")
    print(f"DONE: {len(generated)}/{args.n} schedulers  |  cost=${stats['total_cost']:.4f}")
    print(f"Output: {output_dir}")


def main() -> None:
    assert os.environ.get("OPENAI_API_KEY"), "OPENAI_API_KEY not set"

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--exp", default="reasoning", choices=["reasoning", "estimation", "two_iter"],
                        help="Experiment type (default: reasoning)")
    parser.add_argument("--effort", default="medium", choices=SUPPORTED_EFFORTS,
                        help="Reasoning effort level (default: medium)")
    parser.add_argument("--n", type=int, default=50)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--verbose", "-v", action="store_true")
    # two_iter only
    parser.add_argument("--source", default="best", choices=["best", "worst", "median"],
                        help="Source scheduler for two_iter (default: best)")
    parser.add_argument("--context", default="simple", choices=["simple", "rich"],
                        help="Feedback context for two_iter (default: simple)")
    args = parser.parse_args()

    if args.exp == "two_iter":
        _run_two_iter(args)
        return

    if args.exp == "estimation":
        scheduler_dir = SCHEDULERS_DIR / "estimation"
    else:
        scheduler_dir = SCHEDULERS_DIR / "reasoning" / args.effort
    scheduler_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n{scheduler_dir}  |  exp={args.exp}  effort={args.effort}  n={args.n}  model={args.model}")

    setup_cost_tracking()
    reset_cost_tracking()

    generated = []
    for i in range(1, args.n + 1):
        print(f"\n[{i}/{args.n}]")
        p = generate_one_scheduler(scheduler_dir, i, args.effort, args.model, args.verbose, exp=args.exp)
        if p:
            generated.append(p)

    stats = get_cost_statistics()
    print(f"\n{'='*50}")
    print(f"DONE: {len(generated)}/{args.n} schedulers  |  cost=${stats['total_cost']:.4f}")


if __name__ == "__main__":
    main()
