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
from prompts import get_user_request
from spring2026.tool.config import DEFAULT_MODEL, RESULTS_DIR, SCHEDULERS_DIR, SUPPORTED_EFFORTS, TWO_SHOT_PERF_CONDITIONS


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

    user_request = get_user_request(policy_key, "weighted latency")
    if exp == "estimation":
        context_files = ["eudoxia_bauplan_est.md"]
    else:
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
    seen: dict[str, dict] = {}
    with open(analysis_path) as f:
        for line in f:
            if line.strip():
                r = json.loads(line)
                if fn := r.get("filename"):
                    seen[fn] = r
    records = list(seen.values())

    functional = [r for r in records if r.get("functional") and r.get("median_latency") is not None]
    assert functional, "No functional schedulers in 01_reasoning/low results"

    def _gmean(r):
        finite = [v for v in r.get("metric_values", []) if v != float('inf') and v > 0]
        return _statistics.geometric_mean(finite) if finite else float('inf')

    if source == "best":
        chosen = min(functional, key=_gmean)
    elif source == "worst":
        chosen = max(functional, key=_gmean)
    else:  # median
        med_val = _statistics.median(_gmean(r) for r in functional)
        chosen = min(functional, key=lambda r: abs(_gmean(r) - med_val))

    sched_path = SCHEDULERS_DIR / "reasoning" / "low" / chosen["filename"]
    assert sched_path.exists(), f"Scheduler file not found: {sched_path}"
    return chosen, sched_path


def generate_two_iter_scheduler(
    source_record: dict,
    source_code: str,
    context: str,
    policy_key: str,
    output_dir: Path,
    model: str,
    verbose: bool,
    source_stats: dict | None,
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
        stats_json = json.dumps(source_stats, indent=2) if source_stats else "{}"
        feedback_text = (
            f"This scheduler achieved a median weighted latency of {source_latency:.2f}s.\n\n"
            f"Here are the simulation statistics:\n{stats_json}\n\n"
            f"Please improve it to reduce latency further."
        )

    feedback_history = [{"policy_code": source_code, "feedback": feedback_text}]

    gen_start = time.time()
    try:
        result = generate_policy(
            user_request=get_user_request(policy_key, "weighted latency"),
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



def generate_two_shot_perf_scheduler(
    source_code: str,
    cheap_latency: float,
    sim_label: str,
    policy_key: str,
    output_dir: Path,
    model: str,
    verbose: bool,
    context: str = "simple",
    cheap_stats: dict | None = None,
) -> Path | None:
    """Generate one two-shot-perf scheduler using feedback from a cheap simulation."""
    output_path = output_dir / f"{policy_key}.py"
    if output_path.exists():
        print(f"  {output_path.name} exists, skipping")
        return output_path

    if context == "rich" and cheap_stats:
        stats_json = json.dumps(cheap_stats, indent=2)
        feedback_text = (
            f"This scheduler achieved a median weighted latency of {cheap_latency:.2f}s "
            f"under a coarser simulation ({sim_label}).\n\n"
            f"Simulation statistics:\n{stats_json}\n\n"
            f"Please improve it to reduce latency further."
        )
    else:
        feedback_text = (
            f"This scheduler achieved a median weighted latency of {cheap_latency:.2f}s. "
            f"Please improve it to reduce latency further."
        )
    feedback_history = [{"policy_code": source_code, "feedback": feedback_text}]

    gen_start = time.time()
    try:
        result = generate_policy(
            user_request=get_user_request(policy_key, "weighted latency"),
            feedback_history=feedback_history,
            model=model,
            temperature=1.0,
            policy_key=policy_key,
            verbose=verbose,
            reasoning_effort_override="low",
        )
        code = result["policy_code"]
        if not code or not code.strip():
            _save_failure(output_dir, policy_key, "empty_code")
            return None
    except Exception as exc:
        _save_failure(output_dir, policy_key, str(exc))
        return None

    cost = get_last_request_cost()
    secs = time.time() - gen_start
    header = "\n".join([
        f"# policy_key: {policy_key}",
        f"# sim_label: {sim_label}",
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


def _run_two_shot_perf(args) -> None:
    """Generate schedulers for exp05: two-shot with varied sim fidelity.

    Requires analyze.py 05_two_shot_perf source --source <source> to be run first.
    """
    assert args.sim_label in TWO_SHOT_PERF_CONDITIONS, (
        f"Unknown sim_label '{args.sim_label}'. Choices: {list(TWO_SHOT_PERF_CONDITIONS)}"
    )
    assert args.sim_label != "dur3600_ticks100", (
        "dur3600_ticks100 is full fidelity (same as exp01); no need to run exp05 for it."
    )

    # Read source info and cheap stats produced by analyze.py phase 'source'
    source_stats_dir = RESULTS_DIR / "05_two_shot_perf" / "source" / args.source
    source_info_path = source_stats_dir / "source_info.json"
    assert source_info_path.exists(), (
        f"Source stats not found. Run first:\n"
        f"  python analyze.py 05_two_shot_perf source --source {args.source}"
    )
    cheap_stats_path = source_stats_dir / f"{args.sim_label}.json"
    assert cheap_stats_path.exists(), (
        f"No cheap stats for {args.sim_label}. Run first:\n"
        f"  python analyze.py 05_two_shot_perf source --source {args.source}"
    )

    source_info = json.loads(source_info_path.read_text())
    cheap_stats = json.loads(cheap_stats_path.read_text())
    cheap_latency = cheap_stats.get("median_latency", float("inf"))

    source_filename = source_info["filename"]
    source_median_latency = source_info["full_sim_median_latency"]
    source_path = SCHEDULERS_DIR / "reasoning" / "low" / source_filename
    assert source_path.exists(), f"Source scheduler not found: {source_path}"
    source_code = source_path.read_text()

    print(f"Source: {source_filename}  full-sim latency={source_median_latency:.4f}")
    print(f"Cheap sim ({args.sim_label}): median_latency={cheap_latency:.4f}")
    print(f"context={args.context}  n={args.n}  model={args.model}")

    output_dir = SCHEDULERS_DIR / "two_shot_perf" / f"{args.source}_{args.context}" / args.sim_label
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "meta.json").write_text(json.dumps({
        "source": args.source,
        "context": args.context,
        "sim_label": args.sim_label,
        "sim_overrides": TWO_SHOT_PERF_CONDITIONS[args.sim_label],
        "source_filename": source_filename,
        "source_median_latency": source_median_latency,
        "source_cheap_latency": cheap_latency,
        "model": args.model,
        "n": args.n,
    }, indent=2))

    setup_cost_tracking()
    reset_cost_tracking()

    generated = []
    for i in range(1, args.n + 1):
        print(f"\n[{i}/{args.n}]")
        p = generate_two_shot_perf_scheduler(
            source_code=source_code,
            cheap_latency=cheap_latency,
            sim_label=args.sim_label,
            policy_key=f"scheduler_perf_{args.source}_{args.context}_{args.sim_label}_{i:03d}",
            output_dir=output_dir,
            model=args.model,
            verbose=args.verbose,
            context=args.context,
            cheap_stats=cheap_stats,
        )
        if p:
            generated.append(p)

    stats = get_cost_statistics()
    print(f"\n{'='*50}")
    print(f"DONE: {len(generated)}/{args.n} schedulers  |  cost=${stats['total_cost']:.4f}")
    print(f"Output: {output_dir}")


def _run_two_iter(args) -> None:
    """Run two-iteration scheduler generation."""
    source_record, source_path = _select_source_scheduler(args.source)
    source_code = source_path.read_text()
    source_latency = source_record["median_latency"]

    print(f"\nSource: {source_path.name}  |  median_latency={source_latency:.4f}")
    print(f"context={args.context}  n={args.n}  model={args.model}")

    source_stats = source_record if args.context == "rich" else None

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
            source_stats=source_stats,
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
    parser.add_argument("--exp", default="reasoning",
                        choices=["reasoning", "estimation", "two_iter", "two_shot_perf"],
                        help="Experiment type (default: reasoning)")
    parser.add_argument("--effort", default="low", choices=SUPPORTED_EFFORTS,
                        help="Reasoning effort level (default: low)")
    parser.add_argument("--n", type=int, default=50)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--verbose", "-v", action="store_true")
    # two_iter only
    parser.add_argument("--source", default="best", choices=["best", "worst", "median"],
                        help="Source scheduler for two_iter (default: best)")
    parser.add_argument("--context", default="simple", choices=["simple", "rich"],
                        help="Feedback context for two_iter (default: simple)")
    # two_shot_perf only
    parser.add_argument("--sim_label", default=None,
                        help=f"Sim condition for two_shot_perf. Choices: {list(TWO_SHOT_PERF_CONDITIONS)}")
    args = parser.parse_args()

    if args.exp == "two_iter":
        _run_two_iter(args)
        return

    if args.exp == "two_shot_perf":
        _run_two_shot_perf(args)
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
