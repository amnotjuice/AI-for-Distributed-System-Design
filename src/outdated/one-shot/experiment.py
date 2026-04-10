#!/usr/bin/env python3
"""Generate a dataset of one-shot schedulers for a given reasoning effort.

Usage:
    python experiment.py --effort low --n 20
    python experiment.py --effort medium --n 20
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

os.environ.setdefault("LITELLM_LOG", "ERROR")
import logging
for name in ["eudoxia", "LiteLLM", "httpcore", "httpx", "openai._base_client"]:
    logging.getLogger(name).setLevel(logging.WARNING)

from config import (
    SUPPORTED_EFFORTS,
    ONE_SHOT_DIR,
    PROJECT_ROOT,
    get_canonical_base_params,
)
from simulation_utils import generate_traces
from llm import generate_policy, setup_cost_tracking, reset_cost_tracking, get_cost_statistics, get_last_request_cost
from prompts import get_user_request


def ensure_traces(
    traces_dir: Path,
    base_params: dict,
    n_traces: int = 5,
    traces_per_batch: int = 2,
) -> list[str]:
    """Generate traces if needed, otherwise reuse existing canonical traces."""
    expected_count = n_traces * traces_per_batch
    existing = sorted(traces_dir.glob("*.csv"))
    if len(existing) == expected_count:
        print(f"Reusing {len(existing)} existing traces")
        return [str(p) for p in existing]
    if existing:
        print(
            f"Existing traces count mismatch ({len(existing)} != {expected_count}); "
            "regenerating traces"
        )

    traces_dir.mkdir(parents=True, exist_ok=True)
    for p in traces_dir.glob("*.csv"):
        p.unlink(missing_ok=True)

    trace_files = []
    for batch_idx in range(n_traces):
        scale = 2 ** batch_idx
        bp = base_params.copy()
        bp["num_pipelines"] = base_params["num_pipelines"] * scale
        bp["waiting_seconds_mean"] = base_params["waiting_seconds_mean"] * scale
        prefix = str(traces_dir / f"trace_scale{scale}x_{base_params['duration']}s")
        trace_files.extend(generate_traces(k=traces_per_batch, base_params=bp, file_name_prefix=prefix))
    print(f"Generated {len(trace_files)} trace files")
    return trace_files


def generate_one_scheduler(
    scheduler_dir: Path, index: int, effort: str, model: str, metric: str, verbose: bool
) -> Path | None:
    """Generate a single scheduler .py file via LLM."""
    policy_key = f"scheduler_{effort}_{index:03d}"
    output_path = scheduler_dir / f"{policy_key}.py"
    if output_path.exists():
        print(f"  {output_path.name} exists, skipping")
        return output_path

    gen_start = time.time()
    try:
        result = generate_policy(
            user_request=get_user_request(policy_key, metric),
            feedback_history=[], model=model, temperature=1.0,
            policy_key=policy_key, verbose=verbose, reasoning_effort_override=effort,
        )
        code = result["policy_code"]
        if not code or not code.strip():
            print(f"  FAIL {policy_key}: empty code")
            _save_failure(scheduler_dir, policy_key, "empty_code", None, None)
            return None
    except Exception as exc:
        print(f"  FAIL {policy_key}: {exc}")
        _save_failure(scheduler_dir, policy_key, str(exc), None, None)
        return None

    cost = get_last_request_cost()
    secs = time.time() - gen_start
    header = "\n".join([
        f"# policy_key: {policy_key}",
        f"# reasoning_effort: {effort}",
        f"# model: {model}",
        f"# llm_cost: {cost:.6f}",
        f"# generation_seconds: {secs:.2f}",
        f"# generated_at: {datetime.now().isoformat()}",
        "",
    ])
    with open(output_path, "w") as f:
        f.write(header + code + "\n")
    print(f"  OK {output_path.name}  (${cost:.4f}, {secs:.1f}s)")
    return output_path


def _save_failure(scheduler_dir: Path, policy_key: str, error: str, code: str | None, llm_params: dict | None):
    """Persist generation failure for later analysis."""
    fail_dir = scheduler_dir / "failures"
    fail_dir.mkdir(exist_ok=True)
    record = {"policy_key": policy_key, "error": error, "timestamp": datetime.now().isoformat()}
    if llm_params:
        record["llm_params"] = llm_params
    with open(fail_dir / f"{policy_key}.json", "w") as f:
        json.dump(record, f, indent=2)
    if code:
        with open(fail_dir / f"{policy_key}.py", "w") as f:
            f.write(code)


def main():
    load_dotenv()
    assert os.environ.get("OPENAI_API_KEY"), "OPENAI_API_KEY not set"

    # llm.py uses relative paths like "src/markdown/", so we need project root as cwd
    os.chdir(PROJECT_ROOT)

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--effort", required=True, choices=SUPPORTED_EFFORTS)
    parser.add_argument("--n", type=int, default=20)
    parser.add_argument("--model", default="gpt-5.2-2025-12-11")
    parser.add_argument("--metric", choices=["latency", "throughput"], default="latency")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    base_params = get_canonical_base_params()
    trace_files = ensure_traces(ONE_SHOT_DIR / "traces", base_params)

    scheduler_dir = ONE_SHOT_DIR / f"schedulers-{args.effort}"
    scheduler_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n{scheduler_dir.name}/  |  effort={args.effort}  n={args.n}  model={args.model}")

    setup_cost_tracking(); reset_cost_tracking()
    generated = []
    for i in range(1, args.n + 1):
        print(f"\n[{i}/{args.n}]")
        p = generate_one_scheduler(scheduler_dir, i, args.effort, args.model, args.metric, args.verbose)
        if p: generated.append(p)

    stats = get_cost_statistics()
    print(f"\n{'='*50}")
    print(f"DONE: {len(generated)}/{args.n} schedulers  |  cost=${stats['total_cost']:.4f}")


if __name__ == "__main__":
    main()
