#!/usr/bin/env python3
"""Generate one-shot schedulers with estimator-aware prompt.

Schedulers are generated with op.estimate.mem_peak_gb documented in the
system context, so the LLM can write RAM-aware scheduling policies.

Usage:
    python generate.py --n 50
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
SRC_DIR = THIS_DIR.parent
sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, str(SRC_DIR / "one-shot"))

from dotenv import load_dotenv

os.environ.setdefault("LITELLM_LOG", "ERROR")
import logging
for name in ["eudoxia", "LiteLLM", "httpcore", "httpx", "openai._base_client"]:
    logging.getLogger(name).setLevel(logging.WARNING)

from config import ONE_SHOT_DIR, PROJECT_ROOT, get_canonical_base_params
from simulation_utils import generate_traces
from llm import generate_policy, setup_cost_tracking, reset_cost_tracking, get_cost_statistics, get_last_request_cost
from prompts import get_user_request


def ensure_traces(traces_dir: Path, base_params: dict, n_traces: int = 5, traces_per_batch: int = 2) -> list[str]:
    expected_count = n_traces * traces_per_batch
    existing = sorted(traces_dir.glob("*.csv"))
    if len(existing) == expected_count:
        print(f"Reusing {len(existing)} existing traces")
        return [str(p) for p in existing]
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

MODEL = "gpt-5.2-2025-12-11"


def generate_one_scheduler(index: int, verbose: bool, effort: str, scheduler_dir: Path) -> Path | None:
    policy_key = f"scheduler_est_{index:03d}"
    output_path = scheduler_dir / f"{policy_key}.py"
    if output_path.exists():
        print(f"  {output_path.name} exists, skipping")
        return output_path

    gen_start = time.time()
    try:
        result = generate_policy(
            user_request=get_user_request(policy_key, "latency"),
            feedback_history=[], model=MODEL, temperature=1.0,
            policy_key=policy_key, verbose=verbose,
            reasoning_effort_override=effort,
        )
        code = result["policy_code"]
        if not code or not code.strip():
            print(f"  FAIL {policy_key}: empty code")
            return None
    except Exception as exc:
        print(f"  FAIL {policy_key}: {exc}")
        return None

    cost = get_last_request_cost()
    secs = time.time() - gen_start
    header = "\n".join([
        f"# policy_key: {policy_key}",
        f"# reasoning_effort: {effort}",
        f"# model: {MODEL}",
f"# llm_cost: {cost:.6f}",
        f"# generation_seconds: {secs:.2f}",
        f"# generated_at: {datetime.now().isoformat()}",
        "",
    ])
    with open(output_path, "w") as f:
        f.write(header + code + "\n")
    print(f"  OK {output_path.name}  (${cost:.4f}, {secs:.1f}s)")
    return output_path


def main():
    load_dotenv(SRC_DIR / ".env")
    assert os.environ.get("OPENAI_API_KEY"), "OPENAI_API_KEY not set"
    os.chdir(PROJECT_ROOT)

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=50)
    parser.add_argument("--effort", type=str, default="high", choices=["none", "low", "medium", "high"])
    parser.add_argument("--scheduler-dir", type=Path, default=None,
                        help="Output directory for schedulers (default: schedulers-{effort})")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    scheduler_dir = args.scheduler_dir or (THIS_DIR / f"schedulers-{args.effort}")

    base_params = get_canonical_base_params()
    ensure_traces(ONE_SHOT_DIR / "traces", base_params)

    scheduler_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n{scheduler_dir.relative_to(PROJECT_ROOT)}  |  effort={args.effort}  n={args.n}  model={MODEL}")

    setup_cost_tracking(); reset_cost_tracking()
    generated = []
    for i in range(1, args.n + 1):
        print(f"\n[{i}/{args.n}]")
        p = generate_one_scheduler(i, args.verbose, args.effort, scheduler_dir)
        if p:
            generated.append(p)

    stats = get_cost_statistics()
    print(f"\n{'='*50}")
    print(f"DONE: {len(generated)}/{args.n} schedulers  |  cost=${stats['total_cost']:.4f}")


if __name__ == "__main__":
    main()
