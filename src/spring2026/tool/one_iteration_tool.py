#!/usr/bin/env python3
"""One-iteration LLM scheduler improvement tool.

Evaluates an existing scheduler across multiple cluster sizes (CPU/RAM scaling),
computes the geometric mean of adjusted latency, feeds the results back to an LLM,
and writes an improved scheduler.

Usage:
    python one_iteration_tool.py \\
        --scheduler schedulers-high/scheduler_high_001.py \\
        --trace traces/trace_scale1x_600s_0.csv

    python one_iteration_tool.py \\
        --scheduler sched.py --trace trace.csv \\
        --effort high --scales 1,2,4,8,16
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import subprocess
import sys
import textwrap
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

TOOLS_DIR = Path(__file__).resolve().parent
ONE_SHOT_DIR = TOOLS_DIR.parent
SPRING2026_DIR = ONE_SHOT_DIR.parent
WORKER_SCRIPT = TOOLS_DIR / "_one_iteration_worker.py"
MOCK_DATA_DIR = SPRING2026_DIR / "mock-data"

sys.path.insert(0, str(ONE_SHOT_DIR))
sys.path.insert(0, str(SPRING2026_DIR))

from dotenv import load_dotenv
from config import get_canonical_base_params, PROJECT_ROOT

os.environ.setdefault("LITELLM_LOG", "ERROR")
logging.getLogger("eudoxia").setLevel(logging.CRITICAL)
logging.getLogger("LiteLLM").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# Eudoxia system prompt
# ---------------------------------------------------------------------------

EUDOXIA_SYSTEM_PROMPT = textwrap.dedent("""

""").strip()


# ---------------------------------------------------------------------------
# Subprocess worker for isolated per-scale evaluation
# ---------------------------------------------------------------------------

def _run_worker_subprocess(scheduler_file: str, trace_file: str, params: dict) -> dict:
    """Evaluate one (scheduler, trace, params) triple in a subprocess for isolation."""
    cmd = [
        sys.executable,
        str(WORKER_SCRIPT),
        scheduler_file,
        trace_file,
        json.dumps(params),
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True, text=True,
            cwd=str(PROJECT_ROOT),
            timeout=params.get("subprocess_timeout", 120),
        )
        for line in reversed(proc.stdout.strip().split("\n")):
            if line.startswith("__RESULT__"):
                return json.loads(line[len("__RESULT__"):])
        stderr_snip = proc.stderr[-300:] if proc.stderr else "no stderr"
        return {"ok": False, "error": f"no result marker in output: {stderr_snip!r}"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "subprocess timeout"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def evaluate_across_scales(
    scheduler_path: Path,
    trace_file: str,
    scales: list[int],
    base_params: dict,
) -> dict[int, dict]:
    """Run the scheduler at each scale. Returns {scale: {ok, latency} or {ok, error}}."""
    results = {}
    for scale in scales:
        params = base_params.copy()
        params["cpus_per_pool"] = base_params["cpus_per_pool"] * scale
        params["ram_gb_per_pool"] = base_params["ram_gb_per_pool"] * scale

        cpus = params["cpus_per_pool"]
        ram = params["ram_gb_per_pool"]
        print(f"  scale={scale}x  ({cpus} CPUs, {ram} GB RAM)...", end=" ", flush=True)
        t0 = time.time()
        result = _run_worker_subprocess(str(scheduler_path), trace_file, params)
        elapsed = time.time() - t0

        if result.get("ok"):
            print(f"{result['latency']:.4f}s  ({elapsed:.1f}s)")
        else:
            print(f"FAILED: {result.get('error', '?')}  ({elapsed:.1f}s)")
        results[scale] = result
    return results


def geometric_mean(values: list[float]) -> float:
    if not values:
        return float("nan")
    return math.exp(sum(math.log(max(v, 1e-12)) for v in values) / len(values))


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------

def build_improvement_prompt(
    scheduler_code: str,
    scale_results: dict[int, dict],
    base_params: dict,
) -> str:
    rows = []
    valid_latencies = []
    for scale in sorted(scale_results):
        r = scale_results[scale]
        cpus = base_params["cpus_per_pool"] * scale
        ram = base_params["ram_gb_per_pool"] * scale
        if r.get("ok"):
            lat = r["latency"]
            valid_latencies.append(lat)
            rows.append(f"  {scale:2d}x  ({cpus:5d} CPUs, {ram:6d} GB RAM)  {lat:.4f}s")
        else:
            rows.append(
                f"  {scale:2d}x  ({cpus:5d} CPUs, {ram:6d} GB RAM)  FAILED: {r.get('error', '?')}"
            )

    perf_table = "\n".join(rows)
    n_valid = len(valid_latencies)
    n_total = len(scale_results)

    if valid_latencies:
        gm = geometric_mean(valid_latencies)
        geomean_line = f"Geometric mean across {n_valid}/{n_total} successful runs: {gm:.4f}s"
    else:
        geomean_line = "No successful runs (all failed)."

    return (
        f"I have evaluated the following scheduler on a fixed workload trace across "
        f"{n_total} cluster sizes. Cluster size is varied by scaling both CPUs and RAM "
        f"proportionally from a base of {base_params['cpus_per_pool']} CPUs / "
        f"{base_params['ram_gb_per_pool']} GB RAM.\n\n"
        f"== Performance Results (adjusted latency) ==\n"
        f"{perf_table}\n\n"
        f"{geomean_line}\n\n"
        f"== Current Scheduler Code ==\n"
        f"{scheduler_code}\n\n"
        f"Please analyze the performance results and produce an improved version of this scheduler.\n"
        f"Your goal is to reduce the geometric mean of adjusted latency across all cluster sizes.\n"
        f"Use the same @register_scheduler key as the original. Output ONLY the complete Python code."
    )


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------

def call_llm(prompt: str, model: str, effort: str, verbose: bool) -> str:
    import litellm

    kwargs: dict = {
        "model": model,
        "messages": [
            {"role": "system", "content": EUDOXIA_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "temperature": 1.0,
    }
    if effort and effort != "none":
        kwargs["reasoning_effort"] = effort

    if verbose:
        print(f"[LLM] model={model}  effort={effort}  prompt_chars={len(prompt)}")

    t0 = time.time()
    response = litellm.completion(**kwargs)
    elapsed = time.time() - t0

    content = response.choices[0].message.content or ""
    if verbose:
        print(f"[LLM] response: {len(content)} chars in {elapsed:.1f}s")

    return content


def extract_code(text: str) -> str:
    """Strip markdown code fences if present; otherwise return as-is."""
    m = re.search(r"```(?:python)?\n(.*?)```", text, re.DOTALL)
    return m.group(1).strip() if m else text.strip()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--scheduler", type=Path, required=True,
                        help="Scheduler .py file to improve.")
    parser.add_argument("--trace", type=Path, required=True,
                        help="Fixed trace file (.csv) to evaluate on.")
    parser.add_argument("--output", type=Path, default=None,
                        help="Output path for improved scheduler. "
                             "Default: scheduler_iter/<stem>_iter1.py")
    parser.add_argument("--scales", default="1,2,4,8,16",
                        help="Comma-separated cluster scale multipliers (default: 1,2,4,8,16).")
    parser.add_argument("--model", default="gpt-5.2-2025-12-11",
                        help="LLM model (default: gpt-5.2-2025-12-11).")
    parser.add_argument("--effort", default="high", choices=["none", "low", "medium", "high"],
                        help="Reasoning effort level (default: high).")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip simulator and LLM; use random latencies and copy input scheduler as output.")
    args = parser.parse_args()

    assert args.scheduler.exists(), f"Scheduler not found: {args.scheduler}"
    assert args.trace.exists(), f"Trace not found: {args.trace}"
    if not args.dry_run:
        assert os.environ.get("OPENAI_API_KEY"), "OPENAI_API_KEY not set"

    scales = [int(s.strip()) for s in args.scales.split(",")]
    assert scales, "Need at least one scale."

    output_path = args.output or (ONE_SHOT_DIR / "scheduler_iter" / f"{args.scheduler.stem}_iter1.py")

    base_params = get_canonical_base_params()

    print(f"Scheduler : {args.scheduler}")
    print(f"Trace     : {args.trace}")
    print(f"Scales    : {scales}  (base {base_params['cpus_per_pool']} CPUs / {base_params['ram_gb_per_pool']} GB)")
    print(f"Output    : {output_path}")
    print()

    # 1. Evaluate across scales
    print("=== Evaluating across cluster sizes ===")
    if args.dry_run:
        import random
        rng = random.Random(42)
        scale_results = {
            scale: {"ok": True, "latency": round(rng.uniform(100, 300), 4)}
            for scale in scales
        }
        for scale, r in scale_results.items():
            cpus = base_params["cpus_per_pool"] * scale
            ram = base_params["ram_gb_per_pool"] * scale
            print(f"  scale={scale}x  ({cpus} CPUs, {ram} GB RAM)  {r['latency']:.4f}s  (dry-run)")
    else:
        scale_results = evaluate_across_scales(
            scheduler_path=args.scheduler.resolve(),
            trace_file=str(args.trace.resolve()),
            scales=scales,
            base_params=base_params,
        )

    valid_latencies = [r["latency"] for r in scale_results.values() if r.get("ok")]
    if valid_latencies:
        gm = geometric_mean(valid_latencies)
        print(f"\nGeometric mean ({len(valid_latencies)}/{len(scales)} runs): {gm:.4f}s")
    else:
        print("\nWARNING: all runs failed — LLM will still attempt improvement.")

    # 2. Build prompt and call LLM
    print("\n=== Calling LLM for improvement ===")
    scheduler_code = args.scheduler.read_text()
    if args.dry_run:
        mock_schedulers = sorted(MOCK_DATA_DIR.glob("scheduler_*.py"))
        assert mock_schedulers, f"No scheduler_*.py found in {MOCK_DATA_DIR}"
        mock_path = mock_schedulers[0]
        print(f"  (dry-run: skipping LLM call, using {mock_path.name} as improved scheduler)")
        improved_code = mock_path.read_text()
    else:
        prompt = build_improvement_prompt(scheduler_code, scale_results, base_params)
        raw = call_llm(prompt, args.model, args.effort, args.verbose)
        improved_code = extract_code(raw)

    # 3. Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(improved_code + "\n")
    print(f"\nImproved scheduler written to: {output_path}  ({len(improved_code.splitlines())} lines)")


if __name__ == "__main__":
    main()
