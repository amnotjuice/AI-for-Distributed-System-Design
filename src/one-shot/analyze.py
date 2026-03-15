#!/usr/bin/env python3
"""Analyze schedulers in a schedulers-{effort}/ directory.

Usage:
    python analyze.py schedulers-low/
    python analyze.py schedulers-medium/
"""

from __future__ import annotations

import json
import logging
import os
import re
import statistics
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ.setdefault("LITELLM_LOG", "ERROR")
logging.getLogger("eudoxia").setLevel(logging.CRITICAL)

from config import ONE_SHOT_DIR, get_canonical_base_params
from simulation_utils import (
    extract_metrics_from_stats,
    get_last_simulation_failure,
    get_raw_stats_for_policy,
)


def parse_header(filepath: Path) -> dict:
    """Extract '# key: value' metadata from file header."""
    meta = {}
    with open(filepath) as f:
        for line in f:
            if not line.strip().startswith("#"):
                break
            m = re.match(r"^#\s+(\w+):\s+(.+)$", line.strip())
            if m:
                k, v = m.group(1), m.group(2)
                with suppress(ValueError, OverflowError):
                    fv = float(v)
                    if not (fv != fv) and fv == int(fv):  # safe: excludes nan/inf
                        v = int(fv)
                    else:
                        v = fv
                meta[k] = v
    return meta


def static_analysis(filepath: Path) -> dict:
    """Quick static analysis of scheduler source code."""
    src = filepath.read_text()
    code = [l for l in src.split("\n") if l.strip() and not l.strip().startswith("#")]
    return {
        "code_lines": len(code),
        "uses_priority": "Priority" in src,
        "uses_suspension": "Suspend(" in src,
        "uses_multi_pool": src.count("pool_id") > 1,
    }


# ---------------------------------------------------------------------------
# Subprocess-based evaluation (reliable isolation per scheduler)
# ---------------------------------------------------------------------------

def _evaluate_scheduler_worker(
    scheduler_file: str,
    trace_files: list[str],
    baseline_median: float,
    metric: str,
    base_params: dict,
) -> dict:
    """Worker-side scheduler evaluation logic (called in a subprocess)."""
    src = Path(scheduler_file).read_text()
    key_match = re.search(r"""@register_scheduler\((?:key=)?['"]([^'"]+)['"]\)""", src)
    if not key_match:
        return {"functional": False, "failure_mode": "no_scheduler_key"}

    try:
        from typing import List, Tuple

        from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
        from eudoxia.scheduler.decorators import register_scheduler, register_scheduler_init
        from eudoxia.utils import Priority
        from eudoxia.workload import OperatorState, Pipeline
        from eudoxia.workload.runtime_status import ASSIGNABLE_STATES

        worker_globals = {
            "__builtins__": __builtins__,
            "__name__": "__worker__",
            "List": List, "Tuple": Tuple,
            "Pipeline": Pipeline, "OperatorState": OperatorState,
            "ASSIGNABLE_STATES": ASSIGNABLE_STATES,
            "Assignment": Assignment, "ExecutionResult": ExecutionResult,
            "Suspend": Suspend,
            "register_scheduler_init": register_scheduler_init,
            "register_scheduler": register_scheduler,
            "Priority": Priority,
        }
        exec(src, worker_globals)
    except Exception as e:
        return {"functional": False, "failure_mode": "exec_error", "error_message": str(e)}

    try:
        raw = get_raw_stats_for_policy(base_params, trace_files, key_match.group(1))
        if len(raw) != len(trace_files):
            last_failure = get_last_simulation_failure()
            if last_failure is not None:
                reason = last_failure.get("reason", "unknown")
                trace_file = last_failure.get("trace_file", "unknown")
                detail = last_failure.get("detail", "")
                if reason == "timeout":
                    return {
                        "functional": False, "failure_mode": "simulation_timeout",
                        "error_message": f"Timeout on '{trace_file}' after {last_failure.get('timeout_seconds')}s ({len(raw)}/{len(trace_files)} traces)",
                    }
                return {
                    "functional": False, "failure_mode": "simulation_error",
                    "error_message": f"Failed on '{trace_file}' ({reason}): {detail} ({len(raw)}/{len(trace_files)} traces)",
                }
            return {
                "functional": False, "failure_mode": "simulation_error",
                "error_message": f"Got {len(raw)}/{len(trace_files)} results",
            }

        values = [float(v) for v in extract_metrics_from_stats(raw, metric)]
        med = float(statistics.median(values))
        beats = bool(med < baseline_median) if metric == "latency" else bool(med > baseline_median)
        imp = float((med - baseline_median) / baseline_median * 100) if baseline_median else None
        return {
            "functional": True, "failure_mode": "success",
            f"median_{metric}": med, "metric_values": values,
            "beats_baseline": beats, "improvement_pct": imp,
        }
    except Exception as e:
        return {"functional": False, "failure_mode": "simulation_error", "error_message": str(e)}


def _worker_entrypoint(argv: list[str]) -> int:
    """CLI entrypoint for subprocess worker mode."""
    if len(argv) != 5:
        print(json.dumps({"functional": False, "failure_mode": "invalid_worker_args"}))
        return 2

    scheduler_file, trace_files_raw, baseline_raw, metric, base_params_raw = argv
    try:
        trace_files = json.loads(trace_files_raw)
        baseline_median = float(baseline_raw)
        base_params = json.loads(base_params_raw)
    except Exception as e:
        print(json.dumps({"functional": False, "failure_mode": "invalid_worker_args", "error_message": str(e)}))
        return 2

    result = _evaluate_scheduler_worker(
        scheduler_file=scheduler_file, trace_files=trace_files,
        baseline_median=baseline_median, metric=metric, base_params=base_params,
    )
    # Use a marker so we can reliably extract JSON from stdout
    print(f"__RESULT__{json.dumps(result)}")
    return 0


def load_existing_records(output_path: Path) -> dict[str, dict]:
    """Load existing JSONL results keyed by filename for resume-safe appends."""
    if not output_path.exists():
        return {}
    existing: dict[str, dict] = {}
    with output_path.open() as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                print(f"WARNING: Skipping malformed JSON at {output_path}:{line_no}")
                continue
            filename = record.get("filename")
            if filename:
                existing[filename] = record
    return existing


def evaluate(
    filepath: Path,
    trace_files: list[str],
    baseline_median: float,
    metric: str,
    base_params: dict,
) -> dict:
    """Run scheduler evaluation in a subprocess with reliable isolation."""
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        str(filepath),
        json.dumps(trace_files),
        str(baseline_median),
        metric,
        json.dumps(base_params),
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            cwd=str(Path(__file__).resolve().parent.parent),
            timeout=700,  # 60s/trace × 10 traces + 100s buffer
        )
        # Extract result via marker to avoid interference from stray stdout
        for line in reversed(result.stdout.strip().split("\n")):
            if line.startswith("__RESULT__"):
                return json.loads(line[len("__RESULT__"):])
        stderr_snippet = result.stderr[-200:] if result.stderr else "no output"
        return {"functional": False, "failure_mode": "no_output", "error_message": stderr_snippet}
    except subprocess.TimeoutExpired:
        return {"functional": False, "failure_mode": "total_timeout", "error_message": "Subprocess exceeded 700s total limit"}
    except Exception as e:
        return {"functional": False, "failure_mode": "eval_error", "error_message": str(e)}


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scheduler_dir", type=Path)
    parser.add_argument("--metric", choices=["latency", "throughput"], default="latency")
    args = parser.parse_args()

    traces_dir = ONE_SHOT_DIR / "traces"
    trace_files = sorted(str(p) for p in traces_dir.glob("*.csv"))
    assert trace_files, f"No traces found in {traces_dir}"

    scheduler_files = sorted(args.scheduler_dir.resolve().glob("scheduler_*.py"))
    assert scheduler_files, f"No scheduler_*.py in {args.scheduler_dir}"
    print(f"{len(trace_files)} traces | {len(scheduler_files)} schedulers | metric={args.metric}")

    # Baseline
    base_params = get_canonical_base_params()
    print("Running baseline...")
    naive_raw = get_raw_stats_for_policy(base_params, trace_files, "naive")
    assert len(naive_raw) == len(trace_files), "Baseline failed"
    baseline_med = statistics.median(extract_metrics_from_stats(naive_raw, args.metric))
    print(f"Baseline median {args.metric}: {baseline_med:.4f}")

    # Output
    output_dir = ONE_SHOT_DIR / "output" / args.scheduler_dir.resolve().name
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "analysis.jsonl"
    existing_by_filename = load_existing_records(output_path)
    if existing_by_filename:
        print(f"Resuming: {len(existing_by_filename)} existing records")

    # Analyze each scheduler
    all_records_by_filename = dict(existing_by_filename)
    with output_path.open("a") as out_f:
        for i, fp in enumerate(scheduler_files, 1):
            if fp.name in existing_by_filename:
                print(f"[{i}/{len(scheduler_files)}] {fp.name}  already recorded")
                continue

            print(f"[{i}/{len(scheduler_files)}] {fp.name}")
            t0 = time.time()
            record = {
                "filename": fp.name,
                "baseline_median": baseline_med,
                "metric": args.metric,
                **parse_header(fp),
                **static_analysis(fp),
                **evaluate(fp, trace_files, baseline_med, args.metric, base_params),
                "simulation_seconds": round(time.time() - t0, 2),
            }
            all_records_by_filename[fp.name] = record
            out_f.write(json.dumps(record) + "\n")
            out_f.flush()
            with suppress(OSError):
                os.fsync(out_f.fileno())

            ok = "OK" if record["functional"] else "FAIL"
            detail = (
                f"{args.metric}={record.get(f'median_{args.metric}', 'N/A')}"
                if record["functional"]
                else record.get("failure_mode", "")
            )
            print(f"  {ok} {detail}")

    # Clean up simulator utility logs
    for p in Path(__file__).resolve().parent.parent.glob("pool_*_utility.csv"):
        p.unlink(missing_ok=True)
    for p in ONE_SHOT_DIR.glob("pool_*_utility.csv"):
        p.unlink(missing_ok=True)

    all_records = [all_records_by_filename[fp.name] for fp in scheduler_files if fp.name in all_records_by_filename]
    func = sum(1 for r in all_records if r["functional"])
    beats = sum(1 for r in all_records if r.get("beats_baseline"))
    print("=" * 50)
    print(f"DONE: {func}/{len(all_records)} functional, {beats}/{len(all_records)} beat baseline")
    print(f"Output: {output_path}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        sys.exit(_worker_entrypoint(sys.argv[2:]))
    main()
