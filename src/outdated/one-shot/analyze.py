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
                # Map reason to specific failure_mode for clean analysis
                reason_to_mode = {
                    "timeout": "simulation_timeout",
                    "tick_timeout": "tick_timeout",
                    "no_completed_containers": "no_completed_containers",
                }
                failure_mode = reason_to_mode.get(reason, f"simulation_error_{reason}")
                return {
                    "functional": False, "failure_mode": failure_mode,
                    "reason": reason,
                    "error_message": f"Failed on '{trace_file}': {detail} ({len(raw)}/{len(trace_files)} traces)",
                }
            return {
                "functional": False, "failure_mode": "simulation_error",
                "error_message": f"Got {len(raw)}/{len(trace_files)} results",
            }

        values = [float(v) for v in extract_metrics_from_stats(raw, metric)]
        med = float(statistics.median(values))
        beats = bool(med < baseline_median) if metric == "latency" else bool(med > baseline_median)
        imp = float((med - baseline_median) / baseline_median * 100) if baseline_median else None

        # Aggregate RAM utilization across traces (mean of per-trace means)
        ram_means = [s.ram_utilization_mean for s in raw if getattr(s, "ram_utilization_mean", None) is not None]
        ram_medians = [s.ram_utilization_median for s in raw if getattr(s, "ram_utilization_median", None) is not None]
        ram_p5s = [s.ram_utilization_p5 for s in raw if getattr(s, "ram_utilization_p5", None) is not None]
        ram_ns = [getattr(s, "ram_utilization_n", 0) for s in raw]

        # Aggregate failure counts across traces (sum)
        oom_count = sum(s.failure_error_counts.get("OOM", 0) for s in raw if hasattr(s, "failure_error_counts"))
        total_suspensions = sum(s.suspensions for s in raw if hasattr(s, "suspensions"))
        total_assignments = sum(s.assignments for s in raw if hasattr(s, "assignments"))
        # Throughput: mean containers/sec across traces
        throughput_mean = float(statistics.mean(s.throughput for s in raw)) if raw else None

        # Completion rate: completed pipelines / all arrivals across traces
        total_arrivals = sum(s.pipelines_all.arrival_count for s in raw if hasattr(s, "pipelines_all"))
        total_completions = sum(s.pipelines_all.completion_count for s in raw if hasattr(s, "pipelines_all"))
        completion_rate = total_completions / total_arrivals if total_arrivals > 0 else None

        # Per-priority latency (mean of per-trace mean_latency_seconds, weighted by completion count)
        def _weighted_mean_latency_seconds(attr):
            weighted_sum = 0.0
            weight_total = 0
            for s in raw:
                stats = getattr(s, attr, None)
                if stats and stats.completion_count > 0:
                    weighted_sum += stats.mean_latency_seconds * stats.completion_count
                    weight_total += stats.completion_count
            return weighted_sum / weight_total if weight_total > 0 else None

        def _completion_rate(attr):
            arrivals = sum(getattr(s, attr).arrival_count for s in raw if hasattr(s, attr))
            completions = sum(getattr(s, attr).completion_count for s in raw if hasattr(s, attr))
            return completions / arrivals if arrivals > 0 else None

        return {
            "functional": True, "failure_mode": "success",
            f"median_{metric}": med, "metric_values": values,
            "beats_baseline": beats, "improvement_pct": imp,
            "ram_utilization_mean": float(statistics.mean(ram_means)) if ram_means else None,
            "ram_utilization_median": float(statistics.mean(ram_medians)) if ram_medians else None,
            "ram_utilization_p5": float(statistics.mean(ram_p5s)) if ram_p5s else None,
            "ram_utilization_n": sum(ram_ns),
            # Failure breakdown
            "oom_count": oom_count,
            "suspensions": total_suspensions,
            "assignments": total_assignments,
            "suspension_rate": total_suspensions / total_assignments if total_assignments > 0 else None,
            "throughput_mean": throughput_mean,
            # Pipeline completion
            "completion_rate": completion_rate,
            # Per-priority latency (weighted mean across traces, in seconds)
            "latency_query_s": _weighted_mean_latency_seconds("pipelines_query"),
            "latency_interactive_s": _weighted_mean_latency_seconds("pipelines_interactive"),
            "latency_batch_s": _weighted_mean_latency_seconds("pipelines_batch"),
            # Per-priority completion rates
            "completion_rate_query": _completion_rate("pipelines_query"),
            "completion_rate_interactive": _completion_rate("pipelines_interactive"),
            "completion_rate_batch": _completion_rate("pipelines_batch"),
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
    subprocess_timeout = base_params.get("subprocess_timeout", 700)  # None = disabled
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            cwd=str(Path(__file__).resolve().parent.parent),
            timeout=subprocess_timeout,
        )
        # Extract result via marker to avoid interference from stray stdout
        for line in reversed(result.stdout.strip().split("\n")):
            if line.startswith("__RESULT__"):
                return json.loads(line[len("__RESULT__"):])
        stderr_snippet = result.stderr[-200:] if result.stderr else "no output"
        return {"functional": False, "failure_mode": "no_output", "reason": "no_output", "error_message": stderr_snippet}
    except subprocess.TimeoutExpired:
        return {"functional": False, "failure_mode": "total_timeout", "reason": "total_timeout", "error_message": f"Subprocess exceeded {subprocess_timeout}s total limit"}
    except Exception as e:
        return {"functional": False, "failure_mode": "eval_error", "error_message": str(e)}


def load_experiment_config(experiment_name: str) -> dict:
    """Load PARAM_OVERRIDES from experiments/{name}/config.py."""
    config_path = ONE_SHOT_DIR / "experiments" / experiment_name / "config.py"
    assert config_path.exists(), f"Experiment config not found: {config_path}"
    ns = {}
    exec(config_path.read_text(), ns)
    return ns.get("PARAM_OVERRIDES", {})


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scheduler_dir", type=Path)
    parser.add_argument("--metric", choices=["latency", "throughput"], default="latency")
    parser.add_argument("--experiment", type=str, default=None,
                        help="Experiment name (subdirectory under experiments/). "
                             "Loads param overrides and writes output there.")
    parser.add_argument("--output-dir", type=Path, default=None,
                        help="Override output directory (ignores default path logic).")
    args = parser.parse_args()

    traces_dir = ONE_SHOT_DIR / "traces"
    trace_files = sorted(str(p) for p in traces_dir.glob("*.csv"))
    assert trace_files, f"No traces found in {traces_dir}"

    scheduler_files = sorted(args.scheduler_dir.resolve().glob("scheduler_*.py"))
    assert scheduler_files, f"No scheduler_*.py in {args.scheduler_dir}"

    # Build params: canonical defaults + optional experiment overrides
    base_params = get_canonical_base_params()
    if args.experiment:
        overrides = load_experiment_config(args.experiment)
        base_params.update(overrides)
        print(f"Experiment: {args.experiment} | overrides: {overrides}")

    print(f"{len(trace_files)} traces | {len(scheduler_files)} schedulers | metric={args.metric}")

    # Baseline uses experiment simulation params (e.g. duration) for fair comparison,
    # but NOT scheduler-limiting params that would break or distort the naive scheduler.
    SCHEDULER_LIMIT_KEYS = {"tick_timeout_ms", "max_outstanding"}
    print("Running baseline...")
    baseline_params = {k: v for k, v in base_params.items() if k not in SCHEDULER_LIMIT_KEYS}
    naive_raw = get_raw_stats_for_policy(baseline_params, trace_files, "naive")
    assert len(naive_raw) == len(trace_files), "Baseline failed"
    baseline_med = statistics.median(extract_metrics_from_stats(naive_raw, args.metric))
    print(f"Baseline median {args.metric}: {baseline_med:.4f}")

    # Output: custom dir > experiment dir > default
    if args.output_dir:
        output_dir = args.output_dir
    elif args.experiment:
        output_dir = ONE_SHOT_DIR / "experiments" / args.experiment / "output" / args.scheduler_dir.resolve().name
    else:
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
