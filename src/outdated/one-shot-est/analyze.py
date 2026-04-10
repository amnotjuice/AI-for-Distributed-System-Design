#!/usr/bin/env python3
"""Analyze one-shot-est schedulers under four estimator noise levels.

Evaluates every scheduler_est_*.py in schedulers-high/ under:
  - sigma_0.0:  oracle (perfect estimates)
  - sigma_0.5:  low noise
  - sigma_1.0:  medium noise
  - sigma_1.5:  high noise

Output goes to output/{condition}/analysis.jsonl

Usage:
    python analyze.py [--condition {sigma_0.0,sigma_0.5,sigma_1.0,sigma_1.5,all}]
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

THIS_DIR = Path(__file__).resolve().parent
SRC_DIR = THIS_DIR.parent

sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, str(SRC_DIR / "one-shot"))

os.environ.setdefault("LITELLM_LOG", "ERROR")
logging.getLogger("eudoxia").setLevel(logging.CRITICAL)

# Load both config files via importlib (directory names have hyphens)
import importlib.util as _ilu


def _load_module(name: str, path: Path):
    spec = _ilu.spec_from_file_location(name, path)
    mod = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_est_cfg = _load_module("est_config", THIS_DIR / "config.py")
PARAM_OVERRIDES = _est_cfg.PARAM_OVERRIDES
ESTIMATOR_CONDITIONS = _est_cfg.ESTIMATOR_CONDITIONS

_one_shot_cfg = _load_module("one_shot_config", SRC_DIR / "one-shot" / "config.py")
ONE_SHOT_DIR = _one_shot_cfg.ONE_SHOT_DIR
get_canonical_base_params = _one_shot_cfg.get_canonical_base_params

SCHEDULER_DIR = THIS_DIR / "schedulers-high"
OUTPUT_DIR = THIS_DIR / "output"


# ---------------------------------------------------------------------------
# Utilities (copied from one-shot/analyze.py for subprocess-safe isolation)
# ---------------------------------------------------------------------------

def parse_header(filepath: Path) -> dict:
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
                    if not (fv != fv) and fv == int(fv):
                        v = int(fv)
                    else:
                        v = fv
                meta[k] = v
    return meta


def static_analysis(filepath: Path) -> dict:
    src = filepath.read_text()
    code = [l for l in src.split("\n") if l.strip() and not l.strip().startswith("#")]
    return {
        "code_lines": len(code),
        "uses_priority": "Priority" in src,
        "uses_suspension": "Suspend(" in src,
        "uses_multi_pool": src.count("pool_id") > 1,
        "uses_estimate": "estimate" in src,
    }


def load_existing_records(output_path: Path) -> dict[str, dict]:
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


# ---------------------------------------------------------------------------
# Subprocess worker (must be in this file so --worker dispatch works)
# ---------------------------------------------------------------------------

def _evaluate_scheduler_worker(
    scheduler_file: str,
    trace_files: list[str],
    baseline_median: float,
    metric: str,
    base_params: dict,
) -> dict:
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
        from simulation_utils import (
            extract_metrics_from_stats,
            get_last_simulation_failure,
            get_raw_stats_for_policy,
        )
        raw = get_raw_stats_for_policy(base_params, trace_files, key_match.group(1))
        if len(raw) != len(trace_files):
            last_failure = get_last_simulation_failure()
            if last_failure is not None:
                reason = last_failure.get("reason", "unknown")
                trace_file = last_failure.get("trace_file", "unknown")
                detail = last_failure.get("detail", "")
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
        return {
            "functional": True, "failure_mode": "success",
            f"median_{metric}": med, "metric_values": values,
            "beats_baseline": beats, "improvement_pct": imp,
        }
    except Exception as e:
        return {"functional": False, "failure_mode": "simulation_error", "error_message": str(e)}


def _worker_entrypoint(argv: list[str]) -> int:
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
    print(f"__RESULT__{json.dumps(result)}")
    return 0


def evaluate(
    filepath: Path,
    trace_files: list[str],
    baseline_median: float,
    metric: str,
    base_params: dict,
) -> dict:
    cmd = [
        sys.executable, str(Path(__file__).resolve()),
        "--worker",
        str(filepath),
        json.dumps(trace_files),
        str(baseline_median),
        metric,
        json.dumps(base_params),
    ]
    subprocess_timeout = base_params.get("subprocess_timeout", 700)
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            cwd=str(SRC_DIR),
            timeout=subprocess_timeout,
        )
        for line in reversed(result.stdout.strip().split("\n")):
            if line.startswith("__RESULT__"):
                return json.loads(line[len("__RESULT__"):])
        stderr_snippet = result.stderr[-200:] if result.stderr else "no output"
        return {"functional": False, "failure_mode": "no_output", "error_message": stderr_snippet}
    except subprocess.TimeoutExpired:
        return {"functional": False, "failure_mode": "total_timeout",
                "error_message": f"Subprocess exceeded {subprocess_timeout}s"}
    except Exception as e:
        return {"functional": False, "failure_mode": "eval_error", "error_message": str(e)}


# ---------------------------------------------------------------------------
# Per-condition analysis
# ---------------------------------------------------------------------------

def run_condition(
    condition_name: str,
    condition_params: dict,
    scheduler_files: list[Path],
    trace_files: list[str],
    metric: str,
    output_base: Path | None = None,
) -> None:
    print(f"\n{'='*55}")
    print(f"Condition: {condition_name}  |  {condition_params}")
    print(f"{'='*55}")

    # Build params: canonical + experiment overrides + condition overrides
    base_params = get_canonical_base_params()
    base_params.update(PARAM_OVERRIDES)
    base_params.update(condition_params)

    # Baseline
    from simulation_utils import extract_metrics_from_stats, get_raw_stats_for_policy
    SCHEDULER_LIMIT_KEYS = {"tick_timeout_ms", "max_outstanding"}
    baseline_params = {k: v for k, v in base_params.items() if k not in SCHEDULER_LIMIT_KEYS}
    print("Running baseline (naive)...")
    naive_raw = get_raw_stats_for_policy(baseline_params, trace_files, "naive")
    assert len(naive_raw) == len(trace_files), f"Baseline failed for condition {condition_name}"
    baseline_med = statistics.median(extract_metrics_from_stats(naive_raw, metric))
    print(f"Baseline median {metric}: {baseline_med:.4f}")

    # Output
    out_dir = (output_base or OUTPUT_DIR) / condition_name
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / "analysis.jsonl"
    existing = load_existing_records(output_path)
    if existing:
        print(f"Resuming: {len(existing)} existing records")

    all_records = dict(existing)
    with output_path.open("a") as out_f:
        for i, fp in enumerate(scheduler_files, 1):
            if fp.name in existing:
                print(f"[{i}/{len(scheduler_files)}] {fp.name}  already recorded")
                continue
            print(f"[{i}/{len(scheduler_files)}] {fp.name}")
            t0 = time.time()
            record = {
                "filename": fp.name,
                "condition": condition_name,
                "baseline_median": baseline_med,
                "metric": metric,
                **parse_header(fp),
                **static_analysis(fp),
                **evaluate(fp, trace_files, baseline_med, metric, base_params),
                "simulation_seconds": round(time.time() - t0, 2),
            }
            all_records[fp.name] = record
            out_f.write(json.dumps(record) + "\n")
            out_f.flush()
            with suppress(OSError):
                os.fsync(out_f.fileno())

            ok = "OK" if record["functional"] else "FAIL"
            detail = (
                f"{metric}={record.get(f'median_{metric}', 'N/A')}"
                if record["functional"]
                else record.get("failure_mode", "")
            )
            print(f"  {ok} {detail}")

    records_list = [all_records[fp.name] for fp in scheduler_files if fp.name in all_records]
    func = sum(1 for r in records_list if r["functional"])
    beats = sum(1 for r in records_list if r.get("beats_baseline"))
    print(f"\n{condition_name}: {func}/{len(records_list)} functional, {beats}/{len(records_list)} beat baseline")
    print(f"Output: {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--condition",
        choices=list(ESTIMATOR_CONDITIONS.keys()) + ["all"],
        default="all",
        help="Which estimator condition to evaluate (default: all)",
    )
    parser.add_argument("--metric", choices=["latency", "throughput"], default="latency")
    parser.add_argument("--scheduler-dir", type=Path, default=None,
                        help="Override scheduler directory (default: schedulers-high)")
    parser.add_argument("--output-dir", type=Path, default=None,
                        help="Override output base directory (default: output/)")
    args = parser.parse_args()

    traces_dir = ONE_SHOT_DIR / "traces"
    trace_files = sorted(str(p) for p in traces_dir.glob("*.csv"))
    assert trace_files, f"No traces in {traces_dir}"

    sched_dir = args.scheduler_dir or SCHEDULER_DIR
    scheduler_files = sorted(sched_dir.resolve().glob("scheduler_*.py"))
    assert scheduler_files, f"No scheduler_*.py in {sched_dir}"

    output_base = args.output_dir.resolve() if args.output_dir else None

    print(f"{len(trace_files)} traces | {len(scheduler_files)} schedulers | metric={args.metric}")

    conditions = ESTIMATOR_CONDITIONS if args.condition == "all" else {args.condition: ESTIMATOR_CONDITIONS[args.condition]}

    for cname, cparams in conditions.items():
        run_condition(cname, cparams, scheduler_files, trace_files, args.metric, output_base)

    # Clean up simulator utility logs
    for p in SRC_DIR.glob("pool_*_utility.csv"):
        p.unlink(missing_ok=True)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        sys.exit(_worker_entrypoint(sys.argv[2:]))
    main()
