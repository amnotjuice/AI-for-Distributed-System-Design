#!/usr/bin/env python3
"""Analyze schedulers for spring2026 experiments.

Usage:
    python analyze.py 01_reasoning [--prototype]
    python analyze.py 02_estimation [--prototype]
    python analyze.py all [--prototype]

Each subcommand runs the scheduler evaluation for that experiment and writes
results to spring2026/results/{exp_name}/.

--prototype  Fast/cheap run (1-min simulations, no reasoning). Results won't
             be meaningful, but validates the full pipeline end-to-end.
"""

from __future__ import annotations

import argparse
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

# Ensure src/ is on path
_SRC = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_SRC))
os.environ.setdefault("LITELLM_LOG", "ERROR")
logging.getLogger("eudoxia").setLevel(logging.CRITICAL)

from spring2026.tool.config import (
    CANONICAL_SIM_PARAMS,
    ESTIMATOR_CONDITIONS,
    EXPERIMENTS,
    RESULTS_DIR,
    SCHEDULERS_DIR,
    TRACES_DIR,
    get_canonical_base_params,
    wilson_interval,
)
from simulation_utils import (
    extract_metrics_from_stats,
    get_raw_stats_for_policy,
)


# ---------------------------------------------------------------------------
# Shared evaluation helpers (adapted from one-shot/analyze.py)
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
                    if fv == int(fv):
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
    }


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
        cluster_sizes = base_params.pop("_cluster_sizes", None)
        if cluster_sizes is not None:
            raw = []
            for n_pools in cluster_sizes:
                p = base_params.copy()
                p["num_pools"] = n_pools
                raw.extend(get_raw_stats_for_policy(p, trace_files, key_match.group(1)))
            expected = len(cluster_sizes) * len(trace_files)
        else:
            raw = get_raw_stats_for_policy(base_params, trace_files, key_match.group(1))
            expected = len(trace_files)
        if len(raw) != expected:
            return {
                "functional": False, "failure_mode": "simulation_error",
                "error_message": f"Got {len(raw)}/{expected} results",
            }

        values = [float(v) for v in extract_metrics_from_stats(raw, metric)]
        med = float(statistics.median(values))
        beats = bool(med < baseline_median) if metric == "latency" else bool(med > baseline_median)
        imp = float((med - baseline_median) / baseline_median * 100) if baseline_median else None

        ram_means = [s.ram_utilization_mean for s in raw if getattr(s, "ram_utilization_mean", None) is not None]
        oom_count = sum(s.failure_error_counts.get("OOM", 0) for s in raw if hasattr(s, "failure_error_counts"))
        total_suspensions = sum(s.suspensions for s in raw if hasattr(s, "suspensions"))
        total_assignments = sum(s.assignments for s in raw if hasattr(s, "assignments"))
        total_arrivals = sum(s.pipelines_all.arrival_count for s in raw if hasattr(s, "pipelines_all"))
        total_completions = sum(s.pipelines_all.completion_count for s in raw if hasattr(s, "pipelines_all"))
        completion_rate = total_completions / total_arrivals if total_arrivals > 0 else None

        def _weighted_mean_latency_seconds(attr):
            weighted_sum, weight_total = 0.0, 0
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
            "oom_count": oom_count,
            "suspensions": total_suspensions,
            "assignments": total_assignments,
            "suspension_rate": total_suspensions / total_assignments if total_assignments > 0 else None,
            "completion_rate": completion_rate,
            "latency_query_s": _weighted_mean_latency_seconds("pipelines_query"),
            "latency_interactive_s": _weighted_mean_latency_seconds("pipelines_interactive"),
            "latency_batch_s": _weighted_mean_latency_seconds("pipelines_batch"),
            "completion_rate_query": _completion_rate("pipelines_query"),
            "completion_rate_interactive": _completion_rate("pipelines_interactive"),
            "completion_rate_batch": _completion_rate("pipelines_batch"),
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


def evaluate(filepath: Path, trace_files: list[str], baseline_median: float, metric: str, base_params: dict) -> dict:
    cmd = [
        sys.executable, str(Path(__file__).resolve()),
        "--worker",
        str(filepath), json.dumps(trace_files), str(baseline_median), metric, json.dumps(base_params),
    ]
    subprocess_timeout = base_params.get("subprocess_timeout", 700)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True,
                                cwd=str(_SRC), timeout=subprocess_timeout)
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


def load_existing_records(output_path: Path) -> dict[str, dict]:
    if not output_path.exists():
        return {}
    existing = {}
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
            if fn := record.get("filename"):
                existing[fn] = record
    return existing


def run_analyze(
    scheduler_dirs: list[Path],
    trace_files: list[str],
    output_dir: Path,
    base_params: dict,
    metric: str = "latency",
    exp_label: str = "",
) -> None:
    """Evaluate all schedulers in the given dirs and write JSONL to output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "analysis.jsonl"
    existing = load_existing_records(output_path)

    # Baseline
    SCHEDULER_LIMIT_KEYS = {"tick_timeout_ms", "max_outstanding", "_cluster_sizes"}
    cluster_sizes = base_params.get("_cluster_sizes")
    baseline_params = {k: v for k, v in base_params.items() if k not in SCHEDULER_LIMIT_KEYS}
    print("Running baseline...")
    if cluster_sizes:
        naive_raw = []
        for n_pools in cluster_sizes:
            p = baseline_params.copy()
            p["num_pools"] = n_pools
            naive_raw.extend(get_raw_stats_for_policy(p, trace_files, "naive"))
        assert naive_raw, "Baseline failed"
    else:
        naive_raw = get_raw_stats_for_policy(baseline_params, trace_files, "naive")
        assert len(naive_raw) == len(trace_files), "Baseline failed"
    baseline_med = statistics.median(extract_metrics_from_stats(naive_raw, metric))
    print(f"Baseline median {metric}: {baseline_med:.4f}")

    scheduler_files = []
    for d in scheduler_dirs:
        scheduler_files.extend(sorted(d.glob("scheduler_*.py")))
    assert scheduler_files, f"No scheduler_*.py found in {scheduler_dirs}"
    print(f"{len(trace_files)} traces | {len(scheduler_files)} schedulers | metric={metric}{' | ' + exp_label if exp_label else ''}")

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
                "scheduler_dir": str(fp.parent.name),
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
            detail = (f"{metric}={record.get(f'median_{metric}', 'N/A')}"
                      if record["functional"] else record.get("failure_mode", ""))
            print(f"  {ok} {detail}")

    # Cleanup stray utility logs
    for p in _SRC.glob("pool_*_utility.csv"):
        p.unlink(missing_ok=True)

    all_list = [all_records[fp.name] for fp in scheduler_files if fp.name in all_records]
    func = sum(1 for r in all_list if r["functional"])
    beats = sum(1 for r in all_list if r.get("beats_baseline"))
    print("=" * 50)
    print(f"DONE: {func}/{len(all_list)} functional, {beats}/{len(all_list)} beat baseline")
    print(f"Output: {output_path}")


# ---------------------------------------------------------------------------
# Per-experiment handlers
# ---------------------------------------------------------------------------

def _run_probes_for_dir(sched_dir: Path, out_dir: Path, base_params: dict) -> None:
    """Run all probes on schedulers in sched_dir, write CSV to out_dir/probes.csv."""
    _probe_dir = Path(__file__).resolve().parent / "probe"
    sys.path.insert(0, str(_probe_dir))
    from run_probes import ensure_traces, run_all_probes, write_csv

    traces = ensure_traces()
    scheduler_files = sorted(sched_dir.glob("scheduler_*.py"))
    if not scheduler_files:
        return
    all_results = []
    for sf in scheduler_files:
        results = run_all_probes(sf, base_params, traces)
        all_results.append(results)
        passed = sum(1 for r in results.values() if r.get("functional"))
        print(f"  {sf.name}: {passed}/{len(results)}")
    write_csv(scheduler_files, all_results, out_dir / "probes.csv")


def analyze_01_reasoning(prototype: bool) -> None:
    """Fig 1: one-shot, vary reasoning level, no estimation."""
    base_params = get_canonical_base_params(prototype=prototype)
    canonical = TRACES_DIR / "bench_canonical_train.csv"
    assert canonical.exists(), f"Canonical trace not found: {canonical}"
    trace_files = [str(canonical)]
    base_params["_cluster_sizes"] = [1, 2, 4, 8, 16]

    for effort in ["none", "low", "medium", "high"]:
        sched_dir = SCHEDULERS_DIR / "reasoning" / effort
        if not sched_dir.exists() or not list(sched_dir.glob("scheduler_*.py")):
            print(f"  SKIP {effort}: no schedulers in {sched_dir}")
            continue
        out_dir = RESULTS_DIR / "01_reasoning" / effort

        print(f"\n--- Probes: {effort} ---")
        _run_probes_for_dir(sched_dir, out_dir, base_params)

        print(f"\n--- Latency: {effort} ---")
        run_analyze([sched_dir], trace_files, out_dir, base_params, exp_label=f"reasoning={effort}")


def analyze_02_estimation(prototype: bool) -> None:
    """Fig 2: one-shot, vary estimation noise, medium reasoning."""
    sched_dir = SCHEDULERS_DIR / "estimation"
    if not sched_dir.exists() or not list(sched_dir.glob("scheduler_*.py")):
        print(f"  SKIP: no schedulers in {sched_dir}")
        return

    base_params = get_canonical_base_params(prototype=prototype)
    canonical = TRACES_DIR / "bench_canonical_train.csv"
    assert canonical.exists(), f"Canonical trace not found: {canonical}"
    trace_files = [str(canonical)]
    base_params["_cluster_sizes"] = [1, 2, 4, 8, 16]

    # Run probes once for the estimation schedulers
    out_base = RESULTS_DIR / "02_estimation"
    print("\n--- Probes: estimation ---")
    _run_probes_for_dir(sched_dir, out_base, base_params)

    # Evaluate under each sigma condition
    for sigma_str, sigma_params in ESTIMATOR_CONDITIONS.items():
        params = base_params.copy()
        params.update(sigma_params)
        out_dir = out_base / sigma_str
        print(f"\n--- Latency: {sigma_str} ---")
        run_analyze([sched_dir], trace_files, out_dir, params, exp_label=f"estimation={sigma_str}")


def analyze_03_two_iter_best_worst(prototype: bool) -> None:
    """Fig 3: two-iteration, best/worst/median schedulers, rich vs simple context."""
    base_params = get_canonical_base_params(prototype=prototype)
    canonical = TRACES_DIR / "bench_canonical_train.csv"
    assert canonical.exists(), f"Canonical trace not found: {canonical}"
    trace_files = [str(canonical)]
    base_params["_cluster_sizes"] = [1, 2, 4, 8, 16]

    combos = [
        f"{src}_{ctx}"
        for src in ["best", "worst", "median"]
        for ctx in ["simple", "rich"]
    ]

    for combo in combos:
        sched_dir = SCHEDULERS_DIR / "two_iter" / combo
        if not sched_dir.exists() or not list(sched_dir.glob("scheduler_*.py")):
            print(f"  SKIP {combo}: no schedulers in {sched_dir}")
            continue

        out_dir = RESULTS_DIR / "03_two_iter" / combo
        print(f"\n--- Latency: {combo} ---")
        run_analyze([sched_dir], trace_files, out_dir, base_params.copy(),
                    exp_label=f"two_iter={combo}")

        # Write summary: % improved over source scheduler
        meta_path = sched_dir / "meta.json"
        if not meta_path.exists():
            print(f"  WARNING: no meta.json in {sched_dir}")
            continue

        meta = json.loads(meta_path.read_text())
        source_latency = meta.get("source_median_latency")
        if source_latency is None:
            print(f"  WARNING: source_median_latency missing from meta.json")
            continue

        records = list(load_existing_records(out_dir / "analysis.jsonl").values())
        n_total = len(records)
        n_functional = sum(1 for r in records if r.get("functional"))
        n_improved = sum(
            1 for r in records
            if r.get("functional") and r.get("median_latency") is not None
            and r["median_latency"] < source_latency
        )

        lo, hi = wilson_interval(n_improved, n_total) if n_total > 0 else (None, None)
        summary = {
            "combo": combo,
            "source": meta.get("source"),
            "context": meta.get("context"),
            "source_filename": meta.get("source_filename"),
            "source_median_latency": source_latency,
            "n_total": n_total,
            "n_functional": n_functional,
            "n_improved": n_improved,
            "improved_rate": n_improved / n_total if n_total > 0 else None,
            "improved_lo": lo,
            "improved_hi": hi,
        }
        summary_path = out_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2))
        rate_str = f"{summary['improved_rate']:.1%}" if summary["improved_rate"] is not None else "N/A"
        print(f"  Summary: {n_improved}/{n_total} improved ({rate_str})")


def analyze_04_two_iter_all(prototype: bool) -> None:
    """Fig 4: two-iteration, all contexts, % time v2 beats v1."""
    print("analyze_04: not yet implemented")


def analyze_05_two_shot_perf(prototype: bool) -> None:
    """Fig 5: two-shot perf with shorter/coarser simulations."""
    print("analyze_05: not yet implemented")


def analyze_06_multi_iter(prototype: bool) -> None:
    """Fig 6: 10 scenarios × 50 iterations, latency vs iteration."""
    print("analyze_06: not yet implemented")


def analyze_07_cross_eval(prototype: bool) -> None:
    """Fig 7: cross-eval heatmap."""
    print("analyze_07: not yet implemented")


def analyze_08_adapt_speed(prototype: bool) -> None:
    """Fig 8: iterations to adapt to new scenario."""
    print("analyze_08: not yet implemented")


def analyze_09_general_purpose(prototype: bool) -> None:
    """Fig 9: general-purpose scheduler over all scenarios."""
    print("analyze_09: not yet implemented")


HANDLERS = {
    "01_reasoning":           analyze_01_reasoning,
    "02_estimation":          analyze_02_estimation,
    "03_two_iter_best_worst": analyze_03_two_iter_best_worst,
    "04_two_iter_all":        analyze_04_two_iter_all,
    "05_two_shot_perf":       analyze_05_two_shot_perf,
    "06_multi_iter":          analyze_06_multi_iter,
    "07_cross_eval":          analyze_07_cross_eval,
    "08_adapt_speed":         analyze_08_adapt_speed,
    "09_general_purpose":     analyze_09_general_purpose,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "experiment",
        choices=list(HANDLERS) + ["all"],
        help="Experiment to analyze, or 'all' to run all",
    )
    parser.add_argument(
        "--prototype", action="store_true",
        help="Fast/cheap run (1-min sims, validates infra only)",
    )
    args = parser.parse_args()

    exps = list(HANDLERS) if args.experiment == "all" else [args.experiment]
    for exp in exps:
        print(f"\n{'='*60}")
        print(f"Experiment: {exp} — {EXPERIMENTS.get(exp, '')}")
        if args.prototype:
            print("  [PROTOTYPE MODE — results not meaningful]")
        print("=" * 60)
        HANDLERS[exp](prototype=args.prototype)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        sys.exit(_worker_entrypoint(sys.argv[2:]))
    main()
