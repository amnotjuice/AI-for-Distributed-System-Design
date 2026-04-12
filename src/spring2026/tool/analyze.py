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
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from contextlib import suppress
from pathlib import Path

# Ensure src/ is on path
_SRC = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_SRC))
os.environ.setdefault("LITELLM_LOG", "ERROR")
logging.getLogger("eudoxia").setLevel(logging.CRITICAL)
logging.getLogger("simulation_utils").setLevel(logging.ERROR)

from spring2026.tool.config import (
    ESTIMATOR_CONDITIONS,
    EXPERIMENTS,
    EXPERIMENTS_DIR,
    PROJECT_ROOT,
    PROTOTYPE_OVERRIDES,
    RESULTS_DIR,
    SCHEDULERS_DIR,
    SPRING2026_DIR,
    TRACES_DIR,
    TWO_SHOT_PERF_CONDITIONS,
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

        values = [float(v) for v in extract_metrics_from_stats(raw, metric, base_params=base_params)]
        med = float(statistics.median(values))
        beats = bool(med < baseline_median) if metric == "latency" else bool(med > baseline_median)
        imp = float((med - baseline_median) / baseline_median * 100) if baseline_median else None

        # --- Tyler SimulatorStats fields (exact names, summed/meaned across runs) ---
        total_pipelines_created = sum(s.pipelines_created for s in raw)
        total_containers_completed = sum(s.containers_completed for s in raw)
        throughput = statistics.mean(s.throughput for s in raw)
        p99_latency = statistics.mean(s.p99_latency for s in raw)
        total_assignments = sum(s.assignments for s in raw)
        total_suspensions = sum(s.suspensions for s in raw)
        total_failures = sum(s.failures for s in raw)
        mean_memory_allocated_percent = statistics.mean(s.mean_memory_allocated_percent for s in raw)
        mean_memory_consumed_percent = statistics.mean(s.mean_memory_consumed_percent for s in raw)
        failure_error_counts: dict = {}
        for s in raw:
            for k, v in s.failure_error_counts.items():
                failure_error_counts[k] = failure_error_counts.get(k, 0) + v

        # --- Tyler PipelineStats fields (flattened with category prefix) ---
        def _agg_pipeline_stats(attr: str) -> dict:
            arrivals = sum(getattr(s, attr).arrival_count for s in raw)
            completions = sum(getattr(s, attr).completion_count for s in raw)
            timeouts = sum(getattr(s, attr).timeout_count for s in raw)
            w_lat = sum(
                getattr(s, attr).mean_latency_seconds * getattr(s, attr).completion_count
                for s in raw if getattr(s, attr).completion_count > 0
            )
            w_count = sum(getattr(s, attr).completion_count for s in raw)
            mean_latency_seconds = w_lat / w_count if w_count > 0 else None
            p99s = [getattr(s, attr).p99_latency_seconds for s in raw
                    if getattr(s, attr).completion_count > 0]
            p99_latency_seconds = statistics.mean(p99s) if p99s else None
            return {
                f"{attr}_arrival_count": arrivals,
                f"{attr}_completion_count": completions,
                f"{attr}_timeout_count": timeouts,
                f"{attr}_mean_latency_seconds": mean_latency_seconds,
                f"{attr}_p99_latency_seconds": p99_latency_seconds,
            }

        ps_all = _agg_pipeline_stats("pipelines_all")
        ps_query = _agg_pipeline_stats("pipelines_query")
        ps_interactive = _agg_pipeline_stats("pipelines_interactive")
        ps_batch = _agg_pipeline_stats("pipelines_batch")

        # --- Derived fields (not directly in Tyler, use old naming convention) ---
        oom_count = failure_error_counts.get("OOM", 0)
        total_arrivals = ps_all["pipelines_all_arrival_count"]
        total_completions = ps_all["pipelines_all_completion_count"]
        completion_rate = total_completions / total_arrivals if total_arrivals > 0 else None
        suspension_rate = total_suspensions / total_assignments if total_assignments > 0 else None

        def _completion_rate(ps: dict, attr: str) -> float | None:
            arr = ps[f"{attr}_arrival_count"]
            comp = ps[f"{attr}_completion_count"]
            return comp / arr if arr > 0 else None

        return {
            "functional": True, "failure_mode": "success",
            f"median_{metric}": med, "metric_values": values,
            "beats_baseline": beats, "improvement_pct": imp,
            # Tyler SimulatorStats (exact names)
            "pipelines_created": total_pipelines_created,
            "containers_completed": total_containers_completed,
            "throughput": throughput,
            "p99_latency": p99_latency,
            "assignments": total_assignments,
            "suspensions": total_suspensions,
            "failures": total_failures,
            "failure_error_counts": failure_error_counts,
            "mean_memory_allocated_percent": mean_memory_allocated_percent,
            "mean_memory_consumed_percent": mean_memory_consumed_percent,
            # Tyler PipelineStats (exact names, flattened)
            **ps_all,
            **ps_query,
            **ps_interactive,
            **ps_batch,
            # Derived
            "oom_count": oom_count,
            "suspension_rate": suspension_rate,
            "completion_rate": completion_rate,
            "completion_rate_query": _completion_rate(ps_query, "pipelines_query"),
            "completion_rate_interactive": _completion_rate(ps_interactive, "pipelines_interactive"),
            "completion_rate_batch": _completion_rate(ps_batch, "pipelines_batch"),
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
    subprocess_timeout = base_params.get("subprocess_timeout", None)
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


def _evaluate_one(
    fp: Path,
    trace_files: list[str],
    baseline_med: float,
    metric: str,
    base_params: dict,
) -> dict:
    """Build a complete record for a single scheduler file (usable as worker)."""
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
    return record


def run_analyze(
    scheduler_dirs: list[Path],
    trace_files: list[str],
    output_dir: Path,
    base_params: dict,
    metric: str = "latency",
    exp_label: str = "",
    workers: int = 1,
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
    baseline_med = statistics.median(extract_metrics_from_stats(naive_raw, metric, base_params=baseline_params))
    print(f"Baseline median {metric}: {baseline_med:.4f}")

    scheduler_files = []
    for d in scheduler_dirs:
        scheduler_files.extend(sorted(d.glob("scheduler_*.py")))
    assert scheduler_files, f"No scheduler_*.py found in {scheduler_dirs}"
    print(f"{len(trace_files)} traces | {len(scheduler_files)} schedulers | metric={metric}"
          f"{' | ' + exp_label if exp_label else ''} | workers={workers}")

    if existing:
        print(f"Resuming: {len(existing)} existing records")

    pending = [fp for fp in scheduler_files if fp.name not in existing]
    for fp in scheduler_files:
        if fp.name in existing:
            print(f"[skip] {fp.name}  already recorded")

    all_records = dict(existing)
    total = len(pending)

    def _append_record(fp: Path, record: dict, idx: int, out_f) -> None:
        all_records[fp.name] = record
        out_f.write(json.dumps(record) + "\n")
        out_f.flush()
        with suppress(OSError):
            os.fsync(out_f.fileno())
        ok = "OK" if record["functional"] else "FAIL"
        detail = (f"{metric}={record.get(f'median_{metric}', 'N/A')}"
                  if record["functional"] else record.get("failure_mode", ""))
        print(f"[{idx}/{total}] {fp.name}  {ok} {detail}")

    with output_path.open("a") as out_f:
        if workers <= 1:
            for i, fp in enumerate(pending, 1):
                record = _evaluate_one(fp, trace_files, baseline_med, metric, base_params)
                _append_record(fp, record, i, out_f)
        else:
            completed = 0
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(_evaluate_one, fp, trace_files, baseline_med, metric, base_params): fp
                    for fp in pending
                }
                for future in as_completed(futures):
                    fp = futures[future]
                    record = future.result()
                    completed += 1
                    _append_record(fp, record, completed, out_f)

    # Cleanup stray utility logs (eudoxia writes these to cwd = project root)
    for p in PROJECT_ROOT.glob("pool_*_utility.csv"):
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

def _run_probes_for_dir(sched_dir: Path, out_dir: Path, base_params: dict, workers: int = 1) -> None:
    """Run all probes on schedulers in sched_dir, write CSV to out_dir/probes.csv."""
    _probe_dir = Path(__file__).resolve().parent / "probe"
    sys.path.insert(0, str(_probe_dir))
    from run_probes import ensure_traces, run_all_probes, _worker, write_csv

    traces = ensure_traces()
    scheduler_files = sorted(sched_dir.glob("scheduler_*.py"))
    if not scheduler_files:
        return

    n = len(scheduler_files)
    results_by_file: dict[Path, dict] = {}

    if workers <= 1:
        for i, sf in enumerate(scheduler_files, 1):
            results = run_all_probes(sf, base_params, traces)
            passed = sum(1 for r in results.values() if r.get("functional"))
            print(f"  [{i}/{n}] {sf.name}: {passed}/{len(results)}")
            results_by_file[sf] = results
    else:
        import multiprocessing as _mp
        probe_timeout = base_params.get("subprocess_timeout", 120)
        pool = _mp.Pool(processes=workers)
        async_results = [(sf, pool.apply_async(_worker, ((sf, base_params, traces),)))
                         for sf in scheduler_files]
        pool.close()
        for i, (sf, ar) in enumerate(async_results, 1):
            try:
                _, results = ar.get(timeout=probe_timeout)
            except _mp.TimeoutError:
                results = {"_probe": {"functional": False, "failure_mode": "timeout"}}
                print(f"  [{i}/{n}] {sf.name}: TIMEOUT after {probe_timeout}s")
            except Exception as e:
                results = {"_probe": {"functional": False, "failure_mode": "probe_error",
                                      "error_message": str(e)}}
            results_by_file[sf] = results
            passed = sum(1 for r in results.values() if r.get("functional"))
            print(f"  [{i}/{n}] {sf.name}: {passed}/{len(results)}")
        pool.terminate()
        pool.join()

    all_results = [results_by_file[sf] for sf in scheduler_files]
    write_csv(scheduler_files, all_results, out_dir / "probes.csv")


def analyze_01_reasoning(prototype: bool, workers: int = 1, phase: str = "all") -> None:
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

        if phase in ("all", "probe"):
            print(f"\n--- Probes: {effort} ---")
            probe_params = base_params.copy()
            probe_params["duration"] = 120
            _run_probes_for_dir(sched_dir, out_dir, probe_params, workers=workers)

        if phase in ("all", "latency"):
            print(f"\n--- Latency: {effort} ---")
            run_analyze([sched_dir], trace_files, out_dir, base_params,
                        exp_label=f"reasoning={effort}", workers=workers)


def analyze_02_estimation(prototype: bool, workers: int = 1, phase: str = "all") -> None:
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

    if phase in ("all", "probe"):
        # Run probes once for the estimation schedulers
        out_base = RESULTS_DIR / "02_estimation"
        print("\n--- Probes: estimation ---")
        probe_params = base_params.copy()
        probe_params["duration"] = 120
        _run_probes_for_dir(sched_dir, out_base, probe_params, workers=workers)

    if phase in ("all", "latency"):
        # Evaluate under each sigma condition
        out_base = RESULTS_DIR / "02_estimation"
        for sigma_str, sigma_params in ESTIMATOR_CONDITIONS.items():
            out_dir = out_base / sigma_str
            print(f"\n--- Latency: {sigma_str} ---")

            if sigma_str == "no_estimation":
                src_jsonl = RESULTS_DIR / "01_reasoning" / "low" / "analysis.jsonl"
                out_dir.mkdir(parents=True, exist_ok=True)
                dst_jsonl = out_dir / "analysis.jsonl"
                if src_jsonl.exists():
                    import shutil
                    shutil.copy2(src_jsonl, dst_jsonl)
                    print(f"  [Retrieved] Data directly copied from {src_jsonl}")
                    continue
                else:
                    print(f"  WARNING: {src_jsonl} not found. Falling back to generating data manually.")
                    # Let it fall through instead of continue

            params = base_params.copy()
            params.update(sigma_params)
            run_analyze([sched_dir], trace_files, out_dir, params,
                        exp_label=f"estimation={sigma_str}", workers=workers)


def analyze_03_two_iter_best_worst(prototype: bool, workers: int = 1) -> None:
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
                    exp_label=f"two_iter={combo}", workers=workers)

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
    """Fig 4: two-iteration, median source, evaluate v2 on all 10 train traces."""
    base_params = get_canonical_base_params(prototype=prototype)
    base_params["_cluster_sizes"] = [1, 2, 4, 8, 16]
    cluster_sizes = base_params["_cluster_sizes"]

    all_trace_paths = sorted(TRACES_DIR.glob("bench_*_train.csv"))
    assert all_trace_paths, f"No bench_*_train.csv found in {TRACES_DIR}"
    trace_files = [str(p) for p in all_trace_paths]
    trace_names = [p.name for p in all_trace_paths]
    n_traces = len(trace_files)
    n_clusters = len(cluster_sizes)

    out_dir = RESULTS_DIR / "04_two_iter_all"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / "analysis.jsonl"
    existing = load_existing_records(output_path)

    # --- Source: median scheduler from 01_reasoning/low ---
    analysis_01 = RESULTS_DIR / "01_reasoning" / "low" / "analysis.jsonl"
    assert analysis_01.exists(), "Run analyze.py 01_reasoning first"
    src_pool = [r for r in load_existing_records(analysis_01).values()
                if r.get("functional") and r.get("median_latency") is not None]
    assert src_pool, "No functional schedulers in 01_reasoning/low"
    med_val = statistics.median(r["median_latency"] for r in src_pool)
    source_record = min(src_pool, key=lambda r: abs(r["median_latency"] - med_val))
    source_file = SCHEDULERS_DIR / "reasoning" / "low" / source_record["filename"]
    assert source_file.exists(), f"Source file not found: {source_file}"

    # --- Evaluate source on all traces (cached) ---
    source_cache = out_dir / "source.json"
    if source_cache.exists():
        source_data = json.loads(source_cache.read_text())
        if source_data["filename"] != source_record["filename"]:
            print(f"  Cache mismatch: cached={source_data['filename']}, "
                  f"current median={source_record['filename']} — re-evaluating.")
            source_cache.unlink()
            source_data = None
        else:
            print(f"Source (cached): {source_data['filename']}")
    else:
        source_data = None

    if source_data is None:
        print(f"Evaluating source: {source_record['filename']} on {n_traces} traces...")
        result = evaluate(source_file, trace_files, 0.0, "latency", base_params.copy())
        assert result.get("functional"), f"Source scheduler failed: {result.get('failure_mode')}"
        vals = result["metric_values"]
        per_trace_latency = {}
        for j, name in enumerate(trace_names):
            trace_vals = [vals[i * n_traces + j] for i in range(n_clusters)
                          if i * n_traces + j < len(vals)]
            finite = [v for v in trace_vals if v != float("inf")]
            per_trace_latency[name] = statistics.median(finite) if finite else float("inf")
        source_data = {
            "filename": source_record["filename"],
            "exp01_median_latency": source_record["median_latency"],
            "per_trace_latency": per_trace_latency,
        }
        source_cache.write_text(json.dumps(source_data, indent=2))
        print(f"  Source per-trace latencies saved.")

    per_trace_source = source_data["per_trace_latency"]

    # --- Collect v2 schedulers from median_simple and median_rich ---
    scheduler_files: list[tuple[Path, str]] = []
    for ctx in ["simple", "rich"]:
        sched_dir = SCHEDULERS_DIR / "two_iter" / f"median_{ctx}"
        if not sched_dir.exists() or not list(sched_dir.glob("scheduler_*.py")):
            print(f"  SKIP median_{ctx}: no schedulers in {sched_dir}")
            continue
        scheduler_files.extend((fp, ctx) for fp in sorted(sched_dir.glob("scheduler_*.py")))

    if not scheduler_files:
        print("No v2 schedulers found. Run: python generate.py --exp two_iter --source median --context simple/rich")
        return

    print(f"{n_traces} traces | {len(scheduler_files)} v2 schedulers")
    if existing:
        print(f"Resuming: {len(existing)} existing records")

    with output_path.open("a") as out_f:
        for i, (fp, ctx) in enumerate(scheduler_files, 1):
            if fp.name in existing:
                print(f"[{i}/{len(scheduler_files)}] {fp.name}  already recorded")
                continue
            print(f"[{i}/{len(scheduler_files)}] {fp.name}  (context={ctx})")
            t0 = time.time()
            result = evaluate(fp, trace_files, 0.0, "latency", base_params.copy())

            per_trace_latency: dict[str, float] = {}
            per_trace_beats: dict[str, bool] = {}
            beats_count = 0

            if result.get("functional") and result.get("metric_values"):
                vals = result["metric_values"]
                for j, name in enumerate(trace_names):
                    trace_vals = [vals[i * n_traces + j] for i in range(n_clusters)
                                  if i * n_traces + j < len(vals)]
                    finite = [v for v in trace_vals if v != float("inf")]
                    v2_lat = statistics.median(finite) if finite else float("inf")
                    src_lat = per_trace_source.get(name, float("inf"))
                    per_trace_latency[name] = v2_lat
                    per_trace_beats[name] = bool(v2_lat < src_lat)
                    if v2_lat < src_lat:
                        beats_count += 1

            record = {
                "filename": fp.name,
                "context": ctx,
                "source_filename": source_data["filename"],
                **parse_header(fp),
                "functional": result.get("functional", False),
                "failure_mode": result.get("failure_mode", "unknown"),
                "per_trace_latency": per_trace_latency,
                "per_trace_beats_source": per_trace_beats,
                "beats_source_count": beats_count,
                "n_traces": n_traces,
                "simulation_seconds": round(time.time() - t0, 2),
            }
            out_f.write(json.dumps(record) + "\n")
            out_f.flush()
            with suppress(OSError):
                os.fsync(out_f.fileno())

            status = "OK" if record["functional"] else "FAIL"
            print(f"  {status}  beats_source={beats_count}/{n_traces}")

    # --- Summary ---
    all_records = list(load_existing_records(output_path).values())
    print("=" * 50)
    for ctx in ["simple", "rich"]:
        ctx_recs = [r for r in all_records if r.get("context") == ctx and r.get("functional")]
        if not ctx_recs:
            continue
        total = sum(r["n_traces"] for r in ctx_recs)
        beats = sum(r["beats_source_count"] for r in ctx_recs)
        print(f"{ctx}: {beats}/{total} ({beats/total:.1%}) (scheduler, trace) pairs beat source")
    print(f"Output: {output_path}")


def _select_source_for_05(source: str) -> tuple[dict, Path]:
    """Select best/worst/median scheduler from exp01/low for exp05."""
    analysis_01 = RESULTS_DIR / "01_reasoning" / "low" / "analysis.jsonl"
    assert analysis_01.exists(), "Run analyze.py 01_reasoning first"
    src_pool = [r for r in load_existing_records(analysis_01).values()
                if r.get("functional") and r.get("median_latency") is not None]
    assert src_pool, "No functional schedulers in 01_reasoning/low"

    def _gmean(r: dict) -> float:
        finite = [v for v in r.get("metric_values", []) if v != float("inf") and v > 0]
        return statistics.geometric_mean(finite) if finite else float("inf")

    if source == "best":
        record = min(src_pool, key=_gmean)
    elif source == "worst":
        record = max(src_pool, key=_gmean)
    else:  # median
        med_val = statistics.median(_gmean(r) for r in src_pool)
        record = min(src_pool, key=lambda r: abs(_gmean(r) - med_val))

    path = SCHEDULERS_DIR / "reasoning" / "low" / record["filename"]
    assert path.exists(), f"Scheduler file not found: {path}"
    return record, path


def _analyze_05_source(prototype: bool, source: str) -> None:
    """Phase 'source': run source scheduler under each cheap sim condition, save rich stats."""
    record, path = _select_source_for_05(source)
    print(f"Source: {path.name}  full-sim latency={record['median_latency']:.4f}")

    base_params = get_canonical_base_params(prototype=prototype)
    base_params["_cluster_sizes"] = [1, 2, 4, 8, 16]
    canonical = str(TRACES_DIR / "bench_canonical_train.csv")

    out_dir = RESULTS_DIR / "05_two_shot_perf" / "source" / source
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "source_info.json").write_text(json.dumps({
        "source": source,
        "filename": record["filename"],
        "full_sim_median_latency": record["median_latency"],
    }, indent=2))

    for label, overrides in TWO_SHOT_PERF_CONDITIONS.items():
        if label == "dur3600_ticks100":
            continue  # full fidelity = same as exp01, skip
        out_file = out_dir / f"{label}.json"
        if out_file.exists():
            print(f"  SKIP {label}: already exists")
            continue
        cheap_params = base_params.copy()
        cheap_params.update(overrides)
        print(f"\n--- {label} (duration={overrides.get('duration')}, ticks={overrides.get('ticks_per_second')}) ---")
        result = evaluate(path, [canonical], 0.0, "latency", cheap_params)
        result["sim_label"] = label
        result["sim_overrides"] = overrides
        out_file.write_text(json.dumps(result, indent=2))
        status = "OK" if result.get("functional") else f"FAIL ({result.get('failure_mode')})"
        print(f"  {status}  median_latency={result.get('median_latency', 'N/A')}")


def _analyze_05_eval(prototype: bool, source: str, context: str, workers: int) -> None:
    """Phase 'eval': evaluate generated schedulers under full sim, compute improved_rate."""
    source_info_path = RESULTS_DIR / "05_two_shot_perf" / "source" / source / "source_info.json"
    assert source_info_path.exists(), (
        f"Run 'analyze.py 05_two_shot_perf source --source {source}' first"
    )
    source_info = json.loads(source_info_path.read_text())
    source_latency = source_info["full_sim_median_latency"]
    print(f"Source: {source_info['filename']}  full-sim latency={source_latency:.4f}")

    base_params = get_canonical_base_params(prototype=prototype)
    base_params["_cluster_sizes"] = [1, 2, 4, 8, 16]
    canonical = str(TRACES_DIR / "bench_canonical_train.csv")
    combo = f"{source}_{context}"

    for label in TWO_SHOT_PERF_CONDITIONS:
        if label == "dur3600_ticks100":
            continue
        sched_dir = SCHEDULERS_DIR / "two_shot_perf" / combo / label
        if not sched_dir.exists() or not list(sched_dir.glob("scheduler_*.py")):
            print(f"  SKIP {label}: no schedulers in {sched_dir}")
            continue

        out_dir = RESULTS_DIR / "05_two_shot_perf" / combo / label
        print(f"\n--- {label} ---")
        run_analyze([sched_dir], [canonical], out_dir, base_params.copy(),
                    exp_label=f"two_shot_perf={combo}/{label}", workers=workers)

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
            "source": source, "context": context, "sim_label": label,
            "source_median_latency": source_latency,
            "n_total": n_total, "n_functional": n_functional, "n_improved": n_improved,
            "improved_rate": n_improved / n_total if n_total > 0 else None,
            "improved_lo": lo, "improved_hi": hi,
        }
        (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))
        rate_str = f"{summary['improved_rate']:.1%}" if summary["improved_rate"] is not None else "N/A"
        print(f"  Summary: {n_improved}/{n_total} improved ({rate_str})")


def analyze_05_two_shot_perf(
    prototype: bool,
    phase: str = "all",
    source: str = "worst",
    context: str = "rich",
    workers: int = 1,
) -> None:
    """Fig 5: two-shot perf with shorter/coarser simulations."""
    if phase in ("source", "all"):
        _analyze_05_source(prototype, source)
    if phase in ("eval", "all"):
        _analyze_05_eval(prototype, source, context, workers)


def analyze_06_multi_iter(prototype: bool, dry_run: bool = False) -> None:
    """Fig 6: 10 scenarios × 50 iterations, latency vs iteration."""
    import csv as _csv
    import importlib.util
    import random
    import tomllib

    _spec = importlib.util.spec_from_file_location(
        "exp06_config", EXPERIMENTS_DIR / "06_multi_iter" / "config.py"
    )
    assert _spec is not None and _spec.loader is not None
    _cfg = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_cfg)  # type: ignore[union-attr]

    from datetime import datetime

    out_dir = RESULTS_DIR / "06_multi_iter"
    out_dir.mkdir(parents=True, exist_ok=True)

    n_scenarios = 2 if prototype else _cfg.N_SCENARIOS
    n_iterations = 3 if prototype else _cfg.N_ITERATIONS

    tag = ("_dryrun" if dry_run else "") + ("_prototype" if prototype else "")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"iterations_{timestamp}{tag}.csv"

    for old in out_dir.glob("iterations*.csv"):
        old.unlink()

    with (SPRING2026_DIR / "scenarios" / "scenarios.csv").open() as f:
        _all = list(_csv.DictReader(f))
    _stride = max(1, len(_all) // n_scenarios)
    scenarios = _all[::_stride][:n_scenarios]

    if not dry_run:
        seed_files = sorted((SCHEDULERS_DIR / "reasoning" / "low").glob("scheduler_*.py"))
        assert seed_files, "No seed schedulers in reasoning/low. Run analyze_01_reasoning first."

    with csv_path.open("w", newline="") as out_f:
        writer = _csv.writer(out_f)
        writer.writerow(["scenario_id", "iteration", "geomean_latency"])

        for s_idx, scenario in enumerate(scenarios):
            scenario_id = f"scenario_{s_idx:02d}"
            objective = scenario.get("objective", "")
            print(f"\n--- {scenario_id}: {Path(scenario['train_trace']).name} ---")
            sched_dir = SCHEDULERS_DIR / "multi_iter" / scenario_id
            sched_dir.mkdir(parents=True, exist_ok=True)

            iter_0 = sched_dir / "iter_000.py"
            if dry_run:
                if not iter_0.exists():
                    iter_0.write_text("# dry-run seed\n")
            else:
                iter_0.write_text(seed_files[0].read_text())  # type: ignore[possibly-undefined]

            base_params = get_canonical_base_params(prototype=prototype)
            toml_path = (SPRING2026_DIR / "scenarios" / scenario["params"]).resolve()
            with toml_path.open("rb") as f:
                base_params.update(tomllib.load(f))
            trace_file = str((SPRING2026_DIR / "scenarios" / scenario["train_trace"]).resolve())

            for it in range(n_iterations):
                iter_path = sched_dir / f"iter_{it:03d}.py"
                next_path = sched_dir / f"iter_{it + 1:03d}.py"

                try:
                    if dry_run:
                        random.seed(s_idx * 1000 + it)
                        gm = max(50.0, 500.0 * (0.97 ** it) + random.uniform(-10, 10))
                        next_path.write_text(f"# dry-run iter {it + 1}\n")
                    else:
                        from one_iteration_tool import (
                            evaluate_across_scales, build_improvement_prompt,
                            call_llm, extract_code, geometric_mean,
                        )
                        assert iter_path.exists(), f"Missing {iter_path}"
                        print(f"  [debug] scheduler : {iter_path} (exists={iter_path.exists()})")
                        print(f"  [debug] trace     : {trace_file} (exists={Path(trace_file).exists()})")
                        scale_results = evaluate_across_scales(iter_path, trace_file, [1, 2, 4, 8, 16], base_params)
                        valid = [r["latency"] for r in scale_results.values() if r.get("ok")]
                        gm = geometric_mean(valid) if valid else float("nan")
                        prompt = build_improvement_prompt(iter_path.read_text(), scale_results, base_params)
                        improved = extract_code(call_llm(prompt, _cfg.MODEL, _cfg.REASONING_EFFORT, False))
                        if not re.search(r"""@register_scheduler\((?:key=)?['"]([^'"]+)['"]\)""", improved):
                            raise ValueError("LLM output missing @register_scheduler key")
                        next_path.write_text(improved + "\n")
                except Exception as exc:
                    import traceback
                    print(f"  [{scenario_id}] iter {it + 1:>2}/{n_iterations}: ERROR — {exc}")
                    print(traceback.format_exc())
                    gm = float("nan")
                    if not next_path.exists():
                        next_path.write_text(iter_path.read_text())

                writer.writerow([scenario_id, it, round(gm, 4)])
                out_f.flush()
                print(f"  [{scenario_id}] iter {it + 1:>2}/{n_iterations}: geomean={gm:.4f}s")

    print(f"\nOutput: {csv_path}")


def _cross_eval_geomean_worker(args: tuple) -> tuple[str, str, float]:
    """Module-level worker: evaluate one scheduler across scales on one scenario, return geomean."""
    import math
    scheduler_scenario, eval_scenario, sched_path, trace_file, params, scales = args
    worker_script = Path(__file__).resolve().parent / "_one_iteration_worker.py"
    project_root = Path(__file__).resolve().parent.parent.parent.parent

    latencies = []
    for scale in scales:
        p = params.copy()
        p["cpus_per_pool"] = params["cpus_per_pool"] * scale
        p["ram_gb_per_pool"] = params["ram_gb_per_pool"] * scale
        cmd = [sys.executable, str(worker_script), sched_path, trace_file, json.dumps(p)]
        try:
            proc = subprocess.run(
                cmd, capture_output=True, text=True,
                cwd=str(project_root),
                timeout=p.get("subprocess_timeout", 120),
            )
            for line in reversed(proc.stdout.strip().split("\n")):
                if line.startswith("__RESULT__"):
                    result = json.loads(line[len("__RESULT__"):])
                    if result.get("ok"):
                        latencies.append(result["latency"])
                    break
        except Exception:
            pass

    if latencies:
        gm = math.exp(sum(math.log(max(v, 1e-12)) for v in latencies) / len(latencies))
    else:
        gm = float("nan")
    return scheduler_scenario, eval_scenario, gm


def analyze_07_cross_eval(prototype: bool, workers: int = 16) -> None:
    """Fig 7: cross-eval heatmap — scheduler X evaluated on workload Y."""
    import csv as _csv
    import math
    import tomllib
    from concurrent.futures import ProcessPoolExecutor, as_completed

    with (SPRING2026_DIR / "scenarios" / "scenarios.csv").open() as f:
        scenarios = list(_csv.DictReader(f))

    if prototype:
        # Prototype: use reasoning/high schedulers (quick smoke-test, scale=1 only)
        sched_dir = SCHEDULERS_DIR / "reasoning" / "high"
        sched_files = sorted(sched_dir.glob("scheduler_*.py"))
        assert sched_files, f"No schedulers in {sched_dir}"
        sched_files = sched_files[:12]
        scenarios = scenarios[:12]

        tasks = []
        for w_idx, scenario in enumerate(scenarios):
            eval_scenario = f"scenario_{w_idx:02d}"
            params = get_canonical_base_params(prototype=True)
            toml_path = (SPRING2026_DIR / "scenarios" / scenario["params"]).resolve()
            with toml_path.open("rb") as f:
                params.update(tomllib.load(f))
            trace_file = str((SPRING2026_DIR / "scenarios" / scenario["test_trace"]).resolve())
            for sched_path in sched_files:
                tasks.append((sched_path.stem, eval_scenario, str(sched_path), trace_file, params, [1]))
    else:
        # Full: for each scenario find the best scheduler produced by experiment 06
        # (lowest finite geomean_latency in results/06_multi_iter/iterations*.csv)
        iter_csvs = sorted((RESULTS_DIR / "06_multi_iter").glob("iterations*.csv"))
        assert iter_csvs, "No iterations CSV in results/06_multi_iter — run analyze 06 first"
        iter_csv = iter_csvs[-1]

        best: dict[str, tuple[int, float]] = {}  # scenario_id → (iteration, geomean)
        with iter_csv.open() as f:
            for row in _csv.DictReader(f):
                sid = row["scenario_id"]
                try:
                    gm = float(row["geomean_latency"])
                except ValueError:
                    continue
                if not math.isfinite(gm):
                    continue
                it = int(row["iteration"])
                if sid not in best or gm < best[sid][1]:
                    best[sid] = (it, gm)

        assert best, "No finite geomean rows found in 06 iterations CSV"

        best_scheds: dict[str, Path] = {}
        for sid, (it, _) in best.items():
            p = SCHEDULERS_DIR / "multi_iter" / sid / f"iter_{it:03d}.py"
            assert p.exists(), f"Missing best scheduler: {p}"
            best_scheds[sid] = p

        print(f"Best schedulers from 06 ({iter_csv.name}):")
        for sid in sorted(best_scheds):
            it, gm = best[sid]
            print(f"  {sid}: iter_{it:03d}.py  (geomean={gm:.4f}s)")

        tasks = []
        for w_idx, scenario in enumerate(scenarios):
            eval_scenario = f"scenario_{w_idx:02d}"
            params = get_canonical_base_params(prototype=False)
            toml_path = (SPRING2026_DIR / "scenarios" / scenario["params"]).resolve()
            with toml_path.open("rb") as f:
                params.update(tomllib.load(f))
            trace_file = str((SPRING2026_DIR / "scenarios" / scenario["test_trace"]).resolve())
            for sched_scenario in sorted(best_scheds):
                tasks.append((
                    sched_scenario, eval_scenario,
                    str(best_scheds[sched_scenario]), trace_file,
                    params, [1, 2, 4, 8, 16],
                ))

    out_dir = RESULTS_DIR / "07_cross_eval"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "cross_eval.csv"

    total = len(tasks)
    n_scheds = len({t[0] for t in tasks})
    n_evals = len({t[1] for t in tasks})
    print(f"Schedulers: {n_scheds}  Eval scenarios: {n_evals}  Total cells: {total}  Workers: {workers}")

    with csv_path.open("w", newline="") as out_f:
        writer = _csv.writer(out_f)
        writer.writerow(["scheduler_scenario", "eval_scenario", "geomean_latency"])

        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_cross_eval_geomean_worker, t): t for t in tasks}
            done = 0
            for future in as_completed(futures):
                sched_scenario, eval_scenario, gm = future.result()
                done += 1
                gm_str = f"{gm:.4f}" if math.isfinite(gm) else "nan"
                print(f"  [{done}/{total}] {sched_scenario} × {eval_scenario}: {gm_str}")
                writer.writerow([sched_scenario, eval_scenario, gm_str])
                out_f.flush()

    print(f"\nOutput: {csv_path}")


def analyze_08_adapt_speed(prototype: bool, dry_run: bool = False) -> None:
    """Fig 8: adapt a scheduler from scenario A to scenario B, latency vs iteration."""
    import csv as _csv
    import importlib.util
    import math
    import random
    import tomllib

    _spec = importlib.util.spec_from_file_location(
        "exp08_config", EXPERIMENTS_DIR / "08_adapt_speed" / "config.py"
    )
    assert _spec is not None and _spec.loader is not None
    _cfg = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_cfg)  # type: ignore[union-attr]

    with (SPRING2026_DIR / "scenarios" / "scenarios.csv").open() as f:
        all_scenarios = list(_csv.DictReader(f))

    # Use the same strided sampling as experiment 06 so scenario indices are consistent
    n_samp = 2 if prototype else _cfg.N_SCENARIOS
    _stride = max(1, len(all_scenarios) // n_samp)
    sampled_scenarios = all_scenarios[::_stride][:n_samp]

    # Prototype: one pair per direction using the two sampled scenarios, 3 iterations
    pairs = [(0, 1), (1, 0)] if prototype else _cfg.PAIRS
    n_iterations = 3 if prototype else _cfg.N_ITERATIONS

    # Load best-per-scenario from experiment 06
    best: dict[str, tuple[int, float]] = {}
    if not dry_run:
        iter_csvs = sorted((RESULTS_DIR / "06_multi_iter").glob("iterations*.csv"))
        assert iter_csvs, "No iterations CSV in results/06_multi_iter — run analyze 06 first"
        iter_csv = iter_csvs[-1]
        with iter_csv.open() as f:
            for row in _csv.DictReader(f):
                sid = row["scenario_id"]
                try:
                    gm = float(row["geomean_latency"])
                except ValueError:
                    continue
                if not math.isfinite(gm):
                    continue
                it = int(row["iteration"])
                if sid not in best or gm < best[sid][1]:
                    best[sid] = (it, gm)

    out_dir = RESULTS_DIR / "08_adapt_speed"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "adaptation.csv"

    with csv_path.open("w", newline="") as out_f:
        writer = _csv.writer(out_f)
        writer.writerow(["source_scenario", "target_scenario", "iteration", "geomean_latency"])

        for src_idx, tgt_idx in pairs:
            source_scenario = f"scenario_{src_idx:02d}"
            target_scenario = f"scenario_{tgt_idx:02d}"
            pair_label = f"{source_scenario}→{target_scenario}"
            print(f"\n--- {pair_label} ---")

            assert tgt_idx < len(sampled_scenarios), f"Target index {tgt_idx} out of range (sampled {len(sampled_scenarios)} scenarios)"
            tgt = sampled_scenarios[tgt_idx]
            base_params = get_canonical_base_params(prototype=prototype)
            toml_path = (SPRING2026_DIR / "scenarios" / tgt["params"]).resolve()
            with toml_path.open("rb") as f:
                base_params.update(tomllib.load(f))
            trace_file = str((SPRING2026_DIR / "scenarios" / tgt["train_trace"]).resolve())

            adapt_dir = SCHEDULERS_DIR / "adapt_speed" / f"{source_scenario}_to_{target_scenario}"
            adapt_dir.mkdir(parents=True, exist_ok=True)
            iter_0 = adapt_dir / "iter_000.py"

            if dry_run:
                if not iter_0.exists():
                    iter_0.write_text("# dry-run seed\n")
            else:
                assert source_scenario in best, (
                    f"{source_scenario} not in 06 results — check that 06 ran with enough scenarios"
                )
                src_it, src_gm = best[source_scenario]
                src_path = SCHEDULERS_DIR / "multi_iter" / source_scenario / f"iter_{src_it:03d}.py"
                assert src_path.exists(), f"Missing source scheduler: {src_path}"
                print(f"  Source: {source_scenario} iter_{src_it:03d}.py (geomean={src_gm:.4f}s on source)")
                iter_0.write_text(src_path.read_text())

            for it in range(n_iterations):
                iter_path = adapt_dir / f"iter_{it:03d}.py"
                next_path = adapt_dir / f"iter_{it + 1:03d}.py"

                try:
                    if dry_run:
                        random.seed(src_idx * 10000 + tgt_idx * 1000 + it)
                        gm = max(50.0, 500.0 * (0.97 ** it) + random.uniform(-10, 10))
                        next_path.write_text(f"# dry-run iter {it + 1}\n")
                    else:
                        from one_iteration_tool import (
                            evaluate_across_scales, build_improvement_prompt,
                            call_llm, extract_code, geometric_mean,
                        )
                        assert iter_path.exists(), f"Missing {iter_path}"
                        scale_results = evaluate_across_scales(iter_path, trace_file, [1, 2, 4, 8, 16], base_params)
                        valid = [r["latency"] for r in scale_results.values() if r.get("ok")]
                        gm = geometric_mean(valid) if valid else float("nan")
                        prompt = build_improvement_prompt(iter_path.read_text(), scale_results, base_params)
                        improved = extract_code(call_llm(prompt, _cfg.MODEL, _cfg.REASONING_EFFORT, False))
                        if not re.search(r"""@register_scheduler\((?:key=)?['"]([^'"]+)['"]\)""", improved):
                            raise ValueError("LLM output missing @register_scheduler key")
                        next_path.write_text(improved + "\n")
                except Exception as exc:
                    import traceback
                    print(f"  [{pair_label}] iter {it:>2}/{n_iterations}: ERROR — {exc}")
                    print(traceback.format_exc())
                    gm = float("nan")
                    if not next_path.exists():
                        next_path.write_text(iter_path.read_text())

                writer.writerow([source_scenario, target_scenario, it, round(gm, 4)])
                out_f.flush()
                print(f"  [{pair_label}] iter {it + 1:>2}/{n_iterations}: geomean={gm:.4f}s")

    print(f"\nOutput: {csv_path}")


def analyze_09_general_purpose(prototype: bool, dry_run: bool = False) -> None:
    """Fig 9: one scheduler optimized across all scenarios simultaneously."""
    import csv as _csv
    import importlib.util
    import math
    import random
    import tomllib

    _spec = importlib.util.spec_from_file_location(
        "exp09_config", EXPERIMENTS_DIR / "09_general_purpose" / "config.py"
    )
    assert _spec is not None and _spec.loader is not None
    _cfg = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_cfg)  # type: ignore[union-attr]

    n_scenarios = 3 if prototype else _cfg.N_SCENARIOS
    n_iterations = 3 if prototype else _cfg.N_ITERATIONS

    with (SPRING2026_DIR / "scenarios" / "scenarios.csv").open() as f:
        _all = list(_csv.DictReader(f))
    _stride = max(1, len(_all) // n_scenarios)
    scenarios = _all[::_stride][:n_scenarios]

    # Pre-load per-scenario trace files and params (done once, reused every iteration)
    scenario_ids = [f"scenario_{i:02d}" for i in range(n_scenarios)]
    scenario_traces: list[str] = []
    scenario_params: list[dict] = []
    for scenario in scenarios:
        bp = get_canonical_base_params(prototype=prototype)
        toml_path = (SPRING2026_DIR / "scenarios" / scenario["params"]).resolve()
        with toml_path.open("rb") as f:
            bp.update(tomllib.load(f))
        scenario_params.append(bp)
        scenario_traces.append(str((SPRING2026_DIR / "scenarios" / scenario["train_trace"]).resolve()))

    sched_dir = SCHEDULERS_DIR / "general_purpose"
    sched_dir.mkdir(parents=True, exist_ok=True)
    iter_0 = sched_dir / "iter_000.py"
    if dry_run:
        if not iter_0.exists():
            iter_0.write_text("# dry-run seed\n")
    else:
        seed_files = sorted((SCHEDULERS_DIR / "reasoning" / "low").glob("scheduler_*.py"))
        assert seed_files, "No seed schedulers in reasoning/low. Run analyze_01_reasoning first."
        iter_0.write_text(seed_files[0].read_text())  # type: ignore[possibly-undefined]

    out_dir = RESULTS_DIR / "09_general_purpose"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "iterations.csv"

    with csv_path.open("w", newline="") as out_f:
        writer = _csv.writer(out_f)
        writer.writerow(["iteration", "overall_geomean"] + [f"{sid}_latency" for sid in scenario_ids])

        for it in range(n_iterations):
            iter_path = sched_dir / f"iter_{it:03d}.py"
            next_path = sched_dir / f"iter_{it + 1:03d}.py"
            scenario_gms: list[float] = []

            try:
                if dry_run:
                    for i in range(n_scenarios):
                        random.seed(i * 1000 + it)
                        scenario_gms.append(max(50.0, 500.0 * (0.97 ** it) + random.uniform(-30, 30)))
                    next_path.write_text(f"# dry-run iter {it + 1}\n")
                else:
                    from one_iteration_tool import (
                        evaluate_across_scales, call_llm, extract_code, geometric_mean,
                    )
                    assert iter_path.exists(), f"Missing {iter_path}"
                    for sid, trace_file, bp in zip(scenario_ids, scenario_traces, scenario_params):
                        print(f"  [iter {it:02d}] {sid}...", flush=True)
                        scale_results = evaluate_across_scales(iter_path, trace_file, [1, 2, 4, 8, 16], bp)
                        valid = [r["latency"] for r in scale_results.values() if r.get("ok")]
                        scenario_gms.append(geometric_mean(valid) if valid else float("nan"))

                    finite = [g for g in scenario_gms if math.isfinite(g)]
                    overall_gm = geometric_mean(finite) if finite else float("nan")

                    scheduler_code = iter_path.read_text()
                    m = re.search(r"""@register_scheduler\((?:key=)?['"]([^'"]+)['"]\)""", scheduler_code)
                    policy_key = m.group(1) if m else "unknown"
                    perf_lines = "\n".join(
                        f"  {sid}: {gm:.4f}s" if math.isfinite(gm) else f"  {sid}: FAILED"
                        for sid, gm in zip(scenario_ids, scenario_gms)
                    )
                    prompt = (
                        f"## Performance Results (geomean over cluster scales 1x–16x, per workload)\n\n"
                        f"{perf_lines}\n\n"
                        f"Overall geomean across {n_scenarios} workloads: {overall_gm:.4f}s\n\n"
                        f"## Current Scheduler Code\n\n{scheduler_code}\n\n"
                        f"Produce an improved version that minimizes the overall geomean across all "
                        f"workloads. Use the EXACT key \"{policy_key}\" in both "
                        f"@register_scheduler_init and @register_scheduler. "
                        f"Output ONLY the complete Python code."
                    )
                    improved = extract_code(call_llm(prompt, _cfg.MODEL, _cfg.REASONING_EFFORT, False))
                    if not re.search(r"""@register_scheduler\((?:key=)?['"]([^'"]+)['"]\)""", improved):
                        raise ValueError("LLM output missing @register_scheduler key")
                    next_path.write_text(improved + "\n")

            except Exception as exc:
                import traceback
                print(f"  [iter {it:02d}] ERROR — {exc}")
                print(traceback.format_exc())
                scenario_gms = [float("nan")] * n_scenarios
                if not next_path.exists():
                    next_path.write_text(iter_path.read_text())

            finite = [g for g in scenario_gms if math.isfinite(g)]
            overall_gm = geometric_mean(finite) if finite else float("nan")  # type: ignore[possibly-undefined]
            row = [it, round(overall_gm, 4) if math.isfinite(overall_gm) else "nan"]
            row += [round(g, 4) if math.isfinite(g) else "nan" for g in scenario_gms]
            writer.writerow(row)
            out_f.flush()
            per_s = "  ".join(f"{g:.2f}" if math.isfinite(g) else "nan" for g in scenario_gms)
            print(f"  iter {it + 1:>2}/{n_iterations}: overall={overall_gm:.4f}s  [{per_s}]")

    print(f"\nOutput: {csv_path}")


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
    import inspect
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "experiment",
        choices=list(HANDLERS) + ["all"],
        help="Experiment to analyze, or 'all' to run all",
    )
    parser.add_argument(
        "phase",
        nargs="?",
        default="all",
        choices=["all", "latency", "probe", "source", "eval"],
        help="Phase to run. 'latency'/'probe' for 01/02. 'source'/'eval' for 05_two_shot_perf. "
             "'all' runs everything (default).",
    )
    parser.add_argument(
        "--prototype", action="store_true",
        help="Fast/cheap run (1-min sims, validates infra only)",
    )
    parser.add_argument(
        "--workers", type=int, default=8,
        help="Number of parallel workers for probe and latency evaluation (default: 8).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Skip LLM/simulator calls; use fake data. Only applies to experiments that support it.",
    )
    parser.add_argument(
        "--source", default="worst", choices=["best", "worst", "median"],
        help="Source scheduler type for 05_two_shot_perf (default: worst).",
    )
    parser.add_argument(
        "--context", default="rich", choices=["simple", "rich"],
        help="Feedback context for 05_two_shot_perf eval phase (default: rich).",
    )
    args = parser.parse_args()

    exps = list(HANDLERS) if args.experiment == "all" else [args.experiment]
    for exp in exps:
        print(f"\n{'='*60}")
        print(f"Experiment: {exp} — {EXPERIMENTS.get(exp, '')}")
        if args.prototype:
            print("  [PROTOTYPE MODE — results not meaningful]")
        if args.phase != "all":
            print(f"  [PHASE: {args.phase} only]")
        print("=" * 60)
        handler = HANDLERS[exp]
        sig = inspect.signature(handler).parameters
        kwargs: dict = {"prototype": args.prototype}
        if "workers" in sig:
            kwargs["workers"] = args.workers
        if "dry_run" in sig:
            kwargs["dry_run"] = args.dry_run
        if "phase" in sig:
            kwargs["phase"] = args.phase
        if "source" in sig:
            kwargs["source"] = args.source
        if "context" in sig:
            kwargs["context"] = args.context
        handler(**kwargs)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        sys.exit(_worker_entrypoint(sys.argv[2:]))
    main()
