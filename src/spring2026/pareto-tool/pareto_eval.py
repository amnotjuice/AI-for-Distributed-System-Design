#!/usr/bin/env python3
"""
pareto_eval.py — Evaluate a directory of schedulers and find Pareto-optimal ones.

Usage:
    python pareto_eval.py schedulers/reasoning/high/
    python pareto_eval.py schedulers/reasoning/high/ --prototype
    python pareto_eval.py schedulers/reasoning/high/ --hard-probes

Each scheduler is evaluated in an isolated subprocess with a timeout so that
stuck or infinitely-looping schedulers do not block the run.

Dimensions used for Pareto dominance (matches Fig 2 metrics):
    - probe_syntax, probe_valid_scheduler, probe_basic_run, probe_retry_run,
      probe_suspend_run, probe_grouping, probe_overcommit, probe_priority_ordering,
      probe_starvation, probe_no_deadlock  (each maximize, binary 0/1)
    - adjusted_latency  (minimize — SimulatorStats.adjusted_latency() median)

With --hard-probes: only schedulers passing all probes are considered,
Pareto is then over adjusted_latency only.

Results saved to results/pareto/<scheduler_dir_name>/
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

_HERE  = Path(__file__).resolve().parent.parent   # src/spring2026/
_SRC   = _HERE.parent                             # src/
_TOOL  = _HERE / "tool"
_PROBE = _TOOL / "probe"

for p in [str(_SRC), str(_TOOL), str(_PROBE)]:
    if p not in sys.path:
        sys.path.insert(0, p)

os.environ.setdefault("LITELLM_LOG", "ERROR")
logging.getLogger("eudoxia").setLevel(logging.CRITICAL)


# ---------------------------------------------------------------------------
# Subprocess worker entry point
# Called as:  python pareto_eval.py --worker <scheduler_file> <trace_file>
#             <base_params_json> <traces_json>
# Prints one line:  __RESULT__{...json...}
# ---------------------------------------------------------------------------

def _worker_main(argv: list[str]) -> int:
    if len(argv) != 4:
        print(json.dumps({"ok": False, "reason": "bad_args"}))
        return 2

    scheduler_file_str, trace_file, base_params_json, traces_json = argv
    scheduler_file = Path(scheduler_file_str)
    base_params    = json.loads(base_params_json)
    traces         = json.loads(traces_json)

    from simulation_utils import get_raw_stats_for_policy, extract_metrics_from_stats, deregister_scheduler
    from run_probes import run_all_probes

    # ── probes ────────────────────────────────────────────────────────────
    PROBE_NAMES = [
        "syntax", "valid_scheduler", "basic_run", "retry_run", "suspend_run",
        "grouping", "overcommit", "priority_ordering", "starvation", "no_deadlock",
    ]
    probe_results = run_all_probes(scheduler_file, base_params, traces)
    probe_score   = sum(1 for v in probe_results.values() if v.get("functional"))
    probe_total   = len(probe_results)
    # Individual binary pass/fail per probe (1=pass, 0=fail/missing)
    probe_binary  = {f"probe_{p}": int(probe_results.get(p, {}).get("functional", False))
                     for p in PROBE_NAMES}

    # ── latency ───────────────────────────────────────────────────────────
    src = scheduler_file.read_text()
    m = re.search(r"""@register_scheduler\((?:key=)?['"]([^'"]+)['"]\)""", src)
    if not m:
        print("__RESULT__" + json.dumps({
            "ok": False, "reason": "no_key",
            "probe_score": probe_score, "probe_total": probe_total,
        }))
        return 0

    key = m.group(1)
    try:
        from typing import List, Tuple
        from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
        from eudoxia.scheduler.decorators import register_scheduler, register_scheduler_init
        from eudoxia.utils import Priority
        from eudoxia.workload import OperatorState, Pipeline
        from eudoxia.workload.runtime_status import ASSIGNABLE_STATES

        deregister_scheduler(key)
        exec(src, {
            "__builtins__": __builtins__, "__name__": "__worker__",
            "List": List, "Tuple": Tuple,
            "Pipeline": Pipeline, "OperatorState": OperatorState,
            "ASSIGNABLE_STATES": ASSIGNABLE_STATES,
            "Assignment": Assignment, "ExecutionResult": ExecutionResult,
            "Suspend": Suspend,
            "register_scheduler_init": register_scheduler_init,
            "register_scheduler": register_scheduler,
            "Priority": Priority,
        })
    except Exception as e:
        print("__RESULT__" + json.dumps({
            "ok": False, "reason": f"exec_error: {e}",
            "probe_score": probe_score, "probe_total": probe_total,
        }))
        return 0

    try:
        raw = get_raw_stats_for_policy(base_params, [trace_file], key)
        if not raw:
            print("__RESULT__" + json.dumps({
                "ok": False, "reason": "no_completions",
                "probe_score": probe_score, "probe_total": probe_total,
            }))
            return 0

        def _wm(attr):
            ws, wt = 0.0, 0
            for s in raw:
                st = getattr(s, attr, None)
                if st and st.completion_count > 0:
                    ws += st.mean_latency_seconds * st.completion_count
                    wt += st.completion_count
            return ws / wt if wt > 0 else None

        def _cr(attr):
            arr  = sum(getattr(s, attr).arrival_count    for s in raw if hasattr(s, attr))
            comp = sum(getattr(s, attr).completion_count for s in raw if hasattr(s, attr))
            return comp / arr if arr > 0 else None

        adj_vals = extract_metrics_from_stats(raw, "latency")
        result = {
            "ok":                    True,
            "probe_score":           probe_score,
            "probe_total":           probe_total,
            **probe_binary,
            "latency_query_s":       _wm("pipelines_query"),
            "latency_interactive_s": _wm("pipelines_interactive"),
            "latency_batch_s":       _wm("pipelines_batch"),
            "completion_rate":       _cr("pipelines_all") if hasattr(raw[0], "pipelines_all") else None,
            "adjusted_latency":      statistics.median(adj_vals) if adj_vals else None,
        }
    except Exception as e:
        result = {
            "ok": False, "reason": str(e),
            "probe_score": probe_score, "probe_total": probe_total,
        }
    finally:
        deregister_scheduler(key)

    print("__RESULT__" + json.dumps(result))
    return 0


# ---------------------------------------------------------------------------
# Parent-side: run one scheduler in a subprocess with a timeout
# ---------------------------------------------------------------------------

def evaluate_scheduler(
    scheduler_file: Path,
    trace_file: str,
    base_params: dict,
    traces: dict[str, str],
    timeout_s: int,
) -> dict:
    cmd = [
        sys.executable, str(Path(__file__).resolve()),
        "--worker",
        str(scheduler_file),
        trace_file,
        json.dumps(base_params),
        json.dumps(traces),
    ]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=timeout_s, cwd=str(_SRC),
        )
        for line in reversed(proc.stdout.strip().splitlines()):
            if line.startswith("__RESULT__"):
                return json.loads(line[len("__RESULT__"):])
        snippet = (proc.stderr or "")[-300:] or "no output"
        return {"ok": False, "reason": f"no_result_line: {snippet}",
                "probe_score": 0, "probe_total": 10}
    except subprocess.TimeoutExpired:
        return {"ok": False, "reason": "timeout", "probe_score": 0, "probe_total": 10}
    except Exception as e:
        return {"ok": False, "reason": str(e), "probe_score": 0, "probe_total": 10}


# ---------------------------------------------------------------------------
# Pareto computation
# ---------------------------------------------------------------------------

def _dominates(b: dict, a: dict, dims: list[tuple[str, str]]) -> bool:
    """True if b dominates a (at least as good on all dims, strictly better on one)."""
    at_least_as_good = True
    strictly_better  = False
    for field, direction in dims:
        va, vb = a.get(field), b.get(field)
        if va is None or vb is None:
            return False
        if direction == "min":
            if vb > va:   at_least_as_good = False; break
            if vb < va:   strictly_better = True
        else:
            if vb < va:   at_least_as_good = False; break
            if vb > va:   strictly_better = True
    return at_least_as_good and strictly_better


def compute_pareto(records: list[dict], dims: list[tuple[str, str]]) -> list[dict]:
    return [
        a for i, a in enumerate(records)
        if not any(_dominates(b, a, dims) for j, b in enumerate(records) if i != j)
    ]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scheduler_dir", type=Path)
    parser.add_argument("--prototype",   action="store_true",
                        help="Fast mode: 1-min simulation (results approximate)")
    parser.add_argument("--hard-probes", action="store_true",
                        help="Exclude schedulers failing any probe; Pareto over latencies only")
    parser.add_argument("--timeout",     type=int, default=None,
                        help="Per-scheduler timeout in seconds (default: 120 prototype, 600 full)")
    parser.add_argument("--trace",       type=str, default=None,
                        help="Override trace file")
    parser.add_argument("--out-dir",     type=str, default=None,
                        help="Override output directory (default: results/pareto/<group>)")
    args = parser.parse_args()

    from config import get_canonical_base_params, TRACES_DIR, RESULTS_DIR
    from run_probes import ensure_traces

    sched_dir = args.scheduler_dir.resolve()
    scheduler_files = sorted(sched_dir.glob("scheduler_*.py"))
    if not scheduler_files:
        print(f"No scheduler_*.py files in {sched_dir}"); sys.exit(1)

    base_params = get_canonical_base_params(prototype=args.prototype)
    timeout_s   = args.timeout or (120 if args.prototype else 600)
    trace_file  = str(Path(args.trace).resolve()) if args.trace else str(TRACES_DIR / "bench_canonical_train.csv")

    if not Path(trace_file).exists():
        print(f"Trace not found: {trace_file}\nRun:  python traces/generate.py"); sys.exit(1)

    if args.out_dir:
        out_dir = Path(args.out_dir) / sched_dir.name
    else:
        out_dir = RESULTS_DIR / "pareto" / sched_dir.name
    out_dir.mkdir(parents=True, exist_ok=True)

    n         = len(scheduler_files)
    mode      = "prototype (1-min)" if args.prototype else "full (60-min)"
    run_start = time.time()
    print(f"\nEvaluating {n} schedulers in {sched_dir.name}/  [{mode}]  timeout={timeout_s}s/scheduler")
    print(f"Trace: {Path(trace_file).name}\n")

    # Ensure probe traces exist before spawning subprocesses
    traces = ensure_traces()

    raw_path = out_dir / "results.jsonl"
    existing: dict[str, dict] = {}
    if raw_path.exists():
        with raw_path.open() as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        r = json.loads(line)
                        if fn := r.get("filename"):
                            existing[fn] = r
                    except json.JSONDecodeError:
                        pass
    if existing:
        print(f"Resuming: {len(existing)} already recorded.\n")

    print("-" * 78)
    print(f"{'Scheduler':<32} {'Status':>10} {'Q-lat':>8} {'R-lat':>8} {'B-lat':>8} {'Probes':>7} {'Elapsed':>8}")
    print("-" * 78)

    all_records: list[dict] = list(existing.values())

    with raw_path.open("a") as out_f:
        for i, sf in enumerate(scheduler_files, 1):
            def _elapsed() -> str:
                s = int(time.time() - run_start)
                return f"{s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}"

            if sf.name in existing:
                r = existing[sf.name]
                lq = f"{r['latency_query_s']:.1f}s"       if r.get("latency_query_s")       is not None else "—"
                lr = f"{r['latency_interactive_s']:.1f}s" if r.get("latency_interactive_s") is not None else "—"
                lb = f"{r['latency_batch_s']:.1f}s"       if r.get("latency_batch_s")        is not None else "—"
                ps = f"{r.get('probe_score',0)}/{r.get('probe_total',10)}"
                print(f"  [{i:2d}/{n}] {sf.name:<30}  {'(cached)':>10} {lq:>8} {lr:>8} {lb:>8} {ps:>7} {_elapsed():>8}")
                continue

            result = evaluate_scheduler(sf, trace_file, base_params, traces, timeout_s)
            record = {"filename": sf.name, **result}
            all_records.append(record)
            out_f.write(json.dumps(record) + "\n")
            out_f.flush()

            ok = result.get("ok", False)
            ps = f"{result.get('probe_score',0)}/{result.get('probe_total',10)}"
            if ok:
                lq = f"{result['latency_query_s']:.1f}s"       if result.get("latency_query_s")       is not None else "—"
                lr = f"{result['latency_interactive_s']:.1f}s" if result.get("latency_interactive_s") is not None else "—"
                lb = f"{result['latency_batch_s']:.1f}s"       if result.get("latency_batch_s")        is not None else "—"
                print(f"  [{i:2d}/{n}] {sf.name:<30}  {'ok':>10} {lq:>8} {lr:>8} {lb:>8} {ps:>7} {_elapsed():>8}")
            else:
                reason = result.get("reason", "?")[:18]
                print(f"  [{i:2d}/{n}] {sf.name:<30}  {reason:>10} {'—':>8} {'—':>8} {'—':>8} {ps:>7} {_elapsed():>8}")

    # ── Pareto ───────────────────────────────────────────────────────────────
    functional = [r for r in all_records if r.get("ok")]
    print("\n" + "-" * 70)
    print(f"{len(functional)}/{len(all_records)} schedulers produced valid latency data.")

    PROBE_DIMS = [
        ("probe_syntax","max"), ("probe_valid_scheduler","max"), ("probe_basic_run","max"),
        ("probe_retry_run","max"), ("probe_suspend_run","max"), ("probe_grouping","max"),
        ("probe_overcommit","max"), ("probe_priority_ordering","max"),
        ("probe_starvation","max"), ("probe_no_deadlock","max"),
    ]

    if args.hard_probes:
        candidates = [r for r in functional if r.get("probe_score", 0) == r.get("probe_total", 10)]
        print(f"{len(candidates)} pass all probes (--hard-probes mode).")
        dims = [("adjusted_latency", "min")]
        dim_label = "adjusted_latency"
    else:
        candidates = functional
        dims = PROBE_DIMS + [("adjusted_latency", "min")]
        dim_label = "probe_* (10 binary) × adjusted_latency"

    if not candidates:
        print("No candidates for Pareto analysis."); return

    pareto = compute_pareto(candidates, dims)
    pareto.sort(key=lambda r: r.get("latency_query_s") or float("inf"))

    # ── Unique Pareto: deduplicate on identical objective vectors ─────────────
    PROBE_NAMES_LIST = [
        "probe_syntax", "probe_valid_scheduler", "probe_basic_run", "probe_retry_run",
        "probe_suspend_run", "probe_grouping", "probe_overcommit", "probe_priority_ordering",
        "probe_starvation", "probe_no_deadlock",
    ]
    def _obj_key(r):
        probes = tuple(r.get(p, 0) for p in PROBE_NAMES_LIST)
        al = round(r.get("adjusted_latency") or 0, 1)
        return probes + (al,)

    seen_keys = set()
    unique_pareto = []
    for r in pareto:
        k = _obj_key(r)
        if k not in seen_keys:
            seen_keys.add(k)
            unique_pareto.append(r)

    pct_pareto = 100.0 * len(pareto) / len(candidates) if candidates else 0.0
    pct_unique = 100.0 * len(unique_pareto) / len(candidates) if candidates else 0.0
    print("\n" + "=" * 78)
    print(f"PARETO-OPTIMAL SCHEDULERS  --  {len(pareto)} total  ({pct_pareto:.1f}%)  |  {len(unique_pareto)} unique  ({pct_unique:.1f}%)")
    print(f"Objective space: {dim_label}")
    print("=" * 78)
    print(f"{'Scheduler':<32} {'Adj-lat':>10} {'Q-lat':>8} {'R-lat':>8} {'B-lat':>8} {'Probes':>7} {'Unique':>7}")
    print("-" * 78)
    unique_keys_set = {_obj_key(r) for r in unique_pareto}
    for r in pareto:
        al = f"{r['adjusted_latency']:.1f}s"       if r.get("adjusted_latency")       is not None else "-"
        lq = f"{r['latency_query_s']:.1f}s"        if r.get("latency_query_s")        is not None else "-"
        lr = f"{r['latency_interactive_s']:.1f}s"  if r.get("latency_interactive_s")  is not None else "-"
        lb = f"{r['latency_batch_s']:.1f}s"        if r.get("latency_batch_s")        is not None else "-"
        ps = f"{r.get('probe_score',0)}/{r.get('probe_total',10)}"
        is_unique = "yes" if _obj_key(r) in unique_keys_set else "dup"
        unique_keys_set.discard(_obj_key(r))  # only mark first occurrence as unique
        print(f"  {r['filename']:<30} {al:>10} {lq:>8} {lr:>8} {lb:>8} {ps:>7} {is_unique:>7}")

    # ── Save Pareto CSV ───────────────────────────────────────────────────────
    pareto_path = out_dir / "pareto.csv"
    probe_fields = [
        "probe_syntax", "probe_valid_scheduler", "probe_basic_run", "probe_retry_run",
        "probe_suspend_run", "probe_grouping", "probe_overcommit", "probe_priority_ordering",
        "probe_starvation", "probe_no_deadlock",
    ]
    fields = (["filename", "adjusted_latency", "latency_query_s", "latency_interactive_s",
               "latency_batch_s", "probe_score", "probe_total", "completion_rate"]
              + probe_fields)
    with pareto_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(pareto)

    total_s = int(time.time() - run_start)
    print(f"\nPareto optimal  : {len(pareto)}/{len(candidates)}  ({pct_pareto:.1f}%)")
    print(f"Unique Pareto   : {len(unique_pareto)}/{len(candidates)}  ({pct_unique:.1f}%)")
    print(f"Raw results  -> {raw_path}")
    print(f"Pareto table -> {pareto_path}")
    print(f"Total time   -> {total_s//3600:02d}:{(total_s%3600)//60:02d}:{total_s%60:02d}")


# ---------------------------------------------------------------------------
# Entry point dispatch
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if "--worker" in sys.argv:
        idx = sys.argv.index("--worker")
        sys.exit(_worker_main(sys.argv[idx + 1:]))
    else:
        main()
