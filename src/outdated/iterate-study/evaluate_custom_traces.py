#!/usr/bin/env python3
"""Evaluate an existing iterate-study scheduler set on custom traces."""

from __future__ import annotations

import json
import logging
import os
import re
import statistics
import sys
import time
from pathlib import Path

ITERATE_DIR = Path(__file__).resolve().parent
SRC_DIR = ITERATE_DIR.parent
PROJECT_ROOT = SRC_DIR.parent
ONE_SHOT_DIR = SRC_DIR / "one-shot"
TWO_SHOT_AVG_LOW_DIR = ITERATE_DIR / "two-shot-avg-low"

sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, str(ONE_SHOT_DIR))

os.environ.setdefault("LITELLM_LOG", "ERROR")
logging.getLogger("eudoxia").setLevel(logging.CRITICAL)
logging.getLogger("simulator_ext").setLevel(logging.CRITICAL)
logging.getLogger("simulation_utils").setLevel(logging.CRITICAL)

from config import get_canonical_base_params
from simulation_utils import extract_metrics_from_stats, generate_traces, get_raw_stats_for_policy
from analyze import evaluate, parse_header, static_analysis

SCHEDULER_LIMIT_KEYS = {"tick_timeout_ms", "max_outstanding"}
TRACE_COUNT = 10
METRIC = "latency"


def _candidate_sort_key(path: Path) -> int:
    match = re.search(r"_r(\d+)\.py$", path.name)
    assert match, f"Unexpected scheduler filename: {path.name}"
    return int(match.group(1))


def _build_record(scheduler_file: Path, trace_files: list[str], baseline_med: float, base_params: dict) -> dict:
    t0 = time.time()
    return {
        "filename": scheduler_file.name,
        "baseline_median": baseline_med,
        "metric": METRIC,
        **parse_header(scheduler_file),
        **static_analysis(scheduler_file),
        **evaluate(scheduler_file, trace_files, baseline_med, METRIC, base_params),
        "simulation_seconds": round(time.time() - t0, 2),
    }


def _cleanup_utility_csvs() -> None:
    for pattern_root in (SRC_DIR, ONE_SHOT_DIR):
        for path in pattern_root.glob("pool_*_utility.csv"):
            path.unlink(missing_ok=True)


def _generate_trace_set(trace_dir: Path, override_key: str, override_value: int | float) -> list[str]:
    trace_dir.mkdir(parents=True, exist_ok=True)
    base_params = get_canonical_base_params()
    base_params[override_key] = override_value
    prefix = str(trace_dir / trace_dir.name)
    return generate_traces(TRACE_COUNT, base_params, prefix)


def _evaluate_set(trace_dir: Path, output_dir: Path, override_key: str, override_value: int | float) -> None:
    base_params = get_canonical_base_params()
    base_params[override_key] = override_value
    trace_files = _generate_trace_set(trace_dir, override_key, override_value)

    baseline_params = {k: v for k, v in base_params.items() if k not in SCHEDULER_LIMIT_KEYS}
    naive_raw = get_raw_stats_for_policy(baseline_params, trace_files, "naive")
    assert len(naive_raw) == len(trace_files), f"Baseline failed for {trace_dir}"
    baseline_med = statistics.median(extract_metrics_from_stats(naive_raw, METRIC))

    source_file = ONE_SHOT_DIR / "schedulers-low" / "scheduler_low_011.py"
    candidate_files = sorted(
        TWO_SHOT_AVG_LOW_DIR.joinpath("schedulers").glob("scheduler_low_011_r*.py"),
        key=_candidate_sort_key,
    )
    scheduler_files = [source_file, *candidate_files]
    assert len(scheduler_files) == 21, f"Expected 21 schedulers, got {len(scheduler_files)}"

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "analysis.jsonl"
    print(f"\nEvaluating {len(scheduler_files)} schedulers on {trace_dir.name} ...")
    print(f"  baseline median {METRIC}: {baseline_med:.4f}")
    print(f"  output: {output_path}")

    with output_path.open("w") as out_f:
        for i, scheduler_file in enumerate(scheduler_files, 1):
            print(f"[{i}/{len(scheduler_files)}] {scheduler_file.name}")
            record = _build_record(scheduler_file, trace_files, baseline_med, base_params)
            out_f.write(json.dumps(record) + "\n")
            out_f.flush()

            ok = "OK" if record["functional"] else "FAIL"
            detail = (
                f"{METRIC}={record.get(f'median_{METRIC}', 'N/A')}"
                if record["functional"]
                else record.get("failure_mode", "")
            )
            print(f"  {ok} {detail}")

    _cleanup_utility_csvs()


def main() -> None:
    os.chdir(PROJECT_ROOT)
    _evaluate_set(
        trace_dir=ITERATE_DIR / "trace_pipeline=20",
        output_dir=TWO_SHOT_AVG_LOW_DIR / "output_pipeline=20",
        override_key="num_pipelines",
        override_value=20,
    )
    _evaluate_set(
        trace_dir=ITERATE_DIR / "trace_waitingtime=5",
        output_dir=TWO_SHOT_AVG_LOW_DIR / "output_waiting=5",
        override_key="waiting_seconds_mean",
        override_value=5,
    )


if __name__ == "__main__":
    main()
