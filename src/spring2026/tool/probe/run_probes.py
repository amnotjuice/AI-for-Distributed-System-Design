#!/usr/bin/env python3
"""Run all probes against one or more scheduler files.

Usage:
    python run_probes.py scheduler_low_001.py
    python run_probes.py schedulers-low/
    python run_probes.py schedulers-low/ schedulers-high/

When a directory is given, results are also written to:
    spring2026/one-shot/analyses/schedulers-{effort}.csv
One row per scheduler, one column per probe (pass or failure_mode).
"""
from __future__ import annotations

import csv
import re
import sys
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

PROBE_DIR   = Path(__file__).resolve().parent
TOOL_DIR    = PROBE_DIR.parent                       # src/spring2026/tool (config.py)
SRC_DIR     = PROBE_DIR.parent.parent.parent         # src/ (simulation_utils.py)
RESULTS_DIR = PROBE_DIR.parent.parent / "results" / "probes"
sys.path.insert(0, str(TOOL_DIR))
sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, str(PROBE_DIR))

from config import get_canonical_base_params
from generate_probe_traces import PRESETS, generate_probe_trace
from probe_syntax import probe_syntax
from probe_valid_scheduler import probe_valid_scheduler
from probe_basic_run import probe_basic_run
from probe_grouping import probe_grouping
from probe_overcommit import probe_overcommit
from probe_priority_ordering import probe_priority_ordering
from probe_starvation import probe_starvation
from probe_no_deadlock import probe_no_deadlock

# probe_retry_run and probe_suspend_run have incorrect function names (copy-paste bugs);
# import by attribute so the runner still works until those are fixed.
import probe_retry_run as _retry_mod
import probe_suspend_run as _suspend_mod
probe_retry_run = getattr(_retry_mod, "probe_retry_run", None) or getattr(_retry_mod, "probe_basic_run")
probe_suspend_run = getattr(_suspend_mod, "probe_suspend_run", None) or getattr(_suspend_mod, "probe_retry_run")


TRACES_DIR = PROBE_DIR / "traces"

PROBE_TRACES = {
    "basic":              "probe_basic.csv",
    "oom_retry":          "probe_oom_retry.csv",
    "suspend":            "probe_suspend.csv",
    "priority_ordering":  "probe_priority_ordering.csv",
    "starvation":         "probe_starvation.csv",
    "no_deadlock":        "probe_no_deadlock.csv",
}


def ensure_traces() -> dict[str, str]:
    """Generate any missing probe trace files. Returns dict mapping preset name to path."""
    TRACES_DIR.mkdir(exist_ok=True)
    paths = {}
    for preset_name, filename in PROBE_TRACES.items():
        path = TRACES_DIR / filename
        if not path.exists():
            print(f"  Generating trace: {filename}")
            generate_probe_trace(PRESETS[preset_name], str(path))
        paths[preset_name] = str(path)
    return paths


def run_all_probes(scheduler_file: Path, base_params: dict, traces: dict[str, str]) -> dict[str, dict]:
    results: dict[str, dict] = {}

    results["syntax"] = probe_syntax(str(scheduler_file))
    if not results["syntax"]["functional"]:
        return results

    results["valid_scheduler"] = probe_valid_scheduler(str(scheduler_file))
    if not results["valid_scheduler"]["functional"]:
        return results

    results["basic_run"]         = probe_basic_run(str(scheduler_file),         [traces["basic"]],             base_params)
    results["retry_run"]         = probe_retry_run(str(scheduler_file),         [traces["oom_retry"]],         base_params)
    results["suspend_run"]       = probe_suspend_run(str(scheduler_file),       [traces["suspend"]],           base_params)
    results["grouping"]          = probe_grouping(str(scheduler_file),          [traces["basic"]],             base_params)
    results["overcommit"]        = probe_overcommit(str(scheduler_file),        [traces["basic"]],             base_params)
    results["priority_ordering"] = probe_priority_ordering(str(scheduler_file), [traces["priority_ordering"]], base_params)
    results["starvation"]        = probe_starvation(str(scheduler_file),        [traces["starvation"]],        base_params)
    results["no_deadlock"]       = probe_no_deadlock(str(scheduler_file),       [traces["no_deadlock"]],       base_params)

    return results


def print_results(scheduler_file: Path, results: dict[str, dict]) -> None:
    passed = sum(1 for r in results.values() if r.get("functional"))
    total = len(results)
    status = "PASS" if passed == total else "FAIL"
    print(f"\n[{status}] {scheduler_file.name}  ({passed}/{total} probes passed)")
    for probe_name, result in results.items():
        ok = "  OK  " if result.get("functional") else " FAIL "
        detail = ""
        if not result.get("functional"):
            detail = f"  [{result.get('failure_mode', '?')}] {result.get('error_message', '')}"
        print(f"  {ok}  {probe_name}{detail}")


PROBE_NAMES = [
    "syntax", "valid_scheduler", "basic_run", "retry_run", "suspend_run",
    "grouping", "overcommit", "priority_ordering", "starvation", "no_deadlock",
]


def _effort_from_dir(directory: Path) -> str:
    """Extract effort label from a directory named schedulers-{effort}."""
    m = re.search(r"schedulers-(\w+)", directory.name)
    return m.group(1) if m else directory.name


def _csv_value(results: dict[str, dict], probe: str) -> str:
    if probe not in results:
        return "skipped"
    r = results[probe]
    return "pass" if r.get("functional") else r.get("failure_mode", "fail")


def write_csv(scheduler_files: list[Path], all_results: list[dict[str, dict]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["scheduler"] + PROBE_NAMES)
        for sf, results in zip(scheduler_files, all_results):
            row = [sf.name] + [_csv_value(results, p) for p in PROBE_NAMES]
            writer.writerow(row)
    print(f"\nCSV written to {output_path}")


def collect_scheduler_files(paths: list[Path]) -> list[Path]:
    files = []
    for p in paths:
        if p.is_dir():
            files.extend(sorted(p.glob("scheduler_*.py")))
        elif p.is_file():
            files.append(p)
        else:
            print(f"Warning: {p} does not exist, skipping.")
    return files


def _worker(args: tuple) -> tuple[Path, dict[str, dict]]:
    """Top-level worker function for ProcessPoolExecutor (must be picklable)."""
    scheduler_file, base_params, traces = args
    results = run_all_probes(scheduler_file, base_params, traces)
    return scheduler_file, results


def run_probes_parallel(
    scheduler_files: list[Path],
    base_params: dict,
    traces: dict[str, str],
    workers: int = 1,
) -> list[dict[str, dict]]:
    """Run all probes over scheduler_files, returning results in input order.

    workers=1 runs sequentially. workers>1 uses ProcessPoolExecutor.
    No printing — callers handle their own output.
    """
    if workers <= 1:
        return [run_all_probes(sf, base_params, traces) for sf in scheduler_files]

    results_by_file: dict[Path, dict[str, dict]] = {}
    worker_args = [(sf, base_params, traces) for sf in scheduler_files]
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_worker, wa): wa[0] for wa in worker_args}
        for future in as_completed(futures):
            sf, results = future.result()
            results_by_file[sf] = results
    return [results_by_file[sf] for sf in scheduler_files]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("targets", nargs="+", type=Path,
                        help="Scheduler .py file(s) or director(ies) containing scheduler_*.py files.")
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of parallel workers for directory targets (default: 1).")
    args = parser.parse_args()

    print(f"Ensuring probe traces exist...")
    traces = ensure_traces()
    base_params = get_canonical_base_params()

    # Process directory targets separately so each gets its own CSV
    dir_targets = [p for p in args.targets if Path(p).is_dir()]
    file_targets = [p for p in args.targets if Path(p).is_file()]

    # Individual files — text output only (always sequential)
    for sf in sorted(f for p in file_targets for f in [Path(p)]):
        results = run_all_probes(sf, base_params, traces)
        print_results(sf, results)

    # Directories — text output + CSV per directory
    for directory in dir_targets:
        directory = Path(directory)
        scheduler_files = sorted(directory.glob("scheduler_*.py"))
        if not scheduler_files:
            print(f"No scheduler_*.py found in {directory}, skipping.")
            continue

        results_by_file: dict[Path, dict[str, dict]] = {}

        if args.workers > 1:
            worker_args = [(sf, base_params, traces) for sf in scheduler_files]
            with ProcessPoolExecutor(max_workers=args.workers) as executor:
                futures = {executor.submit(_worker, wa): wa[0] for wa in worker_args}
                for future in as_completed(futures):
                    sf, results = future.result()
                    results_by_file[sf] = results
                    passed = sum(1 for r in results.values() if r.get("functional"))
                    total = len(results)
                    status = "PASS" if passed == total else "FAIL"
                    print(f"  [{status}] {sf.name}  ({passed}/{total})")
            # Print full detail after all complete
            for sf in scheduler_files:
                print_results(sf, results_by_file[sf])
        else:
            for sf in scheduler_files:
                results = run_all_probes(sf, base_params, traces)
                print_results(sf, results)
                results_by_file[sf] = results

        all_results = [results_by_file[sf] for sf in scheduler_files]
        total_pass = sum(1 for r in all_results if all(v.get("functional") for v in r.values()))

        print(f"\n{'='*50}")
        print(f"DONE: {total_pass}/{len(scheduler_files)} schedulers passed all probes")

        effort = _effort_from_dir(directory)
        csv_path = RESULTS_DIR / f"schedulers-{effort}.csv"
        write_csv(scheduler_files, all_results, csv_path)

    if not dir_targets and not file_targets:
        print("No scheduler files found.")
        sys.exit(1)


if __name__ == "__main__":
    main()
