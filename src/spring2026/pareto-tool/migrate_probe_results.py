#!/usr/bin/env python3
"""
migrate_probe_results.py — Patch existing results.jsonl files with individual
probe binary columns and rename median_latency → adjusted_latency.

This avoids re-running the 10-minute latency simulations. Only the probes
(fast, ~30s per scheduler) are re-run.

Usage:
    python migrate_probe_results.py                  # all four groups
    python migrate_probe_results.py --group high     # one group only
    python migrate_probe_results.py --dry-run        # preview, no writes

Each scheduler's probes run in a subprocess with a 60s timeout.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

_HERE  = Path(__file__).resolve().parent
_SRC   = _HERE.parent
_TOOL  = _HERE / "tool"
_PROBE = _TOOL / "probe"

for p in [str(_SRC), str(_TOOL), str(_PROBE)]:
    if p not in sys.path:
        sys.path.insert(0, p)

PROBE_NAMES = [
    "syntax", "valid_scheduler", "basic_run", "retry_run", "suspend_run",
    "grouping", "overcommit", "priority_ordering", "starvation", "no_deadlock",
]
GROUPS = ["none", "low", "medium", "high"]
RESULTS_DIR = _HERE / "results" / "pareto"


# ---------------------------------------------------------------------------
# Subprocess worker: runs probes only, prints __PROBE_RESULT__{...json...}
# ---------------------------------------------------------------------------

def _probe_worker_main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("__PROBE_RESULT__" + json.dumps({"ok": False, "reason": "bad_args"}))
        return 2

    scheduler_file_str, base_params_json, traces_json = argv
    scheduler_file = Path(scheduler_file_str)
    base_params    = json.loads(base_params_json)
    traces         = json.loads(traces_json)

    from run_probes import run_all_probes

    probe_results = run_all_probes(scheduler_file, base_params, traces)
    probe_binary  = {f"probe_{p}": int(probe_results.get(p, {}).get("functional", False))
                     for p in PROBE_NAMES}
    probe_score   = sum(probe_binary.values())

    print("__PROBE_RESULT__" + json.dumps({
        "ok": True,
        "probe_score": probe_score,
        **probe_binary,
    }))
    return 0


def run_probes_subprocess(scheduler_file: Path, base_params: dict,
                           traces: dict, timeout_s: int = 60) -> dict | None:
    cmd = [
        sys.executable, str(Path(__file__).resolve()),
        "--probe-worker",
        str(scheduler_file),
        json.dumps(base_params),
        json.dumps(traces),
    ]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=timeout_s, cwd=str(_SRC),
        )
        for line in reversed(proc.stdout.strip().splitlines()):
            if line.startswith("__PROBE_RESULT__"):
                return json.loads(line[len("__PROBE_RESULT__"):])
        return None
    except subprocess.TimeoutExpired:
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Migration logic
# ---------------------------------------------------------------------------

def migrate_group(group: str, dry_run: bool) -> None:
    from config import get_canonical_base_params
    from run_probes import ensure_traces

    group_dir  = RESULTS_DIR / group
    jsonl_path = group_dir / "results.jsonl"
    sched_dir  = _HERE / "schedulers" / "reasoning" / group

    if not jsonl_path.exists():
        print(f"  [{group}] No results.jsonl found, skipping.")
        return

    records = []
    with jsonl_path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass

    print(f"\n[{group}]  {len(records)} records in results.jsonl")

    # Check if migration is already done
    sample_ok = next((r for r in records if r.get("ok")), None)
    if sample_ok and "probe_syntax" in sample_ok and "adjusted_latency" in sample_ok:
        print(f"  Already migrated (probe_syntax + adjusted_latency present). Skipping.")
        return

    base_params = get_canonical_base_params(prototype=False)
    traces      = ensure_traces()
    run_start   = time.time()

    updated = []
    for i, record in enumerate(records, 1):
        fn   = record.get("filename", "?")
        name = fn.replace(".py", "")

        # ── rename median_latency → adjusted_latency ──────────────────────
        if "median_latency" in record and "adjusted_latency" not in record:
            record["adjusted_latency"] = record.pop("median_latency")

        # ── check if probe binary already present ─────────────────────────
        if "probe_syntax" in record:
            print(f"  [{i:2d}/{len(records)}] {fn}  (already has probe binary, skipping probes)")
            updated.append(record)
            continue

        # ── re-run probes ──────────────────────────────────────────────────
        sched_file = sched_dir / fn
        if not sched_file.exists():
            print(f"  [{i:2d}/{len(records)}] {fn}  MISSING FILE — filling zeros")
            empty = {f"probe_{p}": 0 for p in PROBE_NAMES}
            record.update(empty)
            updated.append(record)
            continue

        result = run_probes_subprocess(sched_file, base_params, traces)

        elapsed_s = int(time.time() - run_start)
        elapsed   = f"{elapsed_s//3600:02d}:{(elapsed_s%3600)//60:02d}:{elapsed_s%60:02d}"

        if result and result.get("ok"):
            probe_bits = [result.get(f"probe_{p}", 0) for p in PROBE_NAMES]
            probe_str  = "".join("#" if b else "." for b in probe_bits)
            ps         = result["probe_score"]
            record.update({f"probe_{p}": result.get(f"probe_{p}", 0) for p in PROBE_NAMES})
            record["probe_score"] = ps
            print(f"  [{i:2d}/{len(records)}] {fn:<36}  {probe_str}  {ps}/10  {elapsed}")
        else:
            empty = {f"probe_{p}": 0 for p in PROBE_NAMES}
            record.update(empty)
            print(f"  [{i:2d}/{len(records)}] {fn:<36}  probe timeout/error          {elapsed}")

        updated.append(record)

    if dry_run:
        print(f"  [dry-run] Would write {len(updated)} records back to {jsonl_path}")
        return

    # Write back
    with jsonl_path.open("w") as f:
        for r in updated:
            f.write(json.dumps(r) + "\n")

    # Rewrite pareto.csv with new fields
    pareto_path = group_dir / "pareto.csv"
    if pareto_path.exists():
        import csv
        pareto_names = set()
        with pareto_path.open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                pareto_names.add(row["filename"])

        probe_fields = [f"probe_{p}" for p in PROBE_NAMES]
        fields = (["filename", "adjusted_latency", "latency_query_s",
                   "latency_interactive_s", "latency_batch_s",
                   "probe_score", "probe_total", "completion_rate"]
                  + probe_fields)

        pareto_records = [r for r in updated if r.get("filename") in pareto_names]
        with pareto_path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(pareto_records)

        print(f"  Updated pareto.csv ({len(pareto_records)} rows)")

    total_s = int(time.time() - run_start)
    print(f"  Done [{group}] — {total_s//60}m {total_s%60}s")


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--group", choices=GROUPS, default=None,
                        help="Migrate one group only (default: all four)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without writing")
    args = parser.parse_args()

    groups = [args.group] if args.group else GROUPS
    print(f"Migrating groups: {groups}  (dry_run={args.dry_run})")

    for group in groups:
        migrate_group(group, dry_run=args.dry_run)

    print("\nMigration complete.")
    print("Now re-run:  python pareto_eval.py schedulers/reasoning/<group>/")
    print("(It will use existing results.jsonl — no re-simulation needed)")


if __name__ == "__main__":
    if "--probe-worker" in sys.argv:
        idx = sys.argv.index("--probe-worker")
        sys.exit(_probe_worker_main(sys.argv[idx + 1:]))
    else:
        main()
