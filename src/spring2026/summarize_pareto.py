#!/usr/bin/env python3
"""
summarize_pareto.py — Merge all four Pareto CSV files into one summary.

Usage:
    python summarize_pareto.py

Outputs:
    results/pareto/summary_merged.csv   — all Pareto-optimal schedulers, all groups
    results/pareto/summary_stats.csv    — one row per group with aggregate stats
"""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent / "results" / "pareto"
GROUPS      = ["none", "low", "medium", "high"]

# ── helpers ──────────────────────────────────────────────────────────────────

def fmt(v, decimals=1):
    if v is None or (isinstance(v, float) and math.isinf(v)):
        return ""
    return f"{v:.{decimals}f}"

def load_jsonl(path: Path) -> list[dict]:
    records = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records

def flag(r: dict) -> str:
    """Assign a short highlight label to notable schedulers."""
    q  = r.get("latency_query_s")
    ri = r.get("latency_interactive_s")
    b  = r.get("latency_batch_s")
    cr = r.get("completion_rate", 0) or 0
    ps = r.get("probe_score", 0)

    if cr >= 0.99:
        return "100% completion"
    if cr >= 0.90:
        return "high throughput"
    if ri is not None and ri < 10:
        return "near-zero interactive"
    if b is not None and b < 200:
        return "low batch latency"
    if ps >= 9:
        return "top probe score"
    if ps >= 8 and q is not None and q < 9:
        return "best probes+query"
    return ""

# ── load all groups ───────────────────────────────────────────────────────────

all_results: dict[str, list[dict]] = {}
pareto_sets: dict[str, list[dict]] = {}

for group in GROUPS:
    group_dir = RESULTS_DIR / group
    jsonl_path = group_dir / "results.jsonl"
    pareto_path = group_dir / "pareto.csv"

    if not jsonl_path.exists():
        print(f"WARNING: {jsonl_path} not found, skipping {group}")
        continue

    all_results[group] = load_jsonl(jsonl_path)

    # Load pareto filenames from pareto.csv
    pareto_names = set()
    if pareto_path.exists():
        with pareto_path.open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                pareto_names.add(row["filename"])

    pareto_sets[group] = [r for r in all_results[group] if r["filename"] in pareto_names]

# ── write merged CSV ──────────────────────────────────────────────────────────

MERGED_FIELDS = [
    "group", "filename",
    "latency_query_s", "latency_interactive_s", "latency_batch_s",
    "probe_score", "probe_total", "completion_rate", "median_latency",
    "highlight",
]

merged_path = RESULTS_DIR / "summary_merged.csv"
with merged_path.open("w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(MERGED_FIELDS)
    for group in GROUPS:
        rows = sorted(
            pareto_sets.get(group, []),
            key=lambda r: r.get("latency_query_s") or float("inf"),
        )
        for r in rows:
            writer.writerow([
                group,
                r["filename"],
                fmt(r.get("latency_query_s")),
                fmt(r.get("latency_interactive_s")),
                fmt(r.get("latency_batch_s")),
                r.get("probe_score", ""),
                r.get("probe_total", ""),
                fmt(r.get("completion_rate"), decimals=3),
                fmt(r.get("median_latency")),
                flag(r),
            ])

print(f"Merged CSV  → {merged_path}")

# ── write stats CSV ───────────────────────────────────────────────────────────

STATS_FIELDS = [
    "group",
    "total_schedulers",
    "functional",
    "functional_pct",
    "pareto_count",
    "best_query_latency_s",
    "best_interactive_latency_s",
    "best_batch_latency_s",
    "max_probe_score",
    "max_completion_rate",
    "top_scheduler",
]

stats_path = RESULTS_DIR / "summary_stats.csv"
with stats_path.open("w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(STATS_FIELDS)
    for group in GROUPS:
        all_r   = all_results.get(group, [])
        func    = [r for r in all_r if r.get("ok")]
        pareto  = pareto_sets.get(group, [])

        best_q  = min((r["latency_query_s"]       for r in func if r.get("latency_query_s")),       default=None)
        best_ri = min((r["latency_interactive_s"]  for r in func if r.get("latency_interactive_s")), default=None)
        best_b  = min((r["latency_batch_s"]        for r in func if r.get("latency_batch_s")),       default=None)
        max_ps  = max((r.get("probe_score", 0)     for r in func), default=0)
        max_cr  = max((r.get("completion_rate", 0) or 0 for r in func), default=0)

        # Top scheduler = highest completion rate among functional with all 3 dims
        complete = [r for r in func if r.get("latency_query_s") and
                    r.get("latency_interactive_s") and r.get("latency_batch_s")]
        top = max(complete, key=lambda r: r.get("completion_rate", 0), default=None)
        top_name = top["filename"].replace(".py", "") if top else "—"

        writer.writerow([
            group,
            len(all_r),
            len(func),
            f"{len(func)/len(all_r)*100:.0f}%" if all_r else "",
            len(pareto),
            fmt(best_q),
            fmt(best_ri),
            fmt(best_b),
            max_ps,
            fmt(max_cr, decimals=3),
            top_name,
        ])

print(f"Stats CSV   → {stats_path}")

# ── print console summary ─────────────────────────────────────────────────────

print()
print("=" * 72)
print("AGGREGATE SUMMARY ACROSS ALL GROUPS")
print("=" * 72)
print(f"{'Group':<10} {'Functional':>12} {'Pareto':>8} {'Best Q-lat':>12} {'Best R-lat':>12} {'Best B-lat':>12} {'Max Probes':>12}")
print("-" * 72)

for group in GROUPS:
    all_r  = all_results.get(group, [])
    func   = [r for r in all_r if r.get("ok")]
    pareto = pareto_sets.get(group, [])

    best_q  = min((r["latency_query_s"]      for r in func if r.get("latency_query_s")),       default=None)
    best_ri = min((r["latency_interactive_s"] for r in func if r.get("latency_interactive_s")), default=None)
    best_b  = min((r["latency_batch_s"]       for r in func if r.get("latency_batch_s")),       default=None)
    max_ps  = max((r.get("probe_score", 0)    for r in func), default=0)

    print(f"{group:<10} {f'{len(func)}/{len(all_r)}':>12} {len(pareto):>8}"
          f" {fmt(best_q)+'s':>12} {fmt(best_ri)+'s':>12} {fmt(best_b)+'s':>12} {str(max_ps)+'/10':>12}")

print()
print("HIGHLIGHTED SCHEDULERS")
print("-" * 72)
all_pareto = [(g, r) for g in GROUPS for r in pareto_sets.get(g, [])]
highlights = [(g, r) for g, r in all_pareto if flag(r)]
for g, r in sorted(highlights, key=lambda x: x[1].get("completion_rate", 0) or 0, reverse=True):
    name = r["filename"].replace(".py", "")
    lbl  = flag(r)
    cr   = fmt(r.get("completion_rate"), decimals=3)
    ps   = f"{r.get('probe_score',0)}/10"
    print(f"  [{g:>6}]  {name:<32}  {lbl:<24}  CR={cr}  probes={ps}")
