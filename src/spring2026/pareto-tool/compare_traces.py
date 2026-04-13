#!/usr/bin/env python3
"""
compare_traces.py — Compare Pareto results from traces v1 vs traces-v2.

Usage:
    python compare_traces.py

Outputs:
    results/comparison.png / .pdf  — side-by-side bar chart
    Console table showing key differences
"""

from __future__ import annotations
import csv, json
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

_HERE       = Path(__file__).resolve().parent.parent
V1_DIR      = _HERE / "results" / "pareto"
V2_DIR      = _HERE / "results" / "pareto-v2"
GROUPS      = ["none", "low", "medium", "high"]
LABELS      = ["None", "Low", "Medium", "High"]

PROBE_NAMES = [
    "probe_syntax", "probe_valid_scheduler", "probe_basic_run", "probe_retry_run",
    "probe_suspend_run", "probe_grouping", "probe_overcommit", "probe_priority_ordering",
    "probe_starvation", "probe_no_deadlock",
]

def obj_key(r):
    return tuple(r.get(p, 0) for p in PROBE_NAMES) + (round(r.get("adjusted_latency") or 0, 1),)

def load_group(results_dir: Path, group: str) -> dict:
    jsonl = results_dir / group / "results.jsonl"
    pareto_csv = results_dir / group / "pareto.csv"

    if not jsonl.exists():
        return None

    records = [json.loads(l) for l in jsonl.open() if l.strip()]
    ok = [r for r in records if r.get("ok")]

    pareto_names = set()
    if pareto_csv.exists():
        with pareto_csv.open() as f:
            for row in csv.DictReader(f):
                pareto_names.add(row["filename"])

    pareto = [r for r in ok if r["filename"] in pareto_names]

    seen, unique = set(), 0
    for r in pareto:
        k = obj_key(r)
        if k not in seen:
            seen.add(k); unique += 1

    top_cr  = max((r.get("completion_rate") or 0 for r in ok), default=0)
    med_lat = sorted([r.get("adjusted_latency") or 0 for r in ok])
    median_lat = med_lat[len(med_lat)//2] if med_lat else 0

    return {
        "functional": len(ok),
        "total":      len(pareto),
        "unique":     unique,
        "pct":        100 * len(pareto) / len(ok) if ok else 0,
        "pct_unique": 100 * unique / len(ok) if ok else 0,
        "top_cr":     top_cr,
        "median_lat": median_lat,
    }

# ── load both ────────────────────────────────────────────────────────────────

v1 = {g: load_group(V1_DIR, g) for g in GROUPS}
v2 = {g: load_group(V2_DIR, g) for g in GROUPS}

# ── console comparison table ──────────────────────────────────────────────────

print("=" * 90)
print(f"{'':10} {'--- Traces V1 (bench_canonical) ---':^38}  {'--- Traces V2 (canonical_steady_balanced) ---':^38}")
print(f"{'Group':<10} {'Functional':>12} {'Pareto':>8} {'Unique':>8} {'TopCR':>7}  {'Functional':>12} {'Pareto':>8} {'Unique':>8} {'TopCR':>7}")
print("-" * 90)

for g, label in zip(GROUPS, LABELS):
    d1, d2 = v1[g], v2[g]
    def _fmt(d):
        if d is None: return f"{'N/A':>12} {'N/A':>8} {'N/A':>8} {'N/A':>7}"
        func_str  = f"{d['functional']}/50"
        total_str = f"{d['total']} ({d['pct']:.0f}%)"
        uniq_str  = str(d['unique'])
        cr_str    = f"{d['top_cr']*100:.0f}%"
        return f"{func_str:>12} {total_str:>8} {uniq_str:>8} {cr_str:>7}"
    print(f"{label:<10} {_fmt(d1)}  {_fmt(d2)}")

print("=" * 90)

# ── plot ──────────────────────────────────────────────────────────────────────

fig, axes = plt.subplots(1, 2, figsize=(10, 4), sharey=True)

for ax, data, title in [
    (axes[0], v1, "Traces V1 (bench_canonical_train)"),
    (axes[1], v2, "Traces V2 (canonical_steady_balanced_train)"),
]:
    if any(data[g] is None for g in GROUPS):
        ax.text(0.5, 0.5, "No results", ha="center", va="center", transform=ax.transAxes)
        ax.set_title(title, fontsize=10)
        continue

    x     = np.arange(len(LABELS))
    width = 0.35

    pct_total  = [data[g]["pct"]        for g in GROUPS]
    pct_unique = [data[g]["pct_unique"] for g in GROUPS]
    n_total    = [data[g]["total"]      for g in GROUPS]
    n_unique   = [data[g]["unique"]     for g in GROUPS]
    n_func     = [data[g]["functional"] for g in GROUPS]

    b1 = ax.bar(x - width/2, pct_total,  width, label="Pareto optimal",        color="#4878cf", zorder=3)
    b2 = ax.bar(x + width/2, pct_unique, width, label="Unique Pareto optimal",  color="#6acc65", zorder=3)

    for bar, n in zip(b1, n_total):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                str(n), ha="center", va="bottom", fontsize=8)
    for bar, n in zip(b2, n_unique):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                str(n), ha="center", va="bottom", fontsize=8)

    ax.set_title(title, fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels(LABELS)
    ax.set_xlabel("Reasoning effort level", fontsize=10)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    if ax == axes[0]:
        ax.set_ylabel("% of functional schedulers", fontsize=10)

axes[0].legend(fontsize=8, loc="upper right")
fig.suptitle("Pareto-optimal schedulers: Traces V1 vs V2", fontsize=11)
plt.tight_layout()

out_dir = _HERE / "results"
out_dir.mkdir(exist_ok=True)
fig.savefig(out_dir / "comparison_v1_v2.pdf", bbox_inches="tight")
fig.savefig(out_dir / "comparison_v1_v2.png", bbox_inches="tight", dpi=150)
print(f"\nSaved -> {out_dir / 'comparison_v1_v2.pdf'}")
print(f"Saved -> {out_dir / 'comparison_v1_v2.png'}")
plt.show()
