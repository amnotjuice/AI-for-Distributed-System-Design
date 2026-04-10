#!/usr/bin/env python3
"""Compare winning policies (beat source) vs source policy across key metrics."""

import json
import statistics
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ITER_DIR = Path(__file__).resolve().parent
OUT_DIR  = ITER_DIR / "plots"
OUT_DIR.mkdir(exist_ok=True)

SOURCE_LATENCY = 134.53617494046102

def load(path):
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]

low  = load(ITER_DIR / "two-shot-avg-low/output/analysis.jsonl")
rich = load(ITER_DIR / "two-shot-avg-rich-low/output/analysis.jsonl")

# Source policy stats
source_path = Path("/tmp/source_only/out/analysis.jsonl")
source = load(source_path)[0]

# Winners = functional + beat source latency
winners_low  = [r for r in low  if r.get("functional") and r.get("median_latency", 9e9) < SOURCE_LATENCY]
winners_rich = [r for r in rich if r.get("functional") and r.get("median_latency", 9e9) < SOURCE_LATENCY]
winners_all  = winners_low + winners_rich

print(f"Source latency: {source['median_latency']:.2f}")
print(f"Winners: low={len(winners_low)}, rich={len(winners_rich)}, total={len(winners_all)}")

# ── helpers ───────────────────────────────────────────────────────────────
def mean_of(recs, key):
    vals = [r[key] for r in recs if r.get(key) is not None]
    return statistics.mean(vals) if vals else 0.0

METRICS = [
    # (key, label, higher_is_better)
    ("oom_count",                  "OOM Count",              False),
    ("ram_utilization_mean",       "RAM Utilization",        None),   # neutral
    ("completion_rate",            "Overall CR",             True),
    ("completion_rate_query",      "CR Query",               True),
    ("completion_rate_interactive","CR Interactive",         True),
    ("throughput_mean",            "Throughput (c/s)",       True),
    ("latency_query_s",            "Latency Query (s)",      False),
    ("latency_interactive_s",      "Latency Interactive (s)",False),
]

groups = {
    "source":   [source],
    "winners":  winners_all,
}
group_labels = list(groups.keys())
group_colors = ["#9E9E9E", "#2196F3"]

fig, axes = plt.subplots(1, 3, figsize=(15, 5))
fig.suptitle("Winners vs Source Policy: Key Metrics (mean, n=6 winners)", fontsize=13, fontweight="bold")

# Panel 1: OOM count
ax = axes[0]
vals = [mean_of(recs, "oom_count") for recs in groups.values()]
bars = ax.bar(group_labels, vals, color=group_colors, alpha=0.8, width=0.5)
vmax = max(vals) if max(vals) > 0 else 1
for bar, v in zip(bars, vals):
    ax.text(bar.get_x() + bar.get_width()/2, v + vmax * 0.03,
            f"{v:.0f}", ha="center", fontsize=11, fontweight="bold")
ax.set_title("OOM Count (mean per scheduler)", fontweight="bold")
ax.set_ylabel("Mean OOM count")
ax.grid(axis="y", alpha=0.3)

# Panel 2: RAM utilization
ax = axes[1]
vals = [mean_of(recs, "ram_utilization_mean") for recs in groups.values()]
bars = ax.bar(group_labels, vals, color=group_colors, alpha=0.8, width=0.5)
for bar, v in zip(bars, vals):
    ax.text(bar.get_x() + bar.get_width()/2, v + 0.008,
            f"{v:.3f}", ha="center", fontsize=11, fontweight="bold")
ax.set_title("RAM Utilization (mean actual/allocated)", fontweight="bold")
ax.set_ylabel("Mean RAM utilization")
ax.set_ylim(0, 0.4)
ax.grid(axis="y", alpha=0.3)

# Panel 3: per-priority completion rate (grouped bar)
ax = axes[2]
priorities = ["query", "interactive", "batch"]
x = np.arange(len(priorities))
width = 0.35
for i, (label, recs, color) in enumerate(zip(group_labels, groups.values(), group_colors)):
    vals = [mean_of(recs, f"completion_rate_{p}") for p in priorities]
    bars = ax.bar(x + (i - 0.5) * width, vals, width, label=label, color=color, alpha=0.8)
    for bar, v in zip(bars, vals):
        if v > 0.001:
            ax.text(bar.get_x() + bar.get_width()/2, v + 0.008,
                    f"{v:.3f}", ha="center", fontsize=8, fontweight="bold", color=color)
ax.set_title("Completion Rate by Priority", fontweight="bold")
ax.set_ylabel("Mean completion rate")
ax.set_xticks(x); ax.set_xticklabels(priorities)
ax.set_xlabel("Pipeline priority")
ax.legend(fontsize=9)
ax.grid(axis="y", alpha=0.3)

plt.tight_layout()
fig.savefig(OUT_DIR / "winners_comparison.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved: plots/winners_comparison.png")

# ── per-priority completion rate panel ────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(12, 5))
fig.suptitle("Winners vs Source: Per-priority Breakdown (mean per group)", fontsize=12, fontweight="bold")

priorities = ["query", "interactive", "batch"]
x = np.arange(len(priorities))
width = 0.25

# completion rate
ax = axes[0]
for i, (label, recs, color) in enumerate(zip(group_labels, groups.values(), group_colors)):
    vals = [mean_of(recs, f"completion_rate_{p}") for p in priorities]
    bars = ax.bar(x + (i - 1) * width, vals, width, label=label.replace("\n", " "),
                  color=color, alpha=0.8)
    for bar, v in zip(bars, vals):
        if v > 0.005:
            ax.text(bar.get_x() + bar.get_width()/2, v + 0.005,
                    f"{v:.3f}", ha="center", fontsize=7, fontweight="bold", color=color)
ax.set_xticks(x); ax.set_xticklabels(priorities)
ax.set_title("Completion Rate by Priority")
ax.set_ylabel("Mean completion rate")
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3)

# latency
ax = axes[1]
lat_keys = ["latency_query_s", "latency_interactive_s", "latency_batch_s"]
for i, (label, recs, color) in enumerate(zip(group_labels, groups.values(), group_colors)):
    vals = [mean_of(recs, k) for k in lat_keys]
    bars = ax.bar(x + (i - 1) * width, vals, width, label=label.replace("\n", " "),
                  color=color, alpha=0.8)
    for bar, v in zip(bars, vals):
        if v > 1:
            ax.text(bar.get_x() + bar.get_width()/2, v + 2,
                    f"{v:.0f}s", ha="center", fontsize=7, fontweight="bold", color=color)
ax.set_xticks(x); ax.set_xticklabels(priorities)
ax.set_title("Mean Latency by Priority")
ax.set_ylabel("Mean latency (s)")
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3)

plt.tight_layout()
fig.savefig(OUT_DIR / "winners_priority.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved: plots/winners_priority.png")
