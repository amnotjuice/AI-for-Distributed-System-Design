#!/usr/bin/env python3
"""Plot comparison between two-shot-avg-low and two-shot-avg-rich-low."""

import json
import statistics
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ITER_DIR = Path(__file__).resolve().parent
OUT_DIR = ITER_DIR / "plots"
OUT_DIR.mkdir(exist_ok=True)

SOURCE_LATENCY = 134.53617494046102

def load(path):
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]

low  = load(ITER_DIR / "two-shot-avg-low/output/analysis.jsonl")
rich = load(ITER_DIR / "two-shot-avg-rich-low/output/analysis.jsonl")

low_f  = [r for r in low  if r.get("functional")]
rich_f = [r for r in rich if r.get("functional")]

COLORS = {"minimal": "#2196F3", "rich": "#FF5722"}
ALPHA  = 0.75

fig, axes = plt.subplots(1, 3, figsize=(15, 5))
fig.suptitle("Minimal vs Rich Feedback: Key Metrics Comparison", fontsize=13, fontweight="bold")

# ── Panel 1: OOM mean bar ─────────────────────────────────────────────────
ax = axes[0]
low_ooms  = [r["oom_count"] for r in low_f  if r.get("oom_count") is not None]
rich_ooms = [r["oom_count"] for r in rich_f if r.get("oom_count") is not None]
means = [statistics.mean(low_ooms), statistics.mean(rich_ooms)]
bars = ax.bar(["minimal", "rich"], means, color=[COLORS["minimal"], COLORS["rich"]], alpha=ALPHA, width=0.5)
for bar, v in zip(bars, means):
    ax.text(bar.get_x() + bar.get_width()/2, v + 80, f"{v:.0f}",
            ha="center", fontsize=11, fontweight="bold")
ax.set_title("OOM Count (mean per scheduler)", fontweight="bold")
ax.set_ylabel("Mean OOM count")
ax.grid(axis="y", alpha=0.3)

# ── Panel 2: RAM utilization mean bar ────────────────────────────────────
ax = axes[1]
low_rams  = [r["ram_utilization_mean"] for r in low_f  if r.get("ram_utilization_mean") is not None]
rich_rams = [r["ram_utilization_mean"] for r in rich_f if r.get("ram_utilization_mean") is not None]
means = [statistics.mean(low_rams), statistics.mean(rich_rams)]
bars = ax.bar(["minimal", "rich"], means, color=[COLORS["minimal"], COLORS["rich"]], alpha=ALPHA, width=0.5)
for bar, v in zip(bars, means):
    ax.text(bar.get_x() + bar.get_width()/2, v + 0.008, f"{v:.3f}",
            ha="center", fontsize=11, fontweight="bold")
ax.set_title("RAM Utilization (mean actual/allocated)", fontweight="bold")
ax.set_ylabel("Mean RAM utilization")
ax.set_ylim(0, 0.7)
ax.grid(axis="y", alpha=0.3)

# ── Panel 3: Per-priority completion rate grouped bar ────────────────────
ax = axes[2]
priorities = ["query", "interactive", "batch"]
x = np.arange(len(priorities))
width = 0.35

low_cr  = [statistics.mean([r[f"completion_rate_{p}"] for r in low_f  if r.get(f"completion_rate_{p}") is not None]) for p in priorities]
rich_cr = [statistics.mean([r[f"completion_rate_{p}"] for r in rich_f if r.get(f"completion_rate_{p}") is not None]) for p in priorities]

bars_l = ax.bar(x - width/2, low_cr,  width, label="minimal", color=COLORS["minimal"], alpha=ALPHA)
bars_r = ax.bar(x + width/2, rich_cr, width, label="rich",    color=COLORS["rich"],    alpha=ALPHA)

for bar, v in zip(bars_l, low_cr):
    if v > 0.001:
        ax.text(bar.get_x() + bar.get_width()/2, v + 0.008, f"{v:.3f}",
                ha="center", fontsize=8, color=COLORS["minimal"], fontweight="bold")
for bar, v in zip(bars_r, rich_cr):
    if v > 0.001:
        ax.text(bar.get_x() + bar.get_width()/2, v + 0.008, f"{v:.3f}",
                ha="center", fontsize=8, color=COLORS["rich"], fontweight="bold")

ax.set_title("Completion Rate by Priority", fontweight="bold")
ax.set_xlabel("Pipeline priority")
ax.set_ylabel("Median completion rate")
ax.set_xticks(x)
ax.set_xticklabels(priorities)
ax.legend(fontsize=8)
ax.grid(axis="y", alpha=0.3)

plt.tight_layout()
fig.savefig(OUT_DIR / "comparison.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved: plots/comparison.png")
