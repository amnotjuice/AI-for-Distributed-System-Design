#!/usr/bin/env python3
"""Scatter plot: OOM count vs RAM utilization, point size = adjusted latency."""

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ITER_DIR = Path(__file__).resolve().parent
OUT_DIR = ITER_DIR / "plots"
OUT_DIR.mkdir(exist_ok=True)

GROUPS = {
    "Minimal": ITER_DIR / "two-shot-avg-low/output/analysis.jsonl",
    "Rich":    ITER_DIR / "two-shot-avg-rich-low/output/analysis.jsonl",
}
COLORS = {"Minimal": "#2196F3", "Rich": "#FF5722"}


def load(path):
    records = []
    with open(path) as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return [r for r in records if r.get("functional")]


def main():
    plt.rcParams.update({
        "font.family": "serif",
        "font.serif":  ["Times New Roman", "DejaVu Serif"],
        "font.size":   9,
        "axes.labelsize": 9,
        "legend.fontsize": 8,
        "figure.facecolor": "white",
        "axes.facecolor": "white",
        "savefig.facecolor": "white",
    })

    fig, ax = plt.subplots(figsize=(5, 3.5))

    all_latencies = []
    for records in [load(p) for p in GROUPS.values()]:
        all_latencies += [r["median_latency"] for r in records if r.get("median_latency") is not None]
    lat_min, lat_max = min(all_latencies), max(all_latencies)

    def size_scale(lat):
        # log scale so the good schedulers (clustered near lat_min) still show size variation
        import math
        norm = (math.log(lat + 1) - math.log(lat_min + 1)) / (math.log(lat_max + 1) - math.log(lat_min + 1) + 1e-9)
        return 20 + norm * 280  # range: 20–300

    for label, path in GROUPS.items():
        records = load(path)
        ooms  = [r.get("oom_count", 0) or 0 for r in records]
        ram   = [r.get("ram_utilization_mean") for r in records]
        lats  = [r.get("median_latency") for r in records]

        # filter rows where all three exist
        pts = [(o, r, l) for o, r, l in zip(ooms, ram, lats) if r is not None and l is not None]
        if not pts:
            continue
        xs, ys, ls = zip(*pts)

        sizes = [size_scale(l) for l in ls]
        ax.scatter(
            xs, ys,
            s=sizes,
            c=COLORS[label],
            alpha=0.65,
            edgecolors="white",
            linewidths=0.4,
            label=label,
            zorder=3,
        )

    ax.set_xlabel("OOM Count")
    ax.set_ylabel("RAM Utilization (mean)")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_linewidth(0.8)
    ax.spines["bottom"].set_linewidth(0.8)
    ax.grid(axis="both", linestyle="--", linewidth=0.4, alpha=0.4)

    # Legend 1: group colors — fixed marker size
    color_legend = ax.legend(frameon=False, loc="upper right", borderaxespad=0.3)
    for handle in color_legend.legend_handles:
        handle.set_sizes([40])
    ax.add_artist(color_legend)

    fig.tight_layout(pad=0.6)

    out = OUT_DIR / "scatter_oom_ram_latency.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


if __name__ == "__main__":
    main()
