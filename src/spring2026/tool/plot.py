#!/usr/bin/env python3
"""Generate paper plots for spring2026 experiments.

Usage:
    python plot.py 01_reasoning
    python plot.py 02_estimation
    python plot.py all

Reads from spring2026/results/{exp_name}/ and writes PDFs to spring2026/plots/.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import Counter
from pathlib import Path

_SRC = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_SRC))

os.environ.setdefault("MPLCONFIGDIR", str(Path("/tmp") / "matplotlib-cache"))
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import to_hex, to_rgb
from matplotlib.patches import Patch

from spring2026.tool.config import (
    EXPERIMENTS,
    PLOTS_DIR,
    RESULTS_DIR,
    TWO_SHOT_PERF_CONDITIONS,
    wilson_interval,
)


# ---------------------------------------------------------------------------
# Shared plot utilities (adapted from one-shot/plot.py)
# ---------------------------------------------------------------------------

EFFORT_ORDER  = ["none", "low", "medium", "high"]
EFFORT_SCORE  = {"none": 0.0, "low": 0.3, "medium": 0.6, "high": 1.0}
EFFORT_COLORS = {"none": "#c6dbef", "low": "#6baed6", "medium": "#2171b5", "high": "#08306b"}

SIGMA_ORDER  = ["no_estimation", "sigma_0.0", "sigma_0.5", "sigma_1.0", "sigma_1.5"]
SIGMA_LABELS = {
    "no_estimation": "No Est.",
    "sigma_0.0":     "σ=0.0",
    "sigma_0.5":     "σ=0.5",
    "sigma_1.0":     "σ=1.0",
    "sigma_1.5":     "σ=1.5",
}
SIGMA_COLORS = {
    "no_estimation": "#bdbdbd",
    "sigma_0.0":     "#fdd49e",
    "sigma_0.5":     "#fc8d59",
    "sigma_1.0":     "#e34a33",
    "sigma_1.5":     "#b30000",
}

PROBE_LABELS = {
    "syntax":            "Syntax",
    "valid_scheduler":   "Valid",
    "basic_run":         "Basic",
    "retry_run":         "Retry",
    "suspend_run":       "Suspend",
    "grouping":          "Grouping",
    "overcommit":        "Overcommit",
    "priority_ordering": "Priority\nOrder",
    "starvation":        "Starvation",
    "no_deadlock":       "No\nDeadlock",
}


def apply_plot_style() -> None:
    plt.style.use("seaborn-v0_8-whitegrid")
    plt.rcParams.update({
        "figure.facecolor": "white",
        "axes.facecolor": "#fbfcfe",
        "axes.edgecolor": "#d8e0e8",
        "axes.labelcolor": "#25364a",
        "axes.titleweight": "semibold",
        "xtick.color": "#3b4d5f",
        "ytick.color": "#3b4d5f",
        "grid.color": "#d9e2ec",
        "font.size": 11,
    })


def _mix(color: str, target: tuple, amount: float) -> str:
    rgb = np.array(to_rgb(color))
    mixed = (1.0 - amount) * rgb + amount * np.array(target)
    return to_hex(np.clip(mixed, 0.0, 1.0))


def _lighten(color: str, amount: float = 0.2) -> str:
    return _mix(color, (1.0, 1.0, 1.0), amount)


def _darken(color: str, amount: float = 0.2) -> str:
    return _mix(color, (0.0, 0.0, 0.0), amount)


def _gradient_colors(labels: list[str], cmap_name: str = "YlGnBu") -> list[str]:
    cmap = plt.get_cmap(cmap_name)
    return [to_hex(cmap(0.22 + 0.70 * EFFORT_SCORE.get(lbl, 0.5))) for lbl in labels]


def _style_axes(ax: plt.Axes) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.35, linewidth=0.8)
    ax.set_axisbelow(True)


def _save(fig: plt.Figure, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)
    print(f"  -> {path.with_suffix('.pdf').relative_to(PLOTS_DIR.parent)}")


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    seen: dict[str, dict] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)
        fn = record.get("filename")
        if fn:
            seen[fn] = record  # last write wins, consistent with load_existing_records
        else:
            seen[id(record)] = record
    return list(seen.values())


def load_results(exp_name: str) -> dict[str, list[dict]]:
    """Load all analysis.jsonl files under results/{exp_name}/, keyed by subdir name."""
    exp_dir = RESULTS_DIR / exp_name
    if not exp_dir.exists():
        return {}
    grouped = {}
    for subdir in sorted(exp_dir.iterdir()):
        if subdir.is_dir():
            records = load_jsonl(subdir / "analysis.jsonl")
            if records:
                grouped[subdir.name] = records
    return grouped


# ---------------------------------------------------------------------------
# Fig 1 helpers
# ---------------------------------------------------------------------------

def geometric_mean(values: list[float]) -> float:
    product = 1.0
    for v in values:
        product *= v
    return product ** (1.0 / len(values))


def _load_probe_data_01() -> dict[str, dict[str, float]]:
    """Load probe pass rates from results/01_reasoning/{effort}/probes.csv."""
    probes = list(PROBE_LABELS.keys())
    data: dict[str, dict[str, float]] = {}
    for effort in EFFORT_ORDER:
        path = RESULTS_DIR / "01_reasoning" / effort / "probes.csv"
        if not path.exists():
            continue
        with open(path, newline="") as f:
            rows = list(csv.DictReader(f))
        if not rows:
            continue
        rates: dict[str, float] = {}
        for probe in probes:
            valid = [r[probe] for r in rows if r.get(probe) and r[probe] != "skipped"]
            rates[probe] = sum(1 for v in valid if v == "pass") / len(valid) * 100 if valid else float("nan")
        data[effort] = rates
    return data


def _load_latency_by_effort_01() -> dict[str, np.ndarray]:
    """Load per-scheduler geometric-mean latency from results/01_reasoning/{effort}/analysis.jsonl."""
    result: dict[str, np.ndarray] = {}
    for effort in EFFORT_ORDER:
        records = load_jsonl(RESULTS_DIR / "01_reasoning" / effort / "analysis.jsonl")
        vals = []
        for r in records:
            if not r.get("functional"):
                continue
            mv = r.get("metric_values")
            if not mv:
                continue
            try:
                finite = [float(v) for v in mv if np.isfinite(float(v))]
                if finite:
                    vals.append(geometric_mean(finite))
            except (ValueError, ZeroDivisionError):
                continue
        if vals:
            result[effort] = np.array(vals)
    return result


def _draw_probe_pass_rates(ax: plt.Axes, data: dict[str, dict[str, float]], bar_width_total: float = 0.65) -> None:
    probes = list(PROBE_LABELS.keys())
    efforts = [e for e in EFFORT_ORDER if e in data]
    n = len(efforts)
    bar_width = bar_width_total / n
    centers = np.arange(len(probes))

    for i, effort in enumerate(efforts):
        offsets = centers + (i - (n - 1) / 2) * bar_width
        values = [data[effort].get(p, float("nan")) for p in probes]
        ax.bar(offsets, values, width=bar_width, label=effort.capitalize(),
               color=EFFORT_COLORS[effort], zorder=2, linewidth=0)

    ax.set_xticks(centers)
    ax.set_xticklabels([PROBE_LABELS[p] for p in probes], fontsize=4, rotation=90, ha="center")
    ax.set_ylabel("% Passing", fontsize=5)
    ax.tick_params(axis="y", labelsize=5)
    ax.set_ylim(0, 100)
    ax.yaxis.grid(True, linewidth=0.4, color="#dddddd", zorder=1)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def _draw_latency_cdf_by_effort(ax: plt.Axes, data: dict[str, np.ndarray]) -> None:
    for effort in EFFORT_ORDER:
        if effort not in data:
            continue
        vals = np.sort(data[effort])
        cdf = np.arange(1, len(vals) + 1) / len(vals) * 100
        ax.step(vals, cdf, where="post", label=effort.capitalize(),
                color=EFFORT_COLORS[effort], linewidth=1.0)
    ax.set_xlabel("Latency (s)", fontsize=5)
    ax.set_ylabel("% of Schedulers", fontsize=5)
    ax.tick_params(axis="both", labelsize=5)
    ax.set_ylim(0, 100)
    ax.yaxis.grid(True, linewidth=0.4, color="#dddddd", zorder=1)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


# ---------------------------------------------------------------------------
# Fig 2 helpers
# ---------------------------------------------------------------------------

def _load_probe_data_02() -> dict[str, float]:
    """Load probe pass rates from results/02_estimation/probes.csv (single group)."""
    path = RESULTS_DIR / "02_estimation" / "probes.csv"
    if not path.exists():
        return {}
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return {}
    rates: dict[str, float] = {}
    for probe in PROBE_LABELS:
        valid = [r[probe] for r in rows if r.get(probe) and r[probe] != "skipped"]
        rates[probe] = sum(1 for v in valid if v == "pass") / len(valid) * 100 if valid else float("nan")
    return rates


def _load_latency_by_sigma() -> dict[str, np.ndarray]:
    """Load per-scheduler geometric-mean latency from results/02_estimation/{sigma}/analysis.jsonl."""
    result: dict[str, np.ndarray] = {}
    for sigma in SIGMA_ORDER:
        records = load_jsonl(RESULTS_DIR / "02_estimation" / sigma / "analysis.jsonl")
        vals = []
        for r in records:
            if not r.get("functional"):
                continue
            mv = r.get("metric_values")
            if not mv:
                continue
            try:
                finite = [float(v) for v in mv if np.isfinite(float(v))]
                if finite:
                    vals.append(geometric_mean(finite))
            except (ValueError, ZeroDivisionError):
                continue
        if vals:
            result[sigma] = np.array(vals)
    return result


def _draw_probe_pass_rates_single(ax: plt.Axes, rates: dict[str, float]) -> None:
    """Simple bar chart for a single group of probe pass rates."""
    probes = list(PROBE_LABELS.keys())
    x = np.arange(len(probes))
    values = [rates.get(p, float("nan")) for p in probes]
    ax.bar(x, values, color="#6baed6", zorder=2, linewidth=0)
    ax.set_xticks(x)
    ax.set_xticklabels([PROBE_LABELS[p] for p in probes], fontsize=4, rotation=90, ha="center")
    ax.set_ylabel("% Passing", fontsize=5)
    ax.tick_params(axis="y", labelsize=5)
    ax.set_ylim(0, 100)
    ax.yaxis.grid(True, linewidth=0.4, color="#dddddd", zorder=1)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def _draw_latency_cdf_by_sigma(ax: plt.Axes, data: dict[str, np.ndarray]) -> None:
    for sigma in SIGMA_ORDER:
        if sigma not in data:
            continue
        vals = np.sort(data[sigma])
        cdf = np.arange(1, len(vals) + 1) / len(vals) * 100
        ax.step(vals, cdf, where="post", label=SIGMA_LABELS[sigma],
                color=SIGMA_COLORS[sigma], linewidth=1.0)
    ax.set_xlabel("Latency (s)", fontsize=5)
    ax.set_ylabel("% of Schedulers", fontsize=5)
    ax.tick_params(axis="both", labelsize=5)
    ax.set_ylim(0, 100)
    ax.yaxis.grid(True, linewidth=0.4, color="#dddddd", zorder=1)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


# ---------------------------------------------------------------------------
# Shared chart: success rates with CI (used by exp 01 and 02)
# ---------------------------------------------------------------------------

def _build_summary_rows(grouped: dict[str, list[dict]], metric: str = "latency") -> list[dict]:
    rows = []
    for label, records in grouped.items():
        n = len(records)
        func_n = sum(1 for r in records if r.get("functional"))
        beat_n = sum(1 for r in records if r.get("beats_baseline"))
        func_lo, func_hi = wilson_interval(func_n, n)
        beat_lo, beat_hi = wilson_interval(beat_n, n)

        latencies = [r[f"median_{metric}"] for r in records if r.get("functional") and r.get(f"median_{metric}") is not None]
        failure_counts = Counter(str(r.get("failure_mode", "unknown")) for r in records)

        rows.append({
            "label": label,
            "n": n,
            "func_n": func_n,
            "beat_n": beat_n,
            "functional_rate": func_n / n if n else None,
            "beat_rate": beat_n / n if n else None,
            "func_lo": func_lo, "func_hi": func_hi,
            "beat_lo": beat_lo, "beat_hi": beat_hi,
            "median_latency": float(np.median(latencies)) if latencies else None,
            "failure_counts": dict(failure_counts),
        })
    return rows


def _plot_success_rates(rows: list[dict], output_path: Path, title: str) -> None:
    labels = [r["label"] for r in rows]
    x = np.arange(len(rows))
    width = 0.36

    func = np.array([r["functional_rate"] * 100.0 for r in rows])
    beat = np.array([r["beat_rate"] * 100.0 for r in rows])
    func_err = np.array([
        [(r["functional_rate"] - r["func_lo"]) * 100.0, (r["func_hi"] - r["functional_rate"]) * 100.0]
        for r in rows
    ]).T
    beat_err = np.array([
        [(r["beat_rate"] - r["beat_lo"]) * 100.0, (r["beat_hi"] - r["beat_rate"]) * 100.0]
        for r in rows
    ]).T

    colors = _gradient_colors(labels)
    fig, ax = plt.subplots(figsize=(max(8.0, len(rows) * 2.2), 5.6))
    for i in range(len(rows)):
        ax.bar(x[i] - width/2, func[i], width, color=_darken(colors[i], 0.05),
               edgecolor=_darken(colors[i], 0.3), linewidth=0.6)
        ax.bar(x[i] + width/2, beat[i], width, color=_lighten(colors[i], 0.23),
               edgecolor=_darken(colors[i], 0.3), linewidth=0.6, hatch="//")
        ax.errorbar(x[i] - width/2, func[i], yerr=func_err[:, i:i+1], fmt="none",
                    ecolor=_darken(colors[i], 0.2), capsize=4, linewidth=1.2)
        ax.errorbar(x[i] + width/2, beat[i], yerr=beat_err[:, i:i+1], fmt="none",
                    ecolor=_darken(colors[i], 0.25), capsize=4, linewidth=1.2)

    ax.set_xticks(x, labels)
    ax.set_ylabel("Rate (%)")
    ax.set_title(title)
    ax.set_ylim(0, 100)
    ax.legend(handles=[
        Patch(facecolor="#4a5a6a", label="Functional"),
        Patch(facecolor="#9aa6b2", hatch="//", label="Beat baseline"),
    ], frameon=False, loc="upper left")
    _style_axes(ax)
    _save(fig, output_path)


def _plot_latency_cdf(grouped: dict[str, list[dict]], output_path: Path, title: str, metric: str = "latency") -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    colors = plt.get_cmap("tab10")
    for i, (label, records) in enumerate(grouped.items()):
        vals = sorted(r[f"median_{metric}"] for r in records
                      if r.get("functional") and r.get(f"median_{metric}") is not None)
        if not vals:
            continue
        y = np.arange(1, len(vals) + 1) / len(vals)
        ax.plot(vals, y, label=label, color=colors(i), linewidth=2)

    ax.set_xlabel(f"Median {metric.capitalize()} (s)")
    ax.set_ylabel("CDF")
    ax.set_title(title)
    ax.legend(frameon=False)
    _style_axes(ax)
    _save(fig, output_path)


# ---------------------------------------------------------------------------
# Fig 3 helpers
# ---------------------------------------------------------------------------

SOURCE_ORDER   = ["best", "worst", "median"]
SOURCE_COLORS  = {"best": "#2ca02c", "worst": "#d62728", "median": "#1f77b4"}
CONTEXT_HATCH  = {"simple": "", "rich": "///"}
CONTEXT_LABELS = {"simple": "Simple context", "rich": "Rich context"}


def _load_summaries_03() -> dict[str, dict]:
    """Load summary.json for each combo in 03_two_iter."""
    summaries = {}
    for src in SOURCE_ORDER:
        for ctx in ["simple", "rich"]:
            combo = f"{src}_{ctx}"
            path = RESULTS_DIR / "03_two_iter" / combo / "summary.json"
            if path.exists():
                summaries[combo] = json.loads(path.read_text())
    return summaries


# ---------------------------------------------------------------------------
# Per-experiment plot handlers
# ---------------------------------------------------------------------------

def plot_01_reasoning() -> None:
    """Fig 1: probe pass rates (left) + latency CDF by reasoning level (right)."""
    probe_data   = _load_probe_data_01()
    latency_data = _load_latency_by_effort_01()

    if not probe_data and not latency_data:
        print("  No results yet for 01_reasoning — run analyze.py 01_reasoning first")
        return

    fig, (ax_probes, ax_cdf) = plt.subplots(
        1, 2, figsize=(3.3, 1.8),
        gridspec_kw={"width_ratios": [1, 1]},
    )

    _draw_probe_pass_rates(ax_probes, probe_data)
    ax_probes.set_title("(a) Scheduler Properties", fontsize=6)

    _draw_latency_cdf_by_effort(ax_cdf, latency_data)
    ax_cdf.set_title("(b) Latency Distribution", fontsize=6)

    present = [e for e in EFFORT_ORDER if e in probe_data or e in latency_data]
    handles = [Patch(facecolor=EFFORT_COLORS[e], label=e.capitalize()) for e in present]
    fig.legend(handles=handles, frameon=False, fontsize=4, ncol=len(present),
               loc="upper center", bbox_to_anchor=(0.5, 1.05))

    _save(fig, PLOTS_DIR / "01_reasoning" / "fig1")


def plot_02_estimation() -> None:
    """Fig 2: probe pass rates (left) + latency CDF by sigma level (right)."""
    probe_data   = _load_probe_data_02()
    latency_data = _load_latency_by_sigma()

    if not probe_data and not latency_data:
        print("  No results yet for 02_estimation — run analyze.py 02_estimation first")
        return

    fig, (ax_probes, ax_cdf) = plt.subplots(
        1, 2, figsize=(3.3, 1.8),
        gridspec_kw={"width_ratios": [1, 1]},
    )

    _draw_probe_pass_rates_single(ax_probes, probe_data)
    ax_probes.set_title("(a) Scheduler Properties", fontsize=6)

    _draw_latency_cdf_by_sigma(ax_cdf, latency_data)
    ax_cdf.set_title("(b) Latency by Estimation Noise", fontsize=6)

    present = [s for s in SIGMA_ORDER if s in latency_data]
    handles = [Patch(facecolor=SIGMA_COLORS[s], label=SIGMA_LABELS[s]) for s in present]
    fig.legend(handles=handles, frameon=False, fontsize=4, ncol=len(present),
               loc="upper center", bbox_to_anchor=(0.5, 1.05))

    _save(fig, PLOTS_DIR / "02_estimation" / "fig2")


def plot_03_two_iter_best_worst() -> None:
    """Fig 3: % improved over source for best/worst/median × simple/rich context."""
    summaries = _load_summaries_03()
    if not summaries:
        print("  No results yet for 03_two_iter — run analyze.py 03_two_iter_best_worst first")
        return

    fig, ax = plt.subplots(figsize=(4.5, 2.5))

    bar_width = 0.28
    group_gap = 0.75
    contexts = ["simple", "rich"]

    for i, src in enumerate(SOURCE_ORDER):
        for j, ctx in enumerate(contexts):
            combo = f"{src}_{ctx}"
            summary = summaries.get(combo)
            if summary is None:
                continue

            x = i * group_gap + j * bar_width
            rate = (summary.get("improved_rate") or 0) * 100
            lo   = (summary.get("improved_lo")   or 0) * 100
            hi   = (summary.get("improved_hi")   or 0) * 100

            base_color = SOURCE_COLORS[src]
            bar_color = _lighten(base_color, 0.35) if ctx == "simple" else base_color

            ax.bar(x, rate, width=bar_width * 0.9,
                   color=bar_color, hatch=CONTEXT_HATCH[ctx],
                   edgecolor=_darken(base_color, 0.2), linewidth=0.5, zorder=2)

            n = summary.get("n_total", 0)
            if n > 0 and summary.get("improved_rate") is not None:
                ax.errorbar(x, rate, yerr=[[rate - lo], [hi - rate]],
                            fmt="none", ecolor="#333333", capsize=3, linewidth=0.8, zorder=3)

    # Group tick labels
    centers = [i * group_gap + (len(contexts) - 1) * bar_width / 2 for i in range(len(SOURCE_ORDER))]
    ax.set_xticks(centers)
    ax.set_xticklabels([s.capitalize() for s in SOURCE_ORDER], fontsize=7)
    ax.set_ylabel("% Improved Over Source", fontsize=7)
    ax.set_title("(a) Two-Iteration Improvement Rate", fontsize=8)
    ax.set_ylim(0, 100)
    ax.tick_params(axis="y", labelsize=6)

    handles = [
        Patch(facecolor="#aaaaaa", hatch="",    edgecolor="#555555", label="Simple context"),
        Patch(facecolor="#aaaaaa", hatch="///", edgecolor="#555555", label="Rich context"),
    ]
    ax.legend(handles=handles, frameon=False, fontsize=5, loc="upper right")
    _style_axes(ax)
    _save(fig, PLOTS_DIR / "03_two_iter" / "fig3")


def plot_04_two_iter_all() -> None:
    """Fig 4: per-trace beat rate for median source, simple vs rich context."""
    out_dir = RESULTS_DIR / "04_two_iter_all"
    records = load_jsonl(out_dir / "analysis.jsonl")

    if not records:
        print("  No results yet for 04_two_iter_all — run analyze.py 04_two_iter_all first")
        return

    functional = [r for r in records if r.get("functional")]
    if not functional:
        print("  No functional v2 schedulers in 04_two_iter_all results")
        return

    # Collect all trace names in stable order
    trace_names = sorted({name for r in functional for name in r.get("per_trace_beats_source", {})})
    short_names = [n.replace("bench_", "").replace("_train.csv", "") for n in trace_names]
    contexts = ["simple", "rich"]

    # Per-trace beat rate: for each context, fraction of schedulers that beat source
    beat_rates: dict[str, list[float]] = {ctx: [] for ctx in contexts}
    for ctx in contexts:
        ctx_recs = [r for r in functional if r.get("context") == ctx]
        for trace in trace_names:
            beats = sum(1 for r in ctx_recs if r.get("per_trace_beats_source", {}).get(trace))
            beat_rates[ctx].append(beats / len(ctx_recs) * 100 if ctx_recs else 0.0)

    fig, (ax_traces, ax_agg) = plt.subplots(1, 2, figsize=(8, 3),
                                             gridspec_kw={"width_ratios": [3, 1]})

    x = np.arange(len(trace_names))
    bar_width = 0.35
    ctx_colors = {"simple": "#6baed6", "rich": "#08519c"}

    for i, ctx in enumerate(contexts):
        offset = (i - 0.5) * bar_width
        ax_traces.bar(x + offset, beat_rates[ctx], bar_width,
                      label=ctx.capitalize(), color=ctx_colors[ctx], zorder=2)

    ax_traces.set_xticks(x)
    ax_traces.set_xticklabels(short_names, rotation=45, ha="right", fontsize=7)
    ax_traces.set_ylabel("% v2 Beat Source", fontsize=8)
    ax_traces.set_title("(a) Per-Context Beat Rate", fontsize=9)
    ax_traces.set_ylim(0, 100)
    ax_traces.axhline(50, color="#aaaaaa", linewidth=0.8, linestyle="--", zorder=1)
    ax_traces.legend(frameon=False, fontsize=7)
    _style_axes(ax_traces)

    # Aggregated bar
    for i, ctx in enumerate(contexts):
        ctx_recs = [r for r in functional if r.get("context") == ctx]
        total = sum(r["n_traces"] for r in ctx_recs)
        beats = sum(r["beats_source_count"] for r in ctx_recs)
        rate = beats / total * 100 if total else 0
        n = len(ctx_recs)
        lo, hi = wilson_interval(beats, total)
        ax_agg.bar(i, rate, color=ctx_colors[ctx], zorder=2)
        if total > 0 and lo is not None:
            ax_agg.errorbar(i, rate, yerr=[[rate - lo * 100], [hi * 100 - rate]],
                            fmt="none", ecolor="#333", capsize=4, linewidth=1, zorder=3)
        ax_agg.text(i, rate + 3, f"n={n}", ha="center", fontsize=7)

    ax_agg.set_xticks([0, 1])
    ax_agg.set_xticklabels(["Simple", "Rich"], fontsize=8)
    ax_agg.set_ylabel("% (scheduler, trace) pairs beat source", fontsize=7)
    ax_agg.set_title("(b) Overall", fontsize=9)
    ax_agg.set_ylim(0, 100)
    ax_agg.axhline(50, color="#aaaaaa", linewidth=0.8, linestyle="--", zorder=1)
    _style_axes(ax_agg)

    _save(fig, PLOTS_DIR / "04_two_iter_all" / "fig4")


def plot_05_two_shot_perf() -> None:
    """Fig 5: beat-source rate vs sim fidelity (shorter duration / coarser ticks)."""
    out_base = RESULTS_DIR / "05_two_shot_perf"
    source_path = out_base / "source.json"
    if not source_path.exists():
        print("  No results yet for 05_two_shot_perf — run analyze.py 05_two_shot_perf first")
        return

    source_latency = json.loads(source_path.read_text())["full_sim_median_latency"]

    labels, rates, errs, colors = [], [], [], []
    for label, cond in TWO_SHOT_PERF_CONDITIONS.items():
        records = load_jsonl(out_base / label / "analysis.jsonl")
        functional = [r for r in records
                      if r.get("functional") and r.get("median_latency") is not None]
        if not functional:
            continue
        n = len(functional)
        beats = sum(1 for r in functional if r["median_latency"] < source_latency)
        lo, hi = wilson_interval(beats, n)
        labels.append(label.replace("dur", "d=").replace("_ticks", "\nt="))
        rates.append(beats / n * 100)
        errs.append([(beats / n - lo) * 100, (hi - beats / n) * 100])
        # blue = ticks vary (duration fixed at 3600), orange = duration varies
        colors.append("#2171b5" if cond["duration"] == 3600 else "#e6550d")

    if not labels:
        print("  No results yet for 05_two_shot_perf")
        return

    x = np.arange(len(labels))
    errs_arr = np.array(errs).T  # shape (2, n)

    fig, ax = plt.subplots(figsize=(max(5.0, len(labels) * 1.4), 4))
    ax.bar(x, rates, color=colors, zorder=2)
    ax.errorbar(x, rates, yerr=errs_arr, fmt="none", ecolor="#333",
                capsize=4, linewidth=1, zorder=3)
    ax.axhline(50, color="#aaaaaa", linewidth=0.8, linestyle="--")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("% v1 Beat Source", fontsize=9)
    ax.set_title("Two-Shot Improvement Rate by Sim Fidelity", fontsize=10)
    ax.set_ylim(0, 100)
    ax.legend(handles=[
        Patch(facecolor="#e6550d", label="Shorter duration"),
        Patch(facecolor="#2171b5", label="Coarser ticks"),
    ], frameon=False, fontsize=7)
    _style_axes(ax)
    _save(fig, PLOTS_DIR / "05_two_shot_perf" / "fig5")


def plot_06_multi_iter() -> None:
    """Fig 6: geomean latency vs iteration, one line per scenario."""
    import csv
    candidates = sorted((RESULTS_DIR / "06_multi_iter").glob("iterations*.csv"))
    if not candidates:
        print("  No results yet for 06_multi_iter — run analyze.py 06_multi_iter first")
        return
    csv_path = candidates[-1]

    scenarios: dict[str, list[tuple[int, float]]] = {}
    with csv_path.open() as f:
        for row in csv.DictReader(f):
            scenarios.setdefault(row["scenario_id"], []).append(
                (int(row["iteration"]), float(row["geomean_latency"]))
            )

    if not scenarios:
        print("  iterations.csv is empty")
        return

    fig, ax = plt.subplots(figsize=(3.5, 2.4))
    colors = plt.get_cmap("tab10")
    for i, sid in enumerate(sorted(scenarios)):
        points = sorted(scenarios[sid])
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        ax.plot(xs, ys, color=colors(i), linewidth=0.9, label=sid)

    ax.set_xlabel("Iteration", fontsize=7)
    ax.set_ylabel("Geomean Latency (s)", fontsize=7)
    ax.tick_params(axis="both", labelsize=6)
    ax.legend(frameon=False, fontsize=5, ncol=2, loc="upper right")
    _style_axes(ax)
    _save(fig, PLOTS_DIR / "06_multi_iter" / "fig6")


def plot_07_cross_eval() -> None:
    """Fig 7: cross-eval heatmap."""
    print("plot_07: not yet implemented")


def plot_08_adapt_speed() -> None:
    """Fig 8: iterations to adapt to new scenario."""
    print("plot_08: not yet implemented")


def plot_09_general_purpose() -> None:
    """Fig 9: general-purpose scheduler performance."""
    print("plot_09: not yet implemented")


PLOT_HANDLERS = {
    "01_reasoning":           plot_01_reasoning,
    "02_estimation":          plot_02_estimation,
    "03_two_iter_best_worst": plot_03_two_iter_best_worst,
    "04_two_iter_all":        plot_04_two_iter_all,
    "05_two_shot_perf":       plot_05_two_shot_perf,
    "06_multi_iter":          plot_06_multi_iter,
    "07_cross_eval":          plot_07_cross_eval,
    "08_adapt_speed":         plot_08_adapt_speed,
    "09_general_purpose":     plot_09_general_purpose,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "experiment",
        choices=list(PLOT_HANDLERS) + ["all"],
        help="Experiment to plot, or 'all'",
    )
    args = parser.parse_args()

    apply_plot_style()
    exps = list(PLOT_HANDLERS) if args.experiment == "all" else [args.experiment]
    for exp in exps:
        print(f"\n{'='*60}")
        print(f"Plot: {exp} — {EXPERIMENTS.get(exp, '')}")
        print("=" * 60)
        PLOT_HANDLERS[exp]()

    print(f"\nDONE. PDFs in {PLOTS_DIR}/")


if __name__ == "__main__":
    main()
