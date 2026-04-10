#!/usr/bin/env python3
"""Generate one-shot charts aligned with the original experiment style.

Usage:
    python plot.py output/
    python plot.py output/schedulers-low/ output/schedulers-high/
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from collections import Counter
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", str(Path("/tmp") / "matplotlib-cache"))
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import to_hex, to_rgb
from matplotlib.patches import Patch

from config import SUPPORTED_EFFORTS, wilson_interval

EFFORT_ORDER = SUPPORTED_EFFORTS
EFFORT_SCORE = {
    "none": 0.0,
    "minimal": 0.2,
    "low": 0.4,
    "medium": 0.6,
    "high": 0.8,
    "xhigh": 1.0,
}


def load_records(paths: list[Path]) -> list[dict]:
    records: list[dict] = []
    for p in paths:
        files = p.rglob("analysis.jsonl") if p.is_dir() else [p]
        for jf in files:
            for line in jf.read_text().splitlines():
                line = line.strip()
                if not line:
                    continue
                records.append(json.loads(line))
    return records


def sorted_efforts(efforts: list[str]) -> list[str]:
    known = [e for e in EFFORT_ORDER if e in efforts]
    unknown = sorted(set(efforts) - set(EFFORT_ORDER))
    return known + unknown


def _mix(color: str, target: tuple[float, float, float], amount: float) -> str:
    rgb = np.array(to_rgb(color))
    mixed = (1.0 - amount) * rgb + amount * np.array(target)
    return to_hex(np.clip(mixed, 0.0, 1.0))


def _lighten(color: str, amount: float = 0.2) -> str:
    return _mix(color, (1.0, 1.0, 1.0), amount)


def _darken(color: str, amount: float = 0.2) -> str:
    return _mix(color, (0.0, 0.0, 0.0), amount)


def _gradient_colors(efforts: list[str], cmap_name: str = "YlGnBu") -> list[str]:
    cmap = plt.get_cmap(cmap_name)
    mapped = []
    for e in efforts:
        pos = EFFORT_SCORE.get(e, 0.5)
        mapped.append(0.22 + 0.70 * pos)
    return [to_hex(cmap(v)) for v in mapped]


def _style_axes(ax: plt.Axes) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.35, linewidth=0.8)
    ax.set_axisbelow(True)


def _save_figure(fig: plt.Figure, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pdf_path = output_path.with_suffix(".pdf")
    fig.tight_layout()
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  📄 {pdf_path.name}")


def apply_plot_style() -> None:
    plt.style.use("seaborn-v0_8-whitegrid")
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "#fbfcfe",
            "axes.edgecolor": "#d8e0e8",
            "axes.labelcolor": "#25364a",
            "axes.titleweight": "semibold",
            "xtick.color": "#3b4d5f",
            "ytick.color": "#3b4d5f",
            "grid.color": "#d9e2ec",
            "font.size": 11,
        }
    )


def build_summary_rows(records: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for r in records:
        effort = str(r.get("reasoning_effort", "unknown"))
        grouped.setdefault(effort, []).append(r)

    rows: list[dict] = []
    for effort in sorted_efforts(list(grouped)):
        rr = grouped[effort]
        n = len(rr)
        functional_successes = sum(1 for r in rr if bool(r.get("functional")))
        beat_successes = sum(1 for r in rr if bool(r.get("beats_baseline")))
        functional_mean = functional_successes / n if n else None
        beat_mean = beat_successes / n if n else None
        func_lo, func_hi = wilson_interval(functional_successes, n)
        beat_lo, beat_hi = wilson_interval(beat_successes, n)

        gen_samples = [float(r["generation_seconds"]) for r in rr if r.get("generation_seconds") is not None]
        if not gen_samples:
            gen_samples = [float(r["llm_first_attempt_wait_seconds"]) for r in rr if r.get("llm_first_attempt_wait_seconds") is not None]
        mean_wait = (sum(gen_samples) / len(gen_samples)) if gen_samples else None

        failure_counts = Counter(str(r.get("failure_mode", "unknown")) for r in rr)
        rows.append(
            {
                "condition_id": f"effort_{effort}",
                "temperature": 1.0,
                "reasoning_effort": effort,
                "n_trials": n,
                "functional_successes": functional_successes,
                "better_than_baseline_successes": beat_successes,
                "functional_mean": functional_mean,
                "functional_wilson_95_low": func_lo,
                "functional_wilson_95_high": func_hi,
                "beat_baseline_mean": beat_mean,
                "beat_baseline_wilson_95_low": beat_lo,
                "beat_baseline_wilson_95_high": beat_hi,
                "mean_llm_wait_seconds": mean_wait,
                "failure_mode_counts": dict(failure_counts),
            }
        )
    return rows


def build_change_vs_medium(rows: list[dict]) -> list[dict]:
    medium = next((r for r in rows if r["reasoning_effort"] == "medium"), None)
    if medium is None:
        return []
    m_func = medium.get("functional_mean")
    m_beat = medium.get("beat_baseline_mean")
    if not m_func or not m_beat:
        return []

    out = []
    for r in rows:
        if r["reasoning_effort"] == "medium":
            continue
        cur = dict(r)
        cur["functional_pct_change_vs_medium"] = ((r["functional_mean"] - m_func) / m_func * 100.0) if r.get("functional_mean") is not None else None
        cur["beat_baseline_pct_change_vs_medium"] = ((r["beat_baseline_mean"] - m_beat) / m_beat * 100.0) if r.get("beat_baseline_mean") is not None else None
        out.append(cur)
    return out


def save_condition_summary(rows: list[dict], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / "condition_summary.json").open("w") as f:
        json.dump(rows, f, indent=2)

    cols = [
        "condition_id",
        "temperature",
        "reasoning_effort",
        "n_trials",
        "functional_successes",
        "better_than_baseline_successes",
        "functional_mean",
        "functional_wilson_95_low",
        "functional_wilson_95_high",
        "beat_baseline_mean",
        "beat_baseline_wilson_95_low",
        "beat_baseline_wilson_95_high",
        "mean_llm_wait_seconds",
        "failure_mode_counts",
    ]
    with (output_dir / "condition_summary.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for r in rows:
            row = dict(r)
            row["failure_mode_counts"] = json.dumps(row["failure_mode_counts"], sort_keys=True)
            writer.writerow(row)


def save_change_vs_medium_csv(change_rows: list[dict], output_dir: Path) -> None:
    fields = [
        "temperature",
        "reasoning_effort",
        "functional_mean",
        "beat_baseline_mean",
        "functional_pct_change_vs_medium",
        "beat_baseline_pct_change_vs_medium",
    ]
    with (output_dir / "thinktime_change_vs_medium.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in change_rows:
            writer.writerow(
                {
                    "temperature": r.get("temperature"),
                    "reasoning_effort": r.get("reasoning_effort"),
                    "functional_mean": r.get("functional_mean"),
                    "beat_baseline_mean": r.get("beat_baseline_mean"),
                    "functional_pct_change_vs_medium": r.get("functional_pct_change_vs_medium"),
                    "beat_baseline_pct_change_vs_medium": r.get("beat_baseline_pct_change_vs_medium"),
                }
            )


def plot_success_rates(rows: list[dict], output_dir: Path) -> Path:
    labels = [r["reasoning_effort"] for r in rows]
    x = np.arange(len(rows))
    width = 0.36

    functional = np.array([r["functional_mean"] * 100.0 for r in rows])
    beat = np.array([r["beat_baseline_mean"] * 100.0 for r in rows])

    func_err_low = np.array([(r["functional_mean"] - r["functional_wilson_95_low"]) * 100.0 for r in rows])
    func_err_high = np.array([(r["functional_wilson_95_high"] - r["functional_mean"]) * 100.0 for r in rows])
    beat_err_low = np.array([(r["beat_baseline_mean"] - r["beat_baseline_wilson_95_low"]) * 100.0 for r in rows])
    beat_err_high = np.array([(r["beat_baseline_wilson_95_high"] - r["beat_baseline_mean"]) * 100.0 for r in rows])

    base_colors = _gradient_colors(labels)
    func_colors = [_darken(c, 0.05) for c in base_colors]
    beat_colors = [_lighten(c, 0.23) for c in base_colors]

    fig, ax = plt.subplots(figsize=(max(8.0, len(rows) * 2.2), 5.6))
    for i in range(len(rows)):
        ax.bar(x[i] - width / 2, functional[i], width, color=func_colors[i], edgecolor=_darken(func_colors[i], 0.30), linewidth=0.6)
        ax.bar(x[i] + width / 2, beat[i], width, color=beat_colors[i], edgecolor=_darken(beat_colors[i], 0.30), linewidth=0.6, hatch="//")
        ax.errorbar(x[i] - width / 2, functional[i], yerr=[[func_err_low[i]], [func_err_high[i]]], fmt="none", ecolor=_darken(func_colors[i], 0.2), capsize=4, linewidth=1.2)
        ax.errorbar(x[i] + width / 2, beat[i], yerr=[[beat_err_low[i]], [beat_err_high[i]]], fmt="none", ecolor=_darken(beat_colors[i], 0.25), capsize=4, linewidth=1.2)

    ax.set_xticks(x, labels)
    ax.set_ylabel("Success Rate (%)")
    ax.set_title("Condition Success Rates (Gradient by Reasoning Effort, 95% CI)")
    ax.set_ylim(0, 100)
    ax.legend(
        handles=[
            Patch(facecolor="#4a5a6a", label="Functional success"),
            Patch(facecolor="#9aa6b2", hatch="//", label="Beat baseline"),
        ],
        frameon=False,
        loc="upper left",
    )
    _style_axes(ax)
    out = output_dir / "success_rates_with_ci.pdf"
    _save_figure(fig, out)
    return out


def plot_wait_time(rows: list[dict], output_dir: Path) -> Path | None:
    valid = [r for r in rows if r.get("mean_llm_wait_seconds") is not None]
    if not valid:
        return None
    labels = [r["reasoning_effort"] for r in valid]
    x = np.arange(len(valid))
    wait_s = np.array([r["mean_llm_wait_seconds"] for r in valid])
    colors = _gradient_colors(labels)

    fig, ax = plt.subplots(figsize=(max(8.0, len(valid) * 2.2), 5.2))
    bars = ax.bar(x, wait_s, color=colors, edgecolor=[_darken(c, 0.30) for c in colors], linewidth=0.7)
    ax.set_xticks(x, labels)
    ax.set_ylabel("Mean LLM Wait Time (s)")
    ax.set_title("Mean LLM Wait Time by Condition (Effort Gradient)")
    _style_axes(ax)
    for bar, value in zip(bars, wait_s):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{value:.1f}s", ha="center", va="bottom", fontsize=9)
    out = output_dir / "mean_llm_wait_seconds.pdf"
    _save_figure(fig, out)
    return out


def plot_change_vs_medium(change_rows: list[dict], output_dir: Path) -> Path | None:
    if not change_rows:
        return None
    labels = [r["reasoning_effort"] for r in change_rows]
    x = np.arange(len(change_rows))
    width = 0.36
    func_change = np.array([r["functional_pct_change_vs_medium"] for r in change_rows])
    beat_change = np.array([r["beat_baseline_pct_change_vs_medium"] for r in change_rows])
    base_colors = _gradient_colors(labels)
    func_colors = [_darken(c, 0.05) for c in base_colors]
    beat_colors = [_lighten(c, 0.23) for c in base_colors]

    fig, ax = plt.subplots(figsize=(max(8.0, len(change_rows) * 2.4), 5.2))
    for i in range(len(change_rows)):
        ax.bar(x[i] - width / 2, func_change[i], width, color=func_colors[i], edgecolor=_darken(func_colors[i], 0.30), linewidth=0.6)
        ax.bar(x[i] + width / 2, beat_change[i], width, color=beat_colors[i], edgecolor=_darken(beat_colors[i], 0.30), linewidth=0.6, hatch="//")
    ax.axhline(0.0, color="black", linewidth=1.0, alpha=0.6)
    ax.set_xticks(x, labels)
    ax.set_ylabel("Change vs Medium (%)")
    ax.set_title("Percent Change vs Medium (Effort Gradient)")
    ax.legend(
        handles=[
            Patch(facecolor="#4a5a6a", label="Functional"),
            Patch(facecolor="#9aa6b2", hatch="//", label="Beat baseline"),
        ],
        frameon=False,
        loc="upper left",
    )
    _style_axes(ax)
    out = output_dir / "thinktime_change_vs_medium.pdf"
    _save_figure(fig, out)
    return out


def plot_failure_modes(rows: list[dict], output_dir: Path) -> Path | None:
    if not rows:
        return None
    labels = [r["reasoning_effort"] for r in rows]
    raw_counts = [r.get("failure_mode_counts", {}) for r in rows]
    all_modes = sorted({k for c in raw_counts for k in c})
    if not all_modes:
        return None
    if "success" in all_modes:
        all_modes = ["success"] + [m for m in all_modes if m != "success"]

    data = {m: np.array([c.get(m, 0) for c in raw_counts]) for m in all_modes}
    x = np.arange(len(rows))
    bottoms = np.zeros(len(rows))
    mode_palette = {
        "success": "#2ca58d",
        "simulation_timeout": "#f2b134",
        "simulation_error": "#ef6f6c",
        "exec_error": "#e879a0",
    }
    colors = [mode_palette.get(m, to_hex(plt.cm.Set2(i / max(len(all_modes) - 1, 1)))) for i, m in enumerate(all_modes)]

    fig, ax = plt.subplots(figsize=(max(8.0, len(rows) * 2.2), 5.6))
    for mode, color in zip(all_modes, colors):
        vals = data[mode]
        ax.bar(x, vals, bottom=bottoms, label=mode, color=color)
        bottoms += vals
    ax.set_xticks(x, labels)
    ax.set_ylabel("Count")
    ax.set_title("Outcome / Failure Mode Counts by Condition")
    ax.legend(title="mode", loc="upper left", bbox_to_anchor=(1.01, 1), borderaxespad=0)
    fig.tight_layout()
    _style_axes(ax)
    out = output_dir / "failure_mode_counts_stacked.pdf"
    _save_figure(fig, out)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).resolve().parent / "plots")
    args = parser.parse_args()

    records = load_records(args.paths)
    assert records, "No analysis records found"
    efforts = sorted_efforts(list({str(r.get("reasoning_effort", "unknown")) for r in records}))
    print(f"📊 {len(records)} records  |  efforts: {', '.join(efforts)}")
    print(f"📈 Generating charts in {args.output_dir}/")

    apply_plot_style()

    rows = build_summary_rows(records)
    change_rows = build_change_vs_medium(rows)
    save_condition_summary(rows, args.output_dir)
    save_change_vs_medium_csv(change_rows, args.output_dir)

    generated = []
    generated.append(plot_success_rates(rows, args.output_dir))
    wait_chart = plot_wait_time(rows, args.output_dir)
    if wait_chart is not None:
        generated.append(wait_chart)
    change_chart = plot_change_vs_medium(change_rows, args.output_dir)
    if change_chart is not None:
        generated.append(change_chart)
    failure_chart = plot_failure_modes(rows, args.output_dir)
    if failure_chart is not None:
        generated.append(failure_chart)

    print(f"\nDONE: {len(generated)} charts in {args.output_dir}/")


if __name__ == "__main__":
    main()
