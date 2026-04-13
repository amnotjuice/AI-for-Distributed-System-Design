#!/usr/bin/env python3
"""
plot_pareto_bar.py — Bar chart: % Pareto-optimal (total vs unique) per reasoning level.

Produces results/pareto/pareto_pct_bar.pdf (and .png).

Usage:
    python plot_pareto_bar.py
"""

from __future__ import annotations
import csv
import json
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

import argparse as _ap
_p = _ap.ArgumentParser()
_p.add_argument("--results-dir", default=None)
_p.add_argument("--out-suffix", default="")
_args = _p.parse_args()

RESULTS_DIR = Path(_args.results_dir).resolve() if _args.results_dir else \
              Path(__file__).resolve().parent.parent / "results" / "pareto"
OUT_SUFFIX  = _args.out_suffix
GROUPS      = ["none", "low", "medium", "high"]
LABELS      = ["None", "Low", "Medium", "High"]

PROBE_NAMES_LIST = [
    "probe_syntax", "probe_valid_scheduler", "probe_basic_run", "probe_retry_run",
    "probe_suspend_run", "probe_grouping", "probe_overcommit", "probe_priority_ordering",
    "probe_starvation", "probe_no_deadlock",
]

def obj_key(r):
    probes = tuple(r.get(p, 0) for p in PROBE_NAMES_LIST)
    al = round(r.get("adjusted_latency") or 0, 1)
    return probes + (al,)

# ── collect numbers ───────────────────────────────────────────────────────────

functional_counts  = []
pareto_counts      = []
unique_counts      = []
pct_pareto_list    = []
pct_unique_list    = []

for group in GROUPS:
    jsonl_path  = RESULTS_DIR / group / "results.jsonl"
    pareto_path = RESULTS_DIR / group / "pareto.csv"

    records = [json.loads(l) for l in jsonl_path.open() if l.strip()]
    ok      = [r for r in records if r.get("ok")]

    pareto_names = set()
    with pareto_path.open() as f:
        for row in csv.DictReader(f):
            pareto_names.add(row["filename"])

    pareto  = [r for r in ok if r["filename"] in pareto_names]
    n_func  = len(ok)
    n_par   = len(pareto)

    # unique = deduplicate on identical objective vectors
    seen, unique = set(), []
    for r in pareto:
        k = obj_key(r)
        if k not in seen:
            seen.add(k)
            unique.append(r)

    n_uniq = len(unique)

    functional_counts.append(n_func)
    pareto_counts.append(n_par)
    unique_counts.append(n_uniq)
    pct_pareto_list.append(100.0 * n_par  / n_func if n_func else 0)
    pct_unique_list.append(100.0 * n_uniq / n_func if n_func else 0)

# ── plot ──────────────────────────────────────────────────────────────────────

fig, ax = plt.subplots(figsize=(5.5, 3.8))

x     = np.arange(len(LABELS))
width = 0.35

bars1 = ax.bar(x - width/2, pct_pareto_list, width, label="Pareto optimal",
               color="#4878cf", zorder=3)
bars2 = ax.bar(x + width/2, pct_unique_list,  width, label="Unique Pareto optimal",
               color="#6acc65", zorder=3)

# annotate with counts
for bar, n_p, n_f in zip(bars1, pareto_counts, functional_counts):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.4,
            f"{n_p}/{n_f}", ha="center", va="bottom", fontsize=8, color="#333333")

for bar, n_u, n_f in zip(bars2, unique_counts, functional_counts):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.4,
            f"{n_u}/{n_f}", ha="center", va="bottom", fontsize=8, color="#333333")

ax.set_xlabel("Reasoning effort level", fontsize=11)
ax.set_ylabel("% of functional schedulers", fontsize=11)
ax.set_title("Pareto-optimal schedulers\n(total vs. unique objective vectors)", fontsize=11)
ax.set_xticks(x)
ax.set_xticklabels(LABELS)
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax.set_ylim(0, max(pct_pareto_list) * 1.3)
ax.yaxis.grid(True, linestyle="--", alpha=0.6, zorder=0)
ax.set_axisbelow(True)
ax.legend(fontsize=9)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

plt.tight_layout()

out_pdf = RESULTS_DIR / f"pareto_pct_bar{OUT_SUFFIX}.pdf"
out_png = RESULTS_DIR / f"pareto_pct_bar{OUT_SUFFIX}.png"
fig.savefig(out_pdf, bbox_inches="tight")
fig.savefig(out_png, bbox_inches="tight", dpi=150)
print(f"Saved -> {out_pdf}")
print(f"Saved -> {out_png}")
plt.show()
