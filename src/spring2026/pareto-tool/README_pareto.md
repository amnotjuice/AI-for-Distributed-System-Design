# Pareto Analysis — Usage Guide

Scripts for evaluating and analyzing Pareto-optimal schedulers across the four
reasoning effort levels (none, low, medium, high).

---

## Scripts Overview

| Script                     | Purpose                                                                    |
| -------------------------- | -------------------------------------------------------------------------- |
| `pareto_eval.py`           | Evaluate schedulers and compute the Pareto frontier                        |
| `migrate_probe_results.py` | Patch existing results with individual probe binary columns (one-time use) |
| `summarize_pareto.py`      | Merge all four groups into summary CSVs                                    |
| `plot_pareto_bar.py`       | Generate the % Pareto-optimal bar chart (Fig 2)                            |

---

## Step-by-Step Usage

### Prerequisites

Generate the benchmark traces once before anything else:

```bash
cd src/spring2026
python traces/generate.py
```

---

### Step 1 — Evaluate schedulers (`pareto_eval.py`)

Run once per reasoning effort group. Each run evaluates 50 schedulers against
`bench_canonical_train.csv`, runs all 10 probes, and computes the
Pareto frontier.

```bash
python pareto_eval.py schedulers/reasoning/none/
python pareto_eval.py schedulers/reasoning/low/
python pareto_eval.py schedulers/reasoning/medium/
python pareto_eval.py schedulers/reasoning/high/
```

You can run all four in parallel in separate terminals. Each group takes ~2–4 hours.

**Options:**

| Flag            | Effect                                                                               |
| --------------- | ------------------------------------------------------------------------------------ |
| `--prototype`   | Fast mode: 1-min simulation. Good for testing the pipeline, not for paper results.   |
| `--hard-probes` | Only consider schedulers that pass all 10 probes; Pareto over adjusted_latency only. |
| `--timeout N`   | Per-scheduler timeout in seconds (default: 600).                                     |

**Resume support:** If the run is interrupted, re-running the same command picks
up where it left off — already-completed schedulers are skipped.

---

### Step 2 — Merge results (`summarize_pareto.py`)

After all four groups finish, generate the combined summary files:

```bash
python summarize_pareto.py
```

---

### Step 3 — Plot (`plot_pareto_bar.py`)

Generate the bar chart showing % Pareto-optimal per reasoning level:

```bash
python plot_pareto_bar.py
```

---

## Results Layout

All results are written to `results/pareto/`.

```
results/pareto/
├── none/
│   ├── results.jsonl       # One JSON record per scheduler (all 50)
│   └── pareto.csv          # Only the Pareto-optimal schedulers
├── low/
│   ├── results.jsonl
│   └── pareto.csv
├── medium/
│   ├── results.jsonl
│   └── pareto.csv
├── high/
│   ├── results.jsonl
│   └── pareto.csv
├── summary_merged.csv      # All Pareto-optimal schedulers across all groups
├── summary_stats.csv       # One row per group with aggregate statistics
├── pareto_pct_bar.pdf      # Bar chart (use this for the paper)
└── pareto_pct_bar.png      # Bar chart (use this for Slack/email)
```

### `results.jsonl` fields (one record per scheduler)

| Field                     | Description                                                |
| ------------------------- | ---------------------------------------------------------- |
| `filename`                | Scheduler file name                                        |
| `ok`                      | `true` if simulation completed successfully                |
| `adjusted_latency`        | Median adjusted latency in seconds (primary Pareto metric) |
| `latency_query_s`         | Mean latency for QUERY pipelines                           |
| `latency_interactive_s`   | Mean latency for INTERACTIVE pipelines                     |
| `latency_batch_s`         | Mean latency for BATCH pipelines                           |
| `completion_rate`         | Fraction of pipelines that completed (0–1)                 |
| `probe_score`             | Total probes passed (0–10)                                 |
| `probe_syntax`            | Binary: 1 = pass, 0 = fail                                 |
| `probe_valid_scheduler`   | Binary                                                     |
| `probe_basic_run`         | Binary                                                     |
| `probe_retry_run`         | Binary                                                     |
| `probe_suspend_run`       | Binary                                                     |
| `probe_grouping`          | Binary                                                     |
| `probe_overcommit`        | Binary                                                     |
| `probe_priority_ordering` | Binary                                                     |
| `probe_starvation`        | Binary                                                     |
| `probe_no_deadlock`       | Binary                                                     |

### `pareto.csv` fields

Same as above but only includes Pareto-optimal schedulers, sorted by
`adjusted_latency` ascending.

---

## Pareto Dimensions

A scheduler is Pareto-optimal if no other functional scheduler dominates it
across all 11 dimensions simultaneously:

- **10 binary probe results** — each maximize (1 > 0)
- **adjusted_latency** — minimize

Scheduler A dominates B if A passes every probe B passes (and possibly more),
and A has lower or equal adjusted_latency, with at least one strict improvement.

---

## Migration (one-time, already done)

If you have old `results.jsonl` files that only contain a summed `probe_score`
(not individual probe binary columns), run:

```bash
python migrate_probe_results.py
```

This re-runs only the probes (fast, ~30 min total) and patches the existing
results in place — no need to redo the 4-hour latency simulations.
