# One-Shot Scheduler Evaluation

## Overview

This one-shot experiment evaluates whether an LLM can produce effective schedulers from a single generation pass. For each reasoning effort level, we generate 50 candidate policies and evaluate them on the same trace set. The core evaluation metric is **latency** (median over traces per policy), and cross-effort comparisons focus on both **functional success rate** and **beat-baseline rate**.

## Directory Structure

```
one-shot/
├── config.py                   # shared simulation parameters
├── experiment.py               # generates schedulers via one-shot LLM calls
├── analyze.py                  # evaluates schedulers (subprocess isolation)
├── plot.py                     # generates PDF charts from analysis output
├── traces/                     # 10 pre-generated workload traces
├── schedulers-{none,low,medium,high}/  # 50 generated schedulers per effort
├── output/                     # baseline results
├── plots/                      # generated charts
└── experiments/
    ├── per_trace_timeout/      # 60s wall-clock limit per trace
    ├── per_tick_timeout/       # 50ms per scheduler tick
    ├── shorter_sim/            # duration=200s
    └── max_outstanding/        # cap outstanding queue
```

---

## Timeout

Each full simulation run (10 traces) is guarded by a `subprocess_timeout=600s` safety net. Some LLM-generated schedulers have O(n²) or even infinite-loop tick implementations (e.g. `scheduler_none_004`), where a single tick can exceed 600s. 

---

## Experiment Comparison

Comparing 5 experiment configurations against no_limit (600s per simulation run) as baseline, measuring simulation efficiency and ranking fidelity:

| Experiment | Total Sim Time | Avg per Scheduler | Functional | Beat Baseline | Rho Range |
|---|---|---|---|---|---|
| **no_limit**  | 20703s (~5.7h) | 103.5s | 149/200 | 144/200 | — |
| **per_trace_timeout** (60s) | 8823s (~2.5h) | 44.1s | 140/200 | 136/200 | 1.000 |
| **per_tick_timeout** (50ms) | 15244s (~4.2h) | 76.2s | 142/200 | 137/200 | 1.000 |
| **shorter_sim** (200s) | 9612s (~2.7h) | 48.1s | 158/200 | 150/200 | 0.62–0.80 |
| **max_outstanding** (200) | 17415s (~4.8h) | 87.1s | 154/200 | 148/200 | 0.38–0.73 |

- **Rho**: Spearman rank correlation vs no_limit baseline (1.0 = identical ranking)
