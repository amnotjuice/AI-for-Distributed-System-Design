# Iterate Study: Two-Shot Scheduler Improvement

Can an LLM improve an already-good scheduler with one round of feedback?

## Setup

- **Source scheduler**: `scheduler_high_013` (best one-shot result, median latency = 77.29)
- **Model**: GPT-5.2 (reasoning_effort=high, temperature=1.0)
- **Metric**: median adjusted latency across 10 traces (lower is better)
- **Baseline**: naive FIFO scheduler (median latency = 12999.70)

The LLM receives the original code + feedback from the first run, then generates an improved version — simulating main.py's second iteration.

## Feedback Conditions

- **Minimal**: only the median latency value (matches main.py's actual feedback format)
  - `"This policy achieved latency of 77.29 (vs previous best: 77.29). This is an improvement!"`
- **Rich**: median latency + per-trace SimulatorStats (completion rates, failure counts, resource utilization)

## Results (n=70 per condition)

| Condition | Functional | Beat Source (<77.29) | Best | Median |
|-----------|-----------|---------------------|------|--------|
| Minimal   | 47/70 (67%) | **4/70 (5.7%)** | 70.75 | 348.16 |
| Rich      | 60/70 (86%) | **0/70 (0.0%)** | 82.31 | 267.84 |

### Schedulers that beat source (all from Minimal)

| Scheduler | Median Latency | Improvement |
|-----------|---------------|-------------|
| r36       | 70.75 | 8.5% |
| r16       | 71.55 | 7.4% |
| r28       | 71.55 | 7.4% |
| r1        | 74.52 | 3.6% |

## Findings

1. **Rich feedback increases functional rate** (86% vs 67%) — detailed stats help the LLM avoid writing code that crashes or times out.

2. **Rich feedback produces zero improvements** (0/70) — the LLM becomes more conservative with more information, generating "safer" but less innovative code.

3. **Minimal feedback enables rare breakthroughs** (5.7%) — less information forces the LLM to take larger creative leaps, which usually fail but occasionally produce genuine improvements.

4. **Improvement ceiling is ~8.5%** — the best iteration achieved 70.75 vs 77.29, suggesting diminishing returns from single-round feedback.

## Replication

Each condition was run in two batches (n=20 + n=30) to verify consistency:

| Condition | Run 1 (n=20) | Run 2 (n=50) |
|-----------|-------------|-------------|
| Minimal functional | 60% | 70% |
| Minimal beat source | 5% | 6% |
| Rich functional | 100% | 80% |
| Rich beat source | 0% | 0% |

## Directory Structure

```
iterate-study/
├── generate.py              # generation script (--feedback minimal|rich)
├── two-shot/                # Minimal, run 1 (n=20)
│   ├── schedulers/
│   └── output/analysis.jsonl
├── two-shot-val/            # Minimal, run 2 (n=50)
│   ├── schedulers/
│   └── output/analysis.jsonl
├── two-shot-rich/           # Rich, run 1 (n=20)
│   ├── schedulers/
│   └── output/analysis.jsonl
└── two-shot-rich-val/       # Rich, run 2 (n=50)
    ├── schedulers/
    └── output/analysis.jsonl
```
