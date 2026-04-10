"""Experiment 06: 10 scenarios x 50 iterations, latency vs iteration.

Fig 6 from the paper.
- Pick 10 scenarios (randomly or representative)
- For each scenario, run 50 iterations of scheduler improvement
- Plot: latency (y-axis) vs iteration (x-axis), one line per scenario
"""

PARAM_OVERRIDES = {
    "per_trace_timeout": None,
}

N_SCENARIOS = 10
N_ITERATIONS = 50
REASONING_EFFORT = "medium"
FEEDBACK_MODE = "rich"
MODEL = "gpt-5.2-2025-12-11"
