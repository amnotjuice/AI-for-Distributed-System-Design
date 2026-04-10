"""Experiment 07: cross-evaluation heatmap.

Fig 7 from the paper.
- Run scheduler X (optimized for workload A) on workload B
- x-axis: workload the scheduler is evaluated on
- y-axis: workload the scheduler was optimized for
- Cell value: latency relative to naive baseline
"""

PARAM_OVERRIDES = {
    "per_trace_timeout": None,
}

N_SCENARIOS = 10
REASONING_EFFORT = "medium"
MODEL = "gpt-5.2-2025-12-11"
