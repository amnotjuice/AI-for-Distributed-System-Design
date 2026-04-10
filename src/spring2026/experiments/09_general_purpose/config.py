"""Experiment 09: general-purpose scheduler optimized over all 10 scenarios.

Fig 9 from the paper.
- 50 iterations
- Each iteration: evaluate on all 10 scenarios, average results
- Feedback includes averaged stats across all scenarios
"""

PARAM_OVERRIDES = {
    "per_trace_timeout": None,
}

N_SCENARIOS = 10
N_ITERATIONS = 50
REASONING_EFFORT = "medium"
FEEDBACK_MODE = "rich"
MODEL = "gpt-5.2-2025-12-11"
