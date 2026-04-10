"""Experiment 01: one-shot, vary reasoning level, no estimation.

Fig 1 from the paper.
- 4 conditions: none / low / medium / high reasoning
- No estimation (estimator off)
- 50 schedulers per condition
- Single scenario (to be decided by Jackson)
"""

PARAM_OVERRIDES = {
    "per_trace_timeout": None,
}

N_SCHEDULERS = 50
REASONING_EFFORTS = ["none", "low", "medium", "high"]
MODEL = "gpt-5.2-2025-12-11"
