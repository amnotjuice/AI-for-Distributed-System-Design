"""Experiment 08: how many iterations to adapt a scheduler to a new scenario.

Fig 8 from the paper.
- Take a scheduler optimized for scenario A
- Transfer it to scenario B and iterate
- Measure: how many iterations until it reaches parity with a scheduler
  trained from scratch on scenario B?
"""

PARAM_OVERRIDES = {
    "per_trace_timeout": None,
}

N_SCENARIOS = 10
MAX_ITERATIONS = 50
REASONING_EFFORT = "medium"
FEEDBACK_MODE = "rich"
MODEL = "gpt-5.2-2025-12-11"
