"""Experiment 04: two-iteration across ALL contexts, % time v2 beats v1.

Fig 4 from the paper.
- For each reasoning level from exp 01, take all functional schedulers
- Run one iteration of improvement with minimal vs rich feedback
- Measure: what % of time does v2 beat v1?
"""

PARAM_OVERRIDES = {
    "per_trace_timeout": None,
}

SOURCE_EXPERIMENT = "01_reasoning"
FEEDBACK_MODES = ["minimal", "rich"]
MODEL = "gpt-5.2-2025-12-11"
