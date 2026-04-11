"""Experiment 03: two-iteration improvement for best/worst/median schedulers.

Fig 3 from the paper.
- Source pool: top / bottom / median schedulers from exp 01 one-shot results
- Two feedback modes: minimal vs rich (per-trace SimulatorStats breakdown)
- Measures: what % chance of improving vs the v1 scheduler?

Uses iterate_once() from tool/iterate_once.py, same as iterate-study/generate.py.
"""

PARAM_OVERRIDES = {
    "per_trace_timeout": None,
}

SOURCE_EXPERIMENT = "01_reasoning"
SELECTIONS = ["best", "median", "worst"]  # which one-shot schedulers to start from
FEEDBACK_MODES = ["simple", "rich"]
N_ATTEMPTS = 20  # attempts per (selection, feedback_mode) combo
MODEL = "gpt-5.2-2025-12-11"
