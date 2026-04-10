"""Experiment 05: two-shot perf with shorter/coarser simulations.

Fig 5 from the paper.
- Takes good schedulers from exp 01/02
- Evaluates them under different simulation fidelities
- Measures: % improvement relative to full-fidelity simulation
"""

PARAM_OVERRIDES = {
    "per_trace_timeout": None,
}

# Simulation fidelity conditions to sweep
DURATION_CONDITIONS = [60, 200, 600, 3600]       # seconds
TICKS_CONDITIONS    = [10, 100, 1000]             # ticks/sec
MODEL = "gpt-5.2-2025-12-11"
