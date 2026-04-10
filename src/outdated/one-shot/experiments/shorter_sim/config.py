"""Experiment: shorter simulation duration (200s instead of 600s).

Tests whether reducing ticks from 600k to 200k is enough to separate
good schedulers from bad ones, while cutting wall-clock time ~3x.
"""

EXPERIMENT_NAME = "shorter_sim"

PARAM_OVERRIDES = {
    "duration": 200,
    "per_trace_timeout": None,   # disable — let it run freely
    "subprocess_timeout": 600,   # 10-min safety net
}
