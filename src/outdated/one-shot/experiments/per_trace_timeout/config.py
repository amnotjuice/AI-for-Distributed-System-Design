"""Experiment: per-trace wall-clock timeout (original default).

Kills any single trace simulation that exceeds 60s wall-clock time.
This is the original default behaviour — kept here as a reference experiment.
"""

EXPERIMENT_NAME = "per_trace_timeout"

PARAM_OVERRIDES = {
    "per_trace_timeout": 60,     # 60s wall-clock limit per trace
    "subprocess_timeout": 600,   # 10-min safety net
}
