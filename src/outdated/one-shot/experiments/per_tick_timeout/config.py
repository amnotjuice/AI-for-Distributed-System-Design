"""Experiment: per-tick scheduler timeout.

Fails any simulation where a single scheduler.run_one_tick() call
exceeds tick_timeout_ms. This directly penalises schedulers with
expensive per-tick logic (O(n) queue scans, list.pop(0), etc.).
"""

EXPERIMENT_NAME = "per_tick_timeout"

PARAM_OVERRIDES = {
    "tick_timeout_ms": 50.0,     # 50ms per tick — preserves top schedulers, kills O(n²)
    "per_trace_timeout": None,   # disable — tick timeout is the constraint
    "subprocess_timeout": 600,   # 10-min safety net
}
