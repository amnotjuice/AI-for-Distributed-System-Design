"""Shared config for spring2026 experiments."""

from __future__ import annotations

import math
from pathlib import Path

SPRING2026_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = SPRING2026_DIR.parent.parent
TRACES_DIR = SPRING2026_DIR / "traces"
SCHEDULERS_DIR = SPRING2026_DIR / "schedulers"
RESULTS_DIR = SPRING2026_DIR / "results"
PLOTS_DIR = SPRING2026_DIR / "plots"
EXPERIMENTS_DIR = SPRING2026_DIR / "experiments"

# ---------------------------------------------------------------------------
# Experiment registry
# ---------------------------------------------------------------------------

EXPERIMENTS = {
    "01_reasoning":           "Fig 1 — one-shot, vary reasoning level (no estimation)",
    "02_estimation":          "Fig 2 — one-shot, vary estimation noise (medium reasoning)",
    "03_two_iter_best_worst": "Fig 3 — two-iter: best/worst/median schedulers, rich vs simple context",
    "04_two_iter_all":        "Fig 4 — two-iter: all contexts, % time v2 beats v1",
    "05_two_shot_perf":       "Fig 5 — two-shot perf: shorter/coarser simulations",
    "06_multi_iter":          "Fig 6 — 10 scenarios × 50 iterations, latency vs iteration",
    "07_cross_eval":          "Fig 7 — cross-eval heatmap: scheduler X on workload Y",
    "08_adapt_speed":         "Fig 8 — iterations to adapt scheduler to new scenario",
    "09_general_purpose":     "Fig 9 — general-purpose scheduler: 50 iters, avg over 10 scenarios",
}

# ---------------------------------------------------------------------------
# Canonical simulation parameters
# ---------------------------------------------------------------------------

CANONICAL_SIM_PARAMS = {
    "duration": 3600,           # 60 minutes
    "ticks_per_second": 100,    # coarser than old 1000
    "num_pools": 1,
    "cpus_per_pool": 64,
    "ram_gb_per_pool": 500,
    "num_pipelines": 10,
    "num_operators": 10,
    "waiting_seconds_mean": 10.0,
    "multi_operator_containers": False,
    "interactive_prob": 0.3,
    "query_prob": 0.1,
    "batch_prob": 0.6,
    "random_seed": 42,
    "per_trace_timeout": None,
    "subprocess_timeout": 600,   # 10-min wall clock safety net per scheduler
    # Max job time = 6 minutes (eudoxia param name: max_job_seconds)
    "max_job_seconds": 360,
}

# Prototype mode: fast/cheap, results won't be meaningful but infra is validated
PROTOTYPE_OVERRIDES = {
    "duration": 60,             # 1 minute
    "ticks_per_second": 10,
    "subprocess_timeout": 120,
}

DEFAULT_MODEL = "gpt-5.2-2025-12-11"
SUPPORTED_EFFORTS = ["none", "low", "medium", "high"]

# Estimation conditions: maps label → sim param overrides
ESTIMATOR_CONDITIONS = {
    "no_estimation": {},  # no estimator at all
    "sigma_0.0": {"estimator_algo": "noisy", "noisy_estimator_sigma": 0.0},     # oracle
    "sigma_0.75": {"estimator_algo": "noisy", "noisy_estimator_sigma": 0.75},   # medium noise
    "sigma_1.5": {"estimator_algo": "noisy", "noisy_estimator_sigma": 1.5},     # high noise
}

# Feedback modes for iteration experiments
FEEDBACK_MODES = ["simple", "rich"]

# Two-shot perf conditions: maps label → sim param overrides (one param varied at a time)
TWO_SHOT_PERF_CONDITIONS = {
    "dur3600_ticks100": {"duration": 3600, "ticks_per_second": 100},  # full (baseline)
    "dur1800_ticks100": {"duration": 1800, "ticks_per_second": 100},
    "dur900_ticks100":  {"duration":  900, "ticks_per_second": 100},
    "dur3600_ticks50":  {"duration": 3600, "ticks_per_second":  50},
    "dur3600_ticks25":  {"duration": 3600, "ticks_per_second":  25},
}


def get_canonical_base_params(prototype: bool = False) -> dict:
    from eudoxia.simulator import get_param_defaults

    params = get_param_defaults()
    params.update(CANONICAL_SIM_PARAMS)
    if prototype:
        params.update(PROTOTYPE_OVERRIDES)
    return params


def wilson_interval(
    successes: int, n: int, z: float = 1.96
) -> tuple[float | None, float | None]:
    if n <= 0:
        return None, None
    p = successes / n
    denom = 1.0 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / denom
    return max(0.0, center - margin), min(1.0, center + margin)
