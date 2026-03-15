"""Shared constants and helpers for one-shot experiments."""

from __future__ import annotations

import math
from pathlib import Path

# Keep aligned with src/main.py and src/one_shot_study.py defaults.
CANONICAL_SIM_PARAMS = {
    "duration": 600,
    "ticks_per_second": 1000,
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
}

SUPPORTED_EFFORTS = ["none", "low", "medium", "high"]

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ONE_SHOT_DIR = Path(__file__).resolve().parent


def get_canonical_base_params() -> dict:
    from eudoxia.simulator import get_param_defaults

    params = get_param_defaults()
    params.update(CANONICAL_SIM_PARAMS)
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
