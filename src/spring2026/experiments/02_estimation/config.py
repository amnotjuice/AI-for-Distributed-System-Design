"""Experiment 02: one-shot, vary estimation noise, low reasoning.

Fig 2 from the paper.
- Conditions: no estimation / sigma=0.0 (oracle) / sigma=0.75 / sigma=1.5
- Reasoning level: low (fixed)
- 50 schedulers per condition

Estimation conditions use estimator_algo="noisy" + noisy_estimator_sigma,
consistent with one-shot-est/config.py.
"""

PARAM_OVERRIDES = {
    "per_trace_timeout": None,
}

N_SCHEDULERS = 50
REASONING_EFFORT = "low"
# Keys into config.ESTIMATOR_CONDITIONS
ESTIMATION_CONDITIONS = ["no_estimation", "sigma_0.0", "sigma_0.75", "sigma_1.5"]
MODEL = "gpt-5.2-2025-12-11"
