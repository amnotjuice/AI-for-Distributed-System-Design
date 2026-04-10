"""Experiment: one-shot scheduling with memory estimator.

Schedulers are generated with knowledge of op.estimate.mem_peak_gb,
allowing RAM-aware sizing decisions without relying on OOM feedback.

Evaluate the same generated schedulers under four estimator noise levels:
  - sigma_0.0:  oracle (perfect estimates)
  - sigma_0.5:  low noise
  - sigma_1.0:  medium noise
  - sigma_1.5:  high noise
"""

EXPERIMENT_NAME = "estimator"

# Base overrides applied to all evaluations in this experiment.
# Individual evaluation conditions set estimator_algo and sigma separately.
PARAM_OVERRIDES = {
    "per_trace_timeout": None,
    "subprocess_timeout": 600,
}

ESTIMATOR_CONDITIONS = {
    "sigma_0.0": {
        "estimator_algo": "noisy",
        "noisy_estimator_sigma": 0.0,
    },
    "sigma_0.5": {
        "estimator_algo": "noisy",
        "noisy_estimator_sigma": 0.5,
    },
    "sigma_1.0": {
        "estimator_algo": "noisy",
        "noisy_estimator_sigma": 1.0,
    },
    "sigma_1.5": {
        "estimator_algo": "noisy",
        "noisy_estimator_sigma": 1.5,
    },
}
