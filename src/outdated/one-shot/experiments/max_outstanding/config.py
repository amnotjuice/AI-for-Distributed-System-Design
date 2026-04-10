"""Experiment: limit maximum outstanding pipelines.

Caps the number of pipelines in the outstanding queue. Arrivals beyond
the limit are dropped. This bounds queue size and prevents O(n) scans
from becoming expensive, at the cost of failing excess pipelines.
"""

EXPERIMENT_NAME = "max_outstanding"

PARAM_OVERRIDES = {
    "max_outstanding": 200,      # bounds queue while keeping ~45% of pipelines
    "per_trace_timeout": None,   # disable — queue cap should bound runtime
    "subprocess_timeout": 600,   # 10-min safety net
}
