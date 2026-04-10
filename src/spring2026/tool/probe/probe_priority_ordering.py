import re
import sys
import logging
from pathlib import Path

_PROBE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_PROBE_DIR.parent))                # src/spring2026/tool  (config.py)
sys.path.insert(0, str(_PROBE_DIR.parent.parent.parent))  # src/                 (simulation_utils.py)
logging.getLogger("eudoxia").setLevel(logging.CRITICAL)

from simulation_utils import get_raw_stats_for_policy, deregister_scheduler


def probe_priority_ordering(
    scheduler_file: str,
    trace_files: list[str],
    base_params: dict,
) -> dict:
    """Verify the scheduler services QUERY pipelines before BATCH.

    Runs with ram_gb_per_pool=64 so only one 40 GB operator fits at a time,
    forcing the scheduler to choose which priority class runs first.
    Passes if mean QUERY latency < mean BATCH latency and both classes complete.
    """
    src = Path(scheduler_file).read_text()
    key_match = re.search(r"""@register_scheduler\((?:key=)?['"]([^'"]+)['"]\)""", src)
    if not key_match:
        return {"functional": False, "failure_mode": "no_scheduler_key"}

    try:
        from typing import List, Tuple

        from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
        from eudoxia.scheduler.decorators import register_scheduler, register_scheduler_init
        from eudoxia.utils import Priority
        from eudoxia.workload import OperatorState, Pipeline
        from eudoxia.workload.runtime_status import ASSIGNABLE_STATES

        worker_globals = {
            "__builtins__": __builtins__,
            "__name__": "__worker__",
            "List": List, "Tuple": Tuple,
            "Pipeline": Pipeline, "OperatorState": OperatorState,
            "ASSIGNABLE_STATES": ASSIGNABLE_STATES,
            "Assignment": Assignment, "ExecutionResult": ExecutionResult,
            "Suspend": Suspend,
            "register_scheduler_init": register_scheduler_init,
            "register_scheduler": register_scheduler,
            "Priority": Priority,
        }
        deregister_scheduler(key_match.group(1))
        exec(src, worker_globals)
    except Exception as e:
        return {"functional": False, "failure_mode": "exec_error", "error_message": str(e)}

    params = base_params.copy()
    params["ram_gb_per_pool"] = 64

    try:
        raw = get_raw_stats_for_policy(params, trace_files[:1], key_match.group(1))

        if len(raw) != 1:
            return {
                "functional": False, "failure_mode": "simulation_error",
                "error_message": f"Got {len(raw)}/1 results",
            }

        stats = raw[0]

        if stats.pipelines_query.completion_count == 0:
            return {
                "functional": False, "failure_mode": "no_query_completions",
                "error_message": "No QUERY pipelines completed",
            }
        if stats.pipelines_batch.completion_count == 0:
            return {
                "functional": False, "failure_mode": "no_batch_completions",
                "error_message": "No BATCH pipelines completed",
            }

        query_lat = stats.pipelines_query.mean_latency_seconds
        batch_lat = stats.pipelines_batch.mean_latency_seconds

        if query_lat >= batch_lat:
            return {
                "functional": False, "failure_mode": "priority_inversion",
                "error_message": (
                    f"QUERY mean latency ({query_lat:.2f}s) >= BATCH mean latency ({batch_lat:.2f}s); "
                    "scheduler does not honour priority ordering"
                ),
            }

        return {"functional": True, "failure_mode": "success"}

    except Exception as e:
        return {"functional": False, "failure_mode": "simulation_error", "error_message": str(e)}
