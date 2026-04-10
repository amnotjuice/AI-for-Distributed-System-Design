import re
import sys
import logging
from pathlib import Path

_PROBE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_PROBE_DIR.parent))                # src/spring2026/tool  (config.py)
sys.path.insert(0, str(_PROBE_DIR.parent.parent.parent))  # src/                 (simulation_utils.py)
logging.getLogger("eudoxia").setLevel(logging.CRITICAL)

from simulation_utils import (
    get_raw_stats_for_policy,
    deregister_scheduler,
)

def probe_suspend_run(
    scheduler_file: str,
    trace_files: list[str],
    base_params: dict,
) -> dict:

    src = Path(scheduler_file).read_text()

    if "Suspend(" not in src:
        return {"functional": False, "failure_mode": "no_suspend_usage", "error_message": "Source code could does not contain line to suspend pipelines"}

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

    simulation_params = base_params.copy()
    simulation_params["ram_gb_per_pool"] = 64
    
    try:
        raw = get_raw_stats_for_policy(simulation_params, trace_files[:1], key_match.group(1))    

        if len(raw) != 1:

            return {
                "functional": False, "failure_mode": "simulation_error",
                "error_message": f"Got {len(raw)}/1 results",
            }

        
        if raw[0].suspensions == 0:
            return {
                "functional": False, "failure_mode": "no_suspensions",
                "error_message": f"Got a suspension rate of 0 when suspensions should be higher",
            }


        return {
            "functional": True, "failure_mode": "success",
        }

    except Exception as e:
        return {"functional": False, "failure_mode": "simulation_error", "error_message": str(e)}
