#!/usr/bin/env python3
"""Subprocess worker for one_iteration_tool.py.

Runs one (scheduler, trace, params) simulation and prints the result as:
    __RESULT__{"ok": true, "latency": 1.234}
or
    __RESULT__{"ok": false, "error": "..."}

Called by one_iteration_tool.py — not intended to be run directly.
"""
import json
import re
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 4:
        print("__RESULT__" + json.dumps({"ok": False, "error": "expected 3 args"}))
        return

    _, scheduler_file, trace_file, params_json = sys.argv
    params = json.loads(params_json)

    src = Path(scheduler_file).read_text()
    key_match = re.search(r"""@register_scheduler\((?:key=)?['"]([^'"]+)['"]\)""", src)
    if not key_match:
        print("__RESULT__" + json.dumps({"ok": False, "error": "no @register_scheduler key found"}))
        return

    policy_key = key_match.group(1)

    try:
        from typing import List, Tuple
        from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
        from eudoxia.scheduler.decorators import register_scheduler, register_scheduler_init
        from eudoxia.utils import Priority
        from eudoxia.workload import OperatorState, Pipeline
        from eudoxia.workload.runtime_status import ASSIGNABLE_STATES

        exec(src, {
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
        })
    except Exception as exc:
        print("__RESULT__" + json.dumps({"ok": False, "error": f"exec error: {exc}"}))
        return

    try:
        from eudoxia.simulator import run_simulator
        from eudoxia.workload.csv_io import CSVWorkloadReader

        params["scheduler_algo"] = policy_key
        with open(trace_file) as f:
            reader = CSVWorkloadReader(f)
            workload = reader.get_workload(params["ticks_per_second"])
        stats = run_simulator(params, workload=workload)
        print("__RESULT__" + json.dumps({"ok": True, "latency": stats.adjusted_latency()}))
    except Exception as exc:
        print("__RESULT__" + json.dumps({"ok": False, "error": str(exc)}))


if __name__ == "__main__":
    main()
