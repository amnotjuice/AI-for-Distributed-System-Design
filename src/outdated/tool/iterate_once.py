"""Tool: iterate once on an existing scheduler using LLM.

Input:  scheduler source code + simulation results
Output: improved scheduler source code

Usage:
    from tool.iterate_once import iterate_once

    new_code = iterate_once(
        scheduler_code=old_code,
        simulation_results={"functional": True, "median_latency": 105.5, ...},
        policy_key="scheduler_high_003_r1",
        metric="latency",
    )
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ.setdefault("LITELLM_LOG", "ERROR")

from llm import generate_policy_with_llm, build_system_context, clean_generated_code
from prompts import get_user_request
from eudoxia.__main__ import SCHEDULER_TEMPLATE


def _build_system_context() -> str:
    """Build the same system context used by one-shot and main.py."""
    starter_template = SCHEDULER_TEMPLATE[
        SCHEDULER_TEMPLATE.index("@register_scheduler_init") :
    ]
    starter_template = starter_template.format(scheduler_name="example")
    return build_system_context(
        files=["eudoxia_bauplan.md"],
        sections={"Starter Scheduler Template": f"```python\n{starter_template}\n```"},
    )


def _build_feedback_minimal(simulation_results: dict, metric: str) -> str:
    """Build feedback string matching main.py's format (minimal info)."""
    if not simulation_results.get("functional"):
        failure_mode = simulation_results.get("failure_mode", "unknown")
        error_msg = simulation_results.get("error_message", "")
        return f"The policy code resulted in a simulation error: {failure_mode} {error_msg}\n\nPlease fix the error and generate a corrected policy."

    median_val = simulation_results.get(f"median_{metric}", 0)

    # main.py updates best_median_metric BEFORE building feedback,
    # so it always compares against itself
    return f"Great! This policy achieved {metric} of {median_val:.2f} (vs previous best: {median_val:.2f}). This is an improvement! Can you further optimize it?"


def _build_feedback_rich(simulation_results: dict, metric: str, raw_stats: list[dict] | None = None) -> str:
    """Build rich feedback with per-trace SimulatorStats breakdown.

    Args:
        simulation_results: Dict from analysis.jsonl.
        metric: "latency" or "throughput".
        raw_stats: List of SimulatorStats.to_dict() per trace (from re-running simulation).
    """
    if not simulation_results.get("functional"):
        failure_mode = simulation_results.get("failure_mode", "unknown")
        error_msg = simulation_results.get("error_message", "")
        return f"The policy code resulted in a simulation error: {failure_mode} {error_msg}\n\nPlease fix the error and generate a corrected policy."

    median_val = simulation_results.get(f"median_{metric}", 0)
    baseline_median = simulation_results.get("baseline_median", None)

    lines = [f"This policy achieved median {metric} of {median_val:.2f}."]

    if baseline_median is not None:
        improvement = (baseline_median - median_val) / baseline_median * 100 if metric == "latency" else (median_val - baseline_median) / baseline_median * 100
        lines.append(f"Baseline (naive scheduler) median {metric}: {baseline_median:.2f} — your policy is {improvement:.1f}% better.")

    if raw_stats:
        lines.append("\nPer-trace simulation statistics (JSON):")
        lines.append(json.dumps(raw_stats, indent=2))

    lines.append(f"\nCan you further optimize this scheduler to reduce {metric}?")

    return "\n".join(lines)


def iterate_once(
    scheduler_code: str,
    simulation_results: dict,
    policy_key: str,
    metric: str = "latency",
    model: str = "gpt-5.2-2025-12-11",
    feedback_mode: str = "minimal",
    raw_stats: list[dict] | None = None,
) -> str:
    """Generate an improved scheduler from existing code + results.

    Args:
        scheduler_code: Source code of the existing scheduler.
        simulation_results: Dict from analysis.jsonl (functional, median_latency, etc.)
        policy_key: Key the LLM must use in @register_scheduler decorators.
        metric: "latency" or "throughput".
        model: LLM model to use.
        feedback_mode: "minimal" (matches main.py) or "rich" (per-trace stats from SimulatorStats).
        raw_stats: List of SimulatorStats.to_dict() per trace. Required for "rich" mode.

    Returns:
        Improved scheduler source code (string).
    """
    system_context = _build_system_context()
    user_prompt = get_user_request(policy_key, metric)

    if feedback_mode == "rich":
        feedback = _build_feedback_rich(simulation_results, metric, raw_stats=raw_stats)
    else:
        feedback = _build_feedback_minimal(simulation_results, metric)

    # Simulate main.py's second round: code as assistant message + feedback
    feedback_history = [
        {"policy_code": scheduler_code, "feedback": feedback},
    ]

    reasoning_effort = None
    temperature = 1.0
    if model.startswith("gpt-5") or model.startswith("claude-opus-4"):
        reasoning_effort = "high"

    generated, _, _ = generate_policy_with_llm(
        user_request=user_prompt,
        system_context=system_context,
        feedback_history=feedback_history,
        model=model,
        temperature=temperature,
        reasoning_effort_override=reasoning_effort,
    )

    code = clean_generated_code(generated)

    # Fix key if LLM used a different one
    key_match = re.search(r"""@register_scheduler\((?:key=)?['"]([^'"]+)['"]\)""", code)
    if key_match and key_match.group(1) != policy_key:
        code = code.replace(key_match.group(1), policy_key)

    return code
