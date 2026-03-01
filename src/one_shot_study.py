"""One-shot scheduler study with temperature/reasoning-effort sweeps."""

# we need to import some modules because they are used in the policy-as-string
# but we mark them so that linters ignore the fact that they are unused here
from typing import List, Tuple  # noqa: F401
from dotenv import load_dotenv
import argparse
import csv
import json
import logging
import math
import os
from pathlib import Path
import random
import statistics
import time
from datetime import datetime
import uuid

os.environ.setdefault("LITELLM_LOG", "ERROR")
logging.getLogger("eudoxia").setLevel(logging.CRITICAL)
logging.getLogger("LiteLLM").setLevel(logging.CRITICAL)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai._base_client").setLevel(logging.WARNING)

# eudoxia-specific imports
from eudoxia.workload import Pipeline, OperatorState  # noqa: F401
from eudoxia.workload.runtime_status import ASSIGNABLE_STATES  # noqa: F401
from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend  # noqa: F401
from eudoxia.scheduler.decorators import register_scheduler_init, register_scheduler  # noqa: F401
from eudoxia.utils import Priority  # noqa: F401
from eudoxia.simulator import get_param_defaults

from llm import (
    generate_policy,
    setup_cost_tracking,
    reset_cost_tracking,
    get_cost_statistics,
    get_last_request_cost,
)
from prompts import get_user_request
from simulation_utils import generate_traces, get_raw_stats_for_policy, extract_metrics_from_stats


def convert_to_json_serializable(obj):
    """Recursively convert numpy and special float values to JSON-serializable types."""
    import numpy as np

    if isinstance(obj, dict):
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [convert_to_json_serializable(item) for item in obj]
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    if isinstance(obj, (np.floating, np.float64, np.float32)):
        val = float(obj)
        if math.isnan(val):
            return None
        if math.isinf(val):
            return "Infinity" if val > 0 else "-Infinity"
        return val
    if isinstance(obj, float):
        if math.isnan(obj):
            return None
        if math.isinf(obj):
            return "Infinity" if obj > 0 else "-Infinity"
        return obj
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return [convert_to_json_serializable(item) for item in obj.tolist()]
    if hasattr(obj, "__dict__"):
        return convert_to_json_serializable(obj.__dict__)
    return obj


def parse_float_list(raw: str) -> list[float]:
    values = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            values.append(float(part))
    if not values:
        raise ValueError("At least one temperature must be provided")
    return values


def parse_effort_list(raw: str) -> list[str]:
    allowed = {"low", "medium", "high"}
    values = []
    for part in raw.split(","):
        effort = part.strip().lower()
        if effort:
            if effort not in allowed:
                raise ValueError(
                    f"Unsupported reasoning effort '{effort}'. Allowed: {sorted(allowed)}"
                )
            values.append(effort)
    if not values:
        raise ValueError("At least one reasoning effort must be provided")
    return values


def sanitize_model_name(model: str) -> str:
    return model.replace("/", "_").replace(":", "_")


def create_run_dir(model: str) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_safe = sanitize_model_name(model)
    run_dir = Path("src/experiment_runs") / f"one_shot_{model_safe}_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "traces").mkdir(parents=True, exist_ok=True)
    (run_dir / "failed_samples").mkdir(parents=True, exist_ok=True)
    return run_dir


def build_base_params(args: argparse.Namespace) -> dict:
    base_params = get_param_defaults()
    base_params["duration"] = args.duration
    base_params["ticks_per_second"] = args.ticks_per_second
    base_params["num_pools"] = args.num_pools
    base_params["cpus_per_pool"] = args.cpus_per_pool
    base_params["ram_gb_per_pool"] = args.ram_gb_per_pool
    base_params["num_pipelines"] = args.num_pipelines
    base_params["num_operators"] = args.num_operators
    base_params["waiting_seconds_mean"] = args.waiting_seconds_mean
    base_params["multi_operator_containers"] = args.multi_operator_containers
    base_params["interactive_prob"] = args.interactive_prob
    base_params["query_prob"] = args.query_prob
    base_params["batch_prob"] = args.batch_prob
    base_params["random_seed"] = args.random_seed
    return base_params


def generate_trace_set(
    run_dir: Path, base_params: dict, n_traces: int, traces_per_batch: int
) -> list[str]:
    trace_files = []
    original_num_pipelines = base_params["num_pipelines"]
    original_waiting_seconds_mean = base_params["waiting_seconds_mean"]
    trace_dir = run_dir / "traces"

    for batch_idx in range(n_traces):
        scale_factor = 2**batch_idx
        batch_params = base_params.copy()
        batch_params["num_pipelines"] = original_num_pipelines * scale_factor
        batch_params["waiting_seconds_mean"] = original_waiting_seconds_mean * scale_factor
        file_name_prefix = str(
            trace_dir / f"trace_scale{scale_factor}x_{base_params['duration']}s"
        )
        batch_trace_files = generate_traces(
            k=traces_per_batch,
            base_params=batch_params,
            file_name_prefix=file_name_prefix,
        )
        trace_files.extend(batch_trace_files)

    assert len(trace_files) == len(set(trace_files)), "Generated duplicate trace names"
    return trace_files


def build_trial_plan(
    temperatures: list[float], efforts: list[str], trials_per_condition: int, seed: int
) -> list[dict]:
    plan = []
    trial_id = 1
    for temperature in temperatures:
        for effort in efforts:
            condition_id = condition_key(temperature, effort)
            for trial_index in range(1, trials_per_condition + 1):
                plan.append(
                    {
                        "trial_id": trial_id,
                        "condition_id": condition_id,
                        "temperature": temperature,
                        "reasoning_effort": effort,
                        "trial_index_within_condition": trial_index,
                    }
                )
                trial_id += 1
    rng = random.Random(seed)
    rng.shuffle(plan)
    for order, entry in enumerate(plan, start=1):
        entry["execution_order"] = order
    return plan


def condition_key(temperature: float, effort: str) -> str:
    temp_str = str(temperature).replace(".", "p")
    return f"temp_{temp_str}__effort_{effort}"


def save_json(path: Path, payload):
    with open(path, "w") as f:
        json.dump(convert_to_json_serializable(payload), f, indent=2)


def append_jsonl(path: Path, payload):
    with open(path, "a") as f:
        f.write(json.dumps(convert_to_json_serializable(payload)) + "\n")


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def compute_wilson_interval(successes: int, n: int, z: float = 1.959963984540054) -> tuple:
    if n == 0:
        return (None, None)
    p = successes / n
    denom = 1.0 + (z * z) / n
    center = (p + (z * z) / (2.0 * n)) / denom
    margin = z * math.sqrt((p * (1.0 - p) + (z * z) / (4.0 * n)) / n) / denom
    return (center - margin, center + margin)


def pct_change(value: float | None, baseline: float | None) -> float | None:
    if value is None or baseline is None or baseline == 0:
        return None
    return ((value - baseline) / baseline) * 100.0


def classify_generation_error(exc: Exception) -> str:
    if isinstance(exc, AssertionError) and "Extracted key" in str(exc):
        return "key_mismatch"
    return "llm_error"


def write_failed_sample(
    run_dir: Path,
    trial_result: dict,
    policy_code: str | None,
):
    failure_dir = run_dir / "failed_samples" / f"trial_{trial_result['trial_id']:04d}"
    failure_dir.mkdir(parents=True, exist_ok=True)
    save_json(failure_dir / "metadata.json", trial_result)
    if policy_code:
        with open(failure_dir / "policy.py", "w") as f:
            f.write(policy_code)


def run_single_trial(
    trial: dict,
    model: str,
    metric: str,
    baseline_median_metric: float,
    base_params: dict,
    trace_files: list[str],
    verbose: bool,
) -> tuple[dict, str | None]:
    trial_start = time.time()
    trial_start_iso = datetime.fromtimestamp(trial_start).isoformat()
    policy_key = (
        f"oneshot_{trial['condition_id']}_n{trial['trial_index_within_condition']}_"
        f"{uuid.uuid4().hex[:8]}"
    )

    trial_result = {
        "trial_id": trial["trial_id"],
        "execution_order": trial["execution_order"],
        "condition_id": trial["condition_id"],
        "trial_index_within_condition": trial["trial_index_within_condition"],
        "model": model,
        "metric": metric,
        "temperature": trial["temperature"],
        "reasoning_effort": trial["reasoning_effort"],
        "policy_key": policy_key,
        "functional_success": False,
        "better_than_baseline": False,
        "failure_mode": None,
        "error_message": None,
        "median_metric": None,
        "baseline_median_metric": baseline_median_metric,
        "llm_cost": 0.0,
        "llm_first_attempt_wait_seconds": None,
        "generation_seconds": 0.0,
        "simulation_seconds": 0.0,
        "total_seconds": 0.0,
        "start_time": trial_start_iso,
        "end_time": None,
        "llm_params": None,
    }

    generated_policy_code = None
    simulation_start = None
    generation_start = time.time()
    llm_call_start = None
    req_count_before = get_cost_statistics()["num_requests"]

    try:
        try:
            user_request = get_user_request(policy_key, metric)
            llm_call_start = time.time()
            policy_result = generate_policy(
                user_request=user_request,
                feedback_history=[],
                model=model,
                temperature=trial["temperature"],
                policy_key=policy_key,
                verbose=verbose,
                reasoning_effort_override=trial["reasoning_effort"],
            )
            generated_policy_code = policy_result["policy_code"]
            trial_result["llm_params"] = policy_result["llm_params"]
            if not generated_policy_code or not generated_policy_code.strip():
                trial_result["failure_mode"] = "empty_or_parse_error"
                trial_result["error_message"] = "Generated code is empty after cleaning"
                return trial_result, generated_policy_code
        except Exception as exc:
            trial_result["failure_mode"] = classify_generation_error(exc)
            trial_result["error_message"] = str(exc)
            return trial_result, generated_policy_code
        finally:
            trial_result["generation_seconds"] = time.time() - generation_start
            req_count_after = get_cost_statistics()["num_requests"]
            if req_count_after > req_count_before:
                trial_result["llm_cost"] = get_last_request_cost()
            if llm_call_start is not None:
                trial_result["llm_first_attempt_wait_seconds"] = (
                    time.time() - llm_call_start
                )

        try:
            exec(generated_policy_code, globals())
        except Exception as exc:
            trial_result["failure_mode"] = "exec_error"
            trial_result["error_message"] = str(exc)
            return trial_result, generated_policy_code

        simulation_start = time.time()
        try:
            policy_raw_stats = get_raw_stats_for_policy(base_params, trace_files, policy_key)
            if len(policy_raw_stats) != len(trace_files):
                raise RuntimeError(
                    f"Expected {len(trace_files)} trace results, got {len(policy_raw_stats)}"
                )
            policy_stats = extract_metrics_from_stats(policy_raw_stats, metric)
            if len(policy_stats) != len(trace_files):
                raise RuntimeError(
                    f"Expected {len(trace_files)} metric values, got {len(policy_stats)}"
                )
            median_metric = statistics.median(policy_stats)
            trial_result["median_metric"] = median_metric
            trial_result["functional_success"] = True
            if metric == "latency":
                trial_result["better_than_baseline"] = median_metric < baseline_median_metric
            else:
                trial_result["better_than_baseline"] = median_metric > baseline_median_metric
            trial_result["failure_mode"] = "success"
            trial_result["error_message"] = None
        except Exception as exc:
            trial_result["failure_mode"] = "simulation_error"
            trial_result["error_message"] = str(exc)

        return trial_result, generated_policy_code
    finally:
        if simulation_start is not None:
            trial_result["simulation_seconds"] = time.time() - simulation_start
        trial_end = time.time()
        trial_result["total_seconds"] = trial_end - trial_start
        trial_result["end_time"] = datetime.fromtimestamp(trial_end).isoformat()


def write_condition_summary(
    run_dir: Path,
    trial_results: list[dict],
    temperatures: list[float],
    efforts: list[str],
):
    grouped = {}
    for record in trial_results:
        grouped.setdefault(record["condition_id"], []).append(record)

    summary_rows = []
    for temperature in temperatures:
        for effort in efforts:
            ckey = condition_key(temperature, effort)
            rows = grouped.get(ckey, [])
            n = len(rows)
            functional_successes = sum(1 for r in rows if r["functional_success"])
            better_successes = sum(1 for r in rows if r["better_than_baseline"])
            functional_mean = functional_successes / n if n else None
            better_mean = better_successes / n if n else None
            functional_ci_low, functional_ci_high = compute_wilson_interval(
                functional_successes, n
            )
            better_ci_low, better_ci_high = compute_wilson_interval(better_successes, n)
            failure_mode_counts = {}
            for row in rows:
                failure_mode = row["failure_mode"]
                failure_mode_counts[failure_mode] = failure_mode_counts.get(failure_mode, 0) + 1
            llm_wait_samples = [
                r["llm_first_attempt_wait_seconds"]
                for r in rows
                if r["llm_first_attempt_wait_seconds"] is not None
            ]
            mean_llm_wait_seconds = (
                sum(llm_wait_samples) / len(llm_wait_samples) if llm_wait_samples else None
            )

            summary_rows.append(
                {
                    "condition_id": ckey,
                    "temperature": temperature,
                    "reasoning_effort": effort,
                    "n_trials": n,
                    "functional_successes": functional_successes,
                    "better_than_baseline_successes": better_successes,
                    "functional_mean": functional_mean,
                    "functional_wilson_95_low": functional_ci_low,
                    "functional_wilson_95_high": functional_ci_high,
                    "beat_baseline_mean": better_mean,
                    "beat_baseline_wilson_95_low": better_ci_low,
                    "beat_baseline_wilson_95_high": better_ci_high,
                    "mean_llm_wait_seconds": mean_llm_wait_seconds,
                    "failure_mode_counts": failure_mode_counts,
                }
            )

    save_json(run_dir / "condition_summary.json", summary_rows)

    csv_fields = [
        "condition_id",
        "temperature",
        "reasoning_effort",
        "n_trials",
        "functional_successes",
        "better_than_baseline_successes",
        "functional_mean",
        "functional_wilson_95_low",
        "functional_wilson_95_high",
        "beat_baseline_mean",
        "beat_baseline_wilson_95_low",
        "beat_baseline_wilson_95_high",
        "mean_llm_wait_seconds",
        "failure_mode_counts",
    ]
    with open(run_dir / "condition_summary.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()
        for row in summary_rows:
            row_copy = row.copy()
            row_copy["failure_mode_counts"] = json.dumps(row_copy["failure_mode_counts"])
            writer.writerow(convert_to_json_serializable(row_copy))

    return summary_rows


def write_thinktime_change(run_dir: Path, summary_rows: list[dict], temperatures: list[float]):
    by_condition = {row["condition_id"]: row for row in summary_rows}
    out_rows = []
    for temperature in temperatures:
        medium = by_condition.get(condition_key(temperature, "medium"))
        low = by_condition.get(condition_key(temperature, "low"))
        high = by_condition.get(condition_key(temperature, "high"))
        if medium is None:
            continue
        for effort, row in [("low", low), ("high", high)]:
            if row is None:
                continue
            out_rows.append(
                {
                    "temperature": temperature,
                    "reasoning_effort": effort,
                    "functional_mean": row["functional_mean"],
                    "beat_baseline_mean": row["beat_baseline_mean"],
                    "functional_pct_change_vs_medium": pct_change(
                        row["functional_mean"], medium["functional_mean"]
                    ),
                    "beat_baseline_pct_change_vs_medium": pct_change(
                        row["beat_baseline_mean"], medium["beat_baseline_mean"]
                    ),
                }
            )

    fields = [
        "temperature",
        "reasoning_effort",
        "functional_mean",
        "beat_baseline_mean",
        "functional_pct_change_vs_medium",
        "beat_baseline_pct_change_vs_medium",
    ]
    with open(run_dir / "thinktime_change_vs_medium.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in out_rows:
            writer.writerow(convert_to_json_serializable(row))

    return out_rows


def write_overall_summary(
    run_dir: Path,
    trial_results: list[dict],
    summary_rows: list[dict],
    baseline_median_metric: float,
    metric: str,
):
    total_trials = len(trial_results)
    total_functional = sum(1 for r in trial_results if r["functional_success"])
    total_beats = sum(1 for r in trial_results if r["better_than_baseline"])
    total_cost = sum(float(r.get("llm_cost", 0.0) or 0.0) for r in trial_results)
    llm_wait_values = [
        r["llm_first_attempt_wait_seconds"]
        for r in trial_results
        if r["llm_first_attempt_wait_seconds"] is not None
    ]
    avg_llm_wait = (
        sum(llm_wait_values) / len(llm_wait_values) if llm_wait_values else None
    )

    best_functional = None
    best_beating = None
    if summary_rows:
        best_functional = max(
            summary_rows,
            key=lambda r: -1.0 if r["functional_mean"] is None else r["functional_mean"],
        )
        best_beating = max(
            summary_rows,
            key=lambda r: -1.0 if r["beat_baseline_mean"] is None else r["beat_baseline_mean"],
        )

    def fmt_optional(value):
        if value is None:
            return "N/A"
        return f"{value:.4f}"

    lines = [
        "# One-shot Scheduler Study Summary",
        "",
        f"- Total trials: {total_trials}",
        f"- Functional successes: {total_functional} ({(total_functional / total_trials * 100.0) if total_trials else 0.0:.2f}%)",
        f"- Better-than-baseline successes: {total_beats} ({(total_beats / total_trials * 100.0) if total_trials else 0.0:.2f}%)",
        f"- Baseline median {metric}: {baseline_median_metric:.6f}",
        f"- Total LLM cost (sum across trials): ${total_cost:.6f}",
        f"- Average LLM wait per trial: {avg_llm_wait:.4f}s" if avg_llm_wait is not None else "- Average LLM wait per trial: N/A",
        "",
    ]

    if best_functional is not None:
        lines.append(
            "- Best functional mean condition: "
            f"{best_functional['condition_id']} "
            f"(functional_mean={fmt_optional(best_functional['functional_mean'])})"
        )
    if best_beating is not None:
        lines.append(
            "- Best beat-baseline mean condition: "
            f"{best_beating['condition_id']} "
            f"(beat_baseline_mean={fmt_optional(best_beating['beat_baseline_mean'])})"
        )

    with open(run_dir / "overall_summary.md", "w") as f:
        f.write("\n".join(lines).strip() + "\n")


def load_baseline_median(path: Path) -> float:
    with open(path, "r") as f:
        baseline = json.load(f)
    return float(baseline["median_metric"])


def run_study(args: argparse.Namespace):
    load_dotenv()
    assert os.environ.get("OPENAI_API_KEY"), "OPENAI_API_KEY not set in env"

    temperatures = parse_float_list(args.temperatures)
    efforts = parse_effort_list(args.reasoning_efforts)
    if any(abs(t - 1.0) > 1e-9 for t in temperatures):
        raise ValueError(
            "Temperature sweep is disabled for this study version. Use --temperatures 1.0."
        )
    if args.trials_per_condition <= 0:
        raise ValueError("trials-per-condition must be > 0")

    if args.resume:
        if not args.run_dir:
            raise ValueError("--resume requires --run-dir")
        run_dir = Path(args.run_dir)
        if not run_dir.exists():
            raise ValueError(f"Run directory does not exist: {run_dir}")
        with open(run_dir / "run_config.json", "r") as f:
            run_config = json.load(f)
        if run_config.get("study_type") != "one_shot":
            raise ValueError(
                f"Run directory is not a one-shot study (study_type={run_config.get('study_type')})"
            )
        with open(run_dir / "trial_plan.json", "r") as f:
            trial_plan = json.load(f)
        trace_files = run_config["trace_files"]
        base_params = run_config["simulation_params"]
        model = run_config["model"]
        metric = run_config["metric"]
        temperatures = run_config["temperatures"]
        efforts = run_config["reasoning_efforts"]
        baseline_median_metric = load_baseline_median(run_dir / "baseline_results.json")
    else:
        model = args.model
        metric = args.metric
        run_dir = create_run_dir(model)
        base_params = build_base_params(args)
        trace_files = generate_trace_set(
            run_dir=run_dir,
            base_params=base_params,
            n_traces=args.n_traces,
            traces_per_batch=args.traces_per_batch,
        )
        naive_raw_stats = get_raw_stats_for_policy(base_params, trace_files, "naive")
        if len(naive_raw_stats) != len(trace_files):
            raise RuntimeError(
                f"Baseline simulation failed on {len(trace_files) - len(naive_raw_stats)} traces"
            )
        naive_stats = extract_metrics_from_stats(naive_raw_stats, metric)
        if not naive_stats:
            raise RuntimeError("Baseline produced no valid metric values")
        baseline_median_metric = statistics.median(naive_stats)

        save_json(
            run_dir / "baseline_results.json",
            {
                "policy": "naive",
                "metric": metric,
                "metric_values": naive_stats,
                "median_metric": baseline_median_metric,
                "raw_stats": [s.to_dict() for s in naive_raw_stats],
            },
        )

        trial_plan = build_trial_plan(
            temperatures=temperatures,
            efforts=efforts,
            trials_per_condition=args.trials_per_condition,
            seed=args.shuffle_seed,
        )
        save_json(run_dir / "trial_plan.json", trial_plan)
        save_json(
            run_dir / "run_config.json",
            {
                "study_type": "one_shot",
                "model": model,
                "metric": metric,
                "temperatures": temperatures,
                "reasoning_efforts": efforts,
                "trials_per_condition": args.trials_per_condition,
                "shuffle_seed": args.shuffle_seed,
                "simulation_params": base_params,
                "n_traces": args.n_traces,
                "traces_per_batch": args.traces_per_batch,
                "trace_files": trace_files,
                "run_start_time": datetime.now().isoformat(),
            },
        )

    trials_path = run_dir / "trials.jsonl"
    existing_trials = load_jsonl(trials_path)
    inconsistent_existing_trials = []
    for entry in existing_trials:
        llm_params = entry.get("llm_params") or {}
        llm_reasoning_effort = llm_params.get("reasoning_effort")
        planned_reasoning_effort = entry.get("reasoning_effort")
        if (
            planned_reasoning_effort is not None
            and llm_reasoning_effort != planned_reasoning_effort
        ):
            inconsistent_existing_trials.append(int(entry["trial_id"]))
            continue

        llm_temperature = llm_params.get("temperature")
        planned_temperature = entry.get("temperature")
        if (
            planned_temperature is not None
            and "temperature" in llm_params
            and float(llm_temperature) != float(planned_temperature)
        ):
            inconsistent_existing_trials.append(int(entry["trial_id"]))
    if inconsistent_existing_trials:
        sample_ids = inconsistent_existing_trials[:5]
        raise RuntimeError(
            "Found existing trials where llm_params do not match planned condition "
            "(sample trial_ids: "
            f"{sample_ids}). Start a new run to keep conditions comparable."
        )

    completed_ids = {int(entry["trial_id"]) for entry in existing_trials}
    pending_trials = [trial for trial in trial_plan if int(trial["trial_id"]) not in completed_ids]

    print(f"Run directory: {run_dir}")
    print(f"Total trials in plan: {len(trial_plan)}")
    print(f"Already completed: {len(completed_ids)}")
    print(f"Pending: {len(pending_trials)}")
    print(f"Baseline median {metric}: {baseline_median_metric:.6f}")

    setup_cost_tracking()
    reset_cost_tracking()

    for idx, trial in enumerate(pending_trials, start=1):
        print(
            f"\n[{idx}/{len(pending_trials)}] trial_id={trial['trial_id']} "
            f"temp={trial['temperature']} effort={trial['reasoning_effort']}"
        )
        result, generated_policy_code = run_single_trial(
            trial=trial,
            model=model,
            metric=metric,
            baseline_median_metric=baseline_median_metric,
            base_params=base_params,
            trace_files=trace_files,
            verbose=args.verbose,
        )
        append_jsonl(trials_path, result)
        if result["failure_mode"] != "success":
            write_failed_sample(run_dir, result, policy_code=generated_policy_code)
        print(
            f"  outcome={result['failure_mode']} "
            f"functional={result['functional_success']} "
            f"better={result['better_than_baseline']} "
            f"time={result['total_seconds']:.2f}s "
            f"cost=${result['llm_cost']:.6f} "
            f"first_wait={result['llm_first_attempt_wait_seconds']}"
        )

    final_trials = load_jsonl(trials_path)
    summary_rows = write_condition_summary(
        run_dir=run_dir,
        trial_results=final_trials,
        temperatures=temperatures,
        efforts=efforts,
    )
    write_thinktime_change(run_dir, summary_rows, temperatures)
    write_overall_summary(run_dir, final_trials, summary_rows, baseline_median_metric, metric)

    print("\nStudy complete.")
    print(f"- trials: {run_dir / 'trials.jsonl'}")
    print(f"- summary json: {run_dir / 'condition_summary.json'}")
    print(f"- summary csv: {run_dir / 'condition_summary.csv'}")
    print(f"- thinktime deltas: {run_dir / 'thinktime_change_vs_medium.csv'}")
    print(f"- overall summary: {run_dir / 'overall_summary.md'}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one-shot scheduler study with temperature and reasoning-effort sweeps.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--model", default="gpt-5.2-2025-12-11", help="Model name")
    parser.add_argument(
        "--metric",
        choices=["latency", "throughput"],
        default="latency",
        help="Optimization metric for better-than-baseline comparison",
    )
    parser.add_argument(
        "--temperatures",
        default="1.0",
        help="Comma-separated temperature list",
    )
    parser.add_argument(
        "--reasoning-efforts",
        default="low,medium,high",
        help="Comma-separated reasoning effort list",
    )
    parser.add_argument(
        "--trials-per-condition",
        type=int,
        default=20,
        help="Number of independent one-shot trials per condition",
    )
    parser.add_argument(
        "--shuffle-seed", type=int, default=42, help="Shuffle seed for trial execution order"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume an interrupted run (requires --run-dir)",
    )
    parser.add_argument(
        "--run-dir",
        type=str,
        default=None,
        help="Existing run directory to resume",
    )

    # full defaults (aligned with src/main.py CLI defaults where applicable)
    parser.add_argument("--n-traces", type=int, default=5)
    parser.add_argument("--traces-per-batch", type=int, default=2)
    parser.add_argument("--duration", type=int, default=600)
    parser.add_argument("--ticks-per-second", type=int, default=1000)
    parser.add_argument("--num-pools", type=int, default=1)
    parser.add_argument("--cpus-per-pool", type=int, default=64)
    parser.add_argument("--ram-gb-per-pool", type=int, default=500)
    parser.add_argument("--num-pipelines", type=int, default=10)
    parser.add_argument("--num-operators", type=int, default=10)
    parser.add_argument("--waiting-seconds-mean", type=float, default=10.0)
    parser.add_argument("--multi-operator-containers", action="store_true")
    parser.add_argument("--interactive-prob", type=float, default=0.3)
    parser.add_argument("--query-prob", type=float, default=0.1)
    parser.add_argument("--batch-prob", type=float, default=0.6)
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--verbose", "-v", action="store_true")
    return parser


if __name__ == "__main__":
    run_study(build_parser().parse_args())
