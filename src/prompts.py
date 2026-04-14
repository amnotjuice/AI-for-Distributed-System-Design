# System prompt template for policy generation
POLICY_GENERATION_SYSTEM_PROMPT = """{context}

# Instructions

Based on the above context about the Eudoxia simulator and Bauplan scheduling, generate a new scheduling policy.

Your response should be ONLY valid Python code that:
1. Uses the @register_scheduler_init decorator with the EXACT key provided in the user request (do NOT generate your own key)
2. Uses the @register_scheduler decorator with the SAME EXACT key provided in the user request
3. Implements the init function to set up any necessary state on the scheduler object `s`
4. Implements the scheduler function with the correct signature: (s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]
5. Returns proper suspensions and assignments lists
6. Follow the access patterns from the examples, especially for accessing executor pools and creating Assignment objects

Available classes and their usage based on the examples:
- Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE - priority levels
- Assignment(ops=op_list, cpu=cpu_amount, ram=ram_amount, priority=priority, pool_id=pool_id, pipeline_id=pipeline.pipeline_id) - to create assignments
- Suspend(container_id, pool_id) - to create suspensions
- s.executor.num_pools - number of available pools
- s.executor.pools[i].avail_cpu_pool - available CPU in pool i
- s.executor.pools[i].avail_ram_pool - available RAM in pool i
- s.executor.pools[i].max_cpu_pool - max CPU in pool i
- s.executor.pools[i].max_ram_pool - max RAM in pool i
- Pipeline has .priority, .pipeline_id, .values (DAG of operators), and .runtime_status() method
- ExecutionResult has .priority, .ops, .container_id, .pool_id, .ram, .cpu, .error, and .failed() method
- pipeline.runtime_status().get_ops(states, require_parents_complete=True/False) - get operators matching criteria
- pipeline.runtime_status().is_pipeline_successful() - returns True if all operators completed
- OperatorState enum - PENDING, ASSIGNED, RUNNING, SUSPENDING, COMPLETED, FAILED (ASSIGNABLE_STATES lists states that can transition to ASSIGNED: PENDING and FAILED)

Do NOT include any explanations, markdown formatting, or import statements. Return ONLY the code for the policy functions, but rememember to add comments inside the Python code itself to clearly explain the logic of your policy, and use the function docstring
to give an overview of the main idea as if it were a standard functin comment. If you make use of any new helper functions, define them within the same code block and make sure to add the relevant
import statements inside the function itself, should they be needed.

Do NOT generate your own poliy key - you MUST use exactly what is provided in the user request.
"""

ITERATION_SYSTEM_PROMPT = """{context}

## Scheduler Registration

Every scheduler must register two functions using decorators:

@register_scheduler_init(key="myscheduler")
def init(s):
    s.queue = []  # s also has s.executor, s.params

@register_scheduler(key="myscheduler")
def schedule(s, results, pipelines):
    suspensions = []
    assignments = []
    return suspensions, assignments

## Operator State Machine

PENDING -> ASSIGNED -> RUNNING -> COMPLETED; FAILED -> ASSIGNED (retry); SUSPENDING -> PENDING
ASSIGNABLE_STATES = {{PENDING, FAILED}}

## OOM Behavior

When a container exceeds its RAM limit, it is killed and operators fail. Retry failed operators with more RAM. Resource estimates can be wrong in either direction: overestimates waste capacity, underestimates cause OOM. OOM failures are recoverable through retry, so be slightly aggressive with allocation rather than overly conservative.

## API Reference

- Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE — priority levels (QUERY is highest)
- Assignment(ops=op_list, cpu=cpu_amount, ram=ram_amount, priority=priority, pool_id=pool_id, pipeline_id=pipeline.pipeline_id) — to create assignments
- Suspend(container_id, pool_id) — to create suspensions
- s.executor.num_pools — number of available pools
- s.executor.pools[i].avail_cpu_pool — available CPU in pool i
- s.executor.pools[i].avail_ram_pool — available RAM in pool i
- s.executor.pools[i].max_cpu_pool — max CPU in pool i
- s.executor.pools[i].max_ram_pool — max RAM in pool i
- Pipeline has .priority, .pipeline_id, .values (DAG of operators), and .runtime_status() method
- ExecutionResult has .priority, .ops, .container_id, .pool_id, .ram, .cpu, .error, and .failed() method
- pipeline.runtime_status().get_ops(states, require_parents_complete=True/False) — get operators matching states
- pipeline.runtime_status().is_pipeline_successful() — returns True if all operators completed
- OperatorState enum: PENDING, ASSIGNED, RUNNING, SUSPENDING, COMPLETED, FAILED (ASSIGNABLE_STATES = {{PENDING, FAILED}})

Available globals (no imports needed): List, Tuple, Pipeline, OperatorState, ASSIGNABLE_STATES, Assignment, ExecutionResult, Suspend, register_scheduler_init, register_scheduler, Priority

Output ONLY valid Python code. No markdown fences, no explanation.
"""


def get_user_request(policy_key: str, metric: str) -> str:
    """Generate user request with the provided policy key.

    Args:
        policy_key: The policy key that the LLM should use in @register_scheduler decorators
        metric: The metric to focus on improving in the policy
    Returns:
        The formatted user request string
    """
    return f"""
Starting from the naive policy provided as example, try to improve it by focusing on improving {metric}, for example leveraging the concept of priority.
Start with small improvements first, targeting obvious flaws, and make the policy complex gradually, only after you
have working code and a direction for improvement. Make sure to consider the results of previous attempts and the feedback provided
as you generate a new policy.

IMPORTANT: Use the following EXACT key in both @register_scheduler_init and @register_scheduler decorators: "{policy_key}"
Do NOT generate your own key - you MUST use exactly: "{policy_key}"
""".strip()


def get_user_request_v2_est(policy_key: str) -> str:
    """get_user_request_v2 with estimator context added.

    Args:
        policy_key: The policy key that the LLM should use in @register_scheduler decorators
    Returns:
        The formatted user request string
    """
    return f"""
Design a scheduling policy that minimizes the following objective (lower is better):

## Objective Function

For each pipeline, assign a latency value:
  - Completed pipeline: actual end-to-end latency in seconds
  - Incomplete or failed pipeline: 2 × max_job_seconds (e.g. 720s if max_job_seconds=360)

These per-pipeline latencies are then combined into a weighted average across all arrived pipelines:
  score = (sum of query_latency × 10  +  sum of interactive_latency × 5  +  sum of batch_latency × 1)
          / (query_arrivals × 10  +  interactive_arrivals × 5  +  batch_arrivals × 1)

Priority weights: query=10x, interactive=5x, batch=1x.
Incomplete and failed pipelines directly inflate the score through the penalty term — they are not ignored.
Aim for high completion rate alongside low latency.

## Estimator

Each operator has an `op.estimate.mem_peak_gb` field (float or None).
This is a pre-computed estimate of the operator's peak memory usage in GB.
It may be None if the estimator has not yet run for that operator — always check before using.

You can use this estimate to make smarter memory-aware scheduling decisions, for example:
  - Skip assigning an operator if `op.estimate.mem_peak_gb` exceeds available RAM in all pools
  - Prefer pools where the estimated memory fits without causing OOM
  - Prioritize operators with smaller memory footprint when resources are tight

Note: the estimate is noisy — treat it as a hint, not a guarantee.

## Guidelines

- Start with small improvements over the naive FIFO baseline.
- Protect query and interactive latency; they dominate the score.
- Avoid designs that drop or starve pipelines — each failure adds 720s to the weighted sum.
- Use `op.estimate.mem_peak_gb` to reduce OOM failures and improve memory utilization.
- Make the policy complex gradually, only after you have working code.
- Make sure to consider the results of previous attempts and the feedback provided.

IMPORTANT: Use the following EXACT key in both @register_scheduler_init and @register_scheduler decorators: "{policy_key}"
Do NOT generate your own key - you MUST use exactly: "{policy_key}"
""".strip()


def get_user_request_v2(policy_key: str) -> str:
    """Generate user request with explicit weighted-latency objective.

    Args:
        policy_key: The policy key that the LLM should use in @register_scheduler decorators
    Returns:
        The formatted user request string
    """
    return f"""
Design a scheduling policy that minimizes the following objective (lower is better):

## Objective Function

For each pipeline, assign a latency value:
  - Completed pipeline: actual end-to-end latency in seconds
  - Incomplete or failed pipeline: 2 × max_job_seconds (e.g. 720s if max_job_seconds=360)

These per-pipeline latencies are then combined into a weighted average across all arrived pipelines:
  score = (sum of query_latency × 10  +  sum of interactive_latency × 5  +  sum of batch_latency × 1)
          / (query_arrivals × 10  +  interactive_arrivals × 5  +  batch_arrivals × 1)

Priority weights: query=10x, interactive=5x, batch=1x.
Incomplete and failed pipelines directly inflate the score through the penalty term — they are not ignored.
Aim for high completion rate alongside low latency.

## Guidelines

- Start with small improvements over the naive FIFO baseline.
- Protect query and interactive latency; they dominate the score.
- Avoid designs that drop or starve pipelines — each failure adds 720s to the weighted sum.
- Make the policy complex gradually, only after you have working code.
- Make sure to consider the results of previous attempts and the feedback provided.

IMPORTANT: Use the following EXACT key in both @register_scheduler_init and @register_scheduler decorators: "{policy_key}"
Do NOT generate your own key - you MUST use exactly: "{policy_key}"
""".strip()

def get_iteration_feedback_prompt(
    policy_key: str,
    scheduler_code: str,
    scale_results: dict,
    base_params: dict,
) -> str:
    """Build the user-turn feedback prompt for one iteration of LLM improvement.

    Combines the weighted-latency objective function with the performance table
    from the current scheduler run. Handles the all-failed case separately.
    """
    import math

    rows = []
    valid_latencies: list[float] = []
    errors: list[str] = []
    def _lat(v):
        return f"{v:.2f}s" if v is not None else "n/a"

    for scale in sorted(scale_results):
        r = scale_results[scale]
        cpus = base_params["cpus_per_pool"] * scale
        ram = base_params["ram_gb_per_pool"] * scale
        if r.get("ok"):
            lat = r["latency"]
            valid_latencies.append(lat)
            rows.append(f"  {scale:2d}x  ({cpus:5d} CPUs, {ram:6d} GB RAM)  adj={lat:.4f}s"
                        f"  failures={r.get('failures', '?')}  suspensions={r.get('suspensions', '?')}")
            for prio in ("query", "interactive", "batch"):
                ps = r.get(prio)
                if ps:
                    arr, comp = ps["arrived"], ps["completed"]
                    rows.append(f"       {prio:<13s} {comp:3d}/{arr:3d} completed"
                                f"  mean={_lat(ps['mean_s'])}  p99={_lat(ps['p99_s'])}")
        else:
            err = r.get("error", "unknown error")
            errors.append(err)
            rows.append(f"  {scale:2d}x  ({cpus:5d} CPUs, {ram:6d} GB RAM)  FAILED: {err}")

    perf_table = "\n".join(rows)
    n_valid = len(valid_latencies)
    n_total = len(scale_results)

    if valid_latencies:
        gm = math.exp(sum(math.log(max(v, 1e-12)) for v in valid_latencies) / len(valid_latencies))
        geomean_line = f"Geometric mean across {n_valid}/{n_total} successful runs: {gm:.4f}s"
        return f"""\
## Objective Function

For each pipeline, assign a latency value:
  - Completed pipeline: actual end-to-end latency in seconds
  - Incomplete or failed pipeline: 2 × max_job_seconds (e.g. 720s if max_job_seconds=360)

These per-pipeline latencies are then combined into a weighted average across all arrived pipelines:
  score = (sum of query_latency × 10 + sum of interactive_latency × 5 + sum of batch_latency × 1)
        / (query_arrivals × 10 + interactive_arrivals × 5 + batch_arrivals × 1)

Incomplete and failed pipelines directly inflate the score through the penalty term — they are not ignored.
High completion rate matters as much as low latency.

## Performance Results (adjusted latency across cluster sizes)

{perf_table}

{geomean_line}

## Current Scheduler Code

{scheduler_code}

Produce an improved version of this scheduler that reduces the geometric mean of adjusted latency across all cluster sizes. The best schedulers achieve high RAM utilization with zero or near-zero OOM failures and complete all pipeline classes.

Use the EXACT key "{policy_key}" in both @register_scheduler_init and @register_scheduler decorators.
Output ONLY the complete Python code."""

    # All runs failed — lead with errors and structural requirements
    unique_errors = list(dict.fromkeys(errors))
    error_list = "\n".join(f"  {e}" for e in unique_errors)
    return f"""\
!! ALL {n_total} RUNS FAILED — STRUCTURAL ERRORS MUST BE FIXED BEFORE OPTIMIZING !!

## Errors (deduplicated)

{error_list}

## Failed Run Details

{perf_table}

These are not performance problems — the scheduler could not execute at all. Fix the structural issues first.

## Scheduler Interface Reference

Required structure — both decorators must be present with the same key:

  @register_scheduler_init(key="{policy_key}")
  def init(s):
      s.waiting_queue = []  # attach any state to s here

  @register_scheduler(key="{policy_key}")
  def scheduler(s, results, pipelines):
      # results: List[ExecutionResult]  pipelines: List[Pipeline]
      return suspensions, assignments   # List[Suspend], List[Assignment]

Available globals (no imports needed): List, Tuple, Pipeline, OperatorState,
  ASSIGNABLE_STATES, Assignment, ExecutionResult, Suspend,
  register_scheduler_init, register_scheduler, Priority

Critical API facts:
  - Assignment requires a pipeline_id= keyword argument
  - s.executor.pools[i].avail_cpu_pool / avail_ram_pool do NOT update within
    a single scheduler call — track allocated resources manually
  - Priority levels: Priority.QUERY > Priority.INTERACTIVE > Priority.BATCH_PIPELINE

## Current (Broken) Scheduler

{scheduler_code}

Rewrite this scheduler so it runs without errors. Use the EXACT key "{policy_key}" in both @register_scheduler_init and @register_scheduler decorators. Output ONLY the complete Python code."""