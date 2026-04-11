# policy_key: scheduler_low_011
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.054487
# generation_seconds: 42.64
# generated_at: 2026-03-12T21:42:43.184661
@register_scheduler_init(key="scheduler_low_011")
def scheduler_low_011_init(s):
    """Priority-aware FIFO with simple, low-risk improvements over naive FIFO.

    Improvements (kept intentionally small and robust):
    - Maintain separate FIFO queues per priority and always schedule higher priority first.
    - Prefer placing high-priority work into a dedicated "interactive" pool when multiple pools exist.
    - Use conservative default CPU/RAM sizing per priority to reduce interference and improve tail latency.
    - On OOM-like failures, retry the same operator with increased RAM (exponential backoff), capped by pool max.
    """
    # Per-priority waiting queues (pipelines can appear multiple times; we de-dup at pop time by checking status)
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Resource hints learned from failures and prior attempts:
    # key: (pipeline_id, op_identity) -> {"ram": float, "cpu": float}
    s.op_hints = {}

    # How many assignment attempts we've made per operator (to bound retries)
    s.op_attempts = {}

    # Configuration knobs (small, safe defaults)
    s.max_retries_per_op = 3

    # Conservative per-priority sizing fractions (of pool MAX) to improve latency isolation
    s.size_fracs = {
        Priority.QUERY: {"cpu": 0.5, "ram": 0.5},
        Priority.INTERACTIVE: {"cpu": 0.5, "ram": 0.5},
        Priority.BATCH_PIPELINE: {"cpu": 1.0, "ram": 1.0},
    }

    # If multiple pools exist, prefer pool 0 for interactive/query by default
    s.interactive_pool_id = 0


def _priority_order():
    # Highest to lowest
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    # Heuristics: different runtimes spell this differently
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_key(pipeline_id, op):
    # Use object identity for robustness across unknown operator schemas.
    # (Eudoxia typically keeps objects stable within the simulation process.)
    return (pipeline_id, id(op))


def _get_next_assignable_op(pipeline):
    status = pipeline.runtime_status()
    # Only consider operators whose parents are complete
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _pick_pool_order(s, priority):
    # If multiple pools exist, try the interactive pool first for high-priority work.
    if s.executor.num_pools <= 1:
        return list(range(s.executor.num_pools))

    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        # Try interactive pool first, then others
        order = [s.interactive_pool_id] + [i for i in range(s.executor.num_pools) if i != s.interactive_pool_id]
        return order

    # Batch: try non-interactive pools first, keep pool 0 available when possible
    order = [i for i in range(s.executor.num_pools) if i != s.interactive_pool_id] + [s.interactive_pool_id]
    return order


def _default_request_for_pool(s, pool, priority):
    # Bound by available resources and at least small positive allocations
    fr = s.size_fracs.get(priority, {"cpu": 1.0, "ram": 1.0})
    cpu = pool.max_cpu_pool * fr["cpu"]
    ram = pool.max_ram_pool * fr["ram"]

    # Avoid extreme tiny/zero allocations; keep these conservative.
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)

    # Also cap to currently available in the pool
    cpu = min(cpu, pool.avail_cpu_pool)
    ram = min(ram, pool.avail_ram_pool)
    return cpu, ram


def _apply_hints_and_caps(s, pool, pipeline_id, op, priority):
    # Start with default request
    cpu, ram = _default_request_for_pool(s, pool, priority)

    # Apply learned hints (e.g., from prior OOM retries)
    k = _op_key(pipeline_id, op)
    hint = s.op_hints.get(k)
    if hint:
        # Use the larger of default and hint to avoid regressing
        cpu = max(cpu, float(hint.get("cpu", cpu)))
        ram = max(ram, float(hint.get("ram", ram)))

    # Cap by pool availability and max
    cpu = min(cpu, pool.avail_cpu_pool, pool.max_cpu_pool)
    ram = min(ram, pool.avail_ram_pool, pool.max_ram_pool)

    # Ensure still positive
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


@register_scheduler(key="scheduler_low_011")
def scheduler_low_011(s, results, pipelines):
    """
    Priority-first FIFO scheduler with conservative sizing and OOM-aware retry.

    Behavior:
    - Enqueue new pipelines into per-priority FIFO queues.
    - Process execution results to update resource hints:
        * If an operator fails with OOM-like error, increase RAM hint (double) and retry (up to N times).
        * Otherwise, do not retry failed operators (pipeline will be dropped when detected failed).
    - For each pool, assign at most one ready operator per tick, choosing from highest priority queue first.
    """
    # Enqueue new arrivals
    for p in pipelines:
        # Default unknown priorities into batch queue (defensive)
        pr = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
        s.waiting_queues[pr].append(p)

    # Update hints based on results (especially OOM)
    for r in results:
        try:
            if not r.failed():
                continue
        except Exception:
            # If failed() isn't available for some reason, fall back on error presence
            if getattr(r, "error", None) is None:
                continue

        # If it's an OOM, increase RAM hint and allow retry; otherwise do nothing special
        if _is_oom_error(getattr(r, "error", None)):
            # We expect r.ops to be a list; update hints for each (typically one)
            ops = getattr(r, "ops", []) or []
            for op in ops:
                # Try to find pipeline_id from result; if missing, we cannot key properly.
                pipeline_id = getattr(r, "pipeline_id", None)
                if pipeline_id is None:
                    # Best-effort: cannot learn a stable key without pipeline id
                    continue

                k = _op_key(pipeline_id, op)
                prev_hint = s.op_hints.get(k, {})
                prev_ram = float(prev_hint.get("ram", getattr(r, "ram", 0.0) or 0.0))
                prev_cpu = float(prev_hint.get("cpu", getattr(r, "cpu", 1.0) or 1.0))

                # If we have an observed allocation, use it as the baseline; otherwise start from a small default
                baseline_ram = prev_ram if prev_ram > 0 else float(getattr(r, "ram", 1.0) or 1.0)
                new_ram = max(1.0, baseline_ram * 2.0)

                s.op_hints[k] = {"ram": new_ram, "cpu": max(1.0, prev_cpu)}
                s.op_attempts[k] = int(s.op_attempts.get(k, 0)) + 1

                # If we've exceeded retry budget, stop inflating further (pipeline will eventually be treated as failed)
                if s.op_attempts[k] > s.max_retries_per_op:
                    # Clamp to something reasonable; no more retries are intended
                    s.op_hints[k]["ram"] = new_ram

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: pop next schedulable pipeline in priority order (FIFO), skipping completed/failed
    def pop_next_pipeline(pr):
        q = s.waiting_queues[pr]
        while q:
            p = q.pop(0)
            status = p.runtime_status()
            # Drop completed pipelines
            if status.is_pipeline_successful():
                continue
            # Drop pipelines with failures (non-OOM errors should not be retried by this low-risk policy)
            if status.state_counts.get(OperatorState.FAILED, 0) > 0:
                continue
            return p
        return None

    # For each pool, schedule one operator (small improvement over naive: don't let one pipeline dominate all pools)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Try to find some work that fits this pool, priority-first
        chosen = None
        chosen_pr = None
        chosen_op = None
        chosen_cpu = None
        chosen_ram = None

        # We will look for a pipeline to schedule; if not scheduled, put it back to preserve fairness.
        deferred = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}

        for pr in _priority_order():
            # If pool preference suggests not to place this priority here, we still allow it, but later.
            # We handle pool preference by iterating pools in a preferred order per pipeline below.
            # Here, we just pick work; placement check happens when computing request.
            p = pop_next_pipeline(pr)
            while p is not None:
                op = _get_next_assignable_op(p)
                if op is None:
                    # Not ready yet; keep it around for later ticks
                    deferred[pr].append(p)
                    p = pop_next_pipeline(pr)
                    continue

                # Enforce pool preference: for each pipeline, try preferred pools only.
                preferred_pools = _pick_pool_order(s, pr)
                if preferred_pools and preferred_pools[0] != pool_id and pool_id not in preferred_pools:
                    # Defensive, should not happen
                    deferred[pr].append(p)
                    p = pop_next_pipeline(pr)
                    continue

                # If this pool is not among the earlier preferred pools, we can still place it,
                # but try to keep interactive/query on the interactive pool when possible.
                if preferred_pools and pool_id != preferred_pools[0] and pr in (Priority.QUERY, Priority.INTERACTIVE):
                    # Defer this pipeline so interactive pool gets first shot
                    deferred[pr].append(p)
                    p = pop_next_pipeline(pr)
                    continue

                cpu, ram = _apply_hints_and_caps(s, pool, p.pipeline_id, op, pr)

                # If we cannot allocate even minimally, defer
                if cpu > pool.avail_cpu_pool or ram > pool.avail_ram_pool:
                    deferred[pr].append(p)
                    p = pop_next_pipeline(pr)
                    continue

                chosen = p
                chosen_pr = pr
                chosen_op = op
                chosen_cpu = cpu
                chosen_ram = ram
                break

            if chosen is not None:
                break

        # Put deferred pipelines back at the front (preserve FIFO-ish order without starvation)
        for pr in _priority_order():
            if deferred[pr]:
                s.waiting_queues[pr] = deferred[pr] + s.waiting_queues[pr]

        if chosen is None:
            continue

        # Make the assignment (single op)
        assignments.append(
            Assignment(
                ops=[chosen_op],
                cpu=chosen_cpu,
                ram=chosen_ram,
                priority=chosen_pr,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
        )

        # Re-enqueue pipeline so subsequent operators can be scheduled later
        s.waiting_queues[chosen_pr].append(chosen)

    return suspensions, assignments
