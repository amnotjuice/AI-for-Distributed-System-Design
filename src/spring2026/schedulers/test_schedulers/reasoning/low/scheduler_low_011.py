# policy_key: scheduler_low_011
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052413
# generation_seconds: 47.04
# generated_at: 2026-04-09T21:10:28.407133
@register_scheduler_init(key="scheduler_low_011")
def scheduler_low_011_init(s):
    """Priority-aware, OOM-resilient scheduler to minimize weighted latency + failure penalties.

    Main ideas:
    - Strict priority ordering for ready work: QUERY > INTERACTIVE > BATCH (protects the score).
    - Conservative "fair-share" sizing: avoid giving one op the whole pool; leave headroom.
    - OOM-aware retries: if an op fails with an OOM-like error, retry it with increased RAM (exponential backoff).
    - Bounded non-OOM retries: avoid infinite retry loops while still trying to complete.
    - Gentle anti-starvation: allow a small amount of batch progress even when higher priorities exist.
    """
    # Monotonic tick counter (simulation time proxy for aging and ordering).
    s._tick = 0

    # Queues by priority (pipelines, not ops).
    s._q_query = []
    s._q_interactive = []
    s._q_batch = []

    # Pipeline arrival ordering / "age" tracking.
    s._enqueue_tick = {}  # pipeline_id -> first seen tick
    s._last_scheduled_tick = {}  # pipeline_id -> last tick when we assigned an op

    # Failure / retry tracking.
    # Keyed by (pipeline_id, op_key) to adapt per-operator sizing when OOM occurs.
    s._oom_ram_multiplier = {}  # (pipeline_id, op_key) -> float (>=1.0)
    s._retry_count = {}  # (pipeline_id, op_key) -> int
    s._pipeline_giveup = set()  # pipeline_id set: stop scheduling if too many non-OOM failures

    # Global knobs (kept simple; can be tuned in subsequent iterations).
    s._max_retries_per_op = 4
    s._max_non_oom_retries_per_op = 2

    # Headroom reservations per pool to avoid thrashing and keep capacity for arrivals.
    # (Used as "do not consume beyond this fraction if there is higher-priority demand".)
    s._reserve_frac_for_query = 0.20
    s._reserve_frac_for_interactive = 0.10

    # Per-priority caps to avoid one assignment consuming the entire pool.
    s._cpu_cap_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.65,
        Priority.BATCH_PIPELINE: 0.50,
    }
    s._ram_cap_frac = {
        Priority.QUERY: 0.80,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.60,
    }

    # Batch progress budget: in each tick/pool, allow at least this fraction of the pool
    # to be used for batch if batch is ready, even if higher-priority queues are non-empty.
    s._batch_min_progress_frac = 0.15


def _pqueue_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s._q_query
    if prio == Priority.INTERACTIVE:
        return s._q_interactive
    return s._q_batch


def _op_key(op):
    # Stable-ish key for per-op adaptation without relying on specific fields.
    # repr(op) tends to include operator identity deterministically in the simulator.
    return repr(op)


def _is_oom_error(err):
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("killed" in msg)


def _effective_priority_rank(prio):
    # Lower is better.
    if prio == Priority.QUERY:
        return 0
    if prio == Priority.INTERACTIVE:
        return 1
    return 2


def _pick_next_pipeline(s, allow_batch, require_ready_op=True):
    """Pick the next pipeline to schedule, respecting priority and mild anti-starvation."""
    # Queries first, then interactive, then batch (optionally).
    candidates = []
    if s._q_query:
        candidates.append(s._q_query)
    if s._q_interactive:
        candidates.append(s._q_interactive)
    if allow_batch and s._q_batch:
        candidates.append(s._q_batch)

    for q in candidates:
        # Pop from head, but skip completed/given-up/non-ready pipelines while keeping queue order.
        # We rotate a bounded number of times to avoid O(n^2) behavior.
        rotations = 0
        max_rot = len(q)
        while q and rotations < max_rot:
            p = q.pop(0)
            rotations += 1

            if p.pipeline_id in s._pipeline_giveup:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            if not require_ready_op:
                return p

            # Require at least one assignable op whose parents are complete.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if op_list:
                return p

            # Not ready yet; requeue at end.
            q.append(p)

    return None


def _estimate_resources(s, pool, pipeline, op):
    """Compute (cpu, ram) request with OOM-aware RAM backoff and per-priority caps."""
    prio = pipeline.priority

    # Base allocations: give a meaningful slice but do not monopolize the pool.
    cpu_cap = max(1.0, pool.max_cpu_pool * s._cpu_cap_frac.get(prio, 0.5))
    ram_cap = max(1.0, pool.max_ram_pool * s._ram_cap_frac.get(prio, 0.6))

    # Start from available resources but cap them.
    cpu = min(pool.avail_cpu_pool, cpu_cap)
    ram = min(pool.avail_ram_pool, ram_cap)

    # Ensure we always request at least a small amount to make progress.
    cpu = max(1.0, cpu)

    # OOM backoff per (pipeline, op).
    ok = _op_key(op)
    mult = s._oom_ram_multiplier.get((pipeline.pipeline_id, ok), 1.0)

    # Apply multiplier, but never exceed what the pool can provide right now.
    ram = min(pool.avail_ram_pool, min(ram_cap, max(1.0, ram * mult)))

    return cpu, ram


def _update_from_results(s, results):
    """Learn from execution results: adjust OOM multipliers and bound retries."""
    for r in results:
        # Some simulators may return results without ops; handle defensively.
        ops = getattr(r, "ops", None) or []
        if not ops:
            continue

        for op in ops:
            ok = _op_key(op)
            key = (getattr(r, "pipeline_id", None), ok)

            # If pipeline_id is not present on ExecutionResult, fall back to op_key-only adaptation.
            # (We keep it per-"None pipeline" bucket to avoid crashes.)
            if key[0] is None:
                key = ("__unknown_pipeline__", ok)

            if r.failed():
                err = getattr(r, "error", None)

                # Track retries.
                s._retry_count[key] = s._retry_count.get(key, 0) + 1

                if _is_oom_error(err):
                    # Exponential RAM backoff on OOM.
                    cur = s._oom_ram_multiplier.get(key, 1.0)
                    # Aggressive enough to quickly avoid repeated 720s, but capped to prevent runaway.
                    nxt = min(8.0, max(cur * 1.8, cur + 0.5))
                    s._oom_ram_multiplier[key] = nxt
                else:
                    # Non-OOM failure: allow a small number of retries, then give up to avoid infinite loops.
                    # Giving up will likely incur 720, but infinite incomplete also incurs 720.
                    non_oom_retries = 0
                    # Count non-OOM retries by subtracting implicit OOMs is hard without storing; approximate:
                    # if we did not classify as OOM, treat this as a non-OOM retry.
                    non_oom_retries = s._retry_count.get(("__non_oom__", key[0], ok), 0) + 1
                    s._retry_count[("__non_oom__", key[0], ok)] = non_oom_retries


@register_scheduler(key="scheduler_low_011")
def scheduler_low_011(s, results, pipelines):
    """
    Scheduler step:
    - Ingest new pipelines into priority queues with stable FIFO per class.
    - Update sizing heuristics from recent results (OOM backoff, retry tracking).
    - For each pool, issue at most one Assignment at a time (simple baseline improvement).
    - Respect priority ordering and keep small headroom for queries/interactive.
    - Ensure batch makes some progress (small reserved fraction) to reduce 720 penalties from starvation.
    """
    s._tick += 1

    # Learn from results (OOM backoff + retry tracking).
    if results:
        _update_from_results(s, results)

    # Enqueue new pipelines.
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s._enqueue_tick:
            s._enqueue_tick[pid] = s._tick
        _pqueue_for_priority(s, p.priority).append(p)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: whether higher-priority demand exists.
    def has_query_demand():
        return len(s._q_query) > 0

    def has_interactive_demand():
        return len(s._q_interactive) > 0

    # For each pool, schedule one ready op with priority and headroom logic.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Skip if no resources.
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Determine if we should allow batch this cycle for this pool:
        # - If no query/interactive queued, allow.
        # - Otherwise, only allow if a small slice is available beyond reserved headroom.
        allow_batch = True
        if has_query_demand() or has_interactive_demand():
            # Compute a "batch slice" as the minimum progress budget.
            min_cpu_for_batch = pool.max_cpu_pool * s._batch_min_progress_frac
            min_ram_for_batch = pool.max_ram_pool * s._batch_min_progress_frac
            allow_batch = (pool.avail_cpu_pool >= min_cpu_for_batch) and (pool.avail_ram_pool >= min_ram_for_batch)

        # Additional headroom reservation: if query exists, avoid consuming last reserved fraction with lower priorities.
        # This is a soft rule applied when selecting from queues.
        # We'll attempt in order; if insufficient headroom, skip to higher priority.
        picked = None

        # First try query, then interactive, then possibly batch.
        # (We also avoid scheduling pipelines we've "given up" on.)
        picked = _pick_next_pipeline(s, allow_batch=allow_batch, require_ready_op=True)
        if picked is None:
            continue

        status = picked.runtime_status()
        # If a pipeline has too many non-OOM retries for any ready op, mark give-up.
        # (This bounds pathological retries.)
        ready_ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not ready_ops:
            # Not ready after all; requeue.
            _pqueue_for_priority(s, picked.priority).append(picked)
            continue

        op = ready_ops[0]
        ok = _op_key(op)
        pid = picked.pipeline_id

        # Determine retry keys (pipeline-aware if possible).
        retry_key = (pid, ok)
        non_oom_retry_key = ("__non_oom__", pid, ok)

        # If non-OOM failures exceeded, give up scheduling this pipeline (prevents infinite loops).
        if s._retry_count.get(non_oom_retry_key, 0) > s._max_non_oom_retries_per_op:
            s._pipeline_giveup.add(pid)
            continue

        # If total retries exceeded, also give up.
        if s._retry_count.get(retry_key, 0) > s._max_retries_per_op:
            s._pipeline_giveup.add(pid)
            continue

        # Enforce headroom: do not let BATCH consume reserved capacity needed for queries/interactive.
        # (We do this by reducing caps via available resource checks.)
        if picked.priority == Priority.BATCH_PIPELINE:
            # If queries exist, reserve a fraction of pool resources.
            if has_query_demand():
                reserve_cpu = pool.max_cpu_pool * s._reserve_frac_for_query
                reserve_ram = pool.max_ram_pool * s._reserve_frac_for_query
                if pool.avail_cpu_pool <= reserve_cpu or pool.avail_ram_pool <= reserve_ram:
                    # Requeue batch and try higher priority only.
                    s._q_batch.append(picked)
                    # Try query / interactive only (no batch).
                    picked2 = _pick_next_pipeline(s, allow_batch=False, require_ready_op=True)
                    if picked2 is None:
                        continue
                    picked = picked2
                    status = picked.runtime_status()
                    ready_ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not ready_ops:
                        _pqueue_for_priority(s, picked.priority).append(picked)
                        continue
                    op = ready_ops[0]
                    ok = _op_key(op)
                    pid = picked.pipeline_id

            elif has_interactive_demand():
                reserve_cpu = pool.max_cpu_pool * s._reserve_frac_for_interactive
                reserve_ram = pool.max_ram_pool * s._reserve_frac_for_interactive
                if pool.avail_cpu_pool <= reserve_cpu or pool.avail_ram_pool <= reserve_ram:
                    s._q_batch.append(picked)
                    picked2 = _pick_next_pipeline(s, allow_batch=False, require_ready_op=True)
                    if picked2 is None:
                        continue
                    picked = picked2
                    status = picked.runtime_status()
                    ready_ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not ready_ops:
                        _pqueue_for_priority(s, picked.priority).append(picked)
                        continue
                    op = ready_ops[0]
                    ok = _op_key(op)
                    pid = picked.pipeline_id

        # Allocate resources for the chosen op.
        cpu, ram = _estimate_resources(s, pool, picked, op)

        # If we somehow can't allocate meaningful RAM, requeue and skip.
        if ram <= 0 or cpu <= 0:
            _pqueue_for_priority(s, picked.priority).append(picked)
            continue

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=picked.priority,
                pool_id=pool_id,
                pipeline_id=picked.pipeline_id,
            )
        )

        s._last_scheduled_tick[picked.pipeline_id] = s._tick

        # Requeue the pipeline to allow subsequent ops to be scheduled in later ticks.
        _pqueue_for_priority(s, picked.priority).append(picked)

    return suspensions, assignments
