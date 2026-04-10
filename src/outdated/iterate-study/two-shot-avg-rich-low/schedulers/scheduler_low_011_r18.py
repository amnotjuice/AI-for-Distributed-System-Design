# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r18
@register_scheduler_init(key="scheduler_low_011_r18")
def scheduler_low_011_r18_init(s):
    """Priority-aware, work-conserving scheduler with better utilization and reduced queueing latency.

    Small, safe improvements over the prior attempt:
    - Work-conserving packing: fill each pool with multiple assignments per tick (instead of 1), up to a cap.
    - Weighted fairness across priorities (deficit round-robin) to prevent starvation while still favoring queries.
    - Head-of-line blocking avoidance: scan within each priority queue to find a ready operator.
    - OOM-aware RAM backoff retries keyed by operator identity (no preemption; keep it robust).

    Goals:
    - Reduce latency by improving throughput/utilization and avoiding idle resources.
    - Keep strong preference for high priority while ensuring interactive/batch make progress.
    """
    from collections import deque

    s.tick = 0

    # Per-priority FIFO queues of pipelines
    s.q = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Deficit Round Robin (DRR) state
    s.priorities = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    s.rr_idx = 0
    s.deficit = {p: 0.0 for p in s.priorities}

    # Larger quantum => more share. Queries favored, interactive gets meaningful share, batch gets some progress.
    s.quantum = {
        Priority.QUERY: 6.0,
        Priority.INTERACTIVE: 3.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.max_deficit_factor = 4.0  # cap deficit to avoid unbounded accumulation

    # Conservative default sizing fractions per operator (of pool max). RAM is a starting guess; OOM triggers backoff.
    s.base_cpu_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.base_ram_frac = {
        Priority.QUERY: 0.35,
        Priority.INTERACTIVE: 0.35,
        Priority.BATCH_PIPELINE: 0.35,
    }

    # Caps per assignment to keep packing effective (avoid one container consuming the whole pool)
    s.max_cpu_frac_per_op = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Retry policy for OOM
    s.max_oom_retries_per_op = 3
    s.oom_backoff = 2.0

    # Learned per-operator hints (min resources to avoid OOM / pathological underprovisioning)
    # key: (pipeline_id, op_id) -> {"ram": float, "cpu": float}
    s.op_hints = {}
    s.op_attempts = {}

    # Map op_id -> pipeline_id for correlating ExecutionResults (since results may not always carry pipeline_id)
    s.op_to_pipeline = {}

    # Pipelines that had a non-OOM failure; we stop retrying/scheduling them
    s.perma_failed_pipelines = set()

    # Pool preference: if multiple pools exist, try to pack pool 0 first (often "interactive-ish")
    s.preferred_pool_id = 0

    # Scheduling bounds (avoid long loops)
    s.max_assignments_per_pool_per_tick = 8
    s.scan_limit_per_pick = 32


def _sl011r18_is_oom(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("killed process" in msg)


def _sl011r18_norm_prio(s, pr):
    return pr if pr in s.q else Priority.BATCH_PIPELINE


def _sl011r18_pool_order(s):
    n = s.executor.num_pools
    if n <= 1:
        return list(range(n))
    first = s.preferred_pool_id if 0 <= s.preferred_pool_id < n else 0
    return [first] + [i for i in range(n) if i != first]


def _sl011r18_op_id(op):
    # Stable enough within the simulation process
    return id(op)


def _sl011r18_key(pipeline_id, op):
    return (pipeline_id, _sl011r18_op_id(op))


def _sl011r18_pipeline_done_or_dropped(s, p):
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return True
    if p.pipeline_id in s.perma_failed_pipelines:
        return True
    return False


def _sl011r18_next_ready_op(pipeline):
    st = pipeline.runtime_status()
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _sl011r18_pick_from_queue(s, pr):
    """Pick a pipeline with a ready op from priority queue pr, scanning to avoid HOL blocking.

    Returns: (pipeline, op) or (None, None)
    """
    q = s.q[pr]
    if not q:
        return None, None

    scans = min(len(q), s.scan_limit_per_pick)
    for _ in range(scans):
        p = q.popleft()

        if _sl011r18_pipeline_done_or_dropped(s, p):
            continue

        op = _sl011r18_next_ready_op(p)
        if op is None:
            # Not ready yet; keep FIFO-ish behavior by appending back
            q.append(p)
            continue

        return p, op

    return None, None


def _sl011r18_bump_deficits(s):
    # Add quantum each tick, but cap deficits to avoid unbounded growth.
    for pr in s.priorities:
        s.deficit[pr] += float(s.quantum.get(pr, 1.0))
        cap = float(s.quantum.get(pr, 1.0)) * float(s.max_deficit_factor)
        if s.deficit[pr] > cap:
            s.deficit[pr] = cap


def _sl011r18_choose_priority_drr(s, require_deficit=True):
    """Return a priority to schedule next.
    If require_deficit=True, only consider priorities with deficit >= 1.
    Otherwise, pick highest-importance available (QUERY->INTERACTIVE->BATCH).
    """
    if not require_deficit:
        for pr in s.priorities:
            if s.q[pr]:
                return pr
        return None

    # Round-robin starting point to avoid fixed bias
    n = len(s.priorities)
    for k in range(n):
        pr = s.priorities[(s.rr_idx + k) % n]
        if s.deficit.get(pr, 0.0) >= 1.0 and s.q[pr]:
            s.rr_idx = (s.rr_idx + k + 1) % n
            return pr
    return None


def _sl011r18_request_resources(s, pool, pr, pipeline_id, op, avail_cpu, avail_ram):
    """Compute cpu/ram request for this op in this pool, respecting hints and local availability.

    Returns: (cpu, ram) or (None, None) if it cannot fit now.
    """
    # Base sizing
    base_cpu = max(1.0, float(pool.max_cpu_pool) * float(s.base_cpu_frac.get(pr, 0.25)))
    base_ram = max(1.0, float(pool.max_ram_pool) * float(s.base_ram_frac.get(pr, 0.35)))

    # Cap CPU per op to keep packing effective
    cpu_cap = max(1.0, float(pool.max_cpu_pool) * float(s.max_cpu_frac_per_op.get(pr, 0.50)))

    # Apply learned hints (treated as minima)
    k = (pipeline_id, _sl011r18_op_id(op))
    hint = s.op_hints.get(k, {})
    hint_cpu = float(hint.get("cpu", 0.0) or 0.0)
    hint_ram = float(hint.get("ram", 0.0) or 0.0)

    need_ram = max(base_ram, hint_ram, 1.0)
    if need_ram > avail_ram:
        return None, None

    # CPU: we can shrink to fit available; still keep >= 1 and >= hint if possible
    need_cpu = max(base_cpu, hint_cpu, 1.0)
    need_cpu = min(need_cpu, cpu_cap)

    if avail_cpu < 1.0:
        return None, None

    cpu = min(need_cpu, avail_cpu)
    if cpu < min(1.0, hint_cpu if hint_cpu > 0 else 1.0):
        # Can't satisfy hint minimum CPU (rare); skip for now
        return None, None

    ram = need_ram  # RAM doesn't speed up; request the minimum we believe is safe
    return cpu, ram


@register_scheduler(key="scheduler_low_011_r18")
def scheduler_low_011_r18(s, results, pipelines):
    # Enqueue new pipelines
    for p in pipelines:
        pr = _sl011r18_norm_prio(s, getattr(p, "priority", Priority.BATCH_PIPELINE))
        s.q[pr].append(p)

    # Process results to learn OOM requirements and mark permanent failures
    for r in results:
        # Best-effort extract pipeline_id via result or via op->pipeline mapping
        pipeline_id = getattr(r, "pipeline_id", None)

        ops = getattr(r, "ops", None) or []
        if pipeline_id is None and ops:
            pid = s.op_to_pipeline.get(_sl011r18_op_id(ops[0]))
            if pid is not None:
                pipeline_id = pid

        # If we still don't know pipeline_id, we can only do minimal handling
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        err = getattr(r, "error", None)
        is_oom = _sl011r18_is_oom(err)

        if pipeline_id is None:
            # Can't safely attribute; nothing else to do
            continue

        if not ops:
            # No op identity; mark pipeline as failed if non-OOM, otherwise do nothing
            if not is_oom:
                s.perma_failed_pipelines.add(pipeline_id)
            continue

        # Update per-op hints / attempts
        pool_id = getattr(r, "pool_id", None)
        max_ram = None
        if pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
            max_ram = float(s.executor.pools[int(pool_id)].max_ram_pool)

        for op in ops:
            k = _sl011r18_key(pipeline_id, op)
            s.op_attempts[k] = int(s.op_attempts.get(k, 0)) + 1

            if is_oom:
                # Exponential RAM backoff; capped by pool max RAM if known
                prev = s.op_hints.get(k, {})
                prev_ram = float(prev.get("ram", 0.0) or 0.0)
                observed_ram = float(getattr(r, "ram", 0.0) or 0.0)
                baseline = max(prev_ram, observed_ram, 1.0)
                new_ram = baseline * float(s.oom_backoff)

                if max_ram is not None:
                    new_ram = min(new_ram, max_ram)

                # Preserve any existing CPU hint (rarely used); keep >=1
                prev_cpu = float(prev.get("cpu", getattr(r, "cpu", 1.0) or 1.0) or 1.0)
                s.op_hints[k] = {"ram": max(1.0, new_ram), "cpu": max(1.0, prev_cpu)}
            else:
                # Non-OOM failure: stop scheduling this pipeline
                s.perma_failed_pipelines.add(pipeline_id)

            # If too many OOM retries, also stop (prevents infinite loops)
            if is_oom and s.op_attempts[k] > int(s.max_oom_retries_per_op):
                s.perma_failed_pipelines.add(pipeline_id)

    # Fast path: no changes => no actions
    if not pipelines and not results:
        return [], []

    s.tick += 1
    _sl011r18_bump_deficits(s)

    suspensions = []
    assignments = []

    # Iterate pools in a preferred order (pack preferred pool first)
    for pool_id in _sl011r18_pool_order(s):
        pool = s.executor.pools[pool_id]

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < 1.0 or avail_ram < 1.0:
            continue

        made = 0
        while made < int(s.max_assignments_per_pool_per_tick):
            if avail_cpu < 1.0 or avail_ram < 1.0:
                break

            # First try DRR-eligible priorities; if none, borrow to stay work-conserving
            pr = _sl011r18_choose_priority_drr(s, require_deficit=True)
            if pr is None:
                pr = _sl011r18_choose_priority_drr(s, require_deficit=False)
                if pr is None:
                    break

            p, op = _sl011r18_pick_from_queue(s, pr)
            if p is None:
                # This priority had no schedulable ready ops; try again (maybe other priorities)
                # Move rr_idx forward to avoid re-checking the same empty queue repeatedly.
                s.rr_idx = (s.rr_idx + 1) % len(s.priorities)
                # If we were deficit-gated, drop into a borrow attempt next iteration.
                continue

            # Record op->pipeline mapping for result correlation
            s.op_to_pipeline[_sl011r18_op_id(op)] = p.pipeline_id

            cpu, ram = _sl011r18_request_resources(
                s, pool, pr, p.pipeline_id, op, avail_cpu=avail_cpu, avail_ram=avail_ram
            )

            if cpu is None or ram is None:
                # Can't fit this op in this pool right now; requeue pipeline and try other work.
                s.q[pr].appendleft(p)
                # Try another priority/op without spinning forever on one too-large op.
                # (If everything is too large, loop will eventually stop due to no progress.)
                s.rr_idx = (s.rr_idx + 1) % len(s.priorities)
                # Guard: if nothing fits CPU-wise, break early
                if avail_cpu < 1.0 or avail_ram < 1.0:
                    break
                # Continue attempting to find other smaller work
                continue

            # Create assignment for single ready operator
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pr,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Consume deficit only if this priority was deficit-eligible; if we borrowed, still charge it.
            s.deficit[pr] = float(s.deficit.get(pr, 0.0)) - 1.0

            # Update local availability to allow packing multiple assignments in same tick
            avail_cpu -= float(cpu)
            avail_ram -= float(ram)

            made += 1

            # Re-enqueue pipeline so future operators can be scheduled later (maintains fairness)
            s.q[pr].append(p)

        # End while per pool

    return suspensions, assignments