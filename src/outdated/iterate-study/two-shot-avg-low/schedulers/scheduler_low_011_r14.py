# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r14
@register_scheduler_init(key="scheduler_low_011_r14")
def scheduler_low_011_r14_init(s):
    """Iteration 2: Priority-first + head-of-line blocking avoidance + OOM-aware retry that actually retries.

    Key improvements over the previous attempt:
    1) Fix obvious bug: do NOT drop pipelines just because they have FAILED ops; allow OOM-failed ops to be retried.
    2) Avoid head-of-line blocking within each priority queue by scanning a small window for a ready-to-run op.
    3) Schedule interactive pools first; keep a small reserve on the interactive pool so batch doesn't consume it all.
    4) Allow multiple assignments per pool per tick (bounded), using small per-op CPU caps for concurrency/latency.
    """
    # Per-priority FIFO pipeline queues
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Learned per-operator hints keyed by op object id (best effort; ExecutionResult doesn't expose pipeline_id)
    # op_id -> {"ram": float, "cpu": float}
    s.op_hints = {}

    # Track OOM retries per op_id
    s.op_oom_attempts = {}

    # Track which failed ops are considered OOM-retriable (op_id set)
    s.oom_retriable_ops = set()

    # Config knobs (kept simple)
    s.max_oom_retries_per_op = 3

    # Small scan window per queue to reduce HOL blocking without O(n) scanning
    s.queue_scan_limit = 24

    # Prefer pool 0 for interactive/query when multiple pools exist
    s.interactive_pool_id = 0

    # Interactive-pool reserve to protect tail latency when batch is present
    # Reserve is activated if any high-priority work is waiting.
    s.interactive_reserve_cpu_frac = 0.25
    s.interactive_reserve_ram_frac = 0.25
    s.interactive_reserve_cpu_min = 1.0
    s.interactive_reserve_ram_min = 1.0

    # Per-op CPU caps to allow some concurrency and reduce queueing latency
    s.hp_cpu_cap = 8.0
    s.batch_cpu_cap = 16.0

    # Default RAM fractions (RAM doesn't speed up; keep modest to allow concurrency, rely on OOM backoff)
    s.hp_ram_frac = 0.20
    s.batch_ram_frac = 0.30

    # Bound number of assignments per pool per tick (avoid overreacting / excessive churn)
    s.max_assignments_per_pool_per_tick = 4


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_id(op):
    # Best-effort stable identity within the simulator process
    return id(op)


def _get_ready_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _has_nonretriable_failures(s, pipeline):
    """Return True if pipeline has FAILED ops that are not OOM-retriable."""
    status = pipeline.runtime_status()
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    if not failed_ops:
        return False
    for op in failed_ops:
        if _op_id(op) not in s.oom_retriable_ops:
            return True
    return False


def _high_priority_waiting(s):
    return bool(s.waiting_queues[Priority.QUERY] or s.waiting_queues[Priority.INTERACTIVE])


def _pool_iteration_order(s):
    # Put interactive pool first so high priority gets first shot there.
    n = s.executor.num_pools
    if n <= 1:
        return list(range(n))
    ip = s.interactive_pool_id
    if ip < 0 or ip >= n:
        return list(range(n))
    return [ip] + [i for i in range(n) if i != ip]


def _request_for_op(s, pool, priority, op, rem_cpu, rem_ram):
    """Compute a conservative resource request that fits within remaining pool resources."""
    # CPU: cap per-op to allow concurrency; prioritize latency for high priority.
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        cpu_cap = min(s.hp_cpu_cap, pool.max_cpu_pool)
    else:
        cpu_cap = min(s.batch_cpu_cap, pool.max_cpu_pool)

    # Keep at least 1 vCPU
    cpu = max(1.0, min(cpu_cap, rem_cpu))

    # RAM: start modest; rely on OOM backoff / hints to converge.
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        base_ram = max(1.0, pool.max_ram_pool * s.hp_ram_frac)
    else:
        base_ram = max(1.0, pool.max_ram_pool * s.batch_ram_frac)

    oid = _op_id(op)
    hint = s.op_hints.get(oid, {})
    hint_ram = float(hint.get("ram", 0.0) or 0.0)
    hint_cpu = float(hint.get("cpu", 0.0) or 0.0)

    # Respect learned hints, but never exceed remaining
    ram = max(base_ram, hint_ram, 1.0)
    ram = min(ram, rem_ram, pool.max_ram_pool)

    # CPU hint is weak; use it only if it stays within cap
    if hint_cpu > 0:
        cpu = max(cpu, min(hint_cpu, cpu_cap, rem_cpu))
    cpu = min(cpu, rem_cpu, pool.max_cpu_pool)

    # If we cannot fit minimal RAM, signal infeasible by returning (None, None)
    if ram > rem_ram or rem_ram <= 0 or rem_cpu <= 0:
        return None, None
    return cpu, ram


def _dequeue_candidate_pipeline(s, pr, scan_limit):
    """Pop a pipeline candidate from the front of the queue, scanning up to scan_limit items.

    Returns:
        (pipeline or None, rotated_list)
    where rotated_list are pipelines we pulled but didn't choose; caller should reinsert them preserving order.
    """
    q = s.waiting_queues[pr]
    rotated = []
    chosen = None

    n = min(scan_limit, len(q))
    for _ in range(n):
        p = q.pop(0)
        status = p.runtime_status()

        # Drop completed pipelines
        if status.is_pipeline_successful():
            continue

        # Drop pipelines with non-retriable failures
        if _has_nonretriable_failures(s, p):
            continue

        # Choose the first pipeline that has a ready op
        op = _get_ready_op(p)
        if op is not None:
            chosen = p
            # Put back the rest later; keep chosen out for scheduling
            break

        # Not ready -> rotate
        rotated.append(p)

    return chosen, rotated


@register_scheduler(key="scheduler_low_011_r14")
def scheduler_low_011_r14(s, results, pipelines):
    """
    Priority-first scheduler optimized for latency with modest complexity.

    - Priority queues (QUERY > INTERACTIVE > BATCH)
    - Head-of-line blocking avoidance by scanning a small window for ready ops
    - Interactive pool reserve to reduce latency regression under batch load
    - Multi-assignment per pool per tick (bounded)
    - OOM-aware retry: on OOM failure, increase RAM hint (exponential) and allow retry up to a limit
    """
    # Enqueue new arrivals
    for p in pipelines:
        pr = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
        s.waiting_queues[pr].append(p)

    # Process results to learn hints and retry eligibility
    for r in results:
        ops = getattr(r, "ops", None) or []
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        if failed:
            if _is_oom_error(getattr(r, "error", None)):
                # Allow retry with more RAM
                pool_id = getattr(r, "pool_id", None)
                pool = None
                if pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
                    pool = s.executor.pools[int(pool_id)]

                for op in ops:
                    oid = _op_id(op)
                    attempts = int(s.op_oom_attempts.get(oid, 0)) + 1
                    s.op_oom_attempts[oid] = attempts

                    # If retry budget exceeded, mark as non-retriable (so pipeline gets dropped)
                    if attempts > s.max_oom_retries_per_op:
                        if oid in s.oom_retriable_ops:
                            s.oom_retriable_ops.discard(oid)
                        continue

                    # Increase RAM hint exponentially based on the last allocated RAM
                    last_ram = float(getattr(r, "ram", 0.0) or 0.0)
                    prev_hint = float(s.op_hints.get(oid, {}).get("ram", 0.0) or 0.0)
                    baseline = max(prev_hint, last_ram, 1.0)
                    new_hint = baseline * 2.0

                    # Cap by pool max RAM if known
                    if pool is not None:
                        new_hint = min(new_hint, float(pool.max_ram_pool))

                    s.op_hints[oid] = {
                        "ram": new_hint,
                        "cpu": float(getattr(r, "cpu", 0.0) or 0.0) or s.op_hints.get(oid, {}).get("cpu", 0.0),
                    }
                    s.oom_retriable_ops.add(oid)
            else:
                # Non-OOM failures are treated as non-retriable; do not keep FAILED ops schedulable.
                for op in ops:
                    oid = _op_id(op)
                    if oid in s.oom_retriable_ops:
                        s.oom_retriable_ops.discard(oid)
        else:
            # Success: record that this RAM amount worked (reduces future OOMs); clear OOM retry marker.
            pool_id = getattr(r, "pool_id", None)
            pool = None
            if pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
                pool = s.executor.pools[int(pool_id)]

            for op in ops:
                oid = _op_id(op)
                ok_ram = float(getattr(r, "ram", 0.0) or 0.0)
                ok_cpu = float(getattr(r, "cpu", 0.0) or 0.0)

                prev = s.op_hints.get(oid, {})
                prev_ram = float(prev.get("ram", 0.0) or 0.0)
                prev_cpu = float(prev.get("cpu", 0.0) or 0.0)

                # Keep the max of known-safe RAM; cap to pool max if known
                safe_ram = max(prev_ram, ok_ram, 0.0)
                if pool is not None:
                    safe_ram = min(safe_ram, float(pool.max_ram_pool))

                # CPU hint is optional; keep the observed CPU if it exists
                safe_cpu = max(prev_cpu, ok_cpu, 0.0)

                if safe_ram > 0 or safe_cpu > 0:
                    s.op_hints[oid] = {"ram": safe_ram, "cpu": safe_cpu}

                if oid in s.oom_retriable_ops:
                    s.oom_retriable_ops.discard(oid)

    # Early exit if no changes
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Schedule pools (interactive pool first)
    for pool_id in _pool_iteration_order(s):
        pool = s.executor.pools[pool_id]
        rem_cpu = float(pool.avail_cpu_pool)
        rem_ram = float(pool.avail_ram_pool)

        if rem_cpu <= 0 or rem_ram <= 0:
            continue

        # Reserve capacity on interactive pool if high priority is waiting
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if pool_id == s.interactive_pool_id and _high_priority_waiting(s):
            reserve_cpu = max(s.interactive_reserve_cpu_min, pool.max_cpu_pool * s.interactive_reserve_cpu_frac)
            reserve_ram = max(s.interactive_reserve_ram_min, pool.max_ram_pool * s.interactive_reserve_ram_frac)
            reserve_cpu = min(reserve_cpu, pool.max_cpu_pool)
            reserve_ram = min(reserve_ram, pool.max_ram_pool)

        scheduled_this_pool = 0
        scheduled_pipelines_this_tick = set()

        # Greedily fill the pool with a bounded number of assignments
        while (
            scheduled_this_pool < s.max_assignments_per_pool_per_tick
            and rem_cpu >= 1.0
            and rem_ram >= 1.0
        ):
            made_assignment = False

            for pr in _prio_order():
                # Enforce reserve only against batch on the interactive pool
                eff_rem_cpu = rem_cpu
                eff_rem_ram = rem_ram
                if pool_id == s.interactive_pool_id and pr == Priority.BATCH_PIPELINE and _high_priority_waiting(s):
                    eff_rem_cpu = max(0.0, rem_cpu - reserve_cpu)
                    eff_rem_ram = max(0.0, rem_ram - reserve_ram)

                if eff_rem_cpu < 1.0 or eff_rem_ram < 1.0:
                    continue

                # Find a candidate pipeline within scan window
                candidate, rotated = _dequeue_candidate_pipeline(s, pr, s.queue_scan_limit)
                # Put rotated pipelines back (preserving order)
                if rotated:
                    s.waiting_queues[pr] = rotated + s.waiting_queues[pr]

                if candidate is None:
                    continue

                # Avoid scheduling the same pipeline multiple times in the same tick across pools
                if candidate.pipeline_id in scheduled_pipelines_this_tick:
                    s.waiting_queues[pr].append(candidate)
                    continue

                op = _get_ready_op(candidate)
                if op is None:
                    # Became not-ready; keep it
                    s.waiting_queues[pr].append(candidate)
                    continue

                cpu, ram = _request_for_op(s, pool, pr, op, eff_rem_cpu, eff_rem_ram)
                if cpu is None or ram is None:
                    # Doesn't fit now; push to back to avoid sticking at front
                    s.waiting_queues[pr].append(candidate)
                    continue

                # Create assignment
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=pr,
                        pool_id=pool_id,
                        pipeline_id=candidate.pipeline_id,
                    )
                )

                # Update remaining pool resources for planning this tick
                rem_cpu -= cpu
                rem_ram -= ram

                scheduled_this_pool += 1
                scheduled_pipelines_this_tick.add(candidate.pipeline_id)

                # Re-enqueue pipeline for future ops
                s.waiting_queues[pr].append(candidate)

                made_assignment = True
                break  # move to next assignment slot

            if not made_assignment:
                break

    return suspensions, assignments