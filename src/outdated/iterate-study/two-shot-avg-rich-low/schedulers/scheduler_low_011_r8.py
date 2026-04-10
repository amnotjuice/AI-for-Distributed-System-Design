# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r8
@register_scheduler_init(key="scheduler_low_011_r8")
def scheduler_low_011_r8_init(s):
    """Iteration 2: Priority-aware, throughput-friendly scheduler to reduce queueing latency.

    Small, obvious fixes vs prior iteration:
    - Fill each pool with multiple assignments per tick (not just one), avoiding artificial under-utilization.
    - Add weighted round-robin across priorities to prevent total starvation (interactive/batch were not completing).
    - Dedicate pool 0 (when multiple pools exist) to latency-sensitive work (query+interactive) to protect tail latency.
    - Use smaller default RAM slices for high-priority work to increase concurrency; rely on OOM backoff if needed.
    - Keep simple OOM-aware RAM backoff; do not drop pipelines just because they contain FAILED ops (allow retries).
    """
    from collections import deque

    s.waiting_queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    # De-dup by pipeline_id to avoid queue blowups and unfairness.
    s.in_queue = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Resource hints learned per (pipeline_id, op identity)
    s.op_hints = {}       # (pipeline_id, id(op)) -> {"ram": float, "cpu": float}
    s.op_attempts = {}    # (pipeline_id, id(op)) -> int
    s.max_retries_per_op = 3

    # Pipelines with a known non-OOM failure: do not waste cycles retrying.
    s.dead_pipelines = set()

    # Per-pool round-robin pointer (to make scheduling stable and fair).
    s.rr_ptr = {}  # pool_id -> int

    # Caps to keep a single task from consuming a whole pool (helps concurrency/latency under load).
    s.cpu_cap = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 8.0,
    }

    # Default request as fraction of pool max.
    # Note: RAM beyond minimum doesn't speed up compute; lower RAM improves concurrency (with OOM backoff safety).
    s.frac = {
        Priority.QUERY: {"cpu": 0.25, "ram": 0.25},
        Priority.INTERACTIVE: {"cpu": 0.25, "ram": 0.30},
        Priority.BATCH_PIPELINE: {"cpu": 0.50, "ram": 0.50},
    }

    # Scheduling loop bounds (avoid pathological long loops).
    s.max_assignments_per_pool_tick = 32
    s.max_pipeline_pops_per_pool_tick = 256


def _r8_is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _r8_op_key(pipeline_id, op):
    return (pipeline_id, id(op))


def _r8_enqueue(s, pipeline):
    # Defensive: unknown priorities treated as batch.
    pr = pipeline.priority if pipeline.priority in s.waiting_queues else Priority.BATCH_PIPELINE
    pid = pipeline.pipeline_id
    if pid in s.dead_pipelines:
        return
    if pid not in s.in_queue[pr]:
        s.waiting_queues[pr].append(pipeline)
        s.in_queue[pr].add(pid)


def _r8_dequeue(s, pr):
    q = s.waiting_queues[pr]
    while q:
        p = q.popleft()
        s.in_queue[pr].discard(p.pipeline_id)
        if p.pipeline_id in s.dead_pipelines:
            continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        return p
    return None


def _r8_pick_assignable_op(pipeline):
    st = pipeline.runtime_status()
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _r8_cycle_for_pool(num_pools, pool_id):
    # Weighted mix to protect latency for high-priority while ensuring progress for others.
    # If multiple pools exist, reserve pool 0 for latency-sensitive work.
    if num_pools > 1 and pool_id == 0:
        # No batch on pool 0 by default: keep it for query/interactive.
        return ([Priority.QUERY] * 6) + ([Priority.INTERACTIVE] * 3)
    # Other pools: make batch progress while still servicing some query/interactive.
    return ([Priority.QUERY] * 2) + ([Priority.INTERACTIVE] * 1) + ([Priority.BATCH_PIPELINE] * 6)


def _r8_default_request(s, pool, pr, backlog_len):
    # Backlog-aware sizing:
    # Under heavy queueing, use smaller per-task CPU/RAM to increase parallelism and reduce queueing delay.
    fr = s.frac.get(pr, {"cpu": 1.0, "ram": 1.0})

    cpu = pool.max_cpu_pool * fr["cpu"]
    ram = pool.max_ram_pool * fr["ram"]

    # Scale down when backlog is high (simple, stable heuristic).
    if backlog_len >= 50:
        cpu *= 0.60
        ram *= 0.70
    elif backlog_len >= 20:
        cpu *= 0.75
        ram *= 0.85

    # Caps and floors
    cpu = min(cpu, s.cpu_cap.get(pr, cpu))
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)

    # Cap to current pool availability
    cpu = min(cpu, pool.avail_cpu_pool)
    ram = min(ram, pool.avail_ram_pool)
    return cpu, ram


def _r8_apply_hints(s, pool, pr, pipeline_id, op, cpu, ram):
    k = _r8_op_key(pipeline_id, op)
    hint = s.op_hints.get(k)
    if hint:
        cpu = max(cpu, float(hint.get("cpu", cpu)))
        ram = max(ram, float(hint.get("ram", ram)))

    # Final caps
    cpu = min(cpu, pool.avail_cpu_pool, pool.max_cpu_pool)
    ram = min(ram, pool.avail_ram_pool, pool.max_ram_pool)
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


@register_scheduler(key="scheduler_low_011_r8")
def scheduler_low_011_r8(s, results, pipelines):
    """
    Priority-aware weighted RR scheduler that fills pools to reduce queueing latency.

    Key latency improvements:
    - Avoid "one op per pool per tick" under-utilization by packing multiple assignments.
    - Weighted RR prevents starvation; pool 0 reserved for query+interactive when multi-pool.
    - Smaller default RAM for high-priority increases concurrency; OOM triggers RAM backoff retries.
    """
    # Enqueue new arrivals
    for p in pipelines:
        _r8_enqueue(s, p)

    # Update hints based on execution results (OOM backoff; mark non-OOM failed pipelines as dead).
    for r in results:
        # Determine failed status robustly
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            # Can't safely attribute to a pipeline; skip learning.
            continue

        err = getattr(r, "error", None)
        if _r8_is_oom_error(err):
            ops = getattr(r, "ops", []) or []
            for op in ops:
                k = _r8_op_key(pid, op)
                prev = s.op_hints.get(k, {})
                prev_ram = float(prev.get("ram", getattr(r, "ram", 1.0) or 1.0))
                prev_cpu = float(prev.get("cpu", getattr(r, "cpu", 1.0) or 1.0))

                attempts = int(s.op_attempts.get(k, 0)) + 1
                s.op_attempts[k] = attempts

                # Exponential RAM backoff (bounded later by pool caps when assigning)
                new_ram = max(1.0, prev_ram * 2.0)
                s.op_hints[k] = {"ram": new_ram, "cpu": max(1.0, prev_cpu)}

                # If we've retried too many times, stop spending scheduler effort on this pipeline.
                if attempts > s.max_retries_per_op:
                    s.dead_pipelines.add(pid)
        else:
            # Non-OOM failure: do not retry indefinitely.
            s.dead_pipelines.add(pid)

    # Early exit if no changes
    if not results and not pipelines:
        return [], []

    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools

    # Track pipelines scheduled in this tick to avoid duplicate assignments of the same op
    scheduled_this_tick = set()

    # Fill each pool as much as possible
    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]

        # Local accounting to avoid over-committing within this scheduling call
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        cycle = _r8_cycle_for_pool(num_pools, pool_id)
        if not cycle:
            continue

        rr = int(s.rr_ptr.get(pool_id, 0)) % len(cycle)

        made = 0
        pops = 0

        while (
            made < s.max_assignments_per_pool_tick
            and pops < s.max_pipeline_pops_per_pool_tick
            and avail_cpu >= 1.0
            and avail_ram >= 1.0
        ):
            # Choose next priority with something pending (scan at most one full cycle)
            chosen_pr = None
            for _ in range(len(cycle)):
                pr = cycle[rr]
                rr = (rr + 1) % len(cycle)
                # Quick empty check; also ignore dead pipelines via dequeue logic
                if s.waiting_queues[pr]:
                    chosen_pr = pr
                    break
            if chosen_pr is None:
                break

            p = _r8_dequeue(s, chosen_pr)
            pops += 1
            if p is None:
                continue

            pid = p.pipeline_id
            if pid in scheduled_this_tick:
                # Already scheduled from this pipeline in this tick; put back and move on.
                _r8_enqueue(s, p)
                continue

            op = _r8_pick_assignable_op(p)
            if op is None:
                # Not ready (parents incomplete / nothing assignable yet); requeue.
                _r8_enqueue(s, p)
                continue

            # Compute request using pool max, backlog size, and any learned hints.
            backlog_len = len(s.waiting_queues[chosen_pr]) + 1  # +1 for the pipeline we just popped
            cpu_req, ram_req = _r8_default_request(s, pool, chosen_pr, backlog_len)
            cpu_req, ram_req = _r8_apply_hints(s, pool, chosen_pr, pid, op, cpu_req, ram_req)

            # Fit check using local availability (not the original pool.avail_* which won't update until after return)
            if cpu_req > avail_cpu or ram_req > avail_ram:
                # Can't fit now; requeue and stop trying to pack further if we're resource-fragmented.
                _r8_enqueue(s, p)
                # If we still have plenty of one resource, it may be worth continuing; otherwise break.
                # Simple heuristic: if both are tight, stop.
                if avail_cpu < 2.0 or avail_ram < 2.0:
                    break
                continue

            # Make assignment (single op per container)
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen_pr,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Update local availability
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            made += 1
            scheduled_this_tick.add(pid)

            # Requeue pipeline so its next ops can be scheduled in later ticks
            _r8_enqueue(s, p)

        s.rr_ptr[pool_id] = rr

    return suspensions, assignments