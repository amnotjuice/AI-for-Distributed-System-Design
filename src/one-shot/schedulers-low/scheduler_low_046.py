# policy_key: scheduler_low_046
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043987
# generation_seconds: 35.28
# generated_at: 2026-03-14T02:37:54.047810
@register_scheduler_init(key="scheduler_low_046")
def scheduler_low_046_init(s):
    """Priority-aware FIFO scheduler (small, safe improvements over naive FIFO).

    Improvements vs. naive baseline:
    - Maintains an active pipeline set instead of repeatedly cycling a single FIFO list.
    - Always schedules higher-priority ready work first (QUERY > INTERACTIVE > BATCH).
    - Uses simple per-priority resource caps to reduce head-of-line blocking and
      preserve headroom for subsequent high-priority arrivals (latency-oriented).
    - Keeps batching conservative (smaller slices) to avoid starving interactive work.

    Notes:
    - No preemption yet (kept intentionally simple and robust).
    - No OOM-based resizing yet (requires reliable linkage from ExecutionResult -> pipeline/op).
    """
    from collections import deque

    # Active pipelines we are responsible for (arrived but not yet terminal).
    s._active = {}  # pipeline_id -> Pipeline

    # Round-robin queues per priority for ready pipelines (rebuilt opportunistically each tick).
    s._rr = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # A small guard to avoid repeatedly enqueuing same pipeline multiple times in one tick.
    s._enqueued_this_tick = set()


def _scheduler_low_046_priority_order():
    # Highest urgency first; QUERY typically expects tight latency.
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _scheduler_low_046_is_terminal(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If anything has failed, we treat the pipeline as terminal for now (no retries here).
    # (A later, more complex policy could differentiate OOM vs non-OOM and retry.)
    return status.state_counts[OperatorState.FAILED] > 0


def _scheduler_low_046_get_one_ready_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _scheduler_low_046_enqueue_ready(s, pipeline):
    """If pipeline has a ready op, enqueue it into its priority RR queue once per tick."""
    if pipeline.pipeline_id in s._enqueued_this_tick:
        return
    if _scheduler_low_046_is_terminal(pipeline):
        return
    op = _scheduler_low_046_get_one_ready_op(pipeline)
    if op is None:
        return
    s._rr[pipeline.priority].append(pipeline)
    s._enqueued_this_tick.add(pipeline.pipeline_id)


def _scheduler_low_046_pick_caps(pool, priority, avail_cpu, avail_ram):
    """Latency-oriented resource sizing: cap per assignment based on priority."""
    # Ensure we don't request 0 resources.
    min_cpu = 1e-9  # allow float CPU models; if CPU is integer in the sim this still works via min()
    min_ram = 1e-9

    # Conservative caps: leave headroom for other work, especially high priority.
    if priority == Priority.QUERY:
        cpu_cap = max(min_cpu, 0.50 * pool.max_cpu_pool)
        ram_cap = max(min_ram, 0.60 * pool.max_ram_pool)
    elif priority == Priority.INTERACTIVE:
        cpu_cap = max(min_cpu, 0.40 * pool.max_cpu_pool)
        ram_cap = max(min_ram, 0.50 * pool.max_ram_pool)
    else:  # BATCH_PIPELINE
        cpu_cap = max(min_cpu, 0.25 * pool.max_cpu_pool)
        ram_cap = max(min_ram, 0.35 * pool.max_ram_pool)

    cpu = min(avail_cpu, cpu_cap)
    ram = min(avail_ram, ram_cap)
    return cpu, ram


@register_scheduler(key="scheduler_low_046")
def scheduler_low_046(s, results: list, pipelines: list):
    """
    Priority-aware, latency-oriented FIFO scheduling.

    Each tick:
    1) Add new pipelines to active set; drop terminal pipelines.
    2) Build/refresh per-priority round-robin queues of pipelines that have at least one ready op.
    3) For each pool, schedule at most one operator (to reduce contention and preserve headroom),
       always preferring higher-priority queues.
    """
    # Record arrivals.
    for p in pipelines:
        s._active[p.pipeline_id] = p

    # Prune terminal pipelines opportunistically (based on latest runtime_status()).
    # We also prune on every tick that has any activity (arrivals or results).
    if pipelines or results:
        to_delete = []
        for pid, p in s._active.items():
            if _scheduler_low_046_is_terminal(p):
                to_delete.append(pid)
        for pid in to_delete:
            del s._active[pid]

    # Early exit if nothing changed and we already have no active work.
    if not pipelines and not results and not s._active:
        return [], []

    # Rebuild enqueue guards for this tick.
    s._enqueued_this_tick = set()

    # Refresh queues: keep existing RR order but top-up with currently-ready pipelines.
    # Also, drop pipelines from queues that have become terminal.
    for prio in _scheduler_low_046_priority_order():
        # Clean queue of pipelines that are no longer active/ready.
        cleaned = []
        while s._rr[prio]:
            p = s._rr[prio].popleft()
            if p.pipeline_id in s._active and not _scheduler_low_046_is_terminal(p):
                cleaned.append(p)
        for p in cleaned:
            s._rr[prio].append(p)

    # Top-up: enqueue any active pipeline with ready work not already enqueued this tick.
    for p in s._active.values():
        _scheduler_low_046_enqueue_ready(s, p)

    suspensions = []
    assignments = []

    # Schedule per pool: at most one assignment per pool per tick (simple but good for latency).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        made_assignment = False

        for prio in _scheduler_low_046_priority_order():
            # Try a few pipelines from this priority in RR order.
            # Bound attempts to current queue length to avoid infinite loops.
            q = s._rr[prio]
            for _ in range(len(q)):
                pipeline = q.popleft()

                # Pipeline might have changed since enqueued.
                if pipeline.pipeline_id not in s._active or _scheduler_low_046_is_terminal(pipeline):
                    continue

                op = _scheduler_low_046_get_one_ready_op(pipeline)
                if op is None:
                    # Not ready anymore; do not requeue now (will be re-enqueued when ready).
                    continue

                cpu, ram = _scheduler_low_046_pick_caps(pool, prio, avail_cpu, avail_ram)
                if cpu <= 0 or ram <= 0:
                    # Can't fit anything right now in this pool.
                    q.appendleft(pipeline)
                    break

                assignment = Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
                assignments.append(assignment)

                # Requeue pipeline to allow RR across its subsequent operators.
                q.append(pipeline)
                made_assignment = True
                break

            if made_assignment:
                break

    return suspensions, assignments
