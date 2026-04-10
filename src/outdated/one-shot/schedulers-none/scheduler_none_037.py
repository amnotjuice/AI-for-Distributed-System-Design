# policy_key: scheduler_none_037
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.038317
# generation_seconds: 35.00
# generated_at: 2026-03-14T02:08:01.096796
@register_scheduler_init(key="scheduler_none_037")
def scheduler_none_037_init(s):
    """Priority-aware incremental scheduler (small, safe improvements over naive FIFO).

    Improvements vs naive:
    1) Maintain per-priority queues so interactive/query work is admitted first.
    2) Apply simple per-op right-sizing (avoid "give everything" to one op): allocate a bounded
       CPU/RAM slice per op to reduce head-of-line blocking and improve latency under contention.
    3) React to OOM by bumping the pipeline's RAM request for subsequent retries (bounded exponential backoff).
    4) Basic preemption: when high-priority work arrives and no pool has headroom, suspend one
       low-priority running container to free resources (minimize churn: at most 1 preemption per tick).

    Notes:
    - This policy intentionally remains conservative: it does not try to model CPU scaling curves.
    - It schedules at most one operator per pool per tick to keep behavior stable and easy to iterate on.
    """
    # Waiting queues per priority (higher first when scheduling)
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track per-pipeline resource hints learned from failures (only RAM bump for now)
    # pipeline_id -> {"ram_hint": float}
    s.pipeline_hints = {}

    # Remember last-seen pipelines (for reference / potential future aging)
    s.known_pipelines = {}

    # Simple knobs (kept small and safe)
    s.min_cpu_per_op = 1.0          # don't allocate less than this if possible
    s.max_cpu_per_op = 4.0          # cap CPU per op to prevent a single op from monopolizing a pool
    s.min_ram_per_op_frac = 0.10    # allocate at least 10% of pool RAM when possible
    s.max_ram_per_op_frac = 0.60    # cap RAM per op at 60% to reduce blocking
    s.oom_ram_bump = 1.5            # bump RAM hint by 1.5x on OOM
    s.max_ram_hint_frac = 0.90      # never ask for more than 90% of pool RAM (avoid impossible fits)
    s.max_preemptions_per_tick = 1  # limit churn


def _prio_rank(priority):
    # Higher number = higher priority in our ordering
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1


def _iter_pipelines_by_priority(s):
    # Deterministic ordering: QUERY -> INTERACTIVE -> BATCH_PIPELINE
    for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        q = s.wait_q.get(prio, [])
        for p in q:
            yield p


def _remove_pipeline_from_queues(s, pipeline):
    # Remove a pipeline instance from its priority queue if present
    q = s.wait_q.get(pipeline.priority, [])
    for i, p in enumerate(q):
        if p.pipeline_id == pipeline.pipeline_id:
            q.pop(i)
            return


def _pick_assignable_op(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return None
    # Only schedule ops whose parents are complete
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[:1]


def _compute_request(s, pipeline, pool):
    """Compute bounded CPU/RAM request for one operator in this pool."""
    # CPU: bounded slice
    cpu = min(max(pool.avail_cpu_pool, 0.0), s.max_cpu_per_op)
    cpu = max(min(cpu, pool.avail_cpu_pool), 0.0)
    if cpu > 0:
        cpu = max(min(cpu, pool.avail_cpu_pool), s.min_cpu_per_op) if pool.avail_cpu_pool >= s.min_cpu_per_op else pool.avail_cpu_pool

    # RAM: bounded fraction of pool + per-pipeline hint
    base_ram = pool.max_ram_pool * s.max_ram_per_op_frac
    base_ram = min(base_ram, pool.avail_ram_pool)

    min_ram = min(pool.avail_ram_pool, pool.max_ram_pool * s.min_ram_per_op_frac)

    hint = s.pipeline_hints.get(pipeline.pipeline_id, {}).get("ram_hint", 0.0)
    # If we have a hint, honor it (up to caps); else use base.
    ram = base_ram
    if hint and hint > 0:
        ram = min(pool.avail_ram_pool, hint)
    # Enforce bounds
    ram = max(ram, min_ram) if pool.avail_ram_pool >= min_ram else pool.avail_ram_pool
    ram_cap = min(pool.avail_ram_pool, pool.max_ram_pool * s.max_ram_hint_frac)
    ram = min(ram, ram_cap)

    return cpu, ram


def _needs_preemption(s, incoming_high, pool):
    # If no resources at all, preemption may help; we keep this conservative.
    if pool.avail_cpu_pool >= s.min_cpu_per_op and pool.avail_ram_pool > 0:
        return False
    return incoming_high


def _find_low_prio_running_to_preempt(results, target_pool_id):
    """Pick one low-priority running container to preempt in a given pool (best effort).

    We approximate 'running' as 'recently reported result without completion'; however, we don't
    have a direct list of active containers here. Many simulators emit periodic results; if not,
    preemption will simply not trigger.
    """
    # Prefer preempting BATCH over INTERACTIVE over QUERY (never preempt QUERY here)
    candidate = None
    candidate_rank = 999
    for r in results:
        if r.pool_id != target_pool_id:
            continue
        if r.container_id is None:
            continue
        pr = r.priority
        if pr == Priority.QUERY:
            continue
        rank = 1 if pr == Priority.BATCH_PIPELINE else 2  # BATCH best to preempt
        if rank < candidate_rank:
            candidate = r
            candidate_rank = rank
    return candidate


@register_scheduler(key="scheduler_none_037")
def scheduler_none_037_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler with simple right-sizing, OOM-driven RAM bump, and limited preemption.
    """
    # Enqueue new pipelines by priority
    for p in pipelines:
        s.known_pipelines[p.pipeline_id] = p
        s.wait_q[p.priority].append(p)

    # Learn from results (OOM -> bump RAM hint)
    # Also prune completed/failed pipelines from queues when observed.
    for r in results:
        # If an op failed due to OOM, increase RAM hint for that pipeline for next attempt.
        # We treat any failure with "oom" substring as an OOM signal (common pattern).
        if getattr(r, "failed", None) and r.failed():
            err = (r.error or "")
            if "oom" in err.lower() or "out of memory" in err.lower():
                # r.ram is the RAM that was insufficient; bump from that baseline.
                prev = s.pipeline_hints.get(r.ops[0].pipeline_id if (hasattr(r, "ops") and r.ops) else None, {}).get("ram_hint", 0.0)
                # Pipeline id is not guaranteed on result; fall back to matching via known pipelines
                # by searching which pipeline currently has this op. Best-effort.
                pid = None
                if hasattr(r, "ops") and r.ops:
                    # Operators usually carry pipeline_id in this simulator; best-effort access.
                    pid = getattr(r.ops[0], "pipeline_id", None)
                if pid is None:
                    # Can't attribute; skip.
                    continue
                cur = max(prev, float(r.ram or 0.0))
                bumped = cur * s.oom_ram_bump if cur > 0 else 0.0
                s.pipeline_hints[pid] = {"ram_hint": bumped}

    # Early exit
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Determine if any high-priority work is waiting (for preemption decision)
    incoming_high = len(s.wait_q[Priority.QUERY]) > 0 or len(s.wait_q[Priority.INTERACTIVE]) > 0

    # Limited preemption: if high-priority work exists and a pool is fully blocked, suspend one low-priority
    preemptions_left = s.max_preemptions_per_tick
    if incoming_high and preemptions_left > 0 and results:
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if not _needs_preemption(s, incoming_high=True, pool=pool):
                continue
            cand = _find_low_prio_running_to_preempt(results, pool_id)
            if cand is None:
                continue
            suspensions.append(Suspend(cand.container_id, cand.pool_id))
            preemptions_left -= 1
            if preemptions_left <= 0:
                break

    # Scheduling: one op per pool per tick, highest priority first
    # We rotate through priorities but keep stable ordering within each queue.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        chosen = None
        # Find first pipeline with an assignable op
        for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            q = s.wait_q[prio]
            for p in q:
                status = p.runtime_status()
                # Drop completed pipelines from queue
                if status.is_pipeline_successful():
                    chosen = None
                    _remove_pipeline_from_queues(s, p)
                    break
                # If pipeline has any failures, we conservatively stop retrying (like naive example)
                # except OOM which will be retried because FAILED ops are assignable.
                if status.state_counts.get(OperatorState.FAILED, 0) > 0:
                    # If FAILED ops are due to OOM they'll be retried; we still allow scheduling
                    # by relying on get_ops(ASSIGNABLE_STATES).
                    pass

                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if op_list:
                    chosen = p
                    chosen_ops = op_list[:1]
                    break
            if chosen is not None:
                break

        if chosen is None:
            continue

        cpu, ram = _compute_request(s, chosen, pool)
        if cpu <= 0 or ram <= 0:
            continue

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=cpu,
                ram=ram,
                priority=chosen.priority,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
        )

    return suspensions, assignments
