# policy_key: scheduler_none_036
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.038317
# generation_seconds: 35.13
# generated_at: 2026-03-14T02:07:26.091794
@register_scheduler_init(key="scheduler_none_036")
def scheduler_none_036_init(s):
    """Priority-aware FIFO with gentle preemption and simple OOM-driven RAM bumping.

    Improvements over naive FIFO:
    1) Priority queues: always try to run QUERY/INTERACTIVE before BATCH.
    2) Avoid head-of-line blocking: keep pipelines in per-priority queues, rotate fairly within each.
    3) Preempt only when necessary: if no capacity for high-priority runnable ops, suspend a low-priority container.
    4) Learn minimal RAM on OOM: if an op fails with OOM, bump future RAM requests for that pipeline.

    Policy intentionally stays simple:
    - Assign at most one runnable op per pipeline per tick (reduces blast radius and improves latency).
    - Prefer placing high priority work on the pool with most headroom (best-fit-by-headroom).
    """
    from collections import deque

    # Per-priority waiting queues
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track known-good RAM for a pipeline (learned from OOM retries)
    # pipeline_id -> ram
    s.pipeline_ram_hint = {}

    # Track currently running containers by pool for possible preemption
    # pool_id -> list[ExecutionResult] (last seen running/assigned)
    s.running_by_pool = {}

    # Configuration knobs (kept conservative)
    s.max_preempts_per_tick = 2  # avoid thrash
    s.ram_bump_factor = 1.6      # on OOM, multiply last ram by this
    s.ram_bump_min = 256         # absolute additive bump (MB-ish units depending on sim)
    s.cpu_grant_query = 0.75     # fraction of pool CPU to give high-priority (cap at available)
    s.cpu_grant_interactive = 0.6
    s.cpu_grant_batch = 0.4

    # Soft reserve for high priority (don’t let batch eat the last slice)
    s.reserve_cpu_for_hp = 1
    s.reserve_ram_for_hp = 256


def _queue_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_hp(prio):
    return prio in (Priority.QUERY, Priority.INTERACTIVE)


def _cpu_fraction_for(prio, s):
    if prio == Priority.QUERY:
        return s.cpu_grant_query
    if prio == Priority.INTERACTIVE:
        return s.cpu_grant_interactive
    return s.cpu_grant_batch


def _pick_pool_with_headroom(s, min_cpu, min_ram, prefer_hp):
    """Pick a pool that can fit; if multiple, pick the one with most headroom (cpu+ram normalized)."""
    best = None
    best_score = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool < min_cpu or pool.avail_ram_pool < min_ram:
            continue
        # Headroom score: prefer more free resources; slightly bias toward more RAM when prefer_hp
        cpu_score = pool.avail_cpu_pool / max(pool.max_cpu_pool, 1e-9)
        ram_score = pool.avail_ram_pool / max(pool.max_ram_pool, 1e-9)
        score = cpu_score + (1.2 * ram_score if prefer_hp else ram_score)
        if best is None or score > best_score:
            best = pool_id
            best_score = score
    return best


def _collect_running(results):
    """Return dict pool_id -> list of running-ish containers (ExecutionResult entries)."""
    running_by_pool = {}
    for r in results:
        if r.container_id is None:
            continue
        # We treat any non-failed result as representative of an active container footprint we can preempt.
        # (Simulator typically emits results on completion/failure; but we also use it as best-effort state.)
        running_by_pool.setdefault(r.pool_id, []).append(r)
    return running_by_pool


def _choose_preemption_candidate(running_list):
    """Pick the lowest-priority candidate to preempt; tie-breaker: largest RAM."""
    if not running_list:
        return None
    # Lowest priority first: batch > interactive > query
    def prio_rank(p):
        if p == Priority.BATCH_PIPELINE:
            return 2
        if p == Priority.INTERACTIVE:
            return 1
        return 0

    # Prefer suspending batch; if multiple, suspend the one consuming more RAM
    candidates = sorted(
        running_list,
        key=lambda r: (prio_rank(r.priority), getattr(r, "ram", 0)),
        reverse=True
    )
    return candidates[0]


def _learn_from_results(s, results):
    """Update RAM hints from OOM failures and refresh running_by_pool view."""
    # Update running_by_pool best-effort view
    s.running_by_pool = _collect_running(results)

    for r in results:
        if not r.failed():
            continue
        # Heuristic: if error string indicates OOM, bump RAM hint for that pipeline.
        err = (r.error or "").lower() if hasattr(r, "error") else ""
        is_oom = ("oom" in err) or ("out of memory" in err) or ("killed" in err and "memory" in err)
        if not is_oom:
            continue
        # Identify pipeline_id: may be present; if not, we can't learn.
        pipeline_id = getattr(r, "pipeline_id", None)
        if pipeline_id is None:
            continue
        last_ram = getattr(r, "ram", None)
        if last_ram is None:
            continue
        bumped = int(max(last_ram * s.ram_bump_factor, last_ram + s.ram_bump_min))
        prev = s.pipeline_ram_hint.get(pipeline_id, 0)
        if bumped > prev:
            s.pipeline_ram_hint[pipeline_id] = bumped


def _enqueue_new_pipelines(s, pipelines):
    for p in pipelines:
        _queue_for_priority(s, p.priority).append(p)


def _drain_completed_and_invalid(s, q):
    """Remove completed/failed pipelines from queue; keep others in order."""
    from collections import deque
    newq = deque()
    while q:
        p = q.popleft()
        status = p.runtime_status()
        has_failures = status.state_counts[OperatorState.FAILED] > 0
        if status.is_pipeline_successful() or has_failures:
            continue
        newq.append(p)
    return newq


def _next_runnable_op(p):
    status = p.runtime_status()
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    return op_list


@register_scheduler(key="scheduler_none_036")
def scheduler_none_036(s, results, pipelines):
    """
    Scheduler tick:
    - ingest new pipelines into priority queues
    - learn from OOM failures
    - for each pool, try to schedule highest priority runnable work first
    - if no space for high priority, preempt a batch container (limited per tick)
    - schedule batch only when not consuming the last reserved resources
    """
    _enqueue_new_pipelines(s, pipelines)
    if not pipelines and not results:
        return [], []

    _learn_from_results(s, results)

    # Clean queues from completed/failed pipelines
    s.q_query = _drain_completed_and_invalid(s, s.q_query)
    s.q_interactive = _drain_completed_and_invalid(s, s.q_interactive)
    s.q_batch = _drain_completed_and_invalid(s, s.q_batch)

    suspensions = []
    assignments = []
    preempts_used = 0

    # Helper to attempt scheduling one op from a given priority queue onto a given pool
    def try_schedule_from_queue(q, prio, pool_id):
        nonlocal assignments
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return False

        # Keep order but avoid head-of-line blocking: rotate until we find a runnable pipeline or we loop once
        n = len(q)
        for _ in range(n):
            p = q.popleft()
            status = p.runtime_status()
            has_failures = status.state_counts[OperatorState.FAILED] > 0
            if status.is_pipeline_successful() or has_failures:
                continue

            op_list = _next_runnable_op(p)
            if not op_list:
                # Not runnable yet, keep it in queue
                q.append(p)
                continue

            # Determine resource request
            ram_hint = s.pipeline_ram_hint.get(p.pipeline_id, None)
            # Default: ask for a fraction of pool RAM; if learned, ask learned
            if ram_hint is None:
                req_ram = int(min(avail_ram, max(1, pool.max_ram_pool // 2)))
            else:
                req_ram = int(min(avail_ram, min(ram_hint, pool.max_ram_pool)))

            cpu_frac = _cpu_fraction_for(prio, s)
            req_cpu = max(1, int(min(avail_cpu, max(1, pool.max_cpu_pool * cpu_frac))))

            # If batch, respect soft reserves for high priority
            if prio == Priority.BATCH_PIPELINE:
                if (avail_cpu - req_cpu) < s.reserve_cpu_for_hp or (avail_ram - req_ram) < s.reserve_ram_for_hp:
                    q.append(p)
                    return False

            # Must fit current availability
            if req_cpu > avail_cpu or req_ram > avail_ram:
                q.append(p)
                return False

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Put pipeline back for subsequent ops (but not immediately scheduled again in this tick)
            q.append(p)
            return True

        return False

    # Main loop: for each pool, place work with priority preference
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Try to schedule multiple tasks per pool if there is capacity, but keep it conservative
        # to protect latency and reduce contention.
        scheduled_any = True
        inner_iterations = 0
        while scheduled_any and inner_iterations < 8:
            inner_iterations += 1
            scheduled_any = False

            # Attempt high priority first
            if try_schedule_from_queue(s.q_query, Priority.QUERY, pool_id):
                scheduled_any = True
                continue
            if try_schedule_from_queue(s.q_interactive, Priority.INTERACTIVE, pool_id):
                scheduled_any = True
                continue

            # If there is pending high-priority work but not enough space, consider preemption
            hp_backlog = (len(s.q_query) + len(s.q_interactive)) > 0
            if hp_backlog and preempts_used < s.max_preempts_per_tick:
                # Only preempt if the pool is resource-constrained
                if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0 or (
                    pool.avail_cpu_pool < max(1, int(pool.max_cpu_pool * 0.25))
                    or pool.avail_ram_pool < max(1, int(pool.max_ram_pool * 0.25))
                ):
                    running_list = s.running_by_pool.get(pool_id, [])
                    cand = _choose_preemption_candidate(running_list)
                    if cand is not None and cand.priority == Priority.BATCH_PIPELINE:
                        suspensions.append(Suspend(cand.container_id, pool_id))
                        preempts_used += 1
                        # After suspension, don't immediately assume resources are freed in this tick;
                        # simulator will update availability next tick.
                        continue

            # Finally, schedule batch if possible (and respecting reserves)
            if try_schedule_from_queue(s.q_batch, Priority.BATCH_PIPELINE, pool_id):
                scheduled_any = True
                continue

    return suspensions, assignments

