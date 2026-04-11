# policy_key: scheduler_high_013
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.128967
# generation_seconds: 86.22
# generated_at: 2026-03-12T22:52:37.692700
@register_scheduler_init(key="scheduler_high_013")
def scheduler_high_013_init(s):
    """Priority-aware, latency-oriented scheduler (small incremental improvements over naive FIFO).

    Key ideas:
      1) Maintain per-priority FIFO queues (QUERY > INTERACTIVE > BATCH) to reduce tail latency.
      2) Avoid "give the whole pool to the next op" by using conservative per-op CPU/RAM sizing,
         enabling concurrency and preserving headroom for interactive work.
      3) Keep a small "interactive reserve" (CPU/RAM) in shared pools so background work doesn't
         consume everything and block high-priority arrivals.
      4) Simple OOM-aware retries: if a container fails with an OOM-like error, bump RAM request
         multiplier for that pipeline and retry failed ops; otherwise, drop the pipeline on failure.
    """
    # Persistent per-priority queues (round-robin within each queue)
    s.q_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Light-weight time to enable simple anti-starvation knobs
    s.tick = 0
    s.enqueue_tick = {}  # pipeline_id -> first time seen

    # Per-pipeline sizing hints (learned via OOM failures)
    s.ram_mult = {}  # pipeline_id -> multiplier for requested RAM
    s.drop_pipelines = set()  # pipeline_ids to drop (non-OOM failures)

    # Policy knobs (kept intentionally simple)
    s.interactive_reserve_frac_cpu = 0.20
    s.interactive_reserve_frac_ram = 0.20
    s.batch_can_borrow_reserve_after_ticks = 200  # prevent indefinite starvation


def _sched_high_013_is_oom(err) -> bool:
    if err is None:
        return False
    try:
        e = str(err).lower()
    except Exception:
        return False
    return ("oom" in e) or ("out of memory" in e) or ("cuda out of memory" in e) or ("killed" in e and "memory" in e)


def _sched_high_013_pipeline_done_or_invalid(pipeline, status, drop_set) -> bool:
    # Drop completed pipelines; also drop pipelines we marked as invalid (non-OOM failures)
    if pipeline.pipeline_id in drop_set:
        return True
    if status.is_pipeline_successful():
        return True
    return False


def _sched_high_013_has_in_flight_work(status) -> bool:
    # Keep it conservative: at most one op in-flight per pipeline to reduce interference and
    # avoid blasting a single pipeline with many containers.
    try:
        running = status.state_counts.get(OperatorState.RUNNING, 0)
        assigned = status.state_counts.get(OperatorState.ASSIGNED, 0)
    except Exception:
        # If state_counts isn't dict-like, just assume it's safe to schedule
        return False
    return (running + assigned) > 0


def _sched_high_013_next_ready_op(pipeline):
    status = pipeline.runtime_status()
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    return status, op_list


def _sched_high_013_prio_order_for_pool(num_pools: int, pool_id: int):
    # If multiple pools exist, lightly bias pool 0 towards interactive/query.
    if num_pools > 1 and pool_id == 0:
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _sched_high_013_base_request(pool, priority):
    # Conservative defaults: enough to finish interactive quickly, but still allow concurrency.
    max_cpu = pool.max_cpu_pool
    max_ram = pool.max_ram_pool

    if priority == Priority.QUERY:
        cpu = min(max_cpu * 0.50, 8.0)
        ram = min(max_ram * 0.50, 16.0)
    elif priority == Priority.INTERACTIVE:
        cpu = min(max_cpu * 0.40, 6.0)
        ram = min(max_ram * 0.40, 12.0)
    else:  # Priority.BATCH_PIPELINE
        cpu = min(max_cpu * 0.25, 4.0)
        ram = min(max_ram * 0.25, 8.0)

    # Ensure non-zero requests when pools are tiny
    cpu = max(cpu, max_cpu * 0.10, 0.1)
    ram = max(ram, max_ram * 0.10, 0.1)
    return cpu, ram


def _sched_high_013_queue_cleanup_inplace(q, drop_set):
    # Remove pipelines that have completed or are marked as dropped
    i = 0
    while i < len(q):
        p = q[i]
        st = p.runtime_status()
        if _sched_high_013_pipeline_done_or_invalid(p, st, drop_set):
            q.pop(i)
        else:
            i += 1


def _sched_high_013_any_interactive_waiting(s) -> bool:
    # Determine if there's any ready-to-run QUERY/INTERACTIVE work waiting.
    for pr in (Priority.QUERY, Priority.INTERACTIVE):
        q = s.q_by_prio.get(pr, [])
        for p in q:
            if p.pipeline_id in s.drop_pipelines:
                continue
            st, op_list = _sched_high_013_next_ready_op(p)
            if st.is_pipeline_successful():
                continue
            if _sched_high_013_has_in_flight_work(st):
                continue
            if op_list:
                return True
    return False


@register_scheduler(key="scheduler_high_013")
def scheduler_high_013(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Priority-aware scheduler with:
      - per-priority queues + round-robin fairness within each priority
      - conservative per-op sizing (avoid monopolizing the pool)
      - small interactive reserve in shared pools
      - OOM-aware RAM backoff + retry for FAILED ops
    """
    s.tick += 1

    # Incorporate new arrivals into priority queues
    for p in pipelines:
        if p.pipeline_id not in s.enqueue_tick:
            s.enqueue_tick[p.pipeline_id] = s.tick
        if p.pipeline_id not in s.ram_mult:
            s.ram_mult[p.pipeline_id] = 1.0
        # Enqueue by priority
        s.q_by_prio[p.priority].append(p)

    # Process results: learn from failures (OOM -> increase RAM), otherwise drop pipeline
    for r in results:
        if not r.failed():
            continue
        # Any non-OOM failure: drop pipeline to avoid infinite retries
        if _sched_high_013_is_oom(getattr(r, "error", None)):
            # Increase RAM multiplier for this pipeline; cap to avoid unbounded growth
            pid = getattr(r, "pipeline_id", None)
            # Some sims may not include pipeline_id in ExecutionResult; fall back to "best effort"
            if pid is None:
                # If pipeline_id isn't available, we can't target learning precisely; do nothing.
                continue
            cur = s.ram_mult.get(pid, 1.0)
            s.ram_mult[pid] = min(cur * 1.6, 16.0)
        else:
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.drop_pipelines.add(pid)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # Cleanup queues (remove completed/dropped pipelines)
    for pr in list(s.q_by_prio.keys()):
        _sched_high_013_queue_cleanup_inplace(s.q_by_prio[pr], s.drop_pipelines)

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Determine whether we should protect headroom for interactive/query work
    interactive_waiting = _sched_high_013_any_interactive_waiting(s)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If interactive work is waiting, keep a reserve in this pool (unless batch has waited long).
        reserve_cpu = pool.max_cpu_pool * s.interactive_reserve_frac_cpu
        reserve_ram = pool.max_ram_pool * s.interactive_reserve_frac_ram

        prio_order = _sched_high_013_prio_order_for_pool(s.executor.num_pools, pool_id)

        # Try to schedule multiple assignments per pool per tick (unlike the naive "one and break").
        # But avoid infinite loops if nothing is schedulable.
        made_progress = True
        guard = 0
        while made_progress and avail_cpu > 0 and avail_ram > 0 and guard < 200:
            guard += 1
            made_progress = False

            for pr in prio_order:
                q = s.q_by_prio.get(pr, [])
                if not q:
                    continue

                # Round-robin: pop front, attempt schedule, then re-append if still active
                p = q.pop(0)
                st, op_list = _sched_high_013_next_ready_op(p)

                # Drop/ignore pipelines that are done or invalid
                if _sched_high_013_pipeline_done_or_invalid(p, st, s.drop_pipelines):
                    continue

                # Avoid piling multiple concurrent ops from the same pipeline
                if _sched_high_013_has_in_flight_work(st):
                    q.append(p)
                    continue

                if not op_list:
                    # Not ready due to dependencies; requeue
                    q.append(p)
                    continue

                # Compute request size
                base_cpu, base_ram = _sched_high_013_base_request(pool, pr)
                ram_mult = s.ram_mult.get(p.pipeline_id, 1.0)
                req_cpu = base_cpu
                req_ram = base_ram * ram_mult

                # Enforce interactive reserve for batch work when interactive is waiting,
                # unless this batch pipeline has been waiting long enough.
                usable_cpu = avail_cpu
                usable_ram = avail_ram
                if pr == Priority.BATCH_PIPELINE and interactive_waiting:
                    enq = s.enqueue_tick.get(p.pipeline_id, s.tick)
                    waited = s.tick - enq
                    if waited < s.batch_can_borrow_reserve_after_ticks:
                        usable_cpu = max(0.0, avail_cpu - reserve_cpu)
                        usable_ram = max(0.0, avail_ram - reserve_ram)

                # If we can't satisfy the request, don't start it (avoid predictable OOM / churn).
                if req_cpu <= 0 or req_ram <= 0 or usable_cpu <= 0 or usable_ram <= 0:
                    q.append(p)
                    continue
                if req_cpu > usable_cpu or req_ram > usable_ram:
                    # Can't place now; keep it queued
                    q.append(p)
                    continue

                # Admit the op
                a = Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(a)

                # Update available pool headroom
                avail_cpu -= req_cpu
                avail_ram -= req_ram

                # Requeue pipeline for subsequent ops
                q.append(p)
                made_progress = True
                break  # after scheduling one op, restart scanning from highest priority

    return suspensions, assignments
