# policy_key: scheduler_low_008
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043833
# generation_seconds: 44.44
# generated_at: 2026-03-12T21:40:15.738013
@register_scheduler_init(key="scheduler_low_008")
def scheduler_low_008_init(s):
    """
    Priority-aware, capacity-filling scheduler (small step up from naive FIFO).

    Improvements over the example FIFO:
    - Priority queues: always prefer QUERY/INTERACTIVE work over BATCH.
    - Fill a pool: schedule multiple ready operators per pool in one tick (instead of at most 1),
      using conservative per-op CPU/RAM "slices" to improve latency via more concurrency and
      avoid head-of-line blocking.
    - Basic anti-starvation: occasionally allow BATCH through if it has been waiting "too long".

    Notes/limits:
    - No preemption (insufficient visibility into currently-running containers in the provided API).
    - No OOM-driven adaptive resizing (ExecutionResult does not provide pipeline_id in the given API).
    """
    # Per-priority FIFO queues of pipelines
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Light anti-starvation accounting
    s.tick = 0
    s.enqueued_tick = {}  # pipeline_id -> tick when first seen

    # Track failed-op counts to avoid infinite retry loops
    s.prev_failed_ops = {}  # pipeline_id -> last observed FAILED count
    s.retry_budget = {}     # pipeline_id -> remaining retry "events" allowed
    s.max_retry_events = 3  # small, conservative


def _prio_order():
    # Highest to lowest
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _target_slice(pool, priority):
    """
    Return (cpu, ram) target slice for one operator, based on priority.
    Kept intentionally simple and conservative.
    """
    # Fractions tuned to: give interactive more, but still allow concurrency.
    if priority == Priority.QUERY:
        cpu_frac, ram_frac = 0.60, 0.60
    elif priority == Priority.INTERACTIVE:
        cpu_frac, ram_frac = 0.45, 0.45
    else:  # Priority.BATCH_PIPELINE
        cpu_frac, ram_frac = 0.25, 0.25

    cpu = max(1, int(pool.max_cpu_pool * cpu_frac))
    ram = max(1, int(pool.max_ram_pool * ram_frac))
    return cpu, ram


def _pipeline_is_done_or_give_up(s, pipeline):
    """
    Decide whether to drop a pipeline from queues.
    - Drop on success.
    - Drop if exceeded retry budget (based on observed FAILED operator counts).
    """
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True

    failed_now = status.state_counts.get(OperatorState.FAILED, 0)
    prev_failed = s.prev_failed_ops.get(pipeline.pipeline_id, 0)

    # If new failures appeared since last time we checked, consume retry budget.
    if failed_now > prev_failed:
        s.prev_failed_ops[pipeline.pipeline_id] = failed_now
        if pipeline.pipeline_id not in s.retry_budget:
            s.retry_budget[pipeline.pipeline_id] = s.max_retry_events
        s.retry_budget[pipeline.pipeline_id] -= (failed_now - prev_failed)

    # If we've burned all retry events, stop trying this pipeline.
    if pipeline.pipeline_id in s.retry_budget and s.retry_budget[pipeline.pipeline_id] < 0:
        return True

    return False


def _dequeue_next_pipeline(s, allow_batch):
    """
    Pick the next pipeline to consider.
    - Always prefer QUERY then INTERACTIVE.
    - BATCH only if allow_batch=True.
    """
    for pr in _prio_order():
        if pr == Priority.BATCH_PIPELINE and not allow_batch:
            continue
        q = s.wait_q[pr]
        while q:
            p = q.pop(0)
            if _pipeline_is_done_or_give_up(s, p):
                continue
            return p
    return None


@register_scheduler(key="scheduler_low_008")
def scheduler_low_008(s, results, pipelines):
    """
    Priority-aware, capacity-filling scheduler.

    Policy sketch:
    1) Enqueue new pipelines into per-priority FIFO queues.
    2) For each pool, repeatedly:
       - Choose next eligible pipeline (prioritize QUERY/INTERACTIVE; allow occasional BATCH).
       - Pick at most one ready operator from that pipeline.
       - Allocate a per-op CPU/RAM slice (priority-dependent), not the entire pool.
       - Continue until pool resources are insufficient or no eligible work remains.
    """
    # Enqueue new pipelines
    for p in pipelines:
        pr = p.priority
        if pr not in s.wait_q:
            # Unknown priority class; treat as lowest
            pr = Priority.BATCH_PIPELINE
        s.wait_q[pr].append(p)
        s.enqueued_tick.setdefault(p.pipeline_id, s.tick)
        s.prev_failed_ops.setdefault(p.pipeline_id, 0)

    # Early exit when no new info and no new work
    if not pipelines and not results:
        return [], []

    s.tick += 1
    suspensions = []
    assignments = []

    # Anti-starvation: allow a batch pick if the oldest batch has waited long enough
    starvation_ticks = 25
    def batch_starved():
        q = s.wait_q[Priority.BATCH_PIPELINE]
        if not q:
            return False
        # Oldest batch at head (FIFO). If it waited long enough, let one through.
        oldest = q[0]
        waited = s.tick - s.enqueued_tick.get(oldest.pipeline_id, s.tick)
        return waited >= starvation_ticks

    # Scheduling per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Bound per-tick work to avoid pathological long loops in huge backlogs
        max_assignments_this_pool = 32
        made = 0

        # Keep a local "deferred" list to requeue pipelines we looked at but couldn't run now
        deferred = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}

        while made < max_assignments_this_pool and avail_cpu > 0 and avail_ram > 0:
            allow_batch = batch_starved() or (avail_cpu > pool.max_cpu_pool * 0.5 and avail_ram > pool.max_ram_pool * 0.5)

            pipeline = _dequeue_next_pipeline(s, allow_batch=allow_batch)
            if pipeline is None:
                break

            status = pipeline.runtime_status()

            # Pick exactly one ready operator to keep things simple and to spread starts across pipelines
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable right now; requeue to its priority (but behind others we haven't examined)
                pr = pipeline.priority if pipeline.priority in deferred else Priority.BATCH_PIPELINE
                deferred[pr].append(pipeline)
                continue

            pr = pipeline.priority if pipeline.priority in s.wait_q else Priority.BATCH_PIPELINE
            tgt_cpu, tgt_ram = _target_slice(pool, pr)

            # Clamp to available
            cpu = min(avail_cpu, tgt_cpu)
            ram = min(avail_ram, tgt_ram)

            # If we cannot give at least 1 unit each, stop for this pool
            if cpu <= 0 or ram <= 0:
                # Put pipeline back to be considered later
                deferred[pr].append(pipeline)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            avail_cpu -= cpu
            avail_ram -= ram
            made += 1

            # Requeue pipeline to allow round-robin across pipelines of same priority
            deferred[pr].append(pipeline)

        # Requeue deferred pipelines preserving relative order: put them back at the end
        for pr in _prio_order():
            s.wait_q[pr].extend(deferred.get(pr, []))

    return suspensions, assignments
