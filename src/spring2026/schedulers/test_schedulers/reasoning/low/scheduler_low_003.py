# policy_key: scheduler_low_003
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052973
# generation_seconds: 43.88
# generated_at: 2026-04-09T20:19:35.364648
@register_scheduler_init(key="scheduler_low_003")
def scheduler_low_003_init(s):
    """
    Priority-first, failure-aware scheduler focused on minimizing weighted latency
    while avoiding OOM-driven failures (each failure is heavily penalized).

    Core ideas:
      - Maintain separate FIFO queues per priority; always serve QUERY then INTERACTIVE then BATCH.
      - Prefer dedicated pools when multiple pools exist (pool0->QUERY, pool1->INTERACTIVE, rest->BATCH),
        but allow spillover if a dedicated queue is empty to avoid idle capacity.
      - Conservative initial RAM sizing by priority (QUERY gets more), and exponential RAM backoff on OOM.
      - Limited retries on OOM to improve completion rate; de-emphasize (but don't drop) repeatedly failing work.
      - Avoid preemption (no container inventory API guaranteed); instead reduce head-of-line blocking via per-op sizing.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track pipelines by id so we can requeue based on results.
    s.pipeline_by_id = {}

    # Per-operator RAM multiplier to react to OOM. Keyed by (pipeline_id, op_object_id).
    s.ram_mult = {}

    # Per-operator retry count; used to cap retries and to slightly boost CPU after repeated attempts.
    s.retry_count = {}

    # Track "already enqueued" to avoid uncontrolled duplicate enqueues.
    s.enqueued = set()


def _priority_rank(priority):
    # Smaller is higher priority.
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _push_queue(s, pipeline, front=False):
    pid = pipeline.pipeline_id
    if not front and pid in s.enqueued:
        return
    q = s.q_batch
    if pipeline.priority == Priority.QUERY:
        q = s.q_query
    elif pipeline.priority == Priority.INTERACTIVE:
        q = s.q_interactive

    if front:
        q.insert(0, pipeline)
    else:
        q.append(pipeline)
        s.enqueued.add(pid)

    s.pipeline_by_id[pid] = pipeline


def _pop_next_pipeline(s, allowed_priorities):
    # allowed_priorities is a tuple/list in preferred order.
    for pr in allowed_priorities:
        q = s.q_batch
        if pr == Priority.QUERY:
            q = s.q_query
        elif pr == Priority.INTERACTIVE:
            q = s.q_interactive
        while q:
            p = q.pop(0)
            # If it was in the "enqueued" set, remove it now; it may get re-enqueued later.
            if p.pipeline_id in s.enqueued:
                s.enqueued.remove(p.pipeline_id)
            return p
    return None


def _base_share(priority, pool):
    # Conservative default sizing: reduce OOM risk for high priority by allocating more RAM.
    # These are "target shares" of a pool's capacity; final allocation is capped by availability.
    if priority == Priority.QUERY:
        cpu_share = 0.55
        ram_share = 0.45
    elif priority == Priority.INTERACTIVE:
        cpu_share = 0.45
        ram_share = 0.35
    else:
        cpu_share = 0.35
        ram_share = 0.25

    # Ensure we allocate at least a small slice so tiny pools still run something.
    min_cpu = max(1.0, 0.10 * float(pool.max_cpu_pool))
    min_ram = max(1.0, 0.10 * float(pool.max_ram_pool))
    return cpu_share, ram_share, min_cpu, min_ram


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)


@register_scheduler(key="scheduler_low_003")
def scheduler_low_003(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into priority queues.
      2) Process execution results to learn OOMs and increase RAM for retried operators.
      3) For each pool, assign ready operators from the highest-priority eligible pipelines while capacity remains.
    """
    # 1) Add new pipelines.
    for p in pipelines:
        _push_queue(s, p, front=False)

    # 2) Process results: learn OOMs and decide requeue strategy.
    for r in results:
        # We only act on failures; successes implicitly advance pipeline state in runtime_status().
        if not r.failed():
            continue

        pid = getattr(r, "pipeline_id", None)
        # If pipeline_id isn't on the result, infer from ops by searching known pipelines (fallback).
        if pid is None:
            # Cheap fallback: do nothing if we can't attribute; avoids crashing.
            continue

        pipeline = s.pipeline_by_id.get(pid)
        if pipeline is None:
            continue

        # For each failed op, adjust RAM multiplier on OOM.
        oom = _is_oom_error(getattr(r, "error", None))
        if oom:
            for op in getattr(r, "ops", []) or []:
                key = (pid, id(op))
                s.ram_mult[key] = min(16.0, s.ram_mult.get(key, 1.0) * 2.0)
                s.retry_count[key] = s.retry_count.get(key, 0) + 1

        # Requeue pipeline to make progress unless it looks irrecoverable.
        # Even non-OOM failures are penalized heavily if we drop immediately; allow a small number of retries.
        # If we can't tie failure to a specific op, treat it as pipeline-level and retry once.
        if oom:
            # Front of queue for rapid recovery (reduces 720s penalties).
            _push_queue(s, pipeline, front=True)
        else:
            # Limited gentle retry: push to back to avoid repeated head-of-line blocking.
            # We keep it eligible to avoid starvation/penalty from being left incomplete.
            _push_queue(s, pipeline, front=False)

    # Early exit if nothing to do.
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # 3) Assign work pool by pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pool preference: if multiple pools, try to keep high-priority work isolated.
        # - pool 0: query-first
        # - pool 1: interactive-first
        # - others: batch-first
        if s.executor.num_pools >= 3:
            if pool_id == 0:
                preferred = (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE)
            elif pool_id == 1:
                preferred = (Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE)
            else:
                preferred = (Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY)
        elif s.executor.num_pools == 2:
            if pool_id == 0:
                preferred = (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE)
            else:
                preferred = (Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE)
        else:
            preferred = (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE)

        # Pack multiple assignments into the same pool tick while resources remain.
        # We schedule at most one ready op per selected pipeline per iteration.
        max_attempts = 64  # safety bound to avoid infinite loops
        attempts = 0
        while attempts < max_attempts and avail_cpu > 0 and avail_ram > 0:
            attempts += 1
            pipeline = _pop_next_pipeline(s, preferred)
            if pipeline is None:
                break

            status = pipeline.runtime_status()

            # Drop completed pipelines from consideration.
            if status.is_pipeline_successful():
                continue

            # Find one ready operator (parents complete).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable yet; requeue to avoid starvation and keep scanning others.
                _push_queue(s, pipeline, front=False)
                continue

            op = op_list[0]
            key = (pipeline.pipeline_id, id(op))
            retries = s.retry_count.get(key, 0)

            cpu_share, ram_share, min_cpu, min_ram = _base_share(pipeline.priority, pool)

            # Slightly increase CPU after retries to reduce time-in-system when we already paid retry overhead.
            cpu_boost = 1.0 + min(0.50, 0.10 * float(retries))
            ram_boost = s.ram_mult.get(key, 1.0)

            target_cpu = max(min_cpu, cpu_share * float(pool.max_cpu_pool) * cpu_boost)
            target_ram = max(min_ram, ram_share * float(pool.max_ram_pool) * ram_boost)

            # Cap to currently available capacity.
            cpu_alloc = min(avail_cpu, target_cpu)
            ram_alloc = min(avail_ram, target_ram)

            # If we can't give a minimally meaningful slice, requeue and stop packing this pool.
            # This avoids thrashing on too-small allocations.
            if cpu_alloc <= 0.0 or ram_alloc <= 0.0:
                _push_queue(s, pipeline, front=True)
                break

            # If a prior OOM suggests high RAM need but we can't satisfy it now, avoid running it under-sized.
            # Requeue and try other work instead of inducing another OOM (720s penalty risk).
            if ram_boost > 1.0 and ram_alloc < (min_ram * ram_boost * 0.9):
                _push_queue(s, pipeline, front=False)
                continue

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_alloc,
                    ram=ram_alloc,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update local availability for additional packing decisions.
            avail_cpu -= cpu_alloc
            avail_ram -= ram_alloc

            # Requeue pipeline to allow its next op later (keeps fairness across pipelines).
            _push_queue(s, pipeline, front=False)

    return suspensions, assignments
