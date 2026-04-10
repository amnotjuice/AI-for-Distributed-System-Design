# policy_key: scheduler_medium_013
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.083929
# generation_seconds: 114.99
# generated_at: 2026-03-12T22:12:05.885752
@register_scheduler_init(key="scheduler_medium_013")
def scheduler_medium_013_init(s):
    """
    Priority-aware, OOM-adaptive scheduler.

    Small, obvious improvements over naive FIFO:
    - Separate queues per priority (QUERY > INTERACTIVE > BATCH) to improve latency for high-priority work.
    - Retry OOM-failed operators with increased RAM instead of dropping the whole pipeline immediately.
    - Avoid letting batch consume all headroom when higher-priority work is waiting (light reservation).
    - Pack multiple small assignments per pool per tick (instead of 1) to reduce head-of-line blocking.
    """
    # Per-priority FIFO queues of Pipeline objects
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Track which pipelines are currently enqueued to avoid duplicates
    s.in_queue = set()

    # Per-operator adaptive RAM multiplier (keyed by operator object identity)
    s.ram_multiplier = {}  # op_key -> float
    s.oom_attempts = {}  # op_key -> int

    # Track last failure type per operator; used to decide if we should stop retrying
    s.last_error = {}  # op_key -> str
    s.non_oom_failed_ops = set()  # op_key set

    # Tuning knobs (kept small/simple on purpose)
    s.max_oom_retries = 5
    s.max_ram_multiplier = 16.0  # cap exponential backoff
    s.high_prio_reserve_frac = 0.15  # reserve only when high-priority work is waiting


def _op_key(op):
    # Use object identity so we don't depend on any specific operator id field existing.
    return id(op)


def _is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("out-of-memory" in e) or ("cuda oom" in e)


def _base_resources(priority, pool):
    """
    Pick conservative default container sizes by priority.
    - Query: small but responsive
    - Interactive: moderate
    - Batch: larger chunks for throughput (but still not "take the whole pool")
    """
    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    # CPU: keep modest caps to allow concurrency
    if priority == Priority.QUERY:
        cpu = max(1.0, min(4.0, max_cpu * 0.125))
        ram = max(1.0, max_ram * 0.10)
    elif priority == Priority.INTERACTIVE:
        cpu = max(1.0, min(8.0, max_cpu * 0.25))
        ram = max(1.0, max_ram * 0.15)
    else:  # Priority.BATCH_PIPELINE
        cpu = max(1.0, min(16.0, max_cpu * 0.50))
        ram = max(1.0, max_ram * 0.25)

    return cpu, ram


def _remove_from_queue_membership(s, pipeline):
    pid = pipeline.pipeline_id
    if pid in s.in_queue:
        s.in_queue.remove(pid)


def _enqueue_pipeline(s, pipeline):
    pid = pipeline.pipeline_id
    if pid in s.in_queue:
        return
    s.in_queue.add(pid)
    s.queues.get(pipeline.priority, s.queues[Priority.BATCH_PIPELINE]).append(pipeline)


def _pipeline_should_drop(s, pipeline):
    """
    Drop pipeline only if it has a FAILED op with a known non-OOM error,
    or if it has an op that exceeded max OOM retries.
    """
    status = pipeline.runtime_status()
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    for op in failed_ops:
        k = _op_key(op)
        if k in s.non_oom_failed_ops:
            return True
        if s.oom_attempts.get(k, 0) > s.max_oom_retries:
            return True
    return False


@register_scheduler(key="scheduler_medium_013")
def scheduler_medium_013_scheduler(s, results, pipelines):
    """
    Priority-aware scheduling with:
    - per-priority FIFO queues
    - light headroom reservation for high-priority waiting work
    - OOM-driven RAM backoff per operator (retry FAILED ops with more RAM)
    - multiple assignments per pool per tick (packing)
    """
    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from previous tick results (primarily OOM vs non-OOM)
    for r in results:
        if not r.failed():
            continue

        is_oom = _is_oom_error(getattr(r, "error", None))
        for op in getattr(r, "ops", []) or []:
            k = _op_key(op)
            s.last_error[k] = str(getattr(r, "error", "") or "")
            if is_oom:
                prev = float(s.ram_multiplier.get(k, 1.0))
                s.ram_multiplier[k] = min(s.max_ram_multiplier, max(1.0, prev * 2.0))
                s.oom_attempts[k] = int(s.oom_attempts.get(k, 0)) + 1
            else:
                s.non_oom_failed_ops.add(k)

    # If nothing changed, exit early (saves sim time)
    if not pipelines and not results:
        return [], []

    suspensions = []  # no preemption in this iteration (kept simple/robust)
    assignments = []

    # Determine whether to reserve headroom for high-priority work
    high_waiting = (len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE])) > 0
    reserve_frac = s.high_prio_reserve_frac if high_waiting else 0.0

    priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Try to pack multiple assignments per pool per tick
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < 1.0 or avail_ram <= 0.0:
            continue

        reserve_cpu = float(pool.max_cpu_pool) * reserve_frac
        reserve_ram = float(pool.max_ram_pool) * reserve_frac

        # Hard cap to avoid generating too many assignments in one tick
        max_new = int(min(16, max(1, float(pool.max_cpu_pool))))
        made = 0

        while made < max_new:
            scheduled_one = False

            for prio in priority_order:
                q = s.queues.get(prio, [])
                if not q:
                    continue

                # For batch, enforce reservation when high-priority is waiting
                if prio == Priority.BATCH_PIPELINE and high_waiting:
                    eff_cpu = avail_cpu - reserve_cpu
                    eff_ram = avail_ram - reserve_ram
                else:
                    eff_cpu = avail_cpu
                    eff_ram = avail_ram

                if eff_cpu < 1.0 or eff_ram <= 0.0:
                    continue

                # Scan queue at most once to find a schedulable pipeline
                scan = len(q)
                chosen_pipeline = None
                chosen_ops = None
                chosen_cpu = None
                chosen_ram = None

                for _ in range(scan):
                    pipeline = q.pop(0)
                    status = pipeline.runtime_status()

                    # Drop completed pipelines
                    if status.is_pipeline_successful():
                        _remove_from_queue_membership(s, pipeline)
                        continue

                    # Drop pipelines with clear non-OOM failures or too many OOM retries
                    if _pipeline_should_drop(s, pipeline):
                        _remove_from_queue_membership(s, pipeline)
                        continue

                    # Find one assignable op whose parents are complete
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        # Not ready yet; keep it queued
                        q.append(pipeline)
                        continue

                    # Size this operator
                    base_cpu, base_ram = _base_resources(prio, pool)
                    k = _op_key(op_list[0])
                    mult = float(s.ram_multiplier.get(k, 1.0))
                    ram_target = min(float(pool.max_ram_pool), base_ram * mult)

                    # Require enough RAM to hit our target (to reduce OOM churn)
                    if eff_ram < ram_target or eff_cpu < 1.0:
                        # Can't place right now; keep queued and try others
                        q.append(pipeline)
                        continue

                    # CPU can be flexed down to available (>= 1)
                    cpu_assign = max(1.0, min(base_cpu, eff_cpu))
                    ram_assign = ram_target

                    chosen_pipeline = pipeline
                    chosen_ops = op_list
                    chosen_cpu = cpu_assign
                    chosen_ram = ram_assign
                    break

                if chosen_pipeline is None:
                    continue

                assignments.append(
                    Assignment(
                        ops=chosen_ops,
                        cpu=chosen_cpu,
                        ram=chosen_ram,
                        priority=chosen_pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=chosen_pipeline.pipeline_id,
                    )
                )

                # Requeue pipeline so it can make progress on later ops in future ticks
                q.append(chosen_pipeline)

                # Update available resources
                avail_cpu -= float(chosen_cpu)
                avail_ram -= float(chosen_ram)

                made += 1
                scheduled_one = True
                break  # restart from highest priority

            if not scheduled_one:
                break

    return suspensions, assignments
