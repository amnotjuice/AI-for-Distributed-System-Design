# policy_key: scheduler_medium_014
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.116323
# generation_seconds: 84.61
# generated_at: 2026-04-09T22:47:40.106990
@register_scheduler_init(key="scheduler_medium_014")
def scheduler_medium_014_init(s):
    """
    Priority-aware, failure-averse scheduler with conservative default sizing and OOM-driven RAM learning.

    Core ideas:
      - Strict priority ordering for QUERY > INTERACTIVE > BATCH (with batch aging to avoid starvation)
      - Right-sized CPU allocations by priority (avoid giving 100% of a pool to one op by default)
      - Per-(pipeline, op) RAM estimates that grow aggressively on OOM failures to drive completion rate up
      - Round-robin within each priority class for fairness and to reduce head-of-line blocking
      - No intentional dropping of failed pipelines; FAILED operators are treated as retryable (up to a cap)
    """
    s.tick = 0

    # Per-priority FIFO queues (pipelines are re-enqueued after each assignment).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Pipeline metadata for aging/fairness.
    s.pipeline_meta = {}  # pipeline_id -> {"enq_tick": int, "priority": Priority}

    # Round-robin cursor per priority queue.
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Map operator identity to its pipeline for back-referencing failures (results may omit pipeline_id).
    s.op_to_pipeline = {}  # id(op) -> pipeline_id

    # Learned per-(pipeline, op) RAM targets (in same units as pool RAM).
    s.op_ram_target = {}  # (pipeline_id, op_uid) -> ram_target

    # Retry tracking per operator.
    s.op_retries = {}  # (pipeline_id, op_uid) -> int

    # Tunables (kept conservative; strongly prioritize completion to avoid 720s penalties).
    s.max_retries = 6
    s.ram_growth_oom = 1.85
    s.ram_growth_other = 1.35

    # Default sizing fractions (of pool maxima) when no history exists.
    s.default_ram_frac = {
        Priority.QUERY: 0.30,
        Priority.INTERACTIVE: 0.25,
        Priority.BATCH_PIPELINE: 0.22,
    }
    s.default_cpu_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.30,
    }

    # Soft reservation for responsiveness: only applies when higher-priority backlog exists.
    s.reserve_cpu_frac = 0.18
    s.reserve_ram_frac = 0.18

    # Batch aging: after waiting long enough, treat as interactive for scheduling order.
    s.batch_aging_ticks = 35


def _op_uid(op):
    """Best-effort stable operator identifier."""
    uid = getattr(op, "op_id", None)
    if uid is None:
        uid = getattr(op, "operator_id", None)
    if uid is None:
        uid = getattr(op, "name", None)
    if uid is None:
        # Fallback: object identity (stable as long as the same object instance is used across ticks).
        uid = id(op)
    return uid


def _is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("killed process" in e and "memory" in e) or ("cuda out of memory" in e)


def _queue_has_backlog(s, prio):
    q = s.queues.get(prio, [])
    return len(q) > 0


def _any_high_prio_backlog(s):
    return _queue_has_backlog(s, Priority.QUERY) or _queue_has_backlog(s, Priority.INTERACTIVE)


def _effective_priority(s, pipeline):
    """Batch aging to avoid starvation under persistent high-priority load."""
    prio = pipeline.priority
    if prio != Priority.BATCH_PIPELINE:
        return prio
    meta = s.pipeline_meta.get(pipeline.pipeline_id)
    if not meta:
        return prio
    waited = s.tick - meta.get("enq_tick", s.tick)
    if waited >= s.batch_aging_ticks:
        return Priority.INTERACTIVE
    return prio


def _cleanup_completed(s):
    """Remove completed pipelines from queues (keep failed/incomplete pipelines for retries)."""
    for prio in list(s.queues.keys()):
        new_q = []
        for p in s.queues[prio]:
            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
            except Exception:
                # If status isn't accessible for some reason, err on the side of keeping it.
                pass
            new_q.append(p)
        s.queues[prio] = new_q


def _choose_pipeline_rr(s, prio):
    """Round-robin pick from a priority queue; returns None if empty."""
    q = s.queues.get(prio, [])
    if not q:
        return None
    i = s.rr_cursor[prio] % len(q)
    s.rr_cursor[prio] = (i + 1) % max(1, len(q))
    return q.pop(i)


def _requeue_pipeline(s, pipeline):
    """Re-enqueue pipeline into its base priority queue (aging handled at selection time)."""
    s.queues[pipeline.priority].append(pipeline)


@register_scheduler(key="scheduler_medium_014")
def scheduler_medium_014(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue new pipelines
      2) Update RAM targets on failures (especially OOM)
      3) Clean completed pipelines from queues
      4) For each pool, pack assignments in priority order with soft reservations for responsiveness
    """
    s.tick += 1

    # 1) Enqueue new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.pipeline_meta:
            s.pipeline_meta[pid] = {"enq_tick": s.tick, "priority": p.priority}
        s.queues[p.priority].append(p)

    # 2) Update learning state from execution results
    for r in results:
        try:
            failed = r.failed()
        except Exception:
            failed = False

        if not failed:
            continue

        # Determine whether this looks like an OOM.
        err = getattr(r, "error", None)
        is_oom = _is_oom_error(err)

        # Try to determine pool max RAM for capping.
        pool_id = getattr(r, "pool_id", None)
        pool_max_ram = None
        if pool_id is not None:
            try:
                pool_max_ram = s.executor.pools[pool_id].max_ram_pool
            except Exception:
                pool_max_ram = None

        # Identify pipeline id if provided; otherwise infer via op->pipeline mapping.
        r_pid = getattr(r, "pipeline_id", None)

        for op in getattr(r, "ops", []) or []:
            pid = r_pid
            if pid is None:
                pid = s.op_to_pipeline.get(id(op))

            if pid is None:
                continue

            op_key = (pid, _op_uid(op))
            s.op_retries[op_key] = s.op_retries.get(op_key, 0) + 1

            # Grow RAM target based on the last attempted RAM if available.
            last_ram = getattr(r, "ram", None)
            if last_ram is None:
                continue

            growth = s.ram_growth_oom if is_oom else s.ram_growth_other
            new_target = int(max(int(last_ram) + 1, int(last_ram * growth)))

            # Cap to pool max RAM if known.
            if pool_max_ram is not None:
                try:
                    new_target = min(int(pool_max_ram), new_target)
                except Exception:
                    pass

            prev = s.op_ram_target.get(op_key, 0)
            if new_target > prev:
                s.op_ram_target[op_key] = new_target

    # 3) Remove completed pipelines from queues (keep retrying failed/incomplete).
    _cleanup_completed(s)

    # Early exit only if nothing changed and no work is queued.
    if not pipelines and not results:
        any_queued = any(len(q) for q in s.queues.values())
        if not any_queued:
            return [], []

    suspensions = []  # We avoid preemption here to reduce churn/wasted work without reliable visibility.
    assignments = []

    # Helper: try to find a ready op from a pipeline; return (op_list, op) or (None, None)
    def _next_ready_op(pipeline):
        try:
            st = pipeline.runtime_status()
        except Exception:
            return None, None

        # If the pipeline already succeeded, don't schedule it.
        try:
            if st.is_pipeline_successful():
                return None, None
        except Exception:
            pass

        # One-op-at-a-time scheduling for fairness and predictable resource packing.
        try:
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        except Exception:
            ops = []
        if not ops:
            return None, None
        return ops, ops[0]

    # 4) Pack each pool with assignments
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        try:
            avail_cpu = int(pool.avail_cpu_pool)
            avail_ram = int(pool.avail_ram_pool)
            max_cpu = int(pool.max_cpu_pool)
            max_ram = int(pool.max_ram_pool)
        except Exception:
            # If pool accounting isn't accessible, skip scheduling on it.
            continue

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Reserve some headroom only if there is higher-priority backlog (to protect tail latency).
        reserve_cpu = max(0, int(max_cpu * s.reserve_cpu_frac)) if _any_high_prio_backlog(s) else 0
        reserve_ram = max(0, int(max_ram * s.reserve_ram_frac)) if _any_high_prio_backlog(s) else 0

        # Limit loop iterations to avoid spinning on non-ready pipelines.
        safety_iters = 0
        while avail_cpu > 0 and avail_ram > 0 and safety_iters < 200:
            safety_iters += 1

            # Priority selection (with batch aging).
            # We implement an "aged batch" lane by checking the head of batch queue opportunistically.
            picked_pipeline = None
            picked_base_prio = None

            # First: QUERY
            if s.queues[Priority.QUERY]:
                picked_pipeline = _choose_pipeline_rr(s, Priority.QUERY)
                picked_base_prio = Priority.QUERY
            # Second: INTERACTIVE
            elif s.queues[Priority.INTERACTIVE]:
                picked_pipeline = _choose_pipeline_rr(s, Priority.INTERACTIVE)
                picked_base_prio = Priority.INTERACTIVE
            # Third: aged BATCH (treated like interactive for ordering)
            else:
                # Peek/pop batch RR and see if it's aged; if not aged, we still may run it as batch.
                if s.queues[Priority.BATCH_PIPELINE]:
                    picked_pipeline = _choose_pipeline_rr(s, Priority.BATCH_PIPELINE)
                    picked_base_prio = Priority.BATCH_PIPELINE

            if picked_pipeline is None:
                break

            eff_prio = _effective_priority(s, picked_pipeline)

            # If we're about to schedule a batch op while higher-priority backlog exists,
            # keep soft reserve to remain responsive for future query/interactive arrivals.
            if picked_pipeline.priority == Priority.BATCH_PIPELINE and _any_high_prio_backlog(s):
                if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                    # Not enough non-reserved capacity: put it back and stop filling this pool.
                    _requeue_pipeline(s, picked_pipeline)
                    break

            op_list, op = _next_ready_op(picked_pipeline)
            if not op_list:
                # Not ready yet (e.g., waiting on parents); requeue and continue.
                _requeue_pipeline(s, picked_pipeline)
                continue

            # Resource sizing
            pid = picked_pipeline.pipeline_id
            op_key = (pid, _op_uid(op))
            retries = s.op_retries.get(op_key, 0)

            # Default targets as a fraction of pool maxima.
            # If batch was aged, treat it closer to interactive for speed.
            sizing_prio = eff_prio
            if sizing_prio not in s.default_cpu_frac:
                sizing_prio = picked_pipeline.priority

            cpu_target = max(1, int(max_cpu * s.default_cpu_frac.get(sizing_prio, 0.35)))
            ram_target = int(max_ram * s.default_ram_frac.get(sizing_prio, 0.25))
            ram_target = max(1, ram_target)

            # Use learned RAM if present (dominant goal: avoid OOM -> avoid 720s penalty).
            learned_ram = s.op_ram_target.get(op_key)
            if learned_ram is not None:
                ram_target = max(ram_target, int(learned_ram))

            # If we've retried multiple times, be more aggressive (prefer completion over utilization).
            if retries >= 2:
                cpu_target = max(cpu_target, int(max_cpu * 0.60))
                ram_target = max(ram_target, int(max_ram * 0.40))
            if retries >= 4:
                cpu_target = max(cpu_target, int(max_cpu * 0.80))
                ram_target = max(ram_target, int(max_ram * 0.55))

            # If exceeding retry cap, try one "last resort" big allocation if possible.
            if retries > s.max_retries:
                cpu_target = max_cpu
                ram_target = max_ram

            # Clamp to what's available (respecting reserves only for batch under high-pri backlog).
            effective_avail_cpu = avail_cpu
            effective_avail_ram = avail_ram
            if picked_pipeline.priority == Priority.BATCH_PIPELINE and _any_high_prio_backlog(s):
                effective_avail_cpu = max(0, avail_cpu - reserve_cpu)
                effective_avail_ram = max(0, avail_ram - reserve_ram)

            cpu_target = min(cpu_target, effective_avail_cpu)
            ram_target = min(ram_target, effective_avail_ram)

            # If we can't allocate minimum viable resources, requeue and stop packing this pool.
            if cpu_target <= 0 or ram_target <= 0:
                _requeue_pipeline(s, picked_pipeline)
                break

            # Create assignment and account locally.
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_target,
                    ram=ram_target,
                    priority=picked_pipeline.priority,  # keep original priority for metrics
                    pool_id=pool_id,
                    pipeline_id=picked_pipeline.pipeline_id,
                )
            )
            avail_cpu -= cpu_target
            avail_ram -= ram_target

            # Record op->pipeline mapping for future failure attribution.
            s.op_to_pipeline[id(op)] = picked_pipeline.pipeline_id

            # Requeue pipeline (lets other pipelines at same priority make progress too).
            _requeue_pipeline(s, picked_pipeline)

    return suspensions, assignments
