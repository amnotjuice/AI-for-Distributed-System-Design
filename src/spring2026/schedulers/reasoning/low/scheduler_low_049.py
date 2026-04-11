# policy_key: scheduler_low_049
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.035853
# generation_seconds: 41.06
# generated_at: 2026-03-14T02:40:03.208751
@register_scheduler_init(key="scheduler_low_049")
def scheduler_low_049_init(s):
    """Priority-aware FIFO with simple OOM-driven RAM backoff.

    Improvements over naive FIFO:
      - Maintains separate queues per priority and schedules higher priority first.
      - Avoids giving all resources to batch when higher priority work is waiting.
      - On OOM failure, increases the next RAM request for that specific operator.
    """
    # Per-priority waiting queues (FIFO within a priority).
    s.waiting_query = []
    s.waiting_interactive = []
    s.waiting_batch = []

    # Per-operator RAM hint that is increased on OOM.
    # Keyed by (pipeline_id, str(op)) to avoid relying on undocumented op identifiers.
    s.op_ram_hint = {}

    # Simple policy knobs (kept conservative / incremental).
    s.oom_ram_backoff_factor = 2.0
    s.max_backoff_steps = 6  # prevents runaway RAM growth loops


def _pqueue_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.waiting_query
    if prio == Priority.INTERACTIVE:
        return s.waiting_interactive
    return s.waiting_batch


def _all_queues(s):
    return [s.waiting_query, s.waiting_interactive, s.waiting_batch]


def _has_high_priority_waiting(s):
    return bool(s.waiting_query) or bool(s.waiting_interactive)


def _op_key(pipeline_id, op):
    return (pipeline_id, str(op))


def _update_hints_from_results(s, results):
    # Learn only from failures (currently focusing on OOM-like failures).
    for r in results:
        if not r.failed():
            continue

        err = (r.error or "").lower()
        is_oom = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "exceed" in err)

        if not is_oom:
            continue

        # Apply backoff to each failed op in the result.
        for op in (r.ops or []):
            key = _op_key(getattr(r, "pipeline_id", None), op)
            # pipeline_id isn't on ExecutionResult per the spec; fall back to op-scoped key if missing.
            if key[0] is None:
                key = ("<unknown_pipeline>", str(op))

            prev = s.op_ram_hint.get(key, r.ram if getattr(r, "ram", None) else 0.0)
            # If prev is 0 (unknown), start from the last attempted RAM if available, else a small base.
            base = prev if prev and prev > 0 else (r.ram if getattr(r, "ram", None) else 1.0)
            # Track backoff steps to avoid infinite doubling.
            steps_key = ("__steps__",) + key
            steps = s.op_ram_hint.get(steps_key, 0)
            if steps < s.max_backoff_steps:
                s.op_ram_hint[key] = base * s.oom_ram_backoff_factor
                s.op_ram_hint[steps_key] = steps + 1


def _pipeline_done_or_failed(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If any op is FAILED, we treat pipeline as failed and do not retry (same as baseline).
    return status.state_counts.get(OperatorState.FAILED, 0) > 0


def _pop_next_runnable_pipeline(s):
    # Priority order: QUERY > INTERACTIVE > BATCH
    for q in (s.waiting_query, s.waiting_interactive, s.waiting_batch):
        while q:
            p = q.pop(0)
            if _pipeline_done_or_failed(p):
                continue
            return p
    return None


def _requeue_pipeline(s, pipeline):
    _pqueue_for_priority(s, pipeline.priority).append(pipeline)


def _cpu_cap_for_priority(pool, prio):
    # Small, incremental improvement: cap batch so high-priority latency doesn't suffer as much.
    # If no high-priority waiting, let batch use more.
    if prio == Priority.QUERY:
        return pool.max_cpu_pool
    if prio == Priority.INTERACTIVE:
        return max(1.0, pool.max_cpu_pool * 0.60)
    # BATCH
    return max(1.0, pool.max_cpu_pool * (0.35 if _has_high_priority_waiting(pool.sched_ref) else 1.0))


def _ram_cap_for_priority(pool, prio):
    if prio == Priority.QUERY:
        return pool.max_ram_pool * 0.70
    if prio == Priority.INTERACTIVE:
        return pool.max_ram_pool * 0.55
    return pool.max_ram_pool * (0.40 if _has_high_priority_waiting(pool.sched_ref) else 0.80)


@register_scheduler(key="scheduler_low_049")
def scheduler_low_049(s, results, pipelines):
    """
    Priority-aware scheduler:
      1) Enqueue incoming pipelines into per-priority FIFO queues.
      2) Update RAM hints on OOM failures (per operator).
      3) For each pool, schedule at most one ready operator from the highest priority runnable pipeline.
         - Allocate CPU/RAM with priority-based caps.
         - If an OOM hint exists for the operator, use it as the RAM request (bounded by pool and availability).
      4) No preemption (kept incremental; interface for enumerating running containers is not assumed).
    """
    # Enqueue new arrivals.
    for p in pipelines:
        _pqueue_for_priority(s, p.priority).append(p)

    # Early exit if nothing new to act on.
    if not pipelines and not results:
        return [], []

    # Learn from OOM results.
    _update_hints_from_results(s, results)

    suspensions = []
    assignments = []

    # Attach a back-reference used by helper caps without global state.
    # (Safe: we only store a transient attribute on pool objects.)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pool.sched_ref = s

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pull the next runnable pipeline by priority.
        pipeline = _pop_next_runnable_pipeline(s)
        if pipeline is None:
            continue

        status = pipeline.runtime_status()
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not runnable yet; requeue and move on.
            _requeue_pipeline(s, pipeline)
            continue

        op = op_list[0]

        # Decide resource request based on priority and learned RAM hint.
        prio = pipeline.priority
        cpu_cap = _cpu_cap_for_priority(pool, prio)
        ram_cap = _ram_cap_for_priority(pool, prio)

        cpu_req = min(avail_cpu, cpu_cap)
        if cpu_req <= 0:
            _requeue_pipeline(s, pipeline)
            continue

        # Operator-specific RAM hint (from previous OOM), otherwise a conservative default.
        key = _op_key(pipeline.pipeline_id, op)
        hinted_ram = s.op_ram_hint.get(key, None)
        default_ram = min(avail_ram, ram_cap)

        ram_req = hinted_ram if (hinted_ram is not None and hinted_ram > 0) else default_ram
        ram_req = min(ram_req, avail_ram, pool.max_ram_pool)

        # If we can't meet the hinted RAM, don't schedule it now; requeue to avoid known OOM.
        if hinted_ram is not None and hinted_ram > avail_ram:
            _requeue_pipeline(s, pipeline)
            continue

        # Ensure minimal positive RAM request.
        if ram_req <= 0:
            _requeue_pipeline(s, pipeline)
            continue

        assignments.append(
            Assignment(
                ops=op_list,
                cpu=cpu_req,
                ram=ram_req,
                priority=prio,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
        )

        # Requeue pipeline to schedule subsequent ops in later ticks.
        _requeue_pipeline(s, pipeline)

    # Cleanup transient pool attribute if it exists.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if hasattr(pool, "sched_ref"):
            delattr(pool, "sched_ref")

    return suspensions, assignments
