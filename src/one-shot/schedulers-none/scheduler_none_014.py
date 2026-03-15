# policy_key: scheduler_none_014
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.037841
# generation_seconds: 33.69
# generated_at: 2026-03-12T21:30:36.971973
@register_scheduler_init(key="scheduler_none_014")
def scheduler_none_014_init(s):
    """Priority-aware FIFO with conservative resource slicing + simple OOM backoff.

    Incremental improvements over naive FIFO:
      1) Maintain per-priority queues (QUERY/INTERACTIVE ahead of BATCH).
      2) Avoid "give everything to one op": cap per-assignment CPU/RAM to reduce head-of-line blocking.
      3) On OOM, retry the pipeline later with increased RAM guess (per-pipeline backoff).
      4) Keep batch moving via small guaranteed slices when resources are tight.

    Notes/assumptions:
      - We do not preempt (Suspend) because we don't have a reliable way to discover running containers
        from the public interface shown in the template.
      - We schedule at most one operator per pool per tick (like the example) to keep behavior stable.
    """
    # Per-priority queues of pipelines
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-pipeline RAM guess multiplier (starts at 1.0; increases on OOM)
    s.ram_mult = {}  # pipeline_id -> float

    # Basic knobs (kept simple; tune in sim)
    s.max_ram_mult = 8.0
    s.ram_bump_factor = 1.75  # on OOM, multiply RAM guess by this
    s.min_cpu_slice = 1.0     # don't schedule with < 1 vCPU if possible
    s.min_ram_slice = 0.5     # don't schedule with < 0.5 GB if possible (units depend on sim)

    # Per-priority caps as a fraction of pool capacity
    s.cap_cpu_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.35,
    }
    s.cap_ram_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.40,
    }

    # If pool is empty-ish, allow higher burst for top priorities
    s.burst_cpu_frac = {
        Priority.QUERY: 0.90,
        Priority.INTERACTIVE: 1.00,
        Priority.BATCH_PIPELINE: 0.50,
    }
    s.burst_ram_frac = {
        Priority.QUERY: 0.90,
        Priority.INTERACTIVE: 1.00,
        Priority.BATCH_PIPELINE: 0.55,
    }


def _priority_queue(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _iter_queues_in_order(s):
    # Highest priority first
    return [s.q_query, s.q_interactive, s.q_batch]


def _pipeline_done_or_dead(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # Naive policy drops any pipeline with failures; we keep that behavior,
    # but we will requeue on OOM by not marking operator FAILED (sim does).
    # If the simulator marks OOM as FAILED operator, we will still drop unless we detect OOM
    # and choose to keep it. This is handled in result processing by resetting expectations
    # (we cannot mutate operator states here), so we instead keep pipelines regardless of FAILED
    # and rely on ASSIGNABLE_STATES to include FAILED per prompt. So: never "dead" here.
    return False


def _choose_next_pipeline(s):
    # Pop from the highest-priority non-empty queue
    for q in _iter_queues_in_order(s):
        if q:
            return q.pop(0)
    return None


def _requeue_pipeline(s, pipeline):
    _priority_queue(s, pipeline.priority).append(pipeline)


def _clamp(x, lo, hi):
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


@register_scheduler(key="scheduler_none_014")
def scheduler_none_014(s, results, pipelines):
    """
    Priority-aware single-op-per-pool scheduler with capped allocations and OOM backoff.

    Decision outline per tick:
      - Enqueue new pipelines into per-priority FIFO queues.
      - Process results: on OOM failures, increase per-pipeline RAM multiplier.
      - For each pool:
          - Pick next runnable op from highest-priority pipeline available.
          - Allocate a capped CPU/RAM slice (priority-dependent), with optional burst if pool has headroom.
          - Apply pipeline RAM multiplier to reduce repeated OOMs.
    """
    # Enqueue new pipelines
    for p in pipelines:
        _priority_queue(s, p.priority).append(p)
        if p.pipeline_id not in s.ram_mult:
            s.ram_mult[p.pipeline_id] = 1.0

    # Process results for feedback (OOM -> increase RAM multiplier)
    for r in results:
        if not r.failed():
            continue
        # Heuristic: treat any error containing "oom" as OOM.
        # If error is None or non-string, ignore.
        err = r.error
        if isinstance(err, str) and ("oom" in err.lower() or "out of memory" in err.lower()):
            # Increase guess for this pipeline to reduce repeated OOMs.
            # Note: ExecutionResult does not provide pipeline_id, so use priority/ops only is weak.
            # We conservatively bump based on ops' pipeline_id if present; otherwise no-op.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                cur = s.ram_mult.get(pid, 1.0)
                s.ram_mult[pid] = _clamp(cur * s.ram_bump_factor, 1.0, s.max_ram_mult)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper to find an assignable op for a given pipeline
    def next_assignable_op(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return None
        # Choose exactly one runnable op (parents complete)
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0:1]

    # For each pool, schedule at most one op (like baseline), but prefer high priority.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try a bounded number of pulls to avoid infinite loops when pipelines aren't ready
        pulled = []
        assignment_made = False

        for _ in range(len(s.q_query) + len(s.q_interactive) + len(s.q_batch)):
            p = _choose_next_pipeline(s)
            if p is None:
                break
            pulled.append(p)

            if _pipeline_done_or_dead(p):
                continue

            op_list = next_assignable_op(p)
            if not op_list:
                continue

            prio = p.priority

            # Determine caps; allow burst if pool has plenty of headroom
            # Burst condition: >= 80% of pool free.
            free_cpu_frac = avail_cpu / max(pool.max_cpu_pool, 1e-9)
            free_ram_frac = avail_ram / max(pool.max_ram_pool, 1e-9)
            burst = (free_cpu_frac >= 0.80 and free_ram_frac >= 0.80)

            cpu_frac = s.burst_cpu_frac[prio] if burst else s.cap_cpu_frac[prio]
            ram_frac = s.burst_ram_frac[prio] if burst else s.cap_ram_frac[prio]

            # Base slices
            cpu_cap = max(s.min_cpu_slice, pool.max_cpu_pool * cpu_frac)
            ram_cap = max(s.min_ram_slice, pool.max_ram_pool * ram_frac)

            # Respect current availability
            cpu_alloc = min(avail_cpu, cpu_cap)
            ram_alloc = min(avail_ram, ram_cap)

            # Apply OOM backoff multiplier by requesting more RAM (up to remaining availability)
            mult = s.ram_mult.get(p.pipeline_id, 1.0)
            ram_alloc = min(avail_ram, ram_alloc * mult)

            # If we can't allocate minimally, skip this pool for now
            if cpu_alloc < min(s.min_cpu_slice, avail_cpu) or ram_alloc < min(s.min_ram_slice, avail_ram):
                continue

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_alloc,
                    ram=ram_alloc,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            assignment_made = True
            break

        # Requeue any pulled pipelines we didn't consume (keep FIFO ordering within each priority)
        # We requeue in the same order they were pulled to avoid starving earlier items.
        for p in pulled:
            # If we assigned from this pipeline, we still requeue it (it likely has more ops).
            # But keep it at the end to maintain fair-ish rotation.
            _requeue_pipeline(s, p)

        if assignment_made:
            # One assignment per pool per tick for stability (same as baseline).
            continue

    return suspensions, assignments
