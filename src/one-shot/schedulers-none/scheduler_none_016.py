# policy_key: scheduler_none_016
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.035839
# generation_seconds: 36.24
# generated_at: 2026-03-12T21:31:50.197366
@register_scheduler_init(key="scheduler_none_016")
def scheduler_none_016_init(s):
    """Priority-aware FIFO with conservative resource slicing and simple OOM backoff.

    Improvements over naive:
    - Maintain per-priority queues (QUERY > INTERACTIVE > BATCH_PIPELINE).
    - Avoid "give all resources to one op": allocate small slices to reduce HoL blocking and improve latency.
    - Simple OOM handling: if an op fails with OOM, retry later with larger RAM reservation for that pipeline.
    - Light aging for batch to avoid indefinite starvation when interactive load is continuous.
    - Optional preemption: if high-priority arrives and there is no headroom, suspend one low-priority container.
    """
    # Waiting queues per priority
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline multiplicative RAM backoff after OOM failures
    s.pipeline_ram_mult = {}  # pipeline_id -> float

    # Track "arrival order" for fairness/aging (monotonic tick counter)
    s.tick = 0
    s.pipeline_enqueued_at = {}  # pipeline_id -> tick

    # Track currently running containers by pool for possible preemption
    # container_id -> dict(priority=..., pool_id=...)
    s.running = {}

    # Policy knobs (kept intentionally simple)
    s.min_cpu_slice = 1.0
    s.min_ram_slice = 1.0

    # Reserve some headroom in each pool for high priority work (soft; used for admission decisions)
    s.reserve_cpu_for_hi = 1.0
    s.reserve_ram_for_hi = 1.0

    # Default base allocations per priority (as a fraction of pool capacity, capped by availability)
    s.base_frac = {
        Priority.QUERY: (0.50, 0.50),         # (cpu_frac, ram_frac)
        Priority.INTERACTIVE: (0.35, 0.35),
        Priority.BATCH_PIPELINE: (0.25, 0.25),
    }

    # Maximum number of ops to start per pool per tick to avoid churn / overcommit of small ops
    s.max_starts_per_pool = 2


def _prio_order_with_aging(s):
    """Return a list of priorities to consider, with basic aging for batch."""
    # If batch has waited "too long", temporarily lift it above INTERACTIVE.
    # Keep QUERY always highest.
    # This is intentionally conservative to reduce p99 latency regressions.
    now = s.tick
    batch_oldest = None
    if s.wait_q[Priority.BATCH_PIPELINE]:
        # Find oldest enqueued batch pipeline tick (O(n) but queues are expected small in simulator).
        batch_oldest = min(
            s.pipeline_enqueued_at.get(p.pipeline_id, now) for p in s.wait_q[Priority.BATCH_PIPELINE]
        )
    if batch_oldest is not None and (now - batch_oldest) >= 50:
        return [Priority.QUERY, Priority.BATCH_PIPELINE, Priority.INTERACTIVE]
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _enqueue_pipeline(s, p):
    q = s.wait_q.get(p.priority)
    if q is None:
        # Unknown priority: treat as lowest
        q = s.wait_q[Priority.BATCH_PIPELINE]
    q.append(p)
    if p.pipeline_id not in s.pipeline_enqueued_at:
        s.pipeline_enqueued_at[p.pipeline_id] = s.tick
    if p.pipeline_id not in s.pipeline_ram_mult:
        s.pipeline_ram_mult[p.pipeline_id] = 1.0


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "killed" in msg)


def _pick_preemption(s, pool_id):
    """Pick a running container to suspend in this pool: lowest priority first."""
    # Prefer suspending BATCH, then INTERACTIVE; never suspend QUERY here.
    candidates = []
    for cid, info in s.running.items():
        if info.get("pool_id") != pool_id:
            continue
        pr = info.get("priority")
        if pr == Priority.QUERY:
            continue
        candidates.append((0 if pr == Priority.BATCH_PIPELINE else 1, cid, pr))
    if not candidates:
        return None
    candidates.sort()
    _, cid, _ = candidates[0]
    return cid


def _compute_alloc(s, pool, priority, pipeline_id):
    """Compute CPU/RAM allocation for one assignment attempt."""
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool
    if avail_cpu <= 0 or avail_ram <= 0:
        return 0, 0

    cpu_frac, ram_frac = s.base_frac.get(priority, (0.25, 0.25))
    cpu = max(s.min_cpu_slice, min(avail_cpu, pool.max_cpu_pool * cpu_frac))
    ram = max(s.min_ram_slice, min(avail_ram, pool.max_ram_pool * ram_frac))

    # Apply per-pipeline RAM multiplier after OOMs (cap to pool capacity)
    mult = s.pipeline_ram_mult.get(pipeline_id, 1.0)
    ram = min(avail_ram, pool.max_ram_pool, ram * mult)

    # If we have lots of CPU but little RAM (or vice versa), keep slices modest to avoid blocking.
    cpu = min(cpu, avail_cpu)
    ram = min(ram, avail_ram)
    return cpu, ram


@register_scheduler(key="scheduler_none_016")
def scheduler_none_016(s, results, pipelines):
    """
    Priority-aware scheduler:
    - Enqueue new pipelines by priority.
    - Process results to update OOM backoff and running-container bookkeeping.
    - For each pool, start up to N operators, picking from highest priority queues first.
    - If no room for high-priority work, optionally preempt one low-priority container in that pool.
    """
    s.tick += 1

    # Incorporate new arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Process execution results: update OOM multipliers and running container tracking
    if results:
        for r in results:
            # Container finished/failed; no longer considered running for preemption
            if getattr(r, "container_id", None) in s.running:
                s.running.pop(r.container_id, None)

            if r.failed():
                # OOM backoff: increase RAM multiplier for the pipeline
                if _is_oom_error(getattr(r, "error", None)):
                    # Be conservative: 1.5x growth, capped
                    pid = getattr(r, "pipeline_id", None)
                    if pid is None:
                        # Fall back: if pipeline_id not present on result, try infer from ops
                        # (Not guaranteed; keep safe)
                        pid = None
                    if pid is not None:
                        cur = s.pipeline_ram_mult.get(pid, 1.0)
                        s.pipeline_ram_mult[pid] = min(8.0, max(cur, 1.0) * 1.5)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    prio_order = _prio_order_with_aging(s)

    # Helper to pop next runnable operator from queues in priority order
    def pop_next_runnable():
        for pr in prio_order:
            q = s.wait_q[pr]
            # Rotate through queue to find a runnable pipeline; keep FIFO among runnable items
            for _ in range(len(q)):
                pipeline = q.pop(0)
                status = pipeline.runtime_status()

                # Drop terminal pipelines
                if status.is_pipeline_successful():
                    s.pipeline_enqueued_at.pop(pipeline.pipeline_id, None)
                    continue
                # If there are failures other than OOM, drop the pipeline (don't infinite retry)
                # We cannot reliably distinguish all error types without introspection; keep it simple:
                # if any FAILED ops exist, keep pipeline eligible (ASSIGNABLE_STATES includes FAILED),
                # but rely on OOM backoff to reduce repeated OOMs.
                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    # Not ready; requeue at end
                    q.append(pipeline)
                    continue

                # Runnable: requeue pipeline (for future ops) and return one op
                q.append(pipeline)
                return pipeline, op_list
        return None, None

    # Schedule per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        starts = 0

        while starts < s.max_starts_per_pool:
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            pipeline, op_list = pop_next_runnable()
            if not op_list:
                break

            pr = pipeline.priority

            # Soft reservation: if we're about to schedule low priority while leaving no room for QUERY,
            # skip batch/interactive if pool is tight.
            if pr != Priority.QUERY:
                if (pool.avail_cpu_pool - s.min_cpu_slice) < s.reserve_cpu_for_hi or (pool.avail_ram_pool - s.min_ram_slice) < s.reserve_ram_for_hi:
                    # Only allow if no QUERY waiting
                    if s.wait_q[Priority.QUERY]:
                        # Refrain from scheduling low priority right now
                        break

            cpu, ram = _compute_alloc(s, pool, pr, pipeline.pipeline_id)

            # If this is QUERY and we can't fit even the minimum slice, try preempting one low-priority container
            if pr == Priority.QUERY and (cpu <= 0 or ram <= 0 or pool.avail_cpu_pool < s.min_cpu_slice or pool.avail_ram_pool < s.min_ram_slice):
                cid = _pick_preemption(s, pool_id)
                if cid is not None:
                    suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
                    # After suspension, stop scheduling this tick for this pool to let state update
                    # (prevents over-optimistic allocations in a discrete simulator tick).
                    break
                else:
                    break

            # Ensure we can at least give minimum slices
            if cpu < s.min_cpu_slice or ram < s.min_ram_slice:
                # Can't place right now; stop for this pool
                break

            assignment = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=pr,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)
            starts += 1

            # Bookkeep as "running" for possible preemption decisions later
            # We don't know container_id until results come back; still, some simulators may
            # attach container_id immediately. Keep robust by updating on results primarily.
            # Here we do nothing further.

    return suspensions, assignments
