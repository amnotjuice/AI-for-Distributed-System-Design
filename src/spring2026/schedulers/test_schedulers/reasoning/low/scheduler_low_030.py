# policy_key: scheduler_low_030
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.055829
# generation_seconds: 42.02
# generated_at: 2026-04-09T21:26:46.667125
@register_scheduler_init(key="scheduler_low_030")
def scheduler_low_030_init(s):
    """
    Priority-aware, failure-averse scheduler.

    Core ideas (kept intentionally simple vs. FIFO):
      - Strict priority queues (QUERY > INTERACTIVE > BATCH) with light aging for BATCH.
      - Conservative initial sizing to reduce OOM risk; on OOM, increase RAM request for that pipeline.
      - Avoid starving low priority: if batch waits long enough, it can be scheduled even when
        higher-priority queues are non-empty (but only if there is ample headroom).
      - No preemption (churn can waste work and increase failures); instead, keep headroom when
        launching batch work to preserve capacity for bursts of high priority.
    """
    from collections import deque

    s.ticks = 0

    # Per-priority FIFO queues
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Pipeline bookkeeping
    s.arrival_tick = {}          # pipeline_id -> tick when first seen
    s.ram_boost = {}             # pipeline_id -> multiplicative boost for RAM on retry (starts at 1.0)
    s.fail_count = {}            # pipeline_id -> number of failed operator attempts observed

    # Tuning knobs (conservative defaults)
    s.max_failures_per_pipeline = 6

    # Batch aging (ticks are scheduler invocations; deterministic in Eudoxia)
    s.batch_aging_threshold = 30  # after this, allow a batch to compete if headroom exists

    # Headroom policy to protect high priority latency:
    # while high-priority work is waiting, we avoid consuming the last X% of pool resources with batch.
    s.protect_headroom_cpu_frac = 0.25
    s.protect_headroom_ram_frac = 0.25

    # Sizing heuristics: fraction of pool maximum to request (capped by availability)
    s.frac_cpu_query = 0.60
    s.frac_cpu_interactive = 0.50
    s.frac_cpu_batch = 0.35

    s.frac_ram_query = 0.55
    s.frac_ram_interactive = 0.45
    s.frac_ram_batch = 0.35


def _prio_rank(priority):
    # Smaller is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _queue_for(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    # Keep this intentionally broad to match different error strings in simulations.
    return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "exceed" in e)


def _pick_sizing_fracs(s, priority):
    if priority == Priority.QUERY:
        return s.frac_cpu_query, s.frac_ram_query
    if priority == Priority.INTERACTIVE:
        return s.frac_cpu_interactive, s.frac_ram_interactive
    return s.frac_cpu_batch, s.frac_ram_batch


def _min_nonzero(x, floor=1.0):
    # Eudoxia CPU/RAM likely continuous; keep a small floor to avoid zero-alloc assignments.
    return x if x > floor else floor


def _pipeline_done_or_abandoned(s, pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    pid = pipeline.pipeline_id
    if s.fail_count.get(pid, 0) >= s.max_failures_per_pipeline:
        # Abandon further scheduling attempts to avoid wasting resources.
        return True
    return False


def _next_ready_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _has_high_pri_waiting(s):
    return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)


def _eligible_batch_due_to_aging(s):
    # If the oldest batch has waited long enough, allow it to compete if there is headroom.
    if not s.q_batch:
        return False
    oldest = s.q_batch[0]
    pid = oldest.pipeline_id
    at = s.arrival_tick.get(pid, s.ticks)
    return (s.ticks - at) >= s.batch_aging_threshold


def _headroom_ok_for_batch(s, pool):
    # Permit batch scheduling only if we won't consume protected headroom.
    return (pool.avail_cpu_pool >= pool.max_cpu_pool * s.protect_headroom_cpu_frac) and (
        pool.avail_ram_pool >= pool.max_ram_pool * s.protect_headroom_ram_frac
    )


def _pop_next_pipeline(s, pool):
    """
    Choose next pipeline to schedule on this pool.

    Rules:
      1) Prefer QUERY, then INTERACTIVE.
      2) Choose BATCH only when:
         - no high-priority waiting, OR
         - batch has aged AND pool has substantial headroom.
    """
    if s.q_query:
        return s.q_query.popleft()
    if s.q_interactive:
        return s.q_interactive.popleft()

    if s.q_batch:
        if (not _has_high_pri_waiting(s)) or (_eligible_batch_due_to_aging(s) and _headroom_ok_for_batch(s, pool)):
            return s.q_batch.popleft()

    return None


@register_scheduler(key="scheduler_low_030")
def scheduler_low_030(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Deterministic step function for Eudoxia.

    - Ingest new pipelines into priority queues.
    - Update per-pipeline RAM boost and failure counts from execution results.
    - For each pool, greedily assign ready operators from the highest-eligible priority queue,
      using conservative sizing and boosting RAM after OOMs.

    Returns:
      (suspensions, assignments)
    """
    s.ticks += 1

    # --- Update state from results (failures/oom -> adjust RAM boost) ---
    if results:
        for r in results:
            # Only react to failed results; successes need no special handling here.
            if hasattr(r, "failed") and r.failed():
                pid = getattr(r, "pipeline_id", None)
                # Some result types might not expose pipeline_id; fall back by not updating.
                if pid is None:
                    continue

                s.fail_count[pid] = s.fail_count.get(pid, 0) + 1

                # If OOM, increase RAM boost multiplicatively to reduce repeated failures.
                if _is_oom_error(getattr(r, "error", None)):
                    prev = s.ram_boost.get(pid, 1.0)
                    # Escalation is moderate to avoid instantly monopolizing pools.
                    s.ram_boost[pid] = min(prev * 1.6, 4.0)

    # --- Ingest new pipelines ---
    if pipelines:
        for p in pipelines:
            pid = p.pipeline_id
            if pid not in s.arrival_tick:
                s.arrival_tick[pid] = s.ticks
            if pid not in s.ram_boost:
                s.ram_boost[pid] = 1.0
            if pid not in s.fail_count:
                s.fail_count[pid] = 0
            _queue_for(s, p.priority).append(p)

    # Early exit if nothing new to decide
    if not pipelines and not results:
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # --- Scheduling loop: per pool, assign as many ops as fit ---
    # We do not preempt (suspensions stays empty) to reduce churn and wasted work.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Continue launching while there is meaningful capacity.
        # Keep a small floor to avoid issuing tiny allocations when near empty.
        while pool.avail_cpu_pool > 0.5 and pool.avail_ram_pool > 0.5:
            pipeline = _pop_next_pipeline(s, pool)
            if pipeline is None:
                break

            # Drop completed/abandoned pipelines from consideration
            if _pipeline_done_or_abandoned(s, pipeline):
                continue

            op = _next_ready_op(pipeline)
            if op is None:
                # Not ready yet; requeue to preserve FIFO within its priority class.
                _queue_for(s, pipeline.priority).append(pipeline)
                break

            # Batch protection: if high-priority work is waiting, don't start batch unless headroom ok.
            if pipeline.priority == Priority.BATCH_PIPELINE and _has_high_pri_waiting(s) and not _headroom_ok_for_batch(s, pool):
                # Put it back and stop trying to place more batch on this pool right now.
                s.q_batch.appendleft(pipeline)
                break

            cpu_frac, ram_frac = _pick_sizing_fracs(s, pipeline.priority)

            # Compute requested resources based on pool max, bounded by current availability.
            # RAM boost applies to reduce repeated OOMs for the same pipeline.
            pid = pipeline.pipeline_id
            boost = s.ram_boost.get(pid, 1.0)

            req_cpu = min(pool.avail_cpu_pool, _min_nonzero(pool.max_cpu_pool * cpu_frac, floor=1.0))
            req_ram = min(pool.avail_ram_pool, _min_nonzero(pool.max_ram_pool * ram_frac * boost, floor=1.0))

            # If after bounding, we cannot allocate at least some CPU and RAM, stop on this pool.
            if req_cpu <= 0 or req_ram <= 0:
                # Requeue the pipeline for later.
                _queue_for(s, pipeline.priority).appendleft(pipeline)
                break

            assignment = Assignment(
                ops=[op],
                cpu=req_cpu,
                ram=req_ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Requeue pipeline to allow subsequent operators to be scheduled later.
            _queue_for(s, pipeline.priority).append(pipeline)

            # Refresh pool availability for next loop iteration (simulator will apply assignments after return,
            # but within a tick we conservatively decrement using current known avail).
            # This mirrors greedy packing and avoids over-issuing.
            pool.avail_cpu_pool -= req_cpu
            pool.avail_ram_pool -= req_ram

    return suspensions, assignments
