# policy_key: scheduler_none_009
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.032395
# generation_seconds: 36.65
# generated_at: 2026-03-12T21:27:20.055431
@register_scheduler_init(key="scheduler_none_009")
def scheduler_none_009_init(s):
    """Priority-aware FIFO with conservative reservations + simple OOM-driven RAM bump.

    Incremental improvements over naive FIFO:
      1) Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
      2) Reserve a small fraction of each pool for higher priority work to protect tail latency.
      3) On OOM failure, retry the same pipeline/operator later with increased RAM for that pipeline.
      4) Keep packing simple: at most one operator assignment per pool per tick (like the baseline),
         but choose the best next pipeline by priority and readiness.

    Notes:
      - This policy intentionally avoids complex preemption and multi-op packing to stay robust.
      - Reservations are "soft": if no high-priority work is waiting, lower priority can consume all.
    """
    # Priority queues (FIFO)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-pipeline RAM multiplier, bumped on OOM to avoid repeated failures.
    # multiplier applies to the pool's available RAM at assignment time.
    s.ram_mult = {}

    # Track last seen failure to drive adaptive behavior (optional, conservative).
    s.last_error = {}

    # Tuning knobs (keep small, obvious improvements)
    s.max_attempts_per_tick = 8  # bound scanning work per pool
    s.ram_bump_factor = 1.5      # OOM -> increase multiplier
    s.ram_mult_cap = 1.0         # never ask for more than pool's avail RAM (mult is capped later)
    s.min_ram_fraction = 0.10    # never allocate less than 10% of pool's avail RAM when scheduling

    # Reservations (fractions of pool capacity) to protect higher priority latency
    # Interpreted as minimum headroom to keep free if there is pending higher-priority work.
    s.reserve_for_query_cpu = 0.25
    s.reserve_for_query_ram = 0.25
    s.reserve_for_interactive_cpu = 0.15
    s.reserve_for_interactive_ram = 0.15


def _q_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _has_waiting_higher(s, prio):
    """Whether any higher-priority pipeline is waiting (queued), used for reservation checks."""
    if prio == Priority.BATCH_PIPELINE:
        return bool(s.q_query) or bool(s.q_interactive)
    if prio == Priority.INTERACTIVE:
        return bool(s.q_query)
    return False


def _reservation_for_priority(s, prio, pool):
    """Return (reserved_cpu, reserved_ram) to keep free for higher-priority work."""
    # If there is no higher priority work waiting, do not reserve.
    if not _has_waiting_higher(s, prio):
        return 0.0, 0.0

    # If scheduling BATCH, reserve for both QUERY and INTERACTIVE (more strict).
    if prio == Priority.BATCH_PIPELINE:
        r_cpu = max(pool.max_cpu_pool * s.reserve_for_query_cpu,
                    pool.max_cpu_pool * s.reserve_for_interactive_cpu)
        r_ram = max(pool.max_ram_pool * s.reserve_for_query_ram,
                    pool.max_ram_pool * s.reserve_for_interactive_ram)
        return r_cpu, r_ram

    # If scheduling INTERACTIVE, reserve for QUERY.
    if prio == Priority.INTERACTIVE:
        return pool.max_cpu_pool * s.reserve_for_query_cpu, pool.max_ram_pool * s.reserve_for_query_ram

    # QUERY: no one above it
    return 0.0, 0.0


def _pop_next_runnable_pipeline(s, q):
    """Pop next pipeline that has at least one runnable op; otherwise rotate/drop as needed."""
    # We'll rotate through the queue to avoid being stuck behind non-runnable pipelines.
    # Keep bounded to avoid O(n^2) behavior.
    n = len(q)
    for _ in range(n):
        p = q.pop(0)
        status = p.runtime_status()

        # Drop finished or definitively failed pipelines (baseline behavior).
        has_failures = status.state_counts[OperatorState.FAILED] > 0
        if status.is_pipeline_successful() or has_failures:
            continue

        # Check if there's an op we can assign now (parents complete).
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if op_list:
            return p, op_list

        # Not runnable yet; rotate to back.
        q.append(p)

    return None, []


def _choose_next_pipeline(s):
    """Priority order selection: QUERY, then INTERACTIVE, then BATCH."""
    p, ops = _pop_next_runnable_pipeline(s, s.q_query)
    if p is not None:
        return p, ops
    p, ops = _pop_next_runnable_pipeline(s, s.q_interactive)
    if p is not None:
        return p, ops
    p, ops = _pop_next_runnable_pipeline(s, s.q_batch)
    if p is not None:
        return p, ops
    return None, []


def _enqueue_pipeline(s, p):
    _q_for_priority(s, p.priority).append(p)


@register_scheduler(key="scheduler_none_009")
def scheduler_none_009(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into per-priority FIFO queues.
      - Process execution results to learn OOM signals and bump per-pipeline RAM multiplier.
      - For each pool, try to assign 1 runnable operator from the highest-priority runnable pipeline,
        respecting reservations if higher-priority work is waiting.
    """
    # Ingest newly arrived pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from results (OOM -> increase RAM multiplier for that pipeline)
    for r in results:
        if r.failed():
            # Track error to drive RAM bump on OOM-like errors
            s.last_error[r.container_id] = r.error
            # Heuristic: treat any error containing "oom" as memory failure
            err = (r.error or "")
            if "oom" in err.lower() or "out of memory" in err.lower():
                # Best-effort: results include ops, but not pipeline_id; we rely on ops holding pipeline_id sometimes.
                # If not available, we cannot attribute; so we conservatively do nothing in that case.
                pid = None
                # Try common patterns: op has pipeline_id attribute OR it is a tuple-like with first element pipeline_id.
                try:
                    if r.ops and hasattr(r.ops[0], "pipeline_id"):
                        pid = r.ops[0].pipeline_id
                except Exception:
                    pid = None
                if pid is not None:
                    cur = s.ram_mult.get(pid, 0.5)  # start at 50% of avail RAM
                    bumped = min(cur * s.ram_bump_factor, s.ram_mult_cap)
                    s.ram_mult[pid] = bumped

    # Early exit if nothing changed; still keep ability to schedule when queues non-empty.
    if not pipelines and not results and (not s.q_query and not s.q_interactive and not s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # One assignment per pool per tick, but priority-aware and reservation-aware.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Attempt bounded number of times to find a runnable pipeline that fits reservation constraints.
        for _ in range(s.max_attempts_per_tick):
            pipeline, op_list = _choose_next_pipeline(s)
            if pipeline is None or not op_list:
                break

            # Respect reservations if higher-priority work is queued.
            reserved_cpu, reserved_ram = _reservation_for_priority(s, pipeline.priority, pool)
            eff_cpu = max(0.0, avail_cpu - reserved_cpu)
            eff_ram = max(0.0, avail_ram - reserved_ram)

            # If we can't allocate anything meaningful under reservations, requeue and stop for this pool.
            if eff_cpu <= 0 or eff_ram <= 0:
                _enqueue_pipeline(s, pipeline)
                break

            # Size CPU: give the effective remaining CPU for best latency (simple, consistent with baseline).
            cpu = eff_cpu

            # Size RAM: use per-pipeline multiplier * effective RAM, with a floor to avoid trivially small allocations.
            pid = pipeline.pipeline_id
            mult = s.ram_mult.get(pid, 0.5)  # start conservative; increase on OOM
            # Convert multiplier to an actual amount, bounded within [min_fraction*eff_ram, eff_ram]
            ram = mult * eff_ram
            min_ram = s.min_ram_fraction * eff_ram
            if ram < min_ram:
                ram = min_ram
            if ram > eff_ram:
                ram = eff_ram

            # If still too small to be useful, requeue and stop for this pool.
            if cpu <= 0 or ram <= 0:
                _enqueue_pipeline(s, pipeline)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Requeue pipeline to continue later (other ops will become runnable over time).
            _enqueue_pipeline(s, pipeline)
            break

    return suspensions, assignments
