# policy_key: scheduler_est_046
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.039094
# generation_seconds: 45.26
# generated_at: 2026-04-01T00:15:21.435511
@register_scheduler_init(key="scheduler_est_046")
def scheduler_est_046_init(s):
    """Priority-aware scheduler with basic RAM estimation + OOM backoff.

    Improvements over naive FIFO:
    - Maintain per-priority waiting queues (QUERY/INTERACTIVE ahead of BATCH).
    - Use op.estimate.mem_peak_gb as a RAM floor (extra RAM is "free", but below floor risks OOM).
    - On OOM-like failures, retry the pipeline with increased RAM floor (exponential backoff).
    - Avoid giving an entire pool to a single low-priority op: apply simple per-priority CPU caps
      to reduce head-of-line blocking and improve latency for interactive work.
    - If multiple pools exist, bias high-priority work to pool 0 (soft partitioning).
    """
    # Waiting queues store tuples: (enqueue_tick, pipeline)
    s.waiting_query = []
    s.waiting_interactive = []
    s.waiting_batch = []

    # Pipeline-level RAM floors (GB) that we learn from OOM retries.
    s.pipeline_ram_floor_gb = {}  # pipeline_id -> float

    # Track last seen tick to support simple aging/ordering.
    s._tick = 0

    # Remember recent failures to avoid spinning on non-OOM errors.
    s.pipeline_hard_failed = set()  # pipeline_id


@register_scheduler(key="scheduler_est_046")
def scheduler_est_046_scheduler(s, results, pipelines):
    """
    Policy step: enqueue arrivals, learn from results, then greedily schedule runnable ops:
    - Choose next runnable op by priority (QUERY > INTERACTIVE > BATCH).
    - Size RAM = min(avail, max(estimate, learned_floor, small_min)).
    - Size CPU with per-priority caps; interactive gets more, batch gets less.
    - Schedule multiple assignments per pool per tick while capacity remains.
    """
    # ---- helpers (kept inside for "single code block" requirement) ----
    def _prio_rank(prio):
        # Higher number means higher priority
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1  # Priority.BATCH_PIPELINE and anything else

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.waiting_query
        if prio == Priority.INTERACTIVE:
            return s.waiting_interactive
        return s.waiting_batch

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.pipeline_hard_failed:
            return
        q = _queue_for_priority(p.priority)
        q.append((s._tick, p))

    def _is_pipeline_done_or_hard_failed(p):
        if p.pipeline_id in s.pipeline_hard_failed:
            return True
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If there are failures, we only want to retry if they look like OOM; otherwise hard-fail.
        # We don't have the exact failure reason at pipeline status level; we infer from results.
        return False

    def _pop_next_candidate():
        # Strict priority ordering; within each queue, FIFO by enqueue_tick.
        for q in (s.waiting_query, s.waiting_interactive, s.waiting_batch):
            while q:
                _, p = q.pop(0)
                if _is_pipeline_done_or_hard_failed(p):
                    continue
                return p
        return None

    def _requeue_pipeline(p, bump_tick=False):
        # Requeue at the end of its priority queue; optionally bump tick to preserve FIFO-ish behavior.
        q = _queue_for_priority(p.priority)
        q.append(((s._tick if bump_tick else s._tick), p))

    def _mem_estimate_floor_gb(op):
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None
        if est is None:
            return 0.0
        try:
            return float(est)
        except Exception:
            return 0.0

    def _cpu_cap_for_priority(pool, prio):
        # Simple caps to reduce HOL blocking:
        # - QUERY: up to 100% of pool
        # - INTERACTIVE: up to 75% of pool
        # - BATCH: up to 50% of pool
        max_cpu = float(pool.max_cpu_pool)
        if prio == Priority.QUERY:
            return max_cpu
        if prio == Priority.INTERACTIVE:
            return max_cpu * 0.75
        return max_cpu * 0.50

    def _pool_order_for_priority(prio):
        # If multiple pools exist, bias high-priority to pool 0.
        # Otherwise, just iterate in order.
        n = s.executor.num_pools
        if n <= 1:
            return [0]
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, n)]
        # Batch: prefer non-0 pools to keep pool 0 responsive.
        return [i for i in range(1, n)] + [0]

    # ---- tick bookkeeping ----
    s._tick += 1

    # ---- process new arrivals ----
    for p in pipelines:
        _enqueue_pipeline(p)

    # ---- learn from execution results (OOM backoff + hard fail detection) ----
    for r in results:
        # If a pipeline previously hard-failed, ignore.
        # (We don't have pipeline_id on results; but we can still use r.ops metadata.)
        # However, ExecutionResult includes ops and priority/pool/container, not pipeline_id.
        # We'll infer pipeline_id from op if it carries it, else skip learning.
        pipeline_id = None
        if getattr(r, "ops", None):
            try:
                pipeline_id = getattr(r.ops[0], "pipeline_id", None)
            except Exception:
                pipeline_id = None

        if not r.failed():
            continue

        err = str(getattr(r, "error", "") or "")
        is_oomish = ("oom" in err.lower()) or ("out of memory" in err.lower()) or ("mem" in err.lower())

        if pipeline_id is None:
            # Can't attribute; nothing more we can do safely.
            continue

        if is_oomish:
            # Exponential-ish backoff: next attempt requests at least 2x last RAM (and never decreases).
            prev = float(s.pipeline_ram_floor_gb.get(pipeline_id, 0.0))
            try:
                last_ram = float(getattr(r, "ram", 0.0) or 0.0)
            except Exception:
                last_ram = 0.0
            new_floor = max(prev, max(0.0, last_ram) * 2.0, 1.0)
            s.pipeline_ram_floor_gb[pipeline_id] = new_floor
        else:
            # Non-OOM failures are treated as hard failures to avoid infinite retries.
            s.pipeline_hard_failed.add(pipeline_id)

    # Early exit if nothing changed that would affect scheduling decisions.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ---- scheduling loop ----
    # We try to schedule multiple ops per pool per tick while capacity remains.
    # Placement: attempt to place per-pipeline based on its priority-biased pool ordering.
    # Because the interface doesn't let us "peek" runnable ops without popping, we:
    #  - pop a candidate pipeline
    #  - find its runnable op
    #  - try to place; if it doesn't fit anywhere, requeue and continue
    # To avoid O(N^2), we cap attempts per tick.
    max_attempts = 200  # guardrail for determinism and runtime
    attempts = 0

    while attempts < max_attempts:
        attempts += 1
        pipeline = _pop_next_candidate()
        if pipeline is None:
            break

        status = pipeline.runtime_status()
        # Skip if already completed.
        if status.is_pipeline_successful():
            continue

        # Get at most one runnable operator (parents complete).
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not runnable yet; put it back and move on.
            _requeue_pipeline(pipeline, bump_tick=True)
            continue

        op = op_list[0]

        # Determine RAM floor: max(op estimate, learned pipeline floor).
        learned_floor = float(s.pipeline_ram_floor_gb.get(pipeline.pipeline_id, 0.0))
        est_floor = _mem_estimate_floor_gb(op)
        ram_floor = max(learned_floor, est_floor, 0.25)  # small minimum to avoid 0 allocations

        # Try pools in priority-biased order.
        placed = False
        for pool_id in _pool_order_for_priority(pipeline.priority):
            pool = s.executor.pools[pool_id]
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)

            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                continue

            # Must meet RAM floor to avoid likely OOM.
            if avail_ram < ram_floor:
                continue

            # CPU sizing: cap by priority to reduce HOL blocking; also don't exceed avail.
            cpu_cap = _cpu_cap_for_priority(pool, pipeline.priority)
            cpu = min(avail_cpu, cpu_cap)

            # Ensure we allocate something; if pool has fractional CPU, still allow small >0.
            if cpu <= 0.0:
                continue

            # RAM sizing: allocate as much as is available up to pool's remaining,
            # but don't starve the pool completely for high priority? Keep it simple:
            # - QUERY/INTERACTIVE: allocate generously (up to all avail).
            # - BATCH: allocate only what's needed (floor) plus a little slack.
            if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                ram = avail_ram
            else:
                ram = min(avail_ram, max(ram_floor, ram_floor * 1.25))

            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)
            placed = True
            break

        if not placed:
            # Couldn't fit now; requeue to try later.
            _requeue_pipeline(pipeline, bump_tick=True)

    return suspensions, assignments
