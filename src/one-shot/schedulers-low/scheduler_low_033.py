# policy_key: scheduler_low_033
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042573
# generation_seconds: 49.78
# generated_at: 2026-03-14T02:27:57.994106
@register_scheduler_init(key="scheduler_low_033")
def scheduler_low_033_init(s):
    """
    Priority-aware FIFO with simple right-sizing.

    Incremental improvements over naive FIFO:
      1) Maintain separate FIFO queues per priority (QUERY/INTERACTIVE/BATCH).
      2) Avoid "allocate entire pool to one op" by using per-op CPU/RAM quanta and
         allowing multiple assignments per pool per tick.
      3) Basic OOM-driven RAM backoff: if an op fails with an OOM-like error, bump
         future RAM requests for that pipeline (exponential backoff, capped by pool).
      4) Soft reservation: keep some headroom in pools to reduce interference and
         preserve latency for high-priority work.

    Notes:
      - This policy does NOT preempt because the minimal API surface shown does not
        expose currently-running containers. We instead reduce contention via quanta
        + headroom reservation and prioritize admission.
    """
    from collections import deque

    # Per-priority FIFO queues of Pipeline objects
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track pipeline arrival order (for stable FIFO across internal moves)
    s._arrival_seq = 0
    s._arrival_order = {}  # pipeline_id -> seq int

    # Simple RAM backoff on OOM-ish failures; multiplier applied to requested RAM quantum
    s.ram_mult_by_pipeline = {}  # pipeline_id -> float

    # Keep a small "cooldown" to avoid immediate re-scheduling of repeatedly failing pipelines
    s.fail_streak = {}  # pipeline_id -> int

    # Tuning knobs (small/obvious improvements first)
    s.soft_headroom_frac = 0.10  # keep 10% CPU/RAM free if possible
    s.min_cpu_per_op = 1.0
    s.min_ram_per_op = 0.5  # in "RAM units" of the simulator; kept conservative
    s.max_ops_per_pool_per_tick = 8  # cap per pool per tick to prevent overpacking


def _low033_is_oom_error(err) -> bool:
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "killed" in msg)


def _low033_prio_rank(priority):
    # Smaller is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and anything else


def _low033_enqueue(s, pipeline):
    pid = pipeline.pipeline_id
    if pid not in s._arrival_order:
        s._arrival_order[pid] = s._arrival_seq
        s._arrival_seq += 1

    if pipeline.priority == Priority.QUERY:
        s.q_query.append(pipeline)
    elif pipeline.priority == Priority.INTERACTIVE:
        s.q_interactive.append(pipeline)
    else:
        s.q_batch.append(pipeline)


def _low033_all_queues_empty(s) -> bool:
    return (not s.q_query) and (not s.q_interactive) and (not s.q_batch)


def _low033_pop_next_pipeline(s):
    # Strict priority order, FIFO within each class.
    if s.q_query:
        return s.q_query.popleft()
    if s.q_interactive:
        return s.q_interactive.popleft()
    if s.q_batch:
        return s.q_batch.popleft()
    return None


def _low033_requeue(s, pipeline):
    # Requeue to tail of its priority queue for round-robin-ish behavior within class.
    _low033_enqueue(s, pipeline)


def _low033_base_quanta(pool, priority):
    """
    Choose a conservative per-op resource quantum, rather than consuming the entire pool.
    High priority gets a bit more CPU to reduce latency.
    """
    # CPU quantum: aim for a modest slice of the pool, but never below 1 vCPU if available.
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        cpu_frac = 0.50
        ram_frac = 0.30
    else:
        cpu_frac = 0.35
        ram_frac = 0.25

    cpu_q = max(1.0, pool.max_cpu_pool * cpu_frac)
    ram_q = max(1.0, pool.max_ram_pool * ram_frac)
    return cpu_q, ram_q


@register_scheduler(key="scheduler_low_033")
def scheduler_low_033(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Priority-aware multi-assignment scheduler with simple OOM backoff and soft headroom.

    Core loop:
      - Incorporate new pipelines into priority queues.
      - Update RAM multipliers based on recent failures (OOM => increase).
      - For each pool, assign multiple ready operators in priority order using quanta,
        leaving soft headroom to reduce interference and tail latency.
    """
    # Ingest new pipelines
    for p in pipelines:
        _low033_enqueue(s, p)

    # Update feedback-driven RAM multipliers based on failures
    if results:
        for r in results:
            # We only have pipeline_id on Assignment, but ExecutionResult doesn't list it in the prompt.
            # We can still adapt when possible by attributing failure to the pipeline owning the op
            # via op.pipeline_id if present, else skip.
            pid = None
            try:
                # Some simulators attach pipeline_id to ops or result; best-effort.
                if hasattr(r, "pipeline_id"):
                    pid = r.pipeline_id
                elif getattr(r, "ops", None) and hasattr(r.ops[0], "pipeline_id"):
                    pid = r.ops[0].pipeline_id
            except Exception:
                pid = None

            if pid is None:
                continue

            if r.failed():
                s.fail_streak[pid] = s.fail_streak.get(pid, 0) + 1
                if _low033_is_oom_error(r.error):
                    cur = s.ram_mult_by_pipeline.get(pid, 1.0)
                    # Exponential backoff capped to prevent runaway
                    s.ram_mult_by_pipeline[pid] = min(cur * 2.0, 16.0)
            else:
                # On success, gently decay the multiplier (avoid being stuck oversized forever)
                if pid in s.ram_mult_by_pipeline:
                    s.ram_mult_by_pipeline[pid] = max(1.0, s.ram_mult_by_pipeline[pid] * 0.8)
                s.fail_streak[pid] = 0

    # Early exit: no new info and nothing to do
    if not pipelines and not results and _low033_all_queues_empty(s):
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # We'll cycle through pools, scheduling multiple ops per pool while resources remain.
    # Keep track to avoid scheduling multiple ops from the same pipeline in one tick (reduces burstiness).
    scheduled_pipelines_this_tick = set()

    # Build pool ordering: if there is high-priority work waiting, prefer the pool with most headroom first.
    # Otherwise, any order is fine.
    have_high_prio_waiting = bool(s.q_query) or bool(s.q_interactive)

    pool_order = list(range(s.executor.num_pools))
    if have_high_prio_waiting:
        pool_order.sort(
            key=lambda i: (s.executor.pools[i].avail_cpu_pool + s.executor.pools[i].avail_ram_pool),
            reverse=True,
        )

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft headroom reservation to reduce interference / preserve tail latency
        headroom_cpu = float(pool.max_cpu_pool) * float(getattr(s, "soft_headroom_frac", 0.0))
        headroom_ram = float(pool.max_ram_pool) * float(getattr(s, "soft_headroom_frac", 0.0))

        # Usable resources this tick (don't go below zero)
        usable_cpu = max(0.0, avail_cpu - headroom_cpu)
        usable_ram = max(0.0, avail_ram - headroom_ram)

        # If we're under contention, still allow at least minimal scheduling (especially for high-priority).
        # We'll treat "usable" as a preference, not a hard constraint.
        ops_scheduled_in_pool = 0

        while ops_scheduled_in_pool < s.max_ops_per_pool_per_tick:
            # Stop if we can't even allocate a minimal container
            if avail_cpu < s.min_cpu_per_op or avail_ram < s.min_ram_per_op:
                break

            pipeline = _low033_pop_next_pipeline(s)
            if pipeline is None:
                break

            status = pipeline.runtime_status()

            # Drop completed pipelines
            if status.is_pipeline_successful():
                continue

            # Avoid infinite hot-loop on repeatedly failing pipelines by giving others a chance
            pid = pipeline.pipeline_id
            if s.fail_streak.get(pid, 0) >= 5:
                # Still requeue, but deprioritize implicitly by pushing to the back
                _low033_requeue(s, pipeline)
                continue

            # Only schedule one op per pipeline per tick to reduce internal contention and bursty packing
            if pid in scheduled_pipelines_this_tick:
                _low033_requeue(s, pipeline)
                continue

            # Find next ready op (parents complete)
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # No ready ops yet, requeue and try next pipeline
                _low033_requeue(s, pipeline)
                continue

            # Choose quanta and apply RAM backoff multiplier
            cpu_q, ram_q = _low033_base_quanta(pool, pipeline.priority)
            ram_mult = float(s.ram_mult_by_pipeline.get(pid, 1.0))

            # Aim to fit within usable resources; if usable is zero (due to headroom), fall back to avail.
            cpu_cap = usable_cpu if usable_cpu >= s.min_cpu_per_op else avail_cpu
            ram_cap = usable_ram if usable_ram >= s.min_ram_per_op else avail_ram

            req_cpu = max(s.min_cpu_per_op, min(cpu_q, cpu_cap))
            req_ram = max(s.min_ram_per_op, min(ram_q * ram_mult, ram_cap))

            # Ensure we don't exceed actual availability
            req_cpu = min(req_cpu, avail_cpu)
            req_ram = min(req_ram, avail_ram)

            if req_cpu < s.min_cpu_per_op or req_ram < s.min_ram_per_op:
                # Can't place now; put back and stop trying to pack this pool further
                _low033_requeue(s, pipeline)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Consume resources locally so we can place multiple ops
            avail_cpu -= req_cpu
            avail_ram -= req_ram
            usable_cpu = max(0.0, avail_cpu - headroom_cpu)
            usable_ram = max(0.0, avail_ram - headroom_ram)

            scheduled_pipelines_this_tick.add(pid)
            ops_scheduled_in_pool += 1

            # Requeue pipeline to continue later (it likely has more stages)
            _low033_requeue(s, pipeline)

    return suspensions, assignments
