# policy_key: scheduler_low_017
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044365
# generation_seconds: 45.56
# generated_at: 2026-03-12T21:47:23.350768
@register_scheduler_init(key="scheduler_low_017")
def scheduler_low_017_init(s):
    """
    Priority-aware FIFO with small, safe improvements over naive:
      1) Maintain separate queues per priority; always service higher priority first.
      2) Avoid "allocate entire pool to one op" by using per-op CPU/RAM slices to increase concurrency.
      3) Simple OOM-aware retry sizing: if an op fails with an OOM-like error, re-run it with more RAM (exponential backoff).
      4) Gentle anti-starvation aging: very old BATCH items can be treated like INTERACTIVE for ordering.

    Notes:
      - No preemption (Suspend) yet; this keeps the policy simple/robust as a first improvement step.
      - Placement: prefer pool 0 for QUERY/INTERACTIVE when multiple pools exist.
    """
    from collections import deque

    s.ticks = 0
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track per-op sizing overrides after failures (e.g., OOM).
    # Keyed by (pipeline_id, op_key) -> {"ram": float, "cpu": float, "ooms": int}
    s.op_overrides = {}

    # Track enqueue time for simple aging
    # Keyed by pipeline_id -> tick
    s.enqueue_tick = {}

    # Config knobs (kept conservative)
    s.min_cpu_slice = 1.0
    s.min_ram_slice = 0.5  # in whatever units the simulator uses (typically GB)
    s.high_cpu_frac = 0.50
    s.high_ram_frac = 0.50
    s.low_cpu_frac = 0.25
    s.low_ram_frac = 0.25

    # Reserve some headroom in each pool for higher-priority work.
    # Batch work only uses resources beyond these reserves when there is contention.
    s.reserve_cpu_for_high_frac = 0.20
    s.reserve_ram_for_high_frac = 0.20

    # Aging threshold (ticks) after which a batch pipeline is treated with higher priority ordering.
    s.batch_aging_ticks = 50


@register_scheduler(key="scheduler_low_017")
def scheduler_low_017_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into priority queues.
      - Process results to update OOM-based sizing overrides.
      - For each pool, allocate multiple small slices to runnable ops, prioritizing QUERY/INTERACTIVE.
      - Batch runs only if it doesn't consume reserved headroom needed to protect tail latency for high priority.
    """
    from collections import deque

    s.ticks += 1

    suspensions = []
    assignments = []

    def _prio_queue_for(p):
        if p.priority == Priority.QUERY:
            return s.q_query
        if p.priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        q = _prio_queue_for(p)
        q.append(p)
        if p.pipeline_id not in s.enqueue_tick:
            s.enqueue_tick[p.pipeline_id] = s.ticks

    def _op_key(op):
        # Best-effort stable key without relying on specific op attributes
        try:
            return getattr(op, "op_id")
        except Exception:
            return None

    def _op_key_str(pipeline_id, op):
        k = _op_key(op)
        if k is None:
            k = str(op)
        return (pipeline_id, k)

    def _is_oom_error(err):
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "exceed" in e)

    def _default_slices(pool, priority):
        # Use fractions of pool capacity, but never below minimum slices
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            cpu = max(s.min_cpu_slice, pool.max_cpu_pool * s.high_cpu_frac)
            ram = max(s.min_ram_slice, pool.max_ram_pool * s.high_ram_frac)
        else:
            cpu = max(s.min_cpu_slice, pool.max_cpu_pool * s.low_cpu_frac)
            ram = max(s.min_ram_slice, pool.max_ram_pool * s.low_ram_frac)
        return cpu, ram

    def _select_pool_ids_for_priority(priority):
        # Prefer pool 0 for high-priority when multiple pools exist.
        if s.executor.num_pools <= 1:
            return list(range(s.executor.num_pools))
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, s.executor.num_pools)]
        return [i for i in range(1, s.executor.num_pools)] + [0]

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator is FAILED we *may* retry; don't drop pipeline here.
        return False

    def _next_assignable_op(p):
        st = p.runtime_status()
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        return op_list[0]

    def _pop_next_pipeline_for_pool(pool_id):
        # Aging: very old batch pipelines are treated like interactive for ordering (only for picking).
        def batch_is_aged(p):
            enq = s.enqueue_tick.get(p.pipeline_id, s.ticks)
            return (s.ticks - enq) >= s.batch_aging_ticks

        # Try query -> interactive -> aged batch -> batch
        for q in (s.q_query, s.q_interactive):
            while q:
                p = q.popleft()
                if not _pipeline_done_or_failed(p):
                    return p
        # Aged batch
        if s.q_batch:
            # scan a few items to find an aged one without O(n) always
            scan = min(len(s.q_batch), 8)
            tmp = deque()
            chosen = None
            for _ in range(scan):
                p = s.q_batch.popleft()
                if chosen is None and (not _pipeline_done_or_failed(p)) and batch_is_aged(p):
                    chosen = p
                else:
                    tmp.append(p)
            # put back the rest
            s.q_batch.extendleft(reversed(tmp))
            if chosen is not None:
                return chosen

        # Regular batch
        while s.q_batch:
            p = s.q_batch.popleft()
            if not _pipeline_done_or_failed(p):
                return p

        return None

    # 1) Ingest pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # 2) Update overrides from results (OOM-aware RAM backoff)
    for r in results:
        try:
            if r.failed() and _is_oom_error(r.error):
                # Update override for the first op in r.ops (typical single-op assignment)
                if r.ops:
                    op = r.ops[0]
                    key = _op_key_str(getattr(r, "pipeline_id", None), op) if hasattr(r, "pipeline_id") else None
                    # If pipeline_id isn't available on results, fall back to op identity only.
                    if key is None or key[0] is None:
                        key = ("_unknown_pipeline", str(op))

                    ov = s.op_overrides.get(key, {"ram": float(r.ram) if r.ram else 0.0, "cpu": float(r.cpu) if r.cpu else 0.0, "ooms": 0})
                    ov["ooms"] += 1
                    # Exponential backoff; ensure it increases meaningfully
                    base_ram = float(r.ram) if r.ram else max(s.min_ram_slice, ov["ram"])
                    ov["ram"] = max(base_ram * 2.0, ov["ram"] * 2.0 if ov["ram"] else base_ram * 2.0)
                    # CPU unchanged for OOM; keep whatever was used
                    ov["cpu"] = float(r.cpu) if r.cpu else ov["cpu"]
                    s.op_overrides[key] = ov
        except Exception:
            # Keep scheduler robust if result schema varies
            continue

    # Early exit if nothing new (but still safe to continue)
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    # 3) Per-pool scheduling: fill with slices, protecting high-priority headroom from batch
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Compute reserves for high-priority to reduce tail latency when batch is present
        reserve_cpu = float(pool.max_cpu_pool) * s.reserve_cpu_for_high_frac
        reserve_ram = float(pool.max_ram_pool) * s.reserve_ram_for_high_frac

        # We'll try to make multiple assignments per pool per tick.
        # Avoid infinite loops via a bounded number of attempts.
        attempts = 0
        max_attempts = 32

        while attempts < max_attempts and avail_cpu > 0 and avail_ram > 0:
            attempts += 1

            # Pick next pipeline (global ordering), then decide if it can run in this pool
            p = _pop_next_pipeline_for_pool(pool_id)
            if p is None:
                break

            # Determine runnable op
            op = _next_assignable_op(p)
            if op is None:
                # Not runnable yet; requeue to preserve fairness
                _enqueue_pipeline(p)
                continue

            # Determine requested slices (with overrides on OOM)
            base_cpu, base_ram = _default_slices(pool, p.priority)
            key = _op_key_str(p.pipeline_id, op)
            ov = s.op_overrides.get(key)

            req_cpu = base_cpu
            req_ram = base_ram
            if ov:
                if ov.get("ram", 0) > 0:
                    req_ram = max(req_ram, float(ov["ram"]))
                if ov.get("cpu", 0) > 0:
                    req_cpu = max(s.min_cpu_slice, float(ov["cpu"]))

            # Clamp to pool maxima
            req_cpu = min(req_cpu, float(pool.max_cpu_pool))
            req_ram = min(req_ram, float(pool.max_ram_pool))

            # Clamp to currently available
            req_cpu = min(req_cpu, avail_cpu)
            req_ram = min(req_ram, avail_ram)

            # Enforce minimum slices; if we can't fit minimum, stop filling this pool
            if req_cpu < s.min_cpu_slice or req_ram < s.min_ram_slice:
                # Put back and stop; resources too fragmented
                _enqueue_pipeline(p)
                break

            # Protect reserves: if this is batch and would dip into reserved headroom, defer it.
            if p.priority == Priority.BATCH_PIPELINE:
                # Only allow batch to consume beyond reserves, unless there's no high-priority waiting.
                high_waiting = bool(s.q_query or s.q_interactive)
                if high_waiting:
                    if (avail_cpu - req_cpu) < reserve_cpu or (avail_ram - req_ram) < reserve_ram:
                        _enqueue_pipeline(p)
                        # If batch can't run due to reserves, stop trying batch-heavy fills this tick.
                        # (We may still have no high-priority runnable, but we avoid thrashing.)
                        break

            # Assign one op at a time
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Update available resources for subsequent packing decisions in this pool
            avail_cpu -= req_cpu
            avail_ram -= req_ram

            # Requeue pipeline to allow round-robin progression through its DAG
            _enqueue_pipeline(p)

    return suspensions, assignments
