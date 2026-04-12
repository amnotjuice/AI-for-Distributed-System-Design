# policy_key: scheduler_iter_worst_simple_016
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050940
# generation_seconds: 41.16
# generated_at: 2026-04-12T00:45:21.318342
@register_scheduler_init(key="scheduler_iter_worst_simple_016")
def scheduler_iter_worst_simple_016_init(s):
    """
    Iteration 2: Priority-first packing with (soft) pool dedication and multi-assign per tick.

    Changes vs previous attempt to reduce weighted latency:
      - Fill each pool with multiple assignments per tick (not just one), improving responsiveness.
      - If multiple pools exist: dedicate pool 0 to QUERY/INTERACTIVE (no BATCH there).
      - Always schedule highest-priority runnable ops first; BATCH only uses leftover capacity.
      - Learn per-op RAM requirements from failures; retry FAILED ops instead of dropping pipelines.
      - Keep batch slices small to preserve headroom and reduce interference.

    Notes:
      - This policy avoids preemption because container inventory APIs are not guaranteed here.
      - It focuses on obvious flaws: underutilization (one assignment per pool) and batch interference.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    s.pipeline_enqueued_at = {}  # pipeline_id -> tick (for aging)
    s.tick = 0

    # Anti-starvation: promote old batch to interactive scheduling class after threshold
    s.batch_promotion_ticks = 250

    # Learned per-op hints (best-effort keys)
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}  # (pipeline_id, op_key) -> cpu

    # Retry limits to avoid infinite fail loops
    s.op_fail_count = {}  # (pipeline_id, op_key) -> count
    s.max_retries_per_op = 3

    # Minimal alloc slices (sim uses integer cpu/ram units)
    s.min_cpu_slice = 1
    s.min_ram_slice = 1


@register_scheduler(key="scheduler_iter_worst_simple_016")
def scheduler_iter_worst_simple_016_scheduler(s, results, pipelines):
    """
    Priority-first packing scheduler.
    - Enqueue new pipelines into per-priority queues.
    - Update per-op sizing hints from failures (RAM-first).
    - For each pool, repeatedly dispatch runnable ops while capacity remains:
        QUERY > INTERACTIVE > BATCH (with aging promotion).
      If num_pools > 1, pool 0 is reserved for QUERY/INTERACTIVE to reduce latency.
    """
    s.tick += 1

    # ---------------- Helpers ----------------
    def _op_key(op):
        # Best-effort stable identity within a run
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.pipeline_enqueued_at:
            s.pipeline_enqueued_at[pid] = s.tick
            _queue_for_priority(p.priority).append(p)

    def _drop_pipeline_bookkeeping(pid):
        if pid in s.pipeline_enqueued_at:
            del s.pipeline_enqueued_at[pid]

    def _effective_priority(p):
        # Promote old batch to interactive for scheduling decisions (keeps original priority in Assignment)
        if p.priority != Priority.BATCH_PIPELINE:
            return p.priority
        enq = s.pipeline_enqueued_at.get(p.pipeline_id, s.tick)
        if (s.tick - enq) >= s.batch_promotion_ticks:
            return Priority.INTERACTIVE
        return Priority.BATCH_PIPELINE

    def _pipeline_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _runnable_ops(p):
        st = p.runtime_status()
        # Require parents complete; FAILED is included in ASSIGNABLE_STATES by simulator contract
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        # Prefer retrying FAILED ops first if present (so pipelines can make progress)
        failed_ops = [op for op in ops if getattr(op, "state", None) == OperatorState.FAILED]
        if failed_ops:
            return failed_ops[:1]
        return ops[:1]

    def _pool_allows_priority(pool_id, eff_prio):
        # If multiple pools exist, reserve pool 0 for latency-sensitive work
        if s.executor.num_pools > 1 and pool_id == 0:
            return eff_prio in (Priority.QUERY, Priority.INTERACTIVE)
        return True

    def _caps(pool, eff_prio):
        # Caps as fraction of pool max to avoid single-container monopolization
        if eff_prio == Priority.QUERY:
            return 0.95, 0.95
        if eff_prio == Priority.INTERACTIVE:
            return 0.85, 0.90
        # Batch: keep slices small to reduce interference and improve weighted latency
        return 0.25, 0.40

    def _default_base(pool, eff_prio):
        # Default slice sizes (can be increased by hints)
        if eff_prio == Priority.QUERY:
            base_cpu = min(max(4, s.min_cpu_slice), pool.max_cpu_pool)
            base_ram = min(max(8, s.min_ram_slice), pool.max_ram_pool)
        elif eff_prio == Priority.INTERACTIVE:
            base_cpu = min(max(2, s.min_cpu_slice), pool.max_cpu_pool)
            base_ram = min(max(6, s.min_ram_slice), pool.max_ram_pool)
        else:
            base_cpu = min(max(1, s.min_cpu_slice), pool.max_cpu_pool)
            base_ram = min(max(4, s.min_ram_slice), pool.max_ram_pool)
        return base_cpu, base_ram

    def _size_for_op(p, op, pool_id):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        eff_prio = _effective_priority(p)
        cpu_cap_frac, ram_cap_frac = _caps(pool, eff_prio)
        cpu_cap = max(s.min_cpu_slice, int(pool.max_cpu_pool * cpu_cap_frac))
        ram_cap = max(s.min_ram_slice, int(pool.max_ram_pool * ram_cap_frac))

        base_cpu, base_ram = _default_base(pool, eff_prio)

        key = (p.pipeline_id, _op_key(op))
        hinted_cpu = s.op_cpu_hint.get(key, base_cpu)
        hinted_ram = s.op_ram_hint.get(key, base_ram)

        # For latency-sensitive, be more aggressive: if we have extra CPU, give it (up to cap)
        if eff_prio in (Priority.QUERY, Priority.INTERACTIVE):
            req_cpu = max(hinted_cpu, base_cpu)
            req_ram = max(hinted_ram, base_ram)
            # If plenty of CPU is free, allow larger allocations for faster completion
            req_cpu = max(req_cpu, min(avail_cpu, cpu_cap))
            req_ram = max(req_ram, min(avail_ram, ram_cap))
        else:
            # Batch stays conservative even if free capacity exists
            req_cpu = max(base_cpu, hinted_cpu)
            req_ram = max(base_ram, hinted_ram)

        # Enforce caps + availability
        req_cpu = max(s.min_cpu_slice, min(int(req_cpu), cpu_cap, avail_cpu))
        req_ram = max(s.min_ram_slice, min(int(req_ram), ram_cap, avail_ram))
        return req_cpu, req_ram

    # ---------------- Ingest new pipelines ----------------
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ---------------- Learn from results ----------------
    for r in results:
        if hasattr(r, "failed") and r.failed():
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                continue  # cannot reliably key hints

            err = str(getattr(r, "error", "") or "")
            oom_like = ("oom" in err.lower()) or ("out of memory" in err.lower()) or ("memory" in err.lower())

            ran_cpu = int(getattr(r, "cpu", 1) or 1)
            ran_ram = int(getattr(r, "ram", 1) or 1)
            ops = getattr(r, "ops", []) or []

            for op in ops:
                ok = (pid, _op_key(op))
                s.op_fail_count[ok] = s.op_fail_count.get(ok, 0) + 1

                # RAM-first backoff on OOM-ish errors; small bump otherwise
                if oom_like:
                    s.op_ram_hint[ok] = max(s.op_ram_hint.get(ok, ran_ram), ran_ram * 2)
                    # Keep CPU steady on OOM (often not the fix)
                    s.op_cpu_hint[ok] = max(s.op_cpu_hint.get(ok, ran_cpu), ran_cpu)
                else:
                    s.op_ram_hint[ok] = max(s.op_ram_hint.get(ok, ran_ram), int(ran_ram * 1.25) + 1)
                    s.op_cpu_hint[ok] = max(s.op_cpu_hint.get(ok, ran_cpu), int(ran_cpu * 1.25) + 1)

    # ---------------- Dispatch ----------------
    suspensions = []
    assignments = []

    # Create a view of queues in strict priority order; batch may be promoted by _effective_priority()
    # We'll scan queues and rotate elements to preserve FIFO within each base class.
    base_queues = [s.q_query, s.q_interactive, s.q_batch]

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # While we can still fit at least a minimal container, keep assigning
        while pool.avail_cpu_pool >= s.min_cpu_slice and pool.avail_ram_pool >= s.min_ram_slice:
            chosen_p = None
            chosen_ops = None
            chosen_eff_prio = None
            chosen_base_queue = None

            # Find next runnable pipeline under pool constraints (bounded scan per queue)
            for q in base_queues:
                q_len = len(q)
                if q_len == 0:
                    continue

                for _ in range(q_len):
                    p = q.popleft()

                    # Drop completed pipelines
                    if _pipeline_done(p):
                        _drop_pipeline_bookkeeping(p.pipeline_id)
                        continue

                    eff_prio = _effective_priority(p)
                    if not _pool_allows_priority(pool_id, eff_prio):
                        # Pool 0 reserved; requeue unchanged and continue scanning
                        q.append(p)
                        continue

                    ops = _runnable_ops(p)
                    if not ops:
                        q.append(p)
                        continue

                    # Respect retry limit: if this op has failed too many times, skip it for now
                    opk = (p.pipeline_id, _op_key(ops[0]))
                    if s.op_fail_count.get(opk, 0) > s.max_retries_per_op:
                        q.append(p)
                        continue

                    chosen_p = p
                    chosen_ops = ops
                    chosen_eff_prio = eff_prio
                    chosen_base_queue = q
                    break

                if chosen_p is not None:
                    break

            if chosen_p is None:
                break  # nothing runnable for this pool

            # Compute request size
            req_cpu, req_ram = _size_for_op(chosen_p, chosen_ops[0], pool_id)

            # If we cannot fit it, put it back and stop filling this pool (avoid spinning)
            if req_cpu <= 0 or req_ram <= 0 or req_cpu > pool.avail_cpu_pool or req_ram > pool.avail_ram_pool:
                chosen_base_queue.append(chosen_p)
                break

            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=chosen_p.priority,  # preserve original priority class for metrics
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            # Requeue pipeline for subsequent operators
            chosen_base_queue.append(chosen_p)

            # Refresh local pool reference (in case executor updates avail immediately; but we also read from pool each loop)
            pool = s.executor.pools[pool_id]

    return suspensions, assignments
