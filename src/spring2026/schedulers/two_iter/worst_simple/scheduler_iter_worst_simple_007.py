# policy_key: scheduler_iter_worst_simple_007
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048630
# generation_seconds: 38.37
# generated_at: 2026-04-12T00:39:11.853643
@register_scheduler_init(key="scheduler_iter_worst_simple_007")
def scheduler_iter_worst_simple_007_init(s):
    """
    Iteration 2: Priority-first + multi-assign packing + SRPT-ish tie-break + OOM RAM backoff.

    Main changes vs naive FIFO (and the previous attempt):
      - Never drop pipelines just because they have FAILED ops; allow retries.
      - Dispatch in strict priority order (QUERY > INTERACTIVE > BATCH) across all pools.
      - Pack multiple assignments per pool per tick (instead of max 1) to reduce queueing delay.
      - Pool preference: keep pool 0 primarily for QUERY/INTERACTIVE when multiple pools exist.
      - For high priority, bias toward "shorter remaining work" pipelines (approx via remaining op counts).
      - Conservative per-op caps for BATCH to keep concurrency/headroom.
      - On OOM-like failures, increase RAM hint aggressively for that op on retry.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    s.pipeline_enqueued_at = {}  # pipeline_id -> tick first seen (for light aging if needed)
    s.tick = 0

    # Per-op learned hints (best-effort keys)
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}  # (pipeline_id, op_key) -> cpu

    # Basic knobs (kept small and robust)
    s.min_cpu = 1
    s.min_ram = 1

    # Optional aging to prevent indefinite batch starvation (kept mild)
    s.batch_soft_promote_ticks = 400  # after this, batch can use pool 0 if idle


@register_scheduler(key="scheduler_iter_worst_simple_007")
def scheduler_iter_worst_simple_007_scheduler(s, results, pipelines):
    """
    Priority-first global dispatch with per-pool packing.

    Returns:
      suspensions: unused in this iteration (no explicit preemption due to limited executor visibility).
      assignments: list of new container assignments to launch this tick.
    """
    from collections import deque

    s.tick += 1

    def _prio_rank(prio):
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1

    def _queue_for_prio(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.pipeline_enqueued_at:
            s.pipeline_enqueued_at[pid] = s.tick
            _queue_for_prio(p.priority).append(p)

    def _drop_pipeline_bookkeeping(pid):
        if pid in s.pipeline_enqueued_at:
            del s.pipeline_enqueued_at[pid]

    def _op_key(op):
        # Best-effort stable op identity
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _pipeline_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _ready_ops_one(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]

    def _remaining_work_score(p):
        """
        Approximate remaining work for SRPT-ish tie-break: count non-completed ops.
        Smaller score => schedule earlier within same priority.
        """
        st = p.runtime_status()
        sc = getattr(st, "state_counts", {}) or {}
        remaining = 0
        for state in (OperatorState.PENDING, OperatorState.ASSIGNED, OperatorState.RUNNING,
                      OperatorState.SUSPENDING, OperatorState.FAILED):
            remaining += int(sc.get(state, 0) or 0)
        # Prefer pipelines that have something ready right now
        ready = 1 if _ready_ops_one(p) else 0
        return (remaining, -ready, s.pipeline_enqueued_at.get(p.pipeline_id, s.tick))

    def _pool_order_for_priority(prio):
        # Pool 0 is "latency pool" when multiple pools exist.
        n = s.executor.num_pools
        if n <= 1:
            return [0] if n == 1 else []
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, n)]
        return [i for i in range(1, n)] + [0]

    def _batch_allowed_on_pool0():
        # Allow batch on pool 0 only if pool 0 would otherwise idle for a while
        # (soft policy: batch waits long enough => permitted).
        # Since we don't have idle history, use queue waiting as a proxy.
        if not s.q_batch:
            return False
        # If any batch pipeline has waited long enough, we allow it.
        for p in list(s.q_batch)[:min(len(s.q_batch), 16)]:
            enq = s.pipeline_enqueued_at.get(p.pipeline_id, s.tick)
            if (s.tick - enq) >= s.batch_soft_promote_ticks:
                return True
        return False

    def _caps(prio, pool):
        # Caps are fractions of pool max for a single operator assignment.
        # QUERY gets large slices (reduce latency); INTERACTIVE moderate; BATCH small to improve concurrency.
        if prio == Priority.QUERY:
            return 0.95, 0.95
        if prio == Priority.INTERACTIVE:
            return 0.85, 0.90
        return 0.35, 0.45

    def _default_slices(prio, pool):
        # Start values before hints/caps. Favor CPU for latency-sensitive ops.
        if prio == Priority.QUERY:
            base_cpu = min(max(s.min_cpu, 4), pool.max_cpu_pool)
            base_ram = min(max(s.min_ram, 4), pool.max_ram_pool)
        elif prio == Priority.INTERACTIVE:
            base_cpu = min(max(s.min_cpu, 2), pool.max_cpu_pool)
            base_ram = min(max(s.min_ram, 4), pool.max_ram_pool)
        else:
            base_cpu = min(max(s.min_cpu, 1), pool.max_cpu_pool)
            base_ram = min(max(s.min_ram, 2), pool.max_ram_pool)
        return base_cpu, base_ram

    def _size_for_op(p, op, pool_id, effective_prio):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        cpu_cap_frac, ram_cap_frac = _caps(effective_prio, pool)
        cpu_cap = max(s.min_cpu, int(pool.max_cpu_pool * cpu_cap_frac))
        ram_cap = max(s.min_ram, int(pool.max_ram_pool * ram_cap_frac))

        base_cpu, base_ram = _default_slices(effective_prio, pool)
        key = (p.pipeline_id, _op_key(op))
        hint_cpu = int(s.op_cpu_hint.get(key, base_cpu) or base_cpu)
        hint_ram = int(s.op_ram_hint.get(key, base_ram) or base_ram)

        req_cpu = max(base_cpu, hint_cpu)
        req_ram = max(base_ram, hint_ram)

        # Apply caps and availability
        req_cpu = max(s.min_cpu, min(req_cpu, cpu_cap, avail_cpu))
        req_ram = max(s.min_ram, min(req_ram, ram_cap, avail_ram))

        return req_cpu, req_ram

    def _learn_from_results():
        for r in results:
            if not (hasattr(r, "failed") and r.failed()):
                continue

            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                # Can't key hints reliably
                continue

            err = str(getattr(r, "error", "") or "")
            oom_like = ("oom" in err.lower()) or ("out of memory" in err.lower())

            ran_cpu = int(getattr(r, "cpu", 1) or 1)
            ran_ram = int(getattr(r, "ram", 1) or 1)
            ops = getattr(r, "ops", []) or []

            for op in ops:
                key = (pid, _op_key(op))
                prev_ram = int(s.op_ram_hint.get(key, ran_ram) or ran_ram)
                prev_cpu = int(s.op_cpu_hint.get(key, ran_cpu) or ran_cpu)

                if oom_like:
                    # RAM is usually the decisive factor; double it (bounded later by pool caps)
                    s.op_ram_hint[key] = max(prev_ram, max(ran_ram, 1) * 2)
                    # Keep CPU stable on OOM
                    s.op_cpu_hint[key] = prev_cpu
                else:
                    # Conservative bump on non-OOM failures
                    s.op_ram_hint[key] = max(prev_ram, int(max(ran_ram, 1) * 1.25) + 1)
                    s.op_cpu_hint[key] = max(prev_cpu, int(max(ran_cpu, 1) * 1.10) + 1)

    def _pop_best_runnable_from_queue(q):
        """
        Pick the "best" runnable pipeline from this priority queue:
          - must not be completed
          - must have at least one ready op
          - among candidates, choose smallest remaining_work_score (SRPT-ish)
        Non-runnable pipelines are rotated back (stable).
        """
        best = None
        best_ops = None
        best_score = None

        n = len(q)
        for _ in range(n):
            p = q.popleft()

            if _pipeline_done(p):
                _drop_pipeline_bookkeeping(p.pipeline_id)
                continue

            ops = _ready_ops_one(p)
            if not ops:
                q.append(p)
                continue

            score = _remaining_work_score(p)
            if best is None or score < best_score:
                if best is not None:
                    q.append(best)
                best = p
                best_ops = ops
                best_score = score
            else:
                q.append(p)

        return best, best_ops

    # Ingest new arrivals
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # Learn from previous tick
    _learn_from_results()

    suspensions = []
    assignments = []

    # Global strict priority pass: attempt to fill pools for QUERY, then INTERACTIVE, then BATCH.
    # We pack multiple ops per pool while resources remain.
    prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    for prio in prio_order:
        q = _queue_for_prio(prio)

        # Nothing to schedule at this priority
        if not q:
            continue

        # Determine pool iteration order for this priority
        pool_ids = _pool_order_for_priority(prio)

        for pool_id in pool_ids:
            pool = s.executor.pools[pool_id]

            # Batch avoidance on pool 0 unless allowed (when multiple pools exist)
            if (s.executor.num_pools > 1) and (pool_id == 0) and (prio == Priority.BATCH_PIPELINE):
                if not _batch_allowed_on_pool0():
                    continue

            # Pack as many as we can in this pool for this priority.
            # Stop when queue yields no runnable pipeline or pool exhausted.
            while pool.avail_cpu_pool >= s.min_cpu and pool.avail_ram_pool >= s.min_ram and len(q) > 0:
                p, ops = _pop_best_runnable_from_queue(q)
                if p is None:
                    break

                # Size and assign
                op0 = ops[0]
                req_cpu, req_ram = _size_for_op(p, op0, pool_id, effective_prio=prio)
                if req_cpu <= 0 or req_ram <= 0:
                    # Can't place now; put back and stop packing this pool (avoid tight loops)
                    q.append(p)
                    break

                assignments.append(
                    Assignment(
                        ops=ops,
                        cpu=req_cpu,
                        ram=req_ram,
                        priority=p.priority,  # preserve original priority for accounting
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # Requeue pipeline for subsequent ops
                q.append(p)

                # Refresh pool view (avail changes after tentative assignment in simulator)
                pool = s.executor.pools[pool_id]

    return suspensions, assignments
