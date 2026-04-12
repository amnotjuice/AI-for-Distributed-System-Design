# policy_key: scheduler_iter_worst_simple_013
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051850
# generation_seconds: 42.43
# generated_at: 2026-04-12T00:43:22.066821
@register_scheduler_init(key="scheduler_iter_worst_simple_013")
def scheduler_iter_worst_simple_013_init(s):
    """
    Iteration 2: Priority-first packing with headroom reservation + multi-dispatch per pool.

    Key changes vs previous iteration:
      - Never drop a pipeline just because it has FAILED ops; FAILED is assignable in this simulator.
      - Dispatch multiple assignments per pool per tick (until headroom exhausted).
      - Hard preference: keep pool 0 mostly for QUERY/INTERACTIVE; only run BATCH there if no
        latency-sensitive work is runnable.
      - Reserve CPU/RAM headroom for high priority so BATCH can't consume the whole pool.
      - More aggressive sizing for QUERY (finish fast), modest for INTERACTIVE, minimal for BATCH.
      - Keep simple OOM backoff via per-op RAM hints learned from failures.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    s.pipeline_seen = set()  # pipeline_id seen/enqueued

    # Per-op learned resource hints (best-effort)
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}  # (pipeline_id, op_key) -> cpu

    # Scheduler tick counter
    s.tick = 0

    # Simple anti-starvation: promote old batch
    s.pipeline_enqueued_at = {}  # pipeline_id -> tick
    s.batch_promotion_ticks = 150

    # Minimal allocation slices
    s.min_cpu_slice = 1
    s.min_ram_slice = 1


@register_scheduler(key="scheduler_iter_worst_simple_013")
def scheduler_iter_worst_simple_013_scheduler(s, results, pipelines):
    """
    Priority-aware, headroom-preserving, multi-dispatch scheduler.

    Strategy:
      - Maintain 3 queues (QUERY > INTERACTIVE > BATCH).
      - For each pool, repeatedly pick next runnable op, preferring higher priority and
        respecting pool specialization (pool 0 for latency work).
      - Enforce reserved headroom when scheduling BATCH so new QUERY/INTERACTIVE can start quickly.
      - Learn RAM hints on failures (OOM-like => double RAM) and retry FAILED ops.
    """
    from collections import deque

    s.tick += 1

    # ---------- helpers ----------
    def _op_key(op):
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
        if pid in s.pipeline_seen:
            return
        s.pipeline_seen.add(pid)
        s.pipeline_enqueued_at[pid] = s.tick
        _queue_for_priority(p.priority).append(p)

    def _effective_priority(p):
        # Promote batch that waited too long into interactive for dispatch only.
        if p.priority == Priority.BATCH_PIPELINE:
            waited = s.tick - s.pipeline_enqueued_at.get(p.pipeline_id, s.tick)
            if waited >= s.batch_promotion_ticks:
                return Priority.INTERACTIVE
        return p.priority

    def _pipeline_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _ready_ops_one(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]

    def _learn_from_results():
        for r in results:
            if not (hasattr(r, "failed") and r.failed()):
                continue
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                continue

            err = str(getattr(r, "error", "") or "")
            oom_like = ("oom" in err.lower()) or ("out of memory" in err.lower())

            ran_ops = getattr(r, "ops", []) or []
            used_ram = int(getattr(r, "ram", 1) or 1)
            used_cpu = int(getattr(r, "cpu", 1) or 1)

            for op in ran_ops:
                k = (pid, _op_key(op))
                prev_ram = int(s.op_ram_hint.get(k, used_ram))
                prev_cpu = int(s.op_cpu_hint.get(k, used_cpu))

                if oom_like:
                    s.op_ram_hint[k] = max(prev_ram, used_ram * 2)
                    s.op_cpu_hint[k] = max(prev_cpu, used_cpu)
                else:
                    # Small bump if it failed for other reasons; keep bounded by later sizing.
                    s.op_ram_hint[k] = max(prev_ram, int(used_ram * 1.25) + 1)
                    s.op_cpu_hint[k] = max(prev_cpu, int(used_cpu * 1.10) + 1)

    def _reserved_headroom(pool_id, pool):
        # Keep headroom for high-priority arrivals, most strongly on pool 0.
        # Values are fractions of pool max, not of current availability.
        if s.executor.num_pools <= 1:
            # With one pool, reserve a bit but not too much to avoid underutilization.
            return int(pool.max_cpu_pool * 0.20), int(pool.max_ram_pool * 0.20)

        if pool_id == 0:
            # Latency pool: reserve significant headroom.
            return int(pool.max_cpu_pool * 0.50), int(pool.max_ram_pool * 0.50)
        else:
            # Throughput pools: reserve modest headroom.
            return int(pool.max_cpu_pool * 0.15), int(pool.max_ram_pool * 0.15)

    def _size_request(p, op, pool_id, pool, allow_full_use):
        """
        allow_full_use:
          - True for QUERY/INTERACTIVE (can consume most of avail)
          - False for BATCH (must respect reserved headroom)
        """
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = int(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        eff = _effective_priority(p)

        # Caps (fractions of pool max) to avoid giant single allocations.
        if eff == Priority.QUERY:
            cpu_cap = int(pool.max_cpu_pool * 0.95)
            ram_cap = int(pool.max_ram_pool * 0.95)
            base_cpu = max(s.min_cpu_slice, min(8, pool.max_cpu_pool))
            base_ram = max(s.min_ram_slice, min(8, pool.max_ram_pool))
        elif eff == Priority.INTERACTIVE:
            cpu_cap = int(pool.max_cpu_pool * 0.80)
            ram_cap = int(pool.max_ram_pool * 0.85)
            base_cpu = max(s.min_cpu_slice, min(4, pool.max_cpu_pool))
            base_ram = max(s.min_ram_slice, min(6, pool.max_ram_pool))
        else:
            cpu_cap = int(pool.max_cpu_pool * 0.50)
            ram_cap = int(pool.max_ram_pool * 0.60)
            base_cpu = s.min_cpu_slice  # minimal to keep batch from blocking latency work
            base_ram = max(s.min_ram_slice, min(4, pool.max_ram_pool))

        k = (p.pipeline_id, _op_key(op))
        hint_cpu = int(s.op_cpu_hint.get(k, base_cpu))
        hint_ram = int(s.op_ram_hint.get(k, base_ram))

        req_cpu = max(base_cpu, hint_cpu)
        req_ram = max(base_ram, hint_ram)

        # Respect caps
        req_cpu = min(req_cpu, max(s.min_cpu_slice, cpu_cap))
        req_ram = min(req_ram, max(s.min_ram_slice, ram_cap))

        # Respect reservation for batch (or when allow_full_use is False)
        if not allow_full_use:
            res_cpu, res_ram = _reserved_headroom(pool_id, pool)
            budget_cpu = max(0, avail_cpu - res_cpu)
            budget_ram = max(0, avail_ram - res_ram)
            if budget_cpu < s.min_cpu_slice or budget_ram < s.min_ram_slice:
                return 0, 0
            req_cpu = min(req_cpu, budget_cpu)
            req_ram = min(req_ram, budget_ram)

        # Finally bound by availability
        req_cpu = max(0, min(req_cpu, avail_cpu))
        req_ram = max(0, min(req_ram, avail_ram))

        if req_cpu < s.min_cpu_slice or req_ram < s.min_ram_slice:
            return 0, 0
        return req_cpu, req_ram

    def _has_runnable_latency_work():
        # Quick check: are there any runnable ops in query/interactive queues?
        # Bounded scan (does not reorder permanently; we rotate back).
        for q in (s.q_query, s.q_interactive):
            n = len(q)
            for _ in range(n):
                p = q.popleft()
                if _pipeline_done(p):
                    continue
                ops = _ready_ops_one(p)
                q.append(p)
                if ops:
                    return True
        return False

    # ---------- ingest ----------
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ---------- learn ----------
    _learn_from_results()

    suspensions = []
    assignments = []

    # Determine if pool 0 should be protected aggressively this tick
    runnable_latency_exists = _has_runnable_latency_work()

    # ---------- dispatch ----------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Try to fill the pool with multiple assignments until we run out of usable headroom.
        # Hard cap: avoid pathological long loops when many tiny ops exist.
        max_dispatch = 32
        dispatched = 0

        while dispatched < max_dispatch:
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

            chosen = None
            chosen_ops = None
            chosen_eff = None

            # Pool specialization:
            # - If pool 0 and any latency work runnable, only consider QUERY/INTERACTIVE first.
            # - Otherwise: consider all priorities.
            if s.executor.num_pools > 1 and pool_id == 0 and runnable_latency_exists:
                queue_order = (s.q_query, s.q_interactive, s.q_batch)
            else:
                queue_order = (s.q_query, s.q_interactive, s.q_batch)

            # Pick the first runnable pipeline from the highest priority queue (bounded scan).
            for q in queue_order:
                n = len(q)
                for _ in range(n):
                    p = q.popleft()

                    # Drop completed pipelines from bookkeeping (lazy queue cleanup)
                    if _pipeline_done(p):
                        continue

                    ops = _ready_ops_one(p)
                    if not ops:
                        q.append(p)
                        continue

                    chosen = p
                    chosen_ops = ops
                    chosen_eff = _effective_priority(p)
                    # Requeue chosen immediately to preserve fairness among pipelines.
                    q.append(p)
                    break
                if chosen is not None:
                    break

            if chosen is None:
                break

            # Decide whether this assignment can use "full" available resources.
            allow_full_use = chosen_eff in (Priority.QUERY, Priority.INTERACTIVE)

            # Additional rule: keep pool 0 "batch-free" when latency work exists.
            if (
                s.executor.num_pools > 1
                and pool_id == 0
                and runnable_latency_exists
                and chosen_eff == Priority.BATCH_PIPELINE
            ):
                # Can't place batch here right now.
                break

            op = chosen_ops[0]
            req_cpu, req_ram = _size_request(chosen, op, pool_id, pool, allow_full_use=allow_full_use)
            if req_cpu <= 0 or req_ram <= 0:
                # Can't fit anything sensible right now.
                break

            # Clamp any learned hints to pool maxima to prevent runaway.
            k = (chosen.pipeline_id, _op_key(op))
            if k in s.op_ram_hint:
                s.op_ram_hint[k] = min(int(s.op_ram_hint[k]), int(pool.max_ram_pool))
            if k in s.op_cpu_hint:
                s.op_cpu_hint[k] = min(int(s.op_cpu_hint[k]), int(pool.max_cpu_pool))

            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=chosen.priority,  # preserve original priority label
                    pool_id=pool_id,
                    pipeline_id=chosen.pipeline_id,
                )
            )
            dispatched += 1

    return suspensions, assignments
