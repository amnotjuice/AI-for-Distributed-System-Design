# policy_key: scheduler_iter_worst_rich_001
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041448
# generation_seconds: 28.17
# generated_at: 2026-04-12T00:48:58.503374
@register_scheduler_init(key="scheduler_iter_worst_rich_001")
def scheduler_iter_worst_rich_001_init(s):
    """
    Iteration 2: Fix catastrophic OOMs first, then improve weighted latency.

    Key changes vs the prior attempt:
      - Stop under-allocating RAM: default to "give the op almost all currently available RAM"
        (OOMs were dominating and prevented any completions).
      - Priority-aware selection (QUERY > INTERACTIVE > BATCH) across pools, with mild pool affinity:
        keep pool 0 preferentially for QUERY/INTERACTIVE when multiple pools exist.
      - Simple per-op RAM backoff on OOM: next retry demands more RAM (bounded by pool max).
      - Optional headroom reservation on pool 0 to reduce interference from BATCH.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # pipeline_id -> tick enqueued (for optional aging in later iterations)
    s.pipeline_enqueued_at = {}

    # (pipeline_id, op_key) -> ram_hint
    s.op_ram_hint = {}

    s.tick = 0

    # Keep some headroom in pool 0 for latency-sensitive work (only affects BATCH on pool 0)
    s.pool0_cpu_reserve_frac = 0.20
    s.pool0_ram_reserve_frac = 0.20

    # Minimal slices (avoid zero allocations)
    s.min_cpu_slice = 1
    s.min_ram_slice = 1


@register_scheduler(key="scheduler_iter_worst_rich_001")
def scheduler_iter_worst_rich_001_scheduler(s, results, pipelines):
    """
    Priority-aware greedy scheduler with "allocate big to avoid OOM".

    Strategy per tick:
      1) Enqueue new pipelines into per-priority queues.
      2) Learn from failures: if OOM-like, increase RAM hint for the specific operator.
      3) For each pool, choose the highest-priority pipeline that has a ready op.
      4) Allocate CPU/RAM aggressively (near all currently available) to avoid OOM and finish quickly.
         - Exception: BATCH in pool 0 respects a reserve so QUERY/INTERACTIVE can start promptly.
    """
    s.tick += 1

    # ---- Helpers ----
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

    def _drop_pipeline(pid):
        if pid in s.pipeline_enqueued_at:
            del s.pipeline_enqueued_at[pid]

    def _op_key(op):
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _is_done_or_failed(p):
        st = p.runtime_status()
        # Keep baseline semantics: pipelines with failures can still have FAILED ops re-assigned,
        # but if simulator marks terminal failure, it should appear as no assignable ops.
        return st.is_pipeline_successful()

    def _ready_ops(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return ops[:1] if ops else []

    def _learn_from_results():
        for r in results:
            if not (hasattr(r, "failed") and r.failed()):
                continue

            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                continue

            err = str(getattr(r, "error", "") or "")
            oom_like = ("oom" in err.lower()) or ("out of memory" in err.lower())

            if not oom_like:
                continue

            ran_ops = getattr(r, "ops", []) or []
            prev_ram = int(getattr(r, "ram", 0) or 0)
            if prev_ram <= 0:
                prev_ram = s.min_ram_slice

            # OOM backoff: next time ask for 2x the RAM we used when it OOMed.
            for op in ran_ops:
                key = (pid, _op_key(op))
                s.op_ram_hint[key] = max(s.op_ram_hint.get(key, s.min_ram_slice), prev_ram * 2)

    def _pool_order_for_priority(prio):
        # Mild affinity: when multiple pools, treat pool 0 as latency pool.
        if s.executor.num_pools <= 1:
            return list(range(s.executor.num_pools))
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, s.executor.num_pools)]
        return [i for i in range(1, s.executor.num_pools)] + [0]

    def _pick_pipeline_for_pool(pool_id):
        # Scan queues in strict priority order; bounded scan within each queue.
        # Lazy removal: pipelines stay in queue; we rotate items to preserve fairness within priority.
        for q in (s.q_query, s.q_interactive, s.q_batch):
            for _ in range(len(q)):
                p = q.popleft()

                if _is_done_or_failed(p):
                    _drop_pipeline(p.pipeline_id)
                    continue

                ops = _ready_ops(p)
                if not ops:
                    q.append(p)
                    continue

                # Found a runnable pipeline for this pool
                return p, ops, q

                # (unreachable)
        return None, None, None

    def _compute_request(p, op, pool_id):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = int(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        # Start by trying to consume almost all available resources.
        req_cpu = max(s.min_cpu_slice, avail_cpu)
        req_ram = max(s.min_ram_slice, avail_ram)

        # Apply BATCH reserve on pool 0 (to keep headroom for QUERY/INTERACTIVE arrivals).
        if s.executor.num_pools > 1 and pool_id == 0 and p.priority == Priority.BATCH_PIPELINE:
            cpu_reserve = int(pool.max_cpu_pool * s.pool0_cpu_reserve_frac)
            ram_reserve = int(pool.max_ram_pool * s.pool0_ram_reserve_frac)
            req_cpu = max(s.min_cpu_slice, min(req_cpu, max(s.min_cpu_slice, avail_cpu - cpu_reserve)))
            req_ram = max(s.min_ram_slice, min(req_ram, max(s.min_ram_slice, avail_ram - ram_reserve)))

        # Respect learned RAM hint (especially after OOM): ask for at least the hint if possible.
        hint = s.op_ram_hint.get((p.pipeline_id, _op_key(op)), 0)
        if hint and hint > req_ram:
            # Can't exceed available; if hint exceeds availability, take all we can (still may OOM,
            # but this drives the system to allocate maximum feasible RAM for that op).
            req_ram = min(avail_ram, hint)

        # Clamp to pool maxima defensively (avail already implies this, but keep safe)
        req_cpu = max(s.min_cpu_slice, min(req_cpu, int(pool.max_cpu_pool)))
        req_ram = max(s.min_ram_slice, min(req_ram, int(pool.max_ram_pool)))

        return req_cpu, req_ram

    # ---- Ingest ----
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ---- Learn ----
    _learn_from_results()

    suspensions = []
    assignments = []

    # ---- Assign ----
    # We iterate pools but bias which pools get first crack at high priority by reordering pools
    # (small effect, but helps weighted latency when contention exists).
    pool_sequence = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1:
        # Let pool 0 get scheduled first (latency pool)
        pool_sequence = [0] + [i for i in range(1, s.executor.num_pools)]

    for pool_id in pool_sequence:
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # For this pool, try to pick a pipeline; if pool 0 is busy, that's fine.
        p, ops, originating_q = _pick_pipeline_for_pool(pool_id)
        if p is None:
            continue

        op = ops[0]
        req_cpu, req_ram = _compute_request(p, op, pool_id)

        if req_cpu <= 0 or req_ram <= 0:
            # Put it back and move on
            originating_q.append(p)
            continue

        assignments.append(
            Assignment(
                ops=ops,
                cpu=req_cpu,
                ram=req_ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
        )

        # Requeue pipeline for subsequent ops
        originating_q.append(p)

    return suspensions, assignments
