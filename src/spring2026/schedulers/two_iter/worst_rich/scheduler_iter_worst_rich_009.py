# policy_key: scheduler_iter_worst_rich_009
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052270
# generation_seconds: 35.70
# generated_at: 2026-04-12T00:54:00.461966
@register_scheduler_init(key="scheduler_iter_worst_rich_009")
def scheduler_iter_worst_rich_009_init(s):
    """
    Priority-aware, OOM-avoiding scheduler (incremental improvement over naive FIFO).

    Key fixes vs prior attempt:
      - Never drop a pipeline just because it has FAILED ops; FAILED is retryable in this simulator.
      - Allocate RAM aggressively (up to all available in the pool) because extra RAM has no perf penalty
        and under-allocation causes catastrophic OOM + zero throughput.
      - Learn per-operator RAM hints from OOM failures and prefer pools with more RAM headroom.
      - Use strict priority ordering for weighted latency: QUERY > INTERACTIVE > BATCH.
      - Keep batching conservative on CPU only (optional), but not on RAM.

    Design goal: first restore completions (avoid OOM storm), then reduce weighted latency via priority.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # pipeline_id -> last seen Pipeline object (so results can re-enqueue if needed)
    s.pipeline_by_id = {}

    # pipeline_id -> enqueued tick (for mild aging)
    s.enqueued_at = {}

    # Per-op RAM hint inferred from OOMs. Key: (pipeline_id, op_key) -> hint_ram
    s.op_ram_hint = {}

    # Track consecutive OOMs for an op to escalate quickly
    s.op_oom_count = {}

    s.tick = 0

    # Aging: after this many ticks, treat BATCH as INTERACTIVE to avoid infinite starvation
    s.batch_aging_ticks = 400

    # Minimum slices (defensive)
    s.min_cpu = 1
    s.min_ram = 1


@register_scheduler(key="scheduler_iter_worst_rich_009")
def scheduler_iter_worst_rich_009_scheduler(s, results, pipelines):
    """
    One scheduling tick:
      1) Ingest new pipelines into per-priority queues.
      2) Update RAM hints on OOM failures (exponential backoff) to prevent repeated OOM.
      3) For each pool, pick the highest effective-priority runnable op that "fits" by RAM hint.
         - Prefer pools with more available RAM for high-priority work.
      4) Assign with RAM = as much as possible (all pool available), CPU = most available
         (batch may be capped slightly to preserve headroom).
    """
    s.tick += 1

    def _prio_rank(p):
        if p == Priority.QUERY:
            return 3
        if p == Priority.INTERACTIVE:
            return 2
        return 1  # BATCH_PIPELINE or others

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _effective_priority(pipeline):
        # Mild anti-starvation: age BATCH into INTERACTIVE
        if pipeline.priority != Priority.BATCH_PIPELINE:
            return pipeline.priority
        enq = s.enqueued_at.get(pipeline.pipeline_id, s.tick)
        if (s.tick - enq) >= s.batch_aging_ticks:
            return Priority.INTERACTIVE
        return pipeline.priority

    def _op_key(op):
        # Best-effort stable identifier.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _is_oom_result(r):
        err = str(getattr(r, "error", "") or "")
        e = err.lower()
        return ("oom" in e) or ("out of memory" in e) or (e.strip() == "oom")

    def _learn_from_results():
        # On OOM: raise RAM hint for each op in the container; escalate quickly to avoid storms.
        for r in results:
            if not (hasattr(r, "failed") and r.failed()):
                continue

            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                continue

            if not _is_oom_result(r):
                continue

            ran_ops = getattr(r, "ops", None) or []
            # If ops list is absent, we can't key per-op; skip.
            for op in ran_ops:
                key = (pid, _op_key(op))
                prev_hint = int(s.op_ram_hint.get(key, max(s.min_ram, int(getattr(r, "ram", 1) or 1))))
                prev_ooms = int(s.op_oom_count.get(key, 0)) + 1
                s.op_oom_count[key] = prev_ooms

                # Exponential backoff; after a couple OOMs, jump aggressively.
                if prev_ooms <= 1:
                    new_hint = max(prev_hint * 2, prev_hint + 1)
                elif prev_ooms == 2:
                    new_hint = max(prev_hint * 3, prev_hint + 4)
                else:
                    new_hint = max(prev_hint * 4, prev_hint + 8)

                s.op_ram_hint[key] = new_hint

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        s.pipeline_by_id[pid] = p
        if pid not in s.enqueued_at:
            s.enqueued_at[pid] = s.tick
            _queue_for_priority(p.priority).append(p)

    def _pipeline_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _next_ready_op(p):
        st = p.runtime_status()
        # Allow retry of FAILED ops; only require parents complete.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _ram_hint_for(p, op):
        # If no hint, assume it can fit.
        return int(s.op_ram_hint.get((p.pipeline_id, _op_key(op)), 0))

    def _fits(pool, hint_ram):
        # hint_ram==0 means unknown -> assume it fits.
        if hint_ram <= 0:
            return pool.avail_ram_pool > 0 and pool.avail_cpu_pool > 0
        return (pool.avail_ram_pool >= hint_ram) and (pool.avail_cpu_pool > 0)

    def _pick_cpu_for(priority, pool):
        avail = int(pool.avail_cpu_pool)
        if avail <= 0:
            return 0
        # Let high-priority take the pool to reduce latency; batch slightly capped.
        if priority == Priority.BATCH_PIPELINE and s.executor.num_pools > 1:
            # Keep some headroom for unexpected interactive arrivals.
            return max(s.min_cpu, int(max(1, avail * 0.75)))
        return max(s.min_cpu, avail)

    def _pick_ram_for(pool):
        # Critical: allocate as much RAM as possible to avoid OOM storms.
        avail = int(pool.avail_ram_pool)
        if avail <= 0:
            return 0
        return max(s.min_ram, avail)

    # 1) Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # 2) Learn from failures
    if results:
        _learn_from_results()

    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Build pool order for each tick:
    # - For latency, we try to place QUERY/INTERACTIVE on the pool with the most RAM available.
    pool_ids_by_avail_ram = list(range(s.executor.num_pools))
    pool_ids_by_avail_ram.sort(
        key=lambda i: (s.executor.pools[i].avail_ram_pool, s.executor.pools[i].avail_cpu_pool),
        reverse=True,
    )

    # Helper to pull one runnable pipeline from queues that can fit on a given pool.
    def _pop_runnable_for_pool(pool_id):
        pool = s.executor.pools[pool_id]

        # Scan queues in strict effective-priority order; rotate to preserve FIFO-ish behavior.
        queues = (s.q_query, s.q_interactive, s.q_batch)
        for q in queues:
            n = len(q)
            for _ in range(n):
                p = q.popleft()

                # Drop completed pipelines from bookkeeping; do not treat FAILED as terminal.
                if _pipeline_done(p):
                    pid = p.pipeline_id
                    if pid in s.enqueued_at:
                        del s.enqueued_at[pid]
                    # keep pipeline_by_id; harmless
                    continue

                op = _next_ready_op(p)
                if op is None:
                    # Not runnable yet; keep it in the same queue.
                    q.append(p)
                    continue

                # Apply aging effect only for selection (not changing pipeline.priority itself).
                eff_prio = _effective_priority(p)
                hint = _ram_hint_for(p, op)

                if _fits(pool, hint):
                    return p, op, eff_prio

                # Doesn't fit this pool right now; keep it in queue.
                q.append(p)
        return None, None, None

    # 3) Assign at most one op per pool per tick (keeps policy simple and stable).
    for pool_id in pool_ids_by_avail_ram:
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        p, op, eff_prio = _pop_runnable_for_pool(pool_id)
        if p is None:
            continue

        cpu = _pick_cpu_for(p.priority, pool)
        ram = _pick_ram_for(pool)

        if cpu <= 0 or ram <= 0:
            # Put it back (best-effort) and skip.
            _queue_for_priority(p.priority).append(p)
            continue

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
        )

        # Requeue pipeline so subsequent ops can be scheduled later.
        _queue_for_priority(p.priority).append(p)

    return suspensions, assignments
