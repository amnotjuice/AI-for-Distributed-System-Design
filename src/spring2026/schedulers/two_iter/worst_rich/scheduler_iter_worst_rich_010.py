# policy_key: scheduler_iter_worst_rich_010
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044500
# generation_seconds: 30.35
# generated_at: 2026-04-12T00:54:30.817932
@register_scheduler_init(key="scheduler_iter_worst_rich_010")
def scheduler_iter_worst_rich_010_init(s):
    """
    Iteration 2: Fix catastrophic OOM + make priority actually help weighted latency.

    Key changes vs prior attempt:
      - Stop allocating tiny fixed RAM slices (caused near-100% OOM). Instead allocate a large
        fraction of *available* RAM (extra RAM is "free" in model; only under-alloc OOMs).
      - Do NOT drop pipelines just because some operator is FAILED; treat FAILED as retryable
        (ASSIGNABLE_STATES includes FAILED). Learn per-op RAM hints from OOM and retry.
      - Priority queues (QUERY > INTERACTIVE > BATCH). Multi-pool: bias pool 0 for latency work.
      - Keep batch from stealing all CPU (cap CPU), but still give it ample RAM to avoid OOM.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # pipeline_id -> first tick seen (for mild aging)
    s.enqueued_at = {}

    # (pipeline_id, op_key) -> learned minimum RAM to avoid OOM (best-effort)
    s.op_ram_hint = {}

    s.tick = 0

    # Mild anti-starvation: after enough waits, allow batch to be treated as interactive
    # (kept large to avoid harming weighted latency)
    s.batch_promote_ticks = 500

    # Guardrails
    s.min_cpu = 1
    s.min_ram = 1


@register_scheduler(key="scheduler_iter_worst_rich_010")
def scheduler_iter_worst_rich_010_scheduler(s, results, pipelines):
    """
    Priority-aware, OOM-avoiding allocator.

    Strategy:
      - Always schedule highest priority runnable ops first.
      - Allocate RAM aggressively (large fraction of currently available RAM) to avoid OOM.
      - On OOM failure, double the per-op RAM hint (bounded by pool max) for subsequent retries.
      - Allocate CPU more conservatively for BATCH to preserve headroom for interactive latency.
      - One assignment per pool per tick (like baseline template), but with correct retry behavior.
    """
    s.tick += 1

    # ---------- helpers ----------
    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.enqueued_at:
            s.enqueued_at[pid] = s.tick
            _queue_for_priority(p.priority).append(p)

    def _drop_pipeline(p):
        pid = p.pipeline_id
        if pid in s.enqueued_at:
            del s.enqueued_at[pid]
        # lazy removal from deques (we won't try to purge)

    def _effective_priority(p):
        # Very mild aging for batch only (avoid starvation without disturbing weighted latency much).
        if p.priority != Priority.BATCH_PIPELINE:
            return p.priority
        waited = s.tick - s.enqueued_at.get(p.pipeline_id, s.tick)
        if waited >= s.batch_promote_ticks:
            return Priority.INTERACTIVE
        return p.priority

    def _prio_order_deques():
        # We choose based on *effective* priority at selection time, but keep
        # deques segregated by base priority for simplicity.
        return (s.q_query, s.q_interactive, s.q_batch)

    def _op_key(op):
        # Best-effort stable identity.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _learn_from_results():
        # Learn RAM hints from OOM-like failures. This is crucial for convergence.
        for r in results:
            if not (hasattr(r, "failed") and r.failed()):
                continue

            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                continue

            err = str(getattr(r, "error", "") or "").lower()
            oom_like = ("oom" in err) or ("out of memory" in err) or ("memory" in err)

            if not oom_like:
                continue

            ran_ops = getattr(r, "ops", []) or []
            prev_ram_req = int(getattr(r, "ram", 0) or 0)
            if prev_ram_req <= 0:
                prev_ram_req = s.min_ram

            for op in ran_ops:
                k = (pid, _op_key(op))
                old = int(s.op_ram_hint.get(k, prev_ram_req))
                # Double on OOM; this converges quickly.
                s.op_ram_hint[k] = max(old, prev_ram_req) * 2

    def _pipeline_is_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _get_ready_ops(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]

    def _preferred_pools(eff_prio):
        # pool 0 is treated as latency pool when multiple pools exist
        n = s.executor.num_pools
        if n <= 1:
            return [0] if n == 1 else []
        if eff_prio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + list(range(1, n))
        # batch prefers non-0 pools first
        return list(range(1, n)) + [0]

    def _size_request(p, op, pool_id):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = int(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        eff = _effective_priority(p)

        # CPU: keep batch from grabbing all CPU, but let interactive use more.
        if eff == Priority.QUERY:
            cpu_frac = 1.0
        elif eff == Priority.INTERACTIVE:
            cpu_frac = 0.75
        else:
            cpu_frac = 0.50

        # RAM: allocate aggressively to avoid OOM (OOM dominated prior attempt).
        # Extra RAM has no performance cost in this simulator; under-alloc is catastrophic.
        if eff == Priority.QUERY:
            ram_frac = 0.95
        elif eff == Priority.INTERACTIVE:
            ram_frac = 0.90
        else:
            ram_frac = 0.85

        # Start from fractions of currently available resources (not max), so we don't request
        # more than exists in the pool at scheduling time.
        req_cpu = max(s.min_cpu, int(avail_cpu * cpu_frac))
        req_ram = max(s.min_ram, int(avail_ram * ram_frac))

        # Apply learned per-op RAM hint (bounded by pool max and current availability).
        k = (p.pipeline_id, _op_key(op))
        hint_ram = int(s.op_ram_hint.get(k, 0) or 0)
        if hint_ram > 0:
            req_ram = max(req_ram, hint_ram)

        # Never exceed availability.
        req_cpu = min(req_cpu, avail_cpu)
        req_ram = min(req_ram, avail_ram)

        # If we still ended up with zeros (due to tiny availability), refuse.
        if req_cpu <= 0 or req_ram <= 0:
            return 0, 0
        return req_cpu, req_ram

    # ---------- ingest ----------
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ---------- learning ----------
    _learn_from_results()

    # ---------- scheduling ----------
    suspensions = []
    assignments = []

    # We schedule per pool, choosing the best runnable pipeline for that pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        chosen_p = None
        chosen_ops = None

        # We do a bounded scan of the deques to find runnable work.
        # For pool 0, avoid taking batch if higher priority runnable exists.
        # For other pools, allow batch earlier.
        # We implement this by two passes: first "preferred priorities", then "anything".
        def _try_pick(allow_batch_on_pool0):
            nonlocal chosen_p, chosen_ops
            for q in _prio_order_deques():
                # optionally skip batch on pool0 in first pass
                if (not allow_batch_on_pool0) and pool_id == 0 and q is s.q_batch and s.executor.num_pools > 1:
                    continue

                for _ in range(len(q)):
                    p = q.popleft()

                    if _pipeline_is_done(p):
                        _drop_pipeline(p)
                        continue

                    eff = _effective_priority(p)
                    # If there are multiple pools, respect coarse pool preference:
                    # if this pool is not the first preference for the pipeline, keep it in queue.
                    prefs = _preferred_pools(eff)
                    if prefs and prefs[0] != pool_id and s.executor.num_pools > 1:
                        q.append(p)
                        continue

                    ops = _get_ready_ops(p)
                    if not ops:
                        q.append(p)
                        continue

                    chosen_p = p
                    chosen_ops = ops
                    # Requeue immediately (like baseline); we only schedule one op from it now.
                    q.append(p)
                    return

        _try_pick(allow_batch_on_pool0=False)
        if chosen_p is None:
            _try_pick(allow_batch_on_pool0=True)

        if chosen_p is None:
            continue

        op = chosen_ops[0]
        req_cpu, req_ram = _size_request(chosen_p, op, pool_id)
        if req_cpu <= 0 or req_ram <= 0:
            continue

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=req_cpu,
                ram=req_ram,
                priority=chosen_p.priority,
                pool_id=pool_id,
                pipeline_id=chosen_p.pipeline_id,
            )
        )

    return suspensions, assignments
