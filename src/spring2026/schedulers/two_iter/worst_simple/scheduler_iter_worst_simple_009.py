# policy_key: scheduler_iter_worst_simple_009
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052746
# generation_seconds: 41.47
# generated_at: 2026-04-12T00:40:32.176335
@register_scheduler_init(key="scheduler_iter_worst_simple_009")
def scheduler_iter_worst_simple_009_init(s):
    """
    Iteration 2: Priority-aware, pool-filling scheduler focused on weighted latency.

    Key changes vs previous attempt / naive:
      - Always fill each pool with as many assignments as available resources allow (not 1-op-per-pool).
      - Strict priority ordering for dispatch (QUERY > INTERACTIVE > BATCH), with round-robin within each class.
      - Do NOT drop pipelines just because they contain FAILED ops; FAILED is assignable and should be retried.
      - OOM-aware RAM backoff per (pipeline_id, op) with retry cap; prevents permanent dead-end and reduces repeated OOM churn.
      - Gentle batch caps to preserve headroom; aggressive sizing for QUERY to reduce queueing + completion latency.
      - Simple batch aging promotion to avoid indefinite starvation (kept, but less central than strict priority).
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Bookkeeping
    s.tick = 0
    s.enqueued_at = {}  # pipeline_id -> tick (first time seen)

    # Per-op learned hints
    s.op_ram_hint = {}      # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}      # (pipeline_id, op_key) -> cpu
    s.op_retry_count = {}   # (pipeline_id, op_key) -> int

    # Tunables
    s.batch_promotion_ticks = 250

    # Retry and sizing behavior
    s.max_retries_per_op = 6
    s.min_cpu = 1
    s.min_ram = 1

    # Default starting points (in "resource units" of the simulator)
    s.default_query_cpu = 4
    s.default_interactive_cpu = 2
    s.default_batch_cpu = 1
    s.default_ram = 2


@register_scheduler(key="scheduler_iter_worst_simple_009")
def scheduler_iter_worst_simple_009_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue new pipelines into per-priority queues (dedupe by pipeline_id).
      2) Learn from failures: on OOM-like errors, bump RAM hint aggressively; otherwise bump slightly.
      3) For each pool, repeatedly dispatch ready ops until CPU/RAM is exhausted.
         - Pick next runnable pipeline in strict effective priority order.
         - Round-robin within each priority to avoid head-of-line blocking.
    """
    s.tick += 1

    # ---------------- Helpers ----------------
    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _effective_priority(p):
        # Promote old batch slightly to avoid starvation (but QUERY still dominates).
        if p.priority == Priority.BATCH_PIPELINE:
            first = s.enqueued_at.get(p.pipeline_id, s.tick)
            if (s.tick - first) >= s.batch_promotion_ticks:
                return Priority.INTERACTIVE
        return p.priority

    def _prio_order_queues():
        # Strict order for weighted latency: QUERY then INTERACTIVE then BATCH.
        # (We still allow batch via its queue and aging promotion.)
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

    def _pipeline_done(p):
        return p.runtime_status().is_pipeline_successful()

    def _get_one_ready_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _oom_like(err_str):
        e = (err_str or "").lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e)

    def _size_for(pool_id, p, op):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        eff = _effective_priority(p)
        key = (p.pipeline_id, _op_key(op))

        # Start from defaults, then apply hints.
        if eff == Priority.QUERY:
            base_cpu = s.default_query_cpu
            # For latency, let QUERY take a large share if available.
            cpu_cap = max(s.min_cpu, int(pool.max_cpu_pool * 0.95))
            ram_cap = max(s.min_ram, int(pool.max_ram_pool * 0.95))
        elif eff == Priority.INTERACTIVE:
            base_cpu = s.default_interactive_cpu
            cpu_cap = max(s.min_cpu, int(pool.max_cpu_pool * 0.85))
            ram_cap = max(s.min_ram, int(pool.max_ram_pool * 0.90))
        else:
            base_cpu = s.default_batch_cpu
            # Preserve headroom; batch should not consume the pool in one shot.
            cpu_cap = max(s.min_cpu, int(pool.max_cpu_pool * 0.40))
            ram_cap = max(s.min_ram, int(pool.max_ram_pool * 0.55))

        req_cpu = max(base_cpu, int(s.op_cpu_hint.get(key, base_cpu)))
        req_ram = max(s.default_ram, int(s.op_ram_hint.get(key, s.default_ram)))

        # Bound by caps and current availability; never request 0.
        req_cpu = max(s.min_cpu, min(req_cpu, cpu_cap, avail_cpu))
        req_ram = max(s.min_ram, min(req_ram, ram_cap, avail_ram))

        return req_cpu, req_ram

    def _should_drop_op(p, op):
        # If an op has exceeded retry cap, we give up on this pipeline to avoid infinite churn.
        key = (p.pipeline_id, _op_key(op))
        return s.op_retry_count.get(key, 0) > s.max_retries_per_op

    # ---------------- Enqueue new pipelines ----------------
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.enqueued_at:
            s.enqueued_at[pid] = s.tick
            _queue_for_priority(p.priority).append(p)

    if not pipelines and not results:
        return [], []

    # ---------------- Learn from results (RAM backoff) ----------------
    for r in results:
        if hasattr(r, "failed") and r.failed():
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                # Can't reliably attribute hints; skip.
                continue

            err = str(getattr(r, "error", "") or "")
            oom = _oom_like(err)

            ops = getattr(r, "ops", []) or []
            for op in ops:
                k = (pid, _op_key(op))
                s.op_retry_count[k] = s.op_retry_count.get(k, 0) + 1

                # Previous request (best effort)
                prev_ram = int(getattr(r, "ram", 0) or 0)
                prev_cpu = int(getattr(r, "cpu", 0) or 0)
                prev_ram = max(s.min_ram, prev_ram if prev_ram > 0 else s.op_ram_hint.get(k, s.default_ram))
                prev_cpu = max(s.min_cpu, prev_cpu if prev_cpu > 0 else s.op_cpu_hint.get(k, s.default_batch_cpu))

                if oom:
                    # OOM: RAM is the critical dimension; double to converge fast.
                    s.op_ram_hint[k] = max(s.op_ram_hint.get(k, s.default_ram), prev_ram * 2)
                    # CPU hint unchanged on OOM.
                    s.op_cpu_hint[k] = max(s.op_cpu_hint.get(k, 0), prev_cpu)
                else:
                    # Non-OOM failure: small bump to reduce repeated underprovisioning.
                    s.op_ram_hint[k] = max(s.op_ram_hint.get(k, s.default_ram), prev_ram + 1)
                    s.op_cpu_hint[k] = max(s.op_cpu_hint.get(k, 0), prev_cpu + 1)

    # ---------------- Dispatch (fill pools) ----------------
    suspensions = []
    assignments = []

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Keep scheduling until the pool can't fit even minimal work.
        while pool.avail_cpu_pool >= s.min_cpu and pool.avail_ram_pool >= s.min_ram:
            chosen_p = None
            chosen_op = None
            chosen_queue = None

            # Strict priority scan, but round-robin within each queue.
            for q in _prio_order_queues():
                n = len(q)
                if n == 0:
                    continue

                for _ in range(n):
                    p = q.popleft()

                    # Drop completed pipelines eagerly.
                    if _pipeline_done(p):
                        s.enqueued_at.pop(p.pipeline_id, None)
                        continue

                    # If aged batch, treat as interactive by moving queues (optional but helps).
                    eff = _effective_priority(p)
                    if eff != p.priority:
                        # Move to interactive queue for subsequent decisions.
                        s.q_interactive.append(p)
                        continue

                    op = _get_one_ready_op(p)
                    if op is None:
                        # Not ready; keep it in rotation.
                        q.append(p)
                        continue

                    if _should_drop_op(p, op):
                        # Give up on pipeline (prevents infinite retry churn).
                        s.enqueued_at.pop(p.pipeline_id, None)
                        continue

                    chosen_p = p
                    chosen_op = op
                    chosen_queue = q
                    break

                if chosen_p is not None:
                    break

            if chosen_p is None:
                break

            req_cpu, req_ram = _size_for(pool_id, chosen_p, chosen_op)
            if req_cpu < s.min_cpu or req_ram < s.min_ram:
                # Can't fit now; requeue and stop trying to fill this pool.
                chosen_queue.append(chosen_p)
                break

            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=chosen_p.priority,  # keep original priority label
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            # Put the pipeline back to enable round-robin and subsequent ops.
            chosen_queue.append(chosen_p)

            # Refresh pool pointer (simulator may update avail after we return, but we use local view).
            # To avoid infinite loops when avail doesn't change until after return, pessimistically
            # decrement our local view by mirroring the request into a shadow state.
            pool.avail_cpu_pool -= req_cpu
            pool.avail_ram_pool -= req_ram

    return suspensions, assignments
