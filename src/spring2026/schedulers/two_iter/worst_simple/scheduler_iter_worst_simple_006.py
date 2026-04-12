# policy_key: scheduler_iter_worst_simple_006
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.058010
# generation_seconds: 38.12
# generated_at: 2026-04-12T00:38:33.486060
@register_scheduler_init(key="scheduler_iter_worst_simple_006")
def scheduler_iter_worst_simple_006_init(s):
    """
    Iteration 2: Strict priority + pool partitioning + multi-dispatch per tick + retry-friendly failure handling.

    Key changes vs prior attempt (to reduce weighted latency):
      - Never drop pipelines just because some ops are FAILED; FAILED ops are retryable (ASSIGNABLE_STATES includes FAILED).
      - Strict priority order when dispatching (QUERY > INTERACTIVE > BATCH) to reduce high-weight latency.
      - Pool partitioning:
          * pool 0 (if exists): reserved for QUERY/INTERACTIVE (batch only if nothing else runnable).
          * other pools: prefer BATCH, but allow spillover of QUERY/INTERACTIVE if they are waiting.
      - Multi-dispatch: keep assigning ops within a pool in one tick until resources are exhausted.
      - Resource sizing:
          * QUERY: scale up (use most of pool) to finish quickly.
          * INTERACTIVE: moderately large slice.
          * BATCH: small capped slices to avoid stealing headroom.
      - OOM-aware RAM backoff per (pipeline, op) using ExecutionResult.error signal.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Bookkeeping: pipeline_id -> last_seen_tick (used to avoid duplicate enqueue storms)
    s.seen_tick = {}

    # Per-op learned hints for retries
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}  # (pipeline_id, op_key) -> cpu

    s.tick = 0

    # Minimum request sizes (best-effort; bounded by pool availability)
    s.min_cpu = 1
    s.min_ram = 1

    # Batch caps (fractions of pool max) to protect latency-sensitive work
    s.batch_cpu_cap_frac = 0.25
    s.batch_ram_cap_frac = 0.35

    # Interactive defaults (fractions of pool max)
    s.interactive_cpu_target_frac = 0.60
    s.interactive_ram_target_frac = 0.70

    # Query defaults (fractions of pool max)
    s.query_cpu_target_frac = 0.90
    s.query_ram_target_frac = 0.90


@register_scheduler(key="scheduler_iter_worst_simple_006")
def scheduler_iter_worst_simple_006_scheduler(s, results, pipelines):
    """
    Dispatch as many runnable ops as possible each tick while respecting strict priority and pool preferences.

    Returns:
      (suspensions, assignments)
    """
    from collections import deque

    s.tick += 1

    def _q_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        # Avoid repeatedly enqueuing the same pipeline in the same tick
        pid = p.pipeline_id
        if s.seen_tick.get(pid, None) == s.tick:
            return
        s.seen_tick[pid] = s.tick
        _q_for_priority(p.priority).append(p)

    def _op_key(op):
        # Best-effort stable op identity
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _is_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _next_runnable_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _learn_from_results():
        for r in results:
            if not (hasattr(r, "failed") and r.failed()):
                continue

            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                continue

            err = str(getattr(r, "error", "") or "")
            err_l = err.lower()
            oom_like = ("oom" in err_l) or ("out of memory" in err_l) or ("memory" in err_l)

            ran_cpu = int(getattr(r, "cpu", 1) or 1)
            ran_ram = int(getattr(r, "ram", 1) or 1)

            for op in (getattr(r, "ops", None) or []):
                key = (pid, _op_key(op))
                prev_ram = int(s.op_ram_hint.get(key, max(s.min_ram, ran_ram)))
                prev_cpu = int(s.op_cpu_hint.get(key, max(s.min_cpu, ran_cpu)))

                # RAM-first backoff on OOM; small bump on other failures
                if oom_like:
                    s.op_ram_hint[key] = max(prev_ram, ran_ram * 2, prev_ram * 2)
                    s.op_cpu_hint[key] = prev_cpu
                else:
                    s.op_ram_hint[key] = max(prev_ram, ran_ram + 1, int(prev_ram * 1.25) + 1)
                    s.op_cpu_hint[key] = max(prev_cpu, ran_cpu + 1, int(prev_cpu * 1.10) + 1)

    def _pool_prefers_latency(pool_id):
        # If multiple pools exist, treat pool 0 as latency pool.
        return (s.executor.num_pools > 1 and pool_id == 0)

    def _target_request(pipeline_priority, pool, avail_cpu, avail_ram, pid, op):
        # Caps / targets by priority
        if pipeline_priority == Priority.QUERY:
            cpu_tgt = max(s.min_cpu, int(pool.max_cpu_pool * s.query_cpu_target_frac))
            ram_tgt = max(s.min_ram, int(pool.max_ram_pool * s.query_ram_target_frac))
        elif pipeline_priority == Priority.INTERACTIVE:
            cpu_tgt = max(s.min_cpu, int(pool.max_cpu_pool * s.interactive_cpu_target_frac))
            ram_tgt = max(s.min_ram, int(pool.max_ram_pool * s.interactive_ram_target_frac))
        else:
            cpu_cap = max(s.min_cpu, int(pool.max_cpu_pool * s.batch_cpu_cap_frac))
            ram_cap = max(s.min_ram, int(pool.max_ram_pool * s.batch_ram_cap_frac))
            # Keep batch small: at most cap, but also not more than a couple CPUs by default
            cpu_tgt = min(cpu_cap, max(s.min_cpu, min(2, pool.max_cpu_pool)))
            ram_tgt = min(ram_cap, max(s.min_ram, min(4, pool.max_ram_pool)))

        # Apply learned per-op hints (bounded by availability and batch caps via cpu_tgt/ram_tgt)
        key = (pid, _op_key(op))
        hinted_cpu = int(s.op_cpu_hint.get(key, cpu_tgt))
        hinted_ram = int(s.op_ram_hint.get(key, ram_tgt))

        # For QUERY/INTERACTIVE, allow hints to increase request beyond target (but not beyond pool max).
        # For BATCH, keep within its conservative target.
        if pipeline_priority in (Priority.QUERY, Priority.INTERACTIVE):
            cpu_req = min(pool.max_cpu_pool, max(cpu_tgt, hinted_cpu))
            ram_req = min(pool.max_ram_pool, max(ram_tgt, hinted_ram))
        else:
            cpu_req = min(cpu_tgt, hinted_cpu)
            ram_req = min(ram_tgt, hinted_ram)

        # Bound by current availability and minimums
        cpu_req = max(s.min_cpu, min(cpu_req, avail_cpu))
        ram_req = max(s.min_ram, min(ram_req, avail_ram))
        return cpu_req, ram_req

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    _learn_from_results()

    suspensions = []
    assignments = []

    # Dispatch loop: fill each pool as much as possible this tick
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Try to keep assigning while resources remain
        while pool.avail_cpu_pool >= s.min_cpu and pool.avail_ram_pool >= s.min_ram:
            picked = None
            picked_op = None

            # Pool preference:
            #  - pool 0 (latency pool): only QUERY/INTERACTIVE unless none runnable.
            #  - other pools: prefer BATCH first, but allow spillover of QUERY/INTERACTIVE if they exist.
            if _pool_prefers_latency(pool_id):
                queue_order = (s.q_query, s.q_interactive, s.q_batch)
            else:
                queue_order = (s.q_batch, s.q_query, s.q_interactive)

            # Scan queues in order; bounded scan to avoid infinite loops
            for q in queue_order:
                for _ in range(len(q)):
                    p = q.popleft()

                    # Drop completed pipelines
                    if _is_done(p):
                        continue

                    op = _next_runnable_op(p)
                    if op is None:
                        # Not runnable yet; keep it for later
                        q.append(p)
                        continue

                    # Enforce "no batch on latency pool unless nothing else runnable"
                    if _pool_prefers_latency(pool_id) and p.priority == Priority.BATCH_PIPELINE:
                        # We'll only accept batch if there are truly no runnable QUERY/INTERACTIVE.
                        # Implemented by deferring batch by re-queueing and continuing scan.
                        q.append(p)
                        continue

                    picked = p
                    picked_op = op
                    break

                if picked is not None:
                    break

            # If we didn't pick anything and this is the latency pool, allow batch as a fallback
            if picked is None and _pool_prefers_latency(pool_id):
                q = s.q_batch
                for _ in range(len(q)):
                    p = q.popleft()
                    if _is_done(p):
                        continue
                    op = _next_runnable_op(p)
                    if op is None:
                        q.append(p)
                        continue
                    picked = p
                    picked_op = op
                    break

            if picked is None:
                break  # nothing runnable for this pool

            # Size request and issue assignment
            req_cpu, req_ram = _target_request(
                picked.priority, pool, pool.avail_cpu_pool, pool.avail_ram_pool, picked.pipeline_id, picked_op
            )

            if req_cpu < s.min_cpu or req_ram < s.min_ram:
                # Can't fit; requeue and stop trying to pack this pool this tick
                _q_for_priority(picked.priority).append(picked)
                break

            assignments.append(
                Assignment(
                    ops=[picked_op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=picked.priority,
                    pool_id=pool_id,
                    pipeline_id=picked.pipeline_id,
                )
            )

            # Requeue pipeline so later ops can be scheduled; keep same priority queue
            _q_for_priority(picked.priority).append(picked)

            # Loop continues with updated pool availability (executor model updates avail_* after assignments are applied)

    return suspensions, assignments
