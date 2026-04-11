# policy_key: scheduler_low_014
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045023
# generation_seconds: 50.79
# generated_at: 2026-03-12T21:45:14.841143
@register_scheduler_init(key="scheduler_low_014")
def scheduler_low_014_init(s):
    """
    Priority-aware FIFO with conservative batch sizing + simple OOM backoff.

    Improvements over naive FIFO:
      - Separate queues per priority; always admit higher priority first.
      - If multiple pools exist, prefer keeping pool 0 for QUERY/INTERACTIVE.
      - Prevent BATCH from consuming an entire pool by capping its per-assignment CPU/RAM.
      - On failure (esp. OOM-like), retry by increasing RAM request for that operator (bounded by pool max).
      - Simple anti-starvation aging: old BATCH pipelines are promoted into INTERACTIVE after a threshold.
    """
    from collections import deque

    # Per-priority waiting queues
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track when a pipeline entered the scheduler to support aging / de-dup
    s.pipeline_enqueued_at = {}  # pipeline_id -> tick

    # Track per-operator resource hints learned from failures
    # Keyed by (pipeline_id, op_identity)
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}  # (pipeline_id, op_key) -> cpu

    # Logical tick counter (scheduler step count)
    s.tick = 0

    # Aging threshold (in scheduler invocations) before promoting BATCH to reduce starvation
    s.batch_promotion_ticks = 200

    # Minimum slices to avoid tiny, slow allocations (best-effort; bounded by availability)
    s.min_cpu_slice = 1
    s.min_ram_slice = 1


@register_scheduler(key="scheduler_low_014")
def scheduler_low_014_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into priority queues (QUERY > INTERACTIVE > BATCH).
      2) Learn from failures by bumping RAM/CPU hints for the failed ops.
      3) Age BATCH: if it waits too long, treat it as INTERACTIVE for dispatch purposes.
      4) For each pool, dispatch at most one ready operator, picking highest effective priority.
         - Prefer pool 0 for QUERY/INTERACTIVE when multiple pools exist.
         - Cap BATCH per-assignment resource usage to keep headroom for latency-sensitive work.
    """
    from collections import deque

    s.tick += 1

    # ---- Helpers (kept local to avoid imports at module scope) ----
    def _prio_rank(prio):
        # Higher number = more important
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1  # Priority.BATCH_PIPELINE (or anything else)

    def _choose_queue_for_pipeline(p):
        if p.priority == Priority.QUERY:
            return s.q_query
        if p.priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _maybe_enqueue_pipeline(p):
        # Avoid duplicate enqueues across ticks for the same pipeline_id
        pid = p.pipeline_id
        if pid not in s.pipeline_enqueued_at:
            s.pipeline_enqueued_at[pid] = s.tick
            _choose_queue_for_pipeline(p).append(p)

    def _drop_pipeline(pid):
        # Lazy removal from queues; here we only clear bookkeeping
        if pid in s.pipeline_enqueued_at:
            del s.pipeline_enqueued_at[pid]

    def _iter_deques():
        # Ordered from highest to lowest base priority
        return (s.q_query, s.q_interactive, s.q_batch)

    def _is_done_or_failed(p):
        st = p.runtime_status()
        # Keep semantics similar to the naive policy: drop pipelines with failures.
        # (We still allow operator-level retry by re-assigning FAILED ops; but if the pipeline has
        # a terminal failure mode, runtime_status() should reflect it consistently.)
        has_failures = st.state_counts.get(OperatorState.FAILED, 0) > 0
        return st.is_pipeline_successful(), has_failures

    def _get_ready_ops(p):
        st = p.runtime_status()
        # In Eudoxia, FAILED can be re-assigned (ASSIGNABLE_STATES). Require parents complete.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]  # schedule one operator at a time per pipeline

    def _op_key(op):
        # Best-effort stable identity within a run.
        # If op has an id/name, use it; otherwise fall back to Python identity.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _effective_priority(p):
        # Anti-starvation aging: promote old BATCH into INTERACTIVE bucket.
        pid = p.pipeline_id
        enq = s.pipeline_enqueued_at.get(pid, s.tick)
        waited = s.tick - enq
        if p.priority == Priority.BATCH_PIPELINE and waited >= s.batch_promotion_ticks:
            return Priority.INTERACTIVE
        return p.priority

    def _preferred_pools_for_priority(prio):
        # If multiple pools exist, treat pool 0 as "latency pool" for QUERY/INTERACTIVE.
        if s.executor.num_pools <= 1:
            return list(range(s.executor.num_pools))
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, s.executor.num_pools)]
        # Batch should avoid pool 0 when possible
        return [i for i in range(1, s.executor.num_pools)] + [0]

    def _cap_for_priority(prio, pool):
        # Caps expressed as fractions of pool max; these are "obvious first" latency improvements:
        # don't let batch greedily take the whole pool in one shot.
        if prio == Priority.QUERY:
            return 0.9, 0.9
        if prio == Priority.INTERACTIVE:
            return 0.8, 0.85
        # Batch: more conservative to preserve headroom
        return 0.5, 0.6

    def _size_resources(p, op, pool_id):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        eff_prio = _effective_priority(p)
        cpu_cap_frac, ram_cap_frac = _cap_for_priority(eff_prio, pool)

        # Base request: a modest slice, then scale within caps and availability.
        cpu_cap = max(s.min_cpu_slice, int(pool.max_cpu_pool * cpu_cap_frac))
        ram_cap = max(s.min_ram_slice, int(pool.max_ram_pool * ram_cap_frac))

        # Start with some default slices; higher priority gets a bit more CPU by default.
        if eff_prio == Priority.QUERY:
            base_cpu = max(s.min_cpu_slice, min(4, pool.max_cpu_pool))
        elif eff_prio == Priority.INTERACTIVE:
            base_cpu = max(s.min_cpu_slice, min(2, pool.max_cpu_pool))
        else:
            base_cpu = max(s.min_cpu_slice, min(2, pool.max_cpu_pool))

        base_ram = max(s.min_ram_slice, min(4, pool.max_ram_pool))

        # Apply learned hints (from failures/oom), bounded by caps.
        key = (p.pipeline_id, _op_key(op))
        hinted_ram = s.op_ram_hint.get(key, base_ram)
        hinted_cpu = s.op_cpu_hint.get(key, base_cpu)

        req_cpu = max(base_cpu, hinted_cpu)
        req_ram = max(base_ram, hinted_ram)

        # Enforce caps and availability (and never request 0).
        req_cpu = max(s.min_cpu_slice, min(req_cpu, cpu_cap, avail_cpu))
        req_ram = max(s.min_ram_slice, min(req_ram, ram_cap, avail_ram))

        return req_cpu, req_ram

    # ---- Ingest new pipelines ----
    for p in pipelines:
        _maybe_enqueue_pipeline(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # ---- Learn from execution results (OOM/backoff) ----
    for r in results:
        # If an op failed, increase resource hints (especially RAM) for retry.
        if hasattr(r, "failed") and r.failed():
            # r.ops is a list of ops that ran in this container
            ops = getattr(r, "ops", []) or []
            pid = getattr(r, "pipeline_id", None)

            # If pipeline_id isn't present on result, we can't key precisely; skip in that case.
            # (We still rely on queue ordering + conservative sizing.)
            if pid is None:
                continue

            # Determine if error smells like OOM; otherwise apply a smaller bump.
            err = str(getattr(r, "error", "") or "")
            oom_like = ("oom" in err.lower()) or ("out of memory" in err.lower()) or ("memory" in err.lower())

            for op in ops:
                key = (pid, _op_key(op))
                prev_ram = s.op_ram_hint.get(key, max(s.min_ram_slice, int(getattr(r, "ram", 1) or 1)))
                prev_cpu = s.op_cpu_hint.get(key, max(s.min_cpu_slice, int(getattr(r, "cpu", 1) or 1)))

                if oom_like:
                    s.op_ram_hint[key] = max(prev_ram, int((getattr(r, "ram", prev_ram) or prev_ram) * 2))
                    # Keep CPU the same on OOM; RAM is the usual fix.
                    s.op_cpu_hint[key] = prev_cpu
                else:
                    # Non-OOM failure: cautiously bump both a bit (may help if under-provisioned).
                    s.op_ram_hint[key] = max(prev_ram, int((getattr(r, "ram", prev_ram) or prev_ram) * 1.25) + 1)
                    s.op_cpu_hint[key] = max(prev_cpu, int((getattr(r, "cpu", prev_cpu) or prev_cpu) * 1.25) + 1)

    # ---- Dispatch: one assignment per pool per tick ----
    suspensions = []
    assignments = []

    # We'll try pools in an order that tends to keep pool 0 for latency-sensitive tasks.
    # But we still evaluate each pool independently based on its availability.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Select a pipeline suitable for this pool.
        # We perform a bounded scan of each queue to find the first runnable pipeline.
        chosen = None
        chosen_ops = None
        chosen_eff_prio = None

        # Prefer queues in effective priority order, but also consider pool preference:
        # if pool 0 and we have multiple pools, try to pick QUERY/INTERACTIVE first;
        # if non-zero pool, it's okay to pick BATCH earlier if no high-priority runnable.
        for q in _iter_deques():
            # Rotate through queue elements at most once (bounded scan).
            for _ in range(len(q)):
                p = q.popleft()

                # Pipeline might be done/failed since it was enqueued; drop bookkeeping and skip.
                done, failed = _is_done_or_failed(p)
                if done or failed:
                    _drop_pipeline(p.pipeline_id)
                    continue

                eff_prio = _effective_priority(p)

                # Enforce pool preference: if multiple pools exist, strongly prefer
                # QUERY/INTERACTIVE on pool 0 and BATCH on non-0 when possible.
                if s.executor.num_pools > 1:
                    pref = _preferred_pools_for_priority(eff_prio)
                    # If this pool is not among the top 1 preferred pools and we have other pools,
                    # we still allow it, but only after we fail to find a better match in this tick.
                    # Simple approach: skip mismatched BATCH on pool 0 when there are other pools.
                    if pool_id == 0 and eff_prio == Priority.BATCH_PIPELINE and 1 in pref:
                        # Put back and continue scanning; batch should go elsewhere if possible.
                        q.append(p)
                        continue

                ops = _get_ready_ops(p)
                if not ops:
                    # Not ready yet; keep it in the same queue.
                    q.append(p)
                    continue

                chosen = p
                chosen_ops = ops
                chosen_eff_prio = eff_prio
                # Keep chosen out of queue for now; we will requeue after assignment attempt.
                break

            if chosen is not None:
                break

        if chosen is None:
            continue

        # Compute request size with caps and hints.
        op = chosen_ops[0]
        req_cpu, req_ram = _size_resources(chosen, op, pool_id)

        # If we couldn't allocate even minimal slices, requeue and move on.
        if req_cpu <= 0 or req_ram <= 0:
            _choose_queue_for_pipeline(chosen).append(chosen)
            continue

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=req_cpu,
                ram=req_ram,
                priority=chosen.priority,  # keep original priority for executor accounting
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
        )

        # Requeue chosen pipeline so its next ops (or retries) can be scheduled later.
        _choose_queue_for_pipeline(chosen).append(chosen)

    return suspensions, assignments
