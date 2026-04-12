# policy_key: scheduler_iter_worst_simple_014
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.053124
# generation_seconds: 37.77
# generated_at: 2026-04-12T00:43:59.837059
@register_scheduler_init(key="scheduler_iter_worst_simple_014")
def scheduler_iter_worst_simple_014_init(s):
    """
    Priority-first, work-conserving scheduler that fills pools with ready ops.

    Changes vs prior attempt (to reduce weighted latency):
      - Strict priority dispatch (QUERY > INTERACTIVE > BATCH) with pool-0 preference for latency work.
      - Work-conserving packing: assign multiple operators per pool per tick until resources are exhausted.
      - Do NOT drop pipelines just because some ops are FAILED; retry FAILED ops (ASSIGNABLE_STATES) with RAM backoff.
      - Global (best-effort) per-operator RAM hints learned from failures, keyed by (priority, op identity).
      - Dynamic "batch throttling": when any QUERY/INTERACTIVE is waiting, cap batch allocations more aggressively
        and keep batch off pool 0 when multiple pools exist.
      - Light anti-starvation aging for BATCH (only after long waits).
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    s.enqueued_at = {}  # pipeline_id -> tick (first seen)
    s.tick = 0

    # Failure-driven hints (best-effort; ExecutionResult may not include pipeline_id)
    s.op_ram_hint = {}  # (priority, op_key) -> ram_units
    s.op_cpu_hint = {}  # (priority, op_key) -> cpu_units

    # Tuning knobs
    s.batch_promotion_ticks = 600  # keep small to preserve fairness but avoid hurting weighted latency
    s.min_cpu_slice = 1
    s.min_ram_slice = 1

    # Default starting sizes (kept modest; QUERY gets more CPU to cut tail/weighted latency)
    s.query_base_cpu = 4
    s.interactive_base_cpu = 2
    s.batch_base_cpu = 1

    s.query_base_ram = 4
    s.interactive_base_ram = 4
    s.batch_base_ram = 2


@register_scheduler(key="scheduler_iter_worst_simple_014")
def scheduler_iter_worst_simple_014_scheduler(s, results, pipelines):
    """
    Each tick:
      1) Enqueue new pipelines into per-priority queues.
      2) Update global per-op hints on failures (OOM-like -> RAM x2; otherwise small bump).
      3) Fill each pool with as many ready ops as possible, in strict priority order.
         - If multiple pools: pool 0 is treated as latency-oriented (QUERY/INTERACTIVE preferred).
         - If any latency work is waiting, batch is throttled to preserve headroom.
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

    def _drop_pipeline_bookkeeping(pid):
        if pid in s.enqueued_at:
            del s.enqueued_at[pid]

    def _op_key(op):
        # Best-effort stable identity within a simulation run.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _effective_priority(p):
        # Aging only for BATCH; promotion is gentle and only after long waits.
        if p.priority != Priority.BATCH_PIPELINE:
            return p.priority
        waited = s.tick - s.enqueued_at.get(p.pipeline_id, s.tick)
        if waited >= s.batch_promotion_ticks:
            return Priority.INTERACTIVE
        return p.priority

    def _latency_waiting():
        # Any QUERY/INTERACTIVE pipeline present (even if not runnable yet) => protect headroom.
        return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    def _get_ready_ops(p):
        st = p.runtime_status()
        # Pipeline completion check
        if st.is_pipeline_successful():
            return []
        # Retry FAILED ops as well (ASSIGNABLE_STATES includes FAILED), but only when parents complete.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]  # schedule one operator at a time per pipeline to keep fairness across pipelines

    def _base_sizes(prio):
        if prio == Priority.QUERY:
            return s.query_base_cpu, s.query_base_ram
        if prio == Priority.INTERACTIVE:
            return s.interactive_base_cpu, s.interactive_base_ram
        return s.batch_base_cpu, s.batch_base_ram

    def _caps_for(prio, pool, protect_latency):
        # Fractions of pool max; batch caps tighten if latency work exists.
        if prio == Priority.QUERY:
            return 0.95, 0.95
        if prio == Priority.INTERACTIVE:
            return 0.85, 0.90
        # Batch
        if protect_latency:
            return 0.20, 0.25
        return 0.50, 0.60

    def _size_for(p, op, pool_id, protect_latency):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        eff_prio = _effective_priority(p)
        base_cpu, base_ram = _base_sizes(eff_prio)
        cpu_cap_frac, ram_cap_frac = _caps_for(eff_prio, pool, protect_latency)

        cpu_cap = max(s.min_cpu_slice, int(pool.max_cpu_pool * cpu_cap_frac))
        ram_cap = max(s.min_ram_slice, int(pool.max_ram_pool * ram_cap_frac))

        # Apply learned hints (best-effort, global by (priority, op_key)).
        k = (eff_prio, _op_key(op))
        hinted_ram = s.op_ram_hint.get(k, base_ram)
        hinted_cpu = s.op_cpu_hint.get(k, base_cpu)

        # For QUERY, be more aggressive: grab more CPU when available to reduce latency.
        if eff_prio == Priority.QUERY:
            target_cpu = max(hinted_cpu, base_cpu)
            # opportunistically scale up within caps/availability
            target_cpu = min(max(target_cpu, 1), cpu_cap, avail_cpu)
        else:
            target_cpu = min(max(hinted_cpu, base_cpu, 1), cpu_cap, avail_cpu)

        target_ram = min(max(hinted_ram, base_ram, 1), ram_cap, avail_ram)

        return target_cpu, target_ram

    def _pool_order_for(prio):
        # If multiple pools, reserve pool 0 as "latency pool".
        n = s.executor.num_pools
        if n <= 1:
            return [0]
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + list(range(1, n))
        return list(range(1, n)) + [0]

    # ---------- ingest ----------
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ---------- learn from failures ----------
    for r in results:
        if hasattr(r, "failed") and r.failed():
            err = str(getattr(r, "error", "") or "")
            oom_like = ("oom" in err.lower()) or ("out of memory" in err.lower())

            prio = getattr(r, "priority", Priority.BATCH_PIPELINE)
            ran_ops = getattr(r, "ops", []) or []
            used_ram = int(getattr(r, "ram", 1) or 1)
            used_cpu = int(getattr(r, "cpu", 1) or 1)

            for op in ran_ops:
                k = (prio, _op_key(op))
                prev_ram = int(s.op_ram_hint.get(k, max(s.min_ram_slice, used_ram)))
                prev_cpu = int(s.op_cpu_hint.get(k, max(s.min_cpu_slice, used_cpu)))

                if oom_like:
                    # OOM: RAM is the primary fix; back off quickly to converge.
                    s.op_ram_hint[k] = max(prev_ram, used_ram * 2, prev_ram * 2)
                    s.op_cpu_hint[k] = max(prev_cpu, used_cpu)
                else:
                    # Other failure: small bump only (avoid runaway allocations).
                    s.op_ram_hint[k] = max(prev_ram, int(prev_ram * 1.25) + 1, used_ram)
                    s.op_cpu_hint[k] = max(prev_cpu, int(prev_cpu * 1.20) + 1, used_cpu)

    # ---------- dispatch ----------
    suspensions = []
    assignments = []

    protect_latency = _latency_waiting()

    # We fill pools in an order that helps latency: schedule QUERY/INTERACTIVE onto preferred pools first.
    # Implementation: iterate pools in natural order, but within each pool always pick highest-priority runnable work.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Keep assigning while there is capacity for at least minimal work.
        # (We rely on pool.avail_* updating as assignments are applied by the simulator.)
        while pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
            chosen_p = None
            chosen_ops = None
            chosen_queue = None

            # Strict priority order, but try to keep batch off pool 0 when multiple pools exist and
            # there is latency work waiting somewhere.
            deques = (s.q_query, s.q_interactive, s.q_batch)

            for q in deques:
                # If this is batch queue and pool is 0, avoid batch unless nothing else runnable.
                if q is s.q_batch and s.executor.num_pools > 1 and pool_id == 0 and protect_latency:
                    continue

                # Bounded scan of queue to find first runnable pipeline; rotate to preserve FIFO within priority.
                found = False
                for _ in range(len(q)):
                    p = q.popleft()

                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        _drop_pipeline_bookkeeping(p.pipeline_id)
                        continue

                    ops = _get_ready_ops(p)
                    if not ops:
                        q.append(p)
                        continue

                    chosen_p = p
                    chosen_ops = ops
                    chosen_queue = q
                    found = True
                    break

                if found:
                    break

            # If we skipped batch on pool 0, we might still want to run it if nothing else is runnable.
            if chosen_p is None and (pool_id == 0) and (s.executor.num_pools > 1):
                q = s.q_batch
                for _ in range(len(q)):
                    p = q.popleft()
                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        _drop_pipeline_bookkeeping(p.pipeline_id)
                        continue
                    ops = _get_ready_ops(p)
                    if not ops:
                        q.append(p)
                        continue
                    chosen_p = p
                    chosen_ops = ops
                    chosen_queue = q
                    break

            if chosen_p is None:
                break  # no runnable work for this pool right now

            # Determine request size.
            op = chosen_ops[0]
            req_cpu, req_ram = _size_for(chosen_p, op, pool_id, protect_latency)

            # If we can't fit minimal slices, put pipeline back and stop trying on this pool.
            if req_cpu <= 0 or req_ram <= 0:
                chosen_queue.append(chosen_p)
                break

            # Make assignment.
            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=chosen_p.priority,  # preserve original priority label
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            # Requeue pipeline for subsequent ops/retries.
            chosen_queue.append(chosen_p)

            # Recompute protect_latency after potentially draining queues.
            protect_latency = _latency_waiting()

    return suspensions, assignments
