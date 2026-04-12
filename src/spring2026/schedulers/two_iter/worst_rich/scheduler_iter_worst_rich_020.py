# policy_key: scheduler_iter_worst_rich_020
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042414
# generation_seconds: 37.78
# generated_at: 2026-04-12T01:00:27.391291
@register_scheduler_init(key="scheduler_iter_worst_rich_020")
def scheduler_iter_worst_rich_020_init(s):
    """
    Iteration 2: Fix catastrophic OOMs first, then add small priority-based latency improvements.

    Core changes vs prior attempt:
      - Never drop pipelines just because they have FAILED ops; allow retries (FAILED is assignable).
      - Start with large, safe RAM allocations to avoid OOM storms (primary cause of 0% completion).
      - Learn per-operator RAM hints from OOM failures and retry with higher RAM (bounded by pool max).
      - Simple strict priority queuing (QUERY > INTERACTIVE > BATCH).
      - Multi-pool preference: keep pool 0 biased toward latency-sensitive work when possible.
      - Batch is capped only when there is higher-priority backlog; otherwise it can use the pool.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Best-effort de-dup / bookkeeping
    s.enqueued_at = {}  # pipeline_id -> tick
    s.tick = 0

    # Learned RAM hints per operator identity (best-effort stable within a run)
    s.op_ram_hint = {}  # op_key -> ram

    # Small knobs (kept conservative)
    s.reserve_ram_for_latency_frac = 0.10  # when serving batch under contention, keep headroom
    s.reserve_cpu_for_latency_frac = 0.10
    s.max_scan_per_queue = 64  # bound scanning cost per pool


@register_scheduler(key="scheduler_iter_worst_rich_020")
def scheduler_iter_worst_rich_020_scheduler(s, results, pipelines):
    """
    Priority-aware, OOM-resistant scheduler.

    Strategy:
      - Enqueue new pipelines into per-priority FIFO queues.
      - Update per-operator RAM hints on OOM failures (double RAM next time, capped).
      - For each pool, assign at most one ready operator:
          * Prefer QUERY/INTERACTIVE; keep pool 0 mostly for latency-sensitive work.
          * Allocate "big RAM" by default (near all available) to prevent OOM thrash.
          * If dispatching BATCH while higher-priority backlog exists, cap its allocation to preserve headroom.
    """
    s.tick += 1

    # ---- helpers ----
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

    def _drop_pipeline(pid):
        if pid in s.enqueued_at:
            del s.enqueued_at[pid]

    def _op_key(op):
        # Best-effort stable identity for hinting.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _has_ready_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return []
        # FAILED is included in ASSIGNABLE_STATES; we rely on that for retries.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return ops[:1] if ops else []

    def _higher_prio_backlog_exists():
        # Any QUERY/INTERACTIVE waiting (not necessarily runnable).
        return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    def _prefer_latency_pool(prio, pool_id):
        # Bias: pool 0 is latency pool when multiple pools exist.
        if s.executor.num_pools <= 1:
            return True
        if pool_id != 0:
            return True
        return prio in (Priority.QUERY, Priority.INTERACTIVE)

    def _size_request(p, op, pool_id, serve_batch_under_contention):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        # Default: allocate large RAM to avoid OOM; CPU uses what's available.
        # Start from hint if we have one; otherwise take almost all available RAM.
        key = _op_key(op)
        hinted_ram = s.op_ram_hint.get(key, None)

        # Leave 1 unit slack to avoid edge cases with exact-fit; clamp to at least 1.
        big_ram = max(1, int(avail_ram * 0.95))
        req_ram = big_ram if hinted_ram is None else max(1, min(int(hinted_ram), int(pool.max_ram_pool), int(avail_ram)))

        # CPU: for latency-sensitive work, take most CPU; for batch under contention, cap a bit.
        if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            req_cpu = max(1, int(avail_cpu))  # take what is available
        else:
            if serve_batch_under_contention:
                # Preserve some headroom in the pool for potential arrivals.
                req_cpu = max(1, int(avail_cpu * (1.0 - s.reserve_cpu_for_latency_frac)))
                req_ram = max(1, min(req_ram, int(avail_ram * (1.0 - s.reserve_ram_for_latency_frac))))
            else:
                # If no latency backlog, let batch use the pool.
                req_cpu = max(1, int(avail_cpu))

        # Final clamp to available.
        req_cpu = max(1, min(int(req_cpu), int(avail_cpu)))
        req_ram = max(1, min(int(req_ram), int(avail_ram)))
        return req_cpu, req_ram

    # ---- ingest ----
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ---- learn from failures (OOM backoff) ----
    for r in results:
        if hasattr(r, "failed") and r.failed():
            err = str(getattr(r, "error", "") or "")
            oom_like = ("oom" in err.lower()) or ("out of memory" in err.lower())

            if not oom_like:
                continue

            pool_id = getattr(r, "pool_id", None)
            if pool_id is None or pool_id >= s.executor.num_pools:
                continue
            pool = s.executor.pools[pool_id]
            ran_ram = int(getattr(r, "ram", 1) or 1)

            for op in (getattr(r, "ops", []) or []):
                key = _op_key(op)
                prev = int(s.op_ram_hint.get(key, ran_ram) or ran_ram)
                # Double, but don't exceed pool max; if already near max, stick to max.
                bumped = min(int(pool.max_ram_pool), max(prev, ran_ram) * 2)
                s.op_ram_hint[key] = max(prev, bumped)

    # ---- dispatch ----
    suspensions = []
    assignments = []

    # For each pool, schedule at most one operator.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Choose a runnable pipeline, strict priority; bounded scans.
        chosen_p = None
        chosen_ops = None

        # Under contention, we try harder to keep pool 0 for latency-sensitive work.
        queues_in_order = (s.q_query, s.q_interactive, s.q_batch)

        for q in queues_in_order:
            scans = min(len(q), s.max_scan_per_queue)
            for _ in range(scans):
                p = q.popleft()

                # Drop completed pipelines.
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    _drop_pipeline(p.pipeline_id)
                    continue

                # Enforce pool preference if possible (only for pool 0).
                if not _prefer_latency_pool(p.priority, pool_id):
                    # Requeue and keep scanning; maybe another pool will pick it up.
                    q.append(p)
                    continue

                ops = _has_ready_op(p)
                if not ops:
                    # Not runnable yet; keep it in queue.
                    q.append(p)
                    continue

                chosen_p = p
                chosen_ops = ops
                # Requeue pipeline immediately for fairness; we only schedule one op now.
                q.append(p)
                break

            if chosen_p is not None:
                break

        if chosen_p is None:
            # If pool 0 found nothing due to preference, allow batch as a fallback (avoid idling).
            if s.executor.num_pools > 1 and pool_id == 0 and len(s.q_batch) > 0:
                q = s.q_batch
                scans = min(len(q), s.max_scan_per_queue)
                for _ in range(scans):
                    p = q.popleft()
                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        _drop_pipeline(p.pipeline_id)
                        continue
                    ops = _has_ready_op(p)
                    if not ops:
                        q.append(p)
                        continue
                    chosen_p = p
                    chosen_ops = ops
                    q.append(p)
                    break

        if chosen_p is None:
            continue

        # Size request. If batch is being served while latency backlog exists, cap batch a bit.
        serve_batch_under_contention = (chosen_p.priority == Priority.BATCH_PIPELINE) and _higher_prio_backlog_exists()
        op0 = chosen_ops[0]
        req_cpu, req_ram = _size_request(chosen_p, op0, pool_id, serve_batch_under_contention)

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
