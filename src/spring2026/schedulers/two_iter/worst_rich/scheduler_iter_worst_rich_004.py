# policy_key: scheduler_iter_worst_rich_004
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048322
# generation_seconds: 34.89
# generated_at: 2026-04-12T00:50:51.762595
@register_scheduler_init(key="scheduler_iter_worst_rich_004")
def scheduler_iter_worst_rich_004_init(s):
    """
    Priority-aware FIFO with safe sizing + OOM-driven RAM backoff (working-first iteration).

    Fixes obvious flaws from the prior attempt:
      - Never drop a pipeline just because some ops are in FAILED; FAILED is retryable.
      - Start with "safe" RAM sizing (large fractions of available/max) to avoid mass OOMs.
      - On OOM failures, aggressively increase RAM hint for the specific (pipeline, op) and retry.
      - Simple pool preference: keep pool 0 for QUERY/INTERACTIVE when multiple pools exist.
      - Simple headroom reservation: BATCH cannot consume reserved CPU/RAM needed for latency classes.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Prevent duplicate enqueues; we requeue pipelines ourselves after considering them.
    s.enqueued = set()  # pipeline_id

    # Per (pipeline, op) learned RAM/CPU hints; primarily RAM for OOM handling.
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}  # (pipeline_id, op_key) -> cpu

    # Lightweight tick counter (may be useful later; not required for correctness).
    s.tick = 0

    # Reservation for higher-priority work when scheduling BATCH.
    s.reserve_cpu_frac = 0.25
    s.reserve_ram_frac = 0.25

    # Absolute minimums
    s.min_cpu = 1
    s.min_ram = 1


@register_scheduler(key="scheduler_iter_worst_rich_004")
def scheduler_iter_worst_rich_004_scheduler(s, results, pipelines):
    """
    Step policy:
      1) Enqueue new pipelines into per-priority queues.
      2) Update per-op hints from results (OOM -> double RAM).
      3) For each pool, pick the first runnable pipeline in priority order (QUERY > INTERACTIVE > BATCH)
         with pool preference (pool0 for latency-sensitive if multiple pools).
      4) Assign one runnable op per pool per tick, sized to avoid OOM and protect headroom:
           - QUERY/INTERACTIVE: take large fraction of available resources (finish fast).
           - BATCH: capped to leave reserved CPU/RAM for latency-sensitive arrivals.
    """
    s.tick += 1

    # ---- Helpers ----
    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue(p):
        pid = p.pipeline_id
        if pid in s.enqueued:
            return
        s.enqueued.add(pid)
        _queue_for_priority(p.priority).append(p)

    def _dequeue_mark(p):
        # Called when we pop from a queue; allows re-enqueue without duplicates.
        pid = p.pipeline_id
        if pid in s.enqueued:
            s.enqueued.remove(pid)

    def _op_key(op):
        # Best-effort stable key
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _is_success(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _ready_ops(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]

    def _pool_preference_order(prio):
        # If multiple pools exist, pool 0 is preferred for QUERY/INTERACTIVE; BATCH prefers non-zero.
        if s.executor.num_pools <= 1:
            return list(range(s.executor.num_pools))
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, s.executor.num_pools)]
        return [i for i in range(1, s.executor.num_pools)] + [0]

    def _size_request(p, op, pool_id):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        key = (p.pipeline_id, _op_key(op))

        # Apply learned hints if present; else choose safe defaults.
        hinted_ram = s.op_ram_hint.get(key, None)
        hinted_cpu = s.op_cpu_hint.get(key, None)

        # Default sizing: prioritize avoiding OOMs (RAM) and finishing quickly for high priority.
        if p.priority == Priority.QUERY:
            target_cpu = min(avail_cpu, max(s.min_cpu, int(pool.max_cpu_pool * 0.9)))
            target_ram = min(avail_ram, max(s.min_ram, int(pool.max_ram_pool * 0.9)))
        elif p.priority == Priority.INTERACTIVE:
            target_cpu = min(avail_cpu, max(s.min_cpu, int(pool.max_cpu_pool * 0.7)))
            target_ram = min(avail_ram, max(s.min_ram, int(pool.max_ram_pool * 0.8)))
        else:
            # BATCH: leave headroom for future QUERY/INTERACTIVE
            reserve_cpu = int(pool.max_cpu_pool * s.reserve_cpu_frac)
            reserve_ram = int(pool.max_ram_pool * s.reserve_ram_frac)

            cap_cpu = max(s.min_cpu, min(avail_cpu, avail_cpu - reserve_cpu))
            cap_ram = max(s.min_ram, min(avail_ram, avail_ram - reserve_ram))

            # Additional cap to reduce greedy single-op monopolization
            cap_cpu = min(cap_cpu, max(s.min_cpu, int(pool.max_cpu_pool * 0.6)))
            cap_ram = min(cap_ram, max(s.min_ram, int(pool.max_ram_pool * 0.6)))

            target_cpu = cap_cpu
            target_ram = cap_ram

        if hinted_cpu is not None:
            target_cpu = max(target_cpu, hinted_cpu)
        if hinted_ram is not None:
            target_ram = max(target_ram, hinted_ram)

        # Final clamp to what we can actually allocate right now.
        target_cpu = max(s.min_cpu, min(target_cpu, avail_cpu))
        target_ram = max(s.min_ram, min(target_ram, avail_ram))

        return target_cpu, target_ram

    # ---- Ingest ----
    for p in pipelines:
        _enqueue(p)

    # Early exit
    if not pipelines and not results:
        return [], []

    # ---- Learn from results ----
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        # Identify pipeline and ops; if pipeline_id isn't present we can't key precisely.
        pid = getattr(r, "pipeline_id", None)
        ops = getattr(r, "ops", []) or []
        if pid is None or not ops:
            continue

        err = str(getattr(r, "error", "") or "")
        oom_like = ("oom" in err.lower()) or ("out of memory" in err.lower())

        for op in ops:
            key = (pid, _op_key(op))
            prev_ram = s.op_ram_hint.get(key, max(s.min_ram, int(getattr(r, "ram", 1) or 1)))
            prev_cpu = s.op_cpu_hint.get(key, max(s.min_cpu, int(getattr(r, "cpu", 1) or 1)))

            # OOM => double RAM aggressively; keep CPU same.
            if oom_like:
                s.op_ram_hint[key] = max(prev_ram, int((getattr(r, "ram", prev_ram) or prev_ram) * 2))
                s.op_cpu_hint[key] = prev_cpu
            else:
                # Non-OOM failures: modest bump.
                s.op_ram_hint[key] = max(prev_ram, int((getattr(r, "ram", prev_ram) or prev_ram) * 1.25) + 1)
                s.op_cpu_hint[key] = max(prev_cpu, int((getattr(r, "cpu", prev_cpu) or prev_cpu) * 1.10) + 1)

    # ---- Schedule ----
    suspensions = []
    assignments = []

    # Choose pipelines for each pool, one assignment per pool per tick.
    # We iterate pools in numeric order; pool-preference is applied when selecting a pipeline.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        chosen_p = None
        chosen_ops = None

        # Priority order: query -> interactive -> batch
        queues = (s.q_query, s.q_interactive, s.q_batch)

        # For this pool, we scan each queue once (bounded) to find a runnable pipeline that "fits" pool preference.
        # If multiple pools exist, we avoid running BATCH on pool 0 unless nothing else runnable.
        for q in queues:
            qlen = len(q)
            for _ in range(qlen):
                p = q.popleft()
                _dequeue_mark(p)

                # Drop completed pipelines (only condition we treat as terminal here).
                if _is_success(p):
                    continue

                ops = _ready_ops(p)
                if not ops:
                    # Not ready yet; re-enqueue.
                    _enqueue(p)
                    continue

                # Pool preference rule (soft):
                if s.executor.num_pools > 1 and pool_id == 0 and p.priority == Priority.BATCH_PIPELINE:
                    # Defer batch on pool 0; keep scanning for latency-sensitive work.
                    _enqueue(p)
                    continue

                chosen_p = p
                chosen_ops = ops
                break

            if chosen_p is not None:
                break

        # If we found nothing runnable under the soft rule and this is pool 0, allow batch as fallback.
        if chosen_p is None and s.executor.num_pools > 1 and pool_id == 0:
            q = s.q_batch
            qlen = len(q)
            for _ in range(qlen):
                p = q.popleft()
                _dequeue_mark(p)
                if _is_success(p):
                    continue
                ops = _ready_ops(p)
                if not ops:
                    _enqueue(p)
                    continue
                chosen_p = p
                chosen_ops = ops
                break

        if chosen_p is None:
            continue

        req_cpu, req_ram = _size_request(chosen_p, chosen_ops[0], pool_id)
        if req_cpu <= 0 or req_ram <= 0:
            # Can't place now; re-enqueue.
            _enqueue(chosen_p)
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

        # Re-enqueue the pipeline for future ops.
        _enqueue(chosen_p)

    return suspensions, assignments
