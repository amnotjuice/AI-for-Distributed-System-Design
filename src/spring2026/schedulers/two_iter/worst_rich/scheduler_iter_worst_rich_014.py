# policy_key: scheduler_iter_worst_rich_014
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.038858
# generation_seconds: 31.90
# generated_at: 2026-04-12T00:56:38.444003
@register_scheduler_init(key="scheduler_iter_worst_rich_014")
def scheduler_iter_worst_rich_014_init(s):
    """
    Iteration 2: Fix obvious correctness flaws and reduce OOM-driven timeouts.

    Key changes vs prior attempt:
      - Never drop pipelines just because they have FAILED ops; FAILED is re-assignable in Eudoxia.
      - Stop starving operators with tiny RAM caps; allocate RAM generously (close to pool availability)
        because extra RAM has no perf penalty in this simulator but under-allocation causes OOM.
      - Priority-aware admission: schedule QUERY > INTERACTIVE > BATCH.
      - Keep CPU modest for batch to reduce interference, but still give enough to make progress.
      - Basic OOM learning: if an op fails with OOM, record a higher RAM hint for that (pipeline, op).
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track enqueued pipelines to avoid duplicates; lazy-removal on completion.
    s.enqueued = set()

    # Per-(pipeline, op) RAM hints learned from OOM failures.
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram_hint

    # Heuristic defaults
    s.min_cpu = 1
    s.min_ram = 1

    # Batch CPU cap as a fraction of pool max (RAM is not capped to avoid OOM storms)
    s.batch_cpu_cap_frac = 0.5

    # Try to keep a little CPU headroom for interactive if multiple ops contend within a pool
    s.query_cpu_target = 0.9
    s.interactive_cpu_target = 0.8
    s.batch_cpu_target = 0.6


@register_scheduler(key="scheduler_iter_worst_rich_014")
def scheduler_iter_worst_rich_014_scheduler(s, results, pipelines):
    """
    Priority-aware FIFO + OOM-robust sizing.

    Dispatch policy (per tick):
      1) Enqueue new pipelines into per-priority queues (QUERY > INTERACTIVE > BATCH).
      2) Update RAM hints from OOM failures to avoid repeat failures.
      3) For each pool, schedule at most one ready operator:
           - Pick first runnable pipeline from highest priority queue (round-robin within each queue).
           - Allocate RAM generously (typically all available RAM in pool, bounded by max).
           - Allocate CPU based on priority (QUERY/INTERACTIVE can take most of pool; BATCH is capped).
      4) Requeue chosen pipeline (lazy, to keep scanning simple).
    """
    # -------- helpers --------
    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(op):
        # Best-effort stable identifier across retries within a run.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _is_successful(p):
        return p.runtime_status().is_pipeline_successful()

    def _get_ready_ops(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return ops[:1] if ops else []

    def _is_oom_error(err):
        msg = str(err or "").lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _target_cpu_frac(prio):
        if prio == Priority.QUERY:
            return s.query_cpu_target
        if prio == Priority.INTERACTIVE:
            return s.interactive_cpu_target
        return s.batch_cpu_target

    def _compute_resources(p, op, pool_id):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        # CPU: choose a priority-based fraction of pool max, bounded by available.
        cpu_target = max(s.min_cpu, int(pool.max_cpu_pool * _target_cpu_frac(p.priority)))
        if p.priority == Priority.BATCH_PIPELINE:
            # Extra cap for batch to reduce latency impact on higher priorities.
            cpu_cap = max(s.min_cpu, int(pool.max_cpu_pool * s.batch_cpu_cap_frac))
            cpu_target = min(cpu_target, cpu_cap)

        req_cpu = max(s.min_cpu, min(avail_cpu, cpu_target))

        # RAM: allocate generously to avoid OOM storms.
        # Since extra RAM has no perf penalty in this simulator, we bias toward safety.
        # Start from an OOM-learned hint if present, otherwise use all available RAM.
        hint = s.op_ram_hint.get((p.pipeline_id, _op_key(op)), 0)
        req_ram = max(s.min_ram, min(avail_ram, pool.max_ram_pool))
        if hint > 0:
            req_ram = max(req_ram, min(hint, avail_ram, pool.max_ram_pool))

        # Ensure we never request more RAM than available, and at least min.
        req_ram = max(s.min_ram, min(req_ram, avail_ram))

        return req_cpu, req_ram

    # -------- ingest pipelines --------
    for p in pipelines:
        if p.pipeline_id not in s.enqueued:
            s.enqueued.add(p.pipeline_id)
            _queue_for_priority(p.priority).append(p)

    # early exit if nothing to do
    if not pipelines and not results:
        return [], []

    # -------- learn from results (OOM backoff) --------
    for r in results:
        # If failed due to OOM, bump RAM hint for retry.
        if hasattr(r, "failed") and r.failed() and _is_oom_error(getattr(r, "error", "")):
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                # Can't safely key without pipeline id; skip learning.
                continue
            ops = getattr(r, "ops", []) or []
            used_ram = int(getattr(r, "ram", 0) or 0)
            for op in ops:
                k = (pid, _op_key(op))
                prev = int(s.op_ram_hint.get(k, 0) or 0)
                # Exponential backoff, but never less than what we just tried.
                # If used_ram missing/0, still nudge upward.
                bump_base = used_ram if used_ram > 0 else max(prev, 1)
                s.op_ram_hint[k] = max(prev, bump_base * 2)

    suspensions = []
    assignments = []

    # -------- dispatch: one assignment per pool --------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        chosen_p = None
        chosen_ops = None
        chosen_q = None

        # Strict priority: QUERY -> INTERACTIVE -> BATCH
        for q in (s.q_query, s.q_interactive, s.q_batch):
            # bounded scan over current queue length
            for _ in range(len(q)):
                p = q.popleft()

                # Drop fully completed pipelines (lazy cleanup).
                if _is_successful(p):
                    s.enqueued.discard(p.pipeline_id)
                    continue

                ops = _get_ready_ops(p)
                if not ops:
                    # Not runnable yet; keep it in queue.
                    q.append(p)
                    continue

                chosen_p = p
                chosen_ops = ops
                chosen_q = q
                break
            if chosen_p is not None:
                break

        if chosen_p is None:
            continue

        req_cpu, req_ram = _compute_resources(chosen_p, chosen_ops[0], pool_id)

        # If we can't allocate minimal resources, just requeue and move on.
        if req_cpu <= 0 or req_ram <= 0:
            chosen_q.append(chosen_p)
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

        # Requeue pipeline so its remaining ops can be scheduled later.
        chosen_q.append(chosen_p)

    return suspensions, assignments
