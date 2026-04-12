# policy_key: scheduler_iter_worst_simple_012
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051304
# generation_seconds: 42.70
# generated_at: 2026-04-12T00:42:39.633408
@register_scheduler_init(key="scheduler_iter_worst_simple_012")
def scheduler_iter_worst_simple_012_init(s):
    """
    Priority-first multi-assignment scheduler focused on weighted latency.

    Key changes vs naive FIFO:
      - Strict priority queues: QUERY > INTERACTIVE > BATCH.
      - Multi-assign per pool per tick (fill capacity), instead of 1 op/pool/tick.
      - Pool partitioning bias: if multiple pools, reserve pool 0 primarily for QUERY/INTERACTIVE.
      - Latency sizing: give high-priority ops most of the available CPU (sublinear scaling but helps finish sooner);
        keep BATCH on small CPU slices to avoid interference.
      - Failure learning: on failed execution, bump RAM hint aggressively if OOM-like; otherwise mild bump.
      - Do NOT drop pipelines just because they have FAILED ops; FAILED is retryable (ASSIGNABLE_STATES).
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track enqueued pipelines to avoid unbounded duplicate entries.
    s.enqueued = set()  # pipeline_id currently present in some queue (best-effort)

    # Resource hints learned per (pipeline_id, op_key)
    s.op_ram_hint = {}  # -> int
    s.op_cpu_hint = {}  # -> int

    # Failure counts to prevent infinite thrash on pathological operators
    s.op_fail_count = {}  # (pipeline_id, op_key) -> int
    s.max_fail_retries = 6

    s.tick = 0

    # Minimum "slice" allocations (units follow simulator's conventions)
    s.min_cpu = 1
    s.min_ram = 1


@register_scheduler(key="scheduler_iter_worst_simple_012")
def scheduler_iter_worst_simple_012_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - ingest new pipelines
      - learn from failures (RAM/CPU hints)
      - for each pool: repeatedly select next runnable op by strict priority and assign
    """
    from collections import deque

    s.tick += 1

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid in s.enqueued:
            return
        _queue_for_priority(p.priority).append(p)
        s.enqueued.add(pid)

    def _op_key(op):
        # Best-effort stable identity within a run.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _ready_ops_one(p):
        st = p.runtime_status()
        # Don't schedule from successful pipelines.
        if st.is_pipeline_successful():
            return []
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]

    def _pool_is_latency_reserved(pool_id):
        # If multiple pools exist, treat pool 0 as latency pool.
        return s.executor.num_pools > 1 and pool_id == 0

    def _can_use_pool(pipeline_priority, pool_id):
        # Hard bias: keep batch off pool 0 when there are other pools.
        if _pool_is_latency_reserved(pool_id) and pipeline_priority == Priority.BATCH_PIPELINE:
            return False
        return True

    def _oom_like(err_str):
        e = (err_str or "").lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e)

    def _learn_from_result(r):
        if not (hasattr(r, "failed") and r.failed()):
            return
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            return
        ops = getattr(r, "ops", None) or []
        err = str(getattr(r, "error", "") or "")
        is_oom = _oom_like(err)

        for op in ops:
            k = (pid, _op_key(op))
            s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1

            # Previous allocation we tried for this container.
            prev_ram = int(getattr(r, "ram", 0) or 0)
            prev_cpu = int(getattr(r, "cpu", 0) or 0)
            if prev_ram <= 0:
                prev_ram = s.op_ram_hint.get(k, s.min_ram)
            if prev_cpu <= 0:
                prev_cpu = s.op_cpu_hint.get(k, s.min_cpu)

            if is_oom:
                # Aggressive RAM backoff on OOM: double +1 (bounded later by pool max at scheduling time).
                s.op_ram_hint[k] = max(s.op_ram_hint.get(k, s.min_ram), prev_ram * 2 + 1)
                # Keep CPU stable on OOM.
                s.op_cpu_hint[k] = max(s.op_cpu_hint.get(k, s.min_cpu), prev_cpu)
            else:
                # Mild bump for non-OOM failures (might be underprovisioned or transient).
                s.op_ram_hint[k] = max(s.op_ram_hint.get(k, s.min_ram), int(prev_ram * 1.25) + 1)
                s.op_cpu_hint[k] = max(s.op_cpu_hint.get(k, s.min_cpu), int(prev_cpu * 1.25) + 1)

    def _size_for_latency(pool, pid, op, pipeline_priority):
        """
        Sizing policy:
          - QUERY: grab most available CPU (up to 90% of pool max and availability) to reduce completion time.
          - INTERACTIVE: similar but slightly less aggressive.
          - BATCH: small CPU slice to preserve headroom; RAM from hint (bounded).
        """
        opk = (pid, _op_key(op))
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = int(pool.avail_ram_pool)

        max_cpu = int(pool.max_cpu_pool)
        max_ram = int(pool.max_ram_pool)

        hinted_cpu = int(s.op_cpu_hint.get(opk, s.min_cpu))
        hinted_ram = int(s.op_ram_hint.get(opk, s.min_ram))

        # Always honor minimums.
        hinted_cpu = max(s.min_cpu, hinted_cpu)
        hinted_ram = max(s.min_ram, hinted_ram)

        if pipeline_priority == Priority.QUERY:
            cpu_cap = max(s.min_cpu, int(max_cpu * 0.90))
            req_cpu = min(avail_cpu, cpu_cap)
            req_cpu = max(req_cpu, hinted_cpu) if avail_cpu >= hinted_cpu else req_cpu
            # Ensure we don't request more than available.
            req_cpu = max(s.min_cpu, min(req_cpu, avail_cpu))

            ram_cap = max(s.min_ram, int(max_ram * 0.90))
            req_ram = max(hinted_ram, s.min_ram)
            req_ram = max(req_ram, min(avail_ram, ram_cap))
            req_ram = max(s.min_ram, min(req_ram, avail_ram))
            return req_cpu, req_ram

        if pipeline_priority == Priority.INTERACTIVE:
            cpu_cap = max(s.min_cpu, int(max_cpu * 0.80))
            req_cpu = min(avail_cpu, cpu_cap)
            req_cpu = max(req_cpu, hinted_cpu) if avail_cpu >= hinted_cpu else req_cpu
            req_cpu = max(s.min_cpu, min(req_cpu, avail_cpu))

            ram_cap = max(s.min_ram, int(max_ram * 0.85))
            req_ram = max(hinted_ram, s.min_ram)
            req_ram = max(req_ram, min(avail_ram, ram_cap))
            req_ram = max(s.min_ram, min(req_ram, avail_ram))
            return req_cpu, req_ram

        # BATCH_PIPELINE
        cpu_cap = max(s.min_cpu, min(2, int(max_cpu * 0.25) if max_cpu > 0 else 2))
        req_cpu = max(s.min_cpu, min(avail_cpu, max(cpu_cap, hinted_cpu if hinted_cpu <= cpu_cap else cpu_cap)))

        # For batch, try to meet hinted RAM (to avoid repeated OOM), but never take too much.
        ram_cap = max(s.min_ram, int(max_ram * 0.60))
        req_ram = max(s.min_ram, min(avail_ram, min(max(hinted_ram, s.min_ram), ram_cap)))
        return req_cpu, req_ram

    def _pop_next_runnable_for_pool(pool_id):
        """
        Strict priority: QUERY > INTERACTIVE > BATCH.
        We scan each queue (rotating) to find a pipeline that:
          - can use this pool (e.g., batch not on pool 0 when multiple pools)
          - has a runnable op (parents complete, op in ASSIGNABLE_STATES)
        """
        for q in (s.q_query, s.q_interactive, s.q_batch):
            n = len(q)
            for _ in range(n):
                p = q.popleft()
                pid = p.pipeline_id

                st = p.runtime_status()
                if st.is_pipeline_successful():
                    # Remove from enqueued set and drop it.
                    if pid in s.enqueued:
                        s.enqueued.remove(pid)
                    continue

                if not _can_use_pool(p.priority, pool_id):
                    # Can't run here; keep it queued.
                    q.append(p)
                    continue

                ops = _ready_ops_one(p)
                if not ops:
                    q.append(p)
                    continue

                opk = (pid, _op_key(ops[0]))
                if s.op_fail_count.get(opk, 0) > s.max_fail_retries:
                    # Give up on repeatedly failing ops to prevent thrash.
                    # We effectively drop the pipeline from scheduling; runtime may still mark failed.
                    if pid in s.enqueued:
                        s.enqueued.remove(pid)
                    continue

                # Keep pipeline queued so future ops can be scheduled; we don't want to "lose" it.
                q.append(p)
                return p, ops
        return None, None

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if no changes
    if not pipelines and not results:
        return [], []

    # Learn from results
    for r in results:
        _learn_from_result(r)

    suspensions = []  # No safe preemption API exposed in the prompt.
    assignments = []

    # Pool scheduling order: schedule latency pool (0) first, then others,
    # so high-priority work gets first dibs on the best pool.
    pool_order = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1:
        pool_order = [0] + [i for i in range(1, s.executor.num_pools)]

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]

        # Fill the pool with as many assignments as feasible.
        # Stop when resources too low for minimum slices or no runnable work.
        while True:
            avail_cpu = int(pool.avail_cpu_pool)
            avail_ram = int(pool.avail_ram_pool)
            if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
                break

            p, ops = _pop_next_runnable_for_pool(pool_id)
            if p is None:
                break

            op = ops[0]
            req_cpu, req_ram = _size_for_latency(pool, p.pipeline_id, op, p.priority)

            # If we can't satisfy even the minimum, stop trying on this pool this tick.
            if req_cpu < s.min_cpu or req_ram < s.min_ram:
                break
            if req_cpu > avail_cpu or req_ram > avail_ram:
                # If sizing overshot due to stale avail, clamp and proceed only if minimum met.
                req_cpu = min(avail_cpu, req_cpu)
                req_ram = min(avail_ram, req_ram)
                if req_cpu < s.min_cpu or req_ram < s.min_ram:
                    break

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

            # Loop again; pool.avail_* should reflect the just-made assignment in the simulator's view.

    return suspensions, assignments
