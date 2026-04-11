# policy_key: scheduler_medium_014
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.090327
# generation_seconds: 81.09
# generated_at: 2026-03-12T22:13:26.982096
@register_scheduler_init(key="scheduler_medium_014")
def scheduler_medium_014_init(s):
    """
    Priority-aware, latency-improving scheduler (incremental improvement over naive FIFO).

    Key ideas (kept intentionally simple / "small improvements first"):
      1) Priority queues: always try to schedule INTERACTIVE/QUERY before BATCH.
      2) Avoid "give whole pool to one op": allocate a bounded slice per op to reduce queueing.
      3) Keep headroom for high priority: when high-priority work exists, limit batch so it
         doesn't consume the last resources.
      4) OOM-aware retry: if an op fails with an OOM-like error, bump its RAM hint and retry.

    Notes:
      - No explicit preemption here (needs visibility into running containers not exposed
        in the minimal interface).
      - We avoid double-assigning the same pipeline within a single tick (fairness + safety).
    """
    s.tick = 0

    # Queues keyed by Priority; store Pipeline objects.
    s.queues = {
        Priority.INTERACTIVE: [],
        Priority.QUERY: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.in_queue = set()  # pipeline_id currently tracked in any queue
    s.arrival_tick = {}  # pipeline_id -> tick first seen

    # Per-op hints derived from failures (primarily OOM).
    # Key: (pipeline_id, op_key) where op_key is op.op_id if present else id(op)
    s.op_ram_hint = {}
    s.op_cpu_hint = {}
    s.op_attempts = {}

    # Basic knobs (kept conservative)
    s.max_retries_per_op = 3
    s.batch_headroom_frac = 0.20  # reserve this fraction for interactive/query if they exist

    # Per-priority sizing fractions of pool capacity (caps per container)
    s.cpu_frac = {
        Priority.INTERACTIVE: 0.50,
        Priority.QUERY: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.ram_frac = {
        Priority.INTERACTIVE: 0.50,
        Priority.QUERY: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Soft minimums to avoid tiny containers (units follow simulator's cpu/ram units)
    s.min_cpu = 1
    s.min_ram = 1


@register_scheduler(key="scheduler_medium_014")
def scheduler_medium_014_scheduler(s, results: List[ExecutionResult],
                                   pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Scheduler step:
      - Ingest new pipelines into priority queues.
      - Update RAM hints on OOM failures.
      - For each pool, greedily pack multiple assignments:
          * Prefer INTERACTIVE then QUERY then BATCH
          * Keep headroom if high-priority exists
          * Cap per-op allocations (avoid single-op monopolization)
    """
    s.tick += 1

    def _prio_bucket(p: Pipeline):
        # Default unknown priorities to BATCH to be safe.
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr in s.queues:
            return pr
        return Priority.BATCH_PIPELINE

    def _op_id(op):
        # Try stable op id first; fall back to Python object id.
        return getattr(op, "op_id", None) if getattr(op, "op_id", None) is not None else id(op)

    def _infer_pipeline_id_from_op(op):
        return getattr(op, "pipeline_id", None)

    def _looks_like_oom(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _enqueue_pipeline(p: Pipeline):
        pid = p.pipeline_id
        if pid in s.in_queue:
            return
        s.in_queue.add(pid)
        s.arrival_tick.setdefault(pid, s.tick)
        s.queues[_prio_bucket(p)].append(p)

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Process results to learn from failures
    if results:
        for r in results:
            if not r.failed():
                continue

            # Update hints per op when possible (especially for OOM).
            is_oom = _looks_like_oom(getattr(r, "error", None))
            for op in getattr(r, "ops", []) or []:
                pid = _infer_pipeline_id_from_op(op)
                if pid is None:
                    continue

                k = (pid, _op_id(op))
                s.op_attempts[k] = s.op_attempts.get(k, 0) + 1

                if is_oom:
                    # Increase RAM hint aggressively to avoid repeated OOM churn.
                    prev = s.op_ram_hint.get(k, 0)
                    tried = getattr(r, "ram", 0) or 0
                    # At minimum double the last tried RAM; also ensure it strictly increases.
                    bumped = max(prev * 2, tried * 2, prev + 1, tried + 1, s.min_ram)
                    s.op_ram_hint[k] = bumped

    def _pipeline_done_or_drop(p: Pipeline) -> bool:
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # Do not immediately drop on failures; we allow retries (FAILED is assignable).
        return False

    def _get_assignable_op(p: Pipeline, assigned_pipeline_ids: set, assigned_op_keys: set):
        """Return a single assignable op for pipeline p, or None."""
        if p.pipeline_id in assigned_pipeline_ids:
            return None
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not ops:
            return None
        op = ops[0]
        opk = (p.pipeline_id, _op_id(op))
        if opk in assigned_op_keys:
            return None
        # If we've exceeded retries for this op, treat as non-assignable (avoid infinite loops).
        if s.op_attempts.get(opk, 0) > s.max_retries_per_op:
            return None
        return op

    def _has_high_priority_work() -> bool:
        # Cheap check: if queues non-empty; more precise requires scanning statuses.
        return bool(s.queues[Priority.INTERACTIVE] or s.queues[Priority.QUERY])

    def _pick_next(prio: Priority, assigned_pipeline_ids: set, assigned_op_keys: set):
        """
        Round-robin: rotate through the queue, return first pipeline with a ready op.
        Pipelines without ready ops are kept in queue (rotated to back).
        """
        q = s.queues.get(prio, [])
        if not q:
            return None, None

        # Limit scan to current queue length to avoid infinite rotation.
        n = len(q)
        for _ in range(n):
            p = q.pop(0)

            # Remove completed pipelines from tracking entirely.
            if _pipeline_done_or_drop(p):
                s.in_queue.discard(p.pipeline_id)
                s.arrival_tick.pop(p.pipeline_id, None)
                continue

            op = _get_assignable_op(p, assigned_pipeline_ids, assigned_op_keys)
            q.append(p)  # keep pipeline in queue regardless; round-robin fairness
            if op is not None:
                return p, op

        return None, None

    def _cap_int(x, lo):
        # Ensure positive int-like values; simulator likely accepts ints.
        try:
            xi = int(x)
        except Exception:
            xi = lo
        return max(lo, xi)

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # If nothing changed, exit early (but keep deterministic tick increments).
    if not pipelines and not results and not (s.queues[Priority.INTERACTIVE] or s.queues[Priority.QUERY] or s.queues[Priority.BATCH_PIPELINE]):
        return suspensions, assignments

    # Track within-tick assignments to avoid double-assigning the same pipeline/op.
    assigned_pipeline_ids = set()
    assigned_op_keys = set()

    # Scheduling: per pool, pack assignments while resources remain.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = getattr(pool, "avail_cpu_pool", 0) or 0
        avail_ram = getattr(pool, "avail_ram_pool", 0) or 0
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        max_cpu = getattr(pool, "max_cpu_pool", avail_cpu) or avail_cpu
        max_ram = getattr(pool, "max_ram_pool", avail_ram) or avail_ram

        high_waiting = _has_high_priority_work()
        reserve_cpu = _cap_int(max_cpu * s.batch_headroom_frac, 0) if high_waiting else 0
        reserve_ram = _cap_int(max_ram * s.batch_headroom_frac, 0) if high_waiting else 0

        # Greedy packing: schedule multiple ops per pool.
        while avail_cpu > 0 and avail_ram > 0:
            # Priority order for latency
            p, op = _pick_next(Priority.INTERACTIVE, assigned_pipeline_ids, assigned_op_keys)
            if p is None:
                p, op = _pick_next(Priority.QUERY, assigned_pipeline_ids, assigned_op_keys)
            if p is None:
                # For batch, keep headroom if high-priority exists
                eff_cpu = avail_cpu - reserve_cpu
                eff_ram = avail_ram - reserve_ram
                if eff_cpu <= 0 or eff_ram <= 0:
                    break
                p, op = _pick_next(Priority.BATCH_PIPELINE, assigned_pipeline_ids, assigned_op_keys)
                if p is None:
                    break
            if p is None or op is None:
                break

            pr = _prio_bucket(p)

            # Compute capped per-op allocation
            base_cpu = _cap_int(max_cpu * s.cpu_frac.get(pr, 0.25), s.min_cpu)
            base_ram = _cap_int(max_ram * s.ram_frac.get(pr, 0.25), s.min_ram)

            opk = (p.pipeline_id, _op_id(op))
            hint_ram = s.op_ram_hint.get(opk, 0)
            hint_cpu = s.op_cpu_hint.get(opk, 0)

            req_cpu = max(base_cpu, _cap_int(hint_cpu, 0))
            req_ram = max(base_ram, _cap_int(hint_ram, 0))

            # For batch, respect headroom limits when high priority exists
            if pr == Priority.BATCH_PIPELINE and high_waiting:
                req_cpu = min(req_cpu, max(0, avail_cpu - reserve_cpu))
                req_ram = min(req_ram, max(0, avail_ram - reserve_ram))

            # Final clamp to available resources
            cpu = min(avail_cpu, req_cpu)
            ram = min(avail_ram, req_ram)

            if cpu < s.min_cpu or ram < s.min_ram:
                # Not enough resources to place anything useful.
                break

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

            # Update within-tick tracking + available resources
            assigned_pipeline_ids.add(p.pipeline_id)
            assigned_op_keys.add(opk)
            avail_cpu -= cpu
            avail_ram -= ram

    return suspensions, assignments
