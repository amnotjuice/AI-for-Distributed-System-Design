# policy_key: scheduler_medium_016
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.066597
# generation_seconds: 61.17
# generated_at: 2026-03-12T22:15:37.340637
@register_scheduler_init(key="scheduler_medium_016")
def scheduler_medium_016_init(s):
    """
    Priority-aware, multi-op scheduler (incremental improvement over naive FIFO).

    Main ideas (kept intentionally simple and robust):
      - Maintain per-priority round-robin queues (QUERY > INTERACTIVE > BATCH).
      - Avoid "give everything to one op": allocate bounded CPU/RAM quanta so we can
        schedule multiple ops per tick and reduce head-of-line blocking.
      - Keep small headroom by preventing BATCH from consuming the last slice of
        pool resources (helps tail latency for late-arriving high-priority work).
      - Learn from OOMs: if an op fails with an OOM-like error, increase a per-pipeline
        RAM multiplier for future attempts.
    """
    # Per-priority pipeline queues (round-robin via index pointers).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.queue_idx = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Track membership to avoid duplicate enqueues.
    s.enqueued = set()

    # OOM learning: pipeline_id -> multiplier for requested RAM.
    s.pipeline_ram_mult = {}

    # Map operator object identity to its pipeline_id to attribute failures to a pipeline.
    s.op_to_pipeline_id = {}

    # Tunables (small/obvious improvements first).
    s.max_scan_per_pick = 32              # scan up to N pipelines to find runnable work
    s.max_assignments_per_pool = 8        # avoid over-assigning in one tick
    s.batch_headroom_frac = 0.15          # keep headroom from batch (CPU+RAM) for latency protection
    s.max_ram_mult = 8.0                  # cap OOM backoff
    s.ram_mult_on_oom = 2.0               # exponential backoff factor on OOM


@register_scheduler(key="scheduler_medium_016")
def scheduler_medium_016_scheduler(s, results: List[ExecutionResult],
                                   pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    # -----------------------------
    # Helper functions (local)
    # -----------------------------
    def _is_oom_error(err) -> bool:
        if not err:
            return False
        msg = str(err).lower()
        # Be permissive: different simulators/engines use different strings.
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)

    def _pipeline_done_or_dropped(p: Pipeline) -> bool:
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # Do not drop on FAILED automatically; FAILED is assignable and we want to retry (esp. OOM).
        return False

    def _get_assignable_ops(p: Pipeline):
        st = p.runtime_status()
        return st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)

    def _get_ram_hint(op, pool) -> float:
        # Try common attribute names; fall back to small fraction of pool size.
        for name in ("ram_min", "min_ram", "min_mem", "mem_min", "memory_min", "ram"):
            v = getattr(op, name, None)
            if isinstance(v, (int, float)) and v > 0:
                return float(v)
        # Fallback: assume small-ish operator; pick a conservative fraction.
        # (Using max pool RAM here avoids allocating near-zero and inducing OOM churn.)
        return max(1.0, float(pool.max_ram_pool) * 0.08)

    def _cpu_quantum(priority, pool) -> float:
        # Bound per-op CPU to avoid monopolization.
        # Use fractions of pool max CPU; also ensure at least 1.
        max_cpu = float(pool.max_cpu_pool)
        if priority == Priority.QUERY:
            frac = 0.50
        elif priority == Priority.INTERACTIVE:
            frac = 0.35
        else:
            frac = 0.25
        return max(1.0, max_cpu * frac)

    def _rotate_pick_pipeline(priority) -> Pipeline:
        q = s.queues[priority]
        if not q:
            return None

        n = len(q)
        start = s.queue_idx[priority] % n
        scanned = 0

        while scanned < min(n, s.max_scan_per_pick):
            idx = (start + scanned) % n
            p = q[idx]
            if _pipeline_done_or_dropped(p):
                # Remove completed pipelines eagerly.
                s.enqueued.discard(p.pipeline_id)
                q.pop(idx)
                n -= 1
                if n <= 0:
                    s.queue_idx[priority] = 0
                    return None
                # Keep start anchored to current idx to avoid skipping.
                start = start % n
                scanned = 0
                continue

            ops = _get_assignable_ops(p)
            if ops:
                # Next RR start is after this pipeline.
                s.queue_idx[priority] = (idx + 1) % max(1, len(q))
                return p

            scanned += 1

        # No runnable pipeline found.
        s.queue_idx[priority] = (start + scanned) % max(1, len(q))
        return None

    def _choose_next_runnable_pipeline(allow_batch: bool) -> Pipeline:
        # Strict priority ordering; batch optionally disabled during high-priority pass.
        for pr in (Priority.QUERY, Priority.INTERACTIVE):
            p = _rotate_pick_pipeline(pr)
            if p is not None:
                return p
        if allow_batch:
            return _rotate_pick_pipeline(Priority.BATCH_PIPELINE)
        return None

    def _enqueue_pipeline(p: Pipeline):
        if p.pipeline_id in s.enqueued:
            return
        s.enqueued.add(p.pipeline_id)
        s.queues[p.priority].append(p)
        if p.pipeline_id not in s.pipeline_ram_mult:
            s.pipeline_ram_mult[p.pipeline_id] = 1.0

    # -----------------------------
    # Ingest new pipelines
    # -----------------------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # -----------------------------
    # Learn from results (OOM backoff)
    # -----------------------------
    for r in results:
        # Attribute OOMs to the originating pipeline, if we can.
        if r.failed() and _is_oom_error(r.error):
            for op in getattr(r, "ops", []) or []:
                pid = s.op_to_pipeline_id.get(id(op))
                if pid is not None:
                    cur = float(s.pipeline_ram_mult.get(pid, 1.0))
                    nxt = min(s.max_ram_mult, cur * float(s.ram_mult_on_oom))
                    s.pipeline_ram_mult[pid] = nxt

        # Cleanup op->pipeline mapping for completed/failed ops.
        for op in getattr(r, "ops", []) or []:
            s.op_to_pipeline_id.pop(id(op), None)

    # Early exit if nothing to do.
    if not pipelines and not results and not any(s.queues.values()):
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # -----------------------------
    # Scheduling loop
    # Two-phase per pool:
    #   (1) schedule high-priority work (QUERY/INTERACTIVE) as much as possible
    #   (2) schedule batch, but keep headroom to reduce p95/p99 latency impact
    # -----------------------------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Phase 1: high priority
        per_pool_assignments = 0
        while per_pool_assignments < s.max_assignments_per_pool and avail_cpu > 0 and avail_ram > 0:
            p = _choose_next_runnable_pipeline(allow_batch=False)
            if p is None:
                break

            ops = _get_assignable_ops(p)
            if not ops:
                continue
            op_list = ops[:1]

            # Size request: RAM hint * learned multiplier, CPU quantum by priority.
            ram_hint = _get_ram_hint(op_list[0], pool)
            ram_mult = float(s.pipeline_ram_mult.get(p.pipeline_id, 1.0))
            req_ram = min(avail_ram, max(1.0, ram_hint * ram_mult))

            req_cpu = min(avail_cpu, _cpu_quantum(p.priority, pool))
            if req_cpu <= 0 or req_ram <= 0:
                break

            assignments.append(Assignment(
                ops=op_list,
                cpu=req_cpu,
                ram=req_ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id
            ))
            for op in op_list:
                s.op_to_pipeline_id[id(op)] = p.pipeline_id

            avail_cpu -= req_cpu
            avail_ram -= req_ram
            per_pool_assignments += 1

        # Refresh pool availability snapshot for batch placement decisions
        # (still approximate within a tick, but consistent with the naive example style).
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Phase 2: batch with headroom reservation
        batch_cpu_cap = max(0.0, avail_cpu * (1.0 - float(s.batch_headroom_frac)))
        batch_ram_cap = max(0.0, avail_ram * (1.0 - float(s.batch_headroom_frac)))
        if batch_cpu_cap <= 0 or batch_ram_cap <= 0:
            continue

        per_pool_batch = 0
        while (per_pool_assignments + per_pool_batch) < s.max_assignments_per_pool and batch_cpu_cap > 0 and batch_ram_cap > 0:
            p = _choose_next_runnable_pipeline(allow_batch=True)
            if p is None or p.priority != Priority.BATCH_PIPELINE:
                break

            ops = _get_assignable_ops(p)
            if not ops:
                continue
            op_list = ops[:1]

            ram_hint = _get_ram_hint(op_list[0], pool)
            ram_mult = float(s.pipeline_ram_mult.get(p.pipeline_id, 1.0))
            req_ram = min(batch_ram_cap, max(1.0, ram_hint * ram_mult))

            req_cpu = min(batch_cpu_cap, _cpu_quantum(p.priority, pool))
            if req_cpu <= 0 or req_ram <= 0:
                break

            assignments.append(Assignment(
                ops=op_list,
                cpu=req_cpu,
                ram=req_ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id
            ))
            for op in op_list:
                s.op_to_pipeline_id[id(op)] = p.pipeline_id

            batch_cpu_cap -= req_cpu
            batch_ram_cap -= req_ram
            per_pool_batch += 1

    return suspensions, assignments
