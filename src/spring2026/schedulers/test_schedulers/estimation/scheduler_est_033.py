# policy_key: scheduler_est_033
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050526
# generation_seconds: 46.38
# generated_at: 2026-04-10T10:12:04.616301
@register_scheduler_init(key="scheduler_est_033")
def scheduler_est_033_init(s):
    """Priority + memory-hinted + bounded-parallelism scheduler.

    Goals:
      - Protect QUERY/INTERACTIVE latency via strict priority ordering.
      - Reduce OOM/failures by sizing RAM from op.estimate.mem_peak_gb (noisy hint).
      - Avoid starvation by round-robin within each priority and soft admission when nothing fits.
      - Improve throughput vs naive FIFO by packing multiple ops per pool tick with per-op CPU/RAM caps.
    """
    import collections

    # Separate FIFO queues per priority (strict service order across priorities).
    s.q_query = collections.deque()
    s.q_interactive = collections.deque()
    s.q_batch = collections.deque()

    # Track pipelines we've enqueued to avoid duplicates if generator re-sends references.
    s._seen_pipeline_ids = set()

    # Per-operator RAM backoff multiplier for retries after failures (OOM or otherwise).
    # Keyed by (pipeline_id, op_key).
    s._op_ram_mult = {}

    # Remember recently failed pipelines to fast-track retries (but still keep priority order).
    s._recent_failures = collections.deque(maxlen=512)

    # Conservative defaults / caps.
    s._safety_mult = 1.20          # extra RAM above estimate to absorb estimator noise
    s._oom_backoff = 1.60          # multiply RAM on failure
    s._max_ram_mult = 12.0         # cap backoff to avoid runaway reservations
    s._min_cpu = 1.0               # minimum CPU per assignment
    s._min_ram_gb = 0.25           # minimum RAM per assignment (GB), avoids zero/denorm


@register_scheduler(key="scheduler_est_033")
def scheduler_est_033_scheduler(s, results, pipelines):
    import math
    import collections

    def _enqueue_pipeline(p):
        if p is None:
            return
        pid = getattr(p, "pipeline_id", None)
        if pid is None:
            # Fall back: allow enqueue, but avoid hard failure
            pid = id(p)
        if pid in s._seen_pipeline_ids:
            return
        s._seen_pipeline_ids.add(pid)
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _op_key(pipeline_id, op):
        # Prefer a stable operator id if present; otherwise fall back to object id.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # Avoid huge/unhashable; name is fine, id is fine
                    if isinstance(v, (int, str)):
                        return (pipeline_id, v)
                except Exception:
                    pass
        return (pipeline_id, id(op))

    def _get_est_mem_gb(op):
        try:
            est = getattr(op, "estimate", None)
            if est is None:
                return None
            v = getattr(est, "mem_peak_gb", None)
            if v is None:
                return None
            # Guard against nonsensical values
            if isinstance(v, (int, float)) and v >= 0:
                return float(v)
            return None
        except Exception:
            return None

    def _choose_next_assignable_op_from_queue(q, pool):
        """Round-robin scan: find first pipeline with an assignable op that plausibly fits."""
        if not q:
            return None, None, None

        n = len(q)
        for _ in range(n):
            p = q.popleft()
            status = p.runtime_status()

            # Drop completed pipelines.
            if status.is_pipeline_successful():
                continue

            # If pipeline has failures, we still allow retry (platform model may permit).
            # We'll just requeue unless we can schedule something now.
            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                q.append(p)
                continue

            # Choose the "best" op among currently assignable ones: smallest estimated memory first.
            # This improves packing and reduces head-of-line blocking.
            def _op_sort_key(op):
                est = _get_est_mem_gb(op)
                if est is None:
                    # Unknown estimates go after known small ops but before huge ones.
                    return (1, 0.0)
                return (0, est)

            ops_sorted = sorted(list(ops), key=_op_sort_key)

            for op in ops_sorted:
                pid = getattr(p, "pipeline_id", id(p))
                key = _op_key(pid, op)
                mult = s._op_ram_mult.get(key, 1.0)

                est_mem = _get_est_mem_gb(op)
                # Compute a RAM ask:
                # - If estimate known: (estimate * safety * mult), with a small floor.
                # - If unknown: reserve a moderate fraction of the pool to avoid OOM cascades.
                if est_mem is not None:
                    ram_ask = max(s._min_ram_gb, est_mem * s._safety_mult * mult)
                else:
                    ram_ask = max(s._min_ram_gb, 0.35 * pool.max_ram_pool * mult)

                # Hard cap ask to pool max; we can still try if estimate seems to overshoot.
                ram_ask = min(ram_ask, pool.max_ram_pool)

                # Feasibility check against current *available* RAM in this pool.
                # If it doesn't fit now, maybe another pool can take it; don't block this pool.
                if ram_ask <= pool.avail_ram_pool + 1e-9:
                    # Found something to schedule; put pipeline back for future ops.
                    q.append(p)
                    return p, op, ram_ask

            # No op in this pipeline fits this pool right now; requeue and keep scanning.
            q.append(p)

        return None, None, None

    def _cpu_cap_for_priority(pool, priority):
        # Keep latency low for high priority while allowing some parallelism.
        if priority == Priority.QUERY:
            frac = 0.60
        elif priority == Priority.INTERACTIVE:
            frac = 0.45
        else:
            frac = 0.30
        return max(s._min_cpu, float(pool.max_cpu_pool) * frac)

    # Enqueue new arrivals.
    for p in pipelines:
        _enqueue_pipeline(p)

    # Process results: on failures, increase RAM multiplier for the involved operators and
    # push the pipeline to the front of its queue (fast retry), but do not drop it.
    for r in results:
        if getattr(r, "failed", lambda: False)():
            pid = getattr(r, "pipeline_id", None)
            # ExecutionResult in the template doesn't include pipeline_id; fall back:
            # infer from ops if possible, else skip multiplier update safely.
            op_list = getattr(r, "ops", []) or []
            for op in op_list:
                # Best-effort: find pipeline_id from op if present, else use None sentinel.
                inferred_pid = pid
                if inferred_pid is None:
                    for attr in ("pipeline_id", "pid"):
                        if hasattr(op, attr):
                            try:
                                inferred_pid = getattr(op, attr)
                                break
                            except Exception:
                                pass
                if inferred_pid is None:
                    inferred_pid = -1
                key = _op_key(inferred_pid, op)
                old = s._op_ram_mult.get(key, 1.0)
                s._op_ram_mult[key] = min(s._max_ram_mult, old * s._oom_backoff)

            # Track recent failures for mild prioritization (handled below via queue rotation).
            s._recent_failures.append((getattr(r, "priority", None), getattr(r, "pool_id", None)))

    # Early exit: if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Schedule per pool, packing multiple ops as resources allow.
    # Strict priority order across queues: QUERY > INTERACTIVE > BATCH.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 1e-9 or avail_ram <= 1e-9:
            continue

        # Try to place as many ops as we can in this pool this tick.
        # We do local accounting to avoid overcommitting in a single scheduler call.
        while avail_cpu >= s._min_cpu and avail_ram >= s._min_ram_gb:
            chosen = None  # (pipeline, op, ram_ask)
            # Priority order: query, interactive, batch.
            for q in (s.q_query, s.q_interactive, s.q_batch):
                p, op, ram_ask = _choose_next_assignable_op_from_queue(q, pool)
                if p is None:
                    continue

                # Also ensure the estimated RAM isn't wildly larger than the pool max;
                # if it is, we still attempt with max RAM (estimator could be too high),
                # but only if we currently have enough available RAM to do so.
                est_mem = _get_est_mem_gb(op)
                if est_mem is not None and est_mem > pool.max_ram_pool * 1.8:
                    ram_ask = pool.max_ram_pool
                    if ram_ask > avail_ram + 1e-9:
                        # Can't fit now; keep searching.
                        continue

                if ram_ask <= avail_ram + 1e-9:
                    chosen = (p, op, ram_ask)
                    break

            if chosen is None:
                break

            p, op, ram_ask = chosen
            prio = p.priority

            # CPU sizing: cap per op by priority; also do not exceed what's left.
            cpu_cap = _cpu_cap_for_priority(pool, prio)
            cpu_ask = min(avail_cpu, cpu_cap)

            # Avoid allocating vanishing CPU.
            if cpu_ask < s._min_cpu - 1e-9:
                break

            # Final RAM ask: don't exceed remaining pool RAM.
            ram_ask = min(avail_ram, max(s._min_ram_gb, float(ram_ask)))

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_ask,
                    ram=ram_ask,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            avail_cpu -= cpu_ask
            avail_ram -= ram_ask

    return suspensions, assignments
