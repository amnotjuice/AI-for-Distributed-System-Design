# policy_key: scheduler_medium_046
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.066345
# generation_seconds: 70.35
# generated_at: 2026-03-14T03:20:43.393717
@register_scheduler_init(key="scheduler_medium_046")
def scheduler_medium_046_init(s):
    """
    Priority-aware, work-conserving scheduler with small incremental improvements over FIFO:
      1) Separate queues by priority (QUERY > INTERACTIVE > BATCH) to reduce tail latency.
      2) Per-pool "soft reservations" for high-priority work (admission control for batch).
      3) Simple OOM/failed retry sizing: increase per-op RAM hint on failure; modest down-hint on success.
      4) Basic aging for batch pipelines to avoid indefinite starvation.
    """
    from collections import deque

    # Per-priority waiting queues (store Pipeline objects)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track whether a pipeline is currently enqueued to avoid duplicates
    s.enqueued_pipeline_ids = set()

    # Simple logical clock (ticks)
    s.ticks = 0

    # Enqueue tick per pipeline for aging
    s.enqueue_tick = {}

    # Retry + sizing hints keyed by (pipeline_id, op_key)
    s.op_ram_hint = {}   # desired RAM for op
    s.op_cpu_hint = {}   # desired CPU for op
    s.op_retry_count = {}

    # Policy knobs (kept conservative for "small improvements first")
    s.max_retries_per_op = 3

    # Soft reservation fractions per pool when high-priority work is waiting
    s.reserve_frac_cpu = 0.30
    s.reserve_frac_ram = 0.30

    # Aging: after this many ticks, batch is treated as interactive for selection
    s.batch_aging_ticks = 50


@register_scheduler(key="scheduler_medium_046")
def scheduler_medium_046_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into per-priority queues.
      - Update RAM/CPU hints based on recent results (failures => bump RAM, successes => gently down-hint).
      - For each pool, place as many ready operators as fit, always choosing highest effective priority first.
      - Apply soft reservations so BATCH does not consume all headroom when high-priority work is waiting.
    """
    # --- helpers (kept local; no imports at module top) ---
    def _is_high_prio(p):
        return p.priority in (Priority.QUERY, Priority.INTERACTIVE)

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(op):
        # Try common identifiers; fall back to repr for stability within a run.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    return str(v() if callable(v) else v)
                except Exception:
                    pass
        try:
            return str(op)
        except Exception:
            return repr(op)

    def _pipeline_done_or_terminal(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # Do NOT drop on failures; we retry with increased RAM hints.
        return False

    def _default_targets(priority, pool):
        # Conservative default sizing: enough to run, but small to reduce interference and allow concurrency.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        if priority == Priority.QUERY:
            cpu = min(2, max_cpu)
            ram = max(1, max_ram * 0.10)
        elif priority == Priority.INTERACTIVE:
            cpu = min(4, max_cpu)
            ram = max(1, max_ram * 0.20)
        else:
            cpu = min(6, max_cpu)
            ram = max(1, max_ram * 0.30)
        return cpu, ram

    def _effective_priority(p):
        # Batch aging: if waited long enough, treat as interactive for selection.
        if p.priority == Priority.BATCH_PIPELINE:
            t0 = s.enqueue_tick.get(p.pipeline_id, s.ticks)
            if (s.ticks - t0) >= s.batch_aging_ticks:
                return Priority.INTERACTIVE
        return p.priority

    def _has_waiting_high_prio():
        # Consider both real high-priority queues and aged batch treated as interactive.
        if s.q_query or s.q_interactive:
            return True
        # If lots of batch are aged, treat that as "interactive pressure" too.
        for p in list(s.q_batch)[:10]:
            if _effective_priority(p) != Priority.BATCH_PIPELINE:
                return True
        return False

    def _try_get_ready_op(p):
        st = p.runtime_status()
        # Only schedule ops whose parents are complete; one op per assignment for predictable latency.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not ops:
            return None
        return ops[0]

    def _pick_next_pipeline_and_op(allow_batch=True):
        """
        Select next (pipeline, op) by effective priority with round-robin fairness inside each queue.
        If allow_batch is False, skip batch pipelines unless aged to interactive.
        """
        # Priority order for selection: QUERY -> INTERACTIVE -> (aged batch as interactive) -> BATCH
        # We'll scan queues in this order and rotate items to preserve fairness.
        queues = [s.q_query, s.q_interactive, s.q_batch]

        for q in queues:
            n = len(q)
            for _ in range(n):
                p = q.popleft()

                # Drop completed pipelines and de-queue tracking
                if _pipeline_done_or_terminal(p):
                    s.enqueued_pipeline_ids.discard(p.pipeline_id)
                    s.enqueue_tick.pop(p.pipeline_id, None)
                    continue

                eff = _effective_priority(p)
                if (not allow_batch) and (eff == Priority.BATCH_PIPELINE):
                    # Keep in queue, but do not select it now.
                    q.append(p)
                    continue

                op = _try_get_ready_op(p)
                if op is None:
                    # Not ready yet; keep in same queue
                    q.append(p)
                    continue

                # Put pipeline back for future ops (we schedule only one op now)
                q.append(p)
                return p, op, eff

        return None, None, None

    # --- ingest new pipelines ---
    for p in pipelines:
        if p.pipeline_id in s.enqueued_pipeline_ids:
            continue
        # Enqueue only if not already terminal
        if _pipeline_done_or_terminal(p):
            continue
        _queue_for_priority(p.priority).append(p)
        s.enqueued_pipeline_ids.add(p.pipeline_id)
        s.enqueue_tick[p.pipeline_id] = s.ticks

    # --- process results: update sizing hints ---
    for r in results:
        # r.ops is a list; we size-hint each op individually
        for op in (r.ops or []):
            ok = (r.pipeline_id, _op_key(op)) if hasattr(r, "pipeline_id") else None

            # Fallback key when result doesn't expose pipeline_id (keep best-effort)
            if ok is None:
                ok = ("unknown", _op_key(op))

            if r.failed():
                # Assume failures are often due to RAM (OOM). Increase RAM hint aggressively.
                prev = s.op_ram_hint.get(ok, r.ram if getattr(r, "ram", None) is not None else 1)
                bumped = prev * 2
                s.op_ram_hint[ok] = bumped

                # Count retries; if excessive, stop retrying by letting the pipeline linger unscheduled
                # (simulator may mark it failed via its own logic if it cannot progress).
                s.op_retry_count[ok] = s.op_retry_count.get(ok, 0) + 1
            else:
                # On success, gently reduce RAM hint to avoid permanent over-allocation.
                if getattr(r, "ram", None) is not None:
                    s.op_ram_hint[ok] = max(1, r.ram * 0.85)
                if getattr(r, "cpu", None) is not None:
                    # Keep CPU hint close to what worked; modestly reduce for better sharing.
                    s.op_cpu_hint[ok] = max(1, r.cpu * 0.90)

    # early exit if no changes (avoid unnecessary scans)
    if not pipelines and not results:
        s.ticks += 1
        return [], []

    suspensions = []  # no preemption in this incremental version
    assignments = []

    # --- scheduling / placement ---
    high_prio_waiting = _has_waiting_high_prio()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft reservation: protect high-priority latency by preventing batch from consuming the last headroom.
        reserve_cpu = (pool.max_cpu_pool * s.reserve_frac_cpu) if high_prio_waiting else 0
        reserve_ram = (pool.max_ram_pool * s.reserve_frac_ram) if high_prio_waiting else 0

        # Keep packing until we can't fit anything else
        made_progress = True
        while made_progress:
            made_progress = False

            # If high-priority work exists, only allow batch when we have headroom above reservation.
            allow_batch = True
            if high_prio_waiting:
                # If we're below reservation, block batch admission.
                if (avail_cpu - reserve_cpu) <= 0 or (avail_ram - reserve_ram) <= 0:
                    allow_batch = False

            p, op, eff_prio = _pick_next_pipeline_and_op(allow_batch=allow_batch)
            if p is None:
                break

            # Retry guard: if an op has failed too many times, skip it for now (avoid thrash)
            opk = (p.pipeline_id, _op_key(op))
            if s.op_retry_count.get(opk, 0) > s.max_retries_per_op:
                # Leave pipeline in queue; but avoid repeatedly selecting the same stuck op this tick
                # by temporarily marking it as "do not schedule" via a one-tick skip set.
                # (We implement minimal behavior by simply breaking out for this pool.)
                break

            # Determine target resources for this op
            def_cpu, def_ram = _default_targets(eff_prio, pool)
            tgt_cpu = s.op_cpu_hint.get(opk, def_cpu)
            tgt_ram = s.op_ram_hint.get(opk, def_ram)

            # Clamp to pool maxima and current availability
            req_cpu = min(max(1, tgt_cpu), pool.max_cpu_pool, avail_cpu)
            req_ram = min(max(1, tgt_ram), pool.max_ram_pool, avail_ram)

            # If high-priority waiting and we're scheduling BATCH, enforce reservation more strictly:
            if high_prio_waiting and (eff_prio == Priority.BATCH_PIPELINE):
                if (avail_cpu - req_cpu) < reserve_cpu or (avail_ram - req_ram) < reserve_ram:
                    # Not enough headroom left above reservation for this batch op.
                    # Try to find a higher-priority op instead.
                    p2, op2, eff2 = _pick_next_pipeline_and_op(allow_batch=False)
                    if p2 is None:
                        break
                    p, op, eff_prio = p2, op2, eff2
                    opk = (p.pipeline_id, _op_key(op))
                    def_cpu, def_ram = _default_targets(eff_prio, pool)
                    tgt_cpu = s.op_cpu_hint.get(opk, def_cpu)
                    tgt_ram = s.op_ram_hint.get(opk, def_ram)
                    req_cpu = min(max(1, tgt_cpu), pool.max_cpu_pool, avail_cpu)
                    req_ram = min(max(1, tgt_ram), pool.max_ram_pool, avail_ram)

            # Final fit check
            if req_cpu <= 0 or req_ram <= 0 or req_cpu > avail_cpu or req_ram > avail_ram:
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,  # keep original pipeline priority for accounting
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            avail_cpu -= req_cpu
            avail_ram -= req_ram
            made_progress = True

            # Recompute whether high-priority is waiting; may change as queues drain
            high_prio_waiting = _has_waiting_high_prio()

            if avail_cpu <= 0 or avail_ram <= 0:
                break

    s.ticks += 1
    return suspensions, assignments
