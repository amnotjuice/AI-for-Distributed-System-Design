# policy_key: scheduler_est_010
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.056438
# generation_seconds: 47.35
# generated_at: 2026-04-01T13:42:55.232551
@register_scheduler_init(key="scheduler_est_010")
def scheduler_est_010_init(s):
    """Priority-aware FIFO with cautious sizing and simple OOM backoff.

    Incremental improvements over naive FIFO:
      1) Separate waiting queues per priority and always prefer higher priority.
      2) Avoid allocating an entire pool to batch work by default (improves latency for arrivals).
      3) Use op memory estimates (if provided) with a safety margin.
      4) On failure (e.g., OOM), back off by increasing the next RAM request for that operator.

    Notes:
      - Keeps logic simple: no explicit preemption (not enough stable introspection hooks here).
      - Tries to pack multiple ops per tick by tracking local (optimistic) pool headroom.
    """
    s.waiting_by_pri = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Backoff multiplier per operator object (keyed by id(op)).
    s.op_ram_backoff = {}
    # Remember last attempted RAM per operator (for additional conservatism on repeated failures).
    s.op_last_ram = {}
    # Round-robin cursor per priority to reduce head-of-line blocking among pipelines of same priority.
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }


def _get_mem_estimate_gb(op):
    """Best-effort extraction of memory estimate from an operator."""
    est = None
    # Preferred path per spec: op.estimate.mem_peak_gb
    if hasattr(op, "estimate"):
        e = getattr(op, "estimate", None)
        if e is not None and hasattr(e, "mem_peak_gb"):
            est = getattr(e, "mem_peak_gb", None)
    # Fallbacks (defensive)
    if est is None and hasattr(op, "estimate_mem_peak_gb"):
        est = getattr(op, "estimate_mem_peak_gb", None)
    if est is None and hasattr(op, "mem_peak_gb"):
        est = getattr(op, "mem_peak_gb", None)
    try:
        if est is not None:
            est = float(est)
            if est <= 0:
                est = None
    except Exception:
        est = None
    return est


def _prio_order():
    # Higher priority first.
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_high_priority(pri):
    return pri in (Priority.QUERY, Priority.INTERACTIVE)


def _append_pipeline(s, p):
    # Unknown priorities get treated as batch-like.
    pri = getattr(p, "priority", Priority.BATCH_PIPELINE)
    if pri not in s.waiting_by_pri:
        pri = Priority.BATCH_PIPELINE
    s.waiting_by_pri[pri].append(p)


def _pipeline_is_done_or_failed(p):
    st = p.runtime_status()
    # Treat any failed op as terminal for now (simple incremental policy).
    has_failures = st.state_counts[OperatorState.FAILED] > 0
    if st.is_pipeline_successful() or has_failures:
        return True
    return False


def _next_ready_op_from_queue(s, pri):
    """Round-robin selection of the next pipeline with a ready op for this priority."""
    q = s.waiting_by_pri[pri]
    if not q:
        return None, None

    n = len(q)
    start = s.rr_cursor.get(pri, 0) % max(n, 1)

    # Scan at most n pipelines to find a ready op; keep non-ready pipelines in queue.
    for offset in range(n):
        idx = (start + offset) % n
        p = q[idx]
        if _pipeline_is_done_or_failed(p):
            continue
        st = p.runtime_status()
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if op_list:
            s.rr_cursor[pri] = (idx + 1) % n
            return p, op_list[0]

    # If none ready, advance cursor a bit to avoid sticking.
    s.rr_cursor[pri] = (start + 1) % n
    return None, None


def _compact_queue_in_place(s, pri):
    """Remove completed/failed pipelines from the queue."""
    q = s.waiting_by_pri[pri]
    if not q:
        return
    kept = []
    for p in q:
        if not _pipeline_is_done_or_failed(p):
            kept.append(p)
    s.waiting_by_pri[pri] = kept
    if kept:
        s.rr_cursor[pri] = s.rr_cursor.get(pri, 0) % len(kept)
    else:
        s.rr_cursor[pri] = 0


@register_scheduler(key="scheduler_est_010")
def scheduler_est_010(s, results, pipelines):
    """
    Priority-aware scheduler with conservative sizing and OOM backoff.

    Scheduling loop (per tick):
      1) Ingest arrivals into per-priority queues.
      2) Process results: if an op failed, increase RAM backoff for that op.
      3) For each pool, allocate work while there is headroom:
         - Always try QUERY/INTERACTIVE first.
         - For BATCH, keep a small reservation if high-priority work is queued.
         - Size RAM using estimate (if any) with safety + backoff, capped by pool availability.
         - Size CPU: high-priority can take most available; batch is capped to leave room for concurrency.
    """
    # 1) Ingest new pipelines
    for p in pipelines:
        _append_pipeline(s, p)

    # 2) Process results: update per-op backoff on failures
    for r in results:
        if r is None:
            continue
        if hasattr(r, "failed") and r.failed():
            # Increase backoff; also remember last attempted RAM.
            # Key by operator object identity.
            try:
                if r.ops and len(r.ops) > 0:
                    op = r.ops[0]
                else:
                    op = None
            except Exception:
                op = None

            if op is not None:
                k = id(op)
                prev = s.op_ram_backoff.get(k, 1.0)
                # Multiplicative backoff; cap to avoid pathological requests.
                new = min(prev * 1.6, 16.0)
                s.op_ram_backoff[k] = new
                try:
                    s.op_last_ram[k] = float(getattr(r, "ram", s.op_last_ram.get(k, 0.0)))
                except Exception:
                    pass

    # If nothing changed, early exit
    if not pipelines and not results:
        return [], []

    # 3) Clean queues (drop completed/failed pipelines)
    for pri in list(s.waiting_by_pri.keys()):
        _compact_queue_in_place(s, pri)

    # Determine if there is any queued high-priority work (for batch reservation)
    has_hp = bool(s.waiting_by_pri.get(Priority.QUERY)) or bool(s.waiting_by_pri.get(Priority.INTERACTIVE))

    suspensions = []
    assignments = []

    # Helper to pick per-pool reservations for batch to preserve interactive latency.
    def batch_reservation(pool):
        # Reserve only if high-priority exists. Keep it modest: 15% of pool or at least 1 CPU / 1 GB.
        if not has_hp:
            return 0.0, 0.0
        cpu_res = max(1.0, 0.15 * float(pool.max_cpu_pool))
        ram_res = max(1.0, 0.15 * float(pool.max_ram_pool))
        return cpu_res, ram_res

    # Per pool, attempt to fill with work
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Local optimistic headroom tracking to enable multiple assignments per tick.
        try:
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
        except Exception:
            continue

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Allow multiple ops per pool per tick; stop when small fragments remain.
        # (Avoid creating tiny containers that are likely inefficient.)
        min_cpu_fragment = 0.9
        min_ram_fragment = 0.9

        while avail_cpu >= min_cpu_fragment and avail_ram >= min_ram_fragment:
            scheduled_any = False

            for pri in _prio_order():
                # For batch, apply a reservation if HP exists.
                if pri == Priority.BATCH_PIPELINE:
                    cpu_res, ram_res = batch_reservation(pool)
                    if avail_cpu - cpu_res < min_cpu_fragment or avail_ram - ram_res < min_ram_fragment:
                        continue

                p, op = _next_ready_op_from_queue(s, pri)
                if p is None or op is None:
                    continue

                # RAM sizing:
                # - start from estimate (if provided) else small default
                # - apply safety margin
                # - apply backoff (from observed failures)
                est = _get_mem_estimate_gb(op)
                base_ram = est if est is not None else 1.0
                base_ram = max(base_ram, 0.5)

                k = id(op)
                backoff = float(s.op_ram_backoff.get(k, 1.0))

                # Safety margin: estimates are noisy and should be treated as a lower bound.
                ram_req = base_ram * 1.25 * backoff

                # If we previously tried a RAM value and failed, don't go below that * 1.2
                last_ram = s.op_last_ram.get(k, None)
                if last_ram is not None:
                    try:
                        ram_req = max(ram_req, float(last_ram) * 1.2)
                    except Exception:
                        pass

                # Cap RAM to what's available (and to pool max as sanity).
                ram = min(avail_ram, float(pool.max_ram_pool), ram_req)
                if ram < min_ram_fragment:
                    continue

                # CPU sizing:
                # - For high priority, give as much as possible (up to pool max and available),
                #   but keep at least 1 vCPU if possible.
                # - For batch, cap at ~50% of pool to allow concurrency and reduce latency impact.
                if _is_high_priority(pri):
                    cpu_target = min(avail_cpu, float(pool.max_cpu_pool))
                else:
                    cpu_cap = max(1.0, 0.5 * float(pool.max_cpu_pool))
                    cpu_target = min(avail_cpu, cpu_cap)

                cpu = max(1.0, cpu_target) if avail_cpu >= 1.0 else avail_cpu
                if cpu < min_cpu_fragment:
                    continue

                # Create assignment: one operator per container (consistent with baseline).
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

                # Update local headroom and remember what we attempted for this op.
                avail_cpu -= cpu
                avail_ram -= ram
                s.op_last_ram[k] = ram

                scheduled_any = True
                break  # re-evaluate from highest priority after each placement

            if not scheduled_any:
                break

    return suspensions, assignments
