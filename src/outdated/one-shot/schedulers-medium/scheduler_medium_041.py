# policy_key: scheduler_medium_041
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.073429
# generation_seconds: 80.38
# generated_at: 2026-03-14T03:14:06.889405
@register_scheduler_init(key="scheduler_medium_041")
def scheduler_medium_041_init(s):
    """Priority-aware, reservation-based scheduler (incremental improvement over naive FIFO).

    Key ideas (kept intentionally simple and robust):
    - Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
    - Reserve a fraction of each pool's CPU/RAM so batch work can't consume all headroom.
    - Round-robin within each priority class to avoid starvation among same-priority pipelines.
    - Basic OOM adaptation: if an op fails with an OOM-like error, retry with more RAM (bounded),
      and cap retries to avoid infinite loops on non-memory failures.

    Notes:
    - No preemption (insufficient runtime visibility in the provided interfaces).
    - One operator per container assignment (matches the starter template behavior).
    """
    s.pipeline_by_id = {}          # pipeline_id -> Pipeline
    s.queued_ids = set()           # pipeline_ids currently present in some queue
    s.q_query = []                 # list[pipeline_id]
    s.q_interactive = []           # list[pipeline_id]
    s.q_batch = []                 # list[pipeline_id]

    # Per-operator hints from past attempts (keyed by operator object identity / attributes).
    s.op_ram_hint = {}             # op_key -> ram
    s.op_cpu_hint = {}             # op_key -> cpu
    s.op_retries = {}              # op_key -> int
    s.op_nonretryable = set()      # op_key

    # Pool headroom protection: keep some capacity available for higher priority arrivals.
    s.reserve_cpu_frac_for_high = 0.25
    s.reserve_ram_frac_for_high = 0.25

    # Retry policy
    s.max_oom_retries = 4
    s.max_other_retries = 1


def _priority_queue_for(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _op_key(op):
    # Best-effort stable key: prefer explicit ids/names if present; fallback to object id.
    oid = getattr(op, "op_id", None)
    if oid is None:
        oid = getattr(op, "operator_id", None)
    name = getattr(op, "name", None)
    if oid is not None:
        return ("op", oid, name)
    return ("obj", id(op), name)


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    # Keep heuristics broad; simulator/runtime error strings may vary.
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)


def _prune_completed_and_dedup(s):
    # Remove completed pipelines from global map and queues; also fix queued_ids.
    alive = set()
    for pid, p in list(s.pipeline_by_id.items()):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            del s.pipeline_by_id[pid]
            s.queued_ids.discard(pid)
        else:
            alive.add(pid)

    def prune_queue(q):
        out = []
        seen = set()
        for pid in q:
            if pid in alive and pid not in seen:
                out.append(pid)
                seen.add(pid)
        return out

    s.q_query = prune_queue(s.q_query)
    s.q_interactive = prune_queue(s.q_interactive)
    s.q_batch = prune_queue(s.q_batch)

    # Rebuild queued_ids from queues (authoritative).
    s.queued_ids = set(s.q_query) | set(s.q_interactive) | set(s.q_batch)


def _select_pool_for(s, pools_avail, priority):
    # Pick the pool with the most headroom (simple interference avoidance).
    # pools_avail: list of dicts {pool_id, avail_cpu, avail_ram, max_cpu, max_ram}
    best = None
    best_score = None
    for pr in pools_avail:
        if pr["avail_cpu"] <= 0 or pr["avail_ram"] <= 0:
            continue

        # For batch, only count capacity above reserved headroom.
        if priority == Priority.BATCH_PIPELINE:
            reserve_cpu = s.reserve_cpu_frac_for_high * pr["max_cpu"]
            reserve_ram = s.reserve_ram_frac_for_high * pr["max_ram"]
            eff_cpu = max(0.0, pr["avail_cpu"] - reserve_cpu)
            eff_ram = max(0.0, pr["avail_ram"] - reserve_ram)
            if eff_cpu <= 0 or eff_ram <= 0:
                continue
            score = eff_cpu + eff_ram
        else:
            score = pr["avail_cpu"] + pr["avail_ram"]

        if best is None or score > best_score:
            best = pr
            best_score = score
    return best


def _size_for(s, pool_rec, priority, op):
    # Start with per-priority defaults; override with learned hints when available.
    max_cpu = pool_rec["max_cpu"]
    max_ram = pool_rec["max_ram"]
    avail_cpu = pool_rec["avail_cpu"]
    avail_ram = pool_rec["avail_ram"]

    # Default targets (small, latency-friendly for high priority; throughput for batch).
    if priority == Priority.QUERY:
        cpu_target = 0.50 * max_cpu
        ram_target = 0.15 * max_ram
    elif priority == Priority.INTERACTIVE:
        cpu_target = 0.40 * max_cpu
        ram_target = 0.20 * max_ram
    else:
        cpu_target = 0.30 * max_cpu
        ram_target = 0.25 * max_ram

    ok = _op_key(op)
    if ok in s.op_cpu_hint:
        cpu_target = max(cpu_target, s.op_cpu_hint[ok])
    if ok in s.op_ram_hint:
        ram_target = max(ram_target, s.op_ram_hint[ok])

    # Bound by what we can currently allocate in the pool.
    cpu = min(avail_cpu, max(1.0, cpu_target))
    ram = min(avail_ram, max(1.0, ram_target))

    # For batch, avoid consuming the entire pool even if available (keep some slack).
    if priority == Priority.BATCH_PIPELINE:
        cpu = min(cpu, max(1.0, 0.75 * avail_cpu))
        ram = min(ram, max(1.0, 0.75 * avail_ram))

    return cpu, ram


def _pipeline_has_nonretryable_failed_op(s, pipeline):
    st = pipeline.runtime_status()
    # If there are FAILED ops, we only want to keep retrying those that look like OOM retries.
    failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False)
    for op in failed_ops:
        if _op_key(op) in s.op_nonretryable:
            return True
    return False


@register_scheduler(key="scheduler_medium_041")
def scheduler_medium_041(s, results, pipelines):
    """
    Scheduler step:
    - Ingest new pipelines into per-priority queues.
    - Process results to learn from OOM failures (increase RAM hint) and stop retrying non-oom failures.
    - Assign ready operators, prioritizing QUERY then INTERACTIVE then BATCH, with pool headroom reservations.
    """
    # Ingest new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        s.pipeline_by_id[pid] = p
        if pid not in s.queued_ids:
            _priority_queue_for(s, p.priority).append(pid)
            s.queued_ids.add(pid)

    # Process results (OOM learning / retry limiting)
    for r in results:
        if not r.failed():
            continue
        err = getattr(r, "error", None)
        is_oom = _is_oom_error(err)

        # Update hints/retry counts for involved ops
        for op in getattr(r, "ops", []) or []:
            ok = _op_key(op)
            s.op_retries[ok] = s.op_retries.get(ok, 0) + 1

            if is_oom:
                # Increase RAM; keep CPU as-is for now (small improvement, avoid overfitting).
                prev = s.op_ram_hint.get(ok, None)
                # Multiply by 2 based on allocated RAM in the failed attempt; ensure monotonic.
                proposed = max(float(getattr(r, "ram", 0) or 0) * 2.0, (prev or 0) * 1.5, (prev or 0) + 1.0)
                # Cap by the pool's max RAM if we can find it
                pool = s.executor.pools[r.pool_id]
                cap = float(getattr(pool, "max_ram_pool", proposed) or proposed)
                s.op_ram_hint[ok] = min(cap, proposed)

                if s.op_retries[ok] > s.max_oom_retries:
                    s.op_nonretryable.add(ok)
            else:
                # Treat as non-retryable quickly (likely user/code error).
                if s.op_retries[ok] > s.max_other_retries:
                    s.op_nonretryable.add(ok)

    # Quick prune of completed pipelines and queue cleanup
    _prune_completed_and_dedup(s)

    # If nothing changed, avoid doing work
    if not pipelines and not results:
        return [], []

    suspensions = []  # no preemption in this version
    assignments = []

    # Snapshot pool availability we can mutate locally while planning assignments
    pools_avail = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pools_avail.append(
            {
                "pool_id": pool_id,
                "avail_cpu": float(pool.avail_cpu_pool),
                "avail_ram": float(pool.avail_ram_pool),
                "max_cpu": float(pool.max_cpu_pool),
                "max_ram": float(pool.max_ram_pool),
            }
        )

    # Helper: attempt to schedule a single op from a given queue (round-robin).
    def try_schedule_from_queue(queue, priority):
        if not queue:
            return False

        qlen = len(queue)
        for _ in range(qlen):
            pid = queue.pop(0)
            p = s.pipeline_by_id.get(pid)
            if p is None:
                continue  # stale

            st = p.runtime_status()
            if st.is_pipeline_successful():
                # Should have been pruned, but safe to skip
                s.pipeline_by_id.pop(pid, None)
                s.queued_ids.discard(pid)
                continue

            # If we decided some failed op is non-retryable, drop the pipeline to avoid infinite loops.
            if _pipeline_has_nonretryable_failed_op(s, p):
                s.pipeline_by_id.pop(pid, None)
                s.queued_ids.discard(pid)
                continue

            # Choose next runnable operator (parents complete)
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready; keep pipeline in queue for later
                queue.append(pid)
                return False

            op = op_list[0]

            # Find a pool to place this op
            pool_rec = _select_pool_for(s, pools_avail, priority)
            if pool_rec is None:
                # No feasible pool right now; keep pipeline queued
                queue.append(pid)
                return False

            cpu, ram = _size_for(s, pool_rec, priority, op)

            # For batch, enforce headroom reservation explicitly.
            if priority == Priority.BATCH_PIPELINE:
                reserve_cpu = s.reserve_cpu_frac_for_high * pool_rec["max_cpu"]
                reserve_ram = s.reserve_ram_frac_for_high * pool_rec["max_ram"]
                if pool_rec["avail_cpu"] - cpu < reserve_cpu or pool_rec["avail_ram"] - ram < reserve_ram:
                    # Can't fit while respecting reservation; keep queued.
                    queue.append(pid)
                    return False

            if cpu <= 0 or ram <= 0:
                queue.append(pid)
                return False

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=priority,
                    pool_id=pool_rec["pool_id"],
                    pipeline_id=pid,
                )
            )

            # Consume planned capacity
            pool_rec["avail_cpu"] -= cpu
            pool_rec["avail_ram"] -= ram

            # Round-robin: put pipeline back to tail (it may have more runnable ops later)
            queue.append(pid)
            return True

        return False

    # Main planning loop: keep scheduling while we can place more work.
    # Priority order: QUERY > INTERACTIVE > BATCH
    while True:
        made = False

        made = try_schedule_from_queue(s.q_query, Priority.QUERY) or made
        made = try_schedule_from_queue(s.q_interactive, Priority.INTERACTIVE) or made
        made = try_schedule_from_queue(s.q_batch, Priority.BATCH_PIPELINE) or made

        if not made:
            break

    return suspensions, assignments
