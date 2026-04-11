# policy_key: scheduler_low_021
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.056714
# generation_seconds: 56.94
# generated_at: 2026-04-09T21:19:17.339912
@register_scheduler_init(key="scheduler_low_021")
def scheduler_low_021_init(s):
    """
    Priority-aware, failure-averse scheduler.

    Core ideas:
      - Strict priority order for ready work: QUERY > INTERACTIVE > BATCH.
      - Conservative packing for BATCH: keep headroom (CPU/RAM) to reduce latency spikes for high priority arrivals.
      - OOM-adaptive retries: track per-operator RAM estimate; on OOM-like failures, increase RAM and retry.
      - Avoid infinite failure loops: if an op fails even at pool max RAM/CPU several times, stop retrying it.

    Notes:
      - This policy intentionally avoids aggressive preemption (not enough runtime surface area is guaranteed).
      - It schedules multiple operators per tick per pool (when resources allow) for higher utilization.
    """
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.pipeline_meta = {}  # pipeline_id -> { "priority": ..., "arrival_tick": int, "seen": bool }
    s.op_ram_est = {}     # op_key -> estimated RAM to allocate
    s.op_cpu_est = {}     # op_key -> estimated CPU to allocate
    s.op_fail_counts = {} # op_key -> int
    s.tick = 0

    # Headroom fractions to keep free when lower priority work is being scheduled
    s.headroom = {
        Priority.QUERY: (0.00, 0.00),         # (cpu_frac, ram_frac) keep-free when scheduling this priority
        Priority.INTERACTIVE: (0.05, 0.05),
        Priority.BATCH_PIPELINE: (0.20, 0.20),
    }

    # Default CPU sizing targets per priority (as a fraction of pool max), later clamped by availability
    s.cpu_target_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # RAM safety multiplier when we don't know the true minimum (and to reduce OOM churn)
    s.unknown_ram_floor_frac = 0.05  # if no op RAM hint exists, start with 5% of pool RAM (clamped)

    # Retry controls
    s.max_failures_before_giveup = 6  # per-operator (op_key) terminal threshold


@register_scheduler(key="scheduler_low_021")
def scheduler_low_021_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into priority queues.
      2) Process execution results to update RAM/CPU estimates and failure counters.
      3) For each pool, schedule ready operators:
           - Always try higher priorities first.
           - Keep headroom when admitting lower priorities to protect p95/p99 for high priorities.
           - Allocate CPU/RAM based on per-op estimates (adaptive on failures).
    """
    s.tick += 1

    def _is_oom_error(err):
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)

    def _op_key(op):
        # Prefer stable identifiers if present; otherwise fall back to repr.
        for attr in ("operator_id", "op_id", "id", "name"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if v is not None:
                    return f"{attr}:{v}"
        return f"repr:{repr(op)}"

    def _get_op_min_ram_hint(op):
        # Best-effort: different generator variants may use different names.
        for attr in ("min_ram", "ram_min", "ram_req", "ram_requirement", "required_ram", "mem_min", "min_memory"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                try:
                    if v is not None and float(v) > 0:
                        return float(v)
                except Exception:
                    pass
        return None

    def _choose_pools_by_headroom():
        # Prefer pools with more available RAM first (reduces OOM risk), then CPU.
        pools = []
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            pools.append((pool.avail_ram_pool, pool.avail_cpu_pool, pool_id))
        pools.sort(reverse=True)
        return [pid for _, _, pid in pools]

    def _enqueue_pipeline(p):
        if p.pipeline_id not in s.pipeline_meta:
            s.pipeline_meta[p.pipeline_id] = {
                "priority": p.priority,
                "arrival_tick": s.tick,
            }
        s.waiting[p.priority].append(p)

    # 1) Ingest new arrivals
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit: nothing changed
    if not pipelines and not results:
        return [], []

    # 2) Process results to update estimates (mainly OOM backoff)
    for r in results:
        if not getattr(r, "ops", None):
            continue
        # Use the first op as the "key op" for sizing feedback; assignment can include multiple ops,
        # but Eudoxia commonly returns a single op per assignment.
        op0 = r.ops[0]
        k = _op_key(op0)

        if r.failed():
            s.op_fail_counts[k] = s.op_fail_counts.get(k, 0) + 1

            # If failure looks like OOM, increase RAM estimate; otherwise slightly increase CPU too.
            if _is_oom_error(getattr(r, "error", None)):
                prev = s.op_ram_est.get(k, None)
                base = prev if prev is not None else (getattr(r, "ram", None) or 0)
                try:
                    base = float(base)
                except Exception:
                    base = 0.0
                new_ram = max(base * 1.5, base + 1.0)  # additive+mult bump to escape small steps
                s.op_ram_est[k] = new_ram
            else:
                prev_cpu = s.op_cpu_est.get(k, None)
                base_cpu = prev_cpu if prev_cpu is not None else (getattr(r, "cpu", None) or 0)
                try:
                    base_cpu = float(base_cpu)
                except Exception:
                    base_cpu = 0.0
                s.op_cpu_est[k] = max(base_cpu * 1.25, base_cpu + 0.5)

        else:
            # On success, keep existing estimates; optionally decrease CPU slightly to improve packing.
            # (We stay conservative to avoid regressing latency.)
            kcpu = s.op_cpu_est.get(k, None)
            if kcpu is not None:
                s.op_cpu_est[k] = max(0.5, float(kcpu) * 0.98)

    suspensions = []
    assignments = []

    # Helper to remove completed/terminal pipelines from queues
    def _filter_and_collect(q):
        kept = []
        for p in q:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            # If the pipeline contains failed ops, we still keep it (we can retry FAILED via ASSIGNABLE_STATES).
            kept.append(p)
        return kept

    # Clean queues (avoid repeatedly scanning completed pipelines)
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        s.waiting[pr] = _filter_and_collect(s.waiting[pr])

    # 3) For each pool, schedule work (higher priority first)
    pool_order = _choose_pools_by_headroom()

    # To reduce starvation for BATCH without hurting high priority too much, we do limited "aging":
    # if batch has waited long and no high-priority is currently assignable, allow it to use more headroom.
    def _has_assignable(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return bool(ops)

    def _any_assignable_in_queue(prio):
        for p in s.waiting[prio]:
            if _has_assignable(p):
                return True
        return False

    hi_assignable = _any_assignable_in_queue(Priority.QUERY) or _any_assignable_in_queue(Priority.INTERACTIVE)

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]

        # We'll schedule multiple ops per pool per tick until we hit headroom constraints.
        # Track available resources locally as we add assignments.
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # When high priority exists, reserve more headroom (i.e., be stricter with batch admission).
        # If no high-priority assignable work exists, relax batch headroom for throughput/completion.
        dynamic_headroom_batch = (0.20, 0.20) if hi_assignable else (0.05, 0.05)

        # Iterate priority levels in order
        for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            # Determine headroom for this prio
            if prio == Priority.BATCH_PIPELINE:
                cpu_keep_frac, ram_keep_frac = dynamic_headroom_batch
            else:
                cpu_keep_frac, ram_keep_frac = s.headroom.get(prio, (0.10, 0.10))

            cpu_keep = pool.max_cpu_pool * cpu_keep_frac
            ram_keep = pool.max_ram_pool * ram_keep_frac

            # Iterate pipelines in FIFO order within each priority.
            # We keep a local requeue list to preserve order while allowing multi-assignments.
            requeue = []
            while s.waiting[prio]:
                p = s.waiting[prio].pop(0)
                st = p.runtime_status()

                if st.is_pipeline_successful():
                    continue

                # Get one ready operator at a time (reduces over-commit and keeps latency predictable)
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    requeue.append(p)
                    continue

                op = op_list[0]
                k = _op_key(op)

                # Terminal give-up to avoid infinite churn on hopeless ops.
                # This will cause the pipeline to remain failed (penalized), but prevents burning the whole cluster.
                if s.op_fail_counts.get(k, 0) >= s.max_failures_before_giveup:
                    # Don't keep retrying this pipeline if its next op is terminally failing.
                    # Leave it out of the queue.
                    continue

                # Size RAM:
                #   - Start from known estimate if available.
                #   - Else use operator minimum hint if present.
                #   - Else allocate a small-but-not-tiny floor of pool RAM.
                min_hint = _get_op_min_ram_hint(op)
                if k in s.op_ram_est:
                    req_ram = float(s.op_ram_est[k])
                elif min_hint is not None:
                    # Add a small safety margin to reduce OOM retries (cheaper than 720s penalties).
                    req_ram = float(min_hint) * 1.10
                else:
                    req_ram = max(1.0, pool.max_ram_pool * s.unknown_ram_floor_frac)

                # Clamp RAM to pool max
                req_ram = min(req_ram, float(pool.max_ram_pool))

                # Size CPU:
                if k in s.op_cpu_est:
                    req_cpu = float(s.op_cpu_est[k])
                else:
                    req_cpu = float(pool.max_cpu_pool) * float(s.cpu_target_frac.get(prio, 0.30))

                # Clamp CPU to pool max
                req_cpu = min(req_cpu, float(pool.max_cpu_pool))
                # Avoid zero-sized allocations
                req_cpu = max(0.5, req_cpu)
                req_ram = max(0.5, req_ram)

                # Enforce headroom: do not consume below the keep-free thresholds for this prio
                if (avail_cpu - req_cpu) < cpu_keep or (avail_ram - req_ram) < ram_keep:
                    # If we can't fit at this size, try a smaller CPU first (RAM is the OOM-critical dimension)
                    # This sacrifices some speed but reduces tail and increases completion probability.
                    shrink_cpu = max(0.5, min(req_cpu, avail_cpu - cpu_keep))
                    if shrink_cpu < 0.5:
                        # Can't fit CPU-wise; stop scheduling this priority in this pool
                        requeue.append(p)
                        break

                    # If RAM doesn't fit, we can't schedule here; keep it queued for another pool/tick.
                    if (avail_ram - req_ram) < ram_keep:
                        requeue.append(p)
                        break

                    req_cpu = shrink_cpu

                # If still can't fit due to hard availability constraints, stop for this pool/priority
                if req_cpu > avail_cpu or req_ram > avail_ram:
                    requeue.append(p)
                    break

                # Create assignment
                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=req_cpu,
                        ram=req_ram,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # Update local availability
                avail_cpu -= req_cpu
                avail_ram -= req_ram

                # Put pipeline back for future ops (preserve progress; avoid starvation within same priority)
                requeue.append(p)

                # If we're nearly out, stop
                if avail_cpu <= 0.5 or avail_ram <= 0.5:
                    break

            # Restore queue for this priority
            if requeue:
                s.waiting[prio] = requeue + s.waiting[prio]

            if avail_cpu <= 0.5 or avail_ram <= 0.5:
                break

    return suspensions, assignments
