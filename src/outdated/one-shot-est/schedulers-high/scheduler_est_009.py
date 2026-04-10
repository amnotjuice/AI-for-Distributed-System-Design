# policy_key: scheduler_est_009
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.144013
# generation_seconds: 128.93
# generated_at: 2026-03-31T18:37:42.414268
@register_scheduler_init(key="scheduler_est_009")
def scheduler_est_009_init(s):
    """
    Priority-aware, estimate-aware scheduler (incremental improvement over naive FIFO).

    Main improvements:
      1) Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
      2) Size RAM per operator using op.estimate.mem_peak_gb with a small safety factor.
      3) On failures (esp. OOM), retry FAILED operators with increased RAM (bounded).
      4) Avoid head-of-line blocking: rotate through queues to find *ready* ops.
      5) Protect latency by reserving a small fraction of each pool for higher priorities,
         so background work doesn't fully consume the pool.
      6) Schedule multiple ops per pool per tick when resources allow (still priority-first).
    """
    import collections

    # Time / ordering
    s.tick = 0

    # Per-priority FIFO queues of Pipeline objects
    s.queues = {
        Priority.QUERY: collections.deque(),
        Priority.INTERACTIVE: collections.deque(),
        Priority.BATCH_PIPELINE: collections.deque(),
    }

    # Track pipelines we've already enqueued to avoid accidental duplicates
    s.known_pipeline_ids = set()
    s.pipeline_enqueue_tick = {}

    # Per-operator adaptive RAM multiplier based on failures/ooms
    # Keyed by id(op) because ExecutionResult doesn't expose pipeline_id reliably.
    s.op_mem_mult = {}
    s.op_retries = {}

    # Tuning knobs (kept simple / conservative)
    s.base_mem_safety = 1.15          # initial headroom on top of estimate
    s.oom_bump = 1.60                 # multiplier increase on OOM-ish failure
    s.fail_bump = 1.25                # multiplier increase on other failures
    s.max_mem_mult = 8.00             # cap to avoid unbounded growth
    s.min_ram_gb = 0.25               # don't schedule tiny RAM allocations

    # Simple retry policy for FAILED ops
    s.max_retries = 3

    # CPU targets by priority (cap at availability); simple latency bias for high-priority
    s.cpu_target = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 2.0,
    }

    # Reserve fractions (soft admission control) to keep headroom for higher priorities
    # - If QUERY work exists, keep this fraction of pool resources available for it.
    # - If INTERACTIVE work exists, keep this fraction available when scheduling BATCH.
    s.reserve_frac_query = 0.20
    s.reserve_frac_interactive = 0.10

    # Prevent scheduling multiple ops from the same pipeline within a single scheduler call
    # (important because pipeline state may not reflect assignments until after return)
    s._last_scheduled_tick = -1
    s._scheduled_pipelines_this_tick = set()


@register_scheduler(key="scheduler_est_009")
def scheduler_est_009(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]):
    """
    See init docstring for the high-level policy overview.
    """
    s.tick += 1
    if s._last_scheduled_tick != s.tick:
        s._last_scheduled_tick = s.tick
        s._scheduled_pipelines_this_tick = set()

    # -----------------------------
    # 1) Ingest new pipelines
    # -----------------------------
    for p in pipelines:
        if p.pipeline_id in s.known_pipeline_ids:
            continue
        s.known_pipeline_ids.add(p.pipeline_id)
        s.pipeline_enqueue_tick[p.pipeline_id] = s.tick
        s.queues[p.priority].append(p)

    # -----------------------------
    # 2) Learn from results (OOM -> increase RAM next time; success -> slight decay)
    # -----------------------------
    for r in results:
        if not hasattr(r, "ops") or r.ops is None:
            continue

        if r.failed():
            err = ""
            try:
                err = str(r.error).lower() if r.error is not None else ""
            except Exception:
                err = ""

            is_oom = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "exceed" in err)
            bump = s.oom_bump if is_oom else s.fail_bump

            for op in r.ops:
                k = id(op)
                s.op_retries[k] = s.op_retries.get(k, 0) + 1
                prev = s.op_mem_mult.get(k, 1.0)
                s.op_mem_mult[k] = min(s.max_mem_mult, prev * bump)
        else:
            # Gentle decay toward 1.0 to avoid over-allocation after transient spikes
            for op in r.ops:
                k = id(op)
                if k in s.op_mem_mult:
                    s.op_mem_mult[k] = max(1.0, s.op_mem_mult[k] * 0.98)

    # Early exit: if nothing new completed/failed and no new arrivals, nothing to do.
    # (Resource availability and readiness won't change without results/arrivals.)
    if not pipelines and not results:
        return [], []

    # -----------------------------
    # Helpers
    # -----------------------------
    def _get_mem_est_gb(op, pool):
        # Use operator-provided estimate when available; otherwise fall back to a small fraction of pool RAM.
        est = None
        try:
            est = op.estimate.mem_peak_gb
        except Exception:
            est = None
        if est is None or est <= 0:
            # Conservative fallback guess: 25% of pool RAM, but not less than min.
            est = max(s.min_ram_gb, 0.25 * float(pool.max_ram_pool))
        return float(est)

    def _ram_request_gb(op, pool):
        est = _get_mem_est_gb(op, pool)
        mult = s.op_mem_mult.get(id(op), 1.0)
        # Safety * learned multiplier; bounded by pool maximum since we can't request more than the pool has.
        req = max(s.min_ram_gb, est * s.base_mem_safety * mult)
        return float(req)

    def _cpu_request(op, pool, prio, avail_cpu):
        # Fixed target per priority (simple latency bias).
        target = float(s.cpu_target.get(prio, 2.0))
        cpu = min(float(avail_cpu), target)
        return float(cpu)

    def _op_retry_exceeded(op):
        return s.op_retries.get(id(op), 0) >= s.max_retries

    def _get_ready_ops(pipeline_status):
        ops = pipeline_status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        # Filter out ops that we consider "hard-failed" due to too many retries.
        return [op for op in ops if not _op_retry_exceeded(op)]

    def _queue_has_any_ready(prio, scan_limit=12):
        # Only scan a prefix for efficiency; this is used only to decide whether to enforce reservations.
        q = s.queues[prio]
        n = min(len(q), scan_limit)
        for i in range(n):
            p = q[i]
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            if _get_ready_ops(st):
                return True
        return False

    # High-priority presence signals (used for reservations)
    query_waiting = _queue_has_any_ready(Priority.QUERY)
    interactive_waiting = _queue_has_any_ready(Priority.INTERACTIVE)

    # -----------------------------
    # 3) Scheduling decisions
    # -----------------------------
    suspensions = []
    assignments = []

    # Try to keep strict priority ordering, but still fill the pool with more than one task if possible.
    prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Compute soft reservations (only applied when higher-priority work exists)
        reserve_cpu_for_query = float(pool.max_cpu_pool) * float(s.reserve_frac_query) if query_waiting else 0.0
        reserve_ram_for_query = float(pool.max_ram_pool) * float(s.reserve_frac_query) if query_waiting else 0.0

        reserve_cpu_for_interactive = (
            float(pool.max_cpu_pool) * float(s.reserve_frac_interactive) if interactive_waiting else 0.0
        )
        reserve_ram_for_interactive = (
            float(pool.max_ram_pool) * float(s.reserve_frac_interactive) if interactive_waiting else 0.0
        )

        # Cap number of "rotation attempts" so we don't spin forever when nothing is runnable.
        # We allow multiple passes because we may skip pipelines that are blocked on deps or too large.
        max_attempts = sum(len(s.queues[p]) for p in prio_order) + 8
        attempts = 0

        while attempts < max_attempts and avail_cpu > 0 and avail_ram > 0:
            attempts += 1
            made_assignment = False

            for prio in prio_order:
                q = s.queues[prio]
                if not q:
                    continue

                # Determine effective headroom for this priority (reservations protect higher priorities)
                # - QUERY: no reservations.
                # - INTERACTIVE: reserve some for QUERY if query exists.
                # - BATCH: reserve for both QUERY and INTERACTIVE if they exist.
                if prio == Priority.QUERY:
                    eff_avail_cpu = avail_cpu
                    eff_avail_ram = avail_ram
                elif prio == Priority.INTERACTIVE:
                    eff_avail_cpu = avail_cpu - reserve_cpu_for_query
                    eff_avail_ram = avail_ram - reserve_ram_for_query
                else:
                    eff_avail_cpu = avail_cpu - reserve_cpu_for_query - reserve_cpu_for_interactive
                    eff_avail_ram = avail_ram - reserve_ram_for_query - reserve_ram_for_interactive

                if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                    continue

                # Rotate within this priority queue to find a runnable pipeline/op.
                # We only rotate at most len(q) to preserve FIFO-ish behavior.
                rotated = 0
                q_len = len(q)

                while rotated < q_len:
                    rotated += 1
                    pipeline = q.popleft()

                    status = pipeline.runtime_status()

                    # Drop completed pipelines from queues
                    if status.is_pipeline_successful():
                        # Let it be forgotten naturally; avoid re-adding.
                        s.known_pipeline_ids.discard(pipeline.pipeline_id)
                        s.pipeline_enqueue_tick.pop(pipeline.pipeline_id, None)
                        continue

                    # Avoid scheduling multiple ops from same pipeline in one scheduler invocation
                    if pipeline.pipeline_id in s._scheduled_pipelines_this_tick:
                        q.append(pipeline)
                        continue

                    ready_ops = _get_ready_ops(status)
                    if not ready_ops:
                        # Not runnable now (deps not done, or only hard-failed ops remain)
                        q.append(pipeline)
                        continue

                    # For batch, prefer smaller-memory ops first to pack more effectively.
                    if prio == Priority.BATCH_PIPELINE:
                        def _key_mem(op):
                            try:
                                return float(op.estimate.mem_peak_gb)
                            except Exception:
                                return 1e18
                        ready_ops = sorted(ready_ops, key=_key_mem)

                    # Find the first op that fits current effective resources.
                    chosen_op = None
                    chosen_ram = None
                    chosen_cpu = None

                    for op in ready_ops:
                        ram_req = _ram_request_gb(op, pool)
                        cpu_req = _cpu_request(op, pool, prio, eff_avail_cpu)

                        if cpu_req <= 0:
                            continue

                        # Fit check against effective availability (respecting reservations)
                        if ram_req <= eff_avail_ram and cpu_req <= eff_avail_cpu:
                            chosen_op = op
                            chosen_ram = ram_req
                            chosen_cpu = cpu_req
                            break

                    if chosen_op is None:
                        # Nothing from this pipeline fits right now on this pool; requeue and try others.
                        q.append(pipeline)
                        continue

                    # Commit assignment
                    assignments.append(
                        Assignment(
                            ops=[chosen_op],
                            cpu=float(chosen_cpu),
                            ram=float(chosen_ram),
                            priority=pipeline.priority,
                            pool_id=pool_id,
                            pipeline_id=pipeline.pipeline_id,
                        )
                    )

                    s._scheduled_pipelines_this_tick.add(pipeline.pipeline_id)

                    avail_cpu -= float(chosen_cpu)
                    avail_ram -= float(chosen_ram)

                    # Requeue pipeline: it likely has more work later (or parallel branches).
                    q.append(pipeline)

                    made_assignment = True
                    break  # stop rotating within this priority; reconsider priorities with updated resources

                if made_assignment:
                    break  # restart from top priority with updated avail

            if not made_assignment:
                # Nothing could be scheduled in this pool given current constraints.
                break

    return suspensions, assignments
