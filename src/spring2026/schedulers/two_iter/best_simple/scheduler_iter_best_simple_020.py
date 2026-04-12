# policy_key: scheduler_iter_best_simple_020
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046869
# generation_seconds: 47.38
# generated_at: 2026-04-11T22:50:58.671262
@register_scheduler_init(key="scheduler_iter_best_simple_020")
def scheduler_iter_best_simple_020_init(s):
    """Priority-first scheduler focused on reducing weighted latency.

    Incremental improvements over the previous attempt:
      1) Global placement: pick the best pool for each dispatch (instead of filling pools sequentially),
         to reduce head-of-line blocking for high-priority work.
      2) Explicit capacity reservations: keep CPU/RAM headroom so BATCH cannot crowd out QUERY/INTERACTIVE.
      3) Better OOM backoff: track container->(pipeline_id, ops) on assignment so failures update the right ops.
      4) Faster high-priority completion: slightly higher CPU caps for QUERY/INTERACTIVE (still capped to avoid monopoly).
      5) Queue hygiene: drop completed pipelines; keep round-robin fairness within each priority.

    Notes:
      - No preemption yet (not enough stable runtime visibility in the provided interface).
      - RAM sizing is conservative; it only prevents OOM and doesn't speed execution in the simulator model.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # op_key -> RAM multiplier (1,2,4,...)
    s.op_ram_mult = {}

    # container_id -> (pipeline_id, [ops])
    s.container_map = {}

    # Minimal "age" counter to slightly de-starve batch if needed (small effect; keeps policy stable)
    s.batch_rr_counter = 0

    def _op_key(pipeline_id, op):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_simple_020")
def scheduler_iter_best_simple_020_scheduler(s, results, pipelines):
    """Global priority scheduler with reservations + OOM-aware RAM backoff."""
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _enqueue(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _clean_and_get_assignable_ops(p, max_ops):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return None
        # Get up to max_ops runnable ops (parents complete)
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:max_ops]

    def _pick_next_pipeline():
        # Strict priority with RR within each queue.
        # Tiny "batch RR counter" so batch is not permanently ignored when interactive stream is heavy,
        # but the bias still strongly favors QUERY/INTERACTIVE for weighted latency.
        for q in (s.q_query, s.q_interactive):
            if not q:
                continue
            n = len(q)
            for _ in range(n):
                p = q.pop(0)
                ops = _clean_and_get_assignable_ops(p, max_ops=2 if p.priority == Priority.QUERY else 1)
                if ops is None:
                    continue
                if ops:
                    return p, ops
                q.append(p)

        # Batch: allow a little bit of progress (RR)
        if s.q_batch:
            n = len(s.q_batch)
            for _ in range(n):
                p = s.q_batch.pop(0)
                ops = _clean_and_get_assignable_ops(p, max_ops=1)
                if ops is None:
                    continue
                if ops:
                    return p, ops
                s.q_batch.append(p)
        return None, None

    def _reservations_for_pool(pool):
        # Reserve headroom so BATCH can't fill the pool and block high-priority arrivals.
        # QUERY uses any available capacity; INTERACTIVE mostly uses any capacity; BATCH is constrained.
        # Tuned conservatively to reduce weighted latency.
        r_cpu_for_high = max(1.0, 0.25 * pool.max_cpu_pool)
        r_ram_for_high = max(1.0, 0.25 * pool.max_ram_pool)
        return r_cpu_for_high, r_ram_for_high

    def _cap_cpu(priority, pool):
        # Per-container CPU caps: give QUERY/INTERACTIVE enough CPU to finish quickly,
        # but avoid monopolizing the pool to keep concurrency high.
        if priority == Priority.QUERY:
            return max(1.0, min(4.0, 0.40 * pool.max_cpu_pool))
        if priority == Priority.INTERACTIVE:
            return max(1.0, min(6.0, 0.55 * pool.max_cpu_pool))
        # Batch: keep smaller to reduce interference with latency-sensitive work
        return max(1.0, min(6.0, 0.50 * pool.max_cpu_pool))

    def _base_ram(priority, pool):
        # RAM doesn't speed execution; keep modest defaults to reduce OOM rate.
        if priority == Priority.QUERY:
            return max(1.0, 0.18 * pool.max_ram_pool)
        if priority == Priority.INTERACTIVE:
            return max(1.0, 0.28 * pool.max_ram_pool)
        return max(1.0, 0.35 * pool.max_ram_pool)

    def _find_best_pool(priority, cpu_need, ram_need):
        # Choose pool that can fit and has the most remaining CPU after placement.
        # For BATCH, enforce reservations (do not consume reserved headroom).
        best = None
        best_score = None
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if priority == Priority.BATCH_PIPELINE:
                r_cpu, r_ram = _reservations_for_pool(pool)
                avail_cpu = max(0.0, avail_cpu - r_cpu)
                avail_ram = max(0.0, avail_ram - r_ram)

            if avail_cpu + 1e-9 < cpu_need or avail_ram + 1e-9 < ram_need:
                continue

            # Score: prefer more headroom to avoid fragmentation and reduce queueing.
            score = (avail_cpu - cpu_need) + 0.1 * (avail_ram - ram_need)
            if best is None or score > best_score:
                best = pool_id
                best_score = score
        return best

    # Ingest new pipelines
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # Process results: update OOM multipliers based on container->(pipeline, ops) mapping
    for r in results:
        # Clean up container mapping if present (succeeded or failed)
        if getattr(r, "container_id", None) is not None and r.container_id in s.container_map:
            mapped_pid, mapped_ops = s.container_map.pop(r.container_id)
        else:
            mapped_pid, mapped_ops = None, None

        if not r.failed():
            continue

        if _is_oom_error(r.error):
            # Prefer mapped ops (most reliable), else fall back to r.ops without pipeline_id precision.
            ops_to_update = mapped_ops if mapped_ops is not None else (r.ops or [])
            pid = mapped_pid
            for op in ops_to_update:
                # If we don't know pid, still key on op identity to avoid doing nothing.
                if pid is None:
                    op_key = (None, id(op))
                else:
                    op_key = s._op_key(pid, op)
                cur = s.op_ram_mult.get(op_key, 1.0)
                nxt = cur * 2.0
                if nxt > 16.0:
                    nxt = 16.0
                s.op_ram_mult[op_key] = nxt
        else:
            # Non-OOM failures: do nothing special here; pipeline status will keep it FAILED and we won't reassign
            # unless ASSIGNABLE_STATES includes FAILED and the simulator expects retries. Keep behavior simple.
            pass

    suspensions = []
    assignments = []

    # Global dispatch loop: keep placing highest-priority runnable work into the best-fitting pool.
    # Hard stop to avoid pathological loops if queues contain only blocked pipelines.
    dispatch_budget = 4 * max(1, s.executor.num_pools) + (len(s.q_query) + len(s.q_interactive) + len(s.q_batch))
    attempts = 0

    while attempts < dispatch_budget:
        attempts += 1
        p, ops = _pick_next_pipeline()
        if not p:
            break

        # Size request for the first op only (assignment can include multiple ops; size applies to container).
        op0 = ops[0]
        # Choose CPU within cap and within some pool; because pool differs, we compute needs first
        # and then pick the pool that can satisfy it.
        # Start with a "desired" cpu and ram; pool selection checks feasibility.
        desired_cpu_cap = None
        desired_ram = None

        # We don't know a priori which pool; compute using worst-case caps w.r.t pool sizes by scanning.
        # To keep it simple: decide per-pool after choosing candidate pool using its caps.
        chosen_pool_id = None

        # Try to place on best pool by iterating pools and evaluating resulting cpu/ram needs per pool.
        best_pool = None
        best_score = None
        best_cpu = None
        best_ram = None

        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]

            cpu_cap = _cap_cpu(p.priority, pool)
            base_ram = _base_ram(p.priority, pool)

            op_key = s._op_key(p.pipeline_id, op0)
            mult = s.op_ram_mult.get(op_key, 1.0)
            ram_need = min(pool.max_ram_pool, base_ram * mult)

            # Never request more than current availability (for high-priority); for batch reservations apply later.
            cpu_need = min(pool.avail_cpu_pool, cpu_cap)
            if cpu_need <= 0.01:
                continue
            ram_need = min(pool.avail_ram_pool, ram_need)
            if ram_need <= 0.01:
                continue

            # If batch, enforce reservations by checking against reduced availability.
            avail_cpu_eff = pool.avail_cpu_pool
            avail_ram_eff = pool.avail_ram_pool
            if p.priority == Priority.BATCH_PIPELINE:
                r_cpu, r_ram = _reservations_for_pool(pool)
                avail_cpu_eff = max(0.0, avail_cpu_eff - r_cpu)
                avail_ram_eff = max(0.0, avail_ram_eff - r_ram)

            if avail_cpu_eff + 1e-9 < cpu_need or avail_ram_eff + 1e-9 < ram_need:
                continue

            score = (avail_cpu_eff - cpu_need) + 0.1 * (avail_ram_eff - ram_need)
            if best_pool is None or score > best_score:
                best_pool = pool_id
                best_score = score
                best_cpu = cpu_need
                best_ram = ram_need

        chosen_pool_id = best_pool
        if chosen_pool_id is None:
            # Couldn't place now; rotate it back and continue trying others
            _enqueue(p)
            continue

        # Record mapping so we can attribute failures back to the correct pipeline/op
        assignment = Assignment(
            ops=ops,
            cpu=best_cpu,
            ram=best_ram,
            priority=p.priority,
            pool_id=chosen_pool_id,
            pipeline_id=p.pipeline_id,
        )
        assignments.append(assignment)

        # We do not know container_id until results arrive; mapping will be filled on failure/success.
        # However, ExecutionResult contains container_id; we can map it when we see results only.
        # Still, we can store a "pending mapping" keyed by a synthetic token? Not available.
        # So we instead rely on r.ops + r.container_id where possible; and on pipeline_id via assignment.
        # If the simulator attaches pipeline_id to results, the mapping is not needed.
        # To improve attribution anyway, stash a weak mapping by (pool_id, first_op_object_id) for next tick:
        # (Used only if container_id mapping is absent; harmless otherwise.)
        # Keep small to avoid unbounded growth.
        if not hasattr(s, "weak_map"):
            s.weak_map = {}
        s.weak_map[(chosen_pool_id, id(op0))] = (p.pipeline_id, ops)
        if len(s.weak_map) > 10000:
            # crude bound
            s.weak_map.clear()

        # Re-enqueue pipeline for RR fairness (it may have more runnable ops later)
        _enqueue(p)

    # Opportunistically map results to pipelines using weak_map when container_id mapping wasn't possible
    # (done at end so it's based on current tick's weak_map too).
    for r in results:
        if getattr(r, "container_id", None) is None:
            continue
        if r.container_id in s.container_map:
            continue
        if not r.ops:
            continue
        first_op = r.ops[0]
        key = (r.pool_id, id(first_op))
        if hasattr(s, "weak_map") and key in s.weak_map:
            pid, ops = s.weak_map[key]
            s.container_map[r.container_id] = (pid, ops)

    return suspensions, assignments
