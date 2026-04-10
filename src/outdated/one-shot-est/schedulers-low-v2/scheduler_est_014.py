# policy_key: scheduler_est_014
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.034710
# generation_seconds: 46.49
# generated_at: 2026-04-01T13:45:46.424157
@register_scheduler_init(key="scheduler_est_014")
def scheduler_est_014_init(s):
    """Priority-aware incremental improvement over naive FIFO.

    Main ideas (kept intentionally simple and robust):
    1) Maintain per-priority pipeline queues and always consider higher priority first.
    2) Pack multiple runnable operators per pool per tick (not just one), with small per-op CPU targets.
    3) Use any provided op.estimate.mem_peak_gb as a conservative RAM floor; on OOM, increase RAM for that op and retry.
    4) If high-priority work is blocked everywhere due to lack of headroom, preempt (suspend) one lowest-priority running
       container to free resources (minimal churn: at most 1 suspension per tick).
    """
    # Per-priority waiting queues (pipelines)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-op RAM backoff after OOM: (pipeline_id, op_id) -> ram_gb
    s.op_ram_hint = {}

    # Track running containers we started so we can preempt low priority if needed.
    # container_id -> dict(priority=..., pool_id=..., cpu=..., ram=..., pipeline_id=...)
    s.running = {}

    # Minor tuning knobs
    s.cpu_target = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.cpu_min = 0.5  # don't start a container with vanishingly small CPU
    s.ram_headroom_frac = 0.10  # keep some headroom to reduce OOM cascades / fragmentation
    s.max_assignments_per_pool_per_tick = 8  # prevent over-scheduling loops


@register_scheduler(key="scheduler_est_014")
def scheduler_est_014(s, results, pipelines):
    """
    Priority-aware scheduler with conservative RAM sizing + simple OOM retry + minimal preemption.
    """
    # -------- helpers --------
    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _all_queues():
        return (s.q_query, s.q_interactive, s.q_batch)

    def _prio_rank(prio):
        # lower is better
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2

    def _iter_pipelines_by_priority():
        # stable within each queue; query -> interactive -> batch
        for q in (s.q_query, s.q_interactive, s.q_batch):
            for p in q:
                yield p

    def _get_next_runnable_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return None
        # Keep policy simple: only schedule ops whose parents are complete
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _op_key(p, op):
        # Best-effort stable identifier for per-op hints across retries
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = getattr(op, "name", None)
        if op_id is None:
            op_id = id(op)
        return (p.pipeline_id, op_id)

    def _estimate_ram_for_op(pool, p, op):
        # Conservative: treat estimator as a floor, add small safety margin.
        # If we have an OOM-driven backoff, that overrides estimator.
        k = _op_key(p, op)
        if k in s.op_ram_hint:
            return float(s.op_ram_hint[k])

        est = getattr(op, "estimate", None)
        est_gb = None
        if est is not None:
            est_gb = getattr(est, "mem_peak_gb", None)

        # Default RAM target: modest chunk of the pool, but never exceed pool capacity.
        # We keep a headroom fraction to reduce fragmentation / churn.
        pool_effective_ram = max(0.0, float(pool.max_ram_pool) * (1.0 - s.ram_headroom_frac))
        default_gb = min(8.0, pool_effective_ram)  # a safe-ish small default
        if est_gb is None:
            return max(1.0, default_gb)

        # Add margin; never allocate below 1GB.
        return max(1.0, min(pool_effective_ram, float(est_gb) * 1.25))

    def _cpu_for_op(pool, prio, avail_cpu):
        tgt = float(s.cpu_target.get(prio, 1.0))
        return max(s.cpu_min, min(float(avail_cpu), min(float(pool.max_cpu_pool), tgt)))

    def _clean_and_requeue_queues():
        # Remove completed pipelines; keep others.
        new_q_query, new_q_interactive, new_q_batch = [], [], []
        for p in s.q_query:
            if not p.runtime_status().is_pipeline_successful():
                new_q_query.append(p)
        for p in s.q_interactive:
            if not p.runtime_status().is_pipeline_successful():
                new_q_interactive.append(p)
        for p in s.q_batch:
            if not p.runtime_status().is_pipeline_successful():
                new_q_batch.append(p)
        s.q_query, s.q_interactive, s.q_batch = new_q_query, new_q_interactive, new_q_batch

    def _have_blocked_high_priority():
        # High priority exists but no pool has enough resources even for minimal footprint.
        # We only check for at least one runnable op at high priorities.
        for p in s.q_query:
            op = _get_next_runnable_op(p)
            if op is not None:
                return True
        for p in s.q_interactive:
            op = _get_next_runnable_op(p)
            if op is not None:
                return True
        return False

    def _any_pool_can_fit(min_cpu, min_ram):
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if float(pool.avail_cpu_pool) >= float(min_cpu) and float(pool.avail_ram_pool) >= float(min_ram):
                return True
        return False

    # -------- ingest new pipelines --------
    for p in pipelines:
        _enqueue_pipeline(p)

    # -------- process results: update running set and OOM backoff hints --------
    # Note: results may include failures/oom; we treat all failures as "increase RAM and retry"
    # (this is a small, obvious improvement; later iterations can classify errors).
    if results:
        for r in results:
            # Remove from running tracking if present (completed or failed)
            cid = getattr(r, "container_id", None)
            if cid in s.running:
                del s.running[cid]

            if getattr(r, "failed", None) and r.failed():
                # On failure, if it looks like an OOM, increase RAM for the op(s) we attempted.
                # We don't have a structured error taxonomy here, so we use a conservative heuristic:
                # any failure triggers RAM backoff for the involved ops.
                # Backoff: double previous, bounded by pool max.
                pool = s.executor.pools[r.pool_id]
                prev_ram = float(getattr(r, "ram", 0.0) or 0.0)
                new_ram = prev_ram * 2.0 if prev_ram > 0 else max(2.0, float(pool.max_ram_pool) * 0.25)

                # Cap at pool effective maximum (leave headroom).
                cap = max(1.0, float(pool.max_ram_pool) * (1.0 - s.ram_headroom_frac))
                new_ram = min(new_ram, cap)

                # Apply to all ops in this failed container (usually one).
                for op in getattr(r, "ops", []) or []:
                    # We don't have pipeline_id on result ops directly, but we do have r.pipeline_id? not specified.
                    # We can fall back to using a per-op id without pipeline_id if unavailable.
                    # Best effort: store under (pipeline_id, op_id) if pipeline_id exists on result; else just op_id.
                    pipeline_id = getattr(r, "pipeline_id", None)
                    op_id = getattr(op, "op_id", None)
                    if op_id is None:
                        op_id = getattr(op, "operator_id", None)
                    if op_id is None:
                        op_id = getattr(op, "name", None)
                    if op_id is None:
                        op_id = id(op)

                    if pipeline_id is None:
                        # If we can't key by pipeline, still store a global hint for this op id.
                        k = ("<unknown_pipeline>", op_id)
                    else:
                        k = (pipeline_id, op_id)

                    prev_hint = float(s.op_ram_hint.get(k, 0.0) or 0.0)
                    s.op_ram_hint[k] = max(prev_hint, float(new_ram))

    # Always clean queues after receiving results to avoid dead weight.
    if pipelines or results:
        _clean_and_requeue_queues()

    # Early exit if no changes and nothing to do
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # -------- minimal preemption trigger (at most one suspension per tick) --------
    # If we have high-priority runnable work but cannot fit even a minimal container anywhere,
    # suspend one lowest-priority running container to create headroom.
    # (We intentionally do not try to immediately schedule into the freed space in the same tick;
    # the simulator will reflect the suspension next tick deterministically.)
    high_prio_present = _have_blocked_high_priority()
    if high_prio_present:
        # Minimal footprint we want to be able to admit
        min_cpu = s.cpu_min
        min_ram = 1.0
        if not _any_pool_can_fit(min_cpu=min_cpu, min_ram=min_ram) and s.running:
            # Pick lowest-priority running container; tie-break by largest RAM to free space quickly
            victims = []
            for cid, info in s.running.items():
                victims.append(( _prio_rank(info["priority"]), -float(info["ram"]), cid, info))
            victims.sort(reverse=True)  # worst (highest rank) first; largest ram first due to -ram
            # reverse=True puts higher prio_rank first; that's what we want for victim (batch before query)
            # But ensure we never suspend QUERY if possible.
            chosen = None
            for _, _, cid, info in victims:
                if info["priority"] != Priority.QUERY:
                    chosen = (cid, info)
                    break
            if chosen is None:
                # As a last resort, suspend interactive if only queries are running (rare), but never suspend queries.
                for _, _, cid, info in victims:
                    if info["priority"] == Priority.INTERACTIVE:
                        chosen = (cid, info)
                        break

            if chosen is not None:
                cid, info = chosen
                suspensions.append(Suspend(container_id=cid, pool_id=info["pool_id"]))
                # Remove from our local running map immediately to avoid repeated suspends
                del s.running[cid]

    # -------- scheduling / placement --------
    # Strategy: for each pool, repeatedly pick the next runnable op from the highest-priority pipeline
    # that can fit in current avail resources (RAM+CPU), until the pool is full or we hit a cap.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        num_assigned = 0
        # Keep some RAM headroom to reduce OOM cascades. Use avail after headroom.
        effective_avail_ram = max(0.0, avail_ram - float(pool.max_ram_pool) * s.ram_headroom_frac)

        while num_assigned < s.max_assignments_per_pool_per_tick:
            # Find best candidate pipeline/op that fits
            chosen = None  # (prio_rank, pipeline, op, cpu, ram)
            for p in _iter_pipelines_by_priority():
                op = _get_next_runnable_op(p)
                if op is None:
                    continue

                cpu_need = _cpu_for_op(pool, p.priority, avail_cpu)
                ram_need = _estimate_ram_for_op(pool, p, op)

                if cpu_need <= avail_cpu and ram_need <= effective_avail_ram:
                    cand = (_prio_rank(p.priority), p, op, cpu_need, ram_need)
                    if chosen is None or cand[0] < chosen[0]:
                        chosen = cand
                        # Can't do better than query; break early if we found one
                        if cand[0] == 0:
                            break

            if chosen is None:
                break

            _, p, op, cpu_need, ram_need = chosen

            assignment = Assignment(
                ops=[op],
                cpu=cpu_need,
                ram=ram_need,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Update local available resources for packing multiple ops in the same tick
            avail_cpu -= float(cpu_need)
            effective_avail_ram -= float(ram_need)
            num_assigned += 1

            # Track running container best-effort: container_id is only known after execution starts,
            # but some simulators may fill it later via results. We can still track intent by storing a placeholder.
            # We'll reconcile by removing on results when container_id appears.
            # Here we do nothing since we don't have container_id yet.

            if avail_cpu < s.cpu_min or effective_avail_ram < 1.0:
                break

    # -------- update running table from results for future preemption selection --------
    # We only know container_id from results; if the simulator provides "started" results, we can track here.
    # As a fallback, also track currently running via results that indicate success/failure; already handled above.
    # (No-op unless the simulator emits intermediate RUNNING events with container_id.)
    if results:
        for r in results:
            cid = getattr(r, "container_id", None)
            if cid is None:
                continue
            # If this result is not a terminal event, keep it as running; best-effort.
            if getattr(r, "failed", None) and r.failed():
                continue
            err = getattr(r, "error", None)
            if err:
                continue
            # If ops exist and this isn't terminal, record as running
            if cid not in s.running and getattr(r, "ops", None):
                s.running[cid] = {
                    "priority": r.priority,
                    "pool_id": r.pool_id,
                    "cpu": float(getattr(r, "cpu", 0.0) or 0.0),
                    "ram": float(getattr(r, "ram", 0.0) or 0.0),
                    "pipeline_id": getattr(r, "pipeline_id", None),
                }

    return suspensions, assignments
