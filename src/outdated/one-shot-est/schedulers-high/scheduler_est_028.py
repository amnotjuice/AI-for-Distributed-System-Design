# policy_key: scheduler_est_028
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.181530
# generation_seconds: 63.23
# generated_at: 2026-03-31T19:13:49.952215
@register_scheduler_init(key="scheduler_est_028")
def scheduler_est_028_init(s):
    """Priority-aware, estimate-based right-sizing scheduler (incremental improvement over naive FIFO).

    Main ideas (kept intentionally simple and robust):
    - Maintain per-priority FIFO queues (QUERY > INTERACTIVE > BATCH).
    - Dedicate pool 0 to latency-sensitive work when multiple pools exist.
    - Right-size RAM per operator using op.estimate.mem_peak_gb with a safety factor.
    - On OOM-like failures, increase the per-operator RAM multiplier and retry (instead of dropping).
    - Avoid giving an operator the entire pool by default; pack multiple ops per pool when possible.
    """
    s.waiting_queues = {}  # Priority -> List[Pipeline]
    s.pipeline_map = {}  # pipeline_id -> Pipeline (to avoid duplicates)
    s.ram_multiplier = {}  # op_key -> float
    s.fail_counts = {}  # op_key -> int
    s.max_retries = 3

    # Default safety factor above the estimator; increased on OOM.
    s.base_ram_safety = 1.20
    s.max_ram_multiplier = 8.0

    # Soft caps to encourage packing; high-priority gets more CPU.
    s.cpu_cap_frac = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.50,
    }


@register_scheduler(key="scheduler_est_028")
def scheduler_est_028(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]):
    # ---- helpers (kept local to avoid imports at module scope) ----
    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)

    def _op_key(pipeline_id, op):
        # Use pipeline_id + Python object id; stable across simulation as long as op objects persist.
        return (pipeline_id, id(op))

    def _get_est_mem_gb(op) -> float:
        est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        if est is None:
            return 1.0
        try:
            est = float(est)
        except Exception:
            est = 1.0
        return max(0.1, est)

    def _ram_request_gb(pipeline_id, op, pool) -> float:
        est = _get_est_mem_gb(op)
        mult = s.ram_multiplier.get(_op_key(pipeline_id, op), s.base_ram_safety)
        # Add a small absolute cushion to reduce borderline OOMs for small ops.
        cushion = max(0.25, 0.10 * est)
        req = est * mult + cushion
        # Bound within pool.
        req = min(req, float(pool.max_ram_pool))
        return max(0.1, req)

    def _cpu_request(pipeline_prio, pool, avail_cpu) -> float:
        cap_frac = s.cpu_cap_frac.get(pipeline_prio, 0.5)
        cap = max(1.0, float(pool.max_cpu_pool) * float(cap_frac))
        return max(1.0, min(float(avail_cpu), cap))

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.pipeline_map:
            return
        s.pipeline_map[p.pipeline_id] = p
        q = s.waiting_queues.setdefault(p.priority, [])
        q.append(p)

    def _compact_queues_drop_completed():
        # Remove successful pipelines; keep failed ones for retry (we handle retry limits per-op).
        for prio, q in list(s.waiting_queues.items()):
            new_q = []
            for p in q:
                status = p.runtime_status()
                if status.is_pipeline_successful():
                    # Finished; drop.
                    s.pipeline_map.pop(p.pipeline_id, None)
                    continue
                new_q.append(p)
            s.waiting_queues[prio] = new_q

    def _has_ready_high_priority_work() -> bool:
        for prio in [Priority.QUERY, Priority.INTERACTIVE]:
            for p in s.waiting_queues.get(prio, []):
                status = p.runtime_status()
                if status.is_pipeline_successful():
                    continue
                ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return True
        return False

    def _pick_next_op_for_pool(pool_id, pool, avail_cpu, avail_ram):
        """
        Find the next (pipeline, op, cpu, ram) to schedule on this pool, respecting:
        - pool dedication (pool 0 prefers high-priority if multiple pools)
        - global priority order
        - fit: ram <= avail_ram and cpu <= avail_cpu
        We keep FIFO within priority classes by rotating the queue when we schedule.
        """
        num_pools = s.executor.num_pools
        prefer_hi_on_pool0 = (num_pools >= 2 and pool_id == 0)

        # Two-pass priority list:
        # - If dedicated pool0, first pass tries QUERY/INTERACTIVE only.
        # - Second pass allows all priorities (so pool0 doesn't idle).
        passes = []
        if prefer_hi_on_pool0:
            passes.append([Priority.QUERY, Priority.INTERACTIVE])
            passes.append(_prio_order())
        else:
            # Non-pool0: prefer batch first (throughput), but allow spill of high-priority if idle.
            passes.append([Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE])

        for prio_list in passes:
            for prio in prio_list:
                q = s.waiting_queues.get(prio, [])
                if not q:
                    continue

                # Iterate FIFO but allow skipping pipelines that currently have no ready ops / don't fit.
                # To preserve FIFO, we scan at most len(q) and rotate.
                n = len(q)
                for _ in range(n):
                    p = q.pop(0)
                    status = p.runtime_status()
                    if status.is_pipeline_successful():
                        s.pipeline_map.pop(p.pipeline_id, None)
                        continue

                    # Only schedule ops whose parents are complete.
                    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                    if not ops:
                        q.append(p)
                        continue

                    op = ops[0]
                    ram_req = _ram_request_gb(p.pipeline_id, op, pool)
                    cpu_req = _cpu_request(p.priority, pool, avail_cpu)

                    # Fit check (RAM is the common hard constraint; CPU at least 1 vCPU)
                    if ram_req <= float(avail_ram) and cpu_req <= float(avail_cpu):
                        # Re-append pipeline to keep it in the active set for future stages.
                        q.append(p)
                        return p, op, cpu_req, ram_req

                    # Doesn't fit right now; rotate.
                    q.append(p)

        return None, None, 0.0, 0.0

    # ---- incorporate new pipelines ----
    for p in pipelines:
        _enqueue_pipeline(p)

    # ---- update RAM multipliers from results ----
    for r in results:
        # On success: optionally decay multiplier slightly towards base safety.
        # On OOM-like failure: increase multiplier and allow retry.
        ops = getattr(r, "ops", []) or []
        if r.failed():
            oomish = _is_oom_error(getattr(r, "error", None))
            for op in ops:
                key = _op_key(getattr(r, "pipeline_id", None) or getattr(op, "pipeline_id", None) or "unknown", op)
                # If we can't form a good key from result, best effort: also try to match by scanning pipelines.
                # (No extra work here; Eudoxia typically keeps op objects stable and r.ops points to them.)
                if oomish:
                    cur = s.ram_multiplier.get(key, s.base_ram_safety)
                    s.ram_multiplier[key] = min(s.max_ram_multiplier, max(cur * 1.50, cur + 0.50))
                    s.fail_counts[key] = s.fail_counts.get(key, 0) + 1
                else:
                    # Non-OOM: count failures, but don't inflate RAM.
                    s.fail_counts[key] = s.fail_counts.get(key, 0) + 1
        else:
            for op in ops:
                key = _op_key(getattr(r, "pipeline_id", None) or getattr(op, "pipeline_id", None) or "unknown", op)
                if key in s.ram_multiplier:
                    # Gentle decay to avoid permanently over-allocating after a single spike.
                    s.ram_multiplier[key] = max(s.base_ram_safety, s.ram_multiplier[key] * 0.97)

    # ---- compact queues (drop completed pipelines) ----
    _compact_queues_drop_completed()

    # Early exit if nothing to do.
    if not pipelines and not results:
        # Still might have queued work, so we cannot early-exit purely on no changes.
        pass

    suspensions = []
    assignments = []

    high_prio_backlog = _has_ready_high_priority_work()

    # ---- schedule: pack multiple ops per pool when possible ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If only one pool exists, do strict priority: do not start batch work while high-priority ops are ready.
        if s.executor.num_pools == 1 and high_prio_backlog:
            # Temporarily hide batch queue during scheduling on this pool.
            saved_batch = s.waiting_queues.get(Priority.BATCH_PIPELINE, [])
            s.waiting_queues[Priority.BATCH_PIPELINE] = []

            while avail_cpu > 0 and avail_ram > 0:
                p, op, cpu_req, ram_req = _pick_next_op_for_pool(pool_id, pool, avail_cpu, avail_ram)
                if p is None:
                    break

                # Enforce retry limit: if an op has repeatedly failed, stop attempting it.
                key = _op_key(p.pipeline_id, op)
                if s.fail_counts.get(key, 0) > s.max_retries:
                    # Skip this pipeline for now by rotating; leaving it queued but unscheduled.
                    # (In a fuller implementation we'd mark pipeline failed.)
                    continue

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )
                avail_cpu -= float(cpu_req)
                avail_ram -= float(ram_req)

            # Restore batch queue.
            s.waiting_queues[Priority.BATCH_PIPELINE] = saved_batch
        else:
            # Multi-pool (or no high backlog): allow all priorities, but pool0 prefers high-priority via picker logic.
            while avail_cpu > 0 and avail_ram > 0:
                p, op, cpu_req, ram_req = _pick_next_op_for_pool(pool_id, pool, avail_cpu, avail_ram)
                if p is None:
                    break

                key = _op_key(p.pipeline_id, op)
                if s.fail_counts.get(key, 0) > s.max_retries:
                    continue

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )
                avail_cpu -= float(cpu_req)
                avail_ram -= float(ram_req)

    return suspensions, assignments
