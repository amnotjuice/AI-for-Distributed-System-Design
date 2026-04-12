# policy_key: scheduler_iter_median_simple_016
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046621
# generation_seconds: 30.16
# generated_at: 2026-04-12T01:40:18.644111
@register_scheduler_init(key="scheduler_iter_median_simple_016")
def scheduler_iter_median_simple_016_init(s):
    """Strict-priority, latency-first scheduler with simple OOM backoff and batch gating.

    Main deltas vs naive FIFO:
    - Strict priority order: QUERY > INTERACTIVE > BATCH.
    - Latency-first sizing: give high-priority ops most of the pool (fewer concurrent ops, faster completion).
    - Batch gating: if any high-priority runnable work exists, do not schedule batch in that tick (prevents HoL blocking).
    - Simple OOM backoff: if a pipeline shows FAILED ops, retry with increased RAM multiplier (assumed OOM).
    - Minimal fairness: periodically allow batch even under sustained HP load (burst window).
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-pipeline hints
    # pstate[pipeline_id] = {"ram_mult": float, "last_failed_cnt": int}
    s.pstate = {}
    s.tick = 0

    # Batch fairness knobs
    s.batch_force_every = 25   # every N ticks, allow some batch even if HP exists
    s.batch_force_quota = 1    # max batch assignments per pool during forced ticks


@register_scheduler(key="scheduler_iter_median_simple_016")
def scheduler_iter_median_simple_016_scheduler(s, results, pipelines):
    """
    Step logic:
    1) Enqueue new pipelines.
    2) Update per-pipeline RAM multiplier when we observe FAILED ops in runtime_status (assume OOM-like).
    3) Compute whether HP runnable backlog exists; gate batch if so (except fairness bursts).
    4) For each pool, repeatedly assign 1 ready op at a time, strict priority, with latency-first sizing.
    """
    s.tick += 1

    def _ensure_pstate(p):
        if p.pipeline_id not in s.pstate:
            s.pstate[p.pipeline_id] = {"ram_mult": 1.0, "last_failed_cnt": 0}
        return s.pstate[p.pipeline_id]

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _is_pipeline_droppable(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _runnable_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[:1]

    def _has_runnable(pri):
        for p in s.queues[pri]:
            if _is_pipeline_droppable(p):
                continue
            if _runnable_op(p):
                return True
        return False

    def _clean_queue(pri):
        # Remove completed pipelines; keep order stable otherwise
        q = s.queues[pri]
        if not q:
            return
        kept = []
        for p in q:
            if not _is_pipeline_droppable(p):
                kept.append(p)
        s.queues[pri] = kept

    def _update_backoff_from_status(p):
        # Without reliable error->pipeline linkage, use runtime_status FAILED delta as a proxy for OOM/backoff need.
        st = p.runtime_status()
        failed_cnt = st.state_counts.get(OperatorState.FAILED, 0)
        ps = _ensure_pstate(p)
        if failed_cnt > ps["last_failed_cnt"]:
            # Backoff RAM aggressively to reduce repeated failures/retries (latency killer).
            ps["ram_mult"] = min(ps["ram_mult"] * 2.0, 32.0)
            ps["last_failed_cnt"] = failed_cnt

    def _pick_sizing(pool, pri, pipeline_id, avail_cpu, avail_ram):
        # Latency-first: allocate most of the pool to HP, to finish in fewer waves.
        ps = s.pstate.get(pipeline_id, {"ram_mult": 1.0})
        ram_mult = ps.get("ram_mult", 1.0)

        if pri == Priority.QUERY:
            cpu_target = max(1.0, min(avail_cpu, pool.max_cpu_pool * 0.90))
            ram_target = max(1.0, min(avail_ram, pool.max_ram_pool * 0.25 * ram_mult))
        elif pri == Priority.INTERACTIVE:
            cpu_target = max(1.0, min(avail_cpu, pool.max_cpu_pool * 0.85))
            ram_target = max(1.0, min(avail_ram, pool.max_ram_pool * 0.35 * ram_mult))
        else:
            # Batch: keep moderate sizing so it doesn't monopolize; throughput-oriented.
            cpu_target = max(1.0, min(avail_cpu, pool.max_cpu_pool * 0.60))
            ram_target = max(1.0, min(avail_ram, pool.max_ram_pool * 0.45 * ram_mult))

        # Clamp again to available
        cpu_target = min(cpu_target, avail_cpu)
        ram_target = min(ram_target, avail_ram)
        return cpu_target, ram_target

    # 1) Enqueue new pipelines
    for p in pipelines:
        _ensure_pstate(p)
        s.queues[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # 2) Clean queues + update OOM backoff proxies
    for pri in _priority_order():
        _clean_queue(pri)
        for p in s.queues[pri]:
            _update_backoff_from_status(p)

    # 3) Compute gating conditions
    hp_runnable = _has_runnable(Priority.QUERY) or _has_runnable(Priority.INTERACTIVE)
    force_batch_tick = (s.tick % s.batch_force_every == 0)

    allowed_priorities = [Priority.QUERY, Priority.INTERACTIVE]
    if not hp_runnable or force_batch_tick:
        allowed_priorities.append(Priority.BATCH_PIPELINE)

    suspensions = []
    assignments = []

    # 4) Schedule per pool with strict priority; rotate each queue by moving scheduled pipeline to tail.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If we're forcing batch on this tick, cap batch assignments per pool to a small quota.
        batch_assigned_this_pool = 0

        made_progress = True
        while made_progress:
            made_progress = False
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            chosen = None
            chosen_ops = None

            for pri in allowed_priorities:
                if pri == Priority.BATCH_PIPELINE and force_batch_tick and batch_assigned_this_pool >= s.batch_force_quota:
                    continue

                q = s.queues[pri]
                if not q:
                    continue

                # Scan in queue order; pick first runnable
                pick_idx = None
                pick_ops = None
                for i, p in enumerate(q):
                    if _is_pipeline_droppable(p):
                        continue
                    ops = _runnable_op(p)
                    if ops:
                        pick_idx = i
                        pick_ops = ops
                        break

                if pick_idx is None:
                    continue

                chosen = q.pop(pick_idx)
                chosen_ops = pick_ops
                # rotate to tail after scheduling attempt to provide fairness within same priority
                q.append(chosen)
                break

            if chosen is None:
                break

            cpu_req, ram_req = _pick_sizing(pool, chosen.priority, chosen.pipeline_id, avail_cpu, avail_ram)
            if cpu_req <= 0 or ram_req <= 0:
                break

            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen.pipeline_id,
                )
            )

            avail_cpu -= cpu_req
            avail_ram -= ram_req
            made_progress = True

            if chosen.priority == Priority.BATCH_PIPELINE:
                batch_assigned_this_pool += 1

    return suspensions, assignments
