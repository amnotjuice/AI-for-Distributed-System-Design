# policy_key: scheduler_high_050
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.132507
# generation_seconds: 139.41
# generated_at: 2026-04-10T07:45:59.243511
@register_scheduler_init(key="scheduler_high_050")
def scheduler_high_050_init(s):
    """
    Priority-aware, failure-averse scheduler.

    Core ideas:
      - Protect QUERY/INTERACTIVE latency via (a) pool preference when multiple pools exist, and
        (b) headroom reservation against BATCH when high-priority work is waiting.
      - Avoid the heavy 720s penalty by retrying failed operators with increasing RAM (OOM-safe),
        rather than dropping pipelines on first failure.
      - Maintain fairness (round-robin within each priority) plus lightweight aging for BATCH.
      - Keep the policy simple: no preemption (minimizes churn/wasted work); rely on reservations instead.
    """
    s.tick = 0

    # Known pipelines by id (lets us keep long-lived metadata).
    s.pipelines_by_id = {}

    # Round-robin queues by priority.
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline tuning knobs learned from failures/successes.
    # Fields:
    #   arrival_tick, last_progress_tick, last_scheduled_tick,
    #   ram_mult, cpu_mult, fail_count
    s.pipeline_info = {}

    # Map operator object identity -> pipeline_id for attributing ExecutionResults.
    s.op_to_pipeline_id = {}

    # Batch fairness knobs.
    s.last_batch_scheduled_tick = -10**9
    s.batch_min_interval_ticks = 40  # allow at least some batch progress even under steady HP load
    s.batch_promote_after_ticks = 200  # aging threshold: old batch treated like interactive for selection

    # Retry/boost caps (avoid runaway).
    s.max_ram_mult = 16.0
    s.max_cpu_mult = 3.0


def _priority_base_fracs(priority):
    # Conservative starting points; RAM beyond minimum doesn't help performance, but avoids OOM.
    if priority == Priority.QUERY:
        return 0.65, 0.55  # cpu_frac, ram_frac
    if priority == Priority.INTERACTIVE:
        return 0.55, 0.45
    return 0.40, 0.30  # batch


def _effective_class(s, pipeline):
    # Aging: old batch gets considered earlier (selection only), but keeps original priority in Assignment.
    if pipeline.priority == Priority.BATCH_PIPELINE:
        info = s.pipeline_info.get(pipeline.pipeline_id)
        if info:
            waited = s.tick - info["arrival_tick"]
            if waited >= s.batch_promote_after_ticks:
                return Priority.INTERACTIVE
    return pipeline.priority


def _get_assignable_ops(pipeline):
    st = pipeline.runtime_status()
    return st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)


def _is_done(pipeline):
    return pipeline.runtime_status().is_pipeline_successful()


def _requeue_rr(queue, pipeline):
    # Keep RR order by pushing to tail.
    queue.append(pipeline)


def _take_next_pipeline_with_ops(s, queue, allow_pred):
    """
    Round-robin scan:
      - Drops completed pipelines from the queue.
      - Returns (pipeline, ops) for first pipeline satisfying allow_pred and having assignable ops.
      - Re-queues non-selected pipelines to preserve fairness.
    """
    n = len(queue)
    for _ in range(n):
        p = queue.pop(0)
        if _is_done(p):
            continue
        ops = _get_assignable_ops(p)
        if ops and allow_pred(p):
            _requeue_rr(queue, p)
            return p, ops
        _requeue_rr(queue, p)
    return None, None


def _has_waiting_high_priority(s):
    # True if any QUERY/INTERACTIVE has an operator ready to run.
    for pr in (Priority.QUERY, Priority.INTERACTIVE):
        q = s.queues.get(pr, [])
        for p in q:
            if _is_done(p):
                continue
            if _get_assignable_ops(p):
                return True
    return False


def _size_request(s, pool, pipeline, avail_cpu, avail_ram, high_waiting, pool_id, prefer_small_batch=False):
    """
    Decide CPU/RAM for this pipeline's next operator.

    Principles:
      - QUERY/INTERACTIVE: bias toward higher CPU to reduce latency, but don't always grab everything.
      - BATCH: smaller footprints to preserve headroom, unless aging promoted or repeated OOMs.
      - On failures: increase RAM aggressively (OOM safety), CPU mildly (helps runtime but sublinear).
    """
    info = s.pipeline_info[pipeline.pipeline_id]
    base_cpu_frac, base_ram_frac = _priority_base_fracs(pipeline.priority)

    # If this batch is "aged", treat it like interactive for sizing too (still labeled batch for metrics).
    eff = _effective_class(s, pipeline)
    if pipeline.priority == Priority.BATCH_PIPELINE and eff == Priority.INTERACTIVE:
        base_cpu_frac, base_ram_frac = _priority_base_fracs(Priority.INTERACTIVE)

    # When high-priority work is waiting, keep some headroom in *all* pools by shrinking batch requests.
    if pipeline.priority == Priority.BATCH_PIPELINE and high_waiting:
        base_cpu_frac *= 0.70
        base_ram_frac *= 0.70

    # Optional "tiny batch" mode to guarantee some progress under heavy HP load.
    if pipeline.priority == Priority.BATCH_PIPELINE and prefer_small_batch:
        base_cpu_frac = min(base_cpu_frac, 0.20)
        base_ram_frac = min(base_ram_frac, 0.20)

    # Compute targets relative to pool maxima (stable), then clamp to available.
    target_cpu = int(max(1, round(pool.max_cpu_pool * base_cpu_frac * info["cpu_mult"])))
    target_ram = int(max(1, round(pool.max_ram_pool * base_ram_frac * info["ram_mult"])))

    cpu = int(min(avail_cpu, max(1, target_cpu)))
    ram = int(min(avail_ram, max(1, target_ram)))

    # If resources are extremely tight, avoid placing "big" tasks that will likely OOM:
    # keep at least minimal granularity.
    cpu = max(1, cpu)
    ram = max(1, ram)
    return cpu, ram


@register_scheduler(key="scheduler_high_050")
def scheduler_high_050(s, results, pipelines):
    suspensions = []
    assignments = []

    s.tick += 1

    # Register new pipelines and enqueue by priority.
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s.pipeline_info:
            s.pipeline_info[p.pipeline_id] = {
                "arrival_tick": s.tick,
                "last_progress_tick": s.tick,
                "last_scheduled_tick": -1,
                "ram_mult": 1.0,
                "cpu_mult": 1.0,
                "fail_count": 0,
            }
            s.queues[p.priority].append(p)
        else:
            # Refresh object reference if simulator hands a new instance with same id.
            # Ensure it's present in the queue at least once.
            found = False
            for q in s.queues.values():
                for existing in q:
                    if existing.pipeline_id == p.pipeline_id:
                        found = True
                        break
                if found:
                    break
            if not found:
                s.queues[p.priority].append(p)

    # Attribute results back to pipelines to tune retries.
    for r in results:
        pid = None
        if getattr(r, "ops", None):
            for op in r.ops:
                pid = s.op_to_pipeline_id.get(id(op))
                if pid is not None:
                    break

        if pid is None or pid not in s.pipeline_info:
            continue

        info = s.pipeline_info[pid]

        # Cleanup op->pipeline mappings for these ops (container is done either way).
        if getattr(r, "ops", None):
            for op in r.ops:
                s.op_to_pipeline_id.pop(id(op), None)

        if r.failed():
            info["fail_count"] += 1

            # Treat failures as potential OOM -> increase RAM multiplier aggressively.
            info["ram_mult"] = min(s.max_ram_mult, max(info["ram_mult"] * 1.7, info["ram_mult"] + 0.5))

            # Mild CPU bump to reduce time-to-retry and improve latency a bit.
            info["cpu_mult"] = min(s.max_cpu_mult, max(info["cpu_mult"] * 1.15, info["cpu_mult"] + 0.1))
        else:
            # Success: record progress and gently decay multipliers toward 1.0.
            info["last_progress_tick"] = s.tick
            info["ram_mult"] = max(1.0, info["ram_mult"] * 0.92)
            info["cpu_mult"] = max(1.0, info["cpu_mult"] * 0.95)

    high_waiting = _has_waiting_high_priority(s)

    # Pool strategy:
    # - If >=2 pools: prefer pool 0 for QUERY/INTERACTIVE; use others mainly for BATCH.
    # - If 1 pool: reserve headroom for high priority by limiting batch scheduling under HP backlog.
    num_pools = s.executor.num_pools

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Headroom reservation (effective only when HP waiting).
        reserve_cpu = int(max(0, round(pool.max_cpu_pool * 0.25))) if high_waiting else 0
        reserve_ram = int(max(0, round(pool.max_ram_pool * 0.25))) if high_waiting else 0

        # Decide what this pool should prefer.
        prefer_high = (num_pools >= 2 and pool_id == 0)

        # Fill the pool greedily with fairness:
        # - Keep scheduling as long as we can allocate at least 1 CPU and 1 RAM.
        # - One operator per assignment to avoid long "locks" and improve tail latency.
        while avail_cpu > 0 and avail_ram > 0:
            picked_pipeline = None
            picked_ops = None

            # Decide if we should force a tiny batch to prevent starvation.
            allow_starvation_break = (
                (s.tick - s.last_batch_scheduled_tick) >= s.batch_min_interval_ticks
            )

            # Build an allow predicate that can enforce pool preference.
            def allow_in_pool(p):
                if num_pools >= 2:
                    # Pool 0: avoid batch unless no HP waiting.
                    if prefer_high and p.priority == Priority.BATCH_PIPELINE and high_waiting:
                        return False
                    # Other pools: avoid scheduling HP if pool 0 is available? We don't know availability.
                    # So we allow HP everywhere, but batch is preferred there.
                    return True
                return True

            # Selection order:
            # - Prefer QUERY/INTERACTIVE (and aged batch via effective class).
            # - In non-preferred pools, still try to keep batch moving.
            # - Under HP backlog, batch may be restricted unless we need to break starvation.
            if prefer_high:
                # Pool 0: high priority first.
                for pr in (Priority.QUERY, Priority.INTERACTIVE):
                    picked_pipeline, picked_ops = _take_next_pipeline_with_ops(
                        s, s.queues[pr], allow_in_pool
                    )
                    if picked_pipeline:
                        break

                # Consider "promoted" batch as interactive if nothing else.
                if not picked_pipeline:
                    def allow_promoted_batch(p):
                        return allow_in_pool(p) and (_effective_class(s, p) == Priority.INTERACTIVE)
                    picked_pipeline, picked_ops = _take_next_pipeline_with_ops(
                        s, s.queues[Priority.BATCH_PIPELINE], allow_promoted_batch
                    )

                # Finally, allow regular batch only if no HP waiting.
                if not picked_pipeline and not high_waiting:
                    picked_pipeline, picked_ops = _take_next_pipeline_with_ops(
                        s, s.queues[Priority.BATCH_PIPELINE], allow_in_pool
                    )
            else:
                # Non-zero pools: keep throughput via batch, but still allow HP (spillover).
                # 1) Query first (score dominates).
                picked_pipeline, picked_ops = _take_next_pipeline_with_ops(
                    s, s.queues[Priority.QUERY], allow_in_pool
                )

                # 2) Interactive next.
                if not picked_pipeline:
                    picked_pipeline, picked_ops = _take_next_pipeline_with_ops(
                        s, s.queues[Priority.INTERACTIVE], allow_in_pool
                    )

                # 3) Promoted batch (aged) next.
                if not picked_pipeline:
                    def allow_promoted_batch(p):
                        return allow_in_pool(p) and (_effective_class(s, p) == Priority.INTERACTIVE)
                    picked_pipeline, picked_ops = _take_next_pipeline_with_ops(
                        s, s.queues[Priority.BATCH_PIPELINE], allow_promoted_batch
                    )

                # 4) Regular batch last, with reservation under HP backlog (unless breaking starvation).
                if not picked_pipeline:
                    if not high_waiting or allow_starvation_break:
                        picked_pipeline, picked_ops = _take_next_pipeline_with_ops(
                            s, s.queues[Priority.BATCH_PIPELINE], allow_in_pool
                        )

            if not picked_pipeline or not picked_ops:
                break

            # Enforce headroom by limiting what batch can consume when high-priority is waiting.
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram
            prefer_small_batch = False

            if picked_pipeline.priority == Priority.BATCH_PIPELINE and high_waiting:
                # Default: only use capacity above reserved headroom.
                eff_avail_cpu = max(0, avail_cpu - reserve_cpu)
                eff_avail_ram = max(0, avail_ram - reserve_ram)

                # If we'd otherwise schedule nothing batch at all, allow an occasional tiny batch.
                if (eff_avail_cpu <= 0 or eff_avail_ram <= 0) and allow_starvation_break:
                    eff_avail_cpu = avail_cpu
                    eff_avail_ram = avail_ram
                    prefer_small_batch = True

            if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                # Can't fit this class without violating reservation; stop filling this pool for now.
                break

            # Size the request.
            cpu, ram = _size_request(
                s, pool, picked_pipeline, eff_avail_cpu, eff_avail_ram,
                high_waiting=high_waiting, pool_id=pool_id, prefer_small_batch=prefer_small_batch
            )

            if cpu <= 0 or ram <= 0 or cpu > eff_avail_cpu or ram > eff_avail_ram:
                # If we can't size it, stop trying to pack this pool this tick.
                break

            op = picked_ops[0]
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=picked_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=picked_pipeline.pipeline_id,
                )
            )

            # Track attribution for tuning on future results.
            s.op_to_pipeline_id[id(op)] = picked_pipeline.pipeline_id

            # Update pipeline metadata.
            info = s.pipeline_info[picked_pipeline.pipeline_id]
            info["last_scheduled_tick"] = s.tick

            # Track batch scheduling for starvation breaking.
            if picked_pipeline.priority == Priority.BATCH_PIPELINE:
                s.last_batch_scheduled_tick = s.tick

            # Consume pool capacity.
            avail_cpu -= cpu
            avail_ram -= ram

    return suspensions, assignments
