# policy_key: scheduler_none_006
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.066399
# generation_seconds: 39.58
# generated_at: 2026-04-09T21:49:30.134894
@register_scheduler_init(key="scheduler_none_006")
def scheduler_none_006_init(s):
    """Priority-aware, OOM-adaptive, low-churn scheduler.

    Goals for the weighted latency objective:
      - Strongly protect QUERY and INTERACTIVE by (1) reserving headroom and (2) preempting BATCH when needed.
      - Avoid failures by learning per-(pipeline, op) RAM needs from OOMs and retrying with higher RAM.
      - Avoid starvation by mild aging for BATCH and by always leaving some capacity for background progress.
      - Keep changes small vs FIFO: still mostly one-op-per-pool per tick, but with priority ordering + OOM backoff.
    """
    # Queues store Pipeline objects (may repeat across ticks; we requeue after scanning).
    s.wait_q = []
    s.wait_i = []
    s.wait_b = []

    # Per-operator learned RAM floor to avoid repeated OOMs:
    # key: (pipeline_id, op_id) -> ram_required (float)
    s.op_ram_floor = {}

    # Per-pipeline failure counts to avoid infinite retry loops; we still retry because failures are expensive.
    s.pipe_fail_count = {}

    # Basic aging counters to prevent indefinite starvation of BATCH (and INTERACTIVE if saturated by QUERY).
    # key: pipeline_id -> accumulated wait "ticks"
    s.age = {}

    # Track pool reservations (fraction of pool resources to keep available for higher priorities).
    # These are soft targets; we only enforce when contention exists.
    s.reserve_cpu_for_q = 0.25
    s.reserve_ram_for_q = 0.25
    s.reserve_cpu_for_i = 0.10
    s.reserve_ram_for_i = 0.10

    # OOM backoff parameters
    s.oom_ram_growth = 1.6
    s.oom_ram_add = 0.5  # additive bump
    s.max_retries_per_op = 6  # after this, keep trying but grow more aggressively

    # CPU sizing strategy:
    # - Give more CPU to queries/interactives (reduces their latency), but avoid monopolizing the pool.
    # - Batch gets a smaller share to keep concurrency.
    s.cpu_share_q = 0.75
    s.cpu_share_i = 0.60
    s.cpu_share_b = 0.40

    # To reduce thrash, only preempt when a high-priority arrival exists and we truly need headroom.
    s.enable_preemption = True

    # Track outstanding "need" triggers (set when new high-priority arrivals or when high-priority blocked).
    s.hp_pressure = 0


def _priority_rank(p):
    # Lower is better in sort.
    if p.priority == Priority.QUERY:
        return 0
    if p.priority == Priority.INTERACTIVE:
        return 1
    return 2


def _weight_for_priority(pr):
    if pr == Priority.QUERY:
        return 10
    if pr == Priority.INTERACTIVE:
        return 5
    return 1


def _queue_for(s, pipeline):
    if pipeline.priority == Priority.QUERY:
        return s.wait_q
    if pipeline.priority == Priority.INTERACTIVE:
        return s.wait_i
    return s.wait_b


def _op_id(op):
    # Best-effort stable identifier.
    return getattr(op, "op_id", getattr(op, "operator_id", getattr(op, "id", repr(op))))


def _pipeline_done_or_failed(pipeline):
    st = pipeline.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If any operator failed, we still want to retry (OOM-adaptive). Treat as not terminal here.
    return False


def _has_any_failed_ops(pipeline):
    st = pipeline.runtime_status()
    return st.state_counts[OperatorState.FAILED] > 0


def _get_assignable_ops(pipeline):
    st = pipeline.runtime_status()
    # Allow retrying FAILED ops; require parents complete so we respect DAG.
    return st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)


def _estimate_ram_need(s, pipeline, op, pool_avail_ram):
    """Return RAM request to avoid OOM and still allow packing."""
    pid = pipeline.pipeline_id
    oid = _op_id(op)
    floor = s.op_ram_floor.get((pid, oid), None)

    # If we don't know, be conservative but not greedy: request a modest slice of available RAM.
    if floor is None:
        # Use up to 60% of available RAM but leave some headroom for others.
        return max(0.1, pool_avail_ram * 0.60)

    # Use learned floor but cap to available (assignment will be skipped if too large).
    return max(0.1, min(floor, pool_avail_ram))


def _estimate_cpu_need(s, pipeline, pool_avail_cpu):
    if pipeline.priority == Priority.QUERY:
        share = s.cpu_share_q
    elif pipeline.priority == Priority.INTERACTIVE:
        share = s.cpu_share_i
    else:
        share = s.cpu_share_b

    # Ensure non-zero CPU if pool has any.
    return max(0.1, pool_avail_cpu * share)


def _push_with_age(s, pipeline):
    pid = pipeline.pipeline_id
    if pid not in s.age:
        s.age[pid] = 0
    _queue_for(s, pipeline).append(pipeline)


def _bump_ages(s):
    # Increment age for all pipelines currently queued.
    for q in (s.wait_q, s.wait_i, s.wait_b):
        for p in q:
            s.age[p.pipeline_id] = s.age.get(p.pipeline_id, 0) + 1


def _pick_next_pipeline(s):
    """Choose next pipeline among queues using priority + mild aging to avoid starvation.

    We keep this simple:
      - Prefer QUERY over INTERACTIVE over BATCH.
      - But if BATCH has waited a long time, occasionally let it through.
    """
    # Hard priority if there are any queries.
    if s.wait_q:
        return s.wait_q.pop(0)

    if s.wait_i:
        # If a batch has extreme age and no queries, occasionally schedule it.
        if s.wait_b:
            b = s.wait_b[0]
            if s.age.get(b.pipeline_id, 0) >= 50 and (len(s.wait_i) > 0):
                return s.wait_b.pop(0)
        return s.wait_i.pop(0)

    if s.wait_b:
        return s.wait_b.pop(0)

    return None


def _requeue(s, pipeline):
    # Put back at end to preserve FIFO within class.
    _queue_for(s, pipeline).append(pipeline)


def _maybe_preempt_for_hp(s, results, pipelines):
    """Preempt low priority containers if new HP work arrives and pools appear tight.

    Eudoxia API doesn't expose running containers directly in the prompt; we can only suspend by container_id/pool_id
    we have seen via results. So we use a conservative approach:
      - When HP pressure exists, suspend recently-seen BATCH containers from results (best-effort).
      - This is intentionally low-churn: only one suspension per tick across all pools.
    """
    if not s.enable_preemption:
        return []

    # Detect pressure: new query/interactive arrivals, or queued HP but no progress recently.
    hp_arrivals = sum(1 for p in pipelines if p.priority in (Priority.QUERY, Priority.INTERACTIVE))
    if hp_arrivals > 0:
        s.hp_pressure = min(10, s.hp_pressure + 2)
    else:
        s.hp_pressure = max(0, s.hp_pressure - 1)

    if s.hp_pressure <= 0:
        return []

    # Identify suspend candidates from recent execution results: batch containers that are still around.
    # (We cannot know if they already completed; suspending a finished container should be a no-op in simulator.)
    candidates = []
    for r in results:
        if getattr(r, "container_id", None) is None:
            continue
        if r.priority == Priority.BATCH_PIPELINE:
            candidates.append(r)

    if not candidates:
        return []

    # Suspend one candidate (lowest value) to free headroom.
    # Prefer suspending the one with largest RAM (frees most).
    candidates.sort(key=lambda rr: (-(getattr(rr, "ram", 0) or 0), -(getattr(rr, "cpu", 0) or 0)))
    c = candidates[0]
    return [Suspend(c.container_id, c.pool_id)]


@register_scheduler(key="scheduler_none_006")
def scheduler_none_006_scheduler(s, results, pipelines):
    """
    Policy behavior per tick:
      1) Update OOM learning: if an op fails due to OOM, increase its learned RAM floor.
      2) Enqueue new pipelines by priority.
      3) Best-effort preempt a batch container if HP pressure exists (low churn).
      4) For each pool, assign at most one operator per tick:
           - Choose next pipeline via priority + mild aging.
           - Choose first assignable operator whose parents are complete.
           - Size RAM via learned floor; size CPU by priority share.
           - Respect soft reservations for higher priorities by limiting BATCH if pool is tight.
    """
    # 1) Learn from failures; keep pipelines eligible for retry.
    for r in results:
        if getattr(r, "pipeline_id", None) is None:
            # Not guaranteed by prompt; ignore.
            continue

        # Update per-pipeline fail count
        s.pipe_fail_count[r.pipeline_id] = s.pipe_fail_count.get(r.pipeline_id, 0)

        if r.failed():
            s.pipe_fail_count[r.pipeline_id] += 1

            # Heuristic: treat any failure as possible OOM and increase RAM floor for each op in r.ops.
            # We don't have explicit error types in prompt; be robust.
            base_ram = getattr(r, "ram", None)
            if base_ram is None:
                base_ram = 0.0
            msg = str(getattr(r, "error", "") or "").lower()
            looks_like_oom = ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg) or (base_ram > 0)

            if looks_like_oom and getattr(r, "ops", None):
                for op in r.ops:
                    oid = _op_id(op)
                    key = (r.pipeline_id, oid)
                    prev = s.op_ram_floor.get(key, None)
                    # Grow from the RAM that was used in the failed attempt (plus bump).
                    retry_count = s.pipe_fail_count.get(r.pipeline_id, 0)
                    growth = s.oom_ram_growth
                    if retry_count > s.max_retries_per_op:
                        growth = max(growth, 2.0)
                    proposed = (base_ram * growth) + s.oom_ram_add
                    if prev is None:
                        s.op_ram_floor[key] = max(proposed, base_ram + s.oom_ram_add, 0.2)
                    else:
                        s.op_ram_floor[key] = max(prev, proposed, prev * 1.25)

    # 2) Enqueue arrivals
    for p in pipelines:
        _push_with_age(s, p)

    # If nothing new and no results, avoid doing work.
    if not pipelines and not results:
        return [], []

    # 3) Age queued pipelines so long-waiting batch eventually makes progress.
    _bump_ages(s)

    # 4) Optional preemption (low churn)
    suspensions = _maybe_preempt_for_hp(s, results, pipelines)

    assignments = []
    requeue = []

    # Iterate pools; attempt to assign one op per pool per tick.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft reservations: if pool is low and we have queued HP, don't schedule batch into the last headroom.
        have_q = len(s.wait_q) > 0
        have_i = len(s.wait_i) > 0

        # We'll try a few pulls to find a runnable pipeline/op; anything not runnable gets requeued.
        attempts = 0
        assigned_this_pool = False
        while attempts < 12:
            attempts += 1
            pipeline = _pick_next_pipeline(s)
            if pipeline is None:
                break

            # Drop completed pipelines silently.
            if _pipeline_done_or_failed(pipeline):
                continue

            # If pipeline has failures, we still retry; no special casing besides RAM learning.
            ops = _get_assignable_ops(pipeline)
            if not ops:
                # Nothing ready; requeue and continue searching.
                requeue.append(pipeline)
                continue

            # Pick one operator at a time (keeps policy stable and reduces churn).
            op = ops[0]

            # Reservation enforcement for BATCH
            if pipeline.priority == Priority.BATCH_PIPELINE and (have_q or have_i):
                # If we are below reservation headroom, don't schedule batch.
                need_cpu_reserve = (pool.max_cpu_pool * (s.reserve_cpu_for_q + (s.reserve_cpu_for_i if have_i else 0.0)))
                need_ram_reserve = (pool.max_ram_pool * (s.reserve_ram_for_q + (s.reserve_ram_for_i if have_i else 0.0)))
                # If available is already scarce, hold it.
                if avail_cpu <= need_cpu_reserve or avail_ram <= need_ram_reserve:
                    requeue.append(pipeline)
                    continue

            req_ram = _estimate_ram_need(s, pipeline, op, avail_ram)
            req_cpu = _estimate_cpu_need(s, pipeline, avail_cpu)

            # If operator's learned floor is higher than available, skip for now (avoid immediate failure).
            if req_ram > avail_ram or req_cpu > avail_cpu:
                requeue.append(pipeline)
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )
            assigned_this_pool = True
            break

        # If we didn't assign, nothing else to do for this pool.
        if not assigned_this_pool:
            pass

    # Requeue pipelines that weren't runnable or that we skipped due to reservations
    for p in requeue:
        _requeue(s, p)

    return suspensions, assignments
