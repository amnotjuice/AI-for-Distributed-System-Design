# policy_key: scheduler_none_044
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048437
# generation_seconds: 42.36
# generated_at: 2026-04-09T22:19:19.064004
@register_scheduler_init(key="scheduler_none_044")
def scheduler_none_044_init(s):
    """
    Priority-aware, failure-averse, low-churn scheduler.

    Core ideas:
    - Protect QUERY/INTERACTIVE latency via (a) strict priority ordering and (b) optional soft reservation.
    - Improve completion rate by learning per-pipeline RAM needs from OOMs and retrying with exponential backoff in RAM.
    - Avoid wasteful preemption: only preempt when a high-priority op is ready and cannot be scheduled otherwise.
    - Keep batching simple: schedule at most one op per pool per tick to reduce interference and tail latency.
    """
    # FIFO queues per priority (pipeline_id order of arrival).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Pipeline bookkeeping.
    s.known_pipelines = {}  # pipeline_id -> Pipeline (latest object reference)
    s.arrival_order = []    # pipeline_id order of arrival (for stable fairness within class)

    # Per-pipeline RAM hinting driven by OOM failures.
    # We learn a "min safe RAM" we should try next time for this pipeline.
    s.ram_hint = {}         # pipeline_id -> float
    s.oom_count = {}        # pipeline_id -> int

    # Conservative defaults for first attempt (fractions of pool RAM).
    s.default_ram_frac = {
        Priority.QUERY: 0.35,
        Priority.INTERACTIVE: 0.30,
        Priority.BATCH_PIPELINE: 0.20,
    }

    # Soft reservation of pool resources for higher priorities:
    # - When scheduling batch, leave headroom so sudden interactive/query arrivals can start immediately.
    s.reserve_frac_cpu_for_hi = 0.30
    s.reserve_frac_ram_for_hi = 0.30

    # Preemption throttling.
    s.last_preempt_time = -1
    s.preempt_cooldown = 3  # ticks; keep small to be responsive but avoid churn

    # If simulator uses tick/time, we don't assume; we derive a counter.
    s._tick = 0


@register_scheduler(key="scheduler_none_044")
def scheduler_none_044(s, results, pipelines):
    """
    Scheduler step:
    1) Incorporate new pipelines into per-priority queues.
    2) Update RAM hints from failures (OOM -> increase RAM hint).
    3) Attempt to schedule one ready operator per pool, in strict priority order.
    4) If a high-priority op cannot be scheduled due to lack of resources, consider preempting
       one low-priority running container (cooldown-limited), then try scheduling again.
    """
    s._tick += 1

    # -------- helpers --------
    def _enqueue(p):
        pid = p.pipeline_id
        s.known_pipelines[pid] = p
        if pid not in s.arrival_order:
            s.arrival_order.append(pid)

        if p.priority == Priority.QUERY:
            s.q_query.append(pid)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(pid)
        else:
            s.q_batch.append(pid)

    def _is_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator failed, we still want to retry (failure-averse),
        # but only if the simulator allows retrying FAILED state ops (ASSIGNABLE_STATES includes FAILED).
        # We treat pipeline as "not terminal" unless there are no assignable ops left.
        return False

    def _priority_rank(pri):
        if pri == Priority.QUERY:
            return 0
        if pri == Priority.INTERACTIVE:
            return 1
        return 2

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _iter_candidate_pipelines(pri):
        # Stable FIFO within priority; skip completed pipelines, keep others in queue.
        q = _queue_for_priority(pri)
        for pid in q:
            p = s.known_pipelines.get(pid)
            if p is None:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            yield p

    def _cleanup_queues():
        # Remove completed pipelines to avoid queue growth.
        for q in (s.q_query, s.q_interactive, s.q_batch):
            keep = []
            for pid in q:
                p = s.known_pipelines.get(pid)
                if p is None:
                    continue
                if p.runtime_status().is_pipeline_successful():
                    continue
                keep.append(pid)
            q[:] = keep

    def _update_from_results(res_list):
        # On OOM, increase RAM hint for that pipeline (exponential-ish), and allow retry.
        for r in res_list:
            if not r.failed():
                continue
            pid = getattr(r, "pipeline_id", None)
            # pipeline_id is not guaranteed on ExecutionResult per prompt; fall back to mapping by ops.
            if pid is None:
                # Try to infer pipeline_id by scanning known pipelines and matching operator identity.
                # This is best-effort; if no match, we still avoid crashing.
                pid = None
                for k, p in s.known_pipelines.items():
                    try:
                        st = p.runtime_status()
                        # If any op in r.ops belongs to p.values, assume match.
                        # This is intentionally loose to avoid API assumptions.
                        p_ops = getattr(p, "values", None)
                        if p_ops is None:
                            continue
                        # p.values may be a dict/iterable; just attempt membership checks.
                        for op in r.ops:
                            try:
                                if op in p_ops:
                                    pid = k
                                    break
                            except Exception:
                                pass
                        if pid is not None:
                            break
                    except Exception:
                        continue

            # Only treat as OOM if error string suggests it; otherwise, still mildly increase hint to reduce failures.
            err = (r.error or "")
            is_oom = ("OOM" in err) or ("out of memory" in err.lower()) or ("memory" in err.lower())

            if pid is None:
                continue

            prev = s.ram_hint.get(pid)
            used = getattr(r, "ram", None)
            if used is None:
                continue

            cnt = s.oom_count.get(pid, 0)
            if is_oom:
                cnt += 1
                s.oom_count[pid] = cnt
                # Increase to next attempt: 1.5x, bounded by pool max at assignment time.
                new_hint = used * 1.5
            else:
                # Non-OOM failure: smaller bump to avoid repeated failures.
                new_hint = used * 1.2

            if prev is None:
                s.ram_hint[pid] = new_hint
            else:
                s.ram_hint[pid] = max(prev, new_hint)

    def _choose_op(p):
        st = p.runtime_status()
        # Prefer ready ops (parents complete). Take one to reduce interference and latency variance.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        return ops

    def _compute_request(p, pool):
        # Determine RAM/CPU request for the next op of pipeline p in the given pool.
        pid = p.pipeline_id

        # CPU: give more to high priority, but keep it conservative to allow concurrency.
        # Use a fraction of available CPU, capped, and at least 1.
        avail_cpu = pool.avail_cpu_pool
        max_cpu = pool.max_cpu_pool

        if p.priority == Priority.QUERY:
            cpu = min(max_cpu, max(1.0, avail_cpu * 0.60))
        elif p.priority == Priority.INTERACTIVE:
            cpu = min(max_cpu, max(1.0, avail_cpu * 0.50))
        else:
            cpu = min(max_cpu, max(1.0, avail_cpu * 0.40))

        # RAM: use hint if present; else a conservative fraction of pool RAM.
        avail_ram = pool.avail_ram_pool
        max_ram = pool.max_ram_pool
        base = max_ram * s.default_ram_frac.get(p.priority, 0.20)

        hint = s.ram_hint.get(pid)
        if hint is None:
            ram = base
        else:
            # Use at least base, and at least hint, but never exceed max.
            ram = max(base, hint)

        # If we're close to exhausting avail, trim to avail for feasibility (won't exceed max anyway).
        ram = min(ram, max_ram, avail_ram)
        cpu = min(cpu, avail_cpu)

        # Ensure positive.
        if ram <= 0:
            ram = min(max_ram, max(0.0, avail_ram))
        if cpu <= 0:
            cpu = min(max_cpu, max(0.0, avail_cpu))

        return cpu, ram

    def _has_high_priority_waiting():
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in _iter_candidate_pipelines(pri):
                ops = _choose_op(p)
                if ops:
                    return True
        return False

    def _schedule_one_on_pool(pool_id, allow_batch=True):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return None

        # If we are scheduling batch and there is any high-priority waiting, keep headroom.
        keep_headroom = _has_high_priority_waiting()

        def _effective_avail_for_batch():
            if not keep_headroom:
                return avail_cpu, avail_ram
            # Leave some headroom for HI priority to reduce their queueing latency.
            cpu_limit = max(0.0, avail_cpu - pool.max_cpu_pool * s.reserve_frac_cpu_for_hi)
            ram_limit = max(0.0, avail_ram - pool.max_ram_pool * s.reserve_frac_ram_for_hi)
            return cpu_limit, ram_limit

        # Try in strict priority order.
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if pri == Priority.BATCH_PIPELINE and not allow_batch:
                continue

            eff_cpu, eff_ram = (avail_cpu, avail_ram)
            if pri == Priority.BATCH_PIPELINE:
                eff_cpu, eff_ram = _effective_avail_for_batch()
                if eff_cpu <= 0 or eff_ram <= 0:
                    continue

            for p in _iter_candidate_pipelines(pri):
                ops = _choose_op(p)
                if not ops:
                    continue

                # Compute request and check feasibility against effective availability.
                cpu_req, ram_req = _compute_request(p, pool)
                if cpu_req <= 0 or ram_req <= 0:
                    continue
                if cpu_req <= eff_cpu and ram_req <= eff_ram:
                    return Assignment(
                        ops=ops,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )

        return None

    def _maybe_preempt_for_high_priority():
        # Preempt only if a QUERY/INTERACTIVE op is ready but cannot be scheduled anywhere.
        if s.last_preempt_time >= 0 and (s._tick - s.last_preempt_time) < s.preempt_cooldown:
            return []

        # Check if any hi-priority work is waiting.
        hi_waiting = []
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in _iter_candidate_pipelines(pri):
                ops = _choose_op(p)
                if ops:
                    hi_waiting.append((pri, p))
                    break
            if hi_waiting:
                break
        if not hi_waiting:
            return []

        # If we can already schedule without preemption, don't preempt.
        for pool_id in range(s.executor.num_pools):
            a = _schedule_one_on_pool(pool_id, allow_batch=False)
            if a is not None:
                return []

        # Otherwise, preempt one lowest-priority running container observed in results this tick (best-effort),
        # preferring BATCH then INTERACTIVE. This is intentionally conservative and low-churn.
        # Note: we only have container_ids in results, not full executor state; preempt based on what we saw.
        candidates = []
        for r in results:
            # We can only suspend a container if we have its id and pool.
            cid = getattr(r, "container_id", None)
            pid = getattr(r, "pool_id", None)
            if cid is None or pid is None:
                continue
            pri = getattr(r, "priority", None)
            if pri is None:
                continue
            candidates.append(( _priority_rank(pri), pri, cid, pid))

        # Pick worst priority (highest rank number). Only preempt if it's batch; interactive preemption is last resort.
        candidates.sort(reverse=True)
        for rank, pri, cid, pid in candidates:
            if pri == Priority.BATCH_PIPELINE:
                s.last_preempt_time = s._tick
                return [Suspend(container_id=cid, pool_id=pid)]

        # If no batch candidate seen, avoid preempting interactive/query from this heuristic.
        return []

    # -------- main logic --------
    for p in pipelines:
        _enqueue(p)

    # Update hints based on failures; retrying is handled by allowing FAILED in ASSIGNABLE_STATES.
    if results:
        _update_from_results(results)

    # Fast path: if nothing new and no results, no changes.
    if not pipelines and not results:
        return [], []

    _cleanup_queues()

    suspensions = []
    assignments = []

    # Consider minimal, cooldown-limited preemption for high-priority admission.
    suspensions.extend(_maybe_preempt_for_high_priority())

    # Make at most one assignment per pool per tick to reduce contention and protect tail latency.
    for pool_id in range(s.executor.num_pools):
        a = _schedule_one_on_pool(pool_id, allow_batch=True)
        if a is not None:
            assignments.append(a)

    return suspensions, assignments
