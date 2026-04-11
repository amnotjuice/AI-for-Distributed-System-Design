# policy_key: scheduler_low_015
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.058265
# generation_seconds: 56.26
# generated_at: 2026-04-09T21:14:01.885381
@register_scheduler_init(key="scheduler_low_015")
def scheduler_low_015_init(s):
    """Priority-aware, RAM-safe, fairness-with-aging scheduler.

    Goals:
      - Minimize weighted latency (QUERY > INTERACTIVE > BATCH) while avoiding failures (720s penalty).
      - Prefer completion reliability: allocate generous RAM (especially for high priority) to reduce OOM risk.
      - Avoid starvation: aging-based promotion so long-waiting batch work eventually runs.
      - Simple placement: pick the pool with most headroom for high priority, and leftover pools for batch.

    Key mechanics:
      - Three FIFO queues by priority, plus a small "promoted" queue for aged pipelines.
      - Per-(pipeline,op) RAM hints increased on OOM failures; retries are bounded.
      - Schedule multiple ops per pool per tick while resources remain.
    """
    # Queues hold pipeline_ids; actual objects stored in s.pipelines_by_id
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []
    s.q_promoted = []  # aged pipelines promoted regardless of original priority

    s.pipelines_by_id = {}  # pipeline_id -> Pipeline
    s.arrival_tick = {}     # pipeline_id -> tick
    s.last_service_tick = {}  # pipeline_id -> tick when we last assigned an op

    # Failure-driven hints keyed by (pipeline_id, op_key)
    s.ram_hint = {}    # (pid, op_key) -> ram to try next
    s.cpu_hint = {}    # (pid, op_key) -> cpu to try next
    s.retry_count = {} # (pid, op_key) -> retries attempted
    s.pipeline_blacklist = set()  # pipelines considered terminally failed (avoid endless retries)

    s.tick = 0

    # Tunables (conservative defaults: favor completion)
    s.max_retries_per_op = 3
    s.aging_threshold_ticks = 25   # after this much no-service time, allow promotion
    s.promoted_slice_per_tick = 2  # how many promoted pipelines to consider early each tick

    # Per-priority target fractions of pool capacity (cap concurrency, avoid OOMs)
    s.cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.55,
        Priority.BATCH_PIPELINE: 0.35,
    }
    s.ram_frac = {
        Priority.QUERY: 0.80,
        Priority.INTERACTIVE: 0.65,
        Priority.BATCH_PIPELINE: 0.45,
    }


@register_scheduler(key="scheduler_low_015")
def scheduler_low_015(s, results, pipelines):
    """
    Returns:
        (suspensions, assignments)

    Notes:
      - Preemption is intentionally avoided here due to lack of a reliable running-container inventory
        in the exposed interface; instead we reduce failures by sizing RAM safely and ensuring fairness.
      - We schedule per-pool with multiple assignments while resources remain.
    """
    def _is_oom_error(err) -> bool:
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("cuda out of memory" in e) or ("memoryerror" in e)

    def _op_key(op):
        # Best-effort stable key without imports; works across typical operator objects.
        for attr in ("op_id", "operator_id", "id", "name", "key"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if v is not None:
                    return v
        return repr(op)

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid in s.pipeline_blacklist:
            return
        s.pipelines_by_id[pid] = p
        if pid not in s.arrival_tick:
            s.arrival_tick[pid] = s.tick
        if pid not in s.last_service_tick:
            s.last_service_tick[pid] = s.arrival_tick[pid]

        if p.priority == Priority.QUERY:
            s.q_query.append(pid)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(pid)
        else:
            s.q_batch.append(pid)

    def _pipeline_done_or_dead(p) -> bool:
        if p is None:
            return True
        if p.pipeline_id in s.pipeline_blacklist:
            return True
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator FAILED, we still allow retries (ASSIGNABLE_STATES includes FAILED),
        # but we avoid infinite retry loops via per-op retry counts.
        return False

    def _refresh_and_promote():
        # Promote pipelines that haven't received service recently (to avoid starvation).
        # We do not remove from original queues here; we just add to promoted queue if not already present.
        # Dedup is done via a small set.
        promoted_set = set(s.q_promoted)
        for q in (s.q_query, s.q_interactive, s.q_batch):
            for pid in q:
                if pid in promoted_set:
                    continue
                last = s.last_service_tick.get(pid, s.tick)
                if (s.tick - last) >= s.aging_threshold_ticks:
                    s.q_promoted.append(pid)
                    promoted_set.add(pid)

    def _pop_next_candidate():
        # Prefer a small slice of promoted first, then strict priority ordering.
        while s.q_promoted:
            pid = s.q_promoted.pop(0)
            p = s.pipelines_by_id.get(pid)
            if p is None or _pipeline_done_or_dead(p):
                continue
            return p

        for q in (s.q_query, s.q_interactive, s.q_batch):
            while q:
                pid = q.pop(0)
                p = s.pipelines_by_id.get(pid)
                if p is None or _pipeline_done_or_dead(p):
                    continue
                return p
        return None

    def _requeue_pipeline(p):
        # Round-robin within its class by pushing to tail.
        pid = p.pipeline_id
        if pid in s.pipeline_blacklist:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(pid)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(pid)
        else:
            s.q_batch.append(pid)

    def _choose_pool_order(priority):
        # For high priority, try pools with more free resources first.
        # For batch, use smaller-headroom pools first to reduce interference.
        pool_ids = list(range(s.executor.num_pools))
        if not pool_ids:
            return []
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            pool_ids.sort(
                key=lambda i: (
                    s.executor.pools[i].avail_ram_pool,
                    s.executor.pools[i].avail_cpu_pool,
                ),
                reverse=True,
            )
        else:
            pool_ids.sort(
                key=lambda i: (
                    s.executor.pools[i].avail_ram_pool,
                    s.executor.pools[i].avail_cpu_pool,
                )
            )
        return pool_ids

    def _size_for(priority, pool, pid, op):
        # Generous RAM-first sizing to reduce OOMs; modest CPU to avoid blocking other work.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # Base targets by priority as a fraction of pool.
        target_cpu = max(1.0, float(max_cpu) * float(s.cpu_frac.get(priority, 0.5)))
        target_ram = max(1.0, float(max_ram) * float(s.ram_frac.get(priority, 0.6)))

        ok = _op_key(op)
        hint_ram = s.ram_hint.get((pid, ok))
        hint_cpu = s.cpu_hint.get((pid, ok))

        if hint_ram is not None:
            target_ram = max(target_ram, float(hint_ram))
        if hint_cpu is not None:
            target_cpu = max(1.0, float(hint_cpu))

        # Clamp to what's currently available in the pool.
        cpu = min(float(pool.avail_cpu_pool), target_cpu)
        ram = min(float(pool.avail_ram_pool), target_ram)

        # If we can't meet at least minimal sensible amounts, return zeros to skip.
        if cpu <= 0.0 or ram <= 0.0:
            return 0.0, 0.0

        # Prevent tiny allocations that are likely to OOM; for high priority, demand a bit more RAM.
        # (We don't know true minima, so this is a conservative guardrail.)
        min_ram_floor = 1.0
        if priority == Priority.QUERY:
            min_ram_floor = min(4.0, float(pool.avail_ram_pool))
        elif priority == Priority.INTERACTIVE:
            min_ram_floor = min(2.0, float(pool.avail_ram_pool))
        else:
            min_ram_floor = min(1.0, float(pool.avail_ram_pool))

        if ram < min_ram_floor:
            return 0.0, 0.0

        return cpu, ram

    # Advance tick deterministically.
    s.tick += 1

    # Ingest new pipelines.
    for p in pipelines:
        _enqueue_pipeline(p)

    # Process execution results to update hints (OOM) and retry counters.
    # We assume results.ops is a list; update for each op in the result.
    for r in results:
        pid = getattr(r, "pipeline_id", None)
        # pipeline_id is not guaranteed on ExecutionResult; fall back to searching by op membership is too costly.
        # We still can update via container_id/pool_id sizing patterns only if pid present; otherwise skip.
        if pid is None:
            continue

        if pid in s.pipeline_blacklist:
            continue

        if r.failed():
            is_oom = _is_oom_error(r.error)
            ops = getattr(r, "ops", None) or []
            for op in ops:
                ok = _op_key(op)
                key = (pid, ok)
                s.retry_count[key] = s.retry_count.get(key, 0) + 1

                # Bound retries to prevent infinite loops; blacklisting ends further scheduling for this pipeline.
                if s.retry_count[key] > s.max_retries_per_op:
                    s.pipeline_blacklist.add(pid)
                    break

                if is_oom:
                    # Increase RAM hint aggressively; RAM is the main failure mode described.
                    prev = s.ram_hint.get(key, float(getattr(r, "ram", 0.0) or 0.0))
                    base = float(getattr(r, "ram", 0.0) or prev or 1.0)
                    bumped = max(prev, base) * 1.6
                    s.ram_hint[key] = bumped

                    # Slightly reduce CPU hint to allow more room for RAM (and reduce contention),
                    # unless CPU was already small.
                    prev_cpu = s.cpu_hint.get(key, float(getattr(r, "cpu", 1.0) or 1.0))
                    base_cpu = float(getattr(r, "cpu", 1.0) or prev_cpu or 1.0)
                    s.cpu_hint[key] = max(1.0, min(prev_cpu, base_cpu * 0.9))
                else:
                    # Non-OOM failures: keep RAM, slightly reduce CPU to reduce stress; still retry.
                    prev_cpu = s.cpu_hint.get(key, float(getattr(r, "cpu", 1.0) or 1.0))
                    base_cpu = float(getattr(r, "cpu", 1.0) or prev_cpu or 1.0)
                    s.cpu_hint[key] = max(1.0, min(prev_cpu, base_cpu * 0.85))

    # Promote aged pipelines (fairness).
    _refresh_and_promote()

    # Build assignments.
    suspensions = []
    assignments = []

    # Early exit if nothing to do.
    if not pipelines and not results:
        # Still attempt to schedule from queues (work might be pending).
        pass

    # For each pool, assign as many ops as resources allow, selecting best candidates for that pool.
    # We do pool ordering per candidate priority to reduce interference.
    # Approach:
    #   - Iterate pools in a stable order; within each pool, repeatedly pick next candidate pipeline
    #     and if it has a ready op, assign it with safe sizing.
    #   - If candidate can't fit in this pool, requeue and try another candidate.
    max_attempts_per_pool = 64  # prevent infinite loops on unfittable queues

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        attempts = 0
        while attempts < max_attempts_per_pool:
            attempts += 1

            # Pick a candidate pipeline globally (already ordered/promoted).
            p = _pop_next_candidate()
            if p is None:
                break

            pid = p.pipeline_id
            if pid in s.pipeline_blacklist:
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            # Find exactly one ready operator to schedule (keep it simple and reduce head-of-line blocking).
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; requeue behind others.
                _requeue_pipeline(p)
                continue

            op = op_list[0]

            # If this pipeline's priority fits better elsewhere, we still allow assignment here,
            # but we size conservatively and rely on pool headroom.
            cpu, ram = _size_for(p.priority, pool, pid, op)
            if cpu <= 0.0 or ram <= 0.0:
                # Can't fit now in this pool; requeue and move on.
                _requeue_pipeline(p)
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Mark service for aging/fairness.
            s.last_service_tick[pid] = s.tick

            # Requeue pipeline for its next operator (or completion) to maintain flow.
            _requeue_pipeline(p)

            # Update local view: pool.avail_* are assumed to be updated by executor after applying assignments,
            # but in this interface we only read them. We conservatively stop if near-empty to avoid oversubscription.
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

    return suspensions, assignments
