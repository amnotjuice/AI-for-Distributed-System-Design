# policy_key: scheduler_medium_036
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.096289
# generation_seconds: 89.15
# generated_at: 2026-04-09T23:18:25.188050
@register_scheduler_init(key="scheduler_medium_036")
def scheduler_medium_036_init(s):
    """
    Priority-aware, OOM-averse round-robin scheduler with conservative sizing + OOM feedback.

    Goals aligned to the weighted-latency objective:
      - Protect QUERY and INTERACTIVE by giving them first access to a "high" pool (pool 0) when possible.
      - Reduce 720s penalties by (a) avoiding knowingly-underprovisioned RAM and (b) aggressively retrying
        OOM failures with increased RAM estimates.
      - Avoid starvation via round-robin within each priority + limited "aging" that lets old batch work run
        when high-priority demand is low.
    """
    # Global tick for lightweight aging.
    s._tick = 0

    # Per-priority FIFO queues of Pipeline objects.
    s._queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track pipelines we've seen and where they live.
    s._pipelines_by_id = {}   # pipeline_id -> Pipeline
    s._pipeline_prio = {}     # pipeline_id -> Priority
    s._enqueued_tick = {}     # pipeline_id -> tick first seen
    s._in_queue = set()       # pipeline_ids currently tracked

    # Boost set: pipelines that just had an OOM should be retried promptly (after increasing RAM).
    s._boost = set()          # pipeline_ids

    # Operator-level resource estimates keyed by id(op) (object identity).
    s._op_ram_est = {}        # id(op) -> ram_estimate
    s._op_cpu_est = {}        # id(op) -> cpu_estimate (lightly used)
    s._op_to_pipeline = {}    # id(op) -> pipeline_id

    # Policy knobs (kept simple and robust).
    s._batch_age_soft = 80      # after this many ticks, allow batch to use high pool if idle
    s._batch_age_hard = 200     # after this many ticks, batch may use high pool even with light interactive load
    s._max_scan_factor = 2      # scanning bound to avoid infinite loops


def _bp036_is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _bp036_initial_ram(pool, priority):
    # RAM over-allocation hurts throughput, but OOMs are catastrophic for the objective.
    # Be conservative for QUERY/INTERACTIVE, more modest for BATCH.
    if priority == Priority.QUERY:
        frac = 0.35
    elif priority == Priority.INTERACTIVE:
        frac = 0.25
    else:
        frac = 0.15
    # Avoid tiny allocations; keep units generic (sim uses consistent units).
    return max(1.0, pool.max_ram_pool * frac)


def _bp036_initial_cpu(pool, priority):
    # CPU has sublinear scaling. Give enough to keep latency down for high priority,
    # but avoid grabbing the entire pool by default.
    if priority == Priority.QUERY:
        frac = 0.50
    elif priority == Priority.INTERACTIVE:
        frac = 0.33
    else:
        frac = 0.25
    return max(1.0, pool.max_cpu_pool * frac)


def _bp036_pick_pool_ids(s, priority):
    # Placement: when multiple pools exist, treat pool 0 as "high" for QUERY/INTERACTIVE.
    # Batch prefers non-zero pools to reduce interference.
    n = s.executor.num_pools
    if n <= 1:
        return [0]
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        return [0] + list(range(1, n))
    # Batch: try non-high pools first.
    return list(range(1, n)) + [0]


def _bp036_remove_pipeline_from_queue(s, pipeline_id):
    pr = s._pipeline_prio.get(pipeline_id, None)
    if pr is None:
        return
    q = s._queues[pr]
    # Remove all occurrences defensively (should be at most one).
    s._queues[pr] = [p for p in q if p.pipeline_id != pipeline_id]
    s._in_queue.discard(pipeline_id)
    s._boost.discard(pipeline_id)
    s._pipelines_by_id.pop(pipeline_id, None)
    s._pipeline_prio.pop(pipeline_id, None)
    s._enqueued_tick.pop(pipeline_id, None)


def _bp036_queue_pipeline(s, pipeline):
    pid = pipeline.pipeline_id
    if pid in s._in_queue:
        # Update pointer, but don't duplicate queue entries.
        s._pipelines_by_id[pid] = pipeline
        return
    s._in_queue.add(pid)
    s._pipelines_by_id[pid] = pipeline
    s._pipeline_prio[pid] = pipeline.priority
    s._enqueued_tick.setdefault(pid, s._tick)
    s._queues[pipeline.priority].append(pipeline)


def _bp036_try_pop_boosted(s, priority):
    # If any boosted pipeline exists in this priority, pull it to the front.
    q = s._queues[priority]
    if not q:
        return None
    for i, p in enumerate(q):
        if p.pipeline_id in s._boost:
            q.pop(i)
            return p
    return None


def _bp036_should_allow_batch_on_high_pool(s):
    # Let batch use high pool only when high-priority demand is low, or when batch has aged significantly.
    if s._queues[Priority.QUERY] or s._queues[Priority.INTERACTIVE]:
        # If batch is very old, allow it occasionally to prevent starvation/incompletion.
        oldest = None
        for p in s._queues[Priority.BATCH_PIPELINE]:
            t0 = s._enqueued_tick.get(p.pipeline_id, s._tick)
            age = s._tick - t0
            if oldest is None or age > oldest:
                oldest = age
        return (oldest is not None) and (oldest >= s._batch_age_hard)
    # No high-priority waiting -> batch can use high pool freely.
    return True


@register_scheduler(key="scheduler_medium_036")
def scheduler_medium_036(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue new pipelines by priority (no duplication).
      2) Process results to update RAM estimates; OOM => increase RAM estimate and boost pipeline for retry.
      3) For each pool, schedule as many ready ops as fit, prioritizing QUERY > INTERACTIVE > BATCH.
         - pool 0 is treated as a "high" pool when multiple pools exist.
         - batch mostly stays off pool 0 unless high-priority queues are empty or batch has aged heavily.
      4) Never intentionally schedule an op with RAM below our current estimate (OOM-averse).
    """
    s._tick += 1

    # Enqueue arrivals.
    for p in pipelines:
        _bp036_queue_pipeline(s, p)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # Process results: update resource estimates and boosting behavior.
    # We don't drop pipelines on failures; we rely on retrying FAILED ops (ASSIGNABLE_STATES includes FAILED).
    for r in results:
        if not getattr(r, "ops", None):
            continue

        pool = s.executor.pools[r.pool_id]
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = False

        # For each op, update estimates and mark its pipeline for boost if needed.
        for op in r.ops:
            op_id = id(op)
            pid = s._op_to_pipeline.get(op_id, None)

            if failed:
                # Increase RAM estimate aggressively on OOM. This is the main lever to avoid 720s penalties.
                prev = s._op_ram_est.get(op_id, max(1.0, float(getattr(r, "ram", 0) or 0)))
                if prev <= 0:
                    prev = _bp036_initial_ram(pool, getattr(r, "priority", Priority.BATCH_PIPELINE))

                if _bp036_is_oom_error(getattr(r, "error", None)):
                    new_est = max(prev * 2.0, float(getattr(r, "ram", 0) or 0) * 1.5, prev + 1.0)
                else:
                    # Unknown failure: small bump, keep retrying rather than giving up.
                    new_est = max(prev * 1.25, prev + 1.0)

                # Cap at pool max to avoid impossible sizes.
                new_est = min(new_est, float(pool.max_ram_pool))
                s._op_ram_est[op_id] = new_est

                # Boost the pipeline to retry quickly (reduces tail latency after OOM).
                if pid is not None:
                    s._boost.add(pid)
            else:
                # On success: keep RAM estimate (don't reduce to avoid oscillations).
                # Optionally store a reasonable CPU hint.
                cpu_used = float(getattr(r, "cpu", 0) or 0)
                if cpu_used > 0:
                    s._op_cpu_est[op_id] = max(s._op_cpu_est.get(op_id, 1.0), cpu_used)

    suspensions = []
    assignments = []

    # Utility: check if pipeline is done; also clean up finished pipelines.
    def pipeline_still_active(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            _bp036_remove_pipeline_from_queue(s, p.pipeline_id)
            return False
        return True

    # Attempt scheduling per pool with local accounting to reflect new assignments this tick.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Not enough to run anything.
        if avail_cpu < 1.0 or avail_ram <= 0.0:
            continue

        # Determine which priorities to consider for this pool.
        high_pool = (s.executor.num_pools > 1 and pool_id == 0)
        if high_pool:
            prios = [Priority.QUERY, Priority.INTERACTIVE]
            if _bp036_should_allow_batch_on_high_pool(s):
                prios.append(Priority.BATCH_PIPELINE)
        else:
            # Non-high pools: primarily batch; allow interactive spillover only if batch is empty.
            prios = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]

        made_progress = True
        # Keep scheduling while we can fit at least one container.
        while made_progress:
            made_progress = False

            # Hard stop if pool is too fragmented.
            if avail_cpu < 1.0 or avail_ram <= 0.0:
                break

            scheduled_one = False

            for pr in prios:
                q = s._queues[pr]
                if not q:
                    continue

                # Scan bounded number of pipelines to find a runnable op that fits.
                max_scans = max(1, len(q) * s._max_scan_factor)
                scans = 0

                while scans < max_scans and q and avail_cpu >= 1.0 and avail_ram > 0.0:
                    scans += 1

                    # Prefer boosted pipelines for this priority.
                    p = _bp036_try_pop_boosted(s, pr)
                    if p is None:
                        p = q.pop(0)

                    # Pipeline might be stale (completed) or object reference updated.
                    pid = p.pipeline_id
                    p = s._pipelines_by_id.get(pid, p)

                    if not pipeline_still_active(p):
                        continue

                    st = p.runtime_status()
                    has_failures = st.state_counts[OperatorState.FAILED] > 0
                    # We do retry FAILED ops; do not drop the pipeline.
                    # If parents aren't complete, we cannot schedule now.
                    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not ops:
                        # Requeue and try another pipeline.
                        q.append(p)
                        continue

                    op = ops[0]
                    op_id = id(op)
                    s._op_to_pipeline[op_id] = pid

                    # Compute target sizes.
                    base_ram = _bp036_initial_ram(pool, p.priority)
                    ram_need = float(s._op_ram_est.get(op_id, base_ram))
                    ram_need = min(ram_need, float(pool.max_ram_pool))

                    base_cpu = _bp036_initial_cpu(pool, p.priority)
                    cpu_hint = float(s._op_cpu_est.get(op_id, base_cpu))
                    cpu_need = max(1.0, min(cpu_hint, float(pool.max_cpu_pool)))

                    # Fit-to-available CPU (safe). But do not undercut RAM estimate (OOM-averse).
                    cpu_assign = min(cpu_need, avail_cpu)
                    if cpu_assign < 1.0:
                        q.append(p)
                        continue

                    if ram_need > avail_ram:
                        # Not enough RAM to safely run this op. Requeue and move on.
                        q.append(p)
                        continue

                    # Clear boost once we actually schedule something from the boosted pipeline.
                    s._boost.discard(pid)

                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu_assign,
                            ram=ram_need,
                            priority=p.priority,
                            pool_id=pool_id,
                            pipeline_id=pid,
                        )
                    )

                    # Local accounting.
                    avail_cpu -= cpu_assign
                    avail_ram -= ram_need

                    # Keep pipeline in queue for subsequent ops (round-robin).
                    q.append(p)

                    scheduled_one = True
                    made_progress = True
                    break  # move to outer loop, re-evaluate priorities with updated resources

                if scheduled_one:
                    break  # proceed to next scheduling opportunity

    return suspensions, assignments
