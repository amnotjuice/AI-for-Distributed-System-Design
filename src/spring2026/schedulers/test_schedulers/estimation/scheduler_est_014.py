# policy_key: scheduler_est_014
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.060746
# generation_seconds: 49.11
# generated_at: 2026-04-10T07:58:00.445784
@register_scheduler_init(key="scheduler_est_014")
def scheduler_est_014_init(s):
    """Memory-aware, priority-weighted deficit scheduler.

    Goals:
      - Minimize weighted latency (query >> interactive >> batch) while avoiding failures (720s penalty).
      - Use op.estimate.mem_peak_gb (noisy) to reduce OOM by only placing where it likely fits, and by
        sizing RAM with a safety margin + exponential backoff on failures.
      - Maintain progress for lower-priority pipelines using deficit round-robin (prevents starvation).
    """
    # Per-priority waiting queues (pipelines can appear multiple times over time; we dedupe via active set)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track pipelines we've seen but not terminal yet
    s.active_pipeline_ids = set()

    # Deficit counters for weighted fair scheduling across priorities
    s.deficit = {
        Priority.QUERY: 0.0,
        Priority.INTERACTIVE: 0.0,
        Priority.BATCH_PIPELINE: 0.0,
    }
    s.quantum = {
        Priority.QUERY: 10.0,
        Priority.INTERACTIVE: 5.0,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # Failure-driven RAM backoff per operator (keyed by id(op))
    # multiplier >= 1.0; doubles on failure (esp. OOM) up to a cap
    s.op_ram_mult = {}

    # Conservative defaults / caps
    s.safety_margin = 1.25          # multiply estimator by this to reduce OOM risk
    s.min_ram_gb_default = 0.5      # if no estimate, start small to avoid hoarding
    s.max_backoff_mult = 8.0        # cap RAM backoff multiplier


def _priority_of(pipeline):
    return pipeline.priority


def _enqueue_pipeline(s, pipeline):
    pid = pipeline.pipeline_id
    if pid not in s.active_pipeline_ids:
        s.active_pipeline_ids.add(pid)
    pr = _priority_of(pipeline)
    if pr == Priority.QUERY:
        s.q_query.append(pipeline)
    elif pr == Priority.INTERACTIVE:
        s.q_interactive.append(pipeline)
    else:
        s.q_batch.append(pipeline)


def _is_terminal(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If any operator failed, we treat the pipeline as terminal (sim baseline also does this).
    # This avoids repeated retries that could increase contention and overall latency.
    return status.state_counts[OperatorState.FAILED] > 0


def _get_next_assignable_op(pipeline):
    status = pipeline.runtime_status()
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[0]


def _est_mem_gb(op):
    est = getattr(op, "estimate", None)
    if est is None:
        return None
    return getattr(est, "mem_peak_gb", None)


def _largest_pool_id_by_ram(s):
    best = 0
    best_ram = -1
    for i in range(s.executor.num_pools):
        r = s.executor.pools[i].max_ram_pool
        if r > best_ram:
            best_ram = r
            best = i
    return best


def _choose_pool_for_op(s, op, priority, require_fit=True):
    """Pick a pool for this operator based on estimated memory fit and headroom.

    If require_fit=True and estimate exists, only consider pools where estimate fits in *available* RAM.
    Otherwise, fall back to considering max RAM pools (last resort).
    """
    est = _est_mem_gb(op)
    best_pool = None
    best_score = None

    # Heuristic: prefer pools with enough available RAM after placing this op,
    # and with enough CPU to run it quickly (esp. for high priority).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        if est is not None and require_fit:
            if est * s.safety_margin > avail_ram:
                continue

        # Score: higher is better
        # - Headroom RAM after placement (approx) dominates to reduce OOM cascades
        # - CPU availability adds a smaller bonus; scaled by priority importance
        est_for_score = est if est is not None else min(avail_ram, s.min_ram_gb_default)
        ram_after = avail_ram - min(avail_ram, est_for_score * s.safety_margin)
        pr_bonus = 3.0 if priority == Priority.QUERY else (2.0 if priority == Priority.INTERACTIVE else 1.0)
        score = (ram_after * 10.0) + (avail_cpu * pr_bonus)

        if best_score is None or score > best_score:
            best_score = score
            best_pool = pool_id

    if best_pool is not None:
        return best_pool

    # If we couldn't find a fit (common when estimator is large), return None to indicate waiting,
    # unless we want to force placement (caller can decide).
    return None


def _ram_request_gb(s, op, pool):
    """Decide RAM allocation for an operator in a specific pool."""
    est = _est_mem_gb(op)
    mult = s.op_ram_mult.get(id(op), 1.0)

    # If no estimate, start small; if estimate exists, allocate with safety margin.
    if est is None:
        req = s.min_ram_gb_default * mult
    else:
        req = est * s.safety_margin * mult

    # Clamp to pool's available RAM (never request more than available at assignment time)
    req = min(req, pool.avail_ram_pool)

    # Ensure a tiny positive request to avoid zero-ram corner cases
    if req <= 0:
        req = min(pool.avail_ram_pool, 0.1)
    return req


def _cpu_request(s, priority, pool):
    """Allocate CPU; give more to query/interactive to reduce latency."""
    avail = pool.avail_cpu_pool
    if avail <= 0:
        return 0

    # Favor latency for high priority, but avoid draining all CPU for one op if possible.
    # (Many operators sublinearly scale; spreading can improve completion rate.)
    if priority == Priority.QUERY:
        frac = 0.75
    elif priority == Priority.INTERACTIVE:
        frac = 0.60
    else:
        frac = 0.40

    cpu = max(1.0, avail * frac)
    cpu = min(cpu, avail)
    return cpu


def _pop_next_pipeline_weighted(s):
    """Deficit round-robin selection across priority queues."""
    # Add quantum each time we attempt selection (acts like a "tick" in fairness space).
    for pr, q in s.quantum.items():
        s.deficit[pr] += q

    def q_nonempty(pr):
        if pr == Priority.QUERY:
            return len(s.q_query) > 0
        if pr == Priority.INTERACTIVE:
            return len(s.q_interactive) > 0
        return len(s.q_batch) > 0

    # Try to pick the highest deficit among non-empty queues.
    candidates = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    candidates = [pr for pr in candidates if q_nonempty(pr)]
    if not candidates:
        return None

    pr_pick = max(candidates, key=lambda pr: s.deficit[pr])

    # Consume one "unit" of deficit for a scheduled operator
    s.deficit[pr_pick] -= 1.0

    if pr_pick == Priority.QUERY:
        return s.q_query.pop(0)
    if pr_pick == Priority.INTERACTIVE:
        return s.q_interactive.pop(0)
    return s.q_batch.pop(0)


def _requeue_pipeline(s, pipeline):
    """Requeue pipeline at the tail of its priority queue."""
    pr = _priority_of(pipeline)
    if pr == Priority.QUERY:
        s.q_query.append(pipeline)
    elif pr == Priority.INTERACTIVE:
        s.q_interactive.append(pipeline)
    else:
        s.q_batch.append(pipeline)


@register_scheduler(key="scheduler_est_014")
def scheduler_est_014(s, results, pipelines):
    """
    Scheduling steps:
      1) Ingest new pipelines into priority queues.
      2) Process results: on failures, increase RAM multiplier for those operators (backoff).
      3) For each pool, repeatedly schedule one assignable operator at a time while resources remain:
         - Choose next pipeline via weighted deficit scheduling (query-heavy but starvation-safe).
         - Choose operator (first assignable with parents complete).
         - Use estimator to pick a pool where it fits; size RAM with safety margin and backoff.
         - Allocate CPU fraction based on priority to reduce tail latency.
    """
    # Ingest arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update backoff on failures
    for r in results:
        if hasattr(r, "failed") and r.failed():
            # Increase RAM multiplier for all involved ops; this is a blunt but effective anti-OOM tactic.
            for op in getattr(r, "ops", []) or []:
                key = id(op)
                cur = s.op_ram_mult.get(key, 1.0)
                s.op_ram_mult[key] = min(s.max_backoff_mult, cur * 2.0)

    # Early exit if nothing to do
    if not pipelines and not results and (len(s.q_query) + len(s.q_interactive) + len(s.q_batch) == 0):
        return [], []

    suspensions = []
    assignments = []

    # Clean up terminal pipelines lazily (we drop them when they surface)
    # Scheduling loop: pack each pool with at most a few assignments per tick to avoid over-committing.
    max_assignments_per_pool = 4

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        made = 0
        # Attempt multiple assignments in this pool, but stop if queues are empty or resources too tight.
        while made < max_assignments_per_pool:
            pipeline = _pop_next_pipeline_weighted(s)
            if pipeline is None:
                break

            # Skip terminal pipelines; they should not be rescheduled.
            if _is_terminal(pipeline):
                # If terminal, forget it entirely.
                if pipeline.pipeline_id in s.active_pipeline_ids:
                    s.active_pipeline_ids.discard(pipeline.pipeline_id)
                continue

            op = _get_next_assignable_op(pipeline)
            if op is None:
                # Not ready yet; put it back.
                _requeue_pipeline(s, pipeline)
                break

            pr = _priority_of(pipeline)

            # Pool choice:
            # - Prefer a pool where estimated mem fits now; otherwise requeue to wait.
            # - If operator estimate is enormous, try the largest pool as a last resort to avoid indefinite waiting.
            chosen_pool = _choose_pool_for_op(s, op, pr, require_fit=True)
            if chosen_pool is None:
                est = _est_mem_gb(op)
                if est is not None:
                    # If it can never fit any pool max RAM, try the largest pool (best-effort).
                    largest = _largest_pool_id_by_ram(s)
                    if est * s.safety_margin > s.executor.pools[largest].max_ram_pool:
                        chosen_pool = largest
                    else:
                        # It might fit later; requeue and let other work proceed.
                        _requeue_pipeline(s, pipeline)
                        break
                else:
                    # No estimate: allow placement in current pool only if it has some RAM.
                    chosen_pool = pool_id

            # If chosen_pool differs from current pool iteration, requeue and let that pool handle it later.
            if chosen_pool != pool_id:
                _requeue_pipeline(s, pipeline)
                # Avoid spinning on the same pipeline; continue to pick another one for this pool.
                continue

            # Re-check available resources in this pool (they may have changed within the loop).
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                _requeue_pipeline(s, pipeline)
                break

            ram_req = _ram_request_gb(s, op, pool)
            cpu_req = _cpu_request(s, pr, pool)

            if cpu_req <= 0 or ram_req <= 0:
                _requeue_pipeline(s, pipeline)
                break

            # If estimate exists and even our request cannot cover it (due to low avail RAM), wait.
            est = _est_mem_gb(op)
            if est is not None and est * s.safety_margin > pool.avail_ram_pool:
                _requeue_pipeline(s, pipeline)
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pr,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )
            made += 1

            # Put pipeline back to allow its next ops to be scheduled later
            _requeue_pipeline(s, pipeline)

    return suspensions, assignments
