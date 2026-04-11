# policy_key: scheduler_none_004
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.056613
# generation_seconds: 40.69
# generated_at: 2026-04-09T21:47:54.034209
@register_scheduler_init(key="scheduler_none_004")
def scheduler_none_004_init(s):
    """Priority-aware, OOM-adaptive, anti-starvation scheduler.

    Goals aligned to weighted latency objective:
      - Strongly protect QUERY/INTERACTIVE by (a) preferential admission and (b) preempting BATCH when needed.
      - Reduce failure penalty by learning per-operator RAM minima from OOMs and retrying with increased RAM.
      - Avoid starvation by aging: if a pipeline waits long enough, it gains effective priority for admission.
      - Keep it simple: one op per pipeline at a time, small CPU shares to pack more high-priority work.
    """
    # Queue of pipelines we know about (may reappear across ticks).
    s.waiting_queue = []

    # Operator-level learned RAM floors: (pipeline_id, op_id) -> ram_floor
    s.op_ram_floor = {}

    # Pipeline wait accounting to implement aging.
    # pipeline_id -> ticks waited since last progress (completion of any op)
    s.pipeline_wait_ticks = {}

    # Pipeline last completed op count to detect progress.
    s.pipeline_progress_count = {}

    # Remember arrivals by pipeline_id (for stable ordering / accounting).
    s.known_pipeline_ids = set()

    # Simple tick counter for aging.
    s.tick = 0

    # Tunables (kept conservative to avoid thrash).
    s.aging_tick_threshold = 30          # after this, pipeline gets a boost
    s.max_preemptions_per_tick = 2       # avoid excessive churn
    s.max_assignments_per_pool = 4       # keep concurrency moderate per pool
    s.cpu_share_query = 0.60
    s.cpu_share_interactive = 0.50
    s.cpu_share_batch = 0.40
    s.min_cpu_absolute = 0.25            # don't allocate too tiny cpu
    s.ram_headroom_factor = 1.15         # slight headroom over learned floor
    s.ram_backoff_factor = 1.50          # on OOM, increase learned floor
    s.ram_backoff_add = 0.5              # and add a small constant (GB-like units)
    s.preempt_priority_floor = Priority.BATCH_PIPELINE  # only preempt batch by default


def _policy_none_004_priority_rank(priority):
    # Lower rank value = more important
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _policy_none_004_cpu_share(s, priority):
    if priority == Priority.QUERY:
        return s.cpu_share_query
    if priority == Priority.INTERACTIVE:
        return s.cpu_share_interactive
    return s.cpu_share_batch


def _policy_none_004_effective_rank(s, pipeline):
    """Combine base priority with aging to prevent starvation."""
    base = _policy_none_004_priority_rank(pipeline.priority)
    waited = s.pipeline_wait_ticks.get(pipeline.pipeline_id, 0)
    # If waited long, boost by one rank (cannot exceed best rank 0).
    if waited >= s.aging_tick_threshold:
        return max(0, base - 1)
    return base


def _policy_none_004_op_id(op):
    # Try multiple common id attributes; fallback to Python object id for stability within a run.
    return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "id", None) or id(op)


def _policy_none_004_estimate_op_ram_floor(s, pipeline, op, pool):
    """Return RAM to allocate for this op, using learned floor and pool cap."""
    pid = pipeline.pipeline_id
    oid = _policy_none_004_op_id(op)
    key = (pid, oid)

    learned = s.op_ram_floor.get(key, None)

    # If operator exposes a minimum RAM, use it as starting point; else start small but safe.
    op_min = getattr(op, "min_ram", None)
    if op_min is None:
        op_min = getattr(op, "ram_min", None)
    if op_min is None:
        op_min = 1.0  # conservative default unit

    floor = max(op_min, learned) if learned is not None else op_min
    alloc = floor * s.ram_headroom_factor

    # Ensure alloc doesn't exceed pool max (admission control will handle if still too large).
    return min(alloc, pool.max_ram_pool)


def _policy_none_004_choose_pool_for_op(s, pipeline, op):
    """Pick the best pool to run this op: prefer headroom and a light bias for higher priority."""
    best = None
    best_score = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        # If nothing available, skip.
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        ram_need = _policy_none_004_estimate_op_ram_floor(s, pipeline, op, pool)
        if ram_need > pool.max_ram_pool:
            continue

        # Score: prefer pools where the op fits now, else where it will fit soon (more headroom).
        # Higher priority gets a mild preference to larger headroom pools.
        fits_now = (ram_need <= pool.avail_ram_pool)
        headroom_ram = pool.avail_ram_pool - ram_need
        headroom_cpu = pool.avail_cpu_pool

        pr = _policy_none_004_priority_rank(pipeline.priority)
        # Priority bias: query prefers headroom more strongly.
        pr_bias = 1.0 + (2 - pr) * 0.15

        score = 0.0
        score += (1.0 if fits_now else 0.0) * 1000.0
        score += pr_bias * headroom_ram * 10.0
        score += pr_bias * headroom_cpu * 1.0

        if best is None or score > best_score:
            best = pool_id
            best_score = score

    return best


def _policy_none_004_collect_running_containers(results):
    """Best-effort inventory of running containers based on result events."""
    # We don't have direct executor API here; keep it minimal and safe.
    running = []
    for r in results:
        # Some simulators emit periodic running updates; treat any non-terminal with container_id as running.
        if getattr(r, "container_id", None) is None:
            continue
        if not r.failed() and getattr(r, "error", None) is None:
            # Could be completion too; we can't know. Keep but will be used conservatively.
            running.append(r)
    return running


@register_scheduler(key="scheduler_none_004")
def scheduler_none_004_scheduler(s, results, pipelines):
    """
    Scheduler policy:
      1) Enqueue new pipelines; update per-pipeline waiting/progress ticks.
      2) Learn from failures: on OOM-like failure, increase learned RAM floor for the failed op.
      3) Preempt BATCH work if a QUERY/INTERACTIVE op is ready but cannot fit due to lack of RAM.
      4) Assign ready ops in order of effective priority (with aging), one op per pipeline, using
         modest CPU shares to improve concurrency and reduce tail latency for high-priority work.
    """
    s.tick += 1

    # Add new pipelines to queue (dedupe by pipeline_id).
    for p in pipelines:
        if p.pipeline_id not in s.known_pipeline_ids:
            s.known_pipeline_ids.add(p.pipeline_id)
            s.waiting_queue.append(p)
            s.pipeline_wait_ticks[p.pipeline_id] = 0
            s.pipeline_progress_count[p.pipeline_id] = 0
        else:
            # If re-sent, ensure it's in queue.
            if p not in s.waiting_queue:
                s.waiting_queue.append(p)

    # Early exit if no changes.
    if not pipelines and not results:
        return [], []

    # Update learning/progress from results.
    # - If any op completes successfully, reset wait tick (progress made).
    # - If failed and looks like OOM, increase learned RAM for that op and let it retry (via ASSIGNABLE_STATES).
    for r in results:
        pid = getattr(r, "pipeline_id", None)
        # pipeline_id is not guaranteed in ExecutionResult per prompt; infer from ops if possible.
        # We'll instead update progress conservatively using ops' pipeline reference if present.
        ops = getattr(r, "ops", None) or []
        for op in ops:
            pipeline = getattr(op, "pipeline", None)
            if pipeline is not None:
                pid = pipeline.pipeline_id

            if pid is not None:
                # Any terminal event indicates some activity; reset wait a bit.
                s.pipeline_wait_ticks[pid] = 0

            if r.failed():
                err = str(getattr(r, "error", "") or "").lower()
                # Heuristic detection of OOM-like failure.
                is_oom = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "killed" in err)
                if is_oom:
                    # Learn RAM floor from the attempted RAM, then increase.
                    op_id = _policy_none_004_op_id(op)
                    key = (pid, op_id) if pid is not None else None
                    if key is not None:
                        attempted = getattr(r, "ram", None)
                        if attempted is None:
                            attempted = s.op_ram_floor.get(key, 1.0)
                        new_floor = max(s.op_ram_floor.get(key, 0.0), attempted) * s.ram_backoff_factor + s.ram_backoff_add
                        s.op_ram_floor[key] = new_floor
            else:
                # Non-failure event: treat as progress (best-effort).
                if pid is not None:
                    s.pipeline_progress_count[pid] = s.pipeline_progress_count.get(pid, 0) + 1

    # Increment wait ticks for pipelines that are not making progress and not completed.
    for p in list(s.waiting_queue):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        s.pipeline_wait_ticks[p.pipeline_id] = s.pipeline_wait_ticks.get(p.pipeline_id, 0) + 1

    # Helper to get next runnable op per pipeline.
    def next_ready_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return None
        # If pipeline has any failures, we still allow retry because ASSIGNABLE_STATES includes FAILED.
        ops_ready = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops_ready:
            return None
        return ops_ready[0]

    # Build candidates: one ready op per pipeline.
    candidates = []
    kept = []
    for p in s.waiting_queue:
        st = p.runtime_status()
        # Drop completed pipelines from our queue to reduce work.
        if st.is_pipeline_successful():
            continue

        # If there are failures, we still keep the pipeline (avoid 720s penalty from dropping).
        op = next_ready_op(p)
        kept.append(p)
        if op is not None:
            candidates.append((p, op))

    s.waiting_queue = kept

    # Sort by effective rank (priority + aging), then by waited ticks (desc) to break ties.
    candidates.sort(key=lambda x: (_policy_none_004_effective_rank(s, x[0]), -s.pipeline_wait_ticks.get(x[0].pipeline_id, 0)))

    suspensions = []
    assignments = []

    # Preemption step:
    # If we have high-priority work ready but no pool can fit its RAM now, preempt batch containers (limited).
    # We can only act with Suspend(container_id, pool_id) and don't have full running set;
    # still, use best-effort from recent results if present.
    preemptions_left = s.max_preemptions_per_tick
    if candidates and preemptions_left > 0:
        high_needed = [c for c in candidates if c[0].priority in (Priority.QUERY, Priority.INTERACTIVE)]
        # Try preemption only if any high-priority candidate cannot fit anywhere now due to RAM.
        for p, op in high_needed:
            can_fit_somewhere_now = False
            for pool_id in range(s.executor.num_pools):
                pool = s.executor.pools[pool_id]
                if pool.avail_ram_pool <= 0 or pool.avail_cpu_pool <= 0:
                    continue
                ram_need = _policy_none_004_estimate_op_ram_floor(s, p, op, pool)
                if ram_need <= pool.avail_ram_pool and ram_need <= pool.max_ram_pool:
                    can_fit_somewhere_now = True
                    break

            if can_fit_somewhere_now:
                continue

            # If cannot fit now, preempt some batch containers if we can identify them.
            running = _policy_none_004_collect_running_containers(results)
            # Prefer suspending batch first.
            running.sort(key=lambda r: _policy_none_004_priority_rank(getattr(r, "priority", Priority.BATCH_PIPELINE)), reverse=True)

            for r in running:
                if preemptions_left <= 0:
                    break
                r_pr = getattr(r, "priority", Priority.BATCH_PIPELINE)
                if _policy_none_004_priority_rank(r_pr) < _policy_none_004_priority_rank(s.preempt_priority_floor):
                    # Don't preempt query/interactive by default.
                    continue
                cid = getattr(r, "container_id", None)
                pool_id = getattr(r, "pool_id", None)
                if cid is None or pool_id is None:
                    continue
                suspensions.append(Suspend(cid, pool_id))
                preemptions_left -= 1

            # Only attempt once per tick to avoid thrash.
            break

    # Assignment step: try to place candidates, limiting assignments per pool.
    per_pool_assigned = {pid: 0 for pid in range(s.executor.num_pools)}

    for p, op in candidates:
        # Skip if we already scheduled this many tasks overall in this tick (soft cap via per-pool caps).
        chosen_pool_id = _policy_none_004_choose_pool_for_op(s, p, op)
        if chosen_pool_id is None:
            continue

        pool = s.executor.pools[chosen_pool_id]
        if per_pool_assigned[chosen_pool_id] >= s.max_assignments_per_pool:
            continue

        # Determine RAM allocation using learned floor.
        ram_alloc = _policy_none_004_estimate_op_ram_floor(s, p, op, pool)
        if ram_alloc > pool.avail_ram_pool:
            continue

        # Determine CPU allocation: share of available CPU to allow concurrency, with minimum.
        cpu_share = _policy_none_004_cpu_share(s, p.priority)
        cpu_alloc = max(s.min_cpu_absolute, pool.avail_cpu_pool * cpu_share)
        cpu_alloc = min(cpu_alloc, pool.avail_cpu_pool)

        # If CPU is extremely scarce, skip.
        if cpu_alloc <= 0:
            continue

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu_alloc,
                ram=ram_alloc,
                priority=p.priority,
                pool_id=chosen_pool_id,
                pipeline_id=p.pipeline_id,
            )
        )
        per_pool_assigned[chosen_pool_id] += 1

        # Reset wait ticks on admission to avoid repeated aging boosts while running.
        s.pipeline_wait_ticks[p.pipeline_id] = 0

    return suspensions, assignments
