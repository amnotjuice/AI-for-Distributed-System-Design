# policy_key: scheduler_none_008
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048661
# generation_seconds: 55.70
# generated_at: 2026-04-09T21:51:14.734406
@register_scheduler_init(key="scheduler_none_008")
def scheduler_none_008_init(s):
    """Priority-aware, failure-averse scheduler with conservative sizing and bounded preemption.

    Main ideas:
    - Two-level queueing by priority with aging to avoid starvation.
    - Conservative RAM sizing with per-op adaptive retries on OOM to reduce failures (720s penalty).
    - CPU sizing biased to keep latency low for query/interactive while still allowing batch progress.
    - Limited preemption: only when high-priority work is ready and no pool can fit it otherwise.
    """
    # Queues per priority: store pipelines (not individual ops) to preserve DAG ordering.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track per-(pipeline_id, op_id) resource hints learned from past runs.
    # Values are "next try" sizes, adjusted on OOM. We keep small safety margins.
    s.op_ram_hint = {}  # (pipeline_id, op_id) -> ram
    s.op_cpu_hint = {}  # (pipeline_id, op_id) -> cpu

    # Track how many times an op has OOMed to cap exponential growth.
    s.op_oom_count = {}  # (pipeline_id, op_id) -> int

    # Simple aging to avoid starvation: count how many scheduler ticks pipeline has waited.
    s.pipeline_wait_ticks = {}  # pipeline_id -> ticks
    s._tick = 0

    # Preemption throttle to avoid churn.
    s._last_preempt_tick = -10
    s.preempt_cooldown_ticks = 3

    # When preempting, only target batch first, then interactive, never query.
    # If simulator exposes running containers via results, we can preempt recent low-priority.
    # We'll keep lightweight "running" bookkeeping from results.
    s.running = {}  # container_id -> dict(priority, pool_id, cpu, ram)


def _prio_rank(priority):
    # Higher is more important
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1


def _enqueue_by_priority(s, pipeline):
    if pipeline.priority == Priority.QUERY:
        s.q_query.append(pipeline)
    elif pipeline.priority == Priority.INTERACTIVE:
        s.q_interactive.append(pipeline)
    else:
        s.q_batch.append(pipeline)


def _all_queues(s):
    return [s.q_query, s.q_interactive, s.q_batch]


def _pick_next_pipeline(s):
    """Pick next pipeline considering priority and aging to prevent starvation."""
    # Update wait ticks lazily when picking: we increment globally elsewhere.
    # Choose best among the heads of each queue using (effective_priority, wait_ticks).
    candidates = []
    for q in _all_queues(s):
        if q:
            p = q[0]
            wt = s.pipeline_wait_ticks.get(p.pipeline_id, 0)
            candidates.append((p, wt))

    if not candidates:
        return None

    # Effective score: prioritize class strongly, but allow aging to bubble older batch.
    # Query dominates; interactive next; batch can rise with sufficient waiting.
    best = None
    best_score = None
    for p, wt in candidates:
        base = _prio_rank(p.priority) * 1000
        # Aging term: slower for higher priority to keep their tail low; faster for batch to ensure progress.
        if p.priority == Priority.BATCH_PIPELINE:
            age = wt * 6
        elif p.priority == Priority.INTERACTIVE:
            age = wt * 3
        else:
            age = wt * 1
        score = base + age
        if best_score is None or score > best_score:
            best_score = score
            best = p
    return best


def _pop_pipeline(s, pipeline):
    """Remove pipeline from its queue head (it should be at head)."""
    if pipeline.priority == Priority.QUERY:
        q = s.q_query
    elif pipeline.priority == Priority.INTERACTIVE:
        q = s.q_interactive
    else:
        q = s.q_batch
    if q and q[0].pipeline_id == pipeline.pipeline_id:
        q.pop(0)
    else:
        # Fallback: remove by search if ordering got disrupted.
        for i, p in enumerate(q):
            if p.pipeline_id == pipeline.pipeline_id:
                q.pop(i)
                break


def _get_assignable_op(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return None
    # Avoid retrying pipelines with a FAILED op unless FAILED is in ASSIGNABLE_STATES (it is).
    # We only assign one ready op at a time per pipeline for better latency and lower interference.
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _op_id(op):
    # Support both attribute or dict-like id.
    if hasattr(op, "op_id"):
        return op.op_id
    if hasattr(op, "operator_id"):
        return op.operator_id
    if hasattr(op, "id"):
        return op.id
    # Last resort: stable repr hash (not ideal but deterministic within a run).
    return str(op)


def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


def _default_cpu_for(priority, pool):
    # Keep latency low for query/interactive by allocating a reasonable chunk, but not all.
    if priority == Priority.QUERY:
        return _clamp(pool.max_cpu_pool * 0.60, 1, pool.max_cpu_pool)
    if priority == Priority.INTERACTIVE:
        return _clamp(pool.max_cpu_pool * 0.45, 1, pool.max_cpu_pool)
    return _clamp(pool.max_cpu_pool * 0.30, 1, pool.max_cpu_pool)


def _default_ram_for(priority, pool):
    # Allocate moderate RAM to reduce OOM risk. RAM beyond min doesn't improve runtime, but avoids failures.
    if priority == Priority.QUERY:
        return _clamp(pool.max_ram_pool * 0.55, 1, pool.max_ram_pool)
    if priority == Priority.INTERACTIVE:
        return _clamp(pool.max_ram_pool * 0.45, 1, pool.max_ram_pool)
    return _clamp(pool.max_ram_pool * 0.35, 1, pool.max_ram_pool)


def _choose_pool_for(op, pipeline, s):
    """Choose the pool with best headroom fit, preferring to keep query/interactive unblocked."""
    best_pool_id = None
    best_score = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Determine requested sizes given hints and defaults, then see fit.
        pid = pipeline.pipeline_id
        oid = _op_id(op)
        req_cpu = s.op_cpu_hint.get((pid, oid), _default_cpu_for(pipeline.priority, pool))
        req_ram = s.op_ram_hint.get((pid, oid), _default_ram_for(pipeline.priority, pool))

        # If cannot fit, skip. We avoid overcommitting to reduce OOM and queue churn.
        if req_cpu > avail_cpu or req_ram > avail_ram:
            continue

        # Score: for high priority, prefer pools with more remaining headroom after placement.
        # For batch, prefer tighter packing (less leftover) to reduce fragmentation.
        rem_cpu = avail_cpu - req_cpu
        rem_ram = avail_ram - req_ram
        if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
            score = rem_cpu * 2.0 + rem_ram * 1.0
        else:
            # Negative leftover -> tighter fit -> higher score (less waste)
            score = -(rem_cpu * 2.0 + rem_ram * 1.0)

        # Slightly prefer earlier pools for determinism tie-break.
        score = score - (pool_id * 1e-6)

        if best_score is None or score > best_score:
            best_score = score
            best_pool_id = pool_id

    return best_pool_id


def _compute_request(op, pipeline, s, pool):
    pid = pipeline.pipeline_id
    oid = _op_id(op)

    # Start with defaults; override with hints learned from previous execution results / OOMs.
    cpu = s.op_cpu_hint.get((pid, oid), _default_cpu_for(pipeline.priority, pool))
    ram = s.op_ram_hint.get((pid, oid), _default_ram_for(pipeline.priority, pool))

    # Keep some headroom for interactive arrivals: cap per-op CPU to avoid monopolizing a pool.
    # Query can be larger but still bounded.
    if pipeline.priority == Priority.QUERY:
        cpu_cap = pool.max_cpu_pool * 0.80
    elif pipeline.priority == Priority.INTERACTIVE:
        cpu_cap = pool.max_cpu_pool * 0.65
    else:
        cpu_cap = pool.max_cpu_pool * 0.50
    cpu = _clamp(cpu, 1, cpu_cap)

    # RAM should be at least a small fraction of pool to reduce OOM; cap to pool.
    ram = _clamp(ram, 1, pool.max_ram_pool)

    # Also don't request more than currently available when we actually assign.
    cpu = min(cpu, pool.avail_cpu_pool)
    ram = min(ram, pool.avail_ram_pool)
    return cpu, ram


def _maybe_preempt_for(s, need_cpu, need_ram, target_pool_id, min_priority_to_protect):
    """Try to free capacity by suspending one low-priority container in the target pool.

    We only preempt when:
    - There is a high-priority (query or interactive) op ready
    - Cooldown passed
    - We can identify a low-priority running container to suspend

    Returns: list of Suspend actions.
    """
    if s._tick - s._last_preempt_tick < s.preempt_cooldown_ticks:
        return []

    # Determine which priorities are eligible to preempt.
    # Protect queries always; preempt batch first; interactive only if protecting query.
    eligible = []
    for cid, info in s.running.items():
        if info["pool_id"] != target_pool_id:
            continue
        pr = info["priority"]
        if min_priority_to_protect == Priority.QUERY:
            if pr == Priority.BATCH_PIPELINE or pr == Priority.INTERACTIVE:
                eligible.append((cid, info))
        else:
            # Protect interactive: only preempt batch
            if pr == Priority.BATCH_PIPELINE:
                eligible.append((cid, info))

    if not eligible:
        return []

    # Pick a container that frees enough or is largest low-priority to maximize chance to fit.
    best = None
    best_score = None
    for cid, info in eligible:
        freed_cpu = info.get("cpu", 0)
        freed_ram = info.get("ram", 0)
        # Score: prefer those that help meet requirement; otherwise choose largest.
        meets = (freed_cpu >= need_cpu) or (freed_ram >= need_ram)
        score = (1000 if meets else 0) + freed_cpu * 2 + freed_ram * 1
        if best_score is None or score > best_score:
            best_score = score
            best = (cid, info)

    if best is None:
        return []

    cid, info = best
    s._last_preempt_tick = s._tick
    return [Suspend(container_id=cid, pool_id=info["pool_id"])]


@register_scheduler(key="scheduler_none_008")
def scheduler_none_008(s, results, pipelines):
    """
    Policy loop:
    - Ingest new pipelines into priority queues.
    - Update learned hints from execution results (especially OOM).
    - Schedule at most one ready op per pool per tick using priority + aging.
    - If a query/interactive op cannot fit anywhere, attempt limited preemption in the best pool.
    """
    s._tick += 1

    # Add new pipelines; init wait ticks.
    for p in pipelines:
        _enqueue_by_priority(s, p)
        s.pipeline_wait_ticks[p.pipeline_id] = 0

    # Update running bookkeeping and resource hints from results.
    for r in results:
        # If a container finished (success or failure), remove from running map.
        if getattr(r, "container_id", None) in s.running:
            # Remove on any reported result for that container to avoid stale state.
            s.running.pop(r.container_id, None)

        # Update hints:
        # - On OOM: increase RAM multiplicatively, keep CPU similar.
        # - On non-OOM failure: small RAM bump to be safer.
        # We assume error string may contain "OOM" or similar.
        if getattr(r, "ops", None):
            for op in r.ops:
                pid = getattr(r, "pipeline_id", None)
                # ExecutionResult doesn't expose pipeline_id in the provided list; use op's pipeline if available.
                if pid is None:
                    # Fallback: cannot learn without pipeline_id; skip.
                    continue
                oid = _op_id(op)
                key = (pid, oid)

                if r.failed():
                    err = str(getattr(r, "error", "")).lower()
                    if "oom" in err or "out of memory" in err:
                        cnt = s.op_oom_count.get(key, 0) + 1
                        s.op_oom_count[key] = cnt
                        # Exponential backoff capped; add 15% safety margin.
                        prev = s.op_ram_hint.get(key, max(1, getattr(r, "ram", 1)))
                        new_ram = prev * (1.8 if cnt <= 2 else 1.4)
                        new_ram = new_ram * 1.15
                        # Cap to largest pool ram if available.
                        max_pool_ram = 0
                        for pool_id in range(s.executor.num_pools):
                            max_pool_ram = max(max_pool_ram, s.executor.pools[pool_id].max_ram_pool)
                        if max_pool_ram > 0:
                            new_ram = min(new_ram, max_pool_ram)
                        s.op_ram_hint[key] = new_ram
                        # Keep CPU; but if we were using tiny CPU, bump a bit to reduce time-in-memory pressure.
                        prev_cpu = s.op_cpu_hint.get(key, max(1, getattr(r, "cpu", 1)))
                        s.op_cpu_hint[key] = prev_cpu
                    else:
                        # Unknown failure: slight RAM increase to reduce chance of memory-related issues.
                        prev = s.op_ram_hint.get(key, max(1, getattr(r, "ram", 1)))
                        s.op_ram_hint[key] = prev * 1.10
                else:
                    # Success: optionally record that the used RAM/CPU is sufficient (keep as hint).
                    # Use a small safety margin on RAM to prevent future OOM due to variance.
                    used_ram = max(1, getattr(r, "ram", 1))
                    used_cpu = max(1, getattr(r, "cpu", 1))
                    s.op_ram_hint[key] = max(s.op_ram_hint.get(key, 0), used_ram * 1.05)
                    s.op_cpu_hint[key] = max(s.op_cpu_hint.get(key, 0), used_cpu)

    # Age pipelines in queues to prevent starvation.
    for q in _all_queues(s):
        for p in q:
            s.pipeline_wait_ticks[p.pipeline_id] = s.pipeline_wait_ticks.get(p.pipeline_id, 0) + 1

    # Clean up completed pipelines from heads opportunistically (and anywhere in queue).
    def _filter_queue(q):
        out = []
        for p in q:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                s.pipeline_wait_ticks.pop(p.pipeline_id, None)
                continue
            # If the pipeline has failures, we still keep it because FAILED ops are assignable/retriable in this sim model.
            out.append(p)
        return out

    s.q_query = _filter_queue(s.q_query)
    s.q_interactive = _filter_queue(s.q_interactive)
    s.q_batch = _filter_queue(s.q_batch)

    # Early exit if nothing changed.
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # For each pool, schedule one op (small improvement over naive FIFO while avoiding head-of-line blocking).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Attempt a few picks to find a pipeline with an op ready that fits.
        # We keep this bounded for determinism and speed.
        tried = 0
        scheduled = False
        while tried < 6 and (s.q_query or s.q_interactive or s.q_batch) and not scheduled:
            p = _pick_next_pipeline(s)
            if p is None:
                break

            # Pop it for consideration; we'll requeue if not schedulable.
            _pop_pipeline(s, p)

            status = p.runtime_status()
            if status.is_pipeline_successful():
                s.pipeline_wait_ticks.pop(p.pipeline_id, None)
                tried += 1
                continue

            op = _get_assignable_op(p)
            if op is None:
                # Not ready yet; requeue and try another.
                _enqueue_by_priority(s, p)
                tried += 1
                continue

            # Prefer best pool overall (may differ from current pool); if current isn't best, requeue and let other pool pick.
            best_pool = _choose_pool_for(op, p, s)
            if best_pool is None:
                # Can't fit anywhere right now; consider preemption if high-priority.
                if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                    # Try preempt in the pool with most available resources to maximize chance.
                    best_candidate_pool = None
                    best_headroom = None
                    for pid2 in range(s.executor.num_pools):
                        pl2 = s.executor.pools[pid2]
                        headroom = pl2.avail_cpu_pool * 2.0 + pl2.avail_ram_pool * 1.0
                        if best_headroom is None or headroom > best_headroom:
                            best_headroom = headroom
                            best_candidate_pool = pid2

                    if best_candidate_pool is not None:
                        pl2 = s.executor.pools[best_candidate_pool]
                        # Compute needed based on defaults/hints relative to that pool.
                        cpu_need, ram_need = _compute_request(op, p, s, pl2)
                        cpu_short = max(0, cpu_need - pl2.avail_cpu_pool)
                        ram_short = max(0, ram_need - pl2.avail_ram_pool)
                        if cpu_short > 0 or ram_short > 0:
                            suspensions.extend(
                                _maybe_preempt_for(
                                    s,
                                    need_cpu=cpu_short,
                                    need_ram=ram_short,
                                    target_pool_id=best_candidate_pool,
                                    min_priority_to_protect=p.priority
                                )
                            )
                # Requeue and move on; avoid dropping.
                _enqueue_by_priority(s, p)
                tried += 1
                continue

            if best_pool != pool_id:
                # Let the selected best pool handle it; requeue.
                _enqueue_by_priority(s, p)
                tried += 1
                continue

            # Now schedule on this pool with computed request.
            cpu_req, ram_req = _compute_request(op, p, s, pool)
            if cpu_req <= 0 or ram_req <= 0 or cpu_req > pool.avail_cpu_pool or ram_req > pool.avail_ram_pool:
                # Can't actually fit; requeue.
                _enqueue_by_priority(s, p)
                tried += 1
                continue

            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id
            )
            assignments.append(assignment)

            # Reset waiting ticks since it got service; requeue pipeline for subsequent ops.
            s.pipeline_wait_ticks[p.pipeline_id] = 0
            _enqueue_by_priority(s, p)

            # Record as running if we can infer a container id later; we store on next results.
            # Here we cannot know container_id; running bookkeeping is best-effort.

            scheduled = True

    return suspensions, assignments
