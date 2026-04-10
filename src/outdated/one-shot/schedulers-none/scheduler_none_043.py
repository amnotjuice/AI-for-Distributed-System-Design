# policy_key: scheduler_none_043
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049800
# generation_seconds: 51.29
# generated_at: 2026-03-14T02:12:25.792734
@register_scheduler_init(key="scheduler_none_043")
def scheduler_none_043_init(s):
    """Priority-aware FIFO with gentle preemption and simple OOM backoff.

    Small, incremental improvements over naive FIFO:
      1) Maintain separate queues per priority; always try higher priority first.
      2) Size assignments conservatively (small default slices) to reduce head-of-line blocking.
      3) On OOM, increase RAM request for that operator (per-pipeline op-key) and retry.
      4) If a high-priority op can't fit, preempt a low-priority container in the same pool (at most 1/tick).
      5) Avoid retrying non-OOM failures; drop failed pipelines.

    Notes:
      - We only schedule ops whose parents are complete.
      - We try to place QUERY/INTERACTIVE into the pool with most free RAM (to reduce OOM risk).
      - We keep complexity low: no CPU-scaling inference, no multi-op packing per container.
    """
    from collections import deque, defaultdict

    # Separate queues per priority to reduce tail latency for interactive work.
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per (pipeline_id, op_key) RAM request backoff after OOM.
    s.op_ram_req = defaultdict(float)

    # Simple per tick limit to reduce churn.
    s.max_preempts_per_tick = 1

    # Defaults: assign small slices first (reduces blocking), but still meaningful.
    s.default_cpu_slice = 1.0
    s.default_ram_slice_frac = 0.25  # 25% of pool RAM, capped by availability

    # Backoff parameters for OOM retries.
    s.oom_ram_multiplier = 2.0
    s.oom_ram_additive = 0.0  # keep simple; multiplier usually enough

    # Track currently running containers we might preempt (from results stream).
    # container_id -> dict(pool_id, priority)
    s.running_containers = {}


def _q_for_priority(s, prio):
    # Prefer to avoid importing Priority at top-level; rely on runtime environment.
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _all_queues_empty(s):
    return (not s.q_query) and (not s.q_interactive) and (not s.q_batch)


def _pop_next_pipeline(s):
    # Strict priority order; FIFO within each priority.
    if s.q_query:
        return s.q_query.popleft()
    if s.q_interactive:
        return s.q_interactive.popleft()
    if s.q_batch:
        return s.q_batch.popleft()
    return None


def _requeue_pipeline(s, p):
    _q_for_priority(s, p.priority).append(p)


def _op_key(op):
    # Make a stable key for per-operator backoff.
    # Try common fields; fallback to repr.
    for attr in ("op_id", "operator_id", "node_id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return str(v)
            except Exception:
                pass
    return repr(op)


def _choose_pool_for_priority(s, prio):
    # For high priority, choose pool with most available RAM (lower OOM risk / faster admission).
    # For batch, choose pool with most available CPU (throughput).
    best_pool = None
    best_score = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            score = (avail_ram, avail_cpu)
        else:
            score = (avail_cpu, avail_ram)

        if best_score is None or score > best_score:
            best_score = score
            best_pool = pool_id
    return best_pool


def _compute_request(s, pool, pipeline_id, op, prio):
    # Conservative CPU: small slice, but not more than available.
    cpu_req = min(pool.avail_cpu_pool, max(0.1, float(getattr(s, "default_cpu_slice", 1.0))))

    # RAM: start with a fraction of pool max; cap by availability.
    base = float(pool.max_ram_pool) * float(getattr(s, "default_ram_slice_frac", 0.25))
    base = max(1.0, base)  # avoid tiny zero/negative cases

    # Apply per-op OOM backoff if any.
    k = (pipeline_id, _op_key(op))
    ram_req = s.op_ram_req.get(k, base)

    # Cap to available RAM; if it doesn't fit, caller will handle (preempt or skip).
    ram_req = min(pool.avail_ram_pool, ram_req)

    # For QUERY, bias to give a bit more CPU if available (reduces latency).
    if prio == Priority.QUERY:
        cpu_req = min(pool.avail_cpu_pool, max(cpu_req, min(2.0, pool.avail_cpu_pool)))
    elif prio == Priority.INTERACTIVE:
        cpu_req = min(pool.avail_cpu_pool, max(cpu_req, min(1.5, pool.avail_cpu_pool)))

    return cpu_req, ram_req


def _record_oom_backoff(s, pipeline_id, op, prev_ram):
    k = (pipeline_id, _op_key(op))
    new_ram = max(prev_ram * float(getattr(s, "oom_ram_multiplier", 2.0)) + float(getattr(s, "oom_ram_additive", 0.0)),
                  prev_ram + 1.0)
    s.op_ram_req[k] = new_ram


def _update_running_from_results(s, results):
    # Update our view of running containers based on observed results.
    # We treat any successful completion or failure as no longer running.
    for r in results:
        try:
            cid = r.container_id
            if cid is None:
                continue
            if r.failed():
                # On failure, container is done.
                if cid in s.running_containers:
                    del s.running_containers[cid]
            else:
                # Non-failed result could be completion. Best effort: if ops present and no error, assume done.
                # Some simulators may emit intermediate updates; to be safe, remove only if ops exist.
                if getattr(r, "ops", None):
                    if cid in s.running_containers:
                        del s.running_containers[cid]
        except Exception:
            continue


def _consider_preempt(s, pool_id, needed_ram, needed_cpu, preempts_left):
    # Preempt at most one low-priority container per call if it helps.
    if preempts_left <= 0:
        return None

    # Choose a BATCH container first; if none, then INTERACTIVE (never preempt QUERY).
    # We don't know resource usage beyond what we recorded.
    candidate = None
    candidate_prio = None
    for cid, meta in s.running_containers.items():
        if meta.get("pool_id") != pool_id:
            continue
        pr = meta.get("priority")
        if pr == Priority.QUERY:
            continue
        if pr == Priority.BATCH_PIPELINE:
            candidate = cid
            candidate_prio = pr
            break
        if pr == Priority.INTERACTIVE and candidate is None:
            candidate = cid
            candidate_prio = pr

    if candidate is None:
        return None

    # We don't have precise freed amounts; still, preempting can increase headroom next tick.
    return Suspend(container_id=candidate, pool_id=pool_id)


@register_scheduler(key="scheduler_none_043")
def scheduler_none_043(s, results, pipelines):
    """
    Priority-aware scheduling loop.

    - Enqueue new pipelines by priority.
    - Process results: apply OOM backoff; track running containers; ignore non-OOM failures.
    - For each tick:
        * Try to schedule one ready op per pool (keeps it simple and reduces interference).
        * Pick pipelines in priority order.
        * If high-priority work can't fit, attempt small preemption of low-priority in that pool.
    """
    # Enqueue new arrivals.
    for p in pipelines:
        _q_for_priority(s, p.priority).append(p)

    # Early exit: if no new inputs, no results, and no queued work.
    if not pipelines and not results and _all_queues_empty(s):
        return [], []

    # Update running containers view from results (best-effort).
    _update_running_from_results(s, results)

    # React to results: OOM -> increase RAM request for affected ops and requeue pipeline.
    # Also record currently running containers from result metadata if available (best-effort).
    pipelines_to_requeue = []

    for r in results:
        # Track as running if the simulator reports it (best effort).
        try:
            if getattr(r, "container_id", None) is not None and getattr(r, "pool_id", None) is not None:
                # If it failed/completed, _update_running_from_results removed it. If still here, re-add.
                if not r.failed() and not getattr(r, "error", None):
                    s.running_containers[r.container_id] = {"pool_id": r.pool_id, "priority": r.priority}
        except Exception:
            pass

        if not r.failed():
            continue

        err = getattr(r, "error", None)
        is_oom = False
        if err is not None:
            es = str(err).lower()
            if "oom" in es or "out of memory" in es or "memory" in es:
                is_oom = True

        if not is_oom:
            # Non-OOM failure: do not retry; pipeline will be dropped when seen as having failures.
            continue

        # OOM: backoff RAM request for the involved ops and requeue pipeline (if we can find it).
        # We don't have direct pipeline object on result; assume ops contain enough to key backoff
        # and that the pipeline will still be in queues / will arrive in subsequent calls.
        try:
            for op in getattr(r, "ops", []) or []:
                # Use the RAM we attempted (r.ram) as baseline.
                prev_ram = float(getattr(r, "ram", 0.0)) or 1.0
                # Can't get pipeline_id reliably from op/result; attempt to use op.pipeline_id if present.
                pid = None
                if hasattr(op, "pipeline_id"):
                    pid = getattr(op, "pipeline_id")
                if pid is None:
                    # Fall back: if result carries pipeline_id (some implementations do)
                    pid = getattr(r, "pipeline_id", None)
                if pid is None:
                    # Last resort: skip backoff if we can't key it.
                    continue
                _record_oom_backoff(s, pid, op, prev_ram)
        except Exception:
            pass

    suspensions = []
    assignments = []

    # Schedule at most one assignment per pool per tick (simple + avoids oversubscription heuristics).
    preempts_left = int(getattr(s, "max_preempts_per_tick", 1))

    # We'll iterate pools; for each pool choose work in priority order.
    for _ in range(s.executor.num_pools):
        # Choose which pool to fill next: take the pool with the most available CPU (work-conserving).
        # This avoids starving a big-empty pool if earlier pools have tiny leftovers.
        best_pool_id = None
        best_cpu = None
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                continue
            if best_cpu is None or pool.avail_cpu_pool > best_cpu:
                best_cpu = pool.avail_cpu_pool
                best_pool_id = pool_id
        if best_pool_id is None:
            break

        pool_id = best_pool_id
        pool = s.executor.pools[pool_id]

        if _all_queues_empty(s):
            break

        # Try to find the next runnable op from highest priority pipelines.
        tried = 0
        max_to_try = len(s.q_query) + len(s.q_interactive) + len(s.q_batch)
        scheduled = False

        while tried < max_to_try:
            p = _pop_next_pipeline(s)
            if p is None:
                break
            tried += 1

            status = p.runtime_status()
            has_failures = status.state_counts[OperatorState.FAILED] > 0

            if status.is_pipeline_successful() or has_failures:
                # Drop completed/failed pipelines.
                continue

            # Only schedule ops whose parents completed; only one op per assignment.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; keep it around.
                _requeue_pipeline(s, p)
                continue

            op = op_list[0]

            # Prefer a better pool for high-priority work: if this pool isn't a good fit, we can skip
            # and try other pipelines; but keep changes minimal: only do this when multiple pools exist.
            if s.executor.num_pools > 1 and p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                preferred_pool = _choose_pool_for_priority(s, p.priority)
                if preferred_pool is not None and preferred_pool != pool_id:
                    # Requeue and let the preferred pool be filled on its iteration.
                    _requeue_pipeline(s, p)
                    continue

            cpu_req, ram_req = _compute_request(s, pool, p.pipeline_id, op, p.priority)

            # If it doesn't fit, for high priority attempt preemption; otherwise requeue.
            fits = (cpu_req <= pool.avail_cpu_pool) and (ram_req <= pool.avail_ram_pool) and cpu_req > 0 and ram_req > 0
            if not fits:
                if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                    # Attempt one preemption to create headroom.
                    sus = _consider_preempt(s, pool_id, ram_req, cpu_req, preempts_left)
                    if sus is not None:
                        suspensions.append(sus)
                        preempts_left -= 1
                _requeue_pipeline(s, p)
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

            # Mark this container as running (best-effort). The simulator will assign a container_id later;
            # we can't know it now, so we only track preemptions based on results.
            scheduled = True

            # Requeue pipeline for subsequent ops.
            _requeue_pipeline(s, p)
            break

        # If we didn't schedule anything for this pool, avoid looping forever.
        if not scheduled:
            # Nothing runnable/fittable right now; move on.
            continue

    return suspensions, assignments
