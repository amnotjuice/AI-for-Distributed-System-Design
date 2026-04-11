# policy_key: scheduler_high_047
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.184741
# generation_seconds: 185.04
# generated_at: 2026-04-10T07:39:39.234972
@register_scheduler_init(key="scheduler_high_047")
def scheduler_high_047_init(s):
    """
    Priority-aware, completion-focused scheduler with:
      - Separate FIFO queues per priority (QUERY / INTERACTIVE / BATCH).
      - Soft pool partitioning: if multiple pools exist, pool 0 acts as a "latency pool"
        primarily for QUERY/INTERACTIVE; other pools prefer BATCH (but can run anything).
      - Conservative initial RAM sizing plus OOM-driven RAM backoff (doubling) to reduce
        repeated failures (each failure/incompletion is heavily penalized).
      - Small, fixed-ish CPU slices to improve concurrency; larger slices for QUERY.
      - Anti-starvation: when only one pool exists, periodically interleave a small BATCH op
        if backlog exists (without stealing the latency pool when multiple pools exist).
    """
    from collections import deque

    s.tick = 0

    # Per-priority queues (pipelines persist here until completed or we give up).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track whether a pipeline is already enqueued (avoid duplicates).
    s.enqueued_pipeline_ids = set()
    s.enqueue_tick = {}  # pipeline_id -> tick enqueued (for starvation heuristics)

    # Per-operator resource hints (keyed by Python object identity).
    s.op_ram_hint = {}   # op_key -> last known good / required RAM
    s.op_cpu_hint = {}   # op_key -> optional CPU hint (lightly used)
    s.op_attempts = {}   # op_key -> retry attempts count
    s.op_non_oom_failures = {}  # op_key -> count

    # Give up when an op is clearly unschedulable (prevents infinite churn).
    s.max_attempts = 6
    s.max_non_oom_retries = 2

    # If only one pool exists, force occasional batch progress to avoid 720s penalties.
    s.single_pool_batch_interleave = 7  # after this many non-batch assignments, try one batch
    s.non_batch_since_last_batch = 0

    # Starvation control for batch (ticks in queue before we relax constraints).
    s.batch_starvation_ticks = 50

    # How many assignments to attempt per pool per scheduling tick.
    s.max_assignments_per_pool = 4

    # Define assignable states locally to avoid reliance on a global constant.
    s.assignable_states = [OperatorState.PENDING, OperatorState.FAILED]


def _q_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _enqueue_pipeline(s, p):
    pid = p.pipeline_id
    if pid in s.enqueued_pipeline_ids:
        return
    _q_for_priority(s, p.priority).append(p)
    s.enqueued_pipeline_ids.add(pid)
    s.enqueue_tick[pid] = s.tick


def _is_oom_error(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)


def _op_key(op):
    # Stable for the simulation lifetime (operators are objects).
    return id(op)


def _desired_cpu(pool, prio, avail_cpu):
    # Keep CPU allocations modest to improve concurrency; queries get more.
    max_cpu = float(pool.max_cpu_pool) if pool.max_cpu_pool is not None else float(avail_cpu)
    if prio == Priority.QUERY:
        base = max(1.0, 0.50 * max_cpu)
    elif prio == Priority.INTERACTIVE:
        base = max(1.0, 0.33 * max_cpu)
    else:
        base = max(1.0, 0.20 * max_cpu)
    return min(float(avail_cpu), base)


def _desired_ram(pool, prio, avail_ram):
    # Start conservatively high on RAM to reduce OOM retries (OOMs are very costly).
    max_ram = float(pool.max_ram_pool) if pool.max_ram_pool is not None else float(avail_ram)
    if prio == Priority.QUERY:
        base = max(1.0, 0.55 * max_ram)
    elif prio == Priority.INTERACTIVE:
        base = max(1.0, 0.45 * max_ram)
    else:
        base = max(1.0, 0.30 * max_ram)
    return min(float(avail_ram), base)


def _peek_ready_op(s, pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return None
    op_list = status.get_ops(s.assignable_states, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[0]


def _any_ready_in_queue(s, q):
    # Bounded scan to check readiness (avoid O(n) repeatedly on huge queues).
    n = len(q)
    for i in range(min(n, 32)):
        p = q[i]
        if _peek_ready_op(s, p) is not None:
            return True
    return False


def _pick_from_queue_that_fits(s, q, pool, avail_cpu, avail_ram, prio, latency_sensitive):
    """
    Pop/rotate within a single priority queue to find a pipeline with a ready op that fits.
    Returns: (pipeline, op, cpu, ram) or (None, None, None, None)
    """
    n = len(q)
    if n == 0:
        return None, None, None, None

    for _ in range(n):
        p = q.popleft()

        status = p.runtime_status()
        if status.is_pipeline_successful():
            # Pipeline finished; drop from bookkeeping.
            pid = p.pipeline_id
            s.enqueued_pipeline_ids.discard(pid)
            s.enqueue_tick.pop(pid, None)
            continue

        op = _peek_ready_op(s, p)
        if op is None:
            # Not runnable now; rotate.
            q.append(p)
            continue

        ok = _op_key(op)

        # Compute CPU/RAM request from defaults + per-op hints.
        cpu_req = _desired_cpu(pool, prio, avail_cpu)
        ram_req = _desired_ram(pool, prio, avail_ram)

        # Apply per-op RAM hint (from prior OOMs or successes) with a small safety margin.
        if ok in s.op_ram_hint:
            ram_req = max(ram_req, float(s.op_ram_hint[ok]) * 1.05)

        # Apply per-op CPU hint (light-touch).
        if ok in s.op_cpu_hint:
            cpu_req = max(cpu_req, float(s.op_cpu_hint[ok]))

        # Clamp to pool/availability.
        cpu_req = min(float(avail_cpu), float(cpu_req))
        ram_req = min(float(avail_ram), float(ram_req))

        # Minimum viable resources (avoid zero allocations).
        if cpu_req < 1e-9 or ram_req < 1e-9:
            q.append(p)
            continue

        # If the hint is larger than currently available in this pool, rotate and try later.
        # (This avoids assigning an op with known-too-low RAM and re-OOMing.)
        hinted = float(s.op_ram_hint.get(ok, 0.0))
        if hinted > 0.0 and hinted > float(avail_ram):
            q.append(p)
            continue

        # For latency sensitive work, require at least a small slice (avoid tiny allocations).
        if latency_sensitive and cpu_req < 1.0:
            cpu_req = min(float(avail_cpu), 1.0)

        # Found a fit: keep pipeline in queue (append back) so remaining ops can be scheduled later.
        q.append(p)
        return p, op, cpu_req, ram_req

    return None, None, None, None


@register_scheduler(key="scheduler_high_047")
def scheduler_high_047(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Scheduling loop:
      1) Enqueue new pipelines by priority.
      2) Update per-op hints from results:
           - On OOM: double RAM hint; retry quickly (especially for QUERY/INTERACTIVE).
           - On success: gently decay RAM hint toward observed usage to improve packing.
      3) For each pool (sorted by headroom):
           - If multiple pools: pool 0 prioritizes QUERY/INTERACTIVE; other pools prefer BATCH.
           - If single pool: mostly serve QUERY/INTERACTIVE but interleave small BATCH periodically.
      4) Create multiple small assignments per pool (up to max_assignments_per_pool) to raise utilization.
    """
    s.tick += 1

    # Enqueue new arrivals.
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update hints from execution results.
    # (This is the main feedback loop to reduce OOM failures and thus 720s penalties.)
    for r in results:
        if not getattr(r, "ops", None):
            continue

        pool = s.executor.pools[r.pool_id] if r.pool_id is not None else None
        pool_max_ram = float(pool.max_ram_pool) if pool is not None else None

        failed = r.failed()
        is_oom = _is_oom_error(getattr(r, "error", None))

        for op in r.ops:
            ok = _op_key(op)
            s.op_attempts[ok] = int(s.op_attempts.get(ok, 0)) + (1 if failed else 0)

            if failed:
                if is_oom:
                    # Aggressive RAM backoff on OOM.
                    prev = float(s.op_ram_hint.get(ok, float(getattr(r, "ram", 0.0) or 0.0)))
                    base = float(getattr(r, "ram", prev) or prev)
                    new_hint = max(prev, base * 2.0, base + (pool_max_ram * 0.10 if pool_max_ram else 0.0))
                    if pool_max_ram is not None:
                        # Allow hint to exceed this pool; it may fit in other pools.
                        pass
                    s.op_ram_hint[ok] = new_hint
                else:
                    # Non-OOM failure: retry a couple times but don't explode RAM.
                    s.op_non_oom_failures[ok] = int(s.op_non_oom_failures.get(ok, 0)) + 1
            else:
                # Success: record that this RAM worked; gently decay any oversized hint.
                observed_ram = float(getattr(r, "ram", 0.0) or 0.0)
                if observed_ram > 0.0:
                    prev = float(s.op_ram_hint.get(ok, observed_ram))
                    # Keep at least the observed RAM; shrink slowly to avoid re-OOMs.
                    s.op_ram_hint[ok] = max(observed_ram, prev * 0.97)

                observed_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                if observed_cpu > 0.0:
                    prevc = float(s.op_cpu_hint.get(ok, observed_cpu))
                    s.op_cpu_hint[ok] = max(0.5, min(observed_cpu, prevc * 1.05))

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    num_pools = int(s.executor.num_pools)
    pools = s.executor.pools

    # Sort pools by headroom so we place work where it fits best.
    pool_order = list(range(num_pools))
    pool_order.sort(
        key=lambda i: (float(pools[i].avail_cpu_pool) * 1e6 + float(pools[i].avail_ram_pool)),
        reverse=True,
    )

    multi_pool = num_pools > 1
    latency_pool_id = 0 if multi_pool else None

    # Global readiness signals (used to keep latency pool clean when possible).
    any_query_ready = _any_ready_in_queue(s, s.q_query)
    any_interactive_ready = _any_ready_in_queue(s, s.q_interactive)
    any_latency_ready = any_query_ready or any_interactive_ready
    any_batch_ready = _any_ready_in_queue(s, s.q_batch)

    for pool_id in pool_order:
        pool = pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Reserve a small fraction of resources in non-latency pools too, to avoid total lockout
        # when a high-priority job arrives mid-batch. Relaxed by starvation.
        reserve_cpu = 0.15 * float(pool.max_cpu_pool)
        reserve_ram = 0.15 * float(pool.max_ram_pool)

        for _ in range(int(s.max_assignments_per_pool)):
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

            is_latency_pool = (multi_pool and pool_id == latency_pool_id)

            # Pool preference:
            #  - latency pool: prefer QUERY/INTERACTIVE; only run BATCH if no latency work is ready.
            #  - other pools: prefer BATCH first to ensure batch completion (reduce 720s penalties),
            #    but still take QUERY/INTERACTIVE if batch is empty.
            priority_try_order = []
            if is_latency_pool:
                if any_latency_ready:
                    priority_try_order = [Priority.QUERY, Priority.INTERACTIVE]
                else:
                    priority_try_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            else:
                # Non-latency pools: push batch forward to avoid incomplete batch penalties.
                priority_try_order = [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]

            # Single-pool special case: interleave an occasional batch op even if latency exists,
            # to avoid batch starvation and 720s penalties.
            force_batch_now = False
            if not multi_pool and any_batch_ready:
                if s.non_batch_since_last_batch >= int(s.single_pool_batch_interleave):
                    force_batch_now = True
                    priority_try_order = [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]

            made = False
            for prio in priority_try_order:
                q = _q_for_priority(s, prio)

                latency_sensitive = (prio in (Priority.QUERY, Priority.INTERACTIVE))

                # For batch in a latency pool (multi-pool), be extra strict: only if no latency ready.
                if multi_pool and is_latency_pool and prio == Priority.BATCH_PIPELINE and any_latency_ready:
                    continue

                # For batch in non-latency pools, keep a small reserve unless batch is starving.
                if prio == Priority.BATCH_PIPELINE:
                    # Starvation relax: if the oldest batch pipeline waited long, let it run even if reserve is tight.
                    starving = False
                    if len(q) > 0:
                        oldest = q[0]
                        waited = s.tick - int(s.enqueue_tick.get(oldest.pipeline_id, s.tick))
                        starving = waited >= int(s.batch_starvation_ticks)

                    if any_latency_ready and not starving:
                        # When latency work exists, only run batch if we won't consume into reserve.
                        # (This protects query/interactive latency dominating the score.)
                        # We'll check this *after* computing a specific op request.
                        pass

                p, op, cpu_req, ram_req = _pick_from_queue_that_fits(
                    s=s,
                    q=q,
                    pool=pool,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    prio=prio,
                    latency_sensitive=latency_sensitive,
                )
                if p is None:
                    continue

                ok = _op_key(op)

                # Give up logic (only if repeatedly failing and we have no path to increase resources).
                if int(s.op_attempts.get(ok, 0)) >= int(s.max_attempts):
                    # If it's a non-OOM failure and we've already retried enough, stop wasting capacity.
                    if int(s.op_non_oom_failures.get(ok, 0)) > int(s.max_non_oom_retries):
                        # Remove pipeline from queues to prevent perpetual scanning.
                        # (This still yields a 720s penalty, but avoids harming others.)
                        pid = p.pipeline_id
                        s.enqueued_pipeline_ids.discard(pid)
                        s.enqueue_tick.pop(pid, None)
                        # Also remove all occurrences of this pipeline from this queue if present later.
                        # (Since we re-append, it may appear multiple times only if external logic duplicates;
                        #  we avoid that via enqueued_pipeline_ids, so this is typically enough.)
                        continue

                # Batch reserve check (after sizing).
                if prio == Priority.BATCH_PIPELINE and any_latency_ready and not (multi_pool and is_latency_pool):
                    # Starvation relax based on oldest batch wait.
                    starving = False
                    if len(s.q_batch) > 0:
                        oldest = s.q_batch[0]
                        waited = s.tick - int(s.enqueue_tick.get(oldest.pipeline_id, s.tick))
                        starving = waited >= int(s.batch_starvation_ticks)

                    if not starving:
                        if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                            # Not enough headroom; skip batch for now.
                            continue

                # Create the assignment for exactly one operator.
                assignment = Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(assignment)

                # Update local accounting.
                avail_cpu -= float(cpu_req)
                avail_ram -= float(ram_req)

                # Update batch interleave counter.
                if prio == Priority.BATCH_PIPELINE:
                    s.non_batch_since_last_batch = 0
                else:
                    s.non_batch_since_last_batch += 1

                made = True
                break  # proceed to next slot in this pool

            if not made:
                break  # nothing fits / nothing runnable for this pool right now

    return suspensions, assignments
