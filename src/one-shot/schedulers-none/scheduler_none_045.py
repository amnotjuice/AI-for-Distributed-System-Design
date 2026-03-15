# policy_key: scheduler_none_045
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052905
# generation_seconds: 49.59
# generated_at: 2026-03-14T02:14:02.882499
@register_scheduler_init(key="scheduler_none_045")
def scheduler_none_045_init(s):
    """Priority-aware, low-churn scheduler (incremental improvement over naive FIFO).

    Improvements over the naive example:
      1) Separate queues by priority (QUERY > INTERACTIVE > BATCH_PIPELINE) to reduce latency for high-priority work.
      2) Conservative sizing + OOM-driven RAM backoff per (pipeline, op), to avoid repeated OOMs while keeping RAM small.
      3) Minimal preemption: only preempt when a high-priority op is runnable and *nothing* can be scheduled due to lack of headroom.
         Preempt the lowest-priority running containers first, and only as many as needed.
      4) Avoid waste: schedule small bundles of runnable ops from the same pipeline when possible, to reduce per-op latency/overhead,
         but never at the expense of starving higher priorities.
    """
    # Three priority queues: higher index = lower priority
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Remember RAM sizing per (pipeline_id, op_id) across retries; start small and grow on OOM.
    # We don't assume op exposes explicit RAM-min; we learn from failures deterministically.
    s.ram_hint = {}  # (pipeline_id, op_id) -> ram

    # Track last known container allocations to inform future sizing.
    s.last_alloc = {}  # (pipeline_id, op_id) -> (cpu, ram)

    # Basic knobs (keep conservative/simple first)
    s.min_ram_frac = 0.10          # initial RAM = 10% of pool max (capped by avail)
    s.oom_backoff_mult = 2.0       # on OOM, multiply RAM hint by 2
    s.oom_backoff_add_frac = 0.05  # and add +5% of pool max to move off tiny sizes
    s.max_ops_per_assignment = 2   # bundle up to 2 ops from same pipeline when plentiful resources
    s.preempt_enabled = True

    # Optional: protect a small share for non-batch to avoid batch filling everything
    s.batch_max_share = 0.80  # try not to let batch consume >80% of a pool when others are waiting


def _prio_rank(priority):
    # Lower is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE or others


def _enqueue(s, pipeline):
    r = _prio_rank(pipeline.priority)
    if r == 0:
        s.q_query.append(pipeline)
    elif r == 1:
        s.q_interactive.append(pipeline)
    else:
        s.q_batch.append(pipeline)


def _dequeue_in_order(s):
    # Returns (queue_name, pipeline) in priority order, FIFO within each queue.
    if s.q_query:
        return "q_query", s.q_query.pop(0)
    if s.q_interactive:
        return "q_interactive", s.q_interactive.pop(0)
    if s.q_batch:
        return "q_batch", s.q_batch.pop(0)
    return None, None


def _requeue_front(s, queue_name, pipeline):
    if queue_name == "q_query":
        s.q_query.insert(0, pipeline)
    elif queue_name == "q_interactive":
        s.q_interactive.insert(0, pipeline)
    elif queue_name == "q_batch":
        s.q_batch.insert(0, pipeline)
    else:
        _enqueue(s, pipeline)


def _requeue_back(s, queue_name, pipeline):
    if queue_name == "q_query":
        s.q_query.append(pipeline)
    elif queue_name == "q_interactive":
        s.q_interactive.append(pipeline)
    elif queue_name == "q_batch":
        s.q_batch.append(pipeline)
    else:
        _enqueue(s, pipeline)


def _iter_queues_nonempty(s):
    return bool(s.q_query or s.q_interactive or s.q_batch)


def _default_ram_request(s, pool, pipeline_id, op_id):
    # Use learned hint if present, else start at a small fraction of pool max.
    key = (pipeline_id, op_id)
    if key in s.ram_hint:
        return s.ram_hint[key]
    return max(1e-9, pool.max_ram_pool * s.min_ram_frac)


def _apply_oom_backoff(s, pool, pipeline_id, op_id, prev_ram):
    # Backoff strategy: multiplicative + additive floor tied to pool size
    bumped = max(prev_ram * s.oom_backoff_mult, prev_ram + pool.max_ram_pool * s.oom_backoff_add_frac)
    # Never exceed pool max
    return min(bumped, pool.max_ram_pool)


def _pool_budget_for_batch(s, pool):
    # If higher-priority queues have items, try to keep some headroom by reducing
    # what batch is allowed to take from this pool at this tick.
    if s.q_query or s.q_interactive:
        return pool.max_cpu_pool * s.batch_max_share, pool.max_ram_pool * s.batch_max_share
    return pool.max_cpu_pool, pool.max_ram_pool


def _collect_running_containers(results):
    # Best-effort: infer running containers from non-terminal results is not possible.
    # We instead use s.executor state if available; but we don't have an API here.
    # So preemption decisions will be based on recent results only (conservative).
    return []


def _plan_preemptions_if_needed(s, pool_id, need_cpu, need_ram, high_prio_rank):
    # Minimal, conservative preemption: attempt to preempt lowest-priority containers if the
    # scheduler has visibility of active containers via s.executor.pools[pool_id].containers.
    # If not present, do nothing.
    suspensions = []

    pool = s.executor.pools[pool_id]

    containers = getattr(pool, "containers", None)
    if not containers:
        return suspensions

    # containers may be dict-like or list-like; normalize
    try:
        cont_iter = list(containers.values())
    except Exception:
        try:
            cont_iter = list(containers)
        except Exception:
            cont_iter = []

    # Filter to candidates that are actually running/assigned; we only rely on presence of fields.
    # Prefer preempting low-priority (batch) first, then interactive; never preempt QUERY for QUERY.
    def cont_rank(c):
        p = getattr(c, "priority", None)
        if p is None:
            # Unknown containers are treated as batch to avoid preempting critical work accidentally
            return 2
        return _prio_rank(p)

    # Only preempt containers with rank > high_prio_rank (strictly lower priority)
    candidates = [c for c in cont_iter if cont_rank(c) > high_prio_rank]

    # Sort by lowest importance first (higher rank), then by larger resource to free quickly
    def sort_key(c):
        r = cont_rank(c)
        cpu = getattr(c, "cpu", 0.0) or 0.0
        ram = getattr(c, "ram", 0.0) or 0.0
        return (r, -(cpu + ram))

    candidates.sort(key=sort_key, reverse=False)

    freed_cpu = 0.0
    freed_ram = 0.0
    for c in candidates:
        if freed_cpu >= need_cpu and freed_ram >= need_ram:
            break
        cid = getattr(c, "container_id", None)
        if cid is None:
            continue
        suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
        freed_cpu += getattr(c, "cpu", 0.0) or 0.0
        freed_ram += getattr(c, "ram", 0.0) or 0.0

    return suspensions


@register_scheduler(key="scheduler_none_045")
def scheduler_none_045(s, results, pipelines):
    """
    Priority-aware scheduler:
      - Enqueue new pipelines by priority.
      - Learn from OOM failures: increase RAM hint for the failed op and requeue pipeline.
      - For each pool, try to schedule highest-priority runnable ops first.
      - If blocked and high-priority work is waiting, attempt minimal preemption (if pool exposes containers).
    """
    # Ingest new pipelines
    for p in pipelines:
        _enqueue(s, p)

    # Process results: update hints and requeue pipelines if needed
    # Note: Eudoxia provides ExecutionResult with .ops (list), .failed(), .error, .ram, .cpu, .priority
    # We do not receive the pipeline object here; failures will manifest in pipeline runtime_status() as FAILED,
    # which is in ASSIGNABLE_STATES for retry. We just update hints for the ops that failed.
    for r in results:
        try:
            if r.failed() and (r.error is not None):
                # Heuristic: treat any failure containing "OOM" (case-insensitive) as OOM.
                err_str = str(r.error).lower()
                is_oom = ("oom" in err_str) or ("out of memory" in err_str) or ("memory" in err_str and "killed" in err_str)
                if is_oom:
                    for op in (r.ops or []):
                        # We assume op has an identifier; fallback to str(op)
                        op_id = getattr(op, "op_id", None)
                        if op_id is None:
                            op_id = getattr(op, "operator_id", None)
                        if op_id is None:
                            op_id = str(op)

                        # We don't have pipeline_id directly; best-effort: op may carry it
                        pipeline_id = getattr(op, "pipeline_id", None)
                        if pipeline_id is None:
                            # If unavailable, we cannot store per-pipeline hints; store global-ish by op_id keying
                            # using a sentinel pipeline_id from the result if present.
                            pipeline_id = getattr(r, "pipeline_id", None)
                        if pipeline_id is None:
                            pipeline_id = "__unknown_pipeline__"

                        prev_ram = r.ram if (getattr(r, "ram", None) is not None) else None
                        if prev_ram is None:
                            # Fall back to last_alloc, then a small default will be applied later.
                            prev_ram = s.last_alloc.get((pipeline_id, op_id), (None, None))[1]
                        if prev_ram is None:
                            # Can't infer; use minimal baseline from pool max later.
                            prev_ram = 0.0

                        pool = s.executor.pools[r.pool_id]
                        next_ram = _apply_oom_backoff(s, pool, pipeline_id, op_id, max(prev_ram, pool.max_ram_pool * s.min_ram_frac))
                        s.ram_hint[(pipeline_id, op_id)] = next_ram
        except Exception:
            # Never let hinting break scheduling
            pass

        # Track last allocation used for the ops in this result (even on success)
        try:
            for op in (r.ops or []):
                op_id = getattr(op, "op_id", None)
                if op_id is None:
                    op_id = getattr(op, "operator_id", None)
                if op_id is None:
                    op_id = str(op)
                pipeline_id = getattr(op, "pipeline_id", None)
                if pipeline_id is None:
                    pipeline_id = getattr(r, "pipeline_id", None)
                if pipeline_id is None:
                    pipeline_id = "__unknown_pipeline__"
                s.last_alloc[(pipeline_id, op_id)] = (getattr(r, "cpu", None), getattr(r, "ram", None))
        except Exception:
            pass

    # Early exit if nothing to do
    if not pipelines and not results and not _iter_queues_nonempty(s):
        return [], []

    suspensions = []
    assignments = []

    # Scheduling loop per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try multiple scheduling attempts per pool until we can't place more
        placed_any = True
        while placed_any:
            placed_any = False

            queue_name, pipeline = _dequeue_in_order(s)
            if pipeline is None:
                break

            status = pipeline.runtime_status()

            # Drop completed pipelines; for failed ops, allow retry (FAILED is in ASSIGNABLE_STATES)
            if status.is_pipeline_successful():
                continue

            # Find runnable ops whose parents are complete
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not op_list:
                # Nothing runnable yet; put pipeline back (end of same-priority queue)
                _requeue_back(s, queue_name, pipeline)
                continue

            # Bundle a small number of ops from same pipeline to reduce overhead, but only if we have headroom.
            # Start with 1 op for QUERY to minimize contention; allow 2 for others when resources permit.
            max_bundle = 1 if pipeline.priority == Priority.QUERY else s.max_ops_per_assignment
            chosen_ops = op_list[:max_bundle]

            # Determine RAM request as max of hints among bundled ops
            # Identify ops
            op_ids = []
            for op in chosen_ops:
                op_id = getattr(op, "op_id", None)
                if op_id is None:
                    op_id = getattr(op, "operator_id", None)
                if op_id is None:
                    op_id = str(op)
                op_ids.append(op_id)

            # Compute desired RAM
            desired_ram = 0.0
            for op_id in op_ids:
                desired_ram = max(desired_ram, _default_ram_request(s, pool, pipeline.pipeline_id, op_id))
            desired_ram = min(desired_ram, pool.max_ram_pool)

            # CPU policy: give QUERY more CPU to reduce latency; otherwise use what's available but cap to pool max.
            if pipeline.priority == Priority.QUERY:
                desired_cpu = min(pool.max_cpu_pool, max(1.0, avail_cpu))
            elif pipeline.priority == Priority.INTERACTIVE:
                desired_cpu = min(pool.max_cpu_pool, max(1.0, avail_cpu * 0.75))
            else:
                desired_cpu = min(pool.max_cpu_pool, max(1.0, avail_cpu * 0.50))

            # Apply batch headroom protection if higher-priority is waiting
            if pipeline.priority == Priority.BATCH_PIPELINE:
                cpu_budget, ram_budget = _pool_budget_for_batch(s, pool)
                # If batch is trying to take too much while others wait, downsize
                desired_cpu = min(desired_cpu, max(1.0, cpu_budget))
                desired_ram = min(desired_ram, max(pool.max_ram_pool * s.min_ram_frac, ram_budget))

            # Final clamp to current availability
            cpu_req = min(desired_cpu, avail_cpu)
            ram_req = min(desired_ram, avail_ram)

            # If we can't fit, consider preemption for high priority; otherwise requeue and stop using this pool.
            if cpu_req <= 0 or ram_req <= 0 or ram_req < desired_ram:
                # Try to preempt only for query/interactive when we truly have runnable work
                if s.preempt_enabled and pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                    need_cpu = max(0.0, desired_cpu - avail_cpu)
                    need_ram = max(0.0, desired_ram - avail_ram)
                    if need_cpu > 0 or need_ram > 0:
                        susp = _plan_preemptions_if_needed(s, pool_id, need_cpu, need_ram, _prio_rank(pipeline.priority))
                        if susp:
                            suspensions.extend(susp)
                            # Put pipeline back to the front to retry next tick after preemption takes effect
                            _requeue_front(s, queue_name, pipeline)
                            break

                # Can't place now; put back and stop further attempts on this pool this tick (avoid spinning)
                _requeue_front(s, queue_name, pipeline)
                break

            # Create assignment
            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Record the allocation hints for chosen ops (helps in absence of pipeline_id on results)
            for op_id in op_ids:
                s.last_alloc[(pipeline.pipeline_id, op_id)] = (cpu_req, ram_req)

            # Update available resources for further packing in this pool
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            placed_any = True

            # Requeue pipeline if it still has work (likely)
            # Keep it at the back of its priority queue to allow other pipelines of same priority to proceed.
            _requeue_back(s, queue_name, pipeline)

            # If pool resources are low, stop packing
            if avail_cpu <= 0 or avail_ram <= 0:
                break

    return suspensions, assignments

