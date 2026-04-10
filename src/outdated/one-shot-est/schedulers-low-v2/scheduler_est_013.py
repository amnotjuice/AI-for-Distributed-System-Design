# policy_key: scheduler_est_013
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049172
# generation_seconds: 35.63
# generated_at: 2026-04-01T13:44:59.934810
@register_scheduler_init(key="scheduler_est_013")
def scheduler_est_013_init(s):
    """Priority-aware, conservative sizing scheduler (incremental improvement over naive FIFO).

    Key ideas (kept intentionally simple and robust):
      1) Maintain separate waiting queues per priority and always dispatch higher priority first.
      2) Avoid "give all resources to one op" by right-sizing CPU (cap per-op share),
         which reduces head-of-line blocking and improves latency under concurrency.
      3) Use memory estimate hints conservatively: allocate at least the hint (plus small headroom),
         and on any failure, bump the RAM request for that operator on retry (OOM-safe warm start).
      4) Prefer placing higher priority work in the pool with most available headroom.

    Notes:
      - No preemption yet (keeps churn low and behavior predictable).
      - Only schedules assignable ops whose parents are complete.
    """
    # Per-priority FIFO queues of pipelines
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator retry RAM inflation factors keyed by (pipeline_id, op_key)
    s.op_ram_multiplier = {}  # Dict[Tuple[str,int], float]

    # Simple knobs (start small; can evolve later)
    s.min_ram_gb = 0.25                # floor to avoid tiny allocations
    s.mem_hint_headroom = 1.15         # allocate a bit more than the estimator hint
    s.fail_ram_bump = 1.6              # multiply RAM on failure
    s.max_ram_multiplier = 16.0        # cap to avoid runaway allocations

    # CPU sizing caps: limit per-op CPU share to reduce HOL blocking
    # (higher priority gets a bigger cap)
    s.cpu_cap_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.40,
    }

    # How many ops max to schedule per pool per tick (keeps packing sane and avoids overcommit)
    s.max_assignments_per_pool = 4


def _priority_order():
    # Highest first
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _op_key(pipeline, op):
    # Try common fields if present; fall back to object identity (stable within a simulation run)
    if hasattr(op, "op_id"):
        return (pipeline.pipeline_id, getattr(op, "op_id"))
    if hasattr(op, "operator_id"):
        return (pipeline.pipeline_id, getattr(op, "operator_id"))
    if hasattr(op, "name"):
        return (pipeline.pipeline_id, hash(getattr(op, "name")))
    return (pipeline.pipeline_id, id(op))


def _desired_ram_gb(s, pipeline, op):
    # Conservative sizing: base on estimator hint (if present) times a modest headroom.
    hint = None
    if hasattr(op, "estimate") and op.estimate is not None and hasattr(op.estimate, "mem_peak_gb"):
        hint = op.estimate.mem_peak_gb

    base = s.min_ram_gb
    if isinstance(hint, (int, float)) and hint and hint > 0:
        base = max(base, float(hint) * s.mem_hint_headroom)

    mult = s.op_ram_multiplier.get(_op_key(pipeline, op), 1.0)
    return base * mult


def _desired_cpu(s, pool, priority):
    # Cap CPU per assignment to prevent one op from monopolizing a pool.
    cap = max(1.0, pool.max_cpu_pool * s.cpu_cap_frac.get(priority, 0.5))
    return cap


def _select_pool_for_priority(s, priority):
    # Prefer pool with maximum available CPU, tie-break with RAM.
    best_pool_id = None
    best_score = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue
        score = (pool.avail_cpu_pool, pool.avail_ram_pool)
        if best_score is None or score > best_score:
            best_score = score
            best_pool_id = pool_id
    return best_pool_id


def _enqueue_pipeline(s, pipeline):
    # Only enqueue if it still has work to do.
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return
    # If it has failed operators, we still allow retry (Eudoxia marks FAILED; example had an issue here).
    # We'll rely on our RAM bumping logic to make retries safer.
    s.waiting[pipeline.priority].append(pipeline)


@register_scheduler(key="scheduler_est_013")
def scheduler_est_013(s, results, pipelines):
    """
    Priority queues + conservative resource sizing (CPU cap, mem-hint + headroom, failure RAM bump).
    """
    # Ingest new pipelines into priority queues
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update RAM multipliers based on failures (treat all failures as potentially RAM-related)
    for r in results:
        if getattr(r, "failed", None) and r.failed():
            for op in getattr(r, "ops", []) or []:
                # We don't always have pipeline_id in result; but Assignment had it.
                # Fallback: use a sentinel pipeline_id from result if present.
                pipeline_id = getattr(r, "pipeline_id", None)
                if pipeline_id is None:
                    # Best-effort: can't safely key by pipeline_id; use op identity only.
                    key = ("__unknown_pipeline__", id(op))
                else:
                    # Prefer stable op_key behavior if we can reconstruct pipeline_id.
                    if hasattr(op, "op_id"):
                        key = (pipeline_id, getattr(op, "op_id"))
                    elif hasattr(op, "operator_id"):
                        key = (pipeline_id, getattr(op, "operator_id"))
                    elif hasattr(op, "name"):
                        key = (pipeline_id, hash(getattr(op, "name")))
                    else:
                        key = (pipeline_id, id(op))

                cur = s.op_ram_multiplier.get(key, 1.0)
                bumped = min(s.max_ram_multiplier, cur * s.fail_ram_bump)
                s.op_ram_multiplier[key] = bumped

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []  # no preemption in this iteration
    assignments = []

    # Build a quick helper to pop the next runnable op (parents complete) from queues
    def pop_next_runnable():
        # Iterate priorities high to low; within each, FIFO pipelines.
        for prio in _priority_order():
            q = s.waiting[prio]
            # Rotate through pipelines until we find one with an assignable op
            for _ in range(len(q)):
                pipeline = q.pop(0)
                status = pipeline.runtime_status()

                if status.is_pipeline_successful():
                    continue

                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                # Keep pipeline in queue regardless; we want progress across pipelines.
                q.append(pipeline)

                if op_list:
                    return pipeline, op_list[0]
        return None, None

    # Scheduling loop: attempt to place a few assignments per pool per tick.
    # We pick pools dynamically to give higher priority more headroom.
    per_pool_assigned = {pid: 0 for pid in range(s.executor.num_pools)}

    # Upper bound on how many total assignments we attempt in one tick
    total_budget = s.max_assignments_per_pool * max(1, s.executor.num_pools)

    for _ in range(total_budget):
        pipeline, op = pop_next_runnable()
        if pipeline is None:
            break

        # Choose a pool. For simplicity: always pick the pool with most headroom.
        pool_id = _select_pool_for_priority(s, pipeline.priority)
        if pool_id is None:
            break  # no capacity anywhere

        pool = s.executor.pools[pool_id]
        if per_pool_assigned[pool_id] >= s.max_assignments_per_pool:
            # Try again next iteration; capacity might exist in another pool
            # (but our pool selection picks max headroom; if it's capped, others likely are too).
            # Still, we won't hard-fail; just stop to avoid spinning.
            break

        # Decide RAM and CPU sizes
        want_ram = _desired_ram_gb(s, pipeline, op)
        want_cpu = _desired_cpu(s, pool, pipeline.priority)

        # Fit into available resources
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        cpu = min(pool.avail_cpu_pool, want_cpu)
        ram = min(pool.avail_ram_pool, want_ram)

        # If we can't allocate enough to be meaningful, skip for now.
        # (RAM in particular must be non-trivial to avoid immediate failures.)
        if cpu <= 0 or ram < s.min_ram_gb:
            # Leave it queued and try other work (if any).
            continue

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
        )
        per_pool_assigned[pool_id] += 1

        # Note: pool.avail_* won't update until executor applies assignments; we rely on
        # max_assignments_per_pool and conservative sizing to avoid gross over-scheduling.

    return suspensions, assignments
