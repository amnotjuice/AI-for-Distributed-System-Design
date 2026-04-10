# policy_key: scheduler_est_018
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.125292
# generation_seconds: 111.26
# generated_at: 2026-03-31T18:56:16.429419
@register_scheduler_init(key="scheduler_est_018")
def scheduler_est_018_init(s):
    """
    Priority-aware, estimate-guided packing scheduler (small, incremental improvement over naive FIFO).

    Key changes vs. naive:
      1) Priority queues: always try QUERY > INTERACTIVE > BATCH.
      2) Right-size RAM using op.estimate.mem_peak_gb (+ small slack), rather than grabbing the whole pool.
      3) Limit CPU per op (by priority) to allow concurrency and reduce head-of-line blocking.
      4) Simple OOM adaptation: if we observe/assume OOM, bump per-op memory multiplier and retry (bounded).
      5) Soft reservations: when higher-priority work is waiting, keep some headroom so low-priority work
         doesn't consume the entire pool and spike tail latency.
    """
    import collections

    # Priority order (higher first)
    s.prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Per-priority FIFO queues of pipelines
    s.queues = {p: collections.deque() for p in s.prio_order}

    # Per-tick: avoid scheduling multiple ops from the same pipeline in the same tick (fairness)
    s._scheduled_this_tick = set()

    # Memory sizing knobs
    s.base_mem_factor = 1.10          # baseline multiplier on op.estimate.mem_peak_gb
    s.max_mem_factor = 3.50           # cap after repeated OOMs
    s.oom_bump = 1.45                 # multiplicative bump on OOM
    s.success_decay = 0.98            # gently decay factor on success (never below base)
    s.min_ram_gb = 0.25               # never allocate less than this

    # Extra slack per priority (reduce retries for higher-priority ops)
    s.mem_slack_gb = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.25,
        Priority.BATCH_PIPELINE: 0.00,
    }

    # CPU sizing knobs
    s.min_cpu = 1.0
    # Cap fraction of pool CPU per op (lets us pack multiple ops and avoid one op hogging everything)
    s.cpu_cap_frac = {
        Priority.QUERY: 1.00,         # let queries scale up if needed
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Soft reservation (only applied to lower priorities when higher-priority work is waiting)
    # Values are fractions of pool max resources to keep free.
    s.reserve_frac_cpu = {
        Priority.INTERACTIVE: 0.15,   # reserve for QUERY when scheduling INTERACTIVE
        Priority.BATCH_PIPELINE: 0.35 # reserve for QUERY/INTERACTIVE when scheduling BATCH
    }
    s.reserve_frac_ram = {
        Priority.INTERACTIVE: 0.15,
        Priority.BATCH_PIPELINE: 0.35
    }

    # Track per-op memory factor and retry counts
    s.mem_factor = {}   # op_key -> factor
    s.retries = {}      # op_key -> count
    s.max_retries = 4

    # Fallback estimate when op.estimate.mem_peak_gb is missing
    s.default_mem_peak_gb = 1.0


def _op_key(op):
    """Build a stable-ish key for an operator across scheduler calls without assuming a specific schema."""
    for attr in ("op_id", "operator_id", "id", "name", "key"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            try:
                return (attr, str(v))
            except Exception:
                pass
    # Last resort: object identity / repr; may be unstable across runs but still useful within a run
    try:
        return ("repr", repr(op))
    except Exception:
        return ("obj", str(id(op)))


def _mem_peak_est_gb(s, op):
    """Read op.estimate.mem_peak_gb if present; otherwise return a conservative default."""
    est = getattr(op, "estimate", None)
    v = getattr(est, "mem_peak_gb", None) if est is not None else None
    if v is None:
        return float(s.default_mem_peak_gb)
    try:
        return float(v)
    except Exception:
        return float(s.default_mem_peak_gb)


def _bump_mem_on_oom(s, op):
    """Increase the per-op memory factor and retry count, bounded."""
    k = _op_key(op)
    s.retries[k] = s.retries.get(k, 0) + 1
    cur = s.mem_factor.get(k, s.base_mem_factor)
    bumped = cur * s.oom_bump
    if bumped < s.base_mem_factor:
        bumped = s.base_mem_factor
    if bumped > s.max_mem_factor:
        bumped = s.max_mem_factor
    s.mem_factor[k] = bumped


def _decay_mem_on_success(s, op):
    """Slightly decay the memory factor on success (never below base)."""
    k = _op_key(op)
    cur = s.mem_factor.get(k, s.base_mem_factor)
    decayed = cur * s.success_decay
    if decayed < s.base_mem_factor:
        decayed = s.base_mem_factor
    s.mem_factor[k] = decayed


def _has_waiting(q):
    """True if queue has any pipelines at all (cheap proxy for pending work)."""
    return bool(q)


def _reserve_for(priority, pool, has_query_waiting, has_interactive_waiting, kind):
    """
    Compute soft reservation amount (CPU or RAM) for higher-priority work.
    Reservation only applies when there is higher-priority work waiting.
    """
    if priority == Priority.QUERY:
        return 0.0

    if priority == Priority.INTERACTIVE:
        # Reserve for QUERY only
        if not has_query_waiting:
            return 0.0
        frac = (pool.max_cpu_pool if kind == "cpu" else pool.max_ram_pool)
        share = (0.0 if kind == "cpu" else 0.0)
        # use configured shares
        if kind == "cpu":
            share = 0.15
        else:
            share = 0.15
        return frac * share

    # BATCH: reserve for QUERY or INTERACTIVE
    if priority == Priority.BATCH_PIPELINE:
        if not (has_query_waiting or has_interactive_waiting):
            return 0.0
        base = (pool.max_cpu_pool if kind == "cpu" else pool.max_ram_pool)
        share = 0.35
        return base * share

    return 0.0


def _looks_like_oom(err):
    if err is None:
        return False
    try:
        msg = str(err).upper()
    except Exception:
        return False
    return ("OOM" in msg) or ("OUT OF MEMORY" in msg) or ("MEMORY" in msg)


@register_scheduler(key="scheduler_est_018")
def scheduler_est_018_scheduler(s, results: List["ExecutionResult"],
                               pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Main scheduling loop:
      - ingest new pipelines into per-priority queues
      - update memory factors from execution results (OOM bumps, success decay)
      - for each pool, repeatedly try to place ready ops in strict priority order
        using estimate-guided RAM and capped CPU to pack multiple concurrent ops
    """
    # Ingest new pipelines
    for p in pipelines:
        pr = p.priority
        if pr not in s.queues:
            # Allow unknown priority values by creating a queue and placing it after known ones
            import collections
            s.queues[pr] = collections.deque()
            if pr not in s.prio_order:
                s.prio_order.append(pr)
        s.queues[pr].append(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Update adaptive memory factors from execution results
    for r in results:
        try:
            ops = list(getattr(r, "ops", []) or [])
        except Exception:
            ops = []
        if hasattr(r, "failed") and r.failed():
            if _looks_like_oom(getattr(r, "error", None)):
                for op in ops:
                    _bump_mem_on_oom(s, op)
        else:
            for op in ops:
                _decay_mem_on_success(s, op)

    # Avoid multiple ops from same pipeline within a single scheduler tick
    s._scheduled_this_tick = set()

    # Cheap proxies for "higher priority is waiting" (used for soft reservations)
    has_query_waiting = _has_waiting(s.queues.get(Priority.QUERY, []))
    has_interactive_waiting = _has_waiting(s.queues.get(Priority.INTERACTIVE, []))

    # Helper to try scheduling one op of a given priority into a pool
    def try_schedule_one(pool_id, pool, priority, avail_cpu, avail_ram):
        q = s.queues.get(priority, None)
        if not q:
            return None, avail_cpu, avail_ram

        # Soft reservation when scheduling lower priorities
        reserve_cpu = _reserve_for(priority, pool, has_query_waiting, has_interactive_waiting, kind="cpu")
        reserve_ram = _reserve_for(priority, pool, has_query_waiting, has_interactive_waiting, kind="ram")
        allowed_cpu = max(0.0, float(avail_cpu) - float(reserve_cpu))
        allowed_ram = max(0.0, float(avail_ram) - float(reserve_ram))

        if allowed_cpu < s.min_cpu or allowed_ram <= 0:
            return None, avail_cpu, avail_ram

        attempts = len(q)
        for _ in range(attempts):
            pipeline = q.popleft()

            # Skip if we've already scheduled something from this pipeline this tick
            if pipeline.pipeline_id in s._scheduled_this_tick:
                q.append(pipeline)
                continue

            status = pipeline.runtime_status()

            # Drop completed pipelines from the queue
            if status.is_pipeline_successful():
                continue

            # If there are FAILED ops, assume likely OOM and bump memory factor (bounded), retry later.
            # If it keeps failing too many times, drop the pipeline to avoid infinite churn.
            failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
            if failed_ops:
                drop = False
                for op in failed_ops:
                    _bump_mem_on_oom(s, op)
                    if s.retries.get(_op_key(op), 0) > s.max_retries:
                        drop = True
                if drop:
                    continue  # drop pipeline
                q.append(pipeline)  # keep it for retry
                continue

            # Find one ready op (parents complete)
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing ready yet; keep pipeline in queue
                q.append(pipeline)
                continue

            op = op_list[0]

            # Estimate RAM
            mem_est = _mem_peak_est_gb(s, op)
            factor = s.mem_factor.get(_op_key(op), s.base_mem_factor)
            slack = s.mem_slack_gb.get(priority, 0.0)
            ram_req = max(float(s.min_ram_gb), float(mem_est) * float(factor) + float(slack))
            if ram_req > float(pool.max_ram_pool):
                ram_req = float(pool.max_ram_pool)

            # CPU cap by priority and pool size; still bounded by allowed_cpu
            cap = float(pool.max_cpu_pool) * float(s.cpu_cap_frac.get(priority, 1.0))
            cpu_req = min(float(allowed_cpu), float(cap))
            cpu_req = max(float(s.min_cpu), float(cpu_req))

            # Fit check against allowed (post-reservation)
            if cpu_req > float(allowed_cpu) or ram_req > float(allowed_ram):
                # Can't fit now; requeue and try other pipelines in this priority
                q.append(pipeline)
                continue

            # Place assignment
            a = Assignment(
                ops=op_list,
                cpu=cpu_req,
                ram=ram_req,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            # Keep pipeline around for subsequent ops
            q.append(pipeline)
            s._scheduled_this_tick.add(pipeline.pipeline_id)
            return a, float(avail_cpu) - cpu_req, float(avail_ram) - ram_req

        return None, avail_cpu, avail_ram

    # For each pool, keep packing ops in strict priority order until we can't place anything else
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu < s.min_cpu or avail_ram <= 0:
            continue

        while True:
            made_progress = False

            # Strict priority: try higher priorities first each iteration
            for pr in s.prio_order:
                a, new_cpu, new_ram = try_schedule_one(pool_id, pool, pr, avail_cpu, avail_ram)
                if a is not None:
                    assignments.append(a)
                    avail_cpu, avail_ram = new_cpu, new_ram
                    made_progress = True
                    break

            if not made_progress:
                break

            if avail_cpu < s.min_cpu or avail_ram <= 0:
                break

    return suspensions, assignments
