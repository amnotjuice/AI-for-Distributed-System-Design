# policy_key: scheduler_medium_049
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.088561
# generation_seconds: 64.51
# generated_at: 2026-04-09T23:36:27.489849
@register_scheduler_init(key="scheduler_medium_049")
def scheduler_medium_049_init(s):
    """
    Priority-aware, failure-avoiding, round-robin scheduler with:
      - Strict preference for QUERY > INTERACTIVE > BATCH (to minimize weighted latency)
      - Soft reservations so batch cannot consume all headroom (protects tail latency)
      - OOM-adaptive RAM sizing (retry failed ops with larger RAM to avoid 720s penalties)
      - Simple starvation guard for batch via aging (avoid indefinite incompletion penalties)
    """
    from collections import deque

    # Per-priority round-robin queues of pipelines.
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track which pipelines are currently enqueued to avoid duplicates across ticks.
    s.enqueued_pipeline_ids = set()

    # Bookkeeping to implement simple aging / starvation prevention.
    s.step = 0
    s.pipeline_arrival_step = {}  # pipeline_id -> step first seen

    # Per-operator RAM learning keyed by python object identity of the op.
    # We only learn from allocations (success/failure) since "true peak" is unknown.
    s.op_ram_guess = {}           # op_key -> ram to try next time (bytes/units per simulator)
    s.op_ram_success_cap = {}     # op_key -> smallest known successful ram (upper bound on needed)
    s.op_attempts = {}            # op_key -> retry count (failed attempts observed)


def _sm49_op_key(op):
    # Prefer stable ids if present; otherwise fall back to object identity.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return (attr, v)
            except Exception:
                pass
    return ("py_id", id(op))


def _sm49_is_high_priority(priority):
    return priority in (Priority.QUERY, Priority.INTERACTIVE)


def _sm49_queue_for(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _sm49_try_enqueue(s, pipeline):
    # Skip if already enqueued.
    if pipeline.pipeline_id in s.enqueued_pipeline_ids:
        return
    # Skip if already complete.
    try:
        if pipeline.runtime_status().is_pipeline_successful():
            return
    except Exception:
        pass

    q = _sm49_queue_for(s, pipeline.priority)
    q.append(pipeline)
    s.enqueued_pipeline_ids.add(pipeline.pipeline_id)
    s.pipeline_arrival_step.setdefault(pipeline.pipeline_id, s.step)


def _sm49_dequeue_one(q, s):
    # Pops one pipeline and clears its "enqueued" flag; caller must re-enqueue if still active.
    pipeline = q.popleft()
    s.enqueued_pipeline_ids.discard(pipeline.pipeline_id)
    return pipeline


def _sm49_pick_queue(s):
    # Strict priority with aging guard for batch to avoid indefinite waiting/incompletion penalties.
    # If a batch pipeline has waited "too long", let it compete ahead of interactive when no queries.
    if s.q_query:
        return s.q_query

    if s.q_interactive and s.q_batch:
        # Find if the oldest batch is very old; if so, schedule batch first occasionally.
        # We only peek at leftmost (oldest under RR requeue) for O(1) cost.
        oldest_batch = s.q_batch[0]
        waited = s.step - s.pipeline_arrival_step.get(oldest_batch.pipeline_id, s.step)
        if waited >= 50:
            return s.q_batch
        return s.q_interactive

    if s.q_interactive:
        return s.q_interactive
    if s.q_batch:
        return s.q_batch
    return None


def _sm49_size_for_op(s, pool, pipeline_priority, op):
    """
    Choose RAM/CPU:
      - RAM: start conservatively (small) but quickly ramps on failure (from observed allocated RAM).
      - CPU: larger shares for QUERY/INTERACTIVE to reduce latency; batch gets smaller shares.
    """
    opk = _sm49_op_key(op)

    # CPU shares per priority; prefer low tail latency for query/interactive.
    if pipeline_priority == Priority.QUERY:
        cpu_share = 0.60
    elif pipeline_priority == Priority.INTERACTIVE:
        cpu_share = 0.45
    else:
        cpu_share = 0.25

    # Initial RAM guess if unknown: a small fraction of pool, but not too tiny.
    # (We rely on retry-on-failure to converge quickly without dropping pipelines.)
    default_ram = max(pool.max_ram_pool * 0.12, 1)

    ram_guess = s.op_ram_guess.get(opk, default_ram)

    # If we have seen a successful run, cap to the smallest successful allocation (if any).
    if opk in s.op_ram_success_cap:
        ram_guess = min(ram_guess, s.op_ram_success_cap[opk])

    # If this op has failed multiple times, escalate more aggressively.
    attempts = s.op_attempts.get(opk, 0)
    if attempts >= 2:
        ram_guess = max(ram_guess, pool.max_ram_pool * 0.30)
    if attempts >= 4:
        ram_guess = max(ram_guess, pool.max_ram_pool * 0.60)

    # Never exceed pool maximums.
    ram = min(ram_guess, pool.max_ram_pool)

    # CPU is scaled by pool max; cap by pool max.
    cpu = min(max(pool.max_cpu_pool * cpu_share, 1), pool.max_cpu_pool)

    return cpu, ram


@register_scheduler(key="scheduler_medium_049")
def scheduler_medium_049_scheduler(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    # Advance logical time.
    s.step += 1

    # Enqueue newly arrived pipelines.
    for p in pipelines:
        _sm49_try_enqueue(s, p)

    # Learn from execution results (primarily OOM failures) to reduce future failures/incompletions.
    for r in results:
        # Update RAM estimates per-op based on outcomes.
        try:
            ops = r.ops or []
        except Exception:
            ops = []

        for op in ops:
            opk = _sm49_op_key(op)

            if r.failed():
                # Treat any failure as "needs more RAM" (typical in this simulator).
                # Double last allocation, bounded by pool maximum if available.
                s.op_attempts[opk] = s.op_attempts.get(opk, 0) + 1

                # Use the allocation the executor tried as a baseline, then increase.
                # If r.ram is missing/zero, fall back to existing guess.
                prev = s.op_ram_guess.get(opk, None)
                base = None
                try:
                    if r.ram is not None and r.ram > 0:
                        base = r.ram
                except Exception:
                    base = None
                if base is None:
                    base = prev if prev is not None else 1

                new_guess = max(base * 2, (prev * 1.5) if prev is not None else 1)

                # If we can see pool maximums, clamp to that pool's max.
                try:
                    pool = s.executor.pools[r.pool_id]
                    new_guess = min(new_guess, pool.max_ram_pool)
                except Exception:
                    pass

                s.op_ram_guess[opk] = new_guess
            else:
                # Success: record smallest successful RAM allocation as an upper bound.
                try:
                    ok_ram = r.ram
                except Exception:
                    ok_ram = None
                if ok_ram is not None and ok_ram > 0:
                    prev_cap = s.op_ram_success_cap.get(opk, None)
                    s.op_ram_success_cap[opk] = ok_ram if prev_cap is None else min(prev_cap, ok_ram)
                    # Also keep the next guess at/below the successful cap (helps packing).
                    prev_guess = s.op_ram_guess.get(opk, ok_ram)
                    s.op_ram_guess[opk] = min(prev_guess, s.op_ram_success_cap[opk])

    # Early exit if nothing to do.
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Soft reservation per pool to protect headroom for QUERY/INTERACTIVE.
    # Batch can use reserved headroom only when no high-priority work is waiting.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        reserve_cpu = max(pool.max_cpu_pool * 0.15, 0)
        reserve_ram = max(pool.max_ram_pool * 0.15, 0)

        # Keep scheduling while we have headroom.
        # Bound iterations to avoid pathological looping when pipelines have no assignable ops.
        max_iters = 64
        iters = 0

        while iters < max_iters:
            iters += 1

            if avail_cpu <= 0 or avail_ram <= 0:
                break

            q = _sm49_pick_queue(s)
            if q is None:
                break

            pipeline = _sm49_dequeue_one(q, s)

            # Drop completed pipelines from the queue (cleanup metadata).
            try:
                if pipeline.runtime_status().is_pipeline_successful():
                    s.pipeline_arrival_step.pop(pipeline.pipeline_id, None)
                    continue
            except Exception:
                pass

            status = pipeline.runtime_status()

            # Choose one assignable op (parents complete).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing ready right now; requeue so other pipelines can make progress.
                _sm49_try_enqueue(s, pipeline)
                continue

            op = op_list[0]

            # Enforce reservations: avoid letting batch consume last headroom if high-priority exists.
            high_waiting = bool(s.q_query or s.q_interactive)
            if (pipeline.priority == Priority.BATCH_PIPELINE) and high_waiting:
                # If scheduling this batch would dip into reserved headroom, skip it for now.
                if (avail_cpu <= reserve_cpu) or (avail_ram <= reserve_ram):
                    # Put it back and stop scheduling batch in this pool for this tick.
                    _sm49_try_enqueue(s, pipeline)
                    break

            cpu_req, ram_req = _sm49_size_for_op(s, pool, pipeline.priority, op)

            # Fit to remaining resources: for CPU, we can scale down to fit; for RAM, do not go below 1.
            cpu = min(cpu_req, avail_cpu)
            ram = min(ram_req, avail_ram)

            # If RAM doesn't fit at all, requeue and stop for this pool (can't place this op here now).
            if ram <= 0 or cpu <= 0:
                _sm49_try_enqueue(s, pipeline)
                break

            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            avail_cpu -= cpu
            avail_ram -= ram

            # Requeue the pipeline for subsequent operators (round-robin).
            _sm49_try_enqueue(s, pipeline)

    return suspensions, assignments
