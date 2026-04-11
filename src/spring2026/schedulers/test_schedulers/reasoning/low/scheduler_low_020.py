# policy_key: scheduler_low_020
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.057523
# generation_seconds: 45.99
# generated_at: 2026-04-09T21:18:20.396030
@register_scheduler_init(key="scheduler_low_020")
def scheduler_low_020_init(s):
    """Priority-protecting FIFO with conservative sizing and OOM backoff.

    Goals (aligned with weighted latency objective):
      - Keep QUERY/INTERACTIVE latency low via (a) priority queues, (b) pool preference,
        and (c) reserved headroom (soft) so batch doesn't crowd out urgent work.
      - Avoid failures (720s penalty) by starting with safe RAM, then increasing on OOM
        and retrying instead of abandoning pipelines.
      - Avoid starvation by aging BATCH_PIPELINE and allowing it to use preferred pools
        when they've been idle/underutilized for long enough.
    """
    # Separate FIFO queues per priority to reduce head-of-line blocking.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track when a pipeline was first seen (for aging / anti-starvation).
    s.first_seen_tick = {}  # pipeline_id -> tick
    s.tick = 0

    # OOM backoff state:
    # - Per pipeline/op "key" store a RAM multiplier and a minimum RAM floor.
    s.ram_mult = {}   # (pipeline_id, op_key) -> float multiplier
    s.ram_floor = {}  # (pipeline_id, op_key) -> float minimum ram to request next time

    # Track last known "good" RAM for a pipeline/op (if it succeeded).
    s.ram_good = {}   # (pipeline_id, op_key) -> float

    # Soft reservations for high-priority work (fraction of pool capacity kept free).
    # These are "admission controls" rather than hard partitions.
    s.reserve_frac_query = 0.30
    s.reserve_frac_interactive = 0.20

    # Preferred pool for urgent work (if multiple pools exist).
    s.preferred_pool_urgent = 0

    # Anti-starvation: allow batch into urgent pool after this many ticks waiting.
    s.batch_aging_ticks = 30

    # Keep batch from flooding: cap batch concurrency by limiting batch assignments per tick.
    s.max_batch_assignments_per_tick = 2


def _priority_rank(p):
    if p == Priority.QUERY:
        return 0
    if p == Priority.INTERACTIVE:
        return 1
    return 2


def _enqueue_pipeline(s, pipeline):
    pid = pipeline.pipeline_id
    if pid not in s.first_seen_tick:
        s.first_seen_tick[pid] = s.tick

    if pipeline.priority == Priority.QUERY:
        s.q_query.append(pipeline)
    elif pipeline.priority == Priority.INTERACTIVE:
        s.q_interactive.append(pipeline)
    else:
        s.q_batch.append(pipeline)


def _op_key(op):
    # Try common fields first; fall back to stable-ish repr.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            if v is not None:
                return str(v)
    return repr(op)


def _iter_queues_in_order(s):
    # Yield (queue_name, queue_list) in strict priority order.
    yield "query", s.q_query
    yield "interactive", s.q_interactive
    yield "batch", s.q_batch


def _choose_pools_for_priority(s, priority):
    # Prefer pool 0 for urgent work if available; otherwise consider all pools.
    n = s.executor.num_pools
    if n <= 1:
        return list(range(n))

    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        # Try urgent pool first, then others in order of available headroom.
        other = [i for i in range(n) if i != s.preferred_pool_urgent]
        return [s.preferred_pool_urgent] + other

    # Batch: prefer non-urgent pools first.
    other = [i for i in range(n) if i != s.preferred_pool_urgent]
    return other + [s.preferred_pool_urgent]


def _soft_reserved_resources(s, pool_id):
    pool = s.executor.pools[pool_id]
    # Reserve some capacity for urgent classes.
    res_cpu = pool.max_cpu_pool * (s.reserve_frac_query + s.reserve_frac_interactive)
    res_ram = pool.max_ram_pool * (s.reserve_frac_query + s.reserve_frac_interactive)
    return res_cpu, res_ram


def _allowed_for_batch_in_pool(s, pool_id):
    # Batch allowed to use pool capacity excluding reserved headroom (soft reservation).
    pool = s.executor.pools[pool_id]
    res_cpu, res_ram = _soft_reserved_resources(s, pool_id)
    return max(0.0, pool.avail_cpu_pool - res_cpu), max(0.0, pool.avail_ram_pool - res_ram)


def _allowed_for_priority_in_pool(s, priority, pool_id):
    pool = s.executor.pools[pool_id]
    if priority == Priority.QUERY:
        # Query can use everything; but we'll still keep a tiny safety buffer to reduce OOM risk.
        return max(0.0, pool.avail_cpu_pool), max(0.0, pool.avail_ram_pool)
    if priority == Priority.INTERACTIVE:
        # Interactive can use everything except query's reserved slice (softly).
        res_cpu = pool.max_cpu_pool * s.reserve_frac_query
        res_ram = pool.max_ram_pool * s.reserve_frac_query
        return max(0.0, pool.avail_cpu_pool - res_cpu), max(0.0, pool.avail_ram_pool - res_ram)
    # Batch:
    return _allowed_for_batch_in_pool(s, pool_id)


def _cpu_request_for_priority(avail_cpu, pool_max_cpu, priority):
    # Favor fast completion for query/interactive; keep batch modest.
    if avail_cpu <= 0:
        return 0.0
    if priority == Priority.QUERY:
        return max(1.0, min(avail_cpu, max(1.0, 0.75 * pool_max_cpu)))
    if priority == Priority.INTERACTIVE:
        return max(1.0, min(avail_cpu, max(1.0, 0.50 * pool_max_cpu)))
    return max(1.0, min(avail_cpu, max(1.0, 0.25 * pool_max_cpu)))


def _base_ram_request(avail_ram, pool_max_ram, priority):
    # Start with a conservative-but-not-extreme fraction to avoid early OOMs.
    if avail_ram <= 0:
        return 0.0
    if priority == Priority.QUERY:
        return max(0.0, min(avail_ram, max(0.0, 0.60 * pool_max_ram)))
    if priority == Priority.INTERACTIVE:
        return max(0.0, min(avail_ram, max(0.0, 0.50 * pool_max_ram)))
    return max(0.0, min(avail_ram, max(0.0, 0.35 * pool_max_ram)))


def _ram_request_with_backoff(s, pipeline_id, op, base_ram, avail_ram):
    # If we saw OOM before, increase RAM for this pipeline/op.
    key = (pipeline_id, _op_key(op))
    mult = s.ram_mult.get(key, 1.0)
    floor = s.ram_floor.get(key, 0.0)

    # Prefer last known good RAM (if any) as a floor.
    good = s.ram_good.get(key, 0.0)
    floor = max(floor, good)

    req = base_ram * mult
    req = max(req, floor)

    # Never ask for more than currently available.
    return max(0.0, min(req, avail_ram))


def _is_probable_oom(err):
    if err is None:
        return False
    txt = str(err).lower()
    return ("oom" in txt) or ("out of memory" in txt) or ("killed" in txt and "memory" in txt)


@register_scheduler(key="scheduler_low_020")
def scheduler_low_020(s, results, pipelines):
    """
    Scheduling step:
      - Ingest new pipelines into per-priority FIFO queues.
      - Update OOM backoff using execution results.
      - For each pool, assign at most one ready operator per loop iteration, choosing:
          * QUERY first, then INTERACTIVE, then BATCH (with aging).
          * Prefer urgent pool for urgent work; batch prefers non-urgent pools.
          * Size CPU/RAM by priority, with RAM backoff on OOM.
    """
    s.tick += 1

    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update RAM backoff / good-RAM hints based on results.
    # Note: results may include multiple ops; we treat OOM as signal to retry with more RAM.
    for r in results:
        if not getattr(r, "ops", None):
            continue
        # We assume all ops in an assignment shared the same cpu/ram.
        # Record good RAM for success; back off for OOM-like failures.
        for op in r.ops:
            key = (getattr(op, "pipeline_id", None), _op_key(op))
            # If op doesn't carry pipeline_id, we can't key it reliably; skip.
            if key[0] is None:
                continue

            if r.failed():
                if _is_probable_oom(getattr(r, "error", None)):
                    # Increase multiplier and set floor above last attempted RAM.
                    cur_mult = s.ram_mult.get(key, 1.0)
                    # Gentle exponential backoff to avoid repeated 720s penalties.
                    s.ram_mult[key] = min(8.0, max(1.25, cur_mult * 1.7))
                    attempted_ram = float(getattr(r, "ram", 0.0) or 0.0)
                    if attempted_ram > 0:
                        s.ram_floor[key] = max(s.ram_floor.get(key, 0.0), attempted_ram * 1.15)
            else:
                attempted_ram = float(getattr(r, "ram", 0.0) or 0.0)
                if attempted_ram > 0:
                    s.ram_good[key] = max(s.ram_good.get(key, 0.0), attempted_ram)
                    # Reduce multiplier slowly after success to improve packing/utilization.
                    cur_mult = s.ram_mult.get(key, 1.0)
                    if cur_mult > 1.0:
                        s.ram_mult[key] = max(1.0, cur_mult * 0.90)

    # Early exit if nothing changed; still keep deterministic behavior.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper to pop a runnable pipeline from a queue (FIFO) while skipping completed/failed.
    def pop_next_runnable(q):
        requeue = []
        chosen = None
        while q:
            pipe = q.pop(0)
            status = pipe.runtime_status()
            if status.is_pipeline_successful():
                continue
            # We avoid dropping failed pipelines aggressively; but if the pipeline has any FAILED ops,
            # we keep it in the queue so it can retry with higher RAM.
            chosen = pipe
            break
        # Put back any examined-but-not-chosen pipelines (none in this simple pop).
        if requeue:
            q[:0] = requeue
        return chosen

    # Pick an assignable op list (one op) from pipeline with parents complete.
    def get_one_ready_op(pipe):
        status = pipe.runtime_status()
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        return op_list

    # Batch aging: treat very old batch as interactive for admission to urgent pool.
    def batch_is_aged(pipe):
        pid = pipe.pipeline_id
        start = s.first_seen_tick.get(pid, s.tick)
        return (s.tick - start) >= s.batch_aging_ticks

    # Build per-tick budget for batch to reduce impact on urgent latency.
    batch_assigned_this_tick = 0

    # Iterate pools; try to place urgent work first in each pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # If pool has no resources, skip.
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Try multiple attempts in the same pool, but stop if nothing fits.
        made_progress = True
        attempts = 0
        while made_progress and attempts < 8:
            attempts += 1
            made_progress = False

            # Create a candidate list in order, with batch aging considered.
            candidates = []

            # Query and interactive always considered.
            if s.q_query:
                candidates.append(("query", s.q_query, Priority.QUERY))
            if s.q_interactive:
                candidates.append(("interactive", s.q_interactive, Priority.INTERACTIVE))

            # Batch considered if budget allows.
            if s.q_batch and batch_assigned_this_tick < s.max_batch_assignments_per_tick:
                candidates.append(("batch", s.q_batch, Priority.BATCH_PIPELINE))

            # If no candidates, break.
            if not candidates:
                break

            # For this pool, attempt to schedule the highest-priority feasible pipeline.
            scheduled_any = False
            for qname, qref, prio in candidates:
                # Batch on urgent pool is restricted unless aged.
                if prio == Priority.BATCH_PIPELINE and s.executor.num_pools > 1 and pool_id == s.preferred_pool_urgent:
                    # Only allow aged batch into urgent pool.
                    # Peek without popping: scan a few items to find an aged one.
                    idx = None
                    for i in range(min(8, len(qref))):
                        if batch_is_aged(qref[i]):
                            idx = i
                            break
                    if idx is None:
                        continue
                    pipe = qref.pop(idx)
                else:
                    pipe = pop_next_runnable(qref)
                    if pipe is None:
                        continue

                # Choose pools preference: if this pipeline would prefer a different pool, requeue it.
                preferred_pools = _choose_pools_for_priority(s, pipe.priority)
                if preferred_pools and pool_id != preferred_pools[0]:
                    # If pool isn't the top preference and we have multiple pools, try next pipeline.
                    # Requeue at head to preserve FIFO-ish behavior.
                    qref.insert(0, pipe)
                    continue

                # Find a ready operator.
                ops = get_one_ready_op(pipe)
                if not ops:
                    # Nothing ready yet; requeue at tail.
                    qref.append(pipe)
                    continue

                # Check allowed resources (soft reservations).
                allow_cpu, allow_ram = _allowed_for_priority_in_pool(s, pipe.priority, pool_id)
                if allow_cpu <= 0 or allow_ram <= 0:
                    # Can't admit now; requeue at head to preserve urgency.
                    qref.insert(0, pipe)
                    continue

                # Decide CPU/RAM request.
                cpu_req = _cpu_request_for_priority(allow_cpu, pool.max_cpu_pool, pipe.priority)
                base_ram = _base_ram_request(allow_ram, pool.max_ram_pool, pipe.priority)
                ram_req = _ram_request_with_backoff(s, pipe.pipeline_id, ops[0], base_ram, allow_ram)

                # Ensure minimum meaningful request.
                if cpu_req <= 0 or ram_req <= 0:
                    qref.insert(0, pipe)
                    continue

                # Make assignment.
                assignments.append(
                    Assignment(
                        ops=ops,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=pipe.priority,
                        pool_id=pool_id,
                        pipeline_id=pipe.pipeline_id,
                    )
                )

                # Requeue the pipeline so subsequent ops can be scheduled later.
                qref.append(pipe)

                if prio == Priority.BATCH_PIPELINE:
                    batch_assigned_this_tick += 1

                made_progress = True
                scheduled_any = True
                break  # schedule one per attempt loop iteration

            if not scheduled_any:
                break

    return suspensions, assignments
