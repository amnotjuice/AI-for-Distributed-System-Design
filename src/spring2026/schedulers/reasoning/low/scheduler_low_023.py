# policy_key: scheduler_low_023
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046048
# generation_seconds: 53.04
# generated_at: 2026-03-14T02:19:55.679706
@register_scheduler_init(key="scheduler_low_023")
def scheduler_low_023_init(s):
    """
    Priority-aware incremental improvement over naive FIFO.

    Changes vs. naive:
      1) Maintain separate queues per priority (QUERY > INTERACTIVE > BATCH_PIPELINE).
      2) Size allocations conservatively (avoid giving an operator the entire pool),
         which improves tail latency under concurrency by enabling more parallel starts.
      3) Basic OOM-aware RAM retry: on failure, increase the next requested RAM for that operator.
      4) Simple "headroom reservation": keep a small fraction of each pool for high priority work
         (QUERY/INTERACTIVE) by limiting batch allocations.

    Notes:
      - Avoids preemption because the minimal public surface here doesn't expose a robust way
        to discover currently-running containers to suspend safely.
      - Uses per-operator (best-effort) RAM hints keyed by object id(op).
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator RAM hint (bytes/MB units follow simulator's conventions; we just scale)
    # Keyed by id(op) to avoid requiring hashability.
    s.op_ram_hint = {}

    # Track last-seen pipelines so we can keep progressing even if no new arrivals this tick.
    s.known_pipelines = {}

    # Aging to avoid indefinite starvation: count "skips" for pipelines (lightweight).
    s.pipeline_skips = {}

    # Tunables (kept intentionally simple / conservative)
    s.min_cpu = 1.0
    s.batch_cpu_frac = 0.35          # batch gets smaller slices to preserve latency
    s.interactive_cpu_frac = 0.60
    s.query_cpu_frac = 0.75

    s.batch_ram_frac = 0.40
    s.interactive_ram_frac = 0.60
    s.query_ram_frac = 0.75

    # How much of a pool to keep available (in expectation) by limiting batch sizing.
    s.reserved_for_high_frac = 0.20

    # OOM backoff multiplier (applied on subsequent attempts)
    s.oom_ram_multiplier = 2.0
    s.max_hint_frac_of_pool = 0.95  # never request >95% of pool via hint alone


def _prio_rank(priority):
    # Higher number = higher priority
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1  # Priority.BATCH_PIPELINE (or unknown)


def _enqueue_pipeline(s, p):
    if p is None:
        return
    s.known_pipelines[p.pipeline_id] = p
    s.pipeline_skips.setdefault(p.pipeline_id, 0)
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _pipeline_done_or_failed(p):
    status = p.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If any operator has FAILED, treat pipeline as failed and do not retry at pipeline level.
    # (We still do operator-level RAM hinting; but without explicit retry semantics, drop it.)
    if status.state_counts[OperatorState.FAILED] > 0:
        return True
    return False


def _pop_next_pipeline_with_ready_op(s, require_parents_complete=True):
    """
    Return (pipeline, ready_op_list) using priority queues with simple aging.
    Ensures we don't get stuck on a pipeline that has no assignable ops.
    """
    # Priority order, with mild aging: after enough skips, allow lower-priority to run.
    queues = [s.q_query, s.q_interactive, s.q_batch]

    for _ in range(sum(len(q) for q in queues)):
        # Choose which queue to draw from:
        # If batch has been skipped too long globally, occasionally allow it.
        # Keep this simple: try high -> low, but don't spin forever.
        chosen_q = None
        for q in queues:
            if q:
                chosen_q = q
                break
        if chosen_q is None:
            return None, []

        p = chosen_q.pop(0)

        # Drop completed/failed pipelines
        if _pipeline_done_or_failed(p):
            continue

        status = p.runtime_status()
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=require_parents_complete)[:1]
        if ops:
            return p, ops

        # No ready ops; requeue and count skip.
        s.pipeline_skips[p.pipeline_id] = s.pipeline_skips.get(p.pipeline_id, 0) + 1

        # If it is blocked, put it back at the end of its own priority queue.
        _enqueue_pipeline(s, p)

    return None, []


def _cpu_frac_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s.query_cpu_frac
    if priority == Priority.INTERACTIVE:
        return s.interactive_cpu_frac
    return s.batch_cpu_frac


def _ram_frac_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s.query_ram_frac
    if priority == Priority.INTERACTIVE:
        return s.interactive_ram_frac
    return s.batch_ram_frac


def _compute_request(s, pool, priority, op):
    """
    Compute (cpu, ram) request for an operator given pool headroom and priority.
    Uses per-op RAM hint if present; otherwise uses a priority-based fraction of the pool.
    Also limits batch such that we probabilistically reserve some headroom for high priority.
    """
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool
    if avail_cpu <= 0 or avail_ram <= 0:
        return 0.0, 0.0

    # Base fractions
    cpu_frac = _cpu_frac_for_priority(s, priority)
    ram_frac = _ram_frac_for_priority(s, priority)

    # Headroom reservation: for batch, pretend we have less available to encourage leaving room.
    # This is a small improvement that reduces the chance a batch step grabs everything.
    eff_avail_cpu = avail_cpu
    eff_avail_ram = avail_ram
    if priority == Priority.BATCH_PIPELINE:
        eff_avail_cpu = max(0.0, avail_cpu - pool.max_cpu_pool * s.reserved_for_high_frac)
        eff_avail_ram = max(0.0, avail_ram - pool.max_ram_pool * s.reserved_for_high_frac)

    # If after reservation nothing remains, allow batch to still proceed minimally if truly idle.
    if priority == Priority.BATCH_PIPELINE and (eff_avail_cpu <= 0 or eff_avail_ram <= 0):
        # Only proceed if there are no high-priority pipelines waiting at all.
        if s.q_query or s.q_interactive:
            return 0.0, 0.0
        eff_avail_cpu = avail_cpu
        eff_avail_ram = avail_ram

    # CPU request: priority-based slice, bounded by available, and at least min_cpu if possible
    cpu_req = min(eff_avail_cpu, max(s.min_cpu, pool.max_cpu_pool * cpu_frac))
    if cpu_req > eff_avail_cpu:
        cpu_req = eff_avail_cpu

    # RAM request: use hint if we have one; else fraction-based.
    hint = s.op_ram_hint.get(id(op))
    if hint is None:
        ram_req = min(eff_avail_ram, pool.max_ram_pool * ram_frac)
    else:
        # Never request more than a safe fraction of the pool based on hint alone.
        ram_req = min(eff_avail_ram, min(hint, pool.max_ram_pool * s.max_hint_frac_of_pool))

    # If either resource can't meet minimal meaningful allocation, don't schedule.
    if cpu_req <= 0 or ram_req <= 0:
        return 0.0, 0.0

    return cpu_req, ram_req


@register_scheduler(key="scheduler_low_023")
def scheduler_low_023(s, results, pipelines):
    """
    Priority-aware scheduler:
      - Enqueue new pipelines into per-priority queues.
      - Update RAM hints on failures (OOM-agnostic but conservative: increase RAM for failed ops).
      - For each pool, greedily assign as many ready operators as fit, using conservative sizing.
      - Avoid allocating an entire pool to a single op by default to improve concurrency/latency.
    """
    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Incorporate results: update RAM hints on failure to reduce repeated OOMs.
    # We keep it conservative: if something failed, next time try more RAM for those ops.
    for r in results:
        if getattr(r, "failed", None) and r.failed():
            # Best-effort: if ops list exists, update each op's hint.
            ops = getattr(r, "ops", None) or []
            for op in ops:
                prev = s.op_ram_hint.get(id(op))
                base = prev if prev is not None else max(getattr(r, "ram", 0.0), 1.0)
                s.op_ram_hint[id(op)] = base * s.oom_ram_multiplier

    # Early exit only if nothing changed and we have nothing queued.
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Greedily fill each pool with ready ops, but keep sizing conservative.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Attempt to schedule multiple operators per tick per pool while resources allow.
        while True:
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            pipeline, op_list = _pop_next_pipeline_with_ready_op(s, require_parents_complete=True)
            if not pipeline or not op_list:
                break

            op = op_list[0]
            cpu_req, ram_req = _compute_request(s, pool, pipeline.priority, op)

            # If we couldn't get a viable request (e.g., due to reservation), requeue and stop.
            if cpu_req <= 0 or ram_req <= 0:
                _enqueue_pipeline(s, pipeline)
                break

            # If it still doesn't fit, requeue and stop trying this pool this tick.
            if cpu_req > pool.avail_cpu_pool or ram_req > pool.avail_ram_pool:
                _enqueue_pipeline(s, pipeline)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Requeue pipeline to continue progressing future ops in subsequent iterations/ticks.
            _enqueue_pipeline(s, pipeline)

            # Continue loop to see if we can fit another op in the same pool.

    return suspensions, assignments

