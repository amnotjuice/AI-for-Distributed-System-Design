# policy_key: scheduler_low_025
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042025
# generation_seconds: 58.59
# generated_at: 2026-04-09T21:22:22.731089
@register_scheduler_init(key="scheduler_low_025")
def scheduler_low_025_init(s):
    """Priority-aware, failure-averse scheduler.

    Goals (aligned with weighted-latency objective):
      - Strongly protect QUERY and INTERACTIVE latency with preferential admission and larger resource slices.
      - Reduce OOM-driven failures by applying per-pipeline RAM backoff on failures and by not under-allocating RAM.
      - Avoid starvation by aging: long-waiting pipelines are periodically promoted ahead of lower-importance work.
      - Keep implementation simple and robust: schedule one ready operator at a time per assignment.

    Key ideas:
      - Three FIFO queues (QUERY / INTERACTIVE / BATCH).
      - Pool-level soft reservations: batch cannot consume the last slice of CPU/RAM in a pool.
      - Per-pipeline RAM "boost" multiplier that increases after failures and decays slowly after successes.
      - Lightweight aging: if a pipeline waits too long, it can be scheduled ahead of batch (or ahead of interactive).
    """
    from collections import deque

    s.tick = 0

    # Waiting queues by priority
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track when we first enqueued a pipeline (for aging / anti-starvation)
    s.enqueued_at = {}  # pipeline_id -> tick

    # Per-pipeline resource hints / backoff
    s.ram_boost = {}  # pipeline_id -> float (>=1.0)
    s.cpu_hint = {}   # pipeline_id -> last_cpu_used (best-effort)
    s.ram_hint = {}   # pipeline_id -> last_ram_used (best-effort)

    # Failure tracking to avoid infinite thrash on deterministic failures
    s.fail_count = {}     # pipeline_id -> int
    s.max_retries = 5     # keep modest; failures are penalized, but thrash is worse

    # Map operator objects in results back to pipeline_id (since results may not include pipeline_id)
    s.opkey_to_pipeline = {}  # op_key -> pipeline_id


def _op_key(op):
    # Robust key across possible operator representations
    return (
        getattr(op, "operator_id", None)
        or getattr(op, "op_id", None)
        or getattr(op, "id", None)
        or id(op)
    )


def _queue_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _priority_rank(prio):
    # Lower is higher priority
    if prio == Priority.QUERY:
        return 0
    if prio == Priority.INTERACTIVE:
        return 1
    return 2


def _base_fractions_for_priority(prio):
    # Fractions of *pool max* we'd like to allocate to a single operator.
    # RAM has no performance benefit above minimum, but it prevents OOM; so keep RAM fairly generous.
    if prio == Priority.QUERY:
        return 0.75, 0.80  # cpu_frac, ram_frac
    if prio == Priority.INTERACTIVE:
        return 0.55, 0.70
    return 0.35, 0.55


def _reserve_fractions_for_batch():
    # Soft reservations per pool so batch can't eat the last headroom needed for QUERY/INTERACTIVE arrivals.
    return 0.20, 0.20  # reserve_cpu_frac, reserve_ram_frac


def _pick_next_pipeline(s):
    # Aging: after these many ticks, allow lower priority to bubble up to prevent starvation.
    # Also helps reduce 720s penalties from never-completing pipelines.
    AGE_PROMOTE_TO_INTERACTIVE = 50
    AGE_PROMOTE_TO_QUERY = 120

    # Collect candidates from the heads of queues (and potentially deeper for aging),
    # but keep it lightweight: scan up to K entries per queue.
    K = 8
    candidates = []

    def scan_queue(q, promote_rank_limit=None):
        # promote_rank_limit: if set, an aged pipeline can be treated as if it had that better rank
        # (e.g., promote batch to interactive).
        i = 0
        for p in q:
            if i >= K:
                break
            i += 1
            pid = p.pipeline_id
            enq = s.enqueued_at.get(pid, s.tick)
            wait = s.tick - enq
            base_rank = _priority_rank(p.priority)
            eff_rank = base_rank
            if promote_rank_limit is not None:
                # If it waited long enough, promote up to the specified rank
                if promote_rank_limit == 1 and wait >= AGE_PROMOTE_TO_INTERACTIVE:
                    eff_rank = min(eff_rank, 1)
                if promote_rank_limit == 0 and wait >= AGE_PROMOTE_TO_QUERY:
                    eff_rank = min(eff_rank, 0)
            candidates.append((eff_rank, wait, p))

    # Normal priority order, with promotions:
    # - batch can promote to interactive after AGE_PROMOTE_TO_INTERACTIVE
    # - interactive can promote to query after AGE_PROMOTE_TO_QUERY
    scan_queue(s.q_query, promote_rank_limit=0)
    scan_queue(s.q_interactive, promote_rank_limit=0)
    scan_queue(s.q_batch, promote_rank_limit=1)

    if not candidates:
        return None

    # Choose highest effective priority; tie-break by longest wait to reduce tail and avoid starvation.
    candidates.sort(key=lambda x: (x[0], -x[1]))
    return candidates[0][2]


def _rotate_queue_to_front(q, pipeline):
    # Move the chosen pipeline to the front, preserving relative order of others.
    if not q or q[0] is pipeline:
        return
    try:
        q.remove(pipeline)
        q.appendleft(pipeline)
    except ValueError:
        pass


def _size_resources(s, pool, pipeline, prio, allow_use_reserved):
    # Choose CPU/RAM for a single operator assignment in this pool.
    base_cpu_frac, base_ram_frac = _base_fractions_for_priority(prio)

    # Available headroom
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool

    # Batch is constrained by reservations; interactive/query can use all available.
    if not allow_use_reserved:
        r_cpu_frac, r_ram_frac = _reserve_fractions_for_batch()
        avail_cpu = max(0.0, avail_cpu - pool.max_cpu_pool * r_cpu_frac)
        avail_ram = max(0.0, avail_ram - pool.max_ram_pool * r_ram_frac)

    if avail_cpu <= 0 or avail_ram <= 0:
        return 0.0, 0.0

    pid = pipeline.pipeline_id
    boost = float(s.ram_boost.get(pid, 1.0))

    # Start from a fraction of pool max, then cap by currently available.
    target_cpu = min(avail_cpu, max(1.0, pool.max_cpu_pool * base_cpu_frac))
    target_ram = min(avail_ram, max(1.0, pool.max_ram_pool * base_ram_frac))

    # Apply RAM boost after failures (OOM-avoidance); cap by available.
    target_ram = min(avail_ram, max(1.0, target_ram * boost))

    # If we have last-seen hints (from results), respect them as minima where possible.
    # This reduces the chance we oscillate down and OOM again.
    hint_cpu = s.cpu_hint.get(pid, None)
    hint_ram = s.ram_hint.get(pid, None)
    if hint_cpu is not None:
        target_cpu = min(avail_cpu, max(target_cpu, float(hint_cpu)))
    if hint_ram is not None:
        target_ram = min(avail_ram, max(target_ram, float(hint_ram)))

    # Avoid over-allocating CPU when RAM is tight (helps reduce "incomplete pipeline" penalties by keeping more concurrency).
    # Simple heuristic: if we can only give <40% of desired RAM fraction, cap CPU a bit.
    if target_ram < pool.max_ram_pool * 0.40:
        target_cpu = min(target_cpu, max(1.0, pool.max_cpu_pool * 0.35))

    return float(target_cpu), float(target_ram)


@register_scheduler(key="scheduler_low_025")
def scheduler_low_025(s, results, pipelines):
    """
    Step function for the scheduler.

    Produces:
      - suspensions: (unused in this version to avoid churn)
      - assignments: a set of (ops, cpu, ram, priority, pool_id, pipeline_id)

    Behavior:
      - Enqueue new pipelines by priority.
      - Process execution results: update per-pipeline RAM boost on failures; decay boost slowly on successes.
      - For each pool, schedule at most a few operators (one-op assignments) while respecting reservations.
      - Prefer QUERY > INTERACTIVE > BATCH with aging promotions.
    """
    s.tick += 1

    # Enqueue arriving pipelines
    for p in pipelines:
        q = _queue_for_priority(s, p.priority)
        q.append(p)
        s.enqueued_at.setdefault(p.pipeline_id, s.tick)
        s.ram_boost.setdefault(p.pipeline_id, 1.0)
        s.fail_count.setdefault(p.pipeline_id, 0)

    # Fast path: nothing changed
    if not pipelines and not results:
        return [], []

    # Update hints/backoff based on results
    for r in results:
        # Update last used sizing (best effort)
        pid = None
        if hasattr(r, "ops") and r.ops:
            pid = s.opkey_to_pipeline.get(_op_key(r.ops[0]), None)

        if pid is not None:
            s.cpu_hint[pid] = getattr(r, "cpu", s.cpu_hint.get(pid, None))
            s.ram_hint[pid] = getattr(r, "ram", s.ram_hint.get(pid, None))

        if r.failed():
            if pid is not None:
                s.fail_count[pid] = int(s.fail_count.get(pid, 0)) + 1
                # Strongly bias toward avoiding repeated OOM: ramp RAM boost quickly.
                # Cap boost to prevent requesting beyond pool max (still capped by avail at assignment time).
                cur = float(s.ram_boost.get(pid, 1.0))
                new = min(4.0, max(cur, 1.0) * 1.6)
                s.ram_boost[pid] = new
        else:
            # On success, decay RAM boost slowly so we don't regress into OOMs.
            if pid is not None:
                cur = float(s.ram_boost.get(pid, 1.0))
                s.ram_boost[pid] = max(1.0, cur * 0.97)

    suspensions = []
    assignments = []

    # Helper: drop completed pipelines from the front of a queue, and requeue in-place as needed.
    def clean_queue_front(q):
        while q:
            p0 = q[0]
            st = p0.runtime_status()
            if st.is_pipeline_successful():
                q.popleft()
                continue
            # If it's already irrecoverably failed (too many retries), stop scheduling it to avoid wasting capacity.
            # It will likely be counted as failed/incomplete by the simulator.
            if s.fail_count.get(p0.pipeline_id, 0) > s.max_retries:
                q.popleft()
                continue
            break

    # Before scheduling, clean obvious completions
    clean_queue_front(s.q_query)
    clean_queue_front(s.q_interactive)
    clean_queue_front(s.q_batch)

    # Schedule across pools
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Per-pool loop: attempt a small number of placements to avoid over-scheduling a single pool.
        # (We rely on the simulator to model execution; this just fills available headroom.)
        max_assignments_per_pool = 4
        made = 0

        while made < max_assignments_per_pool:
            # If pool is empty, stop
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

            # Pick the next pipeline globally (priority + aging), then rotate it to the front of its queue.
            p = _pick_next_pipeline(s)
            if p is None:
                break

            q = _queue_for_priority(s, p.priority)
            _rotate_queue_to_front(q, p)
            clean_queue_front(q)
            if not q:
                continue

            pipeline = q[0]
            status = pipeline.runtime_status()

            # Do not drop on failures: let it retry, but if it has too many failures, stop trying.
            if s.fail_count.get(pipeline.pipeline_id, 0) > s.max_retries:
                q.popleft()
                continue

            # Identify one ready operator (parents completed)
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; rotate to back to avoid blocking the queue head.
                q.rotate(-1)
                # If all are blocked, we'll eventually stop due to max_assignments_per_pool.
                made += 1  # count as an iteration to avoid infinite loops
                continue

            prio = pipeline.priority

            # Batch uses reservations; query/interactive can consume reserved headroom.
            allow_use_reserved = (prio != Priority.BATCH_PIPELINE)

            cpu, ram = _size_resources(s, pool, pipeline, prio, allow_use_reserved)
            if cpu <= 0 or ram <= 0:
                # Can't place anything meaningful here right now
                break

            # Create assignment
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Track mapping from this operator to pipeline_id for future result processing
            s.opkey_to_pipeline[_op_key(op_list[0])] = pipeline.pipeline_id

            # Rotate to back so other pipelines can make progress (reduces starvation/incompletes)
            q.rotate(-1)

            made += 1

    return suspensions, assignments
