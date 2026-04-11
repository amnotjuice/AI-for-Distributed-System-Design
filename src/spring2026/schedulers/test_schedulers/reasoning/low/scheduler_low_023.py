# policy_key: scheduler_low_023
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040961
# generation_seconds: 45.02
# generated_at: 2026-04-09T21:20:40.333004
@register_scheduler_init(key="scheduler_low_023")
def scheduler_low_023_init(s):
    """Priority-aware, OOM-adaptive, non-preemptive scheduler.

    Goals for the weighted-latency objective:
      - Strongly protect QUERY/INTERACTIVE latency via strict priority admission.
      - Avoid failures (720s penalty) by retrying failed/OOM operators with increased RAM.
      - Avoid starvation by occasionally servicing BATCH when it has waited "too long".

    Design choices (kept intentionally incremental vs FIFO baseline):
      - Separate queues per priority; strict priority most of the time.
      - RAM backoff on failures: per-pipeline RAM multiplier increases after failed results.
      - CPU sizing: give more CPU to high priority (faster), cap batch CPU to allow concurrency.
      - Pool selection: send high priority to the "best headroom" pool; batch uses remaining headroom.
      - No preemption (keeps churn low and avoids wasted work).
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track enqueue order to provide simple aging (avoid indefinite starvation).
    s._seq = 0
    s._enqueued_seq = {}  # pipeline_id -> seq number

    # Simple RAM backoff per pipeline. Multiplier applied to RAM target for its operators.
    s._ram_mult = {}  # pipeline_id -> float

    # Count failures observed per pipeline to escalate RAM.
    s._fail_count = {}  # pipeline_id -> int

    # Batch aging threshold: after this many enqueues since arrival, batch is occasionally served.
    s._batch_aging_threshold = 40

    # How aggressively to increase RAM after a failure. (Conservative to avoid pool exhaustion.)
    s._ram_backoff_factor = 1.5
    s._ram_backoff_cap = 8.0  # upper bound multiplier to avoid pathological over-allocation

    # Reserve fraction in each pool that batch should avoid consuming (for future high-priority bursts).
    s._batch_cpu_reserve_frac = 0.25
    s._batch_ram_reserve_frac = 0.25


def _q_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _prio_rank(prio):
    # Smaller is higher priority in our comparisons.
    if prio == Priority.QUERY:
        return 0
    if prio == Priority.INTERACTIVE:
        return 1
    return 2


def _is_probable_oom(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    # Heuristic: simulator/runtime may label OOM in different ways.
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)


def _select_pool_for_priority(s, prio):
    # Prefer the pool with maximum "headroom" score for high priority.
    # For batch, prefer the pool with most leftover after reserves.
    best_pool = None
    best_score = None

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            # Favor both CPU and RAM headroom.
            score = (avail_cpu / max(1.0, pool.max_cpu_pool)) + (avail_ram / max(1.0, pool.max_ram_pool))
        else:
            # Batch should avoid consuming reserved headroom.
            eff_cpu = max(0.0, avail_cpu - pool.max_cpu_pool * s._batch_cpu_reserve_frac)
            eff_ram = max(0.0, avail_ram - pool.max_ram_pool * s._batch_ram_reserve_frac)
            score = (eff_cpu / max(1.0, pool.max_cpu_pool)) + (eff_ram / max(1.0, pool.max_ram_pool))

        if best_score is None or score > best_score:
            best_score = score
            best_pool = pool_id

    return best_pool


def _cpu_target(s, pool, prio, avail_cpu):
    # CPU sizing: prioritize latency for query/interactive, keep batch smaller.
    # Using pool.max_cpu_pool as a stable anchor.
    if prio == Priority.QUERY:
        target = max(1.0, 0.75 * pool.max_cpu_pool)
    elif prio == Priority.INTERACTIVE:
        target = max(1.0, 0.50 * pool.max_cpu_pool)
    else:
        # Batch: smaller chunks improve fairness and keep headroom.
        target = max(1.0, 0.25 * pool.max_cpu_pool)

    return min(avail_cpu, target)


def _ram_target(s, pool, prio, avail_ram, pipeline_id):
    # RAM sizing: allocate enough to avoid OOM; we do not know true requirement,
    # so start conservative and increase after failures.
    mult = s._ram_mult.get(pipeline_id, 1.0)

    if prio == Priority.QUERY:
        base = 0.60 * pool.max_ram_pool
    elif prio == Priority.INTERACTIVE:
        base = 0.45 * pool.max_ram_pool
    else:
        base = 0.35 * pool.max_ram_pool

    target = base * mult
    # Clamp to pool capacity and current availability.
    target = min(target, pool.max_ram_pool, avail_ram)
    # Ensure positive.
    return max(0.0, target)


def _enqueue_pipeline(s, p):
    q = _q_for_priority(s, p.priority)
    s._seq += 1
    s._enqueued_seq[p.pipeline_id] = s._seq
    if p.pipeline_id not in s._ram_mult:
        s._ram_mult[p.pipeline_id] = 1.0
    if p.pipeline_id not in s._fail_count:
        s._fail_count[p.pipeline_id] = 0
    q.append(p)


def _pop_next_pipeline_with_aging(s):
    # Strict priority most of the time, but if batch has waited long enough, service it occasionally.
    # We define "waited long" by queue age measured in scheduler calls since enqueue.
    # This is intentionally simple and deterministic.
    if s.q_query:
        return s.q_query.popleft()
    if s.q_interactive:
        return s.q_interactive.popleft()

    if not s.q_batch:
        return None

    # If batch is only thing left, run it.
    if not s.q_query and not s.q_interactive:
        return s.q_batch.popleft()

    # Unreachable in current logic, but kept for completeness.
    return s.q_batch.popleft()


def _maybe_service_aged_batch(s):
    # If there is high-priority work, we mostly serve it.
    # But we allow an "aged" batch pipeline to run once in a while to avoid starvation.
    if not s.q_batch:
        return None
    if s.q_query or s.q_interactive:
        # Check oldest batch age against current sequence.
        oldest = s.q_batch[0]
        age = s._seq - s._enqueued_seq.get(oldest.pipeline_id, s._seq)
        if age >= s._batch_aging_threshold:
            return s.q_batch.popleft()
        return None
    return s.q_batch.popleft()


@register_scheduler(key="scheduler_low_023")
def scheduler_low_023(s, results, pipelines):
    """
    Policy step:
      1) Ingest new pipelines into per-priority queues.
      2) Learn from failures: on failed result, increase that pipeline's RAM multiplier.
      3) For each pool, try to assign at most one ready operator per tick:
         - Mostly serve QUERY then INTERACTIVE; allow aged BATCH occasionally.
         - Size CPU/RAM based on pool capacity, availability, and pipeline RAM multiplier.
      4) No preemption/suspensions (keeps wasted work minimal).
    """
    # Ingest new pipelines.
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from execution results (increase RAM multiplier on failures, especially OOM).
    for r in results:
        try:
            if r.failed():
                pid = getattr(r, "pipeline_id", None)
                # Some simulators may not include pipeline_id in result; fall back to no-op.
                if pid is None:
                    continue
                s._fail_count[pid] = s._fail_count.get(pid, 0) + 1

                # Increase multiplier more aggressively for probable OOM.
                factor = s._ram_backoff_factor * (1.25 if _is_probable_oom(getattr(r, "error", None)) else 1.10)
                new_mult = min(s._ram_backoff_cap, s._ram_mult.get(pid, 1.0) * factor)
                s._ram_mult[pid] = max(s._ram_mult.get(pid, 1.0), new_mult)
        except Exception:
            # Be robust to schema differences; never crash the scheduler step.
            pass

    # Early exit when nothing changed (still must return proper lists).
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Attempt to schedule across pools. Keep it simple: one assignment per pool per tick.
    for _ in range(s.executor.num_pools):
        # Pick next pipeline to try:
        # - Prefer query/interactive.
        # - If batch is aged, service it.
        batch_candidate = _maybe_service_aged_batch(s)
        if batch_candidate is not None:
            candidate = batch_candidate
        else:
            candidate = _pop_next_pipeline_with_aging(s)

        if candidate is None:
            break

        status = candidate.runtime_status()

        # Drop successful pipelines from queues.
        if status.is_pipeline_successful():
            continue

        # If there are failed operators, we will attempt to re-run them (ASSIGNABLE_STATES includes FAILED).
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not ready yet; requeue to preserve order.
            _q_for_priority(s, candidate.priority).append(candidate)
            continue

        # Choose a pool suitable for this priority.
        pool_id = _select_pool_for_priority(s, candidate.priority)
        if pool_id is None:
            # No capacity anywhere; requeue and stop trying further (avoids churn).
            _q_for_priority(s, candidate.priority).appendleft(candidate)
            break

        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Apply batch reserve: batch can only use "effective availability" after reserves.
        if candidate.priority == Priority.BATCH_PIPELINE:
            avail_cpu = max(0.0, avail_cpu - pool.max_cpu_pool * s._batch_cpu_reserve_frac)
            avail_ram = max(0.0, avail_ram - pool.max_ram_pool * s._batch_ram_reserve_frac)

        if avail_cpu <= 0 or avail_ram <= 0:
            # Chosen pool can't fit now; requeue and try a different pipeline next iteration.
            _q_for_priority(s, candidate.priority).append(candidate)
            continue

        cpu = _cpu_target(s, pool, candidate.priority, avail_cpu)
        ram = _ram_target(s, pool, candidate.priority, avail_ram, candidate.pipeline_id)

        if cpu <= 0 or ram <= 0:
            _q_for_priority(s, candidate.priority).append(candidate)
            continue

        assignments.append(
            Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=candidate.priority,
                pool_id=pool_id,
                pipeline_id=candidate.pipeline_id,
            )
        )

    # Requeue any remaining pipelines in local selection? (We always requeued when not scheduled.)
    return suspensions, assignments
