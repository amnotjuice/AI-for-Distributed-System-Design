# policy_key: scheduler_low_017
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.054443
# generation_seconds: 43.18
# generated_at: 2026-04-09T21:15:34.788658
@register_scheduler_init(key="scheduler_low_017")
def scheduler_low_017_init(s):
    """
    Priority + aging + conservative right-sizing with automatic RAM backoff.

    Goals for the weighted-latency objective:
      - Strongly protect QUERY and INTERACTIVE latency via priority-biased dispatch.
      - Avoid failures (720s penalty) by retrying FAILED ops and increasing RAM on repeated failures.
      - Avoid starvation by aging all queued pipelines so BATCH eventually runs.
      - Keep placement simple and robust: assign 1 ready op at a time, pack within each pool's current headroom.
    """
    from collections import deque

    # Separate queues per priority to make protection straightforward.
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Bookkeeping for fairness/aging and safer retries.
    s.step = 0
    s.enqueued_at = {}          # pipeline_id -> step when last enqueued
    s.ram_mult = {}             # pipeline_id -> multiplicative RAM bump (>= 1.0)
    s.last_failed_count = {}    # pipeline_id -> last observed FAILED op count


def _prio_weight(priority):
    # Larger means "more important" in dispatch choice.
    if priority == Priority.QUERY:
        return 10.0
    if priority == Priority.INTERACTIVE:
        return 5.0
    return 1.0


def _queue_for(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _iter_all_queues(s):
    # Fixed order: higher priority first, but selection is still score-based with aging.
    return (s.q_query, s.q_interactive, s.q_batch)


def _pipeline_done_or_dead(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # We do NOT drop failures; FAILED ops are retryable (ASSIGNABLE_STATES includes FAILED).
    return False


def _update_ram_backoff(s, pipeline):
    """
    If we observe new FAILED ops since last time, increase RAM multiplier to reduce
    repeated OOM-style failures. We cannot reliably detect OOM cause from status,
    so we treat any new failure as a signal to allocate more RAM next time.
    """
    status = pipeline.runtime_status()
    failed_now = status.state_counts.get(OperatorState.FAILED, 0)
    pid = pipeline.pipeline_id
    failed_prev = s.last_failed_count.get(pid, 0)
    if failed_now > failed_prev:
        # Increase relatively aggressively to avoid repeated 720s penalties.
        # Cap to avoid monopolizing the pool forever.
        prev = s.ram_mult.get(pid, 1.0)
        s.ram_mult[pid] = min(4.0, max(prev, 1.0) * 1.5)
    s.last_failed_count[pid] = failed_now


def _pick_next_pipeline(s):
    """
    Choose a pipeline to schedule next using:
      score = priority_weight + aging_bonus

    Aging bonus grows with wait time, ensuring eventual progress for BATCH without
    sacrificing high-priority tail latency.
    """
    best = None
    best_queue = None
    best_score = -1e30

    for q in _iter_all_queues(s):
        # Scan the queue to find the best candidate without permanent reordering.
        for p in q:
            if _pipeline_done_or_dead(p):
                continue
            pid = p.pipeline_id
            enq_t = s.enqueued_at.get(pid, s.step)
            wait = max(0, s.step - enq_t)

            # Priority dominates; aging prevents starvation.
            score = _prio_weight(p.priority) + 0.02 * wait
            if score > best_score:
                best = p
                best_queue = q
                best_score = score

    if best is None:
        return None

    # Remove the chosen pipeline from its queue (first occurrence).
    # (Queues are small enough that linear remove is fine here.)
    best_queue.remove(best)
    return best


def _target_cpu(pool, priority):
    # Give higher priority more CPU to reduce latency, but avoid grabbing the entire pool.
    if priority == Priority.QUERY:
        frac = 0.75
    elif priority == Priority.INTERACTIVE:
        frac = 0.60
    else:
        frac = 0.40
    return max(1.0, pool.max_cpu_pool * frac)


def _target_ram(pool, priority):
    # RAM is crucial to prevent failures; bias upward for QUERY/INTERACTIVE.
    if priority == Priority.QUERY:
        frac = 0.60
    elif priority == Priority.INTERACTIVE:
        frac = 0.50
    else:
        frac = 0.35
    return max(1.0, pool.max_ram_pool * frac)


@register_scheduler(key="scheduler_low_017")
def scheduler_low_017(s, results: list, pipelines: list):
    """
    Scheduler step:
      1) Enqueue new pipelines; update RAM backoff state from observed failures.
      2) For each pool, repeatedly assign ready ops while headroom allows.
      3) Assign one operator at a time (atomic work unit), placing into the current pool.
      4) If a pipeline cannot run now (no ready ops), requeue it.
    """
    s.step += 1

    # Incorporate new arrivals.
    for p in pipelines:
        q = _queue_for(s, p.priority)
        q.append(p)
        s.enqueued_at[p.pipeline_id] = s.step
        # Initialize per-pipeline state.
        s.ram_mult.setdefault(p.pipeline_id, 1.0)
        s.last_failed_count.setdefault(p.pipeline_id, 0)

    # Early exit if nothing changes that could affect decisions.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Update backoff state based on current pipeline runtime status (includes failures).
    # We do this lazily here because results don't reliably carry pipeline_id.
    for q in _iter_all_queues(s):
        for p in q:
            _update_ram_backoff(s, p)

    # Main placement loop: pack within each pool's available headroom.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Try to keep scheduling while resources remain.
        # Hard cap prevents pathological long loops in a single tick.
        max_assignments_this_pool = 8
        made = 0

        while made < max_assignments_this_pool and avail_cpu > 0 and avail_ram > 0:
            p = _pick_next_pipeline(s)
            if p is None:
                break

            # Drop completed pipelines; retry FAILED ops instead of dropping.
            if _pipeline_done_or_dead(p):
                continue

            status = p.runtime_status()
            _update_ram_backoff(s, p)

            # Only schedule ops whose parents are complete.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; put it back and try another pipeline.
                _queue_for(s, p.priority).append(p)
                s.enqueued_at[p.pipeline_id] = s.enqueued_at.get(p.pipeline_id, s.step)
                continue

            # Right-size resources with priority bias + RAM backoff.
            cpu_need = min(avail_cpu, _target_cpu(pool, p.priority))
            ram_need = min(avail_ram, _target_ram(pool, p.priority) * s.ram_mult.get(p.pipeline_id, 1.0))

            # If we can't fit a minimally meaningful slice, requeue and stop trying in this pool.
            if cpu_need <= 0 or ram_need <= 0:
                _queue_for(s, p.priority).appendleft(p)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_need,
                    ram=ram_need,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            avail_cpu -= cpu_need
            avail_ram -= ram_need
            made += 1

            # Requeue pipeline for subsequent operators.
            _queue_for(s, p.priority).append(p)
            # Keep original enqueue time for aging (don't reset), to prevent starvation.
            s.enqueued_at.setdefault(p.pipeline_id, s.step)

    return suspensions, assignments
