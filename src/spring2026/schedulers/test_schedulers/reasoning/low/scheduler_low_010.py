# policy_key: scheduler_low_010
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041857
# generation_seconds: 46.18
# generated_at: 2026-04-09T21:09:41.361764
@register_scheduler_init(key="scheduler_low_010")
def scheduler_low_010_init(s):
    """Priority-aware, failure-avoiding scheduler.

    Main ideas:
    - Always prefer QUERY then INTERACTIVE then BATCH to reduce weighted latency.
    - Be conservative with failures: retry FAILED ops (esp. OOM) with increased RAM.
    - Avoid starvation: round-robin within each priority + simple aging boost for long-waiting batch.
    - Avoid aggressive preemption (wasted work can increase failures/latency); focus on safe placement.
    """
    from collections import deque, defaultdict

    # Per-priority waiting queues (pipelines). We keep pipelines even if they have failed ops; we retry.
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track when pipeline first entered queue (for aging).
    s.pipeline_first_seen_tick = {}

    # Per-operator retry / sizing hints (keyed by (pipeline_id, op_key)).
    s.op_attempts = defaultdict(int)
    s.op_ram_hint = {}  # bytes/units as used by simulator
    s.op_cpu_hint = {}

    # Remember last known container resources on failure (to bump RAM / adjust CPU).
    s.last_run_resources = {}  # (pipeline_id, op_key) -> (cpu, ram)

    # Tick counter (discrete calls); used for aging.
    s.tick = 0


def _pqueue(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _op_key(op):
    # Use object identity as a stable key within the simulation run.
    return id(op)


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "error" in e)


def _prio_rank(priority) -> int:
    # Lower is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _base_cpu_share(priority) -> float:
    # Conservative shares so we don't starve other work and reduce queue buildup.
    if priority == Priority.QUERY:
        return 0.75
    if priority == Priority.INTERACTIVE:
        return 0.50
    return 0.35


def _base_ram_share(priority) -> float:
    # RAM is the dominant failure mode; allocate meaningfully but not all.
    if priority == Priority.QUERY:
        return 0.70
    if priority == Priority.INTERACTIVE:
        return 0.55
    return 0.45


def _pick_pool_for_op(s, priority, ram_needed, cpu_needed):
    """Choose a pool that can satisfy the request; prefer more RAM headroom for high priority."""
    best = None
    best_score = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool < cpu_needed or pool.avail_ram_pool < ram_needed:
            continue

        # Score: prioritize RAM headroom for QUERY/INTERACTIVE to avoid OOM retries;
        # batch cares more about fitting anywhere.
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            score = (pool.avail_ram_pool, pool.avail_cpu_pool)
        else:
            score = (pool.avail_cpu_pool, pool.avail_ram_pool)

        if best is None or score > best_score:
            best = pool_id
            best_score = score
    return best


def _compute_request(s, pool, priority, pipeline_id, op):
    """Compute (cpu, ram) request using hints + conservative defaults + OOM-based growth."""
    opk = (pipeline_id, _op_key(op))

    # Defaults are fractions of pool capacity, capped by availability later.
    cpu_default = max(1.0, pool.max_cpu_pool * _base_cpu_share(priority))
    ram_default = max(1.0, pool.max_ram_pool * _base_ram_share(priority))

    # Apply hinting if present.
    cpu = s.op_cpu_hint.get(opk, cpu_default)
    ram = s.op_ram_hint.get(opk, ram_default)

    # If we have a last run record and attempts, gently increase CPU a bit on repeated slowdowns/failures.
    attempts = s.op_attempts.get(opk, 0)
    if attempts >= 2:
        cpu = max(cpu, min(pool.max_cpu_pool, cpu_default * 1.25))

    # Never exceed pool max; actual availability clamp occurs at assignment time.
    cpu = min(cpu, pool.max_cpu_pool)
    ram = min(ram, pool.max_ram_pool)
    return cpu, ram


def _bump_on_failure(s, result):
    """Update hints based on a failed ExecutionResult."""
    if result is None or not result.failed():
        return

    # We may receive multiple ops in one result; apply the same bump to each.
    is_oom = _is_oom_error(result.error)

    for op in (result.ops or []):
        opk = (result.pipeline_id, _op_key(op)) if hasattr(result, "pipeline_id") else None
        # If result doesn't carry pipeline_id, we can still update by op identity, but keep it safe.
        if opk is None:
            continue

        s.op_attempts[opk] += 1
        s.last_run_resources[opk] = (result.cpu, result.ram)

        # RAM-first for OOM; otherwise small RAM bump and modest CPU bump.
        prev_ram = s.op_ram_hint.get(opk, max(1.0, result.ram))
        prev_cpu = s.op_cpu_hint.get(opk, max(1.0, result.cpu))

        if is_oom:
            # Double RAM hint with a floor at last allocation; keep CPU stable.
            s.op_ram_hint[opk] = max(prev_ram * 2.0, result.ram * 2.0)
            s.op_cpu_hint[opk] = max(1.0, prev_cpu)
        else:
            # Non-OOM failure: increase RAM a bit (defensive) and CPU slightly (could be timeout-like).
            s.op_ram_hint[opk] = max(prev_ram * 1.25, result.ram)
            s.op_cpu_hint[opk] = max(prev_cpu * 1.10, result.cpu)


def _max_retries_for_op(s, priority, opk):
    # Keep retries bounded to avoid infinite churn, but be generous for high-priority to avoid 720s penalty.
    if priority == Priority.QUERY:
        return 6
    if priority == Priority.INTERACTIVE:
        return 5
    return 4


def _enqueue_pipeline_if_needed(s, pipeline):
    q = _pqueue(s, pipeline.priority)

    # Avoid enqueuing duplicates excessively: only enqueue if not already present.
    # (O(n) but queues are expected to be manageable in the simulator.)
    pid = pipeline.pipeline_id
    for existing in q:
        if existing.pipeline_id == pid:
            return

    q.append(pipeline)
    if pid not in s.pipeline_first_seen_tick:
        s.pipeline_first_seen_tick[pid] = s.tick


def _eligible_ops(pipeline):
    status = pipeline.runtime_status()
    # Allow retrying FAILED ops too; parents must be complete.
    return status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)


def _pipeline_done_or_terminal(s, pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True

    # If pipeline has assignable work, it's not terminal.
    if _eligible_ops(pipeline):
        return False

    # If no eligible ops and not successful, it might be waiting for running parents; keep it.
    # We treat it as non-terminal to avoid dropping.
    return False


def _pop_next_pipeline_with_work(s):
    """Pop next pipeline preferring high priority, with simple aging for batch to avoid starvation."""
    # Aging: if batch has been waiting long, occasionally let it jump ahead of interactive.
    # This prevents indefinite starvation while still protecting top priorities.
    def peek_wait(q):
        if not q:
            return None, None
        p = q[0]
        return p, s.pipeline_first_seen_tick.get(p.pipeline_id, s.tick)

    # First, always attempt to serve QUERY.
    if s.q_query:
        return s.q_query.popleft()

    # Aging check: if batch has waited a lot, serve it before interactive sometimes.
    b_p, b_t = peek_wait(s.q_batch)
    i_p, i_t = peek_wait(s.q_interactive)
    if b_p is not None and i_p is not None:
        batch_wait = s.tick - b_t
        inter_wait = s.tick - i_t
        # If batch has waited significantly longer, let it go first.
        if batch_wait >= 50 and batch_wait >= inter_wait + 30:
            return s.q_batch.popleft()

    if s.q_interactive:
        return s.q_interactive.popleft()
    if s.q_batch:
        return s.q_batch.popleft()
    return None


@register_scheduler(key="scheduler_low_010")
def scheduler_low_010(s, results, pipelines):
    """
    Scheduler step:
    - Update sizing hints from failures (especially OOM).
    - Enqueue arriving pipelines.
    - For each pool, greedily assign ready ops, priority-first, while resources remain.
    """
    s.tick += 1

    # Incorporate results: update hints and re-enqueue pipelines that still need work.
    # Note: results may not include pipeline objects, so we only update hints here.
    for r in (results or []):
        if r is None:
            continue
        if r.failed():
            _bump_on_failure(s, r)

    # Enqueue new pipelines.
    for p in (pipelines or []):
        _enqueue_pipeline_if_needed(s, p)

    # If nothing changed, early exit.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Greedy fill per pool; we only schedule one op per pipeline per "pop" to limit head-of-line blocking.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try to place multiple small-to-medium tasks in the pool.
        # Safety limit to prevent infinite loops when nothing fits.
        no_progress_iters = 0
        while avail_cpu > 0 and avail_ram > 0 and no_progress_iters < 50:
            pipeline = _pop_next_pipeline_with_work(s)
            if pipeline is None:
                break

            # Skip fully completed pipelines.
            if _pipeline_done_or_terminal(s, pipeline):
                continue

            ops = _eligible_ops(pipeline)
            if not ops:
                # Nothing ready yet; requeue it to check later.
                _enqueue_pipeline_if_needed(s, pipeline)
                no_progress_iters += 1
                continue

            # Pick first ready op (stable ordering from status).
            op = ops[0]
            opk = (pipeline.pipeline_id, _op_key(op))

            # Check retry budget; if exceeded, we stop retrying this op but keep pipeline around
            # (it may have other ops; if it doesn't, it'll remain non-progressing but we avoid dropping).
            if s.op_attempts.get(opk, 0) > _max_retries_for_op(s, pipeline.priority, opk):
                # Requeue pipeline but deprioritize by placing it at the end.
                _enqueue_pipeline_if_needed(s, pipeline)
                no_progress_iters += 1
                continue

            # Compute desired request.
            req_cpu, req_ram = _compute_request(s, pool, pipeline.priority, pipeline.pipeline_id, op)

            # Clamp to available resources.
            cpu = min(req_cpu, avail_cpu)
            ram = min(req_ram, avail_ram)

            # If we can't allocate at least 1 CPU and 1 RAM unit, give up for this pool.
            if cpu <= 0 or ram <= 0:
                _enqueue_pipeline_if_needed(s, pipeline)
                no_progress_iters += 1
                break

            # If this op has previously OOM'ed, try to ensure we don't schedule it with too little RAM.
            # If our hint exceeds current availability, attempt a different pool by requeueing.
            hinted_ram = s.op_ram_hint.get(opk, ram)
            if hinted_ram > avail_ram and pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                # High priority: don't risk another OOM if we can avoid it.
                _enqueue_pipeline_if_needed(s, pipeline)
                no_progress_iters += 1
                continue

            # If this pool can't meet reasonable request, attempt to place in another pool (best fit).
            # We treat "reasonable" as the unclamped request.
            best_pool = _pick_pool_for_op(s, pipeline.priority, min(req_ram, pool.max_ram_pool), min(req_cpu, pool.max_cpu_pool))
            if best_pool is not None and best_pool != pool_id:
                # Requeue and let the other pool take it when its loop runs.
                _enqueue_pipeline_if_needed(s, pipeline)
                no_progress_iters += 1
                continue

            # Create assignment for exactly one op.
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

            # Track last assigned resources for potential future failure-based adjustments.
            s.last_run_resources[opk] = (cpu, ram)

            # Decrement local available resources (pool.avail_* will be updated by simulator after applying assignments).
            avail_cpu -= cpu
            avail_ram -= ram

            # Requeue pipeline for subsequent ops (round-robin).
            _enqueue_pipeline_if_needed(s, pipeline)

            no_progress_iters = 0  # progress made

    return suspensions, assignments
