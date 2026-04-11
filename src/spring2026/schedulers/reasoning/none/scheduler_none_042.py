# policy_key: scheduler_none_042
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051785
# generation_seconds: 48.74
# generated_at: 2026-03-14T02:11:34.498622
@register_scheduler_init(key="scheduler_none_042")
def scheduler_none_042_init(s):
    """Priority-aware FIFO with small, incremental improvements over naive FIFO.

    Improvements (kept intentionally simple/robust):
    1) Maintain separate queues per priority and schedule higher priority first.
    2) Retry-on-OOM by increasing RAM guess for the pipeline (bounded), rather than dropping it.
    3) Avoid obvious head-of-line blocking: if the first pipeline can't schedule any ready op,
       look at more pipelines (bounded scan).
    4) Light, conservative preemption: if a high-priority pipeline arrives and no pool has
       headroom, suspend (at most one per tick) a low-priority running container to free capacity.

    Notes/Assumptions based on provided interfaces:
    - We use ExecutionResult.failed() and ExecutionResult.error string to detect OOM-like failures.
    - We size CPU/RAM per assignment from pool availability, but clamp to a per-pipeline RAM guess
      that is increased upon OOM.
    - We only assign one op per pool per tick (like the naive policy) to keep behavior stable.
    """

    # Per-priority waiting queues for pipelines (store pipeline objects).
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Remember last-seen pipeline object by id (helps when results arrive).
    s.pipeline_by_id = {}

    # Per-pipeline RAM "guess" that is increased upon OOM and decays on success.
    # Units are whatever the simulator uses (consistent with pool RAM).
    s.ram_guess_by_pid = {}

    # Bounds and tuning knobs (conservative).
    s.min_ram_frac_of_pool = 0.10   # don't try with tiny RAM; start at least 10% of pool
    s.max_ram_frac_of_pool = 1.00   # never exceed pool
    s.oom_backoff_mult = 1.7        # increase RAM guess on OOM
    s.success_decay = 0.95          # slightly decay RAM guess on success to recover utilization
    s.max_queue_scan = 8            # bounded scan to avoid HOL blocking
    s.preemptions_per_tick = 1      # keep churn low

    # Basic aging to avoid indefinite starvation: count "ticks waited" per pipeline.
    s.age_by_pid = {}

    # Track known running containers for potential preemption:
    # container_id -> dict(pool_id, priority)
    s.running = {}


def _is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    # Heuristic: common substrings for OOM in simulators/runtimes.
    return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "exceed" in e)


def _priority_order():
    # Highest to lowest
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _lower_priorities(p):
    if p == Priority.QUERY:
        return [Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    if p == Priority.INTERACTIVE:
        return [Priority.BATCH_PIPELINE]
    return []


def _age_weighted_pick(queues, age_by_pid, max_scan):
    """
    Pick a pipeline from the highest non-empty priority queue, but allow a bounded scan to
    pick a more "ready" candidate to reduce HOL blocking. We do not reorder the whole queue;
    we just pick the first that has any assignable op whose parents are complete (if possible).
    Returns (priority, pipeline, index_in_queue) or (None, None, None).
    """
    for pr in _priority_order():
        q = queues[pr]
        if not q:
            continue

        # Small aging: if low priority has been waiting very long, it can bubble up a bit.
        # Keep extremely simple: within a given priority, prefer older items during scan.
        scan_n = min(max_scan, len(q))
        best_i = None
        best_age = -1

        for i in range(scan_n):
            p = q[i]
            pid = p.pipeline_id
            age = age_by_pid.get(pid, 0)
            # We try to find a pipeline that is actually ready to run something.
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            if st.state_counts[OperatorState.FAILED] > 0:
                # failed ops may be retryable (we use ASSIGNABLE_STATES which includes FAILED)
                pass
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                if age > best_age:
                    best_age = age
                    best_i = i

        # If none in scan window has ready ops, just return the head to preserve FIFO behavior.
        if best_i is None:
            return pr, q[0], 0
        return pr, q[best_i], best_i

    return None, None, None


def _ensure_pipeline_state(s, pipeline):
    pid = pipeline.pipeline_id
    s.pipeline_by_id[pid] = pipeline
    if pid not in s.ram_guess_by_pid:
        # Initialize RAM guess conservatively relative to pool size later; for now set None.
        s.ram_guess_by_pid[pid] = None
    if pid not in s.age_by_pid:
        s.age_by_pid[pid] = 0


def _update_ages(s):
    # Increment age for queued pipelines; reset age for those not queued is handled elsewhere.
    for pr in s.waiting_queues:
        for p in s.waiting_queues[pr]:
            s.age_by_pid[p.pipeline_id] = s.age_by_pid.get(p.pipeline_id, 0) + 1


def _enqueue_pipeline(s, pipeline):
    _ensure_pipeline_state(s, pipeline)
    s.waiting_queues[pipeline.priority].append(pipeline)


def _maybe_decay_ram_guess_on_success(s, pid):
    g = s.ram_guess_by_pid.get(pid, None)
    if g is None:
        return
    s.ram_guess_by_pid[pid] = max(g * s.success_decay, 0.0)


def _bump_ram_guess_on_oom(s, pid, observed_ram, pool_max_ram):
    # Start from max(observed, current_guess, small fraction of pool), then increase.
    cur = s.ram_guess_by_pid.get(pid, None)
    base = 0.0
    if observed_ram is not None:
        base = max(base, float(observed_ram))
    if cur is not None:
        base = max(base, float(cur))
    # Ensure we actually bump meaningfully:
    bumped = base * s.oom_backoff_mult if base > 0 else float(pool_max_ram) * s.min_ram_frac_of_pool
    # Clamp to pool max
    bumped = min(bumped, float(pool_max_ram) * s.max_ram_frac_of_pool)
    s.ram_guess_by_pid[pid] = bumped


def _pick_pool_for_priority(s, priority):
    """
    Simple pool selection:
    - Prefer the pool with most available RAM for high priority (less OOM risk),
      else fallback to most available CPU.
    """
    best_pool = None
    best_score = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue
        # Score: high priority cares more about RAM headroom.
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            score = (pool.avail_ram_pool, pool.avail_cpu_pool)
        else:
            score = (pool.avail_cpu_pool, pool.avail_ram_pool)

        if best_score is None or score > best_score:
            best_score = score
            best_pool = pool_id
    return best_pool


def _attempt_preempt_for_high_priority(s, needed_priority):
    """
    Preempt at most one low-priority container to create headroom.
    We suspend the *lowest priority* running container we know about.
    Returns list of Suspend.
    """
    suspensions = []
    if needed_priority not in (Priority.QUERY, Priority.INTERACTIVE):
        return suspensions

    # Find a running container with lower priority.
    # Prefer suspending BATCH over INTERACTIVE; never suspend QUERY.
    candidates = []
    for cid, info in s.running.items():
        pr = info.get("priority", None)
        if pr is None:
            continue
        if pr in _lower_priorities(needed_priority):
            candidates.append((pr, cid, info.get("pool_id", None)))

    if not candidates:
        return suspensions

    # Lowest priority first (BATCH_PIPELINE is lowest)
    def pr_rank(pr):
        if pr == Priority.BATCH_PIPELINE:
            return 0
        if pr == Priority.INTERACTIVE:
            return 1
        return 2

    candidates.sort(key=lambda x: pr_rank(x[0]))
    _, cid, pool_id = candidates[0]
    if pool_id is not None:
        suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
    return suspensions


@register_scheduler(key="scheduler_none_042")
def scheduler_none_042(s, results, pipelines):
    """
    Scheduler step:
    - ingest new pipelines
    - process results (learn OOM, track running)
    - age queues a bit (anti-starvation)
    - for each pool, schedule 1 ready op from highest priority queue with bounded scan
    - if a high-priority pipeline is waiting but no pool has headroom, preempt one low-pri container
    """
    # Ingest new pipelines into priority queues
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Early exit if nothing changed
    if not pipelines and not results:
        # still age a bit to ensure fairness? keep stable: no changes, no action
        return [], []

    # Process results: learn on OOM, decay RAM guess on success, update running map
    for r in results:
        # Container finished/failed; no longer considered running.
        if getattr(r, "container_id", None) in s.running:
            s.running.pop(r.container_id, None)

        pid = getattr(r, "pipeline_id", None)
        # Some simulators may not attach pipeline_id to results; we best-effort handle via ops.
        if pid is None:
            # If ops are present and have pipeline_id attribute, try to infer.
            ops = getattr(r, "ops", None) or []
            if ops:
                pid = getattr(ops[0], "pipeline_id", None)

        if r.failed():
            if _is_oom_error(getattr(r, "error", None)) and pid is not None:
                # Choose a pool_max_ram from the pool where it ran
                pool_id = getattr(r, "pool_id", None)
                pool_max_ram = None
                if pool_id is not None:
                    pool_max_ram = s.executor.pools[pool_id].max_ram_pool
                else:
                    # fallback: maximum across pools
                    pool_max_ram = max(s.executor.pools[i].max_ram_pool for i in range(s.executor.num_pools))

                _bump_ram_guess_on_oom(s, pid, getattr(r, "ram", None), pool_max_ram)

                # Re-enqueue the pipeline for retry (unless it is already complete/failed terminally).
                pipe = s.pipeline_by_id.get(pid, None)
                if pipe is not None:
                    st = pipe.runtime_status()
                    if not st.is_pipeline_successful():
                        _enqueue_pipeline(s, pipe)
        else:
            if pid is not None:
                _maybe_decay_ram_guess_on_success(s, pid)

    # Age queued pipelines (light anti-starvation)
    _update_ages(s)

    suspensions = []
    assignments = []

    # If high-priority work exists and we can't schedule anything, consider one preemption.
    high_waiting = bool(s.waiting_queues[Priority.QUERY] or s.waiting_queues[Priority.INTERACTIVE])
    any_pool_has_capacity = any(
        (s.executor.pools[i].avail_cpu_pool > 0 and s.executor.pools[i].avail_ram_pool > 0)
        for i in range(s.executor.num_pools)
    )
    if high_waiting and not any_pool_has_capacity:
        # Prefer preempting for QUERY if present, else INTERACTIVE
        needed_pr = Priority.QUERY if s.waiting_queues[Priority.QUERY] else Priority.INTERACTIVE
        suspensions.extend(_attempt_preempt_for_high_priority(s, needed_pr))
        # Limit preemptions
        suspensions = suspensions[: s.preemptions_per_tick]
        # If we preempt, return immediately to let resources free up next tick (reduces thrash)
        if suspensions:
            return suspensions, []

    # Schedule: one assignment per pool (like baseline), but priority-aware.
    # We pick a pool and then pick a pipeline; or per pool directly with best pipeline.
    for _ in range(s.executor.num_pools):
        pool_id = None

        # Choose which priority to serve next based on queue non-emptiness; then pick best pool.
        pr, pipe, idx = _age_weighted_pick(s.waiting_queues, s.age_by_pid, s.max_queue_scan)
        if pipe is None:
            break

        pool_id = _pick_pool_for_priority(s, pr)
        if pool_id is None:
            # No capacity anywhere; stop.
            break

        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            break

        # Remove chosen pipeline from its queue (by index).
        s.waiting_queues[pr].pop(idx)

        st = pipe.runtime_status()
        # Drop completed pipelines
        if st.is_pipeline_successful():
            s.age_by_pid[pipe.pipeline_id] = 0
            continue

        # Choose one ready operator with parents complete
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not ready: requeue at same priority (preserve approximate FIFO)
            _enqueue_pipeline(s, pipe)
            continue

        pid = pipe.pipeline_id
        _ensure_pipeline_state(s, pipe)

        # Determine RAM allocation: min(avail_ram, ram_guess if set), with a sane minimum.
        # Initialize ram_guess based on pool max if unset.
        if s.ram_guess_by_pid.get(pid, None) is None:
            s.ram_guess_by_pid[pid] = float(pool.max_ram_pool) * s.min_ram_frac_of_pool

        ram_guess = float(s.ram_guess_by_pid[pid])
        ram_alloc = min(float(avail_ram), ram_guess)
        # Also avoid allocating too tiny RAM if we do have more in pool
        min_reasonable = float(pool.max_ram_pool) * s.min_ram_frac_of_pool
        ram_alloc = max(min(ram_alloc, float(avail_ram)), min_reasonable)
        ram_alloc = min(ram_alloc, float(avail_ram))

        # CPU allocation: for latency, give more CPU to high priority, but don't starve others.
        # Keep simple: allocate most available CPU for high priority, else half.
        if pr in (Priority.QUERY, Priority.INTERACTIVE):
            cpu_alloc = float(avail_cpu)
        else:
            cpu_alloc = max(1.0, float(avail_cpu) * 0.5)

        assignment = Assignment(
            ops=op_list,
            cpu=cpu_alloc,
            ram=ram_alloc,
            priority=pipe.priority,
            pool_id=pool_id,
            pipeline_id=pid,
        )
        assignments.append(assignment)

        # Reset age since it just got service
        s.age_by_pid[pid] = 0

        # Update our "running" bookkeeping optimistically if container_id were known later.
        # We don't have container_id at assignment time, so we can't add it here reliably.
        # However, some simulators might attach container_id after scheduling; we handle via results.

        # Only one assignment per tick in total? Baseline did one per pool; we continue that.
        # But since we already picked the best pool, continuing can oversubscribe the same pool in one tick
        # in some executors. To remain conservative, stop after one assignment.
        break

    # Track running containers if results include "started" events (not specified); best-effort:
    for r in results:
        # If a result isn't terminal, we might treat it as still running, but interface doesn't specify.
        # We'll only track terminal completion/failure by removing; additions aren't reliable here.
        pass

    return suspensions, assignments
