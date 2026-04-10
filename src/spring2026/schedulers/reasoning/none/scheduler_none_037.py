# policy_key: scheduler_none_037
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044797
# generation_seconds: 40.80
# generated_at: 2026-04-09T22:13:39.192397
@register_scheduler_init(key="scheduler_none_037")
def scheduler_none_037_init(s):
    """
    Priority-aware, OOM-resilient, low-churn scheduler.

    Main ideas:
    - Three priority queues (QUERY > INTERACTIVE > BATCH) with light aging to prevent starvation.
    - Conservative initial RAM sizing to avoid failures (failures incur huge penalty).
    - On OOM, retry the same op with increased RAM (exponential backoff), capped by pool max.
    - CPU sizing: give small-but-reasonable CPU to start; allow higher CPU for high priority when headroom exists.
    - Minimal preemption: only preempt when high-priority work exists and a pool is completely blocked.
    """
    # Queues hold pipeline_ids; we track pipelines in a dict to avoid stale references
    s.pipelines_by_id = {}
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Pipeline arrival sequence for aging / fairness
    s.arrival_seq = 0
    s.arrival_seq_by_pid = {}

    # OOM retry state: (pipeline_id, op_id) -> next_ram
    s.next_ram_by_op = {}

    # Throttle retries to avoid rapid-fire failures: (pipeline_id, op_id) -> earliest_time_tick
    # We don't have explicit clock API; approximate using scheduler ticks.
    s.tick = 0
    s.cooldown_until_tick = {}

    # Cache of last seen running containers per pool for possible preemption
    # container_id -> (priority, pool_id)
    s.running = {}

    # Track per-pipeline last service tick for aging
    s.last_service_tick = {}

    # Tunables (kept simple)
    s.base_ram_frac = 0.60          # start by allocating this fraction of available RAM to reduce OOM risk
    s.oom_ram_mult = 1.6            # increase RAM by this factor on OOM
    s.max_oom_retries_per_op = 6    # cap to avoid infinite loop; after that we still keep retrying but at max RAM
    s.retry_cooldown_ticks = 2      # small pause after OOM before retrying same op

    # CPU sizing: try to preserve headroom for concurrency, but protect high priority latency
    s.min_cpu_query = 2
    s.min_cpu_interactive = 1
    s.min_cpu_batch = 1
    s.cpu_frac_query = 0.75
    s.cpu_frac_interactive = 0.60
    s.cpu_frac_batch = 0.40

    # Preemption guardrails (minimize churn)
    s.preempt_only_if_no_assignment_possible = True
    s.preempt_max_per_tick = 1


def _prio_rank(priority):
    # Smaller is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _enqueue_pipeline(s, p):
    pid = p.pipeline_id
    s.pipelines_by_id[pid] = p
    if pid not in s.arrival_seq_by_pid:
        s.arrival_seq_by_pid[pid] = s.arrival_seq
        s.arrival_seq += 1

    if p.priority == Priority.QUERY:
        s.q_query.append(pid)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(pid)
    else:
        s.q_batch.append(pid)


def _pipeline_done_or_failed(p):
    status = p.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If any failures exist, we still try to recover (OOM retries) by allowing FAILED ops to be assignable.
    # So we do NOT treat "has failures" as terminal here.
    return False


def _pop_next_pid_with_aging(s):
    # Light aging: occasionally allow lower priority if it has waited much longer.
    # This avoids starvation without sacrificing high-priority latency.
    # We implement a simple check: if oldest batch/interact has waited > threshold, serve it.
    threshold_interactive = 30
    threshold_batch = 80

    def oldest_wait(queue):
        if not queue:
            return None, None
        pid = queue[0]
        return pid, (s.tick - s.arrival_seq_by_pid.get(pid, s.tick))

    # Default strict priority
    if s.q_query:
        return s.q_query.pop(0)

    # If no query, consider aging overrides between interactive and batch
    pid_i, wait_i = oldest_wait(s.q_interactive)
    pid_b, wait_b = oldest_wait(s.q_batch)

    if pid_i is not None and wait_i is not None and wait_i >= threshold_interactive:
        return s.q_interactive.pop(0)
    if pid_b is not None and wait_b is not None and wait_b >= threshold_batch and not s.q_interactive:
        return s.q_batch.pop(0)

    if s.q_interactive:
        return s.q_interactive.pop(0)
    if s.q_batch:
        return s.q_batch.pop(0)
    return None


def _peek_highest_priority_queue(s):
    if s.q_query:
        return Priority.QUERY
    if s.q_interactive:
        return Priority.INTERACTIVE
    if s.q_batch:
        return Priority.BATCH_PIPELINE
    return None


def _pick_assignable_ops(pipeline):
    status = pipeline.runtime_status()
    # Allow FAILED to be re-assigned to support retry after OOM
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return []
    # Keep it simple: assign a single ready op at a time (reduces contention and avoids large blast radius).
    return ops[:1]


def _op_key(pipeline, op):
    # Best-effort stable key for per-op retry state
    pid = pipeline.pipeline_id
    opid = getattr(op, "op_id", None)
    if opid is None:
        opid = getattr(op, "operator_id", None)
    if opid is None:
        opid = getattr(op, "id", None)
    if opid is None:
        # Fall back to object's repr (deterministic enough within a run)
        opid = repr(op)
    return (pid, opid)


def _cpu_for(priority, avail_cpu):
    if avail_cpu <= 0:
        return 0
    if priority == Priority.QUERY:
        cpu = max(2, int(avail_cpu * 0.75))
    elif priority == Priority.INTERACTIVE:
        cpu = max(1, int(avail_cpu * 0.60))
    else:
        cpu = max(1, int(avail_cpu * 0.40))
    return min(cpu, int(avail_cpu))


def _ram_for(s, pool, pipeline, op, avail_ram):
    # Start conservatively to avoid OOM: allocate a chunk of available RAM.
    # If we have a retry target for this op, use it.
    if avail_ram <= 0:
        return 0
    ok = _op_key(pipeline, op)
    target = s.next_ram_by_op.get(ok, None)
    if target is not None:
        return min(target, pool.max_ram_pool, avail_ram)

    # Conservative initial allocation: base fraction of available RAM, but at least some minimum.
    ram = max(1, int(avail_ram * s.base_ram_frac))
    return min(ram, int(avail_ram))


def _update_on_result(s, r):
    # Track running containers (best effort; results may represent completion/failure)
    if getattr(r, "container_id", None) is not None:
        if r.failed():
            s.running.pop(r.container_id, None)
        else:
            # A completion result should also remove it; we don't have explicit state, so remove on any result.
            s.running.pop(r.container_id, None)

    # If failed due to OOM, increase RAM for retry
    if r.failed() and getattr(r, "error", None):
        err = str(r.error).lower()
        is_oom = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "killed" in err)
        if is_oom:
            for op in (r.ops or []):
                # Use the same keying scheme on op objects coming from results
                pid = getattr(r, "pipeline_id", None)
                if pid is None:
                    # Result doesn't carry pipeline id in the provided API; infer via op object if possible
                    pid = getattr(op, "pipeline_id", None)
                if pid is None:
                    # Can't associate; skip
                    continue

                # Build op key with best effort
                opid = getattr(op, "op_id", None)
                if opid is None:
                    opid = getattr(op, "operator_id", None)
                if opid is None:
                    opid = getattr(op, "id", None)
                if opid is None:
                    opid = repr(op)
                ok = (pid, opid)

                current = s.next_ram_by_op.get(ok, None)
                # Start from what we just used (r.ram) if available
                base = getattr(r, "ram", None) or current or 1
                bumped = int(max(base + 1, base * s.oom_ram_mult))
                s.next_ram_by_op[ok] = bumped

                # Apply cooldown
                s.cooldown_until_tick[ok] = max(s.cooldown_until_tick.get(ok, 0), s.tick + s.retry_cooldown_ticks)


def _try_make_assignment(s, pool_id, pool, pid):
    p = s.pipelines_by_id.get(pid, None)
    if p is None:
        return None, False  # (assignment, should_requeue)

    # Drop completed pipelines from queues
    if _pipeline_done_or_failed(p):
        return None, False

    ops = _pick_assignable_ops(p)
    if not ops:
        # Not ready; requeue to allow others to run
        return None, True

    op = ops[0]
    ok = _op_key(p, op)
    if s.cooldown_until_tick.get(ok, 0) > s.tick:
        # Cooldown active after OOM; requeue for later
        return None, True

    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool
    if avail_cpu <= 0 or avail_ram <= 0:
        return None, True

    cpu = _cpu_for(p.priority, avail_cpu)
    ram = _ram_for(s, pool, p, op, avail_ram)
    if cpu <= 0 or ram <= 0:
        return None, True

    a = Assignment(
        ops=[op],
        cpu=cpu,
        ram=ram,
        priority=p.priority,
        pool_id=pool_id,
        pipeline_id=p.pipeline_id
    )
    s.last_service_tick[p.pipeline_id] = s.tick
    return a, True


def _preempt_one_low_priority_if_needed(s, pool_id, pool, needed_priority):
    # Very conservative preemption: at most one per tick, and only if there is a high-priority backlog.
    # We don't have a direct list of running containers from executor in the provided API;
    # we maintain best-effort tracking from results, but it may be incomplete. Hence: only act if we know one.
    # Preempt lowest priority known in this pool.
    candidates = []
    for cid, (prio, pid) in list(s.running.items()):
        if pid != pool_id:
            continue
        if _prio_rank(prio) > _prio_rank(needed_priority):
            candidates.append(( _prio_rank(prio), cid, prio))

    if not candidates:
        return None

    candidates.sort(reverse=True)  # preempt lowest priority (largest rank)
    _, cid, _ = candidates[0]
    return Suspend(container_id=cid, pool_id=pool_id)


@register_scheduler(key="scheduler_none_037")
def scheduler_none_037_scheduler(s, results, pipelines):
    """
    Scheduling step:
    - Ingest new pipelines into priority queues.
    - Process results: update OOM retry state and cooldowns.
    - For each pool, attempt to place one ready operator from the highest available priority pipeline.
    - If no placement is possible anywhere and high-priority backlog exists, attempt minimal preemption.
    """
    s.tick += 1

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Process results (OOM learning, running container bookkeeping)
    for r in results:
        _update_on_result(s, r)

    # Early exit
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Attempt to schedule at most one assignment per pool per tick (reduces contention & churn)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Try a handful of pops to find a runnable pipeline without spinning too long
        attempts = 0
        made = False
        requeue = []
        while attempts < 12:
            attempts += 1
            pid = _pop_next_pid_with_aging(s)
            if pid is None:
                break

            a, should_requeue = _try_make_assignment(s, pool_id, pool, pid)
            if should_requeue:
                requeue.append(pid)

            if a is not None:
                assignments.append(a)
                made = True
                break

        # Requeue anything we popped but didn't schedule (preserve relative order within each class loosely)
        for pid in requeue:
            p = s.pipelines_by_id.get(pid, None)
            if p is None:
                continue
            if _pipeline_done_or_failed(p):
                continue
            if p.priority == Priority.QUERY:
                s.q_query.append(pid)
            elif p.priority == Priority.INTERACTIVE:
                s.q_interactive.append(pid)
            else:
                s.q_batch.append(pid)

        if made:
            continue

    # Minimal preemption: if there is query backlog and no assignments were possible, try preempting 1 container.
    # This is best-effort given limited visibility; we only do it if we have any tracked running container.
    if not assignments:
        highest_backlog = _peek_highest_priority_queue(s)
        if highest_backlog in (Priority.QUERY, Priority.INTERACTIVE):
            preempts_left = s.preempt_max_per_tick
            for pool_id in range(s.executor.num_pools):
                if preempts_left <= 0:
                    break
                pool = s.executor.pools[pool_id]
                # Only preempt if pool appears blocked on both resources (can't schedule anything)
                if pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
                    continue
                sus = _preempt_one_low_priority_if_needed(s, pool_id, pool, highest_backlog)
                if sus is not None:
                    suspensions.append(sus)
                    preempts_left -= 1

    return suspensions, assignments
