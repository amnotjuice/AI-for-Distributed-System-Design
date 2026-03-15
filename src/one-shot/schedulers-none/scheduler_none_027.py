# policy_key: scheduler_none_027
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043147
# generation_seconds: 42.14
# generated_at: 2026-03-14T02:01:06.795482
@register_scheduler_init(key="scheduler_none_027")
def scheduler_none_027_init(s):
    """Priority-aware, latency-first scheduler with cautious preemption and simple OOM backoff.

    Improvements over naive FIFO:
      1) Maintain per-priority queues and always schedule higher priority first.
      2) Use conservative per-op CPU/RAM sizing (avoid "give everything to one op" which can hurt latency via head-of-line blocking).
      3) Learn RAM needs on OOM (per operator) and retry with increased RAM.
      4) If a high-priority op is ready but cannot fit, preempt a low-priority running container (at most one per tick) to make room.
      5) Mild fairness: ensure batch is not starved by limiting consecutive high-priority dispatches.
    """
    # Waiting queues per priority (store Pipeline objects)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator learned RAM floors after OOM: (pipeline_id, op_id) -> ram
    s.learned_ram = {}

    # Track running containers we last saw, by pool: pool_id -> {container_id: meta}
    # meta: {"priority": Priority, "cpu": float, "ram": float, "pipeline_id": str/int}
    s.running = {}

    # Fairness knobs
    s.consecutive_hi = 0
    s.max_consecutive_hi = 8  # after this, allow one batch dispatch if possible

    # Backoff factor on OOM
    s.oom_ram_mult = 1.6
    s.oom_ram_add = 0.5  # additive GB-equivalent in same units as pool RAM; still helps if tiny

    # Sizing knobs: keep containers reasonably sized to reduce interference and keep tail latency down
    s.max_share_cpu = 0.60   # don't allocate more than 60% of a pool to one assignment
    s.max_share_ram = 0.70   # don't allocate more than 70% of a pool to one assignment
    s.min_cpu = 1.0
    s.min_ram = 1.0

    # Preemption control
    s.preempt_at_most_one_per_tick = True


def _q_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _enqueue_pipeline(s, p):
    _q_for_priority(s, p.priority).append(p)


def _pop_next_pipeline(s):
    # Mild fairness: if we've dispatched too many high priority ops consecutively,
    # try batch once (if any ready work exists).
    if s.consecutive_hi >= s.max_consecutive_hi and s.q_batch:
        return s.q_batch.pop(0)

    if s.q_query:
        return s.q_query.pop(0)
    if s.q_interactive:
        return s.q_interactive.pop(0)
    if s.q_batch:
        return s.q_batch.pop(0)
    return None


def _peek_has_any(s):
    return bool(s.q_query or s.q_interactive or s.q_batch)


def _iter_queues_in_priority_order(s):
    # For re-queuing and scanning; priority order (high to low)
    return [s.q_query, s.q_interactive, s.q_batch]


def _op_identity(pipeline, op):
    # Best-effort stable identity for per-op learning.
    # Many simulators give operators an "op_id" or "operator_id"; fall back to repr(op).
    op_id = getattr(op, "op_id", None)
    if op_id is None:
        op_id = getattr(op, "operator_id", None)
    if op_id is None:
        op_id = getattr(op, "id", None)
    if op_id is None:
        op_id = repr(op)
    return (pipeline.pipeline_id, op_id)


def _choose_sizing(s, pool, pipeline, ops, priority):
    # Select a conservative CPU/RAM request, using learned RAM floor if available.
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool

    # Cap by share to avoid giving one op the entire pool (head-of-line blocking).
    cpu_cap = min(avail_cpu, max(s.min_cpu, pool.max_cpu_pool * s.max_share_cpu))
    ram_cap = min(avail_ram, max(s.min_ram, pool.max_ram_pool * s.max_share_ram))

    # Priority-sensitive CPU target: give more CPU to high priority (lower latency),
    # but still capped and not monopolizing.
    if priority == Priority.QUERY:
        cpu_target = cpu_cap
        ram_target = ram_cap
    elif priority == Priority.INTERACTIVE:
        cpu_target = max(s.min_cpu, min(cpu_cap, pool.max_cpu_pool * 0.45))
        ram_target = ram_cap
    else:
        cpu_target = max(s.min_cpu, min(cpu_cap, pool.max_cpu_pool * 0.30))
        ram_target = max(s.min_ram, min(ram_cap, pool.max_ram_pool * 0.45))

    # Apply learned RAM floors based on any of the ops we're assigning.
    learned_floor = 0.0
    for op in ops:
        key = _op_identity(pipeline, op)
        learned_floor = max(learned_floor, s.learned_ram.get(key, 0.0))
    if learned_floor > 0:
        ram_target = max(ram_target, min(avail_ram, learned_floor))

    # Ensure within current availability
    cpu_target = min(cpu_target, avail_cpu)
    ram_target = min(ram_target, avail_ram)
    return cpu_target, ram_target


def _collect_running_from_results(s, results):
    # Update running container map from results stream; handle completion/failure by removing.
    for r in results:
        pool_id = r.pool_id
        if pool_id not in s.running:
            s.running[pool_id] = {}

        # If this result indicates container ended (success/fail), remove it.
        # We don't have an explicit "succeeded" method; treat any result as terminal for its ops.
        # Keep conservative: if failed() then remove; else also remove (container likely ended).
        if getattr(r, "container_id", None) is not None:
            if r.failed() or True:
                s.running[pool_id].pop(r.container_id, None)

        # Learn on OOM-like errors: bump RAM floor for the specific op(s) in that container.
        if r.failed() and getattr(r, "error", None):
            err = str(r.error).lower()
            if "oom" in err or "out of memory" in err or "memory" in err:
                # Increase RAM based on what was attempted.
                attempted_ram = float(getattr(r, "ram", 0.0) or 0.0)
                new_floor = max(attempted_ram * s.oom_ram_mult, attempted_ram + s.oom_ram_add, attempted_ram + 1.0)
                # We don't have direct pipeline_id per op in results; but results include ops.
                # We'll key by (unknown pipeline_id, op_id) if pipeline_id unavailable; best effort.
                for op in getattr(r, "ops", []) or []:
                    # If op has pipeline_id, use it; else fall back to "unknown".
                    pid = getattr(op, "pipeline_id", None)
                    if pid is None:
                        pid = "unknown"
                    key = (pid, getattr(op, "op_id", getattr(op, "operator_id", getattr(op, "id", repr(op)))))
                    s.learned_ram[key] = max(s.learned_ram.get(key, 0.0), new_floor)


def _register_new_assignments_as_running(s, assignments):
    # Record new containers as "running" only if we could identify container_id, which we can't at assignment time.
    # So we don't add here; instead preemption relies on pool.running map populated externally.
    # Keep placeholder to allow later extension.
    return


def _find_preemption_candidate(s, pool_id, needed_cpu, needed_ram):
    # Choose a lowest-priority running container to preempt, preferring largest resource holder.
    running = s.running.get(pool_id, {})
    if not running:
        return None

    # Sort by (priority asc, cpu+ram desc). Assuming Priority enum has ordering? Don't rely on it.
    # We'll map to numeric ranks.
    def prio_rank(p):
        if p == Priority.QUERY:
            return 3
        if p == Priority.INTERACTIVE:
            return 2
        return 1

    candidates = []
    for cid, meta in running.items():
        candidates.append((prio_rank(meta.get("priority")), (meta.get("cpu", 0.0) + meta.get("ram", 0.0)), cid, meta))

    # Prefer preempting lowest rank (batch), then interactive, never query unless necessary.
    candidates.sort(key=lambda x: (x[0], -x[1]))
    for rank, _, cid, meta in candidates:
        # Only preempt if it would plausibly free enough resources
        freed_cpu = float(meta.get("cpu", 0.0) or 0.0)
        freed_ram = float(meta.get("ram", 0.0) or 0.0)
        if freed_cpu >= needed_cpu or freed_ram >= needed_ram or (freed_cpu + freed_ram) >= (needed_cpu + needed_ram):
            return cid
    # Otherwise, still allow preempting the biggest low-priority container
    return candidates[0][2] if candidates else None


@register_scheduler(key="scheduler_none_027")
def scheduler_none_027(s, results, pipelines):
    """
    Priority-aware scheduler with OOM backoff and limited preemption.

    Decision procedure per tick:
      - Enqueue new pipelines into per-priority FIFO queues.
      - Process results: learn RAM floors on OOM-ish failures and update running set.
      - For each pool: greedily assign ready operators, always preferring higher priority,
        and using capped sizing to avoid monopolization.
      - If a QUERY/INTERACTIVE op is ready but doesn't fit, preempt one low-priority container
        (at most one per tick) in that pool and retry assignment.
    """
    # Ingest arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update learning and running state from results
    if results:
        _collect_running_from_results(s, results)

    # Early exit
    if not pipelines and not results and not _peek_has_any(s):
        return [], []

    suspensions = []
    assignments = []
    did_preempt = False

    # We'll cycle through pools and attempt to place multiple ops per pool per tick.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # We'll do a bounded number of scheduling attempts per pool to avoid infinite loops.
        attempts = 0
        max_attempts = 64

        while attempts < max_attempts:
            attempts += 1

            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            pipeline = _pop_next_pipeline(s)
            if pipeline is None:
                break

            status = pipeline.runtime_status()
            has_failures = status.state_counts[OperatorState.FAILED] > 0

            # Drop completed or permanently failed pipelines (naive approach: don't keep retrying FAILED pipelines here)
            if status.is_pipeline_successful() or has_failures:
                continue

            # Get one assignable op whose parents are complete
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; requeue and move on
                _enqueue_pipeline(s, pipeline)
                continue

            # Size conservatively; apply learned RAM floor
            cpu_req, ram_req = _choose_sizing(s, pool, pipeline, op_list, pipeline.priority)

            # If it doesn't fit, consider preemption for high priority only.
            if cpu_req <= 0 or ram_req <= 0 or cpu_req > avail_cpu or ram_req > avail_ram:
                # Put pipeline back before preemption to preserve ordering.
                _enqueue_pipeline(s, pipeline)

                if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                    if s.preempt_at_most_one_per_tick and did_preempt:
                        # Can't preempt more this tick
                        break

                    needed_cpu = max(0.0, cpu_req - avail_cpu)
                    needed_ram = max(0.0, ram_req - avail_ram)
                    cid = _find_preemption_candidate(s, pool_id, needed_cpu, needed_ram)
                    if cid is not None:
                        suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
                        # Optimistically remove from running set so we don't select it again this tick
                        s.running.get(pool_id, {}).pop(cid, None)
                        did_preempt = True
                        # After preemption, retry loop to see if resources now available (sim will apply suspension)
                        continue
                # Can't do anything else in this pool right now
                break

            # Create assignment
            assignment = Assignment(
                ops=op_list,
                cpu=cpu_req,
                ram=ram_req,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update fairness counter based on what we dispatched
            if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                s.consecutive_hi += 1
            else:
                s.consecutive_hi = 0

            # Requeue pipeline for remaining operators
            _enqueue_pipeline(s, pipeline)

            # Continue scheduling additional work in this pool if capacity remains
            continue

    _register_new_assignments_as_running(s, assignments)
    return suspensions, assignments
