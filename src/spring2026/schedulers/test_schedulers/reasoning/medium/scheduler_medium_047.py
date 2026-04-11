# policy_key: scheduler_medium_047
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.118143
# generation_seconds: 78.54
# generated_at: 2026-04-09T23:34:00.816790
@register_scheduler_init(key="scheduler_medium_047")
def scheduler_medium_047_init(s):
    """Priority-aware, OOM-adaptive greedy packing with soft reservations + aging fairness.

    Goals (aligned with score):
      - Keep QUERY/INTERACTIVE tail latency low by prioritizing them and reserving headroom.
      - Avoid the 720s penalty by aggressively adapting RAM on OOM and retrying a bounded number of times.
      - Avoid starvation by aging (boosting effective priority with wait time) and never permanently blocking BATCH.
    """
    # Discrete time for simple aging.
    s._tick = 0

    # Per-priority FIFO queues of active pipelines.
    s._q_query = []
    s._q_interactive = []
    s._q_batch = []

    # Pipeline metadata.
    s._pipeline_arrival_tick = {}  # pipeline_id -> tick first seen

    # Per-operator learned RAM estimate (bytes/GB depends on simulator units; we keep in same unit as pool RAM).
    s._op_ram_est = {}  # op_key -> ram_est

    # Per-operator failure tracking to decide retry limits.
    s._op_oom_fails = {}      # op_key -> count
    s._op_nonoom_fails = {}   # op_key -> count


@register_scheduler(key="scheduler_medium_047")
def scheduler_medium_047(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    # ----------------------------
    # Helper functions (no imports)
    # ----------------------------
    def _prio_weight(prio):
        if prio == Priority.QUERY:
            return 10
        if prio == Priority.INTERACTIVE:
            return 5
        return 1  # Priority.BATCH_PIPELINE

    def _is_oom_error(err):
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "alloc" in e)

    def _op_key(op):
        # Try common stable identifiers; fall back to object identity.
        for attr in ("operator_id", "op_id", "id", "name"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if v is not None:
                    return (attr, v)
        return ("py_id", id(op))

    def _default_ram(pool, prio):
        # Conservative but not too small to avoid immediate OOM; tuned to protect high priority completion.
        # Use fractions of pool max, with a small absolute floor.
        floor = max(1, int(0.02 * pool.max_ram_pool))
        if prio == Priority.QUERY:
            return max(floor, int(0.10 * pool.max_ram_pool))
        if prio == Priority.INTERACTIVE:
            return max(floor, int(0.08 * pool.max_ram_pool))
        return max(floor, int(0.06 * pool.max_ram_pool))

    def _desired_cpu(pool, avail_cpu, prio):
        # Sublinear scaling => avoid always taking everything; still bias to high priority.
        if avail_cpu <= 0:
            return 0
        if prio == Priority.QUERY:
            cap = max(2, int(0.60 * pool.max_cpu_pool))
            frac = 0.75
        elif prio == Priority.INTERACTIVE:
            cap = max(1, int(0.45 * pool.max_cpu_pool))
            frac = 0.65
        else:
            cap = max(1, int(0.35 * pool.max_cpu_pool))
            frac = 0.50

        cpu = int(max(1, min(cap, int(frac * avail_cpu) if avail_cpu > 1 else 1)))
        return min(cpu, avail_cpu)

    def _terminal_pipeline(p):
        # If pipeline is successful, or has failures we decided are not worth retrying, treat as terminal.
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True

        # If there are FAILED ops but we exceeded retry budgets for all retryable FAILED ops, treat as terminal.
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
        if not failed_ops:
            return False

        any_retryable = False
        for op in failed_ops:
            k = _op_key(op)
            oom_f = s._op_oom_fails.get(k, 0)
            nonoom_f = s._op_nonoom_fails.get(k, 0)
            # Retry budget: OOM up to 5 times with increasing RAM; non-OOM only once.
            if oom_f < 5 and nonoom_f <= 1:
                any_retryable = True
                break

        return not any_retryable

    def _enqueue_pipeline(p):
        if p.pipeline_id not in s._pipeline_arrival_tick:
            s._pipeline_arrival_tick[p.pipeline_id] = s._tick

        if p.priority == Priority.QUERY:
            s._q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s._q_interactive.append(p)
        else:
            s._q_batch.append(p)

    def _pipeline_age_ticks(p):
        return max(0, s._tick - s._pipeline_arrival_tick.get(p.pipeline_id, s._tick))

    def _pick_ready_op_from_queue(q):
        """Round-robin scan: returns (pipeline, op) or (None, None). Keeps queue order fair."""
        n = len(q)
        for _ in range(n):
            p = q.pop(0)

            # Drop terminal pipelines from the active queue.
            if _terminal_pipeline(p):
                continue

            status = p.runtime_status()
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not op_list:
                q.append(p)
                continue

            # If op is FAILED, ensure we still have retry budget.
            op = op_list[0]
            if getattr(op, "state", None) == OperatorState.FAILED:
                k = _op_key(op)
                if s._op_oom_fails.get(k, 0) >= 5 or s._op_nonoom_fails.get(k, 0) > 1:
                    # Don't spin on hopeless failures.
                    q.append(p)
                    continue

            q.append(p)
            return p, op

        return None, None

    def _has_waiting_high_prio():
        # "Waiting" means present in queues and not terminal; approximate by queue non-empty (cheap).
        return (len(s._q_query) > 0) or (len(s._q_interactive) > 0)

    def _pool_order_for_priority(prio):
        # For high priority: pick pools with most headroom first.
        # For batch: pack into the tightest pool first (better consolidation).
        pools = list(range(s.executor.num_pools))
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            pools.sort(
                key=lambda i: (
                    s.executor.pools[i].avail_cpu_pool / max(1, s.executor.pools[i].max_cpu_pool)
                    + s.executor.pools[i].avail_ram_pool / max(1, s.executor.pools[i].max_ram_pool)
                ),
                reverse=True,
            )
        else:
            pools.sort(
                key=lambda i: (
                    s.executor.pools[i].avail_cpu_pool / max(1, s.executor.pools[i].max_cpu_pool)
                    + s.executor.pools[i].avail_ram_pool / max(1, s.executor.pools[i].max_ram_pool)
                )
            )
        return pools

    def _effective_priority_score(p):
        # Aging: gradually boost older pipelines so BATCH makes progress under sustained high-priority load.
        base = 100 * _prio_weight(p.priority)
        age = _pipeline_age_ticks(p)
        # Add up to +120 points over time (slow ramp), so a very old batch can occasionally jump ahead.
        age_boost = min(120, age // 5)
        return base + age_boost

    def _choose_next_class():
        # Choose which class to schedule next based on:
        #   - readiness (queue non-empty)
        #   - base weights
        #   - aging pressure (max effective score among heads)
        # We approximate by sampling one ready op per queue and using pipeline score.
        candidates = []
        for prio, q in ((Priority.QUERY, s._q_query), (Priority.INTERACTIVE, s._q_interactive), (Priority.BATCH_PIPELINE, s._q_batch)):
            if not q:
                continue
            p, op = _pick_ready_op_from_queue(q)
            if p is None:
                continue
            # We peeked by rotating; nothing else needed.
            candidates.append(( _effective_priority_score(p), prio ))
        if not candidates:
            return None
        candidates.sort(reverse=True)
        return candidates[0][1]

    # ----------------------------
    # Tick / ingest new pipelines
    # ----------------------------
    s._tick += 1

    for p in pipelines:
        _enqueue_pipeline(p)

    # ----------------------------
    # Process results (learn RAM on OOM)
    # ----------------------------
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue

        oom = _is_oom_error(getattr(r, "error", None))
        for op in getattr(r, "ops", []) or []:
            k = _op_key(op)
            if oom:
                s._op_oom_fails[k] = s._op_oom_fails.get(k, 0) + 1
                # Increase RAM estimate aggressively to avoid repeated OOM (completion > wasted time here).
                prev = s._op_ram_est.get(k, None)
                alloc = getattr(r, "ram", None)
                if alloc is None:
                    continue
                if prev is None:
                    new_est = int(max(alloc * 1.7, alloc + 1))
                else:
                    new_est = int(max(prev * 1.5, alloc * 1.7, prev + 1))
                s._op_ram_est[k] = new_est
            else:
                s._op_nonoom_fails[k] = s._op_nonoom_fails.get(k, 0) + 1

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # ----------------------------
    # Scheduling loop (no preemption)
    # ----------------------------
    # Soft reservations to keep headroom for high priority so they don't get stuck behind batch filling the pool.
    # Applied per pool when high-priority is waiting.
    def _reserve_cpu(pool):
        return int(0.20 * pool.max_cpu_pool)

    def _reserve_ram(pool):
        return int(0.20 * pool.max_ram_pool)

    # We schedule in waves: attempt to place high-priority work first (across best pools),
    # then interactive, then batch, but allow aging to occasionally reorder.
    # Implementation: loop until no pool can place any new op.
    made_progress = True
    max_outer_iters = 3 * max(1, s.executor.num_pools)  # prevents pathological loops
    outer = 0

    while made_progress and outer < max_outer_iters:
        outer += 1
        made_progress = False

        next_class = _choose_next_class()
        if next_class is None:
            break

        # Decide pool iteration order based on next class.
        pool_order = _pool_order_for_priority(next_class)

        for pool_id in pool_order:
            pool = s.executor.pools[pool_id]
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                continue

            # If high-priority is waiting, prevent batch from consuming the last reserved share.
            high_waiting = _has_waiting_high_prio()
            if next_class == Priority.BATCH_PIPELINE and high_waiting:
                if avail_cpu <= _reserve_cpu(pool) or avail_ram <= _reserve_ram(pool):
                    continue

            # Pick a ready op from the chosen class queue.
            if next_class == Priority.QUERY:
                q = s._q_query
            elif next_class == Priority.INTERACTIVE:
                q = s._q_interactive
            else:
                q = s._q_batch

            p, op = _pick_ready_op_from_queue(q)
            if p is None:
                continue

            # Determine RAM to allocate.
            k = _op_key(op)
            est_ram = s._op_ram_est.get(k, _default_ram(pool, p.priority))
            est_ram = max(1, int(est_ram))

            # Allow downscaling if pool is tight, but not below 50% of our current estimate (avoid repeated OOM).
            min_acceptable = max(1, int(0.50 * est_ram))
            if avail_ram < min_acceptable:
                # Can't place this op in this pool right now.
                continue
            ram = min(est_ram, avail_ram)

            # Determine CPU to allocate.
            cpu = _desired_cpu(pool, avail_cpu, p.priority)
            if cpu <= 0:
                continue

            # Final batch reservation check after sizing (avoid consuming reserved headroom).
            if p.priority == Priority.BATCH_PIPELINE and high_waiting:
                if (avail_cpu - cpu) < _reserve_cpu(pool) or (avail_ram - ram) < _reserve_ram(pool):
                    continue

            # Create assignment (single-op container).
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            made_progress = True

    return suspensions, assignments
