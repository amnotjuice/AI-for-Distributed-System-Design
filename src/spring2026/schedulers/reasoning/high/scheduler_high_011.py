# policy_key: scheduler_high_011
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.155929
# generation_seconds: 387.99
# generated_at: 2026-04-10T00:47:34.268951
@register_scheduler_init(key="scheduler_high_011")
def scheduler_high_011_init(s):
    """
    Priority-aware, OOM-averse scheduler to reduce weighted latency while avoiding failures.

    Core ideas:
      1) Priority queues + weighted round-robin selection (QUERY > INTERACTIVE > BATCH)
      2) Conservative RAM sizing to avoid OOM (failures are heavily penalized)
      3) Per-operator adaptive RAM hints (exponential backoff on OOM; gentle decay on success)
      4) Soft pool preference: if multiple pools exist, prefer pool 0 for QUERY/INTERACTIVE
         but still allow spillover to other pools to reduce tail latency.
      5) Anti-starvation: aged batch pipelines may occasionally run even under high-priority load.
    """
    from collections import deque, defaultdict

    # Pipeline tracking (lazy removal from deques; we skip stale IDs on pop)
    s.pipeline_map = {}                 # pipeline_id -> Pipeline
    s.enqueue_tick = {}                 # pipeline_id -> tick when first seen
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.in_queue = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }
    s.abandoned_pipelines = set()       # pipeline_ids we stop scheduling to avoid infinite retries

    # Per-operator adaptive hints, keyed by token = id(op)
    s.op_ram_hint = {}                  # token -> ram to request next time (absolute units)
    s.op_cpu_hint = {}                  # token -> cpu to request next time (absolute units)
    s.op_failures = defaultdict(int)    # token -> number of failures observed
    s.op_last_fail_oom = {}             # token -> bool

    # Round-robin priority mix: bias strongly toward QUERY, then INTERACTIVE, then BATCH
    s.rr_order = [
        Priority.QUERY, Priority.QUERY, Priority.QUERY,
        Priority.INTERACTIVE, Priority.INTERACTIVE,
        Priority.BATCH_PIPELINE,
    ]
    s.rr_index = 0

    # Tuning knobs
    s.tick = 0
    s.max_retries_per_op = 4            # retry OOM a few times (exponential RAM backoff)
    s.batch_aging_ticks = 60            # after this, batch gets an "aged" chance to run
    s.max_no_progress_pops = 8          # limit per-pool churn when nothing is runnable
    s.aged_batch_budget_per_tick = 1    # allow at most N aged-batch schedules per tick


@register_scheduler(key="scheduler_high_011")
def scheduler_high_011(s, results: List[ExecutionResult],
                       pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    # ----------------------------
    # Helper functions (no imports)
    # ----------------------------
    def _op_token(op) -> int:
        # Operator objects are assumed stable across ticks; id(op) is sufficient.
        return id(op)

    def _is_oom_error(err) -> bool:
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _base_cpu(priority, pool) -> float:
        max_cpu = float(pool.max_cpu_pool)
        if priority == Priority.QUERY:
            return max(2.0, 0.33 * max_cpu)
        if priority == Priority.INTERACTIVE:
            return max(2.0, 0.25 * max_cpu)
        return max(1.0, 0.20 * max_cpu)

    def _base_ram(priority, pool) -> float:
        max_ram = float(pool.max_ram_pool)
        # Conservative-by-default RAM to reduce OOM risk (failures are extremely costly).
        if priority == Priority.QUERY:
            return max(1.0, 0.18 * max_ram)
        if priority == Priority.INTERACTIVE:
            return max(1.0, 0.14 * max_ram)
        return max(1.0, 0.10 * max_ram)

    def _desired_cpu_for_op(priority, pool, token) -> float:
        base = _base_cpu(priority, pool)
        hint = s.op_cpu_hint.get(token, 0.0)
        desired = max(base, float(hint))
        return min(desired, float(pool.max_cpu_pool))

    def _desired_ram_for_op(priority, pool, token) -> float:
        base = _base_ram(priority, pool)
        hint = s.op_ram_hint.get(token, 0.0)
        desired = max(base, float(hint))
        return min(desired, float(pool.max_ram_pool))

    def _drop_pipeline(pid: str):
        s.pipeline_map.pop(pid, None)
        s.enqueue_tick.pop(pid, None)
        s.abandoned_pipelines.discard(pid)
        # Remove from in-queue sets; deques are lazily cleaned.
        for pr in s.in_queue:
            s.in_queue[pr].discard(pid)

    def _ensure_enqueued(p: Pipeline):
        pid = p.pipeline_id
        if pid in s.abandoned_pipelines:
            return
        if pid not in s.pipeline_map:
            s.pipeline_map[pid] = p
            s.enqueue_tick[pid] = s.tick
        # Enqueue if not already queued for its priority
        pr = p.priority
        if pid not in s.in_queue[pr]:
            s.queues[pr].append(pid)
            s.in_queue[pr].add(pid)

    def _requeue_pipeline(p: Pipeline):
        # Keep it circulating so we can check readiness again later.
        _ensure_enqueued(p)

    def _has_highprio_backlog() -> bool:
        # Backlog defined as any queued QUERY/INTERACTIVE pipelines (not necessarily runnable).
        return (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    def _pop_next_pipeline(allowed_prios, scheduled_this_tick: set, allow_aged_batch: bool):
        """
        Weighted RR over allowed priorities, skipping stale/finished pipelines.
        If allow_aged_batch is True and budget allows, try to pick an aged batch pipeline.
        """
        # 1) Weighted RR among allowed priorities
        for _ in range(len(s.rr_order)):
            pr = s.rr_order[s.rr_index]
            s.rr_index = (s.rr_index + 1) % len(s.rr_order)
            if pr not in allowed_prios:
                continue

            dq = s.queues[pr]
            while dq:
                pid = dq.popleft()
                s.in_queue[pr].discard(pid)

                p = s.pipeline_map.get(pid)
                if p is None:
                    continue
                if pid in s.abandoned_pipelines:
                    continue
                if pid in scheduled_this_tick:
                    # Already scheduled an op from this pipeline this tick; put back for later.
                    _requeue_pipeline(p)
                    continue

                status = p.runtime_status()
                if status.is_pipeline_successful():
                    _drop_pipeline(pid)
                    continue

                return p

        # 2) Anti-starvation: pick an aged batch pipeline even if high-prio backlog exists
        if allow_aged_batch and s.aged_batch_budget > 0:
            dq = s.queues[Priority.BATCH_PIPELINE]
            scanned = 0
            # Scan a bounded number of items to find an aged one without O(n) blowups.
            limit = min(len(dq), 32)
            while dq and scanned < limit:
                pid = dq.popleft()
                s.in_queue[Priority.BATCH_PIPELINE].discard(pid)
                scanned += 1

                p = s.pipeline_map.get(pid)
                if p is None:
                    continue
                if pid in s.abandoned_pipelines or pid in scheduled_this_tick:
                    _requeue_pipeline(p)
                    continue

                status = p.runtime_status()
                if status.is_pipeline_successful():
                    _drop_pipeline(pid)
                    continue

                age = s.tick - s.enqueue_tick.get(pid, s.tick)
                if age >= s.batch_aging_ticks:
                    s.aged_batch_budget -= 1
                    return p

                # Not aged enough, keep it circulating
                _requeue_pipeline(p)

        return None

    # ----------------------------
    # Tick bookkeeping + learning
    # ----------------------------
    s.tick += 1
    s.aged_batch_budget = s.aged_batch_budget_per_tick

    # Update adaptive per-operator hints based on completed execution results
    for r in results:
        pool = s.executor.pools[r.pool_id]
        prio = r.priority
        base_ram = _base_ram(prio, pool)
        base_cpu = _base_cpu(prio, pool)

        for op in (r.ops or []):
            tok = _op_token(op)

            if r.failed():
                s.op_failures[tok] += 1
                oom = _is_oom_error(r.error)
                s.op_last_fail_oom[tok] = oom

                if oom:
                    # Exponential backoff on RAM; also ensure we're above base.
                    prev = float(s.op_ram_hint.get(tok, max(base_ram, float(r.ram))))
                    bumped = max(prev * 2.0, float(r.ram) * 2.0, base_ram * 1.25)
                    s.op_ram_hint[tok] = min(bumped, float(pool.max_ram_pool))
                else:
                    # Non-OOM failures: don't thrash RAM; modestly bump CPU hint once.
                    prev_cpu = float(s.op_cpu_hint.get(tok, base_cpu))
                    s.op_cpu_hint[tok] = min(max(prev_cpu, float(r.cpu)), float(pool.max_cpu_pool))
            else:
                # Success: gently decay RAM hint to improve packing, but stay above base.
                prev = float(s.op_ram_hint.get(tok, 0.0))
                # If we had a hint, decay toward the last successful allocation; else set from it.
                if prev > 0.0:
                    s.op_ram_hint[tok] = max(base_ram, min(prev, float(r.ram) * 0.90))
                else:
                    s.op_ram_hint[tok] = max(base_ram, float(r.ram) * 0.85)

                # CPU hint: keep it close to base unless we explicitly learned a higher need.
                prev_cpu = float(s.op_cpu_hint.get(tok, 0.0))
                learned_cpu = max(base_cpu, float(r.cpu) * 0.90)
                s.op_cpu_hint[tok] = max(prev_cpu, learned_cpu)

                # Reset OOM marker on success to avoid overreacting next time.
                s.op_last_fail_oom[tok] = False

    # ----------------------------
    # Intake: enqueue new pipelines
    # ----------------------------
    for p in pipelines:
        _ensure_enqueued(p)

    # If nothing changed, avoid extra work (but still return correct types)
    if not pipelines and not results:
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Track pipelines we scheduled in this tick (avoid double-scheduling same pipeline)
    scheduled_this_tick = set()

    # ----------------------------
    # Scheduling loop per pool
    # ----------------------------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Pool preference:
        # - If multiple pools, pool 0 is biased toward QUERY/INTERACTIVE when backlog exists.
        # - Otherwise, all priorities allowed everywhere.
        if s.executor.num_pools >= 2 and pool_id == 0 and _has_highprio_backlog():
            allowed = {Priority.QUERY, Priority.INTERACTIVE}
            allow_aged_batch = True  # still allow very old batch occasionally
        else:
            allowed = {Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE}
            allow_aged_batch = True

        no_progress_pops = 0

        while avail_cpu > 0.0 and avail_ram > 0.0 and no_progress_pops < s.max_no_progress_pops:
            p = _pop_next_pipeline(allowed, scheduled_this_tick, allow_aged_batch)
            if p is None:
                break

            pid = p.pipeline_id
            status = p.runtime_status()

            # Skip completed pipelines
            if status.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            # Do not schedule pipelines we deliberately abandoned
            if pid in s.abandoned_pipelines:
                _drop_pipeline(pid)
                continue

            # Find exactly one next runnable op (keeps fairness, reduces resource risk)
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # No runnable ops right now (parents running, etc.); requeue and move on.
                _requeue_pipeline(p)
                no_progress_pops += 1
                continue

            op = op_list[0]
            tok = _op_token(op)

            # Stop infinite retry loops; abandoning is better than thrashing the cluster.
            # (Still yields a penalty, but avoids blocking many others and cascading misses.)
            if s.op_failures.get(tok, 0) >= s.max_retries_per_op:
                s.abandoned_pipelines.add(pid)
                _drop_pipeline(pid)
                continue

            desired_ram = _desired_ram_for_op(p.priority, pool, tok)
            desired_cpu = _desired_cpu_for_op(p.priority, pool, tok)

            # Admission: avoid knowingly under-allocating RAM (likely OOM, which is catastrophic).
            # If we can't give ~90% of desired RAM in this pool right now, defer.
            if avail_ram < 0.90 * desired_ram:
                _requeue_pipeline(p)
                no_progress_pops += 1
                continue

            # CPU can be opportunistic; but don't schedule with near-zero CPU.
            cpu = min(avail_cpu, desired_cpu)
            ram = min(avail_ram, desired_ram)

            if cpu < 0.5 or ram < 0.5:
                _requeue_pipeline(p)
                no_progress_pops += 1
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            avail_cpu -= cpu
            avail_ram -= ram

            scheduled_this_tick.add(pid)
            _requeue_pipeline(p)
            no_progress_pops = 0

    return suspensions, assignments
