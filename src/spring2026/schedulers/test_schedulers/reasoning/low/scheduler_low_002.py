# policy_key: scheduler_low_002
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051254
# generation_seconds: 49.48
# generated_at: 2026-04-09T20:18:51.479309
@register_scheduler_init(key="scheduler_low_002")
def scheduler_low_002_init(s):
    """
    Priority-aware, failure-averse scheduler focused on minimizing weighted latency.

    Main ideas:
    - Strict priority order (QUERY > INTERACTIVE > BATCH) with simple aging for BATCH to avoid starvation.
    - Multi-pool preference: keep pool 0 primarily for QUERY/INTERACTIVE when multiple pools exist.
    - Conservative-but-not-tiny initial RAM allocations to reduce OOM probability (since failures are heavily penalized).
    - OOM-aware retry with RAM backoff per operator; limited retries for non-OOM failures to avoid infinite thrash.
    - Avoids preemption (not enough stable container introspection in the provided interface); instead uses reservations.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track pipelines and arrival ordering / aging
    s.pipelines_by_id = {}
    s.enqueue_seq = 0
    s.enqueue_meta = {}  # pipeline_id -> dict(seq=int, last_enqueue_ts=int)

    # Per-operator retry and sizing memory
    # key: (pipeline_id, op_key) -> dict(retries=int, oom_retries=int, last_ram=float)
    s.op_attempts = {}

    # Track current tick for aging
    s.t = 0

    # Tunables (kept simple and conservative)
    s.batch_aging_ticks = 25  # after this many scheduler ticks waiting, batch is allowed to contend earlier
    s.max_oom_retries = 4
    s.max_non_oom_retries = 2
    s.ram_backoff = 1.6  # multiply on OOM
    s.ram_growth_cap_frac = 0.95  # never ask for > 95% of pool max RAM
    s.min_ram_frac_by_pri = {
        Priority.QUERY: 0.45,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.30,
    }
    s.cpu_frac_by_pri = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.65,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Single-pool soft reservations to protect latency for high priority
    s.single_pool_reserve_cpu_frac = 0.35
    s.single_pool_reserve_ram_frac = 0.35


@register_scheduler(key="scheduler_low_002")
def scheduler_low_002_scheduler(s, results, pipelines):
    """
    Scheduler step:
    - Ingest new pipelines into priority queues.
    - Process results to learn about failures (especially OOM) and adjust retry RAM.
    - For each pool, assign at most a few ops (greedy) prioritizing QUERY/INTERACTIVE, with batch aging.
    """
    def _now_tick():
        s.t += 1
        return s.t

    def _pipeline_queue(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(op):
        # Use a stable operator identifier if present, else fall back to Python object identity.
        return getattr(op, "operator_id", getattr(op, "op_id", id(op)))

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        s.pipelines_by_id[pid] = p
        if pid not in s.enqueue_meta:
            s.enqueue_meta[pid] = {"seq": s.enqueue_seq, "last_enqueue_ts": s.t}
            s.enqueue_seq += 1
        else:
            s.enqueue_meta[pid]["last_enqueue_ts"] = s.t
        _pipeline_queue(p.priority).append(pid)

    def _pipeline_done_or_terminal(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # Do NOT treat "has failures" as terminal; we retry (OOM-aware) up to limits.
        return False

    def _pick_next_assignable_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _attempt_record(pid, op):
        k = (pid, _op_key(op))
        if k not in s.op_attempts:
            s.op_attempts[k] = {"retries": 0, "oom_retries": 0, "last_ram": None}
        return s.op_attempts[k]

    def _can_retry(pid, op, was_oom):
        rec = _attempt_record(pid, op)
        if was_oom:
            return rec["oom_retries"] < s.max_oom_retries
        return rec["retries"] < s.max_non_oom_retries

    def _reserve_allows_batch(pool, avail_cpu, avail_ram):
        # In a single pool scenario, keep headroom for future query/interactive arrivals.
        if s.executor.num_pools != 1:
            return True
        reserve_cpu = pool.max_cpu_pool * s.single_pool_reserve_cpu_frac
        reserve_ram = pool.max_ram_pool * s.single_pool_reserve_ram_frac
        return (avail_cpu - reserve_cpu) > 0 and (avail_ram - reserve_ram) > 0

    def _pool_prefers_high_priority(pool_id):
        # If multiple pools, reserve pool 0 mostly for high priority.
        return (s.executor.num_pools >= 2) and (pool_id == 0)

    def _batch_is_aged(pid):
        meta = s.enqueue_meta.get(pid)
        if not meta:
            return False
        waited = s.t - meta["last_enqueue_ts"]
        return waited >= s.batch_aging_ticks

    def _choose_resources(pool, avail_cpu, avail_ram, pri, pid, op):
        # CPU choice: moderate fractions (avoid giving all CPU; allows more concurrency, reduces queueing).
        cpu_target = pool.max_cpu_pool * s.cpu_frac_by_pri.get(pri, 0.5)
        cpu = min(avail_cpu, max(1.0, float(cpu_target)))

        # RAM choice: start conservatively high to avoid OOM penalties; backoff on OOM.
        rec = _attempt_record(pid, op)

        base_frac = s.min_ram_frac_by_pri.get(pri, 0.30)
        base_ram = pool.max_ram_pool * base_frac

        if rec["last_ram"] is not None:
            # Stick close to the last known attempt size (helps converge after an OOM).
            ram = max(base_ram, rec["last_ram"])
        else:
            ram = base_ram

        # Cap and fit to available.
        ram_cap = pool.max_ram_pool * s.ram_growth_cap_frac
        ram = min(ram, ram_cap)
        ram = min(ram, avail_ram)

        # Ensure a small minimum if anything is available.
        if avail_ram > 0:
            ram = max(min(avail_ram, pool.max_ram_pool * 0.10), ram)

        return cpu, ram

    # Advance time.
    _now_tick()

    # Ingest new pipelines.
    for p in pipelines:
        _enqueue_pipeline(p)

    # Process execution results to update retry sizing / bookkeeping.
    for r in results:
        # Record resource usage on last attempt for each op in the result.
        # (We don't know op identity beyond object; use op_key for each op in r.ops.)
        was_fail = r.failed()
        was_oom = was_fail and _is_oom_error(r.error)
        for op in getattr(r, "ops", []) or []:
            # We may not have pipeline_id in result; we rely on op record only when pipeline reappears.
            # Best-effort: use r.priority and r.ops just to update retry RAM for the next attempt.
            # If pipeline_id isn't available, we can't update the per-(pipeline,op) record reliably.
            # So we conservatively skip unless the result provides pipeline_id.
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                continue
            rec = _attempt_record(pid, op)
            rec["last_ram"] = getattr(r, "ram", rec["last_ram"])
            if was_fail:
                rec["retries"] += 1
                if was_oom:
                    rec["oom_retries"] += 1
                    # Increase RAM for the next attempt (bounded later by pool cap / avail).
                    if rec["last_ram"] is not None:
                        rec["last_ram"] = rec["last_ram"] * s.ram_backoff

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper to iterate candidate pipeline IDs in desired order with minimal churn.
    def _pop_next_pid_from_queue(q):
        while q:
            pid = q.pop(0)
            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue
            if _pipeline_done_or_terminal(p):
                continue
            return pid
        return None

    # Try to schedule across pools.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Greedy loop: try to place a small number of ops per pool per tick.
        # Keep it bounded to reduce churn and keep decisions stable.
        placed_here = 0
        max_place = 3

        while placed_here < max_place and avail_cpu > 0 and avail_ram > 0:
            # Pool-level priority selection:
            # - If pool 0 is "high priority", prefer query/interactive unless none runnable.
            # - Otherwise, global strict priority with batch aging.
            candidates = []

            if _pool_prefers_high_priority(pool_id):
                # Strictly prefer high priority; batch only if nothing else.
                candidates = [
                    ("query", s.q_query),
                    ("interactive", s.q_interactive),
                    ("batch", s.q_batch),
                ]
            else:
                candidates = [
                    ("query", s.q_query),
                    ("interactive", s.q_interactive),
                    ("batch", s.q_batch),
                ]

            chosen_pid = None
            chosen_p = None
            chosen_op = None

            # First pass: strict query -> interactive; batch only if allowed (reservation/aging).
            for _, q in candidates:
                pid = _pop_next_pid_from_queue(q)
                if pid is None:
                    continue
                p = s.pipelines_by_id[pid]
                pri = p.priority

                # If considering batch, enforce reservations unless aged.
                if pri == Priority.BATCH_PIPELINE:
                    if (not _reserve_allows_batch(pool, avail_cpu, avail_ram)) and (not _batch_is_aged(pid)):
                        # Put it back and skip for now.
                        q.append(pid)
                        continue

                op = _pick_next_assignable_op(p)
                if op is None:
                    # Nothing runnable yet; requeue and continue.
                    q.append(pid)
                    continue

                chosen_pid, chosen_p, chosen_op = pid, p, op
                break

            if chosen_p is None:
                break

            pri = chosen_p.priority
            rec = _attempt_record(chosen_pid, chosen_op)

            # If the op has already exceeded retry budget (esp. non-OOM), stop retrying to avoid endless loops.
            # This may still incur the 720s penalty, but reduces damage to other pipelines.
            if rec["retries"] >= (s.max_oom_retries + s.max_non_oom_retries):
                # Do not requeue; effectively abandons further work on this pipeline.
                continue

            cpu, ram = _choose_resources(pool, avail_cpu, avail_ram, pri, chosen_pid, chosen_op)

            # If we can't allocate meaningful resources, requeue and stop.
            if cpu <= 0 or ram <= 0:
                _pipeline_queue(pri).append(chosen_pid)
                break

            # Update last_ram with what we're about to try (helps converge even without result.pipeline_id).
            rec["last_ram"] = ram

            assignment = Assignment(
                ops=[chosen_op],
                cpu=cpu,
                ram=ram,
                priority=pri,
                pool_id=pool_id,
                pipeline_id=chosen_pid,
            )
            assignments.append(assignment)

            # Decrease local availability heuristically (the executor will enforce real constraints).
            avail_cpu -= cpu
            avail_ram -= ram
            placed_here += 1

            # Requeue the pipeline to allow next operators later (keeps fairness among same priority).
            _enqueue_pipeline(chosen_p)

    return suspensions, assignments
