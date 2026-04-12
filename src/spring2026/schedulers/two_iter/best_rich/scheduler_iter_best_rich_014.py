# policy_key: scheduler_iter_best_rich_014
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.059529
# generation_seconds: 40.59
# generated_at: 2026-04-12T00:29:44.621912
@register_scheduler_init(key="scheduler_iter_best_rich_014")
def scheduler_iter_best_rich_014_init(s):
    """Priority + latency-focused scheduler (iteration 2).

    Improvements over prior version:
      - Pool partitioning: prefer running QUERY/INTERACTIVE on a reserved pool (pool 0) when possible.
      - Anti-fragmentation RAM sizing: start with smaller RAM slices; rely on OOM-driven backoff to grow.
      - OOM backoff uses last-attempt RAM (floor) to converge faster and reduce repeated OOM churn.
      - SRPT-ish choice within a priority: prefer pipelines with fewer remaining ops (approx) to cut latency.
      - Batch gating: avoid scheduling batch when higher-priority runnable work exists and needs headroom.

    Intent:
      - Reduce weighted latency by protecting high-priority work and reducing retries/timeouts.
    """
    # Per-priority queues (pipelines can appear multiple times over time; we re-enqueue for round-robin)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track first-seen tick for simple aging / tie-breaks
    s._tick = 0
    s.first_seen_tick = {}  # pipeline_id -> tick

    # Mark pipelines we should stop considering (non-OOM hard failures)
    s.dead_pipelines = set()

    # Per-operator RAM tuning:
    # - multiplier grows on OOM (1,2,4,... capped)
    # - floor jumps to 2x last attempted RAM on OOM to converge quickly
    s.op_ram_mult = {}   # op_key -> float
    s.op_ram_floor = {}  # op_key -> float

    def _op_key(op, pipeline_id):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_rich_014")
def scheduler_iter_best_rich_014_scheduler(s, results, pipelines):
    """Priority-aware, SRPT-ish, OOM-adaptive scheduler with light pool partitioning."""
    s._tick += 1

    # ----------------------------
    # Helpers
    # ----------------------------
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.pipeline_id not in s.first_seen_tick:
            s.first_seen_tick[p.pipeline_id] = s._tick
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pipeline_remaining_ops(p, st) -> int:
        # Approximate "remaining" to prefer shorter pipelines (SRPT-like).
        # Use the DAG size if available; else fall back to a coarse estimate from state_counts.
        total = None
        try:
            total = len(getattr(p, "values", []))
        except Exception:
            total = None
        if isinstance(total, int) and total > 0:
            completed = 0
            try:
                completed = st.state_counts.get(OperatorState.COMPLETED, 0)
            except Exception:
                completed = 0
            rem = total - completed
            return rem if rem > 0 else 1

        # Fallback: count pending/failed/assigned/running as "remaining-ish"
        rem = 0
        try:
            rem += st.state_counts.get(OperatorState.PENDING, 0)
            rem += st.state_counts.get(OperatorState.FAILED, 0)
            rem += st.state_counts.get(OperatorState.ASSIGNED, 0)
            rem += st.state_counts.get(OperatorState.RUNNING, 0)
        except Exception:
            rem = 1
        return rem if rem > 0 else 1

    def _pick_next_runnable_from_queue(q, scan_limit=32):
        # Choose the best runnable pipeline among the first scan_limit items:
        # score = remaining_ops - tiny_age_bonus (older wins ties)
        best_idx = None
        best_op_list = None
        best_score = None

        n = len(q)
        if n == 0:
            return None, None

        lim = scan_limit if scan_limit < n else n
        for i in range(lim):
            p = q[i]
            if p.pipeline_id in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                continue

            rem = _pipeline_remaining_ops(p, st)
            age = s._tick - s.first_seen_tick.get(p.pipeline_id, s._tick)
            score = float(rem) - (age * 1e-4)

            if best_score is None or score < best_score:
                best_score = score
                best_idx = i
                best_op_list = op_list

        if best_idx is None:
            return None, None

        p = q.pop(best_idx)
        return p, best_op_list

    def _any_high_prio_runnable_waiting():
        # Used for batch gating to protect interactive latency.
        for q in (s.q_query, s.q_interactive):
            # Quick scan for runnable work; keep cheap.
            lim = 16 if len(q) > 16 else len(q)
            for i in range(lim):
                p = q[i]
                if p.pipeline_id in s.dead_pipelines:
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if op_list:
                    return True
        return False

    def _size_for(priority, pool, op, pipeline_id, avail_cpu, avail_ram):
        # CPU: give high priority more CPU to reduce risk of timeouts; keep batch smaller.
        # RAM: start small to reduce fragmentation; rely on OOM-driven floor/multiplier to grow.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # Priority-specific CPU targets
        if priority == Priority.QUERY:
            cpu_target = min(max_cpu * 0.50, 6.0)
            cpu_min = 1.0
            ram_frac = 0.06
        elif priority == Priority.INTERACTIVE:
            cpu_target = min(max_cpu * 0.45, 8.0)
            cpu_min = 1.0
            ram_frac = 0.08
        else:  # BATCH_PIPELINE
            cpu_target = min(max_cpu * 0.30, 4.0)
            cpu_min = 0.5
            ram_frac = 0.10

        cpu = min(avail_cpu, cpu_target)
        if cpu < cpu_min:
            # If we can't give at least minimal CPU, don't schedule this op now.
            return 0.0, 0.0

        base_ram = max_ram * ram_frac

        op_key = s._op_key(op, pipeline_id)
        mult = s.op_ram_mult.get(op_key, 1.0)
        floor = s.op_ram_floor.get(op_key, 0.0)

        ram = base_ram * mult
        if ram < floor:
            ram = floor

        # Clamp to availability/pool max
        if ram > avail_ram:
            ram = avail_ram
        if ram > max_ram:
            ram = max_ram

        # Require a small but non-trivial RAM slice to avoid tiny allocations
        if ram <= 0.01:
            return 0.0, 0.0

        return cpu, ram

    # ----------------------------
    # Ingest new pipelines
    # ----------------------------
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ----------------------------
    # Process results (OOM adaptation; drop hard failures)
    # ----------------------------
    for r in results:
        if not r.failed():
            continue

        if _is_oom_error(r.error):
            # Increase RAM multiplier and set a floor based on last attempted RAM to converge faster.
            for op in (r.ops or []):
                pid = getattr(r, "pipeline_id", None)
                op_key = s._op_key(op, pid)
                if op_key[0] is None:
                    # Best-effort fallback if result doesn't contain pipeline_id
                    op_key = (None, op_key[1])

                cur_mult = s.op_ram_mult.get(op_key, 1.0)
                nxt_mult = cur_mult * 2.0
                if nxt_mult > 16.0:
                    nxt_mult = 16.0
                s.op_ram_mult[op_key] = nxt_mult

                # Floor to 2x the RAM we tried (if provided)
                tried_ram = getattr(r, "ram", None)
                if tried_ram is not None:
                    cur_floor = s.op_ram_floor.get(op_key, 0.0)
                    nxt_floor = float(tried_ram) * 2.0
                    if nxt_floor > cur_floor:
                        s.op_ram_floor[op_key] = nxt_floor
        else:
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    # ----------------------------
    # Scheduling
    # ----------------------------
    num_pools = s.executor.num_pools
    high_prio_runnable = _any_high_prio_runnable_waiting()

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]

        # Pool partitioning:
        # - If multiple pools exist, reserve pool 0 primarily for QUERY/INTERACTIVE.
        # - Batch prefers non-zero pools.
        is_reserved_pool = (num_pools >= 2 and pool_id == 0)

        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Fill the pool with multiple small assignments while headroom exists.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            picked = None

            # Decide which priority to pull from:
            if is_reserved_pool:
                # Reserved pool: never pull batch if any higher-priority runnable work exists.
                p, op_list = _pick_next_runnable_from_queue(s.q_query, scan_limit=48)
                if p is None:
                    p, op_list = _pick_next_runnable_from_queue(s.q_interactive, scan_limit=48)
                if p is None and not high_prio_runnable:
                    # Spill to batch only when no high-priority runnable exists globally.
                    p, op_list = _pick_next_runnable_from_queue(s.q_batch, scan_limit=24)
                picked = (p, op_list)
            else:
                # Non-reserved pools: still prioritize high-priority, but allow batch.
                p, op_list = _pick_next_runnable_from_queue(s.q_query, scan_limit=32)
                if p is None:
                    p, op_list = _pick_next_runnable_from_queue(s.q_interactive, scan_limit=32)

                # Batch gating: if high-priority runnable exists globally, require extra headroom
                # before starting more batch (keeps capacity available for spillover).
                if p is None:
                    if high_prio_runnable:
                        # Keep at least 25% CPU and 20% RAM free for potential spillover
                        if avail_cpu < (pool.max_cpu_pool * 0.75) or avail_ram < (pool.max_ram_pool * 0.80):
                            break
                    p, op_list = _pick_next_runnable_from_queue(s.q_batch, scan_limit=32)
                picked = (p, op_list)

            p, op_list = picked
            if p is None or not op_list:
                break

            op = op_list[0]
            cpu, ram = _size_for(p.priority, pool, op, p.pipeline_id, avail_cpu, avail_ram)

            if cpu <= 0.01 or ram <= 0.01:
                # Can't place now; re-enqueue and stop filling this pool to avoid tight loops.
                _enqueue_pipeline(p)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            avail_cpu -= cpu
            avail_ram -= ram

            # Round-robin fairness within priority: put pipeline back for subsequent ops.
            _enqueue_pipeline(p)

    return suspensions, assignments
