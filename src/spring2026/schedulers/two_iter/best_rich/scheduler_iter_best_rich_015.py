# policy_key: scheduler_iter_best_rich_015
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.055735
# generation_seconds: 49.59
# generated_at: 2026-04-12T00:30:34.210556
@register_scheduler_init(key="scheduler_iter_best_rich_015")
def scheduler_iter_best_rich_015_init(s):
    """Incremental improvement over priority FIFO to reduce weighted latency.

    Changes vs prior iteration:
      - Much smaller *initial* RAM allocations to increase concurrency (memory was over-reserved).
      - Adaptive backoff on BOTH OOM (RAM multiplier) and timeout (CPU multiplier).
      - Simple headroom reservation: keep CPU/RAM slack when higher-priority work is waiting,
        so batch does not consume the entire pool and cause queueing/tail latency.
      - Aging within each priority queue using enqueue tick to reduce starvation/indefinite waits.

    Intentionally still avoids preemption (insufficient visibility into running containers with the
    interfaces demonstrated in the template).
    """
    s.tick = 0

    # Per-priority queues of pipeline_ids (store ids to avoid duplicating objects endlessly).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # pipeline_id -> Pipeline (latest seen object)
    s.pipelines_by_id = {}

    # pipeline_id -> first enqueue tick (for aging)
    s.enq_tick = {}

    # To avoid endless retries on non-resource failures
    s.dead_pipelines = set()

    # (pipeline_id, op_id) -> multipliers / history
    s.op_ram_mult = {}   # exponential backoff on OOM
    s.op_cpu_mult = {}   # exponential backoff on timeout
    s.op_last_good = {}  # last (cpu, ram) that succeeded for this op

    s.seq = 0  # stable FIFO tie-breaker if needed

    def _op_id(op):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return oid

    s._op_id = _op_id


@register_scheduler(key="scheduler_iter_best_rich_015")
def scheduler_iter_best_rich_015_scheduler(s, results, pipelines):
    """Priority-aware, adaptive-sizing scheduler with headroom reservation."""
    s.tick += 1

    # --- helpers ---
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _is_timeout_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("time out" in msg)

    def _priority_queue(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            return
        s.pipelines_by_id[pid] = p
        if pid not in s.enq_tick:
            s.enq_tick[pid] = s.tick
        _priority_queue(p.priority).append(pid)

    def _drop_pipeline(pid):
        s.dead_pipelines.add(pid)
        s.pipelines_by_id.pop(pid, None)
        s.enq_tick.pop(pid, None)

    def _pending_counts():
        # Approximate: just queue lengths (ids may include pipelines that became non-runnable;
        # we handle that when popped).
        return len(s.q_query), len(s.q_interactive), len(s.q_batch)

    def _pick_next_runnable_pipeline():
        # Scan priorities in order; within a priority, use aging by cycling the queue.
        # Returns (Pipeline, [op]) or (None, None).
        for q in (s.q_query, s.q_interactive, s.q_batch):
            if not q:
                continue
            n = len(q)
            best_idx = None
            best_age = -1

            # Pick the *oldest enqueued* among a bounded scan of the queue front.
            # (Still O(n), but queues are already being iterated; keeps it simple and robust.)
            for i in range(n):
                pid = q[i]
                if pid in s.dead_pipelines:
                    continue
                age = s.tick - s.enq_tick.get(pid, s.tick)
                if age > best_age:
                    best_age = age
                    best_idx = i

            if best_idx is None:
                # all dead; clear quickly
                q.clear()
                continue

            pid = q.pop(best_idx)
            p = s.pipelines_by_id.get(pid)
            if p is None or pid in s.dead_pipelines:
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                # Completed; purge metadata.
                _drop_pipeline(pid)
                continue

            has_failures = st.state_counts[OperatorState.FAILED] > 0
            if has_failures:
                # Allow retries (ASSIGNABLE_STATES includes FAILED), but if the simulator marks
                # pipeline irrecoverable in some way, it will stop producing ops.
                pass

            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable right now; rotate back to preserve aging.
                q.append(pid)
                continue

            return p, op_list

        return None, None

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _size_for(pool, priority, op, pipeline_id, avail_cpu, avail_ram, reserve_cpu, reserve_ram):
        # Leave headroom for higher priorities if they are waiting.
        usable_cpu = max(0.0, avail_cpu - reserve_cpu)
        usable_ram = max(0.0, avail_ram - reserve_ram)

        if usable_cpu <= 0.01 or usable_ram <= 0.01:
            return 0.0, 0.0

        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # Smaller initial RAM to increase concurrency; rely on OOM backoff for heavy ops.
        # (Prior run showed mean allocated ~93% while consumed ~29%.)
        if priority == Priority.QUERY:
            cpu_base = 0.18
            ram_base = 0.06
            cpu_min = 0.5
            ram_min_frac = 0.02
            cpu_cap_frac = 0.30
            ram_cap_frac = 0.18
        elif priority == Priority.INTERACTIVE:
            cpu_base = 0.22
            ram_base = 0.08
            cpu_min = 1.0
            ram_min_frac = 0.03
            cpu_cap_frac = 0.40
            ram_cap_frac = 0.22
        else:  # BATCH_PIPELINE
            cpu_base = 0.30
            ram_base = 0.10
            cpu_min = 1.0
            ram_min_frac = 0.03
            cpu_cap_frac = 0.60
            ram_cap_frac = 0.28

        op_id = s._op_id(op)
        op_key = (pipeline_id, op_id)

        # If we have a known-good size, start closer to it (helps cut repeated timeouts/OOMs).
        last_good = s.op_last_good.get(op_key)

        ram_mult = s.op_ram_mult.get(op_key, 1.0)
        cpu_mult = s.op_cpu_mult.get(op_key, 1.0)

        # Compute requested CPU.
        if last_good is not None:
            base_cpu = max(cpu_min, last_good[0] * 0.85)
        else:
            base_cpu = max(cpu_min, max_cpu * cpu_base)

        req_cpu = base_cpu * cpu_mult
        req_cpu = _clamp(req_cpu, cpu_min, max_cpu * cpu_cap_frac)
        cpu = min(usable_cpu, req_cpu)

        # Compute requested RAM.
        if last_good is not None:
            base_ram = max(max_ram * ram_min_frac, last_good[1] * 0.85)
        else:
            base_ram = max(max_ram * ram_min_frac, max_ram * ram_base)

        req_ram = base_ram * ram_mult
        req_ram = _clamp(req_ram, max_ram * ram_min_frac, max_ram * ram_cap_frac)
        ram = min(usable_ram, req_ram)

        # If we can't allocate meaningful resources, return 0 to stop.
        if cpu <= 0.01 or ram <= 0.01:
            return 0.0, 0.0
        return cpu, ram

    # --- ingest new pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # --- process results to adapt sizing ---
    for r in results:
        if not r.failed():
            # Success: record last good size for each op in the container.
            for op in (r.ops or []):
                pid = getattr(r, "pipeline_id", None)
                # If result doesn't carry pipeline_id, we can't key per pipeline reliably;
                # but we can still keep a generic fallback.
                op_key = (pid, s._op_id(op))
                s.op_last_good[op_key] = (r.cpu, r.ram)

                # On success, very gently decay multipliers (prevents permanent inflation).
                if op_key in s.op_ram_mult:
                    s.op_ram_mult[op_key] = max(1.0, s.op_ram_mult[op_key] * 0.9)
                if op_key in s.op_cpu_mult:
                    s.op_cpu_mult[op_key] = max(1.0, s.op_cpu_mult[op_key] * 0.9)
            continue

        # Failures: update multipliers based on error type.
        err = r.error
        pid = getattr(r, "pipeline_id", None)

        if _is_oom_error(err):
            for op in (r.ops or []):
                op_key = (pid, s._op_id(op))
                cur = s.op_ram_mult.get(op_key, 1.0)
                nxt = cur * 2.0
                if nxt > 32.0:
                    nxt = 32.0
                s.op_ram_mult[op_key] = nxt
        elif _is_timeout_error(err):
            for op in (r.ops or []):
                op_key = (pid, s._op_id(op))
                cur = s.op_cpu_mult.get(op_key, 1.0)
                nxt = cur * 1.7
                if nxt > 16.0:
                    nxt = 16.0
                s.op_cpu_mult[op_key] = nxt
        else:
            # Unknown / non-resource failures: stop trying the pipeline if we can identify it.
            if pid is not None:
                _drop_pipeline(pid)

    suspensions = []
    assignments = []

    # --- scheduling loop per pool ---
    pending_q, pending_i, _pending_b = _pending_counts()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Reserve headroom if higher-priority work exists anywhere (coarse but effective).
        # This reduces batch's ability to fill the pool and helps interactive/query tail latency.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if pending_q > 0:
            reserve_cpu += pool.max_cpu_pool * 0.20
            reserve_ram += pool.max_ram_pool * 0.08
        if pending_i > 0:
            reserve_cpu += pool.max_cpu_pool * 0.12
            reserve_ram += pool.max_ram_pool * 0.06

        # Cap reserve so we can still make progress.
        reserve_cpu = min(reserve_cpu, pool.max_cpu_pool * 0.60)
        reserve_ram = min(reserve_ram, pool.max_ram_pool * 0.35)

        # Fill the pool with as many runnable ops as possible while respecting headroom.
        # Avoid tiny leftover fragments.
        max_assignments_this_pool = 64
        made = 0

        while avail_cpu > 0.01 and avail_ram > 0.01 and made < max_assignments_this_pool:
            p, op_list = _pick_next_runnable_pipeline()
            if p is None:
                break

            # For batch, enforce headroom; for query/interactive, allow using full avail.
            if p.priority == Priority.BATCH_PIPELINE:
                cpu, ram = _size_for(
                    pool, p.priority, op_list[0], p.pipeline_id,
                    avail_cpu, avail_ram, reserve_cpu, reserve_ram
                )
            else:
                cpu, ram = _size_for(
                    pool, p.priority, op_list[0], p.pipeline_id,
                    avail_cpu, avail_ram, 0.0, 0.0
                )

            if cpu <= 0.01 or ram <= 0.01:
                # Can't place now; re-enqueue and stop trying to pack this pool further.
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
            made += 1
            avail_cpu -= cpu
            avail_ram -= ram

            # Re-enqueue pipeline to allow round-robin progress through its DAG.
            _enqueue_pipeline(p)

    return suspensions, assignments
