# policy_key: scheduler_iter_best_simple_006
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040191
# generation_seconds: 36.45
# generated_at: 2026-04-11T22:42:03.433186
@register_scheduler_init(key="scheduler_iter_best_simple_006")
def scheduler_iter_best_simple_006_init(s):
    """Priority + light SRPT + reservation scheduler (incremental over naive FIFO).

    Goals: reduce *weighted latency* by (a) protecting high-priority work from batch interference,
    (b) finishing short high-priority pipelines sooner, and (c) avoiding pool monopolization.

    Main ideas:
      1) Strict priority queues: QUERY > INTERACTIVE > BATCH.
      2) Headroom reservation: when any high-priority work is waiting, keep CPU/RAM reserved
         in each pool so batch can't consume the last resources.
      3) Light SRPT within a priority: choose the pipeline with fewest remaining operators
         (proxy for shortest remaining processing time).
      4) OOM-aware RAM backoff: if a container fails with an OOM-like error, increase RAM
         multiplier for those ops on retry (bounded).
      5) Conservative per-container caps to avoid one op taking an entire pool.
    """
    # Separate waiting queues per priority
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track pipelines we should stop trying (non-OOM failures can be terminal)
    s.dead_pipelines = set()

    # Per-operator RAM backoff multiplier: key -> multiplier
    s.op_ram_mult = {}

    # Bounded scan length for "pick shortest" within a queue (keeps scheduler cheap)
    s.scan_k = 12

    def _op_key(op, pipeline_id):
        # Prefer stable ids if present; otherwise fall back to object id.
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_simple_006")
def scheduler_iter_best_simple_006_scheduler(s, results, pipelines):
    """See init docstring for the policy overview."""
    # ----- helpers -----
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _enqueue(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _has_hp_backlog() -> bool:
        return bool(s.q_query) or bool(s.q_interactive)

    def _remaining_ops_proxy(p) -> int:
        # Proxy for remaining work: count of non-completed states (as available).
        st = p.runtime_status()
        sc = getattr(st, "state_counts", None)
        if not sc:
            # Fallback: treat as medium-length if we can't see counts
            return 10
        rem = 0
        for state in (OperatorState.PENDING, OperatorState.ASSIGNED, OperatorState.RUNNING,
                      OperatorState.SUSPENDING, OperatorState.FAILED):
            rem += sc.get(state, 0)
        # Never return 0 unless successful, to keep ordering stable.
        return rem if rem > 0 else 1

    def _pick_pipeline_and_op_from_queue(q):
        # Choose a runnable pipeline with smallest remaining-ops proxy among first K items.
        if not q:
            return None, None

        best_idx = None
        best_score = None
        best_op_list = None

        n = len(q)
        k = s.scan_k if s.scan_k < n else n
        # Scan first k pipelines; keep non-runnable ones in place for now.
        for i in range(k):
            p = q[i]
            if p.pipeline_id in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                continue
            score = _remaining_ops_proxy(p)
            if best_score is None or score < best_score:
                best_score = score
                best_idx = i
                best_op_list = op_list

        if best_idx is None:
            return None, None

        p = q.pop(best_idx)
        return p, best_op_list

    def _pick_next():
        # Strict priority across queues.
        p, op_list = _pick_pipeline_and_op_from_queue(s.q_query)
        if p:
            return p, op_list
        p, op_list = _pick_pipeline_and_op_from_queue(s.q_interactive)
        if p:
            return p, op_list
        return _pick_pipeline_and_op_from_queue(s.q_batch)

    def _reserve_for_hp(pool):
        # Keep headroom if any high-priority backlog exists.
        if not _has_hp_backlog():
            return 0.0, 0.0

        # Stronger reservation if queries are waiting.
        if s.q_query:
            cpu_r = max(1.0, 0.35 * pool.max_cpu_pool)
            ram_r = max(0.0, 0.25 * pool.max_ram_pool)
        else:
            cpu_r = max(1.0, 0.25 * pool.max_cpu_pool)
            ram_r = max(0.0, 0.20 * pool.max_ram_pool)

        # Never reserve more than max-1 container worth; keep system making progress.
        cpu_r = min(cpu_r, max(0.0, pool.max_cpu_pool - 1.0))
        ram_r = min(ram_r, max(0.0, pool.max_ram_pool * 0.80))
        return cpu_r, ram_r

    def _size_for(priority, pool, op, pipeline_id, avail_cpu, avail_ram, allow_batch_full_speed: bool):
        # CPU caps (aim to finish queries/interactive faster; keep batch from dominating when HP exists).
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        if priority == Priority.QUERY:
            # Give queries more CPU to reduce latency; still cap to prevent full monopolization.
            cpu_cap = min(max_cpu * 0.70, 10.0)
            ram_frac = 0.18
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(max_cpu * 0.55, 8.0)
            ram_frac = 0.22
        else:
            if allow_batch_full_speed:
                cpu_cap = min(max_cpu * 0.75, 10.0)
                ram_frac = 0.35
            else:
                # When HP backlog exists, throttle batch to reduce interference and keep headroom.
                cpu_cap = min(2.0, max_cpu * 0.20)
                ram_frac = 0.18

        cpu = min(avail_cpu, cpu_cap)
        if cpu <= 0:
            return 0.0, 0.0

        # RAM: baseline fraction + OOM backoff multiplier
        base_ram = max_ram * ram_frac
        mult = s.op_ram_mult.get(s._op_key(op, pipeline_id), 1.0)
        ram = base_ram * mult
        ram = min(ram, avail_ram, max_ram)
        if ram <= 0:
            return 0.0, 0.0

        return cpu, ram

    # ----- ingest new pipelines -----
    for p in pipelines:
        _enqueue(p)

    # Early exit if nothing happened that can change decisions.
    if not pipelines and not results:
        return [], []

    # ----- process execution results -----
    for r in results:
        if not r.failed():
            continue

        if _is_oom_error(r.error):
            # Increase RAM multiplier for the failed ops; retry will pick larger RAM next time.
            for op in (r.ops or []):
                # ExecutionResult doesn't guarantee pipeline_id exists; use None if missing.
                pid = getattr(r, "pipeline_id", None)
                k = s._op_key(op, pid)
                cur = s.op_ram_mult.get(k, 1.0)
                nxt = cur * 2.0
                if nxt > 16.0:
                    nxt = 16.0
                s.op_ram_mult[k] = nxt
        else:
            # If result includes pipeline_id, mark it dead; otherwise let pipeline status handle it.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    # ----- schedule per pool -----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Reserve headroom for high priority
        cpu_reserve, ram_reserve = _reserve_for_hp(pool)

        # Keep scheduling while we have resources
        while avail_cpu > 0.01 and avail_ram > 0.01:
            p, op_list = _pick_next()
            if not p:
                break

            # If this is batch and we need to keep headroom for HP, enforce reservation.
            is_batch = (p.priority == Priority.BATCH_PIPELINE)
            hp_backlog = _has_hp_backlog()

            if is_batch and hp_backlog:
                # If scheduling batch would cut into reserved headroom, stop scheduling batch in this pool.
                if avail_cpu <= cpu_reserve + 0.01 or avail_ram <= ram_reserve + 0.01:
                    # Put it back and stop trying to place more work in this pool right now.
                    _enqueue(p)
                    break

            allow_batch_full_speed = (not hp_backlog)

            op = op_list[0]
            cpu, ram = _size_for(
                p.priority, pool, op, p.pipeline_id, avail_cpu, avail_ram, allow_batch_full_speed
            )

            if cpu <= 0.01 or ram <= 0.01:
                # Can't place now; requeue and stop for this pool to avoid spinning.
                _enqueue(p)
                break

            # If this is batch under HP backlog, ensure we still respect reservations after allocation.
            if is_batch and hp_backlog:
                if (avail_cpu - cpu) < cpu_reserve - 0.01 or (avail_ram - ram) < ram_reserve - 0.01:
                    # Too costly right now; requeue and stop.
                    _enqueue(p)
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

            # Round-robin fairness within priority: put pipeline back for its next runnable op.
            _enqueue(p)

    return suspensions, assignments
