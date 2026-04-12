# policy_key: scheduler_iter_best_simple_011
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.054387
# generation_seconds: 33.41
# generated_at: 2026-04-11T22:45:09.144477
@register_scheduler_init(key="scheduler_iter_best_simple_011")
def scheduler_iter_best_simple_011_init(s):
    """Priority-first + reserved-capacity backfilling + conservative sizing.

    Incremental improvements over the previous attempt to reduce weighted latency:
      1) Hard preference for QUERY/INTERACTIVE admission: keep explicit CPU/RAM headroom
         reserved in every pool so batch work can't crowd out latency-sensitive work.
      2) Backfill batch only with "excess" capacity beyond the reservation.
      3) Tighter per-container sizing for high priority to increase concurrency and reduce queueing.
      4) OOM-aware RAM backoff keyed robustly (handles missing pipeline_id on results).
      5) Simple aging within each priority (older pipelines in that priority get served first),
         without letting batch consume reserved capacity.

    No preemption yet (not enough visibility into currently-running containers in the provided API).
    """
    # Per-priority FIFO queues (each element is a Pipeline).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Arrival order counter for aging within the same priority.
    s._arrival_seq = 0
    s._arrival_order = {}  # pipeline_id -> seq

    # Track pipelines that should not be retried (non-OOM failures).
    s.dead_pipelines = set()

    # Adaptive RAM multiplier for OOM retries: op_key -> multiplier (1,2,4,... capped)
    s.op_ram_mult = {}

    def _op_key(op, pipeline_id):
        # Best-effort stable key; result objects may not carry pipeline_id.
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_simple_011")
def scheduler_iter_best_simple_011_scheduler(s, results, pipelines):
    """Scheduler step: reserve headroom for high priority; backfill with batch; OOM backoff."""
    # --- helpers ---
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg)

    def _mark_arrival(p):
        if p.pipeline_id not in s._arrival_order:
            s._arrival_seq += 1
            s._arrival_order[p.pipeline_id] = s._arrival_seq

    def _enqueue(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        _mark_arrival(p)
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _cleanup_and_sort_queue(q):
        # Remove completed/dead pipelines and apply simple aging (stable order by arrival).
        kept = []
        for p in q:
            if p.pipeline_id in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            kept.append(p)
        kept.sort(key=lambda p: s._arrival_order.get(p.pipeline_id, 0))
        return kept

    def _pop_runnable_from_queue(q, require_parents=True):
        # Find first runnable pipeline; rotate others (preserve aging order roughly).
        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            if p.pipeline_id in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=require_parents)[:1]
            if ops:
                return p, ops
            q.append(p)
        return None, None

    def _size_for(priority, pool, op, pipeline_id, avail_cpu, avail_ram):
        # Goal: reduce queueing for high priority by keeping containers small-ish.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # CPU caps: keep QUERY tiny to allow concurrency; INTERACTIVE modest; BATCH can be larger.
        if priority == Priority.QUERY:
            cpu_cap = min(1.0, max_cpu * 0.15)
            ram_frac = 0.10
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(2.0, max_cpu * 0.25)
            ram_frac = 0.18
        else:
            cpu_cap = min(6.0, max_cpu * 0.55)
            ram_frac = 0.35

        cpu = min(avail_cpu, cpu_cap)
        if cpu <= 0:
            cpu = 0.0

        base_ram = max_ram * ram_frac
        mult = s.op_ram_mult.get(s._op_key(op, pipeline_id), 1.0)
        ram = min(avail_ram, max_ram, base_ram * mult)
        if ram <= 0:
            ram = 0.0
        return cpu, ram

    def _reservation(pool):
        # Reserve a fraction of each pool for high-priority work to protect weighted latency.
        # QUERY gets strongest protection; INTERACTIVE also protected.
        # These numbers are intentionally simple and can be tuned via Eudoxia.
        cpu_res = pool.max_cpu_pool * 0.25
        ram_res = pool.max_ram_pool * 0.25
        return cpu_res, ram_res

    # --- ingest new pipelines ---
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # --- process results: OOM backoff & dead pipelines on non-OOM failures ---
    for r in results:
        if not r.failed():
            continue

        if _is_oom_error(r.error):
            # Increase RAM multiplier for each op in this failed assignment.
            for op in (r.ops or []):
                # Prefer to key with pipeline_id if we can recover it; else use (None, op_id).
                pid = getattr(r, "pipeline_id", None)
                if pid is None:
                    # If ops are objects reused within a pipeline, id(op) still helps.
                    key = (None, getattr(op, "op_id", getattr(op, "operator_id", id(op))))
                else:
                    key = s._op_key(op, pid)

                cur = s.op_ram_mult.get(key, 1.0)
                nxt = cur * 2.0
                if nxt > 16.0:
                    nxt = 16.0
                s.op_ram_mult[key] = nxt
        else:
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.dead_pipelines.add(pid)

    # --- queue maintenance (remove completed, sort by aging within priority) ---
    s.q_query = _cleanup_and_sort_queue(s.q_query)
    s.q_interactive = _cleanup_and_sort_queue(s.q_interactive)
    s.q_batch = _cleanup_and_sort_queue(s.q_batch)

    suspensions = []
    assignments = []

    # --- scheduling per pool ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        cpu_res, ram_res = _reservation(pool)

        # Phase 1: schedule high priority first (QUERY then INTERACTIVE) without considering reservation.
        # We keep placing until we either run out of headroom or no high-priority work is runnable.
        made_progress = True
        while made_progress and avail_cpu > 0.01 and avail_ram > 0.01:
            made_progress = False

            # QUERY
            p, ops = _pop_runnable_from_queue(s.q_query, require_parents=True)
            if p:
                op = ops[0]
                cpu, ram = _size_for(p.priority, pool, op, p.pipeline_id, avail_cpu, avail_ram)
                if cpu > 0.01 and ram > 0.01:
                    assignments.append(
                        Assignment(
                            ops=ops,
                            cpu=cpu,
                            ram=ram,
                            priority=p.priority,
                            pool_id=pool_id,
                            pipeline_id=p.pipeline_id,
                        )
                    )
                    avail_cpu -= cpu
                    avail_ram -= ram
                    _enqueue(p)  # keep pipeline circulating for subsequent ops
                    made_progress = True
                    continue
                else:
                    # Couldn't fit; put back and stop trying in this pool
                    _enqueue(p)

            # INTERACTIVE
            p, ops = _pop_runnable_from_queue(s.q_interactive, require_parents=True)
            if p and avail_cpu > 0.01 and avail_ram > 0.01:
                op = ops[0]
                cpu, ram = _size_for(p.priority, pool, op, p.pipeline_id, avail_cpu, avail_ram)
                if cpu > 0.01 and ram > 0.01:
                    assignments.append(
                        Assignment(
                            ops=ops,
                            cpu=cpu,
                            ram=ram,
                            priority=p.priority,
                            pool_id=pool_id,
                            pipeline_id=p.pipeline_id,
                        )
                    )
                    avail_cpu -= cpu
                    avail_ram -= ram
                    _enqueue(p)
                    made_progress = True
                else:
                    _enqueue(p)

        # Phase 2: backfill with batch, but only using excess beyond reservation.
        # This prevents batch from consuming capacity that high priority may need soon.
        while avail_cpu > max(0.01, cpu_res) and avail_ram > max(0.01, ram_res):
            p, ops = _pop_runnable_from_queue(s.q_batch, require_parents=True)
            if not p:
                break

            # Only allocate from "excess" above reservation.
            excess_cpu = max(0.0, avail_cpu - cpu_res)
            excess_ram = max(0.0, avail_ram - ram_res)
            if excess_cpu <= 0.01 or excess_ram <= 0.01:
                _enqueue(p)
                break

            op = ops[0]
            cpu, ram = _size_for(p.priority, pool, op, p.pipeline_id, excess_cpu, excess_ram)
            if cpu <= 0.01 or ram <= 0.01:
                _enqueue(p)
                break

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            avail_cpu -= cpu
            avail_ram -= ram
            _enqueue(p)

    return suspensions, assignments
