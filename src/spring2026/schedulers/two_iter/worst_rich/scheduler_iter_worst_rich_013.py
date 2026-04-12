# policy_key: scheduler_iter_worst_rich_013
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046740
# generation_seconds: 28.19
# generated_at: 2026-04-12T00:56:06.544756
@register_scheduler_init(key="scheduler_iter_worst_rich_013")
def scheduler_iter_worst_rich_013_init(s):
    """
    Priority-first FIFO with "OOM-avoidant" sizing and retry-on-failure.

    Key fixes vs the previous attempt (which OOMed everything):
      - Do NOT drop pipelines just because they contain FAILED ops; FAILED is retryable in Eudoxia.
      - Allocate RAM aggressively by default (near pool capacity) since extra RAM has no perf penalty,
        and undersizing causes catastrophic OOM + zero throughput.
      - Keep simple per-(pipeline,op) RAM hints that grow on OOM to converge quickly when defaults
        still aren't enough.
      - Soft pool preference: if multiple pools exist, try to keep pool 0 for QUERY/INTERACTIVE by
        not placing BATCH there when any higher-priority runnable work exists.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Prevent duplicate enqueues across ticks
    s.enqueued = set()  # pipeline_id

    # Per-op RAM hints learned from OOM; keyed by (pipeline_id, op_key)
    s.op_ram_hint = {}

    # Scheduler tick counter
    s.tick = 0

    # Default sizing knobs (conservative on CPU, aggressive on RAM)
    s.default_cpu_query = 4
    s.default_cpu_interactive = 2
    s.default_cpu_batch = 2

    # Allocate most of the pool RAM to avoid OOM; bounded by availability
    s.default_ram_frac = 0.85

    # Retry growth factor on OOM
    s.oom_ram_growth = 2.0


@register_scheduler(key="scheduler_iter_worst_rich_013")
def scheduler_iter_worst_rich_013_scheduler(s, results, pipelines):
    """
    Each tick:
      1) Enqueue new pipelines into per-priority queues (QUERY > INTERACTIVE > BATCH).
      2) Learn RAM hints from OOM failures (double requested RAM for that op next time).
      3) For each pool, schedule at most one ready operator, picking highest priority runnable work.
         - If there are any runnable QUERY/INTERACTIVE ops system-wide, avoid scheduling BATCH on pool 0.
      4) Size CPU modestly; size RAM aggressively (extra RAM is safe, too little RAM is fatal).
    """
    s.tick += 1

    # ---- helpers ----
    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue(p):
        pid = p.pipeline_id
        if pid in s.enqueued:
            return
        s.enqueued.add(pid)
        _queue_for_priority(p.priority).append(p)

    def _op_key(op):
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _runnable_ops(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return []
        # Allow retrying FAILED ops (ASSIGNABLE_STATES includes FAILED)
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return ops[:1] if ops else []

    def _any_high_prio_runnable_exists():
        # Bounded scan of queues: check if there is any runnable QUERY/INTERACTIVE pipeline.
        for q in (s.q_query, s.q_interactive):
            for i in range(len(q)):
                p = q[0]
                q.rotate(-1)
                if _runnable_ops(p):
                    return True
        return False

    def _pick_from_queue(q, allow_rotate=True):
        # Find first runnable pipeline in this queue; rotate to preserve FIFO fairness.
        for _ in range(len(q)):
            p = q[0]
            q.rotate(-1)  # move head to tail
            # Note: we keep pipelines in the queue permanently; completion is lazily ignored.
            ops = _runnable_ops(p)
            if ops:
                return p, ops
        return None, None

    def _default_cpu_for_priority(prio, pool_max_cpu):
        if prio == Priority.QUERY:
            return min(max(1, s.default_cpu_query), max(1, int(pool_max_cpu)))
        if prio == Priority.INTERACTIVE:
            return min(max(1, s.default_cpu_interactive), max(1, int(pool_max_cpu)))
        return min(max(1, s.default_cpu_batch), max(1, int(pool_max_cpu)))

    def _size_request(p, op, pool_id):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        # CPU: small fixed default per priority, bounded by availability.
        req_cpu = min(avail_cpu, _default_cpu_for_priority(p.priority, pool.max_cpu_pool))

        # RAM: allocate aggressively (fraction of pool max), then apply per-op hint if larger.
        base_ram = int(max(1, pool.max_ram_pool * s.default_ram_frac))
        hint_key = (p.pipeline_id, _op_key(op))
        hinted_ram = s.op_ram_hint.get(hint_key, 0)
        req_ram = max(base_ram, hinted_ram, 1)

        # Bound by availability and pool max.
        req_ram = min(req_ram, avail_ram, pool.max_ram_pool)

        return req_cpu, req_ram

    # ---- ingest new pipelines ----
    for p in pipelines:
        _enqueue(p)

    # ---- learn from results ----
    for r in results:
        if hasattr(r, "failed") and r.failed():
            err = str(getattr(r, "error", "") or "")
            oom_like = "oom" in err.lower() or "out of memory" in err.lower()
            if not oom_like:
                continue

            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                continue

            ran_ops = getattr(r, "ops", []) or []
            # If we can't identify ops, we can't learn anything useful.
            if not ran_ops:
                continue

            # Double the RAM request used for that container, per op (bounded later by pool max/avail).
            prev_r_ram = int(getattr(r, "ram", 1) or 1)
            bumped = max(prev_r_ram + 1, int(prev_r_ram * s.oom_ram_growth))

            for op in ran_ops:
                k = (pid, _op_key(op))
                s.op_ram_hint[k] = max(s.op_ram_hint.get(k, 0), bumped)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # If multiple pools exist, protect pool 0 when there is high-priority runnable work.
    protect_pool0 = (s.executor.num_pools > 1) and _any_high_prio_runnable_exists()

    # ---- schedule one op per pool per tick ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        chosen_p = None
        chosen_ops = None

        # Priority order: QUERY > INTERACTIVE > BATCH
        # But if protecting pool 0, avoid BATCH on pool 0 when any high-priority runnable exists.
        for q in (s.q_query, s.q_interactive, s.q_batch):
            if protect_pool0 and pool_id == 0 and q is s.q_batch:
                continue
            p, ops = _pick_from_queue(q)
            if p is not None:
                chosen_p, chosen_ops = p, ops
                break

        if chosen_p is None:
            continue

        op = chosen_ops[0]
        req_cpu, req_ram = _size_request(chosen_p, op, pool_id)
        if req_cpu <= 0 or req_ram <= 0:
            continue

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=req_cpu,
                ram=req_ram,
                priority=chosen_p.priority,
                pool_id=pool_id,
                pipeline_id=chosen_p.pipeline_id,
            )
        )

    return suspensions, assignments
