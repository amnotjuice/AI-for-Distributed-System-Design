# policy_key: scheduler_iter_worst_rich_011
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044640
# generation_seconds: 30.74
# generated_at: 2026-04-12T00:55:01.554464
@register_scheduler_init(key="scheduler_iter_worst_rich_011")
def scheduler_iter_worst_rich_011_init(s):
    """
    Priority-aware FIFO with OOM-avoidant sizing.

    Key fixes vs prior attempt (which OOM'd constantly):
      - Never treat FAILED as terminal for the whole pipeline; keep retrying failed ops.
      - Allocate RAM "generously" (RAM beyond minimum is harmless in this simulator); default
        is near-pool-available, with small headroom reserved for higher priority.
      - Keep CPU moderately sized (sublinear scaling) but still priority-biased.
      - Multi-pool preference: keep pool 0 biased toward QUERY/INTERACTIVE when possible.
      - Simple per-op RAM hint on OOM: if an op OOMs, bump its next RAM request.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # De-dup pipeline enqueue
    s.enqueued = set()  # pipeline_id

    # Per-op RAM hint learned from OOMs
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram

    # Mild anti-starvation: after N ticks, batch behaves like interactive for dispatch ordering
    s.tick = 0
    s.batch_promote_after = 400
    s.pipeline_first_seen = {}  # pipeline_id -> tick


@register_scheduler(key="scheduler_iter_worst_rich_011")
def scheduler_iter_worst_rich_011_scheduler(s, results, pipelines):
    """
    Dispatch at most one operator per pool per tick, prioritizing QUERY > INTERACTIVE > BATCH,
    while allocating enough RAM to avoid OOM retries that destroy completion rate.

    Strategy:
      - Keep three FIFO queues by priority.
      - For each pool, pick the first runnable pipeline from highest effective priority queue.
      - Size RAM primarily from pool availability (reserve headroom for higher priority),
        plus per-op OOM hints; cap only for batch to avoid fully consuming a pool.
      - Size CPU with a priority-biased slice (QUERY gets more), but not necessarily all CPU.
    """
    s.tick += 1

    # ---- helpers ----
    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(op):
        # Best-effort stable identity.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _effective_priority(p):
        # Promote long-waiting batch to interactive to prevent indefinite starvation.
        if p.priority != Priority.BATCH_PIPELINE:
            return p.priority
        first = s.pipeline_first_seen.get(p.pipeline_id, s.tick)
        if (s.tick - first) >= s.batch_promote_after:
            return Priority.INTERACTIVE
        return Priority.BATCH_PIPELINE

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid in s.enqueued:
            return
        s.enqueued.add(pid)
        s.pipeline_first_seen.setdefault(pid, s.tick)
        _queue_for_priority(p.priority).append(p)

    def _cleanup_if_done(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            pid = p.pipeline_id
            if pid in s.enqueued:
                s.enqueued.remove(pid)
            # leave s.pipeline_first_seen to avoid unbounded growth? remove it.
            if pid in s.pipeline_first_seen:
                del s.pipeline_first_seen[pid]
            return True
        return False

    def _ready_ops(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return ops[:1] if ops else []

    def _pool_order_for_priority(eprio):
        # If multiple pools exist, bias pool 0 for latency-sensitive work.
        if s.executor.num_pools <= 1:
            return list(range(s.executor.num_pools))
        if eprio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, s.executor.num_pools)]
        return [i for i in range(1, s.executor.num_pools)] + [0]

    def _size_for(pool_id, p, op):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        eprio = _effective_priority(p)

        # CPU slices: keep them reasonable; giving all CPU to one op can increase queueing delays
        # for other high-priority work. (Still, ensure we can use available CPU when idle.)
        if eprio == Priority.QUERY:
            cpu_target = min(pool.max_cpu_pool, max(2, int(0.75 * pool.max_cpu_pool)))
        elif eprio == Priority.INTERACTIVE:
            cpu_target = min(pool.max_cpu_pool, max(1, int(0.50 * pool.max_cpu_pool)))
        else:
            cpu_target = min(pool.max_cpu_pool, max(1, int(0.35 * pool.max_cpu_pool)))

        cpu = max(1, min(avail_cpu, cpu_target))

        # RAM sizing is the crucial OOM lever. Default: use most available RAM.
        # Reserve headroom so queries can still be admitted when batch is running.
        if eprio == Priority.QUERY:
            reserve_frac = 0.05
            batch_cap_frac = 0.95
        elif eprio == Priority.INTERACTIVE:
            reserve_frac = 0.10
            batch_cap_frac = 0.90
        else:
            reserve_frac = 0.20
            batch_cap_frac = 0.80  # batch should not monopolize RAM in one shot

        # Reserve is only meaningful when multiple pools exist; otherwise keep it smaller.
        if s.executor.num_pools <= 1:
            reserve_frac = min(reserve_frac, 0.08)

        reserve = int(pool.max_ram_pool * reserve_frac)
        usable = max(1, avail_ram - reserve)

        # Apply per-op hint (from OOM). If it exists, honor it up to available.
        key = (p.pipeline_id, _op_key(op))
        hinted = s.op_ram_hint.get(key, 0)

        # Default request: "big" to avoid OOM, but cap batch.
        if eprio == Priority.BATCH_PIPELINE:
            cap = max(1, int(pool.max_ram_pool * batch_cap_frac))
            ram_target = min(usable, cap)
        else:
            ram_target = usable  # give as much as we safely can

        if hinted > 0:
            ram_target = max(ram_target, hinted)

        ram = max(1, min(avail_ram, ram_target))
        return cpu, ram

    # ---- ingest ----
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if no changes
    if not pipelines and not results:
        return [], []

    # ---- learn from results ----
    for r in results:
        if hasattr(r, "failed") and r.failed():
            err = str(getattr(r, "error", "") or "").lower()
            oom_like = ("oom" in err) or ("out of memory" in err)
            if not oom_like:
                continue

            pid = getattr(r, "pipeline_id", None)
            ops = getattr(r, "ops", None) or []
            if pid is None or not ops:
                continue

            # On OOM, bump RAM hint aggressively; cap will be applied during sizing.
            ran_ram = int(getattr(r, "ram", 1) or 1)
            for op in ops:
                key = (pid, _op_key(op))
                prev = s.op_ram_hint.get(key, ran_ram)
                s.op_ram_hint[key] = max(prev, ran_ram * 2)

    # ---- dispatch ----
    suspensions = []
    assignments = []

    # For each pool, pick the best runnable pipeline (by effective priority), then assign 1 op.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        chosen_p = None
        chosen_ops = None

        # We scan queues in effective priority order, but also avoid putting batch on pool 0
        # if other pools exist and there's any chance to schedule batch elsewhere.
        queues_in_order = [s.q_query, s.q_interactive, s.q_batch]

        for q in queues_in_order:
            n = len(q)
            for _ in range(n):
                p = q.popleft()

                # Drop completed pipelines
                if _cleanup_if_done(p):
                    continue

                eprio = _effective_priority(p)

                # Pool preference rule: keep pool 0 for latency work when possible.
                if s.executor.num_pools > 1 and pool_id == 0 and eprio == Priority.BATCH_PIPELINE:
                    # Put it back and keep scanning this queue.
                    q.append(p)
                    continue

                ops = _ready_ops(p)
                if not ops:
                    q.append(p)
                    continue

                chosen_p = p
                chosen_ops = ops
                # Requeue immediately (round-robin-ish) regardless of assignment success.
                q.append(p)
                break

            if chosen_p is not None:
                break

        if chosen_p is None:
            continue

        cpu, ram = _size_for(pool_id, chosen_p, chosen_ops[0])
        if cpu <= 0 or ram <= 0:
            continue

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=cpu,
                ram=ram,
                priority=chosen_p.priority,  # keep original priority label
                pool_id=pool_id,
                pipeline_id=chosen_p.pipeline_id,
            )
        )

    return suspensions, assignments
