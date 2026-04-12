# policy_key: scheduler_iter_worst_rich_019
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049736
# generation_seconds: 34.69
# generated_at: 2026-04-12T00:59:49.610988
@register_scheduler_init(key="scheduler_iter_worst_rich_019")
def scheduler_iter_worst_rich_019_init(s):
    """
    Iteration 2: Fix obvious flaws causing OOMs + make priority actually help latency.

    Key changes vs previous attempt:
      - Never drop pipelines just because they have FAILED ops; FAILED is retryable in this simulator.
      - Default RAM sizing is "RAM-first": allocate (nearly) all available pool RAM to the assignment
        to avoid systematic under-allocation that causes OOM storms.
      - Priority-aware dispatch with light multi-pool steering:
          * Pool 0 is preferentially used for QUERY/INTERACTIVE when runnable work exists.
          * Other pools prefer BATCH, but will run higher priority if present.
      - Simple per-op RAM hint learning: on OOM, double RAM hint for that op (bounded by pool max).
    """
    from collections import deque

    # Priority queues (pipelines stay queued; we do lazy skipping of completed pipelines)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # For preventing duplicate enqueues
    s.enqueued = set()  # pipeline_id

    # Per-op RAM hints, learned from OOMs
    # Key: (pipeline_id, op_key) -> ram_units
    s.op_ram_hint = {}

    # Tick counter (used only for light aging if needed later)
    s.tick = 0

    # Basic knobs
    s.min_cpu = 1
    s.min_ram = 1

    # Leave a tiny headroom fraction to reduce fragmentation effects
    s.ram_headroom_frac = 0.02  # allocate up to 98% of available RAM


@register_scheduler(key="scheduler_iter_worst_rich_019")
def scheduler_iter_worst_rich_019_scheduler(s, results, pipelines):
    """
    Priority-aware FIFO with RAM-first sizing and OOM backoff.

    Dispatch strategy:
      - For each pool, pick the highest-priority runnable pipeline (QUERY > INTERACTIVE > BATCH),
        with pool 0 biased toward QUERY/INTERACTIVE.
      - Schedule at most one operator per pool per tick (keeps policy simple and stable).
      - Resource sizing:
          * RAM: allocate as much as possible (up to available) and at least per-op hint.
          * CPU: give more CPU to higher priority, but never below 1 and never above available.
    """
    s.tick += 1

    # -----------------------
    # Helpers
    # -----------------------
    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid in s.enqueued:
            return
        s.enqueued.add(pid)
        _queue_for_priority(p.priority).append(p)

    def _op_key(op):
        # Try common stable identifiers; fall back to object identity.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _is_pipeline_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _get_one_runnable_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _learn_from_result(r):
        # Only act on failures; primarily OOM.
        if not (hasattr(r, "failed") and r.failed()):
            return

        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            return

        err = str(getattr(r, "error", "") or "")
        oom_like = ("oom" in err.lower()) or ("out of memory" in err.lower()) or (err.strip().upper() == "OOM")

        ran_ops = getattr(r, "ops", []) or []
        if not ran_ops:
            return

        prev_ram = int(getattr(r, "ram", 0) or 0)
        if prev_ram <= 0:
            prev_ram = s.min_ram

        for op in ran_ops:
            k = (pid, _op_key(op))
            cur = int(s.op_ram_hint.get(k, prev_ram))
            if oom_like:
                # Double on OOM (bounded later by pool max during sizing)
                s.op_ram_hint[k] = max(cur, prev_ram * 2)
            else:
                # Small bump on other failures (conservative)
                s.op_ram_hint[k] = max(cur, prev_ram + 1)

    def _cpu_target(prio, avail_cpu, pool_max_cpu):
        # Slight CPU preference for higher priorities, but keep it simple.
        if avail_cpu <= 0:
            return 0
        if prio == Priority.QUERY:
            want = min(pool_max_cpu, max(2, int(pool_max_cpu * 0.75)))
        elif prio == Priority.INTERACTIVE:
            want = min(pool_max_cpu, max(1, int(pool_max_cpu * 0.50)))
        else:
            want = min(pool_max_cpu, max(1, int(pool_max_cpu * 0.35)))
        return max(s.min_cpu, min(avail_cpu, want))

    def _ram_target(pid, op, avail_ram, pool_max_ram):
        if avail_ram <= 0:
            return 0
        # Allocate nearly all available RAM by default to avoid OOM storms.
        default_req = max(s.min_ram, int(avail_ram * (1.0 - s.ram_headroom_frac)))
        hint = int(s.op_ram_hint.get((pid, _op_key(op)), 0) or 0)
        req = max(default_req, hint, s.min_ram)
        return max(s.min_ram, min(avail_ram, pool_max_ram, req))

    def _pop_runnable_from_queue(q, max_scan):
        """
        Rotate through up to max_scan elements to find a runnable pipeline.
        Keeps queue order roughly FIFO among runnable pipelines.
        """
        chosen = None
        for _ in range(min(len(q), max_scan)):
            p = q.popleft()
            if _is_pipeline_done(p):
                # pipeline is finished; forget it
                s.enqueued.discard(p.pipeline_id)
                continue
            op = _get_one_runnable_op(p)
            if op is None:
                # Not runnable yet; keep it queued
                q.append(p)
                continue
            chosen = (p, op)
            # Put pipeline back so subsequent ops can be scheduled later
            q.append(p)
            break
        return chosen

    # -----------------------
    # Ingest arrivals
    # -----------------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # -----------------------
    # Learn from results
    # -----------------------
    for r in results:
        _learn_from_result(r)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # -----------------------
    # Dispatch per pool
    # -----------------------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pool steering:
        # - pool 0: prefer QUERY/INTERACTIVE
        # - other pools: prefer BATCH, then INTERACTIVE, then QUERY (still allows high prio through)
        if s.executor.num_pools > 1 and pool_id == 0:
            queue_order = (s.q_query, s.q_interactive, s.q_batch)
        elif s.executor.num_pools > 1:
            queue_order = (s.q_batch, s.q_interactive, s.q_query)
        else:
            queue_order = (s.q_query, s.q_interactive, s.q_batch)

        chosen = None
        # Bounded scanning keeps runtime stable even with large queues.
        scan_budget = 32
        for q in queue_order:
            chosen = _pop_runnable_from_queue(q, max_scan=scan_budget)
            if chosen is not None:
                break

        if chosen is None:
            continue

        p, op = chosen

        cpu_req = _cpu_target(p.priority, avail_cpu=avail_cpu, pool_max_cpu=pool.max_cpu_pool)
        ram_req = _ram_target(p.pipeline_id, op, avail_ram=avail_ram, pool_max_ram=pool.max_ram_pool)

        if cpu_req <= 0 or ram_req <= 0:
            continue

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
        )

    return suspensions, assignments
