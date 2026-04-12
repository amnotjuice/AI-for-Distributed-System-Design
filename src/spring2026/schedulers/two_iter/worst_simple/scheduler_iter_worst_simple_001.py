# policy_key: scheduler_iter_worst_simple_001
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.060924
# generation_seconds: 47.27
# generated_at: 2026-04-12T00:35:03.044450
@register_scheduler_init(key="scheduler_iter_worst_simple_001")
def scheduler_iter_worst_simple_001_init(s):
    """
    Priority-first scheduler (iteration 2) focused on reducing weighted latency.

    Key changes vs naive FIFO / previous attempt:
      - Never drop pipelines just because some ops are FAILED (FAILED is retryable); only drop on success.
      - Strict priority ordering at dispatch time: QUERY > INTERACTIVE > BATCH.
      - Multi-pool aware: treat pool 0 as latency pool by default; keep BATCH off pool 0 unless nothing else runnable.
      - More aggressive resource allocation for high priority: give QUERY/INTERACTIVE most/all available CPU+RAM
        to finish sooner (weighted latency win), while limiting BATCH to preserve headroom.
      - Schedule multiple assignments per pool per tick (locally accounting for resources), instead of at most one.
      - Simple OOM backoff: on OOM-like failure, double the RAM hint for that operator and retry later.
    """
    from collections import deque

    # Priority queues (pipelines), maintained FIFO within each class.
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track which pipelines are currently enqueued (avoid duplicates).
    s.enqueued = set()  # pipeline_id

    # Track when pipeline first enqueued (for mild aging).
    s.enqueued_at = {}  # pipeline_id -> tick
    s.tick = 0

    # Per-operator resource hints learned from failures (esp. OOM).
    # Keyed by (pipeline_id, op_key)
    s.op_ram_hint = {}  # -> int (GiB-like units used by simulator)
    s.op_cpu_hint = {}  # -> int

    # Aging knobs (small, safe improvement): allow old BATCH to occasionally run.
    s.batch_aging_ticks = 300

    # Minimal allocation (avoid zero / pathological tiny requests).
    s.min_cpu = 1
    s.min_ram = 1


@register_scheduler(key="scheduler_iter_worst_simple_001")
def scheduler_iter_worst_simple_001_scheduler(s, results, pipelines):
    """
    Step function:
      1) Enqueue new pipelines into per-priority FIFO queues.
      2) Learn from failures: on OOM-like errors, increase RAM hint for the failed ops.
      3) For each pool, repeatedly assign runnable ops while resources remain:
           - Prefer QUERY/INTERACTIVE on pool 0 (latency pool).
           - Keep BATCH off pool 0 unless no higher-priority runnable work exists.
           - Size resources aggressively for QUERY/INTERACTIVE, conservatively for BATCH.
    """
    from collections import deque

    s.tick += 1

    # ------------------------
    # Helpers
    # ------------------------
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
        s.enqueued_at[pid] = s.tick
        _queue_for_priority(p.priority).append(p)

    def _drop_pipeline(p):
        pid = p.pipeline_id
        if pid in s.enqueued:
            s.enqueued.remove(pid)
        if pid in s.enqueued_at:
            del s.enqueued_at[pid]
        # Lazy removal from queues: we don't try to delete from deques here.

    def _op_key(op):
        # Best-effort stable identity.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _oom_like(err_str):
        e = (err_str or "").lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e)

    def _effective_priority(p):
        # Mild anti-starvation: very old BATCH gets treated as INTERACTIVE for selection only.
        if p.priority != Priority.BATCH_PIPELINE:
            return p.priority
        waited = s.tick - s.enqueued_at.get(p.pipeline_id, s.tick)
        if waited >= s.batch_aging_ticks:
            return Priority.INTERACTIVE
        return p.priority

    def _get_runnable_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return None
        # FAILED is retryable (ASSIGNABLE_STATES includes FAILED)
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _pool_order_for_priority(num_pools, eff_prio):
        # Prefer pool 0 for latency-sensitive work when multiple pools exist.
        if num_pools <= 1:
            return [0]
        if eff_prio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, num_pools)]
        # Batch: avoid pool 0 if possible
        return [i for i in range(1, num_pools)] + [0]

    def _size_request(p, op, pool_id, avail_cpu, avail_ram):
        """
        Return (req_cpu, req_ram) bounded by availability.
        Strategy:
          - QUERY: try to take (almost) all available to finish fast.
          - INTERACTIVE: take most, but not necessarily all.
          - BATCH: capped to leave headroom for later arrivals.
        """
        pool = s.executor.pools[pool_id]
        eff_prio = _effective_priority(p)

        key = (p.pipeline_id, _op_key(op))
        ram_hint = int(s.op_ram_hint.get(key, s.min_ram))
        cpu_hint = int(s.op_cpu_hint.get(key, s.min_cpu))

        # If we can't satisfy RAM hint, can't run here.
        if avail_ram < max(s.min_ram, ram_hint):
            return 0, 0

        # Priority-based caps (fractions of pool max), then bound by current availability.
        if eff_prio == Priority.QUERY:
            cpu_cap = int(pool.max_cpu_pool * 1.00)
            ram_cap = int(pool.max_ram_pool * 1.00)
            cpu_goal = avail_cpu  # greedy: reduce weighted latency
            ram_goal = avail_ram
        elif eff_prio == Priority.INTERACTIVE:
            cpu_cap = int(pool.max_cpu_pool * 0.90)
            ram_cap = int(pool.max_ram_pool * 0.95)
            cpu_goal = max(s.min_cpu, int(avail_cpu * 0.90))
            ram_goal = max(s.min_ram, int(avail_ram * 0.95))
        else:
            cpu_cap = int(pool.max_cpu_pool * 0.50)
            ram_cap = int(pool.max_ram_pool * 0.60)
            cpu_goal = max(s.min_cpu, int(avail_cpu * 0.50))
            ram_goal = max(s.min_ram, int(avail_ram * 0.60))

        # Respect hints (especially RAM), but don't exceed caps/availability.
        req_ram = max(s.min_ram, ram_hint)
        req_ram = min(req_ram, ram_cap if ram_cap > 0 else req_ram, avail_ram)

        # CPU: start from goal, but at least hint, then cap/bound.
        req_cpu = max(s.min_cpu, cpu_hint, cpu_goal)
        req_cpu = min(req_cpu, cpu_cap if cpu_cap > 0 else req_cpu, avail_cpu)

        # If after bounding CPU becomes 0, don't schedule.
        if req_cpu <= 0 or req_ram <= 0:
            return 0, 0
        return int(req_cpu), int(req_ram)

    def _any_high_priority_runnable():
        # Quick check used to keep batch off pool 0 when there is latency-sensitive runnable work.
        for q in (s.q_query, s.q_interactive):
            for _ in range(len(q)):
                p = q.popleft()
                op = _get_runnable_op(p)
                q.append(p)
                if op is not None:
                    return True
        return False

    # ------------------------
    # Ingest new pipelines
    # ------------------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # ------------------------
    # Learn from results (OOM backoff)
    # ------------------------
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            continue

        ops = getattr(r, "ops", None) or []
        err = str(getattr(r, "error", "") or "")
        oom = _oom_like(err)

        # Use reported resources as baseline; if missing, fall back to min.
        used_ram = int(getattr(r, "ram", s.min_ram) or s.min_ram)
        used_cpu = int(getattr(r, "cpu", s.min_cpu) or s.min_cpu)

        for op in ops:
            k = (pid, _op_key(op))
            prev_ram = int(s.op_ram_hint.get(k, max(s.min_ram, used_ram)))
            prev_cpu = int(s.op_cpu_hint.get(k, max(s.min_cpu, used_cpu)))

            if oom:
                # Strong RAM bump on OOM; CPU unchanged.
                s.op_ram_hint[k] = max(prev_ram, used_ram, prev_ram * 2)
                s.op_cpu_hint[k] = prev_cpu
            else:
                # Gentle bump for non-OOM failures (may still be underprovisioned).
                s.op_ram_hint[k] = max(prev_ram, used_ram, prev_ram + 1)
                s.op_cpu_hint[k] = max(prev_cpu, used_cpu, prev_cpu + 1)

    # ------------------------
    # Dispatch
    # ------------------------
    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools
    high_prio_exists = _any_high_priority_runnable()

    # We schedule multiple assignments per pool per tick, accounting locally for resource consumption.
    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = int(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If this is the latency pool (0) and higher priority runnable work exists anywhere,
        # avoid placing batch here in this tick.
        avoid_batch_here = (num_pools > 1 and pool_id == 0 and high_prio_exists)

        # Keep trying to fill the pool while resources remain.
        # Bounded attempts to avoid infinite loops on unrunnable items.
        attempts = 0
        max_attempts = 4 * (len(s.q_query) + len(s.q_interactive) + len(s.q_batch) + 1)

        while avail_cpu > 0 and avail_ram > 0 and attempts < max_attempts:
            attempts += 1

            chosen_p = None
            chosen_op = None
            chosen_eff_prio = None
            chosen_queue = None

            # Strict priority selection: QUERY then INTERACTIVE then BATCH.
            # We do a single pass scan per queue (rotate) to find the first runnable pipeline.
            for q in (s.q_query, s.q_interactive, s.q_batch):
                for _ in range(len(q)):
                    p = q.popleft()

                    # Drop successful pipelines (only terminal condition we enforce here).
                    if p.runtime_status().is_pipeline_successful():
                        _drop_pipeline(p)
                        continue

                    eff_prio = _effective_priority(p)

                    # If batch should be avoided on this pool, keep it queued.
                    if avoid_batch_here and eff_prio == Priority.BATCH_PIPELINE:
                        q.append(p)
                        continue

                    # If multiple pools exist, respect pool preference lightly by skipping
                    # obvious mismatches when alternatives exist.
                    if num_pools > 1:
                        pref = _pool_order_for_priority(num_pools, eff_prio)
                        if pool_id != pref[0] and eff_prio == Priority.BATCH_PIPELINE and pool_id == 0:
                            q.append(p)
                            continue

                    op = _get_runnable_op(p)
                    if op is None:
                        q.append(p)
                        continue

                    chosen_p, chosen_op, chosen_eff_prio, chosen_queue = p, op, eff_prio, q
                    # Put pipeline back immediately (we keep it circulating FIFO-style).
                    q.append(p)
                    break

                if chosen_p is not None:
                    break

            if chosen_p is None:
                break

            # Size request and ensure it fits.
            req_cpu, req_ram = _size_request(chosen_p, chosen_op, pool_id, avail_cpu, avail_ram)
            if req_cpu <= 0 or req_ram <= 0:
                # Can't fit right now; stop trying to pack this pool this tick.
                break

            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=chosen_p.priority,  # preserve original priority for accounting
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            # Local accounting so we can place additional work in the same tick.
            avail_cpu -= req_cpu
            avail_ram -= req_ram

            # If we just scheduled QUERY on latency pool, don't add extra interference in the same tick.
            if chosen_eff_prio == Priority.QUERY and (num_pools > 1 and pool_id == 0):
                break

    return suspensions, assignments
