# policy_key: scheduler_iter_worst_simple_004
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050926
# generation_seconds: 40.09
# generated_at: 2026-04-12T00:37:08.219217
@register_scheduler_init(key="scheduler_iter_worst_simple_004")
def scheduler_iter_worst_simple_004_init(s):
    """
    Iteration 2: strict priority + pool preference + headroom reservation + better retry semantics.

    Key fixes vs prior iteration:
      - Do NOT drop pipelines just because they contain FAILED ops; FAILED is re-assignable in Eudoxia.
      - Strictly prioritize QUERY > INTERACTIVE > BATCH across pools; batch only runs when no runnable
        high-priority ops exist (or only uses leftover capacity beyond reserved headroom).
      - Stronger pool affinity: keep pool 0 primarily for QUERY/INTERACTIVE (when >1 pool).
      - Schedule multiple assignments per pool per tick while capacity remains (improves queueing delay).
      - Simple RAM backoff on failure keyed by (pipeline_id, op_key). CPU bump is smaller than RAM bump.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # pipeline_id -> last seen Pipeline object (pipelines may be re-emitted by generator)
    s.pid_to_pipeline = {}

    # pipeline_id -> tick first enqueued (for mild anti-starvation)
    s.pipeline_enqueued_at = {}

    # learned hints for retries: (pipeline_id, op_key) -> requested resources
    s.op_ram_hint = {}
    s.op_cpu_hint = {}

    s.tick = 0

    # Anti-starvation: after enough waiting, allow a batch pipeline to be considered as INTERACTIVE
    # for dispatch ordering (still keep caps conservative).
    s.batch_promotion_ticks = 400

    # Reservation: keep some headroom in latency pool(s) for QUERY/INTERACTIVE
    # Only applies when there exists at least one runnable high-priority op waiting.
    s.reserve_cpu_frac = 0.25
    s.reserve_ram_frac = 0.25

    # Minimum allocation slices (best-effort)
    s.min_cpu_slice = 1
    s.min_ram_slice = 1

    # Upper caps by effective priority (fraction of pool max)
    s.cap_query_cpu_frac = 1.0
    s.cap_query_ram_frac = 1.0
    s.cap_interactive_cpu_frac = 0.9
    s.cap_interactive_ram_frac = 0.95
    s.cap_batch_cpu_frac = 0.40
    s.cap_batch_ram_frac = 0.55


@register_scheduler(key="scheduler_iter_worst_simple_004")
def scheduler_iter_worst_simple_004_scheduler(s, results, pipelines):
    """
    Priority-first scheduler optimized for weighted latency.

    Core loop:
      - ingest new pipelines into per-priority queues (de-duped by pipeline_id)
      - learn retry hints from failed results (RAM-first growth)
      - for each pool, repeatedly assign ready ops while resources remain:
          * find a runnable pipeline in strict effective priority order
          * size resources with caps + hints
          * for BATCH, enforce reservation headroom when high-priority work is waiting/runnable
    """
    s.tick += 1

    # ---------------- Helpers ----------------
    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        s.pid_to_pipeline[pid] = p
        if pid not in s.pipeline_enqueued_at:
            s.pipeline_enqueued_at[pid] = s.tick
            _queue_for_priority(p.priority).append(pid)

    def _drop_pipeline(pid):
        s.pid_to_pipeline.pop(pid, None)
        s.pipeline_enqueued_at.pop(pid, None)
        # lazy removal from queues: we only store pid; stale pids will be skipped

    def _op_key(op):
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _effective_priority(p):
        if p.priority != Priority.BATCH_PIPELINE:
            return p.priority
        enq = s.pipeline_enqueued_at.get(p.pipeline_id, s.tick)
        if (s.tick - enq) >= s.batch_promotion_ticks:
            return Priority.INTERACTIVE
        return Priority.BATCH_PIPELINE

    def _caps_for_eff_prio(eff_prio):
        if eff_prio == Priority.QUERY:
            return s.cap_query_cpu_frac, s.cap_query_ram_frac
        if eff_prio == Priority.INTERACTIVE:
            return s.cap_interactive_cpu_frac, s.cap_interactive_ram_frac
        return s.cap_batch_cpu_frac, s.cap_batch_ram_frac

    def _is_pipeline_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _get_ready_ops(p):
        st = p.runtime_status()
        # FAILED ops are re-assignable (ASSIGNABLE_STATES typically includes FAILED and PENDING).
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]  # one op per pipeline at a time (keeps placement simple)

    def _pool_order_for_eff_prio(eff_prio):
        # If multiple pools exist: pool 0 is the "latency pool".
        if s.executor.num_pools <= 1:
            return [0]
        if eff_prio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, s.executor.num_pools)]
        return [i for i in range(1, s.executor.num_pools)] + [0]

    def _high_prio_runnable_exists():
        # Quick check: is there any runnable QUERY/INTERACTIVE op?
        for q in (s.q_query, s.q_interactive):
            # bounded scan to avoid O(n^2) worst-case; stop early on first runnable
            n = len(q)
            for _ in range(min(n, 32)):
                pid = q[0]
                q.rotate(-1)
                p = s.pid_to_pipeline.get(pid)
                if not p:
                    continue
                if _is_pipeline_done(p):
                    continue
                if _get_ready_ops(p):
                    return True
        return False

    def _pick_next_runnable_pid(strict_order_queues):
        """
        Returns (pid, ops, eff_prio) by scanning queues in strict priority order.
        Uses rotation to preserve FIFO-ish order within each queue.
        """
        for q in strict_order_queues:
            n = len(q)
            for _ in range(n):
                pid = q[0]
                q.rotate(-1)

                p = s.pid_to_pipeline.get(pid)
                if not p:
                    continue
                if _is_pipeline_done(p):
                    _drop_pipeline(pid)
                    continue

                ops = _get_ready_ops(p)
                if not ops:
                    continue

                return pid, ops, _effective_priority(p)
        return None, None, None

    def _size_request(p, op, pool_id, eff_prio, allow_full_avail=True, cpu_avail_override=None, ram_avail_override=None):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool if cpu_avail_override is None else cpu_avail_override
        avail_ram = pool.avail_ram_pool if ram_avail_override is None else ram_avail_override
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        cpu_cap_frac, ram_cap_frac = _caps_for_eff_prio(eff_prio)
        cpu_cap = max(s.min_cpu_slice, int(pool.max_cpu_pool * cpu_cap_frac))
        ram_cap = max(s.min_ram_slice, int(pool.max_ram_pool * ram_cap_frac))

        # Baselines: give QUERY more CPU to reduce its completion time; batch conservative.
        if eff_prio == Priority.QUERY:
            base_cpu = max(s.min_cpu_slice, min(8, pool.max_cpu_pool))
            base_ram = max(s.min_ram_slice, min(8, pool.max_ram_pool))
        elif eff_prio == Priority.INTERACTIVE:
            base_cpu = max(s.min_cpu_slice, min(4, pool.max_cpu_pool))
            base_ram = max(s.min_ram_slice, min(6, pool.max_ram_pool))
        else:
            base_cpu = max(s.min_cpu_slice, min(2, pool.max_cpu_pool))
            base_ram = max(s.min_ram_slice, min(4, pool.max_ram_pool))

        key = (p.pipeline_id, _op_key(op))
        hinted_cpu = s.op_cpu_hint.get(key, base_cpu)
        hinted_ram = s.op_ram_hint.get(key, base_ram)

        req_cpu = max(base_cpu, hinted_cpu)
        req_ram = max(base_ram, hinted_ram)

        # For QUERY, it's often beneficial to take more CPU when available (scale-up bias),
        # but stay within cap. For others, keep the baseline/hint.
        if eff_prio == Priority.QUERY and allow_full_avail:
            req_cpu = max(req_cpu, min(cpu_cap, avail_cpu))
            req_ram = max(req_ram, min(ram_cap, avail_ram))

        req_cpu = max(s.min_cpu_slice, min(req_cpu, cpu_cap, avail_cpu))
        req_ram = max(s.min_ram_slice, min(req_ram, ram_cap, avail_ram))
        return req_cpu, req_ram

    # ---------------- Ingest pipelines ----------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit
    if not pipelines and not results:
        return [], []

    # ---------------- Learn from results ----------------
    for r in results:
        if hasattr(r, "failed") and r.failed():
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                continue
            ops = getattr(r, "ops", []) or []
            err = str(getattr(r, "error", "") or "").lower()
            oom_like = ("oom" in err) or ("out of memory" in err) or ("memory" in err)

            ran_cpu = int(getattr(r, "cpu", 1) or 1)
            ran_ram = int(getattr(r, "ram", 1) or 1)

            for op in ops:
                key = (pid, _op_key(op))
                prev_cpu = int(s.op_cpu_hint.get(key, ran_cpu) or ran_cpu)
                prev_ram = int(s.op_ram_hint.get(key, ran_ram) or ran_ram)

                if oom_like:
                    # RAM-first doubling; CPU unchanged.
                    s.op_ram_hint[key] = max(prev_ram, max(ran_ram * 2, prev_ram * 2))
                    s.op_cpu_hint[key] = max(prev_cpu, ran_cpu)
                else:
                    # Generic failure: small increase; keep bounded implicitly by pool caps later.
                    s.op_ram_hint[key] = max(prev_ram, int(max(ran_ram, prev_ram) * 1.5) + 1)
                    s.op_cpu_hint[key] = max(prev_cpu, int(max(ran_cpu, prev_cpu) * 1.25) + 1)

    # ---------------- Dispatch ----------------
    suspensions = []
    assignments = []

    high_prio_waiting_runnable = _high_prio_runnable_exists()

    # We fill pools, but keep pool affinity: schedule latency-sensitive work on pool 0 first when possible.
    # We'll compute a pool iteration order that prefers pool 0.
    pool_iter_order = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1:
        pool_iter_order = [0] + [i for i in range(1, s.executor.num_pools)]

    # Strict queue order for selection (effective priority):
    # We select from base queues, but effective priority is applied when sizing/caps/pool affinity matter.
    strict_queues = (s.q_query, s.q_interactive, s.q_batch)

    for pool_id in pool_iter_order:
        pool = s.executor.pools[pool_id]

        # Attempt to schedule multiple ops while there is capacity.
        # Bound the number of assignments per pool per tick to avoid pathological loops.
        for _ in range(16):
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

            pid, ops, eff_prio = _pick_next_runnable_pid(strict_queues)
            if pid is None:
                break

            p = s.pid_to_pipeline.get(pid)
            if not p:
                continue

            # Strong pool affinity: if multiple pools exist, avoid putting BATCH on pool 0
            # unless it's the only pool with resources (or nothing else is runnable).
            if s.executor.num_pools > 1:
                preferred = _pool_order_for_eff_prio(eff_prio)
                if preferred and preferred[0] != pool_id:
                    # If this pool isn't the top preferred for that priority, only allow if
                    # no preferred pool currently has resources (best-effort check).
                    top = preferred[0]
                    top_pool = s.executor.pools[top]
                    if top_pool.avail_cpu_pool > 0 and top_pool.avail_ram_pool > 0:
                        # Defer by rotating pid back to end of its base queue.
                        _queue_for_priority(p.priority).append(pid)
                        continue

            op = ops[0]

            # Reservation logic for BATCH: keep headroom when high-priority work is runnable.
            if eff_prio == Priority.BATCH_PIPELINE and high_prio_waiting_runnable:
                # Only allow batch to use capacity beyond reserved headroom.
                reserve_cpu = int(pool.max_cpu_pool * s.reserve_cpu_frac)
                reserve_ram = int(pool.max_ram_pool * s.reserve_ram_frac)

                cpu_budget = max(0, pool.avail_cpu_pool - reserve_cpu)
                ram_budget = max(0, pool.avail_ram_pool - reserve_ram)

                if cpu_budget < s.min_cpu_slice or ram_budget < s.min_ram_slice:
                    # Can't run batch here without violating headroom; put it back.
                    _queue_for_priority(p.priority).append(pid)
                    break  # stop filling this pool to preserve headroom

                req_cpu, req_ram = _size_request(
                    p, op, pool_id, eff_prio,
                    allow_full_avail=False,
                    cpu_avail_override=cpu_budget,
                    ram_avail_override=ram_budget,
                )
            else:
                req_cpu, req_ram = _size_request(p, op, pool_id, eff_prio, allow_full_avail=True)

            if req_cpu <= 0 or req_ram <= 0:
                # Not enough budget; requeue pid and stop trying in this pool.
                _queue_for_priority(p.priority).append(pid)
                break

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,  # preserve original priority for accounting/metrics
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Requeue pipeline id so next ready op can be discovered later.
            _queue_for_priority(p.priority).append(pid)

    return suspensions, assignments
