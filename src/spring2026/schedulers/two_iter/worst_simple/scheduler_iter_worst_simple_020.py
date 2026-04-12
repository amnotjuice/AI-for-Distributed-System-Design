# policy_key: scheduler_iter_worst_simple_020
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.060950
# generation_seconds: 43.65
# generated_at: 2026-04-12T00:48:24.662385
@register_scheduler_init(key="scheduler_iter_worst_simple_020")
def scheduler_iter_worst_simple_020_init(s):
    """
    Iteration 2: stricter priority + better pool utilization + headroom reservation.

    Changes vs prior attempt:
      - Strict priority queues (QUERY > INTERACTIVE > BATCH), with round-robin within each.
      - Schedule multiple assignments per pool per tick until the pool is full (not just 1).
      - Never drop a pipeline just because it has FAILED ops (FAILED is retryable / assignable).
      - Reserve CPU/RAM headroom when any high-priority work is waiting, so BATCH can't crowd it out.
      - Stronger pool preference: keep pool 0 "latency-first" when multiple pools exist.
      - Keep simple OOM backoff for RAM (and mild CPU bump for non-OOM failures).
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # pipeline_id -> first tick seen (for optional aging)
    s.pipeline_enqueued_at = {}

    # Learned per-op resource hints from failures: (pipeline_id, op_key) -> amount
    s.op_ram_hint = {}
    s.op_cpu_hint = {}

    s.tick = 0

    # Anti-starvation: after this many ticks, treat batch as interactive for dispatch ordering.
    s.batch_promotion_ticks = 300

    # Reservation when high-priority work exists (fractions of pool max).
    s.reserve_cpu_frac_for_hp = 0.30
    s.reserve_ram_frac_for_hp = 0.30

    # Default starting slices (will be bounded by pool availability/caps).
    s.min_cpu_slice = 1
    s.min_ram_slice = 1


@register_scheduler(key="scheduler_iter_worst_simple_020")
def scheduler_iter_worst_simple_020_scheduler(s, results, pipelines):
    """
    Priority-aware, headroom-preserving round-robin scheduler.

    Core idea:
      - Always try to run QUERY then INTERACTIVE before BATCH.
      - When any high-priority work exists, hold back a reserve of resources in each pool
        so batch can't consume everything and inflate weighted latency.
      - Fill each pool with as many assignments as possible each tick (packing),
        using small batch slices and larger slices for high-priority ops.
    """
    s.tick += 1

    # ---- helpers ----
    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _maybe_enqueue(p):
        pid = p.pipeline_id
        if pid not in s.pipeline_enqueued_at:
            s.pipeline_enqueued_at[pid] = s.tick
            _queue_for_priority(p.priority).append(p)

    def _drop_bookkeeping(pid):
        s.pipeline_enqueued_at.pop(pid, None)

    def _op_key(op):
        # Best-effort stable key across retries.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _effective_priority(p):
        # Promote long-waiting batch to interactive to avoid indefinite starvation.
        if p.priority != Priority.BATCH_PIPELINE:
            return p.priority
        enq = s.pipeline_enqueued_at.get(p.pipeline_id, s.tick)
        if (s.tick - enq) >= s.batch_promotion_ticks:
            return Priority.INTERACTIVE
        return Priority.BATCH_PIPELINE

    def _iter_priority_queues():
        # Strict priority ordering by effective priority, but we keep separate deques and
        # handle promotion at selection time.
        return (s.q_query, s.q_interactive, s.q_batch)

    def _pipeline_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _ready_ops_one(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return ops[:1] if ops else []

    def _high_priority_waiting_exists():
        # If either high-priority queue has any runnable work, treat as HP waiting.
        for q in (s.q_query, s.q_interactive):
            if not q:
                continue
            # bounded scan for a runnable op
            for _ in range(min(len(q), 16)):
                p = q[0]
                if _pipeline_done(p):
                    q.popleft()
                    _drop_bookkeeping(p.pipeline_id)
                    continue
                if _ready_ops_one(p):
                    return True
                # rotate to avoid getting stuck on blocked pipeline
                q.rotate(-1)
        return False

    def _pool_order_for(prio):
        # With multiple pools, keep pool 0 biased toward latency-sensitive work.
        if s.executor.num_pools <= 1:
            return list(range(s.executor.num_pools))
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, s.executor.num_pools)]
        return [i for i in range(1, s.executor.num_pools)] + [0]

    def _caps_for(prio, pool):
        # Caps as fraction of pool max for a single assignment.
        if prio == Priority.QUERY:
            return 1.00, 1.00
        if prio == Priority.INTERACTIVE:
            return 0.85, 0.90
        # batch: small to improve responsiveness for others
        return 0.35, 0.50

    def _default_slices(prio, pool):
        # Defaults intentionally give HP more CPU to reduce weighted latency.
        if prio == Priority.QUERY:
            cpu = min(max(4, s.min_cpu_slice), pool.max_cpu_pool)
            ram = min(max(8, s.min_ram_slice), pool.max_ram_pool)
        elif prio == Priority.INTERACTIVE:
            cpu = min(max(2, s.min_cpu_slice), pool.max_cpu_pool)
            ram = min(max(6, s.min_ram_slice), pool.max_ram_pool)
        else:
            cpu = min(max(1, s.min_cpu_slice), pool.max_cpu_pool)
            ram = min(max(4, s.min_ram_slice), pool.max_ram_pool)
        return cpu, ram

    def _size_request(p, op, pool_id, hp_waiting):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        eff_prio = _effective_priority(p)
        cpu_cap_frac, ram_cap_frac = _caps_for(eff_prio, pool)
        cpu_cap = max(s.min_cpu_slice, int(pool.max_cpu_pool * cpu_cap_frac))
        ram_cap = max(s.min_ram_slice, int(pool.max_ram_pool * ram_cap_frac))

        base_cpu, base_ram = _default_slices(eff_prio, pool)

        key = (p.pipeline_id, _op_key(op))
        hinted_cpu = int(s.op_cpu_hint.get(key, base_cpu))
        hinted_ram = int(s.op_ram_hint.get(key, base_ram))

        req_cpu = max(base_cpu, hinted_cpu)
        req_ram = max(base_ram, hinted_ram)

        # If HP is waiting, constrain batch requests further by keeping a reserve unallocated.
        if hp_waiting and eff_prio == Priority.BATCH_PIPELINE:
            reserve_cpu = max(0, int(pool.max_cpu_pool * s.reserve_cpu_frac_for_hp))
            reserve_ram = max(0, int(pool.max_ram_pool * s.reserve_ram_frac_for_hp))
            avail_cpu = max(0, avail_cpu - reserve_cpu)
            avail_ram = max(0, avail_ram - reserve_ram)
            if avail_cpu <= 0 or avail_ram <= 0:
                return 0, 0

        # Bound by caps and availability.
        req_cpu = max(s.min_cpu_slice, min(req_cpu, cpu_cap, avail_cpu))
        req_ram = max(s.min_ram_slice, min(req_ram, ram_cap, avail_ram))
        return req_cpu, req_ram

    def _learn_from_results():
        # Increase hints for failed ops (RAM-first for OOM-like failures).
        for r in results:
            if not (hasattr(r, "failed") and r.failed()):
                continue

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
                prev_cpu = int(s.op_cpu_hint.get(key, ran_cpu))
                prev_ram = int(s.op_ram_hint.get(key, ran_ram))

                if oom_like:
                    # Aggressive RAM backoff.
                    s.op_ram_hint[key] = max(prev_ram, ran_ram * 2)
                    s.op_cpu_hint[key] = max(prev_cpu, ran_cpu)
                else:
                    # Mild bumps in both.
                    s.op_ram_hint[key] = max(prev_ram, int(ran_ram * 1.25) + 1)
                    s.op_cpu_hint[key] = max(prev_cpu, int(ran_cpu * 1.25) + 1)

    def _pick_next_pipeline_for_pool(pool_id, allow_batch_on_pool0):
        """
        Pick next runnable (pipeline, ops, effective_priority).
        Uses round-robin within each queue by rotating items.
        """
        for q in _iter_priority_queues():
            # bounded scan: traverse entire queue once
            for _ in range(len(q)):
                p = q.popleft()

                # Drop completed pipelines.
                if _pipeline_done(p):
                    _drop_bookkeeping(p.pipeline_id)
                    continue

                eff_prio = _effective_priority(p)

                # Pool 0 is latency-first if multiple pools exist.
                if (s.executor.num_pools > 1) and (pool_id == 0) and (not allow_batch_on_pool0):
                    if eff_prio == Priority.BATCH_PIPELINE:
                        q.append(p)
                        continue

                ops = _ready_ops_one(p)
                if not ops:
                    q.append(p)
                    continue

                # Put back at end for RR fairness; caller will schedule now.
                q.append(p)
                return p, ops, eff_prio
        return None, None, None

    # ---- ingest ----
    for p in pipelines:
        _maybe_enqueue(p)

    # early exit
    if not pipelines and not results:
        return [], []

    # learn
    if results:
        _learn_from_results()

    suspensions = []
    assignments = []

    # Detect if any high-priority runnable work exists (to trigger reservation).
    hp_waiting = _high_priority_waiting_exists()

    # Decide whether batch is allowed on pool 0 this tick:
    # If HP is waiting and there is more than one pool, do not place batch on pool 0.
    allow_batch_on_pool0 = not (hp_waiting and s.executor.num_pools > 1)

    # Fill each pool with as many assignments as possible.
    # We always iterate pools in natural order; pool preference is handled in selection gate above.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Keep assigning while we can still make progress.
        # Hard stop to prevent pathological infinite loops in case of inconsistent avail updates.
        safety = 64
        while safety > 0:
            safety -= 1

            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

            p, ops, eff_prio = _pick_next_pipeline_for_pool(pool_id, allow_batch_on_pool0)
            if p is None:
                break

            req_cpu, req_ram = _size_request(p, ops[0], pool_id, hp_waiting)
            if req_cpu <= 0 or req_ram <= 0:
                # Can't place this class here now; stop filling this pool this tick to avoid churn.
                # (Other pools may still accept work.)
                break

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,  # preserve original priority label
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # After scheduling a HP op, keep hp_waiting true; after scheduling batch,
            # hp_waiting can still be true if more HP exists.
            # We recompute cheaply only when we just filled some capacity and HP might have cleared.
            if eff_prio in (Priority.QUERY, Priority.INTERACTIVE) and hp_waiting:
                # bounded recompute to avoid over-reserving after draining HP queue
                hp_waiting = _high_priority_waiting_exists()
                allow_batch_on_pool0 = not (hp_waiting and s.executor.num_pools > 1)

    return suspensions, assignments
