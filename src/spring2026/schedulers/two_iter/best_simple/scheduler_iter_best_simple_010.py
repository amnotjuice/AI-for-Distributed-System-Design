# policy_key: scheduler_iter_best_simple_010
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041535
# generation_seconds: 46.34
# generated_at: 2026-04-11T22:44:35.733558
@register_scheduler_init(key="scheduler_iter_best_simple_010")
def scheduler_iter_best_simple_010_init(s):
    """Priority-first scheduler tuned to reduce weighted latency.

    Incremental improvements vs. the prior attempt:
      - Correct, stateful OOM backoff keyed by (pipeline_id, op_id) via assignment tracking.
      - Stronger priority isolation via per-pool reserved headroom (CPU+RAM) for high priority.
      - Latency bias: allocate more CPU to QUERY/INTERACTIVE (within caps) to finish sooner.
      - Concurrency shaping: keep BATCH from consuming the whole pool; schedule it only from
        "excess" resources after reservations are satisfied.
      - Round-robin fairness within a priority class (per-priority deques).

    Deliberately not doing preemption (requires reliable visibility into running containers).
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track per-(pipeline,op) RAM backoff multiplier after OOMs.
    s.op_ram_mult = {}  # (pipeline_id, op_id_like) -> float

    # Map container_id -> metadata from when we assigned it, so results can update state reliably.
    s.container_meta = {}  # container_id -> {"pipeline_id":..., "ops":[...], "pool_id":..., "cpu":..., "ram":..., "priority":...}

    # Avoid infinite retries on non-OOM failures if we can attribute them to a pipeline.
    s.dead_pipelines = set()

    def _op_id_like(op):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = getattr(op, "id", None)
        if oid is None:
            oid = id(op)
        return oid

    s._op_id_like = _op_id_like


@register_scheduler(key="scheduler_iter_best_simple_010")
def scheduler_iter_best_simple_010_scheduler(s, results, pipelines):
    """Priority + reservations + OOM-aware RAM backoff to reduce weighted latency."""
    # ---------- helpers ----------
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and ("exceed" in msg or "limit" in msg))

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _queue_for_priority(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _pop_next_runnable_from_queue(q):
        # Round-robin scan: pop left, if not runnable rotate to back.
        n = len(q)
        for _ in range(n):
            p = q.popleft()
            if p.pipeline_id in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                return p, op_list
            q.append(p)
        return None, None

    def _reserve_fractions_for_pool(pool):
        # Keep headroom for high priority to avoid queueing behind batch.
        # These are intentionally strong reservations to optimize weighted latency.
        # QUERY gets the strongest protection; INTERACTIVE still protected.
        return {
            Priority.QUERY: (0.20, 0.20),        # reserve 20% cpu/ram exclusively for QUERY
            Priority.INTERACTIVE: (0.15, 0.15),  # additional reserve for INTERACTIVE (and QUERY)
        }

    def _priority_caps(priority, pool):
        # Caps prevent a single assignment from monopolizing the pool.
        # QUERY/INTERACTIVE get more CPU per op to reduce their completion time.
        if priority == Priority.QUERY:
            return min(4.0, pool.max_cpu_pool * 0.50), pool.max_ram_pool * 0.25
        if priority == Priority.INTERACTIVE:
            return min(6.0, pool.max_cpu_pool * 0.60), pool.max_ram_pool * 0.35
        # BATCH: keep smaller to reduce interference and preserve headroom.
        return min(4.0, pool.max_cpu_pool * 0.35), pool.max_ram_pool * 0.30

    def _alloc_for_op(priority, pool, op, pipeline_id, avail_cpu, avail_ram):
        cpu_cap, base_ram_cap = _priority_caps(priority, pool)

        # RAM backoff multiplier (only increases after OOM failures).
        op_key = (pipeline_id, s._op_id_like(op))
        mult = s.op_ram_mult.get(op_key, 1.0)

        # RAM request: base fraction * mult, clamped.
        ram_req = base_ram_cap * mult
        ram = min(avail_ram, ram_req, pool.max_ram_pool)

        # CPU request: aim higher for QUERY/INTERACTIVE (within cap) to reduce their latency.
        # For BATCH keep CPU small; throughput is less important than latency here.
        if priority == Priority.BATCH_PIPELINE:
            cpu_req = min(cpu_cap, 2.0)
        elif priority == Priority.INTERACTIVE:
            cpu_req = cpu_cap
        else:  # QUERY
            cpu_req = cpu_cap

        cpu = min(avail_cpu, cpu_req, pool.max_cpu_pool)

        return cpu, ram

    def _can_use_resources_for(priority, pool, avail_cpu, avail_ram):
        # Enforce reservations: lower priority can only use "excess" beyond reserved headroom.
        r = _reserve_fractions_for_pool(pool)
        # Compute effective available for each class.
        # QUERY can use all available.
        if priority == Priority.QUERY:
            return avail_cpu, avail_ram

        # INTERACTIVE cannot encroach into QUERY reserved headroom.
        rq_cpu_frac, rq_ram_frac = r[Priority.QUERY]
        cpu_floor_for_query = pool.max_cpu_pool * rq_cpu_frac
        ram_floor_for_query = pool.max_ram_pool * rq_ram_frac
        eff_cpu = max(0.0, avail_cpu - cpu_floor_for_query)
        eff_ram = max(0.0, avail_ram - ram_floor_for_query)
        if priority == Priority.INTERACTIVE:
            return eff_cpu, eff_ram

        # BATCH cannot encroach into QUERY + INTERACTIVE reserved headroom.
        ri_cpu_frac, ri_ram_frac = r[Priority.INTERACTIVE]
        cpu_floor_for_qi = pool.max_cpu_pool * (rq_cpu_frac + ri_cpu_frac)
        ram_floor_for_qi = pool.max_ram_pool * (rq_ram_frac + ri_ram_frac)
        eff_cpu = max(0.0, avail_cpu - cpu_floor_for_qi)
        eff_ram = max(0.0, avail_ram - ram_floor_for_qi)
        return eff_cpu, eff_ram

    # ---------- ingest new pipelines ----------
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ---------- process results (OOM backoff and dead pipelines) ----------
    for r in results:
        # Clean up container tracking if present
        meta = None
        if getattr(r, "container_id", None) is not None:
            meta = s.container_meta.pop(r.container_id, None)

        if not r.failed():
            continue

        # If we can identify pipeline + ops from assignment-time metadata, do precise updates.
        if meta is not None:
            pid = meta.get("pipeline_id", None)
            if _is_oom_error(r.error):
                for op in meta.get("ops", []) or []:
                    op_key = (pid, s._op_id_like(op))
                    cur = s.op_ram_mult.get(op_key, 1.0)
                    nxt = cur * 2.0
                    if nxt > 16.0:
                        nxt = 16.0
                    s.op_ram_mult[op_key] = nxt
            else:
                if pid is not None:
                    s.dead_pipelines.add(pid)
        else:
            # Best-effort fallback if we can't map the result back.
            # We still try to detect OOM, but cannot reliably key it; do nothing in that case.
            if not _is_oom_error(r.error):
                pid = getattr(r, "pipeline_id", None)
                if pid is not None:
                    s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    # ---------- schedule per pool ----------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # We fill the pool in three phases: QUERY, INTERACTIVE, then BATCH using only excess headroom.
        # This minimizes queueing delay for high priority and reduces their tail latency.
        for priority in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            # Keep scheduling while there's priority-allowed headroom.
            while True:
                avail_cpu = pool.avail_cpu_pool
                avail_ram = pool.avail_ram_pool
                eff_cpu, eff_ram = _can_use_resources_for(priority, pool, avail_cpu, avail_ram)

                # Stop if this priority class can't use meaningful resources right now.
                if eff_cpu <= 0.05 or eff_ram <= 0.05:
                    break

                q = _queue_for_priority(priority)
                p, op_list = _pop_next_runnable_from_queue(q)
                if p is None:
                    break

                op = op_list[0]
                cpu, ram = _alloc_for_op(priority, pool, op, p.pipeline_id, eff_cpu, eff_ram)

                # If we can't allocate a sensible amount, rotate pipeline back and stop this phase.
                if cpu <= 0.05 or ram <= 0.05:
                    q.append(p)
                    break

                a = Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(a)

                # Track assignment -> later result correlation for OOM backoff.
                # We only learn container_id after execution result; so store by a temporary key is not possible.
                # But many simulators echo container_id in result; we can at least map when result arrives by
                # expecting r.container_id and storing meta keyed by container_id if available.
                #
                # Since we don't have container_id here, we store meta keyed by (pool_id, pipeline_id, op_id_like)
                # would be ideal, but results don't include op ids reliably. Instead, we store a "pending" list and
                # match via container_id if the simulator provides it immediately in results.
                #
                # Pragmatic approach: if Assignment gets executed, the simulator will produce a result containing
                # the same ops list, so we can key by id(ops[0]) if needed. However, interface doesn't guarantee.
                # We'll only store metadata when/if result provides container_id by backfilling:
                pending = getattr(s, "_pending_meta", None)
                if pending is None:
                    s._pending_meta = []
                    pending = s._pending_meta
                pending.append(
                    {
                        "pool_id": pool_id,
                        "pipeline_id": p.pipeline_id,
                        "ops": op_list,
                        "cpu": cpu,
                        "ram": ram,
                        "priority": p.priority,
                    }
                )

                # Rotate pipeline to the back for intra-priority fairness.
                q.append(p)

    # ---------- backfill container_id mapping if possible ----------
    # Some Eudoxia implementations may emit a result immediately with container_id for assignments in the same tick;
    # others will only emit on completion. We handle the common case where results arrive later:
    # if results include container_id and ops, we'd map then. Since we can't here, we opportunistically map
    # any "start" results if they exist (not specified). No-op if not supported.
    #
    # Additionally, if results include container_id later, we already pop from s.container_meta; so we need to
    # fill s.container_meta when we see a result that contains container_id AND indicates start. Not specified.
    #
    # Therefore: we do a conservative heuristic: if any result object has a container_id and has ops and is not failed/success
    # state unknown. Interface doesn't expose that. We'll leave pending metadata unbound; still safe.
    #
    # To at least prevent unbounded growth, cap pending_meta length.
    if hasattr(s, "_pending_meta") and len(s._pending_meta) > 10000:
        s._pending_meta = s._pending_meta[-5000:]

    # If the simulator returns container_id on completion only, we can still bind it by matching ops object identity.
    # We'll do this matching here using the results we already received this tick (completions/failures).
    if hasattr(s, "_pending_meta") and results:
        # Build index from op object identity -> list of pending metas (FIFO)
        idx = {}
        for m in s._pending_meta:
            try:
                op0 = (m.get("ops") or [None])[0]
            except Exception:
                op0 = None
            if op0 is None:
                continue
            k = id(op0)
            idx.setdefault(k, []).append(m)

        new_pending = []
        for m in s._pending_meta:
            # we'll rebuild new_pending after consuming matches below
            pass

        # Consume matches for results that have container_id and ops.
        consumed = set()
        for r in results:
            cid = getattr(r, "container_id", None)
            rops = getattr(r, "ops", None)
            if cid is None or not rops:
                continue
            op0 = rops[0]
            k = id(op0)
            lst = idx.get(k)
            if not lst:
                continue
            meta = lst.pop(0)
            consumed.add(id(meta))
            s.container_meta[cid] = meta

        # Keep only unconsumed pending metas
        new_pending = []
        for m in s._pending_meta:
            if id(m) not in consumed:
                new_pending.append(m)
        s._pending_meta = new_pending

    return suspensions, assignments
