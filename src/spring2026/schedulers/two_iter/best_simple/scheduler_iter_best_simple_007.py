# policy_key: scheduler_iter_best_simple_007
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046225
# generation_seconds: 39.47
# generated_at: 2026-04-11T22:42:42.908196
@register_scheduler_init(key="scheduler_iter_best_simple_007")
def scheduler_iter_best_simple_007_init(s):
    """Priority-first, low-latency scheduler with (1) pool affinity and (2) reserves.

    Incremental improvements aimed at reducing weighted latency vs. the previous attempt:
      - Stronger priority separation: QUERY always scheduled first; INTERACTIVE next; BATCH last.
      - Pool affinity: if multiple pools, prefer keeping pool 0 for QUERY/INTERACTIVE to reduce interference.
      - Resource reserves: prevent BATCH from consuming the last CPU/RAM in a pool so that new high-priority
        arrivals can start immediately (reduces queueing/tail).
      - More aggressive CPU sizing for high priority (finish faster), but still capped to avoid full monopolization.
      - OOM-aware RAM backoff (per-operator) with a small initial RAM footprint to increase packing density.

    Non-goals (kept simple on purpose):
      - No preemption (insufficient visibility into currently running containers in the provided interface).
      - No complex runtime prediction; only priority + simple sizing + OOM backoff.
    """
    # Separate per-priority queues (pipelines), FIFO within each, with light rotation.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track pipelines to ignore if we see persistent non-OOM failures (best-effort; pipeline status also drops them).
    s.dead_pipelines = set()

    # Per-operator RAM multiplier for OOM retries (keyed by operator object identity).
    s.op_ram_mult = {}

    # Track last known "safe" RAM multiplier on success to avoid unnecessary bloat after transient spikes.
    s.op_ram_mult_success = {}

    # Simple counter to occasionally "age" batch by allowing one batch pick even when interactive exists.
    s.tick = 0

    def _enqueue(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    s._enqueue = _enqueue

    def _is_oom(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    s._is_oom = _is_oom

    def _op_key(op):
        # Stable within a simulation run.
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return oid

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_simple_007")
def scheduler_iter_best_simple_007_scheduler(s, results, pipelines):
    """Priority-first scheduler with pool affinity + reserves + OOM-aware RAM sizing."""
    # --- ingest new pipelines ---
    for p in pipelines:
        s._enqueue(p)

    if not pipelines and not results:
        return [], []

    s.tick += 1

    # --- process results (OOM backoff + drop hard failures best-effort) ---
    for r in results:
        if not r.failed():
            # Success: record that current multiplier worked for these ops (so we can avoid unbounded growth).
            for op in (r.ops or []):
                k = s._op_key(op)
                # If we had a multiplier recorded, treat it as "safe".
                if k in s.op_ram_mult:
                    s.op_ram_mult_success[k] = s.op_ram_mult[k]
            continue

        if s._is_oom(r.error):
            for op in (r.ops or []):
                k = s._op_key(op)
                cur = s.op_ram_mult.get(k, 1.0)
                # Backoff quickly; cap to avoid requesting the whole pool routinely.
                nxt = cur * 2.0
                if nxt > 16.0:
                    nxt = 16.0
                s.op_ram_mult[k] = nxt
        else:
            # Best-effort: if ExecutionResult happens to carry pipeline_id in this environment, drop it.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.dead_pipelines.add(pid)

    # --- helpers ---
    def _cleanup_and_pick_op_from_queue(q):
        """Round-robin scan: returns (pipeline, [op]) or (None, None)."""
        if not q:
            return None, None
        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            if p.pipeline_id in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                return p, op_list
            # Not runnable yet; rotate.
            q.append(p)
        return None, None

    def _pick_next_for_pool(pool_id):
        """Pool-affinity selection to reduce interference with high priority."""
        # If multiple pools, keep pool 0 biased to QUERY/INTERACTIVE.
        multi = s.executor.num_pools > 1

        # Light batch aging: every ~8 ticks allow batch to be considered earlier (still after QUERY).
        allow_batch_early = (s.tick % 8 == 0)

        if not multi:
            # Single pool: strict priority.
            p, ops = _cleanup_and_pick_op_from_queue(s.q_query)
            if p:
                return p, ops
            p, ops = _cleanup_and_pick_op_from_queue(s.q_interactive)
            if p:
                return p, ops
            return _cleanup_and_pick_op_from_queue(s.q_batch)

        # Multi-pool:
        if pool_id == 0:
            # "Latency pool"
            p, ops = _cleanup_and_pick_op_from_queue(s.q_query)
            if p:
                return p, ops
            p, ops = _cleanup_and_pick_op_from_queue(s.q_interactive)
            if p:
                return p, ops
            # Only run batch on pool 0 if nothing else is waiting.
            return _cleanup_and_pick_op_from_queue(s.q_batch)
        else:
            # "Throughput pools"
            # If QUERY exists, still allow it to spill over to reduce query queueing.
            p, ops = _cleanup_and_pick_op_from_queue(s.q_query)
            if p:
                return p, ops
            # Otherwise serve interactive, but occasionally let batch in to avoid starvation.
            p, ops = _cleanup_and_pick_op_from_queue(s.q_interactive)
            if p:
                # If batch is piling up, allow batch early sometimes.
                if allow_batch_early and s.q_batch:
                    # Put interactive back and take one batch instead (aging).
                    s._enqueue(p)
                    return _cleanup_and_pick_op_from_queue(s.q_batch)
                return p, ops
            return _cleanup_and_pick_op_from_queue(s.q_batch)

    def _reserve_for_pool(pool, priority):
        """Reserves protect headroom for new high-priority arrivals (reduces queueing latency)."""
        # Reserves apply mainly against BATCH; high-priority can consume more.
        if priority == Priority.BATCH_PIPELINE:
            # Keep a meaningful slice free so queries can start immediately.
            cpu_res = max(0.5, pool.max_cpu_pool * 0.20)
            ram_res = max(0.5, pool.max_ram_pool * 0.20)
        elif priority == Priority.INTERACTIVE:
            cpu_res = max(0.25, pool.max_cpu_pool * 0.10)
            ram_res = max(0.25, pool.max_ram_pool * 0.10)
        else:  # QUERY
            cpu_res = 0.0
            ram_res = 0.0
        return cpu_res, ram_res

    def _size(priority, pool, op, avail_cpu, avail_ram):
        """CPU aggressive for high priority; RAM small-but-safe with OOM backoff."""
        # CPU caps: higher for QUERY to reduce latency; interactive moderate; batch conservative.
        if priority == Priority.QUERY:
            cpu_cap = min(max(1.0, pool.max_cpu_pool * 0.70), 12.0)
            ram_frac = 0.08  # start small to pack more; rely on OOM backoff
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(max(1.0, pool.max_cpu_pool * 0.50), 8.0)
            ram_frac = 0.12
        else:  # BATCH
            cpu_cap = min(max(0.5, pool.max_cpu_pool * 0.35), 6.0)
            ram_frac = 0.18

        cpu_res, ram_res = _reserve_for_pool(pool, priority)
        cpu_budget = max(0.0, avail_cpu - cpu_res)
        ram_budget = max(0.0, avail_ram - ram_res)

        cpu = min(cpu_budget, cpu_cap)
        if cpu <= 0.01:
            return 0.0, 0.0

        # RAM: base fraction of pool, multiplied by backoff if this op OOMed before.
        k = s._op_key(op)
        mult = s.op_ram_mult.get(k, 1.0)

        # If we have a known successful multiplier, don't grow unless we had an OOM after that.
        # This prevents permanent bloat after one rare spike.
        if k in s.op_ram_mult_success:
            mult = max(mult, s.op_ram_mult_success[k])

        base_ram = pool.max_ram_pool * ram_frac
        ram = base_ram * mult
        ram = min(ram, ram_budget, pool.max_ram_pool)
        if ram <= 0.01:
            return 0.0, 0.0

        return cpu, ram

    # --- schedule ---
    suspensions = []
    assignments = []

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Fill the pool; prioritize multiple small(ish) assignments to reduce head-of-line blocking.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            p, op_list = _pick_next_for_pool(pool_id)
            if not p:
                break

            op = op_list[0]
            cpu, ram = _size(p.priority, pool, op, avail_cpu, avail_ram)

            if cpu <= 0.01 or ram <= 0.01:
                # Can't place this op here without violating reserve; put pipeline back and stop for this pool.
                s._enqueue(p)
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

            # Update local headroom and rotate pipeline for fairness within the same priority.
            avail_cpu -= cpu
            avail_ram -= ram
            s._enqueue(p)

    return suspensions, assignments
