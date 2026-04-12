# policy_key: scheduler_iter_best_rich_004
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051213
# generation_seconds: 38.86
# generated_at: 2026-04-12T00:22:59.349213
@register_scheduler_init(key="scheduler_iter_best_rich_004")
def scheduler_iter_best_rich_004_init(s):
    """Priority-aware scheduler (iteration 2): reduce weighted latency by cutting RAM over-allocation,
    prioritizing QUERY/INTERACTIVE placement, and adapting resources from observed failures.

    Improvements vs prior iteration:
      - Much smaller initial RAM requests (memory is the dominant bottleneck; prior policy reserved ~93%).
      - Per-operator adaptive RAM backoff on OOM (learned hints keyed by (pipeline_id, op_id)).
      - Per-operator adaptive CPU bump on timeout (lightweight; helps reduce re-timeouts).
      - Soft pool partitioning when multiple pools exist:
          * pool 0: QUERY + INTERACTIVE preferred (protects tail latency)
          * other pools: BATCH preferred, but can steal INTERACTIVE if it is waiting
      - Reservation logic: when high-priority work is waiting, throttle batch admissions in shared pools.
    """
    # Separate queues per priority to minimize head-of-line blocking.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator learned hints (start small; adjust based on failures/success).
    # Keys are (pipeline_id, op_identifier). Values are floats in pool-units (cpu cores, ram units).
    s.op_ram_hint = {}
    s.op_cpu_hint = {}

    # Track pipelines we should stop considering (optional, conservative).
    s.dead_pipelines = set()

    def _op_identifier(op):
        # Try to use stable IDs if present; else fall back to object id.
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return oid

    def _op_key(pipeline_id, op):
        return (pipeline_id, _op_identifier(op))

    s._op_identifier = _op_identifier
    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_rich_004")
def scheduler_iter_best_rich_004_scheduler(s, results, pipelines):
    """Latency-focused priority scheduler with adaptive sizing and light pool partitioning."""
    # ---------------- helpers ----------------
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _is_timeout_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("time out" in msg)

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _has_waiting_high_pri() -> bool:
        return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    def _queue_order_for_pool(pool_id: int, num_pools: int):
        # If we have multiple pools, prefer to keep pool 0 "snappy" for QUERY/INTERACTIVE.
        if num_pools >= 2 and pool_id == 0:
            return [s.q_query, s.q_interactive, s.q_batch]
        # Other pools primarily do batch, but can take interactive to avoid timeouts/backlog.
        return [s.q_interactive, s.q_query, s.q_batch]

    def _pop_next_runnable_pipeline(pool_id: int, num_pools: int):
        # Round-robin inside each queue; bounded scan prevents spinning.
        for q in _queue_order_for_pool(pool_id, num_pools):
            if not q:
                continue
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

    def _base_ram_frac(priority):
        # Key change: start MUCH smaller than before; let OOM backoff find the right size.
        if priority == Priority.QUERY:
            return 0.04
        if priority == Priority.INTERACTIVE:
            return 0.06
        return 0.08  # batch

    def _base_cpu_cap(priority, pool):
        # Give more CPU to higher priority; keep batch modest to avoid starving many short queries.
        if priority == Priority.QUERY:
            return min(8.0, pool.max_cpu_pool * 0.60)
        if priority == Priority.INTERACTIVE:
            return min(8.0, pool.max_cpu_pool * 0.55)
        return min(4.0, pool.max_cpu_pool * 0.35)

    def _reservation_ok(priority, pool, avail_cpu, avail_ram, high_waiting: bool, pool_id: int, num_pools: int):
        # Throttle batch when high-priority work is waiting, especially in pool 0.
        if priority != Priority.BATCH_PIPELINE:
            return True

        if not high_waiting:
            return True

        # Keep a safety buffer for high-priority arrivals.
        # Stricter on pool 0 to protect tail latency.
        if num_pools >= 2 and pool_id == 0:
            cpu_reserve = pool.max_cpu_pool * 0.35
            ram_reserve = pool.max_ram_pool * 0.35
        else:
            cpu_reserve = pool.max_cpu_pool * 0.20
            ram_reserve = pool.max_ram_pool * 0.20

        return (avail_cpu - 0.01) > cpu_reserve and (avail_ram - 0.01) > ram_reserve

    def _size_for(pool, priority, op_key, avail_cpu, avail_ram):
        # RAM: start small base fraction, then use per-op learned hint (from OOMs) if bigger.
        base_ram = pool.max_ram_pool * _base_ram_frac(priority)
        hint_ram = s.op_ram_hint.get(op_key, 0.0)
        ram = max(base_ram, hint_ram)

        # CPU: cap by priority, but also honor per-op CPU hint (from timeouts) up to cap.
        cap_cpu = _base_cpu_cap(priority, pool)
        hint_cpu = s.op_cpu_hint.get(op_key, 0.0)
        cpu = max(1.0, hint_cpu)  # ensure some CPU
        cpu = min(cpu, cap_cpu)

        # Clamp to available.
        cpu = min(cpu, avail_cpu)
        ram = min(ram, avail_ram)

        return cpu, ram

    # ---------------- ingest pipelines ----------------
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ---------------- process results to learn hints ----------------
    for r in results:
        if not r.failed():
            # Successful completion: we can gently reduce CPU hint (avoid overprovision) and
            # keep RAM hint as-is (we don't know the true minimum).
            for op in (r.ops or []):
                # pipeline_id is not guaranteed on result; best-effort only.
                pid = getattr(r, "pipeline_id", None)
                if pid is None:
                    continue
                k = s._op_key(pid, op)
                if k in s.op_cpu_hint:
                    s.op_cpu_hint[k] = max(1.0, s.op_cpu_hint[k] * 0.9)
            continue

        # Failure handling: distinguish OOM vs timeout and adapt.
        is_oom = _is_oom_error(r.error)
        is_to = _is_timeout_error(r.error)

        # Best-effort operator keying; if pipeline_id missing, we cannot learn stably.
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            continue

        for op in (r.ops or []):
            k = s._op_key(pid, op)

            if is_oom:
                # Increase RAM hint aggressively; keep within pool bounds indirectly via clamping at assignment time.
                # Use the RAM we just tried as a floor, then grow.
                prev = s.op_ram_hint.get(k, 0.0)
                tried = float(getattr(r, "ram", 0.0) or 0.0)
                floor = max(prev, tried)
                nxt = floor * 1.8 if floor > 0 else 0.0
                # Ensure monotonic increase even if tried was tiny.
                if nxt <= prev:
                    nxt = prev + max(0.01, prev * 0.5)
                s.op_ram_hint[k] = nxt

            elif is_to:
                # Timeout: bump CPU hint (helps reduce repeat timeouts / long tails).
                prev = s.op_cpu_hint.get(k, 1.0)
                tried = float(getattr(r, "cpu", 0.0) or 0.0)
                floor = max(prev, tried, 1.0)
                nxt = min(floor * 1.25, floor + 2.0)
                s.op_cpu_hint[k] = nxt
            else:
                # Unknown failure: do not churn forever; mark pipeline dead (conservative).
                s.dead_pipelines.add(pid)

    # ---------------- scheduling ----------------
    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools
    high_waiting = _has_waiting_high_pri()

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Fill the pool with multiple small-to-moderate containers to reduce queueing latency.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            p, op_list = _pop_next_runnable_pipeline(pool_id, num_pools)
            if not p:
                break

            # Soft admission control for batch while high-priority is queued.
            if not _reservation_ok(p.priority, pool, avail_cpu, avail_ram, high_waiting, pool_id, num_pools):
                # Put pipeline back and stop scheduling more batch in this pool right now.
                _enqueue_pipeline(p)
                break

            op = op_list[0]
            op_key = s._op_key(p.pipeline_id, op)
            cpu, ram = _size_for(pool, p.priority, op_key, avail_cpu, avail_ram)

            # If we can't allocate a meaningful container, stop for this pool.
            if cpu <= 0.01 or ram <= 0.01:
                _enqueue_pipeline(p)
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

            # Update local headroom and rotate pipeline (fairness within same priority).
            avail_cpu -= cpu
            avail_ram -= ram
            _enqueue_pipeline(p)

    return suspensions, assignments
