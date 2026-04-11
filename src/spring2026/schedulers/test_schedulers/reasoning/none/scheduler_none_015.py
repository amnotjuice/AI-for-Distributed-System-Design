# policy_key: scheduler_none_015
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.053561
# generation_seconds: 46.43
# generated_at: 2026-04-09T21:56:36.532721
@register_scheduler_init(key="scheduler_none_015")
def scheduler_none_015_init(s):
    """Priority-aware, OOM-safe, low-churn scheduler.

    Goals aligned to weighted-latency objective:
      - Strongly protect QUERY/INTERACTIVE latency via reserved headroom + (limited) preemption.
      - Reduce 720s penalties by aggressively avoiding repeat OOMs with per-op RAM backoff.
      - Preserve throughput without starving BATCH via simple aging (priority boost over wait time).
      - Keep churn low: preempt only when high-priority is blocked and we can likely place it.

    Policy outline:
      1) Maintain per-priority FIFO queues; schedule ready ops (parents complete).
      2) Maintain RAM estimates per operator (keyed by op identity where possible). On OOM -> multiply.
      3) Per pool, reserve a fraction of CPU/RAM for high-priority work.
      4) If high-priority cannot be placed due to running low-priority, preempt at most one container/tick.
      5) Allocate "right-sized" CPU for each op: a cap per priority; don't greedily take all CPU.
    """
    # Queues per priority (arrival order); we will do a light aging boost at selection time.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Pipeline arrival timestamps for aging (simulation time if available, else tick counter).
    s.pipeline_arrival_time = {}

    # Per-op RAM estimate (in same units as pool RAM); multiplicative backoff on OOM.
    s.op_ram_est = {}

    # Track last known successful allocation for an op to avoid thrashing.
    s.op_last_good = {}

    # Track pipelines that have had repeated OOMs to slow down and avoid clogging.
    s.pipeline_oom_count = {}

    # Tick counter fallback if s.executor doesn't expose time.
    s._tick = 0

    # Preemption rate-limit (avoid churn).
    s._preempt_cooldown = 0

    # Tunables (kept conservative).
    s.RESERVED = {
        Priority.QUERY: 0.25,         # reserve 25% of resources for queries
        Priority.INTERACTIVE: 0.15,   # reserve 15% for interactive
        Priority.BATCH_PIPELINE: 0.0,
    }
    s.CPU_CAP_FRAC = {
        Priority.QUERY: 0.75,         # can scale up, but not take entire pool by default
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,
    }
    s.MIN_CPU_FRAC = {
        Priority.QUERY: 0.20,
        Priority.INTERACTIVE: 0.15,
        Priority.BATCH_PIPELINE: 0.10,
    }
    s.RAM_SAFETY_MULT = {
        Priority.QUERY: 1.10,
        Priority.INTERACTIVE: 1.10,
        Priority.BATCH_PIPELINE: 1.05,
    }
    s.OOM_BACKOFF_MULT = 1.6
    s.OOM_BACKOFF_ADD_FRAC_OF_POOL = 0.05  # add 5% of pool RAM on top after OOM
    s.MAX_OOM_RETRIES = 4

    # Aging: after this many seconds (or ticks), treat batch as interactive-ish.
    s.AGING_THRESHOLD = 120.0
    s.PREEMPT_COOLDOWN_TICKS = 2  # at most once every 2 ticks


@register_scheduler(key="scheduler_none_015")
def scheduler_none_015_scheduler(s, results, pipelines):
    """
    Deterministic step function: update estimates from results, enqueue arrivals,
    then (optionally) preempt to make room for high-priority work, then assign ops.
    """
    def _now():
        # Prefer simulator time if present; otherwise use monotonic tick.
        t = getattr(s.executor, "time", None)
        if t is None:
            return float(s._tick)
        try:
            return float(t)
        except Exception:
            return float(s._tick)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _priority_rank(pri):
        # Smaller is higher priority.
        if pri == Priority.QUERY:
            return 0
        if pri == Priority.INTERACTIVE:
            return 1
        return 2

    def _effective_priority(pipeline):
        # Simple aging: if batch has waited long enough, temporarily treat as INTERACTIVE for selection.
        pri = pipeline.priority
        if pri != Priority.BATCH_PIPELINE:
            return pri
        arr_t = s.pipeline_arrival_time.get(pipeline.pipeline_id, _now())
        waited = _now() - arr_t
        if waited >= s.AGING_THRESHOLD:
            return Priority.INTERACTIVE
        return pri

    def _iter_pipelines_in_service_order():
        # Return list of pipelines in selection order with aging boost.
        # We keep FIFO within each queue, but choose between queues by priority.
        # Aging can lift batch to interactive.
        cand = []
        for q in (s.q_query, s.q_interactive, s.q_batch):
            for p in q:
                cand.append(p)
        # Sort by effective priority, then arrival time (FIFO-ish), then pipeline_id for determinism.
        cand.sort(key=lambda p: (_priority_rank(_effective_priority(p)),
                                s.pipeline_arrival_time.get(p.pipeline_id, 0.0),
                                p.pipeline_id))
        return cand

    def _is_done_or_failed(pipeline):
        st = pipeline.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator is FAILED, we may still retry; don't drop here.
        return False

    def _get_ready_ops(pipeline, k=1):
        st = pipeline.runtime_status()
        # We allow retry of FAILED ops (ASSIGNABLE_STATES includes FAILED) once parents are complete.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:k]

    def _op_key(pipeline, op):
        # Best-effort stable key for resource learning.
        # Prefer explicit id fields; fallback to repr/str.
        op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "id", None)
        if op_id is None:
            op_id = str(op)
        return (pipeline.pipeline_id, op_id)

    def _estimate_ram_for_op(pool, pipeline, op):
        # Start conservative: if we have a last good, use it; else a share of pool.
        key = _op_key(pipeline, op)
        if key in s.op_last_good:
            base = s.op_last_good[key]
        else:
            base = s.op_ram_est.get(key, None)
            if base is None:
                # Default to 1/6 of pool RAM for query/interactive; 1/8 for batch.
                pri = pipeline.priority
                frac = 1.0 / 6.0 if pri in (Priority.QUERY, Priority.INTERACTIVE) else 1.0 / 8.0
                base = max(1.0, pool.max_ram_pool * frac)
        # Safety margin by priority.
        base *= s.RAM_SAFETY_MULT.get(pipeline.priority, 1.05)
        # Clamp to pool capacity.
        return max(1.0, min(base, pool.max_ram_pool))

    def _choose_cpu(pool, pipeline):
        # Avoid giving whole pool to one op; cap by priority, but ensure some minimum.
        max_cpu = pool.max_cpu_pool
        avail_cpu = pool.avail_cpu_pool
        if max_cpu <= 0 or avail_cpu <= 0:
            return 0.0
        cap = max_cpu * s.CPU_CAP_FRAC.get(pipeline.priority, 0.5)
        floor = max_cpu * s.MIN_CPU_FRAC.get(pipeline.priority, 0.1)
        cpu = min(avail_cpu, cap)
        cpu = max(cpu, min(avail_cpu, floor))
        # Ensure positive
        return max(0.0, cpu)

    def _reserved_headroom(pool, pri):
        # Reserve higher priority shares from being consumed by lower priorities.
        # For interactive, preserve both query + interactive shares.
        if pri == Priority.QUERY:
            frac = s.RESERVED[Priority.QUERY]
        elif pri == Priority.INTERACTIVE:
            frac = s.RESERVED[Priority.QUERY] + s.RESERVED[Priority.INTERACTIVE]
        else:
            frac = 0.0
        return (pool.max_cpu_pool * frac, pool.max_ram_pool * frac)

    def _can_use_resources(pool, pipeline, need_cpu, need_ram):
        # Enforce reservations: low priority can't consume into reserved headroom.
        pri = pipeline.priority
        res_cpu, res_ram = _reserved_headroom(pool, pri)
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if pri == Priority.BATCH_PIPELINE:
            # Batch cannot dip into query+interactive reserve.
            if (avail_cpu - need_cpu) < res_cpu:
                return False
            if (avail_ram - need_ram) < res_ram:
                return False
        elif pri == Priority.INTERACTIVE:
            # Interactive cannot dip into query reserve.
            q_cpu, q_ram = _reserved_headroom(pool, Priority.QUERY)
            if (avail_cpu - need_cpu) < q_cpu:
                return False
            if (avail_ram - need_ram) < q_ram:
                return False
        # Query can use all available.
        return need_cpu <= avail_cpu and need_ram <= avail_ram

    def _update_from_results():
        # Learn from OOM failures; record last good on success.
        # We cannot reliably map result ops back to pipeline id here, so we learn per op object when possible.
        for r in results:
            if not getattr(r, "ops", None):
                continue
            # Update estimates for each op in result.
            for op in r.ops:
                # We don't have pipeline_id on op; use r.pipeline_id if present else skip learning keying by pipeline.
                pid = getattr(r, "pipeline_id", None)
                if pid is None:
                    # Can't key; skip learning for this op.
                    continue
                key = (pid, getattr(op, "operator_id", getattr(op, "op_id", getattr(op, "id", str(op)))))
                if r.failed():
                    # If error hints at OOM, backoff RAM.
                    err = getattr(r, "error", "") or ""
                    if "oom" in str(err).lower() or "out of memory" in str(err).lower() or "memory" in str(err).lower():
                        prev = s.op_ram_est.get(key, getattr(r, "ram", None))
                        if prev is None:
                            prev = 1.0
                        pool = s.executor.pools[r.pool_id]
                        bumped = prev * s.OOM_BACKOFF_MULT + (pool.max_ram_pool * s.OOM_BACKOFF_ADD_FRAC_OF_POOL)
                        bumped = min(bumped, pool.max_ram_pool)
                        s.op_ram_est[key] = max(1.0, bumped)
                        s.pipeline_oom_count[pid] = s.pipeline_oom_count.get(pid, 0) + 1
                else:
                    # Success: remember last good allocation.
                    good_ram = getattr(r, "ram", None)
                    if good_ram is not None:
                        s.op_last_good[key] = good_ram
                        # Also keep op_ram_est in sync to avoid oscillations.
                        s.op_ram_est[key] = good_ram

    def _enqueue_new_pipelines():
        now = _now()
        for p in pipelines:
            q = _queue_for_priority(p.priority)
            q.append(p)
            if p.pipeline_id not in s.pipeline_arrival_time:
                s.pipeline_arrival_time[p.pipeline_id] = now

    def _garbage_collect_queues():
        # Remove completed pipelines; keep failed pipelines for retry unless too many OOMs.
        def _filter(q):
            out = []
            for p in q:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                # If too many OOMs, stop retrying to avoid clogging. (Still a penalty, but prevents cascading.)
                pid = p.pipeline_id
                if s.pipeline_oom_count.get(pid, 0) > s.MAX_OOM_RETRIES:
                    continue
                out.append(p)
            return out

        s.q_query = _filter(s.q_query)
        s.q_interactive = _filter(s.q_interactive)
        s.q_batch = _filter(s.q_batch)

    def _select_preemption_candidate():
        # Preempt a single low-priority running container if it likely allows a high-priority assignment.
        # Best effort: look for any active container; if executor doesn't expose, do nothing.
        containers = getattr(s.executor, "containers", None)
        if containers is None:
            return None
        # Choose lowest priority, largest resource consumer to free headroom.
        cand = []
        for c in containers:
            # Expect fields: container_id, pool_id, priority, cpu, ram, state
            pri = getattr(c, "priority", None)
            if pri is None:
                continue
            if pri == Priority.QUERY:
                continue
            state = getattr(c, "state", None)
            if state is not None and str(state).upper() in ("SUSPENDING", "COMPLETED", "FAILED"):
                continue
            cand.append(c)
        if not cand:
            return None
        cand.sort(key=lambda c: (_priority_rank(getattr(c, "priority", Priority.BATCH_PIPELINE)) * -1,
                                 getattr(c, "cpu", 0.0) + getattr(c, "ram", 0.0)))
        return cand[0]

    def _high_priority_waiting_ready_exists():
        # Detect if we have any ready QUERY/INTERACTIVE op that is blocked by resources (best effort).
        for p in _iter_pipelines_in_service_order():
            eff = _effective_priority(p)
            if eff not in (Priority.QUERY, Priority.INTERACTIVE):
                continue
            if _is_done_or_failed(p):
                continue
            if _get_ready_ops(p, k=1):
                return True
        return False

    # ---- Step begins ----
    s._tick += 1
    _update_from_results()
    _enqueue_new_pipelines()
    _garbage_collect_queues()

    # Early exit if nothing changes.
    if not pipelines and not results:
        # Still allow scheduling if queues non-empty; don't early-exit.
        pass

    suspensions = []
    assignments = []

    # Preemption logic (very conservative).
    if s._preempt_cooldown > 0:
        s._preempt_cooldown -= 1
    else:
        if _high_priority_waiting_ready_exists():
            victim = _select_preemption_candidate()
            if victim is not None:
                suspensions.append(Suspend(getattr(victim, "container_id"), getattr(victim, "pool_id")))
                s._preempt_cooldown = s.PREEMPT_COOLDOWN_TICKS

    # Placement + assignment: iterate pools; for each, greedily place highest value ready op that fits.
    # We assign at most 1 op per pool per tick to limit interference and improve tail.
    pipelines_ordered = _iter_pipelines_in_service_order()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        chosen = None
        chosen_ops = None
        chosen_cpu = 0.0
        chosen_ram = 0.0
        chosen_pri = None
        chosen_pid = None

        for p in pipelines_ordered:
            if _is_done_or_failed(p):
                continue
            # De-prioritize pipelines that have had many OOMs (keep retrying, but not at expense of queries).
            if p.priority != Priority.QUERY:
                if s.pipeline_oom_count.get(p.pipeline_id, 0) >= 2:
                    continue

            ops = _get_ready_ops(p, k=1)
            if not ops:
                continue

            # Estimate resources.
            cpu = _choose_cpu(pool, p)
            if cpu <= 0:
                continue
            ram = _estimate_ram_for_op(pool, p, ops[0])

            if not _can_use_resources(pool, p, cpu, ram):
                continue

            # Prefer higher effective priority, then smaller RAM (packing), then earlier arrival.
            eff_pri = _effective_priority(p)
            key = (_priority_rank(eff_pri), ram, s.pipeline_arrival_time.get(p.pipeline_id, 0.0), p.pipeline_id)
            if chosen is None:
                chosen = p
                chosen_ops = ops
                chosen_cpu = cpu
                chosen_ram = ram
                chosen_pri = p.priority
                chosen_pid = p.pipeline_id
                chosen_key = key
            else:
                if key < chosen_key:
                    chosen = p
                    chosen_ops = ops
                    chosen_cpu = cpu
                    chosen_ram = ram
                    chosen_pri = p.priority
                    chosen_pid = p.pipeline_id
                    chosen_key = key

        if chosen is not None:
            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=chosen_cpu,
                    ram=chosen_ram,
                    priority=chosen_pri,
                    pool_id=pool_id,
                    pipeline_id=chosen_pid,
                )
            )

    return suspensions, assignments
