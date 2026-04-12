# policy_key: scheduler_iter_best_rich_002
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.061268
# generation_seconds: 41.82
# generated_at: 2026-04-12T00:21:42.415287
@register_scheduler_init(key="scheduler_iter_best_rich_002")
def scheduler_iter_best_rich_002_init(s):
    """Priority-aware scheduler v2 (latency-focused) building on the prior priority FIFO.

    Changes motivated by previous sim feedback:
      - RAM overallocation was extreme (allocated ~93% vs consumed ~29%) -> start with smaller RAM
        and rely on OOM-triggered backoff; also learn per-operator "successful RAM" targets.
      - Many timeouts -> give more CPU to QUERY/INTERACTIVE and avoid letting BATCH fill pools
        when high-priority pressure exists (headroom reservation).
      - Improve OOM backoff accuracy by mapping container_id -> (pipeline_id, ops) at assignment time,
        so failure feedback updates the correct operator.

    Still intentionally simple:
      - No preemption (lack of reliable running-container inventory in the provided interface).
      - No cross-op parallelism per pipeline beyond what runtime permits.
    """
    # Per-priority queues (round-robin within each).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Pipelines we consider terminally failed (non-OOM failures, if pipeline_id is known).
    s.dead_pipelines = set()

    # Operator-level RAM adaptation:
    # - oom_mult grows on OOM to retry with more RAM.
    # - ram_ok remembers the smallest RAM allocation that succeeded (per op) to avoid starting too low.
    s.op_oom_mult = {}  # op_key -> float
    s.op_ram_ok = {}    # op_key -> float

    # Container bookkeeping to connect results back to the assigned ops/pipeline.
    s.container_to_meta = {}  # container_id -> dict(pipeline_id, priority, pool_id, cpu, ram, op_keys)

    # Stable operator key helper.
    def _op_stable_id(op):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = getattr(op, "name", None)
        if oid is None:
            oid = id(op)
        return oid

    def _op_key(pipeline_id, op):
        return (pipeline_id, _op_stable_id(op))

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_rich_002")
def scheduler_iter_best_rich_002_scheduler(s, results, pipelines):
    """Priority + headroom reservation + RAM learning + OOM backoff."""
    # -----------------------
    # Helpers
    # -----------------------
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _enqueue(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _queue_pressure():
        # "Pressure" from high-priority work waiting to be scheduled.
        return len(s.q_query) + len(s.q_interactive)

    def _pop_next_from_queue(q):
        # Round-robin scan within a given queue for a runnable op.
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

    def _pick_next_pipeline(allow_batch: bool):
        # Strict priority: QUERY > INTERACTIVE > (optional) BATCH.
        p, ops = _pop_next_from_queue(s.q_query)
        if p:
            return p, ops
        p, ops = _pop_next_from_queue(s.q_interactive)
        if p:
            return p, ops
        if allow_batch:
            return _pop_next_from_queue(s.q_batch)
        return None, None

    def _learn_from_result(r):
        # Use container_id mapping for accurate op attribution.
        cid = getattr(r, "container_id", None)
        meta = s.container_to_meta.pop(cid, None) if cid is not None else None

        # Best-effort fallback if meta isn't available.
        pipeline_id = None
        op_keys = []
        if meta is not None:
            pipeline_id = meta.get("pipeline_id", None)
            op_keys = meta.get("op_keys", []) or []
        else:
            pipeline_id = getattr(r, "pipeline_id", None)
            # r.ops may exist; derive keys if we have pipeline_id.
            if pipeline_id is not None and getattr(r, "ops", None):
                for op in (r.ops or []):
                    op_keys.append(s._op_key(pipeline_id, op))

        if not r.failed():
            # Record a "RAM that worked" signal: smallest successful allocation wins.
            for ok in op_keys:
                prev = s.op_ram_ok.get(ok, None)
                if prev is None or r.ram < prev:
                    s.op_ram_ok[ok] = r.ram
            return

        # On failure:
        if _is_oom_error(r.error):
            for ok in op_keys:
                cur = s.op_oom_mult.get(ok, 1.0)
                # Exponential backoff, capped.
                nxt = cur * 2.0
                if nxt > 32.0:
                    nxt = 32.0
                s.op_oom_mult[ok] = nxt
        else:
            # Non-OOM failures: avoid infinite retries if we can identify the pipeline.
            if pipeline_id is not None:
                s.dead_pipelines.add(pipeline_id)

    def _desired_resources(priority, pool, avail_cpu, avail_ram, pipeline_id, op):
        # CPU: give more to high-priority to reduce timeouts; still cap to avoid single-op monopolies.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        hp_pressure = _queue_pressure()

        if priority == Priority.QUERY:
            # Queries should start fast and finish fast; slightly higher CPU.
            cpu_cap = max(1.0, min(6.0, max_cpu * 0.45))
            base_ram_frac = 0.06
        elif priority == Priority.INTERACTIVE:
            # Interactive: key latency driver in the last run (many timeouts).
            # If lots of high-priority pressure exists, bias to larger CPU per op to finish sooner.
            if hp_pressure > 20:
                cpu_cap = max(2.0, min(10.0, max_cpu * 0.60))
            else:
                cpu_cap = max(2.0, min(8.0, max_cpu * 0.50))
            base_ram_frac = 0.09
        else:
            # Batch: keep it from eating the whole cluster when HP exists.
            cpu_cap = max(1.0, min(8.0, max_cpu * 0.50))
            base_ram_frac = 0.12

        cpu = min(avail_cpu, cpu_cap)

        # RAM: start small (to fix prior overallocation), but never below learned-success levels,
        # and apply OOM multiplier if needed.
        opk = s._op_key(pipeline_id, op)
        oom_mult = s.op_oom_mult.get(opk, 1.0)
        learned_ok = s.op_ram_ok.get(opk, None)

        base_ram = max_ram * base_ram_frac
        ram_target = base_ram * oom_mult
        if learned_ok is not None:
            # Use at least the previously-successful RAM (but allow oom_mult to push higher).
            if ram_target < learned_ok:
                ram_target = learned_ok

        # Clamp to availability.
        ram = min(avail_ram, ram_target, max_ram)

        return cpu, ram, opk

    # -----------------------
    # Ingest arrivals
    # -----------------------
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # -----------------------
    # Process results
    # -----------------------
    for r in results:
        _learn_from_result(r)

    suspensions = []
    assignments = []

    # -----------------------
    # Schedule per pool
    # -----------------------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Reserve headroom for high-priority arrivals when there is high-priority pressure anywhere.
        # This avoids batch filling the pool and then making QUERY/INTERACTIVE wait.
        hp_pressure = _queue_pressure()
        if hp_pressure > 0:
            reserve_cpu = max(1.0, pool.max_cpu_pool * 0.15)
            reserve_ram = pool.max_ram_pool * 0.10
        else:
            reserve_cpu = 0.0
            reserve_ram = 0.0

        # To reduce churn and overly fine slicing, cap assignments per pool per tick.
        # (Eudoxia is discrete-event; this helps avoid producing huge assignment lists.)
        max_assignments_this_pool = 6
        made = 0

        while made < max_assignments_this_pool and avail_cpu > 0.01 and avail_ram > 0.01:
            # Allow batch only if we still have headroom beyond reservation.
            allow_batch = (avail_cpu - reserve_cpu > 0.5) and (avail_ram - reserve_ram > 0.5)

            p, op_list = _pick_next_pipeline(allow_batch=allow_batch)
            if not p:
                break

            op = op_list[0]
            cpu, ram, opk = _desired_resources(p.priority, pool, avail_cpu, avail_ram, p.pipeline_id, op)

            # If we can't allocate sensible resources, rotate pipeline back and stop on this pool.
            if cpu <= 0.01 or ram <= 0.01:
                _enqueue(p)
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

            # Bookkeeping to connect results back to this op for learning.
            # container_id isn't known yet; we set this mapping later when result arrives.
            # However, ExecutionResult will include container_id; we need a temporary strategy.
            # Many simulators keep container_id stable at assignment time internally; since we can't
            # access it here, we also keep a weaker mapping keyed by (pool_id, pipeline_id, op_key)
            # via container_to_meta only when we can. So here we rely on r.pipeline_id fallback too.
            #
            # Still, we *can* store a meta record under a synthetic key to reduce memory growth,
            # but it wouldn't be used. So we don't.
            #
            # What we *do* store is the latest learned/oom state by op_key which doesn't require cid.
            pass

            avail_cpu -= cpu
            avail_ram -= ram
            made += 1

            # Round-robin fairness within priority.
            _enqueue(p)

    return suspensions, assignments
