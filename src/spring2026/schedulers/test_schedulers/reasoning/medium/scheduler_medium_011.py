# policy_key: scheduler_medium_011
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.079013
# generation_seconds: 89.26
# generated_at: 2026-04-09T22:43:16.102440
@register_scheduler_init(key="scheduler_medium_011")
def scheduler_medium_011_init(s):
    """
    Priority-aware, OOM-avoidant scheduler with soft reservations and retry/backoff.

    Main ideas:
      - Maintain separate FIFO/round-robin queues per priority (QUERY > INTERACTIVE > BATCH).
      - Soft-reserve a slice of each pool's resources when high-priority work is waiting,
        preventing batch from consuming all headroom and inflating tail latency.
      - Avoid failures (720s penalty) via conservative initial RAM sizing + OOM-triggered
        exponential RAM backoff per operator, with a retry cap to prevent infinite thrash.
      - Use modest CPU caps per priority to balance latency vs. concurrency.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Keep strong references for lookup/cleanup.
    s.pipelines_by_id = {}

    # Map operator identity -> pipeline id (so results can be attributed).
    s.opkey_to_pipeline_id = {}

    # Per-operator RAM multiplier (exponential backoff on OOM).
    s.op_ram_mult = {}

    # Per-operator retry counts and last-known failure reason.
    s.op_retry_count = {}
    s.op_last_error = {}

    # Ticks are discrete scheduler invocations; used only for light aging heuristics.
    s.tick = 0

    # Tunables
    s.reserve_frac_cpu = 0.25  # reserved when QUERY/INTERACTIVE backlog exists
    s.reserve_frac_ram = 0.25

    s.cpu_caps = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    # Base RAM fractions of pool.max_ram_pool (scaled by op-specific multiplier).
    s.base_ram_frac = {
        Priority.QUERY: 0.35,
        Priority.INTERACTIVE: 0.25,
        Priority.BATCH_PIPELINE: 0.15,
    }

    # Retry caps: OOM can be retried more; unknown/non-OOM failures get fewer retries.
    s.max_retries_oom = 6
    s.max_retries_other = 1


@register_scheduler(key="scheduler_medium_011")
def scheduler_medium_011(s, results, pipelines):
    """
    See init docstring for overview.
    """
    s.tick += 1

    def _op_key(op):
        # Prefer stable user-level ids if present; otherwise fallback to object id.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if v is not None:
                    return (attr, v)
        return ("pyid", id(op))

    def _is_oom(err):
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)

    def _iter_pipeline_ops(p):
        # Best-effort enumeration of pipeline operators to build op->pipeline index.
        vals = getattr(p, "values", None)
        if vals is None:
            return
        iterable = None
        if isinstance(vals, dict):
            iterable = vals.values()
        else:
            try:
                iter(vals)
                iterable = vals
            except Exception:
                iterable = None

        if iterable is None:
            return

        for v in iterable:
            if v is None:
                continue
            if isinstance(v, (list, tuple)):
                for op in v:
                    if op is not None:
                        yield op
            else:
                yield v

    def _enqueue_pipeline(p):
        s.pipelines_by_id[p.pipeline_id] = p
        # Index operators so we can attribute results to pipelines.
        for op in _iter_pipeline_ops(p):
            s.opkey_to_pipeline_id[_op_key(op)] = p.pipeline_id

        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _pick_next_ready_op(prio):
        """
        Round-robin scan within a priority queue to find the next ready operator.
        Returns (pipeline, op) or (None, None).
        """
        q = _queue_for_priority(prio)
        n = len(q)
        for _ in range(n):
            p = q.popleft()
            st = p.runtime_status()

            # Drop completed pipelines from queue.
            if st.is_pipeline_successful():
                continue

            # Find assignable ops whose parents are complete.
            assignable = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not assignable:
                # Not ready yet; keep it in rotation.
                q.append(p)
                continue

            # Avoid infinite retries on repeatedly failing ops.
            failed_set = set(st.get_ops([OperatorState.FAILED], require_parents_complete=True))
            chosen = None
            for op in assignable:
                if op in failed_set:
                    ok = _op_key(op)
                    last_err = s.op_last_error.get(ok, "")
                    retry_cap = s.max_retries_oom if _is_oom(last_err) else s.max_retries_other
                    if s.op_retry_count.get(ok, 0) >= retry_cap:
                        continue
                chosen = op
                break

            if chosen is None:
                # Nothing worth retrying in this pipeline right now; keep rotating.
                q.append(p)
                continue

            # Keep pipeline in queue for future operators (round-robin).
            q.append(p)
            return p, chosen

        return None, None

    def _desired_resources(pool, prio, op):
        """
        Compute conservative RAM with OOM backoff, and CPU with per-priority cap.
        """
        # CPU: cap to keep concurrency, but allow queries to burst higher.
        cap = min(float(pool.max_cpu_pool), float(s.cpu_caps.get(prio, pool.max_cpu_pool)))
        cpu = max(1.0, min(float(pool.avail_cpu_pool), cap))

        # RAM: base fraction times per-op multiplier, clipped to pool limits.
        ok = _op_key(op)
        mult = float(s.op_ram_mult.get(ok, 1.0))
        base = float(s.base_ram_frac.get(prio, 0.2)) * float(pool.max_ram_pool)
        # Keep a small absolute minimum to avoid degeneracy on tiny pools.
        min_abs = 0.05 * float(pool.max_ram_pool)
        ram = max(min_abs, min(float(pool.max_ram_pool), base * mult))
        return cpu, ram

    # Enqueue new arrivals.
    for p in pipelines:
        _enqueue_pipeline(p)

    # Incorporate execution feedback (especially OOM).
    for r in results:
        if not getattr(r, "ops", None):
            continue
        if not r.failed():
            # Success: we could learn, but simulator says extra RAM doesn't help.
            continue

        err = getattr(r, "error", None)
        oom = _is_oom(err)
        for op in r.ops:
            ok = _op_key(op)
            s.op_last_error[ok] = err
            s.op_retry_count[ok] = s.op_retry_count.get(ok, 0) + 1
            if oom:
                # Exponential RAM backoff on OOM to improve completion rate.
                cur = float(s.op_ram_mult.get(ok, 1.0))
                s.op_ram_mult[ok] = min(cur * 2.0, 32.0)

    # Early exit if nothing changed (but keep deterministic behavior).
    if not pipelines and not results:
        return [], []

    suspensions = []  # We avoid preemption (wasted work can hurt overall score).
    assignments = []

    # Determine whether there is high-priority backlog with ready work.
    def _has_ready_hi():
        for prio in (Priority.QUERY, Priority.INTERACTIVE):
            p, op = _pick_next_ready_op(prio)
            if p is not None:
                # Put back exactly as we took it: _pick_next_ready_op already re-appends.
                return True
        return False

    hi_backlog = _has_ready_hi()

    # For each pool, schedule as much as we can while respecting soft reservations.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Local bookkeeping to avoid over-allocating when making multiple assignments.
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        reserve_cpu = float(s.reserve_frac_cpu) * float(pool.max_cpu_pool) if hi_backlog else 0.0
        reserve_ram = float(s.reserve_frac_ram) * float(pool.max_ram_pool) if hi_backlog else 0.0

        # Limit scheduling loop to avoid long runtimes in pathological cases.
        # (Still allows multiple parallel assignments per tick/pool.)
        for _ in range(64):
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

            # Priority order: QUERY -> INTERACTIVE -> BATCH, but batch can use leftovers.
            chosen_prio = None
            chosen_p = None
            chosen_op = None

            for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                # Enforce soft reservation: if high-priority backlog exists, batch can't
                # consume the reserved headroom (reduces query/interactive queueing).
                if prio == Priority.BATCH_PIPELINE and hi_backlog:
                    if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                        continue

                p, op = _pick_next_ready_op(prio)
                if p is None:
                    continue

                # Compute desired resources and check if we can meet them reasonably.
                cpu_need, ram_need = _desired_resources(pool, prio, op)

                # If RAM is tight, avoid launching with too little (prevents OOM thrash).
                # Allow small underfill for high-priority to reduce queueing.
                if ram_need > avail_ram:
                    underfill = avail_ram / max(ram_need, 1e-9)
                    if prio in (Priority.QUERY, Priority.INTERACTIVE):
                        if underfill < 0.80:
                            continue
                        ram_need = avail_ram
                    else:
                        if underfill < 0.50:
                            continue
                        ram_need = avail_ram

                if cpu_need > avail_cpu:
                    cpu_need = avail_cpu
                if cpu_need < 1.0:
                    continue

                chosen_prio, chosen_p, chosen_op = prio, p, op
                chosen_cpu, chosen_ram = float(cpu_need), float(ram_need)
                break

            if chosen_p is None:
                break

            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=chosen_cpu,
                    ram=chosen_ram,
                    priority=chosen_prio,
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            avail_cpu -= chosen_cpu
            avail_ram -= chosen_ram

    return suspensions, assignments

