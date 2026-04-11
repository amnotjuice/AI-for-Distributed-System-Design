# policy_key: scheduler_none_032
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046757
# generation_seconds: 40.57
# generated_at: 2026-04-09T22:10:00.768507
@register_scheduler_init(key="scheduler_none_032")
def scheduler_none_032_init(s):
    """Priority-aware, OOM-resilient, low-churn scheduler.

    Core ideas:
    - Strictly protect QUERY and INTERACTIVE by reserving a fraction of each pool for them (soft partitioning).
    - Keep batch making progress with leftover capacity, but allow preemption of batch when high-priority work can't fit.
    - Reduce failures (720s penalty) via per-operator RAM learning:
        * Start with a conservative RAM guess derived from pool size and operator history.
        * On OOM, increase RAM multiplicatively for that operator and retry.
    - Limit churn: only preempt when we cannot place any ready high-priority operator otherwise.
    - Simple CPU sizing: moderate CPU for interactive/query to reduce latency; batch gets remaining CPU.
    """
    # Pipelines waiting for scheduling consideration (we keep them here between ticks)
    s.waiting_queue = []

    # Per-operator RAM estimates keyed by (pipeline_id, op_id) when available; fallback to op object identity
    # Value is "next RAM to try" for that operator.
    s.op_ram_hint = {}

    # Track last-seen pipelines to avoid duplicating entries when generator repeats references
    s._seen_pipeline_ids = set()

    # Reservation fractions per pool (soft): portions of CPU/RAM preferentially kept for high priority
    s.reserve = {
        Priority.QUERY: 0.45,
        Priority.INTERACTIVE: 0.25,
        Priority.BATCH_PIPELINE: 0.00,
    }

    # CPU sizing heuristics (fractions of pool max; capped by availability at assignment time)
    s.cpu_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 1.00,  # batch can take whatever is left
    }

    # Bounds to keep assignments reasonable
    s.min_cpu = 0.25  # don't assign tiny CPU slices that cause long tails
    s.min_ram_frac_pool = 0.10  # for unknown ops, start with at least 10% of pool RAM
    s.max_attempts_per_tick_per_pool = 16  # avoid long loops


@register_scheduler(key="scheduler_none_032")
def scheduler_none_032(s, results, pipelines):
    """Scheduler step."""
    # ---- helpers (pure) ----
    def _prio_rank(p):
        if p == Priority.QUERY:
            return 0
        if p == Priority.INTERACTIVE:
            return 1
        return 2

    def _pipeline_sort_key(pipeline):
        # Prefer higher priority, then smaller pipeline_id for determinism
        return (_prio_rank(pipeline.priority), pipeline.pipeline_id)

    def _op_key(pipeline, op):
        # Prefer stable identifiers when available; fall back to object id
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = id(op)
        return (pipeline.pipeline_id, op_id)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _pick_ready_op(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return None
        # If any operator failed, we still allow retry (ASSIGNABLE_STATES includes FAILED in this sim)
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        # Deterministic: pick the first ready operator (topological order from status)
        return op_list[0]

    def _desired_cpu_for(pool, prio, avail_cpu):
        target = pool.max_cpu_pool * s.cpu_frac.get(prio, 0.5)
        cpu = min(avail_cpu, target)
        # ensure minimum slice if possible
        if cpu < s.min_cpu and avail_cpu >= s.min_cpu:
            cpu = s.min_cpu
        # always at least some cpu if anything available
        if cpu <= 0:
            return 0
        return cpu

    def _desired_ram_for(pool, pipeline, op, avail_ram):
        key = _op_key(pipeline, op)
        hint = s.op_ram_hint.get(key)

        # Start conservative: a modest fraction of pool RAM, but never exceed availability.
        base = max(pool.max_ram_pool * s.min_ram_frac_pool, 1e-9)
        ram = hint if hint is not None else base

        # Clamp to something feasible; if we have more, we may give more to reduce OOM risk slightly.
        ram = min(ram, avail_ram)

        # If still very small but there's ample RAM, bump up to reduce OOM failures.
        if hint is None and avail_ram >= base * 2:
            ram = min(avail_ram, base * 2)

        # Ensure positive
        if ram <= 0:
            return 0
        return ram

    def _pool_reserved(pool, prio):
        frac = s.reserve.get(prio, 0.0)
        return (pool.max_cpu_pool * frac, pool.max_ram_pool * frac)

    def _can_use_resources(pool, prio, cpu, ram):
        # Soft reservation: batch should not consume reserved for higher priorities.
        # For QUERY/INTERACTIVE, allow using all available.
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            return cpu <= pool.avail_cpu_pool and ram <= pool.avail_ram_pool

        # Batch: keep QUERY+INTERACTIVE reserves if there is any pending high-priority work.
        reserve_cpu_q, reserve_ram_q = _pool_reserved(pool, Priority.QUERY)
        reserve_cpu_i, reserve_ram_i = _pool_reserved(pool, Priority.INTERACTIVE)
        keep_cpu = reserve_cpu_q + reserve_cpu_i
        keep_ram = reserve_ram_q + reserve_ram_i

        usable_cpu = max(pool.avail_cpu_pool - keep_cpu, 0.0)
        usable_ram = max(pool.avail_ram_pool - keep_ram, 0.0)
        return cpu <= usable_cpu and ram <= usable_ram

    def _preempt_one_batch_in_pool(pool_id):
        # Best-effort: suspend one running batch container to free headroom.
        # We rely on executor exposing containers in some form; if absent, no-op.
        pool = s.executor.pools[pool_id]
        containers = getattr(pool, "containers", None)
        if containers is None:
            containers = getattr(pool, "running_containers", None)
        if containers is None:
            return None

        # containers might be dict or list
        if isinstance(containers, dict):
            it = list(containers.values())
        else:
            it = list(containers)

        # Find a batch container to suspend; if none, do nothing
        for c in it:
            cprio = getattr(c, "priority", None)
            if cprio == Priority.BATCH_PIPELINE:
                cid = getattr(c, "container_id", None)
                if cid is None:
                    cid = getattr(c, "id", None)
                if cid is None:
                    continue
                return Suspend(container_id=cid, pool_id=pool_id)
        return None

    # ---- ingest new pipelines ----
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s._seen_pipeline_ids:
            s._seen_pipeline_ids.add(pid)
            s.waiting_queue.append(p)

    # ---- learn from results (OOM -> increase RAM hint and allow retry) ----
    for r in results:
        if not r.failed():
            continue
        if _is_oom_error(r.error):
            # Increase RAM hint for each op in the failed container
            for op in r.ops:
                # We don't always have pipeline_id here; use r.pipeline_id if present, else infer from op attr
                pid = getattr(r, "pipeline_id", None)
                if pid is None:
                    pid = getattr(op, "pipeline_id", None)
                # If we cannot associate, skip
                if pid is None:
                    continue

                op_id = getattr(op, "op_id", None)
                if op_id is None:
                    op_id = getattr(op, "operator_id", None)
                if op_id is None:
                    op_id = id(op)
                key = (pid, op_id)

                prev = s.op_ram_hint.get(key)
                tried = r.ram if getattr(r, "ram", None) is not None else prev
                if tried is None:
                    continue

                # Multiplicative increase with floor; cap at pool max when scheduling.
                next_try = max(tried * 1.6, tried + 0.05)
                s.op_ram_hint[key] = next_try

    # Early exit if nothing changed that affects decisions
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ---- clean and sort queue ----
    # Keep only active/incomplete pipelines
    active = []
    for p in s.waiting_queue:
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        # Keep failed pipelines too (we'll retry failed operators); do not drop.
        active.append(p)
    active.sort(key=_pipeline_sort_key)
    s.waiting_queue = active

    # ---- two-pass scheduling per pool: high priority first, then batch ----
    # To reduce starvation, we'll do "aging-lite": if batch exists and no high-priority can run, allow batch.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Attempt to schedule multiple ops per pool per tick (up to a cap)
        attempts = 0
        made_progress = True
        while made_progress and attempts < s.max_attempts_per_tick_per_pool:
            attempts += 1
            made_progress = False

            # Recompute avail each loop (assignments consume from pool in sim after return,
            # but we approximate by tracking locally to avoid oversubscribe).
            # We'll maintain local "shadow" availability based on planned assignments.
            planned_cpu = sum(a.cpu for a in assignments if a.pool_id == pool_id)
            planned_ram = sum(a.ram for a in assignments if a.pool_id == pool_id)
            avail_cpu = pool.avail_cpu_pool - planned_cpu
            avail_ram = pool.avail_ram_pool - planned_ram
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Pass 1: QUERY then INTERACTIVE
            scheduled = False
            for prio in (Priority.QUERY, Priority.INTERACTIVE):
                # Find first pipeline of this priority with a ready op
                cand = None
                op = None
                for p in s.waiting_queue:
                    if p.priority != prio:
                        continue
                    op0 = _pick_ready_op(p)
                    if op0 is None:
                        continue
                    cand, op = p, op0
                    break
                if cand is None:
                    continue

                cpu = _desired_cpu_for(pool, prio, avail_cpu)
                ram = _desired_ram_for(pool, cand, op, avail_ram)

                if cpu <= 0 or ram <= 0:
                    continue

                if _can_use_resources(pool, prio, cpu, ram):
                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu,
                            ram=ram,
                            priority=prio,
                            pool_id=pool_id,
                            pipeline_id=cand.pipeline_id,
                        )
                    )
                    scheduled = True
                    made_progress = True
                    break
                else:
                    # If high-priority cannot fit, try preempting a batch container once
                    pre = _preempt_one_batch_in_pool(pool_id)
                    if pre is not None:
                        suspensions.append(pre)
                        # After preemption, we'll try again in next loop iteration
                        made_progress = True
                    scheduled = True  # treat as action taken
                    break

            if scheduled:
                continue

            # Pass 2: BATCH_PIPELINE using leftover after reservations
            cand = None
            op = None
            for p in s.waiting_queue:
                if p.priority != Priority.BATCH_PIPELINE:
                    continue
                op0 = _pick_ready_op(p)
                if op0 is None:
                    continue
                cand, op = p, op0
                break
            if cand is None:
                break

            prio = Priority.BATCH_PIPELINE
            cpu = _desired_cpu_for(pool, prio, avail_cpu)
            ram = _desired_ram_for(pool, cand, op, avail_ram)
            if cpu <= 0 or ram <= 0:
                break

            if _can_use_resources(pool, prio, cpu, ram):
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=prio,
                        pool_id=pool_id,
                        pipeline_id=cand.pipeline_id,
                    )
                )
                made_progress = True
            else:
                # Can't run batch without violating reserves; stop on this pool.
                break

    return suspensions, assignments
