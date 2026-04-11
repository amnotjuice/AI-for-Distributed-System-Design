# policy_key: scheduler_none_007
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040795
# generation_seconds: 40.24
# generated_at: 2026-03-12T21:26:07.358044
@register_scheduler_init(key="scheduler_none_007")
def scheduler_none_007_init(s):
    """Priority-aware FIFO with conservative sizing + basic preemption + OOM retry.

    Small, incremental improvements over naive FIFO:
    1) Maintain separate queues by priority and schedule higher priority first.
    2) Avoid "give everything to one op" by using a conservative per-op CPU/RAM cap.
    3) Learn per-(pipeline, op) minimum RAM from OOMs and retry with higher RAM.
    4) If a high-priority op cannot fit, preempt (suspend) one low-priority running container to make room.

    Notes/assumptions:
    - We only suspend (preempt); resumption behavior is handled by the simulator.
    - We keep logic simple and robust to missing fields by using getattr fallbacks.
    """
    # Per-priority waiting queues (pipelines)
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track per-(pipeline_id, op_id) RAM floor learned from OOMs: bytes or arbitrary sim units
    s.op_ram_floor = {}

    # Track last seen running containers by pool and priority for potential preemption
    # Structure: {(pool_id, priority): [ExecutionResult-like records for RUNNING]}
    s.running_index = {}

    # Conservative default sizing fractions (of pool availability) and caps (of pool max)
    s.default_cpu_frac = 0.50   # use up to 50% of currently available CPU for one assignment
    s.default_ram_frac = 0.50   # use up to 50% of currently available RAM for one assignment
    s.cpu_cap_frac_of_max = 0.75
    s.ram_cap_frac_of_max = 0.75

    # Minimum granularity to avoid assigning tiny resources (helps avoid pathological under-scheduling)
    s.min_cpu = 1
    s.min_ram = 1

    # OOM backoff factor for RAM floor growth
    s.oom_ram_backoff = 1.5

    # How many ops to pack per assignment at most (keep small to reduce head-of-line blocking)
    s.max_ops_per_assignment = 1


@register_scheduler(key="scheduler_none_007")
def scheduler_none_007_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler loop:
    - Enqueue new pipelines into per-priority queues.
    - Process execution results: learn RAM floors from OOM and update running index.
    - For each pool, attempt to schedule highest priority ready op(s).
    - If insufficient headroom for high priority, preempt one lower-priority running container in that pool.
    """
    # --- Helper functions (local, no imports needed) ---
    def _prio_order():
        # Highest to lowest
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _lower_priorities(p):
        order = _prio_order()
        idx = order.index(p)
        return order[idx + 1 :]

    def _safe_err_is_oom(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return "oom" in msg or "out of memory" in msg or "out_of_memory" in msg

    def _op_key(pipeline, op):
        # Best-effort stable key; fall back to id(op)
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = getattr(op, "id", None)
        if op_id is None:
            op_id = id(op)
        return (pipeline.pipeline_id, op_id)

    def _get_ready_ops(pipeline):
        status = pipeline.runtime_status()
        # Skip completed pipelines
        if status.is_pipeline_successful():
            return []
        # Drop pipelines that have failed operators (non-retriable) EXCEPT we allow retry if failures are OOM
        # Since we don't have direct per-op failure cause here, we conservatively still try PENDING ops.
        # Assign only ops whose parents complete.
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
        return ops

    def _cap(val, lo, hi):
        if val < lo:
            return lo
        if val > hi:
            return hi
        return val

    def _compute_request(pool, pipeline, op, avail_cpu, avail_ram):
        # Conservative slice of available resources, also capped by pool max to avoid monopolization.
        cpu_cap = max(s.min_cpu, int(pool.max_cpu_pool * s.cpu_cap_frac_of_max))
        ram_cap = max(s.min_ram, int(pool.max_ram_pool * s.ram_cap_frac_of_max))

        cpu = int(avail_cpu * s.default_cpu_frac)
        ram = int(avail_ram * s.default_ram_frac)

        cpu = _cap(cpu, s.min_cpu, cpu_cap)
        ram = _cap(ram, s.min_ram, ram_cap)

        # Respect learned RAM floor for this op if present
        k = _op_key(pipeline, op)
        if k in s.op_ram_floor:
            ram = max(ram, int(s.op_ram_floor[k]))

        # Also ensure we don't request more than currently available
        cpu = min(cpu, int(avail_cpu))
        ram = min(ram, int(avail_ram))
        return cpu, ram

    def _update_running_index_from_results(results_list):
        # We track the latest observed running containers. ExecutionResult might include ops and container_id.
        # We'll rebuild a small index each tick from results (best-effort).
        s.running_index = {}
        for r in results_list or []:
            # If result indicates still running, it might not be present; but we can at least index non-failed.
            # We'll treat non-failed with a container_id as potentially preemptable if needed.
            if getattr(r, "container_id", None) is None:
                continue
            if r.failed():
                continue
            pool_id = getattr(r, "pool_id", None)
            pr = getattr(r, "priority", None)
            if pool_id is None or pr is None:
                continue
            s.running_index.setdefault((pool_id, pr), []).append(r)

    def _try_preempt_one(pool_id, need_priority):
        # Preempt a single lower-priority container in the same pool.
        for lp in _lower_priorities(need_priority):
            lst = s.running_index.get((pool_id, lp), [])
            if not lst:
                continue
            # Choose one to preempt; simplest: the first seen.
            victim = lst.pop(0)
            cid = getattr(victim, "container_id", None)
            if cid is None:
                continue
            return Suspend(container_id=cid, pool_id=pool_id)
        return None

    # --- Enqueue new pipelines by priority ---
    for p in pipelines or []:
        s.waiting_queues.setdefault(p.priority, []).append(p)

    # Early exit if nothing to do
    if (not pipelines) and (not results):
        return [], []

    suspensions = []
    assignments = []

    # --- Learn from results (OOM -> increase RAM floor) and refresh running index ---
    for r in results or []:
        if not r.failed():
            continue
        if not _safe_err_is_oom(getattr(r, "error", None)):
            continue
        # Increase RAM floor for the failed op(s) in this result
        # r.ops is a list of ops that ran in that container
        ops = getattr(r, "ops", None) or []
        pipeline_id = getattr(r, "pipeline_id", None)
        # If pipeline_id isn't present on ExecutionResult, we can't key precisely.
        # We'll fallback to op identity alone by using pipeline_id=None.
        for op in ops:
            op_id = getattr(op, "op_id", None)
            if op_id is None:
                op_id = getattr(op, "operator_id", None)
            if op_id is None:
                op_id = getattr(op, "id", None)
            if op_id is None:
                op_id = id(op)
            k = (pipeline_id, op_id)

            prev = s.op_ram_floor.get(k, None)
            base = getattr(r, "ram", None)
            if base is None:
                # If unknown, bump minimally
                base = prev if prev is not None else s.min_ram
            if prev is None:
                new_floor = max(s.min_ram, int(base * s.oom_ram_backoff))
            else:
                new_floor = max(int(prev * s.oom_ram_backoff), int(base * s.oom_ram_backoff), s.min_ram)
            s.op_ram_floor[k] = new_floor

    _update_running_index_from_results(results)

    # --- Main scheduling per pool ---
    # Requeue pipelines that are not yet schedulable (no ready ops)
    requeue_by_prio = {pr: [] for pr in s.waiting_queues.keys()}

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        scheduled_something = True
        # Try to schedule multiple assignments per pool per tick if room exists.
        while scheduled_something:
            scheduled_something = False

            # Find next pipeline in priority order that has a ready op
            chosen = None
            chosen_op = None
            for pr in _prio_order():
                q = s.waiting_queues.get(pr, [])
                while q:
                    pipeline = q.pop(0)
                    ops = _get_ready_ops(pipeline)
                    if not ops:
                        # Put it aside to requeue; might become ready later
                        requeue_by_prio.setdefault(pr, []).append(pipeline)
                        continue
                    chosen = pipeline
                    # Keep assignment small to reduce HOL blocking; take first ready op
                    chosen_op = ops[: s.max_ops_per_assignment]
                    break
                if chosen is not None:
                    break

            if chosen is None or not chosen_op:
                break

            # Compute request sizes
            cpu_req, ram_req = _compute_request(pool, chosen, chosen_op[0], avail_cpu, avail_ram)

            # If can't fit minimum request, attempt preemption if high priority
            if cpu_req <= 0 or ram_req <= 0 or cpu_req > avail_cpu or ram_req > avail_ram:
                # Attempt preempt only for QUERY/INTERACTIVE
                if chosen.priority in (Priority.QUERY, Priority.INTERACTIVE):
                    sus = _try_preempt_one(pool_id, chosen.priority)
                    if sus is not None:
                        suspensions.append(sus)
                        # Requeue chosen to try again next tick after resources free
                        requeue_by_prio.setdefault(chosen.priority, []).append(chosen)
                        # Stop scheduling in this pool this tick to avoid thrash
                        break
                # Otherwise requeue and stop (avoid spinning)
                requeue_by_prio.setdefault(chosen.priority, []).append(chosen)
                break

            # Make assignment
            assignment = Assignment(
                ops=chosen_op,
                cpu=cpu_req,
                ram=ram_req,
                priority=chosen.priority,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
            assignments.append(assignment)

            # Update local pool availability to allow packing multiple assignments
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            scheduled_something = True

            # Requeue pipeline for subsequent ops
            requeue_by_prio.setdefault(chosen.priority, []).append(chosen)

            # Stop if pool exhausted
            if avail_cpu <= 0 or avail_ram <= 0:
                break

    # Put requeue pipelines back into queues (preserve rough FIFO within each priority)
    for pr, lst in requeue_by_prio.items():
        if not lst:
            continue
        s.waiting_queues.setdefault(pr, []).extend(lst)

    return suspensions, assignments
