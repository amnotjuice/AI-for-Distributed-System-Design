# policy_key: scheduler_medium_005
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.109241
# generation_seconds: 83.66
# generated_at: 2026-03-12T21:59:04.782843
@register_scheduler_init(key="scheduler_medium_005")
def scheduler_medium_005_init(s):
    """
    Priority-aware, small-step improvement over naive FIFO:

    1) Maintain per-priority round-robin queues to reduce head-of-line blocking.
    2) Schedule multiple containers per pool per tick (instead of one big one).
    3) Conservative per-container sizing by priority (queries get more CPU),
       to improve latency and concurrency.
    4) OOM-aware retries: on OOM failure, increase RAM hint for that (pipeline, op)
       and allow retry; non-OOM failures are treated as terminal for the pipeline.

    Notes:
    - No preemption (we don't assume access to the set of running containers).
    - Uses "soft reservation" for high-priority by not starting new batch work when
      remaining headroom is low; cannot reclaim resources without preemption.
    """
    # Per-priority queues (round-robin within each)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track seen pipeline ids to avoid duplicates
    s.seen_pipeline_ids = set()

    # Per-(pipeline, op) resource hints learned from OOMs/success
    s.op_ram_hint = {}   # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}   # (pipeline_id, op_key) -> cpu (lightweight; mostly defaults)

    # Failure tracking: retry OOM a limited number of times; otherwise mark pipeline dead
    s.op_oom_fail_counts = {}   # (pipeline_id, op_key) -> int
    s.dead_pipelines = set()    # pipeline_id

    # Defaults (chosen to enable concurrency; queries get more CPU to reduce latency)
    s.default_cpu = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.default_ram = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 3.0,
    }

    # Caps: prevent a single op from grabbing the whole pool (improves latency under contention)
    s.max_share_cpu = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.35,
    }
    s.max_share_ram = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Soft reservation: if remaining resources are below these fractions, stop admitting new batch
    s.soft_reserve_cpu_frac = 0.20
    s.soft_reserve_ram_frac = 0.20

    # OOM retry policy
    s.max_oom_retries = 3
    s.oom_ram_growth = 2.0  # multiply RAM on each OOM


@register_scheduler(key="scheduler_medium_005")
def scheduler_medium_005(s, results, pipelines):
    """
    Scheduler tick:
      - ingest new pipelines into per-priority queues
      - update hints from execution results (OOM -> increase RAM hint)
      - for each pool, assign as many ready ops as possible:
          QUERY first, then INTERACTIVE, then BATCH (if enough headroom)
    """
    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _op_key(op):
        # Robust identifier across possible operator object shapes
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return str(v)
                except Exception:
                    pass
        return str(op)

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("killed" in msg and "memory" in msg)

    def _ensure_queue(priority):
        # In case new priorities appear, keep a queue for them
        if priority not in s.queues:
            s.queues[priority] = []
            s.default_cpu[priority] = 1.0
            s.default_ram[priority] = 3.0
            s.max_share_cpu[priority] = 0.35
            s.max_share_ram[priority] = 0.50

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.seen_pipeline_ids:
            return
        s.seen_pipeline_ids.add(p.pipeline_id)
        _ensure_queue(p.priority)
        s.queues[p.priority].append(p)

    def _pipeline_done_or_dead(p):
        if p.pipeline_id in s.dead_pipelines:
            return True
        st = p.runtime_status()
        try:
            if st.is_pipeline_successful():
                return True
        except Exception:
            pass
        return False

    def _next_ready_op_from_queue(q):
        """
        Round-robin scan:
          - rotate through pipelines until we find one with a ready op
          - returns (pipeline, [op]) or (None, None)
        """
        if not q:
            return None, None

        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            if _pipeline_done_or_dead(p):
                # drop completed/dead pipelines
                continue

            st = p.runtime_status()
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # not ready yet; keep it in rotation
                q.append(p)
                continue

            op = op_list[0]
            ok = (p.pipeline_id, _op_key(op))
            # If we've exceeded OOM retries for this op, mark pipeline dead
            if s.op_oom_fail_counts.get(ok, 0) > s.max_oom_retries:
                s.dead_pipelines.add(p.pipeline_id)
                continue

            # Found schedulable (pipeline, op): keep RR fairness by appending pipeline to end
            q.append(p)
            return p, op_list

        return None, None

    # 1) Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # 2) Update hints from results
    for r in results:
        # If we see non-OOM failures, treat pipeline as terminal (avoid retry storms)
        if r is None:
            continue

        if r.failed():
            oom = _is_oom_error(getattr(r, "error", None))
            if oom:
                # Increase RAM hint for each op in the failed container
                pool_id = getattr(r, "pool_id", 0)
                try:
                    pool = s.executor.pools[pool_id]
                    max_ram = float(pool.max_ram_pool)
                except Exception:
                    max_ram = None

                for op in getattr(r, "ops", []) or []:
                    ok = (getattr(r, "pipeline_id", None), _op_key(op)) if hasattr(r, "pipeline_id") else None
                    # If ExecutionResult doesn't carry pipeline_id, fall back to op-only hinting
                    if ok is None or ok[0] is None:
                        continue

                    prev = s.op_ram_hint.get(ok, None)
                    base = float(getattr(r, "ram", 0.0) or 0.0)
                    if prev is None:
                        # If we don't know the previous, start from what we just tried (or a safe small baseline)
                        prev = base if base > 0 else 2.0
                    new_hint = prev * float(s.oom_ram_growth)
                    if max_ram is not None:
                        new_hint = min(new_hint, max_ram)

                    s.op_ram_hint[ok] = new_hint
                    s.op_oom_fail_counts[ok] = s.op_oom_fail_counts.get(ok, 0) + 1
            else:
                # Non-OOM failure: mark pipeline dead if we can identify it via ops/pipeline_id
                pid = getattr(r, "pipeline_id", None)
                if pid is not None:
                    s.dead_pipelines.add(pid)
        else:
            # Optional: record mild cpu hint as what was used (helps stability)
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                for op in getattr(r, "ops", []) or []:
                    ok = (pid, _op_key(op))
                    try:
                        used_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                        if used_cpu > 0:
                            s.op_cpu_hint[ok] = used_cpu
                    except Exception:
                        pass

    # 3) Prune completed/dead pipelines from queues (cheap pass)
    for pr, q in list(s.queues.items()):
        if not q:
            continue
        new_q = []
        for p in q:
            if not _pipeline_done_or_dead(p):
                new_q.append(p)
        s.queues[pr] = new_q

    # Early exit if nothing to do
    if not pipelines and not results:
        # Still might have backlog; don't early-exit based solely on inputs
        backlog = any(len(q) > 0 for q in s.queues.values())
        if not backlog:
            return [], []

    suspensions = []  # no preemption in this iteration
    assignments = []

    # 4) Per-pool packing: assign multiple ops per tick; priority-first
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        try:
            remaining_cpu = float(pool.avail_cpu_pool)
            remaining_ram = float(pool.avail_ram_pool)
            max_cpu = float(pool.max_cpu_pool)
            max_ram = float(pool.max_ram_pool)
        except Exception:
            continue

        if remaining_cpu <= 0 or remaining_ram <= 0:
            continue

        reserve_cpu = max_cpu * float(s.soft_reserve_cpu_frac)
        reserve_ram = max_ram * float(s.soft_reserve_ram_frac)

        # Keep scheduling until we can't place anything else in this pool
        # Also guard against infinite loops if queues have only blocked pipelines
        no_progress_iters = 0
        while remaining_cpu > 0 and remaining_ram > 0:
            made_assignment = False

            for pr in _prio_order():
                q = s.queues.get(pr, [])
                if not q:
                    continue

                # Soft reservation: only admit batch if we still have headroom
                if pr == Priority.BATCH_PIPELINE:
                    if remaining_cpu <= reserve_cpu or remaining_ram <= reserve_ram:
                        continue

                p, op_list = _next_ready_op_from_queue(q)
                if p is None or not op_list:
                    continue

                op = op_list[0]
                ok = (p.pipeline_id, _op_key(op))

                # Determine target sizes: hint > default, then cap by per-priority share, then by remaining
                cpu_target = s.op_cpu_hint.get(ok, s.default_cpu.get(pr, 1.0))
                ram_target = s.op_ram_hint.get(ok, s.default_ram.get(pr, 3.0))

                # Cap to avoid a single op monopolizing the pool
                cpu_cap = max_cpu * float(s.max_share_cpu.get(pr, 0.35))
                ram_cap = max_ram * float(s.max_share_ram.get(pr, 0.50))

                cpu_alloc = min(remaining_cpu, max(0.0, min(float(cpu_target), float(cpu_cap))))
                ram_alloc = min(remaining_ram, max(0.0, min(float(ram_target), float(ram_cap))))

                # Ensure we allocate something meaningful; otherwise skip this op for now
                if cpu_alloc <= 0 or ram_alloc <= 0:
                    continue

                # If we're trying to run high priority and are resource-constrained,
                # prefer giving CPU over RAM within remaining bounds (RAM is typically harder OOM constraint).
                # However we must still allocate positive ram.
                assignment = Assignment(
                    ops=op_list,
                    cpu=cpu_alloc,
                    ram=ram_alloc,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(assignment)

                remaining_cpu -= cpu_alloc
                remaining_ram -= ram_alloc
                made_assignment = True
                break  # re-evaluate from highest priority after each placement

            if not made_assignment:
                no_progress_iters += 1
                if no_progress_iters >= 2:
                    break
            else:
                no_progress_iters = 0

    return suspensions, assignments
