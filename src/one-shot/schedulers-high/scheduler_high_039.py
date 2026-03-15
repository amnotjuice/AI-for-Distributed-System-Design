# policy_key: scheduler_high_039
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.107757
# generation_seconds: 142.38
# generated_at: 2026-03-14T04:12:22.009507
@register_scheduler_init(key="scheduler_high_039")
def scheduler_high_039_init(s):
    """
    Priority-aware, latency-improving scheduler.

    Improvements over naive FIFO:
      - Maintains separate queues per priority (QUERY > INTERACTIVE > BATCH).
      - Avoids "one giant assignment" by using priority-based right-sizing (fractions of pool capacity),
        enabling more high-priority work to start sooner.
      - Adds simple headroom reservation so batch work doesn't consume all resources when
        high-priority work is waiting.
      - Adds basic OOM-adaptive RAM scaling (best-effort; depends on ExecutionResult having pipeline_id).
      - Ensures at most one op per pipeline is scheduled per tick (reduces monopolization).
    """
    from collections import deque

    s.pipelines_by_id = {}  # pipeline_id -> Pipeline

    # Per-priority waiting queues (store pipeline_ids) + membership sets to avoid duplicates
    s.wait_q = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.wait_q_set = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Simple adaptive sizing knobs (pipeline-level; coarse but robust)
    s.pipeline_ram_mult = {}  # pipeline_id -> multiplier
    s.pipeline_cpu_mult = {}  # pipeline_id -> multiplier

    # If we detect a non-OOM fatal error, stop retrying the pipeline
    s.pipeline_fatal = set()

    # Policy constants (tuned for "small improvements first")
    s.reserve_frac_cpu = 0.20  # reserve this fraction of pool for high-priority when they are waiting
    s.reserve_frac_ram = 0.20

    # "Right-size" fractions by priority (fractions of pool max; multiplied by *_mult; then clamped)
    s.frac_cpu = {
        Priority.QUERY: 0.55,
        Priority.INTERACTIVE: 0.35,
        Priority.BATCH_PIPELINE: 0.90,
    }
    s.frac_ram = {
        Priority.QUERY: 0.55,
        Priority.INTERACTIVE: 0.35,
        Priority.BATCH_PIPELINE: 0.90,
    }

    # Bounds on adaptive multipliers
    s.max_ram_mult = 16.0
    s.max_cpu_mult = 4.0


@register_scheduler(key="scheduler_high_039")
def scheduler_high_039(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Step function:
      - Enqueue new pipelines into per-priority queues.
      - Process results to update coarse RAM/CPU multipliers (OOM backoff).
      - Schedule ready operators in strict priority order, using fractional sizing and
        reserving headroom for high-priority work.
    """
    def _is_oom(err) -> bool:
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)

    def _get_mult(dct, pid, default=1.0):
        v = dct.get(pid)
        if v is None:
            dct[pid] = default
            return default
        return v

    def _enqueue_pipeline(p: "Pipeline"):
        # Track latest object reference
        s.pipelines_by_id[p.pipeline_id] = p

        # Do not enqueue completed pipelines
        try:
            if p.runtime_status().is_pipeline_successful():
                return
        except Exception:
            # If status call fails, still enqueue to avoid losing work
            pass

        pr = p.priority
        if pr not in s.wait_q:
            # Unknown priority: treat as batch
            pr = Priority.BATCH_PIPELINE

        if p.pipeline_id not in s.wait_q_set[pr]:
            s.wait_q[pr].append(p.pipeline_id)
            s.wait_q_set[pr].add(p.pipeline_id)

        # Initialize multipliers if missing
        _get_mult(s.pipeline_ram_mult, p.pipeline_id, 1.0)
        _get_mult(s.pipeline_cpu_mult, p.pipeline_id, 1.0)

    def _pop_left(prio):
        pid = s.wait_q[prio].popleft()
        s.wait_q_set[prio].discard(pid)
        return pid

    def _requeue(prio, pid):
        if pid not in s.wait_q_set[prio]:
            s.wait_q[prio].append(pid)
            s.wait_q_set[prio].add(pid)

    def _calc_desired(pool, prio, pid):
        # Fraction of pool max, adjusted by multipliers
        cpu_mult = _get_mult(s.pipeline_cpu_mult, pid, 1.0)
        ram_mult = _get_mult(s.pipeline_ram_mult, pid, 1.0)

        desired_cpu = float(pool.max_cpu_pool) * float(s.frac_cpu.get(prio, 0.5)) * float(cpu_mult)
        desired_ram = float(pool.max_ram_pool) * float(s.frac_ram.get(prio, 0.5)) * float(ram_mult)

        # Keep desired at least 1 unit (assuming integer-like resources)
        desired_cpu = max(1.0, desired_cpu)
        desired_ram = max(1.0, desired_ram)
        return desired_cpu, desired_ram

    def _pick_pool_for(prio, pid, pool_avail_cpu, pool_avail_ram, reserve_cpu, reserve_ram, high_prio_waiting):
        # Choose a pool that can fit the (clamped) allocation.
        # For high-priority: prefer most headroom (start ASAP).
        # For batch: prefer best-fit (leave largest contiguous headroom elsewhere).
        best_pool = None
        best_score = None
        best_alloc = None

        for i in range(s.executor.num_pools):
            pool = s.executor.pools[i]

            avail_cpu = pool_avail_cpu[i]
            avail_ram = pool_avail_ram[i]
            if avail_cpu <= 0 or avail_ram <= 0:
                continue

            # If batch and high-priority is waiting anywhere, enforce reservation.
            eff_cpu = avail_cpu
            eff_ram = avail_ram
            if prio == Priority.BATCH_PIPELINE and high_prio_waiting:
                eff_cpu = max(0.0, avail_cpu - reserve_cpu[i])
                eff_ram = max(0.0, avail_ram - reserve_ram[i])

            if eff_cpu <= 0 or eff_ram <= 0:
                continue

            desired_cpu, desired_ram = _calc_desired(pool, prio, pid)

            # Clamp allocation to effective available
            alloc_cpu = min(eff_cpu, desired_cpu)
            alloc_ram = min(eff_ram, desired_ram)

            # Need strictly positive allocations
            if alloc_cpu <= 0 or alloc_ram <= 0:
                continue

            # Score
            if prio in (Priority.QUERY, Priority.INTERACTIVE):
                # Favor more headroom to reduce queueing/packing friction
                score = (eff_cpu / max(1.0, pool.max_cpu_pool)) + (eff_ram / max(1.0, pool.max_ram_pool))
                if best_score is None or score > best_score:
                    best_pool, best_score, best_alloc = i, score, (alloc_cpu, alloc_ram)
            else:
                # Best-fit: minimize leftover to reduce fragmentation
                leftover = (eff_cpu - alloc_cpu) + (eff_ram - alloc_ram)
                score = -leftover
                if best_score is None or score > best_score:
                    best_pool, best_score, best_alloc = i, score, (alloc_cpu, alloc_ram)

        return best_pool, best_alloc

    # Enqueue newly arrived pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # If nothing changed externally, we can early-exit (queueing/resources won't change without results)
    if not pipelines and not results:
        return [], []

    # Process results to adapt (best-effort)
    for r in results:
        if not getattr(r, "failed", None):
            continue
        try:
            is_failed = r.failed()
        except Exception:
            is_failed = False

        if not is_failed:
            continue

        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            # Can't attribute failure; skip adaptation
            continue

        if _is_oom(getattr(r, "error", None)):
            # Increase RAM multiplier to avoid repeated OOM
            cur = _get_mult(s.pipeline_ram_mult, pid, 1.0)
            s.pipeline_ram_mult[pid] = min(float(cur) * 2.0, float(s.max_ram_mult))
        else:
            # Treat as fatal (avoid infinite retries)
            s.pipeline_fatal.add(pid)

    # Snapshot pool availability and compute reservations
    pool_avail_cpu = []
    pool_avail_ram = []
    reserve_cpu = []
    reserve_ram = []
    for i in range(s.executor.num_pools):
        pool = s.executor.pools[i]
        pool_avail_cpu.append(float(pool.avail_cpu_pool))
        pool_avail_ram.append(float(pool.avail_ram_pool))
        reserve_cpu.append(float(pool.max_cpu_pool) * float(s.reserve_frac_cpu))
        reserve_ram.append(float(pool.max_ram_pool) * float(s.reserve_frac_ram))

    # Determine whether to reserve headroom for high-priority work
    high_prio_waiting = (len(s.wait_q[Priority.QUERY]) > 0) or (len(s.wait_q[Priority.INTERACTIVE]) > 0)

    suspensions: List["Suspend"] = []  # no preemption in this incremental version
    assignments: List["Assignment"] = []

    # One-op-per-pipeline-per-tick to reduce monopolization and improve latency fairness
    scheduled_this_tick = set()

    # Priority order (strict)
    prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Consider each queued pipeline at most once per priority per tick (prevents infinite rotation)
    for prio in prio_order:
        q_len = len(s.wait_q[prio])
        for _ in range(q_len):
            pid = _pop_left(prio)
            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue

            if pid in s.pipeline_fatal:
                # Drop permanently
                continue

            # Skip if already scheduled this tick (avoid multiple concurrent ops from same pipeline)
            if pid in scheduled_this_tick:
                _requeue(prio, pid)
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            # Get a single ready op
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; keep waiting
                _requeue(prio, pid)
                continue

            # Choose a pool and allocation
            pool_id, alloc = _pick_pool_for(
                prio, pid,
                pool_avail_cpu, pool_avail_ram,
                reserve_cpu, reserve_ram,
                high_prio_waiting
            )
            if pool_id is None or alloc is None:
                # No capacity right now; keep in queue
                _requeue(prio, pid)
                continue

            cpu, ram = alloc

            # Final clamp to absolute available (defensive)
            cpu = min(cpu, pool_avail_cpu[pool_id])
            ram = min(ram, pool_avail_ram[pool_id])

            if cpu <= 0 or ram <= 0:
                _requeue(prio, pid)
                continue

            assignment = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Update local availability
            pool_avail_cpu[pool_id] -= cpu
            pool_avail_ram[pool_id] -= ram

            scheduled_this_tick.add(pid)

            # Requeue pipeline for subsequent ops (if any remain)
            _requeue(prio, pid)

    return suspensions, assignments
