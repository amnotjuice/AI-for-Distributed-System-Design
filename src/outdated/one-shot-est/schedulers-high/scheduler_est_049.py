# policy_key: scheduler_est_049
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.099056
# generation_seconds: 145.13
# generated_at: 2026-03-31T19:55:08.243838
@register_scheduler_init(key="scheduler_est_049")
def scheduler_est_049_init(s):
    """Priority-aware scheduler with small, safe improvements over naive FIFO.

    Main ideas (incremental improvements):
    1) Priority-aware ordering: schedule QUERY/INTERACTIVE before BATCH.
    2) Memory sizing via op.estimate.mem_peak_gb (+ small safety buffer).
    3) OOM-aware retries: if an op OOMs, increase its per-op RAM multiplier and retry.
    4) Optional headroom protection:
       - If multiple pools: prefer pool 0 for interactive/query; batch prefers other pools.
       - If single pool: reserve a fraction of CPU/RAM from being consumed by batch when high-priority work exists.
    5) Best-effort (guarded) preemption: if we can introspect running containers, suspend low-priority work to create headroom.
    """
    s.tick = 0

    # Queue/state tracking
    s.waiting_queue = []            # FIFO of pipeline_ids (for stable ordering within same effective priority)
    s.pipeline_by_id = {}           # pipeline_id -> Pipeline
    s.enqueued_tick = {}            # pipeline_id -> tick
    s.terminal_failed_pipelines = set()  # pipeline_ids that failed with non-OOM (if pipeline_id is available from results)

    # OOM adaptation (per-op)
    s.op_mem_mult = {}              # op_key -> multiplier on estimate.mem_peak_gb
    s.op_mem_floor = {}             # op_key -> absolute minimum RAM (GB) learned from failures
    s.op_oom_attempts = {}          # op_key -> attempts count
    s.oom_op_keys = set()           # ops known to have OOM'd (allowed to retry from FAILED)

    # Tunables (kept simple on purpose)
    s.base_mem_mult = 1.10          # small safety buffer on estimates
    s.oom_backoff_mult = 1.50       # multiply RAM after each OOM
    s.max_oom_retries = 3

    s.ram_floor_gb = 0.25           # minimum RAM allocation (GB) to avoid tiny containers
    s.cpu_floor = 1                 # minimum CPU allocation

    # Headroom protection (only applies strongly when high-priority work is waiting)
    s.high_headroom_frac_cpu = 0.25
    s.high_headroom_frac_ram = 0.25

    # Aging to reduce starvation: every N ticks of waiting reduces effective priority rank by 1 (to a floor of top priority).
    s.aging_quantum_ticks = 50

    # Spillover: if high-priority waits too long and its preferred pool is busy, allow spillover to other pools.
    s.spillover_age_ticks = 30


def _p_rank(priority):
    # Lower is better
    if hasattr(Priority, "QUERY") and priority == Priority.QUERY:
        return 0
    if hasattr(Priority, "INTERACTIVE") and priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE (or anything else)


def _is_high_pri(priority):
    return (hasattr(Priority, "QUERY") and priority == Priority.QUERY) or (
        hasattr(Priority, "INTERACTIVE") and priority == Priority.INTERACTIVE
    )


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)


def _get_mem_est_gb(op):
    # Prefer op.estimate.mem_peak_gb if present; fall back to 0 (unknown).
    est = getattr(op, "estimate", None)
    if est is not None:
        v = getattr(est, "mem_peak_gb", None)
        if v is not None:
            try:
                return float(v)
            except Exception:
                return 0.0
    return 0.0


def _op_key(pipeline_id, op):
    # Try to build a stable identifier for the operator within a pipeline.
    for attr in ("op_id", "operator_id", "id", "name"):
        v = getattr(op, attr, None)
        if v is not None:
            return (pipeline_id, attr, v)
    # Last-resort: use object repr (may not be stable, but better than crashing)
    return (pipeline_id, "repr", repr(op))


def _get_running_containers(pool):
    # Best-effort introspection; policy still works if we can't find any.
    for attr in ("running_containers", "active_containers", "containers", "running"):
        cs = getattr(pool, attr, None)
        if cs:
            return cs
    return []


def _c_get(c, name, default=None):
    if isinstance(c, dict):
        return c.get(name, default)
    return getattr(c, name, default)


def _container_priority_rank(c):
    pr = _c_get(c, "priority", None)
    if pr is None:
        return 2
    return _p_rank(pr)


def _container_id(c):
    return _c_get(c, "container_id", _c_get(c, "id", None))


def _container_cpu(c):
    v = _c_get(c, "cpu", None)
    try:
        return float(v) if v is not None else 0.0
    except Exception:
        return 0.0


def _container_ram(c):
    v = _c_get(c, "ram", None)
    try:
        return float(v) if v is not None else 0.0
    except Exception:
        return 0.0


def _maybe_preempt_for_headroom(s, pool_id, need_cpu, need_ram):
    """Try to suspend low-priority running containers to free headroom.

    This is fully guarded: if the executor/pool doesn't expose running containers, it does nothing.
    """
    suspensions = []
    pool = s.executor.pools[pool_id]

    # If we already have enough available headroom, do nothing.
    try:
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
    except Exception:
        return suspensions

    if avail_cpu >= need_cpu and avail_ram >= need_ram:
        return suspensions

    running = _get_running_containers(pool)
    if not running:
        return suspensions

    # Only consider preempting non-high-priority work first.
    candidates = []
    for c in running:
        pr = _container_priority_rank(c)
        cid = _container_id(c)
        if cid is None:
            continue
        # Preempt batch first (rank 2), then interactive (rank 1) only if desperate.
        candidates.append((pr, _container_cpu(c) + _container_ram(c), c))

    # Sort: worst priority (largest rank) first, then largest resource footprint.
    candidates.sort(key=lambda x: (-x[0], -x[1]))

    freed_cpu = 0.0
    freed_ram = 0.0
    for pr, _, c in candidates:
        # Avoid preempting QUERY work entirely if possible.
        if pr <= 0:
            continue
        cid = _container_id(c)
        if cid is None:
            continue
        suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
        freed_cpu += _container_cpu(c)
        freed_ram += _container_ram(c)
        if (avail_cpu + freed_cpu) >= need_cpu and (avail_ram + freed_ram) >= need_ram:
            break

    return suspensions


def _cpu_target_for(priority, pool, avail_cpu):
    # Give more CPU to high-priority for latency; be more conservative for batch.
    max_cpu = float(getattr(pool, "max_cpu_pool", avail_cpu))
    if _is_high_pri(priority):
        target = min(avail_cpu, max(1.0, 0.50 * max_cpu))
    else:
        target = min(avail_cpu, max(1.0, 0.25 * max_cpu))
    return target


@register_scheduler(key="scheduler_est_049")
def scheduler_est_049(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """Priority-aware, estimate-driven scheduler with OOM backoff and headroom protection."""
    s.tick += 1

    # --- Incorporate new pipelines (stable FIFO order by arrival) ---
    for p in pipelines:
        if p.pipeline_id not in s.pipeline_by_id:
            s.pipeline_by_id[p.pipeline_id] = p
            s.waiting_queue.append(p.pipeline_id)
            s.enqueued_tick[p.pipeline_id] = s.tick

    # --- Learn from execution results (OOM backoff, success cleanup, non-OOM terminal failures) ---
    for r in results:
        # Attempt to find pipeline_id if the simulator provides it (guarded).
        r_pid = getattr(r, "pipeline_id", None)

        if r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                for op in getattr(r, "ops", []) or []:
                    pid_for_key = r_pid if r_pid is not None else getattr(op, "pipeline_id", None)
                    pid_for_key = pid_for_key if pid_for_key is not None else "unknown_pipeline"
                    k = _op_key(pid_for_key, op)

                    s.oom_op_keys.add(k)
                    s.op_oom_attempts[k] = s.op_oom_attempts.get(k, 0) + 1

                    # Increase multiplier and absolute floor.
                    prev_mult = s.op_mem_mult.get(k, s.base_mem_mult)
                    s.op_mem_mult[k] = max(prev_mult * s.oom_backoff_mult, s.base_mem_mult)

                    # If we know how much RAM we allocated when it failed, use it as a floor.
                    try:
                        allocated_ram = float(getattr(r, "ram", 0.0) or 0.0)
                    except Exception:
                        allocated_ram = 0.0
                    if allocated_ram > 0:
                        s.op_mem_floor[k] = max(s.op_mem_floor.get(k, 0.0), allocated_ram * s.oom_backoff_mult)
            else:
                # Non-OOM failures: do not keep retrying indefinitely if we can identify the pipeline.
                if r_pid is not None:
                    s.terminal_failed_pipelines.add(r_pid)
        else:
            # On success, clear OOM bookkeeping for those ops.
            for op in getattr(r, "ops", []) or []:
                pid_for_key = r_pid if r_pid is not None else getattr(op, "pipeline_id", None)
                pid_for_key = pid_for_key if pid_for_key is not None else "unknown_pipeline"
                k = _op_key(pid_for_key, op)
                if k in s.oom_op_keys:
                    s.oom_op_keys.discard(k)
                    s.op_oom_attempts.pop(k, None)
                    # Keep learned floor/mult for future similar ops (do not delete).

    # --- Build list of candidate (ready) ops: one per pipeline per tick (keeps fairness + progress) ---
    candidates = []
    prio_waiting = {"high": 0, "low": 0}

    # We will also compact the queue by dropping completed/terminal pipelines.
    new_queue = []
    for pid in s.waiting_queue:
        p = s.pipeline_by_id.get(pid, None)
        if p is None:
            continue

        if pid in s.terminal_failed_pipelines:
            # Drop terminal failures.
            continue

        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue

        # Pick a single assignable op (parents complete) to advance the DAG.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            new_queue.append(pid)
            continue

        op = op_list[0]

        # If this op is FAILED, only retry if we observed it as OOM (and under retry limit).
        op_state = getattr(op, "state", None)
        if op_state == OperatorState.FAILED:
            k = _op_key(pid, op)
            if k not in s.oom_op_keys:
                # Treat as terminal failure (best effort) to avoid infinite retry loops.
                s.terminal_failed_pipelines.add(pid)
                continue
            if s.op_oom_attempts.get(k, 0) > s.max_oom_retries:
                s.terminal_failed_pipelines.add(pid)
                continue

        age = s.tick - s.enqueued_tick.get(pid, s.tick)
        base_rank = _p_rank(p.priority)
        age_boost = age // max(1, int(s.aging_quantum_ticks))
        eff_rank = max(0, base_rank - age_boost)  # aging improves effective priority for long-waiting work

        candidates.append((eff_rank, base_rank, s.enqueued_tick.get(pid, s.tick), pid, p, op, age))
        new_queue.append(pid)

        if _is_high_pri(p.priority):
            prio_waiting["high"] += 1
        else:
            prio_waiting["low"] += 1

    s.waiting_queue = new_queue

    # No ready work
    if not candidates:
        return [], []

    # Sort by effective priority, then base priority, then FIFO arrival.
    candidates.sort(key=lambda x: (x[0], x[1], x[2]))

    # --- Pool strategy ---
    num_pools = s.executor.num_pools
    interactive_pool = 0 if num_pools > 1 else 0

    # Snapshot available resources locally as we build multiple assignments.
    avail_cpu = {}
    avail_ram = {}
    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        try:
            avail_cpu[pool_id] = float(pool.avail_cpu_pool)
            avail_ram[pool_id] = float(pool.avail_ram_pool)
        except Exception:
            avail_cpu[pool_id] = 0.0
            avail_ram[pool_id] = 0.0

    suspensions = []
    assignments = []

    # --- Optional preemption: create headroom if high-priority exists and pool is clogged ---
    if prio_waiting["high"] > 0:
        # Determine a conservative headroom target.
        pool = s.executor.pools[interactive_pool]
        try:
            max_cpu = float(pool.max_cpu_pool)
            max_ram = float(pool.max_ram_pool)
        except Exception:
            max_cpu = avail_cpu.get(interactive_pool, 0.0)
            max_ram = avail_ram.get(interactive_pool, 0.0)

        headroom_cpu = max(1.0, s.high_headroom_frac_cpu * max_cpu)
        headroom_ram = max(s.ram_floor_gb, s.high_headroom_frac_ram * max_ram)

        # If there is a high-priority op that cannot currently fit, try preempting to reach headroom.
        suspensions.extend(_maybe_preempt_for_headroom(s, interactive_pool, headroom_cpu, headroom_ram))

    # --- Admission/placement: greedy by priority, with estimate-driven sizing and headroom protection for batch ---
    for eff_rank, base_rank, enq, pid, p, op, age in candidates:
        # Don't schedule pipelines that became terminal in this tick due to result processing.
        if pid in s.terminal_failed_pipelines:
            continue

        # Pool preference:
        if num_pools > 1:
            if _is_high_pri(p.priority):
                # Prefer interactive pool; allow spillover if waiting too long.
                if age >= s.spillover_age_ticks:
                    pool_order = [interactive_pool] + [i for i in range(num_pools) if i != interactive_pool]
                else:
                    pool_order = [interactive_pool]
            else:
                # Batch prefers non-interactive pools; fallback if only one or none have capacity.
                others = [i for i in range(num_pools) if i != interactive_pool]
                pool_order = others if others else [interactive_pool]
        else:
            pool_order = [0]

        # Compute requested RAM from estimate with OOM-aware backoff.
        k = _op_key(pid, op)
        est_gb = _get_mem_est_gb(op)
        mult = s.op_mem_mult.get(k, s.base_mem_mult)
        learned_floor = s.op_mem_floor.get(k, 0.0)
        ram_req = max(s.ram_floor_gb, est_gb * mult, learned_floor)

        placed = False
        for pool_id in pool_order:
            pool = s.executor.pools[pool_id]
            if avail_cpu.get(pool_id, 0.0) <= 0.0 or avail_ram.get(pool_id, 0.0) <= 0.0:
                continue

            # Cap request by pool limits.
            try:
                pool_max_ram = float(pool.max_ram_pool)
            except Exception:
                pool_max_ram = avail_ram[pool_id]
            ram_req_capped = min(ram_req, pool_max_ram)

            # For high priority, give it what it needs; for batch, enforce headroom when high-priority exists (single-pool case).
            effective_avail_cpu = avail_cpu[pool_id]
            effective_avail_ram = avail_ram[pool_id]

            if num_pools == 1 and prio_waiting["high"] > 0 and not _is_high_pri(p.priority):
                # Protect latency: do not allow batch to consume the last headroom in a shared pool.
                try:
                    max_cpu = float(pool.max_cpu_pool)
                    max_ram = float(pool.max_ram_pool)
                except Exception:
                    max_cpu = effective_avail_cpu
                    max_ram = effective_avail_ram
                reserve_cpu = s.high_headroom_frac_cpu * max_cpu
                reserve_ram = s.high_headroom_frac_ram * max_ram
                effective_avail_cpu = max(0.0, effective_avail_cpu - reserve_cpu)
                effective_avail_ram = max(0.0, effective_avail_ram - reserve_ram)

            if effective_avail_cpu < float(s.cpu_floor) or effective_avail_ram < ram_req_capped:
                continue

            cpu_alloc = _cpu_target_for(p.priority, pool, effective_avail_cpu)
            cpu_alloc = max(float(s.cpu_floor), min(cpu_alloc, effective_avail_cpu))

            # Allocate RAM: just enough for batch; small extra for high-priority if capacity exists.
            if _is_high_pri(p.priority):
                ram_alloc = min(effective_avail_ram, max(ram_req_capped, ram_req_capped * 1.05))
            else:
                ram_alloc = min(effective_avail_ram, ram_req_capped)

            if cpu_alloc <= 0.0 or ram_alloc < ram_req_capped:
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_alloc,
                    ram=ram_alloc,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            avail_cpu[pool_id] -= cpu_alloc
            avail_ram[pool_id] -= ram_alloc
            placed = True
            break

        # If not placed, leave it for a future tick (possibly after completions/preemptions).
        if not placed:
            continue

    return suspensions, assignments
