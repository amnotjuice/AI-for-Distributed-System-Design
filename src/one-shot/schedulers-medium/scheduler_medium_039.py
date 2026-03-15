# policy_key: scheduler_medium_039
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.089277
# generation_seconds: 104.80
# generated_at: 2026-03-14T03:10:20.183874
@register_scheduler_init(key="scheduler_medium_039")
def scheduler_medium_039_init(s):
    """Priority-aware, right-sized, OOM-adaptive scheduler.

    Improvements over naive FIFO:
      1) Priority ordering: INTERACTIVE > QUERY > BATCH_PIPELINE.
      2) Avoid head-of-line blocking: only queue pipelines that have *ready* ops (parents complete).
      3) Right-size per-op allocations: cap batch to smaller fractions to improve concurrency/latency.
      4) Protect high priority: reserve a fraction of each pool for high-priority ready work.
      5) OOM adaptation: if an op OOMs, increase its RAM multiplier for future retries.

    Notes:
      - No preemption (Suspend) to keep behavior safe/simple without relying on executor introspection.
      - Pipelines are re-discovered each tick by scanning runtime_status() (deterministic + robust).
    """
    s.all_pipelines = {}  # pipeline_id -> Pipeline
    s.op_ram_multiplier = {}  # op_key -> multiplier (>=1.0); bumped on OOM
    s.op_last_failed_ram = {}  # op_key -> last RAM that OOM'd (best-effort)
    s.max_ram_multiplier = 16.0

    # Packing / concurrency knobs
    s.max_assignments_per_pool = 4
    s.min_cpu = 1.0
    s.min_ram = 1.0

    # Reserve capacity for high-priority work when any high-priority is ready
    s.reserve_frac_cpu_for_hp = 0.25
    s.reserve_frac_ram_for_hp = 0.25


def _op_key(op):
    # Best-effort stable key across simulator objects.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return (attr, v)
            except Exception:
                pass
    try:
        return ("repr", repr(op))
    except Exception:
        return ("fallback", str(type(op)))


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _priority_order():
    return [Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE]


def _prio_is_high(prio):
    return prio in (Priority.INTERACTIVE, Priority.QUERY)


def _targets_for_priority(pool, prio):
    # Target fractions of *pool max*; final allocation is also limited by current availability.
    # High-priority gets more resources to reduce latency; batch is capped to improve concurrency.
    if prio == Priority.INTERACTIVE:
        return 0.75, 0.75  # (cpu_frac, ram_frac)
    if prio == Priority.QUERY:
        return 0.50, 0.50
    return 0.25, 0.25  # BATCH_PIPELINE


@register_scheduler(key="scheduler_medium_039")
def scheduler_medium_039(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    # ---- Incorporate new pipelines ----
    for p in pipelines:
        s.all_pipelines[p.pipeline_id] = p

    # ---- Update OOM learning from recent results ----
    for r in (results or []):
        if getattr(r, "failed", None) and r.failed() and _is_oom_error(getattr(r, "error", None)):
            for op in (getattr(r, "ops", None) or []):
                k = _op_key(op)
                cur = s.op_ram_multiplier.get(k, 1.0)
                s.op_ram_multiplier[k] = min(cur * 2.0, s.max_ram_multiplier)
                try:
                    if getattr(r, "ram", None) is not None:
                        s.op_last_failed_ram[k] = max(float(r.ram), float(s.op_last_failed_ram.get(k, 0.0)))
                except Exception:
                    # If RAM isn't numeric, ignore.
                    pass

    # ---- Build ready queues (only pipelines with assignable ops whose parents are complete) ----
    ready_queues = {Priority.INTERACTIVE: [], Priority.QUERY: [], Priority.BATCH_PIPELINE: []}
    next_op_for_pid = {}  # pid -> [op]

    completed_pids = []
    hp_ready_count = 0

    for pid, p in list(s.all_pipelines.items()):
        status = p.runtime_status()

        if status.is_pipeline_successful():
            completed_pids.append(pid)
            continue

        # Only queue if there is at least one operator ready to be assigned.
        ops_ready = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops_ready:
            continue

        next_op_for_pid[pid] = ops_ready[:1]
        ready_queues[p.priority].append(pid)
        if _prio_is_high(p.priority):
            hp_ready_count += 1

    for pid in completed_pids:
        s.all_pipelines.pop(pid, None)

    # Early exit: nothing to do
    if not next_op_for_pid:
        return [], []

    # ---- Scheduling: priority-first with high-priority reservation ----
    suspensions = []
    assignments = []

    # Avoid scheduling multiple ops from the same pipeline in a single tick.
    scheduled_pids = set()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_frac_cpu_for_hp) if hp_ready_count > 0 else 0.0
        reserve_ram = float(pool.max_ram_pool) * float(s.reserve_frac_ram_for_hp) if hp_ready_count > 0 else 0.0

        scheduled_here = 0
        while scheduled_here < s.max_assignments_per_pool and avail_cpu >= s.min_cpu and avail_ram >= s.min_ram:
            picked_pid = None
            picked_prio = None

            # Pick highest-priority ready pipeline that we haven't scheduled yet this tick.
            for prio in _priority_order():
                q = ready_queues.get(prio, [])
                while q and q[0] in scheduled_pids:
                    q.pop(0)
                if q:
                    picked_pid = q.pop(0)
                    picked_prio = prio
                    break

            if picked_pid is None:
                break  # nothing left for this pool

            pipeline = s.all_pipelines.get(picked_pid)
            op_list = next_op_for_pid.get(picked_pid)
            if pipeline is None or not op_list:
                # Pipeline finished/changed since we built queues; skip.
                continue

            # Enforce reservation only against batch: keep headroom for high-priority.
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram
            if picked_prio == Priority.BATCH_PIPELINE and hp_ready_count > 0:
                eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                eff_avail_ram = max(0.0, avail_ram - reserve_ram)

            if eff_avail_cpu < s.min_cpu or eff_avail_ram < s.min_ram:
                # Not enough non-reserved capacity for this batch op; try other work.
                if picked_prio == Priority.BATCH_PIPELINE:
                    # Put it back at end and stop scheduling batch in this pool for now.
                    ready_queues[Priority.BATCH_PIPELINE].append(picked_pid)
                    break
                # For high priority, proceed using full availability (reservation doesn't apply).
                eff_avail_cpu = avail_cpu
                eff_avail_ram = avail_ram

            cpu_frac, ram_frac = _targets_for_priority(pool, picked_prio)

            # Base targets from pool max; bounded by effective availability.
            cpu_target = max(s.min_cpu, float(pool.max_cpu_pool) * float(cpu_frac))
            ram_target = max(s.min_ram, float(pool.max_ram_pool) * float(ram_frac))

            cpu = min(eff_avail_cpu, cpu_target)
            ram = min(eff_avail_ram, ram_target)

            # Apply OOM-learned multiplier for this operator.
            try:
                k = _op_key(op_list[0])
                mult = float(s.op_ram_multiplier.get(k, 1.0))
                if mult > 1.0:
                    # Prefer to exceed the last known failing RAM * multiplier when possible.
                    last_fail = float(s.op_last_failed_ram.get(k, 0.0))
                    ram = max(ram, min(eff_avail_ram, last_fail * mult))
                ram = min(eff_avail_ram, ram * mult)
            except Exception:
                pass

            # Final clamps (also ensure positive)
            cpu = max(s.min_cpu, min(cpu, eff_avail_cpu))
            ram = max(s.min_ram, min(ram, eff_avail_ram))

            # If still not feasible, give up on this pool iteration.
            if cpu > eff_avail_cpu or ram > eff_avail_ram:
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            scheduled_pids.add(picked_pid)
            avail_cpu -= cpu
            avail_ram -= ram
            scheduled_here += 1

    return suspensions, assignments
