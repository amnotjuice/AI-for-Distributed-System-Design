# policy_key: scheduler_high_043
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.141430
# generation_seconds: 142.55
# generated_at: 2026-03-14T04:22:45.289636
@register_scheduler_init(key="scheduler_high_043")
def scheduler_high_043_init(s):
    """
    Priority-aware, latency-improving scheduler (small, incremental improvements over naive FIFO).

    Key ideas:
      1) Maintain separate FIFO queues per priority (QUERY/INTERACTIVE before BATCH).
      2) Avoid "give the next op the whole pool" (which harms latency) by using per-op CPU/RAM slices.
      3) Simple OOM-aware RAM autotuning: on OOM, retry the same op with higher RAM next time.
      4) Basic fairness: reserve a small share for batch so it is not starved indefinitely.
      5) Best-effort preemption hook (only if the simulator exposes running containers on pools).
    """
    s.ticks = 0

    # Per-priority FIFO queues storing pipeline_ids; pipelines stored in s.pipelines_by_id.
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.pipelines_by_id = {}

    # Autotuning / safety state, keyed by operator identity (id(op) is stable for the op object).
    s.op_ram_hint = {}          # key -> suggested RAM for next run
    s.op_fail_count = {}        # key -> consecutive non-OOM failures
    s.op_blocked = set()        # keys that exceeded retry budget (non-OOM), so we stop scheduling them
    s.op_to_pipeline_id = {}    # key -> pipeline_id (so results can trigger fast requeue)

    # Retry / sizing knobs
    s.min_cpu = 1.0
    s.min_ram = 0.25  # small absolute floor; units depend on simulator configuration
    s.max_non_oom_retries = 2

    # Slicing fractions (keep modest to allow concurrency; better p95 for interactive)
    s.cpu_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.ram_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Fairness: reserve these fractions for batch if batch is waiting
    s.batch_reserve_cpu_frac = 0.10
    s.batch_reserve_ram_frac = 0.10

    # Enable best-effort preemption if pool exposes running containers (introspected at runtime)
    s.enable_preemption = True


def _op_key(op):
    # Prefer stable logical ids if present, otherwise fall back to object identity.
    return getattr(op, "operator_id", getattr(op, "op_id", id(op)))


def _is_oom_error(err):
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _iter_pipeline_ops(pipeline):
    # pipeline.values may be list-like or dict-like; keep it defensive.
    vals = getattr(pipeline, "values", None)
    if vals is None:
        return []
    try:
        if isinstance(vals, dict):
            return list(vals.values())
        return list(vals)
    except Exception:
        return []


def _enqueue_pipeline(s, pipeline, front=False):
    # Ensure we track pipeline and its operators for result->pipeline correlation.
    pid = pipeline.pipeline_id
    s.pipelines_by_id[pid] = pipeline

    for op in _iter_pipeline_ops(pipeline):
        s.op_to_pipeline_id[_op_key(op)] = pid

    q = s.queues[pipeline.priority]
    if pid in q:
        # Keep FIFO but allow "move to front" behavior on OOM by removing and reinserting.
        q.remove(pid)
    if front:
        q.insert(0, pid)
    else:
        q.append(pid)


def _drop_pipeline_if_done(s, pipeline):
    # Remove pipeline from bookkeeping if completed successfully.
    try:
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            pid = pipeline.pipeline_id
            # Remove from queue (if present) and registry
            q = s.queues[pipeline.priority]
            if pid in q:
                q.remove(pid)
            if pid in s.pipelines_by_id:
                del s.pipelines_by_id[pid]
            return True
    except Exception:
        # If status calls fail for some reason, keep the pipeline in the system.
        pass
    return False


def _get_next_ready_op_from_queue(s, priority, assigned_pids):
    """
    FIFO scan within a priority queue:
      - rotates non-ready pipelines to the back
      - returns (pipeline, op) for the first ready op, while keeping pipeline in queue
    """
    q = s.queues[priority]
    n = len(q)
    for _ in range(n):
        pid = q.pop(0)
        pipeline = s.pipelines_by_id.get(pid)
        if pipeline is None:
            continue

        # Drop completed pipelines quickly.
        if _drop_pipeline_if_done(s, pipeline):
            continue

        # Enforce "at most one op per pipeline per scheduler tick" for simplicity/latency isolation.
        if pid in assigned_pids:
            q.append(pid)
            continue

        status = pipeline.runtime_status()
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            q.append(pid)
            continue

        op = op_list[0]
        k = _op_key(op)
        if k in s.op_blocked:
            # Pipeline might be stuck; rotate to avoid head-of-line blocking.
            q.append(pid)
            continue

        # Keep pipeline enqueued (rotate) even when selected.
        q.append(pid)
        return pipeline, op

    return None, None


def _pool_headroom_score(pool):
    # Higher means more headroom. Use both CPU and RAM, normalized.
    try:
        cpu = float(pool.avail_cpu_pool) / max(1e-9, float(pool.max_cpu_pool))
        ram = float(pool.avail_ram_pool) / max(1e-9, float(pool.max_ram_pool))
        return cpu + ram
    except Exception:
        return 0.0


def _get_pool_containers_best_effort(pool):
    """
    Try to extract running containers from the pool. Simulator implementations may differ.
    Returns list of container-like objects, or [] if not available.
    """
    # Common patterns: pool.containers (dict), pool.running_containers (list), pool.active (dict/list)
    for attr in ("containers", "running_containers", "active_containers", "active", "live_containers"):
        obj = getattr(pool, attr, None)
        if obj is None:
            continue
        try:
            if isinstance(obj, dict):
                return list(obj.values())
            return list(obj)
        except Exception:
            continue
    return []


def _container_fields_best_effort(container):
    """
    Extract (container_id, priority, cpu, ram, state) as best as possible.
    Returns (cid, prio, cpu, ram, state) where values may be None.
    """
    cid = getattr(container, "container_id", None)
    prio = getattr(container, "priority", getattr(container, "assignment_priority", None))
    cpu = getattr(container, "cpu", getattr(container, "allocated_cpu", None))
    ram = getattr(container, "ram", getattr(container, "allocated_ram", None))
    state = getattr(container, "state", None)
    return cid, prio, cpu, ram, state


def _maybe_preempt_for_high_priority(s, pool, pool_id, need_cpu, need_ram):
    """
    Best-effort preemption:
      - If we can see running BATCH containers, suspend them to free resources.
      - Only preempt batch, never preempt QUERY/INTERACTIVE.
    """
    suspensions = []
    if not s.enable_preemption:
        return suspensions, 0.0, 0.0

    freed_cpu = 0.0
    freed_ram = 0.0

    containers = _get_pool_containers_best_effort(pool)
    if not containers:
        return suspensions, freed_cpu, freed_ram

    # Choose batch containers first; within them, suspend larger ones first to reduce churn.
    batch = []
    for c in containers:
        cid, prio, cpu, ram, state = _container_fields_best_effort(c)
        if cid is None:
            continue
        if prio != Priority.BATCH_PIPELINE:
            continue
        # If a state exists, only preempt if it looks runnable.
        if state is not None and str(state) not in ("RUNNING", "ASSIGNED"):
            continue
        batch.append((cid, float(cpu) if cpu is not None else 0.0, float(ram) if ram is not None else 0.0))

    batch.sort(key=lambda x: (x[1] + x[2]), reverse=True)

    for cid, ccpu, cram in batch:
        if freed_cpu >= need_cpu and freed_ram >= need_ram:
            break
        suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
        freed_cpu += max(0.0, ccpu)
        freed_ram += max(0.0, cram)

    return suspensions, freed_cpu, freed_ram


def _compute_allocation(s, pool, avail_cpu, avail_ram, priority, op):
    """
    Choose a conservative per-op slice:
      - Cap by fractions of pool max to preserve concurrency
      - Respect any learned RAM hint and any op-declared minimum RAM if exposed
    """
    # Start from fractions of pool capacity (not the currently-available amount).
    cpu_cap = max(s.min_cpu, float(pool.max_cpu_pool) * float(s.cpu_frac.get(priority, 0.25)))
    ram_cap = max(s.min_ram, float(pool.max_ram_pool) * float(s.ram_frac.get(priority, 0.25)))

    # If the system is idle (lots of resources) and this is high priority, we can be more aggressive.
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        # If we have plenty of available resources, allow up to all available.
        cpu_cap = min(float(pool.max_cpu_pool), max(cpu_cap, float(avail_cpu)))
        ram_cap = min(float(pool.max_ram_pool), max(ram_cap, float(avail_ram)))

    cpu = min(float(avail_cpu), float(cpu_cap))
    ram = min(float(avail_ram), float(ram_cap))

    # Respect op-declared minima if present (best-effort).
    op_min_ram = None
    for attr in ("min_ram", "ram_min", "min_memory", "memory_min", "min_mem"):
        v = getattr(op, attr, None)
        if v is not None:
            op_min_ram = float(v)
            break
    if op_min_ram is not None:
        ram = max(ram, op_min_ram)

    # Apply learned RAM hint (e.g., after OOM).
    k = _op_key(op)
    hint = s.op_ram_hint.get(k, None)
    if hint is not None:
        ram = max(float(ram), float(hint))

    # Clamp to availability; if we can't meet the hinted/min requirement, signal "unschedulable".
    if ram > float(avail_ram) or cpu > float(avail_cpu) or cpu < s.min_cpu or ram < s.min_ram:
        return None

    return cpu, ram


@register_scheduler(key="scheduler_high_043")
def scheduler_high_043(s, results, pipelines):
    """
    Priority-aware scheduling step:
      - ingest new pipelines into per-priority queues
      - update RAM hints based on OOM failures
      - schedule high priority first using resource slices, then batch with remaining capacity
      - reserve small share for batch to avoid starvation
      - best-effort preemption of batch for interactive/query if blocked by lack of resources
    """
    s.ticks += 1

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p, front=False)

    # Update hints based on execution results
    for r in results:
        # Each result may contain multiple ops; handle each.
        for op in getattr(r, "ops", []) or []:
            k = _op_key(op)

            if r.failed():
                if _is_oom_error(getattr(r, "error", None)):
                    # Double RAM on OOM (classic backoff). Ensure we use at least previous hint.
                    prev = float(s.op_ram_hint.get(k, 0.0))
                    used = float(getattr(r, "ram", 0.0) or 0.0)
                    new_hint = max(prev, used, s.min_ram) * 2.0
                    s.op_ram_hint[k] = new_hint
                    s.op_fail_count[k] = 0  # OOM is "actionable", don't count toward non-OOM retry budget.

                    # Fast-track the owning pipeline to the front of its queue to reduce tail latency.
                    pid = s.op_to_pipeline_id.get(k, None)
                    if pid is not None:
                        pipe = s.pipelines_by_id.get(pid, None)
                        if pipe is not None:
                            _enqueue_pipeline(s, pipe, front=True)
                else:
                    # Non-OOM failures: retry a little, then stop scheduling this op to avoid infinite churn.
                    s.op_fail_count[k] = int(s.op_fail_count.get(k, 0)) + 1
                    if s.op_fail_count[k] > s.max_non_oom_retries:
                        s.op_blocked.add(k)
            else:
                # On success, clear non-OOM fail counter.
                s.op_fail_count[k] = 0

    # Early exit
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Determine whether batch is waiting (for fairness reservations).
    batch_waiting = len(s.queues[Priority.BATCH_PIPELINE]) > 0

    # Pool orders: for high priority, use most headroom first; for batch, pack into least headroom first.
    pool_ids = list(range(s.executor.num_pools))
    pool_ids_high = sorted(pool_ids, key=lambda i: _pool_headroom_score(s.executor.pools[i]), reverse=True)
    pool_ids_batch = sorted(pool_ids, key=lambda i: _pool_headroom_score(s.executor.pools[i]), reverse=False)

    assigned_pids = set()

    def schedule_pass(priorities, pool_order, reserve_for_batch):
        nonlocal suspensions, assignments, assigned_pids

        for pool_id in pool_order:
            pool = s.executor.pools[pool_id]
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)

            # Optionally reserve a small share for batch so it is not starved.
            reserve_cpu = 0.0
            reserve_ram = 0.0
            if reserve_for_batch and batch_waiting:
                reserve_cpu = float(pool.max_cpu_pool) * float(s.batch_reserve_cpu_frac)
                reserve_ram = float(pool.max_ram_pool) * float(s.batch_reserve_ram_frac)

            while True:
                cpu_budget = max(0.0, avail_cpu - reserve_cpu)
                ram_budget = max(0.0, avail_ram - reserve_ram)
                if cpu_budget < s.min_cpu or ram_budget < s.min_ram:
                    break

                picked_pipeline = None
                picked_op = None

                # Strict priority within the pass, FIFO within each priority.
                for pr in priorities:
                    pipe, op = _get_next_ready_op_from_queue(s, pr, assigned_pids)
                    if pipe is not None:
                        picked_pipeline, picked_op = pipe, op
                        break

                if picked_pipeline is None:
                    break

                # Compute allocation based on the *budget* (not full avail) to preserve reservations.
                alloc = _compute_allocation(s, pool, cpu_budget, ram_budget, picked_pipeline.priority, picked_op)

                # If we cannot allocate for high priority, attempt best-effort preemption of batch.
                if alloc is None and picked_pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                    # Try to free enough for minimum viable run of this op.
                    # Use its hint/min if known; otherwise target a small slice.
                    k = _op_key(picked_op)
                    need_ram = float(s.op_ram_hint.get(k, max(s.min_ram, float(pool.max_ram_pool) * 0.10)))
                    # If op declares a minimum RAM, respect it.
                    for attr in ("min_ram", "ram_min", "min_memory", "memory_min", "min_mem"):
                        v = getattr(picked_op, attr, None)
                        if v is not None:
                            need_ram = max(need_ram, float(v))
                            break
                    need_cpu = float(s.min_cpu)

                    pre_susp, freed_cpu, freed_ram = _maybe_preempt_for_high_priority(
                        s, pool, pool_id,
                        need_cpu=max(0.0, need_cpu - cpu_budget),
                        need_ram=max(0.0, need_ram - ram_budget),
                    )
                    if pre_susp:
                        suspensions.extend(pre_susp)
                        # Optimistically account for freed resources this tick.
                        avail_cpu += freed_cpu
                        avail_ram += freed_ram
                        cpu_budget = max(0.0, avail_cpu - reserve_cpu)
                        ram_budget = max(0.0, avail_ram - reserve_ram)
                        alloc = _compute_allocation(
                            s, pool, cpu_budget, ram_budget, picked_pipeline.priority, picked_op
                        )

                if alloc is None:
                    # Can't fit now; rotate and try others (break avoids spinning on one op).
                    break

                cpu, ram = alloc

                # Make assignment for exactly one op.
                assignment = Assignment(
                    ops=[picked_op],
                    cpu=cpu,
                    ram=ram,
                    priority=picked_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=picked_pipeline.pipeline_id,
                )
                assignments.append(assignment)

                assigned_pids.add(picked_pipeline.pipeline_id)
                avail_cpu -= float(cpu)
                avail_ram -= float(ram)

                if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
                    break

    # Pass 1: high priority first, preserve a small batch reserve to avoid starvation.
    schedule_pass(
        priorities=[Priority.QUERY, Priority.INTERACTIVE],
        pool_order=pool_ids_high,
        reserve_for_batch=True,
    )

    # Pass 2: schedule batch using remaining resources; pack into tighter pools for better utilization.
    schedule_pass(
        priorities=[Priority.BATCH_PIPELINE],
        pool_order=pool_ids_batch,
        reserve_for_batch=False,
    )

    return suspensions, assignments
