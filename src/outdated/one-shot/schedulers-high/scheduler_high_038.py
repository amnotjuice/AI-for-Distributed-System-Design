# policy_key: scheduler_high_038
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.121732
# generation_seconds: 172.43
# generated_at: 2026-03-14T04:09:59.633518
@register_scheduler_init(key="scheduler_high_038")
def scheduler_high_038_init(s):
    """
    Priority-aware, right-sized, multi-assignment scheduler.

    Small-but-impactful improvements over naive FIFO:
      1) Maintain separate queues per priority and always prefer higher priority.
      2) Avoid giving a single operator the entire pool by using simple per-priority size hints,
         enabling concurrency and improving tail latency for interactive/query work.
      3) Reserve a small headroom slice for high-priority arrivals so batch work is less likely to
         crowd out latency-sensitive tasks.
      4) On failures (especially OOM-like), increase per-pipeline RAM sizing hints and retry a
         bounded number of times (instead of dropping immediately).
    """
    from collections import deque

    # Queues store pipeline_ids; pipeline objects live in s.pipeline_map
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.pipeline_map = {}  # pipeline_id -> Pipeline

    # Per-pipeline sizing hints expressed as fractions of a pool's max resources.
    # (Fractions make the hints portable across heterogeneous pool sizes.)
    s.pipeline_cpu_frac = {}  # pipeline_id -> float in (0, 1]
    s.pipeline_ram_frac = {}  # pipeline_id -> float in (0, 1]

    # Retry tracking (used to avoid infinite loops on real failures).
    s.pipeline_retries = {}  # pipeline_id -> int
    s.max_retries = 3

    # Tunables
    s.reserve_frac_cpu = 0.20  # reserve for high-priority when backlog exists
    s.reserve_frac_ram = 0.20
    s.max_assignments_per_pool = 4  # allow multiple containers per pool per tick (no oversub)
    s.latency_pool_id = 0  # if multiple pools, we bias pool 0 towards high priority


def _sched_high_038_is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "alloc" in e)


def _sched_high_038_base_fracs(priority):
    # Conservative defaults: enough concurrency for latency-sensitive work, while keeping batch efficient.
    if priority == Priority.QUERY:
        return 0.25, 0.25  # cpu_frac, ram_frac
    if priority == Priority.INTERACTIVE:
        return 0.50, 0.40
    return 0.60, 0.60  # batch


def _sched_high_038_clamp(x, lo, hi):
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _sched_high_038_drop_pipeline(s, pipeline_id):
    # Remove from map and hint tables; queues are cleaned lazily.
    s.pipeline_map.pop(pipeline_id, None)
    s.pipeline_cpu_frac.pop(pipeline_id, None)
    s.pipeline_ram_frac.pop(pipeline_id, None)
    s.pipeline_retries.pop(pipeline_id, None)


def _sched_high_038_pipeline_id_from_result(r):
    # Best-effort extraction; simulator variants may expose pipeline_id differently.
    if hasattr(r, "pipeline_id"):
        return getattr(r, "pipeline_id")
    if hasattr(r, "pipeline") and getattr(r, "pipeline") is not None and hasattr(r.pipeline, "pipeline_id"):
        return r.pipeline.pipeline_id

    ops = getattr(r, "ops", None)
    if ops:
        op0 = ops[0]
        if hasattr(op0, "pipeline_id"):
            return getattr(op0, "pipeline_id")
        if hasattr(op0, "pipeline") and getattr(op0, "pipeline") is not None and hasattr(op0.pipeline, "pipeline_id"):
            return op0.pipeline.pipeline_id
    return None


def _sched_high_038_pop_runnable(s, priority, scheduled_pipelines, status_cache):
    """
    Round-robin scan within a priority queue. Returns (pipeline_id, op) or (None, None).
    Lazily skips completed/evicted pipelines and pipelines with no ready ops.
    """
    q = s.queues[priority]
    n = len(q)
    if n == 0:
        return None, None

    assignable_states = [OperatorState.PENDING, OperatorState.FAILED]

    for _ in range(n):
        pid = q.popleft()
        pipeline = s.pipeline_map.get(pid)

        if pipeline is None:
            # Pipeline was dropped; do not re-enqueue.
            continue

        if pid in scheduled_pipelines:
            q.append(pid)
            continue

        status = status_cache.get(pid)
        if status is None:
            status = pipeline.runtime_status()
            status_cache[pid] = status

        if status.is_pipeline_successful():
            _sched_high_038_drop_pipeline(s, pid)
            continue

        # If the pipeline is failing repeatedly, drop it to avoid infinite churn.
        if status.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pipeline_retries.get(pid, 0) >= s.max_retries:
            _sched_high_038_drop_pipeline(s, pid)
            continue

        op_list = status.get_ops(assignable_states, require_parents_complete=True)[:1]
        if not op_list:
            # Not runnable yet (e.g., waiting on parents); keep it in RR.
            q.append(pid)
            continue

        # Runnable: put it at the back (RR) and return the op.
        q.append(pid)
        return pid, op_list[0]

    return None, None


def _sched_high_038_compute_alloc(s, pipeline, pool, avail_cpu, avail_ram, high_backlog, allow_batch):
    """
    Compute right-sized (cpu, ram) allocation based on per-pipeline hint fractions.
    Applies headroom reservation when scheduling batch under high-priority backlog.
    """
    pid = pipeline.pipeline_id
    base_cpu_f, base_ram_f = _sched_high_038_base_fracs(pipeline.priority)

    cpu_f = max(base_cpu_f, s.pipeline_cpu_frac.get(pid, base_cpu_f))
    ram_f = max(base_ram_f, s.pipeline_ram_frac.get(pid, base_ram_f))

    # Keep within sane limits to preserve concurrency.
    cpu_f = _sched_high_038_clamp(cpu_f, 0.10, 1.00)
    ram_f = _sched_high_038_clamp(ram_f, 0.10, 1.00)

    cpu_target = max(1.0, pool.max_cpu_pool * cpu_f)
    ram_target = max(1.0, pool.max_ram_pool * ram_f)

    cpu = min(avail_cpu, cpu_target)
    ram = min(avail_ram, ram_target)

    # If we're trying to schedule batch work while high-priority backlog exists,
    # ensure we keep reserved headroom by shrinking the batch allocation.
    if pipeline.priority == Priority.BATCH_PIPELINE and high_backlog:
        if not allow_batch:
            return 0.0, 0.0

        reserved_cpu = max(0.0, pool.max_cpu_pool * s.reserve_frac_cpu)
        reserved_ram = max(0.0, pool.max_ram_pool * s.reserve_frac_ram)

        # Maximum we can give batch while preserving headroom.
        cpu = min(cpu, max(0.0, avail_cpu - reserved_cpu))
        ram = min(ram, max(0.0, avail_ram - reserved_ram))

    # Enforce minimum viable container.
    if cpu < 1.0 or ram < 1.0:
        return 0.0, 0.0

    return cpu, ram


@register_scheduler(key="scheduler_high_038")
def scheduler_high_038(s, results, pipelines):
    """
    Scheduler step:
      - Enqueue new pipelines by priority.
      - Update retry counters and RAM hints on failures (especially OOM).
      - For each pool, place multiple containers per tick:
          * Prefer QUERY > INTERACTIVE > BATCH
          * If multiple pools exist, bias pool 0 toward high priority
          * Reserve headroom for high priority before admitting batch under contention
    """
    # 1) Enqueue new pipelines (front-load latency-sensitive arrivals).
    for p in pipelines:
        s.pipeline_map[p.pipeline_id] = p
        cpu_f, ram_f = _sched_high_038_base_fracs(p.priority)
        s.pipeline_cpu_frac.setdefault(p.pipeline_id, cpu_f)
        s.pipeline_ram_frac.setdefault(p.pipeline_id, ram_f)
        s.pipeline_retries.setdefault(p.pipeline_id, 0)

        # Bias new high-priority work to be seen quickly.
        if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            s.queues[p.priority].appendleft(p.pipeline_id)
        else:
            s.queues[p.priority].append(p.pipeline_id)

    # If nothing changed, do nothing (fast path).
    if not pipelines and not results:
        return [], []

    # 2) Incorporate feedback from execution results.
    for r in results:
        pid = _sched_high_038_pipeline_id_from_result(r)
        if pid is None:
            continue

        # If the pipeline is already gone, ignore.
        if pid not in s.pipeline_map:
            continue

        if getattr(r, "failed", None) and r.failed():
            s.pipeline_retries[pid] = s.pipeline_retries.get(pid, 0) + 1

            # Increase RAM hint more aggressively on OOM-like failures.
            if _sched_high_038_is_oom_error(getattr(r, "error", None)):
                s.pipeline_ram_frac[pid] = _sched_high_038_clamp(s.pipeline_ram_frac.get(pid, 0.4) * 1.50, 0.10, 1.00)
            else:
                # Mild bump for unknown failures (could still be resource related).
                s.pipeline_ram_frac[pid] = _sched_high_038_clamp(s.pipeline_ram_frac.get(pid, 0.4) * 1.15, 0.10, 1.00)
        else:
            # Gentle decay toward base to avoid permanent over-provisioning.
            prio = getattr(r, "priority", None)
            if prio is None:
                prio = s.pipeline_map[pid].priority
            base_cpu_f, base_ram_f = _sched_high_038_base_fracs(prio)
            s.pipeline_ram_frac[pid] = max(base_ram_f, s.pipeline_ram_frac.get(pid, base_ram_f) * 0.98)
            s.pipeline_cpu_frac[pid] = max(base_cpu_f, s.pipeline_cpu_frac.get(pid, base_cpu_f) * 0.99)

    suspensions = []  # This policy is non-preemptive (small improvement step).
    assignments = []

    # 3) Determine if there is any high-priority backlog (used for batch admission control).
    # Note: backlog check is intentionally cheap/approximate; runnable-ness is checked during pop.
    high_backlog = (len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE])) > 0

    # 4) Schedule per pool with priority ordering and right-sized containers.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If multiple pools exist, bias pool 0 toward latency-sensitive work.
        if s.executor.num_pools > 1 and pool_id == s.latency_pool_id:
            priority_order = [Priority.QUERY, Priority.INTERACTIVE]
        else:
            priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

        status_cache = {}
        scheduled_pipelines = set()

        for _ in range(s.max_assignments_per_pool):
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            chosen_pid, chosen_op, chosen_pipeline = None, None, None

            # Try priorities in order; pick first runnable.
            for prio in priority_order:
                pid, op = _sched_high_038_pop_runnable(s, prio, scheduled_pipelines, status_cache)
                if pid is None:
                    continue
                pipeline = s.pipeline_map.get(pid)
                if pipeline is None:
                    continue
                chosen_pid, chosen_op, chosen_pipeline = pid, op, pipeline
                break

            if chosen_pipeline is None:
                break  # nothing runnable fits right now

            # Under contention, only allow batch if we can preserve headroom.
            allow_batch = True
            if chosen_pipeline.priority == Priority.BATCH_PIPELINE and high_backlog:
                # If pool is already tight, effectively block batch.
                reserved_cpu = pool.max_cpu_pool * s.reserve_frac_cpu
                reserved_ram = pool.max_ram_pool * s.reserve_frac_ram
                if avail_cpu <= reserved_cpu + 1.0 or avail_ram <= reserved_ram + 1.0:
                    allow_batch = False

            cpu, ram = _sched_high_038_compute_alloc(
                s,
                chosen_pipeline,
                pool,
                avail_cpu,
                avail_ram,
                high_backlog=high_backlog,
                allow_batch=allow_batch,
            )

            if cpu <= 0 or ram <= 0:
                # Could not size a container without violating headroom; stop filling this pool this tick.
                break

            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=cpu,
                    ram=ram,
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            scheduled_pipelines.add(chosen_pid)
            avail_cpu -= cpu
            avail_ram -= ram

    return suspensions, assignments
