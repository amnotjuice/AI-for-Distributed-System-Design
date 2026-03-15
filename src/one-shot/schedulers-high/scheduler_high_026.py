# policy_key: scheduler_high_026
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.168044
# generation_seconds: 192.41
# generated_at: 2026-03-14T03:40:13.325861
@register_scheduler_init(key="scheduler_high_026")
def scheduler_high_026_init(s):
    """
    Priority-aware, headroom-preserving scheduler.

    Small, obvious improvements over naive FIFO:
    - Maintain separate FIFO queues per priority and always try higher priority first.
    - Prevent low-priority (batch) work from consuming all pool resources when
      high-priority work may arrive by reserving headroom (per pool).
    - Avoid "dead" pipelines clogging queues by dropping completed/terminal-failed pipelines.
    - Simple OOM backoff: if we can attribute an OOM to a pipeline, increase its RAM multiplier
      for subsequent retries (ASSIGNABLE_STATES includes FAILED so retries can happen).

    Intentionally avoids complex preemption (requires reliable access to running containers).
    """
    from collections import deque, defaultdict

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Lightweight bookkeeping
    s._known_pipelines = set()          # pipeline_id we've enqueued at least once
    s._terminal_pipelines = set()       # pipeline_id that should no longer be scheduled
    s._ram_multiplier = defaultdict(lambda: 1.0)  # pipeline_id -> RAM multiplier after OOM
    s._iter = 0

    # Tuning knobs (start simple; modest headroom, modest sizing)
    s.max_assignments_per_pool = 2  # allow filling leftover capacity a bit
    s.min_cpu = 0.25
    s.min_ram = 0.25

    # Fractions of pool max used per container by priority (encourages some concurrency)
    s.cpu_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.50,
    }
    s.ram_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Headroom reservation for batch when any high-priority backlog exists
    s.reserve_cpu_frac = 0.20
    s.reserve_ram_frac = 0.20

    # If multiple pools exist, prefer to keep pool 0 more available for high-priority work
    s.reserved_pool_id = 0
    s.reserved_pool_extra_reserve = 0.20  # add'l reserve on pool 0 for batch when HP backlog exists

    s.oom_keywords = ("oom", "out of memory", "memoryerror", "killed process", "cannot allocate memory")


def _scheduler_high_026__norm_prio(p):
    pr = getattr(p, "priority", None)
    if pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        return pr
    return Priority.BATCH_PIPELINE


def _scheduler_high_026__result_pipeline_id(r):
    # Best-effort extraction; simulator implementations may expose pipeline_id directly
    pid = getattr(r, "pipeline_id", None)
    if pid is not None:
        return pid
    ops = getattr(r, "ops", None) or []
    if ops:
        pid = getattr(ops[0], "pipeline_id", None)
        if pid is not None:
            return pid
    return None


def _scheduler_high_026__cleanup_pipeline_state(s, pipeline_id):
    s._terminal_pipelines.discard(pipeline_id)
    s._ram_multiplier.pop(pipeline_id, None)
    s._known_pipelines.discard(pipeline_id)


def _scheduler_high_026__dequeue_runnable(s, prio, scheduled_pipeline_ids):
    """
    Rotate through the queue once, looking for a pipeline with at least one assignable op
    (with parents complete). Returns (pipeline, op_list) or (None, None).
    """
    q = s.queues[prio]
    n = len(q)
    for _ in range(n):
        p = q.popleft()
        pid = p.pipeline_id

        # Drop pipelines that are known terminal
        if pid in s._terminal_pipelines:
            _scheduler_high_026__cleanup_pipeline_state(s, pid)
            continue

        st = p.runtime_status()
        if st.is_pipeline_successful():
            _scheduler_high_026__cleanup_pipeline_state(s, pid)
            continue

        # Avoid scheduling multiple ops from the same pipeline in the same scheduler call
        if pid in scheduled_pipeline_ids:
            q.append(p)
            continue

        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        q.append(p)  # keep it in FIFO rotation
        if op_list:
            return p, op_list

    return None, None


def _scheduler_high_026__size_for(s, pool, avail_cpu, avail_ram, prio, pipeline_id, hp_backlog, pool_id):
    """
    Compute (cpu, ram) for an assignment, preserving headroom for high priority
    by capping batch when hp_backlog exists.
    """
    max_cpu = pool.max_cpu_pool
    max_ram = pool.max_ram_pool

    # Batch headroom reservation if there is any high-priority backlog
    cpu_cap = avail_cpu
    ram_cap = avail_ram
    if prio == Priority.BATCH_PIPELINE and hp_backlog:
        reserve_cpu = max_cpu * s.reserve_cpu_frac
        reserve_ram = max_ram * s.reserve_ram_frac
        if pool_id == s.reserved_pool_id and s.executor.num_pools > 1:
            reserve_cpu += max_cpu * s.reserved_pool_extra_reserve
            reserve_ram += max_ram * s.reserved_pool_extra_reserve
        cpu_cap = max(0.0, avail_cpu - reserve_cpu)
        ram_cap = max(0.0, avail_ram - reserve_ram)

    if cpu_cap <= 0 or ram_cap <= 0:
        return None, None

    mult = float(s._ram_multiplier.get(pipeline_id, 1.0))
    target_cpu = max(s.min_cpu, max_cpu * float(s.cpu_frac.get(prio, 0.5)))
    target_ram = max(s.min_ram, max_ram * float(s.ram_frac.get(prio, 0.5)) * mult)

    cpu = min(cpu_cap, target_cpu)
    ram = min(ram_cap, target_ram)

    if cpu <= 0 or ram <= 0:
        return None, None
    return cpu, ram


@register_scheduler(key="scheduler_high_026")
def scheduler_high_026(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Scheduling loop:
    1) Enqueue new pipelines by priority FIFO.
    2) Process results to detect terminal failures vs. OOM (OOM -> increase RAM multiplier).
    3) For each pool, schedule a few high-priority ops first, then batch ops if capacity remains,
       with batch capped by reserved headroom when HP backlog exists.
    """
    s._iter += 1

    # Enqueue new pipelines
    for p in pipelines:
        pr = _scheduler_high_026__norm_prio(p)
        if p.pipeline_id not in s._known_pipelines and p.pipeline_id not in s._terminal_pipelines:
            s._known_pipelines.add(p.pipeline_id)
            s.queues[pr].append(p)

    # Process execution results (best-effort OOM handling)
    for r in results:
        if not r.failed():
            continue

        pid = _scheduler_high_026__result_pipeline_id(r)
        err = str(getattr(r, "error", "")).lower()
        is_oom = any(k in err for k in s.oom_keywords)

        if pid is None:
            # Can't attribute; do nothing
            continue

        if is_oom:
            # Increase RAM on retries; keep pipeline schedulable
            s._ram_multiplier[pid] = min(float(s._ram_multiplier.get(pid, 1.0)) * 2.0, 16.0)
        else:
            # Terminal failure: stop scheduling this pipeline
            s._terminal_pipelines.add(pid)

    # If nothing to do, exit quickly
    if (
        not any(len(q) for q in s.queues.values())
        or all(
            s.executor.pools[i].avail_cpu_pool <= 0 or s.executor.pools[i].avail_ram_pool <= 0
            for i in range(s.executor.num_pools)
        )
    ):
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Simple indicator: is there any high-priority backlog?
    hp_backlog = (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    scheduled_pipeline_ids = set()

    # Prefer pool 0 for high priority when multiple pools exist
    pool_order = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1 and s.reserved_pool_id in pool_order:
        pool_order = [s.reserved_pool_id] + [i for i in pool_order if i != s.reserved_pool_id]

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        made = 0

        # Phase 1: High priority first (QUERY then INTERACTIVE)
        while made < s.max_assignments_per_pool and avail_cpu > 0 and avail_ram > 0:
            p, ops = _scheduler_high_026__dequeue_runnable(s, Priority.QUERY, scheduled_pipeline_ids)
            pr = Priority.QUERY
            if not p:
                p, ops = _scheduler_high_026__dequeue_runnable(s, Priority.INTERACTIVE, scheduled_pipeline_ids)
                pr = Priority.INTERACTIVE

            if not p or not ops:
                break

            cpu, ram = _scheduler_high_026__size_for(
                s, pool, avail_cpu, avail_ram, pr, p.pipeline_id, hp_backlog, pool_id
            )
            if cpu is None or ram is None:
                break

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            scheduled_pipeline_ids.add(p.pipeline_id)
            avail_cpu -= cpu
            avail_ram -= ram
            made += 1

        # Phase 2: Batch work (only if capacity remains)
        # - If HP backlog exists, keep headroom (especially on pool 0) by capping batch allocations.
        # - If no HP backlog, batch can consume remaining resources more freely.
        while made < s.max_assignments_per_pool and avail_cpu > 0 and avail_ram > 0:
            # If pool 0 is "reserved" and HP backlog exists, be conservative and avoid batch entirely.
            if hp_backlog and s.executor.num_pools > 1 and pool_id == s.reserved_pool_id:
                break

            p, ops = _scheduler_high_026__dequeue_runnable(s, Priority.BATCH_PIPELINE, scheduled_pipeline_ids)
            if not p or not ops:
                break

            cpu, ram = _scheduler_high_026__size_for(
                s, pool, avail_cpu, avail_ram, Priority.BATCH_PIPELINE, p.pipeline_id, hp_backlog, pool_id
            )
            if cpu is None or ram is None:
                break

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            scheduled_pipeline_ids.add(p.pipeline_id)
            avail_cpu -= cpu
            avail_ram -= ram
            made += 1

    return suspensions, assignments
