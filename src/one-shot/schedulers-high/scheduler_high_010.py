# policy_key: scheduler_high_010
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.099931
# generation_seconds: 117.66
# generated_at: 2026-03-12T22:45:58.914797
@register_scheduler_init(key="scheduler_high_010")
def scheduler_high_010_init(s):
    """
    Priority-aware, retry-friendly scheduler.

    Improvements over naive FIFO:
      1) Priority queues: always try QUERY first, then INTERACTIVE, then BATCH_PIPELINE.
      2) Better pool filling: assign multiple operators per pool per tick (instead of 1).
      3) OOM-aware retries: if we observe OOM failures, increase RAM (and slightly CPU) for that pipeline.
      4) Gentle decay: if executions succeed, slowly reduce over-allocation multipliers back toward 1.0.

    Notes:
      - This policy intentionally avoids preemption to keep behavior safe and predictable without
        relying on undocumented executor introspection APIs.
      - We keep scheduling at most one op per pipeline per scheduler tick to reduce intra-pipeline
        "branch fanout" from crowding out other pipelines, which helps latency under contention.
    """
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-pipeline sizing state keyed by pipeline_id
    s.pipeline_meta = {}
    s.tick = 0

    # Simple knobs (kept conservative for "small improvements first")
    s.max_assignments_per_pool = 8
    s.max_failures_per_pipeline = 3


def _sh010_is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)


def _sh010_infer_pipeline_id_from_result(r):
    # Best effort: different simulator versions may or may not carry pipeline_id on results/ops.
    pid = getattr(r, "pipeline_id", None)
    if pid is not None:
        return pid
    ops = getattr(r, "ops", None) or []
    if ops:
        pid = getattr(ops[0], "pipeline_id", None)
        if pid is not None:
            return pid
    return None


def _sh010_get_meta(s, pipeline_id):
    m = s.pipeline_meta.get(pipeline_id)
    if m is None:
        m = {
            "ram_mult": 1.0,
            "cpu_mult": 1.0,
            "failures": 0,
            "last_update_tick": -1,
        }
        s.pipeline_meta[pipeline_id] = m
    return m


def _sh010_priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _sh010_base_shares(priority, backlog_count):
    # Shares are of pool max resources (not remaining), then capped by remaining.
    # If backlog is high, reduce per-op share to reduce queueing and improve tail latency.
    if priority == Priority.QUERY:
        return (0.70, 0.60) if backlog_count <= 1 else (0.40, 0.35)
    if priority == Priority.INTERACTIVE:
        return (0.55, 0.50) if backlog_count <= 1 else (0.35, 0.30)
    # Batch: keep smaller shares to protect higher priorities
    return (0.30, 0.30) if backlog_count <= 1 else (0.20, 0.20)


@register_scheduler(key="scheduler_high_010")
def scheduler_high_010(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]):
    """
    Priority-first, pool-filling scheduler with basic OOM-driven resizing.

    Admission:
      - Enqueue pipelines into per-priority FIFO queues.

    Placement:
      - Iterate pools; in each pool, repeatedly pick the next READY operator from the highest
        available priority queue and assign it until pool headroom is small.

    Resizing:
      - Default CPU/RAM are chosen by (priority, queue backlog) as a fraction of pool max.
      - If we observe OOM, increase RAM multiplier for the pipeline (and slightly CPU).

    Fairness:
      - Strict priority; batch runs when high-priority queues cannot make progress or are empty.
      - Within a tick, avoid scheduling multiple ops from the same pipeline.
    """
    s.tick += 1

    # Enqueue arrivals
    for p in pipelines:
        s.waiting_queues[p.priority].append(p)
        _sh010_get_meta(s, p.pipeline_id)  # ensure meta exists

    # Update sizing hints from results (best-effort pipeline_id inference)
    for r in results:
        pid = _sh010_infer_pipeline_id_from_result(r)
        if pid is None:
            continue
        m = _sh010_get_meta(s, pid)
        m["last_update_tick"] = s.tick

        if r.failed():
            m["failures"] = m.get("failures", 0) + 1
            if _sh010_is_oom_error(getattr(r, "error", None)):
                # RAM-first bump; CPU slight bump to avoid very slow retries
                m["ram_mult"] = min(16.0, max(m["ram_mult"] * 1.60, 1.25))
                m["cpu_mult"] = min(4.0, max(m["cpu_mult"] * 1.10, 1.0))
        else:
            # Gentle decay toward 1.0 to avoid permanently pinning big sizes
            m["ram_mult"] = max(1.0, m["ram_mult"] * 0.90)
            m["cpu_mult"] = max(1.0, m["cpu_mult"] * 0.95)
            # On success, gradually forgive previous failures
            m["failures"] = max(0, m.get("failures", 0) - 1)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Prevent scheduling more than one op per pipeline per scheduler invocation
    scheduled_pipeline_ids = set()

    def pop_next_ready():
        """
        Pop the next pipeline+op to run, respecting strict priority.
        Returns: (priority, pipeline, op_list) or (None, None, None)
        """
        for prio in _sh010_priority_order():
            q = s.waiting_queues.get(prio, [])
            n = len(q)
            if n == 0:
                continue

            for _ in range(n):
                p = q.pop(0)
                pid = p.pipeline_id

                # Avoid multiple ops per pipeline per tick (helps latency under fanout)
                if pid in scheduled_pipeline_ids:
                    q.append(p)
                    continue

                status = p.runtime_status()

                # Drop completed pipelines
                if status.is_pipeline_successful():
                    continue

                # Limit retries on repeatedly failing pipelines
                m = _sh010_get_meta(s, pid)
                failed_count = 0
                try:
                    failed_count = status.state_counts.get(OperatorState.FAILED, 0)
                except Exception:
                    # Fallback if state_counts isn't a dict-like
                    try:
                        failed_count = status.state_counts[OperatorState.FAILED]
                    except Exception:
                        failed_count = 0

                if failed_count > 0 and m.get("failures", 0) >= s.max_failures_per_pipeline:
                    # Give up on this pipeline (drop it from the queue)
                    continue

                # Find next assignable op whose parents are complete
                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    # Not ready yet; keep it in the queue
                    q.append(p)
                    continue

                return prio, p, op_list

        return None, None, None

    # Fill each pool with multiple assignments
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        num_assigned_here = 0
        while num_assigned_here < s.max_assignments_per_pool and avail_cpu > 0 and avail_ram > 0:
            prio, pipeline, op_list = pop_next_ready()
            if pipeline is None:
                break

            pid = pipeline.pipeline_id
            scheduled_pipeline_ids.add(pid)

            # Choose per-op size from pool max, adjusted by backlog and OOM multipliers
            backlog = len(s.waiting_queues.get(prio, [])) + 1
            cpu_share, ram_share = _sh010_base_shares(prio, backlog)
            m = _sh010_get_meta(s, pid)

            target_cpu = float(pool.max_cpu_pool) * float(cpu_share) * float(m.get("cpu_mult", 1.0))
            target_ram = float(pool.max_ram_pool) * float(ram_share) * float(m.get("ram_mult", 1.0))

            # Enforce minimums and cap by available headroom
            # (keep small minimum RAM/CPU to avoid zero allocations)
            cpu = min(float(avail_cpu), max(1.0, target_cpu))
            ram = min(float(avail_ram), max(1.0, target_ram))

            # If we cannot allocate meaningful resources, stop filling this pool
            if cpu <= 0 or ram <= 0:
                # Put pipeline back to queue for later
                s.waiting_queues[pipeline.priority].append(pipeline)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Decrement local headroom for continued packing
            avail_cpu -= cpu
            avail_ram -= ram
            num_assigned_here += 1

            # Requeue pipeline (it may have more runnable ops later)
            s.waiting_queues[pipeline.priority].append(pipeline)

    return suspensions, assignments
