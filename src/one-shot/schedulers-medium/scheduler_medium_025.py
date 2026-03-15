# policy_key: scheduler_medium_025
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.099245
# generation_seconds: 115.01
# generated_at: 2026-03-14T02:48:43.968053
@register_scheduler_init(key="scheduler_medium_025")
def scheduler_medium_025_init(s):
    """
    Priority-aware, latency-oriented scheduler with small incremental improvements over naive FIFO:

    1) Priority queues: always prefer QUERY > INTERACTIVE > BATCH.
    2) Headroom reservation: avoid filling pools with BATCH so higher-priority arrivals see lower queueing delay.
    3) Conservative right-sizing: don't assign the entire pool to a single op; use per-priority fractions.
    4) OOM-aware retries: if an op fails with an OOM-like error, increase its RAM hint and retry (bounded).
    5) Limited per-pipeline concurrency: reduce interference for interactive/query by keeping <=1 inflight op.
    """
    # Pipeline bookkeeping
    s.pipelines_by_id = {}          # pipeline_id -> Pipeline
    s.enqueued_tick = {}            # pipeline_id -> tick enqueued (first seen)
    s.tick = 0

    # Per-priority waiting queues (store pipeline_id to avoid holding stale references)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Resource hints by "operator key" (derived from op object)
    s.ram_hint = {}                 # op_key -> suggested RAM
    s.cpu_hint = {}                 # op_key -> suggested CPU (best-effort)

    # Retry control (to avoid infinite reattempt loops)
    s.op_retries = {}               # (pipeline_id, op_key) -> retry count
    s.abandoned_pipelines = set()   # pipeline_ids we will no longer schedule

    # Tunables
    s.max_retries_per_op = 3
    s.batch_aging_ticks = 50  # after this, let batch borrow headroom (prevents starvation)

    # Headroom reserved per pool (fractions of pool max)
    s.reserve_cpu_frac = 0.25
    s.reserve_ram_frac = 0.25

    # Default right-sizing fractions (of pool max) per priority
    s.cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.ram_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Limit concurrent inflight ops per pipeline by priority (reduces interference / tail latency)
    s.inflight_limit = {
        Priority.QUERY: 1,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 2,
    }


def _op_key(op):
    """Best-effort stable key for an operator across retries within the simulator."""
    k = getattr(op, "op_id", None)
    if k is not None:
        return ("op_id", k)
    k = getattr(op, "operator_id", None)
    if k is not None:
        return ("operator_id", k)
    k = getattr(op, "name", None)
    if k is not None:
        return ("name", k)
    # Fall back to repr; in many simulators this is stable enough for a single run.
    return ("repr", repr(op))


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    s = str(err).upper()
    return ("OOM" in s) or ("OUT OF MEMORY" in s) or ("MEMORY" in s and "EXCEEDED" in s)


def _enqueue_pipeline(s, p):
    pid = p.pipeline_id
    if pid in s.abandoned_pipelines:
        return
    if pid not in s.pipelines_by_id:
        s.pipelines_by_id[pid] = p
        s.enqueued_tick[pid] = s.tick

    if p.priority == Priority.QUERY:
        s.q_query.append(pid)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(pid)
    else:
        s.q_batch.append(pid)


def _pipeline_age(s, pid: str) -> int:
    t0 = s.enqueued_tick.get(pid, s.tick)
    return max(0, s.tick - t0)


def _cleanup_queues(s):
    """Remove completed/abandoned pipelines from the heads/tails of queues opportunistically."""
    def keep_pid(pid):
        if pid in s.abandoned_pipelines:
            return False
        p = s.pipelines_by_id.get(pid)
        if p is None:
            return False
        st = p.runtime_status()
        return (not st.is_pipeline_successful())

    s.q_query = [pid for pid in s.q_query if keep_pid(pid)]
    s.q_interactive = [pid for pid in s.q_interactive if keep_pid(pid)]
    s.q_batch = [pid for pid in s.q_batch if keep_pid(pid)]


def _inflight_count(status) -> int:
    # Count ops that are already occupying capacity for this pipeline.
    return status.state_counts.get(OperatorState.ASSIGNED, 0) + status.state_counts.get(OperatorState.RUNNING, 0)


def _pick_next_pipeline(s):
    """
    Pick the next runnable op using strict priority ordering, while keeping FIFO within each class.
    Returns: (pipeline, op_list) or (None, None)
    """
    # Try queues in priority order.
    queues = [s.q_query, s.q_interactive, s.q_batch]

    for q in queues:
        # Scan at most the queue length to preserve order while skipping blocked pipelines.
        n = len(q)
        for _ in range(n):
            pid = q.pop(0)
            p = s.pipelines_by_id.get(pid)
            if p is None or pid in s.abandoned_pipelines:
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            # Limit inflight ops per pipeline to reduce interference.
            inflight = _inflight_count(st)
            limit = s.inflight_limit.get(p.priority, 1)
            if inflight >= limit:
                q.append(pid)
                continue

            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready; keep it in the queue.
                q.append(pid)
                continue

            # Put pipeline back to the tail (round-robin within the class).
            q.append(pid)
            return p, op_list

    return None, None


def _compute_request(s, pool, priority, op):
    """
    Compute a conservative CPU/RAM request using per-priority fractions and learned hints.
    Returns: (cpu, ram)
    """
    # Base right-sizing from pool capacity
    base_cpu = pool.max_cpu_pool * s.cpu_frac.get(priority, 0.25)
    base_ram = pool.max_ram_pool * s.ram_frac.get(priority, 0.25)

    # Learned hints (primarily from past successes and OOM retries)
    k = _op_key(op)
    hint_cpu = s.cpu_hint.get(k, 0)
    hint_ram = s.ram_hint.get(k, 0)

    cpu = max(base_cpu, hint_cpu, 1)
    ram = max(base_ram, hint_ram, 1)

    # Avoid grabbing *everything* even for high priority; keep a little slack.
    cpu = min(cpu, pool.max_cpu_pool * 0.90)
    ram = min(ram, pool.max_ram_pool * 0.90)

    return cpu, ram


def _select_pool_for_op(s, avail_cpu, avail_ram, priority, op, pid):
    """
    Choose a pool based on priority:
    - QUERY/INTERACTIVE: prefer the pool where we can allocate the most (reduce latency).
    - BATCH: prefer packing while respecting headroom reservation (unless aged).
    Returns: (pool_id, cpu, ram) or (None, None, None)
    """
    best = None

    is_batch = (priority == Priority.BATCH_PIPELINE)
    aged_batch = is_batch and (_pipeline_age(s, pid) >= s.batch_aging_ticks)

    for pool_id in range(s.executor.num_pools):
        a_cpu = avail_cpu[pool_id]
        a_ram = avail_ram[pool_id]
        if a_cpu <= 0 or a_ram <= 0:
            continue

        pool = s.executor.pools[pool_id]
        req_cpu, req_ram = _compute_request(s, pool, priority, op)

        # Must fit in currently available capacity.
        cpu = min(req_cpu, a_cpu)
        ram = min(req_ram, a_ram)
        if cpu <= 0 or ram <= 0:
            continue

        # Headroom reservation: don't let BATCH consume the last chunk of resources.
        if is_batch and not aged_batch:
            reserve_cpu = pool.max_cpu_pool * s.reserve_cpu_frac
            reserve_ram = pool.max_ram_pool * s.reserve_ram_frac
            if (a_cpu - cpu) < reserve_cpu or (a_ram - ram) < reserve_ram:
                continue

        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            # Prefer pools that can give us *more* right now (lower runtime and queueing effects).
            score = (cpu / max(pool.max_cpu_pool, 1e-9)) + (ram / max(pool.max_ram_pool, 1e-9))
        else:
            # Pack batch: prefer minimal leftover (while fitting) to concentrate load and preserve headroom elsewhere.
            leftover = (a_cpu - cpu) / max(pool.max_cpu_pool, 1e-9) + (a_ram - ram) / max(pool.max_ram_pool, 1e-9)
            score = -leftover

        if best is None or score > best[0]:
            best = (score, pool_id, cpu, ram)

    if best is None:
        return None, None, None
    return best[1], best[2], best[3]


@register_scheduler(key="scheduler_medium_025")
def scheduler_medium_025(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> "Tuple[List[Suspend], List[Assignment]]":
    """
    Priority + headroom reservation + OOM-aware retry.
    Designed as a modest, safe improvement over naive FIFO focusing on latency for high-priority work.
    """
    s.tick += 1

    # Incorporate new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Process results to learn sizing + decide retries/abandon
    if results:
        for r in results:
            # We may see multiple ops; update hints for each.
            ops = getattr(r, "ops", []) or []
            for op in ops:
                k = _op_key(op)

                if r.failed():
                    # Retry logic: on OOM, increase RAM hint aggressively.
                    if _is_oom_error(getattr(r, "error", None)):
                        # Increase RAM hint based on what we tried.
                        tried_ram = getattr(r, "ram", 0) or 0
                        prev = s.ram_hint.get(k, 0) or 0
                        # Double the larger of previous hint and tried size.
                        s.ram_hint[k] = max(prev, tried_ram, 1) * 2
                    # Track retries per (pipeline_id, op)
                    pid = getattr(r, "pipeline_id", None)
                    if pid is None:
                        # If ExecutionResult doesn't carry pipeline_id, we can't tie retries to a pipeline reliably.
                        # Still, avoid runaway by updating a global-ish counter keyed by container/op.
                        pid = "unknown_pipeline"
                    key = (pid, k)
                    s.op_retries[key] = s.op_retries.get(key, 0) + 1
                    if s.op_retries[key] > s.max_retries_per_op and pid != "unknown_pipeline":
                        s.abandoned_pipelines.add(pid)
                else:
                    # On success, record what worked (helps avoid undersizing later).
                    used_ram = getattr(r, "ram", 0) or 0
                    used_cpu = getattr(r, "cpu", 0) or 0
                    if used_ram:
                        s.ram_hint[k] = max(s.ram_hint.get(k, 0) or 0, used_ram)
                    if used_cpu:
                        s.cpu_hint[k] = max(s.cpu_hint.get(k, 0) or 0, used_cpu)

    # If nothing changed and no arrivals, no new decisions.
    if not pipelines and not results:
        return [], []

    # Remove completed/abandoned pipelines from queues.
    _cleanup_queues(s)

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Snapshot pool availability and update locally as we place work (greedy packing).
    avail_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(s.executor.num_pools)]
    avail_ram = [s.executor.pools[i].avail_ram_pool for i in range(s.executor.num_pools)]

    # Greedy loop: pick next best op by priority, then place it on the best pool.
    # Limit iterations to prevent pathological looping when nothing fits.
    max_iters = 4 * (len(s.q_query) + len(s.q_interactive) + len(s.q_batch) + 1)
    iters = 0
    consecutive_no_place = 0

    while iters < max_iters:
        iters += 1

        p, op_list = _pick_next_pipeline(s)
        if not p or not op_list:
            break

        pid = p.pipeline_id
        op = op_list[0]

        pool_id, cpu, ram = _select_pool_for_op(s, avail_cpu, avail_ram, p.priority, op, pid)
        if pool_id is None:
            # Couldn't place this op anywhere; try others, but stop if we're repeatedly failing.
            consecutive_no_place += 1
            if consecutive_no_place >= 10:
                break
            continue

        consecutive_no_place = 0

        # Create assignment
        assignments.append(
            Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=pid,
            )
        )

        # Update local availability
        avail_cpu[pool_id] -= cpu
        avail_ram[pool_id] -= ram

        # If this pool is effectively full, continue; loop will handle it.

    return suspensions, assignments
