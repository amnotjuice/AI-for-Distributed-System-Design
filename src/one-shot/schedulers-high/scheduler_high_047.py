# policy_key: scheduler_high_047
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.104327
# generation_seconds: 206.05
# generated_at: 2026-03-14T04:32:50.180822
@register_scheduler_init(key="scheduler_high_047")
def scheduler_high_047_init(s):
    """Priority-aware, low-risk upgrade over naive FIFO.

    Main improvements (kept intentionally simple/robust):
      1) Separate per-priority queues; always try INTERACTIVE, then QUERY, then BATCH.
      2) Small headroom reservation when high-priority work is waiting (helps tail latency).
      3) Conservative right-sizing by priority (more CPU/RAM for higher priority).
      4) OOM-aware retries: if a container fails with OOM, retry with more RAM (bounded).
      5) Optional best-effort preemption: if the simulator exposes running containers in a pool,
         suspend lowest-priority containers to make room for high-priority work.

    Notes:
      - We keep logic deterministic and defensive (lots of getattr/try) because the simulator
        may not expose all optional attributes (e.g., running container listing for preemption).
      - We avoid complex predictive modeling; instead we use simple feedback (OOM => more RAM).
    """
    s.clock = 0

    # Per-priority waiting queues (pipelines persist here until completion/fatal failure).
    s.wait_q = {
        Priority.INTERACTIVE: [],
        Priority.QUERY: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Pipeline bookkeeping
    s.known_pipeline_ids = set()
    s.pipeline_by_id = {}
    s.arrival_time = {}

    # Map operator object identity -> pipeline_id (used to connect ExecutionResult back to pipeline).
    s.op_to_pipeline = {}

    # Simple per-pipeline sizing adaptation
    s.ram_scale = {}          # multiplicative factor applied to a per-priority RAM baseline
    s.cpu_scale = {}          # multiplicative factor applied to a per-priority CPU target
    s.min_ram_needed = {}     # if we observed OOM at X, avoid scheduling below this until cleared

    # Failure handling
    s.oom_retry_count = {}
    s.oom_retry_limit = 3
    s.oom_allowed = set()     # pipelines for which FAILED ops are eligible for retry
    s.fatal_pipelines = set() # drop pipelines with non-OOM failures (or too many OOMs)

    # Preemption controls (best effort; depends on simulator exposing pool containers)
    s.preempt_cooldown_ticks = 5
    s.preempted_at = {}       # container_id -> last tick preempted

    # Policy knobs
    s.reserve_frac = 0.20  # reserve this fraction of a pool when high-priority work is waiting

    # Target CPU per container by priority (scaled by cpu_scale[pid])
    s.cpu_target = {
        Priority.INTERACTIVE: 4,
        Priority.QUERY: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Baseline RAM fraction of pool capacity by priority (scaled by ram_scale[pid])
    s.base_ram_frac = {
        Priority.INTERACTIVE: 0.40,
        Priority.QUERY: 0.30,
        Priority.BATCH_PIPELINE: 0.20,
    }

    # Limit how many ops a pipeline can have inflight at once (per priority).
    # This reduces interference and improves tail latency; interactive gets a bit more parallelism.
    s.max_inflight_ops = {
        Priority.INTERACTIVE: 2,
        Priority.QUERY: 2,
        Priority.BATCH_PIPELINE: 1,
    }


def _is_oom_error(err):
    if not err:
        return False
    try:
        e = str(err).lower()
    except Exception:
        return False
    return ("oom" in e) or ("out of memory" in e) or ("killed" in e and "memory" in e)


def _iter_pipeline_ops(pipeline):
    """Best-effort iteration over a pipeline's operator objects to build op->pipeline mapping."""
    vals = getattr(pipeline, "values", None)
    if vals is None:
        return []
    try:
        # Common case: dict-like
        if hasattr(vals, "values"):
            return list(vals.values())
        # Otherwise: iterable
        return list(vals)
    except Exception:
        return []


def _state_count(status, st):
    try:
        return status.state_counts[st]
    except Exception:
        try:
            return status.state_counts.get(st, 0)
        except Exception:
            return 0


def _assignable_states(allow_failed):
    if allow_failed:
        try:
            return ASSIGNABLE_STATES
        except Exception:
            return [OperatorState.PENDING, OperatorState.FAILED]
    return [OperatorState.PENDING]


def _pipeline_done_or_drop(s, pipeline):
    """Return (drop: bool, done: bool)."""
    pid = pipeline.pipeline_id
    if pid in s.fatal_pipelines:
        return True, False

    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True, True

    # If pipeline has FAILED ops, only keep it if we explicitly allow OOM retries.
    has_failures = _state_count(status, OperatorState.FAILED) > 0
    if has_failures and pid not in s.oom_allowed:
        # Non-OOM failures (or unknown) => treat as fatal to avoid thrash.
        s.fatal_pipelines.add(pid)
        return True, False

    return False, False


def _eligible_ops_for_pipeline(s, pipeline):
    """Pick at most one runnable op (parents complete), respecting inflight limit."""
    pid = pipeline.pipeline_id
    status = pipeline.runtime_status()

    inflight = (
        _state_count(status, OperatorState.ASSIGNED)
        + _state_count(status, OperatorState.RUNNING)
        + _state_count(status, OperatorState.SUSPENDING)
    )
    limit = s.max_inflight_ops.get(pipeline.priority, 1)
    if inflight >= limit:
        return []

    allow_failed = (pid in s.oom_allowed)
    states = _assignable_states(allow_failed)

    try:
        ops = status.get_ops(states, require_parents_complete=True)
    except Exception:
        return []
    if not ops:
        return []
    return ops[:1]


def _dequeue_next_pipeline(s, pri, batch_scan_k=8):
    """Pop next candidate pipeline from a priority queue.

    For batch, do a tiny aging tweak: among first K items, pick the oldest arrival time.
    This prevents pathological starvation without heavy reordering logic.
    """
    q = s.wait_q[pri]
    if not q:
        return None

    # For high priorities: strict FIFO
    if pri != Priority.BATCH_PIPELINE or len(q) <= 1:
        return q.pop(0)

    # For batch: pick oldest among the first K to provide gentle aging.
    k = min(batch_scan_k, len(q))
    best_idx = 0
    best_t = None
    for i in range(k):
        pid_i = q[i].pipeline_id
        t = s.arrival_time.get(pid_i, 0)
        if best_t is None or t < best_t:
            best_t = t
            best_idx = i
    return q.pop(best_idx)


def _compute_request(s, pool, pipeline, avail_cpu, avail_ram):
    """Compute (cpu, ram) for this pipeline in this pool, or (0, 0) if we should wait."""
    pid = pipeline.pipeline_id
    pri = pipeline.priority

    # If we already learned we need at least X RAM (OOM feedback), don't schedule below it.
    min_needed = int(s.min_ram_needed.get(pid, 0) or 0)
    if min_needed > 0 and avail_ram < min_needed:
        return 0, 0

    # CPU sizing: simple per-priority target with mild scaling feedback.
    cpu_scale = float(s.cpu_scale.get(pid, 1.0) or 1.0)
    target_cpu = int(round(s.cpu_target.get(pri, 1) * cpu_scale))
    target_cpu = max(1, target_cpu)
    cpu = min(int(avail_cpu), int(getattr(pool, "max_cpu_pool", avail_cpu)), target_cpu)
    if cpu <= 0:
        return 0, 0

    # RAM sizing: per-priority baseline fraction of pool capacity with OOM scaling.
    max_ram = int(getattr(pool, "max_ram_pool", avail_ram))
    base_frac = float(s.base_ram_frac.get(pri, 0.20))
    base_ram = int(max(1, round(max_ram * base_frac)))

    ram_scale = float(s.ram_scale.get(pid, 1.0) or 1.0)
    ram = int(round(base_ram * ram_scale))
    ram = max(1, min(ram, max_ram, int(avail_ram)))
    if ram <= 0:
        return 0, 0

    # If we have a min-needed bound, ensure we meet it.
    if min_needed > 0 and ram < min_needed:
        return 0, 0

    return int(cpu), int(ram)


def _list_pool_containers(pool):
    """Best-effort extraction of running containers from a pool for preemption."""
    # Simulator may expose one of these. We intentionally keep this permissive.
    for attr in ("running_containers", "containers", "active_containers"):
        cs = getattr(pool, attr, None)
        if cs is not None:
            try:
                return list(cs)
            except Exception:
                pass
    return []


def _container_id(c):
    for attr in ("container_id", "id", "cid"):
        v = getattr(c, attr, None)
        if v is not None:
            return v
    return None


def _container_priority(c):
    return getattr(c, "priority", None)


def _container_resources(c):
    cpu = getattr(c, "cpu", None)
    ram = getattr(c, "ram", None)
    try:
        cpu = int(cpu) if cpu is not None else 0
    except Exception:
        cpu = 0
    try:
        ram = int(ram) if ram is not None else 0
    except Exception:
        ram = 0
    return cpu, ram


def _maybe_preempt(s, pool_id, need_cpu, need_ram, protect_priority):
    """Suspend low-priority containers in this pool to free up resources for protect_priority.

    This is best-effort: it only works if the simulator exposes container lists with priority/cpu/ram.
    """
    pool = s.executor.pools[pool_id]
    containers = _list_pool_containers(pool)
    if not containers:
        return []

    # Determine which priorities we are allowed to preempt.
    # Protect INTERACTIVE most strongly; then QUERY.
    def rank(pri):
        if pri == Priority.INTERACTIVE:
            return 0
        if pri == Priority.QUERY:
            return 1
        return 2

    protect_rank = rank(protect_priority)

    # Candidate containers: strictly lower priority than what we're trying to protect.
    candidates = []
    for c in containers:
        cid = _container_id(c)
        cpri = _container_priority(c)
        if cid is None or cpri is None:
            continue
        if rank(cpri) <= protect_rank:
            continue

        last = s.preempted_at.get(cid, -10**9)
        if (s.clock - last) < s.preempt_cooldown_ticks:
            continue

        ccpu, cram = _container_resources(c)
        candidates.append((rank(cpri), -cram, -ccpu, cid, ccpu, cram))  # preempt lowest prio, biggest first

    candidates.sort()
    suspensions = []

    freed_cpu = 0
    freed_ram = 0
    for _, _, _, cid, ccpu, cram in candidates:
        if freed_cpu >= need_cpu and freed_ram >= need_ram:
            break
        suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
        s.preempted_at[cid] = s.clock
        freed_cpu += max(0, ccpu)
        freed_ram += max(0, cram)

    return suspensions


@register_scheduler(key="scheduler_high_047")
def scheduler_high_047_scheduler(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """Priority-aware scheduler with small, obvious improvements over naive FIFO."""
    s.clock += 1

    suspensions = []
    assignments = []

    # Ingest new pipelines (avoid duplicates).
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.known_pipeline_ids or pid in s.fatal_pipelines:
            continue
        s.known_pipeline_ids.add(pid)
        s.pipeline_by_id[pid] = p
        s.arrival_time[pid] = s.clock

        # Initialize sizing state
        s.ram_scale[pid] = 1.0
        s.cpu_scale[pid] = 1.0
        s.min_ram_needed[pid] = 0
        s.oom_retry_count[pid] = 0

        # Build op->pipeline mapping so we can interpret results.
        for op in _iter_pipeline_ops(p):
            s.op_to_pipeline[id(op)] = pid

        s.wait_q.get(p.priority, s.wait_q[Priority.BATCH_PIPELINE]).append(p)

    # Process execution results to adapt sizing + decide retry vs fatal.
    for r in results:
        ops = getattr(r, "ops", None)
        if not ops:
            continue

        # Try to find pipeline_id from any op in the result.
        pid = None
        for op in ops:
            pid = s.op_to_pipeline.get(id(op))
            if pid is not None:
                break
        if pid is None:
            continue

        # If already fatal, ignore
        if pid in s.fatal_pipelines:
            continue

        if r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                # Bounded OOM retries: increase RAM requirement and allow retrying FAILED ops.
                s.oom_retry_count[pid] = int(s.oom_retry_count.get(pid, 0) or 0) + 1
                if s.oom_retry_count[pid] <= s.oom_retry_limit:
                    s.oom_allowed.add(pid)

                    # Increase scaling factor (cap to avoid unbounded growth).
                    s.ram_scale[pid] = min(float(s.ram_scale.get(pid, 1.0) or 1.0) * 2.0, 16.0)

                    # Also record a hard minimum to avoid retrying below a known-bad size.
                    try:
                        pool = s.executor.pools[r.pool_id]
                        pool_max_ram = int(getattr(pool, "max_ram_pool", 0) or 0)
                    except Exception:
                        pool_max_ram = 0
                    prev_ram = int(getattr(r, "ram", 0) or 0)
                    needed = max(int(s.min_ram_needed.get(pid, 0) or 0), prev_ram * 2 if prev_ram > 0 else 0)
                    if pool_max_ram > 0:
                        needed = min(needed, pool_max_ram)
                    s.min_ram_needed[pid] = needed
                else:
                    s.fatal_pipelines.add(pid)
            else:
                # Non-OOM failures are treated as fatal for now (small, safe improvement).
                s.fatal_pipelines.add(pid)
        else:
            # Success: cautiously decay scaling back toward baseline to avoid permanent over-allocation.
            s.ram_scale[pid] = max(1.0, float(s.ram_scale.get(pid, 1.0) or 1.0) * 0.90)
            s.cpu_scale[pid] = max(1.0, float(s.cpu_scale.get(pid, 1.0) or 1.0) * 0.95)

            # If pipeline no longer has FAILED ops, clear OOM retry allowances and hard minimum.
            p = s.pipeline_by_id.get(pid)
            if p is not None:
                st = p.runtime_status()
                if _state_count(st, OperatorState.FAILED) == 0:
                    if pid in s.oom_allowed:
                        s.oom_allowed.remove(pid)
                    s.min_ram_needed[pid] = 0

    # Early exit only if there's truly nothing to do.
    has_waiting = any(s.wait_q[pri] for pri in (Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE))
    if not has_waiting and not pipelines and not results:
        return [], []

    # Determine if we should reserve headroom for high-priority arrivals.
    hi_waiting = bool(s.wait_q[Priority.INTERACTIVE] or s.wait_q[Priority.QUERY])

    # Sort pools by available headroom so we place latency-sensitive work in the best pool first.
    pool_ids = list(range(s.executor.num_pools))
    try:
        pool_ids.sort(
            key=lambda i: (
                int(getattr(s.executor.pools[i], "avail_cpu_pool", 0) or 0),
                int(getattr(s.executor.pools[i], "avail_ram_pool", 0) or 0),
            ),
            reverse=True,
        )
    except Exception:
        pass

    # Schedule loop: per pool, place high priority first, then batch with reservation.
    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = int(getattr(pool, "avail_cpu_pool", 0) or 0)
        avail_ram = int(getattr(pool, "avail_ram_pool", 0) or 0)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Helper: schedule a single op from a given priority, returning whether we made an assignment.
        def try_schedule_from_priority(pri, cpu_budget, ram_budget):
            if cpu_budget <= 0 or ram_budget <= 0:
                return 0, 0, False

            pipeline = _dequeue_next_pipeline(s, pri)
            if pipeline is None:
                return 0, 0, False

            drop, _done = _pipeline_done_or_drop(s, pipeline)
            if drop:
                # Don't requeue.
                return 0, 0, True  # "progress" because we mutated queue state

            ops = _eligible_ops_for_pipeline(s, pipeline)
            if not ops:
                # Not runnable now -> requeue to the end.
                s.wait_q[pri].append(pipeline)
                return 0, 0, True

            # Compute request; if it doesn't fit, keep it near the front to reduce latency.
            req_cpu, req_ram = _compute_request(s, pool, pipeline, cpu_budget, ram_budget)
            if req_cpu <= 0 or req_ram <= 0:
                s.wait_q[pri].insert(0, pipeline)
                return 0, 0, False

            if req_cpu > cpu_budget or req_ram > ram_budget:
                s.wait_q[pri].insert(0, pipeline)
                return 0, 0, False

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Requeue pipeline for future ops (round-robin).
            s.wait_q[pri].append(pipeline)
            return req_cpu, req_ram, True

        # 1) High-priority first: INTERACTIVE then QUERY.
        # Try a few times to avoid getting stuck on a single unrunnable pipeline.
        for pri in (Priority.INTERACTIVE, Priority.QUERY):
            spins = 0
            while spins < 8:
                spins += 1
                used_cpu, used_ram, progressed = try_schedule_from_priority(pri, avail_cpu, avail_ram)
                if used_cpu > 0:
                    avail_cpu -= used_cpu
                    avail_ram -= used_ram
                    if avail_cpu <= 0 or avail_ram <= 0:
                        break
                    continue

                # If we have pending high-priority work but can't fit it, attempt best-effort preemption.
                # We only do this when there is at least one waiting pipeline of that priority.
                if s.wait_q[pri] and progressed is False:
                    # Estimate what we'd like to run for that priority (as the "needed" request).
                    # Use current available as the baseline; preempt tries to free some additional headroom.
                    want_cpu = max(1, int(s.cpu_target.get(pri, 1)))
                    want_ram = max(1, int(round(int(getattr(pool, "max_ram_pool", avail_ram) or avail_ram) * float(s.base_ram_frac.get(pri, 0.3)))))
                    need_cpu = max(0, want_cpu - avail_cpu)
                    need_ram = max(0, want_ram - avail_ram)
                    if need_cpu > 0 or need_ram > 0:
                        suspensions.extend(_maybe_preempt(s, pool_id, need_cpu, need_ram, protect_priority=pri))
                break

        # 2) Batch: use leftover, but keep some headroom if high priority is waiting.
        if hi_waiting:
            max_cpu = int(getattr(pool, "max_cpu_pool", avail_cpu) or avail_cpu)
            max_ram = int(getattr(pool, "max_ram_pool", avail_ram) or avail_ram)
            reserve_cpu = max(1, int(round(max_cpu * float(s.reserve_frac))))
            reserve_ram = max(1, int(round(max_ram * float(s.reserve_frac))))
        else:
            reserve_cpu = 0
            reserve_ram = 0

        batch_cpu_budget = max(0, avail_cpu - reserve_cpu)
        batch_ram_budget = max(0, avail_ram - reserve_ram)

        spins = 0
        while spins < 16 and batch_cpu_budget > 0 and batch_ram_budget > 0:
            spins += 1
            used_cpu, used_ram, progressed = try_schedule_from_priority(
                Priority.BATCH_PIPELINE, batch_cpu_budget, batch_ram_budget
            )
            if used_cpu > 0:
                batch_cpu_budget -= used_cpu
                batch_ram_budget -= used_ram
                continue
            if not progressed:
                break

    return suspensions, assignments
