# policy_key: scheduler_est_027
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.077426
# generation_seconds: 193.10
# generated_at: 2026-03-31T19:12:46.721602
@register_scheduler_init(key="scheduler_est_027")
def scheduler_est_027_init(s):
    """
    Priority-aware, right-sized, multi-assignment scheduler.

    Improvements over naive FIFO:
      - Separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH_PIPELINE)
      - Pack multiple ops per pool per tick (not just one)
      - Right-size RAM from op.estimate.mem_peak_gb (+ safety margin) instead of taking the whole pool
      - Give higher priority ops more CPU (bounded) to reduce latency
      - Keep headroom by limiting batch consumption when high-priority work is waiting
      - Basic retry-on-failure with adaptive RAM boosting (especially for OOM-like errors)
    """
    from collections import deque

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Track pipelines we've already enqueued (avoid duplicates)
    s.known_pipelines = set()

    # Per-op learning keyed by id(op): RAM boost and failure tracking
    # Fields: boost, fails, last_ram, last_cpu, pipeline_id, priority
    s.op_meta = {}

    # Track pipeline failure counts to eventually drop "hopeless" pipelines (avoid sim deadlocks)
    s.pipeline_fail_counts = {}
    s.dropped_pipelines = set()

    # Tuning knobs (kept simple on purpose)
    s.max_pipeline_failures = 6
    s.max_op_failures = 4

    # Starting safety factors by priority (RAM = estimate * safety)
    s.base_ram_safety = {
        Priority.QUERY: 1.25,
        Priority.INTERACTIVE: 1.30,
        Priority.BATCH_PIPELINE: 1.15,
    }
    s.ram_boost_mult_oom = 1.60
    s.ram_boost_mult_other = 1.25
    s.ram_boost_cap = 8.0
    s.ram_boost_floor = 1.05

    # CPU caps by priority (bounds; actual cpu is min(cap, avail))
    s.cpu_cap = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 6.0,
        Priority.BATCH_PIPELINE: 4.0,
    }
    s.min_cpu = 1.0

    # Minimum RAM per op (GB) to avoid pathological tiny allocations
    s.min_ram_gb = 0.25

    # How aggressively to protect headroom from batch when high-priority is waiting
    # (applied per pool as a reservation fraction of pool max)
    s.batch_reserve_frac_single_pool = 0.50
    s.batch_reserve_frac_multi_pool = 0.25


def _is_high_priority(priority):
    return priority in (Priority.QUERY, Priority.INTERACTIVE)


def _err_looks_like_oom(err) -> bool:
    if err is None:
        return False
    s = str(err).lower()
    return ("oom" in s) or ("out of memory" in s) or ("memoryerror" in s)


def _get_mem_est_gb(op, default_gb: float) -> float:
    # Best-effort: simulator guarantees op.estimate.mem_peak_gb, but stay defensive.
    est = getattr(op, "estimate", None)
    v = getattr(est, "mem_peak_gb", None) if est is not None else None
    try:
        if v is None:
            return float(default_gb)
        v = float(v)
        return v if v > 0 else float(default_gb)
    except Exception:
        return float(default_gb)


def _queue_cleanup_inplace(s, q):
    # Remove completed and dropped pipelines; keep others in order.
    n = len(q)
    for _ in range(n):
        p = q.popleft()
        if p.pipeline_id in s.dropped_pipelines:
            continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        q.append(p)


def _next_fittable_op_from_queue(s, q, planned_ops, avail_cpu, avail_ram, pool_max_cpu, pool_max_ram):
    """
    Rotate a single FIFO queue to find the first runnable op that fits the available resources.
    Returns (pipeline, op, cpu_req, ram_req) or (None, None, None, None).
    """
    n = len(q)
    for _ in range(n):
        p = q.popleft()

        if p.pipeline_id in s.dropped_pipelines:
            continue

        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue

        # If we've seen too many failures, drop pipeline to avoid endless retries
        if s.pipeline_fail_counts.get(p.pipeline_id, 0) > s.max_pipeline_failures:
            s.dropped_pipelines.add(p.pipeline_id)
            continue

        # Find an assignable op that's not already planned this tick
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        op = None
        for candidate in ops:
            if id(candidate) not in planned_ops:
                op = candidate
                break

        if op is None:
            q.append(p)
            continue

        # Compute resource request for this op
        meta = s.op_meta.get(id(op))
        if meta is None:
            meta = {
                "boost": s.base_ram_safety.get(p.priority, 1.2),
                "fails": 0,
                "last_ram": None,
                "last_cpu": None,
                "pipeline_id": p.pipeline_id,
                "priority": p.priority,
            }
            s.op_meta[id(op)] = meta
        else:
            # Refresh backrefs (helps tie results back to pipelines even if meta was created from results)
            meta["pipeline_id"] = p.pipeline_id
            meta["priority"] = p.priority

        # CPU: give higher-priority more (bounded), but never exceed available
        cpu_cap = min(float(pool_max_cpu), float(s.cpu_cap.get(p.priority, 4.0)))
        cpu_req = min(float(avail_cpu), max(float(s.min_cpu), cpu_cap))

        # RAM: estimate * (base safety * learned boost), with floors/caps
        mem_est = _get_mem_est_gb(op, default_gb=max(s.min_ram_gb, 0.5))
        boost = float(meta.get("boost", s.base_ram_safety.get(p.priority, 1.2)))
        ram_req = max(s.min_ram_gb, mem_est * boost)

        # If this op previously failed, make sure we actually increase from last attempt (if known)
        last_ram = meta.get("last_ram")
        if last_ram is not None and meta.get("fails", 0) > 0:
            try:
                ram_req = max(ram_req, float(last_ram) * 1.15)
            except Exception:
                pass

        # Never request more than pool can provide
        ram_req = min(float(pool_max_ram), float(ram_req))

        # Fit check: must fit in currently available resources
        if cpu_req <= avail_cpu and ram_req <= avail_ram and avail_cpu >= s.min_cpu and avail_ram >= s.min_ram_gb:
            q.append(p)  # keep pipeline in queue for future ops
            return p, op, cpu_req, ram_req

        # Doesn't fit right now; keep pipeline in queue and try others
        q.append(p)

    return None, None, None, None


@register_scheduler(key="scheduler_est_027")
def scheduler_est_027(s, results: list, pipelines: list):
    """
    Scheduling step:
      1) Enqueue new pipelines into per-priority queues.
      2) Learn from execution results (boost RAM on OOM-like failures).
      3) Clean queues (remove completed/dropped).
      4) Plan assignments:
           - Phase A: schedule high-priority ops first across pools, packing multiple per pool.
           - Phase B: schedule batch ops, but keep headroom if high-priority is waiting.
    """
    # Fast path
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # --- 1) Enqueue new pipelines (dedupe) ---
    for p in pipelines:
        if p.pipeline_id in s.known_pipelines or p.pipeline_id in s.dropped_pipelines:
            continue
        s.known_pipelines.add(p.pipeline_id)
        s.pipeline_fail_counts.setdefault(p.pipeline_id, 0)
        s.queues[p.priority].append(p)

    # --- 2) Learn from results (simple adaptive RAM boosting) ---
    for r in results:
        if not getattr(r, "ops", None):
            continue

        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = False

        for op in r.ops:
            oid = id(op)
            meta = s.op_meta.get(oid)
            if meta is None:
                # We may see results for ops before we've created meta (be defensive)
                meta = {
                    "boost": s.base_ram_safety.get(getattr(r, "priority", Priority.BATCH_PIPELINE), 1.2),
                    "fails": 0,
                    "last_ram": None,
                    "last_cpu": None,
                    "pipeline_id": None,
                    "priority": getattr(r, "priority", Priority.BATCH_PIPELINE),
                }
                s.op_meta[oid] = meta

            # Record last allocation (useful for monotonic ramping)
            try:
                meta["last_ram"] = float(getattr(r, "ram", meta.get("last_ram") or 0.0))
            except Exception:
                pass
            try:
                meta["last_cpu"] = float(getattr(r, "cpu", meta.get("last_cpu") or 0.0))
            except Exception:
                pass

            if failed:
                meta["fails"] = int(meta.get("fails", 0)) + 1

                # Tie failures back to a pipeline if we can
                pid = meta.get("pipeline_id")
                if pid is not None:
                    s.pipeline_fail_counts[pid] = s.pipeline_fail_counts.get(pid, 0) + 1
                    if s.pipeline_fail_counts[pid] > s.max_pipeline_failures:
                        s.dropped_pipelines.add(pid)

                # Boost RAM more aggressively for OOM-like failures
                err = getattr(r, "error", None)
                if _err_looks_like_oom(err):
                    meta["boost"] = min(s.ram_boost_cap, float(meta.get("boost", 1.2)) * s.ram_boost_mult_oom)
                else:
                    meta["boost"] = min(s.ram_boost_cap, float(meta.get("boost", 1.2)) * s.ram_boost_mult_other)

                # If an op seems hopeless, prefer dropping its pipeline rather than looping forever
                if meta["fails"] > s.max_op_failures:
                    pid = meta.get("pipeline_id")
                    if pid is not None:
                        s.dropped_pipelines.add(pid)
            else:
                # Gentle decay toward a safe floor after success (keeps boosts from staying huge forever)
                try:
                    meta["boost"] = max(s.ram_boost_floor, float(meta.get("boost", 1.2)) * 0.97)
                except Exception:
                    meta["boost"] = max(s.ram_boost_floor, s.base_ram_safety.get(meta.get("priority"), 1.2))

    # --- 3) Cleanup queues ---
    for q in s.queues.values():
        _queue_cleanup_inplace(s, q)

    # Detect whether we should protect headroom from batch right now
    high_waiting = (len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE])) > 0

    # Precompute per-pool available resources we can plan against this tick
    num_pools = s.executor.num_pools
    avail_cpu = {}
    avail_ram = {}
    max_cpu = {}
    max_ram = {}
    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu[pool_id] = float(pool.avail_cpu_pool)
        avail_ram[pool_id] = float(pool.avail_ram_pool)
        max_cpu[pool_id] = float(pool.max_cpu_pool)
        max_ram[pool_id] = float(pool.max_ram_pool)

    planned_ops = set()

    # --- 4A) Phase A: schedule high-priority first (QUERY then INTERACTIVE) ---
    # Favor pool 0 a bit by iterating in natural order; still allow high-priority to spill to other pools.
    for pool_id in range(num_pools):
        # Pack as many high-priority ops as fit
        while True:
            if avail_cpu[pool_id] < s.min_cpu or avail_ram[pool_id] < s.min_ram_gb:
                break

            chosen = None
            for pri in (Priority.QUERY, Priority.INTERACTIVE):
                p, op, cpu_req, ram_req = _next_fittable_op_from_queue(
                    s=s,
                    q=s.queues[pri],
                    planned_ops=planned_ops,
                    avail_cpu=avail_cpu[pool_id],
                    avail_ram=avail_ram[pool_id],
                    pool_max_cpu=max_cpu[pool_id],
                    pool_max_ram=max_ram[pool_id],
                )
                if op is not None:
                    chosen = (p, op, cpu_req, ram_req)
                    break

            if chosen is None:
                break

            p, op, cpu_req, ram_req = chosen
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            planned_ops.add(id(op))
            avail_cpu[pool_id] -= float(cpu_req)
            avail_ram[pool_id] -= float(ram_req)

    # --- 4B) Phase B: schedule batch, but keep headroom if high-priority is waiting ---
    # In multi-pool setups, this mostly protects latency by preventing batch from filling *every* pool.
    # In single-pool setups, reservation is stronger because there's no other place for high-priority to run.
    if num_pools <= 1:
        reserve_frac = s.batch_reserve_frac_single_pool
    else:
        reserve_frac = s.batch_reserve_frac_multi_pool

    for pool_id in range(num_pools):
        # Optional: if we have multiple pools and high work is waiting, keep pool 0 extra clean for latency.
        if num_pools > 1 and high_waiting and pool_id == 0:
            continue

        while True:
            if avail_cpu[pool_id] < s.min_cpu or avail_ram[pool_id] < s.min_ram_gb:
                break

            # Enforce reservation only when high-priority is actually waiting
            if high_waiting:
                reserve_cpu = reserve_frac * max_cpu[pool_id]
                reserve_ram = reserve_frac * max_ram[pool_id]
                eff_cpu = avail_cpu[pool_id] - reserve_cpu
                eff_ram = avail_ram[pool_id] - reserve_ram
                if eff_cpu < s.min_cpu or eff_ram < s.min_ram_gb:
                    break
            else:
                eff_cpu = avail_cpu[pool_id]
                eff_ram = avail_ram[pool_id]

            p, op, cpu_req, ram_req = _next_fittable_op_from_queue(
                s=s,
                q=s.queues[Priority.BATCH_PIPELINE],
                planned_ops=planned_ops,
                avail_cpu=eff_cpu,
                avail_ram=eff_ram,
                pool_max_cpu=max_cpu[pool_id],
                pool_max_ram=max_ram[pool_id],
            )
            if op is None:
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            planned_ops.add(id(op))
            avail_cpu[pool_id] -= float(cpu_req)
            avail_ram[pool_id] -= float(ram_req)

    return suspensions, assignments
