# policy_key: scheduler_medium_001
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 121.77
# generated_at: 2026-03-12T21:51:53.598472
@register_scheduler_init(key="scheduler_medium_001")
def scheduler_medium_001_init(s):
    """
    Priority-aware, work-conserving greedy scheduler with:
      - Per-priority queues (QUERY > INTERACTIVE > BATCH_PIPELINE)
      - Soft resource reservation to protect latency (batch cannot consume last headroom)
      - Simple OOM-aware retry sizing (increase RAM exponentially on OOM-like failures)
      - Better packing than naive: schedule multiple ops per tick across pools, not "all resources to one op"
    """
    import collections

    # Active pipeline registry and per-priority queues (store pipeline_ids for stable de-duping)
    s.pipeline_by_id = {}
    s.enqueued = set()
    s.queues = {
        Priority.QUERY: collections.deque(),
        Priority.INTERACTIVE: collections.deque(),
        Priority.BATCH_PIPELINE: collections.deque(),
    }

    # Per-op sizing overrides and failure counters (best-effort keys; simulator may vary op identity fields)
    # key: (pipeline_id, op_key) -> {"ram": int, "cpu": int}
    s.op_overrides = {}
    s.op_failures = {}  # key: (pipeline_id, op_key) -> int

    # Soft reservation: batch is not allowed to consume the last X% of each pool (protects tail latency)
    s.reserve_frac = 0.20

    # Base requests by priority (small, safe defaults; OOM logic bumps RAM when needed)
    # NOTE: units are whatever the simulator uses; these are conservative and intended to pack.
    s.base_request = {
        Priority.QUERY: (4, 8),          # (cpu, ram)
        Priority.INTERACTIVE: (2, 4),
        Priority.BATCH_PIPELINE: (1, 2),
    }

    # For high priority, if headroom exists, scale up a single op to reduce latency (still capped)
    s.highprio_scaleup_frac = 0.50      # try to take up to 50% of effective available resources
    s.highprio_cap_frac_of_pool = 0.75  # but don't take more than 75% of the pool for one op

    # Retry policy
    s.max_oom_retries = 3


def _priority_order():
    # Explicit ordering to avoid relying on enum comparability.
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _as_int_resource(x, minimum=1):
    try:
        xi = int(x)
    except Exception:
        xi = minimum
    return xi if xi >= minimum else minimum


def _op_identity(op):
    # Best-effort stable identity across ticks/results. Fall back to repr.
    for attr in ("op_id", "operator_id", "task_id", "name", "id"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return f"{attr}:{v}"
            except Exception:
                pass
    try:
        return repr(op)
    except Exception:
        return f"op@{id(op)}"


def _looks_like_oom(err):
    if err is None:
        return False
    try:
        s = str(err).lower()
    except Exception:
        return False
    # Common OOM-ish strings
    return ("oom" in s) or ("out of memory" in s) or ("memoryerror" in s) or ("cuda out of memory" in s)


def _effective_avail_for_priority(pool, avail_cpu, avail_ram, prio, reserve_frac):
    # Batch gets "effective" availability reduced by reservation; high priority can use full availability.
    if prio == Priority.BATCH_PIPELINE:
        reserve_cpu = max(0, int(pool.max_cpu_pool * reserve_frac))
        reserve_ram = max(0, int(pool.max_ram_pool * reserve_frac))
        return max(0, avail_cpu - reserve_cpu), max(0, avail_ram - reserve_ram)
    return avail_cpu, avail_ram


def _pick_pool_for_request(s, prio, req_cpu, req_ram, avail_cpu_by_pool, avail_ram_by_pool):
    # Choose a feasible pool with best "slack" after assignment, considering reservations.
    best_pool = None
    best_score = None

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        eff_cpu, eff_ram = _effective_avail_for_priority(
            pool,
            avail_cpu_by_pool[pool_id],
            avail_ram_by_pool[pool_id],
            prio,
            s.reserve_frac,
        )
        if eff_cpu < req_cpu or eff_ram < req_ram:
            continue

        # Score: prefer pools that keep more leftover after placement (reduce fragmentation),
        # but still prefer placing high-priority into roomier pools.
        slack_cpu = eff_cpu - req_cpu
        slack_ram = eff_ram - req_ram
        score = (slack_cpu + slack_ram)

        if best_pool is None or score > best_score:
            best_pool = pool_id
            best_score = score

    return best_pool


def _compute_request(s, pool, prio, pipeline_id, op):
    # Start from base request and apply per-op override (from OOM retries).
    base_cpu, base_ram = s.base_request.get(prio, (1, 1))
    base_cpu = min(_as_int_resource(base_cpu), _as_int_resource(pool.max_cpu_pool))
    base_ram = min(_as_int_resource(base_ram), _as_int_resource(pool.max_ram_pool))

    op_key = (pipeline_id, _op_identity(op))
    override = s.op_overrides.get(op_key)
    if override:
        cpu = min(_as_int_resource(override.get("cpu", base_cpu)), _as_int_resource(pool.max_cpu_pool))
        ram = min(_as_int_resource(override.get("ram", base_ram)), _as_int_resource(pool.max_ram_pool))
        return cpu, ram

    return base_cpu, base_ram


def _maybe_scale_up_for_latency(s, pool, prio, req_cpu, req_ram, eff_avail_cpu, eff_avail_ram):
    # For QUERY/INTERACTIVE, scale up if we have plenty of headroom to reduce runtime/latency.
    if prio not in (Priority.QUERY, Priority.INTERACTIVE):
        return req_cpu, req_ram

    cap_cpu = max(1, int(pool.max_cpu_pool * s.highprio_cap_frac_of_pool))
    cap_ram = max(1, int(pool.max_ram_pool * s.highprio_cap_frac_of_pool))

    target_cpu = max(req_cpu, int(eff_avail_cpu * s.highprio_scaleup_frac))
    target_ram = max(req_ram, int(eff_avail_ram * s.highprio_scaleup_frac))

    cpu = min(_as_int_resource(target_cpu), _as_int_resource(eff_avail_cpu), _as_int_resource(cap_cpu))
    ram = min(_as_int_resource(target_ram), _as_int_resource(eff_avail_ram), _as_int_resource(cap_ram))

    # Never reduce below base request
    cpu = max(cpu, req_cpu)
    ram = max(ram, req_ram)
    return cpu, ram


@register_scheduler(key="scheduler_medium_001")
def scheduler_medium_001_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into per-priority queues (de-duplicated).
      2) Process execution results to detect OOM and increase RAM for retries.
      3) Greedy scheduling loop:
         - Always try to schedule highest priority first
         - Place ops onto the best feasible pool (considering batch reservations)
         - Pack multiple ops per tick when capacity allows
    """
    # --- Ingest new pipelines ---
    for p in pipelines:
        s.pipeline_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s.enqueued:
            s.queues[p.priority].append(p.pipeline_id)
            s.enqueued.add(p.pipeline_id)

    # --- Process results: learn from OOMs and bump RAM for retries ---
    for r in results:
        if not r.failed():
            continue

        # Best-effort pipeline_id association (may or may not exist in simulator)
        pipeline_id = getattr(r, "pipeline_id", None)

        # Only treat OOM specially; otherwise we don't know how to recover deterministically.
        if not _looks_like_oom(getattr(r, "error", None)):
            continue

        # For each op in the failed container, bump RAM override for that (pipeline,op) if possible.
        for op in getattr(r, "ops", []) or []:
            if pipeline_id is None:
                # If we don't know the pipeline_id, we can't make a stable per-pipeline override.
                # Still, try a global-ish key by using "None" pipeline_id (best effort).
                pid = None
            else:
                pid = pipeline_id

            op_key = (pid, _op_identity(op))
            prev_fail = s.op_failures.get(op_key, 0) + 1
            s.op_failures[op_key] = prev_fail

            # Exponential backoff on RAM; CPU unchanged (OOM is usually RAM-driven).
            prev_override = s.op_overrides.get(op_key, {})
            prev_ram = prev_override.get("ram", getattr(r, "ram", 1) or 1)
            prev_cpu = prev_override.get("cpu", getattr(r, "cpu", 1) or 1)

            new_ram = max(_as_int_resource(prev_ram) * 2, _as_int_resource(getattr(r, "ram", 1) or 1) * 2)
            s.op_overrides[op_key] = {"cpu": _as_int_resource(prev_cpu), "ram": _as_int_resource(new_ram)}

    # Early exit: no changes that could affect decisions
    if not pipelines and not results:
        return [], []

    # Local view of remaining resources this tick (we "virtually" consume as we create assignments)
    avail_cpu_by_pool = []
    avail_ram_by_pool = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu_by_pool.append(_as_int_resource(pool.avail_cpu_pool, minimum=0))
        avail_ram_by_pool.append(_as_int_resource(pool.avail_ram_pool, minimum=0))

    suspensions = []
    assignments = []

    # Greedy multi-assignment loop
    made_progress = True
    while made_progress:
        made_progress = False

        for prio in _priority_order():
            q = s.queues.get(prio)
            if not q:
                continue

            # Bounded scan to avoid infinite loops: try each queued pipeline at most once per pass.
            scan_n = len(q)
            for _ in range(scan_n):
                pipeline_id = q.popleft()
                p = s.pipeline_by_id.get(pipeline_id)

                # Pipeline might have been removed/unknown; drop it.
                if p is None:
                    s.enqueued.discard(pipeline_id)
                    continue

                status = p.runtime_status()

                # Drop completed pipelines
                if status.is_pipeline_successful():
                    s.pipeline_by_id.pop(pipeline_id, None)
                    s.enqueued.discard(pipeline_id)
                    continue

                # If there are FAILED ops, only keep retrying if we have an OOM-based override for them;
                # otherwise, drop to avoid deadlock / infinite retries on deterministic failures.
                has_failed = getattr(status, "state_counts", {}).get(OperatorState.FAILED, 0) > 0
                if has_failed:
                    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
                    retriable = False
                    for op in failed_ops:
                        # Check both exact pipeline_id key and fallback None key (if results lacked pipeline_id)
                        if (pipeline_id, _op_identity(op)) in s.op_overrides or (None, _op_identity(op)) in s.op_overrides:
                            # Respect max retries if we have failure counts for this key
                            k1 = (pipeline_id, _op_identity(op))
                            k2 = (None, _op_identity(op))
                            fcnt = max(s.op_failures.get(k1, 0), s.op_failures.get(k2, 0))
                            if fcnt <= s.max_oom_retries:
                                retriable = True
                            break
                    if not retriable:
                        s.pipeline_by_id.pop(pipeline_id, None)
                        s.enqueued.discard(pipeline_id)
                        continue

                # Find a single ready op (keeps scheduling atom simple and avoids resource hogging)
                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    # Not runnable now; keep it in rotation
                    q.append(pipeline_id)
                    continue

                op = op_list[0]

                # Try to place this op: compute minimal request per candidate pool, then choose best feasible pool.
                # We'll start with per-pool base/override request, pick best pool for that request,
                # then possibly scale up for high priority if the chosen pool has headroom.
                feasible_choice = None  # (pool_id, cpu, ram)
                best_score = None

                for pool_id in range(s.executor.num_pools):
                    pool = s.executor.pools[pool_id]
                    eff_cpu, eff_ram = _effective_avail_for_priority(
                        pool, avail_cpu_by_pool[pool_id], avail_ram_by_pool[pool_id], prio, s.reserve_frac
                    )
                    if eff_cpu <= 0 or eff_ram <= 0:
                        continue

                    # Minimal request for this pool (bounded by pool max)
                    req_cpu, req_ram = _compute_request(s, pool, prio, pipeline_id, op)
                    req_cpu = min(req_cpu, _as_int_resource(pool.max_cpu_pool))
                    req_ram = min(req_ram, _as_int_resource(pool.max_ram_pool))

                    if eff_cpu < req_cpu or eff_ram < req_ram:
                        continue

                    # Prefer roomier pools (more headroom for scale-up / less fragmentation)
                    score = eff_cpu + eff_ram
                    if feasible_choice is None or score > best_score:
                        feasible_choice = (pool_id, req_cpu, req_ram, eff_cpu, eff_ram)
                        best_score = score

                if feasible_choice is None:
                    # Can't place anywhere right now; keep it queued
                    q.append(pipeline_id)
                    continue

                pool_id, req_cpu, req_ram, eff_cpu, eff_ram = feasible_choice
                pool = s.executor.pools[pool_id]

                # Optional latency scale-up for high priority
                cpu, ram = _maybe_scale_up_for_latency(s, pool, prio, req_cpu, req_ram, eff_cpu, eff_ram)

                # Final feasibility check against the *effective* available; if scaled too far, clamp.
                cpu = min(cpu, eff_cpu)
                ram = min(ram, eff_ram)
                if cpu < req_cpu or ram < req_ram:
                    # If clamping broke the minimum request, don't schedule.
                    q.append(pipeline_id)
                    continue

                # Create assignment
                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=_as_int_resource(cpu),
                        ram=_as_int_resource(ram),
                        priority=prio,
                        pool_id=pool_id,
                        pipeline_id=pipeline_id,
                    )
                )

                # Virtually consume from real availability (not effective), since that's what the pool tracks.
                avail_cpu_by_pool[pool_id] = max(0, avail_cpu_by_pool[pool_id] - _as_int_resource(cpu))
                avail_ram_by_pool[pool_id] = max(0, avail_ram_by_pool[pool_id] - _as_int_resource(ram))

                # Re-enqueue pipeline to allow future ops (round-robin within its priority)
                q.append(pipeline_id)

                made_progress = True

                # If this pool is exhausted for practical purposes, continue scanning; other pools may still have room.
                # (No "break": we want to keep packing across pipelines while we can.)
            # end scan loop
        # end prio loop

    return suspensions, assignments
