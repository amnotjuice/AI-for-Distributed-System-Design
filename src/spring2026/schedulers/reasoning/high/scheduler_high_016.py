# policy_key: scheduler_high_016
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.180191
# generation_seconds: 169.95
# generated_at: 2026-04-10T01:04:05.730148
@register_scheduler_init(key="scheduler_high_016")
def scheduler_high_016_init(s):
    """
    Priority-first, OOM-averse scheduler.

    Core ideas (small improvements over FIFO):
      1) Strict priority order: QUERY > INTERACTIVE > BATCH, but allow batch to use idle capacity.
      2) OOM recovery: if an operator fails with an OOM-like error, retry it with higher RAM next time.
      3) Light fairness: round-robin within each priority class to avoid starvation.
      4) Conservative batching: optionally reserve some headroom for ready high-priority work when scheduling batch.
    """
    # Per-priority pipeline queues (round-robin via indices).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_index = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.pipeline_ids = set()  # de-dup for safety

    # Per-operator adaptive sizing / retry bookkeeping (keyed by id(op)).
    s.op_failures = {}          # total failures observed
    s.op_oom_failures = {}      # oom-like failures observed
    s.op_min_ram = {}           # minimum RAM to request next time (learned from failures)
    s.op_last_ram = {}          # last requested RAM
    s.op_last_cpu = {}          # last requested CPU
    s.op_to_pipeline = {}       # op_id -> pipeline_id (for potential future use)

    # Limits and heuristics.
    s.max_assignments_per_pool = 2

    # Base fractions of pool capacity to allocate per container by priority.
    # (Bigger for high priority to reduce latency + reduce OOM probability.)
    s.base_cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,
    }
    s.base_ram_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.30,
    }

    # When high-priority work is READY, keep some headroom by reducing how much batch can consume.
    s.batch_reserve_cpu_frac = 0.25
    s.batch_reserve_ram_frac = 0.25

    # Retry caps (avoid infinite thrashing). More patient for high priority.
    s.max_retries = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 6,
        Priority.BATCH_PIPELINE: 4,
    }


def _sch016_is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _sch016_safe_priority(p):
    # Guard against unexpected/unknown priority values.
    if getattr(p, "priority", None) in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        return p.priority
    return Priority.BATCH_PIPELINE


def _sch016_pipeline_ready_op(p):
    """
    Returns a single ready operator for pipeline p, or None if none are assignable now.
    """
    status = p.runtime_status()
    if status.is_pipeline_successful():
        return None
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[0]


def _sch016_any_ready_in_queue(queue):
    for p in queue:
        op = _sch016_pipeline_ready_op(p)
        if op is not None:
            return True
    return False


def _sch016_clean_queue(queue):
    """
    Drop completed pipelines from a queue. Keep failed/incomplete pipelines for potential retry.
    """
    cleaned = []
    for p in queue:
        try:
            if p.runtime_status().is_pipeline_successful():
                continue
        except Exception:
            # If runtime_status() is temporarily unavailable, keep it.
            pass
        cleaned.append(p)
    return cleaned


def _sch016_pick_next_pipeline_rr(s, priority, scheduled_pipeline_ids):
    """
    Round-robin pick of a pipeline with a currently-ready operator.
    Returns (pipeline, op) or (None, None).
    """
    q = s.queues[priority]
    n = len(q)
    if n == 0:
        return None, None

    start = s.rr_index.get(priority, 0)
    if start >= n:
        start = 0

    for step in range(n):
        idx = (start + step) % n
        p = q[idx]
        if p.pipeline_id in scheduled_pipeline_ids:
            continue
        op = _sch016_pipeline_ready_op(p)
        if op is None:
            continue
        # Advance rr pointer past the chosen index for next time.
        s.rr_index[priority] = (idx + 1) % max(1, n)
        return p, op

    # No ready work found; keep rr pointer stable.
    s.rr_index[priority] = start
    return None, None


def _sch016_compute_request(s, pool, priority, op, avail_cpu, avail_ram, high_ready):
    """
    Compute (cpu, ram) to request for this op given pool capacity and available resources.
    Return None if not schedulable (e.g., cannot satisfy learned minimum RAM).
    """
    # Apply headroom reservations only when placing BATCH and high-priority work is ready.
    eff_cpu = float(avail_cpu)
    eff_ram = float(avail_ram)
    if priority == Priority.BATCH_PIPELINE and high_ready:
        eff_cpu = eff_cpu - float(pool.max_cpu_pool) * float(s.batch_reserve_cpu_frac)
        eff_ram = eff_ram - float(pool.max_ram_pool) * float(s.batch_reserve_ram_frac)

    if eff_cpu <= 0.0 or eff_ram <= 0.0:
        return None

    # Target shares by priority; clamp to effective available.
    base_cpu = float(pool.max_cpu_pool) * float(s.base_cpu_frac.get(priority, 0.5))
    base_ram = float(pool.max_ram_pool) * float(s.base_ram_frac.get(priority, 0.3))

    # Learned RAM floor from prior failures (especially OOM).
    op_id = id(op)
    learned_min_ram = float(s.op_min_ram.get(op_id, 0.0))
    req_ram = max(base_ram, learned_min_ram)

    # If we have a last-known RAM and multiple OOMs, bias upward gently.
    # (Avoids repeated OOM retries that would hurt end-to-end latency.)
    oom_cnt = int(s.op_oom_failures.get(op_id, 0))
    if oom_cnt > 0:
        bump = 1.0 + min(1.0, 0.35 * oom_cnt)  # up to 2.0x at high counts
        last_ram = float(s.op_last_ram.get(op_id, 0.0))
        req_ram = max(req_ram, last_ram * bump)

    # Decide CPU: give a healthy chunk to high priority; batch uses moderate CPU.
    req_cpu = max(1.0, base_cpu)

    # Clamp to effective availability.
    req_ram = min(req_ram, eff_ram)
    req_cpu = min(req_cpu, eff_cpu)

    # If learned minimum RAM is above what we can currently provide, skip (avoid likely OOM).
    if learned_min_ram > eff_ram:
        return None

    if req_cpu <= 0.0 or req_ram <= 0.0:
        return None

    return req_cpu, req_ram


@register_scheduler(key="scheduler_high_016")
def scheduler_high_016(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into per-priority queues.
      - Update per-op retry/RAM hints from execution results (especially OOMs).
      - For each pool, assign up to N operators, prioritizing QUERY then INTERACTIVE then BATCH.
      - Let batch use capacity when no high-priority ops are ready; otherwise keep headroom.
    """
    # Enqueue new pipelines (de-dup by pipeline_id).
    for p in pipelines:
        if p.pipeline_id in s.pipeline_ids:
            continue
        pr = _sch016_safe_priority(p)
        s.queues[pr].append(p)
        s.pipeline_ids.add(p.pipeline_id)

    # Update sizing hints from recent results.
    for r in results:
        # If result corresponds to multiple ops, treat them similarly (common in fused execution).
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = bool(getattr(r, "error", None))

        for op in getattr(r, "ops", []) or []:
            op_id = id(op)

            # Track last requested resources (useful for adaptive bumps).
            try:
                s.op_last_ram[op_id] = float(getattr(r, "ram", 0.0) or 0.0)
                s.op_last_cpu[op_id] = float(getattr(r, "cpu", 0.0) or 0.0)
            except Exception:
                pass

            if failed:
                s.op_failures[op_id] = int(s.op_failures.get(op_id, 0)) + 1

                err = getattr(r, "error", None)
                if _sch016_is_oom_error(err):
                    s.op_oom_failures[op_id] = int(s.op_oom_failures.get(op_id, 0)) + 1

                    # Increase learned minimum RAM for next attempt.
                    # Use a multiplicative bump from the last attempt's RAM.
                    last_ram = float(getattr(r, "ram", 0.0) or 0.0)
                    bumped = max(last_ram * 1.7, last_ram + 1.0)  # ensure some movement even if tiny units
                    prev = float(s.op_min_ram.get(op_id, 0.0))
                    s.op_min_ram[op_id] = max(prev, bumped)
            else:
                # On success, no need to shrink learned minima aggressively; keep as-is.
                pass

    # If nothing changed, likely no new opportunities to schedule.
    if not pipelines and not results:
        return [], []

    # Clean completed pipelines out of queues; refresh pipeline_ids set.
    new_pipeline_ids = set()
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        s.queues[pr] = _sch016_clean_queue(s.queues[pr])
        for p in s.queues[pr]:
            new_pipeline_ids.add(p.pipeline_id)
        # Keep rr indices in bounds.
        if s.rr_index.get(pr, 0) >= len(s.queues[pr]):
            s.rr_index[pr] = 0
    s.pipeline_ids = new_pipeline_ids

    suspensions = []
    assignments = []

    # Determine if any high-priority work is actually READY (not just queued).
    query_ready = _sch016_any_ready_in_queue(s.queues[Priority.QUERY])
    interactive_ready = _sch016_any_ready_in_queue(s.queues[Priority.INTERACTIVE])
    high_ready = query_ready or interactive_ready

    # For fairness within a tick: don't schedule the same pipeline multiple times across pools.
    scheduled_pipeline_ids = set()

    # Heuristic: if multiple pools exist, prefer keeping pool 0 focused on high priority when high is ready.
    # (No hard partitioning; other pools can still run high priority.)
    num_pools = s.executor.num_pools

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        max_to_place = int(s.max_assignments_per_pool)
        placed = 0

        while placed < max_to_place and avail_cpu > 0.0 and avail_ram > 0.0:
            # Priority order; suppress batch on pool 0 when high-priority is ready.
            if pool_id == 0 and high_ready:
                pr_order = [Priority.QUERY, Priority.INTERACTIVE]
            else:
                pr_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

            made = False
            for pr in pr_order:
                # Skip batch if high-priority is ready and we cannot keep headroom on this pool.
                p, op = _sch016_pick_next_pipeline_rr(s, pr, scheduled_pipeline_ids)
                if p is None or op is None:
                    continue

                # Retry cap check (avoid endless thrashing).
                op_id = id(op)
                failures = int(s.op_failures.get(op_id, 0))
                cap = int(s.max_retries.get(pr, 4))
                if failures > cap:
                    # Do not schedule this op again; move on to other pipelines.
                    continue

                req = _sch016_compute_request(
                    s=s,
                    pool=pool,
                    priority=pr,
                    op=op,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    high_ready=high_ready,
                )
                if req is None:
                    continue

                cpu_req, ram_req = req
                if cpu_req <= 0.0 or ram_req <= 0.0:
                    continue
                if cpu_req > avail_cpu + 1e-9 or ram_req > avail_ram + 1e-9:
                    continue

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

                # Bookkeeping for adaptive behavior.
                s.op_to_pipeline[op_id] = p.pipeline_id
                s.op_last_cpu[op_id] = float(cpu_req)
                s.op_last_ram[op_id] = float(ram_req)

                avail_cpu -= float(cpu_req)
                avail_ram -= float(ram_req)
                placed += 1
                scheduled_pipeline_ids.add(p.pipeline_id)
                made = True
                break  # re-evaluate priority order with updated availabilities

            if not made:
                break

    return suspensions, assignments
