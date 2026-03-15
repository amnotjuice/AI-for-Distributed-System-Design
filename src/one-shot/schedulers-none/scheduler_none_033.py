# policy_key: scheduler_none_033
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.039409
# generation_seconds: 41.98
# generated_at: 2026-03-14T02:05:34.356988
@register_scheduler_init(key="scheduler_none_033")
def scheduler_none_033_init(s):
    """Priority-aware FIFO with conservative preemption and simple OOM backoff.

    Small, obvious improvements over naive FIFO:
      1) Separate queues per priority; always try higher priority first.
      2) Avoid over-allocating: cap per-assignment CPU/RAM to a fraction of pool.
      3) On OOM, retry the pipeline later with increased RAM target (multiplicative backoff).
      4) If a high-priority op can't fit, preempt at most one low-priority running container
         (best-effort, minimal churn), then try again.

    This keeps the policy simple/deterministic while improving tail latency for interactive/query
    work under contention.
    """
    # Waiting queues per priority (FIFO within each priority)
    s.waiting_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline resource "hints" learned from OOMs (ram floor); cpu target is simple.
    s.pipeline_ram_floor = {}  # pipeline_id -> ram_floor

    # Track currently running containers by priority for possible preemption
    # container_id -> dict(pool_id=..., priority=..., cpu=..., ram=...)
    s.running = {}

    # Tunables (kept conservative to reduce interference and improve latency)
    s.max_cpu_frac_per_assignment = 0.75
    s.max_ram_frac_per_assignment = 0.75
    s.min_cpu_per_assignment = 1.0

    # OOM backoff controls
    s.oom_backoff_mult = 1.6
    s.oom_backoff_add = 0.0  # additive bump can be useful; keep 0 for now

    # Preemption controls
    s.enable_preemption = True
    s.max_preempts_per_tick = 1


@register_scheduler(key="scheduler_none_033")
def scheduler_none_033(s, results, pipelines):
    """
    Scheduler step.

    Admission:
      - Enqueue new pipelines into per-priority FIFO queues.

    React to results:
      - Maintain a view of running containers.
      - On OOM failure: increase RAM floor for that pipeline so future attempts request more.

    Placement:
      - Iterate pools; for each pool, try to schedule one ready op from the highest priority
        queue that can fit the pool headroom and respects per-assignment caps.
      - If a high-priority op can't fit and preemption is enabled, suspend one low-priority
        running container in that pool (minimal churn), then attempt scheduling.

    Returns:
      suspensions, assignments
    """
    # Enqueue arrivals
    for p in pipelines:
        s.waiting_q[p.priority].append(p)

    # Update state from results (running set + OOM backoff)
    for r in results:
        # If we see a result for a container, it's no longer running
        if getattr(r, "container_id", None) in s.running:
            del s.running[r.container_id]

        # Track newly started running containers (some sims may report RUNNING as a "result";
        # we conservatively re-add only on non-failed results where container_id is present).
        # If the simulator only reports on completion, this block is harmless.
        if getattr(r, "container_id", None) is not None and not r.failed():
            # Keep the last known allocation for preemption heuristics
            s.running[r.container_id] = {
                "pool_id": r.pool_id,
                "priority": r.priority,
                "cpu": getattr(r, "cpu", None),
                "ram": getattr(r, "ram", None),
            }

        # Learn from OOMs (or any failure mentioning OOM)
        if r.failed():
            err = getattr(r, "error", "") or ""
            is_oom = ("OOM" in err) or ("out of memory" in err.lower()) or ("OutOfMemory" in err)
            if is_oom:
                # Increase pipeline RAM floor based on what we tried
                # If ram info missing, fall back to a small bump using pool max.
                pid = None
                # Attempt to infer pipeline id from ops if present (best-effort; if not available, skip)
                # Many sims keep pipeline_id elsewhere; we only rely on r having it if it exists.
                pid = getattr(r, "pipeline_id", None)
                if pid is None:
                    # Best-effort: if ops contain pipeline_id attribute, use it
                    try:
                        if r.ops and hasattr(r.ops[0], "pipeline_id"):
                            pid = r.ops[0].pipeline_id
                    except Exception:
                        pid = None

                if pid is not None:
                    tried = getattr(r, "ram", None)
                    if tried is None:
                        # If unknown, bump to 25% of pool max RAM (conservative)
                        tried = 0.25 * s.executor.pools[r.pool_id].max_ram_pool
                    new_floor = tried * s.oom_backoff_mult + s.oom_backoff_add
                    prev = s.pipeline_ram_floor.get(pid, 0.0)
                    if new_floor > prev:
                        s.pipeline_ram_floor[pid] = new_floor

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    def _priority_order():
        # Higher priority first: QUERY > INTERACTIVE > BATCH
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _is_terminal_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If there are failures, we still allow retry (unlike naive example) because we handle OOM backoff.
        # But for non-OOM failures, repeated retries could loop; we keep simple by dropping if any FAILED ops
        # exist and we have no RAM floor change signal.
        # Since we can't reliably distinguish failure cause at pipeline-level here, we drop only if a pipeline
        # has FAILED ops and also has no recorded RAM floor (i.e., likely not OOM-managed).
        has_failed = st.state_counts.get(OperatorState.FAILED, 0) > 0
        if has_failed and p.pipeline_id not in s.pipeline_ram_floor:
            return True
        return False

    def _get_one_ready_op(p):
        st = p.runtime_status()
        # Only schedule ops whose parents are complete; one op per assignment to reduce blast radius
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        return op_list

    def _cap_resources(pool, avail_cpu, avail_ram):
        # Per-assignment caps reduce interference and improve tail latency for mixed workloads.
        cpu_cap = min(avail_cpu, pool.max_cpu_pool * s.max_cpu_frac_per_assignment)
        ram_cap = min(avail_ram, pool.max_ram_pool * s.max_ram_frac_per_assignment)
        return cpu_cap, ram_cap

    def _desired_resources(pool, avail_cpu, avail_ram, pipeline):
        # Choose a conservative CPU target:
        # - ensure minimum CPU
        # - don't exceed capped headroom
        cpu_cap, ram_cap = _cap_resources(pool, avail_cpu, avail_ram)
        cpu = max(s.min_cpu_per_assignment, min(cpu_cap, avail_cpu))

        # RAM: at least pipeline learned floor (from OOMs), otherwise modest share.
        floor = s.pipeline_ram_floor.get(pipeline.pipeline_id, 0.0)
        # Default RAM target: up to 50% of capped RAM, but not below floor.
        ram_default = 0.5 * ram_cap
        ram = max(floor, ram_default)
        ram = min(ram, ram_cap, avail_ram)
        return cpu, ram

    def _pick_preemption_candidate(pool_id, need_cpu, need_ram, min_victim_priority):
        # Find a single lowest-priority container in this pool to preempt.
        # Minimal churn: suspend at most one, prefer smallest CPU+RAM that helps.
        candidates = []
        for cid, info in s.running.items():
            if info.get("pool_id") != pool_id:
                continue
            pr = info.get("priority")
            if pr is None:
                continue
            # Only preempt strictly lower priority than the arriving need
            if pr == Priority.QUERY:
                continue
            if min_victim_priority == Priority.INTERACTIVE and pr != Priority.BATCH_PIPELINE:
                continue
            if min_victim_priority == Priority.QUERY and pr not in (Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                continue

            cpu = info.get("cpu") or 0.0
            ram = info.get("ram") or 0.0
            candidates.append((pr, cpu + ram, cid, cpu, ram))

        if not candidates:
            return None

        # Preempt lowest priority first, then smallest footprint to reduce wasted work
        priority_rank = {Priority.BATCH_PIPELINE: 2, Priority.INTERACTIVE: 1, Priority.QUERY: 0}
        candidates.sort(key=lambda x: (priority_rank.get(x[0], 99), x[1]))
        return candidates[0][2]

    suspensions = []
    assignments = []

    # Requeue pipelines we inspect but can't schedule this tick
    requeue = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}

    preempts_left = s.max_preempts_per_tick

    # Iterate pools and try to schedule one op per pool per tick (simple & stable)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        scheduled_in_pool = False

        # Try priorities in order
        for pr in _priority_order():
            q = s.waiting_q[pr]
            if not q or scheduled_in_pool:
                continue

            # Scan FIFO but allow skipping blocked/terminal pipelines
            # Keep scan bounded to avoid O(n^2) in large queues
            scan_limit = min(len(q), 32)
            idx = 0
            while idx < scan_limit and not scheduled_in_pool:
                pipeline = q.pop(0)
                if _is_terminal_or_failed(pipeline):
                    idx += 1
                    continue

                op_list = _get_one_ready_op(pipeline)
                if not op_list:
                    # Not ready yet; keep it around
                    requeue[pr].append(pipeline)
                    idx += 1
                    continue

                cpu, ram = _desired_resources(pool, avail_cpu, avail_ram, pipeline)

                # If can't fit due to learned floor, we may need preemption (for high priority only)
                fits = (cpu <= avail_cpu) and (ram <= avail_ram) and cpu > 0 and ram > 0

                if not fits and s.enable_preemption and preempts_left > 0 and pr in (Priority.QUERY, Priority.INTERACTIVE):
                    # Try preempting a single lower-priority container in this pool
                    min_victim = pr  # preempt strictly lower than pr
                    victim_cid = _pick_preemption_candidate(pool_id, cpu - avail_cpu, ram - avail_ram, min_victim)
                    if victim_cid is not None:
                        suspensions.append(Suspend(container_id=victim_cid, pool_id=pool_id))
                        # Optimistically assume resources will free; actual free happens next tick.
                        preempts_left -= 1
                        # Requeue current pipeline to try next tick after preemption takes effect
                        requeue[pr].append(pipeline)
                        scheduled_in_pool = True  # treat as "work done" to avoid over-churn
                        break

                if fits:
                    assignment = Assignment(
                        ops=op_list,
                        cpu=cpu,
                        ram=ram,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                    assignments.append(assignment)
                    # Requeue pipeline for remaining ops
                    requeue[pr].append(pipeline)
                    scheduled_in_pool = True
                    break
                else:
                    # Can't fit now; keep it
                    requeue[pr].append(pipeline)
                    idx += 1

            # Put back unscanned items in the same order (already in q after pops)
            # Nothing needed here beyond requeue, since we only popped from front.

        # End pool loop

    # Restore queues: preserve FIFO by appending requeue at end (fair within priority)
    for pr in _priority_order():
        s.waiting_q[pr].extend(requeue[pr])

    return suspensions, assignments
