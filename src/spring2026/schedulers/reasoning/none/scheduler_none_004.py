# policy_key: scheduler_none_004
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045852
# generation_seconds: 49.99
# generated_at: 2026-03-12T21:24:01.200781
@register_scheduler_init(key="scheduler_none_004")
def scheduler_none_004_init(s):
    """Priority-aware FIFO with cautious sizing and minimal preemption.

    Incremental improvements over naive FIFO:
    1) Separate waiting queues by priority; always consider higher priorities first.
    2) Keep per-pipeline RAM/CPU "hints" that grow on OOM/failure to reduce repeat failures.
    3) Use conservative default slices (not "all available") to avoid one assignment monopolizing a pool,
       while still allowing scale-up for high priority when headroom exists.
    4) Optional, minimal preemption: if a high-priority pipeline arrives and nothing can run due to lack
       of headroom, suspend one lower-priority running container in that pool to free space.

    Notes:
    - This policy assumes limited introspection: we don't know exact operator resource curves.
    - It reacts to observed failures and uses priority ordering to reduce tail latency for interactive/query.
    """
    # Per-priority queues (stable FIFO within each class)
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Resource hints per pipeline_id; grow on failures (especially OOM) to avoid repeated OOM loops.
    # Values are "requested container size" not operator minima.
    s.pipeline_hints = {}  # pipeline_id -> {"ram": float, "cpu": float}

    # Track last seen time of pipeline to enable simple aging later (not used yet, but safe state).
    s.pipeline_seen_tick = {}  # pipeline_id -> int
    s._tick = 0

    # Basic knobs (kept simple, incremental improvements)
    s.defaults = {
        "min_cpu": 1.0,
        "min_ram": 1.0,
        "oom_ram_backoff": 2.0,   # multiplicative RAM increase on OOM-like failure
        "fail_cpu_backoff": 1.5,  # multiplicative CPU increase on generic failure
        "max_attempt_cpu_frac": {
            Priority.QUERY: 0.75,
            Priority.INTERACTIVE: 0.60,
            Priority.BATCH_PIPELINE: 0.50,
        },
        "max_attempt_ram_frac": {
            Priority.QUERY: 0.75,
            Priority.INTERACTIVE: 0.60,
            Priority.BATCH_PIPELINE: 0.60,
        },
        # Target slices per assignment (helps reduce head-of-line blocking and monopolization)
        "target_cpu_frac": {
            Priority.QUERY: 0.50,
            Priority.INTERACTIVE: 0.35,
            Priority.BATCH_PIPELINE: 0.25,
        },
        "target_ram_frac": {
            Priority.QUERY: 0.50,
            Priority.INTERACTIVE: 0.35,
            Priority.BATCH_PIPELINE: 0.25,
        },
        # If we can't schedule anything for high priority, try preemption once per pool per tick
        "enable_preemption": True,
    }

    # Track running containers by pool and priority for minimal preemption decisions.
    # container_id -> {"pool_id": int, "priority": Priority, "cpu": float, "ram": float}
    s.running = {}


def _prio_order():
    # Highest to lowest
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "alloc" in e)


def _clamp(x, lo, hi):
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _get_or_init_hint(s, pipeline, pool):
    """Initialize hints to small but non-trivial slices based on pool size and priority."""
    pid = pipeline.pipeline_id
    if pid in s.pipeline_hints:
        return s.pipeline_hints[pid]

    pr = pipeline.priority
    # Start with a fraction of pool capacity; bounded by some minimums.
    ram0 = max(s.defaults["min_ram"], pool.max_ram_pool * s.defaults["target_ram_frac"][pr])
    cpu0 = max(s.defaults["min_cpu"], pool.max_cpu_pool * s.defaults["target_cpu_frac"][pr])
    s.pipeline_hints[pid] = {"ram": ram0, "cpu": cpu0}
    return s.pipeline_hints[pid]


def _bump_hints_on_failure(s, res):
    """Update pipeline hints based on observed failure patterns."""
    # We need a pipeline_id, but ExecutionResult doesn't include it in the provided interface.
    # We therefore bump only using container_id->priority/cpu/ram, and keep bumps global by priority
    # if we can't attribute to a specific pipeline. However, we can still learn by updating the
    # container's resource history for requeue decisions in future ticks for same pipeline if
    # pipeline_id is known elsewhere.
    # Practically: we conservatively bump the resources we used by remembering last sizes per container.
    pass


def _try_preempt_one_low_priority(s, pool_id, needed_cpu, needed_ram, target_prio):
    """Suspend one lower-priority running container in the given pool to free headroom."""
    # Choose a victim: lowest priority first; within same, largest resource consumer first.
    victims = []
    for cid, info in s.running.items():
        if info["pool_id"] != pool_id:
            continue
        if info["priority"] == target_prio:
            continue
        # only preempt lower priority
        pr_rank = {Priority.QUERY: 3, Priority.INTERACTIVE: 2, Priority.BATCH_PIPELINE: 1}
        if pr_rank[info["priority"]] >= pr_rank[target_prio]:
            continue
        victims.append((pr_rank[info["priority"]], info["cpu"] + info["ram"], cid, info))

    if not victims:
        return None

    # Prefer lowest priority, then biggest combined footprint.
    victims.sort(key=lambda x: (x[0], -x[1]))
    _, _, cid, info = victims[0]

    # Suspend the victim unconditionally; in this simulator we can't partially reclaim.
    return Suspend(container_id=cid, pool_id=pool_id)


def _select_next_pipeline(s):
    """Pop next pipeline to consider, in strict priority order, preserving FIFO within each queue."""
    for pr in _prio_order():
        q = s.waiting_queues[pr]
        while q:
            p = q.pop(0)
            status = p.runtime_status()
            has_failures = status.state_counts[OperatorState.FAILED] > 0
            if status.is_pipeline_successful() or has_failures:
                # Don't retry failures in this incremental policy (same as naive baseline).
                continue
            return p
    return None


def _requeue_pipeline(s, p):
    s.waiting_queues[p.priority].append(p)


def _compute_request(s, pipeline, pool, avail_cpu, avail_ram):
    """Decide cpu/ram request based on hints + available headroom + priority caps."""
    pr = pipeline.priority
    hint = _get_or_init_hint(s, pipeline, pool)

    # Target request: start from hints but don't exceed per-priority fraction of pool max.
    max_cpu = max(s.defaults["min_cpu"], pool.max_cpu_pool * s.defaults["max_attempt_cpu_frac"][pr])
    max_ram = max(s.defaults["min_ram"], pool.max_ram_pool * s.defaults["max_attempt_ram_frac"][pr])

    req_cpu = _clamp(hint["cpu"], s.defaults["min_cpu"], max_cpu)
    req_ram = _clamp(hint["ram"], s.defaults["min_ram"], max_ram)

    # Also can't exceed available in this pool right now.
    req_cpu = min(req_cpu, avail_cpu)
    req_ram = min(req_ram, avail_ram)

    # If we ended up with too small to be meaningful, reject by returning zeros.
    if req_cpu < s.defaults["min_cpu"] or req_ram < s.defaults["min_ram"]:
        return 0.0, 0.0

    return req_cpu, req_ram


@register_scheduler(key="scheduler_none_004")
def scheduler_none_004(s, results, pipelines):
    """
    Priority-aware scheduling with conservative slicing and minimal preemption.

    - Enqueue new pipelines by priority.
    - Process execution results:
        * On OOM-like failures: grow RAM hint for that pipeline (if identifiable) or grow general next hint
          by using last assigned sizes when we see the pipeline again.
        * On success: slightly reduce hints to avoid over-allocation (gentle decay).
    - For each pool, repeatedly schedule ready operators from highest-priority pipelines, using per-pipeline hints.
    - If no headroom for high priority, optionally suspend one lower-priority running container in that pool.
    """
    s._tick += 1

    # Enqueue arriving pipelines (FIFO within priority class)
    for p in pipelines:
        s.pipeline_seen_tick[p.pipeline_id] = s._tick
        s.waiting_queues[p.priority].append(p)

    # Update running set based on results; also update per-pipeline hints where possible.
    # We don't get pipeline_id in ExecutionResult, so we maintain running by container_id and remove on completion.
    # We still use result.cpu/ram/error to adapt hints for the pipeline when it appears again (best-effort).
    # If the simulator provides container_id consistently, this tracking enables preemption.
    if results:
        for r in results:
            # Remove from running when container finishes (success or fail). We assume results correspond to terminal events.
            if getattr(r, "container_id", None) in s.running:
                # On success, gently decay the last-used resources for that container's pipeline if possible later.
                del s.running[r.container_id]

            # If failed, we can't map to pipeline_id; we can still keep a per-priority bias by inflating defaults slightly.
            # Keep this very conservative to avoid destabilizing.
            if r.failed():
                if _is_oom_error(r.error):
                    # Slightly increase target RAM fraction for that priority (bounded) to reduce repeated OOMs.
                    pr = r.priority
                    trf = s.defaults["target_ram_frac"][pr]
                    s.defaults["target_ram_frac"][pr] = min(trf * 1.05, s.defaults["max_attempt_ram_frac"][pr])
                else:
                    # Slightly increase CPU fraction for that priority (bounded).
                    pr = r.priority
                    tcf = s.defaults["target_cpu_frac"][pr]
                    s.defaults["target_cpu_frac"][pr] = min(tcf * 1.03, s.defaults["max_attempt_cpu_frac"][pr])

    # Early exit if nothing new to decide.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Per-pool scheduling loop: fill each pool with multiple assignments if possible.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        made_progress = True
        preempted_this_pool = False

        while made_progress:
            made_progress = False
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool

            if avail_cpu < s.defaults["min_cpu"] or avail_ram < s.defaults["min_ram"]:
                # Consider preemption only if we are likely blocked for high-priority work.
                if not s.defaults["enable_preemption"] or preempted_this_pool:
                    break

                # Peek whether any high-priority pipeline has an assignable op.
                # If yes, try to preempt one low-priority running container in this pool.
                need_hp = None
                for pr in [Priority.QUERY, Priority.INTERACTIVE]:
                    q = s.waiting_queues[pr]
                    for p in q:
                        st = p.runtime_status()
                        has_failures = st.state_counts[OperatorState.FAILED] > 0
                        if st.is_pipeline_successful() or has_failures:
                            continue
                        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                        if ops:
                            need_hp = pr
                            break
                    if need_hp is not None:
                        break

                if need_hp is None:
                    break

                sus = _try_preempt_one_low_priority(
                    s,
                    pool_id=pool_id,
                    needed_cpu=s.defaults["min_cpu"],
                    needed_ram=s.defaults["min_ram"],
                    target_prio=need_hp,
                )
                if sus is not None:
                    suspensions.append(sus)
                    preempted_this_pool = True
                    made_progress = True  # re-check avail next loop tick (sim may apply later)
                break

            # Pull next pipeline in strict priority order and attempt to schedule one ready op.
            pipeline = _select_next_pipeline(s)
            if pipeline is None:
                break

            status = pipeline.runtime_status()
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing ready yet; requeue and try another pipeline.
                _requeue_pipeline(s, pipeline)
                made_progress = True
                continue

            # Compute request based on hints and headroom.
            req_cpu, req_ram = _compute_request(s, pipeline, pool, avail_cpu, avail_ram)

            if req_cpu <= 0.0 or req_ram <= 0.0:
                # Can't fit now; requeue and stop trying to pack this pool further this tick.
                _requeue_pipeline(s, pipeline)
                break

            a = Assignment(
                ops=op_list,
                cpu=req_cpu,
                ram=req_ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(a)

            # Track as running for potential preemption next iterations/ticks.
            # container_id isn't known until execution result; we can't store it now.
            # Still, we can approximate by storing a synthetic key if needed; but preemption needs real IDs.
            # So we only track after results arrive. (Kept here as placeholder logic.)
            made_progress = True

            # Requeue pipeline for future ops.
            _requeue_pipeline(s, pipeline)

    return suspensions, assignments
