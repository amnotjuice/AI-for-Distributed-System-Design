# policy_key: scheduler_none_022
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.037452
# generation_seconds: 34.23
# generated_at: 2026-03-14T01:57:39.371613
@register_scheduler_init(key="scheduler_none_022")
def scheduler_none_022_init(s):
    """Priority-aware FIFO with minimal preemption and OOM-driven RAM bump.

    Improvements over naive FIFO:
      1) Separate waiting queues by priority and always try higher priority first.
      2) Lightweight preemption: if a high-priority op can't be admitted due to lack of headroom,
         suspend one low-priority running container in that pool to make room.
      3) OOM awareness: if an op fails with OOM, remember and bump RAM request for that pipeline next time.
      4) Avoid over-allocating: cap per-assignment CPU/RAM to a fraction of pool to reduce head-of-line blocking.
    """
    # Per-priority waiting queues (store Pipeline objects)
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track per-pipeline RAM multiplier based on observed OOMs (pipeline_id -> multiplier)
    s.ram_mult = {}

    # Track running containers per pool for preemption decisions:
    # pool_id -> {container_id: {"priority": Priority, "cpu": float, "ram": float}}
    s.running = {}

    # Heuristic knobs (small, conservative improvements)
    s.max_ops_per_assignment = 1  # keep single-op assignment for now (simple, low risk)
    s.cpu_cap_fraction = 0.75     # don't hand entire pool to one op
    s.ram_cap_fraction = 0.75
    s.min_cpu_grant = 0.25        # avoid tiny CPU that causes extreme slowdowns
    s.min_ram_grant = 0.25        # avoid tiny RAM grants; still may OOM if below true minimum

    # RAM bump behavior on OOM
    s.oom_bump_factor = 1.5
    s.oom_bump_max = 8.0  # cap multiplier to avoid runaway

    # Which priorities are considered "high" for latency protection
    s.high_priorities = {Priority.QUERY, Priority.INTERACTIVE}


def _prio_rank(priority):
    # Smaller is higher priority for sorting
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _is_oom_error(err):
    if not err:
        return False
    # Be robust to different error string formats
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memoryerror" in e) or ("killed" in e and "memory" in e)


def _pick_next_pipeline(s):
    # Strict priority: QUERY -> INTERACTIVE -> BATCH
    for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        q = s.waiting_queues.get(prio, [])
        while q:
            p = q.pop(0)
            status = p.runtime_status()
            # Drop completed pipelines; keep failed pipelines around only if they can retry failed ops
            if status.is_pipeline_successful():
                continue
            return p
    return None


def _requeue_pipeline(s, pipeline):
    s.waiting_queues[pipeline.priority].append(pipeline)


def _find_assignable_ops(pipeline, max_ops):
    status = pipeline.runtime_status()
    # Require parents complete to preserve DAG semantics
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return []
    return ops[:max_ops]


def _pool_headroom_caps(s, pool):
    # Cap per-op allocations to avoid monopolization.
    cpu_cap = max(s.min_cpu_grant, pool.max_cpu_pool * s.cpu_cap_fraction)
    ram_cap = max(s.min_ram_grant, pool.max_ram_pool * s.ram_cap_fraction)
    return cpu_cap, ram_cap


def _desired_ram_for_pipeline(s, pipeline, base_ram):
    mult = s.ram_mult.get(pipeline.pipeline_id, 1.0)
    return base_ram * mult


def _record_result_and_update_state(s, r):
    # Update running map (best-effort bookkeeping)
    if r.pool_id not in s.running:
        s.running[r.pool_id] = {}

    # If container finished/failed, remove it from running map
    # We don't have explicit state transitions here; treat any result as a completion of that container.
    if getattr(r, "container_id", None) is not None:
        s.running[r.pool_id].pop(r.container_id, None)

    # OOM bump: increase RAM multiplier for that pipeline if the op failed with OOM.
    if r.failed() and _is_oom_error(getattr(r, "error", None)):
        # We expect r.ops to belong to a specific pipeline_id (provided on assignment),
        # but ExecutionResult may not carry pipeline_id. We can only bump based on r.ops'
        # pipeline if available; if not, fall back to no-op.
        #
        # Many simulators attach pipeline_id to ops or include it in error; attempt to infer.
        pid = None
        # Try op.pipeline_id for first op
        try:
            if r.ops:
                pid = getattr(r.ops[0], "pipeline_id", None)
        except Exception:
            pid = None

        if pid is not None:
            cur = s.ram_mult.get(pid, 1.0)
            s.ram_mult[pid] = min(s.oom_bump_max, cur * s.oom_bump_factor)


def _choose_preemption_candidate(s, pool_id):
    # Preempt lowest priority running container in this pool (prefer BATCH),
    # and among same priority, preempt the biggest RAM hog (frees space quickly).
    running = s.running.get(pool_id, {})
    if not running:
        return None

    best = None  # (rank, -ram, container_id)
    for cid, info in running.items():
        pr = info.get("priority", Priority.BATCH_PIPELINE)
        rank = _prio_rank(pr)
        ram = float(info.get("ram", 0.0))
        tup = (rank, -ram, cid)
        if best is None or tup > best:
            best = tup
    # best tuple has highest rank number due to tup > (so worst priority); return its cid
    return best[2] if best else None


def _should_preempt_for_priority(incoming_priority, victim_priority):
    # Only preempt if incoming is strictly higher priority than victim.
    return _prio_rank(incoming_priority) < _prio_rank(victim_priority)


@register_scheduler(key="scheduler_none_022")
def scheduler_none_022(s, results, pipelines):
    """
    Scheduler loop:
      - Ingest new pipelines into priority queues.
      - Process results (update running state; bump RAM on OOM if we can infer pipeline id).
      - For each pool, try to schedule one assignable op at a time, preferring higher priority.
      - If a high-priority op can't fit, suspend a low-priority running container in that pool and retry once.
    """
    # Ingest new pipelines
    for p in pipelines:
        s.waiting_queues[p.priority].append(p)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    # Update state from results
    for r in results:
        _record_result_and_update_state(s, r)

    suspensions = []
    assignments = []

    # Simple per-pool pass; keep it conservative (small changes over baseline)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        cpu_cap, ram_cap = _pool_headroom_caps(s, pool)

        # Try to schedule at most a small number per pool per tick to reduce churn.
        # Start with 1 to keep behavior stable.
        scheduled_this_pool = 0
        max_to_schedule = 1

        # We'll attempt: pick next pipeline by priority, schedule one op, then stop.
        while scheduled_this_pool < max_to_schedule:
            pipeline = _pick_next_pipeline(s)
            if pipeline is None:
                break

            # If pipeline is already successful, skip (defensive)
            status = pipeline.runtime_status()
            if status.is_pipeline_successful():
                continue

            ops = _find_assignable_ops(pipeline, s.max_ops_per_assignment)
            if not ops:
                # Not ready; requeue and stop trying to avoid spinning
                _requeue_pipeline(s, pipeline)
                break

            # Compute requested resources (bounded by pool caps and availability)
            req_cpu = min(avail_cpu, cpu_cap)
            req_ram = min(avail_ram, ram_cap)

            # Apply any OOM-driven RAM multiplier (still bounded by caps/availability)
            bumped_ram = _desired_ram_for_pipeline(s, pipeline, req_ram)
            req_ram = min(avail_ram, ram_cap, bumped_ram)

            # If can't fit minimum grants, requeue and stop
            if req_cpu <= 0 or req_ram <= 0:
                _requeue_pipeline(s, pipeline)
                break

            # If high priority and we're tight on RAM/CPU, consider one preemption attempt
            need_preempt = False
            if pipeline.priority in s.high_priorities:
                # Heuristic: if we have less than min grants available, try preemption.
                if avail_cpu < s.min_cpu_grant or avail_ram < s.min_ram_grant:
                    need_preempt = True

            if need_preempt:
                # Try to suspend one low priority container in this pool
                victim_cid = _choose_preemption_candidate(s, pool_id)
                if victim_cid is not None:
                    victim_info = s.running.get(pool_id, {}).get(victim_cid, {})
                    victim_prio = victim_info.get("priority", Priority.BATCH_PIPELINE)
                    if _should_preempt_for_priority(pipeline.priority, victim_prio):
                        suspensions.append(Suspend(container_id=victim_cid, pool_id=pool_id))
                        # Remove victim from bookkeeping immediately to avoid double-preempt
                        s.running.get(pool_id, {}).pop(victim_cid, None)
                        # Requeue pipeline and let next tick see updated pool availability
                        _requeue_pipeline(s, pipeline)
                        break

            # Place assignment
            assignment = Assignment(
                ops=ops,
                cpu=req_cpu,
                ram=req_ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Bookkeep as running using assignment identity (we only learn container_id later from results;
            # store a placeholder entry keyed by a synthetic id derived from python id of assignment)
            if pool_id not in s.running:
                s.running[pool_id] = {}
            synthetic_cid = f"pending:{pipeline.pipeline_id}:{id(assignment)}"
            s.running[pool_id][synthetic_cid] = {
                "priority": pipeline.priority,
                "cpu": req_cpu,
                "ram": req_ram,
            }

            scheduled_this_pool += 1

            # Requeue pipeline so remaining ops can proceed later
            _requeue_pipeline(s, pipeline)

            # Update local availability pessimistically for subsequent scheduling within this tick
            avail_cpu -= req_cpu
            avail_ram -= req_ram

    return suspensions, assignments
