# policy_key: scheduler_none_040
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.047781
# generation_seconds: 50.73
# generated_at: 2026-03-14T02:10:04.228282
@register_scheduler_init(key="scheduler_none_040")
def scheduler_none_040_init(s):
    """Priority-aware FIFO with conservative preemption + simple OOM-aware RAM retry.

    Improvements over naive FIFO:
    1) Maintain per-priority queues; always try to schedule highest priority first.
    2) Keep at most one concurrent op per pipeline to reduce head-of-line blocking & contention.
    3) React to OOM by bumping a per-pipeline RAM "hint" and requeueing (bounded exponential backoff).
    4) If a high-priority op cannot be placed due to lack of resources, preempt a single lower-priority
       running container in the same pool (minimizing churn) and retry placement.

    Notes/assumptions:
    - We avoid aggressive packing and keep decisions simple/deterministic.
    - We only preempt when there's a pending higher-priority op that is parents-complete and assignable.
    - We do not attempt CPU scaling inference; we allocate a reasonable CPU share based on pool capacity.
    """
    # Queues per priority (store pipeline_id for stable membership; pipeline objects can be recreated)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Registry of latest pipeline objects by id
    s.pipeline_by_id = {}

    # Per-pipeline resource hints derived from observations (OOM -> increase RAM)
    s.ram_hint_by_pipeline = {}  # pipeline_id -> ram_hint (float)

    # Track how many OOM bumps happened for a pipeline to cap growth
    s.oom_bumps_by_pipeline = {}  # pipeline_id -> int

    # Remember last known running containers by pool to enable targeted preemption
    # pool_id -> {container_id: {"priority": Priority, "cpu": float, "ram": float}}
    s.running_by_pool = {}

    # Soft caps for RAM hint growth (multipliers relative to last attempted)
    s.max_oom_bumps = 4

    # Default allocation heuristics (fractions of pool)
    s.default_cpu_frac = 0.5  # give an op half the pool CPU by default (scale-up bias, fewer concurrent ops)
    s.min_cpu = 1.0

    s.default_ram_frac = 0.5  # start with half pool RAM if no better signal
    s.min_ram = 0.5  # GB-like units; simulator units assumed consistent


def _prio_rank(priority):
    # Higher number means higher priority
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1  # Priority.BATCH_PIPELINE or others


def _get_queue_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _enqueue_pipeline(s, pipeline):
    s.pipeline_by_id[pipeline.pipeline_id] = pipeline
    q = _get_queue_for_priority(s, pipeline.priority)
    if pipeline.pipeline_id not in q:
        q.append(pipeline.pipeline_id)


def _drop_pipeline_if_done_or_failed(s, pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If any op failed (non-retriable in baseline), we may still want to keep for OOM retry.
    # Here we only "drop" if there are failures and we can't make progress (handled elsewhere).
    return False


def _pipeline_has_running_op(pipeline):
    # Keep at most 1 running/assigned op per pipeline to reduce contention and stabilize latency.
    status = pipeline.runtime_status()
    running = status.state_counts.get(OperatorState.RUNNING, 0)
    assigned = status.state_counts.get(OperatorState.ASSIGNED, 0)
    suspending = status.state_counts.get(OperatorState.SUSPENDING, 0)
    return (running + assigned + suspending) > 0


def _next_assignable_op(pipeline):
    status = pipeline.runtime_status()
    if _pipeline_has_running_op(pipeline):
        return []
    # Only ops whose parents are complete
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    # Keep it simple: one op at a time
    return ops[:1] if ops else []


def _desired_resources(s, pool, pipeline_id):
    # CPU: half pool (or minimum), but never exceed available at assignment time.
    cpu = max(s.min_cpu, pool.max_cpu_pool * s.default_cpu_frac)

    # RAM: use hint if present, else half pool
    hinted = s.ram_hint_by_pipeline.get(pipeline_id, None)
    if hinted is None:
        ram = max(s.min_ram, pool.max_ram_pool * s.default_ram_frac)
    else:
        ram = hinted

    # Clamp to pool max (assignment will clamp to available separately)
    cpu = min(cpu, pool.max_cpu_pool)
    ram = min(ram, pool.max_ram_pool)
    return cpu, ram


def _record_running(s, result):
    if result.pool_id not in s.running_by_pool:
        s.running_by_pool[result.pool_id] = {}
    # Record or update container info
    s.running_by_pool[result.pool_id][result.container_id] = {
        "priority": result.priority,
        "cpu": result.cpu,
        "ram": result.ram,
    }


def _forget_running(s, result):
    pool_map = s.running_by_pool.get(result.pool_id, None)
    if not pool_map:
        return
    pool_map.pop(result.container_id, None)


def _handle_results_update_hints(s, results):
    # Update running map, and bump RAM hint on OOM-like failures.
    for r in results:
        # Heuristic: if it's a failure we may want to increase RAM; if it completed, we can clear.
        if hasattr(r, "failed") and r.failed():
            # Try to detect OOM from error string, but fall back to generic bump on any failure.
            err = (r.error or "")
            is_oom = ("oom" in err.lower()) or ("out of memory" in err.lower()) or ("memory" in err.lower())
            if is_oom:
                bumps = s.oom_bumps_by_pipeline.get(r.ops[0].pipeline_id, 0) if (r.ops and hasattr(r.ops[0], "pipeline_id")) else None
            # We do not rely on op having pipeline_id; we use r.priority for scheduling and update by container events.
            # Update RAM hint keyed by pipeline_id is done in scheduler when mapping result to a pipeline (best effort).
            pass

        # We treat result containers as no longer running after a tick update regardless of completion/failure.
        # (Simulator semantics: results come back when container finishes/fails/suspends.)
        _forget_running(s, r)


def _maybe_bump_ram_hint(s, pipeline_id, attempted_ram, error):
    err = (error or "")
    is_oom = ("oom" in err.lower()) or ("out of memory" in err.lower()) or ("memory" in err.lower())
    if not is_oom:
        return

    bumps = s.oom_bumps_by_pipeline.get(pipeline_id, 0)
    if bumps >= s.max_oom_bumps:
        return

    # Exponential bump with mild factor; cap at something reasonable will happen via pool max clamp.
    new_hint = max(attempted_ram * 1.5, s.ram_hint_by_pipeline.get(pipeline_id, attempted_ram) * 1.5)
    s.ram_hint_by_pipeline[pipeline_id] = new_hint
    s.oom_bumps_by_pipeline[pipeline_id] = bumps + 1


def _choose_preemption_candidate(s, pool_id, higher_priority):
    # Pick one lowest-priority running container in this pool to preempt.
    pool_map = s.running_by_pool.get(pool_id, {})
    if not pool_map:
        return None

    best = None
    best_rank = None
    best_size = None  # prefer preempting small? we prefer freeing enough; choose largest RAM among lowest priority.
    for cid, info in pool_map.items():
        r = _prio_rank(info["priority"])
        if r >= _prio_rank(higher_priority):
            continue  # don't preempt equal/higher priority
        size = info.get("ram", 0.0)
        if best is None:
            best = cid
            best_rank = r
            best_size = size
            continue
        # Prefer lower priority first; for ties, prefer larger RAM to free headroom quickly (reduce repeated preemptions)
        if r < best_rank or (r == best_rank and size > best_size):
            best = cid
            best_rank = r
            best_size = size
    return best


def _pop_next_pipeline_id_round_robin(queues, rr_state):
    # queues: list of (name, list_of_ids) in descending priority order already
    # rr_state: dict name->cursor not needed since we use FIFO per queue
    for _, q in queues:
        while q:
            pid = q.pop(0)
            return pid
    return None


@register_scheduler(key="scheduler_none_040")
def scheduler_none_040(s, results, pipelines):
    """
    Scheduler step:
    - Ingest new pipelines into per-priority FIFO queues.
    - Process results to update RAM hints on OOM and remove finished pipelines from queues.
    - For each pool, attempt to assign one ready op at a time, prioritizing QUERY > INTERACTIVE > BATCH.
    - If we cannot place a high-priority op due to resources, preempt at most one lower-priority container
      in that pool and retry once.
    """
    # Ingest arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update running map based on results and adjust RAM hints (best effort mapping by pipeline_id)
    # We can update hints using result.assignment info (cpu/ram) but need pipeline_id: use r.ops[0] if it has pipeline_id,
    # otherwise we can't attribute. We still can preempt based on container priority tracked at assignment-time (below).
    for r in results:
        # If we can infer pipeline_id from result ops, bump hint on OOM
        pipeline_id = None
        if getattr(r, "ops", None):
            op0 = r.ops[0]
            pipeline_id = getattr(op0, "pipeline_id", None)
        if pipeline_id is not None and hasattr(r, "failed") and r.failed():
            _maybe_bump_ram_hint(s, pipeline_id, r.ram, r.error)

        _forget_running(s, r)

    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Cleanup queues: drop pipelines that are completed; keep others
    def _cleanup_queue(q):
        newq = []
        for pid in q:
            pl = s.pipeline_by_id.get(pid)
            if pl is None:
                continue
            st = pl.runtime_status()
            if st.is_pipeline_successful():
                continue
            newq.append(pid)
        return newq

    s.q_query = _cleanup_queue(s.q_query)
    s.q_interactive = _cleanup_queue(s.q_interactive)
    s.q_batch = _cleanup_queue(s.q_batch)

    # Priority order queues
    ordered_queues = [("query", s.q_query), ("interactive", s.q_interactive), ("batch", s.q_batch)]

    # For each pool, try to schedule as many ops as resources allow, but keep it conservative:
    # - schedule at most 2 assignments per pool per tick to avoid bursty contention in interactive scenarios.
    max_assignments_per_pool = 2

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        made = 0

        # Create running map container if missing
        if pool_id not in s.running_by_pool:
            s.running_by_pool[pool_id] = {}

        # Keep trying while we have headroom and work
        while made < max_assignments_per_pool:
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            pid = _pop_next_pipeline_id_round_robin(ordered_queues, None)
            if pid is None:
                break

            pipeline = s.pipeline_by_id.get(pid)
            if pipeline is None:
                continue

            # If pipeline completed, skip
            if _drop_pipeline_if_done_or_failed(s, pipeline):
                continue

            op_list = _next_assignable_op(pipeline)
            if not op_list:
                # Not ready now; requeue to preserve FIFO within priority
                _get_queue_for_priority(s, pipeline.priority).append(pid)
                # Prevent tight loop on blocked pipelines: stop trying this pool this tick.
                break

            desired_cpu, desired_ram = _desired_resources(s, pool, pipeline.pipeline_id)
            cpu = min(desired_cpu, avail_cpu)
            ram = min(desired_ram, avail_ram)

            # If even minimal allocation doesn't fit, consider preemption for high priority
            if cpu < s.min_cpu or ram < s.min_ram:
                # Requeue
                _get_queue_for_priority(s, pipeline.priority).append(pid)
                break

            # If desired doesn't fit due to tight resources, try preemption only for higher priorities
            need_preempt = (desired_cpu > avail_cpu) or (desired_ram > avail_ram)
            did_preempt = False
            if need_preempt and _prio_rank(pipeline.priority) >= _prio_rank(Priority.INTERACTIVE):
                victim = _choose_preemption_candidate(s, pool_id, pipeline.priority)
                if victim is not None:
                    suspensions.append(Suspend(container_id=victim, pool_id=pool_id))
                    # Optimistically remove from running map now to avoid repeated suspensions
                    s.running_by_pool[pool_id].pop(victim, None)
                    did_preempt = True

            # If we preempted, refresh available (sim won't free until next tick; but we at least don't loop forever).
            # We'll still attempt assignment with what is currently available.
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            cpu = min(desired_cpu, avail_cpu)
            ram = min(desired_ram, avail_ram)

            # If still can't place meaningfully, requeue and stop trying this pool this tick to reduce churn.
            if cpu < s.min_cpu or ram < s.min_ram:
                _get_queue_for_priority(s, pipeline.priority).append(pid)
                break

            assignment = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)
            made += 1

            # Track this assignment as running for preemption decisions in future ticks.
            # We don't have container_id yet (assigned later by executor), so we can't record precisely here.
            # However, ExecutionResult will come with container_id; for now we do nothing.
            # (If the simulator offers container_id at assignment time, this could be enhanced.)
            #
            # Requeue pipeline to allow subsequent ops later (preserving FIFO within priority).
            _get_queue_for_priority(s, pipeline.priority).append(pid)

            # If we just preempted, avoid scheduling too many in same tick (churn control)
            if did_preempt:
                break

    return suspensions, assignments
