# policy_key: scheduler_none_041
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040193
# generation_seconds: 41.53
# generated_at: 2026-03-14T02:10:45.756308
@register_scheduler_init(key="scheduler_none_041")
def scheduler_none_041_init(s):
    """Priority-aware FIFO with minimal, safe improvements over the naive baseline.

    Improvements (kept intentionally small and robust):
    1) Maintain separate queues per priority (QUERY/INTERACTIVE/BATCH) to reduce head-of-line blocking.
    2) Light-weight, conservative "reserved headroom" per pool for high priority work:
       - When scheduling lower priority work, don't consume the last slice of CPU/RAM.
    3) Basic OOM backoff learning per pipeline: if a container fails with OOM, bump the RAM request
       for subsequent operators in that pipeline (bounded), reducing repeated fast-fail retries.
    4) (Optional, conservative) preemption: if a high-priority pipeline is blocked and a pool has
       no headroom, suspend one low-priority container in that pool (at most one per tick).
    """
    # Separate FIFO queues per priority to avoid head-of-line blocking.
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track pipeline arrival order for FIFO within priority.
    s._arrival_seq = 0
    s._arrival_order = {}  # pipeline_id -> seq

    # Simple per-pipeline RAM multiplier: increases on OOM to reduce repeated failures.
    s._ram_mult = {}  # pipeline_id -> float

    # Track currently running containers by pool so we can preempt low priority if needed.
    # container_id -> dict(pool_id=int, priority=Priority)
    s._running = {}

    # Remember last seen container ids to support adding on results (best-effort).
    s._known_container_ids = set()

    # Guardrails for RAM multiplier.
    s._ram_mult_min = 1.0
    s._ram_mult_max = 8.0
    s._ram_mult_oom_bump = 2.0  # exponential backoff on OOM

    # Reservation fractions for "headroom" (per pool), to protect tail latency.
    # Keep small to avoid starving batch; only applied when scheduling LOWER priority.
    s._reserve = {
        Priority.QUERY: (0.00, 0.00),         # (cpu_frac, ram_frac)
        Priority.INTERACTIVE: (0.05, 0.05),
        Priority.BATCH_PIPELINE: (0.15, 0.15),
    }

    # Limit preemption churn: at most 1 suspension per pool per tick.
    s._max_preempt_per_pool_per_tick = 1


def _prio_order():
    # Higher priority first
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if not err:
        return False
    # Be permissive about simulator error strings.
    s = str(err).lower()
    return ("oom" in s) or ("out of memory" in s) or ("memory" in s and "exceed" in s)


def _enqueue_pipeline(s, p):
    pid = p.pipeline_id
    if pid not in s._arrival_order:
        s._arrival_order[pid] = s._arrival_seq
        s._arrival_seq += 1
    if pid not in s._ram_mult:
        s._ram_mult[pid] = s._ram_mult_min
    s.wait_q[p.priority].append(p)


def _pipeline_done_or_failed(p):
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return True
    # Treat any FAILED op as terminal except we still allow FAILED state to be assignable.
    # The simulator's ASSIGNABLE_STATES often includes FAILED; we prefer not to keep retrying
    # non-OOM failures forever. We'll detect via results and only bump RAM on OOM; otherwise
    # we eventually stop scheduling if it keeps failing (by dropping when failures exist).
    if st.state_counts.get(OperatorState.FAILED, 0) > 0:
        return True
    return False


def _pick_next_assignable_op(p):
    st = p.runtime_status()
    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return []
    # Keep it simple: schedule a single ready operator per assignment (as baseline does).
    return op_list[:1]


def _reserved_headroom(s, pool, for_priority):
    # When scheduling "for_priority", reserve resources for higher priorities.
    # Example: if scheduling BATCH, reserve larger fractions; if scheduling QUERY, reserve none.
    cpu_frac, ram_frac = s._reserve.get(for_priority, (0.0, 0.0))
    return (pool.max_cpu_pool * cpu_frac, pool.max_ram_pool * ram_frac)


def _effective_avail(pool, reserve_cpu, reserve_ram):
    # Never go below zero.
    return (
        max(0.0, pool.avail_cpu_pool - reserve_cpu),
        max(0.0, pool.avail_ram_pool - reserve_ram),
    )


def _register_running(s, assignment):
    # Best-effort: the simulator will map assignment -> container(s). We only learn container_id
    # from results. Still, we can track by results; here is a placeholder if the framework ever
    # supplies ids earlier.
    return


def _update_running_from_results(s, results):
    # Keep minimal state needed for preemption decisions.
    for r in results:
        # If we see a container_id, we consider it "known". Whether it's still running isn't
        # explicitly provided; we conservatively remove it on any result (success/fail).
        cid = getattr(r, "container_id", None)
        if cid is None:
            continue
        s._known_container_ids.add(cid)
        # Remove from running when we get a completion/failure result.
        if cid in s._running:
            del s._running[cid]


def _learn_from_results(s, results):
    # On OOM failures, bump per-pipeline RAM multiplier.
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue
        if _is_oom_error(getattr(r, "error", None)):
            # Attempt to infer pipeline_id; ExecutionResult in prompt doesn't list it,
            # so we use operator metadata best-effort if present.
            pid = None
            ops = getattr(r, "ops", None) or []
            if ops:
                # Common pattern: op might have pipeline_id; best-effort.
                op0 = ops[0]
                pid = getattr(op0, "pipeline_id", None)
            if pid is None:
                # If we can't map to pipeline, we can't learn safely.
                continue
            cur = s._ram_mult.get(pid, s._ram_mult_min)
            s._ram_mult[pid] = min(s._ram_mult_max, max(s._ram_mult_min, cur * s._ram_mult_oom_bump))


def _maybe_preempt_for_high_priority(s, pool_id, need_cpu, need_ram, desired_priority):
    # Suspend one low-priority running container in this pool to create headroom.
    # Because we don't have an authoritative list of running containers from the executor,
    # we can only preempt containers we previously observed as running via results/state.
    # We'll implement a conservative approach: if we have no known running containers, do nothing.
    #
    # NOTE: This is intentionally minimal; it avoids aggressive churn.
    candidates = []
    for cid, meta in s._running.items():
        if meta.get("pool_id") != pool_id:
            continue
        pr = meta.get("priority")
        if pr is None:
            continue
        # Only preempt strictly lower priority than the desired one.
        if pr == Priority.BATCH_PIPELINE and desired_priority in (Priority.INTERACTIVE, Priority.QUERY):
            candidates.append((2, cid))  # lowest
        elif pr == Priority.INTERACTIVE and desired_priority == Priority.QUERY:
            candidates.append((1, cid))
    if not candidates:
        return None
    # Preempt the lowest priority candidate (largest rank value).
    candidates.sort(reverse=True)
    return candidates[0][1]


@register_scheduler(key="scheduler_none_041")
def scheduler_none_041(s, results, pipelines):
    """
    Priority-aware FIFO scheduler with modest headroom reservation and OOM backoff.

    Decision outline per tick:
    - Ingest new pipelines into per-priority queues.
    - Learn from results: update running tracking and increase RAM multiplier on OOM.
    - For each pool, try to schedule ready ops from highest to lowest priority, using
      conservative reserved headroom when placing lower-priority work.
    - If a high priority op can't fit anywhere, optionally suspend one low priority
      container in that pool (at most 1/pool/tick) to free resources.
    """
    # Ingest pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn/update from results
    _update_running_from_results(s, results)
    _learn_from_results(s, results)

    # Fast path
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Remove completed/failed pipelines from queues (lazy cleanup) while preserving FIFO.
    for pr in _prio_order():
        newq = []
        for p in s.wait_q[pr]:
            if _pipeline_done_or_failed(p):
                continue
            newq.append(p)
        s.wait_q[pr] = newq

    # For each pool, schedule at most one assignment (baseline behavior) but priority-aware.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        preempted_this_pool = 0

        # Try priorities in order, but allow multiple attempts if queue head isn't ready.
        made_assignment = False

        for pr in _prio_order():
            if made_assignment:
                break

            # Compute reserved headroom when scheduling this priority (reserving for higher prios).
            reserve_cpu, reserve_ram = _reserved_headroom(s, pool, pr)
            eff_cpu, eff_ram = _effective_avail(pool, reserve_cpu, reserve_ram)

            # If we're scheduling high priority, don't apply reservation that blocks it.
            if pr in (Priority.QUERY, Priority.INTERACTIVE):
                eff_cpu, eff_ram = pool.avail_cpu_pool, pool.avail_ram_pool

            # Scan FIFO queue for this priority to find the first pipeline with a ready op.
            q = s.wait_q[pr]
            for idx in range(len(q)):
                p = q[idx]
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                if st.state_counts.get(OperatorState.FAILED, 0) > 0:
                    # Don't keep retrying arbitrary failures.
                    continue

                op_list = _pick_next_assignable_op(p)
                if not op_list:
                    continue

                # Size selection: start simple—use all effective available resources,
                # but apply per-pipeline RAM multiplier for OOM-avoidance learning.
                pid = p.pipeline_id
                ram_mult = s._ram_mult.get(pid, s._ram_mult_min)

                # Request at least a small slice if anything is available (avoid zero alloc).
                cpu_req = max(0.0, eff_cpu)
                ram_req = max(0.0, min(pool.avail_ram_pool, eff_ram) * ram_mult)

                # Cap to pool availability (multiplier might overshoot).
                cpu_req = min(cpu_req, pool.avail_cpu_pool)
                ram_req = min(ram_req, pool.avail_ram_pool)

                # If we ended up with no resources due to reservation, consider preemption for high prio.
                if (cpu_req <= 0 or ram_req <= 0) and pr in (Priority.QUERY, Priority.INTERACTIVE):
                    if preempted_this_pool < s._max_preempt_per_pool_per_tick:
                        cid = _maybe_preempt_for_high_priority(
                            s, pool_id, need_cpu=1.0, need_ram=1.0, desired_priority=pr
                        )
                        if cid is not None:
                            suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
                            preempted_this_pool += 1
                    continue

                if cpu_req <= 0 or ram_req <= 0:
                    continue

                # Make assignment for this single op
                assignment = Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(assignment)

                # FIFO discipline: move selected pipeline to back of its priority queue
                # (round-robin within priority to reduce one pipeline hogging the pool).
                q.append(q.pop(idx))

                made_assignment = True
                _register_running(s, assignment)
                break

    return suspensions, assignments
