# policy_key: scheduler_medium_024
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.087950
# generation_seconds: 73.93
# generated_at: 2026-03-14T02:46:48.953568
@register_scheduler_init(key="scheduler_medium_024")
def scheduler_medium_024_init(s):
    """
    Priority-aware, resource-capped scheduler with simple OOM-aware RAM retries.

    Improvements over naive FIFO:
      1) Priority queues (INTERACTIVE > QUERY > BATCH_PIPELINE).
      2) Avoid allocating the *entire* pool to a single operator; use per-priority CPU/RAM targets
         to increase concurrency and reduce head-of-line blocking.
      3) Reserve a small headroom slice for high-priority work so BATCH can't fully crowd it out.
      4) If an operator fails with an OOM-like error, retry with increased RAM next time (per-op).
    """
    # Per-priority FIFO queues of pipelines
    s.q_interactive = []
    s.q_query = []
    s.q_batch = []

    # Per-(pipeline, op) RAM boost factor for OOM retries
    # key: (pipeline_id, op_key) -> float multiplier
    s.ram_boost = {}

    # Small knobs (kept intentionally simple / robust)
    s.max_ram_boost = 8.0
    s.min_cpu = 1.0
    s.min_ram = 1.0  # in whatever units the simulator uses


def _priority_queue(s, prio):
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    if prio == Priority.QUERY:
        return s.q_query
    return s.q_batch


def _op_identity(op):
    # Best-effort stable identity for per-op retry sizing
    for attr in ("op_id", "operator_id", "id", "name"):
        v = getattr(op, attr, None)
        if v is not None:
            return v
    # Fallback: object identity is stable only within process; acceptable for a single simulation run
    return str(id(op))


def _extract_pipeline_id_from_result(res):
    # Best-effort: ExecutionResult spec doesn't guarantee pipeline_id, so try from ops.
    pid = getattr(res, "pipeline_id", None)
    if pid is not None:
        return pid
    ops = getattr(res, "ops", None) or []
    if ops:
        op0 = ops[0]
        pid = getattr(op0, "pipeline_id", None)
        if pid is not None:
            return pid
    return None


def _is_oom_error(err):
    if not err:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)


def _dequeue_ready_pipeline(queue):
    """
    Pop/scan FIFO queue until we find a pipeline with an assignable op whose parents are complete.
    Returns (pipeline, op_list) or (None, None).
    Keeps pipelines that are blocked (no ready ops) by rotating them to the back.
    Drops pipelines that are already successful.
    """
    n = len(queue)
    for _ in range(n):
        p = queue.pop(0)
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if op_list:
            return p, op_list
        # Not ready yet; rotate
        queue.append(p)
    return None, None


def _reserve_headroom(pool, any_high_prio_waiting):
    # Keep some capacity available for imminent high-priority arrivals to reduce tail latency.
    if not any_high_prio_waiting:
        return 0.0, 0.0
    # Conservative, small reservation; enough to start at least one interactive op on most pools.
    reserve_cpu = max(0.0, 0.15 * pool.max_cpu_pool)
    reserve_ram = max(0.0, 0.15 * pool.max_ram_pool)
    return reserve_cpu, reserve_ram


def _targets_for_priority(pool, prio):
    """
    Per-op target allocation to avoid "one op eats the pool".
    These are heuristics: interactive gets more, batch gets less.
    """
    # CPU targets: bounded by pool size; keep minimum 1 vCPU when possible.
    if prio == Priority.INTERACTIVE:
        cpu_tgt = min(pool.max_cpu_pool, 4.0)
        ram_frac = 0.25
    elif prio == Priority.QUERY:
        cpu_tgt = min(pool.max_cpu_pool, 2.0)
        ram_frac = 0.20
    else:  # batch
        cpu_tgt = min(pool.max_cpu_pool, 1.0)
        ram_frac = 0.15

    # RAM targets: fraction of pool RAM (helps avoid tiny underprovisioning).
    ram_tgt = max(1.0, ram_frac * pool.max_ram_pool)
    return cpu_tgt, ram_tgt


def _pick_pool_for_op(pools_state, prio, reserve_by_pool):
    """
    Choose a pool with sufficient available resources.
    For batch, we respect reserve headroom; for high-priority we can dip into it.
    Returns pool_id or None.
    """
    candidates = []
    for pool_id, ps in enumerate(pools_state):
        avail_cpu = ps["avail_cpu"]
        avail_ram = ps["avail_ram"]
        r_cpu, r_ram = reserve_by_pool[pool_id]

        if prio == Priority.BATCH_PIPELINE:
            eff_cpu = avail_cpu - r_cpu
            eff_ram = avail_ram - r_ram
        else:
            eff_cpu = avail_cpu
            eff_ram = avail_ram

        if eff_cpu >= 1.0 and eff_ram >= 1.0:
            # Score: prefer more CPU headroom primarily (reduces queueing), RAM secondary.
            score = eff_cpu + 0.001 * eff_ram
            candidates.append((score, pool_id))

    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def _compute_allocation(pool, pool_state, prio, ram_boost_factor, min_cpu, min_ram):
    cpu_tgt, ram_tgt = _targets_for_priority(pool, prio)
    ram_tgt *= ram_boost_factor

    cpu = min(pool_state["avail_cpu"], max(min_cpu, cpu_tgt))
    ram = min(pool_state["avail_ram"], max(min_ram, ram_tgt))

    # If we can't satisfy minimums, refuse allocation.
    if cpu < min_cpu or ram < min_ram:
        return None, None
    return cpu, ram


@register_scheduler(key="scheduler_medium_024")
def scheduler_medium_024(s, results, pipelines):
    """
    Priority-first, concurrency-aware scheduler.

    Core loop:
      - Enqueue new pipelines by priority.
      - Update per-op RAM boost on OOM failures.
      - For each priority tier, repeatedly schedule one ready op from the oldest pipeline,
        placing it into the pool with the most headroom. Requeue the pipeline afterwards.
      - BATCH scheduling respects reserved headroom when high-priority work is waiting.
    """
    # Enqueue new pipelines
    for p in pipelines:
        _priority_queue(s, p.priority).append(p)

    # Process results to adjust OOM retry sizing
    for res in results:
        if getattr(res, "failed", None) and res.failed():
            if _is_oom_error(getattr(res, "error", None)):
                pid = _extract_pipeline_id_from_result(res)
                ops = getattr(res, "ops", None) or []
                if pid is not None and ops:
                    opk = _op_identity(ops[0])
                    key = (pid, opk)
                elif pid is not None:
                    key = (pid, "_unknown_op")
                else:
                    key = None

                if key is not None:
                    prev = s.ram_boost.get(key, 1.0)
                    # Exponential backoff for RAM on OOM
                    s.ram_boost[key] = min(s.max_ram_boost, max(1.0, prev * 2.0))

    # If nothing changed, exit early
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Snapshot pool availability into mutable state we can update as we assign
    pools_state = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pools_state.append(
            {
                "avail_cpu": float(pool.avail_cpu_pool),
                "avail_ram": float(pool.avail_ram_pool),
            }
        )

    any_high_prio_waiting = (len(s.q_interactive) > 0) or (len(s.q_query) > 0)

    reserve_by_pool = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        reserve_by_pool.append(_reserve_headroom(pool, any_high_prio_waiting))

    # Priority order: interactive first, then query, then batch
    prio_order = [Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE]

    # Try to schedule multiple ops per tick (increases throughput, reduces queueing)
    # Each iteration schedules at most one op from a pipeline, then requeues it.
    made_progress = True
    while made_progress:
        made_progress = False

        for prio in prio_order:
            q = _priority_queue(s, prio)

            # Attempt a bounded number of schedules per priority per outer loop to avoid spinning
            # when resources are exhausted.
            attempts = len(q)
            for _ in range(attempts):
                p, op_list = _dequeue_ready_pipeline(q)
                if p is None:
                    break

                # Choose best pool for this priority
                pool_id = _pick_pool_for_op(pools_state, prio, reserve_by_pool)
                if pool_id is None:
                    # No capacity anywhere; keep pipeline and stop trying this priority
                    q.append(p)
                    break

                pool = s.executor.pools[pool_id]
                ps = pools_state[pool_id]

                opk = _op_identity(op_list[0])
                boost_key = (p.pipeline_id, opk)
                ram_boost_factor = float(s.ram_boost.get(boost_key, 1.0))

                cpu, ram = _compute_allocation(
                    pool=pool,
                    pool_state=ps,
                    prio=prio,
                    ram_boost_factor=ram_boost_factor,
                    min_cpu=s.min_cpu,
                    min_ram=s.min_ram,
                )

                if cpu is None:
                    # Couldn't satisfy minimums in the chosen pool; try once more by removing reserve
                    # for high-priority (batch doesn't get this privilege).
                    if prio != Priority.BATCH_PIPELINE:
                        cpu = min(ps["avail_cpu"], max(s.min_cpu, 1.0))
                        ram = min(ps["avail_ram"], max(s.min_ram, 1.0))
                        if cpu < s.min_cpu or ram < s.min_ram:
                            q.append(p)
                            break
                    else:
                        q.append(p)
                        break

                # Create assignment
                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu,
                        ram=ram,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # Consume capacity in our local snapshot
                ps["avail_cpu"] = max(0.0, ps["avail_cpu"] - float(cpu))
                ps["avail_ram"] = max(0.0, ps["avail_ram"] - float(ram))

                # Requeue pipeline for its next ops (keeps fairness within priority)
                q.append(p)

                made_progress = True

    return suspensions, assignments
