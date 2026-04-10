# policy_key: scheduler_medium_038
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.100183
# generation_seconds: 109.93
# generated_at: 2026-03-14T03:08:35.384340
@register_scheduler_init(key="scheduler_medium_038")
def scheduler_medium_038_init(s):
    """
    Priority-aware, reservation-based round-robin scheduler.

    Improvements over naive FIFO:
      - Separate per-priority queues; always prefer QUERY/INTERACTIVE over BATCH.
      - Avoid "give everything to one op": allocate small per-op quanta to improve
        concurrency and reduce tail latency for high-priority arrivals.
      - Soft reservations: keep headroom in each pool for high-priority work by
        restricting batch to "excess" capacity when high-priority is waiting.
      - OOM-aware RAM backoff: if an op OOMs, retry it with more RAM (exponential
        increase with caps); treat non-OOM repeated failures as terminal.
      - Round-robin within each priority to avoid a single pipeline dominating.

    Notes:
      - No explicit preemption: the simulator API shown doesn't expose a safe way
        to enumerate/choose running low-priority containers to suspend.
    """
    s.tick = 0

    # Per-priority FIFO queues of Pipeline objects
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Map operator identity -> pipeline_id (used to attribute results to pipelines)
    s._op_to_pipeline_id = {}

    # Failure / sizing history (keyed by (pipeline_id, op_key))
    s._oom_ram_mult = {}        # exponential RAM multiplier on OOM
    s._retry_count = {}         # retries (any failure)
    s._dead_pipelines = set()   # pipelines to stop scheduling

    # Round-robin pointer across priority weights
    s._rr_idx = 0

    # Limit how many ops from the same pipeline we schedule per tick (reduces hogging)
    s._scheduled_per_tick = {}  # pipeline_id -> count scheduled in current tick


def _sm038_iter_ops(pipeline):
    """Best-effort iteration over operators in a pipeline to build reverse maps."""
    vals = getattr(pipeline, "values", None)
    if vals is None:
        return []
    try:
        # pipeline.values may be a list-like or a dict-like
        if hasattr(vals, "values") and callable(vals.values):
            return list(vals.values())
        return list(vals)
    except Exception:
        return []


def _sm038_op_stable_key(op):
    """Best-effort stable identifier for an operator object."""
    for attr in ("op_id", "operator_id", "task_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return str(v)
            except Exception:
                pass
    # Fallback: object identity string (not stable across runs, but fine in-sim)
    return str(id(op))


def _sm038_pipeline_queue_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _sm038_high_waiting(s):
    return len(s.q_query) > 0 or len(s.q_interactive) > 0


def _sm038_is_oom_error(err):
    if not err:
        return False
    try:
        e = str(err).lower()
    except Exception:
        return False
    return ("oom" in e) or ("out of memory" in e) or ("out-of-memory" in e) or ("memoryerror" in e)


def _sm038_pick_pool(pool_avail, pools, prio, cpu_need, ram_need, reserve_cpu, reserve_ram, high_waiting):
    """
    Choose a pool for the assignment.
      - For high priority: pick the pool with most headroom (maximize min(cpu, ram_norm)).
      - For batch: best-fit to reduce fragmentation, but respect reservations if high is waiting.
    """
    best = None
    best_score = None
    for pool_id, (a_cpu, a_ram) in enumerate(pool_avail):
        # Apply soft reservation to BATCH when high-priority is waiting
        eff_cpu = a_cpu
        eff_ram = a_ram
        if prio == Priority.BATCH_PIPELINE and high_waiting:
            eff_cpu = max(0.0, a_cpu - reserve_cpu[pool_id])
            eff_ram = max(0.0, a_ram - reserve_ram[pool_id])

        if eff_cpu < cpu_need or eff_ram < ram_need:
            continue

        pool = pools[pool_id]
        # Normalize RAM so CPU and RAM contribute similarly
        ram_norm = eff_ram / max(1.0, float(pool.max_ram_pool))
        cpu_norm = eff_cpu / max(1.0, float(pool.max_cpu_pool))

        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            # Prefer largest headroom (reduces queuing for future ops of same class)
            score = (min(cpu_norm, ram_norm), cpu_norm + ram_norm)
        else:
            # Best-fit: minimize leftover, prioritize tight packing
            leftover_cpu = eff_cpu - cpu_need
            leftover_ram = eff_ram - ram_need
            # Lower score is better for batch
            score = (leftover_cpu + leftover_ram / max(1.0, float(pool.max_ram_pool)), leftover_cpu)

        if best is None:
            best = pool_id
            best_score = score
        else:
            if prio in (Priority.QUERY, Priority.INTERACTIVE):
                if score > best_score:
                    best = pool_id
                    best_score = score
            else:
                if score < best_score:
                    best = pool_id
                    best_score = score
    return best


def _sm038_quanta(pool, prio):
    """
    Per-op resource quanta: small, priority-aware slices.
    Keep these conservative because we don't know per-op scaling curves.
    """
    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    if prio == Priority.QUERY:
        cpu = max(1.0, min(2.0, max_cpu * 0.25))
        ram = max(1.0, min(max_ram * 0.25, max_ram))
        return cpu, ram

    if prio == Priority.INTERACTIVE:
        cpu = max(1.0, min(4.0, max_cpu * 0.50))
        ram = max(1.0, min(max_ram * 0.40, max_ram))
        return cpu, ram

    # Batch: fewer, fatter tasks; still cap to avoid monopolizing huge pools
    cpu = max(1.0, min(8.0, max_cpu * 0.75))
    ram = max(1.0, min(max_ram * 0.70, max_ram))
    return cpu, ram


@register_scheduler(key="scheduler_medium_038")
def scheduler_medium_038(s, results, pipelines):
    """
    Priority-aware round-robin with batch reservations + OOM RAM backoff.

    Returns:
        (suspensions, assignments)
    """
    # Track "time" and reset per-tick counters
    if pipelines or results:
        s.tick += 1
    s._scheduled_per_tick = {}

    # Ingest new pipelines into per-priority queues and build op->pipeline reverse map
    for p in pipelines:
        q = _sm038_pipeline_queue_for_priority(s, p.priority)
        q.append(p)
        # Reverse map ops to pipeline_id to attribute results
        pid = getattr(p, "pipeline_id", None)
        if pid is not None:
            for op in _sm038_iter_ops(p):
                try:
                    s._op_to_pipeline_id[id(op)] = pid
                except Exception:
                    pass

    # Update failure history from results (OOM-aware)
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue

        err = getattr(r, "error", None)
        is_oom = _sm038_is_oom_error(err)

        # Attribute to pipeline via op reverse map, if possible
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            try:
                ops = getattr(r, "ops", []) or []
                for op in ops:
                    pid = s._op_to_pipeline_id.get(id(op))
                    if pid is not None:
                        break
            except Exception:
                pid = None

        # Update per-op backoff and retries
        ops = getattr(r, "ops", []) or []
        for op in ops:
            opk = _sm038_op_stable_key(op)
            key = (pid, opk)

            s._retry_count[key] = s._retry_count.get(key, 0) + 1

            if is_oom:
                cur = s._oom_ram_mult.get(key, 1.0)
                # Exponential backoff; cap to avoid requesting absurd RAM
                s._oom_ram_mult[key] = min(cur * 2.0, 16.0)
                # If we keep OOMing too long, consider the pipeline dead
                if s._retry_count[key] >= 6 and pid is not None:
                    s._dead_pipelines.add(pid)
            else:
                # Non-OOM failures are likely logical/runtime issues; don't spin forever
                if pid is not None:
                    s._dead_pipelines.add(pid)
                if s._retry_count[key] >= 3 and pid is not None:
                    s._dead_pipelines.add(pid)

    # Helper to prune queues of completed/dead pipelines
    def _prune_queue(q):
        newq = []
        for p in q:
            pid = getattr(p, "pipeline_id", None)
            if pid is not None and pid in s._dead_pipelines:
                continue
            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
            except Exception:
                # If status is unavailable, keep it to avoid losing work
                pass
            newq.append(p)
        return newq

    s.q_query = _prune_queue(s.q_query)
    s.q_interactive = _prune_queue(s.q_interactive)
    s.q_batch = _prune_queue(s.q_batch)

    # Early exit
    if not s.q_query and not s.q_interactive and not s.q_batch:
        return [], []

    suspensions = []

    # Snapshot pool availability we will decrement as we assign
    pools = s.executor.pools
    pool_avail = []
    for i in range(s.executor.num_pools):
        pool_avail.append([float(pools[i].avail_cpu_pool), float(pools[i].avail_ram_pool)])

    # Reservations per pool for high-priority work (soft headroom)
    reserve_cpu = []
    reserve_ram = []
    for i in range(s.executor.num_pools):
        pool = pools[i]
        reserve_cpu.append(max(0.0, float(pool.max_cpu_pool) * 0.20))
        reserve_ram.append(max(0.0, float(pool.max_ram_pool) * 0.20))

    # Weighted round robin preference: mostly QUERY/INTERACTIVE, some BATCH progress
    rr_weights = [
        Priority.QUERY, Priority.QUERY, Priority.QUERY,
        Priority.INTERACTIVE, Priority.INTERACTIVE,
        Priority.BATCH_PIPELINE
    ]

    def _queue_for(pr):
        return _sm038_pipeline_queue_for_priority(s, pr)

    # Attempt to schedule multiple assignments per tick across all pools
    assignments = []

    # Bound iterations to avoid infinite loops when nothing fits
    max_steps = 200
    steps = 0
    no_progress_rounds = 0

    while steps < max_steps:
        steps += 1
        high_waiting = _sm038_high_waiting(s)

        # Stop if all pools have no capacity
        if all(a_cpu <= 0.0 or a_ram <= 0.0 for (a_cpu, a_ram) in pool_avail):
            break

        # Pick next priority to serve
        pr = rr_weights[s._rr_idx % len(rr_weights)]
        s._rr_idx += 1

        q = _queue_for(pr)
        if not q:
            no_progress_rounds += 1
            if no_progress_rounds > len(rr_weights) * 2:
                break
            continue

        # Round-robin within the priority queue
        p = q.pop(0)
        pid = getattr(p, "pipeline_id", None)

        # Drop dead/successful pipelines
        if pid is not None and pid in s._dead_pipelines:
            no_progress_rounds += 1
            continue
        try:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                no_progress_rounds += 1
                continue
        except Exception:
            st = None

        # Avoid scheduling too many ops from one pipeline in one tick (reduces hogging)
        already = s._scheduled_per_tick.get(pid, 0)
        per_tick_cap = 2 if pr in (Priority.QUERY, Priority.INTERACTIVE) else 1
        if pid is not None and already >= per_tick_cap:
            # push back and try others
            q.append(p)
            no_progress_rounds += 1
            continue

        # Find an assignable operator
        op_list = []
        try:
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) if st is not None else []
        except Exception:
            op_list = []

        if not op_list:
            # Not runnable yet; keep it in queue
            q.append(p)
            no_progress_rounds += 1
            continue

        op = op_list[0]
        opk = _sm038_op_stable_key(op)
        backoff_key = (pid, opk)
        oom_mult = float(s._oom_ram_mult.get(backoff_key, 1.0))

        # Choose pool and resource quanta (pool-specific)
        chosen_pool = None
        cpu_need = None
        ram_need = None

        # Try to find a fit; for high priority we consider more pools by using their quanta
        # and pick the best via scoring.
        pool_cpu_need = [0.0] * s.executor.num_pools
        pool_ram_need = [0.0] * s.executor.num_pools
        for pool_id in range(s.executor.num_pools):
            cpu_q, ram_q = _sm038_quanta(pools[pool_id], pr)
            # Apply OOM RAM multiplier with cap to pool max RAM
            ram_req = min(float(pools[pool_id].max_ram_pool), ram_q * oom_mult)
            cpu_req = min(float(pools[pool_id].max_cpu_pool), cpu_q)
            pool_cpu_need[pool_id] = max(1.0, cpu_req)
            pool_ram_need[pool_id] = max(1.0, ram_req)

        # For pool selection, use the smallest quanta that still expresses our intent:
        # we pass one representative need, but selection will check each pool with its own
        # (since pool sizes differ). We'll just compute using the current pool needs in scoring.
        # Implement by trying pick_pool for each pool as if need is that pool's computed need.
        # We'll do a small brute-force: evaluate each pool as candidate.
        best_pool = None
        best_score = None
        for pool_id in range(s.executor.num_pools):
            cpu_req = pool_cpu_need[pool_id]
            ram_req = pool_ram_need[pool_id]

            # Apply reservation constraint for batch when high waiting
            a_cpu, a_ram = pool_avail[pool_id]
            eff_cpu, eff_ram = a_cpu, a_ram
            if pr == Priority.BATCH_PIPELINE and high_waiting:
                eff_cpu = max(0.0, a_cpu - reserve_cpu[pool_id])
                eff_ram = max(0.0, a_ram - reserve_ram[pool_id])

            if eff_cpu < cpu_req or eff_ram < ram_req:
                continue

            pool = pools[pool_id]
            ram_norm = eff_ram / max(1.0, float(pool.max_ram_pool))
            cpu_norm = eff_cpu / max(1.0, float(pool.max_cpu_pool))

            if pr in (Priority.QUERY, Priority.INTERACTIVE):
                score = (min(cpu_norm, ram_norm), cpu_norm + ram_norm)
                if best_pool is None or score > best_score:
                    best_pool, best_score = pool_id, score
            else:
                leftover_cpu = eff_cpu - cpu_req
                leftover_ram = eff_ram - ram_req
                score = (leftover_cpu + leftover_ram / max(1.0, float(pool.max_ram_pool)), leftover_cpu)
                if best_pool is None or score < best_score:
                    best_pool, best_score = pool_id, score

        chosen_pool = best_pool
        if chosen_pool is None:
            # Can't fit anywhere right now; requeue and possibly stop if nothing is fitting
            q.append(p)
            no_progress_rounds += 1
            if no_progress_rounds > len(rr_weights) * 3:
                break
            continue

        cpu_need = pool_cpu_need[chosen_pool]
        ram_need = pool_ram_need[chosen_pool]

        # Final sanity: don't exceed current availability
        a_cpu, a_ram = pool_avail[chosen_pool]
        if cpu_need > a_cpu or ram_need > a_ram:
            q.append(p)
            no_progress_rounds += 1
            continue

        # Create the assignment (single op per container improves responsiveness)
        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu_need,
                ram=ram_need,
                priority=p.priority,
                pool_id=chosen_pool,
                pipeline_id=p.pipeline_id,
            )
        )

        # Update accounting
        pool_avail[chosen_pool][0] -= cpu_need
        pool_avail[chosen_pool][1] -= ram_need
        if pid is not None:
            s._scheduled_per_tick[pid] = s._scheduled_per_tick.get(pid, 0) + 1

        # Requeue pipeline for further ops later (round-robin fairness)
        q.append(p)
        no_progress_rounds = 0

    return suspensions, assignments
