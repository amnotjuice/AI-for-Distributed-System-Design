# policy_key: scheduler_high_034
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.107755
# generation_seconds: 183.91
# generated_at: 2026-04-10T02:49:17.195042
@register_scheduler_init(key="scheduler_high_034")
def scheduler_high_034_init(s):
    """Priority-aware, OOM-adaptive, multi-pool scheduler.

    Main ideas:
      1) Strictly protect high-priority (QUERY/INTERACTIVE) latency, especially on pool 0 when multiple pools exist.
      2) Avoid failures (720s penalty) by learning per-(pipeline,op) RAM needs from past attempts; on failure, increase RAM aggressively.
      3) Keep batch making progress (avoid starvation/incompletes) using a lightweight weighted-deficit mechanism on non-latency pools,
         while reserving headroom for high-priority when they are waiting.
      4) Conservative first-try RAM sizing per priority to reduce initial OOM risk without fully serializing the system.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # container_id -> metadata for interpreting results
    s.container_meta = {}  # container_id -> dict(pipeline_id, priority, ops)

    # (pipeline_id, op_identity) -> learned RAM hint (minimum to avoid OOM)
    s.op_ram_hint = {}

    # (pipeline_id, op_identity) -> retry count
    s.op_retries = {}

    # Pipelines we choose to abandon (e.g., repeated OOM at max RAM)
    s.abandoned = set()

    # Weighted deficit to ensure batch makes progress on shared pools
    s.deficit = {
        Priority.QUERY: 0.0,
        Priority.INTERACTIVE: 0.0,
        Priority.BATCH_PIPELINE: 0.0,
    }
    s.quantum = {
        Priority.QUERY: 10.0,
        Priority.INTERACTIVE: 5.0,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # Default sizing knobs (fraction of pool max); tuned to be conservative on RAM and generous on CPU for latency classes.
    s.cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.55,
        Priority.BATCH_PIPELINE: 0.35,
    }
    s.ram_frac = {
        Priority.QUERY: 0.30,
        Priority.INTERACTIVE: 0.25,
        Priority.BATCH_PIPELINE: 0.15,
    }

    # When high-priority is waiting, keep this headroom in shared pools by throttling batch.
    s.reserve_frac_cpu = 0.12
    s.reserve_frac_ram = 0.12

    # Retry limits (avoid infinite churn if an op cannot ever fit)
    s.max_retries = {
        Priority.QUERY: 5,
        Priority.INTERACTIVE: 4,
        Priority.BATCH_PIPELINE: 3,
    }

    # Bounds for queue scanning per decision (keeps scheduler cheap)
    s.max_scan_per_queue = 64


def _assignable_states():
    # Prefer the simulator-provided constant if present; otherwise define it.
    try:
        return ASSIGNABLE_STATES
    except NameError:
        return [OperatorState.PENDING, OperatorState.FAILED]


def _op_key(pipeline_id, op):
    # Use object identity to distinguish operators within a pipeline deterministically.
    return (pipeline_id, id(op))


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)


def _pool_reserve(pool, s):
    # Headroom targets (only enforced when high-priority is waiting).
    max_cpu = float(getattr(pool, "max_cpu_pool", 0.0) or 0.0)
    max_ram = float(getattr(pool, "max_ram_pool", 0.0) or 0.0)
    reserve_cpu = max(0.0, max_cpu * float(s.reserve_frac_cpu))
    reserve_ram = max(0.0, max_ram * float(s.reserve_frac_ram))
    # If pools are tiny, don't force a 1-core reservation (can deadlock batch forever).
    return reserve_cpu, reserve_ram


def _desired_cpu(pool, s, priority, avail_cpu):
    max_cpu = float(pool.max_cpu_pool)
    # CPU is helpful for latency; still keep it bounded to encourage some concurrency.
    want = max(1.0, max_cpu * float(s.cpu_frac.get(priority, 0.4)))
    want = min(want, max_cpu)
    return max(1.0, min(float(avail_cpu), want))


def _required_ram(pool, s, pipeline_id, op, priority):
    max_ram = float(pool.max_ram_pool)
    base = max(1.0, max_ram * float(s.ram_frac.get(priority, 0.2)))
    hint = float(s.op_ram_hint.get(_op_key(pipeline_id, op), 0.0) or 0.0)
    req = max(base, hint, 1.0)
    return min(req, max_ram)


def _cleanup_queues(s):
    # Remove completed/abandoned pipelines from queues to keep scans short.
    for prio, q in s.queues.items():
        if not q:
            continue
        newq = []
        for p in q:
            if p.pipeline_id in s.abandoned:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            newq.append(p)
        s.queues[prio] = newq


def _has_runnable_waiting(s, priority, limit=12):
    q = s.queues.get(priority, [])
    if not q:
        return False
    states = _assignable_states()
    scanned = 0
    for p in q:
        if p.pipeline_id in s.abandoned:
            continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        ops = st.get_ops(states, require_parents_complete=True)
        if ops:
            return True
        scanned += 1
        if scanned >= limit:
            break
    return False


def _pick_runnable_that_fits(s, priority, pool, avail_cpu, avail_ram):
    """Rotate through the priority queue to find a runnable op that fits."""
    q = s.queues[priority]
    if not q:
        return None, None, None

    states = _assignable_states()
    scans = min(len(q), int(getattr(s, "max_scan_per_queue", 64) or 64))
    for _ in range(scans):
        p = q.pop(0)

        # Drop from scheduling consideration if already done/abandoned.
        if p.pipeline_id in s.abandoned:
            continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue

        ops = st.get_ops(states, require_parents_complete=True)
        if not ops:
            # Not runnable yet; keep it in the queue.
            q.append(p)
            continue

        op = ops[0]
        req_ram = _required_ram(pool, s, p.pipeline_id, op, priority)

        if float(avail_cpu) >= 1.0 and float(avail_ram) >= float(req_ram):
            # Round-robin within the priority class: put back at end.
            q.append(p)
            return p, op, req_ram

        # Doesn't fit now; keep it in the queue.
        q.append(p)

    return None, None, None


def _update_ram_hints_from_results(s, results):
    # Learn RAM requirements and decide whether to abandon hopeless pipelines.
    for r in results:
        meta = s.container_meta.pop(getattr(r, "container_id", None), None)

        # If we can't attribute, still attempt to learn from ops list as best-effort.
        pipeline_id = meta["pipeline_id"] if meta else None
        priority = meta["priority"] if meta else getattr(r, "priority", None)

        pool = s.executor.pools[r.pool_id]
        max_ram = float(pool.max_ram_pool)

        # Normalize ops to a list
        ops_list = []
        try:
            ops_list = list(r.ops) if r.ops is not None else []
        except TypeError:
            ops_list = [r.ops] if r.ops is not None else []

        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = bool(getattr(r, "error", None))

        if not ops_list:
            continue

        for op in ops_list:
            if pipeline_id is None:
                # Can't store per-pipeline hints without pipeline_id.
                continue

            k = _op_key(pipeline_id, op)
            prev_hint = float(s.op_ram_hint.get(k, 0.0) or 0.0)
            assigned_ram = float(getattr(r, "ram", 0.0) or 0.0)

            if failed:
                err = getattr(r, "error", None)
                oom = _is_oom_error(err)

                # Increase aggressively on OOM; otherwise moderately (still favors completion).
                if oom:
                    new_hint = max(prev_hint, assigned_ram * 2.0, 1.0)
                else:
                    new_hint = max(prev_hint, assigned_ram * 1.5, 1.0)

                new_hint = min(new_hint, max_ram)
                s.op_ram_hint[k] = new_hint
                s.op_retries[k] = int(s.op_retries.get(k, 0) or 0) + 1

                # If we're already at max RAM and still OOMing, likely cannot fit in this pool class.
                # Abandon after 2 max-RAM OOMs to avoid burning the whole simulation time.
                if oom and assigned_ram >= 0.99 * max_ram and s.op_retries[k] >= 2:
                    s.abandoned.add(pipeline_id)

                # Cap retries per priority to prevent infinite churn.
                if priority in s.max_retries and s.op_retries[k] >= int(s.max_retries[priority]):
                    s.abandoned.add(pipeline_id)
            else:
                # Success: record that this RAM worked (helps future retries be less risky).
                s.op_ram_hint[k] = max(prev_hint, assigned_ram, 1.0)
                if k in s.op_retries:
                    del s.op_retries[k]


@register_scheduler(key="scheduler_high_034")
def scheduler_high_034(s, results, pipelines):
    """
    Scheduler step:
      - Enqueue new pipelines into per-priority queues.
      - Learn from execution results (esp. OOM) to reduce future failures.
      - Schedule runnable ops onto pools with:
          * strict preference for QUERY/INTERACTIVE on pool 0 if multiple pools exist,
          * weighted-deficit sharing on other pools to keep batch progressing,
          * batch throttling (headroom reservation) when high-priority is waiting.
    """
    # Enqueue arrivals
    for p in pipelines:
        if p.pipeline_id in s.abandoned:
            continue
        s.queues[p.priority].append(p)

    # Learn from results (RAM hints + abandonment decisions)
    if results:
        _update_ram_hints_from_results(s, results)

    # Keep queues clean (completed/abandoned pipelines)
    _cleanup_queues(s)

    # Update deficits once per tick (cheap and stable)
    for prio, qv in s.quantum.items():
        s.deficit[prio] = float(s.deficit.get(prio, 0.0) or 0.0) + float(qv)

    # Detect whether we should protect headroom for latency classes
    high_waiting = _has_runnable_waiting(s, Priority.QUERY) or _has_runnable_waiting(s, Priority.INTERACTIVE)

    suspensions = []
    assignments = []

    num_pools = int(s.executor.num_pools)

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < 1.0 or avail_ram <= 0.0:
            continue

        # Pool role:
        # - If multiple pools exist, treat pool 0 as the "latency pool" (strictly prioritize QUERY/INTERACTIVE).
        latency_pool = (num_pools > 1 and pool_id == 0)

        # Fill the pool with as many assignments as make sense.
        # (Local accounting prevents over-allocating; executor will enforce globally.)
        made_progress = True
        guard = 0
        while made_progress and avail_cpu >= 1.0 and avail_ram > 0.0 and guard < 128:
            guard += 1
            made_progress = False

            # Establish which priorities to try on this pool.
            if latency_pool:
                # Strict: first QUERY, then INTERACTIVE; only run batch if no high-priority runnable exists.
                prio_try = [Priority.QUERY, Priority.INTERACTIVE]
                if not high_waiting:
                    prio_try.append(Priority.BATCH_PIPELINE)
            else:
                # Shared pools: choose by deficit (highest first), but still prefer latency classes naturally.
                candidates = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
                candidates.sort(key=lambda pr: float(s.deficit.get(pr, 0.0) or 0.0), reverse=True)
                prio_try = candidates

            # Attempt to schedule one op from one of the priorities.
            for prio in prio_try:
                # If batch and we have high-priority waiting, enforce headroom reservation in shared pools.
                if (prio == Priority.BATCH_PIPELINE) and (not latency_pool) and high_waiting:
                    reserve_cpu, reserve_ram = _pool_reserve(pool, s)
                    # If we are already below reserve, don't schedule batch here.
                    if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                        continue

                p, op, req_ram = _pick_runnable_that_fits(s, prio, pool, avail_cpu, avail_ram)
                if p is None:
                    continue

                cpu = _desired_cpu(pool, s, prio, avail_cpu)
                ram = float(req_ram)

                # For batch with high-priority waiting, re-check reservation after selecting actual size.
                if (prio == Priority.BATCH_PIPELINE) and (not latency_pool) and high_waiting:
                    reserve_cpu, reserve_ram = _pool_reserve(pool, s)
                    if (avail_cpu - cpu) < reserve_cpu or (avail_ram - ram) < reserve_ram:
                        # Skip batch for now; leave pipeline in queue (already rotated back).
                        continue

                # Create the assignment (one op per container; safest default).
                a = Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(a)

                # Track container->pipeline mapping for RAM learning from results.
                # Note: container_id is only known after execution starts; but result carries it.
                # We store by expected container id only if available elsewhere; since it isn't, we store by
                # writing meta when results arrive is impossible. Instead, we rely on the simulator to return
                # container_id and we map it then. Many simulators set container_id deterministically at submit,
                # but not exposed here. Best-effort: stash by a synthetic key if present.
                #
                # In Eudoxia's API, results include container_id, and we can map that only if we recorded it
                # at assignment time. Since it's not available, we also store a fallback map keyed by (pool_id, op id)
                # is not safe. Therefore, we keep container_meta only when the simulator populates container_id
                # on the Assignment object (some do). Guarded accordingly.
                cid = getattr(a, "container_id", None)
                if cid is not None:
                    s.container_meta[cid] = {"pipeline_id": p.pipeline_id, "priority": prio, "ops": [op]}

                # Local resource accounting
                avail_cpu -= float(cpu)
                avail_ram -= float(ram)

                # Deficit accounting (only on non-latency pools; latency pool is strict by design).
                if not latency_pool:
                    s.deficit[prio] = float(s.deficit.get(prio, 0.0) or 0.0) - 1.0

                made_progress = True
                break

    return suspensions, assignments
