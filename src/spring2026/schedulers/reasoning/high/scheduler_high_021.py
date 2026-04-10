# policy_key: scheduler_high_021
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.124485
# generation_seconds: 266.83
# generated_at: 2026-04-10T01:19:57.180034
@register_scheduler_init(key="scheduler_high_021")
def scheduler_high_021_init(s):
    """
    Priority-aware, failure-avoiding scheduler with adaptive RAM retry.

    Goals aligned to the objective:
      - Strongly protect QUERY/INTERACTIVE latency via strict priority ordering.
      - Avoid OOM-driven failures by using conservative initial RAM and exponential RAM bump on failure.
      - Keep batch progressing (avoid starvation) via small reserved headroom in single-pool or shared pools.
      - No preemption (keeps churn/wasted work low and avoids relying on undocumented executor APIs).
    """
    from collections import deque, defaultdict

    # Per-priority FIFO queues (round-robin within each priority).
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.pipeline_ids_in_queue = set()
    s.pipelines_by_id = {}

    # Operator-level adaptive retry state (keyed by Python object id(op)).
    s.op_next_ram = {}                 # op_key -> next desired RAM
    s.op_fail_count = defaultdict(int) # op_key -> failure count
    s.op_cooldown = defaultdict(int)   # op_key -> ticks to wait before retry
    s.op_give_up = set()               # op_key -> stop retrying (likely unschedulable)

    # Conservative initial RAM fractions by priority (of pool max RAM).
    s.ram_frac = {
        Priority.QUERY: 0.45,
        Priority.INTERACTIVE: 0.35,
        Priority.BATCH_PIPELINE: 0.20,
    }
    # Minimum RAM fractions to avoid tiny, failure-prone allocations.
    s.ram_min_frac = {
        Priority.QUERY: 0.15,
        Priority.INTERACTIVE: 0.10,
        Priority.BATCH_PIPELINE: 0.05,
    }

    # CPU caps as fractions of pool max CPU (sublinear scaling; keep some concurrency).
    s.cpu_cap_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.55,
        Priority.BATCH_PIPELINE: 0.35,
    }

    # Give-up thresholds: try harder for high-priority to avoid the heavy 720s penalty.
    s.fail_threshold = {
        Priority.QUERY: 10,
        Priority.INTERACTIVE: 8,
        Priority.BATCH_PIPELINE: 5,
    }

    # Reservation to ensure batch makes progress (most important in single-pool runs).
    s.reserve_for_batch_frac = 0.15

    # Assignable states (fallback if the simulator doesn't export ASSIGNABLE_STATES).
    try:
        s.assignable_states = ASSIGNABLE_STATES
    except Exception:
        s.assignable_states = [OperatorState.PENDING, OperatorState.FAILED]

    s.tick = 0


def _op_key(op):
    # Stable-enough key across scheduler ticks and execution results (best-effort).
    # Using id(op) is safest if the simulator returns the same operator objects in results.
    try:
        return id(op)
    except Exception:
        return str(op)


def _safe_lower(x):
    try:
        return str(x).lower()
    except Exception:
        return ""


def _is_oom_error(err) -> bool:
    msg = _safe_lower(err)
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("cuda out of memory" in msg)


def _get_inflight_ops_count(status):
    # Count ops already running/assigned to avoid over-parallelizing a single pipeline.
    states = [OperatorState.ASSIGNED, OperatorState.RUNNING]
    try:
        states.append(OperatorState.SUSPENDING)
    except Exception:
        pass
    try:
        return len(status.get_ops(states, require_parents_complete=False))
    except Exception:
        return 0


def _choose_cpu(s, pool, priority, avail_cpu):
    cap_frac = s.cpu_cap_frac.get(priority, 0.35)
    cap = pool.max_cpu_pool * cap_frac
    # Ensure at least 1 CPU if possible.
    cpu = min(avail_cpu, max(1, cap))
    # If avail_cpu is fractional and < 1, don't oversubscribe.
    if cpu > avail_cpu:
        cpu = avail_cpu
    if cpu <= 0:
        return 0
    return cpu


def _choose_ram(s, pool, priority, opk, avail_ram):
    # If we've learned a required RAM for this op (after failures), honor it.
    desired = s.op_next_ram.get(opk, pool.max_ram_pool * s.ram_frac.get(priority, 0.20))

    # Clamp desired to reasonable bounds within a pool.
    min_desired = pool.max_ram_pool * s.ram_min_frac.get(priority, 0.05)
    max_desired = pool.max_ram_pool * 0.95
    if desired < min_desired:
        desired = min_desired
    if desired > max_desired:
        desired = max_desired

    # Prefer not to under-allocate (avoid failures), but allow a best-effort first attempt
    # for high priority if we're close to the target.
    if desired <= avail_ram:
        return desired

    # Best-effort shrink only if this op hasn't failed yet and we're close to desired.
    fails = s.op_fail_count.get(opk, 0)
    if fails == 0 and priority in (Priority.QUERY, Priority.INTERACTIVE):
        if avail_ram >= 0.7 * desired and avail_ram >= min_desired:
            return avail_ram

    return 0


def _cleanup_queues(s):
    # Drop completed pipelines from queues and bookkeeping.
    for prio, q in list(s.queues.items()):
        from collections import deque
        new_q = deque()
        while q:
            p = q.popleft()
            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    s.pipeline_ids_in_queue.discard(p.pipeline_id)
                    s.pipelines_by_id.pop(p.pipeline_id, None)
                    continue
            except Exception:
                # If status is unavailable for any reason, keep it queued.
                pass
            new_q.append(p)
        s.queues[prio] = new_q


def _next_runnable_from_priority_queue(s, prio, assigned_now):
    q = s.queues.get(prio, None)
    if not q:
        return None, None

    n = len(q)
    for _ in range(n):
        p = q.popleft()

        # If completed, drop it.
        try:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                s.pipeline_ids_in_queue.discard(p.pipeline_id)
                s.pipelines_by_id.pop(p.pipeline_id, None)
                continue
        except Exception:
            st = None

        # Enforce per-pipeline inflight limits to reduce memory blowups and keep fairness.
        inflight = 0
        if st is not None:
            inflight = _get_inflight_ops_count(st)
        inflight += assigned_now.get(p.pipeline_id, 0)

        max_inflight = 1
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            max_inflight = 2

        if inflight >= max_inflight:
            q.append(p)
            continue

        # Prefer retrying FAILED ops (often the critical path) before PENDING ones.
        op = None
        if st is not None:
            try:
                failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=True)
                if failed_ops:
                    op = failed_ops[0]
                else:
                    ops = st.get_ops(s.assignable_states, require_parents_complete=True)
                    if ops:
                        op = ops[0]
            except Exception:
                op = None

        q.append(p)
        if op is None:
            continue

        return p, op

    return None, None


@register_scheduler(key="scheduler_high_021")
def scheduler_high_021_scheduler(s, results: List[ExecutionResult],
                                 pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Main scheduling loop:
      1) Ingest new pipelines into per-priority round-robin queues.
      2) Update adaptive RAM targets on failures (OOM -> aggressive bump) with cooldown/backoff.
      3) For each pool:
           - Pool 0 is "latency-protect": don't start batch if any query/interactive is waiting.
           - Other pools run all priorities, but batch is more welcome there.
         Always schedule in strict priority order (query > interactive > batch),
         while keeping a small reserved share for batch in constrained cases to avoid starvation.
    """
    s.tick += 1

    # Decrement cooldowns (only for ops that have failed).
    if s.op_cooldown:
        for k in list(s.op_cooldown.keys()):
            v = s.op_cooldown.get(k, 0)
            if v <= 0:
                continue
            v -= 1
            if v <= 0:
                s.op_cooldown[k] = 0
            else:
                s.op_cooldown[k] = v

    # Process execution results to learn per-operator RAM needs and to backoff on repeated failures.
    for r in results:
        try:
            ops = r.ops or []
        except Exception:
            ops = []
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = False

        if not ops:
            continue

        for op in ops:
            opk = _op_key(op)
            if failed:
                s.op_fail_count[opk] += 1

                # Exponential backoff to avoid immediate re-failure thrash.
                fcnt = s.op_fail_count[opk]
                s.op_cooldown[opk] = min(12, 2 ** min(fcnt, 3))

                # Increase desired RAM (OOM -> double; otherwise milder bump).
                pool_id = getattr(r, "pool_id", 0)
                pool = s.executor.pools[pool_id]
                max_ram = pool.max_ram_pool

                oom = _is_oom_error(getattr(r, "error", ""))
                factor = 2.0 if oom else 1.5

                prev_req = max(float(getattr(r, "ram", 0) or 0), float(s.op_next_ram.get(opk, 0) or 0))
                bump_floor = max_ram * 0.05
                next_req = max(prev_req * factor, prev_req + bump_floor)
                next_req = min(next_req, max_ram * 0.95)

                # If we failed with "near max" RAM many times, give up to avoid blocking the cluster.
                thr = s.fail_threshold.get(getattr(r, "priority", Priority.BATCH_PIPELINE), 6)
                if fcnt >= thr and next_req >= max_ram * 0.90:
                    s.op_give_up.add(opk)
                else:
                    s.op_next_ram[opk] = next_req
            else:
                # Success: clear learned next-ram for that op (it's done) and cooldown.
                s.op_cooldown[opk] = 0
                if opk in s.op_next_ram:
                    del s.op_next_ram[opk]

    # Ingest new pipelines.
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        if p.pipeline_id in s.pipeline_ids_in_queue:
            continue
        prio = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if prio not in s.queues:
            prio = Priority.BATCH_PIPELINE
        s.queues[prio].append(p)
        s.pipeline_ids_in_queue.add(p.pipeline_id)

    # Early exit if no state changes that affect our decisions.
    if not pipelines and not results:
        return [], []

    # Drop completed pipelines from queues.
    _cleanup_queues(s)

    # Compute global backlog flags.
    q_len = len(s.queues[Priority.QUERY])
    i_len = len(s.queues[Priority.INTERACTIVE])
    b_len = len(s.queues[Priority.BATCH_PIPELINE])
    hp_waiting = (q_len + i_len) > 0
    batch_waiting = b_len > 0

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []
    from collections import defaultdict
    assigned_now = defaultdict(int)  # pipeline_id -> count assigned this tick

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pool 0 acts as a latency-protection pool: don't start new batch work if HP is waiting.
        if pool_id == 0 and hp_waiting:
            prio_order = [Priority.QUERY, Priority.INTERACTIVE]
        else:
            prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

        # If we're in a single pool (or a non-protected pool) and batch is waiting, reserve some headroom
        # so batch eventually completes (avoids many 720s penalties for incomplete batch pipelines).
        reserve_cpu = 0
        reserve_ram = 0
        if batch_waiting and (s.executor.num_pools == 1):
            reserve_cpu = max(1, int(pool.max_cpu_pool * s.reserve_for_batch_frac)) if hp_waiting else 0
            reserve_ram = pool.max_ram_pool * s.reserve_for_batch_frac if hp_waiting else 0

        made_progress = True
        while made_progress:
            made_progress = False

            if avail_cpu <= 0 or avail_ram <= 0:
                break

            for prio in prio_order:
                # In protected mode (pool 0), skip batch when HP is waiting.
                if pool_id == 0 and hp_waiting and prio == Priority.BATCH_PIPELINE:
                    continue

                p, op = _next_runnable_from_priority_queue(s, prio, assigned_now)
                if p is None or op is None:
                    continue

                opk = _op_key(op)

                # If we concluded this op is unschedulable, don't waste resources.
                if opk in s.op_give_up:
                    continue

                # Cooldown/backoff to prevent immediate re-failure churn.
                if s.op_cooldown.get(opk, 0) > 0:
                    continue

                cpu = _choose_cpu(s, pool, prio, avail_cpu)
                if cpu <= 0:
                    continue

                ram = _choose_ram(s, pool, prio, opk, avail_ram)
                if ram <= 0:
                    continue

                # Preserve reserved batch share in single-pool scenarios.
                if reserve_cpu > 0 or reserve_ram > 0:
                    if prio in (Priority.QUERY, Priority.INTERACTIVE):
                        # Only enforce reservation if batch is waiting.
                        if batch_waiting and (avail_cpu - cpu < reserve_cpu or avail_ram - ram < reserve_ram):
                            continue

                # Commit assignment.
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )
                assigned_now[p.pipeline_id] += 1
                avail_cpu -= cpu
                avail_ram -= ram

                made_progress = True
                break  # restart from highest priority after each successful assignment

    return suspensions, assignments
