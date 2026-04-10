# policy_key: scheduler_high_028
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.137379
# generation_seconds: 213.92
# generated_at: 2026-04-10T02:08:48.957262
@register_scheduler_init(key="scheduler_high_028")
def scheduler_high_028_init(s):
    """
    Priority-aware, failure-averse policy tuned for low weighted latency while keeping completion high.

    Key ideas:
      - Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
      - Soft reservation: if multiple pools exist, prefer keeping pool 0 for high-priority work
        (batch avoids pool 0 when high-priority is waiting).
      - OOM-aware retry: on OOM failures, increase a per-operator RAM hint and avoid re-running
        the same failing op immediately via small backoff.
      - Gentle fairness: after N high-priority placements, force an attempt to place a batch op
        (if any are runnable), to avoid starvation and reduce incomplete penalties.
      - Conservative sizing for high priority; more packed sizing for batch to preserve throughput.
    """
    from collections import deque

    s.tick = 0

    # Per-priority FIFO queues of pipelines.
    s.q = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Per-operator learned RAM "minimum hint" based on observed OOMs (keyed by id(op)).
    s.op_min_ram_hint = {}  # oid -> float

    # Failure bookkeeping + backoff to reduce churn on repeated failures.
    s.op_failures = {}  # oid -> int
    s.op_backoff_until = {}  # oid -> tick when eligible to retry

    # Fairness knobs: ensure batch makes progress under heavy interactive/query load.
    s.hp_since_batch = 0
    s.max_hp_before_batch = 6

    # Prevent pathological scanning work per scheduling decision.
    s.max_scan_per_decision = 50


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    u = str(err).upper()
    return ("OOM" in u) or ("OUT OF MEMORY" in u) or ("OUT_OF_MEMORY" in u)


def _base_ratios(priority, backlog_len: int):
    # Ratios are chosen to:
    #  - protect query/interactive latency (higher per-op CPU)
    #  - reduce OOM risk (higher per-op RAM for high priorities)
    #  - preserve throughput / avoid incompletes (allow batch concurrency)
    if priority == Priority.QUERY:
        cpu_ratio = 0.85 if backlog_len <= 1 else 0.65
        ram_ratio = 0.60 if backlog_len <= 1 else 0.50
    elif priority == Priority.INTERACTIVE:
        cpu_ratio = 0.70 if backlog_len <= 1 else 0.55
        ram_ratio = 0.50 if backlog_len <= 1 else 0.42
    else:
        cpu_ratio = 0.45
        ram_ratio = 0.30
    return cpu_ratio, ram_ratio


def _min_reasonable(cpu_max: float, ram_max: float, priority):
    # Avoid scheduling with extremely tiny slices (often guarantees slowdowns / churn).
    # Keep minima lower for batch to allow packing.
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        min_cpu = max(0.25, 0.10 * cpu_max)
        min_ram = max(0.25, 0.12 * ram_max)
    else:
        min_cpu = max(0.10, 0.06 * cpu_max)
        min_ram = max(0.10, 0.08 * ram_max)
    return min_cpu, min_ram


def _choose_candidate_pools(num_pools: int, priority, high_waiting: bool):
    # Soft reservation: if multiple pools exist, try to keep pool 0 for high-priority.
    if num_pools <= 1:
        return [0]

    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        return list(range(num_pools))  # allow spillover to avoid blocking
    else:
        # Batch avoids pool 0 while high-priority is waiting; can use it if nothing else.
        pools = [i for i in range(num_pools) if not (high_waiting and i == 0)]
        return pools if pools else [0]


def _select_pool_for_op(s, op, pipeline_priority, pool_avail, candidate_pools, backlog_len: int):
    oid = id(op)

    # Compute "desired" resources per pool; select best feasible pool.
    best = None  # (score, pool_id, alloc_cpu, alloc_ram)
    hint_ram = float(s.op_min_ram_hint.get(oid, 0.0) or 0.0)

    for pool_id in candidate_pools:
        pa = pool_avail[pool_id]
        avail_cpu, avail_ram = pa["cpu"], pa["ram"]
        max_cpu, max_ram = pa["max_cpu"], pa["max_ram"]

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        cpu_ratio, ram_ratio = _base_ratios(pipeline_priority, backlog_len)
        desired_cpu = max_cpu * cpu_ratio
        desired_ram = max(max_ram * ram_ratio, hint_ram)

        min_cpu, min_ram = _min_reasonable(max_cpu, max_ram, pipeline_priority)

        # If we have a learned hint (usually after OOM), don't schedule unless we can meet it.
        if hint_ram > 0 and avail_ram < hint_ram:
            continue

        # Otherwise require at least some non-trivial slice to avoid obvious churn.
        if avail_cpu < min_cpu or avail_ram < min_ram:
            continue

        alloc_cpu = min(desired_cpu, avail_cpu)
        alloc_ram = min(desired_ram, avail_ram)

        # Scoring:
        #  - For high priority, prefer more CPU headroom and pool 0 (lower interference).
        #  - For batch, prefer "best-fit" packing to reduce fragmentation.
        if pipeline_priority == Priority.QUERY:
            score = (10_000_000 if pool_id == 0 else 0) + (1000.0 * avail_cpu) + (1.0 * avail_ram)
        elif pipeline_priority == Priority.INTERACTIVE:
            score = (5_000_000 if pool_id == 0 else 0) + (800.0 * avail_cpu) + (1.0 * avail_ram)
        else:
            # best-fit on RAM first (pack), then CPU.
            leftover_ram = avail_ram - alloc_ram
            score = (-1000.0 * leftover_ram) + (10.0 * avail_cpu)

        if best is None or score > best[0]:
            best = (score, pool_id, alloc_cpu, alloc_ram)

    if best is None:
        return None
    _, pool_id, alloc_cpu, alloc_ram = best
    return pool_id, alloc_cpu, alloc_ram


def _find_placeable_from_queue(s, dq, pool_avail, num_pools: int, high_waiting: bool,
                               planned_pipeline_ids: set, planned_op_ids: set):
    # Try a bounded number of pipelines from this queue to find a runnable + placeable op.
    scan = min(len(dq), s.max_scan_per_decision)
    if scan <= 0:
        return None

    backlog_len = len(dq)

    for _ in range(scan):
        p = dq.popleft()

        # If we already planned something for this pipeline this tick, rotate it.
        if p.pipeline_id in planned_pipeline_ids:
            dq.append(p)
            continue

        status = p.runtime_status()
        if status.is_pipeline_successful():
            # Drop completed pipelines.
            continue

        # Find an assignable op, requiring parents complete, excluding ops already planned.
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        op = None
        for cand in ops:
            oid = id(cand)
            if oid in planned_op_ids:
                continue
            # Backoff gate to reduce repeated fail churn.
            if s.op_backoff_until.get(oid, 0) > s.tick:
                continue
            op = cand
            break

        if op is None:
            dq.append(p)
            continue

        candidate_pools = _choose_candidate_pools(num_pools, p.priority, high_waiting)
        sel = _select_pool_for_op(s, op, p.priority, pool_avail, candidate_pools, backlog_len)
        if sel is None:
            dq.append(p)
            continue

        pool_id, alloc_cpu, alloc_ram = sel
        dq.append(p)  # keep pipeline enqueued for later ops
        return p, op, pool_id, alloc_cpu, alloc_ram

    return None


@register_scheduler(key="scheduler_high_028")
def scheduler_high_028(s, results: List["ExecutionResult"],
                       pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    s.tick += 1

    # Enqueue new pipelines by priority (FIFO within each priority).
    for p in pipelines:
        s.q[p.priority].append(p)

    # Early exit: if nothing new and no results, decisions likely unchanged.
    if not pipelines and not results:
        return [], []

    # Process results to learn from failures (especially OOM) and apply backoff.
    for r in results:
        # r.ops is a list of operator objects run in the same container.
        for op in getattr(r, "ops", []) or []:
            oid = id(op)

            if r.failed():
                cnt = s.op_failures.get(oid, 0) + 1
                s.op_failures[oid] = cnt

                oom = _is_oom_error(getattr(r, "error", None))

                # Backoff: shorter for high priority, longer for repeated non-OOM failures.
                if r.priority in (Priority.QUERY, Priority.INTERACTIVE):
                    backoff = 1 if oom else min(8, 2 ** min(cnt, 3))
                else:
                    backoff = 2 if oom else min(16, 2 ** min(cnt, 4))
                s.op_backoff_until[oid] = s.tick + backoff

                # RAM hint updates: aggressively raise on OOM; mild bump otherwise.
                used_ram = getattr(r, "ram", None)
                if used_ram is not None:
                    prev = float(s.op_min_ram_hint.get(oid, 0.0) or 0.0)
                    if oom:
                        mult = 1.7 if r.priority in (Priority.QUERY, Priority.INTERACTIVE) else 1.5
                        s.op_min_ram_hint[oid] = max(prev, float(used_ram) * mult)
                    else:
                        s.op_min_ram_hint[oid] = max(prev, float(used_ram) * 1.10)

            else:
                # On success: we keep hints (no down-tuning to avoid reintroducing OOM).
                # Still add a tiny backoff to avoid immediate rescheduling loops in tight sims.
                s.op_backoff_until[oid] = max(s.op_backoff_until.get(oid, 0), s.tick)

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    num_pools = s.executor.num_pools
    pool_avail = []
    for i in range(num_pools):
        pool = s.executor.pools[i]
        pool_avail.append({
            "cpu": pool.avail_cpu_pool,
            "ram": pool.avail_ram_pool,
            "max_cpu": pool.max_cpu_pool,
            "max_ram": pool.max_ram_pool,
        })

    # If no pipelines are queued, nothing to do.
    if (len(s.q[Priority.QUERY]) + len(s.q[Priority.INTERACTIVE]) + len(s.q[Priority.BATCH_PIPELINE])) == 0:
        return suspensions, assignments

    planned_pipeline_ids = set()
    planned_op_ids = set()

    # Upper bound on iterations to ensure termination even with many blocked pipelines.
    total_q = len(s.q[Priority.QUERY]) + len(s.q[Priority.INTERACTIVE]) + len(s.q[Priority.BATCH_PIPELINE])
    iter_budget = max(20, 6 * total_q + 10)

    for _ in range(iter_budget):
        # Stop if no pool has any resources.
        any_resource = False
        for pa in pool_avail:
            if pa["cpu"] > 0 and pa["ram"] > 0:
                any_resource = True
                break
        if not any_resource:
            break

        high_waiting = (len(s.q[Priority.QUERY]) > 0) or (len(s.q[Priority.INTERACTIVE]) > 0)

        # Fairness: after enough high-priority placements, force a batch attempt first.
        if len(s.q[Priority.BATCH_PIPELINE]) > 0 and s.hp_since_batch >= s.max_hp_before_batch:
            prio_try = [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]
        else:
            prio_try = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

        picked = None
        for pri in prio_try:
            dq = s.q[pri]
            if not dq:
                continue
            picked = _find_placeable_from_queue(
                s, dq, pool_avail, num_pools, high_waiting,
                planned_pipeline_ids, planned_op_ids
            )
            if picked is not None:
                break

        if picked is None:
            # No placeable op found (likely blocked by resource hints/backoff).
            break

        p, op, pool_id, cpu, ram = picked

        # Create assignment.
        assignments.append(Assignment(
            ops=[op],
            cpu=cpu,
            ram=ram,
            priority=p.priority,
            pool_id=pool_id,
            pipeline_id=p.pipeline_id
        ))

        # Reserve resources in our local view to avoid over-assigning in the same tick.
        pool_avail[pool_id]["cpu"] -= cpu
        pool_avail[pool_id]["ram"] -= ram

        planned_pipeline_ids.add(p.pipeline_id)
        planned_op_ids.add(id(op))

        # Update fairness counter.
        if p.priority == Priority.BATCH_PIPELINE:
            s.hp_since_batch = 0
        else:
            s.hp_since_batch += 1

    return suspensions, assignments
