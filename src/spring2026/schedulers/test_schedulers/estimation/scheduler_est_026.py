# policy_key: scheduler_est_026
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052654
# generation_seconds: 52.53
# generated_at: 2026-04-10T10:05:40.200670
@register_scheduler_init(key="scheduler_est_026")
def scheduler_est_026_init(s):
    """Priority + memory-estimate-aware round-robin scheduler.

    Goals:
      - Minimize weighted latency (query > interactive > batch) by prioritizing high-priority work.
      - Reduce OOM-driven failures (which are extremely costly) by using op.estimate.mem_peak_gb
        with a conservative safety factor and an adaptive per-pipeline RAM multiplier on OOM.
      - Avoid starvation via round-robin within each priority class (and mild aging-like behavior).
    """
    from collections import deque

    # Per-priority round-robin queues of pipeline_ids (pipelines stored in s._pipelines_by_id)
    s._q_query = deque()
    s._q_interactive = deque()
    s._q_batch = deque()

    # Pipeline registry and per-pipeline tuning state
    s._pipelines_by_id = {}          # pipeline_id -> Pipeline
    s._ram_mult_by_pid = {}          # pipeline_id -> float (adaptive multiplier after OOM)
    s._hard_failed_pid = set()       # pipelines that failed non-OOM (do not retry)
    s._arrived_pid = set()           # for idempotent enqueue

    # Tunables (start with small improvements over FIFO; keep logic robust)
    s._scan_cap_per_queue = 25       # cap how many pipelines we scan per queue for a fit
    s._max_assignments_per_pool = 2  # avoid over-fragmenting pools; keep headroom for big ops


def _scheduler_est_026__prio_weight(priority):
    # Larger number => higher scheduling preference
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1  # batch


def _scheduler_est_026__get_queue(s, priority):
    if priority == Priority.QUERY:
        return s._q_query
    if priority == Priority.INTERACTIVE:
        return s._q_interactive
    return s._q_batch


def _scheduler_est_026__iter_queues_in_order(s):
    # Strict priority: query -> interactive -> batch
    return (s._q_query, s._q_interactive, s._q_batch)


def _scheduler_est_026__get_pipeline_id_from_result(r):
    # Be defensive: different sims may attach pipeline_id differently.
    pid = getattr(r, "pipeline_id", None)
    if pid is not None:
        return pid
    ops = getattr(r, "ops", None) or []
    if ops:
        return getattr(ops[0], "pipeline_id", None)
    return None


def _scheduler_est_026__is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).upper()
    return ("OOM" in msg) or ("OUT OF MEMORY" in msg) or ("MEMORY" in msg and "KILL" in msg)


def _scheduler_est_026__estimate_mem_gb(op):
    est = getattr(op, "estimate", None)
    if est is None:
        return None
    return getattr(est, "mem_peak_gb", None)


def _scheduler_est_026__ram_safety_factor(priority):
    # Conservative but not extreme; keep completion high while not over-reserving.
    if priority == Priority.QUERY:
        return 1.30
    if priority == Priority.INTERACTIVE:
        return 1.40
    return 1.55  # batch


def _scheduler_est_026__cpu_fraction(priority):
    # Favor high-priority latency while leaving room for concurrency.
    if priority == Priority.QUERY:
        return 1.00
    if priority == Priority.INTERACTIVE:
        return 0.75
    return 0.50


def _scheduler_est_026__required_ram(op, pool, priority, ram_mult):
    """
    Convert (noisy) memory estimate into a conservative RAM request.
    - If estimate exists: request est * safety * ram_mult, clamped to pool max.
    - If estimate missing: request a conservative fraction of pool max * ram_mult.
    """
    est_mem = _scheduler_est_026__estimate_mem_gb(op)
    safety = _scheduler_est_026__ram_safety_factor(priority)

    if est_mem is None:
        # Unknown: avoid tiny allocations that cause OOM cascades.
        base = 0.55 * pool.max_ram_pool
        req = base * max(1.0, ram_mult)
    else:
        req = est_mem * safety * max(1.0, ram_mult)

        # If estimate is extremely small, still avoid pathological undersizing.
        req = max(req, 0.10 * pool.max_ram_pool)

    # Clamp to pool max (can't request more than pool can provide).
    if req > pool.max_ram_pool:
        req = pool.max_ram_pool

    return req


def _scheduler_est_026__try_pick_pipeline_for_pool(s, pool):
    """
    Choose a (pipeline, op, req_ram, priority) that fits this pool's current headroom.
    Strategy:
      - strict priority among queues
      - within a queue: scan a capped number of pipeline_ids and pick the first that can run now
        (parents complete) and fits in this pool's available RAM.
      - round-robin by rotating scanned pipelines to the back.
    """
    best = None

    for q in _scheduler_est_026__iter_queues_in_order(s):
        if not q:
            continue

        scan_n = min(len(q), s._scan_cap_per_queue)
        for _ in range(scan_n):
            pid = q.popleft()
            p = s._pipelines_by_id.get(pid)
            if p is None:
                continue  # stale

            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue  # drop completed
            if pid in s._hard_failed_pid:
                continue  # don't retry hard failures

            # Consider only ops ready to run (parents complete)
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable yet; keep in RR rotation.
                q.append(pid)
                continue

            op = op_list[0]
            ram_mult = s._ram_mult_by_pid.get(pid, 1.0)
            req_ram = _scheduler_est_026__required_ram(op, pool, p.priority, ram_mult)

            # Fast feasibility check (with noisy estimator as a hint, not a guarantee)
            if req_ram <= pool.avail_ram_pool and pool.avail_cpu_pool >= 1:
                # Found a fit; keep pipeline in rotation for future ops.
                q.append(pid)
                best = (p, op, req_ram, p.priority)
                break

            # Doesn't fit now; rotate to back.
            q.append(pid)

        if best is not None:
            break

    return best


@register_scheduler(key="scheduler_est_026")
def scheduler_est_026_scheduler(s, results, pipelines):
    """
    Main scheduling loop:
      1) Ingest new pipelines into per-priority RR queues.
      2) Process results; on OOM failures, increase RAM multiplier and retry quickly.
      3) For each pool, greedily place up to N ops using strict priority + memory-fit checks.
    """
    from collections import deque

    # 1) Ingest arrivals
    for p in pipelines:
        pid = p.pipeline_id
        s._pipelines_by_id[pid] = p
        if pid not in s._arrived_pid:
            _scheduler_est_026__get_queue(s, p.priority).append(pid)
            s._arrived_pid.add(pid)
        if pid not in s._ram_mult_by_pid:
            s._ram_mult_by_pid[pid] = 1.0

    # Early exit if nothing changed (matches simulator expectations)
    if not pipelines and not results:
        return [], []

    # 2) React to execution results
    for r in results:
        if not r.failed():
            continue

        pid = _scheduler_est_026__get_pipeline_id_from_result(r)
        if pid is None:
            continue

        # If it looks like OOM, retry with more RAM; otherwise mark as hard-failed.
        if _scheduler_est_026__is_oom_error(getattr(r, "error", None)):
            cur = s._ram_mult_by_pid.get(pid, 1.0)
            # Multiplicative backoff; cap to avoid permanently reserving absurd amounts.
            s._ram_mult_by_pid[pid] = min(cur * 1.6, 6.0)

            p = s._pipelines_by_id.get(pid)
            if p is not None and pid not in s._hard_failed_pid:
                # Re-enqueue near the front to reduce repeated long waits (OOM already wasted time).
                q = _scheduler_est_026__get_queue(s, p.priority)
                q.appendleft(pid)
        else:
            s._hard_failed_pid.add(pid)

    # 3) Build assignments pool-by-pool
    suspensions = []
    assignments = []

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        made = 0
        while (
            made < s._max_assignments_per_pool
            and pool.avail_cpu_pool >= 1
            and pool.avail_ram_pool > 0
        ):
            pick = _scheduler_est_026__try_pick_pipeline_for_pool(s, pool)
            if pick is None:
                break

            p, op, req_ram, prio = pick

            # CPU sizing: give high-priority more CPU, but keep some room for concurrency.
            cpu_frac = _scheduler_est_026__cpu_fraction(prio)
            req_cpu = max(1.0, pool.avail_cpu_pool * cpu_frac)

            # Avoid requesting more than available
            req_cpu = min(req_cpu, pool.avail_cpu_pool)
            req_ram = min(req_ram, pool.avail_ram_pool)

            # If after clamping we can't meet minimal viability, stop trying this pool.
            if req_cpu <= 0 or req_ram <= 0:
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            made += 1

            # Update local view of pool resources so we can pack multiple ops in one tick.
            pool.avail_cpu_pool -= req_cpu
            pool.avail_ram_pool -= req_ram

    return suspensions, assignments
