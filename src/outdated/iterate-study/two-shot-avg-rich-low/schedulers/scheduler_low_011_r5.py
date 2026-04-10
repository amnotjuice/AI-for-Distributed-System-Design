# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r5
@register_scheduler_init(key="scheduler_low_011_r5")
def scheduler_low_011_r5_init(s):
    """Priority-first, latency-oriented scheduler (incremental improvement over naive FIFO).

    Key changes vs. the previous iteration:
    - Work-conserving multi-assign per pool per tick (fixes underutilization that inflated queueing delay).
    - Strong isolation preference: keep pool 0 primarily for QUERY/INTERACTIVE to protect tail latency.
    - "Hot pipeline" boost: when an operator finishes, promote that pipeline to be scheduled sooner to reduce
      gaps between dependent operators (end-to-end latency).
    - Safer failure handling: retry FAILED operators a bounded number of times; OOM triggers RAM backoff.
    """
    s.tick = 0

    # Active pipelines by id (lets us react to results via pipeline_id without searching).
    s.active_pipelines = {}  # pipeline_id -> Pipeline
    s.pipeline_priority = {}  # pipeline_id -> Priority
    s.enqueue_tick = {}  # pipeline_id -> tick first seen

    # Per-priority FIFO queues of pipeline_ids (de-duplicated by in_queue set).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.in_queue = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # "Hot" pipelines: pipelines that just made progress (an op finished/failed) get a short-term boost.
    s.hot = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.hot_set = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Failure-driven hints per (pipeline_id, op_identity).
    s.op_hints = {}  # (pid, id(op)) -> {"ram": float, "cpu": float}
    s.op_attempts = {}  # (pid, id(op)) -> int
    s.op_non_retriable = set()  # keys that should not be retried

    # Retry/backoff knobs.
    s.max_retries_per_op = 2
    s.oom_backoff = 2.0

    # Latency vs. parallelism knobs (moderate per-op sizing to reduce queueing delay).
    s.base_cpu_target = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    # Fraction of pool max RAM to request by default (small to allow more parallelism; OOM will backoff).
    s.base_ram_frac = {
        Priority.QUERY: 0.20,
        Priority.INTERACTIVE: 0.20,
        Priority.BATCH_PIPELINE: 0.30,
    }

    # Affinity/isolation: prefer pool 0 for QUERY/INTERACTIVE, keep batch mostly off pool 0.
    s.interactive_pool_id = 0

    # If high-priority waits too long, allow it to spill to other pools.
    s.high_prio_spillover_age_ticks = 8


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_key(pid, op):
    return (pid, id(op))


def _ensure_pipeline_enqueued(s, p):
    pr = p.priority if p.priority in s.queues else Priority.BATCH_PIPELINE
    pid = p.pipeline_id

    # Track active pipeline object + metadata.
    s.active_pipelines[pid] = p
    s.pipeline_priority[pid] = pr
    if pid not in s.enqueue_tick:
        s.enqueue_tick[pid] = s.tick

    # De-duplicated enqueue into the normal FIFO queue.
    if pid not in s.in_queue[pr]:
        s.queues[pr].append(pid)
        s.in_queue[pr].add(pid)


def _mark_hot(s, pid, pr):
    if pid is None:
        return
    if pr not in s.hot:
        return
    if pid in s.hot_set[pr]:
        return
    s.hot[pr].append(pid)
    s.hot_set[pr].add(pid)


def _drop_pipeline(s, pid, pr):
    # Lazy removal: we remove from sets so it won't be re-enqueued; stale ids in lists are skipped later.
    try:
        s.in_queue[pr].discard(pid)
    except Exception:
        pass
    try:
        s.hot_set[pr].discard(pid)
    except Exception:
        pass

    s.active_pipelines.pop(pid, None)
    s.pipeline_priority.pop(pid, None)
    s.enqueue_tick.pop(pid, None)


def _pipeline_unrecoverable(s, p):
    status = p.runtime_status()
    # If there are FAILED ops, allow retry unless we've exceeded retry budget or marked non-retriable.
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    for op in failed_ops:
        k = _op_key(p.pipeline_id, op)
        if k in s.op_non_retriable:
            return True
        if s.op_attempts.get(k, 0) > s.max_retries_per_op:
            return True
    return False


def _has_high_backlog(s):
    # Conservative (may count stale ids); good enough for isolation decisions.
    return (len(s.in_queue[Priority.QUERY]) + len(s.in_queue[Priority.INTERACTIVE])) > 0


def _allowed_on_pool(s, pid, pr, pool_id):
    # Strong preference: keep batch off pool 0 when high-priority exists.
    if pool_id == s.interactive_pool_id:
        if pr == Priority.BATCH_PIPELINE and _has_high_backlog(s):
            return False
        return True

    # Non-zero pools: prefer batch there; allow high-priority spillover after waiting.
    if pr == Priority.BATCH_PIPELINE:
        return True

    age = s.tick - s.enqueue_tick.get(pid, s.tick)
    return age >= s.high_prio_spillover_age_ticks


def _next_candidate_pid(s, pr, use_hot_first=True):
    # Pop one candidate from HOT first (one-shot boost), otherwise from FIFO queue.
    if use_hot_first and s.hot[pr]:
        pid = s.hot[pr].pop(0)
        s.hot_set[pr].discard(pid)
        return pid, True

    q = s.queues[pr]
    if not q:
        return None, False

    pid = q.pop(0)
    # Maintain FIFO: rotate by putting it back immediately; selection logic may still schedule it this tick.
    q.append(pid)
    return pid, False


def _compute_request(s, pool, pr, pid, op, avail_cpu, avail_ram, boost=False, high_backlog_size=0):
    # CPU: moderate target to allow parallelism; boost if pipeline is "hot" or backlog is tiny.
    base_cpu = float(s.base_cpu_target.get(pr, 1.0))
    cpu = min(avail_cpu, base_cpu)

    # If the system isn't backlogged for high priority, give more CPU to finish faster.
    if pr in (Priority.QUERY, Priority.INTERACTIVE) and (boost or high_backlog_size <= 2):
        cpu = min(avail_cpu, max(base_cpu, min(pool.max_cpu_pool, avail_cpu)))

    cpu = max(1.0, cpu)

    # RAM: small default to increase parallelism; apply OOM-driven hint as a floor.
    base_ram = float(pool.max_ram_pool) * float(s.base_ram_frac.get(pr, 0.25))
    base_ram = max(1.0, base_ram)

    k = _op_key(pid, op)
    hint = s.op_hints.get(k, {})
    hint_ram = float(hint.get("ram", 0.0) or 0.0)

    ram = max(base_ram, hint_ram)
    ram = min(ram, pool.max_ram_pool, avail_ram)
    ram = max(1.0, ram)

    return cpu, ram


@register_scheduler(key="scheduler_low_011_r5")
def scheduler_low_011_r5(s, results, pipelines):
    """
    Latency-oriented priority scheduler:
    - Admits new pipelines into per-priority FIFO.
    - Updates "hot" pipelines based on results to reduce dependency gaps.
    - Fills each pool with multiple assignments per tick (work-conserving), respecting pool affinity:
        * Pool 0 prioritizes QUERY/INTERACTIVE to protect latency.
        * Other pools prioritize BATCH; high-priority can spill over after aging.
    - Retries OOM failures with increased RAM (bounded).
    """
    s.tick += 1

    # Admit new pipelines.
    for p in pipelines:
        _ensure_pipeline_enqueued(s, p)

    # Process results: update hotness + retry hints.
    for r in results:
        pr = getattr(r, "priority", None)
        if pr not in s.queues:
            pr = Priority.BATCH_PIPELINE
        pid = getattr(r, "pipeline_id", None)

        # Any state change means the pipeline is a good "hot" candidate (reduce gaps between ops).
        if pid is not None and pid in s.active_pipelines:
            _mark_hot(s, pid, pr)

        # Update retry state based on failure type.
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        if pid is None:
            continue

        ops = getattr(r, "ops", None) or []
        is_oom = _is_oom_error(getattr(r, "error", None))
        for op in ops:
            k = _op_key(pid, op)
            s.op_attempts[k] = int(s.op_attempts.get(k, 0)) + 1

            if is_oom and s.op_attempts[k] <= s.max_retries_per_op + 1:
                # Exponential RAM backoff using last attempted RAM (or prior hint) as baseline.
                prev_hint = s.op_hints.get(k, {})
                prev_hint_ram = float(prev_hint.get("ram", 0.0) or 0.0)
                last_ram = float(getattr(r, "ram", 0.0) or 0.0)
                baseline = max(1.0, prev_hint_ram, last_ram)
                new_ram = baseline * float(s.oom_backoff)

                # Keep CPU hint as last used (helps stability if future policies use it).
                last_cpu = float(getattr(r, "cpu", 1.0) or 1.0)
                s.op_hints[k] = {"ram": new_ram, "cpu": max(1.0, last_cpu)}
            else:
                # Non-OOM failures are treated as non-retriable to avoid churn.
                s.op_non_retriable.add(k)

    # If no arrivals and no results, nothing to do (determinism/perf).
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Avoid scheduling the same pipeline multiple times in a single tick (prevents duplicate assignment races).
    scheduled_this_tick = set()

    # Small helper for backlog size (for CPU boosting).
    high_backlog_size = len(s.in_queue[Priority.QUERY]) + len(s.in_queue[Priority.INTERACTIVE])

    # Fill pools work-conservingly.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Fill loop: keep launching containers until we run out of resources or work.
        # Keep a bounded number of selection attempts to avoid pathological scans on stale queue entries.
        selection_attempts_left = 64

        while avail_cpu >= 1.0 and avail_ram >= 1.0 and selection_attempts_left > 0:
            selection_attempts_left -= 1

            chosen = None  # (pid, pr, op, boost_flag)

            # Pool-aware priority traversal:
            # - Pool 0: QUERY -> INTERACTIVE -> (BATCH only if no high backlog)
            # - Other pools: BATCH -> INTERACTIVE -> QUERY (high pr only via spillover rule)
            if pool_id == s.interactive_pool_id:
                pr_order = [Priority.QUERY, Priority.INTERACTIVE]
                if not _has_high_backlog(s):
                    pr_order.append(Priority.BATCH_PIPELINE)
            else:
                pr_order = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]

            for pr in pr_order:
                # Try a few candidates per priority (hot first, then FIFO rotation).
                # This avoids spending the whole tick scanning a giant queue of blocked pipelines.
                for _ in range(6):
                    pid, boosted = _next_candidate_pid(s, pr, use_hot_first=True)
                    if pid is None:
                        break

                    # Skip stale/inactive pipelines.
                    p = s.active_pipelines.get(pid)
                    if p is None:
                        s.in_queue[pr].discard(pid)
                        continue

                    # Enforce affinity/isolation.
                    if not _allowed_on_pool(s, pid, pr, pool_id):
                        continue

                    # Do not double-schedule the same pipeline in this tick.
                    if pid in scheduled_this_tick:
                        continue

                    status = p.runtime_status()

                    # Drop fully completed pipelines.
                    if status.is_pipeline_successful():
                        _drop_pipeline(s, pid, pr)
                        continue

                    # Drop pipelines that are unrecoverable (bounded retry).
                    if _pipeline_unrecoverable(s, p):
                        _drop_pipeline(s, pid, pr)
                        continue

                    # Choose exactly one assignable op (parents must be complete).
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
                    if not op_list:
                        continue

                    op = op_list[0]
                    chosen = (pid, pr, op, boosted)
                    break

                if chosen is not None:
                    break

            if chosen is None:
                break

            pid, pr, op, boosted = chosen

            cpu_req, ram_req = _compute_request(
                s=s,
                pool=pool,
                pr=pr,
                pid=pid,
                op=op,
                avail_cpu=avail_cpu,
                avail_ram=avail_ram,
                boost=boosted,
                high_backlog_size=high_backlog_size,
            )

            # If we can't fit the request, try to downsize CPU a bit (RAM is usually the hard constraint).
            if ram_req > avail_ram:
                # Can't fit this op now; let other pools/ops try.
                continue

            if cpu_req > avail_cpu:
                cpu_req = max(1.0, min(avail_cpu, cpu_req))

            if cpu_req <= 0 or ram_req <= 0:
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pr,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            scheduled_this_tick.add(pid)
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)

    return suspensions, assignments