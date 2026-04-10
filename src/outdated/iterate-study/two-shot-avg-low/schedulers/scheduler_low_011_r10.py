# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r10
@register_scheduler_init(key="scheduler_low_011_r10")
def scheduler_low_011_r10_init(s):
    """Priority-aware, latency-first scheduler (incremental step up from prior attempt).

    Key changes vs the previous iteration:
    1) Fix obvious flaw: do NOT drop pipelines just because they have FAILED ops.
       - FAILED ops are in ASSIGNABLE_STATES and can be retried (especially after OOM).
       - We only stop retrying after a small retry budget or for non-OOM failures we observe.
    2) Two-phase scheduling:
       - Phase A: schedule high-priority (QUERY, INTERACTIVE) first across pools, potentially multiple per pool
         with small/medium CPU requests to start quickly and reduce queueing latency.
       - Phase B: schedule BATCH with headroom reservation when high-priority backlog exists, and avoid
         placing batch into the "interactive" pool when high-priority work is waiting.
    3) OOM-aware RAM hinting and "boost" to the front of the queue for faster retries.
    4) Prevent duplicate scheduling in the same tick (at most one op per pipeline per tick).
    """
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # RAM hints learned from OOMs: (pipeline_id, op_id) -> ram
    s.ram_hints = {}
    # Attempts for retry bounding: (pipeline_id, op_id) -> count
    s.op_attempts = {}

    # Pipelines we consider terminally failed (non-OOM or exceeded retries)
    s.dead_pipelines = set()

    # Pipeline IDs to boost to the front next tick (e.g., after OOM retry sizing increases)
    s.boost_pipeline_ids = set()

    # Configuration knobs
    s.max_retries_per_op = 3

    # "Interactive" pool preference (best-effort only)
    s.interactive_pool_id = 0

    # CPU caps for latency-first parallelism when backlog exists
    s.hp_cpu_caps = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 4.0,
    }

    # Baseline RAM fractions of pool max for high priority (helps avoid OOM-induced latency spikes)
    s.hp_ram_frac = {
        Priority.QUERY: 0.45,
        Priority.INTERACTIVE: 0.45,
    }

    # When there is any high-priority backlog, reserve headroom for it (esp. in interactive pool)
    s.reserve_frac_interactive_pool = {"cpu": 0.25, "ram": 0.25}
    s.reserve_frac_other_pools = {"cpu": 0.10, "ram": 0.10}

    # Limit scanning per priority queue per allocation attempt (avoid O(n^2) on long queues)
    s.max_queue_scan = 32

    # Limit number of high-priority assignments per pool per tick (avoid over-fragmentation)
    s.max_hp_assignments_per_pool = 8


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_key(pipeline_id, op):
    return (pipeline_id, id(op))


def _pipeline_is_live(s, p):
    if p is None:
        return False
    pid = getattr(p, "pipeline_id", None)
    if pid is not None and pid in s.dead_pipelines:
        return False
    status = p.runtime_status()
    if status.is_pipeline_successful():
        return False
    return True


def _next_assignable_op(p):
    status = p.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _rotate_find_candidate(s, queue, scheduled_pipelines, max_scan):
    """FIFO-preserving scan: rotate unschedulable pipelines to the back."""
    n = min(len(queue), max_scan)
    for _ in range(n):
        p = queue.pop(0)
        pid = getattr(p, "pipeline_id", None)

        if pid is not None and pid in scheduled_pipelines:
            queue.append(p)
            continue

        if not _pipeline_is_live(s, p):
            # Drop completed/dead pipelines from the queue
            continue

        op = _next_assignable_op(p)
        if op is None:
            # Not ready yet; keep it in FIFO rotation
            queue.append(p)
            continue

        return p, op

    return None, None


def _pool_reserve_fracs(s, pool_id):
    if pool_id == s.interactive_pool_id:
        return s.reserve_frac_interactive_pool
    return s.reserve_frac_other_pools


def _hp_backlog_size(s):
    # Approximate backlog size; good enough to decide between "finish fast" vs "start many".
    return len(s.waiting_queues[Priority.QUERY]) + len(s.waiting_queues[Priority.INTERACTIVE])


def _hp_any_backlog(s):
    return _hp_backlog_size(s) > 0


def _hp_request(s, pool, rem_cpu, rem_ram, priority, pipeline_id, op, hp_backlog):
    # CPU strategy:
    # - If only ~1 HP pipeline waiting, finish it fast (use more CPU if available).
    # - If more HP backlog, cap CPU to start more work quickly (lower queueing latency).
    if hp_backlog <= 1:
        cpu = rem_cpu
    else:
        cpu = min(rem_cpu, float(s.hp_cpu_caps.get(priority, 2.0)))

    cpu = max(1.0, cpu)

    # RAM strategy:
    # - Use a healthy fraction of pool max to reduce OOM-driven retries (latency spikes).
    # - Respect any learned OOM hint; if it doesn't fit now, skip scheduling this op.
    base_ram = min(rem_ram, max(1.0, pool.max_ram_pool * float(s.hp_ram_frac.get(priority, 0.45))))
    hint = float(s.ram_hints.get(_op_key(pipeline_id, op), 0.0))
    ram = max(base_ram, hint)
    ram = min(rem_ram, ram)
    ram = max(1.0, ram)

    return cpu, ram


def _batch_request(rem_cpu, rem_ram):
    # Batch: prefer scale-up / packing (use remaining capacity in the pool in one container).
    cpu = max(1.0, rem_cpu)
    ram = max(1.0, rem_ram)
    return cpu, ram


def _stable_boost_front(queue, boosted_id_set):
    # Stable partition: boosted pipelines first.
    if not queue or not boosted_id_set:
        return queue
    boosted = []
    rest = []
    for p in queue:
        pid = getattr(p, "pipeline_id", None)
        if pid is not None and pid in boosted_id_set:
            boosted.append(p)
        else:
            rest.append(p)
    return boosted + rest


@register_scheduler(key="scheduler_low_011_r10")
def scheduler_low_011_r10(s, results, pipelines):
    # Enqueue new arrivals
    for p in pipelines:
        pr = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
        s.waiting_queues[pr].append(p)

    # Learn from results (OOM -> increase RAM hint & retry; non-OOM -> stop retrying this pipeline)
    for r in results:
        # Determine whether it's a failure
        is_failed = False
        try:
            is_failed = bool(r.failed())
        except Exception:
            is_failed = getattr(r, "error", None) is not None

        if not is_failed:
            continue

        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            # Without pipeline_id we can't reliably track retries/hints; be conservative.
            continue

        err = getattr(r, "error", None)
        oom = _is_oom_error(err)

        ops = getattr(r, "ops", None) or []
        if not ops:
            # If no operator objects are provided, treat as terminal failure for safety.
            if not oom:
                s.dead_pipelines.add(pid)
            continue

        for op in ops:
            k = _op_key(pid, op)
            prev_attempts = int(s.op_attempts.get(k, 0))
            new_attempts = prev_attempts + 1
            s.op_attempts[k] = new_attempts

            if oom:
                # Exponential backoff on RAM to converge quickly.
                observed_ram = float(getattr(r, "ram", 0.0) or 0.0)
                prev_hint = float(s.ram_hints.get(k, 0.0))
                baseline = max(1.0, observed_ram, prev_hint)
                s.ram_hints[k] = baseline * 2.0

                if new_attempts <= s.max_retries_per_op:
                    # Boost pipeline to improve retry latency.
                    s.boost_pipeline_ids.add(pid)
                else:
                    # Retry budget exhausted -> stop trying this pipeline.
                    s.dead_pipelines.add(pid)
            else:
                # Non-OOM failure: treat as terminal (avoid infinite retries).
                s.dead_pipelines.add(pid)

    # Reorder queues to front-load boosted pipelines (helps OOM retries)
    if s.boost_pipeline_ids:
        for pr in s.waiting_queues:
            s.waiting_queues[pr] = _stable_boost_front(s.waiting_queues[pr], s.boost_pipeline_ids)
        # Only boost for one scheduling decision epoch
        s.boost_pipeline_ids.clear()

    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Track per-tick scheduling to avoid duplicate assignment from the same pipeline in one tick.
    scheduled_pipelines = set()

    # Decide pool traversal order: interactive pool first (best chance to minimize HP latency)
    pool_order = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1 and 0 <= s.interactive_pool_id < s.executor.num_pools:
        pool_order = [s.interactive_pool_id] + [i for i in range(s.executor.num_pools) if i != s.interactive_pool_id]

    # Compute backlog once (approx). We'll refresh lightly as we schedule.
    hp_backlog = _hp_backlog_size(s)
    hp_waiting = hp_backlog > 0

    # Phase A: schedule high-priority work first, potentially multiple per pool.
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        rem_cpu = float(pool.avail_cpu_pool)
        rem_ram = float(pool.avail_ram_pool)
        if rem_cpu <= 0 or rem_ram <= 0:
            continue

        hp_assigned = 0
        while hp_assigned < s.max_hp_assignments_per_pool and rem_cpu >= 1.0 and rem_ram >= 1.0:
            made_progress = False

            for pr in (Priority.QUERY, Priority.INTERACTIVE):
                q = s.waiting_queues[pr]

                p, op = _rotate_find_candidate(s, q, scheduled_pipelines, s.max_queue_scan)
                if p is None:
                    continue

                pid = getattr(p, "pipeline_id", None)
                if pid is None:
                    # Defensive: if pipeline_id missing, just rotate it back.
                    q.append(p)
                    continue

                # Compute request
                cpu, ram = _hp_request(s, pool, rem_cpu, rem_ram, pr, pid, op, hp_backlog)

                # If a learned OOM hint exists and doesn't fit now, rotate to back and try others.
                hint = float(s.ram_hints.get(_op_key(pid, op), 0.0))
                if hint > 0.0 and hint > rem_ram:
                    q.append(p)
                    continue

                if cpu > rem_cpu or ram > rem_ram:
                    q.append(p)
                    continue

                # Assign one op from this pipeline (one per pipeline per tick)
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=pr,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                )

                scheduled_pipelines.add(pid)
                rem_cpu -= cpu
                rem_ram -= ram
                hp_assigned += 1
                made_progress = True

                # Re-enqueue pipeline for future ops
                q.append(p)

                # Update approximate backlog as we make progress (optional, small improvement)
                hp_backlog = max(0, hp_backlog - 1)
                hp_waiting = hp_backlog > 0
                break  # re-check from highest prio again

            if not made_progress:
                break

    # Phase B: schedule batch work with headroom reservation when HP backlog exists.
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        rem_cpu = float(pool.avail_cpu_pool)
        rem_ram = float(pool.avail_ram_pool)
        if rem_cpu <= 0 or rem_ram <= 0:
            continue

        # Conservative: if HP is waiting, avoid placing batch in the interactive pool entirely.
        if hp_waiting and pool_id == s.interactive_pool_id:
            continue

        # Only attempt one batch assignment per pool per tick (avoid fragmentation & keep behavior stable).
        q = s.waiting_queues[Priority.BATCH_PIPELINE]
        p, op = _rotate_find_candidate(s, q, scheduled_pipelines, s.max_queue_scan)
        if p is None:
            continue

        pid = getattr(p, "pipeline_id", None)
        if pid is None:
            q.append(p)
            continue

        # Reserve headroom if HP exists (so HP can start next tick without being blocked by batch packing).
        if hp_waiting:
            fr = _pool_reserve_fracs(s, pool_id)
            reserve_cpu = float(pool.max_cpu_pool) * float(fr["cpu"])
            reserve_ram = float(pool.max_ram_pool) * float(fr["ram"])
            usable_cpu = max(0.0, rem_cpu - reserve_cpu)
            usable_ram = max(0.0, rem_ram - reserve_ram)
        else:
            usable_cpu = rem_cpu
            usable_ram = rem_ram

        if usable_cpu < 1.0 or usable_ram < 1.0:
            # Not enough leftover after reservation; rotate batch pipeline to back.
            q.append(p)
            continue

        cpu, ram = _batch_request(usable_cpu, usable_ram)

        if cpu > rem_cpu or ram > rem_ram:
            q.append(p)
            continue

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=Priority.BATCH_PIPELINE,
                pool_id=pool_id,
                pipeline_id=pid,
            )
        )
        scheduled_pipelines.add(pid)
        q.append(p)

    return suspensions, assignments