# policy_key: scheduler_high_007
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.159765
# generation_seconds: 198.85
# generated_at: 2026-04-10T00:32:40.221257
@register_scheduler_init(key="scheduler_high_007")
def scheduler_high_007_init(s):
    """
    Priority-aware, OOM-adaptive scheduler aimed at minimizing weighted latency while
    strongly avoiding pipeline failures/incompletes.

    Core ideas:
      - Strict priority order: QUERY > INTERACTIVE > BATCH (with mild batch aging).
      - Per-operator instance RAM hints: start modest, exponentially back off on OOM,
        remember successful RAM to reduce repeat failures.
      - Soft reservations: when high-priority runnable work exists, keep a fraction of
        each pool's CPU/RAM available to reduce head-of-line blocking from batch.
      - Fairness: round-robin within each priority; periodic/aged batch scheduling to
        avoid starvation/incompletes.
      - Safety: "poison" operators that repeatedly fail for non-OOM reasons to avoid
        burning resources on hopeless retries.
    """
    s.ticks = 0

    # Per-priority waiting queues (pipelines stay here until completed).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.in_queue = set()  # pipeline_id present in any queue
    s.pipeline_arrival_tick = {}  # pipeline_id -> tick first seen

    # Per-operator-instance learning (keyed by id(op) for correctness within a run).
    s.op_ram_hint = {}  # op_id -> RAM that previously succeeded (or increased after OOM)
    s.op_oom_failures = {}  # op_id -> count
    s.op_other_failures = {}  # op_id -> count
    s.op_poisoned = set()  # op_id -> don't retry if repeatedly non-OOM failing

    # Tuning knobs (kept intentionally simple/robust).
    s.scan_limit_per_priority = 8          # how many pipelines to scan when picking next runnable
    s.runnable_presence_scan = 6           # how many to scan to decide if high-priority runnable exists
    s.oom_ram_backoff = 2.0                # multiply RAM hint on OOM
    s.max_other_failures = 2               # after this many non-OOM failures, poison the op instance
    s.ram_per_cpu_overcommit = 1.15        # RAM baseline from CPU share: (ram/cpu)*cpu*factor

    # Soft reservations to protect query/interactive latency (pool0 treated as "primary").
    s.reserve_cpu_frac_primary = 0.25
    s.reserve_ram_frac_primary = 0.25
    s.reserve_cpu_frac_other = 0.15
    s.reserve_ram_frac_other = 0.15

    # Batch anti-starvation controls.
    s.batch_aging_threshold = 120          # ticks; if exceeded, batch allowed even under high load
    s.last_batch_tick_by_pool = {}         # pool_id -> last tick we scheduled batch there
    s.batch_min_spacing_primary = 8        # ticks between batch dispatches on primary pool when busy
    s.batch_min_spacing_other = 4          # ticks between batch dispatches on non-primary pools when busy


def _is_oom_error(err) -> bool:
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memoryerror" in e)


def _pipeline_total_ops(pipeline) -> int:
    # pipeline.values is described as the DAG of operators; be defensive about its type.
    try:
        return len(pipeline.values)
    except Exception:
        return 0


def _pipeline_remaining_ops(pipeline, status) -> int:
    total = _pipeline_total_ops(pipeline)
    try:
        completed = status.state_counts.get(OperatorState.COMPLETED, 0)
    except Exception:
        completed = 0
    if total <= 0:
        # Fallback: prioritize by fewer non-completed known states if total unknown.
        try:
            pending = status.state_counts.get(OperatorState.PENDING, 0)
            failed = status.state_counts.get(OperatorState.FAILED, 0)
            assigned = status.state_counts.get(OperatorState.ASSIGNED, 0)
            running = status.state_counts.get(OperatorState.RUNNING, 0)
            return pending + failed + assigned + running
        except Exception:
            return 0
    return max(0, total - completed)


def _queue_has_runnable(s, prio) -> bool:
    q = s.queues.get(prio, [])
    scan = min(len(q), s.runnable_presence_scan)
    for i in range(scan):
        p = q[i]
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if ops:
            op0 = ops[0]
            if id(op0) not in s.op_poisoned:
                return True
    return False


def _oldest_age(s, prio) -> int:
    q = s.queues.get(prio, [])
    if not q:
        return 0
    oldest = None
    for p in q[: min(len(q), 12)]:
        at = s.pipeline_arrival_tick.get(p.pipeline_id, s.ticks)
        age = s.ticks - at
        if oldest is None or age > oldest:
            oldest = age
    return oldest or 0


def _suggest_cpu(priority, pool, avail_cpu):
    # Give higher priority more CPU to reduce latency; keep modest to allow concurrency.
    if priority == Priority.QUERY:
        frac = 0.60
        floor = 1.0
    elif priority == Priority.INTERACTIVE:
        frac = 0.45
        floor = 1.0
    else:
        frac = 0.25
        floor = 1.0

    target = pool.max_cpu_pool * frac
    cpu = min(avail_cpu, max(floor, target))
    # If CPU is extremely scarce, allow tiny slices rather than stalling completely.
    if cpu <= 0 and avail_cpu > 0:
        cpu = min(avail_cpu, floor)
    return cpu


def _suggest_ram(s, priority, pool, cpu, avail_ram, op_id):
    # Baseline RAM tied to CPU share (helps avoid pathological under-allocation),
    # plus a minimum fraction per priority, plus a learned hint if available.
    max_cpu = pool.max_cpu_pool if pool.max_cpu_pool > 0 else 1.0
    per_cpu = pool.max_ram_pool / max_cpu
    baseline = per_cpu * cpu * s.ram_per_cpu_overcommit

    if priority == Priority.QUERY:
        min_frac = 0.18
    elif priority == Priority.INTERACTIVE:
        min_frac = 0.14
    else:
        min_frac = 0.10

    baseline = max(baseline, pool.max_ram_pool * min_frac)

    hint = s.op_ram_hint.get(op_id)
    if hint is not None:
        baseline = max(baseline, hint)

    ram = min(avail_ram, min(pool.max_ram_pool, baseline))
    if ram <= 0 and avail_ram > 0:
        ram = min(avail_ram, pool.max_ram_pool * min_frac)
    return ram


def _process_results_update_hints(s, results):
    for r in results:
        ops = getattr(r, "ops", None) or []
        if not ops:
            continue

        pool = None
        max_ram = None
        try:
            pool = s.executor.pools[r.pool_id]
            max_ram = pool.max_ram_pool
        except Exception:
            pool = None
            max_ram = None

        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = False

        if failed:
            err = getattr(r, "error", None)
            is_oom = _is_oom_error(err)
            for op in ops:
                op_id = id(op)
                if is_oom:
                    s.op_oom_failures[op_id] = s.op_oom_failures.get(op_id, 0) + 1
                    prev = s.op_ram_hint.get(op_id, 0.0)
                    assigned = float(getattr(r, "ram", 0.0) or 0.0)
                    base = max(prev, assigned)
                    new_hint = base * float(s.oom_ram_backoff)
                    if max_ram is not None:
                        new_hint = min(float(max_ram), new_hint)
                    # Ensure we make progress even if assigned RAM wasn't recorded.
                    if new_hint <= 0 and max_ram is not None:
                        new_hint = float(max_ram) * 0.20
                    if new_hint > 0:
                        s.op_ram_hint[op_id] = new_hint
                else:
                    s.op_other_failures[op_id] = s.op_other_failures.get(op_id, 0) + 1
                    if s.op_other_failures[op_id] >= s.max_other_failures:
                        s.op_poisoned.add(op_id)
        else:
            # Success: remember RAM as a safe lower bound for this operator instance; clear failure state.
            assigned = float(getattr(r, "ram", 0.0) or 0.0)
            for op in ops:
                op_id = id(op)
                if assigned > 0:
                    prev = float(s.op_ram_hint.get(op_id, 0.0) or 0.0)
                    s.op_ram_hint[op_id] = max(prev, assigned)
                s.op_oom_failures.pop(op_id, None)
                s.op_other_failures.pop(op_id, None)
                s.op_poisoned.discard(op_id)


def _prune_completed_pipelines(s):
    for prio, q in list(s.queues.items()):
        kept = []
        for p in q:
            try:
                if p.runtime_status().is_pipeline_successful():
                    s.in_queue.discard(p.pipeline_id)
                    continue
            except Exception:
                pass
            kept.append(p)
        s.queues[prio] = kept


def _select_candidate_from_priority_queue(
    s,
    prio,
    pool,
    pool_id,
    avail_cpu,
    avail_ram,
    reserve_cpu,
    reserve_ram,
    high_runnable,
    scheduled_this_tick,
):
    q = s.queues.get(prio, [])
    if not q:
        return None

    # Scan a small prefix to keep overhead bounded.
    scan = min(len(q), s.scan_limit_per_priority)

    best_idx = None
    best_score = None
    best_tuple = None  # (pipeline, op, cpu, ram)

    for i in range(scan):
        p = q[i]
        pid = p.pipeline_id
        if pid in scheduled_this_tick:
            continue

        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue

        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            continue

        op = ops[0]
        op_id = id(op)
        if op_id in s.op_poisoned:
            continue

        cpu = _suggest_cpu(prio, pool, avail_cpu)
        ram = _suggest_ram(s, prio, pool, cpu, avail_ram, op_id)

        if cpu <= 0 or ram <= 0:
            continue
        if cpu > avail_cpu or ram > avail_ram:
            continue

        # When high-priority runnable work exists, batch must not consume into reserved headroom.
        if prio == Priority.BATCH_PIPELINE and high_runnable:
            if (avail_cpu - cpu) < reserve_cpu or (avail_ram - ram) < reserve_ram:
                continue

        # Prefer "shorter remaining" (SRPT-ish), then older to avoid starvation.
        rem = _pipeline_remaining_ops(p, st)
        age = s.ticks - s.pipeline_arrival_tick.get(pid, s.ticks)
        score = (rem, -age, i)

        if best_score is None or score < best_score:
            best_score = score
            best_idx = i
            best_tuple = (p, op, cpu, ram)

    if best_tuple is None:
        return None

    # Remove the chosen pipeline from the queue now; caller will re-append to back after assignment.
    del q[best_idx]
    s.queues[prio] = q
    return best_tuple


def _requeue_pipeline_to_back(s, pipeline):
    # Put pipeline back to its priority queue unless completed.
    try:
        if pipeline.runtime_status().is_pipeline_successful():
            s.in_queue.discard(pipeline.pipeline_id)
            return
    except Exception:
        pass
    s.queues[pipeline.priority].append(pipeline)


@register_scheduler(key="scheduler_high_007")
def scheduler_high_007_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - ingest new pipelines
      - update per-operator RAM hints from results (OOM backoff + success floors)
      - dispatch assignments across pools with soft reservations under high-priority runnable load
    """
    s.ticks += 1

    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.in_queue:
            continue
        s.in_queue.add(pid)
        s.pipeline_arrival_tick[pid] = s.ticks
        s.queues[p.priority].append(p)

    # Early exit if nothing changes.
    if not pipelines and not results:
        return [], []

    # Learn from previous executions (especially OOM) before deciding new placements.
    _process_results_update_hints(s, results)

    # Remove completed pipelines from queues.
    _prune_completed_pipelines(s)

    # Determine if there is runnable high-priority work (not just "present in queue").
    high_runnable = _queue_has_runnable(s, Priority.QUERY) or _queue_has_runnable(s, Priority.INTERACTIVE)

    # If batch has waited too long, relax protections to ensure completion.
    batch_starved = _oldest_age(s, Priority.BATCH_PIPELINE) > s.batch_aging_threshold

    suspensions = []
    assignments = []

    scheduled_this_tick = set()  # pipeline_id -> at most one assignment per tick (reduces hogging)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pool-dependent reservation strength (pool0 acts like "primary").
        if pool_id == 0:
            reserve_cpu_frac = s.reserve_cpu_frac_primary
            reserve_ram_frac = s.reserve_ram_frac_primary
            batch_spacing = s.batch_min_spacing_primary
        else:
            reserve_cpu_frac = s.reserve_cpu_frac_other
            reserve_ram_frac = s.reserve_ram_frac_other
            batch_spacing = s.batch_min_spacing_other

        reserve_cpu = float(pool.max_cpu_pool) * reserve_cpu_frac if high_runnable else 0.0
        reserve_ram = float(pool.max_ram_pool) * reserve_ram_frac if high_runnable else 0.0

        iter_guard = 0
        while avail_cpu > 0 and avail_ram > 0 and iter_guard < 64:
            iter_guard += 1
            made_assignment = False

            # Priority order; batch may be throttled under high-priority runnable load.
            for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                if prio == Priority.BATCH_PIPELINE and high_runnable and (not batch_starved):
                    last_bt = s.last_batch_tick_by_pool.get(pool_id, -10**9)
                    if (s.ticks - last_bt) < batch_spacing:
                        continue

                candidate = _select_candidate_from_priority_queue(
                    s=s,
                    prio=prio,
                    pool=pool,
                    pool_id=pool_id,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    reserve_cpu=reserve_cpu,
                    reserve_ram=reserve_ram,
                    high_runnable=high_runnable,
                    scheduled_this_tick=scheduled_this_tick,
                )
                if candidate is None:
                    continue

                pipeline, op, cpu, ram = candidate

                # Final safety clamp (should already fit).
                cpu = min(cpu, avail_cpu)
                ram = min(ram, avail_ram)
                if cpu <= 0 or ram <= 0:
                    _requeue_pipeline_to_back(s, pipeline)
                    continue

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )

                scheduled_this_tick.add(pipeline.pipeline_id)
                avail_cpu -= cpu
                avail_ram -= ram

                if prio == Priority.BATCH_PIPELINE:
                    s.last_batch_tick_by_pool[pool_id] = s.ticks

                # Round-robin fairness: place pipeline at back for future consideration.
                _requeue_pipeline_to_back(s, pipeline)

                made_assignment = True
                break  # restart from top priority with updated resources

            if not made_assignment:
                break

    return suspensions, assignments
