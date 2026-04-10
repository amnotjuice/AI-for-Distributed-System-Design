# policy_key: scheduler_high_048
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.191183
# generation_seconds: 133.79
# generated_at: 2026-03-14T04:35:03.976075
@register_scheduler_init(key="scheduler_high_048")
def scheduler_high_048_init(s):
    """
    Priority-aware, latency-improving scheduler (incremental upgrade over naive FIFO).

    Core changes vs naive:
      - Maintain per-priority queues (QUERY > INTERACTIVE > BATCH_PIPELINE).
      - Avoid "give entire pool to one op": cap CPU/RAM per assignment and place multiple ops per tick.
      - Light aging to prevent indefinite batch starvation.
      - OOM-aware RAM backoff: on OOM failures, retry the failed op with more RAM (bounded retries).
      - Drop pipelines on non-OOM failures to avoid infinite retry loops.
    """
    from collections import deque, defaultdict

    # Time/aging
    s.tick = 0
    s.age_quantum = 50  # smaller => faster aging into service

    # Queues by priority
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Track when a pipeline last (re-)entered the queue for aging/fairness
    s.enqueue_tick = {}  # pipeline_id -> tick

    # Track pipeline objects (best-effort; used for cleanup bookkeeping)
    s.pipeline_by_id = {}

    # Failure handling
    s.nonretriable_pipelines = set()  # pipeline_id that had non-OOM failures or exceeded OOM retries

    # OOM mitigation: keep per-op RAM estimates and retry counts
    s.op_ram_est = {}  # id(op) -> estimated minimum RAM to avoid OOM
    s.op_oom_retries = defaultdict(int)  # id(op) -> count
    s.max_oom_retries = 3

    # Map operator object -> pipeline_id (so we can attribute ExecutionResult to a pipeline robustly)
    s.op_to_pipeline = {}

    # Guardrails: limit scheduler work per pool/tick
    s.max_assignments_per_tick_per_pool = 8


@register_scheduler(key="scheduler_high_048")
def scheduler_high_048(s, results, pipelines):
    """
    See init docstring for policy overview.

    Scheduler loop:
      1) Ingest new pipelines into priority queues.
      2) Process results: classify failures (OOM vs non-OOM) and update RAM estimates.
      3) For each pool, greedily place small/medium sized operators, prioritizing high-priority queues.
    """

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).upper()
        return ("OOM" in msg) or ("OUT OF MEMORY" in msg) or ("MEMORY" in msg and "KILL" in msg)

    def _base_weight(priority) -> float:
        # Larger => scheduled earlier
        if priority == Priority.QUERY:
            return 3.0
        if priority == Priority.INTERACTIVE:
            return 2.0
        return 1.0  # batch

    def _age_bonus(pipeline_id) -> float:
        # Small bonus to prevent indefinite starvation; capped to preserve latency preference.
        enq = s.enqueue_tick.get(pipeline_id, s.tick)
        age = max(0, s.tick - enq)
        return min(1.5, age / float(max(1, s.age_quantum)))

    def _has_high_waiting() -> bool:
        return bool(s.queues[Priority.QUERY]) or bool(s.queues[Priority.INTERACTIVE])

    def _desired_cpu(priority, pool) -> float:
        # Cap per-op CPU to prevent a single op from hogging the whole pool.
        max_cpu = float(pool.max_cpu_pool)
        if priority == Priority.QUERY:
            return max(1.0, min(6.0, max_cpu * 0.50))
        if priority == Priority.INTERACTIVE:
            return max(1.0, min(4.0, max_cpu * 0.40))
        return max(1.0, min(2.0, max_cpu * 0.25))

    def _desired_ram(priority, pool, op_id: int) -> float:
        # Start with a conservative baseline, then raise to known safe (estimated) RAM.
        max_ram = float(pool.max_ram_pool)
        if priority == Priority.QUERY:
            baseline = max(1.0, max_ram * 0.25)
        elif priority == Priority.INTERACTIVE:
            baseline = max(1.0, max_ram * 0.20)
        else:
            baseline = max(1.0, max_ram * 0.15)

        est = float(s.op_ram_est.get(op_id, 0.0))
        retries = int(s.op_oom_retries.get(op_id, 0))

        # If we have OOM history but no good estimate, still nudge upward.
        backoff = (1.5 ** retries) if retries > 0 else 1.0

        want = max(baseline, est) * backoff
        return max(1.0, min(want, max_ram))

    def _cleanup_pipeline(pipeline_id):
        s.enqueue_tick.pop(pipeline_id, None)
        s.pipeline_by_id.pop(pipeline_id, None)
        s.nonretriable_pipelines.discard(pipeline_id)
        # Note: we intentionally do not delete op_ram_est/op_to_pipeline entries;
        # keeping them can help later retries within the same run.

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        s.pipeline_by_id[pid] = p
        if pid not in s.enqueue_tick:
            s.enqueue_tick[pid] = s.tick
        s.queues[p.priority].append(p)

    def _requeue_pipeline(p, reset_enqueue: bool):
        pid = p.pipeline_id
        if reset_enqueue:
            s.enqueue_tick[pid] = s.tick
        s.queues[p.priority].append(p)

    def _pick_next_pipeline():
        # Choose among the head of each priority queue using base priority + aging bonus.
        best_pri = None
        best_score = None
        for pri, q in s.queues.items():
            if not q:
                continue
            candidate = q[0]
            pid = candidate.pipeline_id
            score = _base_weight(pri) + _age_bonus(pid)
            if best_score is None or score > best_score:
                best_score = score
                best_pri = pri

        if best_pri is None:
            return None

        return s.queues[best_pri].popleft()

    # Advance time (used only for aging; determinism still holds).
    s.tick += 1

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Process results (update OOM estimates / mark non-retriable failures)
    for r in results:
        failed = r.failed()
        oom = failed and _is_oom_error(getattr(r, "error", None))

        # Best-effort pipeline attribution
        res_pid = getattr(r, "pipeline_id", None)

        for op in getattr(r, "ops", []) or []:
            op_id = id(op)
            pid = res_pid if res_pid is not None else s.op_to_pipeline.get(op_id, None)

            if failed:
                if oom:
                    s.op_oom_retries[op_id] += 1

                    # Raise RAM estimate aggressively on OOM to reduce repeated tail latency.
                    used_ram = getattr(r, "ram", None)
                    if used_ram is not None:
                        prev = float(s.op_ram_est.get(op_id, 0.0))
                        # Next guess: at least 2x last allocation; never decrease.
                        s.op_ram_est[op_id] = max(prev, float(used_ram) * 2.0)
                    else:
                        # If we lack a measurement, still bump existing estimate.
                        if op_id in s.op_ram_est:
                            s.op_ram_est[op_id] = float(s.op_ram_est[op_id]) * 1.5

                    # Too many OOMs => give up on this pipeline (otherwise can loop forever).
                    if pid is not None and s.op_oom_retries[op_id] > s.max_oom_retries:
                        s.nonretriable_pipelines.add(pid)
                else:
                    # Non-OOM failure: treat as non-retriable.
                    if pid is not None:
                        s.nonretriable_pipelines.add(pid)
            else:
                # Success: optionally learn a safe RAM floor from what worked.
                used_ram = getattr(r, "ram", None)
                if used_ram is not None:
                    prev = float(s.op_ram_est.get(op_id, 0.0))
                    s.op_ram_est[op_id] = max(prev, float(used_ram))

    # If nothing changed, no need to recompute (resources typically change only on results/new arrivals).
    if not pipelines and not results:
        return [], []

    suspensions = []  # No safe preemption API surface assumed beyond Suspend(container_id, pool_id)
    assignments = []

    # Prefer scheduling on pools with more headroom first.
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(
        key=lambda i: (s.executor.pools[i].avail_cpu_pool, s.executor.pools[i].avail_ram_pool),
        reverse=True,
    )

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < 1.0 or avail_ram <= 0.0:
            continue

        # Greedily schedule multiple ops per pool per tick, but keep bounded work.
        attempts = 0
        placed = 0
        while (
            placed < s.max_assignments_per_tick_per_pool
            and avail_cpu >= 1.0
            and avail_ram > 0.0
        ):
            p = _pick_next_pipeline()
            if p is None:
                break

            pid = p.pipeline_id
            status = p.runtime_status()

            # Drop completed pipelines
            if status.is_pipeline_successful():
                _cleanup_pipeline(pid)
                continue

            # Drop pipelines with non-retriable failure
            if pid in s.nonretriable_pipelines:
                _cleanup_pipeline(pid)
                continue

            # Pick one ready operator (parents complete) to keep latency responsive.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready (e.g., waiting on running parents); keep it in its queue.
                _requeue_pipeline(p, reset_enqueue=False)
                attempts += 1
                if attempts > 20:
                    break
                continue

            op = op_list[0]
            op_id = id(op)

            # If high priority is waiting, avoid scheduling batch too aggressively.
            if p.priority == Priority.BATCH_PIPELINE and _has_high_waiting():
                reserved_cpu = max(1.0, float(pool.max_cpu_pool) * 0.20)
                reserved_ram = max(0.0, float(pool.max_ram_pool) * 0.20)
                # We compute candidate needs first, but we may skip if it would eat too much headroom.
            else:
                reserved_cpu = 0.0
                reserved_ram = 0.0

            want_cpu = _desired_cpu(p.priority, pool)
            want_ram = _desired_ram(p.priority, pool, op_id)

            # Honor learned minimum RAM estimate if present; if it can't fit, try another pipeline.
            min_est = float(s.op_ram_est.get(op_id, 0.0))
            if min_est > 0.0 and min_est > avail_ram:
                _requeue_pipeline(p, reset_enqueue=False)
                attempts += 1
                if attempts > 20:
                    break
                continue

            cpu = min(want_cpu, avail_cpu)
            cpu = max(1.0, cpu)

            ram = min(want_ram, avail_ram)
            if min_est > 0.0:
                ram = max(ram, min_est)

            # If we'd violate reservation for high-priority work, skip this batch placement.
            if p.priority == Priority.BATCH_PIPELINE and _has_high_waiting():
                if (avail_cpu - cpu) < reserved_cpu or (avail_ram - ram) < reserved_ram:
                    _requeue_pipeline(p, reset_enqueue=False)
                    attempts += 1
                    if attempts > 20:
                        break
                    continue

            if ram <= 0.0 or cpu <= 0.0:
                _requeue_pipeline(p, reset_enqueue=False)
                attempts += 1
                if attempts > 20:
                    break
                continue

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Remember attribution for future results.
            s.op_to_pipeline[op_id] = pid

            # Put pipeline back for later ops, but reset enqueue tick to encourage interleaving.
            _requeue_pipeline(p, reset_enqueue=True)

            avail_cpu -= cpu
            avail_ram -= ram
            placed += 1
            attempts = 0  # reset after a successful placement

    return suspensions, assignments
