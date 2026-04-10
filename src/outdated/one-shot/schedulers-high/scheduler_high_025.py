# policy_key: scheduler_high_025
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.140195
# generation_seconds: 166.09
# generated_at: 2026-03-14T03:37:00.916635
@register_scheduler_init(key="scheduler_high_025")
def scheduler_high_025_init(s):
    """Priority-aware, latency-oriented scheduler.

    Improvements over naive FIFO:
      1) Separate per-priority queues (QUERY > INTERACTIVE > BATCH) to reduce head-of-line blocking.
      2) Allow multiple concurrent assignments per pool by capping per-container CPU/RAM (better tail latency).
      3) Simple OOM-aware RAM backoff: on OOM, remember a higher minimum RAM for the pipeline and avoid
         re-running it with too little RAM.
      4) Soft reservation: when high-priority work is waiting, batch is limited to leftover headroom.
      5) Simple fairness within a priority class via round-robin deques, plus basic batch aging.

    Notes:
      - No preemption (keeps churn low); relies on admission ordering + soft reservations.
      - Schedules one operator per assignment for predictability.
    """
    from collections import deque

    s.tick = 0

    # Round-robin queues by priority (store pipeline_id to avoid duplicate objects).
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Pipeline bookkeeping.
    s.pipeline_by_id = {}
    s.enqueued = set()
    s.enqueue_tick = {}

    # Failure / retry controls.
    s.pipeline_failures = {}  # pipeline_id -> count (any failure)
    s.max_failures = 6

    # OOM learning: pipeline_id -> minimum RAM to request next time (hard floor).
    s.pipeline_min_ram = {}   # learned from OOMs
    s.pipeline_last_ram = {}  # last attempted RAM
    s.pipeline_last_cpu = {}  # last attempted CPU

    # Map operator object identity -> pipeline_id so we can attribute results.
    s.op_to_pipeline_id = {}

    # Per-priority sizing knobs (caps) to increase concurrency and reduce HOL blocking.
    # These are "soft" targets; actual assigned sizes are clamped to available resources.
    s.cpu_cap = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 8.0,
    }
    s.ram_frac = {
        Priority.QUERY: 0.20,
        Priority.INTERACTIVE: 0.30,
        Priority.BATCH_PIPELINE: 0.45,
    }

    # If any high-priority is waiting, keep some headroom so batch doesn't fill the pool.
    s.reserve_cpu_frac_for_hp = 0.25
    s.reserve_ram_frac_for_hp = 0.25

    # Batch aging: if a batch pipeline has waited long enough, allow it even under contention.
    s.batch_aging_threshold_ticks = 250

    # Per-tick scheduling limits (avoid pathological loops).
    s.max_assignments_per_pool = 8


@register_scheduler(key="scheduler_high_025")
def scheduler_high_025_scheduler(s, results: List[ExecutionResult],
                                pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    def _is_oom_error(err) -> bool:
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)

    def _get_pid_from_result(res: ExecutionResult):
        # Prefer operator identity mapping (robust across absence of pipeline_id on result).
        try:
            if getattr(res, "ops", None):
                op0 = res.ops[0]
                pid = s.op_to_pipeline_id.get(id(op0))
                if pid is not None:
                    return pid
                # Fallback if operator exposes pipeline_id
                pid2 = getattr(op0, "pipeline_id", None)
                if pid2 is not None:
                    return pid2
        except Exception:
            pass
        return None

    def _cleanup_pipeline(pid: int):
        # Remove from bookkeeping; stale entries in deques are ignored when popped.
        s.enqueued.discard(pid)
        s.pipeline_by_id.pop(pid, None)
        s.enqueue_tick.pop(pid, None)
        s.pipeline_failures.pop(pid, None)
        s.pipeline_min_ram.pop(pid, None)
        s.pipeline_last_ram.pop(pid, None)
        s.pipeline_last_cpu.pop(pid, None)

    def _base_ram_for(pool, prio) -> float:
        # Use a fraction of pool capacity, with a small floor to avoid trivially-small containers.
        max_ram = getattr(pool, "max_ram_pool", 0) or 0
        frac = s.ram_frac.get(prio, 0.30)
        floor = max_ram * 0.05
        return max(floor, max_ram * frac)

    def _cpu_cap_for(pool, prio) -> float:
        max_cpu = getattr(pool, "max_cpu_pool", 0) or 0
        cap = s.cpu_cap.get(prio, 2.0)
        # Never exceed pool max; also avoid 0.
        return max(0.0, min(cap, max_cpu if max_cpu > 0 else cap))

    def _batch_is_aged(pid: int) -> bool:
        t0 = s.enqueue_tick.get(pid, s.tick)
        return (s.tick - t0) >= s.batch_aging_threshold_ticks

    def _pop_rr_schedulable(prio, pool, avail_cpu, avail_ram, scheduled_this_tick: set):
        """Pop (pipeline, op, cpu_req, ram_req) using RR; rotate at most queue length."""
        q = s.queues[prio]
        if not q:
            return None

        # Cap scanning to current queue length to preserve RR behavior.
        scan = len(q)
        for _ in range(scan):
            pid = q.popleft()

            # Skip if no longer tracked.
            pipeline = s.pipeline_by_id.get(pid)
            if pipeline is None or pid not in s.enqueued:
                continue

            # Avoid scheduling multiple ops from the same pipeline in one scheduler call.
            if pid in scheduled_this_tick:
                q.append(pid)
                continue

            status = pipeline.runtime_status()

            # Drop completed pipelines.
            if status.is_pipeline_successful():
                _cleanup_pipeline(pid)
                continue

            # Drop pipelines that have repeatedly failed (prevents infinite retry loops on non-OOM errors).
            if s.pipeline_failures.get(pid, 0) >= s.max_failures:
                _cleanup_pipeline(pid)
                continue

            # Find a single ready op (parents complete).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready; keep in RR.
                q.append(pid)
                continue

            # Compute requested resources.
            cpu_req = _cpu_cap_for(pool, prio)
            if cpu_req <= 0:
                cpu_req = 1.0  # conservative fallback

            base_ram = _base_ram_for(pool, prio)
            min_ram = s.pipeline_min_ram.get(pid, 0.0) or 0.0
            ram_req = max(base_ram, min_ram)

            # Clamp to pool max.
            max_ram = getattr(pool, "max_ram_pool", None)
            if max_ram is not None and max_ram > 0:
                ram_req = min(ram_req, max_ram)

            # Fit logic:
            # - CPU can be reduced to whatever is available (still might help latency).
            # - RAM: if pipeline has a learned min_ram (from OOM), don't run it below that again.
            #        Otherwise, allow "best effort" by using up to available RAM.
            learned = (pid in s.pipeline_min_ram) and (s.pipeline_min_ram[pid] or 0.0) > 0.0
            if learned and avail_ram < ram_req:
                # Can't satisfy known minimum; keep it queued.
                q.append(pid)
                continue

            # If not learned, best-effort RAM (may still OOM, but we learn quickly).
            ram_assign = min(avail_ram, ram_req) if avail_ram > 0 else 0.0
            if ram_assign <= 0:
                q.append(pid)
                continue

            cpu_assign = min(avail_cpu, cpu_req) if avail_cpu > 0 else 0.0
            if cpu_assign <= 0:
                q.append(pid)
                continue

            # Re-append pipeline for RR fairness (it likely has more ops later).
            q.append(pid)
            return pipeline, op_list, cpu_assign, ram_assign

        return None

    # Add new pipelines (deduplicated).
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.enqueued:
            continue
        s.enqueued.add(pid)
        s.pipeline_by_id[pid] = p
        s.enqueue_tick[pid] = s.tick
        s.pipeline_failures.setdefault(pid, 0)
        s.queues[p.priority].append(pid)

    # Early exit if truly nothing changed.
    if not pipelines and not results:
        return [], []

    # Advance a coarse "tick" for aging logic (based on scheduler invocations).
    s.tick += 1

    # Incorporate results: learn from OOMs, count failures.
    for r in results:
        pid = _get_pid_from_result(r)
        if pid is None:
            continue

        # Track last tried resources (useful for OOM backoff).
        try:
            s.pipeline_last_ram[pid] = float(getattr(r, "ram", 0.0) or 0.0)
            s.pipeline_last_cpu[pid] = float(getattr(r, "cpu", 0.0) or 0.0)
        except Exception:
            pass

        if r.failed():
            s.pipeline_failures[pid] = s.pipeline_failures.get(pid, 0) + 1

            if _is_oom_error(getattr(r, "error", None)):
                # Exponential RAM backoff with a floor of 2x last tried RAM.
                pool_id = getattr(r, "pool_id", None)
                pool = s.executor.pools[pool_id] if pool_id is not None else None
                pool_max_ram = getattr(pool, "max_ram_pool", None) if pool is not None else None

                last_ram = s.pipeline_last_ram.get(pid, 0.0) or 0.0
                learned_min = s.pipeline_min_ram.get(pid, 0.0) or 0.0
                new_min = max(learned_min, last_ram * 2.0 if last_ram > 0 else learned_min)

                # If we have pool context, ensure we at least bump to a reasonable fraction of pool RAM.
                if pool is not None and (getattr(pool, "max_ram_pool", 0) or 0) > 0:
                    new_min = max(new_min, 0.35 * pool.max_ram_pool)

                if pool_max_ram is not None and pool_max_ram > 0:
                    new_min = min(new_min, pool_max_ram)

                if new_min > 0:
                    s.pipeline_min_ram[pid] = new_min

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Compute whether high-priority work is waiting (for soft reservations).
    hp_waiting = (len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE])) > 0

    # Prefer scheduling on pools with most headroom first.
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(
        key=lambda i: (
            s.executor.pools[i].avail_cpu_pool,
            s.executor.pools[i].avail_ram_pool,
        ),
        reverse=True,
    )

    scheduled_this_tick = set()

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft reservation thresholds (only constrain batch).
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if hp_waiting:
            reserve_cpu = (getattr(pool, "max_cpu_pool", 0) or 0) * s.reserve_cpu_frac_for_hp
            reserve_ram = (getattr(pool, "max_ram_pool", 0) or 0) * s.reserve_ram_frac_for_hp

        made_progress = True
        iters = 0

        while made_progress and iters < s.max_assignments_per_pool:
            iters += 1
            made_progress = False

            # 1) Always try high priorities first.
            for prio in (Priority.QUERY, Priority.INTERACTIVE):
                if avail_cpu <= 0 or avail_ram <= 0:
                    break
                picked = _pop_rr_schedulable(prio, pool, avail_cpu, avail_ram, scheduled_this_tick)
                if not picked:
                    continue

                pipeline, op_list, cpu_assign, ram_assign = picked

                assignment = Assignment(
                    ops=op_list,
                    cpu=cpu_assign,
                    ram=ram_assign,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
                assignments.append(assignment)

                # Remember mapping from op identity to pipeline_id for later result attribution.
                for op in op_list:
                    s.op_to_pipeline_id[id(op)] = pipeline.pipeline_id

                scheduled_this_tick.add(pipeline.pipeline_id)

                avail_cpu -= cpu_assign
                avail_ram -= ram_assign
                made_progress = True
                break  # re-evaluate priorities with updated headroom

            if made_progress:
                continue

            # 2) Then try batch, but only if we have headroom beyond reserves OR it's aged.
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Determine if we can consider batch right now.
            allow_batch = True
            if hp_waiting:
                # Under contention, only use leftover headroom for non-aged batch.
                allow_batch = (avail_cpu > reserve_cpu) and (avail_ram > reserve_ram)

            if not allow_batch and len(s.queues[Priority.BATCH_PIPELINE]) > 0:
                # Try to find an aged batch pipeline; if found, allow it even under contention.
                # We do a bounded scan by rotating once; if any pid is aged we schedule it.
                q = s.queues[Priority.BATCH_PIPELINE]
                scan = len(q)
                aged_found = False
                for _ in range(scan):
                    pid = q[0]
                    if pid in s.enqueued and _batch_is_aged(pid):
                        aged_found = True
                        break
                    q.rotate(-1)
                allow_batch = aged_found

            if not allow_batch:
                break

            picked = _pop_rr_schedulable(Priority.BATCH_PIPELINE, pool, avail_cpu, avail_ram, scheduled_this_tick)
            if not picked:
                break

            pipeline, op_list, cpu_assign, ram_assign = picked

            # If we are under hp contention, keep the batch allocation modest (avoid latency spikes).
            if hp_waiting:
                cpu_assign = min(cpu_assign, max(1.0, reserve_cpu))
                ram_assign = min(ram_assign, max(0.0, reserve_ram)) if reserve_ram > 0 else ram_assign
                cpu_assign = min(cpu_assign, avail_cpu)
                ram_assign = min(ram_assign, avail_ram)

            if cpu_assign <= 0 or ram_assign <= 0:
                break

            assignment = Assignment(
                ops=op_list,
                cpu=cpu_assign,
                ram=ram_assign,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            for op in op_list:
                s.op_to_pipeline_id[id(op)] = pipeline.pipeline_id

            scheduled_this_tick.add(pipeline.pipeline_id)

            avail_cpu -= cpu_assign
            avail_ram -= ram_assign
            made_progress = True

    return suspensions, assignments
