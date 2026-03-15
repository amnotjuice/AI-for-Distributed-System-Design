# policy_key: scheduler_high_006
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.135144
# generation_seconds: 164.96
# generated_at: 2026-03-12T22:37:12.994350
@register_scheduler_init(key="scheduler_high_006")
def scheduler_high_006_init(s):
    """Priority-aware, reservation-based scheduler (incremental improvement over naive FIFO).

    Main ideas (kept intentionally simple / "small improvements first"):
      1) Priority queues: QUERY > INTERACTIVE > BATCH.
      2) Headroom reservation: prevent BATCH from consuming all resources so new high-priority work
         can start immediately (improves p95/p99 latency without needing preemption visibility).
      3) Better packing than naive: allow multiple assignments per tick across pools while tracking
         remaining pool headroom in local scratch variables.
      4) OOM-aware RAM retry: if an operator fails with an OOM-like error, remember it and increase
         its RAM hint on subsequent retries (instead of dropping failed pipelines).

    Notes:
      - Does not use preemption (Suspend) because we typically don't have visibility into running
        containers beyond completion/failure results.
      - Schedules at most one operator per pipeline per tick to reduce bursty over-parallelization.
    """
    from collections import deque

    # Separate FIFO queues per priority for strict ordering.
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # "Clock" for simple aging / bookkeeping.
    s.tick = 0

    # Pipeline metadata: arrival tick, etc.
    s.pipeline_meta = {}  # pipeline_id -> {"arrival_tick": int}

    # Operator-level RAM hints learned from OOM failures.
    # Keyed by Python object identity (id(op)), which is stable inside this simulator.
    s.op_ram_hint = {}  # id(op) -> ram_target
    s.op_oom_retries = {}  # id(op) -> int

    # Map operator object id back to pipeline_id (so we can attribute failures).
    s.op_to_pipeline = {}  # id(op) -> pipeline_id

    # Pipelines with "non-OOM" failures (we stop retrying to avoid infinite loops).
    s.pipeline_permanent_fail = set()

    # Reservation fractions (protect latency by leaving some resources unused for sudden arrivals).
    # Applied only when placing BATCH work.
    s.reserve_cpu_frac_pool0 = 0.50
    s.reserve_ram_frac_pool0 = 0.50
    s.reserve_cpu_frac_other = 0.25
    s.reserve_ram_frac_other = 0.25

    # Simple aging: if a batch pipeline waits too long, treat it like INTERACTIVE for admission.
    s.aging_threshold_ticks = 50

    # Limits to avoid thrashing on repeated OOMs.
    s.max_oom_retries_per_op = 4


@register_scheduler(key="scheduler_high_006")
def scheduler_high_006_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler with headroom reservation and OOM-informed RAM sizing.

    Returns:
        (suspensions, assignments)
    """
    s.tick += 1

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _ensure_pipeline_meta(p):
        if p.pipeline_id not in s.pipeline_meta:
            s.pipeline_meta[p.pipeline_id] = {"arrival_tick": s.tick}

    def _is_aged_batch(p):
        if p.priority != Priority.BATCH_PIPELINE:
            return False
        meta = s.pipeline_meta.get(p.pipeline_id)
        if not meta:
            return False
        return (s.tick - meta["arrival_tick"]) >= s.aging_threshold_ticks

    def _default_ram_frac(pri):
        # Conservative for latency (more RAM reduces OOM risk); batch gets less by default.
        if pri == Priority.QUERY:
            return 0.50
        if pri == Priority.INTERACTIVE:
            return 0.40
        return 0.25

    def _default_cpu_frac(pri):
        # Give more CPU to high-priority work to reduce response time.
        if pri == Priority.QUERY:
            return 0.75
        if pri == Priority.INTERACTIVE:
            return 0.50
        return 0.25

    def _reserve_fracs(pool_id):
        if pool_id == 0:
            return s.reserve_cpu_frac_pool0, s.reserve_ram_frac_pool0
        return s.reserve_cpu_frac_other, s.reserve_ram_frac_other

    def _pick_runnable_pipeline(q, scheduled_pipeline_ids, only_aged_batch=False):
        """Pop/rotate within a queue to find a runnable pipeline with a ready operator."""
        n = len(q)
        for _ in range(n):
            p = q.popleft()
            _ensure_pipeline_meta(p)

            # Avoid scheduling multiple ops from same pipeline in a single tick.
            if p.pipeline_id in scheduled_pipeline_ids:
                q.append(p)
                continue

            # Skip permanently failed pipelines.
            if p.pipeline_id in s.pipeline_permanent_fail:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            if only_aged_batch and (not _is_aged_batch(p)):
                q.append(p)
                continue

            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable now; keep it in the queue.
                q.append(p)
                continue

            # Found a runnable pipeline and an op to schedule.
            q.append(p)
            return p, op_list

        return None, None

    def _pool_order(avail_cpu, avail_ram):
        # Prefer pools with more headroom.
        return sorted(
            range(s.executor.num_pools),
            key=lambda i: (avail_cpu[i] + 1e-9) * (avail_ram[i] + 1e-9),
            reverse=True,
        )

    def _compute_allocations(pool_id, pri, op_obj, avail_cpu, avail_ram, max_cpu, max_ram, enforce_batch_reserve):
        """Decide cpu/ram for the container that will run op_obj in pool_id."""
        op_id = id(op_obj)

        # Base targets from pool size and priority.
        base_cpu = max_cpu * _default_cpu_frac(pri)
        base_ram = max_ram * _default_ram_frac(pri)

        # Apply learned RAM hint (only increases due to OOM).
        hinted_ram = s.op_ram_hint.get(op_id)
        if hinted_ram is not None:
            base_ram = max(base_ram, hinted_ram)

        # Clip to what's available (and reasonable).
        # Keep at least a tiny positive allocation to avoid zeros.
        cpu = max(0.001, min(avail_cpu, base_cpu))
        ram = max(0.001, min(avail_ram, base_ram))

        if enforce_batch_reserve:
            reserve_cpu_frac, reserve_ram_frac = _reserve_fracs(pool_id)
            reserve_cpu = max_cpu * reserve_cpu_frac
            reserve_ram = max_ram * reserve_ram_frac

            # Ensure we do not eat into reserved headroom.
            usable_cpu = max(0.0, avail_cpu - reserve_cpu)
            usable_ram = max(0.0, avail_ram - reserve_ram)

            # If there's no usable headroom, do not schedule batch here.
            if usable_cpu <= 0.0 or usable_ram <= 0.0:
                return None, None

            cpu = max(0.001, min(cpu, usable_cpu))
            ram = max(0.001, min(ram, usable_ram))

            # Still must fit.
            if cpu <= 0.0 or ram <= 0.0:
                return None, None

        # Avoid extremely tiny batch allocations that would increase latency without improving throughput.
        # (Keep this mild to avoid accidental "can't schedule" states.)
        if pri == Priority.BATCH_PIPELINE:
            cpu = max(cpu, min(avail_cpu, max_cpu * 0.10))
            ram = max(ram, min(avail_ram, max_ram * 0.10))

        return cpu, ram

    # Enqueue new pipelines into their priority queues.
    for p in pipelines:
        _ensure_pipeline_meta(p)
        _queue_for_priority(p.priority).append(p)

    # Learn from results (mainly: OOM -> bump RAM hint; non-OOM -> mark pipeline as permanent fail).
    for r in results:
        if not getattr(r, "ops", None):
            continue

        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = bool(getattr(r, "error", None))

        if not failed:
            continue

        is_oom = _is_oom_error(getattr(r, "error", None))

        # Attribute the failure to the pipeline via the operator identity mapping.
        # If multiple ops are present, update all of them (safe, conservative).
        for op in r.ops:
            op_id = id(op)
            pid = s.op_to_pipeline.get(op_id)

            if is_oom:
                retries = s.op_oom_retries.get(op_id, 0) + 1
                s.op_oom_retries[op_id] = retries

                # If we keep OOMing, eventually stop retrying this pipeline to avoid infinite loops.
                if pid is not None and retries > s.max_oom_retries_per_op:
                    s.pipeline_permanent_fail.add(pid)
                    continue

                # Increase RAM hint aggressively for the next attempt.
                # Prefer doubling last attempted RAM; cap at pool max RAM for the pool where it failed.
                pool_id = getattr(r, "pool_id", None)
                if pool_id is None:
                    # Fallback: just multiply the existing hint if present.
                    prev = s.op_ram_hint.get(op_id, max(0.001, getattr(r, "ram", 0.001)))
                    s.op_ram_hint[op_id] = prev * 2.0
                else:
                    max_ram = s.executor.pools[pool_id].max_ram_pool
                    attempted = max(0.001, getattr(r, "ram", 0.001))
                    new_hint = min(max_ram, attempted * 2.0)
                    prev_hint = s.op_ram_hint.get(op_id, 0.0)
                    s.op_ram_hint[op_id] = max(prev_hint, new_hint)
            else:
                # Non-OOM failures are treated as permanent (do not spin on FAILED ops).
                if pid is not None:
                    s.pipeline_permanent_fail.add(pid)

    # If no new info, we can still try to schedule (priority headroom matters even without arrivals).
    suspensions = []
    assignments = []

    # Snapshot available resources (we will deduct locally as we place multiple assignments).
    num_pools = s.executor.num_pools
    avail_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(num_pools)]
    avail_ram = [s.executor.pools[i].avail_ram_pool for i in range(num_pools)]
    max_cpu = [s.executor.pools[i].max_cpu_pool for i in range(num_pools)]
    max_ram = [s.executor.pools[i].max_ram_pool for i in range(num_pools)]

    scheduled_pipeline_ids = set()

    def _try_schedule_from_queue(pri, q, allow_aged_batch=False, only_aged_batch=False):
        """Greedily place runnable ops from a queue onto pools with most headroom."""
        nonlocal assignments, avail_cpu, avail_ram

        # Keep attempting while there is runnable work and some pool has headroom.
        for _ in range(len(q) + 32):  # small bound to avoid pathological loops
            if all((avail_cpu[i] <= 0.0 or avail_ram[i] <= 0.0) for i in range(num_pools)):
                return

            p, op_list = _pick_runnable_pipeline(
                q,
                scheduled_pipeline_ids,
                only_aged_batch=only_aged_batch
            )
            if p is None or not op_list:
                return

            op = op_list[0]
            op_id = id(op)
            s.op_to_pipeline[op_id] = p.pipeline_id

            # Choose a pool (prefer high headroom). For BATCH, enforce reservation.
            pool_candidates = _pool_order(avail_cpu, avail_ram)
            placed = False
            for pool_id in pool_candidates:
                if avail_cpu[pool_id] <= 0.0 or avail_ram[pool_id] <= 0.0:
                    continue

                enforce_reserve = (pri == Priority.BATCH_PIPELINE)
                cpu, ram = _compute_allocations(
                    pool_id=pool_id,
                    pri=pri,
                    op_obj=op,
                    avail_cpu=avail_cpu[pool_id],
                    avail_ram=avail_ram[pool_id],
                    max_cpu=max_cpu[pool_id],
                    max_ram=max_ram[pool_id],
                    enforce_batch_reserve=enforce_reserve
                )
                if cpu is None or ram is None:
                    continue

                # Place it.
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id
                    )
                )
                avail_cpu[pool_id] -= cpu
                avail_ram[pool_id] -= ram
                scheduled_pipeline_ids.add(p.pipeline_id)
                placed = True
                break

            if not placed:
                # Couldn't place this runnable op anywhere right now; stop for this queue.
                return

    # 1) QUERY first (lowest latency target).
    _try_schedule_from_queue(Priority.QUERY, s.q_query)

    # 2) INTERACTIVE next.
    _try_schedule_from_queue(Priority.INTERACTIVE, s.q_interactive)

    # 2b) Aged BATCH: if a batch job has waited long, allow it to compete earlier to reduce starvation.
    # We still *admit it in this phase*, but keep BATCH reservation rules when it runs in the BATCH phase.
    # Here, we let a few aged batch pipelines start earlier (helps tail latency fairness).
    _try_schedule_from_queue(Priority.INTERACTIVE, s.q_batch, only_aged_batch=True)

    # 3) BATCH last, with reservation to protect future QUERY/INTERACTIVE arrivals.
    _try_schedule_from_queue(Priority.BATCH_PIPELINE, s.q_batch)

    return suspensions, assignments
