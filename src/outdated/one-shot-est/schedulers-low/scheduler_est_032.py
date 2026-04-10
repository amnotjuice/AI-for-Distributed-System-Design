# policy_key: scheduler_est_032
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040858
# generation_seconds: 32.04
# generated_at: 2026-04-01T00:06:02.525849
@register_scheduler_init(key="scheduler_est_032")
def scheduler_est_032_init(s):
    """Priority-aware FIFO with simple resource reservation + OOM-aware RAM floors.

    Improvements over naive FIFO:
      1) Maintain separate FIFO queues per priority, always scheduling higher priority first.
      2) Use operator memory estimate as a RAM floor; keep per-operator RAM floors that increase on OOM.
      3) Reserve a small slice of each pool for high-priority work by limiting how much BATCH can consume.
      4) Avoid head-of-line blocking by scanning within a bounded window of each queue for runnable ops.
    """
    from collections import deque

    # Separate queues by priority (FIFO within each class)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-operator memory floor overrides (in GB), increased on suspected OOM failures
    # Keyed by id(op) to avoid relying on a specific operator-id field.
    s.op_mem_floor_gb = {}

    # Tunables (small, obvious improvements; keep conservative)
    s.scan_window = 32  # bounded scan to find a runnable pipeline without reordering entire queues
    s.batch_cpu_cap_frac = 0.50  # limit a single batch assignment to at most 50% of pool CPU
    s.batch_ram_cap_frac = 0.60  # limit a single batch assignment to at most 60% of pool RAM

    # Soft reservation for high priority; batches can't consume beyond this remaining headroom.
    s.reserve_cpu_for_hp_frac = 0.20  # reserve 20% CPU for QUERY/INTERACTIVE
    s.reserve_ram_for_hp_frac = 0.20  # reserve 20% RAM for QUERY/INTERACTIVE


@register_scheduler(key="scheduler_est_032")
def scheduler_est_032_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Enqueue new pipelines into per-priority queues.
      - Update RAM floors based on failures (grow on OOM-like errors).
      - For each pool, schedule ready operators in strict priority order:
          QUERY -> INTERACTIVE -> BATCH
        while maintaining small reservations so batch doesn't crowd out latency-sensitive work.
    """
    # Helper functions are nested to keep the policy self-contained.
    def _is_oom_error(err) -> bool:
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("out-of-memory" in e) or ("memoryerror" in e)

    def _queue_for_priority(pri):
        # Be defensive: compare by name if enum identity differs.
        name = getattr(pri, "name", str(pri))
        if name == "QUERY":
            return s.q_query
        if name == "INTERACTIVE":
            return s.q_interactive
        return s.q_batch

    def _priority_is_batch(pri) -> bool:
        name = getattr(pri, "name", str(pri))
        return name == "BATCH_PIPELINE"

    def _get_ready_op(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return None
        # If any operator failed, we still allow retry (ASSIGNABLE_STATES includes FAILED in provided context).
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _mem_floor_for_op(op):
        # Use estimator as a floor; override upward if we learned it OOMs.
        est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        est_floor = float(est) if est is not None else 0.0
        learned = s.op_mem_floor_gb.get(id(op), 0.0)
        floor = max(est_floor, learned, 0.0)
        return floor

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    # Enqueue new arrivals.
    for p in pipelines:
        _queue_for_priority(p.priority).append(p)

    # Update memory floors based on failures.
    for r in results:
        if hasattr(r, "failed") and r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                # Increase RAM floors for the ops that were in the failed container.
                # Use a multiplicative bump; also ensure it's at least the last attempted RAM.
                bump_base = float(getattr(r, "ram", 0.0) or 0.0)
                bumped = max(bump_base * 1.5, bump_base + 1.0)  # add a small additive cushion too
                for op in getattr(r, "ops", []) or []:
                    prev = s.op_mem_floor_gb.get(id(op), 0.0)
                    s.op_mem_floor_gb[id(op)] = max(prev, bumped)

    # If nothing new and no results, no decisions needed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Attempt to schedule in each pool independently.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Compute soft reservations: keep headroom for high-priority.
        max_cpu = float(pool.max_cpu_pool)
        max_ram = float(pool.max_ram_pool)
        reserve_cpu_for_hp = max_cpu * float(s.reserve_cpu_for_hp_frac)
        reserve_ram_for_hp = max_ram * float(s.reserve_ram_for_hp_frac)

        # Local function to pick a runnable pipeline from a queue with bounded scan.
        def _pop_next_runnable(q):
            scanned = 0
            # Rotate through up to scan_window entries to find something runnable.
            while q and scanned < s.scan_window:
                p = q.popleft()
                scanned += 1
                status = p.runtime_status()
                # Drop completed pipelines.
                if status.is_pipeline_successful():
                    continue
                op = _get_ready_op(p)
                if op is None:
                    # Not runnable now; keep it in FIFO order by appending back.
                    q.append(p)
                    continue
                return p, op
            return None, None

        # Schedule loop: keep assigning while we have resources.
        while avail_cpu > 0 and avail_ram > 0:
            chosen_p = None
            chosen_op = None

            # Strict priority order.
            for q in (s.q_query, s.q_interactive, s.q_batch):
                p, op = _pop_next_runnable(q)
                if p is None:
                    continue
                chosen_p, chosen_op = p, op
                break

            if chosen_p is None:
                break

            pri = chosen_p.priority
            is_batch = _priority_is_batch(pri)

            # Determine RAM allocation: at least memory floor; otherwise allocate "comfortably"
            # since extra RAM is free (only allocation below peak causes OOM).
            mem_floor = _mem_floor_for_op(chosen_op)
            # If we have no estimate, still allocate a small minimum to avoid trivially tiny containers.
            if mem_floor <= 0:
                mem_floor = 1.0

            # Apply batch caps so batch can't monopolize all RAM and block future interactive work.
            if is_batch:
                # Ensure we keep reserved RAM for high priority if possible.
                max_batch_ram = max(0.0, avail_ram - reserve_ram_for_hp)
                max_batch_ram = min(max_batch_ram, max_ram * float(s.batch_ram_cap_frac))
                target_ram = _clamp(mem_floor, 0.0, max_batch_ram)
            else:
                # For high priority, be generous: allocate as much as available, but not less than floor.
                target_ram = max(mem_floor, min(avail_ram, max_ram))

            # If we cannot satisfy the floor, put back and stop for this pool (avoid thrash).
            if target_ram < mem_floor or target_ram <= 0:
                _queue_for_priority(pri).appendleft(chosen_p)
                break

            # Determine CPU allocation:
            # - High priority: use as much as available to reduce latency.
            # - Batch: cap per assignment to allow more concurrency / preserve headroom.
            if is_batch:
                max_batch_cpu = max(0.0, avail_cpu - reserve_cpu_for_hp)
                max_batch_cpu = min(max_batch_cpu, max_cpu * float(s.batch_cpu_cap_frac))
                target_cpu = _clamp(max_batch_cpu, 0.0, avail_cpu)
            else:
                target_cpu = avail_cpu

            if target_cpu <= 0:
                _queue_for_priority(pri).appendleft(chosen_p)
                break

            # Create assignment for a single ready op from the pipeline.
            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=target_cpu,
                    ram=target_ram,
                    priority=pri,
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            # Update available resources in this pool (optimistic accounting for this tick).
            avail_cpu -= float(target_cpu)
            avail_ram -= float(target_ram)

            # Put the pipeline back for subsequent operators.
            _queue_for_priority(pri).append(chosen_p)

    return suspensions, assignments
