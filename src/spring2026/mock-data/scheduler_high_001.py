# policy_key: scheduler_high_001
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.106052
# generation_seconds: 128.47
# generated_at: 2026-03-12T22:23:42.723447
@register_scheduler_init(key="scheduler_high_001")
def scheduler_high_001_init(s):
    """Priority-aware, multi-assignment-per-pool scheduler with simple RAM OOM backoff.

    Small, incremental improvements over naive FIFO:
      1) Maintain separate FIFO queues per priority; always prefer higher priority work.
      2) Pack multiple containers per pool per tick (instead of 1 op consuming the whole pool),
         using small per-container CPU/RAM quanta to reduce head-of-line blocking.
      3) Keep a small "interactive headroom" reserve in pool 0 (when multiple pools exist)
         so batch work is less likely to block high-priority arrivals.
      4) On OOM-like failures, retry the same operator with increased RAM (exponential backoff),
         while treating non-OOM failures as fatal (do not retry).
    """
    from collections import deque

    # Per-priority waiting queues (FIFO with round-robin via rotation).
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # De-dup and lifecycle tracking.
    s.known_pipeline_ids = set()
    s.fatal_pipeline_ids = set()

    # Per-operator RAM backoff on OOM-like failures.
    # Keyed by a stable-ish operator key (best-effort).
    s.op_ram_mult = {}        # op_key -> multiplier (float)
    s.op_oom_retries = {}     # op_key -> retries (int)
    s.max_oom_retries = 3

    # Soft limits to avoid generating huge assignment lists in a single tick.
    s.max_assignments_per_pool = 8

    # A logical clock for any future aging/tiebreaking (kept minimal here).
    s.tick = 0


def _is_oom_error(err) -> bool:
    """Heuristic: detect OOM-like errors from error objects/strings."""
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)


def _op_key(pipeline_id, op):
    """Best-effort stable key for an operator for backoff bookkeeping."""
    # Try common identifiers first; fall back to object id.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                # Avoid huge/unhashable values.
                return (pipeline_id, attr, str(v))
            except Exception:
                pass
    return (pipeline_id, "pyid", id(op))


def _drop_pipeline_everywhere(s, pipeline):
    """Remove a pipeline from all queues and known set (best-effort)."""
    pid = pipeline.pipeline_id
    s.known_pipeline_ids.discard(pid)
    # Deques don't support efficient delete; rebuild small-ish queues.
    for prio, q in s.queues.items():
        if not q:
            continue
        s.queues[prio] = type(q)([p for p in q if p.pipeline_id != pid])


def _compact_and_pick_op(s, prio_list, require_high_only, pool_id):
    """Pick the next runnable (pipeline, op) from the given priorities.

    Uses round-robin within each priority queue by rotating the deque.

    Args:
        prio_list: priorities to consider in order.
        require_high_only: if True, ignore batch even if provided.
        pool_id: for potential pool-specific heuristics.

    Returns:
        (pipeline, op) or (None, None)
    """
    for prio in prio_list:
        if require_high_only and prio == Priority.BATCH_PIPELINE:
            continue

        q = s.queues.get(prio)
        if not q:
            continue

        # Scan each pipeline at most once to avoid infinite rotation.
        for _ in range(len(q)):
            pipeline = q.popleft()
            pid = pipeline.pipeline_id

            # Skip/destroy pipelines that are known-fatal.
            if pid in s.fatal_pipeline_ids:
                _drop_pipeline_everywhere(s, pipeline)
                continue

            status = pipeline.runtime_status()

            # Drop completed pipelines.
            if status.is_pipeline_successful():
                _drop_pipeline_everywhere(s, pipeline)
                continue

            # Find next runnable operator (parents complete).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if op_list:
                # Put the pipeline back at the tail for round-robin fairness.
                q.append(pipeline)
                return pipeline, op_list[0]

            # Not runnable now; rotate to tail.
            q.append(pipeline)

    return None, None


def _any_waiting_high(s) -> bool:
    """Whether there is any high-priority work waiting (query/interactive)."""
    return bool(s.queues[Priority.QUERY]) or bool(s.queues[Priority.INTERACTIVE])


def _target_quanta(pool, priority):
    """Small per-container quanta to reduce head-of-line blocking.

    Returns (cpu_q, ram_q) before any OOM backoff multiplier.
    """
    # Default baselines: keep them modest so we can pack multiple tasks.
    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    # Absolute minimums (avoid 0-sized containers).
    min_cpu = 1.0
    min_ram = max(1.0, 0.05 * max_ram)

    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        # Give high priority more CPU to reduce their latency.
        cpu_q = min(4.0, max_cpu)
        ram_q = max(min_ram, 0.20 * max_ram)
    else:
        # Batch gets smaller slices to preserve headroom and reduce blocking.
        cpu_q = min(2.0, max_cpu)
        ram_q = max(min_ram, 0.15 * max_ram)

    cpu_q = max(min_cpu, cpu_q)
    ram_q = max(min_ram, ram_q)
    return cpu_q, ram_q


@register_scheduler(key="scheduler_high_001")
def scheduler_high_001_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler with:
      - separate per-priority queues,
      - packing multiple assignments per pool per tick,
      - pool-0 headroom reservation (when multiple pools exist),
      - OOM-driven RAM backoff retries.
    """
    s.tick += 1

    # 1) Ingest new pipelines into per-priority queues (de-dup).
    for p in pipelines:
        if p.pipeline_id in s.known_pipeline_ids:
            continue
        if p.pipeline_id in s.fatal_pipeline_ids:
            continue
        s.known_pipeline_ids.add(p.pipeline_id)
        s.queues[p.priority].append(p)

    # Early exit if nothing changed (keeps sim fast).
    if not pipelines and not results:
        return [], []

    # 2) Learn from execution results: OOM -> increase RAM for that operator and retry;
    #    non-OOM failures -> mark pipeline fatal (do not retry).
    for r in results:
        if not r.failed():
            continue

        # If any op in this container OOMed, backoff their RAM allocation.
        if _is_oom_error(r.error):
            for op in (r.ops or []):
                ok = _op_key(r.pipeline_id, op) if hasattr(r, "pipeline_id") else _op_key("unknown", op)
                # Track retries and cap them; after cap, mark pipeline fatal.
                cnt = s.op_oom_retries.get(ok, 0) + 1
                s.op_oom_retries[ok] = cnt
                if cnt > s.max_oom_retries and hasattr(r, "pipeline_id"):
                    s.fatal_pipeline_ids.add(r.pipeline_id)
                else:
                    s.op_ram_mult[ok] = min(16.0, s.op_ram_mult.get(ok, 1.0) * 2.0)
        else:
            # Treat other failures as fatal to avoid infinite retries.
            if hasattr(r, "pipeline_id"):
                s.fatal_pipeline_ids.add(r.pipeline_id)

    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools
    multiple_pools = num_pools > 1

    # 3) Fill each pool with multiple assignments, prioritizing QUERY > INTERACTIVE > BATCH.
    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Reserve headroom in pool 0 to protect interactive latency when multiple pools exist.
        # This is a simple, obvious improvement to avoid batch consuming all resources.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if multiple_pools and pool_id == 0:
            reserve_cpu = 0.20 * float(pool.max_cpu_pool)
            reserve_ram = 0.20 * float(pool.max_ram_pool)

        # If pool 0, strongly prefer high-priority work. Allow batch only if no high is waiting.
        high_waiting = _any_waiting_high(s)

        per_pool_assignments = 0
        while (
            per_pool_assignments < s.max_assignments_per_pool
            and avail_cpu > 0
            and avail_ram > 0
        ):
            # Priority order (small improvement from naive FIFO).
            prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

            # In pool 0 with multiple pools, avoid running batch if any high-priority is queued.
            require_high_only = bool(multiple_pools and pool_id == 0 and high_waiting)

            pipeline, op = _compact_and_pick_op(
                s,
                prio_list=prio_order,
                require_high_only=require_high_only,
                pool_id=pool_id,
            )
            if pipeline is None or op is None:
                break

            # Compute target per-container resources (quanta) + OOM backoff.
            cpu_q, ram_q = _target_quanta(pool, pipeline.priority)

            # Apply operator-specific RAM multiplier (for OOM retries).
            ok = _op_key(pipeline.pipeline_id, op)
            ram_mult = float(s.op_ram_mult.get(ok, 1.0))
            ram_q = ram_q * ram_mult

            # Fit to available resources.
            cpu = min(avail_cpu, cpu_q)
            ram = min(avail_ram, ram_q)

            # Enforce pool-0 headroom when scheduling batch (only relevant if we ever schedule it there).
            if multiple_pools and pool_id == 0 and pipeline.priority == Priority.BATCH_PIPELINE:
                max_cpu_for_batch = max(0.0, avail_cpu - reserve_cpu)
                max_ram_for_batch = max(0.0, avail_ram - reserve_ram)
                cpu = min(cpu, max_cpu_for_batch)
                ram = min(ram, max_ram_for_batch)

            # If we can't allocate anything meaningful, stop filling this pool.
            if cpu <= 0 or ram <= 0:
                break

            # Create assignment for a single operator (safe: matches baseline semantics).
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

            avail_cpu -= cpu
            avail_ram -= ram
            per_pool_assignments += 1

            # Update high_waiting after consuming some high-priority work.
            high_waiting = _any_waiting_high(s)

    return suspensions, assignments
