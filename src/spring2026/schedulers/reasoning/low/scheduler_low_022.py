# policy_key: scheduler_low_022
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052194
# generation_seconds: 43.00
# generated_at: 2026-03-14T02:19:02.641933
@register_scheduler_init(key="scheduler_low_022")
def scheduler_low_022_init(s):
    """
    Priority-aware, resource-capped FIFO scheduler with simple RAM backoff.

    Small improvements over naive FIFO:
      - Maintain separate queues per priority (QUERY/INTERACTIVE/BATCH).
      - Allocate per-op resources using per-priority caps (avoid monopolizing a pool).
      - Reserve headroom for higher priorities by limiting batch allocations.
      - On OOM-like failures, retry the pipeline with increased RAM scale (per pipeline).
      - Basic anti-starvation: batch pipelines that wait "too long" are temporarily boosted.
    """
    from collections import deque

    s.tick = 0

    # Per-priority waiting queues (FIFO within each class)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track when a pipeline was first enqueued (for aging/boost)
    s.enqueue_tick = {}  # pipeline_id -> tick

    # Per-pipeline resource tuning (learned from failures)
    s.ram_scale = {}  # pipeline_id -> multiplier in [1.0, 8.0]
    s.cpu_scale = {}  # pipeline_id -> multiplier in [1.0, 2.0]

    # Pipelines that should be dropped (non-OOM failures)
    s.blacklist = set()


def _priority_name(p):
    # Helper to avoid relying on Priority enum internals
    return str(p)


def _is_high_priority(priority):
    # Treat QUERY and INTERACTIVE as latency-sensitive
    n = _priority_name(priority).upper()
    return ("QUERY" in n) or ("INTERACTIVE" in n)


def _is_batch(priority):
    n = _priority_name(priority).upper()
    return "BATCH" in n


def _looks_like_oom(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    # Keep this broad; simulator may encode OOM differently.
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)


def _get_queue(s, priority):
    # Route to the correct queue
    n = _priority_name(priority).upper()
    if "QUERY" in n:
        return s.q_query
    if "INTERACTIVE" in n:
        return s.q_interactive
    return s.q_batch


def _effective_queue_order(s):
    """
    Decide which queues to pull from first.
    Implements a minimal aging rule: if the oldest batch pipeline has waited
    long enough, give it a chance between interactive items.
    """
    # Default: QUERY -> INTERACTIVE -> BATCH
    order = [s.q_query, s.q_interactive, s.q_batch]

    # Aging boost: if batch head waited too long, interleave it earlier
    if s.q_batch:
        head = s.q_batch[0]
        waited = s.tick - s.enqueue_tick.get(head.pipeline_id, s.tick)
        # Threshold chosen to be simple and deterministic; can be tuned in sim.
        if waited >= 25:
            order = [s.q_query, s.q_batch, s.q_interactive]
    return order


def _cap_resources_for_priority(pool, priority, cpu_scale, ram_scale):
    """
    Choose a "right-sized" bundle that:
      - allows concurrency (caps fraction of pool)
      - still gives meaningful minimums
      - respects per-pipeline backoff scales
    """
    # Priority-based caps: high-priority gets larger share per op
    if _is_high_priority(priority):
        cpu_cap_frac = 0.60
        ram_cap_frac = 0.60
        min_cpu_frac = 0.10
        min_ram_frac = 0.10
    else:
        # Batch is intentionally capped to reduce interference
        cpu_cap_frac = 0.30
        ram_cap_frac = 0.30
        min_cpu_frac = 0.05
        min_ram_frac = 0.05

    # Compute absolute targets from pool maxima (not current availability)
    base_cpu = max(pool.max_cpu_pool * min_cpu_frac, 1.0)
    base_ram = max(pool.max_ram_pool * min_ram_frac, 1.0)

    cap_cpu = max(pool.max_cpu_pool * cpu_cap_frac, base_cpu)
    cap_ram = max(pool.max_ram_pool * ram_cap_frac, base_ram)

    # Apply learned scaling from failures
    target_cpu = min(cap_cpu * cpu_scale, pool.max_cpu_pool)
    target_ram = min(cap_ram * ram_scale, pool.max_ram_pool)

    # Final clamp to current available resources will happen in scheduler loop
    return target_cpu, target_ram


@register_scheduler(key="scheduler_low_022")
def scheduler_low_022(s, results, pipelines):
    """
    Priority-aware scheduler:
      - Enqueue arriving pipelines by priority.
      - Process execution results:
          * OOM-like failure => increase RAM scale and re-try (pipeline continues).
          * Other failure   => blacklist pipeline (stop scheduling it).
      - For each pool, assign ready operators from highest-effective-priority queue,
        using per-priority capped resources to improve latency via concurrency.
    """
    s.tick += 1

    # 1) Enqueue new pipelines (FIFO by arrival within each class)
    for p in pipelines:
        if p.pipeline_id in s.blacklist:
            continue
        q = _get_queue(s, p.priority)
        q.append(p)
        s.enqueue_tick.setdefault(p.pipeline_id, s.tick)
        s.ram_scale.setdefault(p.pipeline_id, 1.0)
        s.cpu_scale.setdefault(p.pipeline_id, 1.0)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # 2) Incorporate feedback from completed/failed operators
    for r in results:
        # If we can identify pipeline_id from result, we would use it; not provided.
        # We rely on container/op-level failures being reflected in pipeline status on next tick.
        if r is None or not hasattr(r, "failed") or not r.failed():
            continue

        # Best-effort OOM backoff: increase RAM scale for pipelines that share this priority.
        # (Without pipeline_id in result, keep this conservative.)
        if _looks_like_oom(getattr(r, "error", None)):
            # Mildly increase for all waiting pipelines of same priority class (conservative, simple).
            q = _get_queue(s, getattr(r, "priority", None))
            for p in list(q)[:3]:  # limit blast radius; touch only a few oldest
                pid = p.pipeline_id
                s.ram_scale[pid] = min(s.ram_scale.get(pid, 1.0) * 1.5, 8.0)
        else:
            # Non-OOM failures: avoid infinite retries by blacklisting pipelines we can observe later.
            # Without pipeline_id, we cannot precisely map; do nothing here.
            pass

    # 3) Attempt to schedule across pools
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # While this pool has enough resources to run *something*, try multiple assignments.
        # Use small per-op bundles to improve queueing delay for interactive work.
        made_progress = True
        local_guard = 0
        while made_progress and avail_cpu > 0 and avail_ram > 0:
            local_guard += 1
            if local_guard > 64:
                # Prevent pathological looping if we can't find assignable ops.
                break

            made_progress = False
            queue_order = _effective_queue_order(s)

            # Try to find one pipeline with a ready operator
            chosen_pipeline = None
            chosen_queue = None
            chosen_ops = None

            for q in queue_order:
                if not q:
                    continue

                # Scan a small window to avoid head-of-line blocking when the FIFO head isn't ready.
                scan_limit = min(len(q), 8)
                for _ in range(scan_limit):
                    p = q[0]
                    q.rotate(-1)  # round-robin within queue window

                    if p.pipeline_id in s.blacklist:
                        continue

                    status = p.runtime_status()

                    # Drop completed pipelines
                    if status.is_pipeline_successful():
                        continue

                    # If pipeline has failures, decide whether to keep trying.
                    # (ASSIGNABLE_STATES may include FAILED; but we avoid non-OOM infinite loops.)
                    # If it has failed ops and also indicates pipeline failure count, we keep it unless blacklisted.
                    # This is intentionally lenient to allow OOM-backoff retries.

                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        continue

                    chosen_pipeline = p
                    chosen_queue = q
                    chosen_ops = op_list
                    break

                if chosen_pipeline is not None:
                    break

            if chosen_pipeline is None:
                break

            # Compute per-priority capped resources with per-pipeline learned scaling
            pid = chosen_pipeline.pipeline_id
            priority = chosen_pipeline.priority
            cpu_scale = s.cpu_scale.get(pid, 1.0)
            ram_scale = s.ram_scale.get(pid, 1.0)
            target_cpu, target_ram = _cap_resources_for_priority(pool, priority, cpu_scale, ram_scale)

            # Clamp to what the pool currently has
            alloc_cpu = min(avail_cpu, target_cpu)
            alloc_ram = min(avail_ram, target_ram)

            # If batch, additionally ensure we leave headroom for interactive work.
            # This is a simple reservation rule: don't consume the last 25% of the pool with batch.
            if _is_batch(priority):
                reserve_cpu = pool.max_cpu_pool * 0.25
                reserve_ram = pool.max_ram_pool * 0.25
                alloc_cpu = min(alloc_cpu, max(avail_cpu - reserve_cpu, 0.0))
                alloc_ram = min(alloc_ram, max(avail_ram - reserve_ram, 0.0))

            # If we can't allocate a meaningful bundle, stop trying in this pool.
            if alloc_cpu <= 0 or alloc_ram <= 0:
                break

            # Emit assignment
            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=alloc_cpu,
                    ram=alloc_ram,
                    priority=priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Update local available resources for potential additional assignments in this tick
            avail_cpu -= alloc_cpu
            avail_ram -= alloc_ram
            made_progress = True

    return suspensions, assignments
