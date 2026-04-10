# policy_key: scheduler_medium_018
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.085903
# generation_seconds: 88.06
# generated_at: 2026-03-12T22:18:40.324125
@register_scheduler_init(key="scheduler_medium_018")
def scheduler_medium_018_init(s):
    """
    Priority-aware, headroom-reserving FIFO.

    Improvements over naive FIFO:
      1) Separate queues per priority and schedule high-priority first (lower latency).
      2) Reserve per-pool headroom so batch can't fully consume a pool (protects tail latency).
      3) Simple retry sizing: if a pipeline has FAILED ops, retry with larger RAM (OOM-robustness).
      4) Avoid scheduling multiple ops from the same pipeline in a single tick (reduces self-contention).
    """
    import collections

    s.tick = 0

    # Per-priority FIFO queues (pipelines are enqueued once and rotated until runnable).
    s.queues = {
        Priority.QUERY: collections.deque(),
        Priority.INTERACTIVE: collections.deque(),
        Priority.BATCH_PIPELINE: collections.deque(),
    }

    # Per-pipeline metadata for simple adaptation.
    # meta[pipeline_id] = {
    #   "enq_tick": int,
    #   "fail_count": int,  # increases when FAILED ops are observed
    #   "last_fail_seen_tick": int,
    # }
    s.meta = {}

    # Tuning knobs (kept intentionally simple / conservative).
    s.max_retries = 2  # after this many observed failures, we stop trying
    s.reserve_frac_cpu = 0.20  # keep this fraction of each pool's CPU for high priority
    s.reserve_frac_ram = 0.20  # keep this fraction of each pool's RAM for high priority

    # Requested resource fractions of a pool for the next op (before retry boosts).
    # High priority gets more to reduce latency; batch gets less to avoid monopolizing.
    s.cpu_frac = {
        Priority.QUERY: 1.00,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.30,
    }
    s.ram_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.30,
    }

    # Limit batching within one scheduler tick to avoid overly aggressive packing decisions.
    s.max_assignments_per_pool = 4


def _priority_order():
    # Strict priority: protect latency first.
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _ensure_meta(s, pipeline):
    pid = pipeline.pipeline_id
    if pid not in s.meta:
        s.meta[pid] = {"enq_tick": s.tick, "fail_count": 0, "last_fail_seen_tick": -1}


def _enqueue_pipeline(s, pipeline):
    _ensure_meta(s, pipeline)
    s.queues[pipeline.priority].append(pipeline)


def _should_drop_pipeline(s, pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If we've seen repeated failures, stop retrying (avoids infinite loops on logical errors).
    pid = pipeline.pipeline_id
    meta = s.meta.get(pid)
    if meta and meta["fail_count"] > s.max_retries:
        return True
    return False


def _observe_failures_and_update_meta(s, pipeline):
    """If the pipeline currently has FAILED ops, bump fail_count once per tick."""
    status = pipeline.runtime_status()
    if status.state_counts.get(OperatorState.FAILED, 0) > 0:
        pid = pipeline.pipeline_id
        _ensure_meta(s, pipeline)
        if s.meta[pid]["last_fail_seen_tick"] != s.tick:
            s.meta[pid]["fail_count"] += 1
            s.meta[pid]["last_fail_seen_tick"] = s.tick


def _pick_next_runnable_pipeline(s, prio, assigned_this_tick):
    """
    Rotate through the queue for this priority to find a pipeline with a runnable op.
    Returns (pipeline, op_list) or (None, None).
    """
    q = s.queues[prio]
    if not q:
        return None, None

    n = len(q)
    for _ in range(n):
        pipeline = q.popleft()

        # Drop completed or hopeless pipelines.
        if _should_drop_pipeline(s, pipeline):
            s.meta.pop(pipeline.pipeline_id, None)
            continue

        # Don't schedule multiple ops from the same pipeline in one scheduler tick.
        if pipeline.pipeline_id in assigned_this_tick:
            q.append(pipeline)
            continue

        # Update retry metadata if FAILED ops are observed.
        _observe_failures_and_update_meta(s, pipeline)

        status = pipeline.runtime_status()
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not runnable yet; keep it in the queue.
            q.append(pipeline)
            continue

        # Runnable: requeue the pipeline (it likely has more ops later) and return runnable op(s).
        q.append(pipeline)
        return pipeline, op_list

    return None, None


def _pool_sort_key(pool):
    # Prefer pools with more headroom (reduces chance that high-priority waits behind fragmentation).
    return (pool.avail_cpu_pool, pool.avail_ram_pool)


@register_scheduler(key="scheduler_medium_018")
def scheduler_medium_018(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Policy: strict priority + reserved headroom for batch + simple retry RAM scaling.

    - QUERY/INTERACTIVE are scheduled first and can consume full available resources.
    - BATCH is only scheduled from "non-reserved" capacity to keep headroom for latency-sensitive work.
    - If a pipeline accumulates FAILED ops, we retry with higher RAM (exponential-ish),
      but stop after a small number of observed failures.
    """
    s.tick += 1

    # Enqueue newly arrived pipelines.
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Early exit if there's nothing to do.
    if not pipelines and not results:
        # We still might have waiting work; only exit if queues are empty.
        if not any(len(q) for q in s.queues.values()):
            return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Track which pipelines we've scheduled in this tick (avoid parallel self-contention).
    assigned_this_tick = set()

    # Consider pools in order of headroom, so high priority tends to land where it fits best.
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(key=lambda i: _pool_sort_key(s.executor.pools[i]), reverse=True)

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        local_avail_cpu = pool.avail_cpu_pool
        local_avail_ram = pool.avail_ram_pool
        if local_avail_cpu <= 0 or local_avail_ram <= 0:
            continue

        reserve_cpu = pool.max_cpu_pool * s.reserve_frac_cpu
        reserve_ram = pool.max_ram_pool * s.reserve_frac_ram

        per_pool_assignments = 0
        while per_pool_assignments < s.max_assignments_per_pool:
            # If no capacity remains, stop packing this pool.
            if local_avail_cpu <= 0 or local_avail_ram <= 0:
                break

            picked_pipeline = None
            picked_ops = None
            picked_prio = None

            # Strict priority: try high-priority first.
            for prio in _priority_order():
                # Gate batch behind reserved headroom: only schedule batch from "excess" capacity.
                if prio == Priority.BATCH_PIPELINE:
                    eff_cpu = local_avail_cpu - reserve_cpu
                    eff_ram = local_avail_ram - reserve_ram
                    if eff_cpu <= 0 or eff_ram <= 0:
                        continue  # keep headroom for potential high-priority arrivals
                pipeline, op_list = _pick_next_runnable_pipeline(s, prio, assigned_this_tick)
                if pipeline is not None and op_list:
                    picked_pipeline, picked_ops, picked_prio = pipeline, op_list, prio
                    break

            if picked_pipeline is None:
                break

            # Retry scaling: more failures -> more RAM (and a bit more CPU) on subsequent attempts.
            meta = s.meta.get(picked_pipeline.pipeline_id, {"fail_count": 0})
            fail_count = meta.get("fail_count", 0)
            ram_boost = 2 ** max(0, fail_count - 1)  # first observed failure => boost starts at 1x->2x
            cpu_boost = 1.0 + 0.25 * max(0, fail_count - 1)

            # Decide effective capacity based on priority/headroom rules.
            if picked_prio == Priority.BATCH_PIPELINE:
                eff_avail_cpu = max(0, local_avail_cpu - reserve_cpu)
                eff_avail_ram = max(0, local_avail_ram - reserve_ram)
            else:
                eff_avail_cpu = local_avail_cpu
                eff_avail_ram = local_avail_ram

            if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                # Shouldn't happen due to earlier gating, but keep it safe.
                break

            # Request a fraction of the pool; cap by effective availability.
            target_cpu = pool.max_cpu_pool * s.cpu_frac[picked_prio] * cpu_boost
            target_ram = pool.max_ram_pool * s.ram_frac[picked_prio] * ram_boost

            req_cpu = min(eff_avail_cpu, target_cpu)
            req_ram = min(eff_avail_ram, target_ram)

            # Ensure strictly positive allocations.
            if req_cpu <= 0 or req_ram <= 0:
                break

            assignments.append(
                Assignment(
                    ops=picked_ops,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=picked_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=picked_pipeline.pipeline_id,
                )
            )

            assigned_this_tick.add(picked_pipeline.pipeline_id)
            per_pool_assignments += 1

            # Update local capacity so we don't overcommit within this tick.
            local_avail_cpu -= req_cpu
            local_avail_ram -= req_ram

            # To reduce latency variability, keep high priority "big" (at most one per pool per tick).
            if picked_prio in (Priority.QUERY, Priority.INTERACTIVE):
                break

    return suspensions, assignments
