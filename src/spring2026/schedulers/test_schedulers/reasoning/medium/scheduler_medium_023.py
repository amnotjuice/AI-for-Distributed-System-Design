# policy_key: scheduler_medium_023
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.084249
# generation_seconds: 66.52
# generated_at: 2026-04-09T22:59:59.974472
@register_scheduler_init(key="scheduler_medium_023")
def scheduler_medium_023_init(s):
    """
    Priority-aware, completion-focused scheduler with conservative RAM sizing + OOM-driven retries.

    Key ideas:
      - Strict priority order (QUERY > INTERACTIVE > BATCH) with mild anti-starvation aging for BATCH.
      - Conservative initial RAM allocations to reduce OOM-induced failures; on OOM, exponentially increase RAM hint per operator.
      - Per-pipeline concurrency caps to reduce thrash and keep latency low for high-priority pipelines.
      - Optional pool specialization: if multiple pools exist, prefer pool 0 for QUERY/INTERACTIVE and other pools for BATCH,
        but allow spillover when idle.
      - No preemption (uses only admission/placement/sizing) to avoid wasted work and churn.
    """
    from collections import deque

    # Separate queues by priority to keep high-priority latency low.
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Lightweight time base (ticks) for aging / fairness decisions.
    s.tick = 0

    # Per-pipeline metadata to support aging decisions.
    # pipeline_id -> {"arrival_tick": int}
    s.pipeline_meta = {}

    # Per-operator resource hints learned from prior attempts (primarily OOM).
    # (pipeline_id, op_key) -> ram_hint
    s.op_ram_hint = {}
    # (pipeline_id, op_key) -> retry_count
    s.op_retry_count = {}

    # Anti-starvation knobs (in ticks).
    s.batch_aging_threshold = 50

    # Per-pipeline in-flight caps by priority to avoid flooding a single pipeline.
    s.inflight_cap = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 1,
    }


def _op_key(op):
    # Best-effort stable key across the simulation for per-operator hinting.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return str(v)
            except Exception:
                pass
    return str(id(op))


def _is_oom_error(err):
    if err is None:
        return False
    try:
        s = str(err).lower()
    except Exception:
        return False
    return ("oom" in s) or ("out of memory" in s) or ("memory" in s and "alloc" in s)


def _enqueue_pipeline(s, p):
    pid = p.pipeline_id
    if pid not in s.pipeline_meta:
        s.pipeline_meta[pid] = {"arrival_tick": s.tick}

    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _pipeline_age(s, p):
    meta = s.pipeline_meta.get(p.pipeline_id)
    if not meta:
        return 0
    return max(0, s.tick - meta.get("arrival_tick", s.tick))


def _pipeline_inflight(status):
    # Count "currently consuming capacity" ops.
    # Prefer explicit enum values; fall back safely if missing.
    inflight = 0
    try:
        inflight += status.state_counts.get(OperatorState.ASSIGNED, 0)
    except Exception:
        pass
    try:
        inflight += status.state_counts.get(OperatorState.RUNNING, 0)
    except Exception:
        pass
    try:
        inflight += status.state_counts.get(OperatorState.SUSPENDING, 0)
    except Exception:
        pass
    return inflight


def _get_assignable_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _choose_next_pipeline_for_pool(s, pool_id, allow_batch=True):
    """
    Returns a pipeline selected for scheduling on this pool, or None if none is suitable.
    Implements:
      - Priority order (QUERY > INTERACTIVE > BATCH)
      - Batch aging: very old batch can be treated like interactive
      - Skips completed pipelines
      - Skips pipelines exceeding in-flight cap
    """
    # Helper to pop-rotate within a deque while searching for a schedulable pipeline.
    def pick_from_queue(dq, priority, max_scan=32):
        scanned = 0
        while dq and scanned < max_scan:
            p = dq.popleft()
            scanned += 1

            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            cap = s.inflight_cap.get(priority, 1)
            if _pipeline_inflight(status) >= cap:
                dq.append(p)
                continue

            op = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not op:
                dq.append(p)
                continue

            # Candidate found.
            dq.append(p)  # round-robin: keep it in rotation after selecting
            return p

        return None

    # If multiple pools exist, pool 0 is "high priority preferred".
    hi_pool = (s.executor.num_pools > 1 and pool_id == 0)

    # Batch aging: if oldest batch is very old, consider it earlier (like interactive),
    # especially on non-hi pools to prevent starvation and "incomplete pipeline" penalties.
    oldest_batch = None
    if s.q_batch:
        try:
            # Scan a small window for the oldest.
            window = list(s.q_batch)[:16]
            oldest_batch = max(window, key=lambda p: _pipeline_age(s, p))
        except Exception:
            oldest_batch = None

    # Pool preference logic:
    # - hi_pool: query -> interactive -> (aged batch if very old) -> batch
    # - other pools: (aged batch if very old) -> batch -> interactive spillover -> query spillover
    if hi_pool:
        p = pick_from_queue(s.q_query, Priority.QUERY)
        if p:
            return p
        p = pick_from_queue(s.q_interactive, Priority.INTERACTIVE)
        if p:
            return p
        if allow_batch and oldest_batch is not None and _pipeline_age(s, oldest_batch) >= s.batch_aging_threshold:
            return oldest_batch
        if allow_batch:
            return pick_from_queue(s.q_batch, Priority.BATCH_PIPELINE)
        return None
    else:
        if allow_batch and oldest_batch is not None and _pipeline_age(s, oldest_batch) >= s.batch_aging_threshold:
            return oldest_batch
        if allow_batch:
            p = pick_from_queue(s.q_batch, Priority.BATCH_PIPELINE)
            if p:
                return p
        # Spillover for keeping utilization high, but still be careful with interference.
        p = pick_from_queue(s.q_interactive, Priority.INTERACTIVE)
        if p:
            return p
        return pick_from_queue(s.q_query, Priority.QUERY)


def _size_resources_for_op(s, pool, pipeline, op):
    """
    Conservative sizing to reduce OOM (failure is heavily penalized by objective).
    - Start with priority-based fractions of pool capacity.
    - Apply per-op RAM hints that grow exponentially on OOM.
    - Clamp by currently available pool resources.
    """
    # Priority-based defaults: balance latency vs. parallelism.
    if pipeline.priority == Priority.QUERY:
        cpu_ratio, ram_ratio = 0.40, 0.35
        min_cpu = 1.0
    elif pipeline.priority == Priority.INTERACTIVE:
        cpu_ratio, ram_ratio = 0.25, 0.25
        min_cpu = 1.0
    else:
        cpu_ratio, ram_ratio = 0.15, 0.15
        min_cpu = 1.0

    target_cpu = max(min_cpu, pool.max_cpu_pool * cpu_ratio)
    target_ram = max(1.0, pool.max_ram_pool * ram_ratio)

    # Apply learned RAM hint (never go below it).
    key = (pipeline.pipeline_id, _op_key(op))
    hinted_ram = s.op_ram_hint.get(key)
    if hinted_ram is not None:
        try:
            target_ram = max(float(target_ram), float(hinted_ram))
        except Exception:
            pass

    # Clamp to what is currently available.
    cpu = min(pool.avail_cpu_pool, target_cpu)
    ram = min(pool.avail_ram_pool, target_ram)

    # Ensure minimums to avoid pathological tiny allocations.
    cpu = max(0.0, cpu)
    ram = max(0.0, ram)

    return cpu, ram


@register_scheduler(key="scheduler_medium_023")
def scheduler_medium_023(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Priority-aware, completion-focused scheduler.

    - Updates per-operator RAM hints based on execution results (OOM => double RAM next time).
    - Enqueues new pipelines into priority queues.
    - For each pool, repeatedly assigns one ready operator at a time while resources allow:
        * Prefer QUERY/INTERACTIVE to minimize weighted objective.
        * Keep BATCH making progress via aging and non-hi pools.
    """
    s.tick += 1

    # Incorporate new pipelines.
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from results: primarily detect OOM and increase RAM hints to avoid repeat failures.
    for r in results:
        if not getattr(r, "ops", None):
            continue

        pool = None
        try:
            pool = s.executor.pools[r.pool_id]
        except Exception:
            pool = None

        for op in r.ops:
            k = (getattr(r, "pipeline_id", None), _op_key(op))
            # If pipeline_id isn't attached to result, fall back to a weaker key derived from op only.
            if k[0] is None:
                k = ("unknown", k[1])

            if r.failed():
                s.op_retry_count[k] = s.op_retry_count.get(k, 0) + 1

                # Exponential backoff on RAM for OOM; mild increase otherwise.
                prev = s.op_ram_hint.get(k, None)
                if prev is None:
                    prev = getattr(r, "ram", None)
                try:
                    prev = float(prev) if prev is not None else None
                except Exception:
                    prev = None

                max_ram = None
                if pool is not None:
                    try:
                        max_ram = float(pool.max_ram_pool)
                    except Exception:
                        max_ram = None

                if _is_oom_error(getattr(r, "error", None)):
                    if prev is None:
                        # If we don't know what was used, start from a conservative fraction of pool.
                        base = (0.25 * max_ram) if max_ram is not None else 1.0
                        new_hint = max(1.0, base)
                    else:
                        new_hint = max(1.0, prev * 2.0)
                else:
                    # Non-OOM failure: nudge RAM upward slightly (could be borderline).
                    if prev is None:
                        base = (0.20 * max_ram) if max_ram is not None else 1.0
                        new_hint = max(1.0, base)
                    else:
                        new_hint = max(1.0, prev * 1.25)

                if max_ram is not None:
                    new_hint = min(new_hint, max_ram)

                s.op_ram_hint[k] = new_hint
            else:
                # On success, keep RAM hint (do not aggressively reduce; failures are expensive).
                pass

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Scheduling: assign as many as we can per pool while leaving headroom when needed.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # If only one pool, keep some headroom to avoid batch soaking capacity right before a query arrives.
        # This is a soft guard: only limits BATCH allocations when there is Q/Interactive waiting.
        single_pool = (s.executor.num_pools == 1)
        high_waiting = bool(s.q_query) or bool(s.q_interactive)
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if single_pool and high_waiting:
            reserve_cpu = 0.25 * pool.max_cpu_pool
            reserve_ram = 0.25 * pool.max_ram_pool

        # Avoid infinite loops: cap number of assignments per pool per tick.
        for _ in range(16):
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Decide whether to allow batch on this pool right now.
            allow_batch = True
            if single_pool and high_waiting:
                # If we're close to consuming reserved headroom, stop admitting batch.
                if (avail_cpu <= reserve_cpu) or (avail_ram <= reserve_ram):
                    allow_batch = False

            p = _choose_next_pipeline_for_pool(s, pool_id, allow_batch=allow_batch)
            if p is None:
                break

            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            # Ensure we can actually schedule a ready op.
            op = _get_assignable_op(p)
            if op is None:
                continue

            cpu, ram = _size_resources_for_op(s, pool, p, op)

            # If we can't fit our conservative RAM request, avoid under-allocating (OOM is costly).
            if cpu <= 0 or ram <= 0:
                break
            if ram > pool.avail_ram_pool + 1e-9:
                continue
            if cpu > pool.avail_cpu_pool + 1e-9:
                continue

            # For batches under contention, further cap CPU to preserve responsiveness.
            if single_pool and high_waiting and p.priority == Priority.BATCH_PIPELINE:
                cpu = min(cpu, max(1.0, 0.15 * pool.max_cpu_pool))

            # Final clamp to availability.
            cpu = min(cpu, pool.avail_cpu_pool)
            ram = min(ram, pool.avail_ram_pool)

            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # NOTE: We rely on the simulator to update pool.avail_* after applying assignments.
            # We do not manually decrement to avoid diverging from executor accounting.

    return suspensions, assignments
