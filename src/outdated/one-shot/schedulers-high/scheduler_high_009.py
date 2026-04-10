# policy_key: scheduler_high_009
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.123521
# generation_seconds: 122.77
# generated_at: 2026-03-12T22:44:01.256929
@register_scheduler_init(key="scheduler_high_009")
def scheduler_high_009_init(s):
    """
    Priority-aware, pool-filling scheduler (small, safe step up from naive FIFO).

    Improvements vs. the naive example:
      1) Priority queues: always prefer QUERY/INTERACTIVE over BATCH to improve latency.
      2) Fill each pool with multiple assignments per tick (instead of 1 op per pool),
         using conservative per-op CPU/RAM "chunks" to reduce head-of-line blocking.
      3) Simple fairness: batch still makes progress via lightweight token/credit system.
      4) Simple OOM adaptation: if we observe OOM failures for a priority class, increase
         the default RAM fraction used for that priority (coarse but robust without per-op metadata).

    Notes:
      - No preemption: the simulator API surface shown doesn't expose a reliable way to enumerate
        running containers to suspend, so we keep this policy "working-first".
    """
    # Per-priority waiting queues.
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track pipelines we've already enqueued (avoid accidental duplicates).
    s._known_pipeline_ids = set()

    # Token-based fairness (batch gets fewer tokens; high-priority mostly bypasses tokens).
    s.weights = {
        Priority.QUERY: 6,
        Priority.INTERACTIVE: 4,
        Priority.BATCH_PIPELINE: 1,
    }
    s.tokens = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.max_token = 50

    # Default CPU fractions by priority (chunk size).
    s.cpu_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Default RAM fractions by priority; adapted on observed OOMs.
    s.base_ram_frac = {
        Priority.QUERY: 0.20,
        Priority.INTERACTIVE: 0.20,
        Priority.BATCH_PIPELINE: 0.35,
    }
    s.ram_frac_hint = dict(s.base_ram_frac)

    # If multiple pools exist, keep some headroom in pool 0 for latency-sensitive work.
    s.pool0_reserve_cpu_frac = 0.25
    s.pool0_reserve_ram_frac = 0.25

    # Avoid scheduling multiple ops from the same pipeline in the same tick (simple throttling).
    s._assigned_this_tick = set()


def _sh9_is_oom(err) -> bool:
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)


def _sh9_prio_queue(s, prio):
    # Unknown priority falls back to batch.
    return s.wait_q.get(prio, s.wait_q[Priority.BATCH_PIPELINE])


def _sh9_queue_nonempty(s):
    return any(len(q) > 0 for q in s.wait_q.values())


def _sh9_pop_next_pipeline_for_pool(s, pool_id: int):
    """
    Pick next pipeline to try, using:
      - strict priority preference (QUERY > INTERACTIVE > BATCH)
      - token gating mainly for BATCH fairness
    """
    prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Prefer high-priority immediately; tokens are primarily to ensure batch eventually runs.
    for prio in prio_order:
        q = _sh9_prio_queue(s, prio)
        if not q:
            continue
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            return prio, q.pop(0)
        # Batch: require tokens if possible (otherwise it may still run if nothing else runs).
        if s.tokens.get(prio, 0) >= 1:
            return prio, q.pop(0)

    # If we got here, maybe batch has 0 tokens but is the only thing available.
    q = _sh9_prio_queue(s, Priority.BATCH_PIPELINE)
    if q:
        return Priority.BATCH_PIPELINE, q.pop(0)

    return None, None


def _sh9_requeue(s, prio, pipeline):
    _sh9_prio_queue(s, prio).append(pipeline)


def _sh9_desired_resources(s, pool, pool_id: int, prio, avail_cpu, avail_ram):
    """
    Compute conservative CPU/RAM chunk sizes.
    Also enforce a "reserve" in pool 0 for multi-pool setups by limiting batch allocations.
    """
    # Basic minimums.
    min_cpu = 1
    # RAM minimum is tricky without units; use a tiny positive floor plus a small fraction.
    min_ram = max(0.01 * pool.max_ram_pool, 0.1)

    # If this is pool 0 in a multi-pool cluster, avoid letting batch consume the reserved headroom.
    if s.executor.num_pools > 1 and pool_id == 0 and prio == Priority.BATCH_PIPELINE:
        reserve_cpu = s.pool0_reserve_cpu_frac * pool.max_cpu_pool
        reserve_ram = s.pool0_reserve_ram_frac * pool.max_ram_pool
        avail_cpu = max(0, avail_cpu - reserve_cpu)
        avail_ram = max(0, avail_ram - reserve_ram)

    if avail_cpu < min_cpu or avail_ram < min_ram:
        return 0, 0

    cpu_frac = s.cpu_frac.get(prio, 0.25)
    ram_frac = s.ram_frac_hint.get(prio, s.base_ram_frac.get(prio, 0.25))

    # Chunked sizing (do not exceed pool availability).
    desired_cpu = max(min_cpu, int(round(pool.max_cpu_pool * cpu_frac)))
    desired_cpu = min(desired_cpu, int(avail_cpu))

    desired_ram = pool.max_ram_pool * ram_frac
    desired_ram = max(min_ram, desired_ram)
    desired_ram = min(desired_ram, avail_ram)

    # If we're resource starved, still try to schedule with what's left (as long as > minimum).
    if desired_cpu < min_cpu or desired_ram < min_ram:
        return 0, 0

    return desired_cpu, desired_ram


@register_scheduler(key="scheduler_high_009")
def scheduler_high_009(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Scheduler step:
      - enqueue newly arrived pipelines into per-priority queues
      - update coarse RAM hints on OOM failures
      - fill each pool with as many assignments as resources allow, prioritizing latency-sensitive work
    """
    # Enqueue new pipelines.
    for p in pipelines:
        if getattr(p, "pipeline_id", None) in s._known_pipeline_ids:
            continue
        s._known_pipeline_ids.add(p.pipeline_id)
        _sh9_prio_queue(s, p.priority).append(p)

        # Ensure newly arrived high-priority work gets scheduled promptly.
        if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            s.tokens[p.priority] = max(s.tokens.get(p.priority, 0), 1)

    # If nothing changed and nothing waiting, do nothing.
    if not pipelines and not results and not _sh9_queue_nonempty(s):
        return [], []

    # Refresh per-tick guard.
    s._assigned_this_tick = set()

    # Update tokens each tick to ensure batch eventually progresses.
    for prio, w in s.weights.items():
        s.tokens[prio] = min(s.max_token, s.tokens.get(prio, 0) + w)

    # OOM adaptation: bump RAM fraction for that priority (coarse but helps avoid repeated OOM loops).
    for r in results:
        if r is None or not getattr(r, "failed", None):
            continue
        if r.failed() and _sh9_is_oom(getattr(r, "error", None)):
            prio = getattr(r, "priority", Priority.BATCH_PIPELINE)
            pool = s.executor.pools[r.pool_id]
            # Increase RAM fraction based on what we just tried (double, capped).
            tried_frac = 0.0
            try:
                tried_frac = float(r.ram) / float(pool.max_ram_pool) if pool.max_ram_pool else 0.0
            except Exception:
                tried_frac = 0.0
            new_frac = max(s.ram_frac_hint.get(prio, s.base_ram_frac.get(prio, 0.25)), min(1.0, tried_frac * 2.0))
            # Also ensure we never go below base for that priority.
            s.ram_frac_hint[prio] = max(new_frac, s.base_ram_frac.get(prio, 0.25))

        # Gentle decay on non-OOM failures/success isn't possible without reliable success signals here;
        # keep hints stable to avoid oscillation.

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Fill each pool with multiple assignments per tick (fixes "one op per pool" head-of-line blocking).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Limit the number of scans to avoid infinite loops when ops aren't ready.
        # We'll try at most N pipeline pops per pool per tick.
        max_pops = 0
        for q in s.wait_q.values():
            max_pops += len(q)
        max_pops = max(max_pops, 0)

        pops = 0
        made_progress = True

        while made_progress and pops < max_pops:
            made_progress = False

            # If no resources left at all, stop.
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            prio, pipeline = _sh9_pop_next_pipeline_for_pool(s, pool_id)
            if pipeline is None:
                break
            pops += 1

            # Drop completed pipelines.
            status = pipeline.runtime_status()
            if status.is_pipeline_successful():
                continue

            # Don't schedule multiple operators from the same pipeline in one tick.
            if pipeline.pipeline_id in s._assigned_this_tick:
                _sh9_requeue(s, prio, pipeline)
                continue

            # Get a single assignable op whose parents are complete.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; requeue and try others.
                _sh9_requeue(s, prio, pipeline)
                continue

            desired_cpu, desired_ram = _sh9_desired_resources(s, pool, pool_id, prio, avail_cpu, avail_ram)
            if desired_cpu <= 0 or desired_ram <= 0:
                # Not enough headroom in this pool for this priority right now.
                _sh9_requeue(s, prio, pipeline)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=desired_cpu,
                    ram=desired_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Spend token for batch (and optionally for others, but we keep HQ latency-first).
            if prio == Priority.BATCH_PIPELINE and s.tokens.get(prio, 0) >= 1:
                s.tokens[prio] -= 1

            # Update remaining resources and per-tick guard.
            avail_cpu -= desired_cpu
            avail_ram -= desired_ram
            s._assigned_this_tick.add(pipeline.pipeline_id)

            # Requeue the pipeline for subsequent operators.
            _sh9_requeue(s, prio, pipeline)
            made_progress = True

    return suspensions, assignments
