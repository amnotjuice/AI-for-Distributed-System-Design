# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r17
@register_scheduler_init(key="scheduler_low_011_r17")
def scheduler_low_011_r17_init(s):
    """Priority-aware FIFO (incremental upgrade) focused on tail-latency.

    Key incremental fixes vs prior iteration:
    1) Actually retry OOM failures: previous version learned RAM hints but then dropped any pipeline with FAILED ops.
       Here we track which FAILED ops are OOM-retryable and keep those pipelines eligible for rescheduling.
    2) Reserve headroom for high-priority work (QUERY/INTERACTIVE) so BATCH can't fully consume the pool(s).
    3) Allow >1 assignment per pool per tick (bounded by local available resources), improving responsiveness.
    4) Interactive-pool preference with controlled spillover to other pools when the interactive pool is saturated.

    Notes:
    - No preemption used (requires visibility into running containers beyond the minimal interface).
    - Conservative, heuristic sizing: prioritize finishing high-priority ops quickly while preserving concurrency.
    """
    # Per-priority FIFO queues
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Tick counter for simple aging/spillover heuristics
    s.tick = 0

    # When we first saw a pipeline (for aging/spillover)
    s.pipeline_first_seen_tick = {}  # pipeline_id -> tick

    # Map op identity back to its pipeline_id (ExecutionResult may not include pipeline_id)
    s.op_to_pipeline = {}  # id(op) -> pipeline_id

    # Learned hints per operator attempt (primarily RAM bumps after OOM)
    # key: (pipeline_id, id(op)) -> {"ram": float, "cpu": float}
    s.op_hints = {}

    # Retry bookkeeping
    s.op_attempts = {}          # (pipeline_id, id(op)) -> int (OOM retries observed)
    s.oom_retryable_ops = set()  # set of (pipeline_id, id(op)) that failed due to OOM and can be retried
    s.hard_failed_ops = set()    # set of (pipeline_id, id(op)) that failed for non-OOM reasons

    # Policy knobs (small, low-risk)
    s.max_oom_retries_per_op = 3

    # Pool preference: pool 0 is treated as "interactive" if it exists
    s.interactive_pool_id = 0

    # Spillover: how long high-priority work waits before we allow it to run on non-interactive pools
    s.hp_spillover_ticks = 2

    # Default sizing fractions of pool max (used as baseline, then adjusted by queue pressure and hints)
    s.base_fracs = {
        Priority.QUERY: {"cpu": 0.70, "ram": 0.50},
        Priority.INTERACTIVE: {"cpu": 0.70, "ram": 0.50},
        Priority.BATCH_PIPELINE: {"cpu": 1.00, "ram": 1.00},
    }

    # Headroom reservation fractions (to protect latency)
    # Applied only to BATCH when any high-priority work is waiting.
    s.reserve_single_pool = {"cpu": 0.40, "ram": 0.40}
    s.reserve_interactive_pool = {"cpu": 0.50, "ram": 0.50}
    s.reserve_other_pool = {"cpu": 0.20, "ram": 0.20}


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("killed process" in msg)


def _op_key(pipeline_id, op):
    return (pipeline_id, id(op))


def _get_assignable_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _pipeline_is_done(status):
    return status.is_pipeline_successful()


def _pipeline_has_unretryable_failure(s, pipeline):
    """Return True if pipeline should be dropped due to hard failures or exceeded OOM retries."""
    status = pipeline.runtime_status()
    failed_count = status.state_counts.get(OperatorState.FAILED, 0) if hasattr(status, "state_counts") else 0
    if failed_count <= 0:
        return False

    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    for op in failed_ops:
        k = _op_key(pipeline.pipeline_id, op)

        # Non-OOM failure: never retry (low-risk policy)
        if k in s.hard_failed_ops:
            return True

        # If we don't know it's OOM-retryable, treat as hard fail (conservative)
        if k not in s.oom_retryable_ops:
            return True

        # OOM retry budget exceeded
        if int(s.op_attempts.get(k, 0)) > int(s.max_oom_retries_per_op):
            return True

    return False


def _hp_waiting_nonempty(s):
    return bool(s.waiting_queues[Priority.QUERY] or s.waiting_queues[Priority.INTERACTIVE])


def _compute_batch_reserve_fracs(s, pool_id):
    # Single pool: reserve more aggressively
    if s.executor.num_pools <= 1:
        return s.reserve_single_pool["cpu"], s.reserve_single_pool["ram"]

    if pool_id == s.interactive_pool_id:
        return s.reserve_interactive_pool["cpu"], s.reserve_interactive_pool["ram"]

    return s.reserve_other_pool["cpu"], s.reserve_other_pool["ram"]


def _size_request(s, pool, priority, pipeline_id, op, hp_pressure):
    """Compute (cpu, ram) request bounded by pool max; not bounded by local avail (done by caller)."""
    fr = s.base_fracs.get(priority, {"cpu": 1.0, "ram": 1.0})
    cpu = max(1.0, float(pool.max_cpu_pool) * float(fr["cpu"]))
    ram = max(1.0, float(pool.max_ram_pool) * float(fr["ram"]))

    # If lots of high-priority work is waiting, bias toward more concurrency (smaller slices)
    # This is a latency tail heuristic: avoid a single query hogging the whole pool.
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        if hp_pressure >= 3:
            cpu = max(1.0, float(pool.max_cpu_pool) * 0.45)
        elif hp_pressure == 2:
            cpu = max(1.0, float(pool.max_cpu_pool) * 0.55)
        # hp_pressure <= 1 -> keep baseline (~0.70)

    # Apply learned hints (OOM bumps); only grow allocations, never shrink below baseline.
    k = _op_key(pipeline_id, op)
    hint = s.op_hints.get(k)
    if hint:
        try:
            cpu = max(cpu, float(hint.get("cpu", cpu)))
        except Exception:
            pass
        try:
            ram = max(ram, float(hint.get("ram", ram)))
        except Exception:
            pass

    # Cap at pool max
    cpu = min(cpu, float(pool.max_cpu_pool))
    ram = min(ram, float(pool.max_ram_pool))

    # Ensure positive
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


def _pool_iteration_order(s):
    # Give the interactive pool first scheduling chance to reduce spillover churn.
    if s.executor.num_pools <= 1:
        return list(range(s.executor.num_pools))
    first = s.interactive_pool_id if 0 <= s.interactive_pool_id < s.executor.num_pools else 0
    return [first] + [i for i in range(s.executor.num_pools) if i != first]


@register_scheduler(key="scheduler_low_011_r17")
def scheduler_low_011_r17(s, results, pipelines):
    """
    Scheduler step:
    - Enqueue arrivals into per-priority queues.
    - Process results to:
        * map failures to OOM-retryable vs hard-failed
        * bump RAM hints on OOM
    - For each pool (interactive first), schedule as many ops as local availability allows:
        * Always pick highest priority first
        * Reserve headroom for HP when HP waiting (batch throttling)
        * Prefer HP on interactive pool; allow spillover after small wait or if interactive pool is saturated
        * Limit 1 op per pipeline per tick to reduce single-pipeline dominance
    """
    s.tick += 1

    # Enqueue new pipelines
    for p in pipelines:
        pr = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
        s.waiting_queues[pr].append(p)
        if p.pipeline_id not in s.pipeline_first_seen_tick:
            s.pipeline_first_seen_tick[p.pipeline_id] = s.tick

    # Update failure knowledge / hints from execution results
    for r in results:
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        ops = getattr(r, "ops", None) or []
        is_oom = _is_oom_error(getattr(r, "error", None))

        for op in ops:
            pipeline_id = s.op_to_pipeline.get(id(op))
            if pipeline_id is None:
                # Best-effort: without pipeline mapping we cannot safely retry or hint
                continue

            k = _op_key(pipeline_id, op)

            if is_oom:
                # Mark retryable and bump RAM hint exponentially (capped by pool max at assignment time)
                s.oom_retryable_ops.add(k)
                s.op_attempts[k] = int(s.op_attempts.get(k, 0)) + 1

                prev_hint = s.op_hints.get(k, {})
                prev_ram = float(prev_hint.get("ram", 0.0) or 0.0)
                observed_ram = float(getattr(r, "ram", 0.0) or 0.0)
                baseline = prev_ram if prev_ram > 0 else (observed_ram if observed_ram > 0 else 1.0)
                new_ram = max(1.0, baseline * 2.0)

                prev_cpu = float(prev_hint.get("cpu", 0.0) or 0.0)
                observed_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                baseline_cpu = prev_cpu if prev_cpu > 0 else (observed_cpu if observed_cpu > 0 else 1.0)

                s.op_hints[k] = {"ram": new_ram, "cpu": max(1.0, baseline_cpu)}
            else:
                # Hard failure: do not retry
                s.hard_failed_ops.add(k)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # One-op-per-pipeline-per-tick limiter
    assigned_pipeline_ids = set()

    # Approximate pressure for sizing heuristics
    hp_pressure = len(s.waiting_queues[Priority.QUERY]) + len(s.waiting_queues[Priority.INTERACTIVE])

    # Capture interactive pool current availability (used for spillover decisions)
    interactive_pool_avail_cpu = 0.0
    interactive_pool_avail_ram = 0.0
    if s.executor.num_pools > 0 and 0 <= s.interactive_pool_id < s.executor.num_pools:
        ip = s.executor.pools[s.interactive_pool_id]
        interactive_pool_avail_cpu = float(ip.avail_cpu_pool)
        interactive_pool_avail_ram = float(ip.avail_ram_pool)

    def should_allow_hp_on_pool(priority, pipeline, pool_id):
        """Prefer HP on interactive pool, but allow spillover if it has waited or interactive is saturated."""
        if s.executor.num_pools <= 1:
            return True
        if priority not in (Priority.QUERY, Priority.INTERACTIVE):
            return True
        if pool_id == s.interactive_pool_id:
            return True

        waited = s.tick - int(s.pipeline_first_seen_tick.get(pipeline.pipeline_id, s.tick))
        if waited >= int(s.hp_spillover_ticks):
            return True

        # If interactive pool has effectively no room, spill immediately
        if interactive_pool_avail_cpu <= 0 or interactive_pool_avail_ram <= 0:
            return True

        return False

    def pop_next_candidate_for_pool(pool_id, local_avail_cpu, local_avail_ram):
        """Find the next (pipeline, op, priority) candidate for this pool, rotating queues to avoid HOL blocking."""
        # We will rotate within each priority queue to find a runnable pipeline.
        for pr in _prio_order():
            q = s.waiting_queues[pr]
            if not q:
                continue

            # Try up to current queue length to avoid infinite loops
            n = len(q)
            for _ in range(n):
                p = q.pop(0)
                status = p.runtime_status()

                # Drop completed pipelines
                if _pipeline_is_done(status):
                    continue

                # Drop pipelines with non-retryable failures
                if _pipeline_has_unretryable_failure(s, p):
                    continue

                # Avoid running multiple ops from same pipeline in one tick (reduces dominance)
                if p.pipeline_id in assigned_pipeline_ids:
                    q.append(p)
                    continue

                # Placement preference/spillover
                if not should_allow_hp_on_pool(pr, p, pool_id):
                    q.append(p)
                    continue

                op = _get_assignable_op(p)
                if op is None:
                    # Not ready; rotate to back to reduce head-of-line blocking
                    q.append(p)
                    continue

                # For batch, enforce headroom reservation if HP waiting
                if pr == Priority.BATCH_PIPELINE and _hp_waiting_nonempty(s):
                    r_cpu_frac, r_ram_frac = _compute_batch_reserve_fracs(s, pool_id)
                    reserve_cpu = float(s.executor.pools[pool_id].max_cpu_pool) * float(r_cpu_frac)
                    reserve_ram = float(s.executor.pools[pool_id].max_ram_pool) * float(r_ram_frac)
                    if (local_avail_cpu - reserve_cpu) < 1.0 or (local_avail_ram - reserve_ram) < 1.0:
                        # Not enough headroom to safely run batch; keep it queued
                        q.append(p)
                        continue

                # Candidate accepted: requeue pipeline for future ops and return op
                q.append(p)
                return p, op, pr

        return None, None, None

    # Schedule per pool, interactive pool first
    for pool_id in _pool_iteration_order(s):
        pool = s.executor.pools[pool_id]
        local_cpu = float(pool.avail_cpu_pool)
        local_ram = float(pool.avail_ram_pool)
        if local_cpu <= 0 or local_ram <= 0:
            continue

        # Assign as many ops as we can within local availability
        while local_cpu > 0 and local_ram > 0:
            p, op, pr = pop_next_candidate_for_pool(pool_id, local_cpu, local_ram)
            if p is None:
                break

            # Compute request based on pool max, queue pressure, and OOM hints
            req_cpu, req_ram = _size_request(s, pool, pr, p.pipeline_id, op, hp_pressure)

            # If batch with HP waiting, cap batch to (local - reserve) to preserve headroom
            if pr == Priority.BATCH_PIPELINE and _hp_waiting_nonempty(s):
                r_cpu_frac, r_ram_frac = _compute_batch_reserve_fracs(s, pool_id)
                reserve_cpu = float(pool.max_cpu_pool) * float(r_cpu_frac)
                reserve_ram = float(pool.max_ram_pool) * float(r_ram_frac)
                req_cpu = min(req_cpu, max(1.0, local_cpu - reserve_cpu))
                req_ram = min(req_ram, max(1.0, local_ram - reserve_ram))

            # Fit to local available
            req_cpu = min(req_cpu, local_cpu)
            req_ram = min(req_ram, local_ram)

            # Must still be positive
            if req_cpu < 1.0 or req_ram < 1.0:
                break

            # Record mapping so we can interpret future ExecutionResults
            s.op_to_pipeline[id(op)] = p.pipeline_id

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=pr,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Decrement local availability so we don't oversubscribe within this tick
            local_cpu -= req_cpu
            local_ram -= req_ram

            assigned_pipeline_ids.add(p.pipeline_id)

    return suspensions, assignments