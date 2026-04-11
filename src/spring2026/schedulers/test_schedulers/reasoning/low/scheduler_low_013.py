# policy_key: scheduler_low_013
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.057187
# generation_seconds: 49.04
# generated_at: 2026-04-09T21:12:14.265997
@register_scheduler_init(key="scheduler_low_013")
def scheduler_low_013_init(s):
    """
    Priority-aware, failure-averse scheduler tuned for low weighted-latency score.

    Core ideas:
      1) Priority queues (QUERY > INTERACTIVE > BATCH) with gentle aging to avoid starvation.
      2) Conservative resource sizing to reduce OOM failures:
         - Start with modest RAM/CPU.
         - On failure (esp. OOM-like), increase RAM guess for that operator and retry.
      3) Soft reservations: batch work cannot consume the last slice of pool capacity, preserving
         headroom for queries/interactive that dominate the objective.
      4) Simple placement: try to pack onto the pool with most remaining headroom that can fit.
    """
    from collections import deque

    # FIFO queues by priority; we re-enqueue pipelines until completion.
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track when a pipeline first appeared to implement aging and avoid starvation.
    s.pipeline_first_seen_ts = {}  # pipeline_id -> sim_time_seen

    # Per-(pipeline_id, op_key) resource guesses; updated on failures.
    # op_key is best-effort stable key derived from operator object.
    s.op_ram_guess = {}  # (pipeline_id, op_key) -> ram
    s.op_cpu_guess = {}  # (pipeline_id, op_key) -> cpu

    # Retry budgeting to avoid infinite thrash while still preferring completion.
    s.op_fail_count = {}  # (pipeline_id, op_key) -> count

    # Soft reservation fractions (per pool). Batch can only use capacity beyond these.
    s.reserve_cpu_frac = 0.20
    s.reserve_ram_frac = 0.20

    # Conservative baseline sizing defaults (fraction of pool max).
    s.default_cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.45,
    }
    s.default_ram_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.55,
        Priority.BATCH_PIPELINE: 0.45,
    }

    # Caps to prevent a single op from consuming the entire pool (helps tail latency).
    s.max_cpu_frac = {
        Priority.QUERY: 0.90,
        Priority.INTERACTIVE: 0.80,
        Priority.BATCH_PIPELINE: 0.65,
    }
    s.max_ram_frac = {
        Priority.QUERY: 0.90,
        Priority.INTERACTIVE: 0.85,
        Priority.BATCH_PIPELINE: 0.75,
    }

    # If we observe repeated failures, we become more conservative and ramp RAM faster.
    s.ram_backoff_multipliers = [1.0, 1.4, 2.0, 2.8, 3.6]  # index by fail count (clamped)

    # Optional sim-time hook; many simulators expose s.executor.now or s.now; we handle both.
    s._last_known_time = 0.0


@register_scheduler(key="scheduler_low_013")
def scheduler_low_013_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into priority queues.
      - Process execution results to update RAM/CPU guesses (especially on failures) and requeue.
      - For each pool, assign at most one ready operator (keeps contention low and avoids OOMs).
      - Respect soft reservations for batch so high-priority work has headroom upon arrival.
    """
    from collections import deque

    def _now():
        # Best-effort access to simulated time (optional).
        if hasattr(s.executor, "now"):
            return float(s.executor.now)
        if hasattr(s, "now"):
            return float(s.now)
        # Fall back to monotonically increasing tick counter.
        s._last_known_time = float(getattr(s, "_last_known_time", 0.0)) + 1.0
        return s._last_known_time

    def _priority_queue(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(op):
        # Create a stable-ish key without importing anything.
        # Prefer explicit fields if present, otherwise use repr/type+id fallback.
        if hasattr(op, "op_id"):
            return f"op_id:{op.op_id}"
        if hasattr(op, "operator_id"):
            return f"operator_id:{op.operator_id}"
        if hasattr(op, "name"):
            return f"name:{op.name}"
        # repr(op) can include memory addresses; combine with type for some stability.
        return f"type:{type(op).__name__}|repr:{repr(op)}"

    def _is_oom_error(err):
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)

    def _clamp(x, lo, hi):
        return lo if x < lo else hi if x > hi else x

    def _pool_reserve(pool, priority):
        # Soft reserve for high-priority protection:
        # - For batch: keep reserve_cpu/reserve_ram unallocated if possible.
        # - For query/interactive: no reservation applies (they can consume into the reserve).
        if priority == Priority.BATCH_PIPELINE:
            return (pool.max_cpu_pool * s.reserve_cpu_frac, pool.max_ram_pool * s.reserve_ram_frac)
        return (0.0, 0.0)

    def _size_for_op(pool, pipeline, op):
        pri = pipeline.priority
        pid = pipeline.pipeline_id
        ok = _op_key(op)

        # Defaults scaled by pool capacity (conservative to reduce failures).
        base_cpu = max(1.0, pool.max_cpu_pool * s.default_cpu_frac.get(pri, 0.5))
        base_ram = max(1.0, pool.max_ram_pool * s.default_ram_frac.get(pri, 0.5))

        # If we have history for this op, use it.
        cpu_guess = s.op_cpu_guess.get((pid, ok), base_cpu)
        ram_guess = s.op_ram_guess.get((pid, ok), base_ram)

        # Cap per-priority to avoid monopolization.
        cpu_cap = max(1.0, pool.max_cpu_pool * s.max_cpu_frac.get(pri, 0.8))
        ram_cap = max(1.0, pool.max_ram_pool * s.max_ram_frac.get(pri, 0.85))
        cpu_guess = _clamp(cpu_guess, 1.0, cpu_cap)
        ram_guess = _clamp(ram_guess, 1.0, ram_cap)

        # Also don't request more than currently available.
        cpu_req = min(cpu_guess, pool.avail_cpu_pool)
        ram_req = min(ram_guess, pool.avail_ram_pool)

        # Ensure positive.
        cpu_req = max(1.0, cpu_req) if pool.avail_cpu_pool >= 1.0 else pool.avail_cpu_pool
        ram_req = max(1.0, ram_req) if pool.avail_ram_pool >= 1.0 else pool.avail_ram_pool

        return cpu_req, ram_req

    def _try_assign_in_pool(pool_id, pipeline, op):
        pool = s.executor.pools[pool_id]
        pri = pipeline.priority

        # Apply soft reservations for batch only.
        r_cpu, r_ram = _pool_reserve(pool, pri)
        effective_avail_cpu = pool.avail_cpu_pool - r_cpu
        effective_avail_ram = pool.avail_ram_pool - r_ram
        if pri == Priority.BATCH_PIPELINE:
            # If batch cannot fit beyond the reserve, don't schedule it in this pool.
            if effective_avail_cpu < 1.0 or effective_avail_ram < 1.0:
                return None

        # Temporarily treat pool availability as the "effective" capacity for sizing.
        class _TmpPool:
            pass

        tmp = _TmpPool()
        tmp.max_cpu_pool = pool.max_cpu_pool
        tmp.max_ram_pool = pool.max_ram_pool
        tmp.avail_cpu_pool = pool.avail_cpu_pool if pri != Priority.BATCH_PIPELINE else max(0.0, effective_avail_cpu)
        tmp.avail_ram_pool = pool.avail_ram_pool if pri != Priority.BATCH_PIPELINE else max(0.0, effective_avail_ram)

        cpu_req, ram_req = _size_for_op(tmp, pipeline, op)
        if cpu_req <= 0 or ram_req <= 0:
            return None
        if cpu_req > tmp.avail_cpu_pool + 1e-9 or ram_req > tmp.avail_ram_pool + 1e-9:
            return None

        return Assignment(
            ops=[op],
            cpu=cpu_req,
            ram=ram_req,
            priority=pipeline.priority,
            pool_id=pool_id,
            pipeline_id=pipeline.pipeline_id,
        )

    # --- Ingest new pipelines ---
    now = _now()
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.pipeline_first_seen_ts:
            s.pipeline_first_seen_ts[pid] = now
        _priority_queue(p.priority).append(p)

    # --- Process results: update resource guesses on failures and requeue pipelines ---
    # We rely on the fact that pipelines remain in our queues until completion.
    for r in results:
        if not hasattr(r, "ops") or not r.ops:
            continue
        op = r.ops[0] if isinstance(r.ops, list) else r.ops
        ok = _op_key(op)
        pid = getattr(r, "pipeline_id", None)  # may not exist; fallback below

        # If pipeline_id isn't present on results, we can only update based on op identity within queues.
        # We'll attempt to find matching pipeline by scanning queues (bounded small in simulator).
        if pid is None:
            found_pid = None
            for q in (s.q_query, s.q_interactive, s.q_batch):
                for p in q:
                    # Only update if the operator key appears among assignable/running ops.
                    st = p.runtime_status()
                    # Check both running and pending-ish operators to find the pipeline.
                    candidate_ops = []
                    try:
                        candidate_ops.extend(st.get_ops([OperatorState.RUNNING, OperatorState.ASSIGNED], require_parents_complete=False))
                    except Exception:
                        pass
                    try:
                        candidate_ops.extend(st.get_ops(ASSIGNABLE_STATES, require_parents_complete=False))
                    except Exception:
                        pass
                    if any(_op_key(x) == ok for x in candidate_ops):
                        found_pid = p.pipeline_id
                        break
                if found_pid is not None:
                    break
            pid = found_pid

        if pid is None:
            continue

        # On failure, prefer completion: bump RAM (especially on OOM-like errors), and modestly bump CPU.
        if r.failed():
            fail_k = (pid, ok)
            cnt = s.op_fail_count.get(fail_k, 0) + 1
            s.op_fail_count[fail_k] = cnt

            # Use last used resources as a baseline for the next try.
            last_ram = float(getattr(r, "ram", 0.0) or 0.0)
            last_cpu = float(getattr(r, "cpu", 0.0) or 0.0)

            # Aggressive RAM backoff on OOM-like failures; otherwise mild increase.
            oom = _is_oom_error(getattr(r, "error", None))
            idx = cnt if cnt < len(s.ram_backoff_multipliers) else (len(s.ram_backoff_multipliers) - 1)
            mult = s.ram_backoff_multipliers[idx]
            if oom:
                next_ram = max(last_ram * mult, last_ram + 1.0)
            else:
                next_ram = max(last_ram * 1.15, last_ram + 1.0)

            # CPU: small bump to reduce long runtimes for high-priority ops, but avoid over-alloc.
            if r.priority == Priority.QUERY:
                next_cpu = max(last_cpu * 1.10, last_cpu + 1.0)
            elif r.priority == Priority.INTERACTIVE:
                next_cpu = max(last_cpu * 1.07, last_cpu + 1.0)
            else:
                next_cpu = max(last_cpu * 1.03, last_cpu)

            s.op_ram_guess[(pid, ok)] = next_ram
            s.op_cpu_guess[(pid, ok)] = next_cpu

    # --- Early exit if nothing to do ---
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # --- Build an "aged" selection order without starving ---
    # Simple aging: every tick, if the head of a lower-priority queue is old enough, let it compete.
    def _pick_next_pipeline():
        # If all empty, return None.
        if not s.q_query and not s.q_interactive and not s.q_batch:
            return None

        # Hard preference for query, then interactive, but allow aging for batch.
        # Aging thresholds chosen to avoid starvation while protecting high priority.
        batch_age_threshold = 30.0
        interactive_age_threshold = 15.0

        # If query exists, pick it.
        if s.q_query:
            return s.q_query.popleft()

        # If interactive exists, usually pick it.
        if s.q_interactive:
            # If batch is very old, let it run occasionally.
            if s.q_batch:
                b = s.q_batch[0]
                age_b = now - s.pipeline_first_seen_ts.get(b.pipeline_id, now)
                if age_b >= batch_age_threshold:
                    return s.q_batch.popleft()
            return s.q_interactive.popleft()

        # Only batch left.
        return s.q_batch.popleft()

    # --- Scheduling loop: at most one assignment per pool per tick ---
    # We attempt to schedule high-priority first by repeatedly picking next pipeline
    # and trying to place its next ready operator onto the "best" pool.
    # If it can't fit anywhere, we requeue it.
    max_attempts = len(s.q_query) + len(s.q_interactive) + len(s.q_batch) + 16
    attempted = 0

    requeue = []

    # Track pools already assigned this tick (one op per pool per tick).
    pool_used = [False] * s.executor.num_pools

    while attempted < max_attempts and (s.q_query or s.q_interactive or s.q_batch):
        attempted += 1
        p = _pick_next_pipeline()
        if p is None:
            break

        st = p.runtime_status()
        has_failures = st.state_counts[OperatorState.FAILED] > 0
        if st.is_pipeline_successful():
            # Completed: drop from scheduling.
            continue

        # We do not drop on failures; we keep retrying (completion heavily penalizes failures).
        # But if the runtime marks the pipeline as irrecoverably failed, leave it.
        # (We don't have a direct signal; keep it unless there are no assignable ops.)
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            # Nothing to schedule right now: requeue to check later.
            requeue.append(p)
            continue

        # Schedule one operator at a time to reduce interference.
        op = ops[0]

        # Choose best pool: most headroom that can fit (prefer leaving room in other pools).
        best = None
        best_score = None
        for pool_id in range(s.executor.num_pools):
            if pool_used[pool_id]:
                continue
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                continue

            a = _try_assign_in_pool(pool_id, p, op)
            if a is None:
                continue

            # Score: prioritize placing high-priority in the roomiest pool to reduce tail contention.
            # For batch, prefer the tightest fit that still works (packing), preserving room elsewhere.
            cpu_left = pool.avail_cpu_pool - a.cpu
            ram_left = pool.avail_ram_pool - a.ram
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                score = (cpu_left + 0.5 * ram_left)  # larger remaining is better
                # maximize score
                if best is None or score > best_score:
                    best, best_score = a, score
            else:
                score = (abs(cpu_left) + 0.5 * abs(ram_left))  # smaller leftover is better (packing)
                # minimize score
                if best is None or score < best_score:
                    best, best_score = a, score

        if best is None:
            # Could not place now; requeue (do not drop).
            requeue.append(p)
            continue

        assignments.append(best)
        pool_used[best.pool_id] = True
        requeue.append(p)  # keep pipeline in queue for subsequent operators

        # If we've already filled all pools this tick, stop.
        if all(pool_used) or len(assignments) >= s.executor.num_pools:
            break

    # Requeue remaining pipelines (including those not popped yet).
    # Note: we already popped from queues; now restore.
    for p in requeue:
        _priority_queue(p.priority).append(p)

    return suspensions, assignments
