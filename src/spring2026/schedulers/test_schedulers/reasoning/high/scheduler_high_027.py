# policy_key: scheduler_high_027
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.194681
# generation_seconds: 156.35
# generated_at: 2026-04-10T02:05:15.034870
@register_scheduler_init(key="scheduler_high_027")
def scheduler_high_027_init(s):
    """Priority-aware, failure-averse scheduler.

    Main ideas:
      - Strictly prioritize QUERY then INTERACTIVE, but always provide some service to BATCH to avoid
        incomplete-pipeline penalties.
      - Use conservative, pool-fraction-based initial sizing; learn per-operator RAM needs from OOM
        failures and ramp RAM aggressively to avoid repeated failures.
      - Avoid preemption churn (no reliable access to running set here); instead, improve completion
        by retrying failed ops with larger RAM and using mild aging to prevent starvation.
    """
    from collections import deque

    # Per-priority pipeline queues (round-robin within each class).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Logical scheduler tick (not necessarily seconds; used only for aging heuristics).
    s.now = 0

    # Pipeline arrival tick for aging / anti-starvation.
    s.pipeline_arrival_tick = {}

    # Per-operator RAM hints learned from outcomes:
    # - on success: hint >= ram_used
    # - on OOM/failure: hint >= 2 * ram_used (next attempt should be bigger)
    s.op_ram_hint = {}       # key -> float
    s.op_fail_count = {}     # key -> int (for aggressive ramp / speculative scheduling)

    # Heuristic knobs (kept simple / conservative).
    s.batch_aging_threshold = 250          # after this many ticks, batch gets treated as interactive for selection
    s.speculative_age_threshold = 600      # after this many ticks, may schedule even if below RAM hint
    s.max_fail_ramp_hint_mult = 32.0       # cap RAM hint at max_ram_pool via sizing
    s.max_assignments_per_pool_per_tick = 8


def _sched027_is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)


def _sched027_op_key(op):
    # Try to use a stable identifier if present; otherwise fall back to repr().
    # (Avoid using id(op) because objects may be reconstructed across ticks.)
    for attr in ("op_id", "operator_id", "name", "id"):
        try:
            v = getattr(op, attr, None)
            if v is not None:
                return (attr, str(v))
        except Exception:
            pass
    return ("repr", repr(op))


def _sched027_queue_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _sched027_effective_class(s, pipeline) -> str:
    # Used only for selection; we do not physically move pipelines across queues.
    prio = pipeline.priority
    if prio == Priority.BATCH_PIPELINE:
        age = s.now - s.pipeline_arrival_tick.get(pipeline.pipeline_id, s.now)
        if age >= s.batch_aging_threshold:
            return "interactive_like"
    if prio == Priority.QUERY:
        return "query"
    if prio == Priority.INTERACTIVE:
        return "interactive"
    return "batch"


def _sched027_peek_next_runnable(s, q, scheduled_pids, predicate=None):
    """Round-robin scan of a deque to find a runnable (ready) operator.

    Returns:
        (pipeline, [op]) or (None, None)
    """
    n = len(q)
    for _ in range(n):
        p = q[0]
        q.rotate(-1)  # move p to the end for RR fairness

        if predicate is not None and not predicate(p):
            continue

        pid = p.pipeline_id
        if pid in scheduled_pids:
            continue

        status = p.runtime_status()
        if status.is_pipeline_successful():
            # p is currently at the end (after rotate); remove it.
            try:
                if q and q[-1] is p:
                    q.pop()
            except Exception:
                pass
            continue

        # Keep retrying failed ops (ASSIGNABLE_STATES includes FAILED).
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            continue

        return p, op_list

    return None, None


def _sched027_size_for(s, pool, pipeline, op):
    """Compute (cpu, ram) for an operator on a given pool using conservative heuristics + learned hints."""
    prio = pipeline.priority
    pid = pipeline.pipeline_id
    age = s.now - s.pipeline_arrival_tick.get(pid, s.now)

    max_cpu = pool.max_cpu_pool
    max_ram = pool.max_ram_pool

    # Base fractions: conservative to reduce OOMs but allow concurrency.
    # (Queries get more to reduce tail; batch gets less to avoid blocking.)
    if prio == Priority.QUERY:
        base_cpu_frac, base_ram_frac = 0.60, 0.25
    elif prio == Priority.INTERACTIVE:
        base_cpu_frac, base_ram_frac = 0.45, 0.20
    else:
        base_cpu_frac, base_ram_frac = 0.25, 0.12

    # Aging: slightly increase batch sizing if it has waited long (helps completion).
    if prio == Priority.BATCH_PIPELINE and age >= s.batch_aging_threshold:
        base_cpu_frac = max(base_cpu_frac, 0.35)
        base_ram_frac = max(base_ram_frac, 0.16)

    opk = _sched027_op_key(op)
    hint = float(s.op_ram_hint.get(opk, 0.0))
    fail_cnt = int(s.op_fail_count.get(opk, 0))

    # If we've seen repeated failures, bias RAM up (usually indicates OOM threshold).
    # The hint already captures ramps from failures; this is a small extra push.
    if fail_cnt >= 2 and hint > 0:
        hint = min(max_ram, hint * 1.25)

    ideal_cpu = max(1.0, base_cpu_frac * float(max_cpu))
    ideal_ram = max(1.0, base_ram_frac * float(max_ram))
    if hint > 0:
        ideal_ram = max(ideal_ram, hint)

    # Very old pipelines: if we do schedule them, prefer to scale up to finish faster.
    if age >= s.speculative_age_threshold:
        ideal_cpu = max(ideal_cpu, 0.80 * float(max_cpu))
        ideal_ram = max(ideal_ram, 0.35 * float(max_ram))

    # Clamp to pool maxima; caller will clamp to pool remaining availability.
    ideal_cpu = min(float(max_cpu), ideal_cpu)
    ideal_ram = min(float(max_ram), ideal_ram)

    # Use integers for cpu/ram if the simulator expects discrete units; keep >= 1.
    cpu = int(max(1, round(ideal_cpu)))
    ram = float(ideal_ram)
    return cpu, ram, hint, age


@register_scheduler(key="scheduler_high_027")
def scheduler_high_027(s, results, pipelines):
    """Priority-aware RR with OOM-aware RAM ramp and anti-starvation service for batch."""
    s.now += 1

    # Incorporate new arrivals.
    for p in pipelines:
        if p.pipeline_id not in s.pipeline_arrival_tick:
            s.pipeline_arrival_tick[p.pipeline_id] = s.now
        _sched027_queue_for_priority(s, p.priority).append(p)

    # Learn from results: ramp RAM hints on failures (especially OOM), and record successes.
    for r in results:
        try:
            ops = list(getattr(r, "ops", []) or [])
        except Exception:
            ops = []
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = bool(getattr(r, "error", None))

        for op in ops:
            opk = _sched027_op_key(op)
            prev_hint = float(s.op_ram_hint.get(opk, 0.0))
            prev_fail = int(s.op_fail_count.get(opk, 0))

            if failed:
                # Treat failures as likely resource-related; ramp aggressively to reduce repeat failures.
                s.op_fail_count[opk] = prev_fail + 1

                used_ram = float(getattr(r, "ram", 0.0) or 0.0)
                # On OOM, double; on other failures, still increase but a bit less aggressive.
                if _sched027_is_oom_error(getattr(r, "error", None)):
                    next_hint = used_ram * 2.0 if used_ram > 0 else prev_hint * 2.0
                else:
                    next_hint = used_ram * 1.5 if used_ram > 0 else prev_hint * 1.5

                # Cap via pool max at sizing time; keep a local cap multiplier to avoid runaway.
                if prev_hint > 0:
                    next_hint = max(next_hint, prev_hint * 1.25)
                s.op_ram_hint[opk] = max(prev_hint, float(next_hint))
            else:
                # Success: record that this RAM was sufficient; gently reduce fail count.
                used_ram = float(getattr(r, "ram", 0.0) or 0.0)
                if used_ram > 0:
                    s.op_ram_hint[opk] = max(prev_hint, used_ram)
                if prev_fail > 0:
                    s.op_fail_count[opk] = max(0, prev_fail - 1)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Per-tick book-keeping: avoid scheduling multiple ops from same pipeline in the same tick.
    scheduled_pids = set()

    num_pools = s.executor.num_pools

    # Pool service patterns:
    # - If multiple pools: pool 0 biased to high-priority; other pools biased to batch.
    # - If single pool: weighted round-robin among classes to keep batch making progress.
    if num_pools > 1:
        patterns = {}
        for pool_id in range(num_pools):
            if pool_id == 0:
                patterns[pool_id] = ["query", "query", "interactive", "query", "batch"]
            else:
                patterns[pool_id] = ["batch", "batch", "interactive", "batch", "query"]
    else:
        patterns = {0: ["query", "interactive", "query", "batch"]}

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        remaining_cpu = int(pool.avail_cpu_pool)
        remaining_ram = float(pool.avail_ram_pool)

        if remaining_cpu <= 0 or remaining_ram <= 0:
            continue

        pat = patterns.get(pool_id, ["query", "interactive", "batch"])

        made_progress = True
        per_pool_assignments = 0

        while made_progress and per_pool_assignments < s.max_assignments_per_pool_per_tick:
            made_progress = False

            # Stop if we can't even allocate a minimal container.
            if remaining_cpu <= 0 or remaining_ram <= 0:
                break

            # Try one selection according to the pattern, then loop.
            for cls in pat:
                if remaining_cpu <= 0 or remaining_ram <= 0:
                    break

                # Map class to which queue(s) to search.
                if cls == "query":
                    q = s.q_query
                    predicate = None
                elif cls == "interactive":
                    q = s.q_interactive
                    predicate = None
                else:
                    q = s.q_batch
                    predicate = None

                # Aged batch can be treated as interactive-like when selecting interactive.
                if cls == "interactive":
                    p, op_list = _sched027_peek_next_runnable(
                        s, s.q_interactive, scheduled_pids, predicate=None
                    )
                    if p is None:
                        # Look for aged batch as a fallback (prevents long-wait batch incompletes).
                        def _aged_batch_pred(pp):
                            return _sched027_effective_class(s, pp) == "interactive_like"
                        p, op_list = _sched027_peek_next_runnable(
                            s, s.q_batch, scheduled_pids, predicate=_aged_batch_pred
                        )
                else:
                    p, op_list = _sched027_peek_next_runnable(s, q, scheduled_pids, predicate=predicate)

                if p is None or not op_list:
                    continue

                op = op_list[0]
                cpu_req, ram_req, ram_hint, age = _sched027_size_for(s, pool, p, op)

                # Clamp to remaining resources.
                cpu = min(remaining_cpu, cpu_req)

                # Enforce RAM hint if present, unless pipeline is very old (speculative scheduling).
                if ram_hint > 0 and remaining_ram < ram_hint and age < s.speculative_age_threshold:
                    # Not enough RAM to meet learned minimum; try other classes/pools.
                    continue

                ram = min(remaining_ram, ram_req)
                if ram <= 0 or cpu <= 0:
                    continue

                # If we couldn't meet hint but the pipeline is very old, schedule with what we have.
                # This risks OOM, but is better than indefinite incompletion penalties.
                if ram_hint > 0 and ram < ram_hint and age >= s.speculative_age_threshold:
                    ram = float(remaining_ram)

                # Create assignment: one op per container to keep packing flexible.
                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=int(cpu),
                        ram=float(ram),
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                scheduled_pids.add(p.pipeline_id)
                remaining_cpu -= int(cpu)
                remaining_ram -= float(ram)
                per_pool_assignments += 1
                made_progress = True
                break  # re-evaluate from pattern start with updated remaining resources

    return suspensions, assignments
