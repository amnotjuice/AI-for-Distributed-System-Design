# policy_key: scheduler_low_026
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.058489
# generation_seconds: 54.62
# generated_at: 2026-04-09T21:23:17.353055
@register_scheduler_init(key="scheduler_low_026")
def scheduler_low_026_init(s):
    """
    Priority-weighted, failure-averse scheduler.

    Goals:
      - Minimize weighted latency (query>interactive>batch) by always selecting ready work
        from higher priorities first, while ensuring batch still makes progress (aging).
      - Avoid the heavy 720s penalty by aggressively retrying failed operators with
        increased RAM (and modest CPU), rather than dropping pipelines.
      - Use conservative resource sizing (not "all available") to reduce interference and
        keep headroom for high-priority arrivals.
      - Simple multi-pool affinity when multiple pools exist (queries prefer pool 0).

    Key mechanisms:
      - Priority + aging ordering over pipelines with assignable ready ops.
      - Per-(pipeline, op) RAM backoff on failures; bounded attempts to avoid infinite churn.
      - Headroom reservation: if high-priority work is waiting, limit how much batch can take.
    """
    s.waiting_pipelines = []          # type: List[Pipeline]
    s.tick = 0                        # logical time for aging
    s.pipeline_meta = {}              # pipeline_id -> dict(enqueue_tick=int, last_pick_tick=int)
    s.op_overrides = {}               # (pipeline_id, op_key) -> dict(ram=float, cpu=float, attempts=int)
    s.max_attempts_per_op = 8         # keep trying to avoid 720s penalty, but prevent infinite loops


def _prio_weight(priority):
    if priority == Priority.QUERY:
        return 10
    if priority == Priority.INTERACTIVE:
        return 5
    return 1


def _pool_affinity_order(num_pools, priority):
    # Light affinity only; we still allow fallback to any pool.
    if num_pools <= 1:
        return [0]
    if priority == Priority.QUERY:
        return list(range(num_pools))  # prefer 0 naturally by ordering below
    if priority == Priority.INTERACTIVE:
        # Prefer pool 1 if it exists, else 0.
        order = [1, 0] + list(range(2, num_pools))
        return [p for p in order if 0 <= p < num_pools]
    # Batch: prefer later pools to reduce interference with interactive/query.
    return list(range(num_pools - 1, -1, -1))


def _op_key(op):
    # We may not have stable ids on Operator objects; use repr as a best-effort stable key.
    try:
        return op.operator_id  # type: ignore[attr-defined]
    except Exception:
        try:
            return op.op_id  # type: ignore[attr-defined]
        except Exception:
            return repr(op)


def _clamp(x, lo, hi):
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _default_frac(priority):
    # Conservative defaults: keep headroom for future high-priority arrivals.
    if priority == Priority.QUERY:
        return 0.60
    if priority == Priority.INTERACTIVE:
        return 0.45
    return 0.25


def _default_ram_frac(priority):
    # RAM is OOM-critical; give slightly more to reduce failure probability.
    if priority == Priority.QUERY:
        return 0.65
    if priority == Priority.INTERACTIVE:
        return 0.50
    return 0.30


def _is_high_prio(priority):
    return priority in (Priority.QUERY, Priority.INTERACTIVE)


@register_scheduler(key="scheduler_low_026")
def scheduler_low_026(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Tick-based scheduler:
      1) Ingest new pipelines into a waiting set.
      2) Learn from failures: increase RAM (and slightly CPU) for the failed operator(s).
      3) Build a list of candidate pipelines with ready operators.
      4) For each pool, assign at most a small number of ops per tick using:
           - priority weight
           - aging (time since enqueue / last scheduled)
           - headroom reservation when high-priority work exists
    """
    s.tick += 1

    # Ingest newly arrived pipelines.
    for p in pipelines:
        s.waiting_pipelines.append(p)
        if p.pipeline_id not in s.pipeline_meta:
            s.pipeline_meta[p.pipeline_id] = {"enqueue_tick": s.tick, "last_pick_tick": 0}

    # Learn from execution results (especially failures).
    # We treat any failure as a signal to increase RAM (OOM is common and expensive).
    for r in results:
        if not r.failed():
            continue
        # Update overrides for each op in the result.
        for op in getattr(r, "ops", []) or []:
            k = (getattr(r, "pipeline_id", None), _op_key(op))  # pipeline_id may not exist on result
            # If pipeline_id isn't carried, fall back to None-scoped override (still helps within tick).
            if k[0] is None:
                k = ("__unknown_pipeline__", k[1])

            ov = s.op_overrides.get(k, {"ram": None, "cpu": None, "attempts": 0})
            ov["attempts"] = int(ov.get("attempts", 0)) + 1

            # Increase RAM aggressively; cap later by pool max at assignment time.
            prev_ram = ov.get("ram")
            base_ram = r.ram if getattr(r, "ram", None) is not None else 0
            if prev_ram is None:
                new_ram = max(base_ram * 2.0, base_ram + 1.0)
            else:
                new_ram = max(float(prev_ram) * 1.5, float(prev_ram) + 1.0)

            # Slight CPU bump to reduce wall time once it succeeds (helps weighted latency),
            # but keep it modest to avoid starving other work.
            prev_cpu = ov.get("cpu")
            base_cpu = r.cpu if getattr(r, "cpu", None) is not None else 0
            if prev_cpu is None:
                new_cpu = max(base_cpu, 1.0)
            else:
                new_cpu = max(float(prev_cpu), float(base_cpu), 1.0)

            ov["ram"] = new_ram
            ov["cpu"] = new_cpu
            s.op_overrides[k] = ov

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # Build candidate pipelines: those not successful and with at least one ready, assignable op.
    candidates = []
    high_prio_waiting = False
    for p in list(s.waiting_pipelines):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue

        # Identify ready ops (parents completed), in assignable states.
        ready_ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ready_ops:
            continue

        if _is_high_prio(p.priority):
            high_prio_waiting = True

        meta = s.pipeline_meta.get(p.pipeline_id, {"enqueue_tick": s.tick, "last_pick_tick": 0})
        age = s.tick - int(meta.get("enqueue_tick", s.tick))
        since_last = s.tick - int(meta.get("last_pick_tick", 0))
        weight = _prio_weight(p.priority)

        # Pipeline score: priority dominates, then "since last picked" to avoid starvation,
        # then total age to steadily lift long-waiting batch.
        score = (weight * 1_000_000) + (since_last * 1_000) + age
        candidates.append((score, p, ready_ops))

    # Nothing assignable right now.
    if not candidates:
        return [], []

    # Sort once; we will pick from this list per pool with pool affinity.
    candidates.sort(key=lambda x: x[0], reverse=True)

    suspensions = []
    assignments = []

    # Per tick, avoid over-scheduling a single pipeline (keeps fairness and reduces contention).
    scheduled_pipelines_this_tick = set()

    # Helper to pick next pipeline for a given pool, honoring affinity ordering softly.
    def pick_for_pool(pool_id):
        # First pass: prefer candidates whose priority "likes" this pool.
        for score, p, ops in candidates:
            if p.pipeline_id in scheduled_pipelines_this_tick:
                continue
            if pool_id not in _pool_affinity_order(s.executor.num_pools, p.priority):
                continue
            return p, ops
        # Fallback pass: any candidate.
        for score, p, ops in candidates:
            if p.pipeline_id in scheduled_pipelines_this_tick:
                continue
            return p, ops
        return None, None

    # Assign in each pool. Keep assignments per pool modest to reduce thrash.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Reserve headroom when high-priority work exists, limiting batch consumption.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if high_prio_waiting:
            reserve_cpu = 0.20 * pool.max_cpu_pool
            reserve_ram = 0.20 * pool.max_ram_pool

        # Allow up to 2 ops per pool per tick (keeps throughput without over-committing).
        for _ in range(2):
            p, ready_ops = pick_for_pool(pool_id)
            if p is None:
                break

            # Enforce headroom: if this is batch, don't dip into reserved capacity.
            effective_avail_cpu = avail_cpu
            effective_avail_ram = avail_ram
            if high_prio_waiting and p.priority == Priority.BATCH_PIPELINE:
                effective_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                effective_avail_ram = max(0.0, avail_ram - reserve_ram)

            if effective_avail_cpu <= 0 or effective_avail_ram <= 0:
                break

            # Only schedule one ready op at a time per pipeline to reduce memory risk.
            op = ready_ops[0]
            opk = (p.pipeline_id, _op_key(op))
            ov = s.op_overrides.get(opk)

            # If attempts exceeded, deprioritize by skipping this op this tick (avoid infinite loop).
            # We do NOT drop the pipeline; we just avoid burning the tick on it repeatedly.
            if ov is not None and int(ov.get("attempts", 0)) >= s.max_attempts_per_op:
                scheduled_pipelines_this_tick.add(p.pipeline_id)
                s.pipeline_meta[p.pipeline_id]["last_pick_tick"] = s.tick
                continue

            # Compute baseline sizing.
            frac_cpu = _default_frac(p.priority)
            frac_ram = _default_ram_frac(p.priority)

            base_cpu = max(1.0, float(pool.max_cpu_pool) * frac_cpu)
            base_ram = max(1.0, float(pool.max_ram_pool) * frac_ram)

            # Apply override (from previous failure) if present.
            req_cpu = base_cpu
            req_ram = base_ram
            if ov is not None:
                if ov.get("cpu") is not None:
                    req_cpu = max(req_cpu, float(ov["cpu"]))
                if ov.get("ram") is not None:
                    req_ram = max(req_ram, float(ov["ram"]))

            # Clamp to what is effectively available (and pool max), ensuring progress.
            req_cpu = _clamp(req_cpu, 1.0, float(pool.max_cpu_pool))
            req_ram = _clamp(req_ram, 1.0, float(pool.max_ram_pool))

            # If not enough capacity, try shrinking CPU first (CPU scaling is sublinear).
            if req_ram > effective_avail_ram:
                # Can't fit; try another pool by breaking this pool's loop.
                break
            if req_cpu > effective_avail_cpu:
                req_cpu = max(1.0, effective_avail_cpu)

            if req_cpu <= 0 or req_ram <= 0:
                break

            assignment = Assignment(
                ops=[op],
                cpu=req_cpu,
                ram=req_ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Update accounting and fairness markers.
            avail_cpu -= req_cpu
            avail_ram -= req_ram
            scheduled_pipelines_this_tick.add(p.pipeline_id)
            if p.pipeline_id in s.pipeline_meta:
                s.pipeline_meta[p.pipeline_id]["last_pick_tick"] = s.tick

            # If the pool is nearly full, stop.
            if avail_cpu < 1.0 or avail_ram < 1.0:
                break

    return suspensions, assignments
