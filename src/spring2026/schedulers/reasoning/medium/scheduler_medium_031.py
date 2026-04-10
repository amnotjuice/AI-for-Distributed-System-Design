# policy_key: scheduler_medium_031
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.086965
# generation_seconds: 70.03
# generated_at: 2026-04-09T23:11:14.069112
@register_scheduler_init(key="scheduler_medium_031")
def scheduler_medium_031_init(s):
    """
    Priority + aging + OOM-aware retry scheduler.

    Goals:
      - Keep QUERY and INTERACTIVE latency low via strict priority and pool headroom protection.
      - Maintain high completion rate by retrying FAILED ops on OOM with RAM backoff (no dropping).
      - Avoid starvation via simple aging that gradually boosts older pipelines.
      - Improve over naive FIFO by (a) multi-op dispatch per tick per pool, (b) headroom reservation
        that limits BATCH from consuming the last resources needed for high priority arrivals.
    """
    # Queues per priority (round-robin within each priority)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Pipeline metadata for aging/fairness
    # pipeline_id -> {"arrival_tick": int}
    s.pipeline_meta = {}

    # OOM-adaptive RAM target and retry counters per (pipeline, op)
    # (pipeline_id, op_key) -> ram_target
    s.op_ram_target = {}
    s.op_retries = {}

    # Pipelines with non-OOM deterministic failures are quarantined (won't be scheduled further).
    # This protects high-priority latency/throughput by not burning cycles on hopeless retries.
    s.quarantined = set()

    # Discrete "time" for aging (incremented each scheduler call)
    s.tick = 0

    # Headroom reservations to protect latency for high priority work.
    # BATCH can only use resources beyond these soft reservations.
    s.reserve_cpu_frac = 0.30
    s.reserve_ram_frac = 0.30

    # Dispatch controls
    s.max_assignments_per_pool_per_tick = 6
    s.scan_limit_per_queue = 24  # how many pipelines to scan for an assignable op

    # Aging: add (waiting_ticks / age_divisor) to the base priority score
    s.age_divisor = 40.0

    # RAM sizing heuristics (units assumed consistent with simulator, typically GB-like)
    # We combine a fraction-of-pool heuristic with min/max caps to avoid oversizing.
    s.ram_frac = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.20,
        Priority.BATCH_PIPELINE: 0.15,
    }
    s.ram_min_cap = {
        Priority.QUERY: 4,
        Priority.INTERACTIVE: 3,
        Priority.BATCH_PIPELINE: 2,
    }
    s.ram_max_cap = {
        Priority.QUERY: 32,
        Priority.INTERACTIVE: 24,
        Priority.BATCH_PIPELINE: 16,
    }

    # CPU sizing heuristics: modest parallelism by default; higher for queries.
    s.cpu_target = {
        Priority.QUERY: 4,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }


@register_scheduler(key="scheduler_medium_031")
def scheduler_medium_031_scheduler(s, results, pipelines):
    """
    Each tick:
      1) Enqueue new pipelines by priority; track arrival tick for aging.
      2) Consume execution results:
           - On OOM failure: increase per-op RAM target (exponential backoff), keep retrying.
           - On non-OOM failure: quarantine pipeline to avoid repeated wasted work.
      3) For each pool, dispatch up to N assignments:
           - Prefer QUERY > INTERACTIVE > BATCH, with aging within/between classes.
           - Enforce soft headroom reservations against BATCH to protect high-priority latency.
           - Schedule one operator per assignment (reduces OOM blast radius and improves fairness).
    """
    s.tick += 1

    def _prio_base_score(priority):
        # Strong separation to keep high priority latency low; aging can still lift old batch.
        if priority == Priority.QUERY:
            return 300.0
        if priority == Priority.INTERACTIVE:
            return 150.0
        return 30.0  # batch

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            e = str(err).lower()
        except Exception:
            return False
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "alloc" in e)

    def _op_key(op):
        # Robust-ish operator key without assuming exact simulator fields.
        for attr in ("operator_id", "op_id", "task_id", "name"):
            if hasattr(op, attr):
                try:
                    val = getattr(op, attr)
                    if val is not None:
                        return f"{attr}:{val}"
                except Exception:
                    pass
        # Fallback: repr/op string (deterministic enough within a run)
        try:
            return f"repr:{repr(op)}"
        except Exception:
            return f"str:{str(op)}"

    def _cleanup_queue(q):
        # Remove completed pipelines and quarantined ones to avoid infinite growth.
        out = []
        for p in q:
            if p.pipeline_id in s.quarantined:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            out.append(p)
        return out

    def _first_assignable_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return None
        # Require parents complete; only schedule ready ops.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _ram_guess(priority, pool, pipeline_id, op):
        # Per-op adaptive target if we have one (from past OOMs).
        key = (pipeline_id, _op_key(op))
        if key in s.op_ram_target:
            return s.op_ram_target[key]

        # Otherwise use a conservative heuristic: fraction of pool with min/max caps.
        try:
            frac = float(s.ram_frac.get(priority, 0.15))
            base = int(pool.max_ram_pool * frac)
        except Exception:
            base = s.ram_min_cap.get(priority, 2)

        base = max(int(s.ram_min_cap.get(priority, 2)), base)
        base = min(int(s.ram_max_cap.get(priority, 16)), base)

        try:
            base = min(int(pool.max_ram_pool), base)
        except Exception:
            pass

        return max(1, int(base))

    def _cpu_guess(priority, pool):
        # Keep CPU modest to improve concurrency; queries get more.
        tgt = int(s.cpu_target.get(priority, 1))
        try:
            tgt = min(tgt, int(pool.max_cpu_pool))
        except Exception:
            pass
        return max(1, int(tgt))

    def _batch_allowed_in_pool(pool):
        # Enforce soft reservation so that batch doesn't consume last resources.
        reserve_cpu = int(pool.max_cpu_pool * s.reserve_cpu_frac)
        reserve_ram = int(pool.max_ram_pool * s.reserve_ram_frac)
        # Batch allowed only if we have resources beyond reserved headroom.
        return (pool.avail_cpu_pool > reserve_cpu) and (pool.avail_ram_pool > reserve_ram)

    def _pick_next_pipeline_for_pool(pool):
        # Scan a bounded window of each queue and pick the best score pipeline that has a ready op.
        best = None
        best_score = -1e18
        best_q = None
        best_idx = None
        best_op = None

        for priority in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            q = s.queues.get(priority, [])
            if not q:
                continue

            # Respect headroom reservation for batch
            if priority == Priority.BATCH_PIPELINE and not _batch_allowed_in_pool(pool):
                continue

            scan_n = min(len(q), int(s.scan_limit_per_queue))
            for idx in range(scan_n):
                p = q[idx]
                if p.pipeline_id in s.quarantined:
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                op = _first_assignable_op(p)
                if op is None:
                    continue

                meta = s.pipeline_meta.get(p.pipeline_id, {})
                arrival = meta.get("arrival_tick", s.tick)
                waiting = max(0, s.tick - int(arrival))
                score = _prio_base_score(priority) + (waiting / float(s.age_divisor))

                # Slightly prefer continuing pipelines (those that have progressed) to reduce tail.
                try:
                    completed = st.state_counts.get(OperatorState.COMPLETED, 0)
                    score += 0.02 * float(completed)
                except Exception:
                    pass

                if score > best_score:
                    best = p
                    best_score = score
                    best_q = q
                    best_idx = idx
                    best_op = op

        return best, best_q, best_idx, best_op

    # 1) Enqueue new pipelines
    for p in pipelines:
        if p.pipeline_id not in s.pipeline_meta:
            s.pipeline_meta[p.pipeline_id] = {"arrival_tick": s.tick}
        # Insert into the appropriate priority queue
        if p.priority not in s.queues:
            s.queues[p.priority] = []
        s.queues[p.priority].append(p)

    # 2) Process results (OOM backoff + quarantine on non-OOM failures)
    for r in results:
        if not r.failed():
            continue

        # If we can identify the op, apply adaptive RAM. If not, we still quarantine on non-OOM.
        pipeline_id = None
        try:
            pipeline_id = getattr(r, "pipeline_id", None)
        except Exception:
            pipeline_id = None

        # We can still infer pipeline_id from result fields? Not guaranteed; fall back to do nothing.
        # Most simulators provide it; if absent, OOM learning may be partial.
        is_oom = _is_oom_error(getattr(r, "error", None))

        if pipeline_id is not None:
            if is_oom:
                # Increase RAM target for the specific op(s) in this container.
                for op in getattr(r, "ops", []) or []:
                    k = (pipeline_id, _op_key(op))
                    prev = s.op_ram_target.get(k, max(1, int(getattr(r, "ram", 1))))
                    # Exponential backoff, bounded by pool max RAM if known.
                    new_target = max(int(prev), max(1, int(getattr(r, "ram", prev)))) * 2
                    # Apply a conservative upper bound from the pool where it ran.
                    try:
                        pool = s.executor.pools[int(getattr(r, "pool_id", 0))]
                        new_target = min(int(pool.max_ram_pool), int(new_target))
                    except Exception:
                        pass
                    s.op_ram_target[k] = max(1, int(new_target))
                    s.op_retries[k] = int(s.op_retries.get(k, 0)) + 1
            else:
                # Non-OOM failure: quarantine after the first occurrence to avoid repeated waste.
                s.quarantined.add(pipeline_id)

    # 3) Cleanup queues (remove completed/quarantined pipelines)
    for pr in list(s.queues.keys()):
        s.queues[pr] = _cleanup_queue(s.queues[pr])

    # Early exit if no state changes
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # 4) Dispatch across pools
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        try:
            avail_cpu = int(pool.avail_cpu_pool)
            avail_ram = int(pool.avail_ram_pool)
        except Exception:
            continue

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        made = 0
        while made < int(s.max_assignments_per_pool_per_tick):
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            p, q, idx, op = _pick_next_pipeline_for_pool(pool)
            if p is None:
                break

            # Decide resources for this op
            cpu = min(avail_cpu, _cpu_guess(p.priority, pool))
            ram = _ram_guess(p.priority, pool, p.pipeline_id, op)
            ram = min(avail_ram, int(ram))

            # If we can't fit even minimal resources, skip this pipeline for this tick.
            if cpu <= 0 or ram <= 0:
                break

            # If op has a learned RAM target larger than current available, we can't run it now.
            learned = s.op_ram_target.get((p.pipeline_id, _op_key(op)), None)
            if learned is not None and int(learned) > avail_ram:
                # Rotate pipeline to avoid repeated scanning head-of-line.
                try:
                    q.pop(idx)
                    q.append(p)
                except Exception:
                    pass
                # Try another candidate
                made += 0  # no-op; keep loop
                # Guard against infinite loops when nothing fits
                # Break if batch is blocked by reservation or the pool is too small
                # (we rely on subsequent ticks/pool frees).
                break

            # For batch, respect headroom by not consuming into reserved region.
            if p.priority == Priority.BATCH_PIPELINE:
                reserve_cpu = int(pool.max_cpu_pool * s.reserve_cpu_frac)
                reserve_ram = int(pool.max_ram_pool * s.reserve_ram_frac)
                max_cpu_for_batch = max(0, avail_cpu - reserve_cpu)
                max_ram_for_batch = max(0, avail_ram - reserve_ram)
                cpu = min(cpu, max_cpu_for_batch)
                ram = min(ram, max_ram_for_batch)
                if cpu <= 0 or ram <= 0:
                    # No allowable headroom for batch right now; stop dispatching batch in this pool.
                    break

            assignment = Assignment(
                ops=[op],
                cpu=int(cpu),
                ram=int(ram),
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Update local avail for further packing this tick
            avail_cpu -= int(cpu)
            avail_ram -= int(ram)
            made += 1

            # Round-robin rotation: move scheduled pipeline to back of its queue
            try:
                q.pop(idx)
                q.append(p)
            except Exception:
                pass

    return suspensions, assignments
