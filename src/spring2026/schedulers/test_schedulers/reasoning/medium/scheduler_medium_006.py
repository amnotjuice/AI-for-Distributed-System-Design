# policy_key: scheduler_medium_006
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.085761
# generation_seconds: 69.59
# generated_at: 2026-04-09T22:35:48.656656
@register_scheduler_init(key="scheduler_medium_006")
def scheduler_medium_006_init(s):
    """
    Priority-aware, OOM-adaptive, reservation-based scheduler.

    Main ideas:
      - Strict preference: QUERY > INTERACTIVE > BATCH (to optimize weighted latency).
      - Keep headroom when high-priority work exists (CPU/RAM reservation) to reduce queueing delay.
      - OOM-adaptive retries: if an op fails with OOM, retry with increased RAM (capped by pool max).
      - Gentle fairness: round-robin within each priority; allow some batch only when headroom remains.
      - Avoid excessive parallelism per pipeline per tick to reduce interference and churn.
    """
    s.tick = 0

    # Per-priority FIFO queues of pipelines
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Op-level resource learning
    # op_key -> {"ram": float, "ooms": int, "fails": int}
    s.op_stats = {}

    # Conservative defaults / caps
    s.max_total_retries_per_op = 4        # total retries (OOM + non-OOM)
    s.max_non_oom_retries_per_op = 1      # keep low to avoid wasting time on irrecoverable failures


@register_scheduler(key="scheduler_medium_006")
def scheduler_medium_006_scheduler(s, results, pipelines):
    def _prio_bucket(priority):
        if priority == Priority.QUERY:
            return "query"
        if priority == Priority.INTERACTIVE:
            return "interactive"
        return "batch"

    def _enqueue_pipeline(p):
        b = _prio_bucket(p.priority)
        if b == "query":
            s.q_query.append(p)
        elif b == "interactive":
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _op_key(pipeline_id, op):
        # Try stable identifiers if present; fall back to object id.
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = getattr(op, "name", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    def _is_oom_error(err):
        if err is None:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("cuda out of memory" in e) or ("memoryerror" in e)

    def _get_min_ram_hint(op):
        # If the simulator exposes min RAM on the operator, honor it.
        for attr in ("min_ram", "min_ram_gb", "ram_min", "min_memory", "memory_min"):
            v = getattr(op, attr, None)
            if v is not None:
                return float(v)
        return 0.0

    def _priority_cpu_frac(priority):
        # Bias CPU to reduce tail latency for high priorities.
        if priority == Priority.QUERY:
            return 0.75
        if priority == Priority.INTERACTIVE:
            return 0.55
        return 0.40

    def _priority_ram_frac(priority):
        # Over-allocate RAM (relative to pool) to avoid OOM penalties; high priority gets more.
        if priority == Priority.QUERY:
            return 0.70
        if priority == Priority.INTERACTIVE:
            return 0.55
        return 0.40

    def _reservation_fracs():
        # Keep headroom to admit new query/interactive quickly.
        # (Only applied when there exists runnable query/interactive work.)
        return 0.25, 0.25  # (cpu_reserve_frac, ram_reserve_frac)

    def _clean_and_has_runnable(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return False, False

        # If there are runnable ops (parents complete), we consider it runnable.
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return True, (len(ops) > 0)

    def _pick_runnable_from_queue(q, scheduled_pipelines, max_scan=None):
        """
        Round-robin scan: find a pipeline with a runnable op.
        Rotate non-runnable pipelines to the back to avoid head-of-line blocking.
        """
        if not q:
            return None, None

        n = len(q)
        if max_scan is None:
            max_scan = n

        scanned = 0
        while q and scanned < max_scan:
            p = q.pop(0)

            alive, has_runnable = _clean_and_has_runnable(p)
            if not alive:
                scanned += 1
                continue

            # Avoid scheduling too many ops from the same pipeline in one tick.
            if p.pipeline_id in scheduled_pipelines:
                q.append(p)
                scanned += 1
                continue

            if not has_runnable:
                q.append(p)
                scanned += 1
                continue

            op = p.runtime_status().get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[0]
            # Put the pipeline back for future ops; we do RR fairness within the queue.
            q.append(p)
            return p, op

        return None, None

    def _ram_request(pool, p, op):
        key = _op_key(p.pipeline_id, op)
        st = s.op_stats.get(key, {"ram": None, "ooms": 0, "fails": 0})

        # Start from learned RAM if available; otherwise use a conservative fraction of pool.
        base = st["ram"]
        if base is None:
            base = _priority_ram_frac(p.priority) * float(pool.max_ram_pool)

        # Respect any min-RAM hint if present.
        base = max(base, _get_min_ram_hint(op))

        # Add a small safety margin (helps reduce repeat OOMs from slight underestimation).
        base = base * 1.10

        # Clamp to feasible.
        base = min(base, float(pool.max_ram_pool))
        base = min(base, float(pool.avail_ram_pool))

        # Ensure non-zero allocations when possible.
        if base <= 0 and pool.avail_ram_pool > 0:
            base = min(1.0, float(pool.avail_ram_pool))

        return base

    def _cpu_request(pool, p):
        target = _priority_cpu_frac(p.priority) * float(pool.max_cpu_pool)
        target = min(target, float(pool.avail_cpu_pool))
        # Ensure at least 1 CPU if any CPU is available (helps latency and avoids 0 allocations).
        if target < 1.0 and pool.avail_cpu_pool >= 1.0:
            target = 1.0
        elif target <= 0 and pool.avail_cpu_pool > 0:
            target = float(pool.avail_cpu_pool)
        return target

    # ---- Ingest new pipelines ----
    for p in pipelines:
        _enqueue_pipeline(p)

    # ---- Learn from results (especially OOM) ----
    for r in results:
        if not r.failed():
            continue

        # Update per-op stats; bias toward increasing RAM aggressively on OOM to avoid 720s penalty.
        pool = s.executor.pools[r.pool_id]
        oom = _is_oom_error(r.error)

        for op in (r.ops or []):
            key = _op_key(getattr(r, "pipeline_id", None) or getattr(op, "pipeline_id", None) or "unknown", op)
            st = s.op_stats.get(key, {"ram": None, "ooms": 0, "fails": 0})

            st["fails"] = int(st.get("fails", 0)) + 1
            if oom:
                st["ooms"] = int(st.get("ooms", 0)) + 1

                # If we OOM'd at r.ram, try doubling; otherwise bump a lot.
                prev = st["ram"]
                attempted = float(getattr(r, "ram", 0.0) or 0.0)
                if prev is None:
                    prev = attempted if attempted > 0 else (_priority_ram_frac(r.priority) * float(pool.max_ram_pool))

                # Exponential backoff on OOM to converge quickly.
                new_ram = max(prev * 2.0, attempted * 2.0, prev + 1.0)
                new_ram = min(new_ram, float(pool.max_ram_pool))
                st["ram"] = new_ram
            else:
                # Non-OOM: do not inflate RAM blindly; keep last estimate (if any).
                if st["ram"] is None and getattr(r, "ram", None) is not None:
                    st["ram"] = float(r.ram)

            s.op_stats[key] = st

    s.tick += 1

    # ---- Scheduling ----
    suspensions = []
    assignments = []

    # Light per-tick cap: avoid over-scheduling a single pipeline (reduces interference and contention).
    scheduled_pipelines = set()

    # Determine if there's runnable high-priority work to justify reserving headroom.
    def _any_runnable_high():
        # Scan a small number from each queue (cheap approximation).
        for q in (s.q_query, s.q_interactive):
            max_scan = min(len(q), 8)
            for i in range(max_scan):
                p = q[i]
                alive, has_runnable = _clean_and_has_runnable(p)
                if alive and has_runnable:
                    return True
        return False

    reserve_cpu_frac, reserve_ram_frac = _reservation_fracs()
    reserve_for_high = _any_runnable_high()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # If no capacity, skip.
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Compute per-pool reservations if high-priority work exists anywhere.
        cpu_reserve = reserve_cpu_frac * float(pool.max_cpu_pool) if reserve_for_high else 0.0
        ram_reserve = reserve_ram_frac * float(pool.max_ram_pool) if reserve_for_high else 0.0

        # Allow multiple placements per pool per tick while capacity exists.
        # Keep this modest to reduce churn and keep behavior predictable.
        per_pool_budget = 6
        made = 0

        while made < per_pool_budget and pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
            # Try to schedule QUERY, then INTERACTIVE, then BATCH (only if headroom remains).
            p, op = _pick_runnable_from_queue(s.q_query, scheduled_pipelines)
            if p is None:
                p, op = _pick_runnable_from_queue(s.q_interactive, scheduled_pipelines)

            chosen_is_batch = False
            if p is None:
                # Only schedule batch if we still maintain reserved headroom (when applicable).
                eff_avail_cpu_for_batch = float(pool.avail_cpu_pool) - cpu_reserve
                eff_avail_ram_for_batch = float(pool.avail_ram_pool) - ram_reserve
                if eff_avail_cpu_for_batch > 0 and eff_avail_ram_for_batch > 0:
                    p, op = _pick_runnable_from_queue(s.q_batch, scheduled_pipelines)
                    chosen_is_batch = (p is not None)
                else:
                    break  # preserve headroom; don't start batch now

            if p is None or op is None:
                break

            # Retry-limiting per op to avoid infinite loops on irrecoverable failures.
            key = _op_key(p.pipeline_id, op)
            st = s.op_stats.get(key, {"ram": None, "ooms": 0, "fails": 0})
            total_fails = int(st.get("fails", 0))
            non_oom_fails = max(0, total_fails - int(st.get("ooms", 0)))

            if total_fails >= s.max_total_retries_per_op or non_oom_fails > s.max_non_oom_retries_per_op:
                # Skip this pipeline for now (effectively deprioritizes stuck ops without dropping all work).
                # Rotate it back via queue append already done in picker; just mark as "scheduled" to avoid thrash.
                scheduled_pipelines.add(p.pipeline_id)
                made += 1
                continue

            # Compute resource request.
            req_cpu = _cpu_request(pool, p)

            # If batch, also respect effective availability under reservations.
            if chosen_is_batch and reserve_for_high:
                req_cpu = min(req_cpu, max(0.0, float(pool.avail_cpu_pool) - cpu_reserve))
                if req_cpu < 1.0 and (float(pool.avail_cpu_pool) - cpu_reserve) >= 1.0:
                    req_cpu = 1.0

            if req_cpu <= 0:
                break

            req_ram = _ram_request(pool, p, op)
            if chosen_is_batch and reserve_for_high:
                req_ram = min(req_ram, max(0.0, float(pool.avail_ram_pool) - ram_reserve))
                if req_ram <= 0:
                    break

            if req_ram <= 0:
                break

            # Assign exactly one op per container to keep accounting simple and avoid batching-induced tail latency.
            assignment = Assignment(
                ops=[op],
                cpu=req_cpu,
                ram=req_ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Update local available (pool.avail_* will reflect next tick, so decrement here to pack multiple).
            pool.avail_cpu_pool -= req_cpu
            pool.avail_ram_pool -= req_ram

            scheduled_pipelines.add(p.pipeline_id)
            made += 1

    return suspensions, assignments
