# policy_key: scheduler_medium_019
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.079265
# generation_seconds: 73.52
# generated_at: 2026-04-09T22:54:41.979918
@register_scheduler_init(key="scheduler_medium_019")
def scheduler_medium_019_init(s):
    """
    Priority-aware, OOM-adaptive, non-preemptive multi-pool scheduler.

    Main ideas:
      - Strictly prefer QUERY > INTERACTIVE > BATCH for assignment decisions.
      - Keep batch progressing via simple aging (anti-starvation), but protect high-priority
        latency by reserving headroom in the "interactive-preferred" pool (pool 0).
      - Minimize failures by learning per-operator RAM after OOM and retrying with larger RAM.
      - Limit per-assignment CPU to allow some concurrency while still accelerating queries.
    """
    # Pipeline tracking (avoid duplicates in queues)
    s.pipelines_by_id = {}          # pipeline_id -> Pipeline
    s.pipeline_meta = {}            # pipeline_id -> dict(enqueue_tick, last_scheduled_tick)

    # Separate FIFO queues per priority (store pipeline_ids)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator learned RAM estimates (keyed by op identity)
    s.op_ram_est = {}               # op_key -> ram_estimate
    s.op_fail_count = {}            # op_key -> count
    s.op_hard_fail = set()          # op_key that failed with non-OOM error repeatedly

    # Discrete time for aging (incremented each scheduler call)
    s.tick = 0

    # Tuning knobs
    s.batch_aging_threshold = 25    # ticks before batch may run in pool0 even if hi-pri waiting
    s.max_assignments_per_pool = 4  # avoid issuing too many assignments per pool per tick
    s.max_queue_scan = 32           # cap scanning per queue per selection to avoid O(n^2) blowups


@register_scheduler(key="scheduler_medium_019")
def scheduler_medium_019(s, results, pipelines):
    """
    Scheduling step.

    Returns:
      suspensions: [] (non-preemptive policy)
      assignments: list of Assignment(ops=[op], cpu=..., ram=..., priority=..., pool_id=..., pipeline_id=...)
    """
    def _op_key(op):
        # Prefer stable IDs if provided; fall back to object identity
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _is_oom_error(err):
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "exceed" in e)

    def _extract_min_ram(op):
        # Best-effort extraction of an operator's RAM minimum requirement
        for attr in (
            "min_ram", "ram_min", "min_memory", "memory_min", "min_mem",
            "ram_requirement", "ram_required", "required_ram"
        ):
            v = getattr(op, attr, None)
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    pass
        return None

    def _prune_queue(priority):
        # Remove pipelines that are completed (or missing) from the head and also lazily while scanning
        q = s.queues[priority]
        if not q:
            return
        # Fast prune from head
        while q:
            pid = q[0]
            p = s.pipelines_by_id.get(pid)
            if p is None:
                q.pop(0)
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                q.pop(0)
                s.pipelines_by_id.pop(pid, None)
                s.pipeline_meta.pop(pid, None)
                continue
            break

    def _global_hi_pri_waiting():
        _prune_queue(Priority.QUERY)
        _prune_queue(Priority.INTERACTIVE)
        return bool(s.queues[Priority.QUERY]) or bool(s.queues[Priority.INTERACTIVE])

    def _oldest_batch_wait_ticks():
        _prune_queue(Priority.BATCH_PIPELINE)
        q = s.queues[Priority.BATCH_PIPELINE]
        if not q:
            return None
        # Scan a bounded number of items to estimate "oldest"
        best = None
        for pid in q[: min(len(q), s.max_queue_scan)]:
            meta = s.pipeline_meta.get(pid)
            if not meta:
                continue
            w = s.tick - meta.get("enqueue_tick", s.tick)
            best = w if best is None else max(best, w)
        return best

    def _cpu_target(priority, pool, avail_cpu):
        # Reserve some CPU for concurrency; still give queries more
        max_cpu = float(pool.max_cpu_pool)
        if priority == Priority.QUERY:
            share = 0.75
            cap = max(1.0, max_cpu * share)
        elif priority == Priority.INTERACTIVE:
            share = 0.50
            cap = max(1.0, max_cpu * share)
        else:
            share = 0.33
            cap = max(1.0, max_cpu * share)

        cpu = min(float(avail_cpu), cap)

        # Avoid tiny fractional allocations if simulator expects integers
        try:
            cpu_i = int(cpu)
            if cpu_i <= 0 and cpu > 0:
                cpu_i = 1
            return cpu_i
        except Exception:
            return cpu

    def _ram_target(priority, pool, op, avail_ram):
        max_ram = float(pool.max_ram_pool)
        min_ram = _extract_min_ram(op)

        # Conservative starting point (reduce OOMs => fewer 720s penalties)
        if priority == Priority.QUERY:
            base = 0.22 * max_ram
        elif priority == Priority.INTERACTIVE:
            base = 0.18 * max_ram
        else:
            base = 0.12 * max_ram

        if min_ram is not None:
            base = max(base, float(min_ram))

        k = _op_key(op)
        est = s.op_ram_est.get(k, None)
        if est is None:
            est = base
        else:
            est = max(float(est), base)

        # If we have very limited headroom, don't schedule below known minimum
        if min_ram is not None and float(avail_ram) < float(min_ram):
            return None

        ram = min(float(avail_ram), min(max_ram, est))

        # Avoid 0 allocations if simulator expects >0
        try:
            ram_f = float(ram)
            if ram_f <= 0:
                return None
            return ram_f
        except Exception:
            return ram

    def _next_runnable_op(p):
        st = p.runtime_status()
        # Retry FAILED ops (assumed OOM most of the time); only those whose parents completed
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        # Prefer first runnable op (pipeline order) for determinism
        for op in ops:
            if _op_key(op) in s.op_hard_fail:
                continue
            return op
        return None

    def _pick_pipeline_from_queue(priority, pool, avail_cpu, avail_ram, allow_scan=True):
        _prune_queue(priority)
        q = s.queues[priority]
        if not q:
            return None, None, None, None

        scans = min(len(q), s.max_queue_scan) if allow_scan else min(len(q), 1)
        for _ in range(scans):
            pid = q.pop(0)
            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                s.pipelines_by_id.pop(pid, None)
                s.pipeline_meta.pop(pid, None)
                continue

            op = _next_runnable_op(p)
            if op is None:
                # Not runnable now; rotate to back
                q.append(pid)
                continue

            # Compute resource request and check fit
            cpu = _cpu_target(priority, pool, avail_cpu)
            ram = _ram_target(priority, pool, op, avail_ram)
            if ram is None:
                q.append(pid)
                continue

            if float(cpu) <= float(avail_cpu) and float(ram) <= float(avail_ram):
                # Put it back to the back only if we didn't pick it; since we pick it, we keep it out
                return p, op, cpu, ram

            # Doesn't fit; rotate to back
            q.append(pid)

        return None, None, None, None

    # ---- Update time and ingest new pipelines ----
    s.tick += 1

    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.pipelines_by_id:
            continue
        s.pipelines_by_id[pid] = p
        s.pipeline_meta[pid] = {
            "enqueue_tick": s.tick,
            "last_scheduled_tick": None,
        }
        s.queues[p.priority].append(pid)

    # ---- Learn from results (OOM-adaptive RAM) ----
    if results:
        for r in results:
            # Update RAM estimate only on failures; keep it conservative to reduce 720s penalties
            if hasattr(r, "failed") and r.failed():
                oom = _is_oom_error(getattr(r, "error", None))
                for op in getattr(r, "ops", []) or []:
                    k = _op_key(op)
                    s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1

                    if oom:
                        # If we know what we asked for, double it; otherwise start from a conservative base
                        prev = s.op_ram_est.get(k, None)
                        prev = float(prev) if prev is not None else float(getattr(r, "ram", 0) or 0)
                        if prev <= 0:
                            # Fallback: learn from current pool size when we next schedule (no-op here)
                            prev = 0.0
                        new_est = (prev * 2.0) if prev > 0 else None

                        # Cap by pool max if available
                        pool_id = getattr(r, "pool_id", None)
                        if new_est is not None and pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
                            max_ram = float(s.executor.pools[int(pool_id)].max_ram_pool)
                            new_est = min(new_est, max_ram)

                        if new_est is not None and new_est > 0:
                            s.op_ram_est[k] = max(s.op_ram_est.get(k, 0.0), new_est)
                    else:
                        # Non-OOM failures are unlikely to be fixed by resizing; after 1-2 tries, stop burning resources
                        if s.op_fail_count.get(k, 0) >= 2:
                            s.op_hard_fail.add(k)

    # ---- Build assignments (non-preemptive) ----
    suspensions = []
    assignments = []

    hi_pri_waiting = _global_hi_pri_waiting()
    oldest_batch = _oldest_batch_wait_ticks()
    batch_is_very_old = (oldest_batch is not None and oldest_batch >= s.batch_aging_threshold)

    # Placement preferences:
    # - Prefer pool 0 for query/interactive (lower interference).
    # - Prefer non-0 pools for batch when available.
    pool_ids = list(range(s.executor.num_pools))
    if pool_ids and 0 in pool_ids:
        # Keep deterministic ordering but handle preferences per priority below
        pass

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]

        # Local accounting so we don't over-assign within a tick
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # In pool 0, keep headroom if high priority is waiting (protect latency)
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if pool_id == 0 and hi_pri_waiting:
            reserve_cpu = 0.25 * float(pool.max_cpu_pool)
            reserve_ram = 0.25 * float(pool.max_ram_pool)

        issued = 0
        issued_batch_here = 0

        while issued < s.max_assignments_per_pool:
            # Decide which priority to attempt next for this pool
            # Query first; interactive second; batch third with constraints.
            tried_any = False

            # Helper to try a specific priority with optional batch headroom reservation
            def _try_priority(pri):
                nonlocal avail_cpu, avail_ram, issued, issued_batch_here, tried_any
                tried_any = True

                # For batch in pool 0, enforce reservation unless batch is very old and no high-pri can run
                effective_cpu = avail_cpu
                effective_ram = avail_ram
                if pri == Priority.BATCH_PIPELINE and pool_id == 0 and hi_pri_waiting and not batch_is_very_old:
                    effective_cpu = max(0.0, avail_cpu - reserve_cpu)
                    effective_ram = max(0.0, avail_ram - reserve_ram)

                if effective_cpu <= 0 or effective_ram <= 0:
                    return False

                p, op, cpu, ram = _pick_pipeline_from_queue(pri, pool, effective_cpu, effective_ram, allow_scan=True)
                if p is None:
                    return False

                # Issue assignment for a single op to reduce blast radius of mis-sizing
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # Update local headroom
                avail_cpu -= float(cpu)
                avail_ram -= float(ram)
                issued += 1
                if pri == Priority.BATCH_PIPELINE:
                    issued_batch_here += 1

                meta = s.pipeline_meta.get(p.pipeline_id)
                if meta is not None:
                    meta["last_scheduled_tick"] = s.tick

                return True

            # Pool preference logic:
            # - pool 0: focus on query/interactive; allow at most one batch unless batch is old.
            # - other pools: still serve query/interactive first, but happily run batch.
            if pool_id == 0:
                if _try_priority(Priority.QUERY):
                    continue
                if _try_priority(Priority.INTERACTIVE):
                    continue

                # No hi-pri runnable in this pool now; allow batch, but limit churn
                if issued_batch_here >= 1 and not batch_is_very_old:
                    break
                if _try_priority(Priority.BATCH_PIPELINE):
                    continue
            else:
                if _try_priority(Priority.QUERY):
                    continue
                if _try_priority(Priority.INTERACTIVE):
                    continue
                if _try_priority(Priority.BATCH_PIPELINE):
                    continue

            # If we couldn't place anything, stop for this pool
            if not tried_any:
                break
            else:
                break

    return suspensions, assignments
