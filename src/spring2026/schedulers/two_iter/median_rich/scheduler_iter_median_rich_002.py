# policy_key: scheduler_iter_median_rich_002
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.056186
# generation_seconds: 48.71
# generated_at: 2026-04-12T01:44:33.110086
@register_scheduler_init(key="scheduler_iter_median_rich_002")
def scheduler_iter_median_rich_002_init(s):
    """Priority-aware, OOM-adaptive scheduler with light pool partitioning.

    Directional improvements over the previous priority FIFO:
    - Per-pipeline RAM/CPU sizing with fast OOM backoff (when pipeline_id is available on results).
    - Conservative initial RAM floors (especially for INTERACTIVE/BATCH) to reduce OOM churn/timeouts.
    - If multiple pools exist: keep pool 0 mostly for QUERY/INTERACTIVE when there is HP backlog.
    - Keep 1-op granularity per assignment to reduce head-of-line blocking.
    - Simple retry policy: retry FAILED ops unless we infer a non-OOM failure for that pipeline.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-pipeline adaptive state:
    # ram_req/cpu_req are "requested" sizes, clamped by pool availability.
    s.pstate = {}  # pipeline_id -> dict
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    # When result objects don't provide pipeline_id, fall back to coarse per-priority multipliers
    s.pri_fallback = {
        Priority.QUERY: {"ram_mult": 1.0, "cpu_mult": 1.0},
        Priority.INTERACTIVE: {"ram_mult": 1.0, "cpu_mult": 1.0},
        Priority.BATCH_PIPELINE: {"ram_mult": 1.0, "cpu_mult": 1.0},
    }


@register_scheduler(key="scheduler_iter_median_rich_002")
def scheduler_iter_median_rich_002_scheduler(s, results, pipelines):
    """One scheduling tick: update sizing from results, enqueue new pipelines, then assign ops."""

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return "timeout" in msg

    def _ensure_pstate(p, pool=None):
        ps = s.pstate.get(p.pipeline_id)
        if ps is None:
            # Conservative RAM floors to reduce repeated OOMs; CPU moderate for latency.
            # These are "requested" sizes that will be clamped to pool availability.
            if p.priority == Priority.QUERY:
                ram_frac, cpu_frac = 0.16, 0.30
                ram_min_abs, cpu_min_abs = 1.0, 1.0
            elif p.priority == Priority.INTERACTIVE:
                ram_frac, cpu_frac = 0.26, 0.45
                ram_min_abs, cpu_min_abs = 1.0, 1.0
            else:
                ram_frac, cpu_frac = 0.30, 0.55
                ram_min_abs, cpu_min_abs = 1.0, 1.0

            # If pool is known, initialize with pool-based sizes; else use placeholders.
            if pool is not None:
                ram_req = max(ram_min_abs, pool.max_ram_pool * ram_frac)
                cpu_req = max(cpu_min_abs, min(pool.max_cpu_pool, pool.max_cpu_pool * cpu_frac))
            else:
                ram_req = ram_min_abs
                cpu_req = cpu_min_abs

            ps = {
                "ram_req": float(ram_req),
                "cpu_req": float(cpu_req),
                "attempts": 0,
                "non_oom_fail": False,  # do not retry if we see a non-OOM failure
            }
            s.pstate[p.pipeline_id] = ps
        return ps

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _queue_has_runnable(pri):
        q = s.queues[pri]
        for p in q:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            # If pipeline had a definitive non-OOM failure, consider it non-runnable (we drop later)
            if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                return True
        return False

    def _pop_rr(pri):
        q = s.queues[pri]
        if not q:
            return None
        n = len(q)
        start = s.rr_cursor[pri] % max(1, n)

        # Scan at most n elements; remove completed/definitively failed pipelines as we go.
        for k in range(n):
            idx = (start + k) % len(q)
            p = q[idx]
            st = p.runtime_status()

            if st.is_pipeline_successful():
                q.pop(idx)
                if q:
                    s.rr_cursor[pri] = idx % len(q)
                else:
                    s.rr_cursor[pri] = 0
                continue

            if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
                q.pop(idx)
                if q:
                    s.rr_cursor[pri] = idx % len(q)
                else:
                    s.rr_cursor[pri] = 0
                continue

            # Return this pipeline and advance cursor beyond it
            q.pop(idx)
            if q:
                s.rr_cursor[pri] = idx % len(q)
            else:
                s.rr_cursor[pri] = 0
            return p

        return None

    def _pool_accepts_priority(pool_id, pri, hp_backlog):
        # If we have multiple pools, preferentially keep pool 0 for high priority when backlog exists.
        if s.executor.num_pools <= 1:
            return True
        if pool_id == 0 and hp_backlog and pri == Priority.BATCH_PIPELINE:
            return False
        return True

    def _clamp(v, lo, hi):
        if v < lo:
            return lo
        if v > hi:
            return hi
        return v

    # -----------------------------
    # Update sizing policy from results (prefer per-pipeline, fallback per-priority)
    # -----------------------------
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            # On success, gently decay overly-large requests if we can attribute to a pipeline
            pid = getattr(r, "pipeline_id", None)
            if pid is not None and pid in s.pstate:
                ps = s.pstate[pid]
                # Reduce RAM/CPU cautiously to avoid oscillation; keep above 1.0
                ps["ram_req"] = max(1.0, ps["ram_req"] * 0.92)
                ps["cpu_req"] = max(1.0, ps["cpu_req"] * 0.96)
            continue

        err = getattr(r, "error", None)
        pri = getattr(r, "priority", None)
        pid = getattr(r, "pipeline_id", None)

        is_oom = _is_oom_error(err)
        is_to = _is_timeout_error(err)

        if pid is not None and pid in s.pstate:
            ps = s.pstate[pid]
            ps["attempts"] += 1

            if is_oom:
                # Fast RAM backoff: 2x, plus a small additive bump to escape small values.
                ps["ram_req"] = ps["ram_req"] * 2.0 + 1.0
                # Slight CPU bump to reduce time-in-system after a successful retry.
                ps["cpu_req"] = ps["cpu_req"] * 1.15
            elif is_to:
                # Timeouts generally want more CPU (or less contention); we can only change sizing.
                ps["cpu_req"] = ps["cpu_req"] * 1.60
                # Also bump RAM a bit to reduce risk of memory-related slowdowns/near-OOM.
                ps["ram_req"] = ps["ram_req"] * 1.10
            else:
                # Non-OOM failure: stop retrying this pipeline.
                ps["non_oom_fail"] = True
        else:
            # Fallback when pipeline_id isn't available on results.
            if pri is None:
                continue
            fb = s.pri_fallback.get(pri)
            if fb is None:
                continue
            if is_oom:
                fb["ram_mult"] = min(fb["ram_mult"] * 1.35, 16.0)
                fb["cpu_mult"] = min(fb["cpu_mult"] * 1.05, 4.0)
            elif is_to:
                fb["cpu_mult"] = min(fb["cpu_mult"] * 1.25, 4.0)
                fb["ram_mult"] = min(fb["ram_mult"] * 1.10, 16.0)

    # -----------------------------
    # Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        # Initialize pstate without pool; will be refined on first scheduling in a specific pool.
        _ensure_pstate(p, pool=None)
        s.queues[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # -----------------------------
    # Scheduling
    # -----------------------------
    suspensions = []
    assignments = []

    hp_backlog = _queue_has_runnable(Priority.QUERY) or _queue_has_runnable(Priority.INTERACTIVE)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep assigning while we have resources and runnable work
        # (avoid overscheduling by tracking avail_* locally).
        while avail_cpu > 0 and avail_ram > 0:
            chosen = None
            chosen_pri = None
            chosen_ops = None

            # Strict priority order for latency; batch only if no HP runnable in this pool context.
            for pri in _priority_order():
                if not _pool_accepts_priority(pool_id, pri, hp_backlog):
                    continue

                p = _pop_rr(pri)
                if p is None:
                    continue

                st = p.runtime_status()
                # Drop successful pipelines
                if st.is_pipeline_successful():
                    continue

                # If definitively failed, skip
                if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
                    continue

                # One ready op only
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not ops:
                    # Not runnable now; push back
                    s.queues[pri].append(p)
                    continue

                chosen = p
                chosen_pri = pri
                chosen_ops = ops
                break

            if chosen is None:
                break

            # Refine per-pipeline initial sizing using this pool's capacity if not already meaningful.
            ps = _ensure_pstate(chosen, pool=pool)

            # Apply per-priority fallback multipliers if we couldn't attribute failures precisely.
            fb = s.pri_fallback[chosen_pri]
            req_cpu = ps["cpu_req"] * fb["cpu_mult"]
            req_ram = ps["ram_req"] * fb["ram_mult"]

            # Guardrails:
            # - Avoid allocating tiny RAM for interactive/batch (OOM churn dominates latency).
            # - Avoid taking the entire pool with a single batch op when HP backlog exists.
            if chosen_pri == Priority.QUERY:
                ram_floor = max(1.0, pool.max_ram_pool * 0.10)
                cpu_floor = 1.0
                cpu_cap = pool.max_cpu_pool * 0.60
                ram_cap = pool.max_ram_pool * 0.55
            elif chosen_pri == Priority.INTERACTIVE:
                ram_floor = max(1.0, pool.max_ram_pool * 0.16)
                cpu_floor = 1.0
                cpu_cap = pool.max_cpu_pool * 0.75
                ram_cap = pool.max_ram_pool * 0.70
            else:
                ram_floor = max(1.0, pool.max_ram_pool * (0.18 if hp_backlog else 0.14))
                cpu_floor = 1.0
                cpu_cap = pool.max_cpu_pool * (0.55 if hp_backlog else 0.85)
                ram_cap = pool.max_ram_pool * (0.55 if hp_backlog else 0.85)

            req_cpu = _clamp(req_cpu, cpu_floor, cpu_cap)
            req_ram = _clamp(req_ram, ram_floor, ram_cap)

            # Fit to current availability
            cpu = min(avail_cpu, req_cpu, pool.max_cpu_pool)
            ram = min(avail_ram, req_ram, pool.max_ram_pool)

            # If we can't fit a minimally useful container, give up on this pool this tick.
            if cpu <= 0 or ram <= 0:
                s.queues[chosen_pri].append(chosen)
                break

            # Record that we attempted this pipeline (useful for later backoff policies)
            ps["attempts"] += 1

            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=cpu,
                    ram=ram,
                    priority=chosen.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen.pipeline_id,
                )
            )

            avail_cpu -= cpu
            avail_ram -= ram

            # Push pipeline back; it may have more operators later.
            s.queues[chosen_pri].append(chosen)

    return suspensions, assignments
