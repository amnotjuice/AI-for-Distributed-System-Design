# policy_key: scheduler_iter_median_rich_006
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.056424
# generation_seconds: 48.29
# generated_at: 2026-04-12T01:47:52.171439
@register_scheduler_init(key="scheduler_iter_median_rich_006")
def scheduler_iter_median_rich_006_init(s):
    """Priority-first scheduler with targeted OOM avoidance + light pool partitioning.

    Key changes vs prior iteration:
    - Per-op RAM hints learned from actual OOM results (using result.ram) to cut retry loops.
    - Conservative default RAM floors by priority (QUERY/INTERACTIVE get more RAM upfront than before).
    - Light pool partitioning when multiple pools exist:
        * pool 0 prefers high-priority (QUERY/INTERACTIVE)
        * remaining pools prefer BATCH
      with spillover when preferred work is absent.
    - Batch admission throttling when high-priority backlog exists (avoid memory crowd-out).
    - Still simple/robust: no reliance on undocumented executor internals (no preemption).
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline state (fallback when we can't identify the specific op).
    # ram_mult: grows on OOM, slowly decays on success.
    s.pstate = {}

    # Per-op RAM hint: key -> required RAM guess (in "ram units" used by simulator).
    # Learns from OOMs: hint := max(hint, last_ram * growth).
    s.op_ram_hint = {}

    # Round-robin cursor per priority.
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }


@register_scheduler(key="scheduler_iter_median_rich_006")
def scheduler_iter_median_rich_006_scheduler(s, results, pipelines):
    """
    Step logic:
    1) Enqueue arriving pipelines by priority.
    2) Update learned RAM hints from results (esp. OOM).
    3) For each pool, schedule runnable ops in priority order with:
       - pool preference (pool0 for high-priority),
       - batch throttling under high-priority backlog,
       - RAM sizing = max(priority floor, learned op hint, pipeline multiplier floor),
         clamped to availability.
    """
    # -----------------------------
    # Helpers
    # -----------------------------
    def _ensure_pstate(pipeline_id):
        if pipeline_id not in s.pstate:
            s.pstate[pipeline_id] = {"ram_mult": 1.0}
        return s.pstate[pipeline_id]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _try_get_pipeline_id_from_result(r):
        # Best-effort: ExecutionResult doesn't guarantee pipeline_id in the prompt,
        # but ops often carry it. We probe a few common attribute shapes.
        for op in getattr(r, "ops", []) or []:
            for attr in ("pipeline_id", "pipelineId"):
                if hasattr(op, attr):
                    return getattr(op, attr)
            # Sometimes op has a backref to pipeline
            if hasattr(op, "pipeline") and hasattr(op.pipeline, "pipeline_id"):
                return op.pipeline.pipeline_id
        return None

    def _op_key(op):
        # Stable-ish key for per-op hints without relying on exact class API.
        for attr in ("op_id", "operator_id", "node_id", "task_id", "id"):
            if hasattr(op, attr):
                return f"{type(op).__name__}:{getattr(op, attr)}"
        # Fall back to repr; deterministic enough inside simulator.
        return f"{type(op).__name__}:{repr(op)}"

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _priority_floor_frac(pri):
        # Floors are intentionally higher than previous iteration to cut OOM retries.
        if pri == Priority.QUERY:
            return 0.18
        if pri == Priority.INTERACTIVE:
            return 0.28
        return 0.40  # batch more likely to be memory-heavy

    def _cpu_frac(pri):
        # Keep CPU modest for QUERY (many are short), more for INTERACTIVE, throughput for BATCH.
        if pri == Priority.QUERY:
            return 0.20
        if pri == Priority.INTERACTIVE:
            return 0.35
        return 0.55

    def _queue_has_runnable(pri):
        for p in s.queues[pri]:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            if st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True):
                return True
        return False

    def _pop_rr_runnable(pri):
        """Round-robin choose a pipeline that currently has a runnable op; drop completed ones."""
        q = s.queues[pri]
        if not q:
            return None, None

        n = len(q)
        start = s.rr_cursor[pri] % max(1, n)

        tried = 0
        while tried < n and q:
            idx = (start + tried) % len(q)
            p = q[idx]
            st = p.runtime_status()

            if st.is_pipeline_successful():
                q.pop(idx)
                # adjust cursor if we removed before it
                if idx < start:
                    start -= 1
                n = len(q)
                if n == 0:
                    s.rr_cursor[pri] = 0
                    return None, None
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                s.rr_cursor[pri] = (idx + 1) % max(1, len(q))
                return p, ops[:1]  # schedule one op at a time to reduce HoL blocking

            tried += 1

        return None, None

    def _pool_preference(pool_id, pri):
        # Light partition:
        # - If multiple pools: pool 0 prioritizes QUERY/INTERACTIVE; others prioritize BATCH.
        if s.executor.num_pools <= 1:
            return True
        if pool_id == 0:
            return pri in (Priority.QUERY, Priority.INTERACTIVE)
        return pri == Priority.BATCH_PIPELINE

    def _compute_request(pool, pri, pipeline_id, op, avail_cpu, avail_ram):
        # CPU request
        cpu_req = max(1.0, min(avail_cpu, pool.max_cpu_pool * _cpu_frac(pri)))

        # RAM request:
        # - priority-based floor (fraction of pool),
        # - learned per-op hint,
        # - per-pipeline multiplier (to help when we can't fingerprint ops).
        floor = max(1.0, pool.max_ram_pool * _priority_floor_frac(pri))

        hint = 0.0
        if op is not None:
            hint = s.op_ram_hint.get(_op_key(op), 0.0)

        pst = _ensure_pstate(pipeline_id)
        pip_floor = floor * pst["ram_mult"]

        ram_req = max(floor, hint, pip_floor)
        ram_req = max(1.0, min(ram_req, avail_ram, pool.max_ram_pool))

        return cpu_req, ram_req

    def _batch_allowed_under_hp(avail_cpu, avail_ram, pool):
        # When high-priority backlog exists, only let batch run if the pool is quite empty.
        # This reduces tail latency inflation from batch occupying RAM.
        # Thresholds tuned to be simple and conservative.
        return (avail_ram >= pool.max_ram_pool * 0.80) and (avail_cpu >= pool.max_cpu_pool * 0.50)

    # -----------------------------
    # 1) Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        _ensure_pstate(p.pipeline_id)
        s.queues[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # -----------------------------
    # 2) Learn from results (OOM-driven RAM hints)
    # -----------------------------
    # Update per-op hints aggressively on OOM; decay pipeline multiplier slowly on success.
    for r in results:
        # On success: very gentle decay of pipeline multiplier (keeps learned safety margin).
        if hasattr(r, "failed") and not r.failed():
            pid = _try_get_pipeline_id_from_result(r)
            if pid is not None and pid in s.pstate:
                s.pstate[pid]["ram_mult"] = max(1.0, s.pstate[pid]["ram_mult"] * 0.98)
            continue

        # Failed:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        if _is_oom_error(getattr(r, "error", None)):
            # Per-op hint update based on last allocated RAM (r.ram).
            last_ram = float(getattr(r, "ram", 0.0) or 0.0)
            growth = 1.75  # fast ramp to escape OOM loops
            pid = _try_get_pipeline_id_from_result(r)

            # If we can fingerprint ops, update per-op hint.
            for op in getattr(r, "ops", []) or []:
                k = _op_key(op)
                prev = float(s.op_ram_hint.get(k, 0.0) or 0.0)
                # If last_ram is missing/zero, still bump a bit from prev.
                candidate = max(prev * 1.25, last_ram * growth, 1.0)
                s.op_ram_hint[k] = min(candidate, 1e18)  # just prevent NaNs; pool clamp applies later

            # Also bump per-pipeline multiplier as a backstop.
            if pid is not None:
                pst = _ensure_pstate(pid)
                pst["ram_mult"] = min(pst["ram_mult"] * 1.35, 32.0)

        else:
            # Non-OOM failures: don't overreact with RAM.
            # (timeouts are likely queueing/contention; addressed by batch throttling and partitioning)
            pass

    # -----------------------------
    # 3) Schedule
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

        # Try to fill the pool with multiple assignments, respecting priorities.
        while avail_cpu > 0 and avail_ram > 0:
            chosen_p = None
            chosen_ops = None
            chosen_pri = None

            # Two-pass selection:
            # Pass 1: pick preferred priority classes for this pool.
            # Pass 2: allow spillover if nothing preferred is runnable.
            for pass_idx in (1, 2):
                for pri in _priority_order():
                    if pass_idx == 1 and not _pool_preference(pool_id, pri):
                        continue

                    if pri == Priority.BATCH_PIPELINE and hp_backlog:
                        # Throttle batch while high-priority waits, unless pool is very empty.
                        if not _batch_allowed_under_hp(avail_cpu, avail_ram, pool):
                            continue

                    p, ops1 = _pop_rr_runnable(pri)
                    if p is None:
                        continue

                    chosen_p, chosen_ops, chosen_pri = p, ops1, pri
                    break
                if chosen_p is not None:
                    break

            if chosen_p is None or not chosen_ops:
                break

            # Size request using learned RAM hint + floors.
            op0 = chosen_ops[0] if chosen_ops else None
            cpu_req, ram_req = _compute_request(pool, chosen_pri, chosen_p.pipeline_id, op0, avail_cpu, avail_ram)

            # If we can't fit at least minimal resources, stop for this pool.
            if cpu_req <= 0 or ram_req <= 0:
                break

            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen_p.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            avail_cpu -= cpu_req
            avail_ram -= ram_req

    return suspensions, assignments
