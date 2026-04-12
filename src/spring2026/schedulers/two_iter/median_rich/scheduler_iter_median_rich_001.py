# policy_key: scheduler_iter_median_rich_001
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.056186
# generation_seconds: 48.18
# generated_at: 2026-04-12T01:43:44.403709
@register_scheduler_init(key="scheduler_iter_median_rich_001")
def scheduler_iter_median_rich_001_init(s):
    """Iteration 2: Priority-aware RR + per-operator adaptive sizing to cut OOM/retries.

    What changes vs prior iteration:
    - Track per-operator RAM/CPU hints from ExecutionResult (especially OOM/timeout).
      Next attempt allocates >= learned hint instead of pool-fraction guesses.
    - Default RAM requests are smaller (to reduce waste / increase concurrency), but
      never below the learned hint; this aims to reduce queueing latency.
    - Placement: for high-priority work, prefer pools with more free RAM to fit
      memory-heavy ops sooner (reduces OOM and head-of-line blocking).
    - Soft batch gating: when QUERY/INTERACTIVE backlog exists, batch only runs if
      a headroom buffer remains (reduces tail impact on high-priority latency).
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Learned per-operator resource floors. Keys must be stable across scheduler ticks.
    # We derive keys from pipeline_id + operator id-like attribute when possible.
    s.op_ram_hint = {}  # key -> ram floor
    s.op_cpu_hint = {}  # key -> cpu floor

    # Per-pipeline bookkeeping for dropping hard failures (non-OOM, non-timeout).
    s.pipeline_hard_fail = set()


@register_scheduler(key="scheduler_iter_median_rich_001")
def scheduler_iter_median_rich_001_scheduler(s, results, pipelines):
    """
    Step logic:
      1) Enqueue new pipelines into per-priority RR queues.
      2) Update op hints from results:
           - OOM => raise RAM floor for those ops (aggressive multiplier).
           - timeout => raise CPU floor (milder multiplier).
           - other failures => mark pipeline as hard-failed (do not retry).
      3) For each pool (ordered by free RAM for high priority), greedily place
         one ready op at a time, prioritizing QUERY > INTERACTIVE > BATCH.
    """
    # -----------------------------
    # Helpers
    # -----------------------------
    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg)

    def _op_key(pipeline_id, op):
        # Try common stable identifiers; fall back to repr(op) which is usually stable per object.
        oid = None
        for attr in ("op_id", "operator_id", "task_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        oid = v
                        break
                except Exception:
                    pass
        if oid is None:
            oid = repr(op)
        return (pipeline_id, oid)

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _queue_has_runnable(pri):
        for p in s.queues[pri]:
            if p.pipeline_id in s.pipeline_hard_fail:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                return True
        return False

    def _pop_next_pipeline(pri):
        q = s.queues[pri]
        if not q:
            return None

        # Round-robin scan, while cleaning out completed/hard-failed pipelines.
        n = len(q)
        start = s.rr_cursor[pri] % max(1, n)
        scanned = 0
        idx = start
        while scanned < n and q:
            p = q[idx]
            st = p.runtime_status()

            if p.pipeline_id in s.pipeline_hard_fail or st.is_pipeline_successful():
                q.pop(idx)
                n -= 1
                if n <= 0:
                    s.rr_cursor[pri] = 0
                    return None
                if idx >= n:
                    idx = 0
                continue

            # Keep and advance cursor
            s.rr_cursor[pri] = (idx + 1) % max(1, len(q))
            return p

            # (unreachable) scanned update
        return None

    def _defaults_for_priority(pool, pri):
        # Default requests are intentionally modest to improve packing/concurrency.
        # Learned hints will override these when needed.
        if pri == Priority.QUERY:
            base_cpu = max(1.0, min(2.0, pool.max_cpu_pool * 0.20))
            base_ram = max(1.0, pool.max_ram_pool * 0.06)
        elif pri == Priority.INTERACTIVE:
            base_cpu = max(1.0, min(3.0, pool.max_cpu_pool * 0.25))
            base_ram = max(1.0, pool.max_ram_pool * 0.10)
        else:
            base_cpu = max(1.0, min(4.0, pool.max_cpu_pool * 0.35))
            base_ram = max(1.0, pool.max_ram_pool * 0.16)
        return base_cpu, base_ram

    def _batch_headroom_ok(pool, avail_cpu, avail_ram, hp_backlog):
        if not hp_backlog:
            return True
        # Preserve headroom to absorb high-priority arrivals.
        # Tune conservatively: reserve both CPU and RAM buffers.
        cpu_buffer = max(1.0, pool.max_cpu_pool * 0.25)
        ram_buffer = max(1.0, pool.max_ram_pool * 0.25)
        return (avail_cpu - cpu_buffer) > 0 and (avail_ram - ram_buffer) > 0

    def _request_for_op(pool, pipeline, pri, op, avail_cpu, avail_ram, hp_backlog):
        # Gate batch under high-priority backlog.
        if pri == Priority.BATCH_PIPELINE and not _batch_headroom_ok(pool, avail_cpu, avail_ram, hp_backlog):
            return None

        base_cpu, base_ram = _defaults_for_priority(pool, pri)
        key = _op_key(pipeline.pipeline_id, op)

        # Learned floors from prior executions (especially after OOM/timeout)
        ram_floor = s.op_ram_hint.get(key, 0.0)
        cpu_floor = s.op_cpu_hint.get(key, 0.0)

        cpu_req = max(base_cpu, cpu_floor, 1.0)
        ram_req = max(base_ram, ram_floor, 1.0)

        # Clamp to what's available in this pool right now
        cpu_req = min(cpu_req, avail_cpu, pool.max_cpu_pool)
        ram_req = min(ram_req, avail_ram, pool.max_ram_pool)

        # Must fit at least minimally
        if cpu_req <= 0 or ram_req <= 0:
            return None
        return cpu_req, ram_req

    # -----------------------------
    # 1) Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        s.queues[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # -----------------------------
    # 2) Update learned hints from results
    # -----------------------------
    # Note: ExecutionResult may not contain pipeline_id; but it does contain ops.
    # We conservatively update hints using all plausible pipeline_id sources.
    # If we can't get a pipeline_id, we skip (better than poisoning hints).
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        err = getattr(r, "error", None)
        ops = getattr(r, "ops", []) or []

        # Try to locate pipeline_id if present; otherwise cannot build stable keys.
        pipeline_id = getattr(r, "pipeline_id", None)

        if _is_oom_error(err):
            # OOM => bump RAM floor aggressively for the involved ops
            # Use allocated ram as a lower bound, then multiply.
            for op in ops:
                if pipeline_id is None and hasattr(op, "pipeline_id"):
                    pipeline_id = getattr(op, "pipeline_id", None)
                if pipeline_id is None:
                    continue
                key = _op_key(pipeline_id, op)
                prev = s.op_ram_hint.get(key, 0.0)
                allocated = float(getattr(r, "ram", 0.0) or 0.0)
                # Large jump reduces repeated OOM loops.
                new_floor = max(prev, allocated * 2.0, allocated + 1.0)
                s.op_ram_hint[key] = min(new_floor, 1e12)  # effectively unbounded but safe
        elif _is_timeout_error(err):
            # timeout => modest CPU bump for those ops
            for op in ops:
                if pipeline_id is None and hasattr(op, "pipeline_id"):
                    pipeline_id = getattr(op, "pipeline_id", None)
                if pipeline_id is None:
                    continue
                key = _op_key(pipeline_id, op)
                prev = s.op_cpu_hint.get(key, 0.0)
                allocated = float(getattr(r, "cpu", 0.0) or 0.0)
                new_floor = max(prev, max(1.0, allocated * 1.5))
                s.op_cpu_hint[key] = min(new_floor, 1e12)
        else:
            # Other failure => treat as hard failure; avoid infinite retries.
            # If we can infer pipeline_id, drop it permanently.
            if pipeline_id is not None:
                s.pipeline_hard_fail.add(pipeline_id)

    # -----------------------------
    # 3) Build assignments
    # -----------------------------
    suspensions = []
    assignments = []

    hp_backlog = _queue_has_runnable(Priority.QUERY) or _queue_has_runnable(Priority.INTERACTIVE)

    # Pool ordering: for high-priority work, try RAM-richer pools first to reduce OOM/fit failures.
    pool_ids_by_ram = sorted(
        range(s.executor.num_pools),
        key=lambda pid: s.executor.pools[pid].avail_ram_pool,
        reverse=True,
    )

    # Greedy fill: continue placing while progress can be made in any pool.
    # This increases parallelism and reduces queueing latency.
    for pool_id in pool_ids_by_ram:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        made_progress = True
        while made_progress:
            made_progress = False
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            chosen = None
            chosen_pri = None
            chosen_op = None

            # Strict priority selection: reduces latency for high-priority classes.
            for pri in _priority_order():
                p = _pop_next_pipeline(pri)
                if p is None:
                    continue

                st = p.runtime_status()

                # Drop hard-failed pipelines lazily
                if p.pipeline_id in s.pipeline_hard_fail:
                    continue

                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not op_list:
                    # Not runnable yet, keep it for later
                    s.queues[pri].append(p)
                    continue

                # Choose a single op to keep scheduling atomic and responsive
                chosen = p
                chosen_pri = pri
                chosen_op = op_list[0]
                break

            if chosen is None:
                break

            req = _request_for_op(pool, chosen, chosen_pri, chosen_op, avail_cpu, avail_ram, hp_backlog)
            if req is None:
                # Can't fit in this pool now; requeue and stop trying in this pool to avoid spinning.
                s.queues[chosen_pri].append(chosen)
                break

            cpu_req, ram_req = req
            if cpu_req <= 0 or ram_req <= 0:
                s.queues[chosen_pri].append(chosen)
                break

            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen.pipeline_id,
                )
            )

            avail_cpu -= cpu_req
            avail_ram -= ram_req
            made_progress = True

            # Put pipeline back for future ops
            s.queues[chosen_pri].append(chosen)

    return suspensions, assignments
