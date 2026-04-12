# policy_key: scheduler_iter_median_simple_001
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 33.93
# generated_at: 2026-04-12T01:07:32.049426
@register_scheduler_init(key="scheduler_iter_median_simple_001")
def scheduler_iter_median_simple_001_init(s):
    """Iteration 2: Strict-priority, latency-first packing + better retry sizing.

    Changes vs prior attempt:
    - Strict priority gating: do not schedule BATCH if any QUERY/INTERACTIVE runnable exists.
    - Latency-first sizing for QUERY/INTERACTIVE: when they run, give them *more CPU* (finish sooner),
      rather than tiny slices that can inflate service time and median latency.
    - Batch shaping: if scheduled, use smaller bounded slices to reduce head-of-line blocking.
    - Per-pipeline OOM backoff when pipeline_id is available in ExecutionResult; otherwise fallback
      to coarse per-priority bump.
    - Round-robin within each priority queue for fairness, but priority dominates.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # pipeline_id -> {"ram_mult": float, "cpu_mult": float, "drop": bool}
    s.pstate = {}
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    # coarse fallback when pipeline_id is missing from results
    s.pri_ram_mult = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }


@register_scheduler(key="scheduler_iter_median_simple_001")
def scheduler_iter_median_simple_001_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler with latency-first decisions.

    Core rule:
      If any QUERY/INTERACTIVE op is runnable anywhere, we avoid placing new batch work.
    """
    def _ensure_pstate(pipeline_id):
        if pipeline_id not in s.pstate:
            s.pstate[pipeline_id] = {"ram_mult": 1.0, "cpu_mult": 1.0, "drop": False}
        return s.pstate[pipeline_id]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _cleanup_queue(pri):
        """Remove completed pipelines and pipelines marked 'drop' with FAILED ops."""
        q = s.queues[pri]
        i = 0
        while i < len(q):
            p = q[i]
            st = p.runtime_status()
            if st.is_pipeline_successful():
                q.pop(i)
                continue
            pst = s.pstate.get(p.pipeline_id)
            if pst and pst.get("drop", False) and st.state_counts.get(OperatorState.FAILED, 0) > 0:
                q.pop(i)
                continue
            i += 1

    def _pop_rr_runnable_pipeline(pri):
        """Round-robin pick a pipeline that has at least one runnable op; otherwise None."""
        q = s.queues[pri]
        if not q:
            return None, None
        n = len(q)
        start = s.rr_cursor[pri] % n

        for k in range(n):
            idx = (start + k) % n
            p = q[idx]
            st = p.runtime_status()

            if st.is_pipeline_successful():
                continue
            pst = s.pstate.get(p.pipeline_id)
            if pst and pst.get("drop", False) and st.state_counts.get(OperatorState.FAILED, 0) > 0:
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            # Move cursor forward for next time; keep pipeline in queue (we don't remove it)
            s.rr_cursor[pri] = (idx + 1) % n
            return p, ops[:1]

        return None, None

    def _any_runnable(pri):
        q = s.queues[pri]
        for p in q:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            pst = s.pstate.get(p.pipeline_id)
            if pst and pst.get("drop", False) and st.state_counts.get(OperatorState.FAILED, 0) > 0:
                continue
            if st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True):
                return True
        return False

    def _request_resources(pool, pri, pipeline_id, avail_cpu, avail_ram):
        """Latency-first sizing for high-priority; batch is shaped small to avoid hogging."""
        pst = _ensure_pstate(pipeline_id)

        # Start from per-pipeline state; if no per-pipeline signal, use coarse per-priority multiplier.
        ram_mult = pst["ram_mult"] * s.pri_ram_mult.get(pri, 1.0)
        cpu_mult = pst["cpu_mult"]

        if pri == Priority.QUERY:
            # Give QUERY a large share when it runs to reduce service time and median latency.
            cpu = min(avail_cpu, max(1.0, pool.max_cpu_pool * 0.80) * cpu_mult)
            ram = min(avail_ram, max(1.0, pool.max_ram_pool * 0.25) * ram_mult)
        elif pri == Priority.INTERACTIVE:
            cpu = min(avail_cpu, max(1.0, pool.max_cpu_pool * 0.65) * cpu_mult)
            ram = min(avail_ram, max(1.0, pool.max_ram_pool * 0.25) * ram_mult)
        else:
            # Batch: keep chunks smaller so interactive arrivals can slip in sooner.
            cpu = min(avail_cpu, max(1.0, pool.max_cpu_pool * 0.35) * cpu_mult)
            ram = min(avail_ram, max(1.0, pool.max_ram_pool * 0.25) * ram_mult)

        # Hard clamps
        cpu = max(1.0, min(cpu, avail_cpu, pool.max_cpu_pool))
        ram = max(1.0, min(ram, avail_ram, pool.max_ram_pool))
        return cpu, ram

    # Enqueue new pipelines
    for p in pipelines:
        _ensure_pstate(p.pipeline_id)
        s.queues[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # Clean up queues (drop completed and known-unrecoverable failures)
    for pri in _priority_order():
        _cleanup_queue(pri)

    # Update state from results
    # Prefer per-pipeline updates if result.pipeline_id exists; otherwise coarse per-priority.
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        pri = getattr(r, "priority", None)
        pid = getattr(r, "pipeline_id", None)

        if _is_oom_error(getattr(r, "error", None)):
            if pid is not None:
                pst = _ensure_pstate(pid)
                pst["ram_mult"] = min(pst["ram_mult"] * 1.8, 32.0)
            elif pri is not None:
                s.pri_ram_mult[pri] = min(s.pri_ram_mult.get(pri, 1.0) * 1.3, 16.0)
        else:
            # Non-OOM failure: do not loop retries forever. Mark pipeline as droppable if we can.
            if pid is not None:
                pst = _ensure_pstate(pid)
                pst["drop"] = True

    # After updates, cleanup again (might drop pipelines now marked drop)
    for pri in _priority_order():
        _cleanup_queue(pri)

    suspensions = []
    assignments = []

    # Global gating: if any high-priority is runnable, avoid scheduling batch anywhere.
    hp_runnable = _any_runnable(Priority.QUERY) or _any_runnable(Priority.INTERACTIVE)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Fill the pool with as many ops as possible, but always prioritize QUERY then INTERACTIVE.
        # If no high-priority runnable exists (globally), then allow batch.
        while avail_cpu > 0 and avail_ram > 0:
            picked = None
            picked_pri = None
            picked_ops = None

            # Always try QUERY first, then INTERACTIVE
            for pri in (Priority.QUERY, Priority.INTERACTIVE):
                p, ops = _pop_rr_runnable_pipeline(pri)
                if p is not None:
                    picked = p
                    picked_pri = pri
                    picked_ops = ops
                    break

            # Only if no high-priority op is runnable anywhere, schedule batch
            if picked is None and not hp_runnable:
                p, ops = _pop_rr_runnable_pipeline(Priority.BATCH_PIPELINE)
                if p is not None:
                    picked = p
                    picked_pri = Priority.BATCH_PIPELINE
                    picked_ops = ops

            if picked is None:
                break

            cpu_req, ram_req = _request_resources(pool, picked_pri, picked.pipeline_id, avail_cpu, avail_ram)
            if cpu_req <= 0 or ram_req <= 0:
                break

            assignments.append(
                Assignment(
                    ops=picked_ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=picked.priority,
                    pool_id=pool_id,
                    pipeline_id=picked.pipeline_id,
                )
            )

            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # If we just scheduled high priority, keep hp_runnable True; otherwise may remain False.
            if picked_pri in (Priority.QUERY, Priority.INTERACTIVE):
                hp_runnable = True

    return suspensions, assignments
