# policy_key: scheduler_iter_median_simple_012
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.039257
# generation_seconds: 48.04
# generated_at: 2026-04-12T01:37:47.837463
@register_scheduler_init(key="scheduler_iter_median_simple_012")
def scheduler_iter_median_simple_012_init(s):
    """Priority-aware, reservation-based scheduler to reduce latency.

    Incremental improvements over naive FIFO:
    - Separate queues by priority (QUERY > INTERACTIVE > BATCH_PIPELINE).
    - Global soft reservations: when any high-priority backlog exists, do not let batch
      consume the last slice of CPU/RAM (protects tail/median latency).
    - Better placement: pick the pool with best-fit headroom for the op (RAM-first),
      to reduce fragmentation and improve admission for interactive work.
    - Per-pipeline OOM backoff: when an op OOMs, retry that pipeline with increased RAM.
      (Uses op->pipeline mapping derived from Pipeline.values.)
    - High-priority CPU shaping: if HP backlog is small, give more CPU to finish sooner;
      if HP backlog is large, keep CPU modest to increase concurrency.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline adaptive state
    # ram_mult increases on OOM; capped to avoid runaway.
    s.pstate = {}  # pipeline_id -> {"ram_mult": float, "non_oom_fail": bool}

    # Round-robin cursors per priority for fairness within class
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Map operator identity -> pipeline_id so we can attribute ExecutionResult to a pipeline.
    # Keys are stable "op keys" derived from operator objects.
    s.op_to_pid = {}


@register_scheduler(key="scheduler_iter_median_simple_012")
def scheduler_iter_median_simple_012_scheduler(s, results, pipelines):
    """
    Main loop:
    - Ingest new pipelines and build op->pipeline mapping.
    - Update per-pipeline RAM multiplier on OOM failures.
    - Build runnable candidates (one op at a time) in strict priority order.
    - Assign ops to pools using best-fit placement + global HP reservations to prevent
      batch from inflating high-priority latency.
    """
    # -----------------------------
    # Helpers
    # -----------------------------
    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _ensure_pstate(pid):
        if pid not in s.pstate:
            s.pstate[pid] = {"ram_mult": 1.0, "non_oom_fail": False}
        return s.pstate[pid]

    def _op_key(op):
        # Prefer a stable explicit id if present; otherwise fall back to object identity.
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _pipeline_done_or_drop(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If we have marked a pipeline as non-retriable and it has failures, drop it.
        if s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
            if st.state_counts.get(OperatorState.FAILED, 0) > 0:
                return True
        return False

    def _has_assignable_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return bool(ops)

    def _count_backlog(pri):
        # Approximate backlog count = number of pipelines with a ready op.
        c = 0
        for p in s.queues[pri]:
            if _pipeline_done_or_drop(p):
                continue
            if _has_assignable_op(p):
                c += 1
        return c

    def _pop_next_runnable_pipeline(pri):
        """Round-robin scan that returns a runnable pipeline, cleaning up dropped ones."""
        q = s.queues[pri]
        if not q:
            return None

        # Clamp cursor if queue shrank
        s.rr_cursor[pri] = s.rr_cursor[pri] % max(1, len(q))
        start = s.rr_cursor[pri]

        # Try up to len(q) items
        for step in range(len(q)):
            idx = (start + step) % len(q)
            p = q[idx]

            # Remove dropped/completed pipelines eagerly
            if _pipeline_done_or_drop(p):
                q.pop(idx)
                if idx < start:
                    start -= 1
                if not q:
                    s.rr_cursor[pri] = 0
                    return None
                s.rr_cursor[pri] = start % len(q)
                return _pop_next_runnable_pipeline(pri)

            if _has_assignable_op(p):
                # Advance cursor after selecting this pipeline
                s.rr_cursor[pri] = (idx + 1) % len(q)
                # Remove from queue (caller will append back)
                q.pop(idx)
                return p

        return None

    def _pool_totals():
        tot_av_cpu = 0.0
        tot_av_ram = 0.0
        tot_max_cpu = 0.0
        tot_max_ram = 0.0
        for i in range(s.executor.num_pools):
            pool = s.executor.pools[i]
            tot_av_cpu += pool.avail_cpu_pool
            tot_av_ram += pool.avail_ram_pool
            tot_max_cpu += pool.max_cpu_pool
            tot_max_ram += pool.max_ram_pool
        return tot_av_cpu, tot_av_ram, tot_max_cpu, tot_max_ram

    def _reserve_fractions(hp_backlog):
        # Stronger reservation when there is more HP contention.
        # These are global reservations across all pools.
        if hp_backlog <= 0:
            return 0.0, 0.0
        if hp_backlog == 1:
            return 0.25, 0.25
        if hp_backlog <= 3:
            return 0.40, 0.40
        return 0.55, 0.55

    def _hp_cpu_target(pool, pri, hp_backlog):
        # Shape CPU for HP to reduce latency but keep concurrency when backlog is large.
        if pri == Priority.QUERY:
            # Query ops often short; keep small unless only one waiting.
            return min(pool.max_cpu_pool, 4.0 if hp_backlog <= 1 else 2.0)
        if pri == Priority.INTERACTIVE:
            return min(pool.max_cpu_pool, 6.0 if hp_backlog <= 1 else 3.0)
        # Batch: throughput-oriented when allowed
        return min(pool.max_cpu_pool, max(2.0, pool.max_cpu_pool * 0.6))

    def _base_ram_target(pool, pri):
        # RAM beyond minimum doesn't speed up; keep modest, rely on OOM backoff when wrong.
        if pri == Priority.QUERY:
            return max(1.0, pool.max_ram_pool * 0.10)
        if pri == Priority.INTERACTIVE:
            return max(1.0, pool.max_ram_pool * 0.18)
        return max(1.0, pool.max_ram_pool * 0.30)

    def _request_for(pool, pri, pid, hp_backlog):
        pst = _ensure_pstate(pid)
        cpu_req = _hp_cpu_target(pool, pri, hp_backlog)
        ram_req = _base_ram_target(pool, pri) * pst["ram_mult"]
        # Clamp to pool capacity bounds (actual fit checked later)
        cpu_req = max(1.0, min(cpu_req, pool.max_cpu_pool))
        ram_req = max(1.0, min(ram_req, pool.max_ram_pool))
        return cpu_req, ram_req

    def _best_fit_pool(pri, cpu_req, ram_req, reserved_cpu, reserved_ram, tot_av_cpu, tot_av_ram):
        # Choose a pool that fits; for batch, ensure global reservations remain.
        best_pool_id = None
        best_score = None

        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool < cpu_req or pool.avail_ram_pool < ram_req:
                continue

            if pri == Priority.BATCH_PIPELINE:
                # Global reservation check: after allocating batch, we must keep at least
                # reserved resources free globally (soft guard for latency).
                if (tot_av_cpu - cpu_req) < reserved_cpu:
                    continue
                if (tot_av_ram - ram_req) < reserved_ram:
                    continue

            # Best-fit on RAM first (reduce fragmentation), then CPU.
            # Lower leftover is better.
            ram_left = pool.avail_ram_pool - ram_req
            cpu_left = pool.avail_cpu_pool - cpu_req
            score = (ram_left, cpu_left)

            if best_score is None or score < best_score:
                best_score = score
                best_pool_id = pool_id

        return best_pool_id

    # -----------------------------
    # Ingest pipelines + build op map
    # -----------------------------
    for p in pipelines:
        _ensure_pstate(p.pipeline_id)
        # Build mapping from operators to pipeline_id (best effort)
        try:
            for op in getattr(p, "values", []) or []:
                s.op_to_pid[_op_key(op)] = p.pipeline_id
        except Exception:
            # If Pipeline.values isn't iterable in some scenarios, skip mapping.
            pass
        s.queues[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # -----------------------------
    # Update state from results (OOM backoff, non-OOM drop)
    # -----------------------------
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        # Attribute the result to a pipeline if possible
        pid = None
        try:
            rops = getattr(r, "ops", []) or []
            for op in rops:
                k = _op_key(op)
                if k in s.op_to_pid:
                    pid = s.op_to_pid[k]
                    break
        except Exception:
            pid = None

        if pid is None:
            # Can't attribute: do nothing (avoid global overreaction).
            continue

        pst = _ensure_pstate(pid)
        if _is_oom_error(getattr(r, "error", None)):
            pst["ram_mult"] = min(pst["ram_mult"] * 1.8, 32.0)
        else:
            pst["non_oom_fail"] = True

    # -----------------------------
    # Scheduling: protect HP latency with reservations + best-fit placement
    # -----------------------------
    suspensions = []
    assignments = []

    hp_backlog = _count_backlog(Priority.QUERY) + _count_backlog(Priority.INTERACTIVE)
    tot_av_cpu, tot_av_ram, tot_max_cpu, tot_max_ram = _pool_totals()
    cpu_frac, ram_frac = _reserve_fractions(hp_backlog)
    reserved_cpu = tot_max_cpu * cpu_frac
    reserved_ram = tot_max_ram * ram_frac

    # Keep issuing assignments while we can find runnable work that fits.
    # We schedule one op at a time to reduce head-of-line blocking and improve latency.
    max_attempts = 2000  # guard against infinite loops
    attempts = 0

    while attempts < max_attempts:
        attempts += 1

        # Refresh global availability (we also update totals locally after each assignment)
        any_fit = False

        for pri in _prio_order():
            p = _pop_next_runnable_pipeline(pri)
            if p is None:
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                # Already done; don't requeue.
                continue
            if s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False) and st.state_counts.get(OperatorState.FAILED, 0) > 0:
                # Drop non-retriable
                continue

            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable; requeue and move on.
                s.queues[pri].append(p)
                continue

            # We don't know which pool yet; compute a "typical" request per pool by trying best-fit.
            # We'll evaluate request per pool inside the pool loop by recomputing for that pool.
            chosen_pool_id = None
            chosen_cpu = None
            chosen_ram = None

            # Evaluate all pools; pick best-fit pool for this (pri, pipeline).
            for pool_id in range(s.executor.num_pools):
                pool = s.executor.pools[pool_id]
                cpu_req, ram_req = _request_for(pool, pri, p.pipeline_id, hp_backlog)
                # Also clamp to currently available in that pool (for candidate check)
                cpu_req = min(cpu_req, pool.avail_cpu_pool)
                ram_req = min(ram_req, pool.avail_ram_pool)
                if cpu_req < 1.0 or ram_req < 1.0:
                    continue

                # Use the best-fit selector to decide if this pool is acceptable; but since we
                # are iterating pools anyway, compute a local "would choose" score.
                # We'll mimic the selector logic here to avoid recalculating across all pools twice.
                if pool.avail_cpu_pool < cpu_req or pool.avail_ram_pool < ram_req:
                    continue
                if pri == Priority.BATCH_PIPELINE:
                    if (tot_av_cpu - cpu_req) < reserved_cpu:
                        continue
                    if (tot_av_ram - ram_req) < reserved_ram:
                        continue

                ram_left = pool.avail_ram_pool - ram_req
                cpu_left = pool.avail_cpu_pool - cpu_req
                score = (ram_left, cpu_left)

                if chosen_pool_id is None:
                    chosen_pool_id = pool_id
                    chosen_cpu = cpu_req
                    chosen_ram = ram_req
                    chosen_score = score
                else:
                    if score < chosen_score:
                        chosen_pool_id = pool_id
                        chosen_cpu = cpu_req
                        chosen_ram = ram_req
                        chosen_score = score

            if chosen_pool_id is None:
                # Could not fit anywhere now; requeue and try next pipeline/priority.
                s.queues[pri].append(p)
                continue

            # Place the assignment
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=chosen_cpu,
                    ram=chosen_ram,
                    priority=p.priority,
                    pool_id=chosen_pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Update global totals locally (so reservation check remains meaningful within this tick)
            tot_av_cpu -= chosen_cpu
            tot_av_ram -= chosen_ram
            any_fit = True

            # Requeue pipeline for further ops later
            s.queues[pri].append(p)

            # After scheduling one op, restart from highest priority to reduce latency.
            break

        if not any_fit:
            break

    return suspensions, assignments
