# policy_key: scheduler_iter_median_simple_015
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.059039
# generation_seconds: 37.12
# generated_at: 2026-04-12T01:39:48.482723
@register_scheduler_init(key="scheduler_iter_median_simple_015")
def scheduler_iter_median_simple_015_init(s):
    """Priority-first, latency-oriented scheduler with pool headroom + small-slice packing.

    Key changes vs naive FIFO (small, targeted improvements):
    - Strict priority ordering: QUERY > INTERACTIVE > BATCH.
    - Latency bias: allocate small CPU/RAM "slices" to high-priority ops to increase parallelism
      and reduce head-of-line blocking.
    - Headroom protection: when any high-priority backlog exists, reserve a fixed fraction of
      each pool so batch cannot consume the entire pool and inflate tail latency.
    - Simple, safe queue hygiene: drop completed pipelines; avoid infinite retry on failures.

    Notes:
    - We intentionally avoid preemption because we lack a reliable view of currently running
      containers from the exposed API surface in the example.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Conservative global RAM multiplier per priority after OOM bursts (best-effort without pipeline_id).
    s.ram_mult = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # Round-robin cursors to keep fairness within each priority class.
    s.rr = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }


@register_scheduler(key="scheduler_iter_median_simple_015")
def scheduler_iter_median_simple_015_scheduler(s, results, pipelines):
    """Step function: enqueue -> update failure heuristics -> assign in strict priority order."""
    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _enqueue(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _queue_for_pri(pri):
        if pri == Priority.QUERY:
            return s.q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _next_runnable_pipeline(pri):
        """Round-robin select a pipeline that has at least 1 runnable op; returns (pipeline, op_list) or (None, None)."""
        q = _queue_for_pri(pri)
        if not q:
            return None, None

        n = len(q)
        start = s.rr[pri] % n

        # Scan at most once around the queue.
        for k in range(n):
            idx = (start + k) % n
            p = q[idx]
            st = p.runtime_status()

            # Drop completed pipelines
            if st.is_pipeline_successful():
                q.pop(idx)
                if idx < start:
                    start -= 1
                n -= 1
                if n <= 0:
                    s.rr[pri] = 0
                    return None, None
                continue

            # Drop pipelines with any failures (other than OOM we may be retrying as FAILED->ASSIGNABLE)
            # Without pipeline-level error cause, keep it simple: if FAILED exists and we didn't just
            # see an OOM burst for that priority, treat as terminal.
            if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.ram_mult[pri] <= 1.05:
                q.pop(idx)
                if idx < start:
                    start -= 1
                n -= 1
                if n <= 0:
                    s.rr[pri] = 0
                    return None, None
                continue

            # Find one runnable op (fine granularity reduces HoL blocking and improves interactive latency)
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not ops:
                continue

            # Update RR cursor to the element after the selected one
            s.rr[pri] = (idx + 1) % max(1, len(q))
            return p, ops

        return None, None

    def _has_hp_backlog():
        """Fast-ish check: do we have at least one runnable QUERY/INTERACTIVE op somewhere?"""
        # We do not want to mutate RR cursors here; just scan a bit.
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            q = _queue_for_pri(pri)
            for p in q:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return True
        return False

    def _slice_request(pool, pri, avail_cpu, avail_ram, hp_backlog):
        """Choose CPU/RAM for one op. Small slices for high-priority; batch uses leftovers."""
        # Reserve headroom if high-priority backlog exists.
        # This is the main latency protection mechanism.
        if hp_backlog:
            # Keep stronger headroom for batch than for high-priority.
            if pri == Priority.BATCH_PIPELINE:
                # Batch can only use a capped portion of the pool when HP is waiting.
                cap_cpu = min(avail_cpu, pool.max_cpu_pool * 0.35)
                cap_ram = min(avail_ram, pool.max_ram_pool * 0.35)
            else:
                # High priority can use most of what's currently available.
                cap_cpu = avail_cpu
                cap_ram = avail_ram
        else:
            cap_cpu, cap_ram = avail_cpu, avail_ram

        if cap_cpu <= 0 or cap_ram <= 0:
            return 0.0, 0.0

        # Base slice sizes: bias for more concurrency (latency) over single-op throughput.
        if pri == Priority.QUERY:
            base_cpu = min(2.0, pool.max_cpu_pool * 0.20)
            base_ram = pool.max_ram_pool * 0.08
        elif pri == Priority.INTERACTIVE:
            base_cpu = min(3.0, pool.max_cpu_pool * 0.25)
            base_ram = pool.max_ram_pool * 0.12
        else:
            # Batch: larger chunks when allowed (still bounded by caps).
            base_cpu = min(pool.max_cpu_pool * 0.50, max(2.0, pool.max_cpu_pool * 0.30))
            base_ram = pool.max_ram_pool * 0.25

        # Apply best-effort OOM-induced RAM inflation for this priority class.
        base_ram *= s.ram_mult[pri]

        cpu = max(1.0, min(base_cpu, cap_cpu, pool.max_cpu_pool))
        ram = max(1.0, min(base_ram, cap_ram, pool.max_ram_pool))
        return cpu, ram

    # ---- enqueue new pipelines ----
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # ---- update OOM heuristics from results (best-effort) ----
    # We can't map result -> pipeline_id reliably with the exposed API, so we adjust per-priority.
    saw_oom = {Priority.QUERY: False, Priority.INTERACTIVE: False, Priority.BATCH_PIPELINE: False}
    saw_non_oom_fail = {Priority.QUERY: False, Priority.INTERACTIVE: False, Priority.BATCH_PIPELINE: False}
    for r in results:
        if hasattr(r, "failed") and r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                saw_oom[r.priority] = True
            else:
                saw_non_oom_fail[r.priority] = True

    # On OOM, increase RAM multiplier quickly; on stable periods, gently decay back toward 1.0.
    for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        if saw_oom[pri]:
            s.ram_mult[pri] = min(s.ram_mult[pri] * 1.6, 16.0)
        else:
            # decay slowly to avoid oscillation
            s.ram_mult[pri] = max(1.0, s.ram_mult[pri] * 0.97)

        # If we see non-OOM failures for a priority, avoid wasting time retrying forever:
        # we effectively "tighten" by not inflating RAM due to those failures.
        if saw_non_oom_fail[pri] and s.ram_mult[pri] > 1.2:
            s.ram_mult[pri] = max(1.0, s.ram_mult[pri] * 0.90)

    suspensions = []
    assignments = []

    hp_backlog = _has_hp_backlog()

    # ---- schedule per pool ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Fill the pool in strict priority order, repeatedly, using small slices for HP.
        # This tends to lower median latency by allowing more HP ops to start quickly.
        while avail_cpu > 0 and avail_ram > 0:
            scheduled_any = False

            for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                # Hard gate: if HP backlog exists, do not schedule batch at all unless there is
                # clearly surplus capacity (avoid batch creeping in and delaying HP).
                if pri == Priority.BATCH_PIPELINE and hp_backlog:
                    # Allow batch only if plenty of surplus remains.
                    if avail_cpu < pool.max_cpu_pool * 0.25 or avail_ram < pool.max_ram_pool * 0.25:
                        continue

                p, ops = _next_runnable_pipeline(pri)
                if p is None:
                    continue

                cpu_req, ram_req = _slice_request(pool, pri, avail_cpu, avail_ram, hp_backlog)
                if cpu_req <= 0 or ram_req <= 0:
                    continue

                if cpu_req > avail_cpu or ram_req > avail_ram:
                    # Not enough resources left in this pool to schedule more.
                    continue

                assignments.append(
                    Assignment(
                        ops=ops,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                avail_cpu -= cpu_req
                avail_ram -= ram_req
                scheduled_any = True

                # After placing a QUERY/INTERACTIVE op, refresh backlog signal cheaply.
                # This helps keep batch out until HP demand is truly cleared.
                if pri != Priority.BATCH_PIPELINE:
                    hp_backlog = _has_hp_backlog()

                # Keep looping to pack more work into remaining resources.
                break

            if not scheduled_any:
                break

    return suspensions, assignments
