# policy_key: scheduler_iter_median_rich_012
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.064012
# generation_seconds: 47.03
# generated_at: 2026-04-12T01:52:45.577037
@register_scheduler_init(key="scheduler_iter_median_rich_012")
def scheduler_iter_median_rich_012_init(s):
    """Iteration 2: Priority queues + pool partitioning + failure-aware RAM ramp.

    Changes vs prior attempt (aimed at lower latency and fewer timeouts):
    - Harder priority separation across pools (if >=2 pools):
        * pool 0: prefers QUERY/INTERACTIVE (latency pool)
        * other pools: prefer BATCH (throughput pool), but can help with INTERACTIVE when idle
    - Stronger admission control for BATCH when high-priority backlog exists
    - Per-pipeline RAM ramp based on observed FAILED count in runtime_status (proxy for OOM retries)
      This avoids global RAM inflation while still converging to adequate RAM for “big” operators.
    - Smaller default RAM requests to reduce memory hoarding (previous run allocated ~94% RAM on avg)
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-pipeline hints and bookkeeping
    # ram_mult grows with failed-op count (proxy for repeated OOMs); capped to avoid runaway hoarding.
    s.pstate = {}  # pipeline_id -> {"ram_mult": float, "cpu_mult": float}
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }


@register_scheduler(key="scheduler_iter_median_rich_012")
def scheduler_iter_median_rich_012_scheduler(s, results, pipelines):
    """
    Scheduler step:
    1) Enqueue new pipelines by priority.
    2) For each pool, schedule runnable ops with:
       - strict priority ordering (QUERY > INTERACTIVE > BATCH)
       - pool partition preference (if multiple pools)
       - batch throttling when high-priority backlog exists
       - failure-aware RAM ramp per pipeline (based on FAILED count in runtime status)
    """
    # -----------------------------
    # Helpers
    # -----------------------------
    def _ensure_pstate(pipeline_id):
        if pipeline_id not in s.pstate:
            s.pstate[pipeline_id] = {"ram_mult": 1.0, "cpu_mult": 1.0}
        return s.pstate[pipeline_id]

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _drop_if_done_or_hopeless(q, idx):
        """Remove completed pipelines; keep failed pipelines (we treat them as retryable)."""
        p = q[idx]
        st = p.runtime_status()
        if st.is_pipeline_successful():
            q.pop(idx)
            return True
        return False

    def _pop_rr_runnable(pri):
        """Round-robin pick a pipeline that currently has at least one runnable op."""
        q = s.queues[pri]
        if not q:
            return None, None

        n = len(q)
        if n == 0:
            return None, None

        start = s.rr_cursor[pri] % n
        # try at most n items
        k = 0
        while k < n and q:
            n = len(q)
            if n == 0:
                return None, None
            idx = (start + k) % n
            # Drop completed pipelines eagerly
            if _drop_if_done_or_hopeless(q, idx):
                # adjust cursors conservatively
                start = start % max(1, len(q)) if q else 0
                k = 0
                continue

            p = q[idx]
            st = p.runtime_status()
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                # advance cursor to next element for fairness
                s.rr_cursor[pri] = (idx + 1) % max(1, len(q))
                # remove from queue temporarily; caller is expected to append back
                q.pop(idx)
                return p, ops[:1]  # one op per assignment for better latency isolation
            k += 1

        return None, None

    def _has_hp_backlog():
        """Any runnable QUERY/INTERACTIVE work waiting."""
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.queues[pri]:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                if st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True):
                    return True
        return False

    def _pool_pref_allows(pool_id, pri, hp_backlog):
        """Pool partitioning preference."""
        # Single pool: always allow.
        if s.executor.num_pools <= 1:
            return True

        # Pool 0 = latency pool. Strongly prefer high priority.
        if pool_id == 0:
            return pri != Priority.BATCH_PIPELINE

        # Other pools = throughput pools.
        if pri == Priority.BATCH_PIPELINE:
            return True

        # Let other pools help with INTERACTIVE/QUERY if we have HP backlog (or if they are idle).
        return True

    def _batch_throttle(pool, avail_cpu, avail_ram, hp_backlog):
        """When HP backlog exists, only run batch if pool has ample headroom."""
        if not hp_backlog:
            return True
        # Require meaningful slack before admitting batch to avoid inflating HP tail latency.
        return (avail_cpu >= pool.max_cpu_pool * 0.50) and (avail_ram >= pool.max_ram_pool * 0.50)

    def _update_ram_mult_from_failures(pipeline):
        """Increase RAM multiplier based on current FAILED count (proxy for OOM retries)."""
        st = pipeline.runtime_status()
        failed_cnt = st.state_counts.get(OperatorState.FAILED, 0) or 0
        pst = _ensure_pstate(pipeline.pipeline_id)

        # Ramp aggressively early to converge quickly for big ops, but cap to avoid memory hoarding.
        # Using 1.9^k gives: k=0->1.0, 1->1.9, 2->3.6, 3->6.9, 4->13.0 (then capped)
        ram_mult = 1.0 * (1.9 ** min(6, failed_cnt))
        pst["ram_mult"] = max(pst["ram_mult"], min(ram_mult, 12.0))
        return pst

    def _request_size(pool, pri, pipeline_id, avail_cpu, avail_ram):
        """Resource shaping: keep defaults small to reduce RAM hoarding; scale via per-pipeline multipliers."""
        pst = _ensure_pstate(pipeline_id)

        # CPU defaults: small for QUERY, moderate for INTERACTIVE, larger for BATCH.
        if pri == Priority.QUERY:
            base_cpu = 1.0
            base_ram = pool.max_ram_pool * 0.05  # keep small; ramp if failures happen
        elif pri == Priority.INTERACTIVE:
            base_cpu = min(2.0, pool.max_cpu_pool)
            base_ram = pool.max_ram_pool * 0.08
        else:
            base_cpu = min(4.0, pool.max_cpu_pool)
            base_ram = pool.max_ram_pool * 0.10  # batch starts low too; ramp on retries

        cpu_req = base_cpu * pst["cpu_mult"]
        ram_req = base_ram * pst["ram_mult"]

        # Clamp to pool availability/maxima; enforce at least 1 unit.
        cpu_req = max(1.0, min(cpu_req, avail_cpu, pool.max_cpu_pool))
        ram_req = max(1.0, min(ram_req, avail_ram, pool.max_ram_pool))
        return cpu_req, ram_req

    # -----------------------------
    # Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        _ensure_pstate(p.pipeline_id)
        s.queues[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    hp_backlog = _has_hp_backlog()

    # -----------------------------
    # Schedule per pool
    # -----------------------------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Continue assigning in this pool while we have resources and find runnable work.
        while avail_cpu > 0 and avail_ram > 0:
            chosen_p = None
            chosen_ops = None
            chosen_pri = None

            # Try priorities in order, but obey pool partitioning preferences + batch throttling.
            for pri in _priority_order():
                if not _pool_pref_allows(pool_id, pri, hp_backlog):
                    continue
                if pri == Priority.BATCH_PIPELINE and not _batch_throttle(pool, avail_cpu, avail_ram, hp_backlog):
                    continue

                p, ops = _pop_rr_runnable(pri)
                if p is None:
                    continue

                chosen_p = p
                chosen_ops = ops
                chosen_pri = pri
                break

            if chosen_p is None:
                break

            # Update per-pipeline RAM multiplier based on current observed failures (pipeline-local).
            pst = _update_ram_mult_from_failures(chosen_p)

            # Resource request sized for this pool and remaining capacity.
            cpu_req, ram_req = _request_size(pool, chosen_pri, chosen_p.pipeline_id, avail_cpu, avail_ram)

            # If we cannot fit even the minimum, put it back and stop assigning in this pool.
            if cpu_req <= 0 or ram_req <= 0:
                s.queues[chosen_pri].append(chosen_p)
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

            # Decrement local accounting to avoid overscheduling in a single tick.
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Re-enqueue pipeline for future ops.
            s.queues[chosen_pri].append(chosen_p)

    return suspensions, assignments
