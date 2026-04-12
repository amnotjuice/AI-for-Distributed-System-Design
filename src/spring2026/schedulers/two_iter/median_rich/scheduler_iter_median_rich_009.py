# policy_key: scheduler_iter_median_rich_009
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.058972
# generation_seconds: 43.53
# generated_at: 2026-04-12T01:50:09.946556
@register_scheduler_init(key="scheduler_iter_median_rich_009")
def scheduler_iter_median_rich_009_init(s):
    """Priority-first scheduler with (1) pool isolation, (2) stronger headroom, (3) per-pipeline OOM ramp.

    Changes vs prior attempt:
    - Explicit pool isolation when multiple pools exist:
        * pool 0 is reserved for QUERY + INTERACTIVE (latency pool)
        * other pools primarily serve BATCH, but can lend to HP when HP backlog exists
    - Stronger batch headroom on the latency pool (batch never runs there if >1 pool).
    - Per-pipeline OOM handling without relying on ExecutionResult->pipeline mapping:
        * Detect FAILED ops via pipeline.runtime_status() and treat them as probable OOM retries.
        * On each retry episode, ramp that pipeline's RAM multiplier aggressively.
        * When a pipeline is in "oom_mode", schedule its next runnable op with a large RAM slice
          (often most of the pool) to converge quickly and reduce repeated OOM churn.
    - Keep assignments small for QUERY to reduce HoL blocking; INTERACTIVE moderate; BATCH larger only on batch pools.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-pipeline adaptive state
    # ram_mult: multiplies base RAM request
    # oom_mode: if True, next attempt gets a big RAM slice to "just succeed" quickly
    # fail_epochs: counts how many times we've observed FAILED ops while unfinished
    s.pstate = {}
    # Round-robin cursors per priority
    s.rr = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }


@register_scheduler(key="scheduler_iter_median_rich_009")
def scheduler_iter_median_rich_009_scheduler(s, results, pipelines):
    """
    Scheduling logic:
    1) Enqueue new pipelines into priority queues.
    2) Update per-pipeline state by inspecting runtime_status():
       - If pipeline has FAILED ops and is not complete, treat as likely OOM retry and ramp RAM.
    3) For each pool, pick the next runnable op using strict priority order with pool-specific eligibility:
       - If num_pools > 1:
           * pool 0: QUERY/INTERACTIVE only
           * pools 1..: BATCH first, but can run HP if HP backlog exists
       - If num_pools == 1:
           * all priorities share the pool; batch capped when HP backlog exists
    4) Assign one operator at a time per pipeline (fine-grained) and loop within a pool until resources are used.
    """
    # -----------------------------
    # Helpers
    # -----------------------------
    def _ensure_pstate(pipeline_id):
        ps = s.pstate.get(pipeline_id)
        if ps is None:
            ps = {"ram_mult": 1.0, "oom_mode": False, "fail_epochs": 0}
            s.pstate[pipeline_id] = ps
        return ps

    def _is_done_or_hard_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True, True  # done
        # We cannot reliably distinguish OOM vs non-OOM from pipeline alone.
        # For latency-oriented tuning, we prefer retrying FAILED states with more RAM
        # rather than dropping pipelines (dropping increases timeouts/latency).
        return False, False

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _has_hp_backlog():
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.queues[pri]:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return True
        return False

    def _cleanup_and_update_pipeline_state():
        # Remove completed pipelines and ramp RAM on observed FAILED ops.
        for pri in _priority_order():
            q = s.queues[pri]
            i = 0
            while i < len(q):
                p = q[i]
                done, drop = _is_done_or_hard_failed(p)
                if done and drop:
                    q.pop(i)
                    continue

                st = p.runtime_status()
                ps = _ensure_pstate(p.pipeline_id)

                # If the pipeline has any FAILED ops but isn't complete, treat as retry episode.
                # Ramp RAM quickly to reduce repeated OOM loops (major latency and timeout driver).
                failed_cnt = st.state_counts.get(OperatorState.FAILED, 0)
                if failed_cnt > 0 and not st.is_pipeline_successful():
                    # Only count a new "epoch" when we newly observe failures relative to our state.
                    # (We approximate; repeated observation still ramps, but with a cap.)
                    ps["fail_epochs"] += 1

                    # Aggressive ramp: 1.0 -> 2.0 -> 4.0 -> 8.0 (capped)
                    if ps["ram_mult"] < 8.0:
                        ps["ram_mult"] = min(8.0, ps["ram_mult"] * 2.0)
                    ps["oom_mode"] = True
                i += 1

    def _rr_pop_runnable(pri):
        """Round-robin scan for a pipeline with at least one runnable op (parents complete)."""
        q = s.queues[pri]
        n = len(q)
        if n == 0:
            return None, None

        start = s.rr[pri] % n
        for k in range(n):
            idx = (start + k) % n
            p = q[idx]
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                s.rr[pri] = (idx + 1) % max(1, len(q))
                return p, ops[:1]
        return None, None

    def _pool_priority_allowlist(pool_id, num_pools, hp_backlog):
        # If multiple pools: reserve pool 0 for HP. Other pools serve batch, but may lend to HP on backlog.
        if num_pools > 1:
            if pool_id == 0:
                return [Priority.QUERY, Priority.INTERACTIVE]
            # Batch pools:
            if hp_backlog:
                return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            return [Priority.BATCH_PIPELINE]
        # Single pool:
        return _priority_order()

    def _caps_for_priority(pool, pri, avail_cpu, avail_ram, num_pools, pool_id, hp_backlog):
        # Stronger headroom in the single-pool case to protect latency under contention.
        if num_pools == 1 and pri == Priority.BATCH_PIPELINE and hp_backlog:
            return min(avail_cpu, pool.max_cpu_pool * 0.35), min(avail_ram, pool.max_ram_pool * 0.35)
        # In multi-pool case, batch never goes on pool 0 due to allowlist.
        return avail_cpu, avail_ram

    def _request_size(pool, pri, pipeline_id, avail_cpu, avail_ram, pool_id, num_pools):
        ps = _ensure_pstate(pipeline_id)

        # CPU base requests: keep HP small to reduce blocking; batch larger on batch pools.
        if pri == Priority.QUERY:
            base_cpu = max(1.0, min(2.0, pool.max_cpu_pool * 0.20))
            base_ram = max(1.0, pool.max_ram_pool * 0.12)
        elif pri == Priority.INTERACTIVE:
            base_cpu = max(1.0, min(4.0, pool.max_cpu_pool * 0.30))
            base_ram = max(1.0, pool.max_ram_pool * 0.22)
        else:
            # Batch: avoid gobbling the latency pool (handled by allowlist); on batch pools, go larger.
            if num_pools > 1 and pool_id != 0:
                base_cpu = max(1.0, min(pool.max_cpu_pool * 0.70, max(2.0, pool.max_cpu_pool * 0.55)))
                base_ram = max(1.0, pool.max_ram_pool * 0.30)
            else:
                base_cpu = max(1.0, min(pool.max_cpu_pool * 0.50, max(2.0, pool.max_cpu_pool * 0.40)))
                base_ram = max(1.0, pool.max_ram_pool * 0.25)

        # RAM multiplier based on inferred OOM history.
        ram_req = base_ram * ps["ram_mult"]
        cpu_req = base_cpu

        # If we believe this pipeline is in an OOM retry mode, try to "buy certainty" with RAM:
        # request a large slice (up to almost all available) to converge quickly.
        if ps["oom_mode"]:
            # Allocate at least 70% of what's available in this pool to the next retry attempt,
            # but don't exceed pool maximum.
            ram_req = max(ram_req, avail_ram * 0.70, pool.max_ram_pool * 0.50)
            # Also avoid very tiny CPU when giving large RAM; modest CPU helps finish sooner.
            cpu_req = max(cpu_req, min(avail_cpu, max(2.0, pool.max_cpu_pool * 0.30)))

        # Clamp to available and pool limits; keep >= 1.0 to avoid zero-sized allocations.
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

    # -----------------------------
    # Update pipeline state & cleanup
    # -----------------------------
    _cleanup_and_update_pipeline_state()
    hp_backlog = _has_hp_backlog()

    # -----------------------------
    # Schedule
    # -----------------------------
    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        allowlist = _pool_priority_allowlist(pool_id, num_pools, hp_backlog)

        # Keep packing within this pool for the current tick.
        while avail_cpu > 0 and avail_ram > 0:
            picked = False

            # Strict priority order within the pool allowlist.
            for pri in _priority_order():
                if pri not in allowlist:
                    continue

                p, op_list = _rr_pop_runnable(pri)
                if p is None:
                    continue

                # Apply per-priority caps (batch throttling in single-pool contention).
                cpu_cap, ram_cap = _caps_for_priority(pool, pri, avail_cpu, avail_ram, num_pools, pool_id, hp_backlog)
                if cpu_cap <= 0 or ram_cap <= 0:
                    continue

                cpu_req, ram_req = _request_size(pool, pri, p.pipeline_id, cpu_cap, ram_cap, pool_id, num_pools)
                if cpu_req <= 0 or ram_req <= 0:
                    continue

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # If we just scheduled an OOM-mode pipeline, clear oom_mode optimistically.
                # If it fails again, we'll re-enter oom_mode and ramp further.
                ps = _ensure_pstate(p.pipeline_id)
                if ps["oom_mode"]:
                    ps["oom_mode"] = False

                avail_cpu -= cpu_req
                avail_ram -= ram_req
                picked = True
                break

            if not picked:
                break

    return suspensions, assignments
