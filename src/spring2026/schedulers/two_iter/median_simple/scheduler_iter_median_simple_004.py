# policy_key: scheduler_iter_median_simple_004
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.056603
# generation_seconds: 47.10
# generated_at: 2026-04-12T01:09:50.850808
@register_scheduler_init(key="scheduler_iter_median_simple_004")
def scheduler_iter_median_simple_004_init(s):
    """Priority-first latency scheduler (incremental improvement over naive FIFO).

    Goals (latency-focused, especially for QUERY/INTERACTIVE):
    - Strict priority order: QUERY > INTERACTIVE > BATCH.
    - SRPT-ish within each priority: prefer pipelines with fewer remaining ops (proxy).
    - Resource shaping: give MORE CPU to high-priority ops to finish faster; cap batch usage
      when any high-priority backlog exists (prevents batch from inflating tail latency).
    - OOM backoff: detect OOM failures and increase RAM allocation for that pipeline.
      (Best-effort mapping from ExecutionResult.ops back to pipeline.)
    - Pool choice: prefer the pool with most available headroom for high-priority work.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-pipeline hints
    # ram_mult: multiplies baseline RAM request; increases on OOM.
    # cpu_mult: modestly adjustable (kept near 1.0 in this iteration).
    # drop: if pipeline has FAILED ops not attributable to OOM, stop retrying.
    s.pstate = {}  # pipeline_id -> dict
    # A tiny stickiness signal (helps avoid scattering a pipeline across pools).
    s.pipeline_pool_hint = {}  # pipeline_id -> last_pool_id (best-effort)


@register_scheduler(key="scheduler_iter_median_simple_004")
def scheduler_iter_median_simple_004_scheduler(s, results, pipelines):
    """
    Step:
    1) Enqueue new pipelines by priority.
    2) Build a best-effort op->pipeline_id map from queued pipelines to interpret results.
    3) Update per-pipeline RAM multiplier on OOM; drop non-OOM failures.
    4) For each pool (in an order that benefits high-priority), place as many ops as fit:
       - Always try QUERY then INTERACTIVE then BATCH.
       - If any high-priority backlog exists, batch is capped to a small share.
       - Allocate relatively larger CPU slices to high-priority ops to reduce service time.
    """
    # -----------------------------
    # Helpers
    # -----------------------------
    def _ensure_pstate(pid):
        if pid not in s.pstate:
            s.pstate[pid] = {"ram_mult": 1.0, "cpu_mult": 1.0, "drop": False}
        return s.pstate[pid]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _pipeline_remaining_ops_proxy(p):
        """Proxy for remaining work: count ops not completed (parents not required)."""
        st = p.runtime_status()
        # Prefer small pipelines (SRPT-ish) to reduce median latency.
        # We count PENDING+FAILED as remaining-ish, but include all non-COMPLETED states.
        remaining = 0
        try:
            # If get_ops supports require_parents_complete=False, use it.
            remaining = len(st.get_ops(
                [OperatorState.PENDING, OperatorState.ASSIGNED, OperatorState.RUNNING,
                 OperatorState.SUSPENDING, OperatorState.FAILED],
                require_parents_complete=False
            ))
        except Exception:
            # Fallback: coarse estimate from state_counts if available.
            remaining = 0
            if hasattr(st, "state_counts") and isinstance(st.state_counts, dict):
                for k, v in st.state_counts.items():
                    if k != OperatorState.COMPLETED:
                        remaining += int(v)
        return max(1, remaining)

    def _has_high_priority_backlog():
        """True if any QUERY/INTERACTIVE pipeline has an assignable op."""
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.queues[pri]:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                if _ensure_pstate(p.pipeline_id).get("drop", False):
                    continue
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return True
        return False

    def _select_best_pipeline(pri):
        """Pick a runnable pipeline for this priority using SRPT-ish proxy."""
        best = None
        best_score = None
        # Also compact the queue by removing completed/dropped pipelines.
        new_q = []
        for p in s.queues[pri]:
            st = p.runtime_status()
            pst = _ensure_pstate(p.pipeline_id)

            if st.is_pipeline_successful():
                continue
            if pst.get("drop", False):
                # Drop permanently
                continue

            # Keep in queue
            new_q.append(p)

            # Only consider runnable pipelines
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            score = _pipeline_remaining_ops_proxy(p)
            if best is None or score < best_score:
                best = p
                best_score = score

        s.queues[pri] = new_q
        return best

    def _choose_pool_order_for_priority(pri):
        """Order pools to reduce latency: for high priority, start with most headroom."""
        pool_ids = list(range(s.executor.num_pools))
        if pri in (Priority.QUERY, Priority.INTERACTIVE):
            # Prefer pools with more available CPU, then RAM.
            pool_ids.sort(
                key=lambda i: (s.executor.pools[i].avail_cpu_pool, s.executor.pools[i].avail_ram_pool),
                reverse=True,
            )
        else:
            # Batch: pack into pools with less headroom first (bin-pack-ish) to preserve burst capacity.
            pool_ids.sort(
                key=lambda i: (s.executor.pools[i].avail_cpu_pool, s.executor.pools[i].avail_ram_pool)
            )
        return pool_ids

    def _baseline_request(pool, pri, pid, avail_cpu, avail_ram, hp_backlog):
        """Compute cpu/ram request for a single operator."""
        pst = _ensure_pstate(pid)

        # If high-priority backlog exists, constrain batch aggressively.
        # (This single change often has a large impact on weighted latency.)
        if pri == Priority.BATCH_PIPELINE and hp_backlog:
            cpu_cap = min(avail_cpu, max(1.0, pool.max_cpu_pool * 0.25))
            ram_cap = min(avail_ram, max(1.0, pool.max_ram_pool * 0.25))
        else:
            cpu_cap = avail_cpu
            ram_cap = avail_ram

        # Give high priority enough CPU to reduce service time (vs. tiny slices in earlier attempt).
        if pri == Priority.QUERY:
            base_cpu = max(1.0, min(cpu_cap, max(2.0, pool.max_cpu_pool * 0.60)))
            base_ram = max(1.0, min(ram_cap, pool.max_ram_pool * 0.18))
        elif pri == Priority.INTERACTIVE:
            base_cpu = max(1.0, min(cpu_cap, max(2.0, pool.max_cpu_pool * 0.70)))
            base_ram = max(1.0, min(ram_cap, pool.max_ram_pool * 0.25))
        else:
            # Batch: throughput oriented, but will be capped when hp_backlog True.
            base_cpu = max(1.0, min(cpu_cap, pool.max_cpu_pool * 0.80))
            base_ram = max(1.0, min(ram_cap, pool.max_ram_pool * 0.35))

        cpu_req = base_cpu * pst.get("cpu_mult", 1.0)
        ram_req = base_ram * pst.get("ram_mult", 1.0)

        # Clamp to available
        cpu_req = max(1.0, min(cpu_req, cpu_cap, avail_cpu, pool.max_cpu_pool))
        ram_req = max(1.0, min(ram_req, ram_cap, avail_ram, pool.max_ram_pool))
        return cpu_req, ram_req

    # -----------------------------
    # 1) Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        _ensure_pstate(p.pipeline_id)
        s.queues[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # -----------------------------
    # 2) Build best-effort op -> pipeline_id map (for interpreting results)
    # -----------------------------
    # We try to map by Python object identity: result.ops contains operator objects; pipelines contain them in .values.
    # If this fails (due to copying), we simply won't do per-pipeline updates from results.
    op_to_pid = {}
    for pri in _priority_order():
        for p in s.queues[pri]:
            try:
                for op in getattr(p, "values", []):
                    op_to_pid[id(op)] = p.pipeline_id
            except Exception:
                continue

    # -----------------------------
    # 3) Update per-pipeline state from results
    # -----------------------------
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        # Try to find owning pipeline_id
        pid = None
        try:
            if getattr(r, "ops", None):
                for op in r.ops:
                    pid = op_to_pid.get(id(op))
                    if pid is not None:
                        break
        except Exception:
            pid = None

        # If we can't map, skip per-pipeline adjustment.
        if pid is None:
            continue

        pst = _ensure_pstate(pid)
        if _is_oom_error(getattr(r, "error", None)):
            # RAM-first backoff on OOM (bounded)
            pst["ram_mult"] = min(pst.get("ram_mult", 1.0) * 2.0, 32.0)
        else:
            # Non-OOM failures: don't keep retrying indefinitely (helps reduce wasted work).
            pst["drop"] = True

        # Track last pool for mild stickiness
        try:
            s.pipeline_pool_hint[pid] = r.pool_id
        except Exception:
            pass

    # -----------------------------
    # 4) Scheduling
    # -----------------------------
    suspensions = []
    assignments = []

    hp_backlog = _has_high_priority_backlog()

    # We schedule by priority, and within each priority we try multiple pools.
    # This helps ensure high-priority work grabs capacity quickly across pools.
    for pri in _priority_order():
        # For batch, if hp backlog exists, be conservative and schedule fewer placements.
        # (Still allow some progress to avoid total starvation.)
        per_pool_batch_limit = 1 if (pri == Priority.BATCH_PIPELINE and hp_backlog) else 10

        pool_order = _choose_pool_order_for_priority(pri)

        for pool_id in pool_order:
            pool = s.executor.pools[pool_id]
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                continue

            placed_in_this_pool = 0
            # Place multiple ops per pool (bounded) to improve packing/utilization.
            while avail_cpu > 0 and avail_ram > 0 and placed_in_this_pool < per_pool_batch_limit:
                p = _select_best_pipeline(pri)
                if p is None:
                    break

                # Mild pool stickiness: if we have a hint and this is high-priority,
                # try to keep it on the hinted pool when feasible.
                hinted = s.pipeline_pool_hint.get(p.pipeline_id)
                if pri in (Priority.QUERY, Priority.INTERACTIVE) and hinted is not None and hinted != pool_id:
                    # If current pool has very high headroom, ignore hint; otherwise try hinted next.
                    if not (pool.avail_cpu_pool >= (pool.max_cpu_pool * 0.75) and pool.avail_ram_pool >= (pool.max_ram_pool * 0.75)):
                        # Put it back and stop trying further in this pool for this pri.
                        s.queues[pri].append(p)
                        break

                st = p.runtime_status()
                pst = _ensure_pstate(p.pipeline_id)

                # Drop non-retriable failures / completed
                if st.is_pipeline_successful() or pst.get("drop", False):
                    continue

                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    # Not runnable; put it back
                    s.queues[pri].append(p)
                    break

                cpu_req, ram_req = _baseline_request(pool, pri, p.pipeline_id, avail_cpu, avail_ram, hp_backlog)
                if cpu_req <= 0 or ram_req <= 0 or cpu_req > avail_cpu or ram_req > avail_ram:
                    # Can't fit right now; put it back and stop filling this pool.
                    s.queues[pri].append(p)
                    break

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

                # Update local availability to prevent overscheduling in this tick
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                placed_in_this_pool += 1

                # Requeue pipeline for subsequent ops
                s.queues[pri].append(p)
                s.pipeline_pool_hint[p.pipeline_id] = pool_id

    return suspensions, assignments
