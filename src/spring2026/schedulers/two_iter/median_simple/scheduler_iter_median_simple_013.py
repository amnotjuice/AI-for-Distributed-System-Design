# policy_key: scheduler_iter_median_simple_013
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.060131
# generation_seconds: 39.73
# generated_at: 2026-04-12T01:38:27.563915
@register_scheduler_init(key="scheduler_iter_median_simple_013")
def scheduler_iter_median_simple_013_init(s):
    """Iteration 2: Priority-first, per-pipeline OOM backoff, and anti-batch-HoL shaping.

    Changes vs previous attempt (aimed at reducing median weighted latency):
    - Correct per-pipeline failure handling by tracking container_id -> pipeline_id mappings.
      This avoids globally inflating RAM for an entire priority class on a single OOM.
    - Priority-first filling across pools: QUERY > INTERACTIVE > BATCH.
    - Stronger batch gating: if any high-priority backlog exists, batch is limited to a small
      share per pool to preserve headroom and reduce high-priority queueing delays.
    - Smaller CPU slices for high priority to increase concurrency and reduce queueing latency.
    - Mild SRPT-ish tie-break among pipelines: prefer those with fewer remaining operators.
    """
    # Pipelines waiting by priority
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline adaptive state
    # ram_mult increases on OOM for that pipeline; cpu_mult lightly adapts (optional)
    # non_oom_fail indicates we should stop retrying if FAILED ops exist
    s.pstate = {}  # pipeline_id -> dict

    # Track enqueue time (tick counter) for basic aging/fairness
    s.now_tick = 0
    s.enqueued_at = {}  # pipeline_id -> tick

    # Map running container_id back to pipeline_id so we can update the right pipeline on result
    s.container_to_pipeline = {}  # container_id -> pipeline_id

    # Round-robin cursors per priority (used after scoring)
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }


@register_scheduler(key="scheduler_iter_median_simple_013")
def scheduler_iter_median_simple_013_scheduler(s, results, pipelines):
    """
    Scheduler step:
    1) Enqueue new pipelines into priority queues.
    2) Update per-pipeline state from execution results (OOM -> bump RAM multiplier, etc.).
    3) Compute whether high-priority backlog exists; if so, cap batch usage per pool.
    4) Fill each pool with ready ops, prioritizing QUERY/INTERACTIVE and using small CPU slices
       for better concurrency, while avoiding over-aggressive RAM underprovisioning.
    """
    # -----------------------------
    # Local helpers
    # -----------------------------
    def _ensure_pstate(pid):
        if pid not in s.pstate:
            s.pstate[pid] = {
                "ram_mult": 1.0,
                "cpu_mult": 1.0,
                "non_oom_fail": False,
                "oom_count": 0,
            }
        return s.pstate[pid]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _remaining_ops_count(p):
        # Best-effort remaining work estimate using pipeline values.
        # If structure differs, fall back to 0 to avoid crashing.
        try:
            st = p.runtime_status()
            # Values is DAG of operators; assume len(values) works.
            total = len(p.values) if hasattr(p, "values") and p.values is not None else 0
            done = st.state_counts.get(OperatorState.COMPLETED, 0)
            # Failed counts as remaining unless we marked as non-retriable
            rem = max(0, total - done)
            return rem
        except Exception:
            return 0

    def _has_runnable_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return False
        if st.state_counts.get(OperatorState.FAILED, 0) > 0 and _ensure_pstate(p.pipeline_id)["non_oom_fail"]:
            return False
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return bool(ops)

    def _hp_backlog_exists():
        # Any runnable QUERY or INTERACTIVE work anywhere
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.queues[pri]:
                if _has_runnable_op(p):
                    return True
        return False

    def _clean_queue(pri):
        # Remove completed or non-retriable failed pipelines
        q = s.queues[pri]
        keep = []
        for p in q:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            if st.state_counts.get(OperatorState.FAILED, 0) > 0 and _ensure_pstate(p.pipeline_id)["non_oom_fail"]:
                continue
            keep.append(p)
        s.queues[pri] = keep

    def _pick_pipeline(pri):
        """Pick next pipeline using a simple score: SRPT-ish + aging; RR as tie-break."""
        q = s.queues[pri]
        if not q:
            return None

        # Rotate starting point for RR fairness
        start = s.rr_cursor[pri] % max(1, len(q))

        best_idx = None
        best_score = None

        for k in range(len(q)):
            idx = (start + k) % len(q)
            p = q[idx]
            if not _has_runnable_op(p):
                continue

            pid = p.pipeline_id
            age = s.now_tick - s.enqueued_at.get(pid, s.now_tick)
            rem = _remaining_ops_count(p)

            # Lower score is better.
            # - Prefer fewer remaining ops (SRPT-ish) to reduce median completion/latency.
            # - Give a small boost to older pipelines to avoid starvation.
            score = (rem, -min(age, 10_000))

            if best_score is None or score < best_score:
                best_score = score
                best_idx = idx

        if best_idx is None:
            return None

        s.rr_cursor[pri] = (best_idx + 1) % max(1, len(q))
        return q[best_idx]

    def _request_size(pool, pri, pid, avail_cpu, avail_ram, hp_backlog):
        """Compute cpu/ram request; tuned for latency: small CPU slices for HP, safer RAM."""
        pst = _ensure_pstate(pid)

        # Base CPU targets: keep HP small to increase concurrency and reduce queueing latency.
        if pri == Priority.QUERY:
            base_cpu = 1.0
            base_ram_frac = 0.14  # slightly higher than before to reduce OOM retry latency
        elif pri == Priority.INTERACTIVE:
            base_cpu = min(2.0, max(1.0, pool.max_cpu_pool * 0.20))
            base_ram_frac = 0.22
        else:
            # Batch: larger slices when allowed (throughput), but will be capped under HP backlog.
            base_cpu = max(2.0, pool.max_cpu_pool * 0.50)
            base_ram_frac = 0.35

        # Apply per-pipeline multipliers (RAM reacts strongly to OOM)
        cpu_req = base_cpu * pst["cpu_mult"]
        ram_req = (pool.max_ram_pool * base_ram_frac) * pst["ram_mult"]

        # Soft batch gating: keep strong headroom if HP backlog exists
        if pri == Priority.BATCH_PIPELINE and hp_backlog:
            # Let batch use only a small fraction of whatever is currently free.
            cpu_req = min(cpu_req, max(1.0, avail_cpu * 0.25))
            ram_req = min(ram_req, max(1.0, avail_ram * 0.25))

        # Clamp to available and pool maxima; ensure minimums
        cpu_req = max(1.0, min(cpu_req, avail_cpu, pool.max_cpu_pool))
        ram_req = max(1.0, min(ram_req, avail_ram, pool.max_ram_pool))

        return cpu_req, ram_req

    # -----------------------------
    # Tick / enqueue
    # -----------------------------
    s.now_tick += 1

    for p in pipelines:
        _ensure_pstate(p.pipeline_id)
        if p.pipeline_id not in s.enqueued_at:
            s.enqueued_at[p.pipeline_id] = s.now_tick
        s.queues[p.priority].append(p)

    # Early exit
    if not pipelines and not results:
        return [], []

    # -----------------------------
    # Update state from results (per-pipeline via container_id mapping)
    # -----------------------------
    for r in results:
        cid = getattr(r, "container_id", None)
        pid = s.container_to_pipeline.get(cid, None) if cid is not None else None

        # If container finished, we can forget the mapping to prevent growth
        if cid is not None and cid in s.container_to_pipeline:
            # Remove mapping on any terminal result; safe even if op is retried later
            try:
                if not hasattr(r, "failed") or r.failed() or True:
                    s.container_to_pipeline.pop(cid, None)
            except Exception:
                s.container_to_pipeline.pop(cid, None)

        if not (hasattr(r, "failed") and r.failed()):
            continue

        # If we can't map result back to pipeline, skip (no global poisoning)
        if pid is None:
            continue

        pst = _ensure_pstate(pid)

        if _is_oom_error(getattr(r, "error", None)):
            pst["oom_count"] += 1
            # Exponential-ish RAM backoff with a cap; OOM retries are very costly for latency
            pst["ram_mult"] = min(max(pst["ram_mult"] * 1.8, 1.3), 32.0)
            # Slightly reduce cpu_mult so we don't grab extra CPU while still under-RAMed
            pst["cpu_mult"] = max(0.8, pst["cpu_mult"] * 0.95)
        else:
            # Non-OOM failures: stop retrying to avoid wasting time/latency
            pst["non_oom_fail"] = True

    # Clean queues of completed/non-retriable pipelines
    for pri in _priority_order():
        _clean_queue(pri)

    # -----------------------------
    # Scheduling
    # -----------------------------
    suspensions = []
    assignments = []

    hp_backlog = _hp_backlog_exists()

    # Fill each pool. We allow multiple assignments per pool per tick.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # If nothing available, continue
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Greedy fill: always try higher priorities first
        while avail_cpu > 0 and avail_ram > 0:
            chosen_p = None
            chosen_pri = None
            chosen_ops = None

            for pri in _priority_order():
                p = _pick_pipeline(pri)
                if p is None:
                    continue

                st = p.runtime_status()
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    continue

                chosen_p = p
                chosen_pri = pri
                chosen_ops = op_list
                break

            if chosen_p is None:
                break

            cpu_req, ram_req = _request_size(
                pool=pool,
                pri=chosen_pri,
                pid=chosen_p.pipeline_id,
                avail_cpu=avail_cpu,
                avail_ram=avail_ram,
                hp_backlog=hp_backlog,
            )

            # If we cannot fit even minimal request, stop for this pool
            if cpu_req <= 0 or ram_req <= 0:
                break

            a = Assignment(
                ops=chosen_ops,
                cpu=cpu_req,
                ram=ram_req,
                priority=chosen_p.priority,
                pool_id=pool_id,
                pipeline_id=chosen_p.pipeline_id,
            )
            assignments.append(a)

            # Record container->pipeline mapping if container_id becomes available later:
            # We don't have it at assignment time, so we instead map after results.
            # However, many simulators populate container_id only in results.
            # As a workaround, if ops have a stable identifier, we could map op->pid,
            # but that's not part of the provided interface. We keep container mapping
            # best-effort: if future results include unknown cid, we won't update pst.
            #
            # Some Eudoxia implementations attach a predicted container_id to Assignment;
            # if present, record it.
            cid = getattr(a, "container_id", None)
            if cid is not None:
                s.container_to_pipeline[cid] = chosen_p.pipeline_id

            # Decrement local availability
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # If HP backlog exists, stop giving batch too much even if it could fit:
            # the batch gating already caps sizes, but this also prevents packing many tiny
            # batch tasks that could still crowd out HP arrivals next tick.
            if hp_backlog and chosen_pri == Priority.BATCH_PIPELINE:
                break

    return suspensions, assignments
