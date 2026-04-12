# policy_key: scheduler_iter_median_rich_005
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.058776
# generation_seconds: 54.37
# generated_at: 2026-04-12T01:47:03.876948
@register_scheduler_init(key="scheduler_iter_median_rich_005")
def scheduler_iter_median_rich_005_init(s):
    """Priority-aware RR + per-pipeline OOM learning (target: lower latency via fewer retries + less HoL blocking).

    Changes vs naive / prior attempt:
    - Strict priority across classes: QUERY > INTERACTIVE > BATCH, but with per-pool fill loops.
    - Smaller default RAM requests (improves concurrency / reduces queueing latency).
    - Per-pipeline RAM backoff on OOM using ExecutionResult.ops -> pipeline_id mapping (fast convergence, fewer repeated OOMs).
    - More aggressive batch throttling when high-priority backlog exists (keeps headroom for latency-sensitive work).
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-pipeline adaptive hints
    # ram_mult grows on OOM for that pipeline to avoid repeated failures.
    s.pstate = {}  # pipeline_id -> {"ram_mult": float, "non_oom_fail": bool}
    # Round-robin cursors per priority queue
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    # Map operator object -> pipeline_id so we can attribute OOMs precisely
    s.op_to_pipeline = {}


@register_scheduler(key="scheduler_iter_median_rich_005")
def scheduler_iter_median_rich_005_scheduler(s, results, pipelines):
    """
    Step:
    1) Enqueue new pipelines and build op->pipeline mapping.
    2) Process results: on OOM, increase that pipeline's ram_mult (only for the pipeline that failed).
    3) For each pool, repeatedly place 1 ready op at a time, prioritizing QUERY/INTERACTIVE to reduce latency.
    """
    def _ensure_pstate(pipeline_id):
        if pipeline_id not in s.pstate:
            s.pstate[pipeline_id] = {"ram_mult": 1.0, "non_oom_fail": False}
        return s.pstate[pipeline_id]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _has_hp_backlog():
        # High-priority backlog if any QUERY or INTERACTIVE pipeline has a ready op
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.queues[pri]:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
                    continue
                if st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True):
                    return True
        return False

    def _cleanup_and_pick_pipeline(pri):
        """RR pick within a priority queue; removes completed or terminal-failed pipelines."""
        q = s.queues[pri]
        if not q:
            return None

        n = len(q)
        start = s.rr_cursor[pri] % n
        for step in range(n):
            idx = (start + step) % n
            p = q[idx]
            st = p.runtime_status()

            # Drop completed
            if st.is_pipeline_successful():
                q.pop(idx)
                if idx < start:
                    start -= 1
                if not q:
                    s.rr_cursor[pri] = 0
                    return None
                n = len(q)
                start %= n
                continue

            # Drop pipelines we consider terminally failed (non-OOM failures)
            if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
                q.pop(idx)
                if idx < start:
                    start -= 1
                if not q:
                    s.rr_cursor[pri] = 0
                    return None
                n = len(q)
                start %= n
                continue

            s.rr_cursor[pri] = (idx + 1) % len(q)
            return p

        return None

    def _base_cpu(pool, pri):
        # Keep high-priority ops small to reduce HoL blocking; batch can use more when available.
        if pri == Priority.QUERY:
            return min(max(1.0, pool.max_cpu_pool * 0.20), 2.0)
        if pri == Priority.INTERACTIVE:
            return min(max(1.0, pool.max_cpu_pool * 0.30), 4.0)
        return max(1.0, pool.max_cpu_pool * 0.60)

    def _base_ram(pool, pri):
        # Default smaller RAM to increase concurrency; rely on per-pipeline backoff on OOM.
        if pri == Priority.QUERY:
            return max(1.0, pool.max_ram_pool * 0.06)
        if pri == Priority.INTERACTIVE:
            return max(1.0, pool.max_ram_pool * 0.10)
        return max(1.0, pool.max_ram_pool * 0.12)

    def _caps_for_priority(pool, pri, avail_cpu, avail_ram, hp_backlog):
        # When HP backlog exists, throttle batch harder to keep RAM headroom for fast admission.
        if pri == Priority.BATCH_PIPELINE and hp_backlog:
            return min(avail_cpu, pool.max_cpu_pool * 0.35), min(avail_ram, pool.max_ram_pool * 0.35)
        return avail_cpu, avail_ram

    # 1) Enqueue new pipelines + build op mapping
    for p in pipelines:
        _ensure_pstate(p.pipeline_id)
        s.queues[p.priority].append(p)
        # Best-effort: pipeline.values is described as the DAG of operators; treat as iterable.
        try:
            for op in p.values:
                s.op_to_pipeline[op] = p.pipeline_id
        except Exception:
            # If pipeline.values isn't iterable in some workloads, we just won't get op->pipeline attribution.
            pass

    if not pipelines and not results:
        return [], []

    # 2) Update state from results (OOM backoff per pipeline when possible)
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        is_oom = _is_oom_error(getattr(r, "error", None))
        # Attribute to pipelines via r.ops -> s.op_to_pipeline
        attributed_pids = set()
        for op in getattr(r, "ops", []) or []:
            pid = s.op_to_pipeline.get(op, None)
            if pid is not None:
                attributed_pids.add(pid)

        if not attributed_pids:
            # If we can't attribute, do a conservative adjustment only for that priority's queued pipelines:
            # OOM => modest bump for that priority to reduce repeated failures; Non-OOM => no retries if already FAILED.
            if is_oom:
                for p in s.queues.get(r.priority, []):
                    pst = _ensure_pstate(p.pipeline_id)
                    pst["ram_mult"] = min(pst["ram_mult"] * 1.25, 32.0)
            else:
                for p in s.queues.get(r.priority, []):
                    st = p.runtime_status()
                    if st.state_counts.get(OperatorState.FAILED, 0) > 0:
                        pst = _ensure_pstate(p.pipeline_id)
                        pst["non_oom_fail"] = True
            continue

        # Per-pipeline updates
        for pid in attributed_pids:
            pst = _ensure_pstate(pid)
            if is_oom:
                # Fast backoff to converge quickly and reduce latency lost to repeated OOM retries
                pst["ram_mult"] = min(pst["ram_mult"] * 2.0, 64.0)
            else:
                pst["non_oom_fail"] = True

    # 3) Assign work
    suspensions = []
    assignments = []

    hp_backlog = _has_hp_backlog()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Fill the pool incrementally: one ready op per assignment.
        while avail_cpu > 0 and avail_ram > 0:
            picked = None
            picked_pri = None
            picked_ops = None

            # Strict priority selection to reduce latency
            for pri in _prio_order():
                p = _cleanup_and_pick_pipeline(pri)
                if p is None:
                    continue

                st = p.runtime_status()
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    # Not runnable now; keep it around
                    s.queues[pri].append(p)
                    continue

                picked = p
                picked_pri = pri
                picked_ops = op_list
                break

            if picked is None:
                break

            cpu_cap, ram_cap = _caps_for_priority(pool, picked_pri, avail_cpu, avail_ram, hp_backlog)
            if cpu_cap <= 0 or ram_cap <= 0:
                # Can't fit anything of this priority right now in this pool; stop filling it.
                s.queues[picked_pri].append(picked)
                break

            pst = _ensure_pstate(picked.pipeline_id)

            cpu_req = _base_cpu(pool, picked_pri)
            ram_req = _base_ram(pool, picked_pri) * pst["ram_mult"]

            # Clamp to caps and available
            cpu_req = max(1.0, min(cpu_req, cpu_cap, avail_cpu, pool.max_cpu_pool))
            ram_req = max(1.0, min(ram_req, ram_cap, avail_ram, pool.max_ram_pool))

            # If we can't allocate at least minimal resources, stop
            if cpu_req <= 0 or ram_req <= 0:
                s.queues[picked_pri].append(picked)
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

            # Put pipeline back for future ops
            s.queues[picked_pri].append(picked)

    return suspensions, assignments
