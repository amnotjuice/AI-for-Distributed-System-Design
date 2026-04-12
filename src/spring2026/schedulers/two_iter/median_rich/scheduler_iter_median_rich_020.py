# policy_key: scheduler_iter_median_rich_020
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.064348
# generation_seconds: 45.95
# generated_at: 2026-04-12T01:59:30.380352
@register_scheduler_init(key="scheduler_iter_median_rich_020")
def scheduler_iter_median_rich_020_init(s):
    """Priority-aware RR scheduler with per-operator OOM learning + batch caps.

    Goals vs previous iteration:
    - Reduce latency by (a) always serving QUERY/INTERACTIVE first, (b) avoiding global RAM inflation,
      and (c) learning RAM/CPU needs per operator signature from actual failures.
    - Reduce OOM churn by retrying FAILED ops with increased RAM only for that operator signature.
    - Keep batch from consuming the whole pool when high-priority backlog exists.

    Key ideas:
    - Maintain 3 RR queues by priority.
    - Track a stable operator signature -> (ram_mult, cpu_mult) hints.
    - Track op-object-id -> (pipeline_id, signature) for mapping ExecutionResult back to the hint state.
    - Soft-cap batch resource usage when there is pending high-priority work.
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

    # Operator signature -> hint state
    # { sig: {"ram_mult": float, "cpu_mult": float, "ooms": int, "timeouts": int, "succ": int} }
    s.op_hints = {}

    # Map op-object identity to metadata from our last assignment, so results can update hints.
    # { id(op): {"sig": sig, "pipeline_id": str/int, "priority": Priority} }
    s.op_identity = {}

    # Pipeline failure behavior: if we detect non-OOM failure, don't keep retrying forever.
    # { pipeline_id: {"non_oom_fail": bool} }
    s.pipeline_flags = {}


@register_scheduler(key="scheduler_iter_median_rich_020")
def scheduler_iter_median_rich_020_scheduler(s, results, pipelines):
    """Scheduler step: enqueue -> learn from results -> assign ready ops by priority with sizing hints."""
    # ----------------------------
    # Helpers
    # ----------------------------
    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return "timeout" in msg or "timed out" in msg or "deadline" in msg

    def _op_signature(op):
        # Try to form a stable signature across pipeline instances.
        # If the simulator provides any stable ids, use them; otherwise fall back to repr/type.
        for attr in ("op_id", "operator_id", "node_id", "name", "key"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if v is not None:
                    return (attr, v)
        # "repr(op)" may include memory addresses; keep it bounded to reduce churn.
        r = repr(op)
        if len(r) > 160:
            r = r[:160]
        return ("repr", type(op).__name__, r)

    def _ensure_hint(sig):
        if sig not in s.op_hints:
            s.op_hints[sig] = {"ram_mult": 1.0, "cpu_mult": 1.0, "ooms": 0, "timeouts": 0, "succ": 0}
        return s.op_hints[sig]

    def _ensure_pipeline_flags(pipeline_id):
        if pipeline_id not in s.pipeline_flags:
            s.pipeline_flags[pipeline_id] = {"non_oom_fail": False}
        return s.pipeline_flags[pipeline_id]

    def _queue_has_hp_backlog():
        # Backlog if there exists an assignable op in QUERY or INTERACTIVE
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.queues[pri]:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                pf = _ensure_pipeline_flags(p.pipeline_id)
                if pf["non_oom_fail"] and st.state_counts.get(OperatorState.FAILED, 0) > 0:
                    continue
                if st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True):
                    return True
        return False

    def _pop_rr(pri):
        q = s.queues[pri]
        if not q:
            return None
        n = len(q)
        start = s.rr_cursor[pri] % n
        for k in range(n):
            idx = (start + k) % n
            p = q[idx]
            st = p.runtime_status()

            # Drop completed pipelines
            if st.is_pipeline_successful():
                q.pop(idx)
                if q:
                    s.rr_cursor[pri] = idx % len(q)
                else:
                    s.rr_cursor[pri] = 0
                return _pop_rr(pri)

            # Drop pipelines with non-OOM failures (we don't want to spin on them)
            pf = _ensure_pipeline_flags(p.pipeline_id)
            if pf["non_oom_fail"] and st.state_counts.get(OperatorState.FAILED, 0) > 0:
                q.pop(idx)
                if q:
                    s.rr_cursor[pri] = idx % len(q)
                else:
                    s.rr_cursor[pri] = 0
                return _pop_rr(pri)

            # Keep it; advance cursor
            s.rr_cursor[pri] = (idx + 1) % len(q)
            return q.pop(idx)
        return None

    def _base_request(pool, pri):
        # Base CPU/RAM sizes (fractions of pool max) before hint multipliers.
        # High priority: smaller RAM to fit quickly but not tiny; moderate CPU to reduce runtime.
        if pri == Priority.QUERY:
            return (max(1.0, pool.max_cpu_pool * 0.30), max(1.0, pool.max_ram_pool * 0.15))
        if pri == Priority.INTERACTIVE:
            return (max(1.0, pool.max_cpu_pool * 0.40), max(1.0, pool.max_ram_pool * 0.25))
        # Batch: larger chunks for throughput, but will be capped when HP backlog exists.
        return (max(1.0, pool.max_cpu_pool * 0.65), max(1.0, pool.max_ram_pool * 0.35))

    def _cap_for_priority(pool, pri, avail_cpu, avail_ram, hp_backlog):
        # Soft headroom protection: when HP backlog exists, cap batch to leave room.
        if pri != Priority.BATCH_PIPELINE or not hp_backlog:
            return avail_cpu, avail_ram

        # Leave at least ~30% of pool resources unused by batch so HP can start promptly.
        cpu_cap = min(avail_cpu, max(1.0, pool.max_cpu_pool * 0.70))
        ram_cap = min(avail_ram, max(1.0, pool.max_ram_pool * 0.70))
        return cpu_cap, ram_cap

    def _compute_request(pool, pri, sig, avail_cpu, avail_ram, hp_backlog):
        hint = _ensure_hint(sig)
        base_cpu, base_ram = _base_request(pool, pri)
        cpu_cap, ram_cap = _cap_for_priority(pool, pri, avail_cpu, avail_ram, hp_backlog)

        cpu = base_cpu * hint["cpu_mult"]
        ram = base_ram * hint["ram_mult"]

        # Clamp
        cpu = max(1.0, min(cpu, cpu_cap, avail_cpu, pool.max_cpu_pool))
        ram = max(1.0, min(ram, ram_cap, avail_ram, pool.max_ram_pool))
        return cpu, ram

    # ----------------------------
    # 1) Enqueue new pipelines
    # ----------------------------
    for p in pipelines:
        _ensure_pipeline_flags(p.pipeline_id)
        s.queues[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # ----------------------------
    # 2) Learn from results (per-operator, not global)
    # ----------------------------
    for r in results:
        # If we can map back to operator signature, update hints.
        # r.ops is a list; we update each op in it (usually size 1).
        ops = getattr(r, "ops", None) or []
        for op in ops:
            meta = s.op_identity.get(id(op))
            sig = meta["sig"] if meta and "sig" in meta else _op_signature(op)
            hint = _ensure_hint(sig)

            if hasattr(r, "failed") and r.failed():
                err = getattr(r, "error", None)
                if _is_oom_error(err):
                    hint["ooms"] += 1
                    # Strong RAM bump for this signature; keep CPU stable (OOM is RAM issue).
                    hint["ram_mult"] = min(hint["ram_mult"] * 1.9, 32.0)
                elif _is_timeout_error(err):
                    hint["timeouts"] += 1
                    # Timeouts can benefit from extra CPU (within reason) and a modest RAM bump.
                    hint["cpu_mult"] = min(hint["cpu_mult"] * 1.25, 8.0)
                    hint["ram_mult"] = min(hint["ram_mult"] * 1.10, 32.0)
                else:
                    # Non-OOM failures: mark pipeline as not retryable if we can identify it.
                    if meta and "pipeline_id" in meta:
                        pf = _ensure_pipeline_flags(meta["pipeline_id"])
                        pf["non_oom_fail"] = True
            else:
                hint["succ"] += 1
                # Gentle decay after successes to reduce memory over-allocation over time.
                # (But don't decay below 1.0; we don't want to systematically under-provision.)
                if hint["succ"] % 5 == 0:
                    hint["ram_mult"] = max(1.0, hint["ram_mult"] * 0.95)
                    hint["cpu_mult"] = max(1.0, hint["cpu_mult"] * 0.98)

    # ----------------------------
    # 3) Assign work per pool (HP first, then batch)
    # ----------------------------
    suspensions = []
    assignments = []

    hp_backlog = _queue_has_hp_backlog()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep placing ops until we can't fit more
        while avail_cpu > 0 and avail_ram > 0:
            chosen = None
            chosen_pri = None
            chosen_op = None

            # Strict priority for latency
            for pri in _priority_order():
                p = _pop_rr(pri)
                if p is None:
                    continue

                st = p.runtime_status()
                ops_ready = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]

                if not ops_ready:
                    # Not runnable now; put it back.
                    s.queues[pri].append(p)
                    continue

                chosen = p
                chosen_pri = pri
                chosen_op = ops_ready[0]
                break

            if chosen is None:
                break

            sig = _op_signature(chosen_op)

            cpu_req, ram_req = _compute_request(
                pool=pool,
                pri=chosen_pri,
                sig=sig,
                avail_cpu=avail_cpu,
                avail_ram=avail_ram,
                hp_backlog=hp_backlog,
            )

            # If we can't fit minimal request, put it back and stop on this pool.
            if cpu_req <= 0 or ram_req <= 0 or cpu_req > avail_cpu or ram_req > avail_ram:
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

            # Record mapping so results can update the correct signature/pipeline flags.
            s.op_identity[id(chosen_op)] = {
                "sig": sig,
                "pipeline_id": chosen.pipeline_id,
                "priority": chosen.priority,
            }

            # Decrement local view to avoid overscheduling in this tick.
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Put pipeline back so it can be considered again later.
            s.queues[chosen_pri].append(chosen)

    return suspensions, assignments
