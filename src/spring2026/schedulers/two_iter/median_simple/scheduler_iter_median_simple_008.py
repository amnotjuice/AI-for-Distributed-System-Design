# policy_key: scheduler_iter_median_simple_008
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045165
# generation_seconds: 35.12
# generated_at: 2026-04-12T01:12:21.900419
@register_scheduler_init(key="scheduler_iter_median_simple_008")
def scheduler_iter_median_simple_008_init(s):
    """Priority-first, pool-aware scheduler focused on reducing interactive/query latency.

    Incremental improvements over naive FIFO:
    - Strict priority ordering: QUERY > INTERACTIVE > BATCH_PIPELINE.
    - Pool partitioning when multiple pools exist:
        * pool 0 is "latency pool" (prefer QUERY/INTERACTIVE; batch only if no HP backlog)
        * other pools are "throughput pools" (prefer BATCH, but will run HP if present)
    - Resource shaping:
        * HP ops get a larger CPU share to finish sooner (lower tail/median latency).
        * Batch is capped when any HP backlog exists to preserve headroom.
    - Better failure adaptation (best-effort):
        * If we can map a result's op back to a pipeline, OOM => increase that pipeline's RAM multiplier.
        * Non-OOM failure => mark pipeline as non-retriable and stop scheduling it.
    - Round-robin within each priority queue for fairness (but never at the expense of HP latency).
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # per-pipeline adaptive hints
    # {"ram_mult": float, "cpu_mult": float, "drop": bool}
    s.pstate = {}
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }


@register_scheduler(key="scheduler_iter_median_simple_008")
def scheduler_iter_median_simple_008_scheduler(s, results, pipelines):
    """
    Scheduler step:
    - Enqueue incoming pipelines by priority.
    - Update pipeline-local backoff state from recent results (OOM => more RAM).
    - For each pool, schedule ready operators with strict priority and pool partitioning.
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

    def _iter_all_queued_pipelines():
        for pri in _priority_order():
            for p in s.queues[pri]:
                yield p

    def _build_op_to_pipeline_map():
        """Best-effort mapping from op object identity -> pipeline, for failure attribution."""
        op2p = {}
        for p in _iter_all_queued_pipelines():
            # Pipeline.values: "DAG of operators" (may be list-like or dict-like)
            vals = getattr(p, "values", None)
            if vals is None:
                continue
            try:
                it = vals.values() if hasattr(vals, "values") else vals
                for op in it:
                    op2p[id(op)] = p
            except Exception:
                # If values isn't iterable in this simulator version, skip attribution.
                pass
        return op2p

    def _pipeline_has_ready_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return False
        if s.pstate.get(p.pipeline_id, {}).get("drop", False):
            return False
        # if FAILED exists but not dropped, it might be an OOM retry (FAILED is assignable)
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return bool(ops)

    def _hp_backlog_exists():
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.queues[pri]:
                if _pipeline_has_ready_op(p):
                    return True
        return False

    def _pop_rr_ready(pri):
        """Round-robin: find next pipeline of this priority that has a ready op."""
        q = s.queues[pri]
        if not q:
            return None

        n = len(q)
        start = s.rr_cursor[pri] % n

        checked = 0
        idx = start
        while checked < n and q:
            p = q[idx]
            st = p.runtime_status()
            pid = p.pipeline_id
            pst = _ensure_pstate(pid)

            # Drop successful pipelines
            if st.is_pipeline_successful():
                q.pop(idx)
                if not q:
                    s.rr_cursor[pri] = 0
                    return None
                n -= 1
                idx %= n
                continue

            # Drop pipelines flagged as non-retriable
            if pst.get("drop", False):
                q.pop(idx)
                if not q:
                    s.rr_cursor[pri] = 0
                    return None
                n -= 1
                idx %= n
                continue

            # If no ready ops yet, keep it in queue but skip for now
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not ops:
                idx = (idx + 1) % n
                checked += 1
                continue

            # Advance cursor beyond this pipeline for next RR
            s.rr_cursor[pri] = (idx + 1) % n
            return p

        return None

    def _pick_pool_preference(pool_id, hp_backlog):
        """Partitioning:
        - pool 0: protect latency (avoid batch if HP backlog)
        - other pools: run batch preferentially but allow HP
        """
        if s.executor.num_pools <= 1:
            return _priority_order()

        if pool_id == 0:
            if hp_backlog:
                return [Priority.QUERY, Priority.INTERACTIVE]  # no batch on latency pool if HP waiting
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        else:
            # Throughput pools: batch first, but if HP exists they can still take HP
            if hp_backlog:
                return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            return [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]

    def _shape_request(pool, pri, pid, avail_cpu, avail_ram, hp_backlog):
        """CPU-heavy shaping for HP to reduce completion time; cap batch under HP backlog."""
        pst = _ensure_pstate(pid)

        # Defaults based on pool size to remain portable across configs
        max_cpu = max(1.0, float(pool.max_cpu_pool))
        max_ram = max(1.0, float(pool.max_ram_pool))

        # Base sizing: give HP more CPU, modest RAM; batch gets moderate CPU unless HP backlog exists.
        if pri == Priority.QUERY:
            base_cpu = max(1.0, min(max_cpu * 0.80, 8.0))
            base_ram = max(1.0, max_ram * 0.15)
        elif pri == Priority.INTERACTIVE:
            base_cpu = max(1.0, min(max_cpu * 0.70, 8.0))
            base_ram = max(1.0, max_ram * 0.20)
        else:
            # Batch: avoid eating the pool when HP backlog exists (preserve headroom)
            if hp_backlog:
                base_cpu = max(1.0, min(max_cpu * 0.35, 4.0))
                base_ram = max(1.0, max_ram * 0.25)
            else:
                base_cpu = max(1.0, min(max_cpu * 0.60, 8.0))
                base_ram = max(1.0, max_ram * 0.35)

        cpu_req = base_cpu * pst["cpu_mult"]
        ram_req = base_ram * pst["ram_mult"]

        # Clamp to availability and pool maxima; always request at least 1 unit
        cpu_req = max(1.0, min(cpu_req, float(avail_cpu), max_cpu))
        ram_req = max(1.0, min(ram_req, float(avail_ram), max_ram))

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
    # Update state from results (best-effort attribution)
    # -----------------------------
    op2p = _build_op_to_pipeline_map()

    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        # Find owning pipeline if possible by matching op identities
        owner = None
        try:
            for op in getattr(r, "ops", []) or []:
                owner = op2p.get(id(op))
                if owner is not None:
                    break
        except Exception:
            owner = None

        if owner is None:
            # No attribution possible; skip per-pipeline adaptation.
            continue

        pst = _ensure_pstate(owner.pipeline_id)
        if _is_oom_error(getattr(r, "error", None)):
            # OOM => increase RAM for this pipeline's future ops; keep CPU as-is.
            pst["ram_mult"] = min(pst["ram_mult"] * 2.0, 32.0)
        else:
            # Non-OOM failure => drop pipeline to avoid wasting capacity and inflating latency.
            pst["drop"] = True

    # -----------------------------
    # Scheduling
    # -----------------------------
    suspensions = []
    assignments = []

    hp_backlog = _hp_backlog_exists()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        pri_order = _pick_pool_preference(pool_id, hp_backlog)

        # Fill the pool with multiple small assignments to reduce queueing delay for HP.
        # We still assign one op at a time (atomic scheduling unit), but we loop until resources run out.
        while avail_cpu > 0 and avail_ram > 0:
            chosen_p = None
            chosen_pri = None

            # Strict priority within this pool's allowed order
            for pri in pri_order:
                p = _pop_rr_ready(pri)
                if p is None:
                    continue

                # If this is the latency pool and batch isn't allowed right now, enforce it
                if s.executor.num_pools > 1 and pool_id == 0 and hp_backlog and pri == Priority.BATCH_PIPELINE:
                    # push back and skip
                    s.queues[pri].append(p)
                    continue

                chosen_p = p
                chosen_pri = pri
                break

            if chosen_p is None:
                break

            st = chosen_p.runtime_status()
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not actually runnable; push back and stop trying this cycle to avoid thrash
                s.queues[chosen_pri].append(chosen_p)
                break

            cpu_req, ram_req = _shape_request(pool, chosen_pri, chosen_p.pipeline_id, avail_cpu, avail_ram, hp_backlog)

            # If we can't even fit minimal request, stop scheduling on this pool
            if cpu_req <= 0 or ram_req <= 0 or cpu_req > avail_cpu or ram_req > avail_ram:
                s.queues[chosen_pri].append(chosen_p)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen_p.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            # Update local availability to prevent oversubscription within the tick
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Re-enqueue pipeline for subsequent ops
            s.queues[chosen_pri].append(chosen_p)

            # Recompute hp_backlog cheaply: if we just scheduled HP, backlog might still exist; if we scheduled batch, it doesn't help.
            # Keep it simple: only recompute when we scheduled QUERY/INTERACTIVE on latency pool.
            if chosen_pri in (Priority.QUERY, Priority.INTERACTIVE) and pool_id == 0:
                hp_backlog = _hp_backlog_exists()

    return suspensions, assignments
