# policy_key: scheduler_iter_median_simple_020
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048427
# generation_seconds: 39.98
# generated_at: 2026-04-12T01:42:51.732523
@register_scheduler_init(key="scheduler_iter_median_simple_020")
def scheduler_iter_median_simple_020_init(s):
    """Priority-first, latency-oriented scheduler with small SRPT-like tie-breaks.

    Changes vs the previous attempt to reduce weighted median latency:
    - Strict priority across QUERY > INTERACTIVE > BATCH (never let batch jump ahead).
    - Better sizing for high priority: give QUERY/INTERACTIVE more CPU by default to finish sooner.
    - Soft batch caps when any high-priority backlog exists (keep headroom).
    - Per-pipeline OOM backoff using best-effort mapping from ExecutionResult.ops -> pipeline_id.
    - SRPT-like within priority: prefer pipelines with fewer remaining ops (proxy for shorter time).
    - No duplicate-queue churn: maintain per-priority ordered lists + membership sets.
    """
    s.q = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}
    s.qset = {Priority.QUERY: set(), Priority.INTERACTIVE: set(), Priority.BATCH_PIPELINE: set()}
    # per-pipeline adaptive hints
    # ram_mult grows on OOM; cpu_mult can be nudged later (kept constant for now)
    s.pstate = {}  # pipeline_id -> {"ram_mult": float, "cpu_mult": float, "drop": bool}
    # simple aging to avoid indefinite starvation: when batch waits "too long", allow a little more
    s.batch_boost = 0.0  # grows slowly when HP backlog persists


@register_scheduler(key="scheduler_iter_median_simple_020")
def scheduler_iter_median_simple_020_scheduler(s, results, pipelines):
    """
    Step:
    1) Enqueue pipelines by priority (deduped).
    2) Update pstate based on results (OOM => increase ram_mult; non-OOM fail => mark drop).
    3) For each pool, greedily assign ready ops in strict priority order.
       Within a priority class, pick "shortest" pipeline first by remaining-op count.
    """
    def _ensure_pstate(pid):
        if pid not in s.pstate:
            s.pstate[pid] = {"ram_mult": 1.0, "cpu_mult": 1.0, "drop": False}
        return s.pstate[pid]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _infer_pipeline_id_from_result(r):
        # Best-effort: ExecutionResult.ops may hold Operator objects that carry pipeline_id
        try:
            ops = getattr(r, "ops", None)
            if ops:
                pid = getattr(ops[0], "pipeline_id", None)
                if pid is not None:
                    return pid
        except Exception:
            pass
        return None

    def _enqueue(p):
        _ensure_pstate(p.pipeline_id)
        pri = p.priority
        if p.pipeline_id not in s.qset[pri]:
            s.q[pri].append(p)
            s.qset[pri].add(p.pipeline_id)

    def _drop_from_queue(pri, pid):
        if pid in s.qset[pri]:
            s.qset[pri].remove(pid)
            # linear removal is fine for small simulations; keeps implementation robust
            s.q[pri] = [p for p in s.q[pri] if p.pipeline_id != pid]

    def _pipeline_runnable_ops(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return []
        if s.pstate.get(p.pipeline_id, {}).get("drop", False):
            # if we've decided to drop it, don't retry it
            return []
        return st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)

    def _remaining_ops_proxy(p):
        # proxy for SRPT: fewer remaining (non-completed) ops => likely shorter remaining time
        st = p.runtime_status()
        counts = getattr(st, "state_counts", {}) or {}
        pending = counts.get(OperatorState.PENDING, 0)
        failed = counts.get(OperatorState.FAILED, 0)
        assigned = counts.get(OperatorState.ASSIGNED, 0)
        running = counts.get(OperatorState.RUNNING, 0)
        susp = counts.get(OperatorState.SUSPENDING, 0)
        # exclude COMPLETED; include everything else as "remaining"
        return pending + failed + assigned + running + susp

    def _has_hp_backlog():
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.q[pri]:
                if _pipeline_runnable_ops(p):
                    return True
        return False

    def _pick_next_pipeline(pri):
        # Choose runnable pipeline with smallest remaining-op proxy (SRPT-like).
        best = None
        best_score = None
        for p in s.q[pri]:
            ops = _pipeline_runnable_ops(p)
            if not ops:
                continue
            score = _remaining_ops_proxy(p)
            if best is None or score < best_score:
                best = p
                best_score = score
        return best

    def _base_request(pool, pri):
        # Latency-oriented CPU sizing:
        # - QUERY: relatively beefy to finish quickly (reduce tail/median queueing & runtime).
        # - INTERACTIVE: moderate-high.
        # - BATCH: conservative under contention.
        if pri == Priority.QUERY:
            cpu = max(1.0, min(pool.max_cpu_pool, max(2.0, pool.max_cpu_pool * 0.60)))
            ram = max(1.0, pool.max_ram_pool * 0.18)
        elif pri == Priority.INTERACTIVE:
            cpu = max(1.0, min(pool.max_cpu_pool, max(2.0, pool.max_cpu_pool * 0.45)))
            ram = max(1.0, pool.max_ram_pool * 0.25)
        else:
            cpu = max(1.0, min(pool.max_cpu_pool, max(1.0, pool.max_cpu_pool * 0.35)))
            ram = max(1.0, pool.max_ram_pool * 0.30)
        return cpu, ram

    def _cap_for_priority(pool, pri, avail_cpu, avail_ram, hp_backlog):
        # Keep headroom for high priority when backlog exists.
        if pri != Priority.BATCH_PIPELINE:
            return avail_cpu, avail_ram
        if not hp_backlog:
            return avail_cpu, avail_ram
        # Allow batch to use only a limited share; slightly relax with batch_boost to prevent starvation.
        cpu_cap = min(avail_cpu, pool.max_cpu_pool * (0.45 + min(0.15, s.batch_boost)))
        ram_cap = min(avail_ram, pool.max_ram_pool * (0.50 + min(0.10, s.batch_boost)))
        return cpu_cap, ram_cap

    # 1) Enqueue new pipelines
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # 2) Update state from results
    for r in results:
        if hasattr(r, "failed") and r.failed():
            pid = _infer_pipeline_id_from_result(r)
            if _is_oom_error(getattr(r, "error", None)):
                if pid is not None:
                    pst = _ensure_pstate(pid)
                    pst["ram_mult"] = min(pst["ram_mult"] * 2.0, 32.0)
                else:
                    # If we can't map, conservatively bump RAM for queued pipelines of same priority.
                    for p in s.q[r.priority]:
                        pst = _ensure_pstate(p.pipeline_id)
                        pst["ram_mult"] = min(pst["ram_mult"] * 1.25, 32.0)
            else:
                # Non-OOM failure: don't keep retrying forever; mark drop when identifiable.
                if pid is not None:
                    pst = _ensure_pstate(pid)
                    pst["drop"] = True

    # Garbage-collect completed/dropped pipelines from queues (keeps selection fast & avoids churn)
    for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        newq = []
        newset = set()
        for p in s.q[pri]:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            if s.pstate.get(p.pipeline_id, {}).get("drop", False):
                continue
            newq.append(p)
            newset.add(p.pipeline_id)
        s.q[pri] = newq
        s.qset[pri] = newset

    # Update batch_boost: if HP backlog persists, gradually allow a bit more batch to avoid lockout.
    hp_backlog = _has_hp_backlog()
    if hp_backlog:
        s.batch_boost = min(0.30, s.batch_boost + 0.02)
    else:
        s.batch_boost = max(0.0, s.batch_boost - 0.05)

    suspensions = []
    assignments = []

    # 3) Assign work per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Greedily place multiple assignments per pool tick to reduce queueing.
        while avail_cpu > 0 and avail_ram > 0:
            chosen_pri = None
            chosen_pipeline = None

            # Strict priority scan
            for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                p = _pick_next_pipeline(pri)
                if p is not None:
                    chosen_pri = pri
                    chosen_pipeline = p
                    break

            if chosen_pipeline is None:
                break

            # Choose a small batch of ready ops from that pipeline (at most 2) to reduce its latency
            # while not letting one pipeline monopolize the pool.
            runnable_ops = _pipeline_runnable_ops(chosen_pipeline)
            if not runnable_ops:
                break
            op_list = runnable_ops[:2] if chosen_pri != Priority.BATCH_PIPELINE else runnable_ops[:1]

            # Apply caps and compute request size
            cpu_cap, ram_cap = _cap_for_priority(pool, chosen_pri, avail_cpu, avail_ram, hp_backlog)
            if cpu_cap <= 0 or ram_cap <= 0:
                break

            base_cpu, base_ram = _base_request(pool, chosen_pri)
            pst = _ensure_pstate(chosen_pipeline.pipeline_id)

            cpu_req = base_cpu * pst["cpu_mult"]
            ram_req = base_ram * pst["ram_mult"]

            # Clamp to current caps/availability
            cpu_req = max(1.0, min(cpu_req, cpu_cap, avail_cpu, pool.max_cpu_pool))
            ram_req = max(1.0, min(ram_req, ram_cap, avail_ram, pool.max_ram_pool))

            # If we can't fit even minimums, stop scheduling in this pool this tick.
            if cpu_req <= 0 or ram_req <= 0:
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            avail_cpu -= cpu_req
            avail_ram -= ram_req

    return suspensions, assignments
