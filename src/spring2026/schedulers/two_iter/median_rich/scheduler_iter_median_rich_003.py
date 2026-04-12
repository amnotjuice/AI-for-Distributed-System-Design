# policy_key: scheduler_iter_median_rich_003
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.061562
# generation_seconds: 44.42
# generated_at: 2026-04-12T01:45:17.533079
@register_scheduler_init(key="scheduler_iter_median_rich_003")
def scheduler_iter_median_rich_003_init(s):
    """Priority-aware, memory-adaptive scheduler with per-pipeline RAM bounds.

    Goals vs prior attempt:
    - Reduce OOMs (major latency + timeout driver) by learning per-pipeline RAM needs using OOM/success feedback.
    - Reduce over-allocation by learning an upper bound after successful runs.
    - Preserve good latency for QUERY/INTERACTIVE via strict priority admission + mild pool reservation.
    - Keep it incrementally more complex but robust: no dependence on unseen APIs beyond provided fields.

    Key ideas:
    - Maintain per-pipeline RAM bounds: (lb, ub). OOM raises lb; success lowers ub.
    - Track container_id -> pipeline_id for accurate feedback (since ExecutionResult lacks pipeline_id).
    - Size CPU modestly by priority (avoid starving throughput, but keep latency responsive).
    """
    # Waiting queues by priority (simple FIFO within each class)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-pipeline learned sizing state:
    #   ram_lb: minimum RAM we believe is needed (based on OOMs)
    #   ram_ub: maximum RAM we believe is sufficient (based on successes), may be None
    #   last_ram: last RAM we assigned (for fallback if feedback is missing)
    #   last_cpu: last CPU we assigned
    #   drop: if True, do not retry (non-OOM failures)
    s.pstate = {}

    # Map container -> pipeline_id for feedback attribution
    s.container_to_pid = {}

    # Round-robin cursors (avoid one pipeline dominating within a priority class)
    s.rr = {Priority.QUERY: 0, Priority.INTERACTIVE: 0, Priority.BATCH_PIPELINE: 0}


@register_scheduler(key="scheduler_iter_median_rich_003")
def scheduler_iter_median_rich_003_scheduler(s, results, pipelines):
    """
    Step logic:
    1) Enqueue new pipelines by priority.
    2) Incorporate ExecutionResult feedback:
       - If OOM: raise pipeline ram_lb substantially.
       - If success: tighten pipeline ram_ub towards observed good RAM.
       - If non-OOM fail: mark pipeline drop=True (do not spin).
    3) For each pool, schedule ready ops in strict priority order, 1 op at a time,
       using learned RAM bounds to pick a RAM request that avoids OOMs without over-alloc.
    """
    # -----------------------
    # Helpers (local)
    # -----------------------
    def _ensure_pstate(pid):
        if pid not in s.pstate:
            s.pstate[pid] = {
                "ram_lb": 0.0,
                "ram_ub": None,
                "last_ram": 0.0,
                "last_cpu": 0.0,
                "drop": False,
            }
        return s.pstate[pid]

    def _is_oom(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _queue_for(pri):
        if pri == Priority.QUERY:
            return s.q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _pop_rr(pri):
        """Pop next pipeline candidate in RR order; return None if queue empty."""
        q = _queue_for(pri)
        if not q:
            return None
        i = s.rr[pri] % len(q)
        p = q.pop(i)
        # cursor stays at i (next element shifts into i)
        if q:
            s.rr[pri] = i % len(q)
        else:
            s.rr[pri] = 0
        return p

    def _push_back(pri, p):
        _queue_for(pri).append(p)

    def _has_hp_backlog():
        """Any runnable QUERY/INTERACTIVE work waiting (rough signal used to cap batch share)."""
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            q = _queue_for(pri)
            for p in q:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                if _ensure_pstate(p.pipeline_id)["drop"]:
                    continue
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return True
        return False

    def _cleanup_pipeline(pri, p):
        """Return True if pipeline should be dropped from scheduling."""
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        pst = _ensure_pstate(p.pipeline_id)
        # If it has failed ops and we marked drop, remove it.
        if pst["drop"] and st.state_counts.get(OperatorState.FAILED, 0) > 0:
            return True
        return False

    def _base_cpu(pool, pri):
        # Conservative defaults to reduce queueing without hogging all CPUs.
        if pri == Priority.QUERY:
            return max(1.0, min(2.0, pool.max_cpu_pool * 0.25))
        if pri == Priority.INTERACTIVE:
            return max(1.0, min(4.0, pool.max_cpu_pool * 0.40))
        return max(1.0, min(pool.max_cpu_pool * 0.70, max(2.0, pool.max_cpu_pool * 0.55)))

    def _base_ram(pool, pri):
        # Slightly higher starting points than prior attempt to reduce first-try OOM loops.
        if pri == Priority.QUERY:
            return max(1.0, pool.max_ram_pool * 0.14)
        if pri == Priority.INTERACTIVE:
            return max(1.0, pool.max_ram_pool * 0.24)
        return max(1.0, pool.max_ram_pool * 0.38)

    def _choose_ram(pool, pri, pid, avail_ram):
        """Pick RAM using learned bounds; aim to avoid OOMs while tightening towards ub."""
        pst = _ensure_pstate(pid)
        lb = max(0.0, pst["ram_lb"])
        ub = pst["ram_ub"]  # None or float

        # Start from base; if we have a lower bound, honor it.
        target = max(_base_ram(pool, pri), lb)

        # If we have an upper bound (seen success), try to move down toward it,
        # but never below lb. This reduces over-allocation.
        if ub is not None:
            # A small safety margin above lb, but biased towards ub.
            # If lb is close to ub, we converge quickly.
            target = max(lb, min(target, ub))
            # If ub is much higher than lb, try slightly under ub to test tightness.
            # (We still obey lb, so no risk of going below known-safe minimum.)
            target = max(lb, min(target, ub * 0.95))

        # Clamp to what we can actually allocate now.
        target = max(1.0, min(target, avail_ram, pool.max_ram_pool))
        return target

    def _choose_cpu(pool, pri, avail_cpu, hp_backlog):
        """Pick CPU; cap batch when HP backlog exists to protect latency."""
        cpu = _base_cpu(pool, pri)
        if pri == Priority.BATCH_PIPELINE and hp_backlog:
            cpu = min(cpu, max(1.0, pool.max_cpu_pool * 0.50))
        cpu = max(1.0, min(cpu, avail_cpu, pool.max_cpu_pool))
        return cpu

    def _cap_for_batch(pool, avail_cpu, avail_ram, hp_backlog):
        """Soft reservation: keep headroom if HP backlog exists."""
        if not hp_backlog:
            return avail_cpu, avail_ram
        # Keep meaningful headroom for query/interactive.
        return min(avail_cpu, pool.max_cpu_pool * 0.65), min(avail_ram, pool.max_ram_pool * 0.65)

    # -----------------------
    # 1) Enqueue new pipelines
    # -----------------------
    for p in pipelines:
        _ensure_pstate(p.pipeline_id)
        _queue_for(p.priority).append(p)

    if not pipelines and not results:
        return [], []

    # -----------------------
    # 2) Feedback from results
    # -----------------------
    for r in results:
        pid = s.container_to_pid.get(getattr(r, "container_id", None), None)
        # If we cannot attribute, skip; sizing still improves over time when we can attribute.
        if pid is None:
            continue

        pst = _ensure_pstate(pid)

        # Update bounds based on outcome
        if hasattr(r, "failed") and r.failed():
            if _is_oom(getattr(r, "error", None)):
                # OOM => raise lower bound aggressively to break OOM-retry loops.
                # Use observed allocated RAM as a baseline; add safety multiplier.
                observed = float(getattr(r, "ram", 0.0) or pst["last_ram"] or 0.0)
                new_lb = max(pst["ram_lb"], observed * 1.6, observed + 1.0)
                pst["ram_lb"] = new_lb
                # If lb crosses ub, invalidate ub (it was too low).
                if pst["ram_ub"] is not None and pst["ram_lb"] > pst["ram_ub"]:
                    pst["ram_ub"] = None
            else:
                # Non-OOM failure: don't spin retrying forever.
                pst["drop"] = True
        else:
            # Success => tighten upper bound to reduce waste.
            observed = float(getattr(r, "ram", 0.0) or pst["last_ram"] or 0.0)
            if observed > 0:
                pst["ram_ub"] = observed if pst["ram_ub"] is None else min(pst["ram_ub"], observed)
            # Also, if lb is higher than observed (can happen if bounds were inflated), relax lb slightly.
            if pst["ram_lb"] > 0 and observed > 0 and pst["ram_lb"] > observed:
                pst["ram_lb"] = max(0.0, observed * 0.90)

        # Container finished; mapping no longer needed.
        cid = getattr(r, "container_id", None)
        if cid in s.container_to_pid:
            del s.container_to_pid[cid]

    # -----------------------
    # 3) Schedule assignments
    # -----------------------
    suspensions = []
    assignments = []

    hp_backlog = _has_hp_backlog()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        made = True
        while made:
            made = False
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Strict priority selection: QUERY -> INTERACTIVE -> BATCH
            chosen = None
            chosen_pri = None
            chosen_ops = None

            for pri in _prio_order():
                # For batch, apply soft caps when HP backlog exists
                eff_cpu = avail_cpu
                eff_ram = avail_ram
                if pri == Priority.BATCH_PIPELINE:
                    eff_cpu, eff_ram = _cap_for_batch(pool, avail_cpu, avail_ram, hp_backlog)
                    if eff_cpu <= 0 or eff_ram <= 0:
                        continue

                # Try a bounded number of attempts to find a runnable pipeline in this priority class
                q = _queue_for(pri)
                if not q:
                    continue

                attempts = min(len(q), 8)  # keep per-tick work bounded
                for _ in range(attempts):
                    p = _pop_rr(pri)
                    if p is None:
                        break

                    if _cleanup_pipeline(pri, p):
                        continue

                    st = p.runtime_status()
                    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not ops:
                        # Not runnable now; keep it in queue
                        _push_back(pri, p)
                        continue

                    # Found runnable work
                    chosen = p
                    chosen_pri = pri
                    chosen_ops = ops
                    break

                if chosen is not None:
                    break

            if chosen is None:
                break

            # Determine resource request for the chosen op
            # For batch with HP backlog, use the effective caps; otherwise current availability.
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram
            if chosen_pri == Priority.BATCH_PIPELINE:
                eff_avail_cpu, eff_avail_ram = _cap_for_batch(pool, avail_cpu, avail_ram, hp_backlog)
                # If after capping it doesn't fit, push back and stop scheduling batch in this pool this tick.
                if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                    _push_back(chosen_pri, chosen)
                    break

            cpu_req = _choose_cpu(pool, chosen_pri, eff_avail_cpu, hp_backlog)
            ram_req = _choose_ram(pool, chosen_pri, chosen.pipeline_id, eff_avail_ram)

            if cpu_req <= 0 or ram_req <= 0:
                _push_back(chosen_pri, chosen)
                break

            # Record last assigned for fallback; helps if feedback attribution is missing later.
            pst = _ensure_pstate(chosen.pipeline_id)
            pst["last_cpu"] = cpu_req
            pst["last_ram"] = ram_req

            # Make assignment
            a = Assignment(
                ops=chosen_ops,
                cpu=cpu_req,
                ram=ram_req,
                priority=chosen.priority,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
            assignments.append(a)

            # Best-effort: attribute future results by container_id if the simulator provides it.
            # Some sims may only fill container_id in results; we still store mapping when available later.
            # (No harm if container_id isn't present at assignment-time.)
            cid = getattr(a, "container_id", None)
            if cid is not None:
                s.container_to_pid[cid] = chosen.pipeline_id

            # Locally consume capacity to avoid overscheduling within tick
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Put pipeline back for future ops
            _push_back(chosen_pri, chosen)
            made = True

    return suspensions, assignments
