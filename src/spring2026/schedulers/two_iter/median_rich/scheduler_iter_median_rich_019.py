# policy_key: scheduler_iter_median_rich_019
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.059938
# generation_seconds: 54.10
# generated_at: 2026-04-12T01:58:44.432296
@register_scheduler_init(key="scheduler_iter_median_rich_019")
def scheduler_iter_median_rich_019_init(s):
    """Iteration 2: Priority-aware, pool-affinity, and per-pipeline OOM learning.

    Goals vs prior attempt:
    - Reduce OOM retries (major latency + timeout driver) by:
        * higher initial RAM for QUERY/INTERACTIVE
        * per-pipeline RAM backoff using ExecutionResult -> pipeline_id inference
        * gentle "stickiness" to keep retrying the same pipeline with its learned RAM
    - Reduce head-of-line blocking by:
        * strict priority ordering across classes
        * SRPT-ish ordering within a class (prefer pipelines with fewer remaining ops)
    - Improve isolation by:
        * preferring an "HP pool" (pool 0) for QUERY/INTERACTIVE when multiple pools exist
        * keeping BATCH mostly off the HP pool when contention exists
    - Keep the policy simple and safe: no reliance on suspending running containers.
    """
    # Priority queues (each is a list of Pipeline)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline learned resource hints and bookkeeping
    # pstate[pipeline_id] = {
    #   "ram_mult": float,
    #   "cpu_mult": float,
    #   "ooms": int,
    #   "non_oom_fail": bool,
    # }
    s.pstate = {}

    # Per-priority round-robin cursors to avoid permanent bias among equals
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Remember which pool last successfully ran a pipeline (helps locality/stability)
    s.last_pool_for_pipeline = {}  # pipeline_id -> pool_id


@register_scheduler(key="scheduler_iter_median_rich_019")
def scheduler_iter_median_rich_019_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue new pipelines into per-priority queues.
      2) Update per-pipeline RAM multipliers on OOM using ExecutionResult->pipeline_id inference.
      3) For each pool, greedily assign ready ops in priority order using:
          - pool affinity (HP work prefers pool 0 if available)
          - SRPT-ish tie-break within priority (fewest remaining ops)
          - soft caps to keep batch from consuming HP pool under contention
    """
    # -----------------------------
    # Helpers
    # -----------------------------
    def _ensure_pstate(pipeline_id):
        if pipeline_id not in s.pstate:
            s.pstate[pipeline_id] = {"ram_mult": 1.0, "cpu_mult": 1.0, "ooms": 0, "non_oom_fail": False}
        return s.pstate[pipeline_id]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _extract_pipeline_id_from_result(r):
        # Best-effort: different simulator versions may attach pipeline_id in different places.
        # We try several common patterns; fall back to None.
        pid = getattr(r, "pipeline_id", None)
        if pid is not None:
            return pid

        ops = getattr(r, "ops", None) or []
        for op in ops:
            pid = getattr(op, "pipeline_id", None)
            if pid is not None:
                return pid
            pipe = getattr(op, "pipeline", None)
            if pipe is not None and getattr(pipe, "pipeline_id", None) is not None:
                return pipe.pipeline_id
        return None

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _hp_priorities():
        return (Priority.QUERY, Priority.INTERACTIVE)

    def _hp_backlog_exists():
        for pri in _hp_priorities():
            for p in s.queues[pri]:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
                    continue
                if st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True):
                    return True
        return False

    def _pipeline_remaining_ops_score(p):
        # Lower score => "shorter" pipeline => schedule earlier (SRPT-ish).
        st = p.runtime_status()
        sc = getattr(st, "state_counts", {}) or {}
        # Only count work that isn't completed; include failed (retriable) + pending + assigned + running.
        remaining = 0
        remaining += sc.get(OperatorState.PENDING, 0)
        remaining += sc.get(OperatorState.FAILED, 0)
        remaining += sc.get(OperatorState.ASSIGNED, 0)
        remaining += sc.get(OperatorState.RUNNING, 0)
        remaining += sc.get(OperatorState.SUSPENDING, 0)
        return remaining

    def _cleanup_and_pick_runnable(pri):
        """Pick a runnable pipeline from a priority queue using RR + SRPT-ish bias."""
        q = s.queues[pri]
        if not q:
            return None, None

        # First, remove completed / terminal-failed pipelines from the queue.
        kept = []
        for p in q:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
                continue
            kept.append(p)
        s.queues[pri] = kept
        q = kept
        if not q:
            s.rr_cursor[pri] = 0
            return None, None

        # Consider a window starting from the RR cursor to prevent starvation within the class.
        n = len(q)
        start = s.rr_cursor[pri] % n
        window = []
        # Look at up to 16 candidates (enough to find a runnable short pipeline without O(n) scanning).
        max_scan = min(16, n)
        for k in range(max_scan):
            p = q[(start + k) % n]
            st = p.runtime_status()
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not ops:
                continue
            window.append((p, ops, _pipeline_remaining_ops_score(p)))

        if not window:
            # Advance RR cursor to keep rotating even if nothing runnable right now.
            s.rr_cursor[pri] = (start + 1) % n
            return None, None

        # Pick the smallest remaining-ops pipeline (SRPT-ish); tie-break by earlier in RR window.
        window.sort(key=lambda t: t[2])
        chosen_p, chosen_ops, _ = window[0]

        # Advance RR cursor past chosen pipeline for fairness.
        try:
            idx = q.index(chosen_p)
            s.rr_cursor[pri] = (idx + 1) % len(q)
        except Exception:
            s.rr_cursor[pri] = (start + 1) % len(q)

        return chosen_p, chosen_ops

    def _preferred_pool_for_priority(pri):
        # With multiple pools, prefer pool 0 for HP to reduce interference.
        if s.executor.num_pools <= 1:
            return None
        if pri in _hp_priorities():
            return 0
        return None

    def _pool_order_for_pipeline(pri, pipeline_id):
        # Try: last pool -> preferred pool -> others
        pools = list(range(s.executor.num_pools))
        preferred = _preferred_pool_for_priority(pri)
        last = s.last_pool_for_pipeline.get(pipeline_id, None)

        ordered = []
        if last is not None and last in pools:
            ordered.append(last)
        if preferred is not None and preferred in pools and preferred not in ordered:
            ordered.append(preferred)
        for pid in pools:
            if pid not in ordered:
                ordered.append(pid)
        return ordered

    def _compute_request(pool, pri, pipeline_id, avail_cpu, avail_ram, hp_backlog, pool_id):
        pst = _ensure_pstate(pipeline_id)

        # Base sizing:
        # - Increase RAM for QUERY/INTERACTIVE to reduce OOM loops.
        # - Keep CPU moderate; latency is often dominated by queueing/OOM churn in this sim.
        if pri == Priority.QUERY:
            base_cpu = min(max(1.0, pool.max_cpu_pool * 0.25), 3.0)
            base_ram = max(1.0, pool.max_ram_pool * 0.22)
        elif pri == Priority.INTERACTIVE:
            base_cpu = min(max(1.0, pool.max_cpu_pool * 0.35), 5.0)
            base_ram = max(1.0, pool.max_ram_pool * 0.30)
        else:
            base_cpu = max(1.0, pool.max_cpu_pool * 0.55)
            base_ram = max(1.0, pool.max_ram_pool * 0.38)

        # If there's HP backlog, avoid letting batch eat the HP pool.
        if pri == Priority.BATCH_PIPELINE and hp_backlog and s.executor.num_pools > 1 and pool_id == 0:
            # Strong cap on pool 0 for batch during HP contention.
            base_cpu = min(base_cpu, max(1.0, pool.max_cpu_pool * 0.20))
            base_ram = min(base_ram, max(1.0, pool.max_ram_pool * 0.20))

        # Apply learned multipliers (especially RAM after OOM).
        cpu_req = base_cpu * pst["cpu_mult"]
        ram_req = base_ram * pst["ram_mult"]

        # Clamp to available resources.
        cpu_req = max(1.0, min(cpu_req, avail_cpu, pool.max_cpu_pool))
        ram_req = max(1.0, min(ram_req, avail_ram, pool.max_ram_pool))

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
    # 2) Update state from results
    # -----------------------------
    # Use pipeline_id inference to do per-pipeline learning (key improvement vs last iteration).
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        pid = _extract_pipeline_id_from_result(r)
        pri = getattr(r, "priority", None)

        if _is_oom_error(getattr(r, "error", None)):
            if pid is not None:
                pst = _ensure_pstate(pid)
                pst["ooms"] += 1
                # Exponential-ish RAM backoff, bounded. Faster ramp than before to cut repeated OOMs.
                # (OOM retries are pure latency inflation and can trigger timeouts.)
                pst["ram_mult"] = min(max(pst["ram_mult"] * 2.0, 1.5), 32.0)
            else:
                # If we cannot identify the pipeline, we avoid global multipliers (too destructive).
                # Do nothing; the safer fix is better initial RAM sizing (handled above).
                pass
        else:
            # Mark non-OOM failed pipelines as terminal when we can identify them.
            if pid is not None:
                pst = _ensure_pstate(pid)
                pst["non_oom_fail"] = True
            else:
                # No pipeline_id: cannot safely act.
                pass

        # Light hint: remember pool of the failure to improve locality for immediate retry if identified.
        if pid is not None and getattr(r, "pool_id", None) is not None:
            s.last_pool_for_pipeline[pid] = r.pool_id

    # -----------------------------
    # 3) Build assignments
    # -----------------------------
    suspensions = []
    assignments = []

    hp_backlog = _hp_backlog_exists()

    # We'll schedule per pool, but choose candidates with pool affinity:
    # - HP work tries pool 0 first
    # - pipelines may "stick" to last_pool_for_pipeline
    # This reduces bouncing and helps retries land where there is expected headroom.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Greedy fill while resources remain and something runnable exists.
        # Keep ops per assignment to 1 to reduce HoL blocking and keep latency responsive.
        for _ in range(64):  # safety bound per tick per pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            chosen_p = None
            chosen_ops = None
            chosen_pri = None

            # Priority order across classes.
            for pri in _priority_order():
                # Pool affinity:
                # - If this pool isn't preferred for the priority and we have multiple pools,
                #   we may still schedule if other pools are full; handled implicitly by per-pool loop.
                # - For batch, avoid pool 0 under HP backlog (handled in sizing/caps).
                p, ops = _cleanup_and_pick_runnable(pri)
                if p is None:
                    continue

                # Enforce stronger affinity for HP: if multiple pools and this isn't pool 0,
                # and pool 0 has capacity, we prefer to leave HP work for pool 0.
                if s.executor.num_pools > 1 and pri in _hp_priorities() and pool_id != 0:
                    hp_pool = s.executor.pools[0]
                    if hp_pool.avail_cpu_pool > 0 and hp_pool.avail_ram_pool > 0:
                        # Put it back and continue searching (avoid starving: only do this for HP).
                        s.queues[pri].append(p)
                        continue

                # Also prefer the last pool for this pipeline when possible:
                last_pool = s.last_pool_for_pipeline.get(p.pipeline_id, None)
                if last_pool is not None and s.executor.num_pools > 1 and last_pool != pool_id:
                    # If we're not on the last pool and that pool currently has headroom, defer.
                    lp = s.executor.pools[last_pool]
                    if lp.avail_cpu_pool > 0 and lp.avail_ram_pool > 0:
                        s.queues[pri].append(p)
                        continue

                chosen_p, chosen_ops, chosen_pri = p, ops, pri
                break

            if chosen_p is None:
                break

            # Size the request.
            cpu_req, ram_req = _compute_request(
                pool=pool,
                pri=chosen_pri,
                pipeline_id=chosen_p.pipeline_id,
                avail_cpu=avail_cpu,
                avail_ram=avail_ram,
                hp_backlog=hp_backlog,
                pool_id=pool_id,
            )

            # If we cannot fit even the minimum, stop scheduling on this pool this tick.
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

            # Bookkeeping for affinity and local accounting.
            s.last_pool_for_pipeline[chosen_p.pipeline_id] = pool_id
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Put pipeline back for future ops.
            s.queues[chosen_pri].append(chosen_p)

    return suspensions, assignments
