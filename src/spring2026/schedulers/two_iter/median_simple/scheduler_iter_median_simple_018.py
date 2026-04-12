# policy_key: scheduler_iter_median_simple_018
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049897
# generation_seconds: 37.27
# generated_at: 2026-04-12T01:41:31.860407
@register_scheduler_init(key="scheduler_iter_median_simple_018")
def scheduler_iter_median_simple_018_init(s):
    """Priority-aware FIFO v2 (bug-fixed) with strict high-priority protection.

    Key changes vs previous attempt:
    - Fixes a major queueing bug: do NOT duplicate pipelines by appending without removing.
      We use deques and always pop-left then re-append exactly once.
    - Strict priority ordering: QUERY > INTERACTIVE > BATCH.
    - Stronger latency protection: when any high-priority backlog exists, we keep
      per-pool headroom and may completely block batch admission until HP drains.
    - Simple, safe sizing: give more CPU to high-priority ops (finish faster),
      keep RAM modest but increase on observed failures (bounded).
    """
    import collections

    s.q = {
        Priority.QUERY: collections.deque(),
        Priority.INTERACTIVE: collections.deque(),
        Priority.BATCH_PIPELINE: collections.deque(),
    }

    # Per-pipeline adaptive state (best-effort; we don't always see error cause).
    # ram_mult: increases after failures to reduce repeated OOM loops
    # fail_count: count of observed FAILED ops (via runtime_status)
    # retries: how many times we've attempted to run failed ops again
    s.pstate = {}

    # Tuning knobs (kept as state for easy iteration)
    s.cfg = {
        # Reserve headroom per pool when HP backlog exists
        "reserve_cpu_frac_for_hp": 0.25,
        "reserve_ram_frac_for_hp": 0.25,
        # If there is any HP backlog, optionally block batch entirely (strong latency bias)
        "block_batch_when_hp_backlog": True,
        # CPU sizing by priority (fraction of pool max; later clamped to availability)
        "cpu_frac_query": 0.75,
        "cpu_frac_interactive": 0.55,
        "cpu_frac_batch": 0.65,
        # RAM sizing by priority (fraction of pool max; later clamped)
        "ram_frac_query": 0.12,
        "ram_frac_interactive": 0.22,
        "ram_frac_batch": 0.40,
        # Failure backoff
        "ram_backoff_mult": 1.6,
        "ram_mult_cap": 16.0,
        "max_retries_query": 6,
        "max_retries_interactive": 5,
        "max_retries_batch": 3,
    }


@register_scheduler(key="scheduler_iter_median_simple_018")
def scheduler_iter_median_simple_018_scheduler(s, results, pipelines):
    """Scheduler step: strict priority + headroom + bug-free FIFO within each priority."""
    # Enqueue new pipelines
    for p in pipelines:
        if p.pipeline_id not in s.pstate:
            s.pstate[p.pipeline_id] = {"ram_mult": 1.0, "fail_count": 0, "retries": 0}
        s.q[p.priority].append(p)

    # Early exit (still safe because we persist queues)
    if not pipelines and not results:
        return [], []

    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _has_hp_backlog():
        # backlog: any assignable op in QUERY or INTERACTIVE queues
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            q = s.q[pri]
            # scan with bounded effort; queues are typically not enormous
            for p in q:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return True
        return False

    def _max_retries_for_priority(pri):
        if pri == Priority.QUERY:
            return s.cfg["max_retries_query"]
        if pri == Priority.INTERACTIVE:
            return s.cfg["max_retries_interactive"]
        return s.cfg["max_retries_batch"]

    def _maybe_update_failure_backoff(p):
        """Detect increased FAILED op count and react with RAM backoff (bounded)."""
        st = p.runtime_status()
        pst = s.pstate.get(p.pipeline_id)
        if pst is None:
            pst = {"ram_mult": 1.0, "fail_count": 0, "retries": 0}
            s.pstate[p.pipeline_id] = pst

        failed_now = st.state_counts.get(OperatorState.FAILED, 0)
        if failed_now > pst["fail_count"]:
            # Treat as likely OOM (we don't have reliable mapping from results->pipeline).
            pst["fail_count"] = failed_now
            pst["retries"] += 1
            pst["ram_mult"] = min(pst["ram_mult"] * s.cfg["ram_backoff_mult"], s.cfg["ram_mult_cap"])

    def _compute_request(pool, pri, pipeline_id, avail_cpu, avail_ram, hp_backlog):
        """Compute cpu/ram request shaped for latency (more CPU for higher priority)."""
        pst = s.pstate.get(pipeline_id, {"ram_mult": 1.0})

        if pri == Priority.QUERY:
            cpu = max(1.0, pool.max_cpu_pool * s.cfg["cpu_frac_query"])
            ram = max(1.0, pool.max_ram_pool * s.cfg["ram_frac_query"])
        elif pri == Priority.INTERACTIVE:
            cpu = max(1.0, pool.max_cpu_pool * s.cfg["cpu_frac_interactive"])
            ram = max(1.0, pool.max_ram_pool * s.cfg["ram_frac_interactive"])
        else:
            cpu = max(1.0, pool.max_cpu_pool * s.cfg["cpu_frac_batch"])
            ram = max(1.0, pool.max_ram_pool * s.cfg["ram_frac_batch"])

        # Apply RAM backoff after failures (likely OOM)
        ram = ram * pst.get("ram_mult", 1.0)

        # If HP backlog exists, keep some headroom: batch sees effective avail reduced.
        if hp_backlog and pri == Priority.BATCH_PIPELINE:
            cpu = min(cpu, max(1.0, pool.max_cpu_pool * 0.50))
            ram = min(ram, max(1.0, pool.max_ram_pool * 0.50))

        # Clamp to available and pool maxima
        cpu = max(1.0, min(cpu, avail_cpu, pool.max_cpu_pool))
        ram = max(1.0, min(ram, avail_ram, pool.max_ram_pool))
        return cpu, ram

    def _hp_reservation_block(pri, pool, avail_cpu, avail_ram, hp_backlog):
        """Return True if this priority should be blocked in this pool due to reservations."""
        if not hp_backlog:
            return False
        if pri != Priority.BATCH_PIPELINE:
            return False

        if s.cfg["block_batch_when_hp_backlog"]:
            return True

        # Otherwise, block batch only if we'd eat into reserved headroom
        reserve_cpu = pool.max_cpu_pool * s.cfg["reserve_cpu_frac_for_hp"]
        reserve_ram = pool.max_ram_pool * s.cfg["reserve_ram_frac_for_hp"]
        return (avail_cpu <= reserve_cpu) or (avail_ram <= reserve_ram)

    suspensions = []
    assignments = []

    hp_backlog = _has_hp_backlog()

    # For each pool, greedily schedule as many ops as we can in priority order.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Keep scheduling until we can't make progress
        while avail_cpu > 0 and avail_ram > 0:
            scheduled_one = False

            for pri in _prio_order():
                if _hp_reservation_block(pri, pool, avail_cpu, avail_ram, hp_backlog):
                    continue

                q = s.q[pri]
                if not q:
                    continue

                # Try up to len(q) to find a runnable pipeline (FIFO with skipping)
                tries = len(q)
                for _ in range(tries):
                    p = q.popleft()  # remove (prevents duplicates)
                    st = p.runtime_status()

                    if st.is_pipeline_successful():
                        # drop completed pipelines
                        continue

                    # Update backoff/retry tracking if failures increased
                    _maybe_update_failure_backoff(p)
                    pst = s.pstate.get(p.pipeline_id, {"retries": 0})
                    if pst.get("retries", 0) > _max_retries_for_priority(p.priority):
                        # Too many retries -> drop to avoid infinite loops harming latency
                        continue

                    # Get one ready op (smaller unit reduces head-of-line blocking)
                    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        # Not runnable yet; keep it in the queue
                        q.append(p)
                        continue

                    cpu_req, ram_req = _compute_request(pool, pri, p.pipeline_id, avail_cpu, avail_ram, hp_backlog)
                    if cpu_req <= 0 or ram_req <= 0 or cpu_req > avail_cpu or ram_req > avail_ram:
                        # Can't fit right now; put back and stop trying this priority in this pool
                        q.append(p)
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

                    # Update local availability to prevent overscheduling this tick
                    avail_cpu -= cpu_req
                    avail_ram -= ram_req

                    # Put pipeline back for future ops
                    q.append(p)

                    scheduled_one = True
                    break  # move to next scheduling iteration (re-check from top priority)

                if scheduled_one:
                    break  # restart priority order after successful schedule

            if not scheduled_one:
                break  # nothing runnable or nothing fits in this pool

    return suspensions, assignments
