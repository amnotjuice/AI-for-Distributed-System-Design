# policy_key: scheduler_medium_047
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.068725
# generation_seconds: 71.50
# generated_at: 2026-03-14T03:21:54.890516
@register_scheduler_init(key="scheduler_medium_047")
def scheduler_medium_047_init(s):
    """
    Priority-aware, work-conserving extension of the naive FIFO policy.

    Incremental improvements over naive:
      1) Priority queues (QUERY > INTERACTIVE > BATCH).
      2) Work-conserving: schedule multiple assignments per pool per tick (not just one).
      3) Round-robin within each priority to reduce head-of-line blocking.
      4) Simple OOM-aware RAM retry: if an op fails with OOM-like error, retry with higher RAM.
      5) Soft reservation: when high-priority work is waiting, avoid letting BATCH consume the last
         portion of pool resources (reduces tail latency for interactive/query arrivals).
    """
    # One queue per priority class; we keep Pipelines here.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Logical "time" for simple aging/fairness.
    s.epoch = 0
    s.arrival_epoch = {}  # pipeline_id -> epoch

    # Per-operator hints learned from failures (key by object identity).
    # op_hints[id(op)] = {"ram": float, "oom_retries": int}
    s.op_hints = {}

    # If an operator OOMs too many times, consider it permanently failed and stop retrying.
    s.max_oom_retries = 3

    # Soft reservation ratios to protect high-priority latency.
    s.hp_reserve_cpu_ratio = 0.25
    s.hp_reserve_ram_ratio = 0.25

    # Default per-assignment sizing (fractions of pool capacity).
    s.cpu_frac_hp = 0.50
    s.cpu_frac_batch = 0.25
    s.ram_frac_hp = 0.25
    s.ram_frac_batch = 0.20

    # Minimum allocation to avoid zero-sized assignments.
    s.min_cpu = 1.0


@register_scheduler(key="scheduler_medium_047")
def scheduler_medium_047(s, results, pipelines):
    """
    Scheduler step.

    Strategy:
      - Enqueue new pipelines by priority.
      - Update OOM-based RAM hints from results.
      - For each pool, repeatedly pick the best next runnable op (by priority, RR within prio),
        respecting soft reservations when high-priority work is waiting.
      - Emit multiple assignments per pool per tick until resources are exhausted or no runnable ops exist.

    Note: We do not preempt (suspensions empty) because container/running-state introspection is not
    part of the minimal public interface shown in the template. This keeps the policy robust.
    """
    s.epoch += 1

    def _is_oom_error(err):
        if not err:
            return False
        e = str(err).lower()
        # Common variants across systems/simulators.
        return ("oom" in e) or ("out of memory" in e) or ("memoryerror" in e) or ("killed" in e and "memory" in e)

    def _enqueue_pipeline(p):
        # Record arrival once (used for mild aging / bookkeeping).
        if p.pipeline_id not in s.arrival_epoch:
            s.arrival_epoch[p.pipeline_id] = s.epoch

        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _is_pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If there are FAILED ops, we still may retry (especially on OOM), so don't drop here.
        return False

    def _queue_has_runnable_work(q):
        # Check if any pipeline in q has at least one runnable op.
        for p in q:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                return True
        return False

    def _pick_next_pipeline_rr(q):
        """
        Round-robin scan: rotate until we find a pipeline with a runnable op.
        Returns (pipeline, op_list) or (None, None).
        """
        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            st = p.runtime_status()

            # Drop completed pipelines from queue.
            if st.is_pipeline_successful():
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                # Put it back at the end for RR fairness after we schedule one op.
                q.append(p)
                return p, ops[:1]

            # Not runnable right now (waiting on deps); keep it in queue.
            q.append(p)

        return None, None

    def _default_cpu_request(pool, priority, hp_pressure):
        # When there is a lot of HP pressure, slice a bit smaller to increase concurrency.
        max_cpu = float(pool.max_cpu_pool)
        avail_cpu = float(pool.avail_cpu_pool)

        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            frac = s.cpu_frac_hp
            if hp_pressure >= 3:
                frac = min(frac, 0.33)
            elif hp_pressure >= 2:
                frac = min(frac, 0.40)
        else:
            frac = s.cpu_frac_batch

        req = max(s.min_cpu, max_cpu * frac)
        return min(avail_cpu, req)

    def _default_ram_request(pool, priority):
        max_ram = float(pool.max_ram_pool)
        avail_ram = float(pool.avail_ram_pool)
        frac = s.ram_frac_hp if priority in (Priority.QUERY, Priority.INTERACTIVE) else s.ram_frac_batch
        req = max(1.0, max_ram * frac)
        return min(avail_ram, req)

    def _ram_with_hints(pool, priority, op):
        """
        Start from a conservative default, then apply OOM-learned hints (which only increase RAM).
        """
        req = _default_ram_request(pool, priority)
        hint = s.op_hints.get(id(op))
        if hint and hint.get("ram"):
            req = max(req, float(hint["ram"]))
        return min(float(pool.avail_ram_pool), req)

    # --- Intake: enqueue newly arrived pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changed that could affect decisions.
    if not pipelines and not results:
        return [], []

    # --- Learn from results (OOM -> increase RAM hint) ---
    # We can only reliably key by operator object identity (id(op)) based on available interfaces.
    for r in results:
        if not r.failed():
            continue

        if _is_oom_error(getattr(r, "error", None)):
            # For each op in the failed container, increase the RAM hint for retry.
            for op in getattr(r, "ops", []) or []:
                h = s.op_hints.get(id(op), {"ram": None, "oom_retries": 0})
                h["oom_retries"] = int(h.get("oom_retries", 0)) + 1

                # Start from the RAM that was attempted, double it, cap at pool max later on scheduling.
                attempted_ram = float(getattr(r, "ram", 0.0) or 0.0)
                if attempted_ram > 0:
                    new_hint = attempted_ram * 2.0
                else:
                    # If attempted RAM is unknown, still bump to a reasonable chunk on next try.
                    new_hint = None

                if h["oom_retries"] <= s.max_oom_retries:
                    if new_hint is not None:
                        # Store the raw hint; it will be capped by pool availability when assigning.
                        h["ram"] = max(float(h.get("ram") or 0.0), new_hint)
                    s.op_hints[id(op)] = h
                else:
                    # Stop retrying aggressively; keep the last hint but don't increase further.
                    s.op_hints[id(op)] = h
        else:
            # Non-OOM failure: do not learn RAM hints.
            pass

    # --- Cleanup: remove completed pipelines from the heads/tails opportunistically ---
    # (RR picking also drops completed pipelines as it scans, but this keeps queues smaller.)
    def _prune_queue(q):
        keep = []
        for p in q:
            if not _is_pipeline_done_or_failed(p):
                keep.append(p)
        q[:] = keep

    _prune_queue(s.q_query)
    _prune_queue(s.q_interactive)
    _prune_queue(s.q_batch)

    # --- Scheduling ---
    suspensions = []
    assignments = []

    # High-priority runnable work indicator (global).
    hp_waiting = _queue_has_runnable_work(s.q_query) or _queue_has_runnable_work(s.q_interactive)

    # For each pool, greedily fill with assignments.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Snapshot and maintain local counters as we "consume" capacity.
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft reservations apply only if HP work is waiting anywhere.
        reserve_cpu = float(pool.max_cpu_pool) * float(s.hp_reserve_cpu_ratio) if hp_waiting else 0.0
        reserve_ram = float(pool.max_ram_pool) * float(s.hp_reserve_ram_ratio) if hp_waiting else 0.0

        # A small special-case: if we have multiple pools, prefer to keep pool 0 more HP-friendly.
        # This is a "gentle" specialization: BATCH can still run there if no HP runnable work exists.
        hp_only_pool = (s.executor.num_pools > 1 and pool_id == 0)

        while avail_cpu >= s.min_cpu and avail_ram > 0:
            # Recompute HP runnable state as queues may have changed due to RR drops.
            hp_runnable_now = _queue_has_runnable_work(s.q_query) or _queue_has_runnable_work(s.q_interactive)

            # Determine whether BATCH is allowed to take more resources right now.
            batch_allowed = True
            if hp_only_pool and hp_runnable_now:
                batch_allowed = False
            if hp_runnable_now:
                # If taking batch would dip into the reserved headroom, block batch.
                if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                    batch_allowed = False

            # RR pick by priority.
            chosen_p = None
            chosen_ops = None

            # Track HP pressure (roughly how many pipelines are waiting) to size CPU slices.
            hp_pressure = len(s.q_query) + len(s.q_interactive)

            chosen_p, chosen_ops = _pick_next_pipeline_rr(s.q_query)
            if chosen_p is None:
                chosen_p, chosen_ops = _pick_next_pipeline_rr(s.q_interactive)
            if chosen_p is None and batch_allowed:
                chosen_p, chosen_ops = _pick_next_pipeline_rr(s.q_batch)

            if chosen_p is None or not chosen_ops:
                # No runnable work for this pool (or batch blocked by reservation).
                break

            op = chosen_ops[0]

            # Compute requests using pool snapshot (but cap by local remaining).
            cpu_req = _default_cpu_request(pool, chosen_p.priority, hp_pressure)
            cpu_req = min(avail_cpu, float(cpu_req))
            cpu_req = max(s.min_cpu, cpu_req)

            ram_req = _ram_with_hints(pool, chosen_p.priority, op)
            ram_req = min(avail_ram, float(ram_req))
            ram_req = max(1.0, ram_req)

            # If we cannot satisfy minimums, stop trying in this pool.
            if cpu_req > avail_cpu or ram_req > avail_ram:
                break

            # If an op has exceeded OOM retries, deprioritize by refusing tiny RAM increments:
            # here we force it to only run if we can give it a larger chunk (reduces thrash).
            hint = s.op_hints.get(id(op))
            if hint and int(hint.get("oom_retries", 0)) > s.max_oom_retries:
                # Require at least half of pool RAM headroom to attempt again.
                if ram_req < 0.50 * float(pool.max_ram_pool):
                    # Can't safely retry here; skip this scheduling attempt.
                    # Rotate other work by continuing the loop.
                    continue

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

            # Consume local capacity and keep packing.
            avail_cpu -= cpu_req
            avail_ram -= ram_req

    return suspensions, assignments
