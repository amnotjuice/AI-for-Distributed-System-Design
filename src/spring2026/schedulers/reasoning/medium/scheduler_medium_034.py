# policy_key: scheduler_medium_034
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.065295
# generation_seconds: 69.23
# generated_at: 2026-03-14T03:02:34.950608
@register_scheduler_init(key="scheduler_medium_034")
def scheduler_medium_034_init(s):
    """
    Priority-aware FIFO with simple, safe improvements over the naive baseline:

    Improvements (kept intentionally small/robust):
      1) Separate FIFO queues per priority so high-priority work doesn't sit behind batch.
      2) Avoid repeatedly re-adding pipelines every tick; keep persistent queues.
      3) Conservative headroom reservation: don't let batch consume the last slice of a pool
         while high-priority work is waiting.
      4) Basic OOM-aware RAM backoff using operator identity (id(op)) as the key.

    Notes:
      - We intentionally avoid preemption because the minimal public API in the prompt
        doesn't expose currently-running containers reliably across pools.
      - We schedule at most one operator per pool per tick (like the baseline) to keep
        behavior predictable while improving latency for high priority.
    """
    # Per-priority FIFO queues of pipeline_ids (store ids to avoid duplicates)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Pipeline registry: pipeline_id -> Pipeline (latest object reference)
    s.pipelines_by_id = {}

    # Track pipeline enqueue order / membership to prevent duplicates
    s.enqueued = set()

    # Operator -> pipeline mapping for result handling (id(op) -> pipeline_id)
    s.op_to_pipeline = {}

    # OOM/backoff state: per-operator RAM multiplier (id(op) -> multiplier)
    s.op_ram_mult = {}

    # If an operator OOMs, we remember the last attempted RAM to grow from
    s.op_last_ram = {}

    # Track pipelines that should no longer be scheduled (non-OOM failure)
    s.blacklisted_pipelines = set()


@register_scheduler(key="scheduler_medium_034")
def scheduler_medium_034(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into per-priority FIFO queues.
      - Process results; on OOM, increase RAM multiplier for the failed operator and re-queue.
        On non-OOM failure, blacklist the pipeline (drop it).
      - For each pool, schedule one ready operator, prioritizing QUERY > INTERACTIVE > BATCH.
      - While high-priority is waiting, apply a small headroom reservation that limits batch
        from consuming the last portion of CPU/RAM in a pool.
    """
    def _prio_queue_and_tag(priority):
        # Priority order: QUERY highest, then INTERACTIVE, then BATCH_PIPELINE
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _has_high_prio_waiting():
        # "High priority" means QUERY or INTERACTIVE
        return len(s.q_query) > 0 or len(s.q_interactive) > 0

    def _is_oom_error(err):
        if not err:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        # If pipeline successful, drop
        if st.is_pipeline_successful():
            return True
        # If any failures exist, we decide via blacklist; otherwise keep going
        return False

    def _get_next_ready_op(p):
        st = p.runtime_status()
        # We only schedule ops whose parents are complete
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        return op_list[0]

    def _pick_next_pipeline_id_for_pool(pool):
        """
        Pick the next pipeline from queues by priority.
        Rotate within a queue if the head isn't ready.
        Keep loops bounded to avoid infinite rotation.
        """
        # Queue inspection order
        queues = (s.q_query, s.q_interactive, s.q_batch)

        # How many total attempts to rotate across all queues this tick for this pool
        max_attempts = len(s.q_query) + len(s.q_interactive) + len(s.q_batch)
        attempts = 0

        while attempts < max_attempts:
            attempts += 1

            chosen_q = None
            for q in queues:
                if q:
                    chosen_q = q
                    break
            if not chosen_q:
                return None  # nothing waiting

            pid = chosen_q.pop(0)

            # Pipeline may have been removed/blacklisted
            p = s.pipelines_by_id.get(pid)
            if (p is None) or (pid in s.blacklisted_pipelines):
                s.enqueued.discard(pid)
                continue

            # Drop completed pipelines
            if _pipeline_done_or_failed(p):
                s.enqueued.discard(pid)
                continue

            # If pipeline has failed ops, only continue if it wasn't blacklisted.
            # (We blacklist on non-OOM failures when results arrive.)
            op = _get_next_ready_op(p)
            if op is None:
                # Not ready; push it to the back of its priority queue
                _prio_queue_and_tag(p.priority).append(pid)
                continue

            # Found a ready pipeline; put it back (so it remains active) after we attempt to schedule
            _prio_queue_and_tag(p.priority).append(pid)
            return pid

        return None

    def _compute_request(pool, p_priority, op):
        """
        Choose CPU/RAM request based on priority, available headroom, and OOM backoff.
        Keep it simple and stable.
        """
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        # Base fractions by priority (small improvement: give more to interactive/query)
        if p_priority == Priority.QUERY:
            base_cpu_frac = 0.75
            base_ram_frac = 0.60
        elif p_priority == Priority.INTERACTIVE:
            base_cpu_frac = 0.60
            base_ram_frac = 0.50
        else:  # BATCH_PIPELINE
            base_cpu_frac = 0.40
            base_ram_frac = 0.35

        # Convert fractions into requested amounts, capped by available resources.
        # Ensure at least 1 unit to make progress.
        try:
            max_cpu = pool.max_cpu_pool
            max_ram = pool.max_ram_pool
        except Exception:
            max_cpu = avail_cpu
            max_ram = avail_ram

        cpu_req = int(max(1, min(avail_cpu, round(max_cpu * base_cpu_frac))))
        ram_req = int(max(1, min(avail_ram, round(max_ram * base_ram_frac))))

        # Apply OOM backoff per-operator
        op_id = id(op)
        mult = s.op_ram_mult.get(op_id, 1.0)
        last = s.op_last_ram.get(op_id, ram_req)
        # Grow from the last attempted RAM, not from a potentially smaller base
        ram_req = int(max(1, min(avail_ram, round(max(ram_req, last) * mult))))

        # If we have ample RAM available, prefer using a bit more for high-priority
        # to reduce the chance of OOM/slow spill; keep capped by avail.
        if p_priority in (Priority.QUERY, Priority.INTERACTIVE):
            ram_req = int(min(avail_ram, max(ram_req, int(0.50 * avail_ram))))

        # For batch, avoid taking the entire pool in one go (helps latency of others)
        if p_priority == Priority.BATCH_PIPELINE:
            cpu_req = int(min(cpu_req, max(1, round(max_cpu * 0.50))))
            ram_req = int(min(ram_req, max(1, round(max_ram * 0.60))))

        return cpu_req, ram_req

    # --- Ingest new pipelines (persistent queues; avoid duplicates) ---
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        if p.pipeline_id in s.blacklisted_pipelines:
            continue
        if p.pipeline_id in s.enqueued:
            continue
        _prio_queue_and_tag(p.priority).append(p.pipeline_id)
        s.enqueued.add(p.pipeline_id)

    # --- Process results (OOM backoff / blacklist on non-OOM failures) ---
    for r in results:
        if not getattr(r, "failed", None):
            # If failed() isn't present, we can't reliably interpret failures; skip.
            continue

        if not r.failed():
            # On success: slightly decay RAM multiplier for the ops involved (avoid runaway sizes)
            for op in getattr(r, "ops", []) or []:
                op_id = id(op)
                if op_id in s.op_ram_mult:
                    s.op_ram_mult[op_id] = max(1.0, s.op_ram_mult[op_id] * 0.95)
                # Remember last successful RAM as a reasonable floor
                try:
                    s.op_last_ram[op_id] = int(getattr(r, "ram", s.op_last_ram.get(op_id, 0)) or 0)
                except Exception:
                    pass
            continue

        # Failure handling
        err = getattr(r, "error", None)
        oom = _is_oom_error(err)

        # Identify pipeline via operator identity mapping
        pid = None
        ops = getattr(r, "ops", []) or []
        if ops:
            pid = s.op_to_pipeline.get(id(ops[0]))

        if oom:
            # Increase RAM multiplier for all failed ops in the result
            for op in ops:
                op_id = id(op)
                prev = s.op_ram_mult.get(op_id, 1.0)
                # Exponential backoff with a cap
                s.op_ram_mult[op_id] = min(8.0, max(1.5, prev * 2.0))
                # Grow from the last attempted size if available
                try:
                    attempted_ram = int(getattr(r, "ram", 0) or 0)
                    if attempted_ram > 0:
                        s.op_last_ram[op_id] = max(s.op_last_ram.get(op_id, 0), attempted_ram)
                except Exception:
                    pass

            # Ensure pipeline remains in queue (it already should be); if we can identify it and it
            # got dropped earlier, re-enqueue.
            if pid is not None and pid not in s.enqueued and pid not in s.blacklisted_pipelines:
                p = s.pipelines_by_id.get(pid)
                if p is not None:
                    _prio_queue_and_tag(p.priority).append(pid)
                    s.enqueued.add(pid)
        else:
            # Non-OOM failures: drop pipeline to avoid infinite retries
            if pid is not None:
                s.blacklisted_pipelines.add(pid)
                s.enqueued.discard(pid)

    # Early exit if nothing changed that affects decisions
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Schedule high-priority faster by considering pools with the most headroom first
    pool_order = list(range(s.executor.num_pools))
    try:
        pool_order.sort(
            key=lambda i: (s.executor.pools[i].avail_cpu_pool, s.executor.pools[i].avail_ram_pool),
            reverse=True,
        )
    except Exception:
        pass

    # Per-pool headroom reservation: while high priority waits, preserve a slice
    # so batch doesn't fill the pool completely.
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        pid = _pick_next_pipeline_id_for_pool(pool)
        if pid is None:
            continue

        p = s.pipelines_by_id.get(pid)
        if p is None or pid in s.blacklisted_pipelines:
            continue

        # If completed, drop and move on
        if _pipeline_done_or_failed(p):
            s.enqueued.discard(pid)
            continue

        op = _get_next_ready_op(p)
        if op is None:
            continue

        # Apply headroom policy only for batch when high-priority is waiting
        high_waiting = _has_high_prio_waiting()
        if p.priority == Priority.BATCH_PIPELINE and high_waiting:
            # Keep a small reserve to reduce tail latency for interactive arrivals.
            # Use max_* as a stable reference when available.
            try:
                reserve_cpu = max(1, int(round(pool.max_cpu_pool * 0.20)))
                reserve_ram = max(1, int(round(pool.max_ram_pool * 0.20)))
            except Exception:
                reserve_cpu = max(1, int(round(avail_cpu * 0.20)))
                reserve_ram = max(1, int(round(avail_ram * 0.20)))

            # If we are already at/under reserve, skip batch scheduling this pool this tick.
            if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                continue

        cpu_req, ram_req = _compute_request(pool, p.priority, op)
        if cpu_req <= 0 or ram_req <= 0:
            continue

        # Final cap to what's currently available
        cpu_req = min(cpu_req, pool.avail_cpu_pool)
        ram_req = min(ram_req, pool.avail_ram_pool)
        if cpu_req <= 0 or ram_req <= 0:
            continue

        # Remember mapping so we can interpret results later
        s.op_to_pipeline[id(op)] = p.pipeline_id

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
        )

        # One operator per pool per tick (stable incremental improvement over baseline)
        # If you later want more throughput, you can loop here while resources allow.

    return suspensions, assignments
