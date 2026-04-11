# policy_key: scheduler_est_045
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.054404
# generation_seconds: 46.12
# generated_at: 2026-04-10T10:22:00.964591
@register_scheduler_init(key="scheduler_est_045")
def scheduler_est_045_init(s):
    """Memory-aware, priority-first scheduler with conservative retry-on-failure.

    Goals:
    - Minimize weighted latency dominated by QUERY/INTERACTIVE by always serving them first.
    - Reduce 720s penalties by avoiding OOM via estimator-guided RAM sizing and fit checks.
    - Preserve batch progress with mild aging and "small-first" packing under pressure.

    Core ideas:
    - Maintain per-priority FIFO queues with a simple aging signal (enqueue tick).
    - For each pool, repeatedly place the "best" next runnable operator that fits, chosen by:
        (priority weight, aged score) then smaller estimated memory first.
    - RAM request uses op.estimate.mem_peak_gb with safety factor; on failures, bump RAM.
    - Never drop pipelines; if an op doesn't fit anywhere now, keep it queued.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Pipeline bookkeeping
    s._tick = 0
    s._enqueue_tick = {}  # pipeline_id -> tick last enqueued

    # Failure-driven sizing hints (best-effort, since failure cause may not always be OOM)
    # key: (pipeline_id, op_key) -> ram_multiplier
    s._ram_mult = {}

    # Track recently failed ops to avoid immediate thrash
    # key: (pipeline_id, op_key) -> cooldown_until_tick
    s._cooldown = {}

    # Config knobs
    s._safety = 1.30           # estimator safety factor
    s._max_safety = 2.50       # cap on total multiplicative safety (including retries)
    s._cooldown_ticks = 2      # after a failure, wait a couple ticks before retrying
    s._min_ram_gb = 0.25       # never request less than this
    s._min_cpu = 1.0           # never request less than this (if available)
    s._max_assign_ops = 1      # assign 1 op per container for predictability


@register_scheduler(key="scheduler_est_045")
def scheduler_est_045_scheduler(s, results, pipelines):
    def _prio_weight(prio):
        if prio == Priority.QUERY:
            return 10
        if prio == Priority.INTERACTIVE:
            return 5
        return 1

    def _queue_for(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(op):
        # Prefer stable identifiers if present; fall back to object identity.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return v
                except Exception:
                    pass
        return id(op)

    def _is_done_or_failed(pipeline):
        st = pipeline.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator failed, treat pipeline as failed (don't keep retrying endlessly).
        # This is conservative; many failures are OOM and could be retried, but repeated
        # failures hurt the score via 720s anyway. We still allow a small retry via cooldown+ram bump.
        if st.state_counts.get(OperatorState.FAILED, 0) > 0:
            # We don't immediately drop; scheduler may still retry if FAILED is assignable in this sim.
            return False
        return False

    def _get_next_assignable_op(pipeline):
        st = pipeline.runtime_status()
        # Only runnable if parents completed; choose at most one to keep container resource estimates simple.
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        return op_list[0]

    def _estimate_mem_gb(op):
        try:
            est = getattr(op, "estimate", None)
            if est is None:
                return None
            v = getattr(est, "mem_peak_gb", None)
            if v is None:
                return None
            # Guard against nonsense values
            if v < 0:
                return None
            return float(v)
        except Exception:
            return None

    def _choose_pool_and_size(op, pipeline_prio):
        # Returns (pool_id, cpu_req, ram_req) or (None, None, None) if no fit now.
        # Strategy:
        # - Prefer pools where estimated RAM comfortably fits into available RAM.
        # - Within feasible pools, prefer "most slack after placement" to reduce future blocking.
        est_mem = _estimate_mem_gb(op)

        best = None  # tuple(score, pool_id, cpu_req, ram_req)
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu <= 0 or avail_ram <= 0:
                continue

            # RAM request
            mult = 1.0
            k = (pipeline_id, _op_key(op))
            if k in s._ram_mult:
                mult *= s._ram_mult[k]
            mult = min(mult, s._max_safety)

            if est_mem is not None:
                ram_req = est_mem * s._safety * mult
                # Avoid asking for slightly-too-large due to noise; still require it fits.
                if ram_req > avail_ram:
                    continue
                ram_req = max(ram_req, s._min_ram_gb)
            else:
                # No estimate: choose a conservative fraction by priority to reduce OOM risk.
                # High priority gets a bit more RAM to reduce failure probability.
                frac = 0.50 if pipeline_prio == Priority.QUERY else (0.40 if pipeline_prio == Priority.INTERACTIVE else 0.30)
                ram_req = max(s._min_ram_gb, min(avail_ram, float(pool.max_ram_pool) * frac))

            # CPU request: give high priority more CPU but avoid consuming everything for one op.
            # Keep enough CPU headroom to allow concurrency when available.
            if pipeline_prio == Priority.QUERY:
                cpu_target = min(avail_cpu, max(s._min_cpu, float(pool.max_cpu_pool) * 0.75))
            elif pipeline_prio == Priority.INTERACTIVE:
                cpu_target = min(avail_cpu, max(s._min_cpu, float(pool.max_cpu_pool) * 0.60))
            else:
                cpu_target = min(avail_cpu, max(s._min_cpu, float(pool.max_cpu_pool) * 0.40))

            # If CPU is extremely scarce, skip to avoid thrash (but still allow 1 CPU if possible).
            if cpu_target <= 0:
                continue

            # Score this placement:
            # - primary: larger slack after placement (reduces OOM risk spillover / future blocking)
            # - secondary: more CPU for high-priority (reduces latency)
            slack_ram = avail_ram - ram_req
            slack_cpu = avail_cpu - cpu_target
            score = (slack_ram, slack_cpu)
            if best is None or score > best[0]:
                best = (score, pool_id, cpu_target, ram_req)

        if best is None:
            return None, None, None
        return best[1], best[2], best[3]

    def _pipeline_sort_key(pipeline):
        # Priority first (handled by separate queues), then aging then memory-light-first packing.
        # Aging helps avoid indefinite starvation of batch under steady interactive load.
        pid = pipeline.pipeline_id
        age = s._tick - s._enqueue_tick.get(pid, s._tick)
        op = _get_next_assignable_op(pipeline)
        est_mem = _estimate_mem_gb(op) if op is not None else None
        est_mem_sort = est_mem if est_mem is not None else 1e18
        # Higher age should come first => negative age
        return (-age, est_mem_sort, pid)

    def _requeue_pipeline(pipeline):
        q = _queue_for(pipeline.priority)
        q.append(pipeline)
        s._enqueue_tick[pipeline.pipeline_id] = s._tick

    # --- Update state with new pipelines and results ---
    s._tick += 1

    for p in pipelines:
        _requeue_pipeline(p)

    # Incorporate failures: bump RAM multiplier and apply cooldown for the failed ops.
    # We treat any failure as "possibly OOM" and increase RAM slightly; this is a safe heuristic
    # for reducing repeated 720s penalties from pipelines that otherwise might complete.
    for r in results:
        try:
            if r.failed():
                # Increase RAM multiplier for those ops in that pipeline.
                for op in getattr(r, "ops", []) or []:
                    k = (r.pipeline_id if hasattr(r, "pipeline_id") else None, _op_key(op))
                    # If pipeline_id isn't on result, fall back to None-scoped key; still helps within tick.
                    if k[0] is None:
                        k = ("_unknown_pipeline_", _op_key(op))
                    cur = s._ram_mult.get(k, 1.0)
                    # Gentle bump: estimator already has safety; keep increases modest to avoid over-reserving.
                    s._ram_mult[k] = min(cur * 1.35, s._max_safety)
                    s._cooldown[k] = max(s._cooldown.get(k, 0), s._tick + s._cooldown_ticks)
        except Exception:
            pass

    # Early exit if nothing changed (but keep ticking for aging if desired)
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper to pop next schedulable pipeline from a queue.
    def _pop_next_schedulable(q):
        # Filter out completed pipelines; keep others.
        # We will rotate through queue elements to find something runnable that isn't in cooldown.
        if not q:
            return None

        # Build a small candidate set for efficiency, then choose best by sort key.
        # This avoids O(n log n) full sorts every tick.
        candidates = []
        max_scan = min(len(q), 32)
        scanned = 0
        while scanned < max_scan and q:
            p = q.pop(0)
            scanned += 1
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            op = _get_next_assignable_op(p)
            if op is None:
                # Not runnable now; keep it for later
                q.append(p)
                continue

            k = (p.pipeline_id, _op_key(op))
            if s._cooldown.get(k, 0) > s._tick:
                q.append(p)
                continue

            candidates.append(p)

        # Put back any remaining scanned-but-not-candidate pipelines already appended;
        # For candidates, pick best and requeue the rest.
        if not candidates:
            return None

        candidates.sort(key=_pipeline_sort_key)
        chosen = candidates[0]
        for p in candidates[1:]:
            q.append(p)
        return chosen

    # Scheduling loop: iterate pools; for each pool, place as many as fit.
    # Always drain QUERY then INTERACTIVE then BATCH, but allow batch to run if pools have slack.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        while True:
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Choose next pipeline based on priority dominance.
            chosen = None
            for q in (s.q_query, s.q_interactive, s.q_batch):
                chosen = _pop_next_schedulable(q)
                if chosen is not None:
                    break
            if chosen is None:
                break

            pipeline_id = chosen.pipeline_id
            op = _get_next_assignable_op(chosen)
            if op is None:
                # Can't run now; requeue
                _requeue_pipeline(chosen)
                continue

            # Pool selection and sizing:
            # We allow running the op on any pool, but since we're already iterating pool-by-pool,
            # we bias to the current pool if feasible; otherwise, we requeue and let other pool loops try.
            # To still leverage multi-pool fit, we compute best pool globally; if it's not this pool_id,
            # we requeue and allow the best pool to pick it up in its turn.
            best_pool_id, cpu_req, ram_req = _choose_pool_and_size(op, chosen.priority)
            if best_pool_id is None:
                # Doesn't fit anywhere right now; keep queued.
                _requeue_pipeline(chosen)
                break
            if best_pool_id != pool_id:
                # Let the more suitable pool schedule it.
                _requeue_pipeline(chosen)
                break

            # Cap requests to current pool availability.
            cpu_req = min(cpu_req, avail_cpu)
            ram_req = min(ram_req, avail_ram)

            # Final sanity checks
            if cpu_req <= 0 or ram_req <= 0:
                _requeue_pipeline(chosen)
                break

            assignment = Assignment(
                ops=[op][: s._max_assign_ops],
                cpu=cpu_req,
                ram=ram_req,
                priority=chosen.priority,
                pool_id=pool_id,
                pipeline_id=pipeline_id,
            )
            assignments.append(assignment)

            # Requeue the pipeline immediately to allow its next op to be considered in future ticks.
            _requeue_pipeline(chosen)

            # If resources are getting tight, stop packing to reduce fragmentation/overcommit risk.
            # This also protects QUERY/INTERACTIVE tail latency by leaving room for sudden arrivals.
            if chosen.priority in (Priority.QUERY, Priority.INTERACTIVE):
                if pool.avail_ram_pool < float(pool.max_ram_pool) * 0.10 or pool.avail_cpu_pool < float(pool.max_cpu_pool) * 0.10:
                    break
            else:
                if pool.avail_ram_pool < float(pool.max_ram_pool) * 0.05 or pool.avail_cpu_pool < float(pool.max_cpu_pool) * 0.05:
                    break

    return suspensions, assignments
