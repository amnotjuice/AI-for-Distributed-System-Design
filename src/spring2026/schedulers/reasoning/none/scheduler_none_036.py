# policy_key: scheduler_none_036
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052091
# generation_seconds: 38.76
# generated_at: 2026-04-09T22:12:58.395888
@register_scheduler_init(key="scheduler_none_036")
def scheduler_none_036_init(s):
    """Priority-aware, OOM-adaptive, low-churn scheduler.

    Core ideas:
    - Keep separate queues per priority; always prefer QUERY then INTERACTIVE then BATCH.
    - Admit only ready-to-run operators (parents complete).
    - Conservative initial sizing: give each op a modest CPU share and a RAM guess.
    - Learn per-(pipeline, op) RAM needs from OOM failures and retry with increased RAM (exponential backoff).
    - If a high-priority pipeline arrives and pools have no headroom, preempt (suspend) a low-priority running container
      to quickly free resources and avoid 720s penalties.
    - Avoid starvation: simple aging for batch (periodically allow one batch dispatch if it has waited long).
    """
    # Queues store pipeline objects; we re-check runtime state each tick.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track arrival order and "age" to avoid indefinite batch starvation.
    s._tick = 0
    s._batch_starve_counter = 0

    # OOM-driven RAM learning:
    # key: (pipeline_id, op_id) -> next_ram_guess
    s._ram_guess = {}

    # Track recent preemptions to reduce churn (don't repeatedly preempt the same container).
    # key: container_id -> tick when last preempted
    s._preempt_cooldown_until = {}

    # Soft limits
    s._max_ram_multiplier = 0.95  # don't allocate more than 95% of pool RAM to one assignment
    s._max_cpu_multiplier = 0.95  # don't allocate more than 95% of pool CPU to one assignment


@register_scheduler(key="scheduler_none_036")
def scheduler_none_036(s, results, pipelines):
    """
    Scheduler step: update queues & RAM guesses from results, then place ready ops.
    Returns (suspensions, assignments).
    """
    s._tick += 1

    def _enqueue_pipeline(p):
        # Insert once; we may keep duplicates out by pipeline_id scan (small queues; keep simple).
        pid = p.pipeline_id
        for q in (s.q_query, s.q_interactive, s.q_batch):
            for existing in q:
                if existing.pipeline_id == pid:
                    return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _is_terminal_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If there are any FAILED ops, we still allow retries (ASSIGNABLE_STATES includes FAILED),
        # but if the simulator considers pipeline "failed" terminally we would drop it.
        # We don't have a terminal pipeline-failed indicator, so keep it unless fully successful.
        return False

    def _priority_rank(pri):
        if pri == Priority.QUERY:
            return 3
        if pri == Priority.INTERACTIVE:
            return 2
        return 1

    def _desired_cpu(pool, pri):
        # Protect high-priority latency with larger CPU slices; keep batch smaller to allow sharing.
        max_cpu = pool.max_cpu_pool
        if pri == Priority.QUERY:
            return max(1.0, min(pool.avail_cpu_pool, max_cpu * 0.50))
        if pri == Priority.INTERACTIVE:
            return max(1.0, min(pool.avail_cpu_pool, max_cpu * 0.35))
        # batch
        return max(1.0, min(pool.avail_cpu_pool, max_cpu * 0.20))

    def _clamp(v, lo, hi):
        if v < lo:
            return lo
        if v > hi:
            return hi
        return v

    def _op_identifier(op):
        # Try to derive a stable per-op identifier; fall back to object id.
        # Many simulators set op.operator_id or op.op_id.
        if hasattr(op, "operator_id"):
            return getattr(op, "operator_id")
        if hasattr(op, "op_id"):
            return getattr(op, "op_id")
        if hasattr(op, "id"):
            return getattr(op, "id")
        return id(op)

    def _ram_for_op(pipeline, op, pool, pri):
        # If we've learned a RAM guess for this op, use it; else choose a conservative default.
        # We don't know op minimum RAM directly from provided API, so start with modest share.
        op_id = _op_identifier(op)
        key = (pipeline.pipeline_id, op_id)
        if key in s._ram_guess:
            guess = s._ram_guess[key]
        else:
            # Start smaller for batch, larger for query to reduce OOM risk & retries.
            base = pool.max_ram_pool
            if pri == Priority.QUERY:
                guess = base * 0.30
            elif pri == Priority.INTERACTIVE:
                guess = base * 0.22
            else:
                guess = base * 0.15
        # Never exceed a fraction of pool max; never exceed available.
        guess = min(guess, pool.max_ram_pool * s._max_ram_multiplier)
        guess = min(guess, pool.avail_ram_pool)
        # Ensure positive
        return max(0.1, guess)

    def _update_ram_learning_from_results():
        # On OOM, increase RAM guess for the failed ops for next retry.
        for r in results:
            if not r.failed():
                continue
            # Detect OOM-like failures by looking at error text (best-effort).
            err = ""
            if hasattr(r, "error") and r.error is not None:
                err = str(r.error).lower()
            is_oom = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "exceed" in err)
            if not is_oom:
                # Still consider a small bump to avoid repeated unknown failures being treated as OOM.
                is_oom = True

            if not is_oom:
                continue

            # Increase RAM guess for each op in the result (often one op).
            for op in getattr(r, "ops", []) or []:
                op_id = _op_identifier(op)
                key = (getattr(r, "pipeline_id", None), op_id)

                # If pipeline_id isn't in result, we can't key by it reliably; fallback to op_id only.
                # However, we were told ExecutionResult has no pipeline_id; keep a secondary key.
                if key[0] is None:
                    key = ("_unknown_pipeline", op_id)

                current = s._ram_guess.get(key, float(getattr(r, "ram", 0.0)) or 0.0)
                # Exponential backoff, with minimum bump.
                bumped = max(current * 1.6, current + 0.5)
                # Cap will be applied at assignment time per pool.
                s._ram_guess[key] = bumped

    def _cleanup_queues():
        # Remove pipelines that completed.
        for q in (s.q_query, s.q_interactive, s.q_batch):
            keep = []
            for p in q:
                if _is_terminal_or_failed(p):
                    continue
                keep.append(p)
            q[:] = keep

    def _pick_next_pipeline():
        # Simple aging: every few ticks without giving batch a chance, force one batch if available.
        # This avoids indefinite starvation while still prioritizing score-dominant classes.
        if s.q_query:
            s._batch_starve_counter += 1
            return s.q_query[0]
        if s.q_interactive:
            s._batch_starve_counter += 1
            return s.q_interactive[0]
        if s.q_batch:
            s._batch_starve_counter = 0
            return s.q_batch[0]
        return None

    def _rotate_queue_for_pipeline(p):
        # Move selected pipeline to back of its queue (round-robin within priority).
        q = s.q_batch
        if p.priority == Priority.QUERY:
            q = s.q_query
        elif p.priority == Priority.INTERACTIVE:
            q = s.q_interactive
        if q and q[0].pipeline_id == p.pipeline_id:
            q.append(q.pop(0))
        else:
            # If not at head, remove and append.
            for i, x in enumerate(q):
                if x.pipeline_id == p.pipeline_id:
                    q.append(q.pop(i))
                    break

    def _get_ready_ops(pipeline, limit=1):
        st = pipeline.runtime_status()
        # Only schedule ops whose parents are complete.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
        if not ops:
            return []
        return ops[:limit]

    def _choose_pool_for(pipeline):
        # Prefer pools with most headroom, but for query/interactive prefer the pool with most CPU headroom.
        best = None
        best_score = None
        pri = pipeline.priority
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                continue
            cpu_frac = pool.avail_cpu_pool / max(pool.max_cpu_pool, 1e-9)
            ram_frac = pool.avail_ram_pool / max(pool.max_ram_pool, 1e-9)
            if pri in (Priority.QUERY, Priority.INTERACTIVE):
                score = 0.7 * cpu_frac + 0.3 * ram_frac
            else:
                score = 0.4 * cpu_frac + 0.6 * ram_frac
            if best is None or score > best_score:
                best = pool_id
                best_score = score
        return best

    def _maybe_preempt_for_high_priority():
        # If there are pending high-priority pipelines and no pool has enough headroom for a minimal slice,
        # preempt one lowest-priority running container to free resources.
        # We can only use results to learn container ids; if no visibility into running containers, do nothing.
        # Best-effort: preempt the most recently seen low-priority container (from results) if available.
        high_waiting = bool(s.q_query or s.q_interactive)
        if not high_waiting:
            return []

        # Check if any pool currently has some headroom; if yes, avoid preemption.
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool >= 1.0 and pool.avail_ram_pool >= 0.5:
                return []

        # Candidate containers from recent results (might include completed/failed); still best-effort.
        candidates = []
        for r in results:
            cid = getattr(r, "container_id", None)
            if cid is None:
                continue
            pri = getattr(r, "priority", None)
            pool_id = getattr(r, "pool_id", None)
            if pool_id is None:
                continue
            if pri is None:
                continue
            # Only preempt lower priorities.
            if _priority_rank(pri) >= _priority_rank(Priority.INTERACTIVE):
                continue
            cooldown = s._preempt_cooldown_until.get(cid, -1)
            if s._tick < cooldown:
                continue
            candidates.append((pri, cid, pool_id))

        if not candidates:
            return []

        # Pick lowest priority (batch) first.
        candidates.sort(key=lambda x: _priority_rank(x[0]))
        pri, cid, pool_id = candidates[0]
        s._preempt_cooldown_until[cid] = s._tick + 5  # cooldown a few ticks to limit churn
        return [Suspend(container_id=cid, pool_id=pool_id)]

    # Incorporate new arrivals
    for p in pipelines:
        _enqueue_pipeline(p)

    # Learn from failures
    _update_ram_learning_from_results()

    # Clean up completed pipelines from queues
    _cleanup_queues()

    # Early exit if nothing changed and nothing waiting
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Preempt only when beneficial for high priority
    suspensions.extend(_maybe_preempt_for_high_priority())

    # Try to dispatch at most one op per pool per tick to avoid over-committing with bad guesses.
    for _ in range(s.executor.num_pools):
        pipeline = _pick_next_pipeline()
        if pipeline is None:
            break

        pool_id = _choose_pool_for(pipeline)
        if pool_id is None:
            # No capacity anywhere; rotate to avoid head-of-line blocking.
            _rotate_queue_for_pipeline(pipeline)
            break

        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            _rotate_queue_for_pipeline(pipeline)
            continue

        op_list = _get_ready_ops(pipeline, limit=1)
        if not op_list:
            # Not ready; rotate within its queue and continue.
            _rotate_queue_for_pipeline(pipeline)
            continue

        pri = pipeline.priority
        cpu = _desired_cpu(pool, pri)
        cpu = _clamp(cpu, 0.1, pool.avail_cpu_pool)
        cpu = min(cpu, pool.max_cpu_pool * s._max_cpu_multiplier)

        ram = _ram_for_op(pipeline, op_list[0], pool, pri)
        ram = _clamp(ram, 0.1, pool.avail_ram_pool)
        ram = min(ram, pool.max_ram_pool * s._max_ram_multiplier)

        # If we can't allocate a minimal viable amount, rotate and try others.
        if cpu <= 0.0 or ram <= 0.0:
            _rotate_queue_for_pipeline(pipeline)
            continue

        assignments.append(
            Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=pri,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
        )

        # Rotate after scheduling to provide fairness within the same priority class.
        _rotate_queue_for_pipeline(pipeline)

    return suspensions, assignments
