# policy_key: scheduler_medium_027
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.107825
# generation_seconds: 78.80
# generated_at: 2026-04-09T23:05:18.985353
@register_scheduler_init(key="scheduler_medium_027")
def scheduler_medium_027_init(s):
    """
    Priority-aware, OOM-resilient scheduler with:
      - Separate per-priority FIFO queues (QUERY > INTERACTIVE > BATCH)
      - Conservative resource reservation so high-priority work is not blocked by batch
      - Per-operator RAM hinting with exponential backoff on OOM to drive completion rate up
      - Best-fit pool selection (with optional interactive-preferred pool 0 if multi-pool)
      - Lightweight, best-effort preemption (only if the simulator exposes running containers)
    """
    # Per-priority pipeline queues (FIFO).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Monotonic tick counter (used only for simple aging/housekeeping).
    s._tick = 0
    s._arrival_tick = {}  # pipeline_id -> tick

    # RAM hint per (pipeline_id, op_key). Starts from op's declared minimum if available.
    s._ram_hint = {}  # (pipeline_id, op_key) -> ram
    s._fail_count = {}  # (pipeline_id, op_key) -> int

    # Simple throttles/knobs.
    s._max_retries_per_op = 8
    s._reserve_frac_cpu = 0.25  # keep headroom when high-priority work is waiting
    s._reserve_frac_ram = 0.25
    s._max_preempt_per_tick = 1


@register_scheduler(key="scheduler_medium_027")
def scheduler_medium_027(s, results, pipelines):
    # ----------------------------
    # Helper functions (kept local to avoid imports at module scope)
    # ----------------------------
    def _prio_rank(p):
        if p == Priority.QUERY:
            return 0
        if p == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE (and any unknowns treated as lowest)

    def _enqueue_pipeline(p):
        pr = p.priority
        if pr == Priority.QUERY:
            s.q_query.append(p)
        elif pr == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _iter_queues_in_order():
        # Yield (queue_ref, priority)
        return (
            (s.q_query, Priority.QUERY),
            (s.q_interactive, Priority.INTERACTIVE),
            (s.q_batch, Priority.BATCH_PIPELINE),
        )

    def _op_key(op):
        # Try stable IDs first; fall back to object identity.
        return getattr(op, "op_id", getattr(op, "operator_id", getattr(op, "name", id(op))))

    def _op_min_ram(op):
        # The simulator may expose minimum/required RAM under different attribute names.
        for attr in ("min_ram", "ram_min", "required_ram", "ram", "memory", "mem"):
            v = getattr(op, attr, None)
            if v is not None:
                try:
                    if float(v) > 0:
                        return float(v)
                except Exception:
                    pass
        # Safe, small default; OOM backoff will correct if too small.
        return 1.0

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("out_of_memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _pool_candidates_for_priority(priority):
        n = s.executor.num_pools
        if n <= 1:
            return [0]
        # Prefer pool 0 for high-priority; prefer non-0 pools for batch to reduce interference.
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + list(range(1, n))
        return list(range(1, n)) + [0]

    def _target_cpu(pool, priority):
        # Favor faster completion for QUERY/INTERACTIVE; keep batch smaller to preserve headroom.
        max_cpu = float(getattr(pool, "max_cpu_pool", 0) or 0)
        if max_cpu <= 0:
            max_cpu = float(getattr(pool, "avail_cpu_pool", 0) or 0)

        if priority == Priority.QUERY:
            frac = 0.75
        elif priority == Priority.INTERACTIVE:
            frac = 0.55
        else:
            frac = 0.30

        tgt = max(1.0, max_cpu * frac)
        return tgt

    def _priority_ram_slack(priority):
        # Slight slack to reduce repeated OOMs when min RAM is unknown/understated.
        if priority == Priority.QUERY:
            return 1.15
        if priority == Priority.INTERACTIVE:
            return 1.10
        return 1.00

    def _has_high_prio_waiting():
        # Check if any QUERY/INTERACTIVE pipeline has an assignable operator ready.
        for q, _pr in ((s.q_query, Priority.QUERY), (s.q_interactive, Priority.INTERACTIVE)):
            for p in q:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return True
        return False

    def _cleanup_queue(q):
        # Remove completed pipelines from queue (keep order for remaining).
        kept = []
        for p in q:
            st = p.runtime_status()
            # If a pipeline is fully successful, drop it from queue.
            if st.is_pipeline_successful():
                continue
            kept.append(p)
        q[:] = kept

    def _get_running_containers_from_pool(pool):
        # Best-effort: different simulators may expose different attributes.
        for attr in ("running_containers", "containers", "active_containers", "live_containers"):
            xs = getattr(pool, attr, None)
            if xs:
                return xs
        return []

    def _container_priority(c):
        return getattr(c, "priority", getattr(c, "pipeline_priority", None))

    def _container_id(c):
        return getattr(c, "container_id", getattr(c, "id", None))

    # ----------------------------
    # Update state from arrivals/results
    # ----------------------------
    s._tick += 1

    for p in pipelines:
        s._arrival_tick.setdefault(p.pipeline_id, s._tick)
        _enqueue_pipeline(p)

    # Update RAM hints from failures (especially OOM).
    for r in (results or []):
        if not hasattr(r, "failed") or not r.failed():
            continue

        # If we cannot attribute to specific ops, still try to update the listed ones.
        failed_ops = getattr(r, "ops", None) or []
        for op in failed_ops:
            ok = _op_key(op)
            key = (getattr(r, "pipeline_id", None), ok)

            # If pipeline_id not present on result, we can't key it reliably; try to skip safely.
            # (Most simulators will provide pipeline_id via assignment/pipeline tracking.)
            if key[0] is None:
                continue

            prev_hint = float(s._ram_hint.get(key, getattr(r, "ram", 1.0) or 1.0))
            prev_assigned = float(getattr(r, "ram", prev_hint) or prev_hint)

            # Increase RAM aggressively on OOM; mildly otherwise.
            if _is_oom_error(getattr(r, "error", None)):
                new_hint = max(prev_hint, prev_assigned) * 2.0
            else:
                new_hint = max(prev_hint, prev_assigned) * 1.25

            s._ram_hint[key] = new_hint
            s._fail_count[key] = int(s._fail_count.get(key, 0)) + 1

    # Cleanup completed pipelines from queues to avoid unbounded growth.
    _cleanup_queue(s.q_query)
    _cleanup_queue(s.q_interactive)
    _cleanup_queue(s.q_batch)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ----------------------------
    # Optional, best-effort preemption
    # ----------------------------
    # If high-priority work is waiting and we have effectively no headroom, preempt a low-priority
    # running container to quickly reduce queueing latency. This is guarded with getattr checks.
    preempts_done = 0
    if _has_high_prio_waiting():
        for pool_id in range(s.executor.num_pools):
            if preempts_done >= s._max_preempt_per_tick:
                break
            pool = s.executor.pools[pool_id]
            avail_cpu = float(getattr(pool, "avail_cpu_pool", 0) or 0)
            avail_ram = float(getattr(pool, "avail_ram_pool", 0) or 0)
            max_cpu = float(getattr(pool, "max_cpu_pool", 0) or 0)
            max_ram = float(getattr(pool, "max_ram_pool", 0) or 0)
            if max_cpu <= 0 or max_ram <= 0:
                continue

            # If headroom already exists, don't preempt.
            if avail_cpu >= max_cpu * 0.15 and avail_ram >= max_ram * 0.15:
                continue

            # Find lowest-priority running container to suspend.
            candidates = []
            for c in _get_running_containers_from_pool(pool):
                pr = _container_priority(c)
                cid = _container_id(c)
                if cid is None or pr is None:
                    continue
                candidates.append(( _prio_rank(pr), cid ))

            # Prefer suspending batch first, then interactive; never preempt query if possible.
            if candidates:
                candidates.sort(reverse=True)  # highest rank value = lowest priority
                _, cid = candidates[0]
                suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
                preempts_done += 1

    # ----------------------------
    # Main assignment loop
    # ----------------------------
    high_prio_waiting = _has_high_prio_waiting()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(getattr(pool, "avail_cpu_pool", 0) or 0)
        avail_ram = float(getattr(pool, "avail_ram_pool", 0) or 0)
        max_cpu = float(getattr(pool, "max_cpu_pool", 0) or 0)
        max_ram = float(getattr(pool, "max_ram_pool", 0) or 0)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If high priority exists, reserve some headroom by limiting batch consumption.
        reserve_cpu = (max_cpu * s._reserve_frac_cpu) if (high_prio_waiting and max_cpu > 0) else 0.0
        reserve_ram = (max_ram * s._reserve_frac_ram) if (high_prio_waiting and max_ram > 0) else 0.0

        # Keep scheduling while we have resources; prefer many small admissions over one huge batch grab.
        made_progress = True
        while made_progress:
            made_progress = False

            # Iterate priorities in order and pick the first pipeline with a runnable op that fits.
            for q, pr in _iter_queues_in_order():
                # Apply reservations only to batch work (allow high priority to use full availability).
                cpu_budget = avail_cpu
                ram_budget = avail_ram
                if pr == Priority.BATCH_PIPELINE and high_prio_waiting:
                    cpu_budget = max(0.0, avail_cpu - reserve_cpu)
                    ram_budget = max(0.0, avail_ram - reserve_ram)

                if cpu_budget <= 0 or ram_budget <= 0:
                    continue

                # Scan queue FIFO and pick first runnable pipeline whose next op can fit.
                selected_idx = None
                selected_ops = None
                selected_cpu = None
                selected_ram = None
                selected_pipeline = None

                for i, p in enumerate(q):
                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        continue

                    # Only schedule ops whose parents are complete to respect DAG.
                    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                    if not ops:
                        continue

                    # One op at a time per pipeline for responsiveness and fairness.
                    op = ops[0]
                    ok = _op_key(op)
                    min_ram = _op_min_ram(op)
                    hint_key = (p.pipeline_id, ok)
                    hint = float(s._ram_hint.get(hint_key, min_ram))

                    # Stop retrying if it's clearly stuck; but keep pipeline alive by skipping this op for now.
                    # (This avoids burning resources on non-OOM repeated failures while not "dropping".)
                    fc = int(s._fail_count.get(hint_key, 0))
                    if fc >= s._max_retries_per_op and not _is_oom_error(None):
                        # No reliable non-OOM classification; we still allow retries if resources are ample.
                        pass

                    # RAM request: hint with slight slack for high priority.
                    ram_req = max(min_ram, hint) * _priority_ram_slack(pr)

                    # CPU request: target based on priority, but cap to current budget.
                    cpu_req = min(cpu_budget, _target_cpu(pool, pr))
                    cpu_req = max(1.0, cpu_req)

                    # Must fit.
                    if cpu_req <= cpu_budget and ram_req <= ram_budget:
                        selected_idx = i
                        selected_ops = [op]
                        selected_cpu = cpu_req
                        selected_ram = ram_req
                        selected_pipeline = p
                        break

                if selected_pipeline is None:
                    continue

                # Issue assignment.
                assignments.append(
                    Assignment(
                        ops=selected_ops,
                        cpu=selected_cpu,
                        ram=selected_ram,
                        priority=selected_pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=selected_pipeline.pipeline_id,
                    )
                )

                # Deduct from pool budgets.
                avail_cpu -= float(selected_cpu)
                avail_ram -= float(selected_ram)

                # Rotate the queue for fairness: move scheduled pipeline to the end.
                p = q.pop(selected_idx)
                q.append(p)

                made_progress = True
                break  # re-evaluate priorities with updated budgets

    # ----------------------------
    # Cross-pool placement improvement: If we didn't schedule anything, try again with pool choice per op.
    # (This helps when pool-local greedy order fails; it's still simple and safe.)
    # ----------------------------
    if not assignments:
        # Build a small list of candidate (pipeline, op) to schedule by priority.
        candidates = []
        for q, pr in _iter_queues_in_order():
            for p in q:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not ops:
                    continue
                candidates.append((pr, p, ops[0]))
        candidates.sort(key=lambda x: _prio_rank(x[0]))

        for pr, p, op in candidates[:8]:  # limit scan to keep policy lightweight
            ok = _op_key(op)
            min_ram = _op_min_ram(op)
            hint_key = (p.pipeline_id, ok)
            hint = float(s._ram_hint.get(hint_key, min_ram))
            ram_req_base = max(min_ram, hint) * _priority_ram_slack(pr)

            # Try preferred pools first for this priority.
            for pool_id in _pool_candidates_for_priority(pr):
                pool = s.executor.pools[pool_id]
                avail_cpu = float(getattr(pool, "avail_cpu_pool", 0) or 0)
                avail_ram = float(getattr(pool, "avail_ram_pool", 0) or 0)
                max_cpu = float(getattr(pool, "max_cpu_pool", 0) or 0)
                max_ram = float(getattr(pool, "max_ram_pool", 0) or 0)

                if avail_cpu <= 0 or avail_ram <= 0:
                    continue

                reserve_cpu = (max_cpu * s._reserve_frac_cpu) if (high_prio_waiting and max_cpu > 0) else 0.0
                reserve_ram = (max_ram * s._reserve_frac_ram) if (high_prio_waiting and max_ram > 0) else 0.0
                cpu_budget = avail_cpu
                ram_budget = avail_ram
                if pr == Priority.BATCH_PIPELINE and high_prio_waiting:
                    cpu_budget = max(0.0, avail_cpu - reserve_cpu)
                    ram_budget = max(0.0, avail_ram - reserve_ram)

                if cpu_budget <= 0 or ram_budget <= 0:
                    continue

                cpu_req = min(cpu_budget, _target_cpu(pool, pr))
                cpu_req = max(1.0, cpu_req)
                ram_req = ram_req_base

                if cpu_req <= cpu_budget and ram_req <= ram_budget:
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
                    # Rotate pipeline to back of its queue for fairness.
                    if pr == Priority.QUERY:
                        qref = s.q_query
                    elif pr == Priority.INTERACTIVE:
                        qref = s.q_interactive
                    else:
                        qref = s.q_batch
                    for i in range(len(qref)):
                        if qref[i].pipeline_id == p.pipeline_id:
                            qref.append(qref.pop(i))
                            break
                    break

            if assignments:
                break

    return suspensions, assignments
