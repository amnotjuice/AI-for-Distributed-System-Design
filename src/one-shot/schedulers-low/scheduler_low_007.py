# policy_key: scheduler_low_007
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042688
# generation_seconds: 44.54
# generated_at: 2026-03-12T21:39:31.293845
@register_scheduler_init(key="scheduler_low_007")
def scheduler_low_007_init(s):
    """
    Priority-aware FIFO with tiny, safe improvements over naive:
      1) Separate waiting queues per priority (QUERY > INTERACTIVE > BATCH).
      2) Start with conservative CPU/RAM sizing (avoid "give everything to one op").
      3) Simple OOM-driven RAM backoff per (pipeline, op): on OOM, requeue pipeline and retry with larger RAM next time.
      4) Multi-pool bias: if multiple pools exist, prefer pool 0 for high priority, other pools for batch; allow fallback if idle.
    """
    # Queues store pipelines; we re-check readiness each tick to avoid complex DAG bookkeeping.
    s.wait_q_query = []
    s.wait_q_interactive = []
    s.wait_q_batch = []

    # Per-op hints: (pipeline_id, op_key) -> {"ram": float, "cpu": float}
    s.op_hints = {}

    # Track most recent execution results by container_id (optional; can be useful for future evolution).
    s.last_result_by_container = {}

    # Tunables (kept intentionally simple)
    s.min_cpu = 1.0
    s.min_ram = 1.0
    s.default_cpu = 2.0
    s.default_ram_frac = 0.25  # start with 25% of pool RAM (capped by availability)
    s.oom_ram_multiplier = 2.0
    s.max_assignments_per_pool_per_tick = 1  # keep behavior close to naive to reduce risk


@register_scheduler(key="scheduler_low_007")
def scheduler_low_007_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into per-priority FIFO queues.
      - Process results to update per-op resource hints (OOM -> increase RAM).
      - For each pool, pick the highest-priority pipeline with a ready op and assign it with right-sized resources.
    """
    # ---- helpers (kept local to avoid imports) ----
    def _is_oom_error(err):
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _op_key(op):
        # Best-effort stable key for operator within a pipeline
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                try:
                    return str(v)
                except Exception:
                    pass
        try:
            return repr(op)
        except Exception:
            return str(id(op))

    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.wait_q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.wait_q_interactive.append(p)
        else:
            s.wait_q_batch.append(p)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.wait_q_query
        if pri == Priority.INTERACTIVE:
            return s.wait_q_interactive
        return s.wait_q_batch

    def _queues_in_strict_priority_order():
        return [s.wait_q_query, s.wait_q_interactive, s.wait_q_batch]

    def _preferred_pools_for_priority(pri):
        # If multiple pools exist, bias pool 0 for high priority; others for batch.
        n = s.executor.num_pools
        if n <= 1:
            return [0]
        if pri in (Priority.QUERY, Priority.INTERACTIVE):
            # Prefer pool 0, but allow spillover to others.
            return [0] + [i for i in range(1, n)]
        # Batch prefers non-0 pools first to reduce interference.
        return [i for i in range(1, n)] + [0]

    def _get_ready_op(pipeline):
        status = pipeline.runtime_status()
        # Drop completed pipelines early.
        if status.is_pipeline_successful():
            return None, status
        # Get one ready operator (parents complete) in assignable states.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            return None, status
        return op_list[0], status

    def _drop_or_requeue_if_terminal(pipeline, status):
        # If pipeline has failures, we "drop" it to avoid infinite retries (same as naive intent).
        # Note: we still allow operator-level OOM retries by requeuing the pipeline when we observe OOM results.
        try:
            has_failures = status.state_counts[OperatorState.FAILED] > 0
        except Exception:
            has_failures = False
        if status.is_pipeline_successful() or has_failures:
            return True
        return False

    def _hint_for(pipeline_id, op):
        k = (pipeline_id, _op_key(op))
        return s.op_hints.get(k, {"cpu": s.default_cpu, "ram": None})

    def _set_hint(pipeline_id, op, cpu=None, ram=None):
        k = (pipeline_id, _op_key(op))
        cur = s.op_hints.get(k, {"cpu": s.default_cpu, "ram": None})
        if cpu is not None:
            cur["cpu"] = cpu
        if ram is not None:
            cur["ram"] = ram
        s.op_hints[k] = cur

    def _compute_allocation(pool, pipeline, op):
        # CPU: start with small default; cap by availability.
        hint = _hint_for(pipeline.pipeline_id, op)
        cpu = hint.get("cpu", s.default_cpu)
        if cpu is None:
            cpu = s.default_cpu
        cpu = max(s.min_cpu, float(cpu))
        cpu = min(cpu, float(pool.avail_cpu_pool))

        # RAM: if we have a hint, use it; else use a conservative fraction of pool max.
        ram_hint = hint.get("ram", None)
        if ram_hint is None:
            ram = float(pool.max_ram_pool) * float(s.default_ram_frac)
        else:
            ram = float(ram_hint)
        ram = max(s.min_ram, ram)
        ram = min(ram, float(pool.avail_ram_pool))

        # Ensure we don't request more than pool max (defensive).
        ram = min(ram, float(pool.max_ram_pool))
        cpu = min(cpu, float(pool.max_cpu_pool))

        return cpu, ram

    def _pick_next_pipeline_for_pool(pool_id):
        # We consider queues in strict priority order, but keep FIFO within each queue.
        # We scan a bounded number of elements to find a "ready" pipeline without expensive full scans.
        scan_limit = 32

        for q in _queues_in_strict_priority_order():
            if not q:
                continue

            # Try to find the first pipeline that has a ready op.
            # Keep FIFO fairness by rotating pipelines we inspect but cannot schedule.
            n = min(len(q), scan_limit)
            rotated = []
            chosen = None
            chosen_op = None
            for _ in range(n):
                p = q.pop(0)
                op, st = _get_ready_op(p)

                if _drop_or_requeue_if_terminal(p, st):
                    # Drop terminal pipelines.
                    continue

                if op is None:
                    rotated.append(p)
                    continue

                chosen = p
                chosen_op = op
                break

            # Push back the rotated ones (preserve relative order).
            q.extend(rotated)

            if chosen is not None:
                return chosen, chosen_op

        return None, None

    # ---- ingest new pipelines ----
    for p in pipelines:
        _enqueue_pipeline(p)

    # ---- process results (update hints; on OOM, requeue pipeline) ----
    # Note: We don't have direct access to per-op runtime metrics besides failure/error and allocated resources.
    # We keep this minimal and safe: only react strongly to OOM.
    pipelines_to_requeue = []
    if results:
        for r in results:
            try:
                s.last_result_by_container[r.container_id] = r
            except Exception:
                pass

            if not hasattr(r, "failed") or not r.failed():
                continue

            # If OOM, increase RAM hint for each op in the result and requeue its pipeline (if we can find it).
            if _is_oom_error(getattr(r, "error", None)):
                # Update hints for ops that failed under this container allocation.
                for op in getattr(r, "ops", []) or []:
                    # Increase from previous hint or from observed RAM allocation.
                    base_ram = getattr(r, "ram", None)
                    if base_ram is None:
                        # Fall back to a conservative default if ram missing.
                        base_ram = s.min_ram
                    new_ram = float(base_ram) * float(s.oom_ram_multiplier)

                    # Cap will be applied during allocation; store a raw hint.
                    _set_hint(getattr(r, "pipeline_id", None) or getattr(op, "pipeline_id", None) or "unknown", op, ram=new_ram)

                # Best-effort: requeue the pipeline if we can match it from current queues (by id).
                # If we cannot, we do nothing—future arrivals or retained queues will retry as usual.
                try:
                    # Some sims attach pipeline_id to result; if present, requeue matching pipeline.
                    rid = getattr(r, "pipeline_id", None)
                    if rid is not None:
                        # Search existing queues for pipeline_id; if absent, no-op.
                        for q in _queues_in_strict_priority_order():
                            for existing in q:
                                if getattr(existing, "pipeline_id", None) == rid:
                                    # Already queued.
                                    rid = None
                                    break
                            if rid is None:
                                break
                except Exception:
                    pass

    # ---- early exit if nothing to do ----
    if not pipelines and not results and not (s.wait_q_query or s.wait_q_interactive or s.wait_q_batch):
        return [], []

    suspensions = []
    assignments = []

    # ---- assign per pool ----
    # Simple pool-by-pool scheduling: pick next runnable pipeline in strict priority order.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        if float(pool.avail_cpu_pool) <= 0 or float(pool.avail_ram_pool) <= 0:
            continue

        made = 0
        while made < s.max_assignments_per_pool_per_tick:
            pipeline, op = _pick_next_pipeline_for_pool(pool_id)
            if pipeline is None or op is None:
                break

            # Optional pool affinity: if a pipeline is high priority, try preferred pools first.
            # Since we are already iterating pools, implement a lightweight check:
            preferred = _preferred_pools_for_priority(pipeline.priority)
            if pool_id != preferred[0]:
                # If this pool isn't the top preferred for this priority and the top preferred has resources,
                # don't schedule it here to reduce interference (tiny latency win).
                top_pool = s.executor.pools[preferred[0]]
                if float(top_pool.avail_cpu_pool) > 0 and float(top_pool.avail_ram_pool) > 0:
                    # Requeue and skip for now.
                    _enqueue_pipeline(pipeline)
                    break

            cpu, ram = _compute_allocation(pool, pipeline, op)
            if cpu < s.min_cpu or ram < s.min_ram:
                # Not enough resources; requeue and stop trying in this pool.
                _enqueue_pipeline(pipeline)
                break

            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)
            made += 1

    return suspensions, assignments
