# policy_key: scheduler_est_043
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045892
# generation_seconds: 47.62
# generated_at: 2026-04-10T10:20:26.120114
@register_scheduler_init(key="scheduler_est_043")
def scheduler_est_043_init(s):
    """Priority + memory-aware greedy scheduler with OOM-sensitive retries.

    Core ideas:
      - Keep separate FIFO queues per priority; always try to schedule higher priority first.
      - Use op.estimate.mem_peak_gb (when present) to choose a pool that can likely fit, and to
        best-fit pack RAM to reduce fragmentation and OOM cascades.
      - On suspected OOM failure, retry the same operator with increased RAM (backoff multiplier).
      - Avoid dropping/starving pipelines: keep them queued until success or retry budget exhausted.
    """
    # Queues store pipeline_ids (we keep pipeline objects in a dict for easy refresh).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    s.pipelines_by_id = {}     # pipeline_id -> Pipeline
    s.enqueued_at = {}         # pipeline_id -> logical time
    s.now = 0                  # logical time (ticks of scheduler invocations)

    # Per-operator retry + RAM backoff, keyed by (pipeline_id, operator_object_id).
    s.op_attempts = {}         # (pid, op_key) -> attempts
    s.op_ram_mult = {}         # (pid, op_key) -> multiplier (>= 1.0)

    # Retry policy (keep modest to avoid infinite thrash).
    s.max_attempts_oom = 5
    s.max_attempts_other = 2

    # RAM sizing knobs.
    s.min_ram_gb = 1.0
    s.default_ram_gb_when_unknown = 2.0
    s.mem_safety_factor = 1.25  # treat estimator as a hint; small headroom reduces OOMs

    # CPU sizing knobs: per-assignment CPU targets by priority (fraction of pool max).
    s.cpu_frac_query = 0.80
    s.cpu_frac_interactive = 0.60
    s.cpu_frac_batch = 0.40
    s.cpu_min = 1.0


@register_scheduler(key="scheduler_est_043")
def scheduler_est_043(s, results, pipelines):
    # Helper functions are defined inside to avoid module-level imports per instructions.
    def _priority_rank(prio):
        # Higher rank scheduled earlier.
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1

    def _get_queue(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _is_probable_oom(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _op_key(pid, op):
        # Use object identity for stability within a simulation run.
        return (pid, id(op))

    def _est_mem_gb(op):
        try:
            est = op.estimate.mem_peak_gb
        except Exception:
            est = None
        if est is None:
            return None
        try:
            # Ensure sane, non-negative float.
            est_f = float(est)
            if est_f < 0:
                return None
            return est_f
        except Exception:
            return None

    def _ram_request_gb(pid, op, pool):
        est = _est_mem_gb(op)
        base = s.default_ram_gb_when_unknown if est is None else max(s.min_ram_gb, est * s.mem_safety_factor)
        mult = s.op_ram_mult.get(_op_key(pid, op), 1.0)
        req = base * mult
        # Clamp to pool max.
        try:
            req = min(req, float(pool.max_ram_pool))
        except Exception:
            pass
        return max(s.min_ram_gb, req)

    def _cpu_request(pid, pipeline_priority, pool, avail_cpu):
        # Use a target fraction of pool max CPU, capped by availability.
        try:
            max_cpu = float(pool.max_cpu_pool)
        except Exception:
            max_cpu = float(avail_cpu) if avail_cpu is not None else s.cpu_min

        if pipeline_priority == Priority.QUERY:
            target = max_cpu * s.cpu_frac_query
        elif pipeline_priority == Priority.INTERACTIVE:
            target = max_cpu * s.cpu_frac_interactive
        else:
            target = max_cpu * s.cpu_frac_batch

        # Clamp to [cpu_min, avail_cpu]
        cpu = max(s.cpu_min, target)
        if avail_cpu is not None:
            cpu = min(cpu, float(avail_cpu))
        # If availability is fractional/float, keep as-is; Assignment accepts numeric.
        return cpu

    def _best_fit_pool_for_op(pid, op, pipeline_priority, pool_avail_cpu, pool_avail_ram):
        # Choose the pool where the op "fits" and leaves the least RAM slack (best-fit),
        # to reduce fragmentation and preserve headroom for large ops.
        best_pool_id = None
        best_cpu = None
        best_ram = None
        best_slack = None

        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            avail_cpu = pool_avail_cpu[pool_id]
            avail_ram = pool_avail_ram[pool_id]
            if avail_cpu <= 0 or avail_ram <= 0:
                continue

            ram_req = _ram_request_gb(pid, op, pool)
            if ram_req > avail_ram:
                continue

            cpu_req = _cpu_request(pid, pipeline_priority, pool, avail_cpu)
            if cpu_req <= 0:
                continue

            slack = avail_ram - ram_req
            # Prefer: minimal slack; tie-breaker: more CPU available (for latency)
            if (best_slack is None) or (slack < best_slack) or (slack == best_slack and avail_cpu > pool_avail_cpu.get(best_pool_id, 0)):
                best_pool_id = pool_id
                best_cpu = cpu_req
                best_ram = ram_req
                best_slack = slack

        return best_pool_id, best_cpu, best_ram

    # --- Update logical time and ingest pipelines/results ---
    s.now += 1

    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s.enqueued_at:
            s.enqueued_at[p.pipeline_id] = s.now
        q = _get_queue(p.priority)
        q.append(p.pipeline_id)

    # Process results: adjust per-op RAM multipliers on probable OOMs; leave pipeline queued for retry.
    for r in results:
        if not r.failed():
            continue

        # Some sims return r.ops list; handle robustly.
        failed_ops = []
        try:
            failed_ops = list(r.ops) if r.ops is not None else []
        except Exception:
            failed_ops = []

        pid = getattr(r, "pipeline_id", None)
        # If result doesn't carry pipeline_id, try to infer via single-op matching across pipelines is unsafe;
        # therefore only apply per-op tuning when we can map back to a pipeline in our dict.
        # Many Eudoxia setups include pipeline_id on Assignment, but ExecutionResult may or may not.
        if pid is None:
            # Best effort: skip per-op updates if pipeline id is unavailable.
            continue

        if pid not in s.pipelines_by_id:
            continue

        oom = _is_probable_oom(getattr(r, "error", None))
        for op in failed_ops:
            k = _op_key(pid, op)
            s.op_attempts[k] = s.op_attempts.get(k, 0) + 1
            if oom:
                # Increase RAM multiplier moderately; repeated OOMs compound but are capped by pool max RAM.
                s.op_ram_mult[k] = min(16.0, s.op_ram_mult.get(k, 1.0) * 1.6)

    # Early exit if nothing changed; keep deterministic behavior.
    if not pipelines and not results:
        return [], []

    # --- Build candidate list: at most one runnable op per pipeline (parents complete), sorted by priority then mem ---
    candidates = []
    all_queues = [s.q_query, s.q_interactive, s.q_batch]

    # We will rebuild queues without completed/terminal pipelines to avoid unbounded growth.
    new_q_query, new_q_interactive, new_q_batch = [], [], []

    for q in all_queues:
        for pid in q:
            p = s.pipelines_by_id.get(pid, None)
            if p is None:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            # If pipeline has failed ops, we still consider them assignable (ASSIGNABLE_STATES includes FAILED),
            # but enforce a retry budget per operator.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Keep pipeline queued for future (parents may not be complete yet).
                if p.priority == Priority.QUERY:
                    new_q_query.append(pid)
                elif p.priority == Priority.INTERACTIVE:
                    new_q_interactive.append(pid)
                else:
                    new_q_batch.append(pid)
                continue

            op = op_list[0]
            k = _op_key(pid, op)
            attempts = s.op_attempts.get(k, 0)

            # If operator has failed too many times, stop retrying this pipeline (avoid infinite penalty loops).
            # This is a last resort; the objective penalizes failures heavily, so keep budgets non-trivial.
            # We treat OOM as more recoverable than other errors.
            # If we don't know the error type (because we didn't see a result for this op), allow scheduling.
            max_attempts = s.max_attempts_oom  # default to OOM budget; only clamp on known non-OOM below

            # If op is currently FAILED and we have no RAM multiplier, assume it's non-OOM after first failure.
            # (Conservative: it might be OOM but we missed the error mapping; still gives a few retries.)
            # We can't reliably inspect op failure reason from here, so use attempt count only.
            if attempts >= max_attempts:
                continue

            # Candidate tuple includes metadata for sorting and queue rebuilding.
            est_mem = _est_mem_gb(op)
            est_mem_sort = est_mem if est_mem is not None else 1e9  # unknown treated as large for packing caution
            enq = s.enqueued_at.get(pid, 0)

            candidates.append((-_priority_rank(p.priority), est_mem_sort, enq, pid, p, op))

            # Keep the pipeline in queue (we do not dequeue until scheduled; this keeps FIFO-ish behavior).
            if p.priority == Priority.QUERY:
                new_q_query.append(pid)
            elif p.priority == Priority.INTERACTIVE:
                new_q_interactive.append(pid)
            else:
                new_q_batch.append(pid)

    s.q_query, s.q_interactive, s.q_batch = new_q_query, new_q_interactive, new_q_batch

    # Sort candidates: highest priority first, then smaller memory, then older enqueue time.
    candidates.sort()

    # --- Greedy assignment across pools with local availability tracking ---
    pool_avail_cpu = {}
    pool_avail_ram = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pool_avail_cpu[pool_id] = float(pool.avail_cpu_pool)
        pool_avail_ram[pool_id] = float(pool.avail_ram_pool)

    assignments = []
    suspensions = []  # This policy avoids preemption by default (no stable API for listing running containers).

    # To reduce head-of-line blocking, allow multiple assignments per tick, but at most one per pipeline.
    scheduled_pipelines = set()

    for _, _, _, pid, p, op in candidates:
        if pid in scheduled_pipelines:
            continue

        # If all pools cannot fit the op's estimated RAM request, skip for now (avoid guaranteed OOM).
        # This may delay but improves completion probability and prevents cascading failures.
        pool_id, cpu_req, ram_req = _best_fit_pool_for_op(pid, op, p.priority, pool_avail_cpu, pool_avail_ram)
        if pool_id is None:
            continue

        # Commit resource reservation.
        pool_avail_cpu[pool_id] -= float(cpu_req)
        pool_avail_ram[pool_id] -= float(ram_req)

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=pid,
            )
        )
        scheduled_pipelines.add(pid)

    return suspensions, assignments
