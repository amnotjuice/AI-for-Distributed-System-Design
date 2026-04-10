# policy_key: scheduler_est_015
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.083054
# generation_seconds: 121.28
# generated_at: 2026-03-31T18:48:40.879827
@register_scheduler_init(key="scheduler_est_015")
def scheduler_est_015_init(s):
    """Priority-aware, estimate-based scheduler (small, safe improvements over naive FIFO).

    Key ideas (kept intentionally simple/robust):
      1) Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
      2) Right-size RAM from op.estimate.mem_peak_gb with a safety factor, instead of
         grabbing the whole pool (reduces interference and improves latency).
      3) Cap CPU per container by priority (avoid one op monopolizing the pool).
      4) Keep a small headroom reserve when high-priority work is waiting, so batch
         can't fully block interactive/query admission.
      5) If an op fails with OOM, increase its RAM multiplier and retry (bounded).
         Non-OOM failures are not retried (drop pipeline on first such failure).
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-op adaptation (keyed by a stable-ish uid derived from op attributes).
    s.op_mem_mult = {}        # uid -> multiplier applied to estimate.mem_peak_gb
    s.op_fail_counts = {}     # uid -> number of observed failures
    s.op_last_oom = {}        # uid -> last failure was OOM?

    # Tuning knobs (conservative defaults).
    s.mem_safety = 1.20
    s.mem_mult_init = 1.00
    s.mem_mult_oom_increase = 1.50
    s.mem_mult_cap = 6.00
    s.min_ram_gb = 0.25

    # Retry policy: only OOM is retried, capped.
    s.max_oom_retries_per_op = 3

    # Reserve a small slice for high-priority work when it is queued.
    s.reserve_cpu_frac = 0.10
    s.reserve_ram_frac = 0.10

    # CPU caps by priority (fraction of pool max), to reduce tail latency by avoiding contention.
    s.cpu_cap_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.step = 0


@register_scheduler(key="scheduler_est_015")
def scheduler_est_015(s, results, pipelines):
    """
    See init docstring for policy overview.
    """
    s.step += 1

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg)

    def _op_uid(op):
        # Try a few common stable identifiers; fall back to object id.
        for attr in ("op_id", "operator_id", "task_id", "name"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if v is not None:
                    return (attr, v)
        return ("py_id", id(op))

    def _op_est_mem_gb(op):
        # Spec says: op.estimate.mem_peak_gb
        try:
            est = op.estimate.mem_peak_gb
            if est is None:
                return None
            est = float(est)
            if est <= 0:
                return None
            return est
        except Exception:
            return None

    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _hp_backlog_size():
        return len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE])

    # Enqueue new pipelines by priority.
    for p in pipelines:
        # Default unknown priorities to BATCH_PIPELINE.
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s.queues:
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    # Early exit if nothing changed that would affect decisions.
    if not pipelines and not results:
        return [], []

    # Learn from results: detect OOM and adjust memory multiplier; track failure counts.
    for r in results:
        # Update per-op stats. (ExecutionResult doesn't reliably carry pipeline_id in the prompt.)
        for op in getattr(r, "ops", []) or []:
            uid = _op_uid(op)

            if hasattr(r, "failed") and r.failed():
                prev = s.op_fail_counts.get(uid, 0)
                s.op_fail_counts[uid] = prev + 1

                oom = _is_oom_error(getattr(r, "error", None))
                s.op_last_oom[uid] = oom

                if oom:
                    # Increase RAM multiplier aggressively to stop repeated OOMs.
                    cur = s.op_mem_mult.get(uid, s.mem_mult_init)
                    new = min(s.mem_mult_cap, cur * s.mem_mult_oom_increase)
                    s.op_mem_mult[uid] = new
            else:
                # On success, clear failure state for this op uid.
                s.op_fail_counts.pop(uid, None)
                s.op_last_oom.pop(uid, None)

    # Snapshot pool resources to do global placement across pools.
    pool_avail = {}
    pool_max = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pool_avail[pool_id] = [float(pool.avail_cpu_pool), float(pool.avail_ram_pool)]
        pool_max[pool_id] = [float(pool.max_cpu_pool), float(pool.max_ram_pool)]

    def _reserve_for_hp(pool_id):
        """Reserve some headroom if there is any high-priority backlog."""
        if _hp_backlog_size() <= 0:
            return (0.0, 0.0)
        max_cpu, max_ram = pool_max[pool_id]
        return (max_cpu * s.reserve_cpu_frac, max_ram * s.reserve_ram_frac)

    def _cpu_cap_for(pool_id, priority):
        max_cpu = pool_max[pool_id][0]
        frac = s.cpu_cap_frac.get(priority, 0.25)
        cap = max_cpu * frac
        # Also avoid returning fractional CPUs if the simulator expects integral vCPUs.
        # Keep at least 1 if any CPU exists.
        return max(1.0, float(int(cap)) if cap >= 1.0 else cap)

    def _desired_ram_for_op(op):
        est = _op_est_mem_gb(op)
        uid = _op_uid(op)
        mult = s.op_mem_mult.get(uid, s.mem_mult_init)
        if est is None:
            # Conservative default if we don't have an estimate.
            return max(s.min_ram_gb, 1.0)
        return max(s.min_ram_gb, est * s.mem_safety * mult)

    def _op_retry_allowed(op):
        uid = _op_uid(op)
        fails = s.op_fail_counts.get(uid, 0)
        last_oom = s.op_last_oom.get(uid, False)
        if fails <= 0:
            return True
        if not last_oom:
            # Don't retry non-OOM failures.
            return False
        # Allow a bounded number of OOM retries.
        return fails <= s.max_oom_retries_per_op

    def _pipeline_should_drop(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return True

        # If there are FAILED ops, only keep the pipeline if all failed ops are retryable.
        if status.state_counts.get(OperatorState.FAILED, 0) > 0:
            failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
            for op in failed_ops:
                if not _op_retry_allowed(op):
                    return True
        return False

    def _get_next_ready_op(pipeline):
        status = pipeline.runtime_status()
        # Only schedule ops whose parents are complete.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
        if not op_list:
            return None
        # Prefer PENDING over FAILED when both are assignable, to reduce churn.
        pending = [op for op in op_list if getattr(op, "state", None) == OperatorState.PENDING]
        if pending:
            return pending[0]
        # Otherwise FAILED (retryable check happens elsewhere).
        return op_list[0]

    def _choose_pool_for(priority, op, ram_need):
        """Pick a pool that can fit, respecting headroom reserve for batch when HP backlog exists."""
        best_pool = None
        best_score = None

        for pool_id in range(s.executor.num_pools):
            avail_cpu, avail_ram = pool_avail[pool_id]
            if avail_cpu <= 0 or avail_ram <= 0:
                continue

            # Clamp RAM need to pool max (if op "needs more than exists", best we can do is max).
            max_ram = pool_max[pool_id][1]
            ram_alloc = min(ram_need, max_ram)
            if ram_alloc > avail_ram + 1e-9:
                continue

            # Allocate CPU up to cap (and up to available).
            cpu_cap = _cpu_cap_for(pool_id, priority)
            cpu_alloc = min(avail_cpu, cpu_cap)
            if cpu_alloc <= 0:
                continue

            # For batch, ensure we don't consume reserved HP headroom if HP backlog exists.
            if priority == Priority.BATCH_PIPELINE:
                res_cpu, res_ram = _reserve_for_hp(pool_id)
                if (avail_cpu - cpu_alloc) < res_cpu - 1e-9:
                    continue
                if (avail_ram - ram_alloc) < res_ram - 1e-9:
                    continue

            # Score: best-fit packing (minimize leftover), with a slight bias to give HP more CPU.
            max_cpu, _ = pool_max[pool_id]
            left_cpu = (avail_cpu - cpu_alloc) / max(1.0, max_cpu)
            left_ram = (avail_ram - ram_alloc) / max(1.0, max_ram)

            # Lower is better. HP prefers more CPU (implicitly via larger cpu_alloc), but keep simple:
            score = left_ram * 2.0 + left_cpu

            if best_score is None or score < best_score:
                best_score = score
                best_pool = (pool_id, cpu_alloc, ram_alloc)

        return best_pool  # (pool_id, cpu_alloc, ram_alloc) or None

    # Main loop: repeatedly pick the next runnable pipeline/op by priority and place it globally.
    assignments = []
    suspensions = []  # This policy does not preempt (API for running containers is not guaranteed).

    # To avoid O(n^2) infinite spinning, bound the number of selection attempts per call.
    max_attempts = 0
    for pr in s.queues:
        max_attempts += len(s.queues[pr])
    max_attempts = max(1, max_attempts) * 3

    attempts = 0
    while attempts < max_attempts:
        attempts += 1

        made_assignment = False

        for pr in _prio_order():
            q = s.queues[pr]
            if not q:
                continue

            # Rotate through this queue at most once per outer attempt.
            n = len(q)
            for _ in range(n):
                pipeline = q.pop(0)

                # Drop completed pipelines or those with non-retryable failures.
                if _pipeline_should_drop(pipeline):
                    continue

                op = _get_next_ready_op(pipeline)
                if op is None:
                    # Nothing ready yet; keep it in its priority queue.
                    q.append(pipeline)
                    continue

                # If op is a FAILED retry, ensure it's allowed.
                status = pipeline.runtime_status()
                # If it is FAILED (or if pipeline has failed ops), check retryability for this op.
                if status.state_counts.get(OperatorState.FAILED, 0) > 0 and not _op_retry_allowed(op):
                    # Drop the pipeline.
                    continue

                ram_need = _desired_ram_for_op(op)
                choice = _choose_pool_for(pr, op, ram_need)
                if choice is None:
                    # Can't fit anywhere right now. Requeue and try other pipelines (or priorities).
                    q.append(pipeline)
                    continue

                pool_id, cpu_alloc, ram_alloc = choice

                # Create assignment (one op per container keeps behavior close to the naive baseline).
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_alloc,
                        ram=ram_alloc,
                        priority=pr,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )

                # Update local pool availability snapshot.
                pool_avail[pool_id][0] -= cpu_alloc
                pool_avail[pool_id][1] -= ram_alloc

                # Requeue pipeline so subsequent ops can be scheduled in future iterations/ticks.
                q.append(pipeline)

                made_assignment = True
                break

            if made_assignment:
                break

        if not made_assignment:
            break

    return suspensions, assignments
