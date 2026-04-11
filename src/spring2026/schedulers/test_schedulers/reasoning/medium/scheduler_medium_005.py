# policy_key: scheduler_medium_005
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.120929
# generation_seconds: 81.56
# generated_at: 2026-04-09T22:34:39.061101
@register_scheduler_init(key="scheduler_medium_005")
def scheduler_medium_005_init(s):
    """
    Priority-aware, reservation-based round-robin scheduler with simple OOM-adaptive RAM sizing.

    Main ideas:
      - Separate queues per priority; always try QUERY, then INTERACTIVE, then BATCH.
      - Reserve a fraction of each pool's CPU/RAM for high priority to avoid batch crowd-out.
      - Cap per-operator CPU to improve concurrency (avoid "give everything to one op").
      - Learn from failures: if an op fails with an OOM-like error, retry it with more RAM.
      - Avoid infinite retries for non-OOM failures (stop scheduling those pipelines after a cap).
      - Ensure batch makes progress via aging: reservations gradually relax for old batch pipelines.
    """
    from collections import deque

    s.tick = 0

    # Per-priority FIFO (round-robin by popping and re-appending).
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Track first-seen tick for aging/fairness.
    s.first_seen_tick = {}  # pipeline_id -> tick

    # Track pipelines we should stop scheduling (e.g., repeated non-OOM failures).
    s.dead_pipelines = set()  # pipeline_id

    # Per-(pipeline,op) learned multipliers for RAM and retry counts.
    s.op_ram_mult = {}   # (pipeline_id, op_key) -> float
    s.op_retries = {}    # (pipeline_id, op_key) -> int
    s.pipe_failures = {} # pipeline_id -> int (non-OOM-ish)

    # Parameters (conservative for high priority to reduce OOMs; more parallelism for batch).
    s.reserve_frac_cpu = 0.25
    s.reserve_frac_ram = 0.25

    s.cpu_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.35,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.ram_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.ram_floor_frac = {
        Priority.QUERY: 0.10,
        Priority.INTERACTIVE: 0.08,
        Priority.BATCH_PIPELINE: 0.05,
    }

    # Batch aging: after this many ticks, begin relaxing reservations; after full_relax, no reserve.
    s.batch_age_relax_start = 40
    s.batch_age_full_relax = 200

    # Retry policy: allow more OOM retries (resource tuning), fewer non-OOM retries.
    s.max_oom_retries = {
        Priority.QUERY: 6,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 4,
    }
    s.max_nonoom_pipeline_failures = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }


@register_scheduler(key="scheduler_medium_005")
def scheduler_medium_005(s, results, pipelines):
    """
    See init() docstring for full policy overview.
    """
    from collections import deque

    def _op_key(op):
        # Prefer stable identifiers if available; fallback to string form.
        for attr in ("op_id", "operator_id", "task_id", "node_id", "id"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    return v() if callable(v) else v
                except Exception:
                    pass
        try:
            return str(op)
        except Exception:
            return repr(op)

    def _get_result_pipeline_id(r):
        # ExecutionResult may or may not have pipeline_id in a given simulator version.
        pid = getattr(r, "pipeline_id", None)
        if pid is not None:
            return pid
        ops = getattr(r, "ops", None) or []
        if ops:
            pid = getattr(ops[0], "pipeline_id", None)
            if pid is not None:
                return pid
        return None

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            s_err = str(err).lower()
        except Exception:
            return False
        return ("oom" in s_err) or ("out of memory" in s_err) or ("memory" in s_err and "exceed" in s_err)

    def _batch_relax_factor(pipeline_id):
        # Returns in [0,1]; 0 => no relax (full reserve), 1 => fully relaxed (no reserve).
        first = s.first_seen_tick.get(pipeline_id, s.tick)
        age = max(0, s.tick - first)
        if age <= s.batch_age_relax_start:
            return 0.0
        if age >= s.batch_age_full_relax:
            return 1.0
        return float(age - s.batch_age_relax_start) / float(s.batch_age_full_relax - s.batch_age_relax_start)

    def _compute_allocation(priority, pool, pipeline_id, op):
        # CPU cap promotes concurrency; RAM tuned for fewer OOMs (especially for high priority).
        cpu_target = max(1.0, pool.max_cpu_pool * s.cpu_frac.get(priority, 0.25))
        cpu = min(pool.avail_cpu_pool, cpu_target)
        if cpu < 1.0:
            return None, None

        base_ram = pool.max_ram_pool * s.ram_frac.get(priority, 0.25)
        floor_ram = pool.max_ram_pool * s.ram_floor_frac.get(priority, 0.05)

        ok = _op_key(op)
        mult = s.op_ram_mult.get((pipeline_id, ok), 1.0)

        ram_target = max(floor_ram, base_ram * mult)
        ram = min(pool.avail_ram_pool, ram_target)
        if ram <= 0:
            return None, None

        return cpu, ram

    def _batch_allowed(priority, pool, pipeline_id, cpu, ram):
        # Enforce reservation for batch unless it has aged; queries/interactive bypass reservation.
        if priority != Priority.BATCH_PIPELINE:
            return True

        relax = _batch_relax_factor(pipeline_id)  # 0..1
        reserve_cpu = pool.max_cpu_pool * s.reserve_frac_cpu * (1.0 - relax)
        reserve_ram = pool.max_ram_pool * s.reserve_frac_ram * (1.0 - relax)

        # Require keeping some headroom for high priority arrivals.
        if (pool.avail_cpu_pool - cpu) < reserve_cpu:
            return False
        if (pool.avail_ram_pool - ram) < reserve_ram:
            return False
        return True

    # Enqueue new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.first_seen_tick:
            s.first_seen_tick[pid] = s.tick
        # Route to its priority queue
        q = s.queues.get(p.priority, None)
        if q is None:
            # Unknown priority; treat as batch-like
            q = s.queues[Priority.BATCH_PIPELINE]
        q.append(p)

    # Update learning from execution results (OOM => bump RAM multiplier; success => slight decay)
    for r in (results or []):
        pid = _get_result_pipeline_id(r)
        prio = getattr(r, "priority", None)

        # If we cannot attribute to a pipeline, we cannot learn per-pipeline safely.
        if pid is None:
            continue

        if r.failed():
            oomish = _is_oom_error(getattr(r, "error", None))
            ops = getattr(r, "ops", None) or []
            if not ops:
                # Conservative: count as a pipeline failure (non-OOM).
                if not oomish:
                    s.pipe_failures[pid] = s.pipe_failures.get(pid, 0) + 1
                continue

            for op in ops:
                ok = _op_key(op)
                key = (pid, ok)
                s.op_retries[key] = s.op_retries.get(key, 0) + 1

                if oomish:
                    # Increase RAM multiplier aggressively to converge quickly.
                    cur = s.op_ram_mult.get(key, 1.0)
                    # Faster early growth, then slower.
                    bump = 1.8 if cur < 2.5 else 1.4
                    s.op_ram_mult[key] = min(cur * bump, 16.0)

                    # If too many OOM retries, stop scheduling this pipeline to avoid thrash.
                    maxr = s.max_oom_retries.get(prio, 4)
                    if s.op_retries[key] > maxr:
                        s.dead_pipelines.add(pid)
                else:
                    # Non-OOM failure: do not keep retrying forever (protect query latency).
                    s.pipe_failures[pid] = s.pipe_failures.get(pid, 0) + 1
                    max_pf = s.max_nonoom_pipeline_failures.get(prio, 1)
                    if s.pipe_failures[pid] > max_pf:
                        s.dead_pipelines.add(pid)
        else:
            # Successful completion: gently decay RAM multiplier (more parallelism over time).
            pid = _get_result_pipeline_id(r)
            if pid is None:
                continue
            ops = getattr(r, "ops", None) or []
            for op in ops:
                ok = _op_key(op)
                key = (pid, ok)
                if key in s.op_ram_mult:
                    s.op_ram_mult[key] = max(1.0, s.op_ram_mult[key] * 0.97)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    s.tick += 1

    suspensions = []
    assignments = []

    scheduled_this_tick = set()  # pipeline_id

    # Helper: try to schedule one op from a given priority queue onto a given pool.
    def _try_schedule_from_queue(pool_id, priority):
        pool = s.executor.pools[pool_id]
        q = s.queues[priority]
        if not q:
            return False

        # Scan up to current queue length to find an eligible pipeline.
        n = len(q)
        for _ in range(n):
            p = q.popleft()
            pid = p.pipeline_id

            # Skip dead pipelines.
            if pid in s.dead_pipelines:
                continue

            # Skip if already scheduled this tick (avoid multi-op dominance).
            if pid in scheduled_this_tick:
                q.append(p)
                continue

            status = p.runtime_status()

            # Drop successful pipelines from scheduling queues.
            if status.is_pipeline_successful():
                continue

            # If pipeline has failed ops, we still retry (FAILED is assignable).
            # But if it has any failures and is dead, it was already skipped above.

            # Find ready ops (respect parents).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; keep it in queue for later.
                q.append(p)
                continue

            op = op_list[0]
            cpu, ram = _compute_allocation(priority, pool, pid, op)
            if cpu is None or ram is None:
                # Not enough resources on this pool right now; requeue and stop trying this pool/priority.
                q.appendleft(p)
                return False

            if not _batch_allowed(priority, pool, pid, cpu, ram):
                # Batch blocked by reservation; requeue.
                q.append(p)
                continue

            # Create assignment and mark scheduled; pipeline goes to back for round-robin.
            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=pid,
            )
            assignments.append(assignment)
            scheduled_this_tick.add(pid)

            # Requeue pipeline for future ops.
            q.append(p)
            return True

        return False

    # Pool-filling loop: for each pool, keep scheduling until no progress.
    # Always prioritize QUERY then INTERACTIVE then BATCH.
    priorities = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # If no resources at all, skip.
        if pool.avail_cpu_pool < 1.0 or pool.avail_ram_pool <= 0:
            continue

        # Keep attempting until we can't schedule anything more on this pool.
        made_progress = True
        while made_progress:
            made_progress = False

            # Try in strict priority order for latency protection.
            for pr in priorities:
                # If pool is effectively saturated, stop.
                if pool.avail_cpu_pool < 1.0 or pool.avail_ram_pool <= 0:
                    break
                if _try_schedule_from_queue(pool_id, pr):
                    made_progress = True
                    break  # restart at highest priority

    return suspensions, assignments
