# policy_key: scheduler_est_001
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 45.77
# generated_at: 2026-04-03T01:05:01.579582
@register_scheduler_init(key="scheduler_est_001")
def scheduler_est_001_init(s):
    """Priority-aware, estimate-guided scheduler with conservative incremental improvements.

    Improvements over naive FIFO:
      1) Per-priority queues (QUERY > INTERACTIVE > BATCH) to improve tail latency.
      2) Estimate-guided RAM sizing (use op.estimate.mem_peak_gb if present).
      3) OOM-aware retry: if a container fails with an OOM-like error, retry the same op with higher RAM.
      4) Avoid over-allocating whole-pool resources to a single op; use per-priority CPU targets.
      5) Basic per-tick "one op per pipeline" limiting to reduce head-of-line blocking.
    """
    from collections import deque

    s.pipelines_by_id = {}  # pipeline_id -> Pipeline
    s.queue_by_prio = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Retry state keyed by (pipeline_id, op_key)
    s.op_retry_count = {}      # (pid, op_key) -> int
    s.op_retry_ram_gb = {}     # (pid, op_key) -> float (next RAM request floor)

    # Pipelines that have terminal non-retryable failures
    s.pipeline_terminal_fail = set()

    # Tunables (start simple; can be refined with sim feedback)
    s.max_oom_retries = 4
    s.ram_growth_factor = 1.6          # multiply RAM request after each OOM-like failure
    s.ram_estimate_safety = 1.10       # small safety over estimate (aggressive, relies on retry)
    s.min_ram_gb_floor = 0.25          # avoid near-zero RAM requests
    s.batch_min_cpu = 1.0
    s.interactive_min_cpu = 1.0
    s.query_min_cpu = 1.0


@register_scheduler(key="scheduler_est_001")
def scheduler_est_001_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler that assigns as many ready operators as possible per pool.

    Key behaviors:
      - Drain results: on OOM-like failures, increase RAM floor and allow retry; otherwise mark pipeline failed.
      - Enqueue new pipelines by priority.
      - For each pool, repeatedly pick the highest-priority pipeline that has a ready op and fits available RAM/CPU.
      - Allocate CPU using small per-priority targets (prevents single-op monopolization).
      - Allocate RAM using op estimate when present, otherwise a small floor; on retries, honor increased RAM floor.
    """
    from collections import deque

    def _prio_order():
        # Highest to lowest
        return (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE)

    def _is_oom_error(err):
        if err is None:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "exceed" in e)

    def _op_key(op):
        # Prefer stable identifiers if they exist
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if isinstance(v, (int, str)):
                        return v
                except Exception:
                    pass
        return id(op)

    def _get_ready_op(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return None
        # If we've already marked this pipeline as terminal failed, skip it.
        if pipeline.pipeline_id in s.pipeline_terminal_fail:
            return None
        # We only schedule operators whose parents are complete.
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _desired_cpu(pool, prio, avail_cpu):
        # Small, obvious improvement: don't allocate the entire pool to one op.
        # Use per-priority targets, bounded by available CPU and at least 1.
        max_cpu = float(pool.max_cpu_pool)
        if prio == Priority.QUERY:
            target = max(s.query_min_cpu, 0.50 * max_cpu)
        elif prio == Priority.INTERACTIVE:
            target = max(s.interactive_min_cpu, 0.40 * max_cpu)
        else:
            target = max(s.batch_min_cpu, 0.25 * max_cpu)

        cpu = min(float(avail_cpu), float(target))
        if cpu < 1.0 and avail_cpu >= 1.0:
            cpu = 1.0
        return cpu

    def _desired_ram(pool, pid, op, avail_ram):
        # Estimate-guided RAM, aggressive: allocate near estimate; rely on retry if under-shot.
        opk = _op_key(op)
        floor = float(s.op_retry_ram_gb.get((pid, opk), s.min_ram_gb_floor))

        est = None
        try:
            if hasattr(op, "estimate") and op.estimate is not None:
                est = getattr(op.estimate, "mem_peak_gb", None)
        except Exception:
            est = None

        if isinstance(est, (int, float)) and est is not None:
            req = max(floor, float(est) * float(s.ram_estimate_safety))
        else:
            req = floor

        # Bound by pool and current availability
        req = min(float(req), float(pool.max_ram_pool), float(avail_ram))
        return req

    # ---- 1) Ingest new pipelines into priority queues ----
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        # Enqueue by priority; duplicates are okay but we try to keep the queue clean by filtering while popping.
        s.queue_by_prio[p.priority].append(p.pipeline_id)

    # ---- 2) Process results: track OOM retries and terminal failures ----
    # Note: we do not attempt preemption here because we lack a guaranteed API
    # for enumerating running containers within a pool.
    if results:
        for r in results:
            if not getattr(r, "failed", lambda: False)():
                continue

            # Try to infer pipeline_id from result; if not present, fall back to r.ops' pipeline association via state.
            pid = getattr(r, "pipeline_id", None)
            # If pipeline_id isn't on result, we can still record per-op retry RAM using only op identity,
            # but the scheduler needs pipeline_id; so we only do robust retries when pid is known.
            # Many simulators attach pipeline_id; if not, we still mark non-oom as terminal-noop.
            oom_like = _is_oom_error(getattr(r, "error", None))

            if pid is None:
                # Best-effort: cannot safely retry without pipeline id linkage; treat as terminal.
                continue

            if pid in s.pipeline_terminal_fail:
                continue

            if not oom_like:
                # Non-OOM failure: mark pipeline terminal failed.
                s.pipeline_terminal_fail.add(pid)
                continue

            # OOM-like failure: increase RAM floor for each failed op and allow retry up to max_oom_retries.
            for op in getattr(r, "ops", []) or []:
                opk = _op_key(op)
                k = (pid, opk)
                c = int(s.op_retry_count.get(k, 0)) + 1
                s.op_retry_count[k] = c

                if c > int(s.max_oom_retries):
                    # Too many retries; mark pipeline terminal failed.
                    s.pipeline_terminal_fail.add(pid)
                    break

                # Increase next RAM request floor based on last assigned RAM (if available), else previous floor.
                last_ram = getattr(r, "ram", None)
                prev_floor = float(s.op_retry_ram_gb.get(k, s.min_ram_gb_floor))
                if isinstance(last_ram, (int, float)) and last_ram is not None and float(last_ram) > 0:
                    new_floor = max(prev_floor, float(last_ram) * float(s.ram_growth_factor))
                else:
                    new_floor = prev_floor * float(s.ram_growth_factor)
                s.op_retry_ram_gb[k] = new_floor

    # Early exit if nothing new to decide; keep behavior aligned with example.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ---- 3) Schedule: for each pool, fill with highest-priority ready work that fits ----
    scheduled_this_tick = set()  # pipeline_ids we already scheduled an op for

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        made_progress = True
        # Greedy fill loop: keep placing operators while resources remain and we can find fitting work.
        while made_progress and avail_cpu > 0 and avail_ram > 0:
            made_progress = False

            chosen = None  # (pipeline, op)
            chosen_prio = None

            # Find best next runnable op across priority queues (round-robin within priority)
            for prio in _prio_order():
                q = s.queue_by_prio.get(prio)
                if not q:
                    continue

                # Scan up to the current queue length to find a schedulable pipeline.
                # We rotate items to preserve fairness within the same priority.
                n = len(q)
                for _ in range(n):
                    pid = q.popleft()
                    p = s.pipelines_by_id.get(pid)
                    if p is None:
                        continue
                    if pid in s.pipeline_terminal_fail:
                        continue

                    status = p.runtime_status()
                    if status.is_pipeline_successful():
                        continue
                    if pid in scheduled_this_tick:
                        # Only one op per pipeline per tick (simple anti-HOL + fairness).
                        q.append(pid)
                        continue

                    op = _get_ready_op(p)
                    # Keep it in the queue for future consideration regardless.
                    q.append(pid)

                    if op is None:
                        continue

                    # Check if it can fit (RAM first, then CPU).
                    ram_req = _desired_ram(pool, pid, op, avail_ram)
                    cpu_req = _desired_cpu(pool, prio, avail_cpu)

                    if ram_req <= 0 or cpu_req <= 0:
                        continue
                    if ram_req > avail_ram or cpu_req > avail_cpu:
                        continue

                    chosen = (p, op, cpu_req, ram_req)
                    chosen_prio = prio
                    break

                if chosen is not None:
                    break

            if chosen is None:
                break

            p, op, cpu_req, ram_req = chosen

            # Create assignment (one operator per assignment for simplicity and clearer retry accounting).
            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Update local available resources for greedy packing
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)
            scheduled_this_tick.add(p.pipeline_id)
            made_progress = True

    return suspensions, assignments
