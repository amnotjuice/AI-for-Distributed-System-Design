# policy_key: scheduler_medium_021
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.082919
# generation_seconds: 100.62
# generated_at: 2026-04-09T22:57:35.271605
@register_scheduler_init(key="scheduler_medium_021")
def scheduler_medium_021_init(s):
    """
    Priority + SRPT-ish + conservative RAM retry scheduler.

    Goals for weighted latency objective:
      - Keep QUERY/INTERACTIVE latency low by always scheduling them first.
      - Reduce catastrophic 720s penalties by retrying failed ops with increased RAM.
      - Avoid starving BATCH via simple aging that gradually relaxes reservations.
      - Pack multiple assignments per pool (slice CPU/RAM) to reduce queueing delay,
        while giving higher priority ops larger CPU shares.
    """
    # Logical clock (ticks). Used for aging and cooldown after failures.
    s.time = 0

    # Track active pipelines by id (pipeline objects mutate; keep latest reference).
    s.pipelines_by_id = {}

    # Per-priority FIFO queues of pipeline_ids (with de-dup via in_queue sets).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.in_queue = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Pipeline arrival tick (for aging / fairness).
    s.pipeline_arrival_tick = {}

    # Per-(pipeline,op) learned RAM estimate and retry/cooldown state.
    s.op_ram_est = {}          # op_key -> ram to request next time
    s.op_retry = {}            # op_key -> retry count
    s.op_cooldown_until = {}   # op_key -> tick when it can be scheduled again
    s.op_max_ram_seen = {}     # op_key -> max ram ever requested/used

    # Pipelines we stop scheduling (e.g., repeated failures at near-max RAM).
    s.blacklisted_pipelines = set()

    # Reservations: keep some headroom so high-priority arrivals don't queue behind batch.
    s.reserve_cpu_frac = 0.20
    s.reserve_ram_frac = 0.20

    # Batch aging: after enough wait time, allow batch to use reserved resources.
    s.batch_aging_threshold = 60  # ticks

    # Failure handling knobs
    s.base_failure_cooldown = 3   # ticks to wait after a failure before retrying


@register_scheduler(key="scheduler_medium_021")
def scheduler_medium_021(s, results, pipelines):
    """
    Scheduler step.

    Strategy:
      1) Enqueue new pipelines by priority (no drops).
      2) On failures, increase RAM estimate for the failed op(s) and retry later (cooldown).
      3) For each pool, repeatedly place ready operators:
         - Always prefer QUERY, then INTERACTIVE, then BATCH (with aging).
         - Within a class, prefer smaller remaining-work pipelines (SRPT-ish) to reduce mean latency.
         - Allocate CPU slices by priority; allocate conservative RAM to reduce OOM risk.
    """
    s.time += 1

    suspensions = []
    assignments = []

    def _op_id(op):
        # Best-effort stable id for operator object across ticks.
        for attr in ("op_id", "operator_id", "node_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return str(v)
                except Exception:
                    pass
        return repr(op)

    def _op_key(pipeline_id, op):
        return (str(pipeline_id), _op_id(op))

    def _is_oom_error(err):
        if not err:
            return False
        try:
            e = str(err).lower()
        except Exception:
            return False
        return ("oom" in e) or ("out of memory" in e) or ("out-of-memory" in e) or ("memory" in e and "alloc" in e)

    def _remaining_ops_count(status):
        # Approx remaining work: count all non-completed states if present.
        total = 0
        try:
            sc = status.state_counts
            for st in (OperatorState.PENDING, OperatorState.FAILED, OperatorState.ASSIGNED,
                       OperatorState.RUNNING, OperatorState.SUSPENDING):
                try:
                    total += int(sc.get(st, 0))
                except Exception:
                    pass
        except Exception:
            # Fallback: if state_counts isn't well-formed, just avoid SRPT and return a constant.
            total = 999999
        return total

    def _ensure_enqueued(p):
        pid = p.pipeline_id
        pr = p.priority
        s.pipelines_by_id[pid] = p
        if pid not in s.pipeline_arrival_tick:
            s.pipeline_arrival_tick[pid] = s.time
        if pid not in s.in_queue[pr]:
            s.queues[pr].append(pid)
            s.in_queue[pr].add(pid)

    def _prune_queues():
        # Remove completed and blacklisted pipelines; keep queue order for the rest.
        for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            new_q = []
            new_set = set()
            for pid in s.queues[pr]:
                if pid in s.blacklisted_pipelines:
                    continue
                p = s.pipelines_by_id.get(pid)
                if p is None:
                    continue
                try:
                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        continue
                except Exception:
                    # If we can't read status, keep it (better than accidentally dropping).
                    pass
                new_q.append(pid)
                new_set.add(pid)
            s.queues[pr] = new_q
            s.in_queue[pr] = new_set

    def _default_cpu(pr, pool, avail_cpu):
        # Give more CPU to higher priority to reduce their latency.
        max_cpu = float(pool.max_cpu_pool)
        if pr == Priority.QUERY:
            target = max(1.0, 0.75 * max_cpu)
        elif pr == Priority.INTERACTIVE:
            target = max(1.0, 0.50 * max_cpu)
        else:
            target = max(1.0, 0.25 * max_cpu)

        # If pool is tight, take what we can, but avoid tiny slices unless that's all left.
        cpu = min(float(avail_cpu), float(target))
        if cpu <= 0:
            return 0.0
        if cpu < 1.0 and avail_cpu >= 1.0:
            cpu = 1.0
        return cpu

    def _default_ram(pr, pool):
        # Conservative RAM sizing reduces OOMs (720s penalties are expensive).
        max_ram = float(pool.max_ram_pool)
        if pr == Priority.QUERY:
            frac = 0.60
        elif pr == Priority.INTERACTIVE:
            frac = 0.45
        else:
            frac = 0.30
        return max(0.10 * max_ram, frac * max_ram)

    def _retry_caps(pr):
        if pr == Priority.QUERY:
            return 8
        if pr == Priority.INTERACTIVE:
            return 6
        return 4

    def _choose_candidate(pool, avail_cpu, avail_ram, scheduled_pipelines):
        # Select the best (pipeline, op) to schedule next in this pool.
        # Always prefer QUERY, then INTERACTIVE, then BATCH (with aging allowing batch to use reserved).
        reserve_cpu = s.reserve_cpu_frac * float(pool.max_cpu_pool)
        reserve_ram = s.reserve_ram_frac * float(pool.max_ram_pool)

        def _batch_can_use_resources(pid):
            waited = s.time - int(s.pipeline_arrival_tick.get(pid, s.time))
            return waited >= int(s.batch_aging_threshold)

        def _eligible_for_batch_resources(pid):
            # If not aged, batch can only use non-reserved resources.
            if _batch_can_use_resources(pid):
                return avail_cpu, avail_ram
            return (avail_cpu - reserve_cpu), (avail_ram - reserve_ram)

        best = None  # (score, pr, pid, op, cpu, ram)

        for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            # Quick resource gate per class (especially for batch under reservation).
            if pr == Priority.BATCH_PIPELINE:
                # If no non-reserved slack and not aged, skip searching batch.
                eff_cpu, eff_ram = _eligible_for_batch_resources("__dummy__")
                if eff_cpu <= 0 or eff_ram <= 0:
                    continue

            for pid in s.queues[pr]:
                if pid in scheduled_pipelines:
                    continue
                if pid in s.blacklisted_pipelines:
                    continue
                p = s.pipelines_by_id.get(pid)
                if p is None:
                    continue

                # Pipeline status checks
                try:
                    st = p.runtime_status()
                except Exception:
                    continue
                try:
                    if st.is_pipeline_successful():
                        continue
                except Exception:
                    pass

                # Find a ready op whose parents are done
                try:
                    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                except Exception:
                    ops = []
                if not ops:
                    continue
                op = ops[0]
                ok = _op_key(pid, op)

                # Per-op cooldown after failure to avoid thrashing.
                if int(s.op_cooldown_until.get(ok, 0)) > s.time:
                    continue

                # Compute requested RAM (learned estimate or conservative default).
                req_ram = float(s.op_ram_est.get(ok, _default_ram(pr, pool)))

                # If we've already seen this op fail many times, escalate RAM aggressively.
                rcount = int(s.op_retry.get(ok, 0))
                if rcount >= 2:
                    req_ram = max(req_ram, 0.70 * float(pool.max_ram_pool))
                if rcount >= 4:
                    req_ram = max(req_ram, 0.90 * float(pool.max_ram_pool))

                # Cap at pool max.
                req_ram = min(req_ram, float(pool.max_ram_pool))

                # Resource eligibility check (batch may be constrained by reservation unless aged).
                if pr == Priority.BATCH_PIPELINE and not _batch_can_use_resources(pid):
                    eff_cpu, eff_ram = _eligible_for_batch_resources(pid)
                    if eff_cpu <= 0 or eff_ram <= 0:
                        continue
                    # If the op wouldn't fit in the non-reserved slack, skip it.
                    if req_ram > eff_ram:
                        continue

                # Must fit in actual available RAM.
                if req_ram > float(avail_ram):
                    continue

                # CPU sizing
                req_cpu = _default_cpu(pr, pool, avail_cpu)
                if req_cpu <= 0:
                    continue

                # SRPT-ish score: fewer remaining ops is better; break ties by longer waiting.
                remaining = _remaining_ops_count(st)
                waited = s.time - int(s.pipeline_arrival_tick.get(pid, s.time))

                # Heavier preference for high priority: slightly penalize remaining more for batch.
                if pr == Priority.BATCH_PIPELINE:
                    score = remaining * 1000 - waited * 10
                else:
                    score = remaining * 1000 - waited

                cand = (score, pr, pid, op, req_cpu, req_ram)
                if best is None or cand[0] < best[0]:
                    best = cand

            # If we found any candidate in a higher class, do not consider lower classes.
            if best is not None and best[1] == pr:
                break

        return best

    # 1) Enqueue new pipelines
    for p in pipelines:
        _ensure_enqueued(p)

    # 2) Process results: on failure, increase RAM estimate and apply cooldown; blacklist only if hopeless.
    for r in results:
        if r is None:
            continue

        # Update reference to pipeline object if present in current map (it will mutate externally).
        # No action needed unless we want to relocate across queues; priority is fixed.
        try:
            failed = r.failed()
        except Exception:
            failed = False

        if not failed:
            # On success, keep RAM estimate as-is (don't reduce aggressively; avoids future OOMs).
            continue

        try:
            pr = r.priority
        except Exception:
            pr = Priority.BATCH_PIPELINE

        # Conservative RAM bump: double if OOM-like, else +50%.
        oom = _is_oom_error(getattr(r, "error", None))
        bump_mult = 2.0 if oom else 1.5

        # Apply updates for each op in the failed container result.
        for op in getattr(r, "ops", []) or []:
            # We need the pipeline_id to key the op; it's available as r.ops + r.pipeline? not provided.
            # Fall back: if r has pipeline_id use it; else, we can't reliably track per pipeline.
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                # If pipeline_id isn't present on results, approximate by using op's embedded pipeline id if any.
                pid = getattr(op, "pipeline_id", None)
            if pid is None:
                # Last resort: treat as global-op key to still increase RAM if op repeats.
                pid = "unknown_pipeline"

            ok = _op_key(pid, op)

            prev = float(s.op_ram_est.get(ok, float(getattr(r, "ram", 0.0)) or 0.0))
            used = float(getattr(r, "ram", 0.0) or prev or 0.0)
            s.op_max_ram_seen[ok] = max(float(s.op_max_ram_seen.get(ok, 0.0)), used, prev)

            # Increase estimate, at least +1.0 to make progress if values are small.
            new_est = max(prev, used, 1.0) * bump_mult
            # Try to cap using the pool max if we can read it; otherwise leave as is.
            try:
                pool_id = int(getattr(r, "pool_id", 0))
                pool = s.executor.pools[pool_id]
                new_est = min(new_est, float(pool.max_ram_pool))
            except Exception:
                pass

            s.op_ram_est[ok] = new_est
            s.op_retry[ok] = int(s.op_retry.get(ok, 0)) + 1
            s.op_cooldown_until[ok] = s.time + int(s.base_failure_cooldown) + min(10, int(s.op_retry[ok]))

            # If we're failing repeatedly at near-max RAM, stop wasting resources on this pipeline.
            cap = _retry_caps(pr)
            try:
                pool_id = int(getattr(r, "pool_id", 0))
                pool = s.executor.pools[pool_id]
                near_max = float(used) >= 0.95 * float(pool.max_ram_pool)
            except Exception:
                near_max = False

            if int(s.op_retry[ok]) >= cap and near_max:
                # Best-effort: blacklist by pid if it's a real pipeline id.
                if pid != "unknown_pipeline":
                    s.blacklisted_pipelines.add(pid)

    # 3) Prune queues of completed/blacklisted pipelines
    _prune_queues()

    # 4) Build assignments (no preemption in this policy to avoid churn/waste)
    scheduled_pipelines = set()

    # Iterate pools in descending headroom so latency-sensitive work finds a large pool first.
    pool_order = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        headroom = float(pool.avail_cpu_pool) + float(pool.avail_ram_pool)
        pool_order.append((headroom, pool_id))
    pool_order.sort(reverse=True)

    for _, pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Greedily pack multiple ops into the pool this tick.
        # Stop when we can no longer allocate meaningful resources.
        while avail_cpu > 0.0 and avail_ram > 0.0:
            cand = _choose_candidate(pool, avail_cpu, avail_ram, scheduled_pipelines)
            if cand is None:
                break

            _, pr, pid, op, req_cpu, req_ram = cand

            # Final safety clamps
            cpu = min(req_cpu, avail_cpu)
            ram = min(req_ram, avail_ram)

            # Ensure strictly positive allocations; avoid micro-slices.
            if cpu <= 0.0 or ram <= 0.0:
                break
            if cpu < 1.0 and avail_cpu >= 1.0:
                cpu = 1.0
            if ram < 1.0 and avail_ram >= 1.0:
                ram = 1.0

            # If still doesn't fit, stop scheduling in this pool.
            if cpu > avail_cpu or ram > avail_ram:
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pr,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Bookkeeping
            avail_cpu -= cpu
            avail_ram -= ram
            scheduled_pipelines.add(pid)

            # If we are extremely tight, avoid creating additional tiny containers.
            if avail_cpu < 0.5 or avail_ram < 0.5:
                break

    return suspensions, assignments
