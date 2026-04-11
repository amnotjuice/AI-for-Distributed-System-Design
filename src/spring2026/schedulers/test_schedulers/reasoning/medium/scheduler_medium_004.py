# policy_key: scheduler_medium_004
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.082093
# generation_seconds: 116.94
# generated_at: 2026-04-09T22:33:17.501921
@register_scheduler_init(key="scheduler_medium_004")
def scheduler_medium_004_init(s):
    """
    Weighted-latency-aware, failure-averse scheduler.

    Main ideas:
      - Weighted deficit scheduling across priorities (query >> interactive >> batch) to minimize weighted latency.
      - Pool preference: if multiple pools exist, pool 0 is protected for query/interactive when they are queued.
      - Conservative-but-adaptive RAM sizing to reduce OOM-driven failures (which are heavily penalized).
      - Bounded retries (more generous for high priority); after too many retries, stop spending cluster time on hopeless work
        (especially for batch) to protect overall score and avoid thrash.
    """
    from collections import deque

    s.tick = 0

    # Per-priority queues (store Pipeline objects)
    s.q = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Weighted deficit counters (used to choose which priority to schedule next)
    s.weights = {
        Priority.QUERY: 10,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 1,
    }
    s.deficit = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Adaptive hints keyed by operator-object identity (operators are typically unique per pipeline)
    s.ram_hint_by_op = {}         # op_key -> last requested/successful ram (or increased after OOM)
    s.retry_count_by_op = {}      # op_key -> number of failures observed
    s.last_fail_oom_by_op = {}    # op_key -> bool (best-effort)

    # Retry caps: try harder for high priority because failures/incompletes dominate the objective
    s.max_retries = {
        Priority.QUERY: 6,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 3,
    }


@register_scheduler(key="scheduler_medium_004")
def scheduler_medium_004(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into per-priority queues.
      - Update adaptive RAM hints on failures (esp. OOM) based on ExecutionResult.
      - Fill pools with ready operators using weighted deficit across priorities,
        protecting pool 0 for query/interactive when there is a backlog.
    """
    # -----------------------
    # Helper functions
    # -----------------------
    def _op_key(op):
        # Use object identity; stable within the simulation for an operator instance.
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _total_queued(prios=None):
        if prios is None:
            return sum(len(s.q[p]) for p in s.q)
        return sum(len(s.q[p]) for p in prios)

    def _choose_priority(allowed_prios):
        # Pick among non-empty allowed priorities using deficit; top up when all are depleted.
        candidates = [p for p in allowed_prios if len(s.q[p]) > 0]
        if not candidates:
            return None

        # Prefer higher deficit; tie-breaker: higher weight
        best = max(candidates, key=lambda p: (s.deficit[p], s.weights[p]))
        if s.deficit[best] <= 0:
            # Top-up only among currently-eligible candidates to keep behavior local and responsive.
            for p in candidates:
                s.deficit[p] += s.weights[p]
            best = max(candidates, key=lambda p: (s.deficit[p], s.weights[p]))

        # Spend one unit of deficit
        if s.deficit[best] > 0:
            s.deficit[best] -= 1
        return best

    def _base_ram_share(priority):
        # Start higher for latency-sensitive work to reduce OOM risk.
        if priority == Priority.QUERY:
            return 0.60
        if priority == Priority.INTERACTIVE:
            return 0.50
        return 0.35  # batch

    def _base_cpu_share(priority):
        # CPU helps latency; give more to queries while keeping room for concurrency.
        if priority == Priority.QUERY:
            return 0.75
        if priority == Priority.INTERACTIVE:
            return 0.60
        return 0.40

    def _desired_resources(pool, priority, op):
        # RAM: base share plus adaptive hints; aggressively bump after failures (especially OOM).
        max_ram = pool.max_ram_pool
        max_cpu = pool.max_cpu_pool

        ram = max(1.0, float(max_ram) * _base_ram_share(priority))
        cpu = max(1, int(round(float(max_cpu) * _base_cpu_share(priority))))

        ok = _op_key(op)
        if ok in s.ram_hint_by_op:
            ram = max(ram, float(s.ram_hint_by_op[ok]))

        retries = s.retry_count_by_op.get(ok, 0)
        if retries > 0:
            # Multiplicative bump with a cap; bigger bump for high priority to prevent repeated OOMs.
            bump = 1.0 + 0.75 * min(retries, 3)
            ram = min(float(max_ram), ram * bump)
            if priority != Priority.BATCH_PIPELINE and s.last_fail_oom_by_op.get(ok, False):
                # If we saw OOM for query/interactive, jump close to max to avoid costly repeats.
                ram = max(ram, 0.85 * float(max_ram))

        ram = min(float(max_ram), ram)
        return cpu, ram

    def _pipeline_should_drop(pipeline):
        # Decide whether to stop re-queuing a pipeline due to excessive failures.
        status = pipeline.runtime_status()

        # If it's already done, no need to keep it.
        if status.is_pipeline_successful():
            return True

        # If there are failed ops, check if any have exceeded retry limits.
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
        if not failed_ops:
            return False

        cap = s.max_retries.get(pipeline.priority, 3)
        exceeded = 0
        non_oom_exceeded = 0
        for op in failed_ops:
            ok = _op_key(op)
            if s.retry_count_by_op.get(ok, 0) >= cap:
                exceeded += 1
                if not s.last_fail_oom_by_op.get(ok, False):
                    non_oom_exceeded += 1

        # For batch: drop sooner to avoid wasting time and harming query/interactive tail.
        if pipeline.priority == Priority.BATCH_PIPELINE and exceeded > 0:
            return True

        # For query/interactive: only drop if we exceeded caps AND it doesn't look like OOM anymore.
        if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE) and non_oom_exceeded > 0:
            return True

        return False

    # -----------------------
    # Ingest arrivals/results
    # -----------------------
    for p in pipelines:
        # Only enqueue real pipelines; do not attempt dedup here (we manage by pop/reappend).
        if p.priority not in s.q:
            # Unknown priority: treat as batch-like
            s.q[Priority.BATCH_PIPELINE].append(p)
        else:
            s.q[p.priority].append(p)

    # Update failure hints based on completed execution results.
    # We do NOT require pipeline_id on results; we learn using the operator identity.
    for r in results:
        if not r.failed():
            # Optionally remember the RAM used as a lower bound hint for next time (helps reduce future OOM).
            for op in getattr(r, "ops", []) or []:
                ok = _op_key(op)
                # Keep the maximum we've seen; avoid shrinking hints.
                prev = s.ram_hint_by_op.get(ok, 0.0)
                try:
                    s.ram_hint_by_op[ok] = max(float(prev), float(r.ram))
                except Exception:
                    pass
            continue

        oom = _is_oom_error(getattr(r, "error", None))
        for op in getattr(r, "ops", []) or []:
            ok = _op_key(op)
            s.retry_count_by_op[ok] = s.retry_count_by_op.get(ok, 0) + 1
            s.last_fail_oom_by_op[ok] = bool(oom)

            # Increase RAM hint; double on OOM, otherwise modest bump.
            try:
                pool = s.executor.pools[r.pool_id]
                max_ram = float(pool.max_ram_pool)
            except Exception:
                max_ram = None

            prev_hint = float(s.ram_hint_by_op.get(ok, getattr(r, "ram", 0.0) or 0.0))
            requested = float(getattr(r, "ram", 0.0) or 0.0)
            base = max(prev_hint, requested, 1.0)

            if oom:
                new_hint = base * 2.0
            else:
                new_hint = base * 1.25

            if max_ram is not None:
                new_hint = min(new_hint, max_ram)

            s.ram_hint_by_op[ok] = max(prev_hint, new_hint)

    # If nothing changed, we can early out.
    if not pipelines and not results:
        return [], []

    s.tick += 1

    suspensions = []
    assignments = []

    # Global backlog signal used for pool protection and batch throttling.
    hi_backlog = _total_queued([Priority.QUERY, Priority.INTERACTIVE]) > 0

    # Prevent scheduling multiple ops from the same pipeline in the same tick (keeps fairness under pressure).
    scheduled_this_tick = set()

    # -----------------------
    # Place assignments
    # -----------------------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pool preference:
        # - If we have multiple pools, reserve pool 0 for query/interactive when they are queued.
        # - Other pools can run anything, but naturally skew toward batch via the queue mix + deficits.
        if s.executor.num_pools >= 2 and pool_id == 0 and hi_backlog:
            allowed_prios = [Priority.QUERY, Priority.INTERACTIVE]
        else:
            allowed_prios = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

        # Fill the pool with multiple assignments, but cap attempts to avoid infinite cycling.
        max_attempts = max(10, _total_queued(allowed_prios) * 2)
        attempts = 0

        while avail_cpu > 0 and avail_ram > 0 and attempts < max_attempts:
            attempts += 1
            prio = _choose_priority(allowed_prios)
            if prio is None:
                break

            pipeline = s.q[prio].popleft()

            # Drop completed or hopeless pipelines (do not requeue).
            if _pipeline_should_drop(pipeline):
                continue

            # Enforce "one op per pipeline per tick".
            if pipeline.pipeline_id in scheduled_this_tick:
                s.q[prio].append(pipeline)
                continue

            # Get next runnable operators (parents complete).
            status = pipeline.runtime_status()
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing ready; keep it queued.
                s.q[prio].append(pipeline)
                continue

            op = op_list[0]

            # If this operator has exceeded retry caps, consider dropping (especially batch).
            ok = _op_key(op)
            cap = s.max_retries.get(pipeline.priority, 3)
            if s.retry_count_by_op.get(ok, 0) >= cap:
                # Batch: stop trying; Query/interactive: only stop if not OOM-like.
                if pipeline.priority == Priority.BATCH_PIPELINE or (not s.last_fail_oom_by_op.get(ok, False)):
                    # Don't requeue; treat as failed/incomplete ultimately.
                    continue

            desired_cpu, desired_ram = _desired_resources(pool, pipeline.priority, op)

            # If high-priority backlog exists, avoid letting batch consume the last headroom on single-pool setups.
            if hi_backlog and pipeline.priority == Priority.BATCH_PIPELINE and s.executor.num_pools == 1:
                cpu_reserve = 0.25 * float(pool.max_cpu_pool)
                ram_reserve = 0.25 * float(pool.max_ram_pool)
                if float(avail_cpu) < cpu_reserve or float(avail_ram) < ram_reserve:
                    # Requeue batch and try others.
                    s.q[prio].append(pipeline)
                    continue

            # Fit CPU first by shrinking; RAM cannot be safely shrunk below desired without knowing minima.
            cpu_to_use = min(int(avail_cpu), int(desired_cpu))
            if cpu_to_use < 1:
                s.q[prio].append(pipeline)
                break

            ram_to_use = float(desired_ram)
            if float(avail_ram) < ram_to_use:
                # Not enough RAM; requeue and try another pipeline that might fit.
                s.q[prio].append(pipeline)
                continue

            # Make assignment.
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_to_use,
                    ram=ram_to_use,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            scheduled_this_tick.add(pipeline.pipeline_id)

            # Update available resources for further packing.
            avail_cpu -= cpu_to_use
            avail_ram -= ram_to_use

            # Requeue pipeline for subsequent operators in later ticks.
            s.q[prio].append(pipeline)

    return suspensions, assignments
