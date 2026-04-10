# policy_key: scheduler_low_019
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050005
# generation_seconds: 62.89
# generated_at: 2026-04-09T21:17:34.405918
@register_scheduler_init(key="scheduler_low_019")
def scheduler_low_019_init(s):
    """Priority-aware, OOM-robust, fairness-preserving scheduler.

    Main ideas (kept intentionally simple vs. more exotic policies):
      - Strict priority ordering with mild aging to prevent batch starvation.
      - Conservative initial sizing (RAM-first safety) + exponential RAM backoff on OOM.
      - Avoids preemption churn (no suspends by default) to reduce wasted work and failures.
      - Spreads work across pools by choosing the pool with best "fit" for the next op.

    Why this helps the objective:
      - Query + interactive pipelines are admitted and run earlier, reducing weighted latency.
      - OOM failures are a dominant source of 720s penalties; we aggressively retry OOM with more RAM.
      - Batch work still progresses via aging, reducing incomplete pipelines by end of simulation.
    """
    s.waiting = {}  # pipeline_id -> Pipeline
    s.arrival_tick = {}  # pipeline_id -> tick when first seen
    s.ticks = 0

    # Retry control
    s.retryable_fail_pipelines = set()  # pipeline_ids with last failure being OOM-like
    s.nonretryable_fail_pipelines = set()  # pipeline_ids with non-OOM failures

    # Per-(pipeline, op) resource hints (learned from results)
    # Keys: (pipeline_id, op_key)
    s.ram_hint = {}
    s.cpu_hint = {}

    # Config knobs
    s.max_concurrent_assignments_per_pool_per_tick = 4  # keep containers flowing; avoid one huge assignment monopolizing a pool
    s.batch_aging_ticks = 20  # after this many ticks waiting, batch can compete more strongly
    s.interactive_aging_ticks = 10

    # Base "weights" for selecting next pipeline (higher chosen first)
    # These align with the score function (query dominates).
    s.base_select_weight = {
        Priority.QUERY: 100.0,
        Priority.INTERACTIVE: 50.0,
        Priority.BATCH_PIPELINE: 10.0,
    }


@register_scheduler(key="scheduler_low_019")
def scheduler_low_019_scheduler(s, results, pipelines):
    """
    Scheduler step.

    Returns:
      suspensions: empty (we avoid preemption churn here)
      assignments: list of new Assignment objects
    """
    def _op_key(op):
        # Prefer stable identifiers if present; otherwise fallback to object identity.
        return getattr(op, "op_id", getattr(op, "operator_id", id(op)))

    def _is_oom_error(err):
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "exceed" in e)

    def _priority_group(p):
        # Normalize priority names to expected enums.
        return p.priority

    def _effective_weight(p, now_tick):
        pr = _priority_group(p)
        base = s.base_select_weight.get(pr, 1.0)
        age = max(0, now_tick - s.arrival_tick.get(p.pipeline_id, now_tick))

        # Mild aging: prevent starvation without letting batch dominate.
        if pr == Priority.BATCH_PIPELINE:
            bonus = min(40.0, (age / float(max(1, s.batch_aging_ticks))) * 40.0)
            return base + bonus
        if pr == Priority.INTERACTIVE:
            bonus = min(20.0, (age / float(max(1, s.interactive_aging_ticks))) * 20.0)
            return base + bonus
        return base  # queries already dominate

    def _initial_cpu_target(priority, pool, avail_cpu):
        # Keep CPU modest to increase parallelism and reduce head-of-line blocking.
        # (Sublinear scaling + objective favors low latency for many small query/interactive ops.)
        max_cpu = max(1.0, float(pool.max_cpu_pool))
        if priority == Priority.QUERY:
            target = min(avail_cpu, max(1.0, min(4.0, 0.35 * max_cpu)))
        elif priority == Priority.INTERACTIVE:
            target = min(avail_cpu, max(1.0, min(3.0, 0.30 * max_cpu)))
        else:
            target = min(avail_cpu, max(1.0, min(2.0, 0.25 * max_cpu)))
        return max(1.0, float(target))

    def _initial_ram_target(priority, pool, avail_ram):
        # RAM-first safety: OOM causes severe penalties.
        # Start with a meaningful fraction of pool RAM, but bounded by availability.
        max_ram = max(1.0, float(pool.max_ram_pool))
        if priority == Priority.QUERY:
            frac = 0.55
        elif priority == Priority.INTERACTIVE:
            frac = 0.45
        else:
            frac = 0.35
        target = min(avail_ram, max(1.0, frac * max_ram))
        return max(1.0, float(target))

    def _choose_pool_for(pipeline, op, pools):
        # Prefer pool with most headroom to reduce queueing and avoid OOM.
        pr = _priority_group(pipeline)
        best_pool_id = None
        best_score = None

        for pool_id, pool in pools:
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu <= 0 or avail_ram <= 0:
                continue

            # Estimate requested resources for this (pipeline, op).
            ok = _op_key(op)
            hint_ram = s.ram_hint.get((pipeline.pipeline_id, ok))
            hint_cpu = s.cpu_hint.get((pipeline.pipeline_id, ok))

            req_cpu = hint_cpu if hint_cpu is not None else _initial_cpu_target(pr, pool, avail_cpu)
            req_ram = hint_ram if hint_ram is not None else _initial_ram_target(pr, pool, avail_ram)

            # Must fit (at least partially) to avoid obvious failure/pointless assignment.
            if req_cpu > avail_cpu or req_ram > avail_ram:
                continue

            # Score: more leftover is better; prioritize RAM headroom slightly (OOM avoidance).
            left_cpu = avail_cpu - req_cpu
            left_ram = avail_ram - req_ram

            # Priority-sensitive preference: queries/interactive want the "best" pool.
            if pr == Priority.QUERY:
                score = (2.0 * left_ram) + (1.0 * left_cpu)
            elif pr == Priority.INTERACTIVE:
                score = (1.7 * left_ram) + (1.0 * left_cpu)
            else:
                # Batch prefers to backfill; accept tighter fits.
                score = (1.2 * left_ram) + (1.0 * left_cpu)

            if best_score is None or score > best_score:
                best_score = score
                best_pool_id = pool_id

        return best_pool_id

    # Track time
    s.ticks += 1

    # Ingest new pipelines
    for p in pipelines:
        if p.pipeline_id not in s.waiting:
            s.waiting[p.pipeline_id] = p
            s.arrival_tick[p.pipeline_id] = s.ticks

    # Learn from execution results (especially OOM backoff)
    for r in results:
        # r.ops may be a list; associate hints to each op.
        ops = getattr(r, "ops", None) or []
        pipeline_id = getattr(r, "pipeline_id", None)

        # If pipeline_id isn't present on result, try to infer from ops if they carry it.
        # (We keep it best-effort; scheduler will still function without perfect attribution.)
        if pipeline_id is None:
            pipeline_id = getattr(r, "dag_id", None)

        if r.failed():
            # Mark pipeline retryability based on error.
            if _is_oom_error(getattr(r, "error", None)):
                if pipeline_id is not None:
                    s.retryable_fail_pipelines.add(pipeline_id)
                    if pipeline_id in s.nonretryable_fail_pipelines:
                        s.nonretryable_fail_pipelines.discard(pipeline_id)

                # Exponential RAM backoff for each failed op (cap at pool max learned from result's pool).
                # If result.ram is missing, we still set a reasonable bump.
                last_ram = float(getattr(r, "ram", 0.0) or 0.0)
                last_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                pool_id = getattr(r, "pool_id", None)

                pool_max_ram = None
                pool_max_cpu = None
                if pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
                    pool = s.executor.pools[int(pool_id)]
                    pool_max_ram = float(pool.max_ram_pool)
                    pool_max_cpu = float(pool.max_cpu_pool)

                # Heuristic bump: double RAM, nudge CPU slightly (CPU not a cause of OOM, but helps latency).
                new_ram = last_ram * 2.0 if last_ram > 0 else (0.5 * pool_max_ram if pool_max_ram else 8.0)
                if pool_max_ram:
                    new_ram = min(new_ram, pool_max_ram)

                new_cpu = last_cpu
                if last_cpu > 0:
                    new_cpu = min(last_cpu + 1.0, pool_max_cpu if pool_max_cpu else last_cpu + 1.0)

                if pipeline_id is not None:
                    for op in ops:
                        ok = _op_key(op)
                        s.ram_hint[(pipeline_id, ok)] = max(s.ram_hint.get((pipeline_id, ok), 0.0) or 0.0, new_ram)
                        if new_cpu and new_cpu > 0:
                            s.cpu_hint[(pipeline_id, ok)] = max(s.cpu_hint.get((pipeline_id, ok), 0.0) or 0.0, new_cpu)
            else:
                if pipeline_id is not None:
                    s.nonretryable_fail_pipelines.add(pipeline_id)
                    if pipeline_id in s.retryable_fail_pipelines:
                        s.retryable_fail_pipelines.discard(pipeline_id)
        else:
            # Success: optionally record a "known good" RAM/cpu as a floor for this op.
            # This reduces later OOM retries if similar op is retried due to transient conditions.
            if pipeline_id is not None:
                used_ram = float(getattr(r, "ram", 0.0) or 0.0)
                used_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                for op in ops:
                    ok = _op_key(op)
                    if used_ram > 0:
                        s.ram_hint[(pipeline_id, ok)] = max(s.ram_hint.get((pipeline_id, ok), 0.0) or 0.0, used_ram)
                    if used_cpu > 0:
                        s.cpu_hint[(pipeline_id, ok)] = max(s.cpu_hint.get((pipeline_id, ok), 0.0) or 0.0, used_cpu)

    # Early exit if nothing to do
    if not pipelines and not results and not s.waiting:
        return [], []

    suspensions = []  # We avoid preemption in this version to minimize churn/wasted work.
    assignments = []

    # Build list of runnable candidates (pipelines with assignable ops and not terminally failed)
    candidates = []
    to_delete = []

    for pid, p in s.waiting.items():
        status = p.runtime_status()

        # Drop completed pipelines from the waiting map
        if status.is_pipeline_successful():
            to_delete.append(pid)
            continue

        # If pipeline has failures, only keep it if we consider it retryable (OOM).
        has_failures = status.state_counts.get(OperatorState.FAILED, 0) > 0
        if has_failures and pid in s.nonretryable_fail_pipelines and pid not in s.retryable_fail_pipelines:
            # Terminal failure; stop scheduling it.
            to_delete.append(pid)
            continue

        # Find next assignable op(s). We schedule at most one op per pipeline per tick.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            continue

        candidates.append((p, op_list[0]))

    for pid in to_delete:
        if pid in s.waiting:
            del s.waiting[pid]

    if not candidates:
        return suspensions, assignments

    # Sort candidates by priority/aging; higher effective weight first.
    candidates.sort(key=lambda x: _effective_weight(x[0], s.ticks), reverse=True)

    # Pool iteration order: process pools with more headroom first to increase placement success rate.
    pools = [(i, s.executor.pools[i]) for i in range(s.executor.num_pools)]
    pools.sort(key=lambda t: (float(t[1].avail_ram_pool) + float(t[1].avail_cpu_pool)), reverse=True)

    # Greedy placement: repeatedly attempt to place best candidate onto best-fitting pool.
    # Stop when no more placements are possible this tick.
    used_any = True
    per_pool_assigned = {i: 0 for i in range(s.executor.num_pools)}

    while used_any:
        used_any = False
        if not candidates:
            break

        # Iterate over a snapshot of candidates; remove placed ones.
        remaining = []
        for pipeline, op in candidates:
            # Pick the best pool that can fit the op with our current hints.
            pool_choice = _choose_pool_for(pipeline, op, pools)
            if pool_choice is None:
                remaining.append((pipeline, op))
                continue

            pool = s.executor.pools[int(pool_choice)]
            if per_pool_assigned[int(pool_choice)] >= s.max_concurrent_assignments_per_pool_per_tick:
                remaining.append((pipeline, op))
                continue

            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu <= 0 or avail_ram <= 0:
                remaining.append((pipeline, op))
                continue

            pr = _priority_group(pipeline)
            ok = _op_key(op)

            req_cpu = s.cpu_hint.get((pipeline.pipeline_id, ok))
            req_ram = s.ram_hint.get((pipeline.pipeline_id, ok))

            if req_cpu is None:
                req_cpu = _initial_cpu_target(pr, pool, avail_cpu)
            req_cpu = min(float(req_cpu), avail_cpu)
            req_cpu = max(1.0, req_cpu)

            if req_ram is None:
                req_ram = _initial_ram_target(pr, pool, avail_ram)
            req_ram = min(float(req_ram), avail_ram)
            req_ram = max(1.0, req_ram)

            # If hints are too large to fit, skip for now (could fit later as pool frees).
            if req_cpu > avail_cpu or req_ram > avail_ram:
                remaining.append((pipeline, op))
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=pr,
                    pool_id=int(pool_choice),
                    pipeline_id=pipeline.pipeline_id,
                )
            )
            per_pool_assigned[int(pool_choice)] += 1
            used_any = True

        candidates = remaining

    return suspensions, assignments
