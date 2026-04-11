# policy_key: scheduler_medium_027
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.071441
# generation_seconds: 86.04
# generated_at: 2026-03-14T02:51:31.801809
@register_scheduler_init(key="scheduler_medium_027")
def scheduler_medium_027_init(s):
    """Priority-aware, latency-oriented scheduler (incremental improvement over naive FIFO).

    Main improvements vs. naive:
      1) Priority-first selection (QUERY > INTERACTIVE > BATCH).
      2) Avoid "one op consumes the whole pool" by capping per-op CPU/RAM and packing multiple ops.
      3) Simple OOM-reactive RAM upscaling using recent failures (retry FAILED ops instead of dropping).
      4) Basic anti-starvation: periodically schedule BATCH even if high-priority exists (weighted mixing).
    """
    s.step_count = 0

    # Priority queues (pipelines are re-queued each tick until completion).
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline adaptive sizing state.
    # pipeline_id -> dict(ram_mult, cpu_mult, failures, arrival_step)
    s.pipe_state = {}

    # Weighted mixing knobs (small, obvious anti-starvation).
    # After this many high-priority dispatches in a row (per pool per tick), force a batch if available.
    s.hp_streak_target = 4

    # Retry guardrail to avoid infinite churn on pathological pipelines.
    s.max_failures_per_pipeline = 8


@register_scheduler(key="scheduler_medium_027")
def scheduler_medium_027(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """Priority-aware packing with OOM-reactive resizing.

    Policy sketch:
      - Enqueue newly arrived pipelines by priority.
      - Update per-pipeline RAM/CPU multipliers based on recent failures.
      - For each pool, repeatedly:
          * pick next runnable op from highest effective priority (with a small batch "aging" mix-in)
          * assign it with capped CPU/RAM to improve concurrency and latency
      - No preemption (lack of reliable running-container inventory in the provided interface).
    """
    s.step_count += 1

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # -------------------------
    # Helper functions (local)
    # -------------------------
    def _prio_list():
        # Highest to lowest
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _ensure_pipe_state(p: Pipeline):
        if p.pipeline_id not in s.pipe_state:
            s.pipe_state[p.pipeline_id] = {
                "ram_mult": 1.0,
                "cpu_mult": 1.0,
                "failures": 0,
                "arrival_step": s.step_count,
            }

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _update_from_results(res_list: List[ExecutionResult]):
        # We do not drop pipelines on failure; we adapt their sizing.
        for r in res_list:
            # Best effort: infer pipeline_id from ops if present; otherwise cannot update.
            # In the provided interface, ExecutionResult does not expose pipeline_id directly.
            # We therefore only do global adaptation if we can find the pipeline_id on the op object.
            pipe_id = None
            if getattr(r, "ops", None):
                op0 = r.ops[0]
                pipe_id = getattr(op0, "pipeline_id", None) or getattr(op0, "pipeline", None)
                # If op carries a pipeline object, try to resolve its id
                if pipe_id is not None and not isinstance(pipe_id, (str, int)):
                    pipe_id = getattr(pipe_id, "pipeline_id", None)

            if pipe_id is None:
                continue

            st = s.pipe_state.get(pipe_id)
            if st is None:
                continue

            if r.failed():
                st["failures"] += 1
                # RAM-first reaction for OOM; modest CPU bump otherwise (to reduce long tails).
                if _is_oom_error(getattr(r, "error", None)):
                    st["ram_mult"] = min(16.0, st["ram_mult"] * 2.0)
                else:
                    st["cpu_mult"] = min(4.0, st["cpu_mult"] * 1.25)
            else:
                # Gentle decay back toward 1.0 on success (prevents permanent over-allocation).
                st["ram_mult"] = max(1.0, st["ram_mult"] * 0.95)
                st["cpu_mult"] = max(1.0, st["cpu_mult"] * 0.97)

    def _enqueue_new(p_list: List[Pipeline]):
        for p in p_list:
            _ensure_pipe_state(p)
            s.waiting[p.priority].append(p)

    def _pipeline_done_or_abandoned(p: Pipeline) -> bool:
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        pst = s.pipe_state.get(p.pipeline_id)
        if pst and pst["failures"] > s.max_failures_per_pipeline:
            # Abandon after repeated failures to avoid clogging the scheduler indefinitely.
            return True
        return False

    def _get_one_runnable_op(p: Pipeline):
        st = p.runtime_status()
        # Assign only when parents are complete to preserve DAG semantics.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _cap_for_priority(pool, prio):
        # Per-op caps to avoid "single-op monopolization" and improve tail latency.
        # Tuned conservatively; still allows scale-up preference vs. extreme micro-sharding.
        if prio == Priority.QUERY:
            cpu_cap = max(1.0, 0.60 * pool.max_cpu_pool)
            ram_cap = max(1.0, 0.60 * pool.max_ram_pool)
        elif prio == Priority.INTERACTIVE:
            cpu_cap = max(1.0, 0.50 * pool.max_cpu_pool)
            ram_cap = max(1.0, 0.50 * pool.max_ram_pool)
        else:  # BATCH_PIPELINE
            cpu_cap = max(1.0, 0.40 * pool.max_cpu_pool)
            ram_cap = max(1.0, 0.40 * pool.max_ram_pool)
        return cpu_cap, ram_cap

    def _reserve_for_high_prio(pool, high_prio_waiting: bool):
        # If high-priority is waiting anywhere, keep some headroom in each pool by limiting
        # how much batch is allowed to consume *from currently available resources*.
        if not high_prio_waiting:
            return 0.0, 0.0
        # Small reservation that tends to reduce "new query waits for batch to finish".
        return (0.20 * pool.max_cpu_pool), (0.20 * pool.max_ram_pool)

    # -------------------------
    # Update state
    # -------------------------
    _enqueue_new(pipelines)
    if results:
        _update_from_results(results)

    # Early exit if nothing to do
    if not pipelines and not results:
        # Still may have waiting pipelines from before; do not exit early.
        pass

    # Determine if any high-priority work is queued (system-wide signal for reservations).
    high_prio_waiting_any = (len(s.waiting[Priority.QUERY]) > 0) or (len(s.waiting[Priority.INTERACTIVE]) > 0)

    # -------------------------
    # Scheduling loop per pool
    # -------------------------
    # We requeue pipelines we touched to preserve order while still allowing fair mixing.
    requeue = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Bound how many total assignments we attempt per pool per tick to avoid heavy loops.
    max_ops_per_pool_per_tick = 8

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # High-priority streak counter (per pool, per tick) for a mild batch anti-starvation mix-in.
        hp_streak = 0

        # While there are resources and something schedulable
        ops_scheduled = 0
        scheduled_pipeline_ids = set()

        # To avoid scanning forever when queues contain mostly blocked DAG nodes, cap attempts.
        scan_budget = 64

        while ops_scheduled < max_ops_per_pool_per_tick and avail_cpu > 0 and avail_ram > 0 and scan_budget > 0:
            scan_budget -= 1

            # Decide which priority to pull from (weighted mixing).
            # Normally: QUERY > INTERACTIVE > BATCH
            # But if we've scheduled several high-priority ops in a row, try a batch once.
            prio_order = _prio_list()
            if hp_streak >= s.hp_streak_target and len(s.waiting[Priority.BATCH_PIPELINE]) > 0:
                prio_order = [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]

            picked = None
            picked_prio = None

            # Pick next pipeline with a runnable op, respecting "one op per pipeline per tick".
            for prio in prio_order:
                q = s.waiting[prio]
                if not q:
                    continue

                # Round-robin-ish: pop from front, evaluate, then requeue (either global requeue or back).
                p = q.pop(0)

                if _pipeline_done_or_abandoned(p):
                    # Drop completed/abandoned pipeline
                    continue

                if p.pipeline_id in scheduled_pipeline_ids:
                    # Already scheduled one op for this pipeline on this pool this tick; requeue and skip.
                    requeue[prio].append(p)
                    continue

                op = _get_one_runnable_op(p)
                if op is None:
                    # Not runnable yet; keep it around.
                    requeue[prio].append(p)
                    continue

                picked = (p, op)
                picked_prio = prio
                # Put pipeline back to requeue; it will remain pending for future ops.
                requeue[prio].append(p)
                break

            if picked is None:
                # Nothing runnable found right now
                break

            pipeline, op = picked

            # Batch reservation when high-priority exists (keeps some headroom).
            cpu_reserve, ram_reserve = _reserve_for_high_prio(pool, high_prio_waiting_any)
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram
            if picked_prio == Priority.BATCH_PIPELINE and high_prio_waiting_any:
                eff_avail_cpu = max(0.0, avail_cpu - cpu_reserve)
                eff_avail_ram = max(0.0, avail_ram - ram_reserve)

            # If reservation eliminates headroom, skip scheduling batch in this pool this tick.
            if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                # If we were trying to schedule batch due to anti-starvation order, reset streak and continue.
                # Otherwise break to avoid spinning.
                if picked_prio == Priority.BATCH_PIPELINE:
                    hp_streak = 0
                    continue
                break

            # Compute per-op request with caps and adaptive multipliers
            cpu_cap, ram_cap = _cap_for_priority(pool, picked_prio)
            pst = s.pipe_state.get(pipeline.pipeline_id, {"ram_mult": 1.0, "cpu_mult": 1.0})

            # Start from a reasonable share, not the entire pool.
            # Use a small floor to allow scheduling on fractional availability.
            min_cpu = 0.5
            min_ram = 0.5

            # Base request: capped by priority cap and current effective availability.
            req_cpu = min(cpu_cap, eff_avail_cpu)
            req_ram = min(ram_cap, eff_avail_ram)

            # Apply adaptive multipliers; still bounded by effective availability.
            req_cpu = min(eff_avail_cpu, max(min_cpu, req_cpu * float(pst.get("cpu_mult", 1.0))))
            req_ram = min(eff_avail_ram, max(min_ram, req_ram * float(pst.get("ram_mult", 1.0))))

            # If we can't meet minimal sizes, don't schedule.
            if req_cpu < min_cpu or req_ram < min_ram:
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update local accounting
            avail_cpu -= req_cpu
            avail_ram -= req_ram
            ops_scheduled += 1
            scheduled_pipeline_ids.add(pipeline.pipeline_id)

            # Update streak bookkeeping
            if picked_prio in (Priority.QUERY, Priority.INTERACTIVE):
                hp_streak += 1
            else:
                hp_streak = 0

    # Put pipelines back into their waiting queues (preserving relative order within priority class).
    # Note: we also keep any pipelines that were never popped this tick still in s.waiting lists already.
    for prio in _prio_list():
        s.waiting[prio].extend(requeue[prio])

    return suspensions, assignments
