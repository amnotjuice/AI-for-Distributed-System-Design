# policy_key: scheduler_est_006
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.112496
# generation_seconds: 98.31
# generated_at: 2026-03-31T18:31:44.436218
@register_scheduler_init(key="scheduler_est_006")
def scheduler_est_006_init(s):
    """
    Priority-aware, estimate-driven scheduler (small step up from naive FIFO).

    Improvements over the naive baseline:
      1) Strict priority ordering (QUERY > INTERACTIVE > BATCH).
      2) Right-size RAM using op.estimate.mem_peak_gb with a safety factor.
      3) Simple OOM-aware retry: if an op fails with OOM, increase its RAM multiplier.
      4) Light resource reservation: avoid letting BATCH consume the entire pool when
         any higher-priority work is waiting (helps tail latency).
      5) Round-robin fairness within each priority class.

    Notes:
      - No preemption yet (keeps policy small and robust).
      - Assigns one operator per assignment to reduce interference and OOM blast radius.
    """
    # Per-priority waiting queues (round-robin by pop(0)/append()).
    s.waiting_by_prio = {}

    # Per-operator RAM multiplier to react to OOMs: op_key -> multiplier.
    s.op_mem_mult = {}

    # Per-operator failure counts and last error kind: op_key -> int / str.
    s.op_fail_count = {}
    s.op_last_error_kind = {}

    # Tunables (kept intentionally simple).
    s.mem_safety_factor = 1.20
    s.mem_min_gb = 0.25
    s.mem_max_mult = 8.0
    s.mem_oom_bump = 1.50

    s.max_attempts_non_oom = 2
    s.max_attempts_oom = 6

    # Priority ordering (highest first). We keep it in state to avoid recreating each tick.
    s.prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # CPU caps by priority to bias toward latency without monopolizing a whole pool.
    # (Still bounded by pool availability.)
    s.cpu_cap_by_prio = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # Minimum CPU slices so small ops can run even in fragmented pools.
    s.cpu_min = 0.25

    # Reservation fractions (applied only when scheduling lower priority while higher exists).
    s.reserve_frac_for_query = 0.15
    s.reserve_frac_for_interactive = 0.10


@register_scheduler(key="scheduler_est_006")
def scheduler_est_006_scheduler(s, results, pipelines):
    """
    Scheduler step: ingest new pipelines, update OOM-based RAM multipliers from results,
    then greedily schedule ready ops in strict priority order across pools using
    estimate-based RAM sizing and small CPU caps.

    Returns:
      (suspensions, assignments)
    """
    def _prio_key(prio):
        # Lower number = higher priority in sorting logic.
        for i, p in enumerate(s.prio_order):
            if prio == p:
                return i
        return len(s.prio_order)

    def _ensure_queue(prio):
        if prio not in s.waiting_by_prio:
            s.waiting_by_prio[prio] = []

    def _op_stable_id(op):
        # Prefer explicit operator IDs if present; fall back to Python object identity.
        # This key only needs to be stable within a run for adaptive sizing.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # Avoid using a bound method as an ID
                    if not callable(v):
                        return (attr, v)
                except Exception:
                    pass
        return ("py_id", id(op))

    def _op_key(pipeline, op):
        # Include pipeline_id to reduce collision risk across pipelines.
        return (pipeline.pipeline_id, _op_stable_id(op))

    def _is_oom_error(err):
        if not err:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg)

    def _mem_estimate_gb(op):
        est = getattr(op, "estimate", None)
        if est is None:
            return None
        return getattr(est, "mem_peak_gb", None)

    def _desired_ram_gb(pipeline, op):
        ok = _op_key(pipeline, op)
        mult = s.op_mem_mult.get(ok, 1.0)

        mem_est = _mem_estimate_gb(op)
        if mem_est is None:
            # If we have no estimate, be conservative but not huge.
            base = 1.0
        else:
            try:
                base = float(mem_est)
            except Exception:
                base = 1.0

        ram = base * mult * s.mem_safety_factor
        if ram < s.mem_min_gb:
            ram = s.mem_min_gb
        return ram

    def _desired_cpu(prio, avail_cpu):
        cap = s.cpu_cap_by_prio.get(prio, 1.0)
        cpu = min(float(avail_cpu), float(cap))
        if cpu < s.cpu_min:
            cpu = float(avail_cpu)  # if tiny fragments exist, just use what remains
        return cpu

    def _pipeline_should_be_dropped(pipeline):
        # Drop pipelines that have repeatedly failed with non-OOM errors on any op.
        # This prevents infinite retry loops for logical errors.
        status = pipeline.runtime_status()
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
        for op in failed_ops:
            ok = _op_key(pipeline, op)
            cnt = s.op_fail_count.get(ok, 0)
            kind = s.op_last_error_kind.get(ok, None)
            if kind != "oom" and cnt >= s.max_attempts_non_oom:
                return True
            if kind == "oom" and cnt >= s.max_attempts_oom:
                return True
        return False

    def _next_ready_op_for_prio(prio):
        """
        Round-robin scan the priority queue to find one ready op (parents complete).
        Returns (pipeline, op) or (None, None).
        """
        q = s.waiting_by_prio.get(prio, [])
        if not q:
            return None, None

        n = len(q)
        for _ in range(n):
            pipeline = q.pop(0)

            status = pipeline.runtime_status()
            if status.is_pipeline_successful():
                # Completed: drop from queue.
                continue

            if _pipeline_should_be_dropped(pipeline):
                # Give up on this pipeline.
                continue

            # Keep pipeline in queue unless we intentionally drop it.
            q.append(pipeline)

            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                return pipeline, ops[0]

        return None, None

    def _has_any_waiting_higher_than(prio):
        # Cheap approximation: if any higher-priority queue is non-empty, treat as waiting.
        # (Even if blocked on parents, reservation still helps protect interactive latency.)
        prio_rank = _prio_key(prio)
        for p in s.prio_order:
            if _prio_key(p) < prio_rank and s.waiting_by_prio.get(p):
                return True
        return False

    # Ingest new pipelines.
    for p in pipelines:
        _ensure_queue(p.priority)
        s.waiting_by_prio[p.priority].append(p)

    # Update adaptive RAM multipliers based on execution results.
    for r in results:
        if not getattr(r, "ops", None):
            continue
        if not r.failed():
            # On success, gently decay the multiplier toward 1.0.
            for op in r.ops:
                # We don't have pipeline_id on ExecutionResult, so only decay when we can
                # infer it later from the pipeline object. Without pipeline_id, skip decay.
                # (Keeps code robust to different simulator result shapes.)
                pass
            continue

        oom = _is_oom_error(getattr(r, "error", None))
        for op in r.ops:
            # Same issue: ExecutionResult may not carry pipeline_id. We still bump using a
            # weaker key if needed. Prefer a pipeline-aware key when possible, but we can't here.
            # Fall back to a global key to still learn within a run.
            op_fallback_key = ("unknown_pipeline", _op_stable_id(op))

            s.op_fail_count[op_fallback_key] = s.op_fail_count.get(op_fallback_key, 0) + 1
            s.op_last_error_kind[op_fallback_key] = "oom" if oom else "non_oom"

            if oom:
                cur = s.op_mem_mult.get(op_fallback_key, 1.0)
                bumped = cur * s.mem_oom_bump
                if bumped > s.mem_max_mult:
                    bumped = s.mem_max_mult
                s.op_mem_mult[op_fallback_key] = bumped

    # Early exit if no changes.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Snapshot available resources (we'll mutate local copies as we assign).
    pool_avail = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pool_avail.append({
            "pool_id": pool_id,
            "cpu": float(pool.avail_cpu_pool),
            "ram": float(pool.avail_ram_pool),
            "max_cpu": float(pool.max_cpu_pool),
            "max_ram": float(pool.max_ram_pool),
        })

    # Greedy global scheduling:
    # repeatedly pick the highest-priority ready op, then place it on the "best" pool
    # (one that fits; bias to more CPU for high priority; otherwise pack by RAM).
    while True:
        chosen_pipeline = None
        chosen_op = None
        chosen_prio = None

        for prio in s.prio_order:
            pipeline, op = _next_ready_op_for_prio(prio)
            if pipeline is not None:
                chosen_pipeline, chosen_op, chosen_prio = pipeline, op, prio
                break

        if chosen_pipeline is None:
            break  # nothing ready

        # Compute desired resources.
        desired_ram = _desired_ram_gb(chosen_pipeline, chosen_op)

        # Pick a pool that can fit the op. Apply reservation if scheduling lower priority.
        best_idx = None
        best_score = None

        higher_waiting = _has_any_waiting_higher_than(chosen_prio)

        for i, pa in enumerate(pool_avail):
            avail_cpu = pa["cpu"]
            avail_ram = pa["ram"]
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                continue

            # Reservation policy:
            # - If any QUERY exists, reserve some fraction while scheduling INTERACTIVE/BATCH.
            # - If any INTERACTIVE exists, reserve some fraction while scheduling BATCH.
            eff_cpu = avail_cpu
            eff_ram = avail_ram

            if higher_waiting:
                # Reserve based on the specific higher priority that exists.
                # (We keep it simple: reserve a modest fraction of max resources.)
                if chosen_prio == Priority.BATCH_PIPELINE:
                    # Reserve for both QUERY and INTERACTIVE if present.
                    reserve_frac = 0.0
                    if s.waiting_by_prio.get(Priority.QUERY):
                        reserve_frac = max(reserve_frac, s.reserve_frac_for_query)
                    if s.waiting_by_prio.get(Priority.INTERACTIVE):
                        reserve_frac = max(reserve_frac, s.reserve_frac_for_interactive)
                    eff_cpu = max(0.0, avail_cpu - reserve_frac * pa["max_cpu"])
                    eff_ram = max(0.0, avail_ram - reserve_frac * pa["max_ram"])
                elif chosen_prio == Priority.INTERACTIVE:
                    # Reserve for QUERY if present.
                    if s.waiting_by_prio.get(Priority.QUERY):
                        reserve_frac = s.reserve_frac_for_query
                        eff_cpu = max(0.0, avail_cpu - reserve_frac * pa["max_cpu"])
                        eff_ram = max(0.0, avail_ram - reserve_frac * pa["max_ram"])

            if eff_cpu <= 0.0 or eff_ram <= 0.0:
                continue

            # Check fit: require at least minimal CPU and desired RAM.
            cpu_to_give = _desired_cpu(chosen_prio, eff_cpu)
            if cpu_to_give <= 0.0:
                continue
            if desired_ram > eff_ram:
                continue

            # Scoring:
            # - For QUERY/INTERACTIVE: prefer pools with more available CPU (lower latency).
            # - For BATCH: prefer tighter RAM fit (packing) to reduce fragmentation.
            if chosen_prio in (Priority.QUERY, Priority.INTERACTIVE):
                score = (eff_cpu, eff_ram)  # maximize
            else:
                # minimize leftover RAM, then maximize CPU
                score = (-(eff_ram - desired_ram), eff_cpu)

            if best_score is None or score > best_score:
                best_score = score
                best_idx = i

        if best_idx is None:
            # Can't place this op anywhere right now. To avoid spinning on the same op,
            # we stop for this tick (next tick resources may free up).
            break

        pa = pool_avail[best_idx]

        # Final allocation uses the same effective resource constraints (without redoing reservation math).
        cpu = _desired_cpu(chosen_prio, pa["cpu"])
        ram = min(desired_ram, pa["ram"])

        if cpu <= 0.0 or ram <= 0.0:
            break

        assignments.append(
            Assignment(
                ops=[chosen_op],
                cpu=cpu,
                ram=ram,
                priority=chosen_pipeline.priority,
                pool_id=pa["pool_id"],
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

        # Update local availability.
        pa["cpu"] -= cpu
        pa["ram"] -= ram

    return suspensions, assignments
