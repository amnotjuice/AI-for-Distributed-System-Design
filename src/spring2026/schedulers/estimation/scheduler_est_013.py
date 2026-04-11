# policy_key: scheduler_est_013
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.036532
# generation_seconds: 35.66
# generated_at: 2026-03-31T23:53:28.146552
@register_scheduler_init(key="scheduler_est_013")
def scheduler_est_013_init(s):
    """
    Priority-aware, estimate-guided, packing scheduler.

    Improvements over naive FIFO:
      1) Priority ordering: always try to schedule higher-priority pipelines first.
      2) Avoid head-of-line blocking: skip pipelines with no ready ops instead of stopping.
      3) Memory-safe sizing using op.estimate.mem_peak_gb as a RAM floor; learn from OOM and retry with higher RAM.
      4) Better utilization: pack multiple operators per pool tick while resources allow (instead of 1 op/pool).
      5) CPU sizing by priority: give more CPU to latency-sensitive work, cap CPU for batch to avoid monopolization.
    """
    s.waiting_queue = []  # list[Pipeline] preserving arrival order (used as stable tie-breaker)
    s.op_ram_floor_gb = {}  # dict[int(op_id) -> float], learned minimum RAM needed to avoid OOM
    s.pipeline_arrival_seq = {}  # dict[pipeline_id -> int], stable ordering
    s._arrival_counter = 0


@register_scheduler(key="scheduler_est_013")
def scheduler_est_013_scheduler(s, results, pipelines):
    """
    Scheduler step.

    Key ideas:
      - Maintain a global waiting queue of pipelines.
      - On each tick:
          * incorporate newly arrived pipelines,
          * update RAM floors for operators that OOM,
          * for each pool, greedily pack ready operators:
              - choose next ready op by (priority desc, arrival asc),
              - allocate RAM >= max(estimate, learned_floor) with headroom,
              - allocate CPU based on priority (higher for interactive),
              - skip if not enough RAM/CPU; try other pipelines.
      - No preemption yet (kept simple and robust as an incremental improvement).
    """
    # --- Helpers (kept inside for single-file policy) ---
    def _prio_rank(prio):
        # Higher number => more latency-sensitive
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1  # Priority.BATCH_PIPELINE and others

    def _is_oom_error(err):
        if not err:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _op_id(op):
        # Use object identity as a stable key in the simulator process.
        # (If ops expose a unique id attribute, this still works as a fallback.)
        try:
            return op.op_id  # type: ignore[attr-defined]
        except Exception:
            return id(op)

    def _op_est_mem_gb(op):
        # Use estimator as a RAM floor when present; otherwise fall back to 0.
        try:
            est = op.estimate.mem_peak_gb  # type: ignore[attr-defined]
            if est is None:
                return 0.0
            return float(est)
        except Exception:
            return 0.0

    def _next_ready_op(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return None
        # If any failures exist, we still allow retry of FAILED since FAILED is in ASSIGNABLE_STATES.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        return op_list[0]

    def _ram_floor_with_learning(op):
        base = _op_est_mem_gb(op)
        learned = s.op_ram_floor_gb.get(_op_id(op), 0.0)
        return max(base, learned, 0.0)

    def _choose_cpu(prio, avail_cpu, pool_max_cpu):
        # Cap per-op CPU to reduce interference; bias higher CPU to latency-sensitive work.
        # Also avoid assigning tiny CPU fragments.
        if avail_cpu <= 0:
            return 0.0

        if prio == Priority.QUERY:
            cap = max(1.0, 0.75 * pool_max_cpu)
        elif prio == Priority.INTERACTIVE:
            cap = max(1.0, 0.50 * pool_max_cpu)
        else:  # batch
            cap = max(1.0, 0.25 * pool_max_cpu)

        cpu = min(avail_cpu, cap)
        # Round down very small slivers to avoid pathological fragmentation
        if cpu < 0.5:
            return 0.0
        return cpu

    def _choose_ram(ram_floor_gb, avail_ram):
        # Allocate at least the floor; add modest headroom (extra RAM is "free" for perf, but constrained).
        if avail_ram <= 0:
            return 0.0
        if ram_floor_gb <= 0:
            # Unknown: allocate a small conservative baseline if possible
            baseline = min(2.0, avail_ram)
            return baseline if baseline > 0 else 0.0

        # Headroom: 20% + 0.5GB absolute to tolerate estimation noise
        target = ram_floor_gb * 1.2 + 0.5
        if target > avail_ram:
            # If we can't meet headroom but can meet the floor, take the floor.
            if ram_floor_gb <= avail_ram:
                return ram_floor_gb
            return 0.0
        return target

    # --- Incorporate new pipelines ---
    for p in pipelines:
        if p.pipeline_id not in s.pipeline_arrival_seq:
            s.pipeline_arrival_seq[p.pipeline_id] = s._arrival_counter
            s._arrival_counter += 1
        s.waiting_queue.append(p)

    # --- Learn from results (primarily OOM -> raise floor and allow retry) ---
    for r in results:
        if getattr(r, "failed", None) and r.failed() and _is_oom_error(getattr(r, "error", None)):
            # Increase RAM floor for each op in the failed container.
            # Strategy: at least double previous learned floor, and at least the RAM that was assigned.
            try:
                assigned_ram = float(getattr(r, "ram", 0.0) or 0.0)
            except Exception:
                assigned_ram = 0.0

            for op in getattr(r, "ops", []) or []:
                oid = _op_id(op)
                prev = float(s.op_ram_floor_gb.get(oid, 0.0) or 0.0)
                # If we OOMed at assigned_ram, require a meaningfully larger amount next time.
                # Use max(assigned_ram * 1.5, prev * 2, estimate) as new learned floor.
                est = _op_est_mem_gb(op)
                new_floor = max(est, assigned_ram * 1.5, prev * 2.0, prev + 1.0)
                s.op_ram_floor_gb[oid] = new_floor

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []  # keep empty for now (no preemption yet)
    assignments = []

    # --- Clean up finished pipelines and de-duplicate queue references ---
    # Keep only pipelines that are not successful; also de-dup by pipeline_id while preserving order.
    seen = set()
    new_queue = []
    for p in s.waiting_queue:
        pid = p.pipeline_id
        if pid in seen:
            continue
        seen.add(pid)
        try:
            if p.runtime_status().is_pipeline_successful():
                continue
        except Exception:
            pass
        new_queue.append(p)
    s.waiting_queue = new_queue

    # --- Scheduling: greedily pack per pool with priority ordering ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        pool_max_cpu = float(pool.max_cpu_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try to place multiple ops per pool tick while capacity remains.
        # To avoid infinite loops when nothing fits, cap attempts.
        attempts = 0
        max_attempts = max(50, 5 * len(s.waiting_queue))

        while avail_cpu > 0 and avail_ram > 0 and attempts < max_attempts:
            attempts += 1

            # Build candidates: (prio_rank desc, arrival asc, pipeline)
            candidates = []
            for p in s.waiting_queue:
                op = _next_ready_op(p)
                if op is None:
                    continue
                pr = _prio_rank(p.priority)
                arr = s.pipeline_arrival_seq.get(p.pipeline_id, 0)
                candidates.append((pr, -arr, p, op))
                # Note: -arr so that earlier arrival has higher priority when sorting descending by tuple

            if not candidates:
                break

            # Sort by priority (high first), then arrival (old first)
            candidates.sort(reverse=True, key=lambda x: (x[0], x[1]))

            placed_any = False
            for _, __, p, op in candidates:
                ram_floor = _ram_floor_with_learning(op)
                ram = _choose_ram(ram_floor, avail_ram)
                if ram <= 0:
                    continue

                cpu = _choose_cpu(p.priority, avail_cpu, pool_max_cpu)
                if cpu <= 0:
                    continue

                # Make the assignment of exactly one op (atomic scheduling unit)
                assignment = Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(assignment)

                # Update local available resources for further packing in this pool
                avail_cpu -= cpu
                avail_ram -= ram
                placed_any = True
                break  # recompute candidates after each placement to reflect changing readiness

            if not placed_any:
                # Nothing fits given remaining resources; stop trying in this pool.
                break

    return suspensions, assignments
