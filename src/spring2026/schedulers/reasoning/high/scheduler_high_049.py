# policy_key: scheduler_high_049
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.125635
# generation_seconds: 143.06
# generated_at: 2026-03-14T04:37:27.035837
@register_scheduler_init(key="scheduler_high_049")
def scheduler_high_049_init(s):
    """
    Priority-aware, headroom-preserving FIFO scheduler.

    Small, incremental improvements over the naive baseline:
      1) Separate waiting queues by priority and always schedule higher priority first.
      2) Avoid allocating the entire pool to a single operator; use small per-op CPU/RAM targets
         to increase concurrency and reduce head-of-line blocking for latency-sensitive work.
      3) Soft reservations: when scheduling BATCH, keep a small CPU/RAM headroom in each pool
         to reduce the chance that QUERY/INTERACTIVE arrivals get stuck behind batch packing.
      4) Basic OOM learning: on OOM failure, retry the same operator with increased RAM.
    """
    # Global tick counter (used only for simple determinism / future aging extensions).
    s.now = 0

    # Active pipelines by id (lets us keep stable references and lazily clean queues).
    s.active_pipelines = {}  # pipeline_id -> Pipeline

    # Priority-separated FIFO queues of pipeline_ids.
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Operator resource hints learned from failures (mostly RAM growth on OOM).
    # (pipeline_id, op_key) -> {"ram": float, "retries": int, "last_error": str}
    s.op_hints = {}

    # Map op_key -> pipeline_id to connect ExecutionResult back to a pipeline.
    s.op_to_pipeline_id = {}

    # Pipelines to drop due to non-retriable failures / too many retries.
    s.drop_pipelines = set()

    # Policy knobs (conservative defaults; keep changes small vs naive baseline).
    s.max_assignments_per_pool = 4
    s.max_oom_retries_per_op = 4

    # Soft reservation applied ONLY when scheduling batch work.
    s.batch_reserve_cpu_frac = 0.20
    s.batch_reserve_ram_frac = 0.20


@register_scheduler(key="scheduler_high_049")
def scheduler_high_049(s, results: "List[ExecutionResult]", pipelines: "List[Pipeline]") -> "Tuple[List[Suspend], List[Assignment]]":
    def _op_key(op) -> str:
        # Best-effort stable key across events.
        # Many simulators expose an op_id; otherwise we fall back to object identity.
        return str(getattr(op, "op_id", id(op)))

    def _is_oom(err) -> bool:
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "exceed" in e)

    def _prio_list():
        # Highest priority first.
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _has_inflight_ops(status) -> bool:
        inflight = status.get_ops(
            [OperatorState.ASSIGNED, OperatorState.RUNNING, OperatorState.SUSPENDING],
            require_parents_complete=False,
        )
        return bool(inflight)

    def _base_cpu_target(prio, pool_max_cpu):
        # Keep per-op CPU modest to reduce contention and improve latency under concurrency.
        if prio == Priority.QUERY:
            return min(2, pool_max_cpu) if pool_max_cpu > 0 else 0
        if prio == Priority.INTERACTIVE:
            return min(2, pool_max_cpu) if pool_max_cpu > 0 else 0
        # Batch gets smaller CPU by default to preserve headroom.
        return min(1, pool_max_cpu) if pool_max_cpu > 0 else 0

    def _base_ram_target(prio, pool_max_ram):
        # Start with a modest slice of pool RAM; OOM learning will grow this as needed.
        if prio == Priority.QUERY:
            return max(1.0, 0.25 * pool_max_ram)
        if prio == Priority.INTERACTIVE:
            return max(1.0, 0.20 * pool_max_ram)
        return max(1.0, 0.15 * pool_max_ram)

    def _min_ram_ratio_to_try(prio):
        # Prevent absurd under-allocation that would likely OOM immediately and waste time.
        if prio == Priority.QUERY:
            return 0.70
        if prio == Priority.INTERACTIVE:
            return 0.70
        return 0.50

    def _pool_effective_avail(i, for_priority, avail_cpu, avail_ram):
        pool = s.executor.pools[i]
        if for_priority == Priority.BATCH_PIPELINE:
            reserve_cpu = s.batch_reserve_cpu_frac * pool.max_cpu_pool
            reserve_ram = s.batch_reserve_ram_frac * pool.max_ram_pool
            eff_cpu = max(0.0, avail_cpu[i] - reserve_cpu)
            eff_ram = max(0.0, avail_ram[i] - reserve_ram)
            return eff_cpu, eff_ram
        return avail_cpu[i], avail_ram[i]

    def _pick_pool_and_alloc(pipeline_id, prio, op, avail_cpu, avail_ram, per_pool_assigned_counts):
        """
        Choose a pool for this op and decide cpu/ram.

        Rules:
          - If we have an OOM-derived RAM hint, require meeting it (don't downscale).
          - Otherwise, allow downscaling RAM only if we can give a reasonable fraction
            of the baseline target (to reduce immediate OOM churn).
          - Prefer pools with larger effective RAM headroom to reduce contention risk.
        """
        opk = _op_key(op)
        hint = s.op_hints.get((pipeline_id, opk), None)

        best = None  # (score, pool_id, cpu, ram)
        for i in range(s.executor.num_pools):
            pool = s.executor.pools[i]

            if per_pool_assigned_counts[i] >= s.max_assignments_per_pool:
                continue

            eff_cpu, eff_ram = _pool_effective_avail(i, prio, avail_cpu, avail_ram)
            if eff_cpu <= 0 or eff_ram <= 0:
                continue

            base_cpu = float(_base_cpu_target(prio, pool.max_cpu_pool))
            base_ram = float(_base_ram_target(prio, pool.max_ram_pool))

            # CPU can always be downscaled to fit; RAM is more sensitive.
            desired_cpu = base_cpu if base_cpu > 0 else 1.0
            desired_ram = base_ram

            if hint and "ram" in hint and hint["ram"] is not None:
                desired_ram = float(hint["ram"])

            # Decide allocatable CPU (always at least 1 if possible).
            cpu = min(desired_cpu, eff_cpu)
            if cpu < 1:
                continue

            # Decide RAM:
            if hint and "ram" in hint and hint["ram"] is not None:
                # After OOM: require meeting learned RAM.
                if eff_ram < desired_ram:
                    continue
                ram = desired_ram
            else:
                # Before OOM: allow some downscale, but avoid severe under-allocation.
                min_ratio = _min_ram_ratio_to_try(prio)
                if eff_ram < max(1.0, min_ratio * desired_ram):
                    continue
                ram = min(desired_ram, eff_ram)

            # Score: prioritize more effective RAM headroom (less interference / fewer OOMs),
            # break ties with effective CPU.
            score = float(eff_ram) + 0.01 * float(eff_cpu)
            if best is None or score > best[0]:
                best = (score, i, cpu, ram)

        if best is None:
            return None
        _, pool_id, cpu, ram = best
        return pool_id, cpu, ram

    # ---- Update time and ingest new pipelines ----
    s.now += 1

    for p in pipelines:
        pid = p.pipeline_id
        # If a pipeline_id is re-sent, keep the latest object reference.
        s.active_pipelines[pid] = p
        # Enqueue once; if already queued, we still keep FIFO semantics by not duplicating.
        # (We do a small O(n) check; acceptable for a small incremental policy.)
        q = s.waiting_by_prio.get(p.priority, s.waiting_by_prio[Priority.BATCH_PIPELINE])
        if pid not in q:
            q.append(pid)

    # ---- Learn from results (OOM -> raise RAM hint; non-OOM -> drop pipeline) ----
    for r in results:
        if not getattr(r, "ops", None):
            continue

        # We generally schedule one op per assignment, but handle lists defensively.
        op0 = r.ops[0]
        opk = _op_key(op0)
        pid = s.op_to_pipeline_id.get(opk, None)

        # Clear op->pipeline mapping when we see a result for it (successful or failed).
        if opk in s.op_to_pipeline_id:
            del s.op_to_pipeline_id[opk]

        if pid is None:
            continue

        if r.failed():
            err = getattr(r, "error", None)
            if _is_oom(err):
                pool = s.executor.pools[r.pool_id]
                key = (pid, opk)
                old = s.op_hints.get(key, {"ram": None, "retries": 0, "last_error": ""})

                old_ram = old["ram"] if old["ram"] is not None else float(getattr(r, "ram", 1.0))
                # Double RAM on OOM, capped by pool max; ensure monotonic increase.
                new_ram = min(float(pool.max_ram_pool), max(float(old_ram) * 2.0, float(old_ram) + 1.0))

                old["ram"] = new_ram
                old["retries"] = int(old.get("retries", 0)) + 1
                old["last_error"] = str(err) if err is not None else "oom"
                s.op_hints[key] = old

                if old["retries"] > s.max_oom_retries_per_op:
                    s.drop_pipelines.add(pid)
            else:
                # Non-OOM failures are treated as non-retriable for now (small, safe step).
                s.drop_pipelines.add(pid)

    # ---- Early exit if nothing to do ----
    if not pipelines and not results:
        return [], []

    # ---- Plan assignments for this tick ----
    suspensions = []  # No preemption in this incremental version.
    assignments = []

    # Local resource tracking for this scheduling tick.
    avail_cpu = [float(s.executor.pools[i].avail_cpu_pool) for i in range(s.executor.num_pools)]
    avail_ram = [float(s.executor.pools[i].avail_ram_pool) for i in range(s.executor.num_pools)]
    per_pool_assigned_counts = [0 for _ in range(s.executor.num_pools)]

    assigned_pipeline_ids = set()

    def _resources_left():
        # We consider any pool with at least 1 CPU and 1 RAM as potentially usable.
        for i in range(s.executor.num_pools):
            if avail_cpu[i] >= 1 and avail_ram[i] >= 1:
                return True
        return False

    made_progress = True
    while made_progress and _resources_left():
        made_progress = False

        for prio in _prio_list():
            q = s.waiting_by_prio.get(prio, [])
            if not q:
                continue

            # Scan each queue at most once per outer loop to avoid infinite cycling.
            n = len(q)
            for _ in range(n):
                pid = q.pop(0)

                if pid in assigned_pipeline_ids:
                    q.append(pid)
                    continue

                pipeline = s.active_pipelines.get(pid, None)
                if pipeline is None:
                    continue

                if pid in s.drop_pipelines:
                    # Drop terminal pipelines (lazy cleanup).
                    s.active_pipelines.pop(pid, None)
                    continue

                status = pipeline.runtime_status()
                if status.is_pipeline_successful():
                    # Clean up completed pipelines.
                    s.active_pipelines.pop(pid, None)
                    continue

                # Keep at most one in-flight operator per pipeline to reduce interference.
                if _has_inflight_ops(status):
                    q.append(pid)
                    continue

                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    q.append(pid)
                    continue

                op = op_list[0]
                picked = _pick_pool_and_alloc(pid, prio, op, avail_cpu, avail_ram, per_pool_assigned_counts)
                if picked is None:
                    # Can't place now; keep FIFO order by re-appending.
                    q.append(pid)
                    continue

                pool_id, cpu, ram = picked

                # Commit local capacity changes for this tick.
                avail_cpu[pool_id] -= float(cpu)
                avail_ram[pool_id] -= float(ram)
                per_pool_assigned_counts[pool_id] += 1

                # Record mapping so we can attribute results to pipelines later.
                s.op_to_pipeline_id[_op_key(op)] = pid

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )

                assigned_pipeline_ids.add(pid)
                made_progress = True

                # Re-append so the pipeline can continue later (still FIFO within priority).
                q.append(pid)

                # Move on quickly (keep latency for high priorities) but still allow
                # multiple placements per tick via the outer loops.
                if not _resources_left():
                    break

            if not _resources_left():
                break

    return suspensions, assignments
