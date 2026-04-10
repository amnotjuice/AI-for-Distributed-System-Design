# policy_key: scheduler_medium_050
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.086900
# generation_seconds: 90.13
# generated_at: 2026-03-14T03:25:49.900954
@register_scheduler_init(key="scheduler_medium_050")
def scheduler_medium_050_init(s):
    """Priority-aware, latency-oriented improvement over naive FIFO.

    Main changes vs. naive example:
      1) Maintain separate waiting queues per priority (INTERACTIVE > QUERY > BATCH).
      2) Allow multiple concurrent assignments per pool per tick by slicing resources
         into per-container targets (instead of giving one op the whole pool).
      3) Keep small headroom when batch is scheduling and higher-priority work exists,
         to reduce head-of-line blocking for interactive/query arrivals.
      4) Simple OOM-aware retry: if an op fails with an OOM-like error, retry it with
         more RAM (bounded), up to a small retry limit.
    """
    # Priority queues
    s.waiting_queues = {
        Priority.INTERACTIVE: [],
        Priority.QUERY: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.priority_order = [Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE]

    # Sizing knobs (fractions of pool capacity, converted to ints at runtime)
    s.cpu_frac = {
        Priority.INTERACTIVE: 0.25,
        Priority.QUERY: 0.35,
        Priority.BATCH_PIPELINE: 0.50,
    }
    s.ram_frac = {
        Priority.INTERACTIVE: 0.25,
        Priority.QUERY: 0.35,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Keep some headroom when batch is running and higher-priority work is waiting
    s.headroom_cpu_frac = 0.20
    s.headroom_ram_frac = 0.20

    # OOM retry state (tracked per operator object id; pipeline-level counters too)
    s.op_to_pipeline = {}          # id(op) -> pipeline_id
    s.op_ram_mult = {}             # id(op) -> multiplier for requested RAM
    s.pipeline_oom_retries = {}    # pipeline_id -> count
    s.last_fail_is_oom = {}        # pipeline_id -> bool

    # OOM retry knobs
    s.max_oom_retries = 2
    s.ram_growth = 1.6
    s.max_ram_mult = 8.0


@register_scheduler(key="scheduler_medium_050")
def scheduler_medium_050_scheduler(s, results, pipelines):
    """Priority-aware scheduler with resource slicing + light OOM-aware retries."""
    # Helper functions kept local (no imports).
    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg)

    def _has_higher_priority_backlog():
        return (len(s.waiting_queues[Priority.INTERACTIVE]) > 0) or (len(s.waiting_queues[Priority.QUERY]) > 0)

    def _pop_next_runnable(priority):
        """Pop next pipeline from the given priority queue that has a runnable op.
        Returns (pipeline, op_list) or (None, None). Pipelines that aren't runnable
        are rotated to the back to avoid head-of-line blocking.
        """
        q = s.waiting_queues[priority]
        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            status = p.runtime_status()

            # Drop completed pipelines.
            if status.is_pipeline_successful():
                continue

            # Handle pipelines with failures:
            # - If last failure wasn't OOM (or unknown), we treat as terminal (skip).
            # - If last failure was OOM, allow limited retries (FAILED is assignable).
            has_failures = status.state_counts[OperatorState.FAILED] > 0
            if has_failures:
                pid = p.pipeline_id
                if not s.last_fail_is_oom.get(pid, False):
                    # Non-OOM failure: skip pipeline (do not retry).
                    continue
                if s.pipeline_oom_retries.get(pid, 0) > s.max_oom_retries:
                    # Too many OOM retries: skip.
                    continue

            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable right now; rotate to back.
                q.append(p)
                continue

            # Runnable now: return without re-adding; caller will requeue.
            return p, op_list

        return None, None

    def _pick_next_pipeline_and_op():
        """Pick next runnable (pipeline, op_list) across priorities."""
        for pr in s.priority_order:
            p, op_list = _pop_next_runnable(pr)
            if p is not None:
                return pr, p, op_list
        return None, None, None

    # Incorporate new pipelines into per-priority queues.
    for p in pipelines:
        s.waiting_queues[p.priority].append(p)

    # Process results to update OOM retry state.
    for r in results:
        if not getattr(r, "ops", None):
            continue

        failed = r.failed()
        is_oom = _is_oom_error(getattr(r, "error", None))

        for op in r.ops:
            op_key = id(op)
            pid = s.op_to_pipeline.get(op_key)

            if failed:
                if pid is not None:
                    s.last_fail_is_oom[pid] = bool(is_oom)
                    if is_oom:
                        s.pipeline_oom_retries[pid] = s.pipeline_oom_retries.get(pid, 0) + 1

                if is_oom:
                    prev = s.op_ram_mult.get(op_key, 1.0)
                    s.op_ram_mult[op_key] = min(s.max_ram_mult, prev * s.ram_growth)
            else:
                # On success, clear pipeline "last fail was OOM" and gently decay RAM multiplier.
                if pid is not None:
                    s.last_fail_is_oom[pid] = False
                    s.pipeline_oom_retries.pop(pid, None)

                if op_key in s.op_ram_mult:
                    s.op_ram_mult[op_key] = max(1.0, s.op_ram_mult[op_key] * 0.9)

    # Early exit if nothing to do.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Schedule work per pool, allowing multiple assignments as long as resources remain.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try to fill this pool with multiple containers.
        # Leave headroom for high priority when scheduling batch and there is backlog.
        while avail_cpu > 0 and avail_ram > 0:
            pr, pipeline, op_list = _pick_next_pipeline_and_op()
            if pipeline is None:
                break

            # Determine per-container targets based on pool size and priority.
            target_cpu = int(max(1, round(pool.max_cpu_pool * s.cpu_frac.get(pr, 0.5))))
            target_ram = int(max(1, round(pool.max_ram_pool * s.ram_frac.get(pr, 0.5))))

            # Apply OOM RAM multiplier (per-op, bounded).
            op_key = id(op_list[0])
            mult = s.op_ram_mult.get(op_key, 1.0)
            target_ram = int(max(1, min(pool.max_ram_pool * 0.90, target_ram * mult)))

            # If we're about to schedule batch while higher priority is waiting,
            # cap batch usage to preserve headroom.
            if pr == Priority.BATCH_PIPELINE and _has_higher_priority_backlog():
                headroom_cpu = int(max(0, round(pool.max_cpu_pool * s.headroom_cpu_frac)))
                headroom_ram = int(max(0, round(pool.max_ram_pool * s.headroom_ram_frac)))
                cap_cpu = max(0, avail_cpu - headroom_cpu)
                cap_ram = max(0, avail_ram - headroom_ram)
                if cap_cpu <= 0 or cap_ram <= 0:
                    # Put it back and stop scheduling batch on this pool for now.
                    s.waiting_queues[pr].append(pipeline)
                    break
                target_cpu = min(target_cpu, cap_cpu)
                target_ram = min(target_ram, cap_ram)

            # Fit within currently available resources.
            cpu = min(target_cpu, avail_cpu)
            ram = min(target_ram, avail_ram)

            # If we can't allocate at least minimal resources, stop trying on this pool.
            if cpu <= 0 or ram <= 0:
                s.waiting_queues[pr].append(pipeline)
                break

            # Record mapping so results can be attributed back for OOM retry logic.
            s.op_to_pipeline[id(op_list[0])] = pipeline.pipeline_id

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            avail_cpu -= cpu
            avail_ram -= ram

            # Requeue pipeline for its next steps.
            s.waiting_queues[pr].append(pipeline)

    return suspensions, assignments
