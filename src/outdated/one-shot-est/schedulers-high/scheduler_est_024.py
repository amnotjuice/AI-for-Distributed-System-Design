# policy_key: scheduler_est_024
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.093470
# generation_seconds: 86.02
# generated_at: 2026-03-31T19:06:11.950408
@register_scheduler_init(key="scheduler_est_024")
def scheduler_est_024_init(s):
    """Priority-aware, right-sized FIFO with simple OOM-aware retries.

    Small, incremental improvements over naive FIFO:
      1) Priority-aware ordering (QUERY > INTERACTIVE > BATCH).
      2) Avoid "give all resources to one op": size CPU/RAM per op using estimates.
      3) Keep headroom for high-priority work by limiting batch usage when high-priority is waiting.
      4) If an op fails due to OOM, retry it with a higher RAM multiplier (instead of dropping the pipeline).
    """
    from collections import deque

    # Per-priority FIFO queues (pipelines are re-queued until completion/fatal failure).
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Per-operator RAM multiplier to apply on top of op.estimate.mem_peak_gb.
    # Increased on OOM failures to reduce repeated OOM churn.
    s.op_mem_mult = {}  # op -> float

    # Mark operators as non-retriable if they fail for reasons other than OOM (best-effort).
    s.op_nonretriable = set()  # set(op)

    # Tuning knobs (kept simple; can be tuned via simulation).
    s.base_mem_safety = 1.20        # starting safety margin on top of estimate
    s.oom_backoff = 1.50            # multiply RAM on each OOM
    s.max_mem_mult = 8.0            # cap to avoid runaway
    s.min_ram_gb = 0.25             # minimum allocation to avoid pathological tiny RAM
    s.min_cpu = 1.0                 # minimum container CPU request

    # Headroom policy: when high-priority is waiting, reserve a fraction of each pool for it
    # by limiting how much BATCH can consume.
    s.reserve_frac_cpu_for_hp = 0.25
    s.reserve_frac_ram_for_hp = 0.25


@register_scheduler(key="scheduler_est_024")
def scheduler_est_024(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    from collections import deque

    def _lower(s0):
        try:
            return (s0 or "").lower()
        except Exception:
            return ""

    def _is_oom_error(err_str: str) -> bool:
        e = _lower(err_str)
        return ("oom" in e) or ("out of memory" in e) or ("out_of_memory" in e) or ("memory" in e and "exceed" in e)

    def _get_est_mem_gb(op) -> float:
        # Best-effort extraction of op.estimate.mem_peak_gb, with safe fallback.
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
            if est is None:
                return 1.0
            est_f = float(est)
            if est_f > 0:
                return est_f
            return 1.0
        except Exception:
            return 1.0

    def _choose_cpu(priority, avail_cpu, pool_max_cpu):
        # Simple per-priority CPU sizing to improve latency while keeping some concurrency.
        # (Avoid allocating the entire pool to one op, which hurts tail latency of later arrivals.)
        try:
            avail = float(avail_cpu)
        except Exception:
            avail = avail_cpu

        if avail <= 0:
            return 0.0

        # Heuristic caps per op by priority
        if priority == Priority.QUERY:
            cap = max(s.min_cpu, min(4.0, float(pool_max_cpu)))
        elif priority == Priority.INTERACTIVE:
            cap = max(s.min_cpu, min(2.0, float(pool_max_cpu)))
        else:
            cap = max(s.min_cpu, min(1.0, float(pool_max_cpu)))

        return max(s.min_cpu, min(avail, cap))

    def _choose_ram(op, avail_ram, pool_max_ram):
        est = _get_est_mem_gb(op)
        mult = s.op_mem_mult.get(op, s.base_mem_safety)
        mult = max(s.base_mem_safety, min(float(mult), s.max_mem_mult))
        s.op_mem_mult[op] = mult  # ensure presence

        need = max(s.min_ram_gb, est * mult)

        try:
            avail = float(avail_ram)
            pool_cap = float(pool_max_ram)
        except Exception:
            avail = avail_ram
            pool_cap = pool_max_ram

        # Never request above pool max; if it still OOMs, we'll back off until capped.
        ram = min(avail, min(need, pool_cap))
        # Ensure strictly positive if possible.
        if ram <= 0 and avail > 0:
            ram = min(avail, max(s.min_ram_gb, 0.01))
        return ram

    # Enqueue new pipelines by priority.
    for p in pipelines:
        # Unknown priorities are treated as lowest.
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s.queues:
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    # Process results: adjust RAM multipliers on OOM; mark non-OOM failures as non-retriable.
    for r in results:
        try:
            if r.failed():
                if _is_oom_error(getattr(r, "error", "")):
                    for op in getattr(r, "ops", []) or []:
                        cur = float(s.op_mem_mult.get(op, s.base_mem_safety))
                        nxt = min(s.max_mem_mult, max(s.base_mem_safety, cur * s.oom_backoff))
                        s.op_mem_mult[op] = nxt
                else:
                    # Best-effort: don't keep retrying non-OOM failures.
                    for op in getattr(r, "ops", []) or []:
                        s.op_nonretriable.add(op)
        except Exception:
            # If result parsing fails, do nothing (keep scheduler robust).
            pass

    # If nothing is queued and nothing arrived/finished, no action.
    if (
        not results
        and not pipelines
        and all(len(q) == 0 for q in s.queues.values())
    ):
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Detect whether we should keep headroom for high-priority arrivals.
    high_pri_waiting = (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    # Schedule across pools.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        try:
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            max_cpu = float(pool.max_cpu_pool)
            max_ram = float(pool.max_ram_pool)
        except Exception:
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            max_cpu = pool.max_cpu_pool
            max_ram = pool.max_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Within this pool, keep pulling ready ops until we run out of resources or nothing is runnable.
        # To avoid infinite loops when nothing is assignable, bound scanning by queue lengths.
        while avail_cpu > 0 and avail_ram > 0:
            made_assignment = False

            # Priority order: QUERY > INTERACTIVE > BATCH
            for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                q = s.queues.get(pr)
                if not q:
                    continue
                if len(q) == 0:
                    continue

                # If high-priority is waiting, throttle batch so we preserve headroom.
                if pr == Priority.BATCH_PIPELINE and high_pri_waiting:
                    reserve_cpu = max_cpu * float(s.reserve_frac_cpu_for_hp)
                    reserve_ram = max_ram * float(s.reserve_frac_ram_for_hp)
                    usable_cpu = max(0.0, avail_cpu - reserve_cpu)
                    usable_ram = max(0.0, avail_ram - reserve_ram)
                    if usable_cpu < s.min_cpu or usable_ram <= 0:
                        continue
                else:
                    usable_cpu = avail_cpu
                    usable_ram = avail_ram

                # Scan up to the current queue length to find a runnable pipeline/op.
                scan_n = len(q)
                for _ in range(scan_n):
                    pipeline = q.popleft()
                    status = pipeline.runtime_status()

                    # Drop completed pipelines.
                    if status.is_pipeline_successful():
                        continue

                    # If any FAILED ops are marked non-retriable, drop the pipeline.
                    # (Conservative: one non-retriable op means pipeline can't complete.)
                    try:
                        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
                        if any(op in s.op_nonretriable for op in (failed_ops or [])):
                            continue
                    except Exception:
                        pass

                    # Pick one runnable op whose parents are complete.
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        # Not ready yet; requeue and try another pipeline.
                        q.append(pipeline)
                        continue

                    op = op_list[0]

                    # If this op is known non-retriable, skip this pipeline.
                    if op in s.op_nonretriable:
                        continue

                    # Size resources for this single op.
                    cpu_req = _choose_cpu(pr, usable_cpu, max_cpu)
                    ram_req = _choose_ram(op, usable_ram, max_ram)

                    # If we cannot allocate meaningful resources, requeue and stop trying this pool.
                    if cpu_req <= 0 or ram_req <= 0:
                        q.append(pipeline)
                        continue

                    # Create assignment.
                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu_req,
                            ram=ram_req,
                            priority=pr,
                            pool_id=pool_id,
                            pipeline_id=pipeline.pipeline_id,
                        )
                    )

                    # Update remaining pool resources and requeue pipeline for subsequent ops.
                    avail_cpu -= cpu_req
                    avail_ram -= ram_req
                    q.append(pipeline)

                    made_assignment = True
                    break  # stop scanning this priority; go back to while-loop to reassess resources

                if made_assignment:
                    break  # proceed to next while iteration

            if not made_assignment:
                break  # nothing runnable in this pool right now

    return suspensions, assignments
