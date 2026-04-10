# policy_key: scheduler_est_032
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.147398
# generation_seconds: 89.08
# generated_at: 2026-03-31T19:21:49.564010
@register_scheduler_init(key="scheduler_est_032")
def scheduler_est_032_init(s):
    """Priority-aware, estimate-driven, non-preemptive scheduler.

    Improvements over naive FIFO:
      1) Priority queues: always try QUERY, then INTERACTIVE, then BATCH.
      2) Right-sizing: allocate RAM from op.estimate.mem_peak_gb (plus slack) instead of "all available".
      3) OOM retry: if an op fails with an OOM-like error, retry with exponentially increased RAM.
      4) Head-of-line blocking reduction: limited scanning within each priority queue to find a runnable op.
      5) Soft reservations: keep some CPU/RAM headroom for high-priority work by limiting batch admission.
    """
    from collections import deque

    s.tick = 0

    # Per-priority pipeline queues
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Per-op retry sizing state (keyed by id(op) to avoid relying on unknown op identifiers)
    s.op_mem_mult = {}      # id(op) -> multiplier applied to estimate.mem_peak_gb
    s.op_fail_count = {}    # id(op) -> number of observed failures
    s.op_last_oom = {}      # id(op) -> bool (last failure looked like OOM)

    # Tuning knobs (kept simple and conservative)
    s.max_oom_retries = 4
    s.mem_slack_gb = 0.5
    s.mem_growth_factor = 2.0
    s.max_mem_mult = 16.0
    s.min_ram_gb = 0.5

    # CPU shaping: avoid giving a single op the entire pool unless the pool is tiny
    s.cpu_caps = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 8.0,
    }

    # Soft reservations to protect tail latency for high-priority work (non-preemptive)
    s.reserve_frac_cpu = 0.35
    s.reserve_frac_ram = 0.35

    # Bounded scanning per priority to reduce head-of-line blocking
    s.scan_limit_per_priority = 8


def _is_oom_error(err):
    """Heuristic: classify an error string as OOM-like."""
    if not err:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _estimate_ram_gb(s, op, pool_max_ram_gb):
    """Compute RAM request based on estimator and past OOM retries."""
    est = None
    try:
        est_obj = getattr(op, "estimate", None)
        est = getattr(est_obj, "mem_peak_gb", None)
    except Exception:
        est = None

    if est is None:
        # If no estimate is available, pick a small but non-trivial default.
        est = max(s.min_ram_gb, 1.0)

    mult = s.op_mem_mult.get(id(op), 1.0)
    ram = float(est) * float(mult) + float(s.mem_slack_gb)

    # Enforce bounds
    ram = max(float(s.min_ram_gb), ram)
    ram = min(float(pool_max_ram_gb), ram)
    return ram


def _choose_cpu(s, priority, pool_max_cpu, avail_cpu):
    """Choose a CPU request that balances latency and concurrency."""
    pool_max_cpu = float(pool_max_cpu)
    avail_cpu = float(avail_cpu)

    cap = float(s.cpu_caps.get(priority, 1.0))

    # Shape by pool size to avoid hogging large pools with single ops.
    if priority == Priority.BATCH_PIPELINE:
        shaped = max(1.0, pool_max_cpu / 2.0)
    elif priority == Priority.INTERACTIVE:
        shaped = max(1.0, pool_max_cpu / 3.0)
    else:  # Priority.QUERY
        shaped = max(1.0, pool_max_cpu / 4.0)

    cpu = min(cap, shaped, avail_cpu)
    cpu = max(1.0, cpu) if avail_cpu >= 1.0 else avail_cpu
    return cpu


@register_scheduler(key="scheduler_est_032")
def scheduler_est_032_scheduler(s, results, pipelines):
    """
    Scheduling step:
      - Ingest new pipelines into priority queues
      - Learn from failures (OOM -> increase RAM multiplier)
      - For each pool, repeatedly place ready ops:
          QUERY -> INTERACTIVE -> BATCH
        using estimate-based RAM and shaped CPU, while keeping soft headroom for high priority.
    """
    s.tick += 1

    # Learn from execution results (primarily for OOM-driven retries)
    for r in results or []:
        if not r.failed():
            continue
        is_oom = _is_oom_error(getattr(r, "error", None))
        for op in (getattr(r, "ops", None) or []):
            k = id(op)
            s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1
            s.op_last_oom[k] = bool(is_oom)
            if is_oom:
                prev = float(s.op_mem_mult.get(k, 1.0))
                s.op_mem_mult[k] = min(prev * float(s.mem_growth_factor), float(s.max_mem_mult))

    # Enqueue new pipelines by priority
    for p in pipelines or []:
        # Default to batch if unknown priority is encountered
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s.queues:
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    # Match the example's "early exit" optimization
    if not (pipelines or results):
        return [], []

    suspensions = []
    assignments = []

    # Soft reservation only matters if any high-priority pipelines are waiting
    high_waiting = bool(s.queues[Priority.QUERY] or s.queues[Priority.INTERACTIVE])

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        reserved_cpu = float(pool.max_cpu_pool) * float(s.reserve_frac_cpu) if high_waiting else 0.0
        reserved_ram = float(pool.max_ram_pool) * float(s.reserve_frac_ram) if high_waiting else 0.0

        made_assignment = True
        while made_assignment:
            made_assignment = False

            # Always schedule highest priority available work first
            for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                # For batch, respect soft reservation when high-priority work is present
                if pr == Priority.BATCH_PIPELINE and (avail_cpu <= reserved_cpu or avail_ram <= reserved_ram):
                    continue

                q = s.queues.get(pr)
                if not q:
                    continue

                scan = min(len(q), int(s.scan_limit_per_priority))
                chosen = None
                chosen_op = None
                chosen_cpu = None
                chosen_ram = None

                # Limited scan to reduce head-of-line blocking
                for _ in range(scan):
                    pipeline = q.popleft()
                    status = pipeline.runtime_status()

                    # Drop completed pipelines (do not requeue)
                    if status.is_pipeline_successful():
                        continue

                    # If pipeline has FAILED ops, only retry if they look like OOM and under retry cap
                    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
                    if failed_ops:
                        drop = False
                        for opf in failed_ops:
                            k = id(opf)
                            if not s.op_last_oom.get(k, False):
                                drop = True
                                break
                            if s.op_fail_count.get(k, 0) > int(s.max_oom_retries):
                                drop = True
                                break
                        if drop:
                            continue

                    # Find one ready-to-assign op
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        # Not ready yet; keep pipeline circulating
                        q.append(pipeline)
                        continue

                    op = op_list[0]

                    # Right-size resources
                    ram_need = _estimate_ram_gb(s, op, pool.max_ram_pool)
                    if ram_need > avail_ram:
                        q.append(pipeline)
                        continue

                    cpu_need = _choose_cpu(s, pr, pool.max_cpu_pool, avail_cpu)
                    if cpu_need <= 0.0:
                        q.append(pipeline)
                        continue

                    # For batch, enforce headroom for high priority
                    if pr == Priority.BATCH_PIPELINE:
                        if (avail_cpu - cpu_need) < reserved_cpu or (avail_ram - ram_need) < reserved_ram:
                            q.append(pipeline)
                            continue

                    chosen = pipeline
                    chosen_op = op
                    chosen_cpu = cpu_need
                    chosen_ram = ram_need
                    break

                if chosen is None:
                    continue

                assignments.append(
                    Assignment(
                        ops=[chosen_op],
                        cpu=chosen_cpu,
                        ram=chosen_ram,
                        priority=chosen.priority,
                        pool_id=pool_id,
                        pipeline_id=chosen.pipeline_id,
                    )
                )

                # Update local available resources to avoid oversubscription within this tick
                avail_cpu -= float(chosen_cpu)
                avail_ram -= float(chosen_ram)

                # Requeue pipeline for subsequent ops
                s.queues[pr].append(chosen)

                made_assignment = True
                break  # restart from highest priority with updated avail_* in this pool

    return suspensions, assignments
