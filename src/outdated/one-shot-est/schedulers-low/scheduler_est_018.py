# policy_key: scheduler_est_018
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044806
# generation_seconds: 51.91
# generated_at: 2026-03-31T23:57:06.156655
@register_scheduler_init(key="scheduler_est_018")
def scheduler_est_018_init(s):
    """
    Priority-aware FIFO with small, safe improvements over naive FIFO:
      - Separate waiting queues per priority (QUERY > INTERACTIVE > BATCH).
      - Simple pool affinity: reserve pool 0 preferentially for high-priority when possible.
      - Admission control: keep a headroom reserve when high-priority is waiting (avoid batch consuming all).
      - OOM-aware RAM escalation using op.estimate.mem_peak_gb as a floor + retry backoff on failures.
      - Conservative per-op CPU sizing by priority (favor latency for high-priority).
    """
    # Per-priority pipeline queues (FIFO within each priority).
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator RAM override and retry counters to react to failures (e.g., OOM).
    # Keys are based on Python object identity to avoid relying on unknown op id fields.
    s.op_ram_override_gb = {}   # op_key -> ram_gb floor to request
    s.op_retry_count = {}       # op_key -> retries attempted
    s.max_retries_per_op = 3

    # Parameters (kept intentionally simple).
    s.mem_slack_gb = 0.5
    s.reserve_frac_cpu = 0.30   # reserve when high-priority backlog exists
    s.reserve_frac_ram = 0.30

    # CPU sizing by priority as a fraction of pool max CPU (bounded by availability).
    s.cpu_frac_by_prio = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.30,
    }

    # Batch fairness knob: if high-priority backlog is persistent, still allow occasional batch.
    s.max_consecutive_high_prio = 4
    s._consecutive_high_prio_scheduled = 0


def _op_key(op):
    # Use object identity to form a stable key within the simulator run.
    return id(op)


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _has_high_prio_backlog(s):
    return bool(s.waiting_queues[Priority.QUERY] or s.waiting_queues[Priority.INTERACTIVE])


def _select_queue_for_pool(s, pool_id):
    """
    Choose which priority queue to pull from, given the pool.
    - Prefer high-priority on pool 0.
    - Prefer batch on other pools when no high-priority backlog (or when we deliberately allow batch).
    """
    high_backlog = _has_high_prio_backlog(s)

    # If there's high-priority backlog, bias pool 0 to high-priority.
    if pool_id == 0 and high_backlog:
        if s.waiting_queues[Priority.QUERY]:
            return Priority.QUERY
        if s.waiting_queues[Priority.INTERACTIVE]:
            return Priority.INTERACTIVE

    # On other pools, prefer batch unless high-priority is waiting and we're trying to drain it.
    if pool_id != 0:
        if s.waiting_queues[Priority.BATCH_PIPELINE] and (not high_backlog):
            return Priority.BATCH_PIPELINE
        # If high-priority backlog exists, still allow batch occasionally for fairness.
        if s.waiting_queues[Priority.BATCH_PIPELINE] and s._consecutive_high_prio_scheduled >= s.max_consecutive_high_prio:
            return Priority.BATCH_PIPELINE
        if s.waiting_queues[Priority.QUERY]:
            return Priority.QUERY
        if s.waiting_queues[Priority.INTERACTIVE]:
            return Priority.INTERACTIVE
        if s.waiting_queues[Priority.BATCH_PIPELINE]:
            return Priority.BATCH_PIPELINE

    # Fallback: global priority order.
    for pr in _priority_order():
        if s.waiting_queues[pr]:
            return pr
    return None


def _estimate_mem_floor_gb(s, op):
    # Base floor from estimator.
    est = None
    try:
        est = op.estimate.mem_peak_gb
    except Exception:
        est = None

    floor = 1.0
    if isinstance(est, (int, float)) and est and est > 0:
        floor = float(est)

    # Apply any learned override (e.g., after failure).
    k = _op_key(op)
    if k in s.op_ram_override_gb:
        floor = max(floor, float(s.op_ram_override_gb[k]))

    return floor


def _cpu_request(s, pool, priority, avail_cpu):
    # Request a fraction of pool max CPU, bounded to [1, avail_cpu].
    frac = s.cpu_frac_by_prio.get(priority, 0.30)
    req = max(1.0, float(pool.max_cpu_pool) * float(frac))
    return max(1.0, min(float(avail_cpu), req))


def _ram_request(s, avail_ram, mem_floor_gb):
    # Request at least floor + slack; never allocate below floor.
    req = max(mem_floor_gb, mem_floor_gb + s.mem_slack_gb)
    if float(avail_ram) < float(mem_floor_gb):
        return None
    return min(float(avail_ram), float(req))


@register_scheduler(key="scheduler_est_018")
def scheduler_est_018(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue newly arrived pipelines into per-priority FIFO queues.
      2) Observe failures; escalate RAM floors for failed ops with bounded retries.
      3) For each pool, greedily assign ready ops from selected priority queues while respecting:
         - headroom reserves when high-priority backlog exists (avoid batch starving latency)
         - pool 0 affinity for high-priority
         - per-op sizing (CPU by priority, RAM by estimate/override)
    """
    # Enqueue new pipelines.
    for p in pipelines:
        pr = p.priority
        if pr not in s.waiting_queues:
            # Unknown priority: treat as lowest.
            pr = Priority.BATCH_PIPELINE
        s.waiting_queues[pr].append(p)

    # Process execution results to learn from failures (e.g., OOM -> increase RAM floor).
    # We avoid complex error parsing; any failure triggers a conservative RAM bump.
    if results:
        for r in results:
            if not r.failed():
                continue
            # Conservative RAM escalation for all ops in this failed container.
            for op in getattr(r, "ops", []) or []:
                k = _op_key(op)
                prev = s.op_ram_override_gb.get(k, None)
                # If scheduler previously allocated ram=r.ram, double it; else bump from estimate.
                base = float(getattr(r, "ram", 0.0) or 0.0)
                if base <= 0:
                    base = _estimate_mem_floor_gb(s, op)
                bumped = max(base * 2.0, base + 1.0)
                if prev is None:
                    s.op_ram_override_gb[k] = bumped
                else:
                    s.op_ram_override_gb[k] = max(float(prev), bumped)

                s.op_retry_count[k] = int(s.op_retry_count.get(k, 0)) + 1

    # Early exit if nothing new that could affect decisions.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Reset or update fairness counter based on whether we schedule high-priority this tick.
    scheduled_high_prio_this_tick = False

    # Try to place work on each pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Reserve headroom if high priority is waiting; prevents batch from consuming everything.
        high_backlog = _has_high_prio_backlog(s)
        reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_frac_cpu) if high_backlog else 0.0
        reserve_ram = float(pool.max_ram_pool) * float(s.reserve_frac_ram) if high_backlog else 0.0

        # Greedily assign multiple ops per pool per tick as long as resources remain.
        while avail_cpu > 0 and avail_ram > 0:
            chosen_pr = _select_queue_for_pool(s, pool_id)
            if chosen_pr is None:
                break

            # Enforce reserve only against BATCH placements (keep capacity for high-priority).
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram
            if chosen_pr == Priority.BATCH_PIPELINE and high_backlog:
                eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                eff_avail_ram = max(0.0, avail_ram - reserve_ram)
                if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                    break

            # Find a pipeline in the chosen queue that has an assignable op ready.
            q = s.waiting_queues[chosen_pr]
            if not q:
                break

            picked_idx = None
            picked_pipeline = None
            picked_op_list = None

            # Scan a small prefix to avoid head-of-line blocking on DAG dependencies.
            scan_limit = min(len(q), 16)
            for i in range(scan_limit):
                pipeline = q[i]
                status = pipeline.runtime_status()

                if status.is_pipeline_successful():
                    continue

                # Drop pipelines that have exhausted retries for any failed ops.
                # We conservatively check retry counts on FAILED ops (assignable) before re-scheduling them.
                failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=True) or []
                exhausted = False
                for op in failed_ops:
                    if int(s.op_retry_count.get(_op_key(op), 0)) >= int(s.max_retries_per_op):
                        exhausted = True
                        break
                if exhausted:
                    # Skip permanently (do not requeue).
                    continue

                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    continue

                # Check resource feasibility for this op.
                op = op_list[0]
                mem_floor = _estimate_mem_floor_gb(s, op)
                ram_req = _ram_request(s, eff_avail_ram, mem_floor)
                if ram_req is None:
                    continue

                cpu_req = _cpu_request(s, pool, chosen_pr, eff_avail_cpu)
                if cpu_req <= 0 or cpu_req > eff_avail_cpu:
                    continue

                picked_idx = i
                picked_pipeline = pipeline
                picked_op_list = op_list
                break

            if picked_pipeline is None:
                # Nothing feasible right now for this pool; stop attempting further placements.
                break

            # Remove picked pipeline from queue, then re-append to maintain FIFO while allowing progress.
            q.pop(picked_idx)

            # Re-check status: if it completed while scanning, just drop.
            status = picked_pipeline.runtime_status()
            if status.is_pipeline_successful():
                continue

            op = picked_op_list[0]
            mem_floor = _estimate_mem_floor_gb(s, op)

            # Recompute reqs using current avail (not effective) while respecting reserves.
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram
            if chosen_pr == Priority.BATCH_PIPELINE and high_backlog:
                eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                eff_avail_ram = max(0.0, avail_ram - reserve_ram)

            ram_req = _ram_request(s, eff_avail_ram, mem_floor)
            if ram_req is None:
                # Put it back and stop (can't make progress).
                q.append(picked_pipeline)
                break
            cpu_req = _cpu_request(s, pool, chosen_pr, eff_avail_cpu)
            if cpu_req <= 0:
                q.append(picked_pipeline)
                break

            # Issue assignment.
            assignments.append(
                Assignment(
                    ops=picked_op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=picked_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=picked_pipeline.pipeline_id,
                )
            )

            # Update resource tracking for further greedy placements.
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)

            # Requeue pipeline for subsequent ops.
            q.append(picked_pipeline)

            if chosen_pr in (Priority.QUERY, Priority.INTERACTIVE):
                scheduled_high_prio_this_tick = True

    # Update fairness counter.
    if scheduled_high_prio_this_tick:
        s._consecutive_high_prio_scheduled = int(s._consecutive_high_prio_scheduled) + 1
    else:
        s._consecutive_high_prio_scheduled = 0

    return suspensions, assignments
