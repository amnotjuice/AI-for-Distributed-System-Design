# policy_key: scheduler_low_043
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.047093
# generation_seconds: 47.38
# generated_at: 2026-04-09T21:37:07.540795
@register_scheduler_init(key="scheduler_low_043")
def scheduler_low_043_init(s):
    """
    Priority-aware, OOM-averse, non-preemptive packing scheduler.

    Goals:
      - Minimize weighted latency dominated by QUERY/INTERACTIVE.
      - Avoid failures (each failure/incomplete is heavily penalized).
      - Improve over naive FIFO with:
          (1) strict priority queues (QUERY > INTERACTIVE > BATCH),
          (2) conservative initial RAM sizing + exponential RAM backoff on failures,
          (3) soft resource reservations so batch cannot crowd out high-priority work,
          (4) pack multiple ops per pool per tick when feasible (higher throughput).
    """
    from collections import deque

    # Separate waiting queues by priority (pipeline objects).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track pipeline IDs already enqueued to avoid duplication when re-adding.
    s.enqueued = set()

    # Per-(pipeline, op) RAM backoff state: next RAM request and failure count.
    # Key: (pipeline_id, op_key) -> {"ram": float, "fails": int}
    s.op_ram_hint = {}

    # For simple fairness among same-priority pipelines: round-robin cursor is implicit via deque rotation.
    # Track recently seen pipelines in this tick to avoid infinite loops on blocked DAGs.
    s._last_tick = 0


def _prio_rank(priority):
    # Smaller is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _queue_for(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _op_key(op):
    # Best-effort stable key across simulator objects.
    for k in ("op_id", "operator_id", "task_id", "name"):
        if hasattr(op, k):
            try:
                v = getattr(op, k)
                if v is not None:
                    return str(v)
            except Exception:
                pass
    # Fallback: repr (may be stable enough within a run)
    try:
        return repr(op)
    except Exception:
        return str(id(op))


def _pipeline_done_or_failed(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If any operator is FAILED, treat pipeline as failed and stop retrying (avoid wasting time).
    # (The simulator may represent OOM as an operator failure; we still retry at operator level via ASSIGNABLE_STATES,
    # but if failures are terminal in your setup, this prevents thrash.)
    if status.state_counts.get(OperatorState.FAILED, 0) > 0:
        return True
    return False


def _get_next_assignable_op(pipeline):
    status = pipeline.runtime_status()
    # Only schedule ops whose parents are complete.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[0]


def _base_ram_fraction(priority):
    # More conservative RAM for higher priority to reduce OOM risk (failures are very expensive).
    if priority == Priority.QUERY:
        return 0.35
    if priority == Priority.INTERACTIVE:
        return 0.28
    return 0.20


def _base_cpu_fraction(priority):
    # Keep some sharing; queries get more CPU to reduce tail latency.
    if priority == Priority.QUERY:
        return 0.60
    if priority == Priority.INTERACTIVE:
        return 0.45
    return 0.35


def _soft_reservations(s):
    # If high-priority work exists anywhere, reserve capacity in each pool so batch doesn't fully occupy it.
    # This is "soft" since we are non-preemptive; it only influences new placements.
    any_query = len(s.q_query) > 0
    any_interactive = len(s.q_interactive) > 0
    # Reservations are fractions of pool capacity kept available for higher-priority arrivals.
    res = {"cpu": 0.0, "ram": 0.0}
    if any_query:
        res["cpu"] = max(res["cpu"], 0.45)
        res["ram"] = max(res["ram"], 0.45)
    if any_interactive:
        res["cpu"] = max(res["cpu"], 0.25)
        res["ram"] = max(res["ram"], 0.25)
    return res


def _choose_request(s, pool, pipeline, op, avail_cpu, avail_ram):
    """
    Decide (cpu, ram) for this op in this pool.
    Conservative on RAM (avoid OOM), moderate on CPU (avoid hogging, allow packing).
    """
    priority = pipeline.priority
    opk = (_op_key(op))
    key = (pipeline.pipeline_id, opk)

    # Start from base target sizes derived from pool capacity.
    target_ram = pool.max_ram_pool * _base_ram_fraction(priority)
    target_cpu = pool.max_cpu_pool * _base_cpu_fraction(priority)

    # If we have a learned/backed-off RAM hint due to prior failures, honor it.
    hint = s.op_ram_hint.get(key)
    if hint and hint.get("ram") is not None:
        target_ram = max(target_ram, hint["ram"])

    # Never request more than available. Also avoid allocating vanishingly small RAM.
    # Use a floor relative to pool size to reduce OOM risk for unknown operators.
    min_ram_floor = pool.max_ram_pool * 0.08  # 8% of pool RAM floor
    ram = min(avail_ram, max(min_ram_floor, target_ram))
    ram = min(ram, pool.max_ram_pool)

    # CPU: choose moderate chunk; allow multiple concurrent ops.
    min_cpu_floor = max(1.0, pool.max_cpu_pool * 0.10)
    cpu = min(avail_cpu, max(min_cpu_floor, target_cpu))
    cpu = min(cpu, pool.max_cpu_pool)

    # If RAM is tight, scale CPU down slightly to improve packing (RAM is the OOM driver here).
    if avail_ram < pool.max_ram_pool * 0.15:
        cpu = min(cpu, max(1.0, pool.max_cpu_pool * 0.15))

    return cpu, ram


@register_scheduler(key="scheduler_low_043")
def scheduler_low_043(s, results, pipelines):
    """
    Main scheduling loop:
      - Enqueue new pipelines into priority queues.
      - Process execution results to update RAM backoff hints on failures.
      - For each pool, repeatedly place ready ops using priority order and soft reservations:
          QUERY -> INTERACTIVE -> BATCH
        while resources allow.
      - No preemption (keeps churn low; relies on reservations + packing).
    """
    # Add new pipelines to queues (avoid duplicates).
    for p in pipelines:
        if p.pipeline_id in s.enqueued:
            continue
        s.enqueued.add(p.pipeline_id)
        _queue_for(s, p.priority).append(p)

    # Update RAM hints based on failures.
    # We assume any failure may be due to under-provisioned RAM; backoff RAM aggressively to prevent repeats.
    for r in results:
        try:
            if not r.failed():
                continue
        except Exception:
            # If failed() isn't available, treat presence of error as failure signal.
            if not getattr(r, "error", None):
                continue

        pipeline_id = getattr(r, "pipeline_id", None)
        # If pipeline_id isn't present on results, we still can backoff by op identity alone,
        # but the API examples show pipeline_id in Assignment only, so results may omit it.
        # We'll derive a best-effort key per op without pipeline_id if needed.
        for op in getattr(r, "ops", []) or []:
            opk = _op_key(op)
            pid = pipeline_id if pipeline_id is not None else "unknown"
            key = (pid, opk)

            prev = s.op_ram_hint.get(key, {"ram": None, "fails": 0})
            prev_fails = int(prev.get("fails", 0)) + 1

            last_ram = getattr(r, "ram", None)
            # If we don't know last RAM, still increase to a conservative fraction of the pool later via base sizing.
            if last_ram is None:
                new_ram = prev.get("ram", None)
            else:
                # Exponential backoff capped later by pool.max_ram_pool in _choose_request.
                new_ram = float(last_ram) * (2.0 if prev_fails <= 2 else 1.5)

            s.op_ram_hint[key] = {"ram": new_ram, "fails": prev_fails}

    # Early exit if nothing to do.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Soft reservations apply when high-priority queues are non-empty (global signal).
    global_res = _soft_reservations(s)

    # Helper: pull next pipeline from a queue that is still active and has an assignable op.
    def pop_runnable_from_queue(q, max_scan):
        # Scan up to max_scan elements to find a runnable pipeline; rotate others to preserve RR fairness.
        scanned = 0
        while q and scanned < max_scan:
            p = q.popleft()
            scanned += 1

            # Drop completed/failed pipelines from our queues.
            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            # If pipeline has failures, we still allow retry only if simulator marks FAILED as assignable.
            # But if it's terminal, it won't produce assignable ops anyway.
            op = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if op:
                return p, op[0]
            # Not runnable now; requeue to try later.
            q.append(p)
        return None, None

    # Schedule per pool: pack as many ops as feasible.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Compute per-pool reserved headroom (do not let batch consume it).
        res_cpu = pool.max_cpu_pool * global_res["cpu"]
        res_ram = pool.max_ram_pool * global_res["ram"]

        # Local loop to create multiple assignments in this pool this tick.
        # To reduce starvation, allow batch scheduling when no high-priority is runnable or resources are abundant.
        max_iters = 64  # safety bound
        iters = 0

        while iters < max_iters and avail_cpu > 0 and avail_ram > 0:
            iters += 1

            # Determine which priority to schedule next.
            # Prefer QUERY, then INTERACTIVE, then BATCH.
            selected = None

            # Try QUERY
            p, op = pop_runnable_from_queue(s.q_query, max_scan=min(len(s.q_query), 8) if s.q_query else 0)
            if p is not None:
                selected = (p, op)
            else:
                # Try INTERACTIVE
                p, op = pop_runnable_from_queue(
                    s.q_interactive, max_scan=min(len(s.q_interactive), 8) if s.q_interactive else 0
                )
                if p is not None:
                    selected = (p, op)
                else:
                    # Try BATCH, but respect reservations if high-priority exists.
                    effective_avail_cpu = avail_cpu
                    effective_avail_ram = avail_ram
                    if len(s.q_query) > 0 or len(s.q_interactive) > 0:
                        effective_avail_cpu = max(0.0, avail_cpu - res_cpu)
                        effective_avail_ram = max(0.0, avail_ram - res_ram)

                    # If reservations leave nothing, stop scheduling batch in this pool for now.
                    if effective_avail_cpu <= 0 or effective_avail_ram <= 0:
                        break

                    p, op = pop_runnable_from_queue(s.q_batch, max_scan=min(len(s.q_batch), 8) if s.q_batch else 0)
                    if p is not None:
                        # Use the reservation-adjusted resources for sizing.
                        cpu_req, ram_req = _choose_request(s, pool, p, op, effective_avail_cpu, effective_avail_ram)
                        if cpu_req <= 0 or ram_req <= 0:
                            # Can't place; requeue and stop.
                            s.q_batch.appendleft(p)
                            break

                        assignments.append(
                            Assignment(
                                ops=[op],
                                cpu=cpu_req,
                                ram=ram_req,
                                priority=p.priority,
                                pool_id=pool_id,
                                pipeline_id=p.pipeline_id,
                            )
                        )
                        avail_cpu -= cpu_req
                        avail_ram -= ram_req

                        # Requeue pipeline for future ops.
                        s.q_batch.append(p)
                        continue
                    else:
                        break  # nothing runnable anywhere

            if selected is None:
                break

            p, op = selected
            # For high priority, use full available (no reservation reduction), but still keep moderate sizing for packing.
            cpu_req, ram_req = _choose_request(s, pool, p, op, avail_cpu, avail_ram)
            if cpu_req <= 0 or ram_req <= 0:
                # Can't place; requeue and stop for this pool.
                _queue_for(s, p.priority).appendleft(p)
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Requeue pipeline to allow its next op later (keeps throughput and fairness).
            _queue_for(s, p.priority).append(p)

    return suspensions, assignments
