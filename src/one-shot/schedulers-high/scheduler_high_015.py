# policy_key: scheduler_high_015
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.146145
# generation_seconds: 143.95
# generated_at: 2026-03-12T22:57:38.996693
@register_scheduler_init(key="scheduler_high_015")
def scheduler_high_015_init(s):
    """
    Priority-aware, latency-oriented improvement over naive FIFO.

    Key incremental improvements (kept intentionally simple/robust):
      1) Priority queues: QUERY > INTERACTIVE > BATCH.
      2) Soft reservations: when high-priority work is waiting, avoid packing the pool so tightly
         with batch that new interactive/query work must wait.
      3) Multi-assignment per tick: place multiple operators per pool tick with per-op sizing
         (instead of "give the entire pool to the first op").
      4) OOM-aware RAM retry: if an operator OOMs, remember it and exponentially increase RAM
         on retry (bounded by pool capacity). Non-OOM failures are not retried much.
    """
    # Queues contain pipeline_ids; we keep a single entry per pipeline_id across all queues.
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.in_queue = set()  # pipeline_id currently present in some queue
    s.pipelines_by_id = {}  # pipeline_id -> Pipeline

    # Per-operator learning keyed by operator object identity (stable across simulation).
    s.op_ram_est = {}  # op_id(int) -> estimated RAM to request next time
    s.op_last_error = {}  # op_id(int) -> lowercase error string
    s.op_oom_retries = {}  # op_id(int) -> count
    s.op_other_retries = {}  # op_id(int) -> count

    # Policy knobs (conservative defaults)
    s.max_oom_retries = 6
    s.max_other_retries = 1
    s.max_assignments_per_tick = 64

    # Used for minor behavior changes if desired later
    s.tick = 0


def _p15_is_oom_error(err) -> bool:
    if not err:
        return False
    e = str(err).lower()
    # Keep this broad; simulator errors are typically simple strings.
    return ("oom" in e) or ("out of memory" in e) or ("out-of-memory" in e)


def _p15_enqueue(s, pipeline, front: bool = False):
    pid = pipeline.pipeline_id
    if pid in s.in_queue:
        return
    q = s.queues[pipeline.priority]
    if front:
        q.insert(0, pid)
    else:
        q.append(pid)
    s.in_queue.add(pid)
    s.pipelines_by_id[pid] = pipeline


def _p15_drop_pipeline(s, pipeline_id: int):
    # Remove from registry; if present in queue, it will be skipped when popped later
    s.pipelines_by_id.pop(pipeline_id, None)
    # Best-effort removal from in_queue: it is removed on pop; keep robust if not present
    s.in_queue.discard(pipeline_id)


def _p15_pipeline_should_drop(s, pipeline, status) -> bool:
    # If there are failed operators, decide whether to keep retrying.
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    for op in failed_ops:
        oid = id(op)
        err = s.op_last_error.get(oid, "")
        if _p15_is_oom_error(err):
            if s.op_oom_retries.get(oid, 0) > s.max_oom_retries:
                return True
        else:
            # Unknown/non-OOM errors: retry very little.
            if s.op_other_retries.get(oid, 0) > s.max_other_retries:
                return True
    return False


def _p15_default_cpu(pool, priority):
    # Cap per-op CPU so one op doesn't monopolize the pool (helps latency under concurrency).
    max_cpu = pool.max_cpu_pool
    if priority == Priority.QUERY:
        target = 0.60 * max_cpu
        cap = min(max_cpu, 8)
    elif priority == Priority.INTERACTIVE:
        target = 0.45 * max_cpu
        cap = min(max_cpu, 6)
    else:  # batch
        target = 0.25 * max_cpu
        cap = min(max_cpu, 4)
    cpu = min(cap, target)
    if cpu < 1:
        cpu = 1
    return cpu


def _p15_default_ram(pool, priority):
    # Start with moderate RAM slices and learn upwards on OOM.
    max_ram = pool.max_ram_pool
    if priority == Priority.QUERY:
        frac = 0.30
    elif priority == Priority.INTERACTIVE:
        frac = 0.22
    else:
        frac = 0.16
    ram = max(1, frac * max_ram)
    # Prevent pathological "almost full pool" allocations by default.
    ram = min(ram, 0.80 * max_ram)
    return ram


def _p15_ram_request(s, pool, priority, op):
    oid = id(op)
    est = s.op_ram_est.get(oid)
    if est is None or est <= 0:
        est = _p15_default_ram(pool, priority)
    # Clamp to pool capacity.
    if est > pool.max_ram_pool:
        est = pool.max_ram_pool
    if est < 1:
        est = 1
    return est


@register_scheduler(key="scheduler_high_015")
def scheduler_high_015_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler with OOM retry + soft reservations for latency.

    Returns:
      suspensions: currently unused (no safe API to enumerate running containers here).
      assignments: list of new operator container assignments.
    """
    s.tick += 1

    # Ingest new pipelines
    for p in pipelines:
        _p15_enqueue(s, p, front=False)

    # Update per-operator learning from execution results
    for r in results:
        if not hasattr(r, "ops") or not r.ops:
            continue
        if r.failed():
            is_oom = _p15_is_oom_error(r.error)
            for op in r.ops:
                oid = id(op)
                s.op_last_error[oid] = str(r.error).lower() if r.error is not None else ""
                if is_oom:
                    s.op_oom_retries[oid] = s.op_oom_retries.get(oid, 0) + 1
                    # Exponential RAM backoff from the last attempted allocation.
                    base = r.ram if getattr(r, "ram", None) is not None else s.op_ram_est.get(oid, 1)
                    try:
                        new_est = max(1, float(base) * 2.0)
                    except Exception:
                        new_est = max(1, (s.op_ram_est.get(oid, 1) * 2.0))
                    # Keep monotonic non-decreasing estimate.
                    prev = s.op_ram_est.get(oid, 0)
                    s.op_ram_est[oid] = max(prev, new_est)
                else:
                    s.op_other_retries[oid] = s.op_other_retries.get(oid, 0) + 1
        else:
            # On success we keep estimates as-is (no need to shrink aggressively).
            for op in r.ops:
                oid = id(op)
                # Clear last error so future FAILED state decisions are based on the latest failure.
                s.op_last_error.pop(oid, None)

    # Early exit when nothing changes
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Determine if any high-priority work is queued (for soft reservations).
    high_waiting = bool(s.queues[Priority.QUERY] or s.queues[Priority.INTERACTIVE])

    num_pools = s.executor.num_pools
    for pool_id in range(num_pools):
        if len(assignments) >= s.max_assignments_per_tick:
            break

        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pool-local order:
        # - If high is waiting anywhere, prioritize high everywhere (spillover) and stop packing batch too tightly.
        # - Otherwise, allow non-zero pools to prefer batch for throughput.
        if high_waiting:
            prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        else:
            if num_pools > 1 and pool_id > 0:
                prio_order = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]
            else:
                prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

        # Soft reservations: keep some headroom for latency when high-priority work exists.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if high_waiting:
            reserve_cpu = 0.20 * pool.max_cpu_pool
            reserve_ram = 0.20 * pool.max_ram_pool

        # Pack multiple assignments into the pool this tick.
        while avail_cpu >= 1 and avail_ram > 0 and len(assignments) < s.max_assignments_per_tick:
            made = False

            for prio in prio_order:
                q = s.queues[prio]
                if not q:
                    continue

                # If this is batch and high work exists, don't consume reserved headroom.
                if prio == Priority.BATCH_PIPELINE and high_waiting:
                    if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                        continue

                # Try up to current queue length to find a schedulable pipeline (round-robin).
                q_len = len(q)
                if q_len == 0:
                    continue

                selected_pipeline = None
                selected_op = None
                selected_pid = None
                requeue_front = False

                for _ in range(q_len):
                    pid = q.pop(0)
                    s.in_queue.discard(pid)

                    pipeline = s.pipelines_by_id.get(pid)
                    if pipeline is None:
                        continue

                    status = pipeline.runtime_status()

                    # Drop completed pipelines
                    if status.is_pipeline_successful():
                        _p15_drop_pipeline(s, pid)
                        continue

                    # Drop permanently failed pipelines (after limited retries)
                    if status.state_counts.get(OperatorState.FAILED, 0) > 0:
                        if _p15_pipeline_should_drop(s, pipeline, status):
                            _p15_drop_pipeline(s, pid)
                            continue

                    # Find a single ready operator
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        # Not ready yet; keep it in RR order.
                        _p15_enqueue(s, pipeline, front=False)
                        continue

                    op = op_list[0]

                    # Decide resources for this operator
                    ram_req = _p15_ram_request(s, pool, pipeline.priority, op)
                    cpu_req = _p15_default_cpu(pool, pipeline.priority)

                    # Admission check: if it doesn't fit in RAM, try other pipelines that might.
                    if ram_req > avail_ram:
                        # High priority gets put back to the front to reduce extra queuing delay.
                        requeue_front = (pipeline.priority != Priority.BATCH_PIPELINE)
                        _p15_enqueue(s, pipeline, front=requeue_front)
                        continue

                    cpu_alloc = min(avail_cpu, cpu_req)
                    if cpu_alloc < 1:
                        # No CPU left; requeue and stop packing this pool.
                        _p15_enqueue(s, pipeline, front=True)
                        selected_pipeline = None
                        break

                    selected_pipeline = pipeline
                    selected_op = op
                    selected_pid = pid
                    break

                if selected_pipeline is None or selected_op is None:
                    continue

                # Create the assignment (one operator per container)
                assignments.append(
                    Assignment(
                        ops=[selected_op],
                        cpu=min(avail_cpu, _p15_default_cpu(pool, selected_pipeline.priority)),
                        ram=_p15_ram_request(s, pool, selected_pipeline.priority, selected_op),
                        priority=selected_pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=selected_pid,
                    )
                )

                # Update local available resources (must match the assignment we just appended)
                last = assignments[-1]
                avail_cpu -= last.cpu
                avail_ram -= last.ram

                # Requeue pipeline to allow it to make progress step-by-step without hogging.
                _p15_enqueue(s, selected_pipeline, front=False)

                made = True
                break  # go back to while-loop and try to pack another assignment

            if not made:
                break

    return suspensions, assignments
