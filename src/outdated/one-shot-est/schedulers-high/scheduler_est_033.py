# policy_key: scheduler_est_033
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.094716
# generation_seconds: 127.38
# generated_at: 2026-03-31T19:23:56.943572
@register_scheduler_init(key="scheduler_est_033")
def scheduler_est_033_init(s):
    """Priority-aware, estimate-based packing scheduler (incremental improvement over naive FIFO).

    Key improvements vs naive:
      - Priority ordering (QUERY > INTERACTIVE > BATCH) to reduce tail latency for high-priority work.
      - Use op.estimate.mem_peak_gb to right-size RAM (instead of grabbing the whole pool).
      - Pack multiple ops per pool per tick (instead of at most one), improving responsiveness.
      - Conservative "reserve" of CPU/RAM when high-priority work exists, to avoid batch filling the pool.
      - Simple OOM/backoff loop: on failures, increase RAM for the same op on retry; on success, gently decay.
    """
    # Monotonic scheduler time (ticks)
    s.tick = 0

    # Pipeline bookkeeping
    s.pipeline_by_id = {}         # pipeline_id -> Pipeline
    s.enqueue_tick = {}           # pipeline_id -> tick enqueued (for optional aging/debug)

    # Per-priority queues (store pipeline_ids)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.in_queue = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Per-operator retry / sizing state (keyed by best-effort stable op key)
    s.op_fail_count = {}          # op_key -> int failures seen
    s.max_op_retries = 8

    # Resource sizing knobs
    s.min_cpu = 1.0
    s.min_ram_gb = 0.25

    # CPU targets by priority (cap; we still pack multiple ops)
    s.cpu_targets = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # Memory sizing: base safety factor and multiplicative backoff per failure
    s.base_mem_safety = 1.25
    s.mem_backoff = 1.60

    # When any high-priority work is waiting, keep this fraction of each pool available
    # by preventing *new batch* assignments from consuming the reserved headroom.
    s.reserve_frac_when_high_waiting = 0.30

    # If we can't classify a pipeline priority, treat as background/batch.
    s.default_priority = Priority.BATCH_PIPELINE


def _prio_order():
    # Highest priority first
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _normalize_priority(s, p):
    pr = getattr(p, "priority", None)
    if pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        return pr
    return s.default_priority


def _op_key(op):
    """Best-effort stable key for tracking retries/sizing.

    We avoid relying on any single attribute (simulator may vary).
    Fall back to Python object identity, which is typically stable within a sim run.
    """
    for attr in ("op_id", "operator_id", "task_id", "id", "name"):
        v = getattr(op, attr, None)
        if v is not None:
            return (attr, v)
    return ("pyid", id(op))


def _est_mem_gb(op):
    est = getattr(op, "estimate", None)
    mem = getattr(est, "mem_peak_gb", None) if est is not None else None
    if mem is None:
        # Conservative but not huge; we'll back off on failure.
        return 1.0
    try:
        mem_f = float(mem)
        if mem_f > 0:
            return mem_f
    except Exception:
        pass
    return 1.0


def _predict_ram_gb(s, op, pool, avail_ram):
    base = max(s.min_ram_gb, _est_mem_gb(op) * s.base_mem_safety)
    fc = s.op_fail_count.get(_op_key(op), 0)
    ram = base * (s.mem_backoff ** fc)

    # Clamp to what can be allocated
    ram = min(float(pool.max_ram_pool), float(avail_ram), float(ram))
    # Avoid returning negative/NaN
    if ram <= 0:
        return 0.0
    return ram


def _predict_cpu(s, prio, pool, avail_cpu):
    target = float(s.cpu_targets.get(prio, s.min_cpu))
    cpu = min(float(avail_cpu), float(pool.max_cpu_pool), target)
    if cpu <= 0:
        return 0.0
    # If there is enough room, never go below min_cpu for a new assignment.
    if float(avail_cpu) >= float(s.min_cpu):
        cpu = max(cpu, float(s.min_cpu))
    return cpu


def _remove_from_queue(s, prio, pipeline_id):
    q = s.queues.get(prio)
    if q is None:
        return
    if pipeline_id in s.in_queue.get(prio, set()):
        s.in_queue[prio].discard(pipeline_id)
        # Remove a single instance if present
        try:
            q.remove(pipeline_id)
        except ValueError:
            pass


def _enqueue_pipeline(s, pipeline):
    prio = _normalize_priority(s, pipeline)
    pid = pipeline.pipeline_id
    if pid in s.in_queue[prio]:
        return
    s.queues[prio].append(pid)
    s.in_queue[prio].add(pid)


def _pipeline_has_runnable_op(pipeline):
    st = pipeline.runtime_status()
    if st.is_pipeline_successful():
        return False
    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    return bool(op_list)


def _any_high_waiting(s):
    # True if any runnable op exists among QUERY or INTERACTIVE pipelines.
    for prio in (Priority.QUERY, Priority.INTERACTIVE):
        for pid in list(s.queues[prio]):
            p = s.pipeline_by_id.get(pid)
            if p is None:
                continue
            if _pipeline_has_runnable_op(p):
                return True
    return False


def _find_fittable_candidate(s, prio_list, pool, avail_cpu, avail_ram, high_waiting):
    """Find the next (pipeline, op, cpu, ram) that fits in remaining resources.

    We scan queues in priority order. Within a priority, we scan in queue order and
    rotate the chosen pipeline to the back for fairness.
    """
    # If high-priority is waiting, keep headroom by limiting NEW batch assignments.
    reserve_cpu = 0.0
    reserve_ram = 0.0
    if high_waiting:
        reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_frac_when_high_waiting)
        reserve_ram = float(pool.max_ram_pool) * float(s.reserve_frac_when_high_waiting)

    for prio in prio_list:
        q = s.queues.get(prio, [])
        # iterate by index so we can pop/rotate on selection
        i = 0
        while i < len(q):
            pid = q[i]
            pipeline = s.pipeline_by_id.get(pid)
            if pipeline is None:
                # Stale entry
                q.pop(i)
                s.in_queue[prio].discard(pid)
                continue

            st = pipeline.runtime_status()
            if st.is_pipeline_successful():
                # Completed: drop from all structures
                q.pop(i)
                s.in_queue[prio].discard(pid)
                s.pipeline_by_id.pop(pid, None)
                s.enqueue_tick.pop(pid, None)
                continue

            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                i += 1
                continue

            op = op_list[0]
            opk = _op_key(op)
            if s.op_fail_count.get(opk, 0) > s.max_op_retries:
                # Too many retries: drop pipeline to avoid infinite loops.
                q.pop(i)
                s.in_queue[prio].discard(pid)
                s.pipeline_by_id.pop(pid, None)
                s.enqueue_tick.pop(pid, None)
                continue

            # Predict resources
            cpu = _predict_cpu(s, prio, pool, avail_cpu)
            ram = _predict_ram_gb(s, op, pool, avail_ram)

            # Apply batch headroom reservation if high-priority exists
            if high_waiting and prio == Priority.BATCH_PIPELINE:
                # Ensure we keep reserve_* available AFTER the batch assignment.
                cpu_cap = float(avail_cpu) - float(reserve_cpu)
                ram_cap = float(avail_ram) - float(reserve_ram)
                if cpu_cap < float(s.min_cpu) or ram_cap < float(s.min_ram_gb):
                    i += 1
                    continue
                cpu = min(float(cpu), float(cpu_cap))
                ram = min(float(ram), float(ram_cap))

            # Fit check
            if cpu <= 0 or ram <= 0:
                i += 1
                continue
            if float(cpu) > float(avail_cpu) or float(ram) > float(avail_ram):
                i += 1
                continue
            if float(avail_cpu) >= float(s.min_cpu) and float(cpu) < float(s.min_cpu):
                i += 1
                continue
            if float(avail_ram) >= float(s.min_ram_gb) and float(ram) < float(s.min_ram_gb):
                i += 1
                continue

            # Rotate fairness: move selected pipeline to back
            q.pop(i)
            q.append(pid)

            return prio, pid, pipeline, [op], float(cpu), float(ram)

    return None


@register_scheduler(key="scheduler_est_033")
def scheduler_est_033(s, results: List[ExecutionResult],
                      pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """Scheduling step: prioritize high-priority ops, pack by estimates, and adapt on failures."""
    # Add new pipelines to our state
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.pipeline_by_id:
            s.pipeline_by_id[pid] = p
            s.enqueue_tick[pid] = s.tick
            _enqueue_pipeline(s, p)

    # If nothing changed, exit early (keeps simulator fast)
    if not pipelines and not results:
        return [], []

    s.tick += 1

    # Update retry counters / sizing state from execution results
    for r in results:
        # r.ops is the list of ops executed in that container
        ops = getattr(r, "ops", None) or []
        if r.failed():
            for op in ops:
                k = _op_key(op)
                s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1
        else:
            # Gentle decay on success so we don't keep over-provisioning forever.
            for op in ops:
                k = _op_key(op)
                if k in s.op_fail_count and s.op_fail_count[k] > 0:
                    s.op_fail_count[k] -= 1

    # Cleanup: remove pipelines that may have completed since last time
    for prio in _prio_order():
        q = s.queues[prio]
        i = 0
        while i < len(q):
            pid = q[i]
            p = s.pipeline_by_id.get(pid)
            if p is None:
                q.pop(i)
                s.in_queue[prio].discard(pid)
                continue
            if p.runtime_status().is_pipeline_successful():
                q.pop(i)
                s.in_queue[prio].discard(pid)
                s.pipeline_by_id.pop(pid, None)
                s.enqueue_tick.pop(pid, None)
                continue
            i += 1

    high_waiting = _any_high_waiting(s)

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Schedule across pools: pack multiple ops per pool per tick when resources allow.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep selecting the next best fittable candidate until we can't place more.
        # Priority is global; we don't hard-partition pools, but batch gets throttled
        # when high-priority exists.
        while avail_cpu > 0 and avail_ram > 0:
            cand = _find_fittable_candidate(
                s=s,
                prio_list=_prio_order(),
                pool=pool,
                avail_cpu=avail_cpu,
                avail_ram=avail_ram,
                high_waiting=high_waiting,
            )
            if cand is None:
                break

            prio, pid, pipeline, op_list, cpu, ram = cand

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            avail_cpu -= float(cpu)
            avail_ram -= float(ram)

            # Avoid tiny negative drift
            if avail_cpu < 0:
                avail_cpu = 0.0
            if avail_ram < 0:
                avail_ram = 0.0

    return suspensions, assignments
