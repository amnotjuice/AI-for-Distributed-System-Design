# policy_key: scheduler_est_017
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.130808
# generation_seconds: 191.54
# generated_at: 2026-03-31T18:54:25.166398
@register_scheduler_init(key="scheduler_est_017")
def scheduler_est_017_init(s):
    """
    Priority-aware, estimate-driven, small-step improvement over naive FIFO.

    Improvements vs naive:
      1) Priority queues (QUERY > INTERACTIVE > BATCH) to reduce tail latency for high-priority work.
      2) Right-size RAM using op.estimate.mem_peak_gb (with a safety factor) instead of grabbing the whole pool.
      3) Pack multiple ops per pool per tick when possible (better utilization without hurting latency).
      4) Keep headroom when high-priority work is waiting so batch doesn't monopolize the pool.
      5) Simple OOM retry: on OOM, increase the per-op RAM safety multiplier and re-run.
    """
    from collections import deque

    # Per-priority FIFO queues of pipelines.
    s.waiting = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Track which pipelines are currently queued (avoid duplicates).
    s.queued_pipeline_ids = set()

    # Learned per-operator memory safety multiplier and learned minimum RAM floor.
    s.op_ram_mult = {}       # op_uid -> float multiplier (>= 1.0)
    s.op_min_ram_gb = {}     # op_uid -> float minimum RAM floor learned from past attempts

    # Track last failure type per operator to decide whether FAILED ops are retryable.
    s.op_last_fail_was_oom = {}  # op_uid -> bool
    s.op_hard_failed = set()     # op_uid that failed with non-OOM error (do not retry)

    # Scheduling knobs (small, conservative defaults).
    s.scan_limit = 32  # max pipelines to rotate through per pick (avoid head-of-line blocking)
    s.reserve_cpu_abs = 1.0
    s.reserve_ram_abs = 1.0
    s.reserve_cpu_frac = 0.15
    s.reserve_ram_frac = 0.15

    # Simple tick counter (for potential future refinements).
    s.tick = 0


def _est_mem_gb(op) -> float:
    """Best-effort extraction of estimated peak memory in GB."""
    est = getattr(op, "estimate", None)
    if est is None:
        return 1.0
    v = getattr(est, "mem_peak_gb", None)
    try:
        if v is None:
            return 1.0
        v = float(v)
        return max(0.0, v)
    except Exception:
        return 1.0


def _op_uid(op):
    """
    Stable-ish operator identifier:
    prefer explicit IDs if present; fallback to object identity.
    """
    for attr in ("op_id", "operator_id", "id", "uid", "name"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            if v is not None:
                return (attr, v)
    return ("py_id", id(op))


def _is_oom_error(err) -> bool:
    """Heuristic OOM classifier from error string."""
    if err is None:
        return False
    try:
        s = str(err).lower()
    except Exception:
        return False
    needles = ("oom", "out of memory", "out-of-memory", "memoryerror", "cuda out of memory")
    return any(n in s for n in needles)


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _target_cpu_for(priority, pool_max_cpu: float) -> float:
    """
    Conservative CPU right-sizing:
      - QUERY: give more CPU to reduce latency (up to 4 or 50% of pool)
      - INTERACTIVE: moderate (up to 2 or 25% of pool)
      - BATCH: small quantum (1) to avoid monopolizing the pool
    """
    if pool_max_cpu <= 0:
        return 0.0

    if priority == Priority.QUERY:
        return max(1.0, min(4.0, 0.50 * pool_max_cpu))
    if priority == Priority.INTERACTIVE:
        return max(1.0, min(2.0, 0.25 * pool_max_cpu))
    return 1.0  # batch


def _required_ram_gb(s, op, priority, pool_max_ram: float) -> float:
    """
    RAM sizing from estimate with learned multiplier + learned minimum floor.
    Add small overhead to reduce boundary OOMs.
    """
    uid = _op_uid(op)
    est = _est_mem_gb(op)
    mult = float(s.op_ram_mult.get(uid, 1.15))  # small default safety margin
    learned_floor = float(s.op_min_ram_gb.get(uid, 0.0))

    # Extra small bias for interactive/query to reduce probability of OOM retries.
    prio_bias = 1.05 if priority in (Priority.QUERY, Priority.INTERACTIVE) else 1.00

    base = (est * mult * prio_bias) + 0.25
    base = max(base, learned_floor, 0.25)

    # Never request more than pool maximum; if still too high to fit avail, we'll just skip for now.
    if pool_max_ram > 0:
        base = min(base, float(pool_max_ram))
    return base


def _pipeline_is_hard_failed(s, pipeline) -> bool:
    """
    If a pipeline has FAILED ops, only allow retry if every FAILED op is known-OOM.
    Otherwise treat as hard failure and drop it to avoid infinite retries.
    """
    st = pipeline.runtime_status()
    failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False)
    if not failed_ops:
        return False

    # Retry only if all failures were classified as OOM for those ops.
    for op in failed_ops:
        uid = _op_uid(op)
        if uid in s.op_hard_failed:
            return True
        if s.op_last_fail_was_oom.get(uid, None) is not True:
            return True
    return False


@register_scheduler(key="scheduler_est_017")
def scheduler_est_017_scheduler(s, results: List["ExecutionResult"],
                                pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Priority-first, estimate-sized, packing scheduler with simple OOM learning.

    Core loop:
      - Ingest new pipelines into per-priority FIFO queues.
      - Update OOM learning from execution results.
      - For each pool, repeatedly pick the best-fitting next op:
          QUERY -> INTERACTIVE -> BATCH,
        rotating within each queue to avoid head-of-line blocking.
      - Keep headroom for high priority when it is waiting (batch admissions become conservative).
    """
    s.tick += 1

    # Learn from execution results (OOM vs hard failure).
    for r in results:
        if not r.failed():
            # Optional mild decay towards estimate if we are overly conservative.
            try:
                for op in (r.ops or []):
                    uid = _op_uid(op)
                    m = float(s.op_ram_mult.get(uid, 1.15))
                    if m > 1.15:
                        s.op_ram_mult[uid] = max(1.15, m * 0.95)
            except Exception:
                pass
            continue

        is_oom = _is_oom_error(getattr(r, "error", None))
        try:
            ops = r.ops or []
        except Exception:
            ops = []

        for op in ops:
            uid = _op_uid(op)
            s.op_last_fail_was_oom[uid] = bool(is_oom)

            if is_oom:
                # Increase safety factor and learn a minimum RAM floor based on what we tried.
                prev = float(s.op_ram_mult.get(uid, 1.15))
                s.op_ram_mult[uid] = min(prev * 1.5, 8.0)

                tried_ram = getattr(r, "ram", None)
                try:
                    tried_ram = float(tried_ram) if tried_ram is not None else 0.0
                except Exception:
                    tried_ram = 0.0

                est = _est_mem_gb(op)
                new_floor = max(tried_ram * 1.2, est * 1.2, 0.25)
                s.op_min_ram_gb[uid] = max(float(s.op_min_ram_gb.get(uid, 0.0)), new_floor)
            else:
                # Non-OOM failures: do not retry indefinitely.
                s.op_hard_failed.add(uid)

    # Enqueue new pipelines (avoid duplicates).
    for p in pipelines:
        if p.pipeline_id in s.queued_pipeline_ids:
            continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        s.waiting[p.priority].append(p)
        s.queued_pipeline_ids.add(p.pipeline_id)

    # If nothing changed, avoid extra work.
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Track pipelines we already scheduled in this scheduler tick (avoid multi-pool double-scheduling).
    scheduled_this_tick = set()

    # Compute whether high-priority work is waiting (for batch headroom protection).
    high_waiting = (len(s.waiting[Priority.QUERY]) > 0) or (len(s.waiting[Priority.INTERACTIVE]) > 0)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Headroom reservation when high-priority work is waiting.
        reserve_cpu = max(float(s.reserve_cpu_abs), float(pool.max_cpu_pool) * float(s.reserve_cpu_frac))
        reserve_ram = max(float(s.reserve_ram_abs), float(pool.max_ram_pool) * float(s.reserve_ram_frac))

        # Keep packing while there is room to schedule more.
        while avail_cpu > 0 and avail_ram > 0:
            picked = None  # (pipeline, op, cpu, ram, priority)

            for prio in _priority_order():
                q = s.waiting[prio]
                if not q:
                    continue

                # Rotate through up to scan_limit pipelines to find a runnable op that fits.
                scans = min(len(q), int(getattr(s, "scan_limit", 32)))
                for _ in range(scans):
                    p = q.popleft()

                    # Drop already finished pipelines.
                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        s.queued_pipeline_ids.discard(p.pipeline_id)
                        continue

                    # Drop hard-failed pipelines (non-OOM errors).
                    if _pipeline_is_hard_failed(s, p):
                        s.queued_pipeline_ids.discard(p.pipeline_id)
                        continue

                    # Avoid scheduling the same pipeline multiple times in one tick.
                    if p.pipeline_id in scheduled_this_tick:
                        q.append(p)
                        continue

                    # Find next runnable op (only one at a time per pipeline).
                    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        q.append(p)
                        continue

                    op = op_list[0]

                    # Decide resources.
                    cpu_req = _target_cpu_for(prio, float(pool.max_cpu_pool))
                    if cpu_req <= 0 or avail_cpu < 1e-9:
                        q.append(p)
                        continue
                    cpu_req = min(cpu_req, avail_cpu)

                    ram_req = _required_ram_gb(s, op, prio, float(pool.max_ram_pool))
                    if ram_req <= 0:
                        q.append(p)
                        continue

                    # Must fit in currently available resources.
                    if ram_req > avail_ram or cpu_req > avail_cpu:
                        # Put it back; try a different pipeline/op in same priority class.
                        q.append(p)
                        continue

                    # If high priority is waiting, do not let batch consume the last headroom.
                    if prio == Priority.BATCH_PIPELINE and high_waiting:
                        if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                            q.append(p)
                            continue

                    # Pick this pipeline/op.
                    picked = (p, op, cpu_req, ram_req, prio)
                    break

                if picked is not None:
                    break

            if picked is None:
                break

            p, op, cpu_req, ram_req, prio = picked

            # Emit assignment (single-op assignment).
            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Update local available resources (packing).
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)

            # Put the pipeline back into its queue for subsequent ops (unless it completes later).
            s.waiting[prio].append(p)
            scheduled_this_tick.add(p.pipeline_id)

            # If batch and high-priority waiting, stop early once we've preserved headroom.
            if high_waiting and avail_cpu <= reserve_cpu and avail_ram <= reserve_ram:
                break

    return suspensions, assignments
