# policy_key: scheduler_est_020
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043894
# generation_seconds: 36.86
# generated_at: 2026-04-01T13:50:22.081676
@register_scheduler_init(key="scheduler_est_020")
def scheduler_est_020_init(s):
    """Priority-aware, latency-focused scheduler with conservative RAM sizing.

    Incremental improvements over naive FIFO:
      1) Maintain per-priority queues and always dispatch higher priority first.
      2) Allow multiple assignments per pool per tick (avoid head-of-line blocking).
      3) Size RAM conservatively using op.estimate.mem_peak_gb when available.
      4) On failures, retry with increased RAM (simple backoff) rather than dropping the whole pipeline.

    Notes:
      - This policy avoids preemption because the minimal public interface shown does not expose
        currently-running containers to safely suspend without guesswork.
      - CPU is allocated to favor latency for high-priority work while still letting background progress.
    """
    # Waiting pipelines by priority (higher first): QUERY > INTERACTIVE > BATCH_PIPELINE
    s.waiting_by_pri = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator retry RAM multiplier (grows after failures)
    # key: (pipeline_id, op_key) -> float multiplier
    s.op_ram_mult = {}

    # Soft caps to prevent one op from monopolizing a pool (improves tail latency under contention)
    # (These are "targets"; if the pool has plenty of free CPU, ops may get more.)
    s.cpu_target_by_pri = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 8.0,
    }

    # Minimum RAM allocations to reduce obvious OOMs for tiny estimates/unknowns (GB)
    s.min_ram_gb_by_pri = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 2.0,
    }

    # When estimate exists, apply headroom; when not, use a conservative fraction of pool
    s.est_headroom_mult = 1.25
    s.unknown_est_pool_frac_by_pri = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.35,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Avoid creating ultra-tiny CPU slices that can drag latency; also avoid extreme fragmentation
    s.min_cpu_slice = 1.0


def _pri_rank(priority):
    # Higher number => higher scheduling preference
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1


def _op_stable_key(op):
    # Try common ids first; fall back to Python object identity
    return getattr(op, "operator_id", getattr(op, "op_id", id(op)))


def _get_mem_est_gb(op):
    est_obj = getattr(op, "estimate", None)
    if est_obj is None:
        return None
    v = getattr(est_obj, "mem_peak_gb", None)
    try:
        if v is None:
            return None
        v = float(v)
        if v <= 0:
            return None
        return v
    except Exception:
        return None


def _pick_next_pipeline(s):
    # Strict priority: always drain QUERY first, then INTERACTIVE, then BATCH
    for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        q = s.waiting_by_pri.get(pri, [])
        while q:
            p = q.pop(0)
            status = p.runtime_status()
            # Drop completed pipelines immediately
            if status.is_pipeline_successful():
                continue
            return p
    return None


def _requeue_pipeline(s, pipeline):
    s.waiting_by_pri[pipeline.priority].append(pipeline)


@register_scheduler(key="scheduler_est_020")
def scheduler_est_020_scheduler(s, results, pipelines):
    """
    Scheduler loop:
      - Add arriving pipelines to per-priority queues.
      - Process results to update simple per-operator RAM backoff on failures.
      - For each pool, dispatch as many ready operators as resources allow, picking highest priority first.
    """
    # Enqueue new pipelines by priority
    for p in pipelines:
        s.waiting_by_pri[p.priority].append(p)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    # Update RAM multipliers based on failures (treat any failure as "needs more RAM" retry signal)
    # This is intentionally simple and conservative.
    for r in results:
        try:
            if r.failed():
                for op in getattr(r, "ops", []) or []:
                    opk = _op_stable_key(op)
                    key = (getattr(r, "pipeline_id", None), opk)
                    # If pipeline_id isn't present on result, fall back to per-op global keying
                    if key[0] is None:
                        key = ("_no_pid_", opk)
                    prev = s.op_ram_mult.get(key, 1.0)
                    # Backoff: grow multiplier, capped to avoid runaway
                    s.op_ram_mult[key] = min(prev * 1.6, 8.0)
        except Exception:
            # Never let result parsing break scheduling
            continue

    suspensions = []
    assignments = []

    # Scheduling: per pool, place multiple assignments while resources remain
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep a local list of pipelines we pulled but couldn't schedule this tick
        pulled = []

        # Try to fill the pool this tick
        # Stop if we can't allocate at least a minimum CPU+RAM slice to anything.
        while avail_cpu >= s.min_cpu_slice and avail_ram > 0:
            pipeline = _pick_next_pipeline(s)
            if pipeline is None:
                break

            status = pipeline.runtime_status()

            # Find next ready op (parents complete) in assignable states
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable yet; keep it around for later
                pulled.append(pipeline)
                continue

            op = op_list[0]

            # Determine RAM request (GB)
            est_gb = _get_mem_est_gb(op)
            opk = _op_stable_key(op)
            key = (pipeline.pipeline_id, opk)
            ram_mult = s.op_ram_mult.get(key, 1.0)

            min_ram = float(s.min_ram_gb_by_pri.get(pipeline.priority, 1.0))
            if est_gb is not None:
                ram_req = max(min_ram, est_gb * s.est_headroom_mult) * ram_mult
            else:
                # No estimate: allocate a conservative fraction of remaining pool RAM
                frac = float(s.unknown_est_pool_frac_by_pri.get(pipeline.priority, 0.4))
                ram_req = max(min_ram, avail_ram * frac) * ram_mult

            # Clamp RAM to available
            ram_req = min(ram_req, avail_ram)

            # Determine CPU request
            cpu_target = float(s.cpu_target_by_pri.get(pipeline.priority, 2.0))
            # If pool is lightly loaded, allow larger CPU to speed single op
            cpu_req = min(avail_cpu, max(s.min_cpu_slice, cpu_target))

            # If we have too little RAM to make a meaningful allocation, stop trying in this pool.
            # (Avoid spinning across pipelines without making progress.)
            if ram_req <= 0:
                pulled.append(pipeline)
                break

            # Create the assignment
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update local available resources and requeue the pipeline to allow next ops later
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            pulled.append(pipeline)

        # Put back any pipelines we pulled (including ones we scheduled an op for)
        for p in pulled:
            # Drop completed pipelines quickly
            try:
                if p.runtime_status().is_pipeline_successful():
                    continue
            except Exception:
                pass
            _requeue_pipeline(s, p)

    return suspensions, assignments
