# policy_key: scheduler_none_046
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.028965
# generation_seconds: 26.32
# generated_at: 2026-03-14T02:14:29.200850
@register_scheduler_init(key="scheduler_none_046")
def scheduler_none_046_init(s):
    """Priority-aware, conservative scheduler with simple OOM backoff.

    Goals (small, obvious improvements over naive FIFO):
      1) Reduce tail latency for high-priority pipelines by always selecting ready ops
         from higher priorities first.
      2) Avoid over-allocating whole pools to single ops by using conservative CPU/RAM
         sizing (leave headroom so more ops can run concurrently).
      3) React to OOMs by increasing RAM for the *pipeline* on subsequent attempts
         (simple backoff), reducing repeated failures and wasted time.
      4) Keep scheduling stable: no preemption (yet), and at most one new assignment
         per pool per tick to reduce thrash.
    """
    # Pipelines waiting to be scheduled (we keep them and re-check readiness each tick).
    s.waiting_queue = []

    # Per-pipeline RAM multiplier that increases after OOMs.
    # Key: pipeline_id -> multiplier (float)
    s.ram_mult = {}

    # Per-pipeline CPU preference (optional future hook); keep simple for now.
    s.cpu_mult = {}

    # Small constants for sizing.
    s._min_cpu = 0.25
    s._min_ram = 0.25  # in "pool RAM units" (whatever the simulator uses)


def _prio_rank(priority):
    # Smaller rank == higher priority.
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and anything else


def _pick_next_pipeline(waiting):
    # Choose the highest-priority pipeline; within same priority, FIFO by queue order.
    best_i = None
    best_rank = None
    for i, p in enumerate(waiting):
        r = _prio_rank(p.priority)
        if best_i is None or r < best_rank:
            best_i = i
            best_rank = r
    if best_i is None:
        return None, None
    return waiting.pop(best_i), best_rank


def _conservative_slice(priority, avail_cpu, avail_ram):
    # Conservative sizing to allow concurrency and reduce interference.
    # High priority gets a larger slice, but never the whole pool unless that's all that's left.
    if priority == Priority.QUERY:
        cpu_frac = 0.75
        ram_frac = 0.75
    elif priority == Priority.INTERACTIVE:
        cpu_frac = 0.60
        ram_frac = 0.60
    else:
        cpu_frac = 0.40
        ram_frac = 0.40

    cpu = max(0.0, avail_cpu * cpu_frac)
    ram = max(0.0, avail_ram * ram_frac)
    return cpu, ram


def _clamp(x, lo, hi):
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


@register_scheduler(key="scheduler_none_046")
def scheduler_none_046(s, results, pipelines):
    """
    Priority-aware non-preemptive scheduler with OOM-based RAM backoff.

    Strategy per tick:
      - Add arriving pipelines to a waiting queue.
      - Process execution results:
          * Drop completed pipelines naturally (by checking status each tick).
          * If a result failed with an OOM-like error, increase the pipeline's RAM multiplier.
      - For each pool:
          * Pick highest-priority pipeline from queue.
          * Pick first ready operator (parents complete) from ASSIGNABLE_STATES.
          * Assign a conservative slice of available CPU/RAM, scaled by pipeline RAM multiplier.
          * Only one assignment per pool per tick for stability.
      - Requeue pipelines we examined but couldn't schedule.
    """
    # Enqueue new pipelines
    for p in pipelines:
        s.waiting_queue.append(p)
        if p.pipeline_id not in s.ram_mult:
            s.ram_mult[p.pipeline_id] = 1.0
        if p.pipeline_id not in s.cpu_mult:
            s.cpu_mult[p.pipeline_id] = 1.0

    # Update backoff state from results
    for r in results:
        if not r.failed():
            continue
        # Heuristic: treat any error string containing "oom" or "out of memory" as RAM pressure.
        err = ""
        try:
            err = (r.error or "")
        except Exception:
            err = ""
        low = err.lower() if isinstance(err, str) else ""
        if "oom" in low or "out of memory" in low or "out-of-memory" in low:
            # Increase RAM multiplier for this pipeline.
            # This is intentionally gentle at first to avoid monopolizing resources.
            # Cap to avoid runaway.
            # Find pipeline_id from result ops if possible; otherwise skip.
            pid = None
            try:
                # ExecutionResult does not explicitly include pipeline_id per spec;
                # but ops are from a single pipeline assignment. Try to read from op.
                # If not available, we can't attribute.
                if r.ops:
                    op0 = r.ops[0]
                    pid = getattr(op0, "pipeline_id", None)
            except Exception:
                pid = None

            # Fallback: no attribution; do nothing.
            if pid is not None:
                cur = s.ram_mult.get(pid, 1.0)
                s.ram_mult[pid] = _clamp(cur * 1.5, 1.0, 8.0)

    # Early exit if nothing changed
    if not pipelines and not results and not s.waiting_queue:
        return [], []

    suspensions = []
    assignments = []

    # We'll rebuild the queue, dropping completed/failed pipelines and preserving order.
    survivors = []
    s.waiting_queue, to_process = [], s.waiting_queue

    # Filter out finished pipelines up-front
    for p in to_process:
        try:
            status = p.runtime_status()
        except Exception:
            survivors.append(p)
            continue

        has_failures = status.state_counts[OperatorState.FAILED] > 0
        if status.is_pipeline_successful() or has_failures:
            # Drop successful pipelines; also drop pipelines with failures (no retries beyond OOM backoff here).
            # Note: failed operators can be in ASSIGNABLE_STATES depending on simulator, but the example drops them.
            continue
        survivors.append(p)

    # Put survivors back as our working queue
    s.waiting_queue = survivors

    # Schedule: one assignment per pool (keeps policy simple/stable)
    # We pick the best pipeline for each pool independently but remove it from queue
    # only temporarily and requeue if we couldn't schedule an op.
    requeue = []

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try a few pipelines to find one with a ready op, favoring high priority.
        attempts = min(len(s.waiting_queue), 16)  # bound work per tick
        scheduled = False
        examined = []

        for _ in range(attempts):
            pipeline, _rank = _pick_next_pipeline(s.waiting_queue)
            if pipeline is None:
                break
            examined.append(pipeline)

            status = pipeline.runtime_status()

            # Select first ready operator. Keep it simple: schedule one op at a time.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                continue

            # Conservative slicing + per-pipeline RAM multiplier after OOM.
            base_cpu, base_ram = _conservative_slice(pipeline.priority, avail_cpu, avail_ram)

            # Avoid zero allocations; clamp to available.
            cpu = _clamp(base_cpu * s.cpu_mult.get(pipeline.pipeline_id, 1.0), s._min_cpu, avail_cpu)
            ram = _clamp(base_ram * s.ram_mult.get(pipeline.pipeline_id, 1.0), s._min_ram, avail_ram)

            # If we don't have enough resources to give even minimums, skip this pool.
            if cpu <= 0 or ram <= 0:
                continue

            assignment = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)
            scheduled = True

            # Put pipeline back for further ops in later ticks
            requeue.append(pipeline)
            break

        # Requeue examined pipelines that weren't scheduled this pool tick
        if examined:
            # If we scheduled one, it has already been requeued; requeue the rest.
            if scheduled:
                for p in examined:
                    if p is not requeue[-1]:
                        requeue.append(p)
            else:
                requeue.extend(examined)

    # Append any pipelines not examined (still in queue) to requeue list.
    requeue.extend(s.waiting_queue)

    # Restore queue preserving relative order in requeue
    s.waiting_queue = requeue

    return suspensions, assignments
