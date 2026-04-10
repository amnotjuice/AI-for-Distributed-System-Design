# policy_key: scheduler_est_028
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045954
# generation_seconds: 47.36
# generated_at: 2026-04-01T00:03:37.493391
@register_scheduler_init(key="scheduler_est_028")
def scheduler_est_028_init(s):
    """
    Priority-aware FIFO with conservative admission and simple OOM-aware retries.

    Improvements over naive FIFO:
      - Separate waiting queues per priority (QUERY > INTERACTIVE > BATCH).
      - Keep headroom when any high-priority work is waiting (avoid filling pool with batch).
      - Allocate RAM using op.estimate.mem_peak_gb as a floor; on failures, increase RAM floor.
      - Retry failed ops a small number of times (assumed OOM-like) with increased RAM.
      - Schedule multiple assignments per pool per tick (until headroom exhausted).
    """
    # Per-priority FIFO queues of pipelines (pipelines may appear multiple times; we de-dup per tick).
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # RAM floor learned from failures / conservative sizing.
    # Keyed by (pipeline_id, op_identity) -> ram_gb_floor
    s.op_ram_floor_gb = {}

    # Retry counters for failed ops (to avoid infinite loops on non-OOM failures).
    # Keyed by (pipeline_id, op_identity) -> retries
    s.op_retries = {}

    # Track whether a pipeline is already queued (best-effort; pipelines can still re-enter).
    s.queued_pipeline_ids = set()


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _op_identity(op):
    # We do not rely on any specific op id field being present; id(op) is stable for the object lifetime.
    return id(op)


def _enqueue_pipeline(s, p):
    if p.pipeline_id in s.queued_pipeline_ids:
        return
    s.waiting[p.priority].append(p)
    s.queued_pipeline_ids.add(p.pipeline_id)


def _pipeline_done_or_terminal_failed(p):
    status = p.runtime_status()
    if status.is_pipeline_successful():
        return True
    # We do not treat "has FAILED ops" as terminal: FAILED ops are re-assignable in this simulator.
    return False


def _remove_pipeline_from_queued_set(s, p):
    # Best-effort: when we decide a pipeline is done, allow it to be enqueued again if it reappears.
    if p.pipeline_id in s.queued_pipeline_ids:
        s.queued_pipeline_ids.remove(p.pipeline_id)


def _has_any_high_priority_waiting(s):
    return bool(s.waiting[Priority.QUERY]) or bool(s.waiting[Priority.INTERACTIVE])


def _reserved_headroom(pool, has_high_waiting):
    """
    Keep some CPU/RAM headroom if high-priority work is waiting, to reduce latency spikes.
    This is a simple admission control mechanism that avoids saturating the pool with batch.
    """
    if not has_high_waiting:
        return 0.0, 0.0

    # Reserve a fraction of pool capacity.
    # Tuned conservatively: enough to start at least one high-priority op quickly.
    reserve_cpu = max(1.0, 0.30 * float(pool.max_cpu_pool))
    reserve_ram = max(1.0, 0.30 * float(pool.max_ram_pool))
    return reserve_cpu, reserve_ram


def _pick_next_pipeline(s):
    """
    Pop one pipeline from the highest non-empty priority queue.
    Caller is responsible for requeueing it if still active.
    """
    for pr in _prio_order():
        q = s.waiting[pr]
        if q:
            p = q.pop(0)
            # Temporarily remove from queued set; if it remains active, it will be re-enqueued.
            _remove_pipeline_from_queued_set(s, p)
            return p
    return None


def _compute_ram_request_gb(s, pipeline, op, pool, last_attempt_ram_gb=None, failed_before=False):
    """
    RAM sizing:
      - Use estimator as a floor when available.
      - Use learned floor from prior failures.
      - On failure, bump aggressively (x2).
      - Allocate a bit of extra slack; extra RAM has no performance cost in this model.
    """
    op_id = _op_identity(op)
    key = (pipeline.pipeline_id, op_id)

    est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
    est_floor = float(est) if (est is not None) else 0.0

    learned_floor = float(s.op_ram_floor_gb.get(key, 0.0))

    base_floor = max(est_floor, learned_floor, 0.5)

    # Add slack (extra RAM is "free" for speed; only undersizing risks OOM).
    ram = base_floor * 1.20

    if failed_before:
        # If we know it failed before, bump harder. If we know last attempt RAM, double it.
        if last_attempt_ram_gb is not None and last_attempt_ram_gb > 0:
            ram = max(ram, float(last_attempt_ram_gb) * 2.0)
        else:
            ram = max(ram, base_floor * 2.0)

    # Clamp to pool max.
    ram = min(ram, float(pool.max_ram_pool))
    return ram


def _compute_cpu_request(s, pipeline_priority, pool, avail_cpu):
    """
    CPU sizing:
      - High priority (QUERY/INTERACTIVE): grab as much as possible to minimize latency.
      - Batch: take a moderate slice to reduce interference and preserve headroom.
    """
    avail = float(avail_cpu)
    if avail <= 0:
        return 0.0

    if pipeline_priority in (Priority.QUERY, Priority.INTERACTIVE):
        return avail  # minimize latency
    # Batch: cap to half of currently available CPU (but at least 1)
    return min(avail, max(1.0, 0.50 * avail))


@register_scheduler(key="scheduler_est_028")
def scheduler_est_028(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into per-priority queues.
      2) Process results: on failures, learn higher RAM floor and track retries.
      3) For each pool, repeatedly assign ready ops:
           - prioritize high priority queues
           - avoid using reserved headroom if high priority is waiting
           - size RAM using estimator + learned floors
    """
    # Enqueue new arrivals.
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # Learn from results (failures -> increase RAM floor; limit retries).
    for r in results:
        # We only learn about the ops that just ran.
        if not getattr(r, "ops", None):
            continue

        # Heuristic: treat any failure as potentially OOM-like for the purpose of RAM floor bumping.
        # Retry cap prevents infinite loops for non-OOM failures.
        if r.failed():
            for op in r.ops:
                op_id = _op_identity(op)
                key = (getattr(r, "pipeline_id", None), op_id) if getattr(r, "pipeline_id", None) is not None else None
                # If pipeline_id isn't available on result, fall back to best-effort using op id only.
                if key is None:
                    key = ("unknown_pipeline", op_id)

                prev_floor = float(s.op_ram_floor_gb.get(key, 0.0))
                attempt_ram = float(getattr(r, "ram", 0.0) or 0.0)

                # Increase learned floor meaningfully.
                bumped = max(prev_floor, attempt_ram * 2.0, prev_floor * 1.5, 1.0)
                s.op_ram_floor_gb[key] = bumped

                s.op_retries[key] = int(s.op_retries.get(key, 0)) + 1

    suspensions = []
    assignments = []

    has_high_waiting_global = _has_any_high_priority_waiting(s)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Compute reservable headroom if high-priority work exists.
        reserve_cpu, reserve_ram = _reserved_headroom(pool, has_high_waiting_global)

        # We'll assign multiple ops per pool until admission constraints stop us.
        # Note: pool.avail_* values reflect current free capacity before new assignments.
        # We do a local accounting to avoid over-assigning within the tick.
        local_avail_cpu = float(pool.avail_cpu_pool)
        local_avail_ram = float(pool.avail_ram_pool)

        # If there is no capacity at all, skip pool.
        if local_avail_cpu <= 0 or local_avail_ram <= 0:
            continue

        # Try to schedule while we can.
        scheduled_any = True
        safety_iters = 0
        while scheduled_any:
            safety_iters += 1
            if safety_iters > 1000:
                break  # safety: avoid infinite loops

            scheduled_any = False

            # If high priority is waiting, ensure we don't consume reserved headroom with batch.
            effective_cpu_for_batch = max(0.0, local_avail_cpu - reserve_cpu)
            effective_ram_for_batch = max(0.0, local_avail_ram - reserve_ram)

            # Pick next pipeline by priority; if it can't schedule now, we requeue and try next.
            tried = 0
            max_try = sum(len(s.waiting[p]) for p in _prio_order()) + 1
            while tried < max_try:
                tried += 1
                p = _pick_next_pipeline(s)
                if p is None:
                    break

                # Drop completed pipelines.
                if _pipeline_done_or_terminal_failed(p):
                    continue

                status = p.runtime_status()

                # Only schedule ops whose parents are complete.
                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    # Not ready yet; requeue.
                    _enqueue_pipeline(s, p)
                    continue

                op = op_list[0]
                op_id = _op_identity(op)

                # Determine retry key (pipeline+op).
                key = (p.pipeline_id, op_id)

                # If we've retried too many times, stop trying this pipeline (avoid livelock).
                # This is conservative; we simply stop requeueing it.
                if int(s.op_retries.get(key, 0)) >= 3:
                    continue

                # Compute resource requests.
                failed_before = int(s.op_retries.get(key, 0)) > 0
                ram_req = _compute_ram_request_gb(
                    s, p, op, pool, last_attempt_ram_gb=None, failed_before=failed_before
                )
                cpu_req = _compute_cpu_request(s, p.priority, pool, local_avail_cpu)

                # Admission control:
                # - High priority can use full remaining local capacity.
                # - Batch must fit within "non-reserved" headroom when high priority is waiting.
                if p.priority == Priority.BATCH_PIPELINE and has_high_waiting_global:
                    if cpu_req > effective_cpu_for_batch or ram_req > effective_ram_for_batch:
                        # Can't admit batch now; requeue and try something else.
                        _enqueue_pipeline(s, p)
                        continue

                # Must fit in local remaining capacity.
                if cpu_req <= 0 or ram_req <= 0 or cpu_req > local_avail_cpu or ram_req > local_avail_ram:
                    # Not enough capacity; requeue and stop trying further (pool is effectively full).
                    _enqueue_pipeline(s, p)
                    break

                # Create assignment.
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

                # Update local accounting.
                local_avail_cpu -= cpu_req
                local_avail_ram -= ram_req

                # Requeue pipeline if it still has work (likely true).
                _enqueue_pipeline(s, p)

                scheduled_any = True
                break  # after a successful schedule, recompute effective headroom and continue outer loop

    return suspensions, assignments
