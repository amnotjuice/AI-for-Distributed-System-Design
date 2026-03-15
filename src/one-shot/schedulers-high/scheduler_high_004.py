# policy_key: scheduler_high_004
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.112055
# generation_seconds: 206.23
# generated_at: 2026-03-12T22:31:40.091404
@register_scheduler_init(key="scheduler_high_004")
def scheduler_high_004_init(s):
    """
    Priority-aware, OOM-adaptive scheduler (incremental improvement over naive FIFO).

    Key improvements:
      1) Separate per-priority queues; always try to schedule higher priority first.
      2) Avoid head-of-line blocking by scanning within each priority queue for runnable ops.
      3) Right-size per-assignment CPU/RAM by priority (instead of "take everything").
      4) On OOM failures, remember per-operator RAM hints and retry with more RAM.
      5) Reserve some headroom for high-priority work so batch doesn't consume everything.
    """
    from collections import deque, defaultdict

    # Queues per priority (round-robin within each queue)
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.queue_members = set()  # pipeline_id currently enqueued (best-effort; cleaned lazily)

    # Simple clock for aging / promotion
    s.tick = 0
    s.enq_tick = {}  # pipeline_id -> tick enqueued

    # Per-operator adaptive hints keyed by object identity (assumes op objects persist in pipeline DAG)
    s.op_ram_hint = {}  # op_key -> hinted RAM to avoid repeated OOM
    s.op_fail_cnt = defaultdict(int)  # op_key -> number of failures observed
    s.op_non_oom_fail = set()  # op_key that failed with non-OOM error (do not retry forever)

    # Tuning knobs (kept modest to stay robust)
    s.max_assignments_per_pool = 8
    s.max_scan_per_priority = 64  # prevent O(n^2) scanning if queues are huge

    # Priority-based sizing (fractions of pool capacity)
    s.cpu_share = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.30,
    }
    s.ram_share = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Minimum slice to avoid too-tiny allocations that are likely to thrash
    s.min_cpu_frac = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.15,
        Priority.BATCH_PIPELINE: 0.10,
    }
    s.min_ram_frac = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.15,
        Priority.BATCH_PIPELINE: 0.10,
    }

    # Reserve headroom for high priority work (only applied when HP work is waiting)
    s.hp_reserve_cpu_frac = 0.25
    s.hp_reserve_ram_frac = 0.25

    # OOM reaction: multiplicative backoff with retry cap
    s.oom_multiplier = 1.8
    s.max_oom_retries = 4

    # Starvation protection: after waiting long enough, batch is treated like interactive for admission
    s.batch_promote_after = 200  # ticks


def _scheduler_high_004_is_oom_error(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)


def _scheduler_high_004_op_key(op):
    # Use object identity for safety; avoids assuming attribute names on the operator object.
    return id(op)


def _scheduler_high_004_should_drop_pipeline(s, pipeline):
    """Drop if completed or if it has definitive (non-OOM) failures / too many OOM retries."""
    status = pipeline.runtime_status()

    if status.is_pipeline_successful():
        return True

    # If any failed op is known to be non-OOM, or exceeded OOM retry cap, drop the pipeline.
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    for op in failed_ops:
        k = _scheduler_high_004_op_key(op)
        if k in s.op_non_oom_fail:
            return True
        # If we have seen repeated OOM failures for this op, stop retrying forever.
        if s.op_fail_cnt.get(k, 0) > s.max_oom_retries:
            return True

    return False


def _scheduler_high_004_effective_priority_and_age(s, pipeline):
    """Apply aging/promotion for long-waiting batch pipelines."""
    pr = pipeline.priority
    age = s.tick - s.enq_tick.get(pipeline.pipeline_id, s.tick)

    if pr == Priority.BATCH_PIPELINE and age >= s.batch_promote_after:
        # Promote batch to interactive-like treatment (still reported as batch in Assignment priority).
        return Priority.INTERACTIVE, age
    return pr, age


def _scheduler_high_004_compute_request(s, pool, pipeline, op):
    """
    Compute (cpu_req, ram_req, cpu_min, ram_min) using:
      - priority-based baseline sizing
      - per-op OOM RAM hint (hard minimum)
    """
    pr_eff, _age = _scheduler_high_004_effective_priority_and_age(s, pipeline)

    max_cpu = int(pool.max_cpu_pool)
    max_ram = int(pool.max_ram_pool)

    # Baseline requests
    cpu_req = max(1, int(max_cpu * s.cpu_share.get(pr_eff, 0.30)))
    ram_req = max(1, int(max_ram * s.ram_share.get(pr_eff, 0.25)))

    # Minimum slice
    cpu_min = max(1, int(max_cpu * s.min_cpu_frac.get(pr_eff, 0.10)))
    ram_min = max(1, int(max_ram * s.min_ram_frac.get(pr_eff, 0.10)))

    # If we have an OOM-derived hint, make it a hard minimum (up to pool capacity).
    k = _scheduler_high_004_op_key(op)
    hinted_ram = int(s.op_ram_hint.get(k, 0))
    if hinted_ram > 0:
        ram_req = max(ram_req, hinted_ram)
        ram_min = max(ram_min, hinted_ram)

    # Clamp to pool capacity
    cpu_req = min(cpu_req, max_cpu)
    ram_req = min(ram_req, max_ram)
    cpu_min = min(cpu_min, max_cpu)
    ram_min = min(ram_min, max_ram)

    return cpu_req, ram_req, cpu_min, ram_min


@register_scheduler(key="scheduler_high_004")
def scheduler_high_004(s, results, pipelines):
    """
    Scheduler step:
      - ingest new pipelines into per-priority queues
      - update OOM hints from results
      - schedule multiple assignments per pool, prioritizing QUERY > INTERACTIVE > BATCH
      - apply headroom reservation for high-priority work
    """
    s.tick += 1

    # ---- Ingest new pipelines ----
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.queue_members:
            continue
        s.queue_members.add(pid)
        s.enq_tick[pid] = s.tick
        s.queues[p.priority].append(p)

    # ---- Consume results: learn from failures (especially OOM) ----
    for r in results:
        if not r.failed():
            continue

        # Update per-op failure tracking; OOM triggers RAM backoff, non-OOM prevents endless retry.
        is_oom = _scheduler_high_004_is_oom_error(r.error)
        for op in (r.ops or []):
            k = _scheduler_high_004_op_key(op)
            s.op_fail_cnt[k] += 1
            if is_oom:
                # Increase hinted RAM above what we tried; next retry uses this as a minimum.
                tried_ram = int(getattr(r, "ram", 0) or 0)
                if tried_ram > 0:
                    bumped = max(tried_ram + 1, int(tried_ram * s.oom_multiplier))
                    prev = int(s.op_ram_hint.get(k, 0) or 0)
                    if bumped > prev:
                        s.op_ram_hint[k] = bumped
            else:
                s.op_non_oom_fail.add(k)

    # Early exit if nothing changed (still allow periodic cleanup if needed)
    if not pipelines and not results:
        return [], []

    suspensions = []  # This policy does not preempt yet (kept simple and robust).
    assignments = []

    # Determine whether high-priority work is waiting (best-effort: might include some completed pipelines until popped)
    hp_waiting = (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    assigned_this_tick = set()  # pipeline_id -> avoid scheduling multiple ops from same pipeline in a single tick

    # ---- Scheduling loop over pools ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = int(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        scheduled_here = 0

        # Try to schedule multiple containers per pool, slicing resources.
        while scheduled_here < s.max_assignments_per_pool and avail_cpu > 0 and avail_ram > 0:
            made_progress = False

            # Always consider higher priorities first for latency.
            for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                q = s.queues[pr]
                if not q:
                    continue

                # Limit scanning to keep runtime bounded.
                scan = min(len(q), s.max_scan_per_priority)

                for _ in range(scan):
                    pipeline = q.popleft()

                    # Lazy cleanup: drop completed/failed pipelines when we encounter them.
                    if _scheduler_high_004_should_drop_pipeline(s, pipeline):
                        pid = pipeline.pipeline_id
                        s.queue_members.discard(pid)
                        s.enq_tick.pop(pid, None)
                        continue

                    # Keep round-robin order stable.
                    q.append(pipeline)

                    # Avoid multiple ops from the same pipeline in one tick (reduces burstiness).
                    pid = pipeline.pipeline_id
                    if pid in assigned_this_tick:
                        continue

                    status = pipeline.runtime_status()
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        continue
                    op = op_list[0]

                    cpu_req, ram_req, cpu_min, ram_min = _scheduler_high_004_compute_request(
                        s, pool, pipeline, op
                    )

                    # If high-priority work is waiting, reserve some headroom from batch.
                    eff_avail_cpu = avail_cpu
                    eff_avail_ram = avail_ram
                    if pr == Priority.BATCH_PIPELINE and hp_waiting:
                        cpu_res = int(pool.max_cpu_pool * s.hp_reserve_cpu_frac)
                        ram_res = int(pool.max_ram_pool * s.hp_reserve_ram_frac)
                        eff_avail_cpu = max(0, avail_cpu - cpu_res)
                        eff_avail_ram = max(0, avail_ram - ram_res)

                    # Admission check: ensure we can at least satisfy minimums; otherwise try other pipelines.
                    if eff_avail_cpu < cpu_min or eff_avail_ram < ram_min:
                        continue

                    # For latency, prefer requested sizing; but never exceed effective availability.
                    cpu = min(cpu_req, eff_avail_cpu)
                    ram = min(ram_req, eff_avail_ram)

                    if cpu <= 0 or ram <= 0:
                        continue

                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu,
                            ram=ram,
                            priority=pipeline.priority,  # preserve true priority for metrics/accounting
                            pool_id=pool_id,
                            pipeline_id=pid,
                        )
                    )

                    # Update local pool availability and tick-level bookkeeping.
                    avail_cpu -= cpu
                    avail_ram -= ram
                    assigned_this_tick.add(pid)

                    scheduled_here += 1
                    made_progress = True
                    break  # stop scanning this priority; re-evaluate priorities with updated avail

                if made_progress:
                    break  # back to while-loop to schedule next container in this pool

            if not made_progress:
                break  # nothing feasible to schedule in this pool right now

    return suspensions, assignments
