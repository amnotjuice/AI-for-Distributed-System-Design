# policy_key: scheduler_low_039
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052693
# generation_seconds: 55.96
# generated_at: 2026-04-09T21:33:54.321048
@register_scheduler_init(key="scheduler_low_039")
def scheduler_low_039_init(s):
    """Priority-aware, failure-averse scheduler.

    Main ideas:
      - Strict priority ordering (QUERY > INTERACTIVE > BATCH) with mild aging so batch isn't starved.
      - Conservative first-try sizing to allow some concurrency, but rapid RAM backoff on failures to avoid repeated OOMs.
      - Retry failed operators (do NOT drop pipelines on first failure); increase RAM aggressively on any failure.
      - If multiple pools exist, prefer keeping high-priority work on pool 0 (soft affinity), while batch prefers other pools.
    """
    # Queues store pipeline_ids; pipeline objects are kept in a map so we always operate on latest runtime_status().
    s._pipelines_by_id = {}  # pipeline_id -> Pipeline
    s._q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Logical time for aging (we don't assume access to wall-clock time in the simulator).
    s._step = 0
    s._enqueued_at = {}  # pipeline_id -> step

    # Per-(pipeline, op) sizing hints inferred from previous attempts/results.
    # Keys use id(op) because operator objects are stable within a Pipeline instance in typical simulators.
    s._op_attempts = {}   # (pipeline_id, op_key) -> int
    s._op_ram_hint = {}   # (pipeline_id, op_key) -> float (absolute RAM to request next time)
    s._op_cpu_hint = {}   # (pipeline_id, op_key) -> float (absolute CPU to request next time)

    # Tuning knobs
    s._aging_threshold_steps = 50   # after this, batch gets treated as interactive for one pick
    s._max_attempts_before_max = 3  # after this many failures, jump to near-max sizing


def _priority_weight(priority):
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1


def _default_target_fractions(priority):
    # Fractions of a pool's *max* resources to request for a first attempt (if no hints).
    # We keep them moderate to reduce head-of-line blocking, but still favor high priority.
    if priority == Priority.QUERY:
        return 0.60, 0.55  # cpu_frac, ram_frac
    if priority == Priority.INTERACTIVE:
        return 0.50, 0.45
    return 0.45, 0.35


def _op_key(op):
    # Best-effort stable key for operator across retries.
    # id(op) works if the operator object is stable within a pipeline instance.
    return id(op)


def _pick_pool_ids_for_priority(num_pools, priority):
    # Soft pool affinity: keep high-priority on pool 0 when available; batch prefers other pools.
    if num_pools <= 1:
        return [0]
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        return [0] + [i for i in range(1, num_pools)]
    # batch: prefer non-0 pools first
    return [i for i in range(1, num_pools)] + [0]


def _get_assignable_op(pipeline):
    status = pipeline.runtime_status()
    # Only schedule ops whose parents are complete, and are in assignable states.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[0]


def _pipeline_done_or_terminal(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # Do not treat failures as terminal: we try to recover by retrying FAILED ops.
    return False


def _clean_queue(s, prio):
    # Remove pipeline_ids that are done or no longer tracked.
    newq = []
    for pid in s._q[prio]:
        p = s._pipelines_by_id.get(pid)
        if p is None:
            continue
        if _pipeline_done_or_terminal(p):
            continue
        newq.append(pid)
    s._q[prio] = newq


def _pick_next_pipeline_id(s, pool_id):
    # Priority order with mild aging:
    # - Always try QUERY, then INTERACTIVE.
    # - For batch, if it's been waiting long, temporarily treat it as INTERACTIVE for one pick.
    s._clean_queue(s, Priority.QUERY)
    s._clean_queue(s, Priority.INTERACTIVE)
    s._clean_queue(s, Priority.BATCH_PIPELINE)

    # Try QUERY first
    if s._q[Priority.QUERY]:
        pid = s._q[Priority.QUERY].pop(0)
        s._q[Priority.QUERY].append(pid)  # round-robin
        return pid

    # Then INTERACTIVE
    if s._q[Priority.INTERACTIVE]:
        pid = s._q[Priority.INTERACTIVE].pop(0)
        s._q[Priority.INTERACTIVE].append(pid)
        return pid

    # Aging for batch: if oldest batch has waited long, take it now
    if s._q[Priority.BATCH_PIPELINE]:
        # Find the oldest batch pipeline
        oldest_idx = None
        oldest_step = None
        for i, pid in enumerate(s._q[Priority.BATCH_PIPELINE]):
            enq = s._enqueued_at.get(pid, s._step)
            if oldest_step is None or enq < oldest_step:
                oldest_step = enq
                oldest_idx = i

        if oldest_idx is not None:
            pid = s._q[Priority.BATCH_PIPELINE][oldest_idx]
            waited = s._step - s._enqueued_at.get(pid, s._step)
            if waited >= s._aging_threshold_steps:
                # Remove and then append to preserve RR but still pick it now.
                s._q[Priority.BATCH_PIPELINE].pop(oldest_idx)
                s._q[Priority.BATCH_PIPELINE].append(pid)
                return pid

        # Normal batch RR
        pid = s._q[Priority.BATCH_PIPELINE].pop(0)
        s._q[Priority.BATCH_PIPELINE].append(pid)
        return pid

    return None


def _size_for_op(s, pipeline, op, pool):
    pid = pipeline.pipeline_id
    prio = pipeline.priority
    opk = _op_key(op)

    attempts = s._op_attempts.get((pid, opk), 0)
    cpu_hint = s._op_cpu_hint.get((pid, opk))
    ram_hint = s._op_ram_hint.get((pid, opk))

    # Default sizing based on pool maxima (keeps behavior stable even when avail fluctuates).
    cpu_frac, ram_frac = _default_target_fractions(prio)
    cpu = cpu_hint if cpu_hint is not None else max(1.0, pool.max_cpu_pool * cpu_frac)
    ram = ram_hint if ram_hint is not None else max(1.0, pool.max_ram_pool * ram_frac)

    # If we've failed multiple times, escalate closer to max to avoid repeated 720s penalties.
    if attempts >= s._max_attempts_before_max:
        cpu = max(cpu, pool.max_cpu_pool * 0.90)
        ram = max(ram, pool.max_ram_pool * 0.90)

    # Clamp to pool maxima.
    cpu = min(cpu, pool.max_cpu_pool)
    ram = min(ram, pool.max_ram_pool)

    return cpu, ram


def _update_hints_from_result(s, r, pool_max_cpu, pool_max_ram):
    # Update per-op hints. We treat any failure as likely resource-related and increase RAM first.
    if not getattr(r, "ops", None):
        return

    pid = getattr(r, "pipeline_id", None)
    # Some simulators do not include pipeline_id on the result; fall back to None-safe behavior.
    # We can still key by (unknown pid, opk) but that's unsafe across pipelines. So only update if known.
    if pid is None:
        return

    failed = r.failed()
    for op in r.ops:
        opk = _op_key(op)
        key = (pid, opk)
        prev_attempts = s._op_attempts.get(key, 0)

        if failed:
            s._op_attempts[key] = prev_attempts + 1

            # Aggressive RAM backoff to avoid repeated OOM.
            # If we know the last RAM, double it; otherwise jump to a moderate fraction.
            last_ram = getattr(r, "ram", None)
            if last_ram is None:
                new_ram = pool_max_ram * 0.60
            else:
                new_ram = max(last_ram * 2.0, pool_max_ram * 0.40)

            # Also increase CPU modestly on repeated failures (might reduce wall time if not memory-bound).
            last_cpu = getattr(r, "cpu", None)
            if last_cpu is None:
                new_cpu = pool_max_cpu * 0.60
            else:
                new_cpu = max(last_cpu * 1.25, pool_max_cpu * 0.50)

            s._op_ram_hint[key] = min(new_ram, pool_max_ram)
            s._op_cpu_hint[key] = min(new_cpu, pool_max_cpu)
        else:
            # On success, remember a slightly reduced RAM/CPU as a future hint to improve packing.
            last_ram = getattr(r, "ram", None)
            last_cpu = getattr(r, "cpu", None)
            if last_ram is not None:
                s._op_ram_hint[key] = min(max(1.0, last_ram * 0.90), pool_max_ram)
            if last_cpu is not None:
                s._op_cpu_hint[key] = min(max(1.0, last_cpu * 0.90), pool_max_cpu)


@register_scheduler(key="scheduler_low_039")
def scheduler_low_039(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue new pipelines by priority.
      2) Update sizing hints based on execution results (increase RAM on failure, small decay on success).
      3) For each pool, repeatedly assign one ready operator from the highest-priority available pipeline,
         respecting pool affinity and available resources.
    """
    s._step += 1

    # Enqueue new pipelines
    for p in pipelines:
        s._pipelines_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s._enqueued_at:
            s._enqueued_at[p.pipeline_id] = s._step
        s._q[p.priority].append(p.pipeline_id)

    # Update hints from results (use pool maxes from the pool referenced by the result)
    for r in results:
        try:
            pool = s.executor.pools[r.pool_id]
        except Exception:
            continue
        _update_hints_from_result(s, r, pool.max_cpu_pool, pool.max_ram_pool)

    # Early exit if nothing changed and no backlog
    if not pipelines and not results:
        has_backlog = any(len(s._q[p]) > 0 for p in s._q)
        if not has_backlog:
            return [], []

    suspensions = []  # We avoid preemption due to lack of a stable running-container enumeration API.
    assignments = []

    # For each pool, schedule as much as we can (packing with one-op-at-a-time to reduce HOL blocking).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try multiple placements per pool while resources allow.
        # Bound iterations to avoid pathological loops if nothing fits.
        for _ in range(64):
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Pick a candidate pipeline (global priority order), then check pool affinity by reordering pools implicitly:
            pid = _pick_next_pipeline_id(s, pool_id)
            if pid is None:
                break

            pipeline = s._pipelines_by_id.get(pid)
            if pipeline is None or _pipeline_done_or_terminal(pipeline):
                continue

            # Soft pool affinity: if this pipeline would prefer other pools, we still allow it,
            # but we reduce the chance by skipping once when pool isn't preferred and there is other work.
            preferred_pools = _pick_pool_ids_for_priority(s.executor.num_pools, pipeline.priority)
            if preferred_pools and preferred_pools[0] != pool_id:
                # If there exists any pipeline of the same/higher priority, keep this pool for them.
                # This is a gentle nudge, not a hard partition.
                higher_exists = False
                if pipeline.priority == Priority.BATCH_PIPELINE:
                    higher_exists = (len(s._q[Priority.QUERY]) > 0) or (len(s._q[Priority.INTERACTIVE]) > 0)
                if higher_exists:
                    # Put it back and continue.
                    s._q[pipeline.priority].append(pid)
                    continue

            op = _get_assignable_op(pipeline)
            if op is None:
                # Nothing ready; keep it in queue.
                continue

            # Compute desired sizing; then clamp to what's available now.
            desired_cpu, desired_ram = _size_for_op(s, pipeline, op, pool)

            # If it doesn't fit, try a smaller CPU first (RAM is the usual OOM cause).
            cpu = min(desired_cpu, avail_cpu)
            ram = min(desired_ram, avail_ram)

            # Enforce minimums to avoid zero-sized assignments
            cpu = max(1.0, cpu)
            ram = max(1.0, ram)

            # If even minimum doesn't fit, we can't place anything else on this pool now.
            if cpu > avail_cpu or ram > avail_ram:
                # Put pipeline back and stop packing this pool.
                s._q[pipeline.priority].append(pid)
                break

            # Create assignment (one operator at a time to improve responsiveness and reduce tail latency).
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update local availability for packing loop.
            avail_cpu -= cpu
            avail_ram -= ram

    return suspensions, assignments
