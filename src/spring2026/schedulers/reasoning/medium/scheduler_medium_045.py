# policy_key: scheduler_medium_045
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.107055
# generation_seconds: 81.33
# generated_at: 2026-04-09T23:30:55.266072
@register_scheduler_init(key="scheduler_medium_045")
def scheduler_medium_045_init(s):
    """
    Priority-aware, failure-averse scheduler.

    Main ideas:
    - Multi-queue by priority with gentle aging to prevent batch starvation.
    - Conservative RAM sizing with exponential backoff on OOM failures (retry instead of drop).
    - CPU sizing biased toward query/interactive to reduce weighted latency.
    - Pool preference: if multiple pools exist, reserve pool 0 primarily for query/interactive
      (spill over when empty), while other pools prefer batch (but allow spillover if needed).
    """
    # Tick counter for simple aging
    s._tick = 0

    # Waiting queues by priority
    s._queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # When a pipeline was last enqueued (for aging)
    s._enqueued_at = {}  # pipeline_id -> tick

    # Per (pipeline, op_key) resource estimates learned from failures
    s._ram_est = {}  # (pipeline_id, op_key) -> ram
    s._cpu_est = {}  # (pipeline_id, op_key) -> cpu

    # Failure tracking to avoid infinite retries on non-OOM errors
    s._oom_failures = {}       # (pipeline_id, op_key) -> count
    s._nonoom_failures = {}    # pipeline_id -> count
    s._do_not_retry = set()    # pipeline_id

    # Tuning knobs
    s._max_nonoom_failures = 1     # stop retrying quickly on non-OOM
    s._max_oom_failures = 6        # after this, just try max RAM and keep going
    s._batch_aging_boost_tick = 25 # batch gets noticeable boost after waiting this long


def _sm045_is_oom_error(err) -> bool:
    if not err:
        return False
    try:
        e = str(err).lower()
    except Exception:
        return False
    return ("oom" in e) or ("out of memory" in e) or ("cuda out of memory" in e) or ("killed" in e and "memory" in e)


def _sm045_op_key(op) -> str:
    # Best-effort stable key for an operator object across callbacks
    for attr in ("op_id", "operator_id", "id", "name", "task_id"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return str(v)
            except Exception:
                pass
    return str(op)


def _sm045_get_op_min_ram(op):
    # Best-effort: different traces/sim versions may use different field names
    for attr in (
        "ram_min", "min_ram", "min_ram_gb", "ram_gb", "ram_req", "ram_requirement",
        "mem_min", "min_mem", "memory_min", "min_memory"
    ):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return float(v)
            except Exception:
                pass
    return None


def _sm045_priority_order():
    return (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE)


def _sm045_base_weight(priority):
    # Larger means more urgent for selection
    if priority == Priority.QUERY:
        return 100.0
    if priority == Priority.INTERACTIVE:
        return 50.0
    return 10.0


def _sm045_pool_prefers_priority(pool_id: int, priority, num_pools: int) -> bool:
    # If multiple pools: reserve pool 0 mostly for query/interactive
    if num_pools <= 1:
        return True
    if pool_id == 0:
        return priority in (Priority.QUERY, Priority.INTERACTIVE)
    # Other pools prefer batch by default
    return priority == Priority.BATCH_PIPELINE


def _sm045_pick_next_pipeline(s, pool_id: int):
    """
    Pick next pipeline to schedule for a pool, using:
    - pool preference (if any)
    - priority (query > interactive > batch)
    - aging boost for waiting batch to avoid starvation
    """
    num_pools = s.executor.num_pools

    best = None
    best_score = -1e18

    for pr in _sm045_priority_order():
        q = s._queues.get(pr, [])
        if not q:
            continue

        # If pool has a preference, consider preferred class first; otherwise still consider all.
        preferred = _sm045_pool_prefers_priority(pool_id, pr, num_pools)

        for p in q:
            pid = p.pipeline_id
            if pid in s._do_not_retry:
                continue

            # Aging: boost after waiting. Stronger boost for batch, mild for others.
            enq = s._enqueued_at.get(pid, s._tick)
            waited = max(0, s._tick - enq)

            score = _sm045_base_weight(pr)

            if pr == Priority.BATCH_PIPELINE:
                # Gentle but meaningful anti-starvation; step-up after a while
                score += 0.5 * min(waited, 200)
                if waited >= s._batch_aging_boost_tick:
                    score += 25.0
            else:
                # Slight boost so old interactive/query don't get stuck behind newer ones
                score += 0.1 * min(waited, 200)

            # Pool preference bonus
            if preferred:
                score += 7.5
            else:
                score -= 2.5

            # Penalize pipelines that have accrued non-OOM failures (likely deterministic)
            score -= 30.0 * float(s._nonoom_failures.get(pid, 0))

            if score > best_score:
                best_score = score
                best = p

    return best


def _sm045_remove_from_queues(s, pipeline):
    pr = pipeline.priority
    q = s._queues.get(pr, [])
    pid = pipeline.pipeline_id
    if q:
        s._queues[pr] = [x for x in q if x.pipeline_id != pid]
    s._enqueued_at.pop(pid, None)


def _sm045_requeue_pipeline_front(s, pipeline):
    # Requeue to the front to retry soon after learning a better size (esp. for query/interactive).
    pr = pipeline.priority
    pid = pipeline.pipeline_id
    _sm045_remove_from_queues(s, pipeline)
    s._queues[pr].insert(0, pipeline)
    s._enqueued_at[pid] = s._tick


def _sm045_requeue_pipeline_back(s, pipeline):
    pr = pipeline.priority
    pid = pipeline.pipeline_id
    _sm045_remove_from_queues(s, pipeline)
    s._queues[pr].append(pipeline)
    s._enqueued_at[pid] = s._tick


def _sm045_choose_ram_cpu(s, pool, pipeline, op):
    """
    Choose (ram, cpu) for an op given pool capacity and learned estimates.
    Strategy:
    - RAM: start from op min RAM (if known) or a small conservative baseline;
           apply priority multiplier; apply OOM backoff estimate; cap to pool.
    - CPU: bias higher for query/interactive; use learned estimate as a floor;
           avoid giving batch the whole pool if others may run.
    """
    pid = pipeline.pipeline_id
    pr = pipeline.priority
    opk = _sm045_op_key(op)

    max_ram = float(pool.max_ram_pool)
    max_cpu = float(pool.max_cpu_pool)

    min_ram = _sm045_get_op_min_ram(op)

    # Conservative baseline if unknown: small chunk, but not tiny (reduce OOM churn).
    if min_ram is None:
        baseline = max(1.0, min(4.0, 0.12 * max_ram))
    else:
        baseline = max(0.5, float(min_ram))

    # Priority multipliers: give more headroom to reduce OOM risk for high-priority work.
    if pr == Priority.QUERY:
        baseline *= 1.40
    elif pr == Priority.INTERACTIVE:
        baseline *= 1.25
    else:
        baseline *= 1.10

    learned_ram = s._ram_est.get((pid, opk), 0.0)
    ram = max(baseline, float(learned_ram) if learned_ram else 0.0)

    # If many OOMs, attempt near-max RAM to avoid repeated 720s from incomplete runs.
    oom_n = s._oom_failures.get((pid, opk), 0)
    if oom_n >= 4:
        ram = max(ram, 0.75 * max_ram)
    if oom_n >= 6:
        ram = max(ram, max_ram)

    ram = min(ram, max_ram)

    # CPU: assign a sizable fraction to reduce latency, but keep some room for concurrency.
    if pr == Priority.QUERY:
        cpu_target = max(1.0, 0.80 * max_cpu)
    elif pr == Priority.INTERACTIVE:
        cpu_target = max(1.0, 0.55 * max_cpu)
    else:
        cpu_target = max(1.0, 0.30 * max_cpu)

    learned_cpu = s._cpu_est.get((pid, opk), 0.0)
    if learned_cpu:
        cpu_target = max(cpu_target, float(learned_cpu))

    cpu = min(cpu_target, max_cpu)
    return ram, cpu


@register_scheduler(key="scheduler_medium_045")
def scheduler_medium_045(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Scheduler tick:
    - Ingest new pipelines into per-priority queues.
    - Learn from failures:
        * On OOM: increase RAM estimate and retry.
        * On non-OOM: stop retrying after a small number of failures.
    - Assign ready operators, honoring priority and pool preference, while packing within pool capacity.
    """
    s._tick += 1

    # Enqueue new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s._do_not_retry:
            continue
        if pid not in s._enqueued_at:
            s._enqueued_at[pid] = s._tick
        s._queues[p.priority].append(p)

    # Learn from execution results (especially failures) to reduce OOM churn
    for r in (results or []):
        if not r or not getattr(r, "ops", None):
            continue

        # Prefer to learn per-op. r.ops could be a list.
        ops = r.ops if isinstance(r.ops, list) else [r.ops]

        # If failures: update estimates
        if hasattr(r, "failed") and r.failed():
            is_oom = _sm045_is_oom_error(getattr(r, "error", None))

            for op in ops:
                # We need pipeline_id for state keys; best-effort: r may not have it.
                # If unavailable, we only do coarse learning keyed by container_id.
                # Most simulator versions pass ops objects that can be mapped back via op key + pipeline_id in queue,
                # so we attempt to find the owning pipeline by scanning queues (cheap at moderate scale).
                owner_pid = None
                for pr in _sm045_priority_order():
                    for p in s._queues.get(pr, []):
                        # If op object identity is present in pipeline.values, we can attribute by membership.
                        # Otherwise, skip attribution.
                        try:
                            if hasattr(p, "values") and op in getattr(p, "values", []):
                                owner_pid = p.pipeline_id
                                break
                        except Exception:
                            pass
                    if owner_pid is not None:
                        break

                # If we can't attribute to a pipeline, skip learning (safe fallback).
                if owner_pid is None:
                    continue

                opk = _sm045_op_key(op)

                if is_oom:
                    # Exponential RAM backoff; start from observed allocation if available.
                    prev = float(s._ram_est.get((owner_pid, opk), 0.0) or 0.0)
                    base = float(getattr(r, "ram", 0.0) or 0.0)
                    if prev <= 0.0:
                        prev = max(1.0, base)
                    new_est = max(prev * 1.8, prev + max(1.0, 0.15 * prev), base * 2.0 if base > 0 else prev * 2.0)
                    s._ram_est[(owner_pid, opk)] = new_est
                    s._oom_failures[(owner_pid, opk)] = s._oom_failures.get((owner_pid, opk), 0) + 1
                else:
                    s._nonoom_failures[owner_pid] = s._nonoom_failures.get(owner_pid, 0) + 1
                    if s._nonoom_failures[owner_pid] > s._max_nonoom_failures:
                        s._do_not_retry.add(owner_pid)

        else:
            # Successful completion of a container: opportunistically record that the cpu/ram worked.
            for op in ops:
                owner_pid = None
                for pr in _sm045_priority_order():
                    for p in s._queues.get(pr, []):
                        try:
                            if hasattr(p, "values") and op in getattr(p, "values", []):
                                owner_pid = p.pipeline_id
                                break
                        except Exception:
                            pass
                    if owner_pid is not None:
                        break
                if owner_pid is None:
                    continue

                opk = _sm045_op_key(op)
                # Record minimum observed working sizes as a floor for future runs.
                try:
                    if getattr(r, "ram", None) is not None:
                        s._ram_est[(owner_pid, opk)] = max(float(s._ram_est.get((owner_pid, opk), 0.0) or 0.0), float(r.ram))
                    if getattr(r, "cpu", None) is not None:
                        s._cpu_est[(owner_pid, opk)] = max(float(s._cpu_est.get((owner_pid, opk), 0.0) or 0.0), float(r.cpu))
                except Exception:
                    pass

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Clean up completed pipelines from queues (avoid wasting scheduling passes)
    for pr in _sm045_priority_order():
        newq = []
        for p in s._queues.get(pr, []):
            pid = p.pipeline_id
            if pid in s._do_not_retry:
                continue
            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    s._enqueued_at.pop(pid, None)
                    continue
            except Exception:
                pass
            newq.append(p)
        s._queues[pr] = newq

    # Scheduling per pool: pack multiple assignments if capacity allows
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # If pool is full, skip
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Attempt to place multiple ops into this pool
        # Keep a small RAM/CPU guard band to reduce fragmentation and accidental oversubscription.
        cpu_guard = 0.05 * float(pool.max_cpu_pool)
        ram_guard = 0.03 * float(pool.max_ram_pool)

        while avail_cpu > max(0.0, cpu_guard) and avail_ram > max(0.0, ram_guard):
            pipeline = _sm045_pick_next_pipeline(s, pool_id)
            if pipeline is None:
                break

            pid = pipeline.pipeline_id
            if pid in s._do_not_retry:
                _sm045_remove_from_queues(s, pipeline)
                continue

            # If pipeline is completed, drop it
            try:
                status = pipeline.runtime_status()
                if status.is_pipeline_successful():
                    _sm045_remove_from_queues(s, pipeline)
                    continue
            except Exception:
                # If status can't be read, avoid scheduling it repeatedly.
                _sm045_requeue_pipeline_back(s, pipeline)
                break

            # If pipeline has persistent non-OOM failures, stop retrying
            if s._nonoom_failures.get(pid, 0) > s._max_nonoom_failures:
                s._do_not_retry.add(pid)
                _sm045_remove_from_queues(s, pipeline)
                continue

            # Select a single next ready operator
            try:
                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            except Exception:
                op_list = []

            if not op_list:
                # Nothing ready now; rotate to back to avoid head-of-line blocking
                _sm045_requeue_pipeline_back(s, pipeline)
                continue

            op = op_list[0]
            ram_need, cpu_need = _sm045_choose_ram_cpu(s, pool, pipeline, op)

            # Clamp to available
            ram_assign = min(ram_need, avail_ram)
            cpu_assign = min(cpu_need, avail_cpu)

            # Enforce minimum allocations (avoid creating doomed tiny containers)
            if cpu_assign < 1.0:
                # Not enough CPU left; stop scheduling in this pool for this tick
                break
            if ram_assign <= 0.0:
                break

            # If we can't give even a reasonable baseline RAM, stop for now
            min_ram = _sm045_get_op_min_ram(op)
            if min_ram is not None and ram_assign + 1e-9 < float(min_ram):
                break

            # Create assignment
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_assign,
                    ram=ram_assign,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Update pool availability for packing
            avail_cpu -= cpu_assign
            avail_ram -= ram_assign

            # Move pipeline to back (round-robin within priority) to improve fairness
            # Query/interactive still win overall via selection weights.
            _sm045_requeue_pipeline_back(s, pipeline)

    return suspensions, assignments
