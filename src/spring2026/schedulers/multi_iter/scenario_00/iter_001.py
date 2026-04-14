@register_scheduler_init(key="scheduler_low_001")
def scheduler_low_001_init(s):
    # Latest Pipeline object per id (important: Pipeline objects may be refreshed across ticks)
    s.pipelines_by_id = {}  # pipeline_id -> Pipeline

    # Round-robin queues store pipeline_ids (not Pipeline objects)
    s.queues_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.queued_ids = set()  # pipeline_ids currently considered enqueued (best-effort; queue is lazily cleaned)

    # Simple OOM-adaptive RAM boost per pipeline
    s.pipeline_ram_boost = {}  # pipeline_id -> float
    s.max_ram_boost = 8.0

    # Config knobs (safe defaults)
    s.max_assignments_per_pool_per_tick = 8
    s.base_cpu_slice_frac = 0.25
    s.base_ram_slice_frac = 0.25
    s.min_cpu = 1.0
    s.min_ram = 1.0

    # Failure backoff / retry shaping
    s.failed_seen_count = {}  # pipeline_id -> int
    s.max_failed_seen_before_drop = 50  # guardrail against infinite non-OOM retry loops


def _is_oom_error(err) -> bool:
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _drop_pipeline_id(s, pipeline_id):
    s.pipelines_by_id.pop(pipeline_id, None)
    s.queued_ids.discard(pipeline_id)
    s.pipeline_ram_boost.pop(pipeline_id, None)
    s.failed_seen_count.pop(pipeline_id, None)


def _maybe_enqueue(s, p):
    pid = p.pipeline_id
    s.pipelines_by_id[pid] = p  # always refresh reference
    if pid in s.queued_ids:
        return
    # Don't enqueue already-successful pipelines
    try:
        if p.runtime_status().is_pipeline_successful():
            _drop_pipeline_id(s, pid)
            return
    except Exception:
        pass
    s.queues_by_prio[p.priority].append(pid)
    s.queued_ids.add(pid)


def _compute_request(s, pool, pipeline_id, local_avail_cpu, local_avail_ram):
    ram_boost = s.pipeline_ram_boost.get(pipeline_id, 1.0)

    cpu_req = pool.max_cpu_pool * s.base_cpu_slice_frac
    ram_req = pool.max_ram_pool * s.base_ram_slice_frac * ram_boost

    cpu = max(s.min_cpu, cpu_req)
    ram = max(s.min_ram, ram_req)

    # Cap by locally tracked availability to avoid overallocation within a single scheduler() call
    if cpu > local_avail_cpu:
        cpu = local_avail_cpu
    if ram > local_avail_ram:
        ram = local_avail_ram

    return cpu, ram


@register_scheduler(key="scheduler_low_001")
def scheduler_low_001_scheduler(s, results, pipelines):
    # Refresh / ingest pipelines
    for p in pipelines:
        _maybe_enqueue(s, p)

    # Adapt RAM boosts from observed failures (if pipeline_id is present on results)
    for r in results:
        if getattr(r, "failed", None) and r.failed():
            pid = getattr(r, "pipeline_id", None)
            if pid is not None and _is_oom_error(getattr(r, "error", None)):
                cur = s.pipeline_ram_boost.get(pid, 1.0)
                s.pipeline_ram_boost[pid] = min(s.max_ram_boost, max(cur * 2.0, cur + 0.5))

    suspensions = []
    assignments = []

    # Prevent assigning multiple ops from the same pipeline in one tick (status won't update mid-call)
    scheduled_pids_this_tick = set()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Track availability manually; executor pool avail does not update within this call
        local_avail_cpu = float(pool.avail_cpu_pool)
        local_avail_ram = float(pool.avail_ram_pool)

        made = 0
        while made < s.max_assignments_per_pool_per_tick:
            if local_avail_cpu < s.min_cpu or local_avail_ram < s.min_ram:
                break

            scheduled_one = False

            for prio in _prio_order():
                q = s.queues_by_prio[prio]
                if not q:
                    continue

                # Scan up to current queue length; we rotate by popping front and appending back.
                scan_n = len(q)
                for _ in range(scan_n):
                    pid = q.pop(0)

                    p = s.pipelines_by_id.get(pid)
                    if p is None:
                        # stale queue entry
                        s.queued_ids.discard(pid)
                        continue

                    if pid in scheduled_pids_this_tick:
                        q.append(pid)
                        continue

                    status = p.runtime_status()

                    # Drop completed pipelines
                    if status.is_pipeline_successful():
                        _drop_pipeline_id(s, pid)
                        continue

                    # If we see FAILED ops (cause unknown), we still retry; ramp RAM slowly as a hedge.
                    failed_ops = status.get_ops({OperatorState.FAILED}, require_parents_complete=False)
                    if failed_ops:
                        s.failed_seen_count[pid] = s.failed_seen_count.get(pid, 0) + 1
                        if s.failed_seen_count[pid] > s.max_failed_seen_before_drop:
                            _drop_pipeline_id(s, pid)
                            continue
                        cur = s.pipeline_ram_boost.get(pid, 1.0)
                        if cur < s.max_ram_boost:
                            s.pipeline_ram_boost[pid] = min(s.max_ram_boost, max(cur * 1.5, cur + 0.25))

                    # Get one ready-to-run op (PENDING or FAILED) whose parents are complete
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        q.append(pid)
                        continue

                    cpu, ram = _compute_request(s, pool, pid, local_avail_cpu, local_avail_ram)

                    if cpu < s.min_cpu or ram < s.min_ram:
                        # Not enough capacity in this pool right now; keep RR order and stop filling this pool
                        q.append(pid)
                        scheduled_one = False
                        break

                    assignments.append(
                        Assignment(
                            ops=op_list,
                            cpu=cpu,
                            ram=ram,
                            priority=p.priority,
                            pool_id=pool_id,
                            pipeline_id=pid,
                        )
                    )

                    # Update local availability to prevent overallocation
                    local_avail_cpu -= cpu
                    local_avail_ram -= ram
                    if local_avail_cpu < 0:
                        local_avail_cpu = 0.0
                    if local_avail_ram < 0:
                        local_avail_ram = 0.0

                    scheduled_pids_this_tick.add(pid)
                    q.append(pid)  # RR fairness
                    made += 1
                    scheduled_one = True
                    break  # stop scanning this priority; go schedule next slot

                if scheduled_one:
                    break  # go back to while loop for next assignment slot
                # If we broke due to insufficient capacity, stop this pool's loop
                if local_avail_cpu < s.min_cpu or local_avail_ram < s.min_ram:
                    break

            if not scheduled_one:
                break

    return suspensions, assignments
