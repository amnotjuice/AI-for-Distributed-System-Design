@register_scheduler_init(key="scheduler_low_001")
def scheduler_low_001_init(s):
    # Separate waiting queues per priority (lists of Pipeline)
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Best-effort dedupe of pipelines by id
    s.known_pipeline_ids = set()

    # Per-pipeline multiplicative RAM boost on failures (assumed OOM-like in this simplified policy)
    s.pipeline_ram_boost = {}

    # Config knobs (keep simple/safe)
    s.max_assignments_per_pool_per_tick = 8
    s.base_cpu_slice_frac = 0.25
    s.base_ram_slice_frac = 0.25
    s.min_cpu = 1.0
    s.min_ram = 1.0
    s.max_ram_boost = 8.0


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _enqueue_pipeline(s, p):
    pid = getattr(p, "pipeline_id", None)
    if pid is None:
        return
    if pid in s.known_pipeline_ids:
        return
    s.known_pipeline_ids.add(pid)
    s.waiting_by_prio[p.priority].append(p)


def _drop_pipeline(s, p):
    pid = getattr(p, "pipeline_id", None)
    if pid is not None and pid in s.known_pipeline_ids:
        s.known_pipeline_ids.remove(pid)
    if pid is not None:
        s.pipeline_ram_boost.pop(pid, None)


def _compute_request(s, pool, remaining_cpu, remaining_ram, pipeline_id):
    ram_boost = s.pipeline_ram_boost.get(pipeline_id, 1.0)

    # Request based on pool max (scale-up bias), then cap by remaining in this scheduler tick.
    cpu_req = max(s.min_cpu, pool.max_cpu_pool * s.base_cpu_slice_frac)
    ram_req = max(s.min_ram, pool.max_ram_pool * s.base_ram_slice_frac * ram_boost)

    cpu_req = min(cpu_req, remaining_cpu)
    ram_req = min(ram_req, remaining_ram)
    return cpu_req, ram_req


@register_scheduler(key="scheduler_low_001")
def scheduler_low_001_scheduler(s, results, pipelines):
    # Ingest new pipelines (best-effort dedupe)
    for p in pipelines:
        _enqueue_pipeline(s, p)

    suspensions = []
    assignments = []

    # IMPORTANT: s.executor.pools[i].avail_* do not update within this call.
    # Track remaining resources locally per pool to avoid over-allocation.
    remaining_cpu = {}
    remaining_ram = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        remaining_cpu[pool_id] = float(pool.avail_cpu_pool)
        remaining_ram[pool_id] = float(pool.avail_ram_pool)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        made = 0

        while made < s.max_assignments_per_pool_per_tick:
            if remaining_cpu[pool_id] < s.min_cpu or remaining_ram[pool_id] < s.min_ram:
                break

            selected_pipeline = None
            selected_prio = None
            selected_ops = None

            # Choose next runnable op, highest priority first, RR within a priority.
            for prio in _prio_order():
                q = s.waiting_by_prio[prio]
                if not q:
                    continue

                # Scan at most one full rotation to find something runnable/cleanup-able.
                found = False
                for _ in range(len(q)):
                    p = q.pop(0)  # pop front (RR)
                    status = p.runtime_status()

                    if status.is_pipeline_successful():
                        _drop_pipeline(s, p)
                        # Continue scanning; freeing queue space is progress but not a placement.
                        found = True
                        continue

                    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                    if not ops:
                        # Not runnable now; keep in queue
                        q.append(p)
                        continue

                    # If we see FAILED ops, assume OOM-like and increase RAM for subsequent tries.
                    # (OOM is recoverable; be moderately aggressive but bounded.)
                    failed_ops = status.get_ops({OperatorState.FAILED}, require_parents_complete=True)
                    if failed_ops:
                        pid = p.pipeline_id
                        cur = s.pipeline_ram_boost.get(pid, 1.0)
                        s.pipeline_ram_boost[pid] = min(s.max_ram_boost, max(cur * 2.0, cur + 0.5))

                    selected_pipeline = p
                    selected_prio = prio
                    selected_ops = ops[:1]  # schedule one op at a time for fairness
                    found = True
                    break

                if selected_pipeline is not None:
                    break
                if not found:
                    continue

            if selected_pipeline is None:
                break

            cpu_req, ram_req = _compute_request(
                s,
                pool,
                remaining_cpu[pool_id],
                remaining_ram[pool_id],
                selected_pipeline.pipeline_id,
            )

            # If we can't fit the minimum, put it back and stop filling this pool this tick.
            if cpu_req < s.min_cpu or ram_req < s.min_ram:
                s.waiting_by_prio[selected_prio].append(selected_pipeline)
                break

            assignments.append(
                Assignment(
                    ops=selected_ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=selected_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=selected_pipeline.pipeline_id,
                )
            )

            # Decrement local remaining to prevent over-allocation within this scheduler call.
            remaining_cpu[pool_id] -= cpu_req
            remaining_ram[pool_id] -= ram_req

            # RR: put pipeline at end of its priority queue.
            s.waiting_by_prio[selected_prio].append(selected_pipeline)

            made += 1

    return suspensions, assignments
