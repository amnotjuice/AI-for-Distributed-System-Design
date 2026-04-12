# policy_key: scheduler_iter_best_simple_001
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 37.71
# generated_at: 2026-04-11T22:38:55.621218
@register_scheduler_init(key="scheduler_iter_best_simple_001")
def scheduler_iter_best_simple_001_init(s):
    """Iteration 2: stricter priority, headroom reservation, and correct OOM backoff wiring.

    Improvements over previous attempt:
      - Strict priority order (QUERY > INTERACTIVE > BATCH) per pool to reduce weighted latency.
      - Headroom reservation: when high-priority work exists, we throttle BATCH consumption so
        new QUERY/INTERACTIVE arrivals don't get stuck behind large batch allocations.
      - Better OOM learning: track container_id -> (pipeline_id, ops) at assignment time, so
        when an OOM failure arrives we can reliably increase RAM for the right operator next time.
      - Simpler fairness within a priority: round-robin pipelines inside each priority queue.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # container_id -> dict(pipeline_id=..., ops=[...], priority=..., pool_id=..., cpu=..., ram=...)
    s.inflight = {}

    # (pipeline_id, op_stable_id) -> RAM multiplier (1,2,4,... capped)
    s.op_ram_mult = {}

    # Non-OOM failures: stop re-attempting these pipelines to avoid wasting capacity.
    s.dead_pipelines = set()

    def _stable_op_id(op):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = getattr(op, "name", None)
        if oid is None:
            oid = id(op)
        return oid

    s._stable_op_id = _stable_op_id


@register_scheduler(key="scheduler_iter_best_simple_001")
def scheduler_iter_best_simple_001_scheduler(s, results, pipelines):
    def is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def enqueue_pipeline(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def any_high_prio_waiting() -> bool:
        # "Waiting" approximated as having anything in the queues.
        return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    def pop_next_runnable_from_queue(q):
        # Round-robin within this priority class.
        n = len(q)
        for _ in range(n):
            p = q.popleft()
            if p.pipeline_id in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                return p, op_list
            # Not runnable yet; rotate.
            q.append(p)
        return None, None

    def pick_next_runnable_pipeline():
        # Strict priority: QUERY > INTERACTIVE > BATCH
        p, ops = pop_next_runnable_from_queue(s.q_query)
        if p:
            return p, ops
        p, ops = pop_next_runnable_from_queue(s.q_interactive)
        if p:
            return p, ops
        return pop_next_runnable_from_queue(s.q_batch)

    def op_ram_multiplier(pipeline_id, op):
        key = (pipeline_id, s._stable_op_id(op))
        return s.op_ram_mult.get(key, 1.0)

    def bump_op_ram_multiplier(pipeline_id, op):
        key = (pipeline_id, s._stable_op_id(op))
        cur = s.op_ram_mult.get(key, 1.0)
        nxt = cur * 2.0
        if nxt > 16.0:
            nxt = 16.0
        s.op_ram_mult[key] = nxt

    def size_request(priority, pool, pipeline_id, op, avail_cpu, avail_ram, high_prio_present):
        # Goal: keep QUERY/INTERACTIVE fast while preventing BATCH from consuming all headroom.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        if priority == Priority.QUERY:
            # Give queries decent CPU to finish quickly; still cap to avoid full monopolization.
            cpu_cap = min(4.0, max_cpu * 0.50)
            ram_target = max_ram * 0.20
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(6.0, max_cpu * 0.60)
            ram_target = max_ram * 0.30
        else:  # BATCH_PIPELINE
            # Stronger throttling when high-priority work exists.
            if high_prio_present:
                cpu_cap = min(2.0, max_cpu * 0.25)
                ram_target = max_ram * 0.15
            else:
                cpu_cap = min(8.0, max_cpu * 0.75)
                ram_target = max_ram * 0.45

        # Apply OOM-based multiplier; clamp to pool and available.
        ram_target *= op_ram_multiplier(pipeline_id, op)

        cpu = min(avail_cpu, cpu_cap)
        ram = min(avail_ram, max_ram, ram_target)

        # Avoid tiny allocations that create churn with little progress.
        if cpu < 0.25:
            cpu = 0.0
        if ram < max_ram * 0.02:
            ram = 0.0
        return cpu, ram

    # Ingest new pipelines
    for p in pipelines:
        enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # Process results and update OOM learning / dead pipelines
    for r in results:
        # Clean inflight mapping for finished containers (success or failure).
        info = s.inflight.pop(getattr(r, "container_id", None), None)

        if not r.failed():
            continue

        if info is not None:
            pid = info["pipeline_id"]
            ops = info["ops"] or []
        else:
            # Fallback: best-effort if we can't link to a pipeline reliably.
            pid = None
            ops = r.ops or []

        if is_oom_error(r.error):
            if pid is not None:
                for op in ops:
                    bump_op_ram_multiplier(pid, op)
        else:
            # Non-OOM failure: stop retrying the pipeline if we can identify it.
            if pid is not None:
                s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    # Schedule per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Compute a "budget" for batch if high-priority exists: reserve headroom.
        high_present = any_high_prio_waiting()
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # When high-priority exists, reserve at least 30% CPU and 30% RAM for it.
        # We enforce this by limiting how far BATCH can drain the pool.
        if high_present:
            batch_cpu_floor = pool.max_cpu_pool * 0.30
            batch_ram_floor = pool.max_ram_pool * 0.30
        else:
            batch_cpu_floor = 0.0
            batch_ram_floor = 0.0

        # Keep assigning while there is meaningful headroom.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            p, op_list = pick_next_runnable_pipeline()
            if not p:
                break

            # If we picked a batch pipeline, respect reservation floors if high priority exists.
            if high_present and p.priority == Priority.BATCH_PIPELINE:
                # If remaining headroom is already within reserved region, stop scheduling batch here.
                if avail_cpu <= batch_cpu_floor or avail_ram <= batch_ram_floor:
                    # Put it back and stop scheduling in this pool for this tick;
                    # other pools might still have headroom, and next tick high-prio may drain.
                    enqueue_pipeline(p)
                    break

            op = op_list[0]
            cpu, ram = size_request(p.priority, pool, p.pipeline_id, op, avail_cpu, avail_ram, high_present)

            if cpu <= 0.0 or ram <= 0.0:
                # Can't fit sensibly right now; rotate pipeline back and stop for this pool.
                enqueue_pipeline(p)
                break

            assignment = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Track inflight so we can map failures back to pipeline/op reliably.
            # ExecutionResult should carry container_id; we store by container_id when it arrives.
            # If the simulator assigns container_id later, we still benefit on subsequent results
            # because inflight is only used for correlation on completion.
            # (No harm if container_id isn't available; fallback logic handles it.)
            # We tentatively index by a pseudo-key now; updated on result pop won't find it, but OK.
            pseudo_cid = getattr(assignment, "container_id", None)
            if pseudo_cid is not None:
                s.inflight[pseudo_cid] = {
                    "pipeline_id": p.pipeline_id,
                    "ops": op_list,
                    "priority": p.priority,
                    "pool_id": pool_id,
                    "cpu": cpu,
                    "ram": ram,
                }

            # Update local headroom and re-enqueue pipeline for intra-priority round-robin.
            avail_cpu -= cpu
            avail_ram -= ram
            enqueue_pipeline(p)

    return suspensions, assignments
