# policy_key: scheduler_est_044
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.104096
# generation_seconds: 97.69
# generated_at: 2026-03-31T19:44:55.241562
@register_scheduler_init(key="scheduler_est_044")
def scheduler_est_044_init(s):
    """
    Priority-aware FIFO with simple right-sizing using memory estimates.

    Improvements over naive FIFO:
      - Separate FIFO queues per priority; always prefer higher priority work.
      - Request RAM based on op.estimate.mem_peak_gb with a safety factor.
      - If an op fails with an OOM-like error, increase future RAM requests for that pipeline.
      - Avoid scheduling the same pipeline multiple times in a single scheduler tick.
      - Mild "headroom reservation" so batch doesn't consume the entire pool when interactive/query is waiting.
    """
    s.queues_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline RAM inflation multiplier (learned via OOM retries).
    s.ram_mult_by_pipeline = {}

    # Pipelines that failed for non-OOM reasons (best-effort; depends on inferring pipeline_id).
    s.blacklisted_pipelines = set()

    # Tuning knobs (kept intentionally simple).
    s.mem_safety_factor = 1.25
    s.oom_backoff_mult = 1.6
    s.max_ram_mult = 6.0
    s.min_ram_gb = 0.25


def _infer_pipeline_id_from_result(res):
    pid = getattr(res, "pipeline_id", None)
    if pid is not None:
        return pid
    ops = getattr(res, "ops", None) or []
    for op in ops:
        pid = getattr(op, "pipeline_id", None)
        if pid is not None:
            return pid
        pipeline = getattr(op, "pipeline", None)
        pid = getattr(pipeline, "pipeline_id", None)
        if pid is not None:
            return pid
    return None


def _is_oom_error(res) -> bool:
    if not res or not res.failed():
        return False
    err = (getattr(res, "error", None) or "")
    u = err.upper()
    # Keep it permissive across different backends/exception texts.
    return ("OOM" in u) or ("OUT OF MEMORY" in u) or ("MEMORYERROR" in u)


def _op_mem_est_gb(op) -> float:
    est = getattr(op, "estimate", None)
    mem = getattr(est, "mem_peak_gb", None)
    if mem is None:
        return 1.0
    try:
        mem = float(mem)
    except Exception:
        mem = 1.0
    return max(0.0, mem)


def _target_cpu_for_priority(priority, pool_max_cpu: float) -> float:
    # Conservative caps to encourage packing and reduce latency under contention.
    if priority == Priority.QUERY:
        return min(8.0, float(pool_max_cpu))
    if priority == Priority.INTERACTIVE:
        return min(6.0, float(pool_max_cpu))
    return min(4.0, float(pool_max_cpu))


def _reserve_headroom(pool, have_high_waiting: bool):
    # Only reserve if there is any higher-priority backlog.
    if not have_high_waiting:
        return 0.0, 0.0
    # Mild reservation: enough to start a small query quickly.
    reserve_cpu = max(1.0, 0.10 * float(pool.max_cpu_pool))
    reserve_ram = max(1.0, 0.10 * float(pool.max_ram_pool))
    return reserve_cpu, reserve_ram


def _compute_request(s, pipeline, op, pool, avail_cpu: float, avail_ram: float):
    # RAM request: estimate * per-pipeline multiplier * safety, clamped to pool max.
    pid = pipeline.pipeline_id
    mult = float(s.ram_mult_by_pipeline.get(pid, 1.0))
    mem_est = _op_mem_est_gb(op)

    ram_req = max(s.min_ram_gb, mem_est * mult * float(s.mem_safety_factor))
    ram_req = min(ram_req, float(pool.max_ram_pool))

    # CPU request: small capped shares by priority (packing-friendly), at least 1.
    cpu_tgt = _target_cpu_for_priority(pipeline.priority, pool.max_cpu_pool)
    cpu_req = max(1.0, min(float(avail_cpu), float(cpu_tgt)))

    # If it doesn't fit, don't downsize RAM (that would likely OOM); just signal "can't fit now".
    if ram_req > float(avail_ram) or cpu_req > float(avail_cpu):
        return None
    return cpu_req, ram_req


@register_scheduler(key="scheduler_est_044")
def scheduler_est_044(s, results, pipelines):
    """
    Priority-aware scheduler with estimate-based RAM sizing and OOM backoff.

    Core loop:
      - Enqueue new pipelines by priority.
      - Process results to learn per-pipeline RAM multiplier (OOM => increase).
      - For each pool, pack multiple assignments if possible, prioritizing QUERY > INTERACTIVE > BATCH.
      - Prevent scheduling the same pipeline multiple times within one tick (avoids duplicate assignment
        due to runtime_status not reflecting new assignments until after the tick).
    """
    # Enqueue new pipelines.
    for p in pipelines:
        q = s.queues_by_prio.get(p.priority)
        if q is None:
            # Fallback: treat unknown as batch.
            s.queues_by_prio[Priority.BATCH_PIPELINE].append(p)
        else:
            q.append(p)

    # Early exit: if nothing new and nothing completed/failed, no decision changes.
    if not pipelines and not results:
        return [], []

    # Update simple learning state from results.
    for r in results:
        pid = _infer_pipeline_id_from_result(r)
        if pid is None:
            continue

        if r.failed():
            if _is_oom_error(r):
                cur = float(s.ram_mult_by_pipeline.get(pid, 1.0))
                nxt = min(float(s.max_ram_mult), cur * float(s.oom_backoff_mult))
                s.ram_mult_by_pipeline[pid] = max(1.0, nxt)
            else:
                # Best-effort: avoid infinite retries for deterministic failures.
                s.blacklisted_pipelines.add(pid)
        else:
            # Small decay after successes to avoid permanently over-allocating.
            cur = float(s.ram_mult_by_pipeline.get(pid, 1.0))
            s.ram_mult_by_pipeline[pid] = max(1.0, cur * 0.97)

    suspensions = []
    assignments = []

    # Prevent duplicate scheduling of the same pipeline in this tick.
    scheduled_this_tick = set()

    # Priority order: protect latency for high-priority work.
    prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Helper to determine whether there is any high-priority backlog (approximate).
    def have_high_waiting():
        for pr in (Priority.QUERY, Priority.INTERACTIVE):
            q = s.queues_by_prio.get(pr, [])
            for p in q:
                if p.pipeline_id not in s.blacklisted_pipelines:
                    st = p.runtime_status()
                    if not st.is_pipeline_successful():
                        return True
        return False

    high_waiting = have_high_waiting()

    # Pack per pool (bounded to avoid long loops on large queues).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < 1.0 or avail_ram < float(s.min_ram_gb):
            continue

        reserve_cpu, reserve_ram = _reserve_headroom(pool, high_waiting)

        max_assignments_this_pool = 8
        made_progress = True

        for _ in range(max_assignments_this_pool):
            if not made_progress:
                break
            made_progress = False

            if avail_cpu < 1.0 or avail_ram < float(s.min_ram_gb):
                break

            # Try to find a runnable op that fits, starting from highest priority.
            for pr in prio_order:
                q = s.queues_by_prio.get(pr, [])
                if not q:
                    continue

                # Scan at most one full rotation of this queue.
                qlen = len(q)
                for _scan in range(qlen):
                    p = q.pop(0)

                    # Drop terminal/blacklisted pipelines.
                    if p.pipeline_id in s.blacklisted_pipelines:
                        continue
                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        continue

                    # Avoid scheduling the same pipeline multiple times in this tick.
                    if p.pipeline_id in scheduled_this_tick:
                        q.append(p)
                        continue

                    # Only schedule ops whose parents are complete.
                    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        q.append(p)
                        continue
                    op = op_list[0]

                    req = _compute_request(s, p, op, pool, avail_cpu, avail_ram)
                    if req is None:
                        q.append(p)
                        continue
                    cpu_req, ram_req = req

                    # If scheduling batch while high-priority is waiting, keep some headroom.
                    if pr == Priority.BATCH_PIPELINE and high_waiting:
                        if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                            q.append(p)
                            continue

                    # Commit the assignment.
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

                    scheduled_this_tick.add(p.pipeline_id)
                    avail_cpu -= cpu_req
                    avail_ram -= ram_req

                    # Requeue pipeline for subsequent ops in future ticks.
                    q.append(p)

                    made_progress = True
                    break  # stop scanning this priority queue

                if made_progress:
                    break  # proceed to next packing slot in this pool

    return suspensions, assignments
