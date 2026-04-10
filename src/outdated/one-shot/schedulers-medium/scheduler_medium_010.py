# policy_key: scheduler_medium_010
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.077853
# generation_seconds: 88.18
# generated_at: 2026-03-12T22:06:02.079551
@register_scheduler_init(key="scheduler_medium_010")
def scheduler_medium_010_init(s):
    """Priority-aware, resource-slicing FIFO (small, safe improvement over naive FIFO).

    Key ideas:
      - Maintain separate FIFO queues per priority and schedule higher priorities first.
      - Avoid allocating an entire pool to a single operator; instead allocate "slices" to
        allow multiple operators to run concurrently (improves admission/queueing latency).
      - If an operator fails with an OOM-like error, increase the RAM multiplier for its
        pipeline and retry failed ops (FAILED is treated as assignable for retries).
      - If an operator fails for non-OOM reasons, mark the pipeline as drop/terminal to
        avoid infinite retries.
    """
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-pipeline RAM multiplier (only bumped on OOM-like failures).
    s.ram_multiplier = {}
    s.max_ram_multiplier = 16.0

    # Pipelines to stop scheduling due to non-retryable failures.
    s.drop_pipelines = set()


def _prio_order():
    # Higher priority first.
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _slots_for_priority(priority):
    # Fewer slots => bigger slices. Query/interactive get bigger slices, batch gets smaller.
    if priority == Priority.QUERY:
        return 4.0
    if priority == Priority.INTERACTIVE:
        return 6.0
    return 8.0  # batch


def _oom_like(error_str):
    e = (error_str or "").lower()
    return ("oom" in e) or ("out of memory" in e) or ("cuda out of memory" in e)


def _get_pipeline_id_from_result(r):
    # Best-effort extraction; different simulators may expose pipeline_id differently.
    pid = getattr(r, "pipeline_id", None)
    if pid is not None:
        return pid
    if getattr(r, "ops", None):
        try:
            return getattr(r.ops[0], "pipeline_id", None)
        except Exception:
            return None
    return None


def _cpu_slice(pool, priority, avail_cpu):
    # Slice CPU based on pool size and priority; bounded by current availability.
    slots = _slots_for_priority(priority)
    max_cpu = float(getattr(pool, "max_cpu_pool", 0.0) or 0.0)
    if max_cpu <= 0.0:
        # Fallback: if max isn't known, just be conservative.
        target = float(avail_cpu)
    else:
        target = max_cpu / slots

    # Ensure we don't request more than available; allow fractional CPU if supported.
    cpu = min(float(avail_cpu), float(target))
    # If only a tiny amount is available, still try to use it.
    return max(0.0, cpu)


def _ram_slice(pool, priority, avail_ram, ram_mult):
    # Slice RAM based on pool size and priority; multiplied for OOM backoff.
    slots = _slots_for_priority(priority)
    max_ram = float(getattr(pool, "max_ram_pool", 0.0) or 0.0)
    if max_ram <= 0.0:
        target = float(avail_ram)
    else:
        target = max_ram / slots

    ram = min(float(avail_ram), float(target) * float(ram_mult))
    return max(0.0, ram)


@register_scheduler(key="scheduler_medium_010")
def scheduler_medium_010_scheduler(s, results, pipelines):
    """
    Priority-aware FIFO with resource slicing + OOM RAM backoff retries.

    Returns:
      suspensions: none (no preemption in this incremental step)
      assignments: multiple per pool per tick, packing slices until pool resources are used
    """
    # Enqueue new arrivals by priority.
    for p in pipelines:
        s.waiting_queues[p.priority].append(p)

    # Update RAM backoff and drop lists based on execution results.
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue

        pid = _get_pipeline_id_from_result(r)
        if pid is None:
            continue

        err = str(getattr(r, "error", "") or "")
        if _oom_like(err):
            cur = float(s.ram_multiplier.get(pid, 1.0))
            s.ram_multiplier[pid] = min(cur * 2.0, float(s.max_ram_multiplier))
        else:
            # Non-OOM failures are treated as terminal for now (avoid churn/infinite retries).
            s.drop_pipelines.add(pid)

    # If nothing changed, no new resources became available and no new work arrived.
    # (In this simulator model, capacity changes only when we receive results.)
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: remove obviously done/terminal pipelines without spending too much effort.
    # (We only do lightweight cleanup; deeper cleanup happens naturally while scanning.)
    for pr in _prio_order():
        if not s.waiting_queues[pr]:
            continue
        new_q = []
        for p in s.waiting_queues[pr]:
            if p.pipeline_id in s.drop_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            new_q.append(p)
        s.waiting_queues[pr] = new_q

    # Schedule across pools. Each pool independently packs work using the same global queues.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Pack multiple assignments per pool per tick.
        while avail_cpu > 0.0 and avail_ram > 0.0:
            made_assignment = False

            for pr in _prio_order():
                q = s.waiting_queues[pr]
                if not q:
                    continue

                # Round-robin scan within this priority to find a runnable op.
                # Bound scanning to queue length to avoid infinite loops.
                scan_n = len(q)
                for _ in range(scan_n):
                    pipeline = q.pop(0)

                    if pipeline.pipeline_id in s.drop_pipelines:
                        continue

                    status = pipeline.runtime_status()
                    if status.is_pipeline_successful():
                        continue

                    # Only schedule ops whose parents are complete; retry FAILED as assignable.
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        # Not runnable right now; keep it in the queue.
                        q.append(pipeline)
                        continue

                    ram_mult = float(s.ram_multiplier.get(pipeline.pipeline_id, 1.0))
                    cpu_req = _cpu_slice(pool, pipeline.priority, avail_cpu)
                    ram_req = _ram_slice(pool, pipeline.priority, avail_ram, ram_mult)

                    # If we can't allocate any meaningful resources, stop packing this pool.
                    if cpu_req <= 0.0 or ram_req <= 0.0:
                        # Put it back and stop trying for this pool this tick.
                        q.insert(0, pipeline)
                        made_assignment = False
                        break

                    assignments.append(
                        Assignment(
                            ops=op_list,
                            cpu=cpu_req,
                            ram=ram_req,
                            priority=pipeline.priority,
                            pool_id=pool_id,
                            pipeline_id=pipeline.pipeline_id,
                        )
                    )

                    avail_cpu -= cpu_req
                    avail_ram -= ram_req

                    # Keep pipeline in its queue for subsequent runnable ops.
                    q.append(pipeline)
                    made_assignment = True
                    break

                if made_assignment:
                    # After scheduling one op, restart from highest priority for this pool.
                    break

            if not made_assignment:
                break

    return suspensions, assignments
