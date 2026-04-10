# policy_key: scheduler_est_041
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.035118
# generation_seconds: 48.57
# generated_at: 2026-04-01T00:12:06.751321
@register_scheduler_init(key="scheduler_est_041")
def scheduler_est_041_init(s):
    """
    Priority-aware, estimate-guided scheduler (incremental improvement over naive FIFO).

    Main ideas:
    - Maintain separate FIFO queues per priority and schedule higher priority first.
    - Use op.estimate.mem_peak_gb (when available) as a RAM floor to reduce OOMs.
    - Allocate "reasonable" per-op CPU and RAM (avoid giving the entire pool to one op),
      improving latency under contention by enabling more parallelism.
    - On OOM-like failures, increase a per-operator RAM override and allow retry; on other
      failures, drop the pipeline to avoid thrashing.

    Notes/assumptions:
    - Preemption is not used here (no reliable access pattern provided to enumerate running containers).
    - One operator per assignment (conservative and compatible with the naive template).
    """
    # Per-priority FIFO queues of pipelines
    s.waiting_by_pri = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator RAM override (GB) raised after OOM-like failures
    s.op_ram_override_gb = {}

    # Track pipelines we have already enqueued (avoid pathological duplicate insertion)
    s.enqueued_pipeline_ids = set()

    # Simple scheduling knobs (kept intentionally small and obvious)
    s.cpu_target_by_pri = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    s.default_ram_gb_by_pri = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 2.0,
    }

    # If we have multiple pools, bias interactive/query to pool 0, batch to other pools.
    # (This is a soft preference; if a pool is empty, others can still run work.)
    s.preferred_pools_by_pri = {
        Priority.QUERY: [0],
        Priority.INTERACTIVE: [0],
        Priority.BATCH_PIPELINE: [],  # filled dynamically based on num_pools
    }


@register_scheduler(key="scheduler_est_041")
def scheduler_est_041_scheduler(s, results, pipelines):
    """
    Scheduler tick.

    - Ingest new pipelines into per-priority queues.
    - Process results: on OOM failures, bump operator RAM override; on non-OOM failures, stop retrying.
    - For each pool, repeatedly pick the next runnable op from the best available priority queue,
      size it with conservative CPU and estimate-based RAM floor, then assign while capacity remains.
    """
    def _op_key(op):
        # Best-effort stable-ish key for an operator.
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        # Heuristic string match; simulator error types may vary.
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)

    def _enqueue_pipeline(p):
        # Avoid inserting the same pipeline object repeatedly if the simulator passes it again.
        # (We still allow it to exist in the queue once; it stays there until done/dropped.)
        if p.pipeline_id in s.enqueued_pipeline_ids:
            return
        s.enqueued_pipeline_ids.add(p.pipeline_id)
        s.waiting_by_pri[p.priority].append(p)

    def _pipeline_done_or_terminal_failure(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True, False
        # If there are failures, we only keep it if we think failures are retryable (OOM).
        # We infer retryability based on recent ExecutionResult messages and our overrides.
        has_failures = status.state_counts[OperatorState.FAILED] > 0
        return False, has_failures

    def _pick_next_pipeline(pri_list):
        # Pop from the front and requeue to preserve FIFO while allowing us to "scan"
        # for pipelines that actually have runnable ops.
        for pri in pri_list:
            q = s.waiting_by_pri.get(pri, [])
            if q:
                return pri, q
        return None, None

    def _get_runnable_op(p):
        status = p.runtime_status()
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        return op_list[0]

    def _ram_floor_gb_for_op(op, pri):
        # Use estimate.mem_peak_gb (if present) as a RAM floor; apply override if we saw OOM.
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None

        base = s.default_ram_gb_by_pri.get(pri, 2.0)
        floor = base if (est is None) else max(base, float(est))
        override = s.op_ram_override_gb.get(_op_key(op))
        if override is not None:
            floor = max(floor, float(override))
        return max(0.1, floor)

    def _cpu_target_for_pri(pri):
        return float(s.cpu_target_by_pri.get(pri, 2.0))

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Update RAM overrides based on failures
    # (We only get failure reason from results, so we do it here.)
    for r in results:
        if getattr(r, "failed", None) is None:
            continue
        if not r.failed():
            continue
        if not _is_oom_error(getattr(r, "error", None)):
            continue
        # For each op in the failed container, bump override.
        # If multiple ops are reported, they will all get a bump.
        for op in getattr(r, "ops", []) or []:
            k = _op_key(op)
            prev = s.op_ram_override_gb.get(k, 0.0)
            # Use the container's RAM as a lower bound, then add slack and growth.
            # This converges quickly without being too aggressive.
            observed = float(getattr(r, "ram", 0.0) or 0.0)
            bumped = max(prev, observed * 1.5, observed + 1.0, prev * 1.5 if prev > 0 else 0.0)
            if bumped > 0:
                s.op_ram_override_gb[k] = bumped

    # If no new pipelines and no results, nothing to do
    if not pipelines and not results:
        return [], []

    # Preferred pools config for batch (everything except 0 if possible)
    if not s.preferred_pools_by_pri[Priority.BATCH_PIPELINE]:
        if s.executor.num_pools > 1:
            s.preferred_pools_by_pri[Priority.BATCH_PIPELINE] = list(range(1, s.executor.num_pools))
        else:
            s.preferred_pools_by_pri[Priority.BATCH_PIPELINE] = [0]

    suspensions = []
    assignments = []

    # Determine priority order (strict, simple improvement)
    priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Scheduling per pool: keep assigning until capacity is too low or no runnable work
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Very small capacity -> skip
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Choose which priorities to consider first for this pool (soft pool partitioning)
        # Pool 0: prefer interactive/query first; other pools: prefer batch first.
        if pool_id == 0:
            pri_scan = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        else:
            pri_scan = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]

        made_progress = True
        # Avoid infinite loops if queues contain only blocked pipelines
        max_attempts = sum(len(s.waiting_by_pri[p]) for p in priority_order) + 5

        while made_progress and avail_cpu > 0.0 and avail_ram > 0.0 and max_attempts > 0:
            made_progress = False
            max_attempts -= 1

            # Find next pipeline with a runnable op
            chosen_pipeline = None
            chosen_pri = None

            # Scan priorities in pool-biased order; within each, rotate FIFO to find runnable op
            for pri in pri_scan:
                q = s.waiting_by_pri.get(pri, [])
                if not q:
                    continue

                # Soft pool preference: if this priority has preferred pools and this pool is not one of them,
                # we still allow it, but only if the preferred pools list does not include any valid pool
                # or if the work would otherwise stall. (Keep it permissive/small improvement.)
                preferred = s.preferred_pools_by_pri.get(pri, [])
                if preferred and (pool_id not in preferred):
                    # Defer unless there's no alternative in this pool scan order.
                    # We'll just continue scanning other priorities first.
                    pass

                scanned = 0
                qlen = len(q)
                while scanned < qlen:
                    p = q.pop(0)
                    scanned += 1

                    done, has_failures = _pipeline_done_or_terminal_failure(p)
                    if done:
                        # Drop completed pipelines from the system
                        continue

                    if has_failures:
                        # If pipeline has failures, we only keep it if there exists an assignable failed op
                        # (ASSIGNABLE_STATES includes FAILED) and we might have raised RAM for OOM.
                        # Otherwise, drop it to avoid endless retries.
                        # We can't perfectly classify failure cause per-op from status, so we keep it
                        # only if any FAILED op has a RAM override (suggests OOM-retry path).
                        status = p.runtime_status()
                        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
                        retryable = False
                        for op in failed_ops:
                            if _op_key(op) in s.op_ram_override_gb:
                                retryable = True
                                break
                        if not retryable:
                            continue  # drop terminal failures

                    op = _get_runnable_op(p)
                    if op is None:
                        # Not runnable now (parents incomplete); requeue and try next pipeline
                        q.append(p)
                        continue

                    chosen_pipeline = p
                    chosen_pri = pri
                    # Keep pipeline in queue for future operators (we requeue it after assignment)
                    q.append(p)
                    break

                if chosen_pipeline is not None:
                    break

            if chosen_pipeline is None:
                break  # no runnable work for this pool right now

            # Create an assignment for the chosen op with conservative sizing
            op = _get_runnable_op(chosen_pipeline)
            if op is None:
                continue

            ram_floor = _ram_floor_gb_for_op(op, chosen_pri)
            if ram_floor > avail_ram:
                # Can't fit now; try later (do nothing this tick for this pool)
                continue

            cpu_target = _cpu_target_for_pri(chosen_pri)
            cpu_req = min(avail_cpu, cpu_target)
            # Ensure we actually allocate some CPU (if fractional CPUs are supported)
            if cpu_req <= 0.0:
                continue

            # Allocate RAM close to the floor to avoid blocking others; add a small slack if possible.
            slack = max(0.0, min(1.0, avail_ram - ram_floor))  # up to +1GB slack if available
            ram_req = ram_floor + slack
            ram_req = min(ram_req, avail_ram)

            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=chosen_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update local available resources for this pool
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            made_progress = True

    return suspensions, assignments
