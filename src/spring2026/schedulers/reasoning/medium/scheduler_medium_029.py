# policy_key: scheduler_medium_029
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.106581
# generation_seconds: 121.55
# generated_at: 2026-03-14T02:55:29.985980
@register_scheduler_init(key="scheduler_medium_029")
def scheduler_medium_029_init(s):
    """
    Priority-aware, small-step improvement over naive FIFO.

    Main ideas (kept intentionally simple / low-risk):
      1) Maintain separate queues per priority; always prefer QUERY/INTERACTIVE over BATCH.
      2) Avoid "one op grabs the entire pool" by capping per-op CPU/RAM requests, enabling concurrency.
      3) Basic OOM-aware retry: if an op fails with an OOM-like error, retry it with higher RAM (exponential backoff).
      4) If multiple pools exist, bias pool 0 toward QUERY/INTERACTIVE and other pools toward BATCH (soft isolation).

    Notes:
      - We avoid preemption here because the minimal public interface does not guarantee access to all running containers.
      - We only retry FAILED ops that we explicitly observed failing due to OOM; other failures are treated as fatal.
    """
    # Per-priority waiting queues of Pipeline objects (round-robin within each queue).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Remember OOM-retriable failed ops (by object id(op)).
    s.retriable_failed_ops = set()

    # Remember fatal failed ops (by object id(op)) so we don't retry unknown/non-OOM failures.
    s.fatal_failed_ops = set()

    # Resource hints per operator (by object id(op)).
    # Values: {"ram": float, "cpu": float}
    s.op_hints = {}

    # Avoid scheduling multiple ops from the same pipeline in one tick.
    s._tick_scheduled_pipeline_ids = set()

    # Basic packing/concurrency knobs.
    s.min_cpu_per_op = 1.0  # do not schedule if < 1 vCPU is available
    s.min_ram_per_op = 1.0  # do not schedule if < 1 unit RAM is available
    s.max_assignments_per_pool = 6  # cap to avoid too much scan/packing work per tick


@register_scheduler(key="scheduler_medium_029")
def scheduler_medium_029(s, results, pipelines):
    """
    See init() docstring for overview.
    """
    # -------- helpers (kept local to avoid relying on external imports) --------
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _prio_order_for_pool(pool_id: int, num_pools: int):
        # Soft pool isolation when multiple pools exist:
        #   - pool 0: prefer latency-sensitive work first
        #   - other pools: prefer batch first
        if num_pools <= 1:
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        if pool_id == 0:
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        return [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]

    def _default_caps(priority, pool):
        # Conservative per-op caps to prevent a single op from monopolizing the pool.
        # Caps are "targets"; we still must fit within current availability.
        max_cpu = float(pool.max_cpu_pool)
        max_ram = float(pool.max_ram_pool)

        # Hard CPU caps help reduce head-of-line blocking; RAM caps are softer (OOM hints can exceed).
        if priority == Priority.QUERY:
            cpu_cap = min(8.0, max(1.0, 0.50 * max_cpu))
            ram_cap = max(1.0, 0.25 * max_ram)
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(8.0, max(1.0, 0.60 * max_cpu))
            ram_cap = max(1.0, 0.30 * max_ram)
        else:  # Priority.BATCH_PIPELINE
            cpu_cap = min(max_cpu, max(1.0, 0.80 * max_cpu))
            ram_cap = max(1.0, 0.50 * max_ram)

        return cpu_cap, ram_cap

    def _pipeline_is_fatal(pipeline) -> bool:
        status = pipeline.runtime_status()
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
        if not failed_ops:
            return False

        # If any failed op is explicitly marked fatal, the pipeline is fatal.
        for op in failed_ops:
            if id(op) in s.fatal_failed_ops:
                return True

        # If there are failed ops we did not explicitly mark as OOM-retriable, treat as fatal.
        for op in failed_ops:
            if id(op) not in s.retriable_failed_ops:
                return True

        # All failed ops are OOM-retriable.
        return False

    def _pick_runnable_op(pipeline):
        status = pipeline.runtime_status()

        # Prefer fresh pending work.
        pending_ops = status.get_ops([OperatorState.PENDING], require_parents_complete=True)
        if pending_ops:
            return pending_ops[0]

        # Retry only FAILED ops we have marked as OOM-retriable.
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=True)
        for op in failed_ops:
            if id(op) in s.retriable_failed_ops:
                return op

        return None

    def _req_for_op(op, pipeline_priority, pool, avail_cpu, avail_ram):
        # Start from caps, then apply hints if present (OOM backoff).
        cpu_cap, ram_cap = _default_caps(pipeline_priority, pool)

        hint = s.op_hints.get(id(op), {})
        hinted_cpu = float(hint.get("cpu", cpu_cap))
        hinted_ram = float(hint.get("ram", ram_cap))

        # CPU can be flexed down to current availability; RAM is treated as a hard minimum request.
        cpu_req = min(float(pool.max_cpu_pool), max(1.0, hinted_cpu))
        ram_req = min(float(pool.max_ram_pool), max(1.0, hinted_ram))

        # Respect caps unless hints demand more (e.g., after OOM); caps are for default behavior.
        cpu_req = min(cpu_req, cpu_cap) if "cpu" not in hint else cpu_req
        ram_req = min(ram_req, ram_cap) if "ram" not in hint else ram_req

        # Fit to availability (CPU can shrink; RAM cannot).
        if avail_cpu < s.min_cpu_per_op or avail_ram < s.min_ram_per_op:
            return None
        if ram_req > avail_ram:
            return None
        cpu_req = min(cpu_req, avail_cpu)
        if cpu_req < s.min_cpu_per_op:
            return None

        return cpu_req, ram_req

    def _enqueue_pipeline(p):
        s.queues[p.priority].append(p)

    def _pop_candidate(priority, pool, pool_id, avail_cpu, avail_ram):
        q = s.queues[priority]
        if not q:
            return None

        # Scan up to the current queue length (round-robin); avoid infinite loops.
        n = len(q)
        for _ in range(n):
            pipeline = q.pop(0)

            # Skip if already scheduled this tick (avoid intra-tick duplication across pools).
            if pipeline.pipeline_id in s._tick_scheduled_pipeline_ids:
                q.append(pipeline)
                continue

            status = pipeline.runtime_status()

            # Drop completed pipelines.
            if status.is_pipeline_successful():
                continue

            # Drop fatal pipelines (non-OOM failures).
            if _pipeline_is_fatal(pipeline):
                continue

            op = _pick_runnable_op(pipeline)
            if op is None:
                # Nothing runnable yet; keep it in the queue.
                q.append(pipeline)
                continue

            req = _req_for_op(op, pipeline.priority, pool, avail_cpu, avail_ram)
            if req is None:
                # Can't fit right now; keep it in the queue and try another pipeline.
                q.append(pipeline)
                continue

            # Put pipeline back for future ops after we schedule one op now.
            q.append(pipeline)
            return pipeline, op, req

        return None

    # -------- ingest new pipelines --------
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early-exit when nothing changed that might affect decisions.
    if not pipelines and not results:
        return [], []

    # -------- update OOM/failure hints from results --------
    for r in results:
        if r is None:
            continue

        if r.failed():
            if _is_oom_error(r.error):
                # Mark ops retriable and increase RAM via multiplicative backoff.
                for op in (r.ops or []):
                    op_id = id(op)
                    s.retriable_failed_ops.add(op_id)

                    prev = s.op_hints.get(op_id, {})
                    prev_ram = float(prev.get("ram", r.ram if r.ram is not None else 1.0))
                    last_ram = float(r.ram) if r.ram is not None else prev_ram

                    # Exponential-ish RAM bump; keep at least previous hint.
                    new_ram = max(prev_ram, last_ram * 1.6)

                    # Keep CPU hint as last used (or default later); don't aggressively change it on OOM.
                    if r.cpu is not None:
                        prev["cpu"] = float(r.cpu)
                    prev["ram"] = new_ram
                    s.op_hints[op_id] = prev
            else:
                # Unknown/non-OOM failure: mark ops fatal; do not retry.
                for op in (r.ops or []):
                    op_id = id(op)
                    s.fatal_failed_ops.add(op_id)
                    if op_id in s.retriable_failed_ops:
                        s.retriable_failed_ops.discard(op_id)
        else:
            # Success: clear failure markers and remember last allocation as a mild hint.
            for op in (r.ops or []):
                op_id = id(op)
                s.retriable_failed_ops.discard(op_id)
                s.fatal_failed_ops.discard(op_id)

                prev = s.op_hints.get(op_id, {})
                if r.cpu is not None:
                    prev["cpu"] = float(r.cpu)
                if r.ram is not None and "ram" not in prev:
                    # Only set a baseline if we don't already have an OOM-driven RAM hint.
                    prev["ram"] = float(r.ram)
                s.op_hints[op_id] = prev

    # -------- schedule assignments --------
    suspensions = []
    assignments = []

    s._tick_scheduled_pipeline_ids = set()

    num_pools = s.executor.num_pools
    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < s.min_cpu_per_op or avail_ram < s.min_ram_per_op:
            continue

        pool_prio_order = _prio_order_for_pool(pool_id, num_pools)
        assigned_here = 0

        # Greedy packing: keep placing ops until we hit a cap or run out of resources.
        while assigned_here < s.max_assignments_per_pool:
            if avail_cpu < s.min_cpu_per_op or avail_ram < s.min_ram_per_op:
                break

            picked = None
            for pr in pool_prio_order:
                picked = _pop_candidate(pr, pool, pool_id, avail_cpu, avail_ram)
                if picked is not None:
                    break

            if picked is None:
                break

            pipeline, op, (cpu_req, ram_req) = picked

            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update local availability for packing within this scheduler tick.
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)
            assigned_here += 1
            s._tick_scheduled_pipeline_ids.add(pipeline.pipeline_id)

    return suspensions, assignments
