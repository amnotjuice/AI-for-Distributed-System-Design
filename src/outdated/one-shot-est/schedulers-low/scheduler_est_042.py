# policy_key: scheduler_est_042
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051526
# generation_seconds: 38.79
# generated_at: 2026-04-01T00:12:45.545834
@register_scheduler_init(key="scheduler_est_042")
def scheduler_est_042_init(s):
    """
    Priority-aware FIFO with small, low-risk improvements over naive FIFO:
      1) Separate queues per priority (QUERY/INTERACTIVE/BATCH) to reduce tail latency.
      2) RAM sizing uses operator memory estimate as a floor + headroom to avoid OOM retries.
      3) OOM backoff: if an op fails with OOM, bump its future RAM floor multiplicatively.
      4) Simple weighted round-robin across priorities to prevent total batch starvation.
    """
    # Per-priority waiting queues of Pipeline objects (FIFO within each priority).
    s.waiting_by_pri = {}

    # Per-operator memory multiplier after OOM (keyed by python object id(op)).
    s.op_mem_mult = {}

    # Simple WRR pattern: more opportunities for latency-sensitive work.
    s.wrr_pattern = []

    # Default minimum RAM (GB) when no estimate is available.
    s.default_mem_floor_gb = 1.0

    # Headroom factor on top of the floor (extra RAM is "free" in the model; helps reduce OOMs).
    s.mem_headroom = 1.20

    # OOM retry multiplier (compounded per OOM).
    s.oom_retry_mult = 1.60

    # Cap multiplier to avoid exploding RAM requests indefinitely.
    s.oom_retry_mult_cap = 16.0

    # Initialize known priority queues and WRR pattern.
    # (We avoid imports; rely on Priority enum being available in the environment.)
    s.waiting_by_pri[Priority.QUERY] = []
    s.waiting_by_pri[Priority.INTERACTIVE] = []
    s.waiting_by_pri[Priority.BATCH_PIPELINE] = []

    # Weighted pattern: QUERY first, then INTERACTIVE twice, then BATCH.
    s.wrr_pattern = [Priority.QUERY, Priority.INTERACTIVE, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Rolling index into WRR pattern (global across pools/ticks).
    s.wrr_idx = 0


@register_scheduler(key="scheduler_est_042")
def scheduler_est_042_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Enqueue new pipelines by priority.
      - Update OOM multipliers from failed execution results.
      - For each pool, greedily assign runnable operators while resources remain,
        choosing pipelines using a weighted round-robin across priority queues.
    """
    # Local helper: identify OOM-like failures from result.error text.
    def _is_oom_error(err):
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    # Local helper: get memory floor for an op (GB), including OOM backoff and headroom.
    def _mem_request_gb(op, pool_max_ram_gb):
        est = None
        try:
            est = op.estimate.mem_peak_gb
        except Exception:
            est = None

        base_floor = s.default_mem_floor_gb if (est is None or est <= 0) else float(est)
        mult = s.op_mem_mult.get(id(op), 1.0)
        floor = base_floor * mult

        # Add headroom; clamp to pool max.
        req = floor * s.mem_headroom
        if req > pool_max_ram_gb:
            req = pool_max_ram_gb
        # Ensure strictly positive.
        if req <= 0:
            req = min(pool_max_ram_gb, s.default_mem_floor_gb)
        return req

    # Local helper: pick CPU request based on priority and pool size.
    # We keep it simple: higher priority gets a larger slice to reduce latency,
    # but we avoid always taking the entire pool to allow some concurrency.
    def _cpu_request(priority, pool, avail_cpu):
        max_cpu = pool.max_cpu_pool
        # Targets are fractions of pool capacity (rounded down by float->implicit usage).
        if priority == Priority.QUERY:
            target = max(1.0, 0.80 * max_cpu)
        elif priority == Priority.INTERACTIVE:
            target = max(1.0, 0.70 * max_cpu)
        else:
            target = max(1.0, 0.50 * max_cpu)

        # Never request more than available.
        return min(avail_cpu, target)

    # Enqueue new pipelines.
    for p in pipelines:
        pri = p.priority
        if pri not in s.waiting_by_pri:
            s.waiting_by_pri[pri] = []
        s.waiting_by_pri[pri].append(p)

    # Update OOM multipliers based on results.
    # We cannot reliably map results back to pipelines without pipeline_id on result,
    # but we can increase RAM for the specific op objects that failed.
    for r in results:
        if not r.failed():
            continue
        if _is_oom_error(r.error):
            for op in (r.ops or []):
                cur = s.op_mem_mult.get(id(op), 1.0)
                nxt = min(cur * s.oom_retry_mult, s.oom_retry_mult_cap)
                s.op_mem_mult[id(op)] = nxt

    # Fast-path: if nothing changed, do nothing.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Local helper: in a given priority queue, find the first pipeline with a runnable op that fits.
    def _find_and_pop_fit_pipeline(queue, avail_cpu, avail_ram, pool):
        """
        Returns (pipeline, op_list, cpu_req, ram_req) or (None, None, None, None).
        Mutates the queue by removing the selected pipeline; caller is responsible for re-adding it.
        """
        if not queue:
            return None, None, None, None

        n = len(queue)
        for i in range(n):
            pipeline = queue.pop(0)

            status = pipeline.runtime_status()
            if status.is_pipeline_successful():
                # Drop completed pipelines.
                continue

            # If there are failures, we only keep retrying if ops are re-assignable (FAILED is in ASSIGNABLE_STATES).
            # We'll avoid permanently dropping here; Eudoxia will surface repeated failures via results.

            # Select exactly one runnable op to keep scheduling atomic and predictable.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable yet; keep it for later.
                queue.append(pipeline)
                continue

            op = op_list[0]
            ram_req = _mem_request_gb(op, pool.max_ram_pool)
            cpu_req = _cpu_request(pipeline.priority, pool, avail_cpu)

            # Must fit both resources.
            if cpu_req <= 0 or ram_req <= 0 or cpu_req > avail_cpu or ram_req > avail_ram:
                queue.append(pipeline)
                continue

            # Found a fit.
            return pipeline, op_list, cpu_req, ram_req

        return None, None, None, None

    # For each pool, assign as many ops as we can.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Greedy fill loop.
        # We iterate while there is enough resource to run at least something.
        # (The checks inside handle exact fit.)
        guard = 0
        while avail_cpu > 0 and avail_ram > 0:
            guard += 1
            if guard > 10_000:
                # Safety against infinite loops due to unexpected simulator state.
                break

            picked = None

            # Try priorities according to WRR pattern starting from rolling index.
            pat = s.wrr_pattern
            start = s.wrr_idx
            for k in range(len(pat)):
                pri = pat[(start + k) % len(pat)]
                q = s.waiting_by_pri.get(pri, [])
                pipeline, op_list, cpu_req, ram_req = _find_and_pop_fit_pipeline(q, avail_cpu, avail_ram, pool)
                if pipeline is None:
                    continue

                # Update rolling WRR index for next pick.
                s.wrr_idx = (start + k + 1) % len(pat)

                # Assign.
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

                # Requeue the pipeline for its next operators.
                s.waiting_by_pri[pipeline.priority].append(pipeline)

                # Update available resources (pool availability will also update in the simulator,
                # but we keep a local view to decide multiple assignments in this tick).
                avail_cpu -= cpu_req
                avail_ram -= ram_req

                picked = True
                break

            if not picked:
                # No fitting runnable work in any priority for this pool.
                break

    return suspensions, assignments
