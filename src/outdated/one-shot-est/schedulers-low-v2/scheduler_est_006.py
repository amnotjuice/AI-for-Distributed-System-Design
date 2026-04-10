# policy_key: scheduler_est_006
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046431
# generation_seconds: 40.90
# generated_at: 2026-04-01T13:39:47.936455
@register_scheduler_init(key="scheduler_est_006")
def scheduler_est_006_init(s):
    """Priority-aware, modestly more concurrent scheduler than naive FIFO.

    Improvements over the naive example:
      1) Priority queues (QUERY/INTERACTIVE before BATCH_PIPELINE) to reduce tail latency.
      2) Avoid allocating the entire pool to one op; instead, cap per-op CPU to allow concurrency.
      3) Use op.estimate.mem_peak_gb conservatively (+headroom) and retry with larger RAM on OOM.
      4) Do not drop failed pipelines; retry FAILED ops (ASSIGNABLE_STATES) with bounded attempts.
    """
    # Separate FIFO queues per priority to improve latency for high-priority work.
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Remember RAM overrides after OOMs (keyed by operator identity).
    s.op_ram_override_gb = {}  # op_key -> ram_gb

    # Bound retries to avoid infinite thrash.
    s.op_retry_count = {}  # op_key -> int

    # Small defaults; simulator uses GB units for RAM.
    s.default_ram_gb = 1.0

    # Headroom factor on memory estimates to reduce OOM probability.
    s.mem_headroom_factor = 1.25

    # Per-op CPU caps by priority to increase concurrency and reduce queueing latency.
    s.cpu_cap_by_prio = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 3.0,
        Priority.BATCH_PIPELINE: 2.0,
    }

    # Minimum CPU to give an op if we schedule it at all.
    s.min_cpu = 1.0

    # Minimum RAM to give an op if we schedule it at all.
    s.min_ram_gb = 0.5

    # Retry policy
    s.max_oom_retries = 3


@register_scheduler(key="scheduler_est_006")
def scheduler_est_006_scheduler(s, results, pipelines):
    """
    Priority-aware, concurrent scheduler with OOM backoff.

    Core decision rules:
      - Enqueue incoming pipelines into per-priority FIFO queues.
      - Process execution results; on OOM failure, double RAM for that operator on retry.
      - For each pool, repeatedly assign ready operators while resources remain:
          * choose from highest-priority queue first
          * pick first assignable op whose parents are complete
          * allocate CPU up to a per-priority cap (not full pool) to increase concurrency
          * allocate RAM using override > estimate(+headroom) > default
    """
    # --- helpers (defined inside to avoid imports/global deps) ---
    def _prio_order():
        # Highest first. (QUERY vs INTERACTIVE ordering is a product choice; keep QUERY first here.)
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _op_key(op):
        # Prefer a stable explicit ID if present; else fall back to object identity.
        return getattr(op, "op_id", id(op))

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)

    def _next_ready_op_from_pipeline(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return None
        # Only schedule ops whose parents are complete; take at most one op at a time per pipeline
        # to avoid over-allocating to a single DAG and to improve inter-pipeline latency fairness.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        return op_list[0]

    def _pipeline_done_or_terminal(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If there are FAILED ops we still want to retry (ASSIGNABLE_STATES includes FAILED),
        # so do not treat failures as terminal here.
        return False

    def _desired_ram_gb(op):
        k = _op_key(op)
        if k in s.op_ram_override_gb:
            return float(s.op_ram_override_gb[k])

        # Conservative use of estimator: treat as a hint and add headroom.
        est = getattr(op, "estimate", None)
        est_mem = None
        if est is not None:
            est_mem = getattr(est, "mem_peak_gb", None)
        if est_mem is not None:
            try:
                est_mem = float(est_mem)
            except Exception:
                est_mem = None

        if est_mem is None:
            return float(s.default_ram_gb)

        # Add headroom and enforce minimum.
        return max(float(s.min_ram_gb), float(est_mem) * float(s.mem_headroom_factor))

    def _desired_cpu(prio, avail_cpu):
        cap = float(s.cpu_cap_by_prio.get(prio, 2.0))
        return max(float(s.min_cpu), min(float(avail_cpu), cap))

    # --- ingest new pipelines ---
    for p in pipelines:
        # Ensure the priority exists in our dict even if simulator adds new ones.
        if p.priority not in s.wait_q:
            s.wait_q[p.priority] = []
        s.wait_q[p.priority].append(p)

    # --- process results for OOM backoff ---
    for r in results:
        if r is None:
            continue
        if not r.failed():
            continue

        # Update RAM override only for OOM-like failures.
        if not _is_oom_error(getattr(r, "error", None)):
            continue

        # Increase RAM for each failed op (usually one op per assignment).
        for op in getattr(r, "ops", []) or []:
            k = _op_key(op)
            prev = s.op_ram_override_gb.get(k, None)
            # Start from the RAM we actually tried (r.ram), then grow.
            tried = getattr(r, "ram", None)
            try:
                tried = float(tried) if tried is not None else None
            except Exception:
                tried = None

            if tried is None:
                # Fallback to estimate/default if we don't have tried RAM.
                tried = _desired_ram_gb(op)

            # Bound retries.
            cnt = int(s.op_retry_count.get(k, 0)) + 1
            s.op_retry_count[k] = cnt
            if cnt > int(s.max_oom_retries):
                # Stop escalating after max retries; keep the last known override.
                if prev is None:
                    s.op_ram_override_gb[k] = tried
                continue

            # Exponential backoff; ensure monotonic increase.
            new_ram = tried * 2.0
            if prev is not None:
                try:
                    new_ram = max(float(prev), float(new_ram))
                except Exception:
                    pass
            s.op_ram_override_gb[k] = new_ram

    # If nothing new and no results, avoid scanning queues.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # --- scheduling loop per pool ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Keep assigning while we have enough resources to be useful.
        while avail_cpu >= float(s.min_cpu) and avail_ram >= float(s.min_ram_gb):
            chosen_pipeline = None
            chosen_op = None

            # Select next runnable op from highest priority queues first.
            # We rotate by popping from front and re-appending to maintain FIFO while
            # skipping blocked/completed pipelines without starving others.
            for prio in _prio_order():
                q = s.wait_q.get(prio, [])
                if not q:
                    continue

                # Scan up to current queue length to find a runnable pipeline.
                qlen = len(q)
                for _ in range(qlen):
                    p = q.pop(0)

                    # Drop completed pipelines from the queue.
                    if _pipeline_done_or_terminal(p):
                        continue

                    op = _next_ready_op_from_pipeline(p)
                    if op is None:
                        # Not runnable now; keep it in the same queue.
                        q.append(p)
                        continue

                    chosen_pipeline = p
                    chosen_op = op
                    # Keep pipeline in queue for subsequent ops; re-append to preserve fairness.
                    q.append(p)
                    break

                if chosen_op is not None:
                    break

            if chosen_op is None:
                # Nothing runnable in any queue for this pool at the moment.
                break

            prio = chosen_pipeline.priority
            cpu_req = _desired_cpu(prio, avail_cpu)
            ram_req = _desired_ram_gb(chosen_op)

            # If the desired RAM doesn't fit, try a smaller (but safe-ish) fallback:
            # For high priority, we prefer to wait rather than run under-provisioned.
            # For batch, we can try to squeeze only if we still meet minimum RAM.
            if ram_req > avail_ram:
                if prio in (Priority.QUERY, Priority.INTERACTIVE):
                    # Stop scheduling on this pool; maybe other pools have RAM.
                    break
                # Batch: allow scheduling with whatever is left if it meets minimum,
                # to keep throughput up while not risking extremely tiny allocations.
                if avail_ram < float(s.min_ram_gb):
                    break
                ram_req = avail_ram

            # CPU fitting: if cap doesn't fit, reduce; if still below min, stop.
            if cpu_req > avail_cpu:
                cpu_req = avail_cpu
            if cpu_req < float(s.min_cpu):
                break

            # Create assignment (single op per assignment).
            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            # Decrement available resources in this pool for this scheduling tick.
            avail_cpu -= cpu_req
            avail_ram -= ram_req

    return suspensions, assignments
