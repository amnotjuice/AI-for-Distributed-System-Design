# policy_key: scheduler_est_045
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.097446
# generation_seconds: 95.03
# generated_at: 2026-03-31T19:46:30.273870
@register_scheduler_init(key="scheduler_est_045")
def scheduler_est_045_init(s):
    """Priority-aware, estimate-driven scheduler (incremental improvement over naive FIFO).

    Main improvements vs naive:
      - Separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
      - Memory sizing uses op.estimate.mem_peak_gb with a safety factor; on OOM, increase factor and retry.
      - If multiple pools exist: treat pool 0 as a "latency pool" (prefer QUERY/INTERACTIVE),
        remaining pools as "throughput pools" (prefer BATCH) with limited spillover for latency work.
      - Pack multiple BATCH operators per pool (instead of at most one) to improve utilization without
        hurting latency in the latency pool.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator adaptive memory headroom (keyed by operator object identity).
    s.op_mem_factor = {}          # op_id -> multiplier on estimate.mem_peak_gb
    s.op_fail_count = {}          # op_id -> number of failures observed
    s.op_last_ram = {}            # op_id -> last assigned RAM (from results)

    # Policy knobs (kept simple; can be tuned through simulation feedback).
    s.ticks = 0
    s.latency_pool_id = 0

    s.base_mem_safety = 1.20      # initial headroom on top of estimate.mem_peak_gb
    s.oom_backoff = 1.60          # multiplier increase on OOM
    s.success_decay = 0.97        # slowly relax over-reservation after successes
    s.max_mem_factor = 8.0
    s.min_ram_gb = 0.25

    s.max_retries = 3             # after this, avoid repeatedly scheduling the same failing op

    # CPU sizing knobs
    s.batch_cpu_per_op = 2.0      # pack batch ops; give each a modest slice
    s.batch_max_concurrent = 16   # per pool, max number of batch ops to pack in a single tick

    # Spillover: allow some latency work on batch pools if backlog is high.
    s.spill_interactive_threshold = 3
    s.spill_query_threshold = 1


def _is_oom_error(err):
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)


def _get_est_mem_gb(op):
    est = getattr(op, "estimate", None)
    if est is not None:
        v = getattr(est, "mem_peak_gb", None)
        if v is not None:
            return float(v)
    # Fallbacks (best-effort)
    v = getattr(op, "mem_peak_gb", None)
    if v is not None:
        return float(v)
    return None


def _pick_queue_order_for_pool(num_pools, pool_id, latency_pool_id, spill_q, spill_i):
    # Pool 0 (latency pool): always prioritize QUERY/INTERACTIVE first.
    if num_pools <= 1 or pool_id == latency_pool_id:
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Throughput pools: prefer BATCH, allow spillover if backlog exists.
    order = [Priority.BATCH_PIPELINE]
    if spill_q:
        order.insert(0, Priority.QUERY)
    if spill_i:
        # Put INTERACTIVE after QUERY if both spill.
        if Priority.QUERY in order:
            order.insert(order.index(Priority.QUERY) + 1, Priority.INTERACTIVE)
        else:
            order.insert(0, Priority.INTERACTIVE)
    return order


def _cpu_request(s, priority, remaining_cpu):
    if remaining_cpu <= 0:
        return 0.0

    # Latency-sensitive: use whatever is available (finish sooner, reduce tail).
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        return max(1.0, float(remaining_cpu))

    # Batch: give a small slice to allow packing multiple ops.
    return max(1.0, min(float(remaining_cpu), float(s.batch_cpu_per_op)))


def _ram_request(s, op, pool, remaining_ram, oom_hint=False):
    est_gb = _get_est_mem_gb(op)
    if est_gb is None:
        # Unknown estimate: be conservative but not huge.
        est_gb = max(s.min_ram_gb, min(1.0, float(pool.max_ram_pool)))

    op_key = id(op)
    factor = float(s.op_mem_factor.get(op_key, s.base_mem_safety))

    ram = est_gb * factor
    ram = max(ram, float(s.min_ram_gb))

    # If we have last assigned RAM and got an OOM, ensure a meaningful step up.
    if oom_hint:
        last = float(s.op_last_ram.get(op_key, 0.0))
        if last > 0:
            ram = max(ram, last * 1.25)

    # Cap by pool max.
    ram = min(ram, float(pool.max_ram_pool))

    # Must fit within remaining pool RAM to assign now.
    if ram > remaining_ram:
        return None
    return float(ram)


def _pop_next_fit(s, q, pool, remaining_cpu, remaining_ram, oom_suspects):
    """Round-robin within a priority queue: find a pipeline with a ready op that fits."""
    if not q:
        return None

    n = len(q)
    for _ in range(n):
        pipeline = q.pop(0)
        status = pipeline.runtime_status()

        # Drop completed pipelines.
        if status.is_pipeline_successful():
            continue

        # Find a single ready op (atomic scheduling unit).
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            q.append(pipeline)
            continue

        op = op_list[0]
        op_key = id(op)

        # Avoid infinite thrash on repeatedly failing operators.
        if s.op_fail_count.get(op_key, 0) >= s.max_retries:
            q.append(pipeline)
            continue

        cpu = _cpu_request(s, pipeline.priority, remaining_cpu)
        if cpu <= 0 or cpu > remaining_cpu:
            q.append(pipeline)
            continue

        ram = _ram_request(
            s,
            op,
            pool,
            remaining_ram,
            oom_hint=(op_key in oom_suspects),
        )
        if ram is None or ram <= 0 or ram > remaining_ram:
            q.append(pipeline)
            continue

        return pipeline, op, cpu, ram

    return None


@register_scheduler(key="scheduler_est_045")
def scheduler_est_045(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Priority-aware scheduling with estimate-based RAM sizing and OOM backoff.

    Scheduling flow per tick:
      1) Ingest new pipelines into per-priority FIFO queues.
      2) Update per-op memory factors from execution results (increase on OOM, decay on success).
      3) For each pool:
           - If latency pool: schedule at most one QUERY/INTERACTIVE op first (big slice),
             then optionally pack BATCH if no latency work is waiting.
           - If throughput pool: pack multiple BATCH ops; allow limited spillover for latency work
             if backlog thresholds are exceeded.
    """
    s.ticks += 1

    # Enqueue new arrivals (no duplicates; we only enqueue what arrives this tick).
    for p in pipelines:
        s.queues[p.priority].append(p)

    # Track which operators just OOM'd to bias next RAM request upward.
    oom_suspects = set()

    # Learn from previous tick's results.
    for r in results:
        failed = r.failed()
        err_is_oom = _is_oom_error(getattr(r, "error", None)) if failed else False

        # Record last assigned resources (for better OOM step-ups).
        for op in getattr(r, "ops", []) or []:
            k = id(op)
            if getattr(r, "ram", None) is not None:
                s.op_last_ram[k] = float(r.ram)

            if failed:
                s.op_fail_count[k] = int(s.op_fail_count.get(k, 0) + 1)

                cur = float(s.op_mem_factor.get(k, s.base_mem_safety))
                if err_is_oom:
                    oom_suspects.add(k)
                    s.op_mem_factor[k] = min(s.max_mem_factor, max(cur, cur * s.oom_backoff))
                else:
                    # Non-OOM failure: don't blindly balloon memory too much, but add a small cushion.
                    s.op_mem_factor[k] = min(s.max_mem_factor, max(cur, cur * 1.10))
            else:
                # Success: slowly relax toward baseline to avoid permanent over-reservation.
                cur = float(s.op_mem_factor.get(k, s.base_mem_safety))
                s.op_mem_factor[k] = max(s.base_mem_safety, cur * s.success_decay)

    # If nothing changed and no queued work, exit.
    if not pipelines and not results:
        if not s.queues[Priority.QUERY] and not s.queues[Priority.INTERACTIVE] and not s.queues[Priority.BATCH_PIPELINE]:
            return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    num_pools = s.executor.num_pools

    # Spillover decision (global per tick).
    spill_query = len(s.queues[Priority.QUERY]) >= s.spill_query_threshold
    spill_interactive = len(s.queues[Priority.INTERACTIVE]) >= s.spill_interactive_threshold

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        remaining_cpu = float(pool.avail_cpu_pool)
        remaining_ram = float(pool.avail_ram_pool)

        if remaining_cpu <= 0 or remaining_ram <= 0:
            continue

        queue_order = _pick_queue_order_for_pool(
            num_pools=num_pools,
            pool_id=pool_id,
            latency_pool_id=s.latency_pool_id,
            spill_q=spill_query,
            spill_i=spill_interactive,
        )

        # Latency pool behavior: do ONE latency-sensitive op if available, then stop
        # (keeps tail latency low and avoids diluting resources).
        if num_pools == 1 or pool_id == s.latency_pool_id:
            scheduled_latency = False
            for prio in queue_order:
                if prio not in (Priority.QUERY, Priority.INTERACTIVE):
                    continue
                pick = _pop_next_fit(s, s.queues[prio], pool, remaining_cpu, remaining_ram, oom_suspects)
                if pick is None:
                    continue
                pipeline, op, cpu, ram = pick

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )
                # Round-robin fairness across pipelines.
                s.queues[prio].append(pipeline)
                remaining_cpu -= float(cpu)
                remaining_ram -= float(ram)
                scheduled_latency = True
                break

            # If we didn't schedule latency work here, we can use the latency pool for batch packing.
            if scheduled_latency:
                continue

        # Throughput behavior (and idle latency pool): pack multiple ops, preferring batch.
        packed = 0
        max_pack = int(s.batch_max_concurrent)
        while packed < max_pack and remaining_cpu > 0 and remaining_ram > 0:
            picked_any = False

            for prio in queue_order:
                q = s.queues.get(prio)
                if not q:
                    continue

                pick = _pop_next_fit(s, q, pool, remaining_cpu, remaining_ram, oom_suspects)
                if pick is None:
                    continue

                pipeline, op, cpu, ram = pick
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )
                # Round-robin fairness across pipelines.
                q.append(pipeline)

                remaining_cpu -= float(cpu)
                remaining_ram -= float(ram)
                packed += 1
                picked_any = True
                break

            if not picked_any:
                break

    return suspensions, assignments
