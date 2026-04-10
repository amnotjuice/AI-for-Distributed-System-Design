# policy_key: scheduler_est_024
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040186
# generation_seconds: 40.55
# generated_at: 2026-04-01T00:00:46.017402
@register_scheduler_init(key="scheduler_est_024")
def scheduler_est_024_init(s):
    """
    Priority-aware, estimate-aware scheduler (small, incremental improvement over naive FIFO).

    Key ideas:
      1) Separate waiting queues per priority and always prefer higher-priority work.
      2) Right-size per-op RAM using op.estimate.mem_peak_gb as a FLOOR; retry failures with exponential RAM bump.
      3) Avoid "give the whole pool to one op" by using small CPU slices, enabling concurrency and reducing tail latency.

    Notes:
      - No preemption yet (keeps behavior stable and easy to validate).
      - Retries FAILED ops (assumes failures are often OOM; bumps RAM on any failure to be conservative).
    """
    s.tick = 0

    # Per-priority FIFO queues of pipelines.
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Pipeline arrival ticks (used for light aging hooks / debugging if needed).
    s.pipeline_arrival_tick = {}

    # Failure-driven RAM bump tracking per (pipeline_id, op_object_id).
    s.op_fail_count = {}

    # Defaults / knobs.
    s.default_ram_gb = 4.0

    # CPU slice sizes per priority (higher priority gets slightly larger slices).
    s.cpu_slice_by_prio = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 6.0,
        Priority.BATCH_PIPELINE: 4.0,
    }

    # Limit how many assignments we emit per pool per tick to avoid over-scheduling churn.
    s.max_assignments_per_pool_per_tick = 8

    # How many candidate pipelines we will scan to find a "fits in remaining resources" op.
    s.max_scan_per_pool = 64


def _prio_order():
    # Highest to lowest.
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _pipeline_is_done_or_terminal(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # We intentionally do NOT treat failures as terminal; we retry with larger RAM.
    return False


def _get_next_assignable_op(pipeline):
    status = pipeline.runtime_status()
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    if not op_list:
        return None
    return op_list[0]


def _op_mem_floor_gb(s, op, pipeline_id):
    # Use the estimator as a RAM floor. Extra RAM is "free" performance-wise but constrained by pool capacity.
    est = None
    try:
        est = op.estimate.mem_peak_gb
    except Exception:
        est = None

    base = s.default_ram_gb if (est is None) else float(est)
    base = max(base, s.default_ram_gb)

    key = (pipeline_id, id(op))
    fails = s.op_fail_count.get(key, 0)

    # Exponential bump on failure (2^fails), with a soft cap to avoid runaway.
    bump = 2.0 ** min(fails, 10)
    return base * bump


def _record_failures(s, result):
    # Conservative: treat any failure as a signal to increase RAM next time.
    # If result.ops contains multiple ops, bump all of them.
    try:
        if not result.failed():
            return
    except Exception:
        return

    pipeline_id = getattr(result, "pipeline_id", None)
    # If pipeline_id isn't present on result, we can only bump by op object id if available;
    # however Assignment sets pipeline_id; simulator usually wires it through results.
    # We still attempt to use pipeline_id; if missing, fall back to a less specific key space.
    for op in getattr(result, "ops", []) or []:
        key = (pipeline_id, id(op))
        s.op_fail_count[key] = s.op_fail_count.get(key, 0) + 1


def _enqueue_pipeline(s, pipeline):
    prio = pipeline.priority
    if prio not in s.waiting_by_prio:
        # Unknown priority: treat as lowest.
        prio = Priority.BATCH_PIPELINE
    s.waiting_by_prio[prio].append(pipeline)
    s.pipeline_arrival_tick.setdefault(pipeline.pipeline_id, s.tick)


@register_scheduler(key="scheduler_est_024")
def scheduler_est_024(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Main scheduling step:
      - ingest new pipelines
      - process results to update failure-driven RAM bumps
      - for each pool, schedule multiple ops using small CPU slices, prioritizing high-priority queues
      - only schedule ops that fit remaining pool CPU/RAM (simple packing)
    """
    s.tick += 1

    # Update internal state from execution results.
    for r in results:
        _record_failures(s, r)

    # Enqueue new pipelines by priority.
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Early exit if nothing to do.
    if not pipelines and not results:
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Garbage-collect completed pipelines from queues.
    for prio in list(s.waiting_by_prio.keys()):
        if not s.waiting_by_prio[prio]:
            continue
        new_q = []
        for pl in s.waiting_by_prio[prio]:
            if _pipeline_is_done_or_terminal(pl):
                s.pipeline_arrival_tick.pop(pl.pipeline_id, None)
                continue
            new_q.append(pl)
        s.waiting_by_prio[prio] = new_q

    # For each pool, pack as many runnable ops as we can.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        emitted = 0
        scans = 0

        # Simple "scan and place" loop: keep trying to find an op that fits.
        while (
            emitted < s.max_assignments_per_pool_per_tick
            and avail_cpu > 0.0
            and avail_ram > 0.0
            and scans < s.max_scan_per_pool
        ):
            scans += 1
            picked = None  # (priority, pipeline, op, ram_need, cpu_need)

            # Choose the first (highest priority) pipeline with an assignable op that fits.
            for prio in _prio_order():
                q = s.waiting_by_prio.get(prio, [])
                if not q:
                    continue

                # Scan a few pipelines within this priority class to find something that fits.
                # This avoids head-of-line blocking when the first pipeline needs too much RAM.
                scan_n = min(len(q), 8)
                for idx in range(scan_n):
                    pipeline = q.pop(0)

                    if _pipeline_is_done_or_terminal(pipeline):
                        s.pipeline_arrival_tick.pop(pipeline.pipeline_id, None)
                        continue

                    op = _get_next_assignable_op(pipeline)
                    if op is None:
                        # Not ready yet; keep it in the queue.
                        q.append(pipeline)
                        continue

                    ram_need = _op_mem_floor_gb(s, op, pipeline.pipeline_id)

                    # Bound RAM request by pool capacity; if it still doesn't fit remaining, skip for now.
                    ram_need = min(ram_need, float(pool.max_ram_pool))
                    if ram_need > avail_ram:
                        q.append(pipeline)
                        continue

                    # CPU slice per priority, but never exceed remaining pool CPU.
                    cpu_slice = float(s.cpu_slice_by_prio.get(prio, 4.0))
                    cpu_need = min(cpu_slice, avail_cpu)

                    # If even 1 CPU can't be allocated, stop for this pool.
                    if cpu_need <= 0.0:
                        q.insert(0, pipeline)
                        picked = None
                        break

                    picked = (prio, pipeline, op, ram_need, cpu_need)
                    # Round-robin within same priority: put pipeline back after scheduling one op.
                    q.append(pipeline)
                    break

                if picked is not None:
                    break

            if picked is None:
                # No runnable op fits remaining resources in this pool right now.
                break

            prio, pipeline, op, ram_need, cpu_need = picked

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_need,
                    ram=ram_need,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )
            emitted += 1
            avail_cpu -= cpu_need
            avail_ram -= ram_need

    return suspensions, assignments
