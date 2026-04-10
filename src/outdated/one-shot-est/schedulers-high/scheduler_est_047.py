# policy_key: scheduler_est_047
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.126415
# generation_seconds: 149.09
# generated_at: 2026-03-31T19:51:05.318526
@register_scheduler_init(key="scheduler_est_047")
def scheduler_est_047_init(s):
    """Priority-aware, estimate-driven scheduler (incremental improvement over naive FIFO).

    Improvements over naive FIFO:
      1) Priority queues: always try QUERY / INTERACTIVE before BATCH.
      2) Right-sizing: allocate RAM based on op.estimate.mem_peak_gb (+ small headroom) instead of "grab all".
      3) OOM feedback loop: if an op OOMs, retry with a larger RAM multiplier next time.
      4) Headroom protection: when high-priority work is waiting, batch assignments must leave some CPU/RAM slack.
      5) Gentle fairness: round-robin within each priority, with a small cap on per-pipeline parallelism per tick.
    """
    # Queues store pipeline_ids (pipelines are stored in s.pipeline_by_id).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.pipeline_by_id = {}
    s.in_queue = set()  # pipeline_ids currently present in some queue

    # Per-(pipeline, op_signature) RAM multiplier, adjusted on OOM.
    s.mem_mult = {}

    # Reverse map to recover pipeline_id from ExecutionResult.ops (filled when we assign).
    s.op_id_to_pipeline_id = {}

    # Pipelines that failed for non-OOM reasons: don't keep retrying forever.
    s.poisoned_pipelines = set()

    # Simple logical clock for aging/limits.
    s.tick = 0

    # Tunables (kept conservative; simulator is deterministic so iterate after first working version).
    s.mem_headroom_gb = 0.25
    s.min_ram_gb = 0.25

    # OOM backoff behavior.
    s.oom_backoff = 1.5
    s.max_mem_mult = 8.0
    s.success_decay = 0.95  # slowly reduce multiplier after success (toward 1.0)

    # CPU sizing: cap batch CPU so it doesn't monopolize a pool and hurt tail latency.
    s.cpu_cap_frac = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 0.5,
    }
    s.min_cpu = 1.0

    # When high-priority work is waiting, keep some headroom so it can start quickly.
    s.high_reserve_cpu_frac = 0.25
    s.high_reserve_ram_frac = 0.25

    # Control how hard we try to pack multiple ops per pool per tick.
    s.max_assignments_per_pool = 2

    # Limit how many ops from the same pipeline we will schedule in one tick (avoids "hogging" artifacts).
    s.max_new_ops_per_pipeline_per_tick = 2


def _is_oom_error(err) -> bool:
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg)


def _safe_get_priority(pipeline):
    pr = getattr(pipeline, "priority", None)
    if pr is None:
        return Priority.BATCH_PIPELINE
    return pr


def _op_signature(op) -> str:
    # Try a few common stable identifiers; fall back to string form.
    for attr in ("op_id", "operator_id", "name", "id"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return str(v)
            except Exception:
                pass
    return str(op)


def _op_mem_est_gb(op) -> float:
    try:
        est = getattr(op, "estimate", None)
        if est is None:
            return 1.0
        v = getattr(est, "mem_peak_gb", None)
        if v is None:
            return 1.0
        v = float(v)
        if v <= 0:
            return 1.0
        return v
    except Exception:
        return 1.0


def _inflight_ops_count(status) -> int:
    # Best-effort: status.state_counts appears to be a dict-like keyed by OperatorState.
    try:
        sc = getattr(status, "state_counts", None)
        if not sc:
            return 0
        return int(sc.get(OperatorState.ASSIGNED, 0)) + int(sc.get(OperatorState.RUNNING, 0)) + int(sc.get(OperatorState.SUSPENDING, 0))
    except Exception:
        return 0


def _pipeline_has_failures(status) -> bool:
    try:
        sc = getattr(status, "state_counts", None)
        if not sc:
            return False
        return int(sc.get(OperatorState.FAILED, 0)) > 0
    except Exception:
        return False


def _ready_ops(status):
    try:
        return status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
    except Exception:
        return []


def _enqueue_pipeline(s, pipeline):
    pid = pipeline.pipeline_id
    if pid in s.poisoned_pipelines:
        return
    s.pipeline_by_id[pid] = pipeline
    if pid in s.in_queue:
        return
    pr = _safe_get_priority(pipeline)
    if pr not in s.queues:
        pr = Priority.BATCH_PIPELINE
    s.queues[pr].append(pid)
    s.in_queue.add(pid)


def _dequeue_one_rr(queue):
    # Round-robin pop from head.
    if not queue:
        return None
    pid = queue.pop(0)
    return pid


def _requeue_rr(queue, pid):
    queue.append(pid)


def _compute_ram_request(s, pool, pid, op, priority) -> float:
    est = _op_mem_est_gb(op)
    sig = _op_signature(op)
    mult = float(s.mem_mult.get((pid, sig), 1.0))
    # Slightly more conservative for interactive-ish work to avoid OOM-induced tail spikes.
    pr_safety = 1.15 if priority in (Priority.QUERY, Priority.INTERACTIVE) else 1.05
    desired = max(s.min_ram_gb, est * mult * pr_safety + s.mem_headroom_gb)
    # Can't exceed pool maximum.
    return min(float(pool.max_ram_pool), desired)


def _compute_cpu_request(s, pool, avail_cpu, priority) -> float:
    cap_frac = float(s.cpu_cap_frac.get(priority, 0.5))
    cap = float(pool.max_cpu_pool) * cap_frac
    cpu = min(float(avail_cpu), float(pool.max_cpu_pool), cap)
    cpu = max(0.0, cpu)
    if cpu <= 0:
        return 0.0
    # Ensure a minimal quantum if any CPU is available.
    return max(float(s.min_cpu), cpu)


def _priority_order_for_pool(s, pool_id, high_waiting: bool) -> list:
    # Multi-pool heuristic: keep pool 0 "fast lane" when possible.
    if s.executor.num_pools <= 1:
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    if pool_id == 0:
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    # If high-priority is waiting, still allow it, but prefer batch on non-fast-lane pools
    # to preserve throughput (headroom rules below prevent batch from fully monopolizing).
    if high_waiting:
        return [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]
    return [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]


@register_scheduler(key="scheduler_est_047")
def scheduler_est_047(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Priority-aware scheduler with estimate-driven RAM sizing and OOM backoff.

    Key behavior:
      - Enqueue pipelines by priority; round-robin within each priority.
      - For each pool, try to place up to N ready ops, preferring high priorities.
      - Batch ops are prevented from consuming the last chunk of CPU/RAM when high-priority work is waiting.
      - On OOM, we increase RAM multiplier for that (pipeline, op) and retry; on success, slowly decay multiplier.
      - No preemption (keeps code safe/robust with minimal API assumptions).
    """
    s.tick += 1
    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Track how many ops we've newly scheduled per pipeline in this tick.
    scheduled_this_tick = {}

    # 1) Incorporate execution feedback (OOM => increase RAM multiplier; success => decay multiplier).
    for r in (results or []):
        try:
            ops = getattr(r, "ops", None) or []
            failed = bool(r.failed())
            is_oom = _is_oom_error(getattr(r, "error", None)) if failed else False

            for op in ops:
                pid = s.op_id_to_pipeline_id.get(id(op), None)
                if pid is None:
                    continue

                sig = _op_signature(op)
                key = (pid, sig)

                if failed:
                    if is_oom:
                        cur = float(s.mem_mult.get(key, 1.0))
                        nxt = min(float(s.max_mem_mult), cur * float(s.oom_backoff))
                        s.mem_mult[key] = nxt
                    else:
                        # Non-OOM failures are treated as "poison" to avoid infinite retries.
                        s.poisoned_pipelines.add(pid)
                else:
                    # Success: gently decay any inflated multiplier.
                    cur = float(s.mem_mult.get(key, 1.0))
                    if cur > 1.0:
                        s.mem_mult[key] = max(1.0, cur * float(s.success_decay))
        except Exception:
            # Keep scheduler robust: ignore malformed results.
            continue

    # 2) Enqueue new pipelines.
    for p in (pipelines or []):
        _enqueue_pipeline(s, p)

    # Early exit: if nothing changed, do nothing.
    if not (pipelines or results):
        return [], []

    # High-priority demand signal (used for headroom protection).
    high_waiting = (len(s.queues.get(Priority.QUERY, [])) + len(s.queues.get(Priority.INTERACTIVE, []))) > 0

    # 3) For each pool, schedule up to s.max_assignments_per_pool ops (packing improves utilization).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        prio_order = _priority_order_for_pool(s, pool_id, high_waiting=high_waiting)

        # Reserve headroom on non-fast-lane pools when high-priority work is waiting.
        # (This prevents "one big batch op grabbed everything" and improves tail latency.)
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if high_waiting and (s.executor.num_pools == 1 or pool_id != 0):
            reserve_cpu = float(pool.max_cpu_pool) * float(s.high_reserve_cpu_frac)
            reserve_ram = float(pool.max_ram_pool) * float(s.high_reserve_ram_frac)

        made = 0
        while made < int(s.max_assignments_per_pool):
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            assigned_any = False

            # Try priorities in desired order for this pool.
            for pr in prio_order:
                q = s.queues.get(pr, [])
                if not q:
                    continue

                # Scan up to len(q) in RR order, looking for a runnable op that fits.
                scanned = 0
                q_len_at_start = len(q)
                while scanned < q_len_at_start and q:
                    pid = _dequeue_one_rr(q)
                    s.in_queue.discard(pid)  # will re-add if we keep it alive

                    pipeline = s.pipeline_by_id.get(pid, None)
                    if pipeline is None or pid in s.poisoned_pipelines:
                        scanned += 1
                        continue

                    status = pipeline.runtime_status()
                    if status.is_pipeline_successful():
                        scanned += 1
                        continue

                    # If pipeline already has failures, we only keep retrying if it isn't poisoned.
                    if _pipeline_has_failures(status) and pid in s.poisoned_pipelines:
                        scanned += 1
                        continue

                    # Per-tick parallelism cap (avoids a single pipeline dominating placements).
                    already_scheduled = int(scheduled_this_tick.get(pid, 0))
                    if already_scheduled >= int(s.max_new_ops_per_pipeline_per_tick):
                        # Keep it in queue for next tick.
                        _requeue_rr(q, pid)
                        s.in_queue.add(pid)
                        scanned += 1
                        continue

                    # Also respect existing inflight work (ASSIGNED/RUNNING).
                    inflight = _inflight_ops_count(status)
                    if inflight + already_scheduled >= 4:
                        _requeue_rr(q, pid)
                        s.in_queue.add(pid)
                        scanned += 1
                        continue

                    ready = _ready_ops(status)
                    if not ready:
                        # Not currently runnable (waiting for deps). Keep in queue.
                        _requeue_rr(q, pid)
                        s.in_queue.add(pid)
                        scanned += 1
                        continue

                    # Take one op (simple; safe).
                    op_list = ready[:1]
                    op = op_list[0]

                    # Compute resource requests.
                    ram_req = _compute_ram_request(s, pool, pid, op, pr)
                    if ram_req <= 0:
                        _requeue_rr(q, pid)
                        s.in_queue.add(pid)
                        scanned += 1
                        continue

                    cpu_req = _compute_cpu_request(s, pool, avail_cpu, pr)
                    if cpu_req <= 0:
                        _requeue_rr(q, pid)
                        s.in_queue.add(pid)
                        scanned += 1
                        continue

                    # Apply headroom rule for batch when high-priority is waiting.
                    # Batch must fit inside (avail - reserve); high-priority can use full avail.
                    eff_avail_cpu = avail_cpu
                    eff_avail_ram = avail_ram
                    if pr == Priority.BATCH_PIPELINE and high_waiting:
                        eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                        eff_avail_ram = max(0.0, avail_ram - reserve_ram)

                    # Ensure request fits.
                    if ram_req > eff_avail_ram or cpu_req > eff_avail_cpu:
                        # Doesn't fit right now; keep in queue and try others.
                        _requeue_rr(q, pid)
                        s.in_queue.add(pid)
                        scanned += 1
                        continue

                    # Create assignment.
                    assignment = Assignment(
                        ops=op_list,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                    assignments.append(assignment)

                    # Reverse map for result attribution (OOM/backoff/poisoning).
                    s.op_id_to_pipeline_id[id(op)] = pid

                    # Update local availability.
                    avail_cpu -= float(cpu_req)
                    avail_ram -= float(ram_req)

                    # Keep pipeline in queue for subsequent ops (RR).
                    _requeue_rr(q, pid)
                    s.in_queue.add(pid)

                    scheduled_this_tick[pid] = already_scheduled + 1
                    made += 1
                    assigned_any = True
                    break  # move to next placement attempt

                if assigned_any:
                    break  # restart priority loop for next placement attempt

            if not assigned_any:
                break

    return suspensions, assignments
