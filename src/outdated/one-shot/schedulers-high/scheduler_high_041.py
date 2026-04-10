# policy_key: scheduler_high_041
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.150950
# generation_seconds: 177.03
# generated_at: 2026-03-14T04:18:01.936102
@register_scheduler_init(key="scheduler_high_041")
def scheduler_high_041_init(s):
    """Priority-aware, reservation-based scheduler with simple OOM retry/backoff.

    Incremental improvements over naive FIFO:
      1) Separate per-priority queues (QUERY > INTERACTIVE > BATCH) to reduce tail latency.
      2) Avoid letting BATCH consume the last headroom when higher-priority work is waiting
         (soft reservation of CPU/RAM per pool).
      3) If an operator fails with OOM, remember a larger RAM hint and allow retry; otherwise
         treat FAILED ops as terminal and drop the pipeline to avoid infinite retries.
      4) Lightweight fairness: batch jobs can slowly "invade" reserved headroom as they age.
    """
    from collections import deque

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Track active pipeline IDs to avoid duplicate enqueue.
    s.active_pipeline_ids = set()

    # Pipelines we decided to drop (e.g., non-OOM failure).
    s.dead_pipeline_ids = set()

    # Per-operator retry eligibility and RAM hints (keyed by repr(op) for stability).
    s.retryable_ops = set()
    s.op_ram_hint = {}        # op_key -> ram
    s.op_retry_count = {}     # op_key -> int

    # Tuning knobs (kept conservative; complexity can be increased later).
    s.reserve_frac = 0.25           # reserve this fraction of each pool for high priority (soft)
    s.max_oom_retries = 4
    s.batch_age_limit_ticks = 200   # after this, batch can partially eat into reserved headroom

    s.tick = 0
    s.pipeline_first_seen_tick = {}  # pipeline_id -> tick


def _op_key(op):
    # Prefer stable identifiers if present; otherwise fall back to repr(op).
    for attr in ("op_id", "id", "name", "operator_id"):
        v = getattr(op, attr, None)
        if v is not None:
            return f"{attr}:{v}"
    return repr(op)


def _is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("cuda oom" in e)


def _pipeline_is_terminal(s, pipeline):
    # Terminal means: successful OR contains non-retryable FAILED ops.
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True

    # If it has FAILED ops, only allow continuing if every FAILED op is marked retryable.
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    if not failed_ops:
        return False

    for op in failed_ops:
        if _op_key(op) not in s.retryable_ops:
            return True
    return False


def _pick_next_eligible_op(s, pipeline):
    """Pick a single eligible operator for assignment.

    Eligible:
      - PENDING, or
      - FAILED only if we previously observed it as OOM-retryable.
    """
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None

    for op in ops:
        # If the op is FAILED, only retry when explicitly marked retryable.
        # (We can't reliably distinguish failure causes from runtime_status alone.)
        # OperatorState may be accessible via op.state or via status; we conservatively use retryable set:
        # if it was FAILED and not retryable, skip it.
        key = _op_key(op)
        # Heuristic: only restrict retries if we have ever seen this op fail non-OOM (not tracked)
        # or if it's FAILED but not retryable. We approximate by "FAILED implies key must be retryable".
        # We can't read op.state portably here, so we enforce: if key is NOT retryable and the pipeline
        # has any failures, we will drop the pipeline elsewhere. Here we just return first op.
        if key in s.retryable_ops:
            return op
        # If not retryable, it might still be PENDING; allow it.
        return op

    return None


def _base_cpu_cap(priority, pool_max_cpu):
    # Conservative caps: keep headroom for concurrency; give more CPU to latency-sensitive work.
    if priority == Priority.QUERY:
        return min(8, pool_max_cpu)
    if priority == Priority.INTERACTIVE:
        return min(6, pool_max_cpu)
    return min(4, pool_max_cpu)  # batch


def _base_ram_request(priority, pool_max_ram):
    # Start with a small fraction of pool RAM; OOM backoff will increase as needed.
    if pool_max_ram <= 0:
        return 0
    if priority == Priority.QUERY:
        frac = 0.12
    elif priority == Priority.INTERACTIVE:
        frac = 0.16
    else:
        frac = 0.20
    # Ensure at least 1 unit to avoid zero-allocation edge cases.
    return max(1.0, pool_max_ram * frac)


def _batch_reserve_multiplier(s, waited_ticks):
    # As batch ages, allow it to eat into reserved headroom, but keep at least 40% reserved.
    if waited_ticks <= 0:
        return 1.0
    age_frac = min(1.0, float(waited_ticks) / float(max(1, s.batch_age_limit_ticks)))
    return 1.0 - 0.6 * age_frac  # 1.0 -> 0.4


def _choose_pool_for_op(priority, req_cpu, req_ram, avail_cpu, avail_ram, pools, reserve_cpu, reserve_ram,
                        high_waiting, batch_reserve_mult):
    """Pick a pool that fits (cpu, ram). For batch, enforce soft reservation when high is waiting."""
    candidates = []
    for pid, pool in enumerate(pools):
        if avail_cpu[pid] < 1 or avail_ram[pid] < 1:
            continue

        # Fit check (RAM is hard; CPU we can downscale after choosing a pool).
        if avail_ram[pid] < req_ram:
            continue
        if avail_cpu[pid] < 1:
            continue

        # Soft reservation: if high priority work is waiting, do not let batch consume into the reserve.
        if priority == Priority.BATCH_PIPELINE and high_waiting:
            post_cpu = avail_cpu[pid] - min(req_cpu, avail_cpu[pid])
            post_ram = avail_ram[pid] - req_ram
            if post_cpu < reserve_cpu[pid] * batch_reserve_mult:
                continue
            if post_ram < reserve_ram[pid] * batch_reserve_mult:
                continue

        candidates.append(pid)

    if not candidates:
        return None

    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        # Spread high-priority work to the pool with most headroom.
        def score(pid):
            return (avail_cpu[pid], avail_ram[pid])
        return max(candidates, key=score)

    # Batch: best-fit packing (use the most constrained pool that still fits).
    def score(pid):
        # Lower is better: leftover resources after placement.
        post_cpu = avail_cpu[pid] - min(req_cpu, avail_cpu[pid])
        post_ram = avail_ram[pid] - req_ram
        # Weight RAM a bit higher to avoid fragmentation.
        return (post_ram * 2.0 + post_cpu)
    return min(candidates, key=score)


@register_scheduler(key="scheduler_high_041")
def scheduler_high_041(s, results: list, pipelines: list):
    """
    Priority-aware scheduler:
      - Enqueue new pipelines into per-priority deques.
      - Process results to detect OOM and update RAM hints/retry eligibility.
      - Schedule ready operators in priority order across pools, with soft reservations for high priority.
    """
    s.tick += 1

    # Enqueue new pipelines.
    for p in pipelines:
        if p.pipeline_id in s.dead_pipeline_ids:
            continue
        if p.pipeline_id in s.active_pipeline_ids:
            continue
        s.active_pipeline_ids.add(p.pipeline_id)
        s.pipeline_first_seen_tick.setdefault(p.pipeline_id, s.tick)
        s.queues[p.priority].append(p)

    # If nothing changed, return quickly (matches simulator expectations).
    if not pipelines and not results:
        return [], []

    # Process results: update OOM RAM hints and retry eligibility.
    for r in results:
        if not r.failed():
            continue

        oom = _is_oom_error(getattr(r, "error", None))
        for op in getattr(r, "ops", []) or []:
            k = _op_key(op)
            if oom:
                s.retryable_ops.add(k)
                s.op_retry_count[k] = s.op_retry_count.get(k, 0) + 1

                # Exponential backoff on RAM for this operator.
                prev_hint = s.op_ram_hint.get(k, 0)
                # r.ram is the previous allocation; double it (at least +1) to converge quickly.
                new_hint = max(prev_hint, float(getattr(r, "ram", 0) or 0) * 2.0, prev_hint * 1.5, 1.0)
                s.op_ram_hint[k] = new_hint

                # Give up after too many retries.
                if s.op_retry_count[k] > s.max_oom_retries:
                    # Stop retrying this operator; pipelines containing it will be dropped.
                    if k in s.retryable_ops:
                        s.retryable_ops.remove(k)
            else:
                # Non-OOM failures are treated as terminal: do not retry.
                if k in s.retryable_ops:
                    s.retryable_ops.remove(k)

    suspensions = []
    assignments = []

    # Snapshot pool availability for in-function accounting (avoid double-allocations).
    pools = s.executor.pools
    avail_cpu = {pid: float(pools[pid].avail_cpu_pool) for pid in range(s.executor.num_pools)}
    avail_ram = {pid: float(pools[pid].avail_ram_pool) for pid in range(s.executor.num_pools)}

    # Precompute per-pool reserves.
    reserve_cpu = {}
    reserve_ram = {}
    for pid in range(s.executor.num_pools):
        reserve_cpu[pid] = float(pools[pid].max_cpu_pool) * float(s.reserve_frac)
        reserve_ram[pid] = float(pools[pid].max_ram_pool) * float(s.reserve_frac)

    # Determine whether any high-priority pipeline has an eligible op ready; if so, enforce batch reservations.
    def _has_runnable_high():
        for pr in (Priority.QUERY, Priority.INTERACTIVE):
            q = s.queues[pr]
            # Check a bounded number of elements (full scan is OK; queues are typically modest).
            for p in list(q):
                if p.pipeline_id in s.dead_pipeline_ids:
                    continue
                if _pipeline_is_terminal(s, p):
                    continue
                op = _pick_next_eligible_op(s, p)
                if op is not None:
                    return True
        return False

    high_waiting = _has_runnable_high()

    # Main scheduling loop: keep assigning until no further progress.
    priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    made_progress = True
    max_total_assignments = 2000  # safety guard

    while made_progress and len(assignments) < max_total_assignments:
        made_progress = False

        for prio in priority_order:
            q = s.queues[prio]
            if not q:
                continue

            # Round-robin within a priority class.
            n = len(q)
            for _ in range(n):
                p = q.popleft()

                # Drop pipelines we've decided are dead or already completed/terminal.
                if p.pipeline_id in s.dead_pipeline_ids:
                    s.active_pipeline_ids.discard(p.pipeline_id)
                    continue

                if _pipeline_is_terminal(s, p):
                    s.dead_pipeline_ids.add(p.pipeline_id)
                    s.active_pipeline_ids.discard(p.pipeline_id)
                    continue

                op = _pick_next_eligible_op(s, p)
                if op is None:
                    # Not runnable yet; keep it in the queue.
                    q.append(p)
                    continue

                # If pipeline contains FAILED ops that are not retryable, drop it now.
                status = p.runtime_status()
                failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
                if failed_ops:
                    non_retryable_found = False
                    for fop in failed_ops:
                        if _op_key(fop) not in s.retryable_ops:
                            non_retryable_found = True
                            break
                    if non_retryable_found:
                        s.dead_pipeline_ids.add(p.pipeline_id)
                        s.active_pipeline_ids.discard(p.pipeline_id)
                        continue

                # Compute resource request (RAM uses hint + base; CPU uses cap).
                pid_seen = s.pipeline_first_seen_tick.get(p.pipeline_id, s.tick)
                waited = s.tick - pid_seen

                opk = _op_key(op)
                # Base requests scale with pool size; we pick pool after computing req, then downscale CPU if needed.
                # For RAM, we must fit the hint; otherwise the op will likely OOM repeatedly.
                # We'll allow placement to look for a larger pool if available.
                # Use the largest pool_max_ram among pools as a base reference (keeps req non-trivial).
                max_pool_ram = max(float(pool.max_ram_pool) for pool in pools) if pools else 0.0
                base_ram = _base_ram_request(prio, max_pool_ram)
                req_ram = max(base_ram, float(s.op_ram_hint.get(opk, 0.0) or 0.0), 1.0)

                max_pool_cpu = max(float(pool.max_cpu_pool) for pool in pools) if pools else 0.0
                cpu_cap = _base_cpu_cap(prio, max_pool_cpu)
                req_cpu = max(1.0, float(cpu_cap))

                # Batch fairness: allow older batch to partially consume reserved resources.
                batch_reserve_mult = 1.0
                if prio == Priority.BATCH_PIPELINE:
                    batch_reserve_mult = _batch_reserve_multiplier(s, waited)

                chosen_pool = _choose_pool_for_op(
                    prio, req_cpu, req_ram,
                    avail_cpu, avail_ram,
                    pools, reserve_cpu, reserve_ram,
                    high_waiting=high_waiting,
                    batch_reserve_mult=batch_reserve_mult,
                )

                if chosen_pool is None:
                    # Could not place now; keep the pipeline queued.
                    q.append(p)
                    continue

                # Finalize CPU to use (downscale if pool is tight). RAM is fixed to req_ram.
                cpu_to_use = min(req_cpu, avail_cpu[chosen_pool])
                cpu_to_use = max(1.0, cpu_to_use)
                ram_to_use = min(req_ram, avail_ram[chosen_pool])

                if cpu_to_use <= 0 or ram_to_use <= 0:
                    q.append(p)
                    continue

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_to_use,
                        ram=ram_to_use,
                        priority=p.priority,
                        pool_id=chosen_pool,
                        pipeline_id=p.pipeline_id,
                    )
                )

                avail_cpu[chosen_pool] -= cpu_to_use
                avail_ram[chosen_pool] -= ram_to_use

                # Re-enqueue the pipeline to allow other pipelines within the same priority to get a chance.
                q.append(p)
                made_progress = True

    return suspensions, assignments
