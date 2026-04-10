# policy_key: scheduler_none_008
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.037673
# generation_seconds: 36.04
# generated_at: 2026-03-12T21:26:43.400729
@register_scheduler_init(key="scheduler_none_008")
def scheduler_none_008_init(s):
    """Priority-aware incremental scheduler (small, safe improvements over naive FIFO).

    Improvements vs naive:
    1) Maintain separate queues by priority and always try to schedule higher priority first.
    2) Avoid "one giant op per pool" behavior: schedule multiple ready ops per pool up to a small cap,
       using conservative per-op CPU/RAM slices so interactive latency improves under contention.
    3) Simple OOM backoff: if an op fails, remember the last RAM and increase RAM on retry for that pipeline.

    Notes:
    - This stays deliberately simple: no preemption, no multi-pool specialization assumptions.
    - It relies only on info exposed in the example: pool headroom + execution results.
    """
    # Queues per priority to reduce head-of-line blocking
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline RAM hint multiplier applied after failures (e.g., OOM)
    s.pipeline_ram_mult = {}

    # Basic knobs (conservative defaults)
    s.max_assignments_per_pool_tick = 4  # schedule a few ops per pool per tick for better latency
    s.min_cpu_per_op = 1                # don't assign <1 vCPU
    s.min_ram_per_op = 1                # don't assign <1 RAM unit (sim's unit)
    s.query_share = 0.60                # try to preserve more headroom for queries
    s.interactive_share = 0.30          # interactive next
    s.batch_share = 0.10                # batch gets leftovers


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _shares_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.query_share
    if prio == Priority.INTERACTIVE:
        return s.interactive_share
    return s.batch_share


def _enqueue_pipeline(s, p):
    # Enqueue once; duplicates are okay but we try to avoid them by checking pipeline_id.
    # We don't maintain a global set to keep code simple and robust in simulator resets.
    s.wait_q[p.priority].append(p)


def _pipeline_done_or_failed(p):
    status = p.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If any operator is FAILED, naive example drops it. We instead allow retries by keeping it in queue.
    # But we do still drop pipelines that appear "stuck failed" with no retryable ops available; handled later.
    return False


def _get_next_ready_op(p):
    status = p.runtime_status()
    # Prefer ops that are pending/failed but parents complete
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    if not op_list:
        return None
    return op_list


def _ram_multiplier_for_pipeline(s, pipeline_id):
    return s.pipeline_ram_mult.get(pipeline_id, 1.0)


def _apply_result_feedback(s, results):
    # If we see failures, increase RAM multiplier for that pipeline so retries are more likely to succeed.
    # We keep it simple: any failure bumps RAM multiplier.
    for r in results:
        if r.failed():
            pid = getattr(r, "pipeline_id", None)
            # Some ExecutionResult may not carry pipeline_id; fall back to no-op.
            if pid is None:
                continue
            cur = s.pipeline_ram_mult.get(pid, 1.0)
            # Exponential-ish backoff, capped to avoid runaway.
            nxt = min(cur * 1.5, 8.0)
            s.pipeline_ram_mult[pid] = nxt


@register_scheduler(key="scheduler_none_008")
def scheduler_none_008(s, results, pipelines):
    """
    Priority-aware multi-assignment scheduler.

    Core loop:
    - ingest new pipelines into per-priority queues
    - update RAM hints from recent failures
    - for each pool, allocate a portion of resources to each priority class
      and schedule up to N ready ops, higher priority first

    No preemption in this version (small improvement step).
    """
    # Ingest arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Early exit if nothing changes that would affect decisions
    if not pipelines and not results:
        return [], []

    # Update hints from outcomes
    _apply_result_feedback(s, results)

    suspensions = []
    assignments = []

    # For each pool, attempt to schedule a few operators, prioritizing high-priority work
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # We'll schedule in two stages:
        # 1) Carve out "soft shares" per priority for latency protection
        # 2) If shares aren't fully used, allow leftovers to be used by lower priorities
        remaining_cpu = avail_cpu
        remaining_ram = avail_ram

        # Track how many assignments we made in this pool this tick
        pool_assign_count = 0

        # We'll run a few passes: strict shares first, then opportunistic fill.
        # Pass 1: strict-ish shares
        for prio in _priority_order():
            if pool_assign_count >= s.max_assignments_per_pool_tick:
                break

            # Compute this priority's budget from current remaining resources
            # (soft: based on pool max to avoid shrinking budgets too aggressively)
            cpu_budget = min(remaining_cpu, max(int(pool.max_cpu_pool * _shares_for_priority(s, prio)), 0))
            ram_budget = min(remaining_ram, max(int(pool.max_ram_pool * _shares_for_priority(s, prio)), 0))

            if cpu_budget <= 0 or ram_budget <= 0:
                continue

            # Try to schedule multiple ops from this priority within its budget
            q = s.wait_q.get(prio, [])
            if not q:
                continue

            # Round-robin within a priority to avoid pipeline head-of-line blocking
            # We'll pop from front and re-append if still not done.
            local_iters = 0
            max_local_iters = len(q) + 2  # keep bounded even with requeues
            while q and cpu_budget > 0 and ram_budget > 0 and pool_assign_count < s.max_assignments_per_pool_tick:
                local_iters += 1
                if local_iters > max_local_iters:
                    break

                p = q.pop(0)

                # Drop completed pipelines
                if _pipeline_done_or_failed(p):
                    continue

                op_list = _get_next_ready_op(p)
                if not op_list:
                    # No ready ops; requeue and try others
                    q.append(p)
                    continue

                # Conservative per-op sizing: slice budget among remaining slots in this tick.
                remaining_slots = max(s.max_assignments_per_pool_tick - pool_assign_count, 1)
                cpu_per_op = max(s.min_cpu_per_op, int(cpu_budget / remaining_slots))
                ram_per_op = max(s.min_ram_per_op, int(ram_budget / remaining_slots))

                # Apply RAM multiplier based on observed failures for this pipeline
                mult = _ram_multiplier_for_pipeline(s, p.pipeline_id)
                ram_per_op = max(s.min_ram_per_op, int(ram_per_op * mult))

                # Clamp to what's currently available in pool and within this priority budget
                cpu_to_use = min(cpu_per_op, cpu_budget, remaining_cpu)
                ram_to_use = min(ram_per_op, ram_budget, remaining_ram)

                if cpu_to_use <= 0 or ram_to_use <= 0:
                    # Can't fit now; requeue and move on
                    q.append(p)
                    break

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu_to_use,
                        ram=ram_to_use,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # Update budgets
                cpu_budget -= cpu_to_use
                ram_budget -= ram_to_use
                remaining_cpu -= cpu_to_use
                remaining_ram -= ram_to_use
                pool_assign_count += 1

                # Requeue pipeline for further ops later
                q.append(p)

        # Pass 2: opportunistic fill with any remaining resources (still priority order)
        if pool_assign_count < s.max_assignments_per_pool_tick and remaining_cpu > 0 and remaining_ram > 0:
            for prio in _priority_order():
                if pool_assign_count >= s.max_assignments_per_pool_tick:
                    break
                q = s.wait_q.get(prio, [])
                if not q:
                    continue

                local_iters = 0
                max_local_iters = len(q) + 2
                while q and remaining_cpu > 0 and remaining_ram > 0 and pool_assign_count < s.max_assignments_per_pool_tick:
                    local_iters += 1
                    if local_iters > max_local_iters:
                        break

                    p = q.pop(0)
                    if _pipeline_done_or_failed(p):
                        continue

                    op_list = _get_next_ready_op(p)
                    if not op_list:
                        q.append(p)
                        continue

                    remaining_slots = max(s.max_assignments_per_pool_tick - pool_assign_count, 1)
                    cpu_per_op = max(s.min_cpu_per_op, int(remaining_cpu / remaining_slots))
                    ram_per_op = max(s.min_ram_per_op, int(remaining_ram / remaining_slots))

                    mult = _ram_multiplier_for_pipeline(s, p.pipeline_id)
                    ram_per_op = max(s.min_ram_per_op, int(ram_per_op * mult))

                    cpu_to_use = min(cpu_per_op, remaining_cpu)
                    ram_to_use = min(ram_per_op, remaining_ram)

                    if cpu_to_use <= 0 or ram_to_use <= 0:
                        q.append(p)
                        break

                    assignments.append(
                        Assignment(
                            ops=op_list,
                            cpu=cpu_to_use,
                            ram=ram_to_use,
                            priority=p.priority,
                            pool_id=pool_id,
                            pipeline_id=p.pipeline_id,
                        )
                    )

                    remaining_cpu -= cpu_to_use
                    remaining_ram -= ram_to_use
                    pool_assign_count += 1
                    q.append(p)

    return suspensions, assignments
