# policy_key: scheduler_medium_015
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.094679
# generation_seconds: 104.55
# generated_at: 2026-04-09T22:49:24.658285
@register_scheduler_init(key="scheduler_medium_015")
def scheduler_medium_015_init(s):
    """
    Priority-aware, retry-friendly scheduler focused on minimizing weighted latency while
    avoiding failed/incomplete pipelines (which are heavily penalized).

    Core ideas:
      - Strict priority ordering (QUERY > INTERACTIVE > BATCH) with a small anti-starvation rule.
      - Conservative batch "headroom reservation" when high-priority work is waiting.
      - Per-operator RAM estimation with OOM backoff retries to improve completion rate.
      - Avoid scheduling multiple concurrent ops from the same pipeline (reduces interference / churn).
      - Mark pipelines as hopeless only after repeated non-OOM failures or impossible RAM needs.
    """
    from collections import deque

    s.tick = 0

    # Per-priority FIFO queues; keep duplicates out via s.in_queue
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.in_queue = set()  # pipeline_id currently queued

    # Track arrivals (for potential debugging / future aging extensions)
    s.arrival_tick = {}  # pipeline_id -> tick

    # Per-operator learning (keyed by id(op), which is stable for the op object in the sim)
    s.op_ram_est = {}       # op_id -> estimated RAM to request next time
    s.op_retries = {}       # op_id -> retry count
    s.op_last_error = {}    # op_id -> last error string

    # Map operator object -> pipeline id (so results can attribute failures)
    s.op_to_pipeline = {}   # op_id -> pipeline_id

    # Pipelines we stop attempting (to protect overall weighted latency once they're "hopeless")
    s.pipeline_hopeless = set()

    # Anti-starvation: after this many high-priority placements, place one batch if available
    s.hp_streak = 0
    s.hp_streak_limit = 5

    # When high-priority work is waiting, reserve a fraction of each pool for it (batch won't consume it)
    s.batch_reserve_frac_cpu = 0.30
    s.batch_reserve_frac_ram = 0.30

    # Retry policy
    s.max_retries_oom = 8
    s.max_retries_other = 2

    # Default sizing knobs (fractions of pool capacity if no per-op estimate exists yet)
    s.default_ram_frac = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.20,
        Priority.BATCH_PIPELINE: 0.15,
    }
    s.default_ram_safety = {
        Priority.QUERY: 1.20,
        Priority.INTERACTIVE: 1.15,
        Priority.BATCH_PIPELINE: 1.10,
    }


@register_scheduler(key="scheduler_medium_015")
def scheduler_medium_015(s, results, pipelines):
    """
    Priority-aware scheduling with:
      - RAM backoff on OOM (to improve completion rate, reduce 720s penalties),
      - headroom reservation against batch when high-priority is waiting,
      - strict priority selection with a small anti-starvation rule.
    """
    s.tick += 1

    suspensions = []
    assignments = []

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda oom" in msg)

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid in s.pipeline_hopeless:
            return
        if pid not in s.arrival_tick:
            s.arrival_tick[pid] = s.tick
        if pid in s.in_queue:
            return
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return
        s.queues[p.priority].append(p)
        s.in_queue.add(pid)

    def _pop_next_valid_from_queue(priority):
        q = s.queues[priority]
        for _ in range(len(q)):
            p = q.popleft()
            s.in_queue.discard(p.pipeline_id)

            if p.pipeline_id in s.pipeline_hopeless:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            # Avoid multiple concurrent ops from the same pipeline to reduce interference/churn.
            inflight = status.state_counts.get(OperatorState.ASSIGNED, 0) + status.state_counts.get(OperatorState.RUNNING, 0)
            if inflight > 0:
                # Put it back for later; it's already making progress.
                _enqueue_pipeline(p)
                continue

            return p
        return None

    def _get_next_op(p):
        status = p.runtime_status()
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        # Retry control: if an op has repeatedly failed with non-OOM errors, stop burning cycles.
        for op in op_list:
            op_id = id(op)
            retries = s.op_retries.get(op_id, 0)
            last_err = (s.op_last_error.get(op_id, "") or "").lower()
            if retries > s.max_retries_other and not _is_oom_error(last_err):
                s.pipeline_hopeless.add(p.pipeline_id)
                return None
            return op
        return None

    def _ram_request_for(op, priority, pool):
        # If we have an estimate, use it; otherwise derive from a fraction of pool capacity.
        op_id = id(op)
        est = s.op_ram_est.get(op_id, None)
        if est is None:
            frac = s.default_ram_frac.get(priority, 0.15)
            safety = s.default_ram_safety.get(priority, 1.10)
            req = max(1.0, pool.max_ram_pool * frac * safety)
        else:
            # Mild safety factor to reduce repeat OOMs while converging.
            safety = 1.08 if priority != Priority.QUERY else 1.12
            req = max(1.0, est * safety)

        # Never request more than the pool can provide.
        return min(req, pool.max_ram_pool)

    def _cpu_request_for(priority, pool):
        # Keep CPU modest to allow parallelism; queries get more CPU to cut tail latency.
        if priority == Priority.QUERY:
            target = min(max(2.0, 0.50 * pool.max_cpu_pool), 4.0)
        elif priority == Priority.INTERACTIVE:
            target = min(max(1.0, 0.25 * pool.max_cpu_pool), 2.0)
        else:
            target = 1.0
        return max(1.0, min(target, pool.max_cpu_pool))

    # --- Incorporate new arrivals ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # --- Process execution results to update RAM estimates / retries ---
    # Build helper lookup: pool_id -> pool.max_ram_pool
    pool_max_ram = {i: s.executor.pools[i].max_ram_pool for i in range(s.executor.num_pools)}

    for r in results:
        failed = r.failed()
        err = r.error
        is_oom = _is_oom_error(err)

        # Update each op in the result (typically 1 op per container in these sims).
        for op in (r.ops or []):
            op_id = id(op)

            # Ensure we can attribute op -> pipeline for later decisions
            pid = s.op_to_pipeline.get(op_id, None)

            if failed:
                s.op_retries[op_id] = s.op_retries.get(op_id, 0) + 1
                s.op_last_error[op_id] = str(err) if err is not None else "failed"

                if is_oom:
                    # Backoff RAM estimate based on what we just tried.
                    prev = s.op_ram_est.get(op_id, max(1.0, float(getattr(r, "ram", 1.0))))
                    tried = max(prev, float(getattr(r, "ram", prev)))
                    new_est = tried * 1.8  # aggressive enough to converge quickly
                    # Cap at pool maximum if we know where it ran; otherwise leave uncapped (capped at request time).
                    if r.pool_id in pool_max_ram:
                        new_est = min(new_est, pool_max_ram[r.pool_id])
                    s.op_ram_est[op_id] = new_est

                    # If we already tried near the pool max and still OOM repeatedly, deem hopeless.
                    if pid is not None and r.pool_id in pool_max_ram:
                        if tried >= 0.90 * pool_max_ram[r.pool_id] and s.op_retries.get(op_id, 0) >= s.max_retries_oom:
                            s.pipeline_hopeless.add(pid)
                else:
                    # Non-OOM: don't keep hammering; a small RAM bump sometimes helps, but cap retries.
                    prev = s.op_ram_est.get(op_id, max(1.0, float(getattr(r, "ram", 1.0))))
                    s.op_ram_est[op_id] = prev * 1.15
                    if pid is not None and s.op_retries.get(op_id, 0) > s.max_retries_other:
                        s.pipeline_hopeless.add(pid)
            else:
                # Success: gently reduce estimate to improve packing, but keep some stability.
                if op_id in s.op_ram_est:
                    s.op_ram_est[op_id] = max(1.0, s.op_ram_est[op_id] * 0.95)
                # Reset retry count after a success.
                s.op_retries[op_id] = 0
                s.op_last_error[op_id] = ""

    # --- Scheduling decisions ---
    # Shadow available resources so we can place multiple assignments in one tick.
    shadow_cpu = []
    shadow_ram = []
    for i in range(s.executor.num_pools):
        pool = s.executor.pools[i]
        shadow_cpu.append(float(pool.avail_cpu_pool))
        shadow_ram.append(float(pool.avail_ram_pool))

    def _high_priority_waiting():
        return (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    def _select_next_pipeline():
        # Anti-starvation: occasionally place a batch op if high-priority work has dominated.
        if len(s.queues[Priority.BATCH_PIPELINE]) > 0 and s.hp_streak >= s.hp_streak_limit:
            p = _pop_next_valid_from_queue(Priority.BATCH_PIPELINE)
            if p is not None:
                s.hp_streak = 0
                return p

        p = _pop_next_valid_from_queue(Priority.QUERY)
        if p is not None:
            s.hp_streak += 1
            return p

        p = _pop_next_valid_from_queue(Priority.INTERACTIVE)
        if p is not None:
            s.hp_streak += 1
            return p

        p = _pop_next_valid_from_queue(Priority.BATCH_PIPELINE)
        if p is not None:
            s.hp_streak = 0
            return p

        return None

    def _pool_can_fit(pool_id, cpu_req, ram_req, priority):
        pool = s.executor.pools[pool_id]
        avail_cpu = shadow_cpu[pool_id]
        avail_ram = shadow_ram[pool_id]

        if priority == Priority.BATCH_PIPELINE and _high_priority_waiting():
            # Apply reservation only to batch when high-priority is pending.
            reserve_cpu = s.batch_reserve_frac_cpu * float(pool.max_cpu_pool)
            reserve_ram = s.batch_reserve_frac_ram * float(pool.max_ram_pool)
            avail_cpu = max(0.0, avail_cpu - reserve_cpu)
            avail_ram = max(0.0, avail_ram - reserve_ram)

        return (avail_cpu >= cpu_req) and (avail_ram >= ram_req)

    def _choose_pool(priority, cpu_req, ram_req):
        # For query/interactive: pick the pool with the most headroom that fits (reduces queueing / OOM risk).
        # For batch: best-fit packing (reduces fragmentation).
        best_pool = None
        best_score = None

        for pool_id in range(s.executor.num_pools):
            if not _pool_can_fit(pool_id, cpu_req, ram_req, priority):
                continue

            if priority in (Priority.QUERY, Priority.INTERACTIVE):
                # Prefer maximum remaining resources after placement (headroom).
                score = (shadow_ram[pool_id] - ram_req) + 0.25 * (shadow_cpu[pool_id] - cpu_req)
                if best_score is None or score > best_score:
                    best_score = score
                    best_pool = pool_id
            else:
                # Best-fit: minimize leftover RAM first, then leftover CPU.
                leftover_ram = shadow_ram[pool_id] - ram_req
                leftover_cpu = shadow_cpu[pool_id] - cpu_req
                score = (leftover_ram, leftover_cpu)
                if best_score is None or score < best_score:
                    best_score = score
                    best_pool = pool_id

        return best_pool

    # Upper bound attempts to avoid spinning when nothing fits.
    max_attempts = 50 + 10 * s.executor.num_pools
    attempts = 0
    made_progress = True

    while made_progress and attempts < max_attempts:
        attempts += 1
        made_progress = False

        p = _select_next_pipeline()
        if p is None:
            break

        if p.pipeline_id in s.pipeline_hopeless:
            continue

        op = _get_next_op(p)
        if op is None:
            continue

        # If an op has an estimate larger than any pool's max RAM, it's impossible.
        op_id = id(op)
        max_pool_ram_any = max(float(s.executor.pools[i].max_ram_pool) for i in range(s.executor.num_pools)) if s.executor.num_pools > 0 else 0.0
        est = s.op_ram_est.get(op_id, None)
        if est is not None and est > max_pool_ram_any:
            s.pipeline_hopeless.add(p.pipeline_id)
            continue

        # Pick a pool; RAM request is pool-dependent (fraction of that pool), so compute using a representative pool:
        # try each pool during selection by computing pool-specific RAM in _ram_request_for.
        chosen_pool = None
        chosen_cpu = None
        chosen_ram = None

        # First pass: compute cpu target per pool; pick best pool using pool-specific RAM request.
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            cpu_req = _cpu_request_for(p.priority, pool)
            ram_req = _ram_request_for(op, p.priority, pool)

            if not _pool_can_fit(pool_id, cpu_req, ram_req, p.priority):
                continue

            # Select best pool among feasible ones
            candidate_pool = _choose_pool(p.priority, cpu_req, ram_req)
            if candidate_pool is not None:
                # Recompute exact req for that chosen pool (consistent)
                pool2 = s.executor.pools[candidate_pool]
                cpu_req2 = _cpu_request_for(p.priority, pool2)
                ram_req2 = _ram_request_for(op, p.priority, pool2)
                if _pool_can_fit(candidate_pool, cpu_req2, ram_req2, p.priority):
                    chosen_pool, chosen_cpu, chosen_ram = candidate_pool, cpu_req2, ram_req2
                break

        if chosen_pool is None:
            # Couldn't fit anywhere now; re-enqueue and try later.
            _enqueue_pipeline(p)
            continue

        # Record op -> pipeline mapping for results processing.
        s.op_to_pipeline[op_id] = p.pipeline_id

        # Place assignment
        assignments.append(
            Assignment(
                ops=[op],
                cpu=chosen_cpu,
                ram=chosen_ram,
                priority=p.priority,
                pool_id=chosen_pool,
                pipeline_id=p.pipeline_id,
            )
        )

        shadow_cpu[chosen_pool] -= float(chosen_cpu)
        shadow_ram[chosen_pool] -= float(chosen_ram)
        made_progress = True

        # Keep pipeline out of queue until the assigned op completes/fails; it will be re-enqueued later.
        # (If there are more ready ops later, they'll be picked up on subsequent ticks.)

    return suspensions, assignments
