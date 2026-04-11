# policy_key: scheduler_high_011
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.105702
# generation_seconds: 169.67
# generated_at: 2026-03-12T22:48:48.591230
@register_scheduler_init(key="scheduler_high_011")
def scheduler_high_011_init(s):
    """
    Priority-aware, latency-oriented refinement of the naive FIFO policy.

    Small, "obvious" improvements over naive:
      1) Priority ordering: prefer QUERY > INTERACTIVE > BATCH.
      2) Keep headroom for high-priority arrivals: when high-priority work is waiting,
         BATCH is only allowed to consume "leftover" resources beyond a reservation.
      3) Basic OOM resilience: on detected OOM failures, retry with increased RAM and
         exponential backoff to reduce repeated thrash.
      4) Fairness guard: simple aging that gradually boosts older pipelines' ordering score
         (without overriding the batch headroom reservation rules).
      5) Concurrency guard: limit inflight ops per pipeline to avoid one pipeline dominating.
    """
    # Queued pipelines are represented by pipeline_id to avoid duplicates.
    s.waiting_queue = []          # List[pipeline_id]
    s.in_queue = set()            # Set[pipeline_id]
    s.active_pipelines = {}       # pipeline_id -> Pipeline (latest handle)

    # Per-pipeline metadata for retries / aging.
    # meta[pipeline_id] = {
    #   "enqueue_tick": int,
    #   "ram_mult": float,
    #   "retries": int,
    #   "next_eligible_tick": int,
    #   "dead": bool,
    # }
    s.meta = {}

    # Logical scheduler clock (ticks).
    s.tick = 0

    # -----------------------
    # Tunables (small + safe)
    # -----------------------
    s.max_retries = 6
    s.max_ram_mult = 32.0
    s.max_backoff = 64

    # When high-priority work is waiting, keep this fraction of each pool free from BATCH.
    s.reserve_cpu_frac_when_high = 0.25
    s.reserve_ram_frac_when_high = 0.25

    # Minimum allocation to avoid creating tiny, ineffective containers.
    s.min_cpu = 1.0
    s.min_ram = 1.0

    # Typical per-container "slice" sizes by priority (fractions of pool max).
    # (Caps are implicit via pool availability; this aims to allow concurrency while
    # still giving high priority meaningful resources.)
    s.cpu_slice_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.30,
    }
    s.ram_slice_frac = {
        Priority.QUERY: 0.55,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Simple aging: every N ticks of waiting increases ordering score.
    s.aging_step = 25

    # Limit inflight operators per pipeline (ASSIGNED + RUNNING).
    s.max_inflight_ops_per_pipeline = 1


@register_scheduler(key="scheduler_high_011")
def scheduler_high_011(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]):
    """
    Priority-aware scheduler with reservation for latency-sensitive work and OOM backoff.

    Returns:
        (suspensions, assignments)
    """
    s.tick += 1

    def _get_state_count(status, state):
        try:
            return status.state_counts[state]
        except Exception:
            try:
                return status.state_counts.get(state, 0)
            except Exception:
                return 0

    def _pipeline_total_ops(p):
        # Best-effort: pipeline.values is "DAG of operators" (often sized).
        try:
            return len(p.values)
        except Exception:
            return 0

    def _pipeline_remaining_ops(p):
        status = p.runtime_status()
        total = _pipeline_total_ops(p)
        completed = _get_state_count(status, OperatorState.COMPLETED)
        if total <= 0:
            # If unknown, treat as 1 remaining to avoid bias.
            return 1
        rem = total - completed
        return rem if rem > 0 else 0

    def _is_oom_error(err):
        if not err:
            return False
        try:
            e = err.lower()
        except Exception:
            return False
        return ("oom" in e) or ("out of memory" in e) or ("out-of-memory" in e) or ("memoryerror" in e)

    def _result_pipeline_id(r):
        # Prefer explicit pipeline_id if present.
        pid = getattr(r, "pipeline_id", None)
        if pid is not None:
            return pid
        # Attempt inference from operator objects.
        ops = getattr(r, "ops", None) or []
        for op in ops:
            pid = getattr(op, "pipeline_id", None)
            if pid is not None:
                return pid
        return None

    # Ingest new pipelines.
    for p in pipelines:
        pid = p.pipeline_id
        s.active_pipelines[pid] = p
        if pid not in s.meta:
            s.meta[pid] = {
                "enqueue_tick": s.tick,
                "ram_mult": 1.0,
                "retries": 0,
                "next_eligible_tick": 0,
                "dead": False,
            }
        if pid not in s.in_queue:
            s.waiting_queue.append(pid)
            s.in_queue.add(pid)

    # Process execution results: update OOM retry state and drop hard failures.
    for r in results:
        try:
            failed = r.failed()
        except Exception:
            failed = False

        if not failed:
            continue

        pid = _result_pipeline_id(r)
        if pid is None or pid not in s.meta:
            continue

        m = s.meta[pid]
        if m.get("dead", False):
            continue

        m["retries"] = int(m.get("retries", 0)) + 1
        if m["retries"] > s.max_retries:
            m["dead"] = True
            continue

        err = getattr(r, "error", None)
        if _is_oom_error(err):
            # RAM-first retry: double RAM multiplier, add backoff to reduce repeated thrash.
            m["ram_mult"] = min(s.max_ram_mult, float(m.get("ram_mult", 1.0)) * 2.0)

            # Exponential backoff (capped).
            # Note: small + safe; helps keep latency for high-priority by avoiding rapid re-fail loops.
            backoff = 2 ** min(10, m["retries"])
            if backoff > s.max_backoff:
                backoff = s.max_backoff
            m["next_eligible_tick"] = s.tick + int(backoff)
        else:
            # Non-OOM failures are treated as terminal to avoid infinite retry loops.
            m["dead"] = True

    # Cleanup: remove completed / dead / missing pipelines from the queue.
    new_queue = []
    new_in_queue = set()
    for pid in s.waiting_queue:
        p = s.active_pipelines.get(pid, None)
        m = s.meta.get(pid, None)
        if p is None or m is None:
            continue

        if m.get("dead", False) or int(m.get("retries", 0)) > s.max_retries:
            continue

        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue

        # Keep it queued.
        new_queue.append(pid)
        new_in_queue.add(pid)

    s.waiting_queue = new_queue
    s.in_queue = new_in_queue

    # Fast exit if nothing to do.
    if not s.waiting_queue and not pipelines and not results:
        return [], []

    # Determine if there is any *eligible* high-priority work waiting.
    def _eligible(pid):
        m = s.meta.get(pid, None)
        p = s.active_pipelines.get(pid, None)
        if not m or not p or m.get("dead", False):
            return False
        if int(m.get("next_eligible_tick", 0)) > s.tick:
            return False
        if int(m.get("retries", 0)) > s.max_retries:
            return False
        if p.runtime_status().is_pipeline_successful():
            return False
        return True

    high_waiting = False
    for pid in s.waiting_queue:
        if not _eligible(pid):
            continue
        pr = s.active_pipelines[pid].priority
        if pr in (Priority.QUERY, Priority.INTERACTIVE):
            high_waiting = True
            break

    # Compute an ordering score: base priority + aging bonus; tiebreaker by remaining ops.
    def _base_score(priority):
        if priority == Priority.QUERY:
            return 300
        if priority == Priority.INTERACTIVE:
            return 200
        return 100  # BATCH_PIPELINE

    def _ordering_tuple(pid):
        p = s.active_pipelines[pid]
        m = s.meta[pid]
        age = max(0, s.tick - int(m.get("enqueue_tick", s.tick)))
        age_bonus = min(99, age // int(s.aging_step))
        score = _base_score(p.priority) + age_bonus
        rem = _pipeline_remaining_ops(p)
        enqueue_tick = int(m.get("enqueue_tick", s.tick))
        # Higher score first, then fewer remaining ops (SRPT-ish), then older first.
        return (score, -rem, -enqueue_tick)

    def _inflight_ok(p):
        status = p.runtime_status()
        inflight = _get_state_count(status, OperatorState.RUNNING) + _get_state_count(status, OperatorState.ASSIGNED)
        return inflight < s.max_inflight_ops_per_pipeline

    def _find_best_candidate(pool_id, budget_cpu, budget_ram, scheduled_this_tick):
        """
        Find the best eligible pipeline that can run now and (if batch) fits in budget with reservations.
        Returns (pipeline_id, op_list) or (None, None).
        """
        best_pid = None
        best_ops = None
        best_key = None

        pool = s.executor.pools[pool_id]
        reserve_cpu = s.reserve_cpu_frac_when_high * pool.max_cpu_pool
        reserve_ram = s.reserve_ram_frac_when_high * pool.max_ram_pool

        # Scan queue: simple, safe incremental improvement over FIFO.
        for pid in s.waiting_queue:
            if pid in scheduled_this_tick:
                continue
            if not _eligible(pid):
                continue

            p = s.active_pipelines.get(pid, None)
            if p is None:
                continue

            # Concurrency guard.
            if not _inflight_ok(p):
                continue

            # Batch headroom: if high is waiting, batch must leave reservation.
            pr = p.priority
            if high_waiting and pr == Priority.BATCH_PIPELINE:
                # Require that after placing this batch, some reservation remains.
                # This makes batch opportunistic during contention, improving high-priority latency.
                if budget_cpu <= reserve_cpu or budget_ram <= reserve_ram:
                    continue

            status = p.runtime_status()
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                continue

            # Check that we can allocate minimums and (for batch under contention) not violate reservation.
            allowed_cpu = budget_cpu
            allowed_ram = budget_ram
            if high_waiting and pr == Priority.BATCH_PIPELINE:
                allowed_cpu = max(0.0, budget_cpu - reserve_cpu)
                allowed_ram = max(0.0, budget_ram - reserve_ram)

            if allowed_cpu < s.min_cpu or allowed_ram < s.min_ram:
                continue

            key = _ordering_tuple(pid)
            if best_key is None or key > best_key:
                best_key = key
                best_pid = pid
                best_ops = op_list

        return best_pid, best_ops

    suspensions = []
    assignments = []
    scheduled_this_tick = set()

    # Assign work per pool while resources remain.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        budget_cpu = float(pool.avail_cpu_pool)
        budget_ram = float(pool.avail_ram_pool)

        if budget_cpu < s.min_cpu or budget_ram < s.min_ram:
            continue

        # Continue packing within the pool with multiple assignments, updating budgets.
        # (Naive policy did at most 1; this is a small throughput improvement without major complexity.)
        while budget_cpu >= s.min_cpu and budget_ram >= s.min_ram:
            pid, op_list = _find_best_candidate(pool_id, budget_cpu, budget_ram, scheduled_this_tick)
            if pid is None:
                break

            p = s.active_pipelines[pid]
            m = s.meta[pid]
            pr = p.priority

            # Compute allowed budget (apply reservation only to batch when high waiting).
            allowed_cpu = budget_cpu
            allowed_ram = budget_ram
            if high_waiting and pr == Priority.BATCH_PIPELINE:
                reserve_cpu = s.reserve_cpu_frac_when_high * pool.max_cpu_pool
                reserve_ram = s.reserve_ram_frac_when_high * pool.max_ram_pool
                allowed_cpu = max(0.0, budget_cpu - reserve_cpu)
                allowed_ram = max(0.0, budget_ram - reserve_ram)

            # Size container: priority-based slice * pool max, with RAM boosted on prior OOMs.
            cpu_target = s.cpu_slice_frac.get(pr, 0.30) * pool.max_cpu_pool
            ram_target = s.ram_slice_frac.get(pr, 0.45) * pool.max_ram_pool

            ram_mult = float(m.get("ram_mult", 1.0))
            # For repeated OOMs, allow RAM to grow beyond the normal slice, but never above pool max / budget.
            ram_request = ram_target * max(1.0, ram_mult)

            cpu = min(allowed_cpu, max(s.min_cpu, cpu_target))
            ram = min(allowed_ram, max(s.min_ram, ram_request))

            # Final sanity: if we still can't meet minimums, stop packing.
            if cpu < s.min_cpu or ram < s.min_ram:
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pr,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            scheduled_this_tick.add(pid)
            budget_cpu -= cpu
            budget_ram -= ram

    return suspensions, assignments
