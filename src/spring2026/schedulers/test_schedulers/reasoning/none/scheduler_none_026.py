# policy_key: scheduler_none_026
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.053617
# generation_seconds: 45.29
# generated_at: 2026-04-09T22:05:23.184321
@register_scheduler_init(key="scheduler_none_026")
def scheduler_none_026_init(s):
    """Priority-aware, failure-averse scheduler with simple SLO protection.

    Core ideas:
    - Keep separate FIFO queues per priority to protect query/interactive latency.
    - Use small-but-safe initial RAM sizing and exponential backoff on OOM-like failures to avoid repeated failures (720s penalty).
    - Bias placement: prefer a "reserved" share of each pool for query/interactive; only let batch consume leftover headroom.
    - Light preemption: if a high-priority pipeline is blocked and there is no headroom, suspend a batch container in that pool
      (only when it likely enables at least one high-priority assignment).
    - CPU sizing: give high priority more CPU (up to a cap) for latency, but avoid monopolizing entire pools.
    """
    # Per-priority waiting queues (FIFO within each class)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track per-operator RAM guesses keyed by (pipeline_id, op_id-ish)
    # We use a best-effort stable key derived from operator object identity.
    s.op_ram_guess = {}  # (pipeline_id, op_key) -> ram
    s.op_fail_count = {}  # (pipeline_id, op_key) -> int

    # Track pipelines seen (arrivals)
    s.seen_pipelines = set()

    # Per-pool: try to keep some headroom for high-priority work (fraction of max)
    s.reserve_query_frac = 0.25
    s.reserve_interactive_frac = 0.15

    # RAM sizing knobs
    s.min_ram_floor = 0.5  # never allocate below this (GB-ish units, depends on simulator config)
    s.ram_growth_factor = 2.0  # exponential backoff on likely OOM
    s.ram_growth_add = 0.0

    # CPU sizing knobs
    s.query_cpu_cap_frac = 0.75        # cap at fraction of pool max for a single assignment
    s.interactive_cpu_cap_frac = 0.60
    s.batch_cpu_cap_frac = 0.50
    s.query_cpu_min_frac = 0.25
    s.interactive_cpu_min_frac = 0.20
    s.batch_cpu_min_frac = 0.10

    # If a pool has multiple running containers, we avoid preempting repeatedly in the same tick
    s.preempted_this_tick = set()  # set of pool_id


@register_scheduler(key="scheduler_none_026")
def scheduler_none_026(s, results, pipelines):
    """
    Scheduler step:
    1) Ingest new pipelines into per-priority queues.
    2) Update RAM guesses from failures (OOM-like) to reduce repeat failures.
    3) For each pool, attempt to schedule in priority order while respecting reserved headroom.
    4) If blocked for high priority, optionally preempt one batch container in that pool to free resources.
    """
    # Helper functions defined inside to keep this a single code block without imports.
    def _priority_rank(pri):
        if pri == Priority.QUERY:
            return 0
        if pri == Priority.INTERACTIVE:
            return 1
        return 2  # batch

    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _is_done_or_dead(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator is FAILED we *still* want to retry (failure-averse) because failures are expensive.
        # However, if the simulator marks terminal failures differently, we'd need to detect that.
        return False

    def _iter_queues_in_order():
        # Yield (queue_name, queue_list) in priority order
        return (("query", s.q_query), ("interactive", s.q_interactive), ("batch", s.q_batch))

    def _get_assignable_op(p):
        st = p.runtime_status()
        # Only schedule ops whose parents completed; schedule one op at a time to reduce blast radius on sizing.
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        return op_list

    def _op_key(pipeline_id, op):
        # Best effort: stable within run. Prefer explicit id if present.
        if hasattr(op, "operator_id"):
            return (pipeline_id, ("operator_id", op.operator_id))
        if hasattr(op, "op_id"):
            return (pipeline_id, ("op_id", op.op_id))
        if hasattr(op, "task_id"):
            return (pipeline_id, ("task_id", op.task_id))
        return (pipeline_id, ("pyid", id(op)))

    def _pool_reserved(pool, pri):
        # Reserve some headroom for high-priority work; batch only uses leftover.
        if pri == Priority.QUERY:
            return pool.max_ram_pool * s.reserve_query_frac, pool.max_cpu_pool * s.reserve_query_frac
        if pri == Priority.INTERACTIVE:
            return pool.max_ram_pool * s.reserve_interactive_frac, pool.max_cpu_pool * s.reserve_interactive_frac
        return 0.0, 0.0

    def _cpu_target(pool, pri, avail_cpu):
        # Allocate a moderate fraction: enough to reduce latency but not to starve others.
        if pri == Priority.QUERY:
            cap = max(1e-9, pool.max_cpu_pool * s.query_cpu_cap_frac)
            floor = max(1e-9, pool.max_cpu_pool * s.query_cpu_min_frac)
        elif pri == Priority.INTERACTIVE:
            cap = max(1e-9, pool.max_cpu_pool * s.interactive_cpu_cap_frac)
            floor = max(1e-9, pool.max_cpu_pool * s.interactive_cpu_min_frac)
        else:
            cap = max(1e-9, pool.max_cpu_pool * s.batch_cpu_cap_frac)
            floor = max(1e-9, pool.max_cpu_pool * s.batch_cpu_min_frac)

        # Use what we have, but keep within [floor, cap]
        cpu = min(avail_cpu, cap)
        cpu = max(min(cpu, avail_cpu), min(floor, avail_cpu))
        # If avail is tiny, cpu may be tiny; caller will guard.
        return cpu

    def _ram_target(pool, p, op, avail_ram):
        # If we have a learned guess, use it; otherwise be conservative and not allocate the entire pool.
        key = _op_key(p.pipeline_id, op)
        guess = s.op_ram_guess.get(key, None)

        if guess is None:
            # Conservative start: take a modest slice of the pool RAM (reduces OOM risk vs too-small, and avoids monopolizing).
            guess = max(s.min_ram_floor, 0.20 * pool.max_ram_pool)

        # Never exceed available RAM; also never exceed pool max.
        ram = min(guess, avail_ram, pool.max_ram_pool)
        # Ensure at least some minimal meaningful allocation if possible.
        if avail_ram >= s.min_ram_floor:
            ram = max(ram, s.min_ram_floor)
        return ram

    def _is_oom_like(error_str):
        if not error_str:
            return False
        e = str(error_str).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "exceed" in e)

    def _update_from_results():
        for r in results:
            # On failures, increase RAM guess for each op in the result (best effort).
            if r.failed():
                for op in getattr(r, "ops", []) or []:
                    key = _op_key(getattr(r, "pipeline_id", None) or getattr(op, "pipeline_id", None) or "unknown", op)
                    # If we can't associate pipeline_id reliably from result, fall back to scanning queues later;
                    # but still try to use r.ops with "unknown" bucket to not crash.
                    s.op_fail_count[key] = s.op_fail_count.get(key, 0) + 1

                    if _is_oom_like(getattr(r, "error", None)):
                        prev = s.op_ram_guess.get(key, max(s.min_ram_floor, r.ram))
                        # Exponential backoff; if we already tried this RAM, increase.
                        new_guess = max(prev, r.ram) * s.ram_growth_factor + s.ram_growth_add
                        # Keep within a reasonable upper bound (pool max will clip at assignment time).
                        s.op_ram_guess[key] = max(new_guess, prev)
                    else:
                        # Non-OOM failures: still nudge RAM slightly to avoid repeated near-OOM cases,
                        # but much less aggressively.
                        prev = s.op_ram_guess.get(key, max(s.min_ram_floor, r.ram))
                        s.op_ram_guess[key] = max(prev, r.ram)

    def _collect_running_batch_containers_in_pool(pool_id):
        # Best-effort introspection: executor may expose running containers; if not, return empty.
        pool = s.executor.pools[pool_id]
        containers = []
        # Try common attribute names.
        if hasattr(pool, "running_containers"):
            containers = list(pool.running_containers)
        elif hasattr(pool, "containers"):
            containers = list(pool.containers)
        elif hasattr(s.executor, "containers"):
            # Some implementations keep global container registry.
            try:
                containers = [c for c in s.executor.containers if getattr(c, "pool_id", None) == pool_id]
            except Exception:
                containers = []

        batch_ids = []
        for c in containers:
            try:
                pri = getattr(c, "priority", None)
                if pri == Priority.BATCH_PIPELINE:
                    cid = getattr(c, "container_id", None)
                    if cid is not None:
                        batch_ids.append(cid)
            except Exception:
                continue
        return batch_ids

    # 1) ingest new pipelines
    for p in pipelines:
        if p.pipeline_id not in s.seen_pipelines:
            s.seen_pipelines.add(p.pipeline_id)
        _enqueue_pipeline(p)

    # Early exit if no new info
    if not pipelines and not results:
        return [], []

    # 2) update sizing guesses from results
    _update_from_results()

    suspensions = []
    assignments = []

    # 3) scheduling per pool with reserved headroom
    # We do multiple passes: first try schedule query/interactive, then batch with leftovers.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try to schedule multiple ops per tick if resources allow, but keep it simple: up to 2 per pool.
        scheduled_count = 0
        max_per_pool = 2

        # Inner function to pick next pipeline from a queue that has an assignable op
        def _pop_next_runnable(queue_list):
            # Rotate FIFO until we find runnable; keep others in order.
            for _ in range(len(queue_list)):
                p = queue_list.pop(0)
                if _is_done_or_dead(p):
                    continue
                op_list = _get_assignable_op(p)
                if op_list:
                    return p, op_list
                # Not runnable yet; requeue
                queue_list.append(p)
            return None, None

        # Pass A: high priority first (QUERY then INTERACTIVE)
        for pri_name, q in (("query", s.q_query), ("interactive", s.q_interactive)):
            while scheduled_count < max_per_pool:
                # Enforce reserved headroom for this priority: don't let lower priorities have eaten it;
                # but within this loop, we are scheduling high priority so we can dip into reserved.
                if avail_cpu <= 0 or avail_ram <= 0:
                    break

                p, op_list = _pop_next_runnable(q)
                if not p:
                    break

                pri = p.priority
                # For high priority, we can use essentially all current availability
                cpu = _cpu_target(pool, pri, avail_cpu)
                ram = _ram_target(pool, p, op_list[0], avail_ram)

                # Guard: must fit something meaningful
                if cpu <= 0 or ram <= 0:
                    q.insert(0, p)  # put it back
                    break

                # If RAM target is too small relative to availability floor, try to bump modestly.
                if avail_ram >= 2.0 * ram:
                    ram = min(avail_ram, max(ram, 0.30 * pool.max_ram_pool))

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu,
                        ram=ram,
                        priority=pri,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id
                    )
                )

                avail_cpu -= cpu
                avail_ram -= ram
                scheduled_count += 1
                # Requeue pipeline for future operators
                q.append(p)

        # If no high priority scheduled and there is high priority waiting but pool has little headroom,
        # consider preempting one batch container (light preemption).
        high_waiting = len(s.q_query) + len(s.q_interactive) > 0
        if high_waiting and scheduled_count == 0 and pool_id not in s.preempted_this_tick:
            # Check whether a reservation threshold is violated; if so, try to preempt batch.
            need_ram = (pool.max_ram_pool * 0.10)  # enough to run small op
            need_cpu = (pool.max_cpu_pool * 0.10)
            if avail_ram < need_ram or avail_cpu < need_cpu:
                batch_containers = _collect_running_batch_containers_in_pool(pool_id)
                if batch_containers:
                    # Suspend one batch container to free some resources.
                    cid = batch_containers[0]
                    suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
                    s.preempted_this_tick.add(pool_id)

        # Refresh availability after possible preemption (simulator will apply next tick; still proceed with current avail)
        # Pass B: schedule batch but respect reserved headroom for query/interactive (don't consume the last slice).
        # Compute effective available after keeping reservations.
        reserve_q_ram, reserve_q_cpu = _pool_reserved(pool, Priority.QUERY)
        reserve_i_ram, reserve_i_cpu = _pool_reserved(pool, Priority.INTERACTIVE)
        reserve_ram = reserve_q_ram + reserve_i_ram
        reserve_cpu = reserve_q_cpu + reserve_i_cpu

        eff_avail_ram = max(0.0, avail_ram - reserve_ram)
        eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)

        while scheduled_count < max_per_pool and eff_avail_cpu > 0 and eff_avail_ram > 0 and s.q_batch:
            p, op_list = _pop_next_runnable(s.q_batch)
            if not p:
                break

            pri = p.priority
            cpu = _cpu_target(pool, pri, eff_avail_cpu)
            ram = _ram_target(pool, p, op_list[0], eff_avail_ram)

            if cpu <= 0 or ram <= 0:
                s.q_batch.insert(0, p)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pri,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id
                )
            )

            eff_avail_cpu -= cpu
            eff_avail_ram -= ram
            avail_cpu -= cpu
            avail_ram -= ram
            scheduled_count += 1
            s.q_batch.append(p)

    # Clear preemption markers each tick (preemption is conservative; only one per pool per tick)
    s.preempted_this_tick.clear()

    return suspensions, assignments
