# policy_key: scheduler_iter_best_rich_018
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051773
# generation_seconds: 39.25
# generated_at: 2026-04-12T00:32:34.523854
@register_scheduler_init(key="scheduler_iter_best_rich_018")
def scheduler_iter_best_rich_018_init(s):
    """Priority + pool partitioning + adaptive RAM backoff (OOM) with lighter default RAM to improve concurrency.

    Goals vs previous iteration:
      - Reduce weighted latency by isolating high-priority work (QUERY/INTERACTIVE) from BATCH when multiple pools exist.
      - Reduce queueing by avoiding systematic over-allocation of RAM (high allocated%, low consumed% was a signal).
      - Keep OOMs under control via per-operator exponential RAM backoff, with gentle decay on successes.
      - Improve interactive timeout behavior by giving INTERACTIVE more CPU than QUERY by default, without letting
        any single op monopolize an entire pool.

    Notes:
      - No preemption yet (insufficient stable visibility into running containers in the provided interface).
      - Scheduling unit remains one runnable operator per pipeline at a time (keeps behavior predictable).
    """
    # Per-priority queues (round-robin within each).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track pipelines we consider "dead" (non-OOM repeated failures). Used as a safety valve.
    s.dead_pipelines = set()

    # Adaptive RAM multiplier per operator identity (id(op) is stable for the DAG object during sim).
    s.op_ram_mult = {}  # op_id -> float

    # Track recent failure counts per pipeline to stop thrashing on repeated non-OOM failures.
    s.pipeline_fail_counts = {}  # pipeline_id -> int

    # Weighted round-robin credits per priority to prevent total starvation.
    # Higher = more picks when all are backlogged.
    s.rr_credits = {
        "query": 0,
        "interactive": 0,
        "batch": 0,
    }
    s.rr_weights = {
        "query": 6,
        "interactive": 3,
        "batch": 1,
    }


@register_scheduler(key="scheduler_iter_best_rich_018")
def scheduler_iter_best_rich_018_scheduler(s, results, pipelines):
    """Priority-aware scheduler with high-priority pool isolation and adaptive RAM backoff."""
    # -------- helpers --------
    def _prio_bucket(p):
        if p.priority == Priority.QUERY:
            return "query"
        if p.priority == Priority.INTERACTIVE:
            return "interactive"
        return "batch"

    def _enqueue(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        b = _prio_bucket(p)
        if b == "query":
            s.q_query.append(p)
        elif b == "interactive":
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _pick_queue_for_pool(pool_id: int):
        # If we have multiple pools, reserve pool 0 for high priority by default.
        # Other pools are batch-first, but can steal high priority if present and batch is empty.
        if s.executor.num_pools >= 2:
            if pool_id == 0:
                return ["query", "interactive", "batch"]  # allow batch to use idle when no high-prio backlog
            else:
                return ["batch", "interactive", "query"]
        # Single pool: strict priority with WRR fairness.
        return ["query", "interactive", "batch"]

    def _queue_by_name(name: str):
        if name == "query":
            return s.q_query
        if name == "interactive":
            return s.q_interactive
        return s.q_batch

    def _refill_credits_if_needed():
        # If all credits are depleted, refill.
        if s.rr_credits["query"] <= 0 and s.rr_credits["interactive"] <= 0 and s.rr_credits["batch"] <= 0:
            for k, w in s.rr_weights.items():
                s.rr_credits[k] = w

    def _pop_next_runnable_for_names(names):
        # Use weighted round-robin across the allowed names, but still obey the passed order
        # as a tie-breaker (helps latency for high priority).
        _refill_credits_if_needed()

        # Try several passes to find runnable work without spinning forever.
        for _pass in range(3):
            for name in names:
                q = _queue_by_name(name)
                if not q:
                    continue
                if s.rr_credits[name] <= 0:
                    continue

                n = len(q)
                for _ in range(n):
                    p = q.pop(0)
                    if p.pipeline_id in s.dead_pipelines:
                        continue
                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        continue

                    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if op_list:
                        # Consume a credit only when we actually schedule from this bucket.
                        s.rr_credits[name] -= 1
                        return p, op_list, name

                    # Not runnable; rotate to back.
                    q.append(p)

            _refill_credits_if_needed()

        return None, None, None

    def _size(cpu_avail, ram_avail, pool, priority, op):
        # CPU caps: keep latency low by improving concurrency, but avoid starving ops into timeouts.
        # Query tends to be small/latency-sensitive; interactive may need more sustained CPU.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        if priority == Priority.QUERY:
            cpu_cap = min(4.0, max_cpu * 0.30)
            cpu_floor = 1.0
            base_ram_frac = 0.06
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(6.0, max_cpu * 0.45)
            cpu_floor = 2.0
            base_ram_frac = 0.10
        else:
            cpu_cap = min(8.0, max_cpu * 0.60)
            cpu_floor = 1.0
            base_ram_frac = 0.14

        # CPU request
        if cpu_avail < cpu_floor:
            return 0.0, 0.0
        cpu = min(cpu_avail, cpu_cap)
        cpu = max(cpu, cpu_floor)

        # RAM request: start smaller to avoid global RAM saturation; adapt upward on OOM.
        mult = s.op_ram_mult.get(id(op), 1.0)
        ram = (max_ram * base_ram_frac) * mult

        # Ensure minimal meaningful RAM request (avoid tiny fragments that never work).
        ram = max(ram, max_ram * 0.02)
        ram = min(ram, ram_avail, max_ram)

        if ram_avail < ram:
            # If we can't fit the target RAM, try a smaller allocation if still meaningful.
            ram = min(ram_avail, max_ram * 0.05)
            if ram < max_ram * 0.02:
                return 0.0, 0.0

        return cpu, ram

    # -------- ingest new pipelines --------
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # -------- process results: adapt RAM multipliers & stop hopeless thrash --------
    for r in results:
        if r.failed():
            oom = _is_oom_error(r.error)

            # Update operator RAM multipliers on OOM.
            if oom:
                for op in (r.ops or []):
                    k = id(op)
                    cur = s.op_ram_mult.get(k, 1.0)
                    nxt = cur * 2.0
                    if nxt > 32.0:
                        nxt = 32.0
                    s.op_ram_mult[k] = nxt
            else:
                # Non-OOM failures: count them per pipeline if we can infer pipeline id from result.
                pid = getattr(r, "pipeline_id", None)
                if pid is not None:
                    s.pipeline_fail_counts[pid] = s.pipeline_fail_counts.get(pid, 0) + 1
                    # After a few non-OOM failures, stop retrying this pipeline (prevents churn).
                    if s.pipeline_fail_counts[pid] >= 3:
                        s.dead_pipelines.add(pid)
        else:
            # Gentle decay on success to recover from one-off OOM spikes and reduce over-allocation over time.
            for op in (r.ops or []):
                k = id(op)
                cur = s.op_ram_mult.get(k, 1.0)
                if cur > 1.0:
                    cur = cur * 0.90
                    if cur < 1.0:
                        cur = 1.0
                    s.op_ram_mult[k] = cur

    suspensions = []
    assignments = []

    # -------- schedule: fill pools with appropriately prioritized work --------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        allowed = _pick_queue_for_pool(pool_id)

        # If this is the reserved high-priority pool, prevent batch from consuming too much
        # when high-priority backlog exists (soft reservation).
        if s.executor.num_pools >= 2 and pool_id == 0:
            high_backlog = (len(s.q_query) + len(s.q_interactive)) > 0
            batch_cpu_limit = pool.max_cpu_pool * (0.50 if high_backlog else 1.00)
            batch_ram_limit = pool.max_ram_pool * (0.50 if high_backlog else 1.00)
        else:
            batch_cpu_limit = pool.max_cpu_pool
            batch_ram_limit = pool.max_ram_pool

        while avail_cpu > 0.01 and avail_ram > 0.01:
            p, op_list, bucket = _pop_next_runnable_for_names(allowed)
            if p is None:
                break

            op = op_list[0]

            cpu, ram = _size(avail_cpu, avail_ram, pool, p.priority, op)
            if cpu <= 0.01 or ram <= 0.01:
                # Can't fit now; requeue and stop filling this pool this tick to avoid spinning.
                _enqueue(p)
                break

            # Enforce soft reservation on the high-priority pool: batch can't take more than half when backlog exists.
            if s.executor.num_pools >= 2 and pool_id == 0 and bucket == "batch":
                if (pool.max_cpu_pool - avail_cpu) + cpu > batch_cpu_limit or (pool.max_ram_pool - avail_ram) + ram > batch_ram_limit:
                    # Put batch back; try to schedule high-priority instead this tick.
                    s.q_batch.append(p)
                    # If high-priority exists, stop attempting further batch allocations.
                    if (len(s.q_query) + len(s.q_interactive)) > 0:
                        break
                    # Otherwise continue (no high prio waiting).
                    continue

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            avail_cpu -= cpu
            avail_ram -= ram

            # Round-robin fairness: put pipeline back after scheduling one op.
            _enqueue(p)

    return suspensions, assignments
