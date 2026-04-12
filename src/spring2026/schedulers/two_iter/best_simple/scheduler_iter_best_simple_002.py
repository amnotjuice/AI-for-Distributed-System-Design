# policy_key: scheduler_iter_best_simple_002
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046284
# generation_seconds: 50.81
# generated_at: 2026-04-11T22:39:46.430375
@register_scheduler_init(key="scheduler_iter_best_simple_002")
def scheduler_iter_best_simple_002_init(s):
    """Priority-aware scheduler optimized for weighted latency (incremental, safe improvements).

    Improvements vs. naive FIFO / prior iteration:
      1) Strict priority ordering (QUERY > INTERACTIVE > BATCH) with round-robin fairness within class.
      2) Pool-aware placement:
           - If multiple pools: steer QUERY/INTERACTIVE to pool 0 first; steer BATCH away from pool 0.
      3) Soft reservations (admission control) on each pool:
           - When high-priority queues are non-empty, keep a fraction of CPU/RAM unspent by BATCH.
      4) Contention-aware CPU caps:
           - When there are many high-priority items waiting, cap per-op CPU to increase concurrency.
      5) Simple OOM-aware RAM backoff keyed by an operator ¡°signature¡±.

    Notes:
      - No preemption: simulator API here doesn¡¯t expose a reliable list of running containers to suspend.
      - RAM beyond minimum doesn¡¯t speed up ops; we keep RAM tight for concurrency and only grow on OOM.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Drop pipelines that experience non-OOM failures repeatedly (best-effort; often pipeline status handles it).
    s.dead_pipelines = set()

    # Operator RAM backoff multiplier keyed by signature -> multiplier (1,2,4,...)
    s.op_ram_mult = {}

    # Track latest pipeline objects by id to allow cleanup/dedup across ticks.
    s.pipelines_by_id = {}

    def _op_signature(op) -> str:
        # Attempt a stable signature across retries for the "same kind" of operator.
        # Prefer explicit ids/names if available.
        for attr in ("op_id", "operator_id", "name", "op_name", "task_name", "func_name"):
            v = getattr(op, attr, None)
            if v is not None:
                return f"{attr}:{v}"
        # SQL text can be large; just include a prefix if present.
        sql = getattr(op, "sql", None)
        if isinstance(sql, str) and sql:
            return "sql:" + sql[:80]
        # Fall back to type name + string repr prefix.
        r = str(op)
        return f"type:{type(op).__name__}|repr:{r[:80]}"

    s._op_signature = _op_signature


@register_scheduler(key="scheduler_iter_best_simple_002")
def scheduler_iter_best_simple_002_scheduler(s, results, pipelines):
    """Strict-priority, reservation-based scheduler to reduce weighted latency.

    Core loop:
      - Enqueue new pipelines by priority.
      - Process failures: on OOM-like errors, increase RAM multiplier for that operator signature.
      - For each pool, schedule multiple ops while headroom exists:
          * Prefer QUERY, then INTERACTIVE, then BATCH.
          * Keep CPU/RAM reservations for higher priorities so BATCH cannot crowd them out.
          * Size CPU for latency: give QUERY/INTERACTIVE decent CPU, but cap when contention is high.
    """
    # ----- helpers -----
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _enqueue_pipeline(p):
        # Keep latest object; avoid infinite reprocessing of known-dead pipelines.
        s.pipelines_by_id[p.pipeline_id] = p
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p.pipeline_id)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p.pipeline_id)
        else:
            s.q_batch.append(p.pipeline_id)

    def _queue_nonempty(q):
        # q stores pipeline_ids; validate existence and liveness lazily
        while q:
            pid = q[0]
            p = s.pipelines_by_id.get(pid)
            if p is None or pid in s.dead_pipelines:
                q.popleft()
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                q.popleft()
                continue
            return True
        return False

    def _pop_next_runnable_from_queue(q):
        # Round-robin scan bounded by current queue length.
        n = len(q)
        for _ in range(n):
            pid = q.popleft()
            p = s.pipelines_by_id.get(pid)
            if p is None or pid in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            # Pick a single runnable op (parents complete).
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                return pid, p, op_list
            # Not runnable yet; rotate.
            q.append(pid)
        return None, None, None

    def _peek_pressure():
        # Coarse contention signals to cap per-op CPU for better concurrency.
        q_has = _queue_nonempty(s.q_query)
        i_has = _queue_nonempty(s.q_interactive)
        b_has = _queue_nonempty(s.q_batch)
        # Approximate backlog by current queue lengths (includes some non-runnable; fine for heuristic).
        return {
            "query_waiting": q_has,
            "interactive_waiting": i_has,
            "batch_waiting": b_has,
            "query_depth": len(s.q_query),
            "interactive_depth": len(s.q_interactive),
            "batch_depth": len(s.q_batch),
        }

    def _soft_reservations(pool, pressure):
        # Fractions of pool to keep available for high priority; prevents BATCH from crowding out.
        # Tuned for weighted latency: protect QUERY most aggressively.
        if pressure["query_waiting"]:
            return 0.60, 0.50  # reserve 60% CPU, 50% RAM
        if pressure["interactive_waiting"]:
            return 0.35, 0.30
        return 0.0, 0.0

    def _choose_pool_order_for_priority(priority):
        # If multiple pools, prefer a "fast lane" for interactive work (pool 0).
        # Batch prefers other pools first to reduce interference.
        n = s.executor.num_pools
        if n <= 1:
            return [0]
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + list(range(1, n))
        # batch
        return list(range(1, n)) + [0]

    def _size_request(priority, pool, op, pressure, avail_cpu, avail_ram):
        # CPU sizing aims to reduce latency but keep concurrency under contention.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # Base CPU caps by priority (absolute + fraction).
        if priority == Priority.QUERY:
            base_cap = min(4.0, max_cpu * 0.50)
            # Under query contention, cap lower to run more queries concurrently.
            if pressure["query_depth"] >= 3:
                base_cap = min(base_cap, 2.0)
            floor = 1.0
        elif priority == Priority.INTERACTIVE:
            base_cap = min(6.0, max_cpu * 0.60)
            if pressure["interactive_depth"] >= 4 or pressure["query_waiting"]:
                base_cap = min(base_cap, 3.0)
            floor = 1.0
        else:  # BATCH_PIPELINE
            # Batch should fill leftovers; allow larger but don't monopolize.
            base_cap = min(10.0, max_cpu * 0.80)
            if pressure["query_waiting"] or pressure["interactive_waiting"]:
                base_cap = min(base_cap, 4.0)
            floor = 1.0

        cpu = min(avail_cpu, base_cap)
        if cpu < floor:
            # If we can't meet a minimal useful CPU, don't schedule.
            return 0.0, 0.0

        # RAM sizing: keep tight for QUERY, moderate for INTERACTIVE, bigger for BATCH.
        if priority == Priority.QUERY:
            base_ram = max_ram * 0.12
        elif priority == Priority.INTERACTIVE:
            base_ram = max_ram * 0.20
        else:
            base_ram = max_ram * 0.35

        sig = s._op_signature(op)
        mult = s.op_ram_mult.get(sig, 1.0)
        ram = base_ram * mult
        ram = min(ram, avail_ram, max_ram)

        # If RAM too small to be meaningful, don't schedule.
        if ram <= 0.01:
            return 0.0, 0.0

        return cpu, ram

    # ----- ingest new pipelines -----
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ----- process results: adapt RAM on OOM -----
    for r in results:
        if not r.failed():
            continue

        if _is_oom_error(r.error):
            for op in (r.ops or []):
                sig = s._op_signature(op)
                cur = s.op_ram_mult.get(sig, 1.0)
                nxt = cur * 2.0
                if nxt > 16.0:
                    nxt = 16.0
                s.op_ram_mult[sig] = nxt
        else:
            # Best-effort: mark dead only if pipeline_id exists on result in this simulator version.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    # Global pressure snapshot for this tick (cheap, stable decisions).
    pressure = _peek_pressure()

    # ----- schedule across pools, priority-aware -----
    # We iterate pools; within each pool, schedule multiple ops as long as resources remain.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0.01 or avail_ram <= 0.01:
            continue

        # Soft reservations: only apply to BATCH decisions on this pool.
        res_cpu_frac, res_ram_frac = _soft_reservations(pool, pressure)
        res_cpu = pool.max_cpu_pool * res_cpu_frac
        res_ram = pool.max_ram_pool * res_ram_frac

        # Fill the pool. Stop if nothing runnable or can't allocate meaningful resources.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            # Decide which priority to pull next, but avoid letting BATCH consume reserved headroom.
            picked = None

            # Always try QUERY then INTERACTIVE first.
            if _queue_nonempty(s.q_query):
                pid, p, op_list = _pop_next_runnable_from_queue(s.q_query)
                if p is not None:
                    picked = (pid, p, op_list)
            if picked is None and _queue_nonempty(s.q_interactive):
                pid, p, op_list = _pop_next_runnable_from_queue(s.q_interactive)
                if p is not None:
                    picked = (pid, p, op_list)
            if picked is None and _queue_nonempty(s.q_batch):
                # Only allow batch if we won't eat into reserved headroom needed for higher priority.
                if (not pressure["query_waiting"] and not pressure["interactive_waiting"]) or (
                    avail_cpu - 0.01 >= res_cpu and avail_ram - 0.01 >= res_ram
                ):
                    pid, p, op_list = _pop_next_runnable_from_queue(s.q_batch)
                    if p is not None:
                        picked = (pid, p, op_list)

            if picked is None:
                break

            pid, p, op_list = picked
            op = op_list[0]

            # Pool steering: if multiple pools, don't schedule batch on pool 0 unless it's the only option.
            if s.executor.num_pools > 1 and p.priority == Priority.BATCH_PIPELINE and pool_id == 0:
                # If other pools have capacity, defer batch from pool 0 to reduce interference.
                other_has_room = False
                for j in range(1, s.executor.num_pools):
                    pj = s.executor.pools[j]
                    if pj.avail_cpu_pool > 0.5 and pj.avail_ram_pool > 0.5:
                        other_has_room = True
                        break
                if other_has_room:
                    # Re-enqueue and stop scheduling batch on this pool for now.
                    s.q_batch.append(pid)
                    # Try to schedule something else instead of spinning.
                    # Continue loop to attempt QUERY/INTERACTIVE again.
                    continue

            cpu, ram = _size_request(p.priority, pool, op, pressure, avail_cpu, avail_ram)
            if cpu <= 0.01 or ram <= 0.01:
                # Can't place now; rotate pipeline id to back for fairness and stop for this pool.
                if p.priority == Priority.QUERY:
                    s.q_query.append(pid)
                elif p.priority == Priority.INTERACTIVE:
                    s.q_interactive.append(pid)
                else:
                    s.q_batch.append(pid)
                break

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

            # Decrement local headroom; round-robin fairness: push pipeline id to back.
            avail_cpu -= cpu
            avail_ram -= ram
            if p.priority == Priority.QUERY:
                s.q_query.append(pid)
            elif p.priority == Priority.INTERACTIVE:
                s.q_interactive.append(pid)
            else:
                s.q_batch.append(pid)

    # ----- second pass: if multiple pools, attempt steering by reassigning queue processing order -----
    # (No direct migrations; this just ensures next tick has balanced distribution by not over-consuming pool 0.)
    # Nothing to do explicitly here; queues persist.

    return suspensions, assignments
