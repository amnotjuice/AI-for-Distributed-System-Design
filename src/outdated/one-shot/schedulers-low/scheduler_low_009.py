# policy_key: scheduler_low_009
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040823
# generation_seconds: 42.18
# generated_at: 2026-03-12T21:40:57.924391
@register_scheduler_init(key="scheduler_low_009")
def scheduler_low_009_init(s):
    """
    Priority-aware, conservative evolution of the naive FIFO scheduler.

    Improvements vs. naive:
      1) Separate queues by priority (QUERY/INTERACTIVE ahead of BATCH).
      2) Avoid giving *all* pool resources to a single op; cap per-op cpu/ram.
      3) Keep a small reserved headroom in each pool to protect high-priority latency.
      4) If an op fails with OOM, retry it with increased RAM (exponential backoff),
         instead of dropping the whole pipeline immediately.

    Notes:
      - No preemption in this version (keeps changes small/low-risk).
      - Single-op assignments (one op per tick per pool) to reduce interference.
    """
    from collections import deque

    # Per-priority waiting queues of pipelines
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Retry / sizing memory for operators that OOM
    # key: (pipeline_id, op_key) -> {"ram": float, "oom_retries": int}
    s.op_mem = {}

    # Track "fatal" failures to drop pipelines after non-OOM failures
    s.pipeline_fatal = set()

    # Simple fairness aging: count how many times we failed to schedule batch
    s.batch_starvation_ticks = 0


@register_scheduler(key="scheduler_low_009")
def scheduler_low_009_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Incorporate new pipelines into priority queues.
      - Process execution results to adjust memory for OOM failures and mark fatal failures.
      - For each pool, schedule at most one operator:
          * Prefer QUERY, then INTERACTIVE, then BATCH
          * Keep some pool headroom reserved for high-priority work
          * Size cpu/ram with simple caps; use OOM-informed RAM if available
    """
    # --- Helpers (kept inside to avoid module-level imports) ---
    def _prio_rank(p):
        # Smaller is higher priority
        if p == Priority.QUERY:
            return 0
        if p == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE and others

    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _is_oom_error(err):
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memoryerror" in e)

    def _op_key(op):
        # Best-effort stable key for sizing state.
        # Prefer common attributes if present; fallback to repr(op).
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return (attr, str(v))
                except Exception:
                    pass
        try:
            return ("repr", repr(op))
        except Exception:
            return ("fallback", str(id(op)))

    def _pick_next_pipeline():
        # Mild aging: if batch has been starved for a while, occasionally give it a turn.
        # Keeps logic simple and avoids indefinite starvation.
        if s.batch_starvation_ticks >= 20 and len(s.q_batch) > 0:
            s.batch_starvation_ticks = 0
            return s.q_batch.popleft()

        if len(s.q_query) > 0:
            return s.q_query.popleft()
        if len(s.q_interactive) > 0:
            return s.q_interactive.popleft()
        if len(s.q_batch) > 0:
            return s.q_batch.popleft()
        return None

    def _requeue_pipeline(p):
        # Requeue at the end of its priority queue.
        _enqueue_pipeline(p)

    def _reserve_for_high(pool, prio):
        # Keep some reserved capacity so high-priority arrivals don't face full pools.
        # This only constrains low priority; high priority can consume full available.
        # NOTE: These are deliberately small, incremental improvements.
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            return 0.0, 0.0
        # Reserve a fraction of total pool for interactive/query bursts
        cpu_res = 0.15 * pool.max_cpu_pool
        ram_res = 0.15 * pool.max_ram_pool
        return cpu_res, ram_res

    def _cap_for_priority(pool, prio, avail_cpu, avail_ram):
        # Avoid monopolizing the pool per op; cap to improve concurrency and latency.
        if prio == Priority.QUERY:
            cpu_cap = 0.60 * pool.max_cpu_pool
            ram_cap = 0.60 * pool.max_ram_pool
        elif prio == Priority.INTERACTIVE:
            cpu_cap = 0.45 * pool.max_cpu_pool
            ram_cap = 0.50 * pool.max_ram_pool
        else:
            cpu_cap = 0.30 * pool.max_cpu_pool
            ram_cap = 0.35 * pool.max_ram_pool

        cpu = min(avail_cpu, cpu_cap)
        ram = min(avail_ram, ram_cap)

        # Ensure we don't allocate "too tiny" slices that would just thrash.
        # (Still conservative; we don't know true minima here.)
        cpu = max(1.0, cpu) if cpu > 0 else 0.0
        ram = max(0.5, ram) if ram > 0 else 0.0
        return cpu, ram

    def _initial_ram_guess(pool, prio):
        # Starting point for unknown operators.
        if prio == Priority.QUERY:
            return max(0.5, 0.25 * pool.max_ram_pool)
        if prio == Priority.INTERACTIVE:
            return max(0.5, 0.20 * pool.max_ram_pool)
        return max(0.5, 0.15 * pool.max_ram_pool)

    # --- Incorporate new arrivals ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # --- Process results (OOM backoff + fatal failure marking) ---
    # We also re-enqueue pipelines that still have work, so they keep progressing.
    # (In Eudoxia the simulator passes new pipelines separately; we keep our own queue.)
    seen_pipelines_to_requeue = {}

    for r in results:
        # If a container failed with OOM, increase RAM next time for those ops.
        if hasattr(r, "failed") and r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                for op in getattr(r, "ops", []) or []:
                    k = (getattr(r, "pipeline_id", None), _op_key(op))
                    # If pipeline_id isn't present in result, fall back to op_key only
                    if k[0] is None:
                        k = ("unknown_pipeline", _op_key(op))
                    st = s.op_mem.get(k, {"ram": float(getattr(r, "ram", 0.0) or 0.0), "oom_retries": 0})
                    # Exponential backoff in RAM; start from last attempted RAM, else small baseline
                    base = st["ram"] if st["ram"] > 0 else float(getattr(r, "ram", 0.0) or 0.5)
                    st["ram"] = max(base * 2.0, base + 0.5)
                    st["oom_retries"] = int(st.get("oom_retries", 0)) + 1
                    s.op_mem[k] = st
            else:
                # Non-OOM failures are treated as fatal for the pipeline if we can identify it.
                pid = getattr(r, "pipeline_id", None)
                if pid is not None:
                    s.pipeline_fatal.add(pid)

    # --- Scheduling loop: one assignment per pool per tick (small, safe improvement) ---
    suspensions = []
    assignments = []

    # We'll attempt to schedule up to (num_pools) operators, one per pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try a bounded number of queue pops so we don't spin forever on blocked DAGs.
        max_tries = 12
        scheduled = False

        while max_tries > 0 and not scheduled:
            max_tries -= 1
            pipeline = _pick_next_pipeline()
            if pipeline is None:
                break

            status = pipeline.runtime_status()

            # Drop completed pipelines
            if status.is_pipeline_successful():
                continue

            # Drop pipelines with known fatal failure (non-OOM)
            if pipeline.pipeline_id in s.pipeline_fatal:
                continue

            # Choose one ready operator
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # DAG blocked (waiting for parents/running ops). Requeue it.
                _requeue_pipeline(pipeline)
                continue

            prio = pipeline.priority

            # Apply reserved headroom for low-priority only
            cpu_res, ram_res = _reserve_for_high(pool, prio)
            eff_avail_cpu = max(0.0, avail_cpu - cpu_res)
            eff_avail_ram = max(0.0, avail_ram - ram_res)

            if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                # Can't schedule low priority here now; requeue and mark starvation.
                _requeue_pipeline(pipeline)
                if prio == Priority.BATCH_PIPELINE:
                    s.batch_starvation_ticks += 1
                continue

            cpu, ram = _cap_for_priority(pool, prio, eff_avail_cpu, eff_avail_ram)

            # Apply OOM-informed RAM sizing if present
            op = op_list[0]
            mem_key = (pipeline.pipeline_id, _op_key(op))
            if mem_key in s.op_mem:
                # Don't exceed pool available RAM; keep within our cap and availability.
                desired = float(s.op_mem[mem_key].get("ram", 0.0) or 0.0)
                if desired > 0:
                    ram = min(ram, eff_avail_ram, desired)
            else:
                # Use an initial guess, but never exceed caps/availability
                ram = min(ram, eff_avail_ram, _initial_ram_guess(pool, prio))

            # If we ended up with too little to be meaningful, skip/requeue.
            if cpu <= 0 or ram <= 0:
                _requeue_pipeline(pipeline)
                continue

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )
            scheduled = True

            # Reset starvation tick if we successfully scheduled batch.
            if prio == Priority.BATCH_PIPELINE:
                s.batch_starvation_ticks = 0

            # If not scheduled, pipeline was requeued; loop to try another.

        # end while tries

    return suspensions, assignments
