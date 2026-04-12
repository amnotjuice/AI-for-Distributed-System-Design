# policy_key: scheduler_iter_worst_simple_011
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.061370
# generation_seconds: 38.87
# generated_at: 2026-04-12T00:41:56.932148
@register_scheduler_init(key="scheduler_iter_worst_simple_011")
def scheduler_iter_worst_simple_011_init(s):
    """
    Iteration 2: Priority-first + SRPT-ish within priority + better packing.

    Changes vs naive FIFO (and fixes obvious flaws):
      - Never drop a pipeline just because it has FAILED ops; FAILED is retryable (ASSIGNABLE_STATES).
      - Maintain a single waiting set, and choose next runnable pipeline by:
          (effective_priority, estimated_remaining_ops, enqueue_time)
        This approximates minimizing (weighted) latency by preferring high priority and shorter remaining work.
      - Fill each pool with multiple assignments per tick (while resources remain), instead of 1-op-per-pool.
      - Keep pool 0 "latency-friendly" when multiple pools exist: avoid scheduling batch there if any
        QUERY/INTERACTIVE runnable work exists anywhere in the queues.
      - Conservative initial RAM (pack more concurrent ops) + exponential RAM backoff on OOM-like failures.
    """
    s.waiting = []  # List[Pipeline]
    s.enqueued_at = {}  # pipeline_id -> tick (for tie-breaks and aging)
    s.tick = 0

    # Per-op learned minimum RAM (from OOM-like failures). Keyed by (pipeline_id, op_key)
    s.op_ram_hint = {}

    # Anti-starvation promotion for BATCH after enough scheduler steps waiting
    s.batch_promotion_ticks = 250

    # Minimal slices (units are whatever the simulator uses for pool cpu/ram quantities)
    s.min_cpu = 1
    s.min_ram = 1

    # For packing: don't let a single assignment consume *all* remaining resources
    s.max_share_query = 0.75
    s.max_share_interactive = 0.60
    s.max_share_batch = 0.40

    # Upper cap as fraction of pool max (avoid one big batch op monopolizing)
    s.cap_frac_query = 0.95
    s.cap_frac_interactive = 0.85
    s.cap_frac_batch = 0.60


@register_scheduler(key="scheduler_iter_worst_simple_011")
def scheduler_iter_worst_simple_011_scheduler(s, results, pipelines):
    """
    Main scheduling loop:
      1) Ingest pipelines (dedupe by pipeline_id).
      2) Update RAM hints from failures (OOM-like => exponential RAM bump).
      3) For each pool, repeatedly pick the best runnable pipeline for that pool and assign 1 ready op
         until resources are too low to place another minimal container.
    """
    s.tick += 1

    # ---------------- Helpers ----------------
    def _prio_rank(prio):
        # Higher is better.
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1  # BATCH_PIPELINE (or unknown)

    def _effective_priority(p):
        # Promote old batch to interactive to avoid indefinite starvation.
        pid = p.pipeline_id
        waited = s.tick - s.enqueued_at.get(pid, s.tick)
        if p.priority == Priority.BATCH_PIPELINE and waited >= s.batch_promotion_ticks:
            return Priority.INTERACTIVE
        return p.priority

    def _op_key(op):
        # Stable-ish identity within a run.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _pipeline_total_ops(p):
        # Best-effort total ops count to estimate remaining work.
        try:
            return len(p.values)
        except Exception:
            return 0

    def _pipeline_remaining_ops(p):
        st = p.runtime_status()
        total = _pipeline_total_ops(p)
        # If total unknown, approximate using state counts (pending+assigned+running+failed+suspending)
        if total <= 0:
            total = 0
            for k in (OperatorState.PENDING, OperatorState.ASSIGNED, OperatorState.RUNNING,
                      OperatorState.SUSPENDING, OperatorState.FAILED, OperatorState.COMPLETED):
                total += int(st.state_counts.get(k, 0) or 0)
        completed = int(st.state_counts.get(OperatorState.COMPLETED, 0) or 0)
        rem = total - completed
        return rem if rem > 0 else 1

    def _ready_ops_one(p):
        st = p.runtime_status()
        # Allow retrying FAILED ops; only schedule if parents are complete.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]

    def _is_pipeline_done(p):
        st = p.runtime_status()
        return bool(st.is_pipeline_successful())

    def _any_high_prio_runnable():
        # Used to protect pool 0 from batch if QUERY/INTERACTIVE runnable exists.
        for p in s.waiting:
            if _is_pipeline_done(p):
                continue
            ep = _effective_priority(p)
            if ep in (Priority.QUERY, Priority.INTERACTIVE):
                if _ready_ops_one(p):
                    return True
        return False

    def _pool_caps(pool, eff_prio):
        if eff_prio == Priority.QUERY:
            cap_frac = s.cap_frac_query
            max_share = s.max_share_query
        elif eff_prio == Priority.INTERACTIVE:
            cap_frac = s.cap_frac_interactive
            max_share = s.max_share_interactive
        else:
            cap_frac = s.cap_frac_batch
            max_share = s.max_share_batch

        cpu_cap = max(s.min_cpu, int(pool.max_cpu_pool * cap_frac))
        ram_cap = max(s.min_ram, int(pool.max_ram_pool * cap_frac))
        return cpu_cap, ram_cap, max_share

    def _size_for_op(p, op, pool_id):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
            return 0, 0

        eff_prio = _effective_priority(p)
        cpu_cap, ram_cap, max_share = _pool_caps(pool, eff_prio)

        # Start small for packing; rely on backoff on OOM.
        # CPU: give QUERY a bit more by default, but avoid monopolizing remaining capacity.
        if eff_prio == Priority.QUERY:
            base_cpu = 2 if pool.max_cpu_pool >= 2 else 1
        elif eff_prio == Priority.INTERACTIVE:
            base_cpu = 1
        else:
            base_cpu = 1

        base_ram = s.min_ram

        # Apply learned RAM hint for this operator (OOM backoff).
        key = (p.pipeline_id, _op_key(op))
        hinted_ram = int(s.op_ram_hint.get(key, base_ram) or base_ram)

        # Share-based limiter: don't consume more than a fraction of remaining resources.
        cpu_share_lim = max(s.min_cpu, int(avail_cpu * max_share))
        ram_share_lim = max(s.min_ram, int(avail_ram * max_share))

        req_cpu = max(s.min_cpu, min(base_cpu, avail_cpu, cpu_cap, cpu_share_lim))
        req_ram = max(s.min_ram, min(hinted_ram, avail_ram, ram_cap, ram_share_lim))

        return req_cpu, req_ram

    def _best_runnable_for_pool(pool_id, protect_latency_pool):
        # Pick best runnable pipeline for this pool by:
        #   (effective_priority desc, remaining_ops asc, enqueue_time asc)
        # Additionally, if protect_latency_pool and this is pool 0, avoid BATCH.
        best_idx = None
        best_key = None
        best_ops = None

        for idx, p in enumerate(s.waiting):
            if _is_pipeline_done(p):
                continue

            eff_prio = _effective_priority(p)

            if protect_latency_pool and pool_id == 0 and eff_prio == Priority.BATCH_PIPELINE:
                # Keep pool 0 for latency-sensitive work when any exists.
                continue

            ops = _ready_ops_one(p)
            if not ops:
                continue

            key = (-_prio_rank(eff_prio), _pipeline_remaining_ops(p), s.enqueued_at.get(p.pipeline_id, 0))
            if best_key is None or key < best_key:
                best_key = key
                best_idx = idx
                best_ops = ops

        return best_idx, best_ops

    # ---------------- Ingest pipelines ----------------
    # Deduplicate by pipeline_id: if already enqueued, ignore this arrival event.
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.enqueued_at:
            s.enqueued_at[pid] = s.tick
            s.waiting.append(p)

    # ---------------- Learn from results ----------------
    # Focus on RAM backoff for OOM-like failures to converge quickly without wasting time.
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            continue

        err = str(getattr(r, "error", "") or "").lower()
        oom_like = ("oom" in err) or ("out of memory" in err) or ("memory" in err)

        ops = getattr(r, "ops", []) or []
        for op in ops:
            k = (pid, _op_key(op))

            prev = int(s.op_ram_hint.get(k, s.min_ram) or s.min_ram)
            used = int(getattr(r, "ram", prev) or prev)

            if oom_like:
                # Exponential bump from the last tried amount.
                bumped = max(prev, used) * 2
            else:
                # Non-OOM failure: smaller, still monotonic bump (sometimes failures are under-provision).
                bumped = max(prev, used) + 1

            s.op_ram_hint[k] = max(s.min_ram, bumped)

    # Prune completed pipelines (lazy cleanup).
    if results or pipelines:
        alive = []
        for p in s.waiting:
            if _is_pipeline_done(p):
                s.enqueued_at.pop(p.pipeline_id, None)
                continue
            alive.append(p)
        s.waiting = alive

    # Early exit if no candidates.
    if not s.waiting:
        return [], []

    # ---------------- Assignments (pack each pool) ----------------
    suspensions = []
    assignments = []

    protect_latency_pool = (s.executor.num_pools > 1) and _any_high_prio_runnable()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Repeatedly place one op at a time while resources remain.
        # Bound iterations to avoid pathological loops when hints/caps prevent progress.
        max_iters = max(1, len(s.waiting))
        iters = 0

        while iters < max_iters:
            iters += 1

            if pool.avail_cpu_pool < s.min_cpu or pool.avail_ram_pool < s.min_ram:
                break

            idx, ops = _best_runnable_for_pool(pool_id, protect_latency_pool)
            if idx is None or not ops:
                break

            p = s.waiting[idx]
            op = ops[0]
            req_cpu, req_ram = _size_for_op(p, op, pool_id)

            # If sizing fails due to limits, stop trying to pack this pool in this tick.
            if req_cpu < s.min_cpu or req_ram < s.min_ram:
                break

            # Final availability check (defensive; pool values may have changed after prior assignment).
            if req_cpu > pool.avail_cpu_pool or req_ram > pool.avail_ram_pool:
                break

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,  # keep original priority for metrics/accounting
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Continue packing; executor will deduct avail_{cpu,ram}_pool after this tick's assignments.

    return suspensions, assignments
