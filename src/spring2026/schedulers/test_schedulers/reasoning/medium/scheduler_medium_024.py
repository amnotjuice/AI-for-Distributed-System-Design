# policy_key: scheduler_medium_024
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.077291
# generation_seconds: 57.39
# generated_at: 2026-04-09T23:00:57.367096
@register_scheduler_init(key="scheduler_medium_024")
def scheduler_medium_024_init(s):
    """
    Priority-weighted fair scheduler with OOM-adaptive RAM hints.

    Goals (aligned with objective):
      - Protect QUERY and INTERACTIVE tail latency via strict priority bias.
      - Avoid 720s penalties by aggressively learning per-operator RAM needs after failures.
      - Avoid starvation by using a weighted round-robin pick order across priorities.
      - Keep concurrency by not always grabbing the entire pool CPU for a single op.

    Key ideas:
      - Maintain per-priority pipeline queues.
      - Use a rotating pick list with weights (QUERY:10, INTERACTIVE:5, BATCH:1).
      - For each ready operator, request (cpu, ram) based on priority shares + learned RAM hint.
      - On any failure, increase RAM hint for the involved operators and retry (FAILED is assignable).
    """
    # Per-priority queues (store pipeline_ids; we keep an id->pipeline map too)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    s.pipeline_by_id = {}        # pipeline_id -> Pipeline
    s.enqueued = set()           # pipeline_ids currently present in some queue

    # Weighted RR schedule over priorities (more frequent service to high priority)
    s.pick_order = (
        [Priority.QUERY] * 10 +
        [Priority.INTERACTIVE] * 5 +
        [Priority.BATCH_PIPELINE] * 1
    )
    s.pick_ptr = 0

    # RAM hints and retry tracking per operator key
    s.op_ram_hint = {}           # (op_key) -> ram
    s.op_fail_count = {}         # (op_key) -> int

    # Light CPU sizing memory (optional; currently conservative constant shares)
    s.tick = 0


@register_scheduler(key="scheduler_medium_024")
def scheduler_medium_024_scheduler(s, results, pipelines):
    def _prio_queue(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(op, pipeline_id):
        # Best-effort stable key across ticks; fall back to repr().
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = getattr(op, "name", None)
        if op_id is None:
            op_id = repr(op)
        return (pipeline_id, op_id)

    def _is_done_or_terminal(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # We do NOT treat failures as terminal because FAILED ops are retryable (ASSIGNABLE_STATES).
        return False

    def _has_ready_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return ops[0] if ops else None

    def _base_shares(priority):
        # Conservative CPU to allow some concurrency, generous RAM to reduce OOM penalties.
        if priority == Priority.QUERY:
            return 0.50, 0.35   # cpu_share, ram_share
        if priority == Priority.INTERACTIVE:
            return 0.45, 0.30
        return 0.40, 0.22

    def _choose_pool_for(priority):
        # Prefer pools with more headroom for high priority; otherwise any with capacity.
        best = None
        best_score = None
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                continue
            # Score emphasizes RAM headroom (to reduce OOM), then CPU.
            score = (pool.avail_ram_pool / max(1e-9, pool.max_ram_pool)) * 2.0 + (
                pool.avail_cpu_pool / max(1e-9, pool.max_cpu_pool)
            )
            if priority == Priority.BATCH_PIPELINE:
                # Batch can opportunistically use fragmented pools; reduce tie-breaking strictness.
                score += 0.05 * (pool.avail_cpu_pool + pool.avail_ram_pool)
            if best is None or score > best_score:
                best = pool_id
                best_score = score
        return best

    def _request_resources(pool, priority, op_key):
        cpu_share, ram_share = _base_shares(priority)

        # CPU: don't monopolize; but ensure at least 1.
        base_cpu = int(max(1, round(pool.max_cpu_pool * cpu_share)))
        req_cpu = int(min(pool.avail_cpu_pool, base_cpu))
        if req_cpu <= 0:
            return 0, 0

        # RAM: start moderately high; raise on failures per-op.
        base_ram = pool.max_ram_pool * ram_share
        hint = s.op_ram_hint.get(op_key, 0.0)
        req_ram = max(base_ram, hint)

        # If this op has failed multiple times, be more aggressive (completion > utilization).
        fails = s.op_fail_count.get(op_key, 0)
        if fails >= 1:
            req_ram = max(req_ram, base_ram + 0.05 * pool.max_ram_pool)
        if fails >= 2:
            req_ram = max(req_ram, base_ram + 0.10 * pool.max_ram_pool)
        if fails >= 3:
            req_ram = max(req_ram, base_ram + 0.18 * pool.max_ram_pool)

        req_ram = min(pool.avail_ram_pool, min(pool.max_ram_pool, req_ram))
        if req_ram <= 0:
            return 0, 0

        return req_cpu, req_ram

    # --- ingest new pipelines ---
    for p in pipelines:
        s.pipeline_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s.enqueued:
            _prio_queue(p.priority).append(p.pipeline_id)
            s.enqueued.add(p.pipeline_id)

    # --- update RAM hints from results ---
    for r in results:
        if not hasattr(r, "ops") or not r.ops:
            continue

        # If failed, assume RAM may be insufficient; increase hint for those ops.
        if r.failed():
            # Use the pool where it ran to compute escalation and caps.
            pool = s.executor.pools[r.pool_id]
            ran_ram = float(getattr(r, "ram", 0.0) or 0.0)

            for op in r.ops:
                k = _op_key(op, getattr(r, "pipeline_id", None))
                # If pipeline_id isn't present on result, fall back to op's pipeline linkage via repr.
                if k[0] is None:
                    k = (repr(op), k[1])

                s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1

                prev = float(s.op_ram_hint.get(k, 0.0) or 0.0)
                # Escalate from what we tried (if known), otherwise from previous hint or base.
                start = max(prev, ran_ram, 0.10 * pool.max_ram_pool)
                # Multiplicative + additive bump to converge quickly and reduce repeated 720s penalties.
                bumped = max(start * 1.6, start + 0.08 * pool.max_ram_pool)
                s.op_ram_hint[k] = min(pool.max_ram_pool, bumped)
        else:
            # On success, keep hint as-is (no down-tuning to avoid regressions/penalties).
            pass

    # If no changes, exit early (keeps determinism and avoids wasted cycles).
    if not pipelines and not results:
        return [], []

    s.tick += 1
    suspensions = []
    assignments = []

    # Helper to pick next pipeline id using weighted RR, with basic queue hygiene.
    def _pick_next_pipeline_id():
        # Try up to one full rotation through pick_order to find a schedulable candidate.
        for _ in range(len(s.pick_order)):
            pr = s.pick_order[s.pick_ptr]
            s.pick_ptr = (s.pick_ptr + 1) % len(s.pick_order)
            q = _prio_queue(pr)

            # Pop/rotate until we find a pipeline that still exists and isn't completed.
            for _scan in range(len(q)):
                pid = q.pop(0)
                p = s.pipeline_by_id.get(pid, None)
                if p is None:
                    s.enqueued.discard(pid)
                    continue
                if _is_done_or_terminal(p):
                    s.enqueued.discard(pid)
                    continue

                # Keep it enqueued; rotate to end (round-robin within same priority).
                q.append(pid)
                return pid
        return None

    # For each pool, schedule as many ops as we can (one op per assignment for per-op sizing).
    for _ in range(s.executor.num_pools):
        # Choose which pool to fill next: prefer the best pool for high-priority work, but
        # iterate multiple times to give other pools opportunities too.
        # Strategy: attempt QUERY pool first, then INTERACTIVE, then BATCH; fall back to any.
        pool_id = _choose_pool_for(Priority.QUERY)
        if pool_id is None:
            pool_id = _choose_pool_for(Priority.INTERACTIVE)
        if pool_id is None:
            pool_id = _choose_pool_for(Priority.BATCH_PIPELINE)
        if pool_id is None:
            break

        pool = s.executor.pools[pool_id]

        # Local accounting for this call; we optimistically decrement after each assignment.
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Fill the selected pool
        while avail_cpu > 0 and avail_ram > 0:
            pid = _pick_next_pipeline_id()
            if pid is None:
                break

            p = s.pipeline_by_id.get(pid, None)
            if p is None:
                s.enqueued.discard(pid)
                continue

            op = _has_ready_op(p)
            if op is None:
                # Nothing ready yet; keep pipeline enqueued and try another.
                continue

            k = _op_key(op, p.pipeline_id)

            # Compute request against *current* pool snapshot, but also ensure it fits local avail.
            req_cpu, req_ram = _request_resources(pool, p.priority, k)
            req_cpu = int(min(avail_cpu, req_cpu))
            req_ram = float(min(avail_ram, req_ram))

            # If it doesn't fit, try another pool next iteration (don't spin on one pool).
            if req_cpu <= 0 or req_ram <= 0:
                break

            # Create the assignment for exactly one operator (per-container sizing).
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Update local availability; continue trying to pack more work.
            avail_cpu -= req_cpu
            avail_ram -= req_ram

    return suspensions, assignments
