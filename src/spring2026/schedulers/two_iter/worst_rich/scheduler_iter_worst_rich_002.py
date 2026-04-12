# policy_key: scheduler_iter_worst_rich_002
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041448
# generation_seconds: 42.14
# generated_at: 2026-04-12T00:49:40.640074
@register_scheduler_init(key="scheduler_iter_worst_rich_002")
def scheduler_iter_worst_rich_002_init(s):
    """
    Priority-aware, OOM-resistant FIFO with retries and multi-op packing per pool.

    Fixes obvious flaws from the previous attempt:
      - Do NOT drop pipelines just because an operator failed; retry FAILED ops.
      - Request materially larger RAM by default (OOMs dominated previously).
      - Learn per-operator RAM requirement from OOM failures via exponential backoff.
      - Pack multiple assignments per pool in one tick while resources remain.
      - Keep simple priority queues (QUERY > INTERACTIVE > BATCH) and mild anti-starvation aging.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # tick counter for simple aging
    s.tick = 0
    s.enqueued_at = {}  # pipeline_id -> tick

    # per-op learned hints + retry counts
    s.op_ram_hint = {}      # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}      # (pipeline_id, op_key) -> cpu
    s.op_retry_count = {}   # (pipeline_id, op_key) -> int

    # if a pipeline has many repeated failures, we can dampen its priority a bit by not over-packing it
    s.pipeline_fail_count = {}  # pipeline_id -> int

    # configuration knobs (small, robust improvements first)
    s.batch_promotion_ticks = 400  # after this, treat batch as interactive for dispatch
    s.max_retries_per_op = 8       # beyond this, stop retrying that operator

    # default sizing fractions of pool max RAM (big enough to avoid OOM storms)
    s.default_ram_frac_query = 0.85
    s.default_ram_frac_interactive = 0.80
    s.default_ram_frac_batch = 0.65

    # CPU defaults: keep moderate to allow more concurrency in the pool
    s.default_cpu_query = 4
    s.default_cpu_interactive = 2
    s.default_cpu_batch = 2


@register_scheduler(key="scheduler_iter_worst_rich_002")
def scheduler_iter_worst_rich_002_scheduler(s, results, pipelines):
    """
    Each step:
      1) Enqueue new pipelines into per-priority queues.
      2) Update per-operator hints from results (OOM => RAM backoff).
      3) For each pool, repeatedly pick highest-priority runnable op and assign while resources remain.
         - Prefer keeping pool 0 for QUERY/INTERACTIVE when there are multiple pools.
         - Use learned min-RAM (or operator-provided min RAM if available) to avoid OOM.
    """
    s.tick += 1

    # ---------------- Helpers ----------------
    def _base_queue(p):
        if p.priority == Priority.QUERY:
            return s.q_query
        if p.priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.enqueued_at:
            s.enqueued_at[pid] = s.tick
            _base_queue(p).append(p)

    def _drop_pipeline_bookkeeping(pid):
        s.enqueued_at.pop(pid, None)
        s.pipeline_fail_count.pop(pid, None)
        # Keep op hints; they might help if the pipeline is re-submitted later with same IDs.

    def _op_key(op):
        # Best-effort stable identity
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _effective_priority(p):
        # simple aging to reduce starvation
        pid = p.pipeline_id
        waited = s.tick - s.enqueued_at.get(pid, s.tick)
        if p.priority == Priority.BATCH_PIPELINE and waited >= s.batch_promotion_ticks:
            return Priority.INTERACTIVE
        return p.priority

    def _prio_rank(prio):
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1

    def _pool_pref_ok(pool_id, eff_prio, num_pools):
        # If multiple pools: pool 0 is latency pool. Try to keep BATCH off pool 0 when possible.
        if num_pools <= 1:
            return True
        if pool_id == 0 and eff_prio == Priority.BATCH_PIPELINE:
            return False
        return True

    def _get_ready_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return None
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _op_min_ram(op):
        # Try common attribute names for operator RAM minimum requirement.
        for attr in ("min_ram", "ram_min", "min_ram_gb", "ram_min_gb", "min_memory", "memory_min"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is None:
                        continue
                    # If it's float (e.g., GB), keep as-is; simulator likely uses consistent units.
                    return float(v)
                except Exception:
                    pass
        return None

    def _oom_like(err):
        e = str(err or "").lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e)

    def _default_ram_frac_for_priority(prio):
        if prio == Priority.QUERY:
            return s.default_ram_frac_query
        if prio == Priority.INTERACTIVE:
            return s.default_ram_frac_interactive
        return s.default_ram_frac_batch

    def _default_cpu_for_priority(prio):
        if prio == Priority.QUERY:
            return s.default_cpu_query
        if prio == Priority.INTERACTIVE:
            return s.default_cpu_interactive
        return s.default_cpu_batch

    def _size_request(p, op, pool_id):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        eff_prio = _effective_priority(p)

        # CPU: moderate defaults; never exceed availability
        base_cpu = min(avail_cpu, max(1, min(pool.max_cpu_pool, _default_cpu_for_priority(eff_prio))))

        # RAM: big default fraction of pool max to avoid initial OOM storm,
        # then max with operator min_ram (if exposed) and learned hint.
        frac = _default_ram_frac_for_priority(eff_prio)
        base_ram = frac * float(pool.max_ram_pool)

        min_ram = _op_min_ram(op)
        if min_ram is not None:
            base_ram = max(base_ram, float(min_ram))

        key = (p.pipeline_id, _op_key(op))
        hinted_ram = s.op_ram_hint.get(key, None)
        if hinted_ram is not None:
            base_ram = max(base_ram, float(hinted_ram))

        # Bound by availability/max; ensure at least 1 unit to make progress
        req_ram = min(float(pool.max_ram_pool), float(avail_ram), base_ram)
        req_cpu = int(min(pool.max_cpu_pool, avail_cpu, max(1, int(base_cpu))))

        # If req_ram is fractional, keep as-is if simulator accepts floats; otherwise cast safely.
        try:
            req_ram_cast = int(req_ram)
            if req_ram_cast <= 0:
                req_ram_cast = 1
            req_ram = req_ram_cast
        except Exception:
            # leave as float
            if req_ram <= 0:
                req_ram = 1

        return req_cpu, req_ram

    # ---------------- Ingest pipelines ----------------
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ---------------- Learn from results (especially OOM) ----------------
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        pid = getattr(r, "pipeline_id", None)
        if pid is not None:
            s.pipeline_fail_count[pid] = s.pipeline_fail_count.get(pid, 0) + 1

        ops = getattr(r, "ops", []) or []
        if pid is None or not ops:
            continue

        err = getattr(r, "error", "")
        is_oom = _oom_like(err)

        prev_ram_alloc = getattr(r, "ram", None)
        prev_cpu_alloc = getattr(r, "cpu", None)

        for op in ops:
            ok = (pid, _op_key(op))
            s.op_retry_count[ok] = s.op_retry_count.get(ok, 0) + 1

            # Exponential backoff on RAM for OOM; gentler bump otherwise.
            cur_hint = s.op_ram_hint.get(ok, prev_ram_alloc if prev_ram_alloc is not None else 1)
            try:
                cur_hint_f = float(cur_hint)
            except Exception:
                cur_hint_f = 1.0

            if is_oom:
                # If previous allocation unknown/tiny, jump to a meaningful floor next time.
                base = float(prev_ram_alloc) if prev_ram_alloc is not None else cur_hint_f
                if base < 1:
                    base = 1.0
                new_hint = max(cur_hint_f, base * 2.0)
                s.op_ram_hint[ok] = new_hint
            else:
                base = float(prev_ram_alloc) if prev_ram_alloc is not None else cur_hint_f
                if base < 1:
                    base = 1.0
                new_hint = max(cur_hint_f, base * 1.25 + 1.0)
                s.op_ram_hint[ok] = new_hint

            # CPU hint: only lightly bump on non-OOM failures
            if prev_cpu_alloc is not None:
                cur_cpu = s.op_cpu_hint.get(ok, prev_cpu_alloc)
                try:
                    cur_cpu_i = int(cur_cpu)
                except Exception:
                    cur_cpu_i = 1
                if not is_oom:
                    s.op_cpu_hint[ok] = max(cur_cpu_i, int(prev_cpu_alloc) + 1)

    # ---------------- Dispatch ----------------
    suspensions = []
    assignments = []

    # We keep pool 0 as latency pool when multiple pools exist by iterating it first,
    # but restricting batch placements to non-zero pools unless forced.
    num_pools = s.executor.num_pools
    pool_order = list(range(num_pools))

    # We will pack multiple assignments per pool while capacity remains.
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # keep trying to fill this pool this tick
        made_progress = True
        while made_progress:
            made_progress = False

            chosen_p = None
            chosen_op = None
            chosen_eff_prio = None
            chosen_queue = None

            # Scan queues in priority order; bounded scan per queue (one pass).
            for q in (s.q_query, s.q_interactive, s.q_batch):
                qlen = len(q)
                for _ in range(qlen):
                    p = q.popleft()
                    st = p.runtime_status()

                    if st.is_pipeline_successful():
                        _drop_pipeline_bookkeeping(p.pipeline_id)
                        continue

                    eff_prio = _effective_priority(p)

                    # If this pool is "latency pool", avoid batch here when there are other pools.
                    if not _pool_pref_ok(pool_id, eff_prio, num_pools):
                        q.append(p)
                        continue

                    op = _get_ready_op(p)
                    if op is None:
                        q.append(p)
                        continue

                    # Respect retry limit per op to avoid infinite OOM loops.
                    ok = (p.pipeline_id, _op_key(op))
                    if s.op_retry_count.get(ok, 0) > s.max_retries_per_op:
                        # Give up on this op by not scheduling it further; keep pipeline in queue
                        # so other independent ops (if any) might run. If DAG is blocked, it will time out.
                        q.append(p)
                        continue

                    chosen_p = p
                    chosen_op = op
                    chosen_eff_prio = eff_prio
                    chosen_queue = q
                    break

                if chosen_p is not None:
                    break

            if chosen_p is None:
                break

            # Size request based on pool availability and OOM hints
            req_cpu, req_ram = _size_request(chosen_p, chosen_op, pool_id)

            # If cannot fit, requeue and stop trying to pack this pool further this tick
            if req_cpu <= 0 or req_ram <= 0 or req_cpu > pool.avail_cpu_pool or req_ram > pool.avail_ram_pool:
                # Put pipeline back (front-ish) to avoid losing it; simple append for determinism.
                chosen_queue.append(chosen_p)
                break

            # Create assignment (one op at a time)
            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=chosen_p.priority,  # keep original priority for accounting
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            # Requeue the pipeline for subsequent ops
            chosen_queue.append(chosen_p)

            # Update pool view for continued packing (pool object reflects executor state next tick,
            # so we conservatively decrement our local notion by mutating temporary variables only).
            # We rely on checking pool.avail_* each loop; since it won't change until next tick,
            # we manually adjust via shadowing to avoid over-allocating in one tick.
            try:
                pool.avail_cpu_pool -= req_cpu
                pool.avail_ram_pool -= req_ram
            except Exception:
                # If pool fields are read-only, just stop after one assignment for safety.
                break

            made_progress = True

    return suspensions, assignments
