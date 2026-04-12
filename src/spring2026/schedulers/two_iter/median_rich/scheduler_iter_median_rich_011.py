# policy_key: scheduler_iter_median_rich_011
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.063844
# generation_seconds: 52.47
# generated_at: 2026-04-12T01:51:58.544017
@register_scheduler_init(key="scheduler_iter_median_rich_011")
def scheduler_iter_median_rich_011_init(s):
    """Priority-aware RR scheduler with per-operator RAM hints to suppress OOMs.

    Changes vs naive FIFO / prior attempt:
    - Strict priority ordering (QUERY > INTERACTIVE > BATCH) but with round-robin within each class.
    - Per-operator adaptive RAM sizing using feedback from ExecutionResult (OOM => raise RAM for that op).
      This fixes the biggest flaw in the previous policy: global/coarse RAM multiplier causing both many OOMs
      and wasteful over-allocation across unrelated pipelines.
    - Conservative initial RAM baselines (higher than before) to reduce first-try OOMs and timeouts.
    - Light CPU shaping by priority to improve queueing latency (favor concurrency for high priority).
    - Soft headroom: when high-priority backlog exists, cap batch allocations per pool (CPU+RAM caps).
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Adaptive sizing based on observed results
    # op_hint_ram[op_key] := recommended RAM for this operator on next retry
    # op_hint_cpu[op_key] := recommended CPU for this operator on next retry (mostly for timeouts)
    s.op_hint_ram = {}
    s.op_hint_cpu = {}

    # Map ops -> pipeline_id (learned at assignment time) so we can attribute failures precisely
    s.op_to_pipeline = {}

    # Remember last requested resources per op for smoother backoff even if result omits something
    s.op_last_req = {}

    # Track pipelines that had a definite non-OOM failure (do not retry FAILED ops indefinitely)
    s.pipeline_drop = set()


@register_scheduler(key="scheduler_iter_median_rich_011")
def scheduler_iter_median_rich_011_scheduler(s, results, pipelines):
    """
    Scheduling loop:
    1) Enqueue new pipelines.
    2) Consume results to update per-op resource hints:
       - OOM: increase RAM hint aggressively for the specific op that failed.
       - timeout: increase CPU hint mildly (and a touch of RAM) for the specific op.
       - success: gently decay hints to avoid permanent over-allocation.
    3) For each pool, repeatedly:
       - pick the next runnable op from the highest priority queue (RR within priority),
       - size (cpu, ram) from op hints or priority baselines,
       - apply batch caps when high-priority backlog exists,
       - emit Assignment.
    """
    # -----------------------------
    # Helpers
    # -----------------------------
    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _op_key(op):
        # Prefer stable explicit id if present, else fall back to Python object identity
        return getattr(op, "op_id", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return "timeout" in msg or "timed out" in msg

    def _has_runnable_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return False
        if p.pipeline_id in s.pipeline_drop and st.state_counts.get(OperatorState.FAILED, 0) > 0:
            return False
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return bool(ops)

    def _hp_backlog_exists():
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.queues[pri]:
                if _has_runnable_op(p):
                    return True
        return False

    def _pop_next_pipeline(pri):
        """Round-robin pop (logical) from a priority queue, skipping completed/dropped pipelines."""
        q = s.queues[pri]
        if not q:
            return None

        n = len(q)
        start = s.rr_cursor[pri] % max(1, n)

        for k in range(n):
            idx = (start + k) % n
            p = q[idx]
            st = p.runtime_status()

            # Remove completed pipelines
            if st.is_pipeline_successful():
                q.pop(idx)
                n -= 1
                if n <= 0:
                    s.rr_cursor[pri] = 0
                    return None
                if idx < start:
                    start -= 1
                continue

            # Remove pipelines we decided to drop after non-OOM failure (only if still failed)
            if p.pipeline_id in s.pipeline_drop and st.state_counts.get(OperatorState.FAILED, 0) > 0:
                q.pop(idx)
                n -= 1
                if n <= 0:
                    s.rr_cursor[pri] = 0
                    return None
                if idx < start:
                    start -= 1
                continue

            # Keep pipeline; advance cursor
            s.rr_cursor[pri] = (idx + 1) % max(1, len(q))
            return p

        return None

    def _baseline_request(pool, pri):
        """Priority-based default request when we have no per-op hint."""
        # CPU: favor concurrency for high priority; batch gets more CPU when allowed.
        if pri == Priority.QUERY:
            base_cpu = max(1.0, min(2.0, pool.max_cpu_pool * 0.25))
            base_ram = max(1.0, pool.max_ram_pool * 0.20)  # higher to reduce first-try OOM
        elif pri == Priority.INTERACTIVE:
            base_cpu = max(1.0, min(3.0, pool.max_cpu_pool * 0.33))
            base_ram = max(1.0, pool.max_ram_pool * 0.30)
        else:
            base_cpu = max(1.0, min(pool.max_cpu_pool * 0.60, max(2.0, pool.max_cpu_pool * 0.50)))
            base_ram = max(1.0, pool.max_ram_pool * 0.45)
        return base_cpu, base_ram

    def _apply_caps(pool, pri, cpu_req, ram_req, hp_backlog):
        """Soft reservation: if hp backlog exists, batch can't consume the whole pool."""
        if pri != Priority.BATCH_PIPELINE or not hp_backlog:
            return cpu_req, ram_req
        # Keep meaningful headroom for new HP arrivals.
        cpu_cap = max(1.0, pool.max_cpu_pool * 0.55)
        ram_cap = max(1.0, pool.max_ram_pool * 0.55)
        return min(cpu_req, cpu_cap), min(ram_req, ram_cap)

    def _compute_request(pool, pri, op, avail_cpu, avail_ram, hp_backlog):
        """Compute cpu/ram using per-op hints with baseline fallback, then clamp to availability."""
        base_cpu, base_ram = _baseline_request(pool, pri)
        k = _op_key(op)
        hint_cpu = s.op_hint_cpu.get(k, None)
        hint_ram = s.op_hint_ram.get(k, None)

        cpu_req = hint_cpu if hint_cpu is not None else base_cpu
        ram_req = hint_ram if hint_ram is not None else base_ram

        # Mild "anti-micro" floor: avoid allocating so tiny that runtimes explode.
        cpu_req = max(1.0, cpu_req)
        ram_req = max(1.0, ram_req)

        # Apply batch caps when HP backlog exists
        cpu_req, ram_req = _apply_caps(pool, pri, cpu_req, ram_req, hp_backlog)

        # Clamp by pool availability
        cpu_req = min(cpu_req, avail_cpu, pool.max_cpu_pool)
        ram_req = min(ram_req, avail_ram, pool.max_ram_pool)

        # If we can't fit at least 1 unit, signal failure to place
        if cpu_req < 1.0 or ram_req < 1.0:
            return 0.0, 0.0
        return cpu_req, ram_req

    # -----------------------------
    # 1) Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        s.queues[p.priority].append(p)

    # Early exit (keeps simulator fast)
    if not pipelines and not results:
        return [], []

    # -----------------------------
    # 2) Update hints from results
    # -----------------------------
    for r in results:
        if not getattr(r, "ops", None):
            continue

        failed = (hasattr(r, "failed") and r.failed())
        is_oom = failed and _is_oom_error(getattr(r, "error", None))
        is_timeout = failed and _is_timeout_error(getattr(r, "error", None))
        is_other_fail = failed and (not is_oom) and (not is_timeout)

        for op in r.ops:
            k = _op_key(op)

            # If we can, attribute ops to pipeline for future decisions
            # (We already set this on assignment; this is a no-op if missing.)
            pid = s.op_to_pipeline.get(k, None)
            if is_other_fail and pid is not None:
                # Do not keep retrying "real" failures forever; they cause queue bloat and latency.
                s.pipeline_drop.add(pid)

            # Use last request as a fallback baseline for backoff calculations.
            last_cpu, last_ram = s.op_last_req.get(k, (getattr(r, "cpu", None), getattr(r, "ram", None)))
            cur_cpu = getattr(r, "cpu", None) if getattr(r, "cpu", None) is not None else last_cpu
            cur_ram = getattr(r, "ram", None) if getattr(r, "ram", None) is not None else last_ram
            cur_cpu = cur_cpu if cur_cpu is not None else 1.0
            cur_ram = cur_ram if cur_ram is not None else 1.0

            if is_oom:
                # Aggressive RAM ramp for that specific op; keep CPU the same to preserve concurrency.
                prev = s.op_hint_ram.get(k, cur_ram)
                s.op_hint_ram[k] = min(max(prev, cur_ram) * 1.9, 1e12)  # clamp huge but finite
                # Optionally nudge CPU slightly (not required for OOM)
                if k not in s.op_hint_cpu:
                    s.op_hint_cpu[k] = max(1.0, cur_cpu)
            elif is_timeout:
                # Mild CPU increase for that op; slight RAM increase in case spilling caused slowdown.
                prev_c = s.op_hint_cpu.get(k, cur_cpu)
                prev_r = s.op_hint_ram.get(k, cur_ram)
                s.op_hint_cpu[k] = min(max(prev_c, cur_cpu) * 1.25, 1e12)
                s.op_hint_ram[k] = min(max(prev_r, cur_ram) * 1.10, 1e12)
            elif not failed:
                # Success: gentle decay of hints to avoid permanent over-allocation.
                if k in s.op_hint_ram:
                    s.op_hint_ram[k] = max(1.0, s.op_hint_ram[k] * 0.97)
                if k in s.op_hint_cpu:
                    s.op_hint_cpu[k] = max(1.0, s.op_hint_cpu[k] * 0.98)

    # -----------------------------
    # 3) Build assignments
    # -----------------------------
    suspensions = []
    assignments = []

    hp_backlog = _hp_backlog_exists()

    # Prefer scheduling HP first across pools with most headroom.
    pool_order = list(range(s.executor.num_pools))
    pool_order.sort(key=lambda i: (s.executor.pools[i].avail_ram_pool, s.executor.pools[i].avail_cpu_pool), reverse=True)

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep placing work until we can't make progress.
        while True:
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            chosen = None
            chosen_pri = None
            chosen_op = None

            # Strict priority selection: pick first runnable op from highest priority.
            for pri in _priority_order():
                p = _pop_next_pipeline(pri)
                if p is None:
                    continue

                st = p.runtime_status()
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not ops:
                    # Not runnable yet; keep it in queue for later.
                    s.queues[pri].append(p)
                    continue

                chosen = p
                chosen_pri = pri
                chosen_op = ops[0]
                break

            if chosen is None:
                break

            cpu_req, ram_req = _compute_request(pool, chosen_pri, chosen_op, avail_cpu, avail_ram, hp_backlog)
            if cpu_req <= 0 or ram_req <= 0:
                # Can't fit this op in this pool right now; requeue and stop scheduling in this pool.
                s.queues[chosen_pri].append(chosen)
                break

            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen.pipeline_id,
                )
            )

            # Record mapping for precise failure attribution and backoff later.
            ok = _op_key(chosen_op)
            s.op_to_pipeline[ok] = chosen.pipeline_id
            s.op_last_req[ok] = (cpu_req, ram_req)

            # Update local availability to avoid overscheduling within this tick.
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Requeue pipeline for subsequent ops.
            s.queues[chosen_pri].append(chosen)

    return suspensions, assignments
