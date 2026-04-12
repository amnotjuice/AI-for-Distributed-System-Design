# policy_key: scheduler_iter_median_rich_008
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051440
# generation_seconds: 55.19
# generated_at: 2026-04-12T01:49:26.411358
@register_scheduler_init(key="scheduler_iter_median_rich_008")
def scheduler_iter_median_rich_008_init(s):
    """Priority-aware, multi-pool scheduler with per-operator adaptive sizing.

    Iteration goals vs prior attempt:
    - Reduce OOMs without globally inflating RAM (avoid allocating ~all pool RAM all the time).
    - Reduce timeouts by boosting CPU only for ops that exhibit timeouts.
    - Improve latency by:
        * strict priority ordering (QUERY > INTERACTIVE > BATCH),
        * pool "affinity": keep pool 0 biased toward high priority when multiple pools exist,
        * smaller initial footprints for unknown ops to increase concurrency,
        * per-operator RAM/CPU hints updated from ExecutionResult feedback.
    Notes/constraints:
    - ExecutionResult does not reliably expose pipeline_id in the provided interface, so we learn per-operator.
    - We avoid suspensions/preemption because container inventory APIs are not specified.
    """
    # Priority queues of pipelines (round-robin within each priority)
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

    # Per-operator sizing hints (learned online from successes/failures)
    # ram_hint[key] is the next RAM to try (soft target); cpu_hint likewise.
    s.ram_hint = {}  # op_key -> float
    s.cpu_hint = {}  # op_key -> float

    # Lightweight per-operator stats (for optional decay / stability)
    s.op_stats = {}  # op_key -> dict(successes=int, ooms=int, timeouts=int)

    # Guardrails
    s.max_ram_multiplier = 1.0  # never request more than pool available; multiplier kept for future extensions


@register_scheduler(key="scheduler_iter_median_rich_008")
def scheduler_iter_median_rich_008_scheduler(s, results, pipelines):
    """
    Step:
    1) Enqueue new pipelines by priority.
    2) Update per-operator RAM/CPU hints based on execution results:
       - OOM: increase RAM for that operator key aggressively.
       - timeout: increase CPU moderately.
       - success: gently decay RAM toward lower values to reduce chronic overallocation.
    3) For each pool, schedule as many ready ops as possible:
       - strict priority order,
       - pool 0 biased to serve QUERY/INTERACTIVE if multiple pools exist (batch avoids it unless idle),
       - conservative initial sizing + learned hints.
    """
    # -----------------------------
    # Helpers
    # -----------------------------
    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        return "timeout" in str(err).lower()

    def _op_key(op):
        # Try common identifiers; fall back to repr (stable enough within a sim run)
        for attr in ("op_id", "operator_id", "node_id", "name"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if v is not None:
                    return f"{attr}:{v}"
        return repr(op)

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _ensure_stats(key):
        st = s.op_stats.get(key)
        if st is None:
            st = {"successes": 0, "ooms": 0, "timeouts": 0}
            s.op_stats[key] = st
        return st

    def _base_request(pool, pri):
        # Conservative defaults to enable concurrency; rely on hints to scale up.
        # CPU bases are small for QUERY to reduce queueing; interactive slightly larger.
        if pri == Priority.QUERY:
            base_cpu = max(1.0, min(3.0, pool.max_cpu_pool * 0.25))
            base_ram = max(1.0, pool.max_ram_pool * 0.08)
        elif pri == Priority.INTERACTIVE:
            base_cpu = max(1.0, min(5.0, pool.max_cpu_pool * 0.35))
            base_ram = max(1.0, pool.max_ram_pool * 0.12)
        else:
            # Batch: start moderate, but not "take the whole pool".
            base_cpu = max(1.0, min(6.0, pool.max_cpu_pool * 0.45))
            base_ram = max(1.0, pool.max_ram_pool * 0.18)
        return base_cpu, base_ram

    def _pool_allows_priority(pool_id, pri):
        # Bias pool 0 toward high priority if we have >1 pool.
        if s.executor.num_pools <= 1:
            return True
        if pool_id == 0:
            return pri in (Priority.QUERY, Priority.INTERACTIVE)
        return True

    def _pool_is_idleish(pool):
        # If a pool has most resources free, we consider it idle enough to lend to batch.
        return (pool.avail_cpu_pool >= pool.max_cpu_pool * 0.75) and (pool.avail_ram_pool >= pool.max_ram_pool * 0.75)

    def _pick_pipeline_rr(pri):
        q = s.queues[pri]
        if not q:
            return None

        n = len(q)
        start = s.rr_cursor[pri] % n

        for k in range(n):
            idx = (start + k) % n
            p = q[idx]
            st = p.runtime_status()

            # Drop completed pipelines eagerly
            if st.is_pipeline_successful():
                q.pop(idx)
                if idx < start:
                    start -= 1
                if not q:
                    s.rr_cursor[pri] = 0
                else:
                    s.rr_cursor[pri] = (idx) % len(q)
                continue

            # Keep it and advance cursor
            s.rr_cursor[pri] = (idx + 1) % len(q)
            return p

        return None

    def _get_ready_op(pipeline):
        st = pipeline.runtime_status()
        # Only schedule ops whose parents are complete; one op at a time per pipeline for latency fairness
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _compute_request(pool, pri, op, avail_cpu, avail_ram):
        base_cpu, base_ram = _base_request(pool, pri)
        key = _op_key(op)

        # Use learned hints if present; otherwise base.
        cpu_req = s.cpu_hint.get(key, base_cpu)
        ram_req = s.ram_hint.get(key, base_ram)

        # Priority shaping:
        # - For QUERY/INTERACTIVE, slightly prefer CPU to cut latency.
        # - For BATCH, prefer fitting more ops unless the op has shown timeouts.
        if pri == Priority.QUERY:
            cpu_req = max(cpu_req, min(4.0, pool.max_cpu_pool * 0.40))
        elif pri == Priority.INTERACTIVE:
            cpu_req = max(cpu_req, min(6.0, pool.max_cpu_pool * 0.45))

        # Clamp to available and pool maxima
        cpu_req = max(1.0, min(cpu_req, avail_cpu, pool.max_cpu_pool))
        ram_req = max(1.0, min(ram_req, avail_ram, pool.max_ram_pool))

        return cpu_req, ram_req

    # -----------------------------
    # 1) Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        s.queues[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # -----------------------------
    # 2) Update hints from results
    # -----------------------------
    for r in results:
        # r.ops may be list-like; normalize
        rops = getattr(r, "ops", None)
        if not rops:
            continue

        failed = (hasattr(r, "failed") and r.failed())
        is_oom = failed and _is_oom_error(getattr(r, "error", None))
        is_timeout = failed and _is_timeout_error(getattr(r, "error", None))

        # We learn from the RAM/CPU we just tried (r.ram, r.cpu)
        tried_ram = float(getattr(r, "ram", 0) or 0)
        tried_cpu = float(getattr(r, "cpu", 0) or 0)

        for op in rops:
            key = _op_key(op)
            st = _ensure_stats(key)

            if is_oom:
                st["ooms"] += 1
                # Aggressive RAM ramp-up: double what we just tried, with a small floor.
                # This targets the OOM-heavy workload without inflating unrelated ops.
                prev = s.ram_hint.get(key, max(1.0, tried_ram))
                bump = max(prev, max(1.0, tried_ram) * 2.0)
                s.ram_hint[key] = bump
            elif is_timeout:
                st["timeouts"] += 1
                # CPU bump for timeouts (moderate, to avoid starving others)
                prev = s.cpu_hint.get(key, max(1.0, tried_cpu))
                bump = max(prev, max(1.0, tried_cpu) * 1.5)
                s.cpu_hint[key] = bump
            elif not failed:
                st["successes"] += 1
                # Gentle decay to reduce chronic overallocation:
                # if we succeeded with tried_ram, slowly move hint downward.
                if tried_ram > 0:
                    prev = s.ram_hint.get(key, tried_ram)
                    # Decay but don't go below 60% of what just worked (keeps stability under noise)
                    target = max(tried_ram * 0.60, prev * 0.90)
                    s.ram_hint[key] = min(prev, target) if prev > target else target

                # Similarly, avoid hinting huge CPU forever if success happens.
                if tried_cpu > 0:
                    prevc = s.cpu_hint.get(key, tried_cpu)
                    s.cpu_hint[key] = max(1.0, min(prevc, max(tried_cpu * 1.05, prevc * 0.95)))

    # -----------------------------
    # 3) Schedule per pool
    # -----------------------------
    suspensions = []
    assignments = []

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Fill the pool with as many ops as possible this tick.
        # Stop when no runnable op fits.
        while avail_cpu > 0 and avail_ram > 0:
            made_assignment = False

            # Determine if pool 0 can lend to batch (only when "idleish")
            pool0_lend_to_batch = True
            if s.executor.num_pools > 1 and pool_id == 0 and not _pool_is_idleish(pool):
                pool0_lend_to_batch = False

            for pri in _priority_order():
                # Enforce pool affinity bias
                if not _pool_allows_priority(pool_id, pri):
                    # allow batch to use pool 0 only if idleish
                    if not (pri == Priority.BATCH_PIPELINE and pool0_lend_to_batch):
                        continue

                p = _pick_pipeline_rr(pri)
                if p is None:
                    continue

                op = _get_ready_op(p)
                if op is None:
                    # Not runnable now; keep it for later
                    s.queues[pri].append(p)
                    continue

                # Compute request size using hints
                cpu_req, ram_req = _compute_request(pool, pri, op, avail_cpu, avail_ram)

                # If we can't fit even minimal request, try lower priority? (no, lower is worse)
                if cpu_req <= 0 or ram_req <= 0 or cpu_req > avail_cpu or ram_req > avail_ram:
                    s.queues[pri].append(p)
                    continue

                # Create assignment
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # Update local availability to avoid overscheduling within this tick
                avail_cpu -= cpu_req
                avail_ram -= ram_req

                # Put pipeline back for subsequent ops
                s.queues[pri].append(p)

                made_assignment = True
                break  # re-check from highest priority with updated avail

            if not made_assignment:
                break

    return suspensions, assignments
