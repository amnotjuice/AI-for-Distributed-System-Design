# policy_key: scheduler_iter_median_rich_018
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.053498
# generation_seconds: 52.92
# generated_at: 2026-04-12T01:57:50.332476
@register_scheduler_init(key="scheduler_iter_median_rich_018")
def scheduler_iter_median_rich_018_init(s):
    """Iteration 2: Priority-aware, OOM-informed sizing, and pool partitioning.

    Key improvements vs prior version:
    - Per-operator RAM hinting from actual OOM results: next retry uses >= 2x last attempted RAM.
    - Conservative retry escalation: after several OOMs for the same op, jump close to pool max RAM.
    - Pool partitioning (when num_pools > 1):
        * Prefer pool 0 for QUERY/INTERACTIVE.
        * Prefer non-0 pools for BATCH (unless they are full and pool 0 is idle).
    - Keep assignments small-granularity (1 op) to reduce head-of-line blocking.

    Notes/constraints:
    - We avoid suspensions because the public API shown doesn't expose a reliable way to list running
      containers by priority. This policy focuses on admission/placement/sizing to cut queueing + OOM churn.
    """
    import collections

    # Priority queues of Pipeline objects (round-robin within each priority).
    s.q = {
        Priority.QUERY: collections.deque(),
        Priority.INTERACTIVE: collections.deque(),
        Priority.BATCH_PIPELINE: collections.deque(),
    }

    # Per-op hints keyed by a stable-ish identifier derived from the operator object.
    # {
    #   op_key: {
    #     "ram_floor": float,   # minimum RAM to request next time (learned from OOM)
    #     "oom_count": int,     # consecutive-ish OOMs observed for this op
    #   }
    # }
    s.op_hints = {}

    # Soft per-pipeline bookkeeping (mainly for cleanup / deprioritization of hopeless pipelines).
    # { pipeline_id: { "drop": bool } }
    s.pstate = {}

    # Small knobs (kept in state so future iterations can tune easily).
    s.cfg = {
        "oom_multiplier": 2.0,          # next RAM floor after OOM = attempted_ram * multiplier
        "oom_multiplier_cap": 32.0,     # cap for RAM floor growth relative to attempted
        "oom_escalate_after": 3,        # after this many OOMs for an op, jump close to max RAM
        "oom_escalate_to": 0.90,        # fraction of pool.max_ram_pool to request after escalation
        "batch_cap_when_hp_backlog": 0.55,  # cap batch share per pool when high-priority backlog exists
        "batch_min_headroom_for_hp": 0.20,  # try to leave this fraction headroom (softly) for HP
    }


@register_scheduler(key="scheduler_iter_median_rich_018")
def scheduler_iter_median_rich_018_scheduler(s, results, pipelines):
    """
    Scheduling step:
    - Enqueue new pipelines by priority.
    - Learn RAM floors from OOM execution results per operator (op-level hints).
    - For each pool, repeatedly assign at most one ready operator at a time, selecting:
        * pool placement biased by priority (pool partitioning)
        * pipeline in strict priority order, round-robin within priority
        * resource request shaped by priority + op-level RAM floor
    """
    # -----------------------------
    # Helpers (local)
    # -----------------------------
    def _pri_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _op_key(op):
        # Use repr() as a best-effort stable identifier (no guaranteed op_id in the given API).
        # If repr includes memory addresses, stability may degrade; still better than nothing.
        try:
            return repr(op)
        except Exception:
            return str(type(op))

    def _pipeline_drop(p):
        pst = s.pstate.get(p.pipeline_id)
        return bool(pst and pst.get("drop", False))

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _has_hp_backlog():
        # Any ready-to-assign op in QUERY or INTERACTIVE queues?
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in list(s.q[pri]):
                if _pipeline_drop(p):
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                if st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True):
                    return True
        return False

    def _cleanup_queue(pri):
        # Remove completed pipelines (and those marked drop) from the left as we encounter them.
        # We don't exhaustively scan to keep step cost bounded; round-robin will eventually clean.
        q = s.q[pri]
        for _ in range(min(len(q), 32)):
            if not q:
                break
            p = q[0]
            if _pipeline_drop(p):
                q.popleft()
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                q.popleft()
                continue
            # keep it
            break

    def _pick_pipeline_with_ready_op(pri):
        """Round-robin pick: rotate until we find a pipeline with a ready op, or give up."""
        q = s.q[pri]
        n = len(q)
        for _ in range(n):
            if not q:
                return None, None
            p = q[0]
            q.rotate(-1)

            if _pipeline_drop(p):
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            # Do not retry generic failures indefinitely; if FAILED exists and we didn't see OOM hints
            # we mark pipeline to drop (best-effort because we can't map results->pipeline reliably).
            if st.state_counts.get(OperatorState.FAILED, 0) > 0:
                # If any failed op has a RAM floor hint, assume it's OOM-related; otherwise drop.
                failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
                any_has_hint = False
                for fo in failed_ops:
                    if _op_key(fo) in s.op_hints:
                        any_has_hint = True
                        break
                if not any_has_hint:
                    s.pstate[p.pipeline_id]["drop"] = True
                    continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if ops:
                return p, ops
        return None, None

    def _pool_order_for_priority(pri):
        # Pool partitioning:
        # - If only 1 pool, trivial.
        # - Else prefer pool 0 for HP; prefer pools 1.. for batch.
        if s.executor.num_pools <= 1:
            return [0]
        if pri in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + list(range(1, s.executor.num_pools))
        return list(range(1, s.executor.num_pools)) + [0]

    def _base_request(pool, pri):
        # Smaller slices for latency-sensitive work; batch uses larger chunks when available.
        if pri == Priority.QUERY:
            cpu = max(1.0, min(2.0, pool.max_cpu_pool * 0.25))
            ram = max(1.0, pool.max_ram_pool * 0.12)
        elif pri == Priority.INTERACTIVE:
            cpu = max(1.0, min(4.0, pool.max_cpu_pool * 0.40))
            ram = max(1.0, pool.max_ram_pool * 0.22)
        else:
            cpu = max(1.0, min(pool.max_cpu_pool * 0.70, max(2.0, pool.max_cpu_pool * 0.55)))
            ram = max(1.0, pool.max_ram_pool * 0.35)
        return cpu, ram

    def _apply_op_ram_floor(op, pool, pri, ram_req):
        # If we learned an op-specific RAM floor (from OOM), apply it.
        key = _op_key(op)
        hint = s.op_hints.get(key)
        if not hint:
            return ram_req

        # Escalate aggressively after repeated OOMs for this operator.
        if hint.get("oom_count", 0) >= s.cfg["oom_escalate_after"]:
            escalated = pool.max_ram_pool * s.cfg["oom_escalate_to"]
            return max(ram_req, escalated, hint.get("ram_floor", 0.0))

        return max(ram_req, hint.get("ram_floor", 0.0))

    def _cap_batch_if_hp_backlog(pool, pri, avail_cpu, avail_ram, hp_backlog):
        # Soft batch cap: if HP backlog exists, try not to let batch consume whole pools.
        if pri != Priority.BATCH_PIPELINE or not hp_backlog:
            return avail_cpu, avail_ram

        cpu_cap = min(avail_cpu, pool.max_cpu_pool * s.cfg["batch_cap_when_hp_backlog"])
        ram_cap = min(avail_ram, pool.max_ram_pool * s.cfg["batch_cap_when_hp_backlog"])
        return cpu_cap, ram_cap

    # -----------------------------
    # 1) Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        if p.pipeline_id not in s.pstate:
            s.pstate[p.pipeline_id] = {"drop": False}
        s.q[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # -----------------------------
    # 2) Learn from results (OOM -> op RAM floors)
    # -----------------------------
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        if _is_oom_error(getattr(r, "error", None)):
            # Apply hint to each op in this container (often one op, but handle list).
            attempted_ram = float(getattr(r, "ram", 0.0) or 0.0)
            if attempted_ram <= 0:
                continue

            for op in getattr(r, "ops", []) or []:
                key = _op_key(op)
                hint = s.op_hints.get(key)
                if hint is None:
                    hint = {"ram_floor": 0.0, "oom_count": 0}
                    s.op_hints[key] = hint

                hint["oom_count"] = int(hint.get("oom_count", 0)) + 1

                # New RAM floor: >= attempted_ram * multiplier, capped to avoid runaway from bad signals.
                new_floor = attempted_ram * s.cfg["oom_multiplier"]
                # Cap the "jump" based on the last floor (not pool max, because pool differs per assignment).
                # This cap is primarily to prevent extreme oscillations.
                old_floor = float(hint.get("ram_floor", 0.0) or 0.0)
                if old_floor > 0:
                    new_floor = min(new_floor, old_floor * s.cfg["oom_multiplier_cap"])

                hint["ram_floor"] = max(old_floor, new_floor)
        else:
            # For non-OOM failures, we do nothing here; pipeline cleanup handles it best-effort.
            pass

    # Opportunistic cleanup
    for pri in _pri_order():
        _cleanup_queue(pri)

    hp_backlog = _has_hp_backlog()

    suspensions = []
    assignments = []

    # -----------------------------
    # 3) Schedule per pool, priority first
    # -----------------------------
    # We schedule by iterating priorities and trying preferred pools for that priority.
    # This tends to reduce HP tail latency via pool partitioning.
    for pri in _pri_order():
        pool_ids = _pool_order_for_priority(pri)

        # Attempt multiple placements per priority to keep pools busy,
        # but keep the step bounded to avoid O(n) scans.
        placement_budget = 64

        while placement_budget > 0:
            placement_budget -= 1

            # Pick a runnable pipeline/op for this priority
            pipeline, op_list = _pick_pipeline_with_ready_op(pri)
            if pipeline is None:
                break

            op = op_list[0]

            placed = False
            for pool_id in pool_ids:
                pool = s.executor.pools[pool_id]
                avail_cpu = pool.avail_cpu_pool
                avail_ram = pool.avail_ram_pool
                if avail_cpu <= 0 or avail_ram <= 0:
                    continue

                # Soft cap for batch when HP is waiting
                cpu_cap, ram_cap = _cap_batch_if_hp_backlog(pool, pri, avail_cpu, avail_ram, hp_backlog)
                if cpu_cap <= 0 or ram_cap <= 0:
                    continue

                base_cpu, base_ram = _base_request(pool, pri)

                # Apply op-specific RAM floor (learned from OOMs)
                ram_req = _apply_op_ram_floor(op, pool, pri, base_ram)

                # Clamp to caps and availability
                cpu_req = max(1.0, min(base_cpu, cpu_cap, avail_cpu, pool.max_cpu_pool))
                ram_req = max(1.0, min(ram_req, ram_cap, avail_ram, pool.max_ram_pool))

                # If we still can't fit a minimal container, try next pool
                if cpu_req <= 0 or ram_req <= 0:
                    continue

                # Additional soft headroom protection for HP:
                # If pri is batch and HP backlog exists, avoid consuming the last chunk of RAM/CPU.
                if pri == Priority.BATCH_PIPELINE and hp_backlog:
                    # If this assignment would leave almost no headroom, skip unless pool is mostly idle.
                    post_cpu = avail_cpu - cpu_req
                    post_ram = avail_ram - ram_req
                    min_head_cpu = pool.max_cpu_pool * s.cfg["batch_min_headroom_for_hp"]
                    min_head_ram = pool.max_ram_pool * s.cfg["batch_min_headroom_for_hp"]
                    if (post_cpu < min_head_cpu or post_ram < min_head_ram) and (avail_cpu > pool.max_cpu_pool * 0.30 and avail_ram > pool.max_ram_pool * 0.30):
                        continue

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )
                placed = True
                break

            if not placed:
                # Couldn't place now; stop trying this priority this tick to avoid spinning.
                break

    return suspensions, assignments
