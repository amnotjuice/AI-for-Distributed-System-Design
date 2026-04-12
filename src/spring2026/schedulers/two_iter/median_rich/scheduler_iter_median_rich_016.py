# policy_key: scheduler_iter_median_rich_016
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.059182
# generation_seconds: 53.08
# generated_at: 2026-04-12T01:56:19.614124
@register_scheduler_init(key="scheduler_iter_median_rich_016")
def scheduler_iter_median_rich_016_init(s):
    """Priority-aware, multi-pool scheduler with per-(pipeline,op) RAM learning to cut OOMs and latency.

    Directional improvements vs prior attempt:
    - Keep strict priority ordering (QUERY > INTERACTIVE > BATCH), but improve within-priority selection:
        prefer "shorter remaining work" pipelines to reduce median latency (SRPT-ish proxy).
    - Learn RAM requirements from OOMs *per pipeline and per operator signature* (when IDs available),
        instead of globally inflating an entire priority class (which wastes RAM and increases queueing).
    - Gentle RAM decay on successes to reduce chronic over-allocation and improve admission.
    - Stronger batch caps when any high-priority backlog exists, to protect tail latency.
    - Pool preference: try to place high-priority work in the pool with most headroom.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Logical time for aging / bookkeeping
    s._tick = 0

    # Per-pipeline state
    # {pipeline_id: {"ram_mult": float, "cpu_mult": float, "enqueue_tick": int, "non_oom_fail": bool}}
    s.pstate = {}

    # Per-operator state (best-effort; key is a stable-ish string derived from op identity)
    # {op_key: {"ram_mult": float, "ooms": int, "succ": int}}
    s.opstate = {}

    # Round-robin cursor within each priority queue (to avoid pathological rescans)
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }


@register_scheduler(key="scheduler_iter_median_rich_016")
def scheduler_iter_median_rich_016_scheduler(s, results, pipelines):
    """
    Step:
      1) Enqueue new pipelines into priority queues.
      2) Update RAM learning from execution results (OOM => multiplicative increase; success => mild decay).
      3) For each pool, greedily assign ready operators:
           - strict priority order
           - within priority: SRPT-ish (fewest remaining ops) then age (older first)
           - resource sizing: RAM learned (pipeline/op), CPU modest for HP to reduce HoL blocking
           - batch capped when HP backlog exists
    """
    s._tick += 1

    # -----------------------------
    # Helpers
    # -----------------------------
    def _ensure_pstate(p):
        pid = p.pipeline_id
        if pid not in s.pstate:
            s.pstate[pid] = {
                "ram_mult": 1.0,
                "cpu_mult": 1.0,
                "enqueue_tick": s._tick,
                "non_oom_fail": False,
            }
        return s.pstate[pid]

    def _get_op_key(op):
        # Try common stable identifiers; fall back to type/name/str.
        for attr in ("op_id", "operator_id", "node_id", "task_id", "name"):
            v = getattr(op, attr, None)
            if v is not None:
                return f"{attr}:{v}"
        # If it's a dict-like object, try keys
        try:
            if isinstance(op, dict):
                for k in ("op_id", "operator_id", "name"):
                    if k in op:
                        return f"{k}:{op[k]}"
        except Exception:
            pass
        return f"type:{type(op).__name__}|str:{str(op)}"

    def _ensure_opstate(op_key):
        if op_key not in s.opstate:
            s.opstate[op_key] = {"ram_mult": 1.0, "ooms": 0, "succ": 0}
        return s.opstate[op_key]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _has_hp_backlog():
        # backlog = any runnable op in QUERY or INTERACTIVE
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.queues[pri]:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
                    continue
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return True
        return False

    def _remaining_ops_proxy(status):
        # Proxy for "work left": count of ops not completed.
        sc = getattr(status, "state_counts", {}) or {}
        # Include FAILED because FAILED is assignable (retry) in this simulator.
        return (
            sc.get(OperatorState.PENDING, 0)
            + sc.get(OperatorState.ASSIGNED, 0)
            + sc.get(OperatorState.RUNNING, 0)
            + sc.get(OperatorState.SUSPENDING, 0)
            + sc.get(OperatorState.FAILED, 0)
        )

    def _score_pipeline(p):
        # Lower score => scheduled earlier (within priority).
        st = p.runtime_status()
        rem = _remaining_ops_proxy(st)
        pst = s.pstate.get(p.pipeline_id, None)
        age = 0
        if pst is not None:
            age = s._tick - pst["enqueue_tick"]
        # SRPT-ish first (rem), then older first (-age)
        return (rem, -age)

    def _select_runnable_pipeline(pri, scan_limit=24):
        """Pick a runnable pipeline in this priority with minimal score; remove it from queue."""
        q = s.queues[pri]
        if not q:
            return None, None

        n = len(q)
        if n == 0:
            return None, None

        start = s.rr_cursor[pri] % n
        best_idx = None
        best_score = None

        # bounded scan for performance and fairness
        limit = min(scan_limit, n)
        for k in range(limit):
            idx = (start + k) % n
            p = q[idx]
            st = p.runtime_status()

            # Drop completed pipelines
            if st.is_pipeline_successful():
                continue

            # Drop permanently failed pipelines (non-OOM fail)
            if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
                continue

            # Must have runnable ops
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            sc = _score_pipeline(p)
            if best_score is None or sc < best_score:
                best_score = sc
                best_idx = idx

        if best_idx is None:
            # Still advance rr cursor to avoid getting stuck
            s.rr_cursor[pri] = (start + limit) % max(1, n)
            return None, None

        # Pop selected pipeline and advance cursor past it
        p = q.pop(best_idx)
        s.rr_cursor[pri] = best_idx % max(1, len(q)) if q else 0
        return p, p.runtime_status()

    def _base_request(pool, pri):
        # Small CPU slices for HP to reduce head-of-line blocking; batch gets larger slices.
        if pri == Priority.QUERY:
            base_cpu = max(1.0, min(2.0, pool.max_cpu_pool * 0.25))
            base_ram = max(1.0, pool.max_ram_pool * 0.08)
        elif pri == Priority.INTERACTIVE:
            base_cpu = max(1.0, min(3.0, pool.max_cpu_pool * 0.33))
            base_ram = max(1.0, pool.max_ram_pool * 0.14)
        else:
            base_cpu = max(1.0, min(pool.max_cpu_pool * 0.60, max(2.0, pool.max_cpu_pool * 0.50)))
            base_ram = max(1.0, pool.max_ram_pool * 0.22)
        return base_cpu, base_ram

    def _apply_caps(pool, pri, avail_cpu, avail_ram, hp_backlog):
        # Batch is throttled hard when HP backlog exists; HP runs unthrottled.
        if pri != Priority.BATCH_PIPELINE:
            return avail_cpu, avail_ram
        if not hp_backlog:
            return avail_cpu, avail_ram
        # Reserve ~50% headroom for HP bursts
        return min(avail_cpu, pool.max_cpu_pool * 0.50), min(avail_ram, pool.max_ram_pool * 0.50)

    def _compute_request(pool, pri, pipeline_id, op_key, avail_cpu, avail_ram):
        pst = s.pstate.get(pipeline_id, {"ram_mult": 1.0, "cpu_mult": 1.0})
        ost = s.opstate.get(op_key, {"ram_mult": 1.0})

        base_cpu, base_ram = _base_request(pool, pri)

        # RAM: take the max multiplier from pipeline/op learning (OOM should be highly localized).
        ram_mult = max(1.0, float(pst.get("ram_mult", 1.0)), float(ost.get("ram_mult", 1.0)))
        ram_req = base_ram * ram_mult

        # CPU: keep bounded; too much CPU doesn't help if RAM is the limiter and increases contention.
        cpu_mult = max(1.0, float(pst.get("cpu_mult", 1.0)))
        cpu_req = base_cpu * cpu_mult

        # Clamp to availability/pool maxima
        cpu_req = max(1.0, min(cpu_req, avail_cpu, pool.max_cpu_pool))
        ram_req = max(1.0, min(ram_req, avail_ram, pool.max_ram_pool))
        return cpu_req, ram_req

    def _pool_order_for_priority(pri):
        # High priority: try pools with most headroom first.
        # Batch: try the most utilized-first to pack and leave empty pool headroom for HP spikes.
        pool_ids = list(range(s.executor.num_pools))
        scored = []
        for pid in pool_ids:
            pool = s.executor.pools[pid]
            scored.append((pool.avail_ram_pool + pool.avail_cpu_pool, pid))
        if pri in (Priority.QUERY, Priority.INTERACTIVE):
            scored.sort(reverse=True)  # most headroom first
        else:
            scored.sort()  # least headroom first (packing)
        return [pid for _, pid in scored]

    # -----------------------------
    # 1) Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        _ensure_pstate(p)
        s.queues[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # -----------------------------
    # 2) Update learning from results
    # -----------------------------
    # Best-effort mapping of results to pipeline/op identity.
    for r in results:
        # Mark non-OOM failures as non-retriable if we can map to pipeline.
        pid = getattr(r, "pipeline_id", None)

        # Derive an op key from the first op in result, if present.
        op_key = None
        ops = getattr(r, "ops", None)
        if ops and len(ops) > 0:
            try:
                op_key = _get_op_key(ops[0])
            except Exception:
                op_key = None

        is_failed = False
        try:
            is_failed = r.failed()
        except Exception:
            is_failed = bool(getattr(r, "error", None))

        if not is_failed:
            # Success: mild decay of RAM multipliers (prevents permanent bloat and improves admission).
            if pid is not None and pid in s.pstate:
                s.pstate[pid]["ram_mult"] = max(1.0, s.pstate[pid]["ram_mult"] * 0.97)
            if op_key is not None:
                ost = _ensure_opstate(op_key)
                ost["succ"] += 1
                ost["ram_mult"] = max(1.0, ost["ram_mult"] * 0.985)
            continue

        # Failure path
        if _is_oom_error(getattr(r, "error", None)):
            # OOM: aggressively increase RAM for the responsible pipeline/op only.
            if pid is not None:
                # If pipeline is unseen, create placeholder state.
                if pid not in s.pstate:
                    s.pstate[pid] = {"ram_mult": 1.0, "cpu_mult": 1.0, "enqueue_tick": s._tick, "non_oom_fail": False}
                s.pstate[pid]["ram_mult"] = min(32.0, max(1.3, s.pstate[pid]["ram_mult"] * 1.8))
            if op_key is not None:
                ost = _ensure_opstate(op_key)
                ost["ooms"] += 1
                ost["ram_mult"] = min(64.0, max(1.4, ost["ram_mult"] * 2.0))
        else:
            # Non-OOM failure: stop retrying if possible (otherwise can spin).
            if pid is not None:
                if pid not in s.pstate:
                    s.pstate[pid] = {"ram_mult": 1.0, "cpu_mult": 1.0, "enqueue_tick": s._tick, "non_oom_fail": False}
                s.pstate[pid]["non_oom_fail"] = True

    # Also, if runtime_status shows FAILED ops but we never saw a mapped OOM result, treat as non-retriable.
    # (Prevents silent infinite retries when result->pipeline mapping is absent.)
    for pri in _priority_order():
        for p in s.queues[pri]:
            st = p.runtime_status()
            if st.state_counts.get(OperatorState.FAILED, 0) > 0:
                pst = _ensure_pstate(p)
                # If we haven't boosted RAM much, it's likely not OOM-driven, so mark non-retriable.
                if pst["ram_mult"] <= 1.2:
                    pst["non_oom_fail"] = True

    # -----------------------------
    # 3) Schedule
    # -----------------------------
    suspensions = []
    assignments = []

    hp_backlog = _has_hp_backlog()

    # We schedule per priority, but allow pool choice to vary by priority to reduce latency.
    for pri in _priority_order():
        for pool_id in _pool_order_for_priority(pri):
            pool = s.executor.pools[pool_id]
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool

            # Apply soft caps (especially for batch under HP backlog)
            cap_cpu, cap_ram = _apply_caps(pool, pri, avail_cpu, avail_ram, hp_backlog)

            # If we can't schedule anything meaningful in this pool, skip.
            if cap_cpu <= 0 or cap_ram <= 0:
                continue

            # Greedy fill per pool for this priority, but keep granularity small (one op at a time).
            # This reduces head-of-line blocking and improves median latency for HP work.
            made_progress = True
            loop_guard = 0
            while made_progress and loop_guard < 64:
                loop_guard += 1
                made_progress = False

                # Refresh caps after local decrement
                if cap_cpu <= 0 or cap_ram <= 0:
                    break

                p, st = _select_runnable_pipeline(pri)
                if p is None:
                    break

                # Find exactly one runnable op
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    # Put pipeline back and stop trying for it
                    s.queues[pri].append(p)
                    continue

                op_key = _get_op_key(op_list[0])

                cpu_req, ram_req = _compute_request(
                    pool=pool,
                    pri=pri,
                    pipeline_id=p.pipeline_id,
                    op_key=op_key,
                    avail_cpu=cap_cpu,
                    avail_ram=cap_ram,
                )

                if cpu_req <= 0 or ram_req <= 0:
                    # Can't fit; put it back and stop filling this pool for this priority
                    s.queues[pri].append(p)
                    break

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # Local decrement to avoid over-issuing within this tick
                cap_cpu -= cpu_req
                cap_ram -= ram_req
                made_progress = True

                # Put pipeline back for its next ops (or later scheduling)
                s.queues[pri].append(p)

    return suspensions, assignments
