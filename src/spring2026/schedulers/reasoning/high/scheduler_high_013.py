# policy_key: scheduler_high_013
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.151421
# generation_seconds: 210.50
# generated_at: 2026-04-10T00:53:48.164776
@register_scheduler_init(key="scheduler_high_013")
def scheduler_high_013_init(s):
    """Priority-aware, OOM-resilient scheduler.

    Main ideas:
    - Separate per-priority queues; schedule QUERY first, then INTERACTIVE, then BATCH.
    - Use conservative CPU caps for BATCH when higher priorities are waiting, to keep headroom
      (since we don't rely on preemption).
    - Retry FAILED (e.g., OOM) operators by increasing a per-operator RAM hint multiplicatively,
      and preferentially place large-RAM operators on pools that can satisfy them.
    - Keep fairness within each priority via round-robin cursors and limit per-pipeline inflight ops.
    """
    # Pipelines by id (stable reference to Pipeline objects)
    s.pipeline_map = {}

    # Round-robin queues per priority store pipeline_id (not the Pipeline object) for easy cleanup
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.queue_pos = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Per-operator RAM hint keyed by Python object identity of the operator (id(op)).
    # This is updated on failures to reduce repeated OOMs (which are costly for latency/SLOs).
    s.op_ram_hint = {}       # op_key -> ram
    s.op_fail_count = {}     # op_key -> int

    # Mild per-pipeline concurrency control (avoid one pipeline hogging)
    s.inflight_limits = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 1,
    }

    # Safety caps
    s.max_assignments_per_pool = 8

    # Deterministic tick counter (for debugging / stable behavior)
    s.tick = 0


@register_scheduler(key="scheduler_high_013")
def scheduler_high_013(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """See init() docstring for overview."""
    s.tick += 1

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # -------- Helpers (defined inside to keep the policy self-contained) --------
    def _assignable_states():
        # Avoid relying on ASSIGNABLE_STATES global; simulator guarantees these are the assignable ones.
        return [OperatorState.PENDING, OperatorState.FAILED]

    def _inflight_count(status) -> int:
        return status.state_counts[OperatorState.RUNNING] + status.state_counts[OperatorState.ASSIGNED]

    def _cleanup_completed_pipelines():
        # Remove completed pipelines from pipeline_map and all queues.
        done_ids = []
        for pid, p in list(s.pipeline_map.items()):
            try:
                if p.runtime_status().is_pipeline_successful():
                    done_ids.append(pid)
            except Exception:
                # If status isn't accessible for some reason, keep it.
                pass

        if not done_ids:
            return

        done_set = set(done_ids)
        for prio in list(s.queues.keys()):
            q = s.queues[prio]
            if not q:
                continue
            s.queues[prio] = [pid for pid in q if pid not in done_set]
            if s.queues[prio]:
                s.queue_pos[prio] = min(s.queue_pos[prio], len(s.queues[prio]) - 1)
            else:
                s.queue_pos[prio] = 0

        for pid in done_ids:
            s.pipeline_map.pop(pid, None)

    def _has_ready_pipeline(prio: Priority) -> bool:
        q = s.queues.get(prio, [])
        if not q:
            return False
        # Scan up to full queue once
        for pid in q:
            p = s.pipeline_map.get(pid)
            if p is None:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            if _inflight_count(st) >= s.inflight_limits.get(prio, 1):
                continue
            ops = st.get_ops(_assignable_states(), require_parents_complete=True)
            if ops:
                return True
        return False

    def _pool_ids_sorted_by_capacity():
        # Prefer to keep "largest" pool(s) available for high priority / large RAM hints.
        n = s.executor.num_pools
        pools = s.executor.pools
        # Sort by (max_ram, max_cpu)
        return sorted(range(n), key=lambda i: (pools[i].max_ram_pool, pools[i].max_cpu_pool))

    def _calc_request(pool, prio: Priority, op, avail_cpu, avail_ram, hp_waiting: bool):
        # If we already learned this op needs more RAM than this pool can ever provide, skip here.
        op_key = id(op)
        hint = float(s.op_ram_hint.get(op_key, 0.0))
        if hint > float(pool.max_ram_pool) + 1e-9:
            return None

        # Base RAM fractions: start moderately conservative for QUERY/INTERACTIVE to avoid OOM retries.
        # For BATCH, be smaller when high-priority work is waiting to reduce head-of-line blocking.
        if prio == Priority.QUERY:
            base_frac = 0.22
        elif prio == Priority.INTERACTIVE:
            base_frac = 0.16
        else:
            base_frac = 0.12 if not hp_waiting else 0.08

        base_ram = float(pool.max_ram_pool) * base_frac
        # Never request truly tiny RAM if the simulator uses absolute units.
        base_ram = max(base_ram, 1.0)

        # Requested RAM: either our learned hint or the baseline (whichever is larger).
        ram_req = max(hint, base_ram)
        ram_req = min(ram_req, float(pool.max_ram_pool))

        # If we don't fit, allow an opportunistic run only when we have no hint yet.
        # This helps avoid idling when the op might actually be small, but still avoids retry loops
        # when we already know it needs more.
        if float(avail_ram) + 1e-9 < ram_req:
            if hint <= 0.0 and float(avail_ram) >= max(1.0, base_ram * 0.6):
                ram_req = float(avail_ram)
            else:
                return None

        # CPU caps by priority. When high-priority is waiting, keep BATCH CPU small to preserve
        # headroom for new QUERY/INTERACTIVE arrivals (no preemption assumed).
        if prio == Priority.QUERY:
            cpu_frac = 0.75
        elif prio == Priority.INTERACTIVE:
            cpu_frac = 0.55
        else:
            cpu_frac = 0.85 if not hp_waiting else 0.25

        cpu_cap = max(1.0, float(pool.max_cpu_pool) * cpu_frac)
        cpu_req = min(float(avail_cpu), cpu_cap)

        # Don't schedule if we can't allocate at least 1 CPU.
        if cpu_req + 1e-9 < 1.0:
            return None

        # Keep batch from grabbing large chunks even if pool is huge and hp_waiting is True.
        if prio == Priority.BATCH_PIPELINE and hp_waiting:
            cpu_req = min(cpu_req, max(1.0, float(pool.max_cpu_pool) * 0.25), 2.0)

        return cpu_req, ram_req

    def _pick_candidate_from_queue(prio: Priority, pool_id: int, avail_cpu, avail_ram, hp_waiting: bool):
        q = s.queues.get(prio, [])
        if not q:
            return None

        pool = s.executor.pools[pool_id]
        n = len(q)
        start = 0 if n == 0 else (s.queue_pos[prio] % n)

        for step in range(n):
            idx = (start + step) % n
            pid = q[idx]
            p = s.pipeline_map.get(pid)
            if p is None:
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            if _inflight_count(st) >= s.inflight_limits.get(prio, 1):
                continue

            ops = st.get_ops(_assignable_states(), require_parents_complete=True)
            if not ops:
                continue
            op = ops[0]

            req = _calc_request(pool, prio, op, avail_cpu, avail_ram, hp_waiting)
            if req is None:
                continue
            cpu_req, ram_req = req

            # Advance RR cursor for fairness within the same priority class
            s.queue_pos[prio] = (idx + 1) % n
            return p, [op], cpu_req, ram_req

        # No change to cursor if nothing found
        return None

    # -------- Ingest new arrivals --------
    for p in pipelines:
        if p.pipeline_id in s.pipeline_map:
            continue
        s.pipeline_map[p.pipeline_id] = p
        if p.priority not in s.queues:
            # Defensive fallback
            s.queues[p.priority] = []
            s.queue_pos[p.priority] = 0
        s.queues[p.priority].append(p.pipeline_id)

    # -------- Learn from results (OOM/backoff via RAM hints) --------
    # We treat any failure as likely resource-related (OOM), and increase RAM hint.
    # This strongly improves completion rate (avoids 720s penalties).
    max_ram_any = 0.0
    for i in range(s.executor.num_pools):
        max_ram_any = max(max_ram_any, float(s.executor.pools[i].max_ram_pool))

    for r in results:
        if not r.failed():
            continue
        alloc_ram = getattr(r, "ram", None)
        if alloc_ram is None:
            alloc_ram = 0.0
        else:
            alloc_ram = float(alloc_ram)

        # Multiplicative backoff with a small additive margin.
        # Cap at the largest pool's RAM, so placement can move to a bigger pool if needed.
        for op in (getattr(r, "ops", None) or []):
            op_key = id(op)
            prev = float(s.op_ram_hint.get(op_key, 0.0))
            s.op_fail_count[op_key] = int(s.op_fail_count.get(op_key, 0)) + 1

            # If alloc_ram is unknown/zero, still bump meaningfully using previous hint or a small baseline.
            baseline = prev if prev > 0.0 else (0.10 * max_ram_any if max_ram_any > 0 else 1.0)
            effective = alloc_ram if alloc_ram > 0.0 else baseline

            new_hint = max(prev, min(max_ram_any, effective * 1.8 + 1.0))
            s.op_ram_hint[op_key] = new_hint

    # -------- Cleanup completed pipelines from queues/map --------
    _cleanup_completed_pipelines()

    # If nothing changed, do nothing
    if not pipelines and not results:
        return suspensions, assignments

    # -------- Scheduling loop --------
    # Determine whether any high-priority (QUERY/INTERACTIVE) work is actually ready.
    query_ready = _has_ready_pipeline(Priority.QUERY)
    interactive_ready = _has_ready_pipeline(Priority.INTERACTIVE)
    hp_waiting = query_ready or interactive_ready

    pool_ids_small_to_large = _pool_ids_sorted_by_capacity()
    if pool_ids_small_to_large:
        largest_pool_id = pool_ids_small_to_large[-1]
    else:
        largest_pool_id = 0

    # We'll schedule per-pool, attempting multiple assignments while capacity remains.
    # Heuristic: avoid running BATCH on the largest pool when hp_waiting, reserving it for
    # big-memory/high-priority operators.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu + 1e-9 < 1.0 or avail_ram <= 0.0:
            continue

        made = 0
        while made < s.max_assignments_per_pool:
            if avail_cpu + 1e-9 < 1.0 or avail_ram <= 0.0:
                break

            # Pool-specific priority order:
            # - When hp_waiting, bias the largest pool heavily toward QUERY/INTERACTIVE.
            # - Otherwise, let BATCH use more capacity to maximize completion rate (avoid 720s).
            if hp_waiting and pool_id == largest_pool_id:
                prio_order = [Priority.QUERY, Priority.INTERACTIVE]
            else:
                prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

            picked = None
            for prio in prio_order:
                # If no high-priority is waiting, QUERY/INTERACTIVE queues may be empty anyway.
                picked = _pick_candidate_from_queue(prio, pool_id, avail_cpu, avail_ram, hp_waiting)
                if picked is not None:
                    p, op_list, cpu_req, ram_req = picked
                    assignment = Assignment(
                        ops=op_list,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                    assignments.append(assignment)
                    avail_cpu -= float(cpu_req)
                    avail_ram -= float(ram_req)
                    made += 1
                    break

            if picked is None:
                break

    return suspensions, assignments
