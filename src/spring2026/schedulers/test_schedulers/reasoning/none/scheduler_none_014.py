# policy_key: scheduler_none_014
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052469
# generation_seconds: 42.98
# generated_at: 2026-04-09T21:55:50.104989
@register_scheduler_init(key="scheduler_none_014")
def scheduler_none_014_init(s):
    """Priority-aware, OOM-adaptive, aging scheduler with gentle preemption.

    Goals for the weighted-latency objective:
      - Strongly protect QUERY and INTERACTIVE latency (dominant weights).
      - Avoid failures (720s penalty) via OOM-triggered RAM backoff and conservative sizing.
      - Avoid starvation of BATCH via priority aging and per-tick batch budget.
      - Keep it simple: one-op-at-a-time per pool per tick, with limited preemption.

    Core ideas:
      1) Maintain per-pipeline state: enqueue time, OOM RAM floor, failure counts.
      2) Choose next runnable op using a score combining priority weight, aging, and OOM risk.
      3) Size RAM as: max(op_min_ram, learned_oom_floor) with multiplicative bump on OOM.
      4) Size CPU modestly (avoid hogging) but allow more for high priority.
      5) Preempt low priority only when high priority is waiting and pool has no headroom.
    """
    # Waiting pipelines that have arrived but are not fully completed/failed.
    s.waiting_queue = []

    # Monotonic tick counter for aging.
    s.tick = 0

    # pipeline_id -> dict(state)
    # {
    #   "enq_tick": int,
    #   "oom_ram_floor": float,  # minimum RAM we should try next time due to past OOM
    #   "oom_count": int,
    #   "fail_count": int,
    # }
    s.pstate = {}

    # Track running containers so we can preempt if needed.
    # container_id -> dict(meta)
    # { "priority": Priority, "pool_id": int, "cpu": float, "ram": float, "pipeline_id": str/int }
    s.running = {}

    # Budget to ensure batch work progresses even under constant interactive load.
    s.batch_budget_tokens = 0.0  # accumulates over time

    # Conservative sizing knobs.
    s.ram_bump_factor = 1.6       # multiply RAM floor after OOM
    s.ram_bump_add = 0.0          # optional additive bump
    s.max_ram_cap_ratio = 0.95    # don't allocate more than this fraction of pool ram to one op

    # CPU sizing knobs (sublinear scaling; avoid giving everything "all cores").
    s.cpu_query_frac = 0.75
    s.cpu_interactive_frac = 0.60
    s.cpu_batch_frac = 0.40
    s.cpu_min = 1.0

    # Preemption knobs.
    s.preempt_enabled = True
    s.preempt_only_if_waiting_hp = True
    s.preempt_spare_ram_target_ratio = 0.15  # try to create at least this much free RAM in pool
    s.preempt_spare_cpu_target_ratio = 0.10  # and this much CPU
    s.max_preemptions_per_tick = 2

    # Aging knobs.
    s.aging_rate = 0.02  # score boost per tick waited (scaled by base weight)


@register_scheduler(key="scheduler_none_014")
def scheduler_none_014(s, results, pipelines):
    """
    Step scheduler.

    Returns:
      suspensions: List[Suspend]
      assignments: List[Assignment]
    """
    # ---- Helpers (no imports needed) ----
    def _prio_weight(prio):
        if prio == Priority.QUERY:
            return 10.0
        if prio == Priority.INTERACTIVE:
            return 5.0
        return 1.0

    def _cpu_frac(prio):
        if prio == Priority.QUERY:
            return s.cpu_query_frac
        if prio == Priority.INTERACTIVE:
            return s.cpu_interactive_frac
        return s.cpu_batch_frac

    def _ensure_pstate(p):
        pid = p.pipeline_id
        if pid not in s.pstate:
            s.pstate[pid] = {
                "enq_tick": s.tick,
                "oom_ram_floor": 0.0,
                "oom_count": 0,
                "fail_count": 0,
            }
        return s.pstate[pid]

    def _get_op_min_ram(op):
        # Best effort: different simulator versions may store RAM minima differently.
        # Fall back to 1.0 if unknown to reduce immediate OOM risk in toy setups.
        for attr in ("min_ram", "ram_min", "min_memory", "memory_min", "required_ram"):
            if hasattr(op, attr):
                try:
                    v = float(getattr(op, attr))
                    if v > 0:
                        return v
                except Exception:
                    pass
        # Sometimes op may be a dict-like.
        try:
            v = op.get("min_ram", None)
            if v is not None:
                v = float(v)
                if v > 0:
                    return v
        except Exception:
            pass
        return 1.0

    def _pick_runnable_op(pipeline):
        status = pipeline.runtime_status()
        # Skip if already complete.
        if status.is_pipeline_successful():
            return None
        # If any failed ops exist, they are retryable (ASSIGNABLE_STATES includes FAILED).
        # We still require parents complete to keep DAG semantics.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        return op_list[0]

    def _pipeline_is_terminal(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If failed ops exist, we may retry; but if simulator marks pipeline irrecoverable
        # we don't know. We'll keep it unless it has no assignable ops at all.
        return False

    def _score_pipeline(pipeline):
        """Higher is better."""
        ps = _ensure_pstate(pipeline)
        base = _prio_weight(pipeline.priority)

        # Aging: keep batches making progress while still prioritizing high-priority.
        waited = max(0, s.tick - ps["enq_tick"])
        aging = base * (s.aging_rate * waited)

        # OOM-risk penalty: pipelines that already OOMed should be scheduled, but with enough RAM.
        # Penalize slightly to avoid thrashing if pool RAM is tight (we'll also size RAM up).
        oom_pen = 0.15 * ps["oom_count"] * base

        # Failure penalty: deprioritize repeated failures a bit (but not starvation).
        fail_pen = 0.05 * ps["fail_count"] * base

        return base + aging - oom_pen - fail_pen

    def _ram_for_op(pool, pipeline, op):
        ps = _ensure_pstate(pipeline)
        min_ram = _get_op_min_ram(op)
        ram = max(min_ram, ps["oom_ram_floor"], 1.0)

        # Cap to keep one op from monopolizing the pool.
        cap = max(1.0, pool.max_ram_pool * s.max_ram_cap_ratio)
        if ram > cap:
            ram = cap
        # Also cannot exceed what pool can ever provide.
        if ram > pool.max_ram_pool:
            ram = pool.max_ram_pool
        return ram

    def _cpu_for_op(pool, pipeline):
        frac = _cpu_frac(pipeline.priority)
        cpu = max(s.cpu_min, pool.max_cpu_pool * frac)
        if cpu > pool.max_cpu_pool:
            cpu = pool.max_cpu_pool
        return cpu

    def _record_result(res):
        # Update running map regardless of success/failure completion events.
        # If container_id is known, remove from running when we receive a result.
        cid = getattr(res, "container_id", None)
        if cid in s.running:
            del s.running[cid]

        # Update pipeline OOM info on failures.
        if res.failed():
            # Mark pipeline fail counts.
            try:
                pid = getattr(res, "pipeline_id", None)
            except Exception:
                pid = None

            # We don't always get pipeline_id in ExecutionResult per spec; infer via ops.
            # If not available, try to match by running metadata (already removed above),
            # otherwise skip.
            inferred_pid = None
            if pid is not None:
                inferred_pid = pid
            elif cid is not None and cid in s.running:
                inferred_pid = s.running[cid].get("pipeline_id", None)

            if inferred_pid is not None:
                ps = s.pstate.get(inferred_pid, None)
                if ps is None:
                    ps = {"enq_tick": s.tick, "oom_ram_floor": 0.0, "oom_count": 0, "fail_count": 0}
                    s.pstate[inferred_pid] = ps
                ps["fail_count"] += 1

                # If error looks like OOM, bump RAM floor.
                err = getattr(res, "error", "") or ""
                err_l = str(err).lower()
                if "oom" in err_l or "out of memory" in err_l or "out_of_memory" in err_l:
                    ps["oom_count"] += 1
                    # Use last attempted RAM as a new floor with multiplicative bump.
                    tried_ram = float(getattr(res, "ram", 0.0) or 0.0)
                    bumped = max(tried_ram * s.ram_bump_factor + s.ram_bump_add, tried_ram + 1.0)
                    if bumped > ps["oom_ram_floor"]:
                        ps["oom_ram_floor"] = bumped

    def _collect_waiting(pipeline_list):
        for p in pipeline_list:
            _ensure_pstate(p)
            s.waiting_queue.append(p)

    def _clean_waiting_queue():
        # Remove completed pipelines to reduce scanning.
        new_q = []
        for p in s.waiting_queue:
            if _pipeline_is_terminal(p) and p.runtime_status().is_pipeline_successful():
                continue
            new_q.append(p)
        s.waiting_queue = new_q

    def _maybe_preempt_for_high_priority(pool_id, need_cpu, need_ram, highest_waiting_weight):
        """Preempt low-priority containers in this pool to create headroom."""
        if not s.preempt_enabled:
            return []

        # Only preempt if the waiting work is QUERY/INTERACTIVE (weight >= 5).
        if s.preempt_only_if_waiting_hp and highest_waiting_weight < 5.0:
            return []

        pool = s.executor.pools[pool_id]
        # If we already have enough, don't preempt.
        if pool.avail_cpu_pool >= need_cpu and pool.avail_ram_pool >= need_ram:
            return []

        # Target some spare headroom (to reduce immediate contention/queueing).
        target_cpu = max(need_cpu, pool.max_cpu_pool * s.preempt_spare_cpu_target_ratio)
        target_ram = max(need_ram, pool.max_ram_pool * s.preempt_spare_ram_target_ratio)

        # Gather candidates in this pool, lowest priority first, then largest resource usage.
        candidates = []
        for cid, meta in s.running.items():
            if meta.get("pool_id") != pool_id:
                continue
            pr = meta.get("priority", Priority.BATCH_PIPELINE)
            candidates.append(( _prio_weight(pr), float(meta.get("cpu", 0.0)), float(meta.get("ram", 0.0)), cid, pr))

        # Sort: smaller weight first (batch), then bigger ram, then bigger cpu.
        candidates.sort(key=lambda x: (x[0], -x[2], -x[1]))

        susp = []
        freed_cpu = 0.0
        freed_ram = 0.0
        for w, cpu, ram, cid, pr in candidates:
            # Never preempt QUERY to make room for QUERY/INTERACTIVE; protect top tier.
            if w >= 10.0:
                continue
            # Prefer preempting BATCH before INTERACTIVE.
            if highest_waiting_weight >= 10.0 and w >= 5.0:
                # If waiting is QUERY, avoid preempting INTERACTIVE unless necessary.
                continue

            susp.append(Suspend(container_id=cid, pool_id=pool_id))
            freed_cpu += cpu
            freed_ram += ram
            if len(susp) >= s.max_preemptions_per_tick:
                break
            if (pool.avail_cpu_pool + freed_cpu) >= target_cpu and (pool.avail_ram_pool + freed_ram) >= target_ram:
                break

        return susp

    # ---- Update time and ingest events ----
    s.tick += 1
    _collect_waiting(pipelines)

    # Update from execution results (OOM learning, running bookkeeping)
    for r in results:
        _record_result(r)

    # Early exit if nothing to do.
    if not pipelines and not results and not s.waiting_queue:
        return [], []

    _clean_waiting_queue()

    # Refill batch budget tokens slowly; ensures eventual progress.
    # Token meaning: fraction of "assignment opportunities" reserved for batch.
    s.batch_budget_tokens = min(3.0, s.batch_budget_tokens + 0.35)

    suspensions = []
    assignments = []

    # Pre-compute if any high-priority work is waiting.
    highest_waiting_weight = 0.0
    for p in s.waiting_queue:
        # Consider only if runnable now (parents complete).
        if _pick_runnable_op(p) is None:
            continue
        highest_waiting_weight = max(highest_waiting_weight, _prio_weight(p.priority))

    # ---- For each pool, schedule at most one operator per tick (keeps churn low) ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # If pool has no resources, maybe preempt for HP work.
        # We'll decide preemption based on a rough need of "some" hp work.
        if highest_waiting_weight >= 5.0:
            # Rough desired: some CPU/RAM chunk to run one HP op.
            # Use pool fractions; RAM need estimated as 20% of pool (or whatever's free).
            need_cpu = max(s.cpu_min, pool.max_cpu_pool * (0.5 if highest_waiting_weight >= 10.0 else 0.4))
            need_ram = max(1.0, pool.max_ram_pool * 0.20)
            suspensions.extend(_maybe_preempt_for_high_priority(pool_id, need_cpu, need_ram, highest_waiting_weight))

        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Choose best pipeline to run next in this pool.
        best = None
        best_score = None
        best_op = None
        best_ram = None
        best_cpu = None

        # If batch budget is empty, temporarily downweight batch selection.
        for p in s.waiting_queue:
            op = _pick_runnable_op(p)
            if op is None:
                continue

            # Enforce a soft batch gate to prevent it from crowding out interactive.
            if p.priority == Priority.BATCH_PIPELINE and s.batch_budget_tokens <= 0.0 and highest_waiting_weight >= 5.0:
                continue

            ram_need = _ram_for_op(pool, p, op)
            cpu_need = _cpu_for_op(pool, p)

            # Fit check.
            if ram_need > avail_ram or cpu_need > avail_cpu:
                continue

            sc = _score_pipeline(p)

            # Slightly prefer smaller RAM among equal scores to pack better and reduce future OOMs.
            if best is None or sc > best_score or (sc == best_score and ram_need < best_ram):
                best = p
                best_score = sc
                best_op = op
                best_ram = ram_need
                best_cpu = cpu_need

        if best is None:
            continue

        # Create the assignment.
        assignment = Assignment(
            ops=[best_op],
            cpu=best_cpu,
            ram=best_ram,
            priority=best.priority,
            pool_id=pool_id,
            pipeline_id=best.pipeline_id,
        )
        assignments.append(assignment)

        # Consume batch budget if we scheduled batch.
        if best.priority == Priority.BATCH_PIPELINE:
            s.batch_budget_tokens = max(0.0, s.batch_budget_tokens - 1.0)

    # Track running containers for potential preemption next tick.
    # Note: We don't know container_id until results; some simulators may not expose it here.
    # We'll opportunistically record if Assignment has container_id later via results only.
    # Still, if the simulator attaches a synthetic id, handle it.
    for a in assignments:
        cid = getattr(a, "container_id", None)
        if cid is not None:
            s.running[cid] = {
                "priority": a.priority,
                "pool_id": a.pool_id,
                "cpu": float(getattr(a, "cpu", 0.0) or 0.0),
                "ram": float(getattr(a, "ram", 0.0) or 0.0),
                "pipeline_id": getattr(a, "pipeline_id", None),
            }

    return suspensions, assignments
