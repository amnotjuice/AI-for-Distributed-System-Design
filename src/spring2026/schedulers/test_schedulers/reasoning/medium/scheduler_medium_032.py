# policy_key: scheduler_medium_032
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.079447
# generation_seconds: 94.92
# generated_at: 2026-04-09T23:12:48.992680
@register_scheduler_init(key="scheduler_medium_032")
def scheduler_medium_032_init(s):
    """
    Priority-aware, completion-biased scheduler.

    Goals:
      - Keep query/interactive tail latency low via priority scheduling and optional batch preemption.
      - Avoid failed/incomplete pipelines (720s penalty) via conservative RAM sizing + OOM-driven retries.
      - Avoid starvation by letting batch make progress even under sustained high-priority load.
    """
    import collections

    # Per-priority round-robin queues of pipeline IDs
    s.q_query = collections.deque()
    s.q_interactive = collections.deque()
    s.q_batch = collections.deque()

    # Membership sets to prevent duplicates
    s.in_q_query = set()
    s.in_q_interactive = set()
    s.in_q_batch = set()

    # Pipeline registry
    s.pipeline_by_id = {}

    # Per-operator hints: (pipeline_id, op_key) -> hint value
    s.op_ram_hint = {}     # absolute RAM amount
    s.op_cpu_hint = {}     # absolute CPU amount
    s.op_retries = {}      # (pipeline_id, op_key) -> retries

    # To avoid repeatedly suspending the same container
    s.suspended_recent = set()

    # Simple fairness knobs
    s.hp_streak = 0  # consecutive high-priority assignments since last batch assignment

    # A conservative retry budget; beyond this, keep trying but do not keep inflating indefinitely
    s.max_retries = 6

    # If multiple pools exist, pool 0 is preferred for latency-sensitive work
    s.latency_pool_id = 0


@register_scheduler(key="scheduler_medium_032")
def scheduler_medium_032(s, results, pipelines):
    """
    Scheduling steps:
      1) Ingest new pipelines -> enqueue by priority.
      2) Process results -> update per-op RAM hints (especially on OOM) and re-enqueue pipeline if needed.
      3) For each pool, repeatedly pick a runnable op using:
           - Priority order: QUERY > INTERACTIVE > BATCH
           - Fairness: force occasional batch progress when HP is sustained
           - Placement: prefer pool 0 for HP; prefer non-0 pools for batch (when available)
         Size resources conservatively (RAM-first) to reduce OOMs, and allocate more CPU to HP.
      4) Optionally preempt batch containers when HP cannot be scheduled due to lack of headroom.
    """
    # -----------------------------
    # Helpers (kept inside function)
    # -----------------------------
    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        s.pipeline_by_id[pid] = p
        pr = p.priority
        if pr == Priority.QUERY:
            if pid not in s.in_q_query:
                s.q_query.append(pid)
                s.in_q_query.add(pid)
        elif pr == Priority.INTERACTIVE:
            if pid not in s.in_q_interactive:
                s.q_interactive.append(pid)
                s.in_q_interactive.add(pid)
        else:
            if pid not in s.in_q_batch:
                s.q_batch.append(pid)
                s.in_q_batch.add(pid)

    def _dequeue_membership_remove(pid):
        # Lazy removal is done on pop; this is used when we know a pipeline is terminal.
        s.in_q_query.discard(pid)
        s.in_q_interactive.discard(pid)
        s.in_q_batch.discard(pid)

    def _op_key(op):
        # Try common identifiers; otherwise fall back to object identity.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return v
                except Exception:
                    pass
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            e = str(err).lower()
        except Exception:
            return False
        return ("oom" in e) or ("out of memory" in e) or ("cuda out of memory" in e) or ("memoryerror" in e)

    def _get_runnable_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return None
        # Prefer ops whose parents are complete (typical DAG execution)
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if ops:
            return ops[0]
        return None

    def _has_any_runnable(pids):
        # Best-effort check without changing queue order.
        for pid in list(pids)[:32]:  # cap scan for performance
            p = s.pipeline_by_id.get(pid)
            if p is None:
                continue
            if _get_runnable_op(p) is not None:
                return True
        return False

    def _pool_containers(pool):
        # Try multiple common attributes; return iterable of container-like objects or empty list.
        for attr in ("containers", "running_containers", "active_containers"):
            if hasattr(pool, attr):
                try:
                    c = getattr(pool, attr)
                    if c is None:
                        continue
                    if isinstance(c, dict):
                        return list(c.values())
                    return list(c)
                except Exception:
                    continue
        return []

    def _container_fields(c):
        # Returns (container_id, priority, cpu, ram) with None for missing fields.
        cid = getattr(c, "container_id", None)
        cpr = getattr(c, "priority", None)
        ccpu = getattr(c, "cpu", None)
        cram = getattr(c, "ram", None)
        return cid, cpr, ccpu, cram

    def _maybe_preempt_for_hp(pool_id, needed_cpu, needed_ram):
        # Preempt batch containers first to make room for QUERY/INTERACTIVE.
        pool = s.executor.pools[pool_id]
        susp = []
        freed_cpu = 0.0
        freed_ram = 0.0

        for c in _pool_containers(pool):
            cid, cpr, ccpu, cram = _container_fields(c)
            if cid is None or cid in s.suspended_recent:
                continue
            # Only preempt batch by default to reduce churn and avoid hurting HP latency.
            if cpr != Priority.BATCH_PIPELINE:
                continue
            # If we can't learn resources, still allow preemption as a last resort.
            if ccpu is None:
                ccpu = 0.0
            if cram is None:
                cram = 0.0
            susp.append(Suspend(container_id=cid, pool_id=pool_id))
            s.suspended_recent.add(cid)
            freed_cpu += float(ccpu) if ccpu is not None else 0.0
            freed_ram += float(cram) if cram is not None else 0.0
            if freed_cpu >= needed_cpu and freed_ram >= needed_ram:
                break

        return susp

    def _priority_base_fracs(priority):
        # Conservative RAM to avoid OOM; more CPU to reduce latency for HP.
        if priority == Priority.QUERY:
            return 0.55, 0.80  # (ram_frac, cpu_frac)
        if priority == Priority.INTERACTIVE:
            return 0.50, 0.70
        return 0.40, 0.55

    def _choose_resources(pool, priority, pid, op, avail_cpu, avail_ram):
        # Determine CPU/RAM requests using hints with conservative bases; clamp to pool/avail.
        opk = _op_key(op)
        hk = (pid, opk)

        base_ram_frac, base_cpu_frac = _priority_base_fracs(priority)
        base_ram = max(1.0, pool.max_ram_pool * base_ram_frac)
        base_cpu = max(1.0, pool.max_cpu_pool * base_cpu_frac)

        hint_ram = s.op_ram_hint.get(hk, 0.0)
        hint_cpu = s.op_cpu_hint.get(hk, 0.0)

        # RAM-first: prefer higher of base vs hint; keep within pool max.
        req_ram = min(pool.max_ram_pool, max(base_ram, float(hint_ram or 0.0)))
        # CPU: prioritize HP, but don't claim the entire pool if not available.
        req_cpu = min(pool.max_cpu_pool, max(base_cpu, float(hint_cpu or 0.0)))

        # Clamp to availability; if we cannot meet hint/base minimally, return (0,0) to skip.
        # For CPU: allow down-clamp to whatever is available, as CPU under-allocation just slows.
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            return 0.0, 0.0

        # For RAM: if we cannot meet at least a safe minimum (max of 1 and 50% of base), better to wait/preempt.
        min_safe_ram = max(1.0, base_ram * 0.50, float(hint_ram or 0.0) * 0.85 if hint_ram else 0.0)
        if avail_ram < min_safe_ram:
            return 0.0, 0.0

        req_ram = min(req_ram, avail_ram)

        # CPU can be scaled down to fit; ensure at least 1 vCPU if any is available.
        req_cpu = min(req_cpu, avail_cpu)
        if req_cpu < 1.0 and avail_cpu >= 1.0:
            req_cpu = 1.0

        if req_cpu <= 0.0 or req_ram <= 0.0:
            return 0.0, 0.0
        return req_cpu, req_ram

    def _pop_next_runnable(q, in_set):
        # Round-robin within a queue; return pipeline object or None.
        # Lazily drops completed pipelines.
        for _ in range(len(q)):
            pid = q.popleft()
            p = s.pipeline_by_id.get(pid)
            if p is None:
                in_set.discard(pid)
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                in_set.discard(pid)
                continue
            # Keep it in rotation
            q.append(pid)
            return p
        return None

    def _pick_priority_for_pool(pool_id, pending_hp):
        # Enforce occasional batch scheduling to avoid starvation/incomplete penalties.
        # If HP exists, allow batch only if we've done enough HP in a row.
        if pending_hp:
            if s.hp_streak >= 4 and len(s.q_batch) > 0:
                return Priority.BATCH_PIPELINE
            # Otherwise choose highest available HP
            if len(s.q_query) > 0:
                return Priority.QUERY
            if len(s.q_interactive) > 0:
                return Priority.INTERACTIVE
            return Priority.BATCH_PIPELINE
        else:
            # No runnable HP; let batch fill the system.
            if len(s.q_batch) > 0:
                return Priority.BATCH_PIPELINE
            if len(s.q_query) > 0:
                return Priority.QUERY
            return Priority.INTERACTIVE

    def _queue_for_priority(priority):
        if priority == Priority.QUERY:
            return s.q_query, s.in_q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive, s.in_q_interactive
        return s.q_batch, s.in_q_batch

    # -----------------------------
    # 1) Ingest new pipelines
    # -----------------------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # -----------------------------
    # 2) Process results -> update hints and re-enqueue
    # -----------------------------
    if results:
        for r in results:
            # Any result indicates pipeline still active or just finished; keep it queued if not terminal.
            pid = getattr(r, "pipeline_id", None)
            # If pipeline_id isn't on result, infer via existing registry by scanning (avoid; best-effort skip).
            # Most simulator setups include pipeline_id in Assignment, and results should map back internally.
            if pid is not None and pid in s.pipeline_by_id:
                _enqueue_pipeline(s.pipeline_by_id[pid])

            if r.failed():
                # Update hints per failed op
                for op in (r.ops or []):
                    opk = _op_key(op)
                    hk = (pid, opk) if pid is not None else (None, opk)

                    prev_retry = s.op_retries.get(hk, 0)
                    s.op_retries[hk] = prev_retry + 1

                    # RAM inflation strategy:
                    # - OOM: double + bump; clamp to pool max observed via allocated RAM if available.
                    # - Non-OOM: small bump (in case of memory underestimate); don't grow too aggressively.
                    prev_hint = float(s.op_ram_hint.get(hk, 0.0) or 0.0)
                    allocated_ram = float(getattr(r, "ram", 0.0) or 0.0)
                    base = max(prev_hint, allocated_ram, 1.0)

                    if _is_oom_error(getattr(r, "error", None)):
                        # Exponential backoff, but avoid unbounded growth when retries are very high.
                        if prev_retry < s.max_retries:
                            new_hint = base * 2.0 + 0.5
                        else:
                            new_hint = base * 1.25 + 0.5
                    else:
                        # Mild bump; many failures are transient/modelled; keep trying but don't explode RAM.
                        new_hint = base * 1.15 + 0.25

                    s.op_ram_hint[hk] = new_hint

                    # CPU hint: if we already asked for large CPU, keep; otherwise gently increase for HP.
                    allocated_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                    prev_cpu_hint = float(s.op_cpu_hint.get(hk, 0.0) or 0.0)
                    if r.priority in (Priority.QUERY, Priority.INTERACTIVE):
                        s.op_cpu_hint[hk] = max(prev_cpu_hint, allocated_cpu * 1.10, 1.0)
                    else:
                        s.op_cpu_hint[hk] = max(prev_cpu_hint, allocated_cpu, 1.0)

            else:
                # On success: clear recent suspension marker if this container finished.
                cid = getattr(r, "container_id", None)
                if cid is not None:
                    s.suspended_recent.discard(cid)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # Determine whether any HP is likely runnable (best-effort)
    pending_hp = _has_any_runnable(s.q_query) or _has_any_runnable(s.q_interactive)

    suspensions = []
    assignments = []

    # -----------------------------
    # 3) Schedule per pool
    # -----------------------------
    # Pool preference: if multiple pools, push batch to non-latency pools first.
    pool_order = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1:
        # Keep latency pool first for HP, but schedule batch on other pools first by iterating them earlier for batch decisions.
        # We'll still iterate in fixed order; placement preference is handled below.
        pass

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]

        # Protect latency pool: avoid running batch there if any HP is pending (to reduce interference).
        latency_pool = (s.executor.num_pools > 1 and pool_id == s.latency_pool_id)

        # Keep filling the pool while there are resources and runnable work.
        # Use a bounded loop to avoid infinite churn when work isn't runnable.
        for _ in range(64):
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

            # If this is the latency pool and HP exists, don't admit batch here.
            local_pending_hp = pending_hp

            # Decide which priority to try first
            pr = _pick_priority_for_pool(pool_id, local_pending_hp)

            # Enforce "no batch on latency pool while HP pending"
            if latency_pool and local_pending_hp and pr == Priority.BATCH_PIPELINE:
                # Try HP instead; if none available, allow batch
                if len(s.q_query) > 0:
                    pr = Priority.QUERY
                elif len(s.q_interactive) > 0:
                    pr = Priority.INTERACTIVE

            # For non-latency pools, prefer batch when HP exists (so pool 0 stays responsive)
            if s.executor.num_pools > 1 and (not latency_pool) and local_pending_hp:
                if len(s.q_batch) > 0 and s.hp_streak < 6:
                    pr = Priority.BATCH_PIPELINE

            # Try up to three priorities to find something schedulable on this pool
            tried = []
            scheduled = False
            for _try in range(3):
                if pr in tried:
                    # pick next priority
                    if Priority.QUERY not in tried:
                        pr = Priority.QUERY
                    elif Priority.INTERACTIVE not in tried:
                        pr = Priority.INTERACTIVE
                    else:
                        pr = Priority.BATCH_PIPELINE
                tried.append(pr)

                q, in_set = _queue_for_priority(pr)
                p = _pop_next_runnable(q, in_set)
                if p is None:
                    continue

                # If we picked a batch pipeline but should avoid it on latency pool while HP pending, skip it.
                if latency_pool and local_pending_hp and p.priority == Priority.BATCH_PIPELINE:
                    continue

                op = _get_runnable_op(p)
                if op is None:
                    # Not runnable yet; keep pipeline in queue rotation.
                    continue

                # If no pipeline_id is on results, we still use p.pipeline_id for hint keys.
                req_cpu, req_ram = _choose_resources(
                    pool=pool,
                    priority=p.priority,
                    pid=p.pipeline_id,
                    op=op,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                )

                if req_cpu <= 0.0 or req_ram <= 0.0:
                    # If HP couldn't fit, consider preemption of batch containers to make headroom.
                    if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                        # Estimate needed as base request (use base fractions) to drive preemption
                        base_ram_frac, base_cpu_frac = _priority_base_fracs(p.priority)
                        needed_ram = max(1.0, pool.max_ram_pool * base_ram_frac) - avail_ram
                        needed_cpu = max(1.0, pool.max_cpu_pool * base_cpu_frac) - avail_cpu
                        if needed_ram > 0.0 or needed_cpu > 0.0:
                            suspensions.extend(_maybe_preempt_for_hp(pool_id, max(0.0, needed_cpu), max(0.0, needed_ram)))
                    continue

                # Make assignment (one op at a time to reduce contention and improve responsiveness)
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

                # Update fairness streak
                if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                    s.hp_streak += 1
                else:
                    s.hp_streak = 0

                scheduled = True
                break

            if not scheduled:
                # Nothing schedulable on this pool right now.
                break

    return suspensions, assignments
