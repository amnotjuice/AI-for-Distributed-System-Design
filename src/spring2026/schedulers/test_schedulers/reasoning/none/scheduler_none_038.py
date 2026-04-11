# policy_key: scheduler_none_038
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.047149
# generation_seconds: 51.83
# generated_at: 2026-04-09T22:14:31.026581
@register_scheduler_init(key="scheduler_none_038")
def scheduler_none_038_init(s):
    """Priority-aware, OOM-adaptive, fairness-preserving scheduler.

    Goals aligned to the weighted latency objective:
      - Strongly protect QUERY / INTERACTIVE tail latency with reserved headroom + selective preemption.
      - Avoid failures (720s penalty) via RAM-first sizing, OOM-triggered RAM backoff, and conservative CPU boosts.
      - Prevent starvation of BATCH by aging and guaranteed small service when the system is busy.
    """
    # Pipeline queues (store pipeline objects)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-pipeline aging counters to prevent starvation (in ticks)
    s.age = {}  # pipeline_id -> int

    # Per-pipeline resource "hints" learned from past attempts
    # ram_hint in absolute units; cpu_hint in absolute units
    s.hints = {}  # pipeline_id -> {"ram_hint": float, "cpu_hint": float, "ooms": int}

    # Simple bookkeeping of arrived pipelines (used for aging only; scoring handled by simulator)
    s.seen = set()

    # Knobs (kept conservative; improvements over FIFO without being too aggressive)
    s.MIN_CPU = 1.0
    s.MIN_RAM_FRAC = 0.08          # base fraction of pool RAM for new ops (per assignment)
    s.MAX_RAM_FRAC = 0.85          # never allocate > this fraction of pool RAM to one container
    s.CPU_FRAC_Q = 0.75            # target CPU fraction for query assignments when possible
    s.CPU_FRAC_I = 0.60            # target CPU fraction for interactive assignments when possible
    s.CPU_FRAC_B = 0.35            # target CPU fraction for batch assignments when possible
    s.RESERVE_CPU_FRAC = 0.20      # reserve CPU for potential high-priority arrivals
    s.RESERVE_RAM_FRAC = 0.20      # reserve RAM for potential high-priority arrivals
    s.AGING_TICKS = 25             # after this, a lower-priority pipeline gets boosted
    s.BATCH_GUARANTEE_EVERY = 6    # guarantee at least one batch dispatch every N scheduling calls if batch exists
    s._tick = 0
    s._since_batch = 0

    # Preemption knobs: keep churn low
    s.PREEMPT_MIN_AGE = 8          # only preempt a container if it has run at least this many ticks (roughly)
    s.PREEMPT_COOLDOWN = {}        # container_id -> tick last preempted (avoid repeated churn)
    s.PREEMPT_COOLDOWN_TICKS = 10


@register_scheduler(key="scheduler_none_038")
def scheduler_none_038(s, results, pipelines):
    """
    Policy outline:
      1) Enqueue new pipelines by priority; maintain aging.
      2) Learn from results:
         - On OOM: increase RAM hint substantially and requeue (do not drop).
         - On other failures: mildly increase RAM and CPU hints (still requeue).
      3) For each pool, schedule ready operators with:
         - Priority first (QUERY > INTERACTIVE > BATCH), but with aging to prevent starvation.
         - RAM-first safe sizing using hints; CPU sized by priority fractions.
         - Keep headroom reserved for high-priority arrivals; if blocked, selectively preempt low-priority running work.
      4) Ensure batch makes progress with a periodic guarantee if batch backlog exists.
    """
    s._tick += 1

    def _prio_rank(p):
        # Smaller is higher priority
        if p.priority == Priority.QUERY:
            return 0
        if p.priority == Priority.INTERACTIVE:
            return 1
        return 2

    def _queue_for_priority(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        if p.pipeline_id not in s.seen:
            s.seen.add(p.pipeline_id)
            s.age[p.pipeline_id] = 0
            if p.pipeline_id not in s.hints:
                s.hints[p.pipeline_id] = {"ram_hint": 0.0, "cpu_hint": 0.0, "ooms": 0}
        _queue_for_priority(p.priority).append(p)

    def _pipeline_done_or_dead(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # We do not permanently drop failed pipelines; instead, we try to retry failed ops.
        # But if the whole pipeline is in a state with no assignable ops and has failures,
        # we still keep it queued (it may be retryable via FAILED ops being assignable).
        return False

    def _pop_next_candidate():
        # Apply aging: if any batch pipeline has aged beyond threshold, allow it to compete.
        # Likewise, interactive aging helps against query storms.
        def _aged_front(q):
            while q:
                p = q[0]
                if _pipeline_done_or_dead(p):
                    q.pop(0)
                    continue
                return p
            return None

        q0 = _aged_front(s.q_query)
        q1 = _aged_front(s.q_interactive)
        q2 = _aged_front(s.q_batch)

        candidates = []
        if q0 is not None:
            candidates.append(q0)
        if q1 is not None:
            candidates.append(q1)
        if q2 is not None:
            candidates.append(q2)
        if not candidates:
            return None

        # Aging boosts: subtract a bonus from the rank based on age
        def _score(p):
            base = _prio_rank(p)
            age = s.age.get(p.pipeline_id, 0)
            # Stronger aging for lower priority to ensure eventual progress
            if p.priority == Priority.BATCH_PIPELINE:
                bonus = age / float(s.AGING_TICKS)
            elif p.priority == Priority.INTERACTIVE:
                bonus = age / float(s.AGING_TICKS * 1.5)
            else:
                bonus = age / float(s.AGING_TICKS * 3.0)
            return base - bonus

        best = min(candidates, key=_score)
        # Remove it from its queue (front only if present at front; otherwise linear remove)
        q = _queue_for_priority(best.priority)
        try:
            if q and q[0].pipeline_id == best.pipeline_id:
                q.pop(0)
            else:
                for i, pp in enumerate(q):
                    if pp.pipeline_id == best.pipeline_id:
                        q.pop(i)
                        break
        except Exception:
            pass
        return best

    def _ready_ops_for_pipeline(p, limit=1):
        st = p.runtime_status()
        # We consider FAILED as assignable as per ASSIGNABLE_STATES; require parents complete.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:limit]

    def _compute_request(p, pool, want_cpu_frac, want_ram_frac):
        # Hint-based sizing with RAM-first safety. We keep a reserved headroom unless the job is high priority.
        hint = s.hints.get(p.pipeline_id, {"ram_hint": 0.0, "cpu_hint": 0.0, "ooms": 0})
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # Base requests (fractions of total pool) capped by available.
        base_cpu = max(s.MIN_CPU, want_cpu_frac * max_cpu)
        base_ram = max(s.MIN_RAM_FRAC * max_ram, want_ram_frac * max_ram)

        # Apply hints; RAM hint dominates to avoid OOM penalties.
        ram_req = max(base_ram, float(hint.get("ram_hint", 0.0) or 0.0))
        cpu_req = max(base_cpu, float(hint.get("cpu_hint", 0.0) or 0.0))

        # Cap per-container sizes
        ram_req = min(ram_req, s.MAX_RAM_FRAC * max_ram)
        cpu_req = min(cpu_req, max_cpu)

        # Never request more than available, but keep a small reserve for high priority arrivals
        # (reserve relaxed for query).
        if p.priority == Priority.QUERY:
            reserve_cpu = 0.0
            reserve_ram = 0.0
        elif p.priority == Priority.INTERACTIVE:
            reserve_cpu = s.RESERVE_CPU_FRAC * max_cpu * 0.5
            reserve_ram = s.RESERVE_RAM_FRAC * max_ram * 0.5
        else:
            reserve_cpu = s.RESERVE_CPU_FRAC * max_cpu
            reserve_ram = s.RESERVE_RAM_FRAC * max_ram

        cpu_cap = max(0.0, pool.avail_cpu_pool - reserve_cpu)
        ram_cap = max(0.0, pool.avail_ram_pool - reserve_ram)

        cpu_req = min(cpu_req, cpu_cap if cpu_cap > 0 else pool.avail_cpu_pool)
        ram_req = min(ram_req, ram_cap if ram_cap > 0 else pool.avail_ram_pool)

        # Enforce minima
        if cpu_req < s.MIN_CPU:
            cpu_req = 0.0
        if ram_req <= 0:
            ram_req = 0.0

        return cpu_req, ram_req

    def _maybe_update_hints_from_result(r):
        # Increase hints to reduce repeat failures. Avoid extreme jumps except on OOM.
        # We assume r.ram and r.cpu reflect what we last allocated.
        if r is None or not getattr(r, "pipeline_id", None):
            # Some simulators might not include pipeline_id; we fall back to using ops' pipeline id if present.
            return

    def _update_hints_on_failure(pipeline_id, allocated_cpu, allocated_ram, is_oom):
        h = s.hints.get(pipeline_id)
        if h is None:
            h = {"ram_hint": 0.0, "cpu_hint": 0.0, "ooms": 0}
            s.hints[pipeline_id] = h

        if is_oom:
            h["ooms"] = int(h.get("ooms", 0)) + 1
            # RAM-first backoff: big jump early, then taper.
            factor = 1.6 if h["ooms"] <= 2 else 1.35
            new_ram = max(float(h.get("ram_hint", 0.0) or 0.0), allocated_ram * factor)
            h["ram_hint"] = new_ram
            # Slight CPU bump to reduce retry wall time, but keep modest.
            h["cpu_hint"] = max(float(h.get("cpu_hint", 0.0) or 0.0), allocated_cpu)
        else:
            # Non-OOM failure: modestly increase both; could be underprovisioned.
            h["ram_hint"] = max(float(h.get("ram_hint", 0.0) or 0.0), allocated_ram * 1.15)
            h["cpu_hint"] = max(float(h.get("cpu_hint", 0.0) or 0.0), max(s.MIN_CPU, allocated_cpu * 1.10))

    def _infer_oom(r):
        # Best-effort: rely on error string if present.
        err = getattr(r, "error", None)
        if not err:
            return False
        try:
            e = str(err).lower()
            return ("oom" in e) or ("out of memory" in e) or ("memoryerror" in e)
        except Exception:
            return False

    # 1) Enqueue new arrivals
    for p in pipelines:
        _enqueue_pipeline(p)

    # 2) Process results: requeue pipelines as needed and adjust hints
    # Also maintain a lightweight view of "recent failures" to prioritize retries (reduce incomplete penalties).
    for r in results:
        if getattr(r, "failed", None) and r.failed():
            # If we can identify pipeline_id, update hint; otherwise skip hint update.
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                # Attempt to extract from r.ops[0] if it has pipeline_id-like attribute
                try:
                    if r.ops and hasattr(r.ops[0], "pipeline_id"):
                        pid = r.ops[0].pipeline_id
                except Exception:
                    pid = None
            if pid is not None:
                _update_hints_on_failure(pid, getattr(r, "cpu", 0.0) or 0.0, getattr(r, "ram", 0.0) or 0.0, _infer_oom(r))

    # Aging: increment for all queued pipelines
    for q in (s.q_query, s.q_interactive, s.q_batch):
        for p in q:
            s.age[p.pipeline_id] = s.age.get(p.pipeline_id, 0) + 1

    # Helper: identify running containers to preempt (if needed)
    def _select_preemptions_for_pool(pool_id, need_cpu, need_ram):
        # We only preempt when a high priority job cannot fit and we need to protect latency.
        # Heuristic: preempt lowest priority containers first, avoiding repeated churn.
        susp = []
        if need_cpu <= 0 and need_ram <= 0:
            return susp

        # Best-effort introspection: try to find running containers list on pool.
        pool = s.executor.pools[pool_id]
        containers = []
        for attr in ("containers", "running_containers", "active_containers"):
            if hasattr(pool, attr):
                try:
                    containers = list(getattr(pool, attr))
                    break
                except Exception:
                    containers = []
        if not containers:
            # No visibility; cannot preempt safely
            return susp

        def _c_prio(c):
            # Container object may have .priority; otherwise treat as batch.
            pr = getattr(c, "priority", Priority.BATCH_PIPELINE)
            if pr == Priority.QUERY:
                return 0
            if pr == Priority.INTERACTIVE:
                return 1
            return 2

        def _c_age_ok(c):
            # If container has start time, use it; else assume ok.
            # We model preemption conservatively with a cooldown to reduce churn.
            cid = getattr(c, "container_id", None)
            if cid is None:
                return True
            last = s.PREEMPT_COOLDOWN.get(cid, -10**9)
            if s._tick - last < s.PREEMPT_COOLDOWN_TICKS:
                return False
            start = getattr(c, "start_tick", None)
            if start is None:
                return True
            return (s._tick - start) >= s.PREEMPT_MIN_AGE

        # Sort by (priority desc, bigger resource consumption first) to free quickly.
        def _c_size(c):
            return (getattr(c, "cpu", 0.0) or 0.0) + 0.5 * (getattr(c, "ram", 0.0) or 0.0)

        candidates = [c for c in containers if _c_prio(c) >= 2 and _c_age_ok(c)]
        candidates.sort(key=lambda c: (_c_prio(c), -_c_size(c)), reverse=True)

        freed_cpu = 0.0
        freed_ram = 0.0
        for c in candidates:
            cid = getattr(c, "container_id", None)
            if cid is None:
                continue
            susp.append(Suspend(container_id=cid, pool_id=pool_id))
            s.PREEMPT_COOLDOWN[cid] = s._tick
            freed_cpu += (getattr(c, "cpu", 0.0) or 0.0)
            freed_ram += (getattr(c, "ram", 0.0) or 0.0)
            if freed_cpu >= need_cpu and freed_ram >= need_ram:
                break
        return susp

    suspensions = []
    assignments = []

    # 3) Schedule per pool
    # Prefer filling pools with high-priority work; keep small reserved headroom for new arrivals.
    # To reduce incomplete pipelines, we try to assign at most 1 op per pool per tick.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # If batch backlog exists, periodically guarantee a batch dispatch to avoid starvation.
        force_batch = False
        if s.q_batch and s._since_batch >= s.BATCH_GUARANTEE_EVERY:
            force_batch = True

        chosen = None
        if force_batch:
            # Pop the first viable batch pipeline
            # Clean completed ones as we go
            while s.q_batch:
                p = s.q_batch.pop(0)
                if _pipeline_done_or_dead(p):
                    continue
                chosen = p
                break
        else:
            chosen = _pop_next_candidate()

        if chosen is None:
            continue

        # Skip pipelines with nothing ready; requeue them to avoid drops.
        ops = _ready_ops_for_pipeline(chosen, limit=1)
        if not ops:
            _enqueue_pipeline(chosen)
            continue

        # Determine target resource fractions by priority
        if chosen.priority == Priority.QUERY:
            want_cpu_frac = s.CPU_FRAC_Q
            want_ram_frac = 0.20
        elif chosen.priority == Priority.INTERACTIVE:
            want_cpu_frac = s.CPU_FRAC_I
            want_ram_frac = 0.18
        else:
            want_cpu_frac = s.CPU_FRAC_B
            want_ram_frac = 0.14

        cpu_req, ram_req = _compute_request(chosen, pool, want_cpu_frac, want_ram_frac)

        # If it doesn't fit, attempt limited preemption for high priority only.
        if (cpu_req <= 0 or ram_req <= 0) and chosen.priority in (Priority.QUERY, Priority.INTERACTIVE):
            # Compute "needed" vs current available (no reserve for this emergency)
            need_cpu = max(0.0, max(s.MIN_CPU, want_cpu_frac * pool.max_cpu_pool) - pool.avail_cpu_pool)
            need_ram = max(0.0, max(s.MIN_RAM_FRAC * pool.max_ram_pool, want_ram_frac * pool.max_ram_pool) - pool.avail_ram_pool)
            suspensions.extend(_select_preemptions_for_pool(pool_id, need_cpu, need_ram))
            # After suspensions, try again with direct available (reserve disabled for query; reduced for interactive)
            cpu_req, ram_req = _compute_request(chosen, pool, want_cpu_frac, want_ram_frac)

        # If still cannot place, requeue (avoid dropping / failing by forcing undersized allocations)
        if cpu_req < s.MIN_CPU or ram_req <= 0:
            _enqueue_pipeline(chosen)
            continue

        assignments.append(
            Assignment(
                ops=ops,
                cpu=cpu_req,
                ram=ram_req,
                priority=chosen.priority,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
        )

        if chosen.priority == Priority.BATCH_PIPELINE:
            s._since_batch = 0
        else:
            s._since_batch += 1

        # Requeue pipeline for its next operator(s); we keep it in circulation until success.
        _enqueue_pipeline(chosen)

    # Clean queues of completed pipelines opportunistically (keeps queues from bloating)
    for q in (s.q_query, s.q_interactive, s.q_batch):
        keep = []
        for p in q:
            if _pipeline_done_or_dead(p):
                continue
            keep.append(p)
        q[:] = keep

    return suspensions, assignments
