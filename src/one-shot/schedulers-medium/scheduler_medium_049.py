# policy_key: scheduler_medium_049
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.078651
# generation_seconds: 85.76
# generated_at: 2026-03-14T03:24:19.767388
@register_scheduler_init(key="scheduler_medium_049")
def scheduler_medium_049_init(s):
    """Priority-aware, headroom-preserving scheduler.

    Incremental improvements over naive FIFO:
      1) Priority ordering with simple aging (prevents starvation).
      2) Right-sizing: cap per-assignment CPU/RAM by priority to avoid one task
         grabbing an entire pool (reduces head-of-line blocking -> better latency).
      3) Soft pool affinity: if multiple pools exist, prefer keeping pool 0 for
         higher priority work, but allow backfill with batch when idle.
      4) Best-effort OOM retry: if we can attribute an OOM failure to a pipeline,
         increase a per-pipeline RAM hint and allow retry of FAILED ops.
    """
    s.waiting_pipelines = []  # List[Pipeline]
    s.tick = 0

    # Per-pipeline hints/state (keyed by pipeline_id)
    # {
    #   "arrival_tick": int,
    #   "ram_hint": float|None,
    #   "oom_retries": int,
    #   "allow_failed_retry": bool,
    # }
    s.pinfo = {}

    # Tuning knobs (kept conservative for "small improvements first")
    s.scan_limit = 64  # max pipelines to consider per pool per scheduling step
    s.max_assignments_per_pool = 8  # avoid overscheduling into a single pool per tick
    s.max_oom_retries = 3


@register_scheduler(key="scheduler_medium_049")
def scheduler_medium_049(s, results, pipelines):
    """Scheduler step: assign ready operators to pools with priority + aging + caps."""
    # ---- helpers (kept inside to avoid top-level imports) ----
    def _priority_weight(prio):
        # Higher is more important.
        if prio == Priority.INTERACTIVE:
            return 300
        if prio == Priority.QUERY:
            return 200
        return 100  # Priority.BATCH_PIPELINE or unknown

    def _age_boost(pipeline_id):
        info = s.pinfo.get(pipeline_id)
        if not info:
            return 0
        # Aging scaled to matter but still respect priority.
        # This prevents indefinite starvation under continuous interactive load.
        waited = max(0, s.tick - info.get("arrival_tick", s.tick))
        return 5 * waited

    def _score(p):
        return _priority_weight(p.priority) + _age_boost(p.pipeline_id)

    def _is_oom_error(err):
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memoryerror" in e)

    def _result_pipeline_ids(r):
        # Best-effort mapping from result -> pipeline_id (API may vary).
        pid = getattr(r, "pipeline_id", None)
        if pid is not None:
            return [pid]
        pids = []
        ops = getattr(r, "ops", None) or []
        for op in ops:
            op_pid = getattr(op, "pipeline_id", None)
            if op_pid is not None:
                pids.append(op_pid)
        # de-dup while preserving order
        seen = set()
        uniq = []
        for x in pids:
            if x not in seen:
                seen.add(x)
                uniq.append(x)
        return uniq

    def _desired_caps(priority, pool):
        # Caps are fractions of pool maxima to preserve headroom and reduce HoL blocking.
        if priority == Priority.INTERACTIVE:
            return 0.50, 0.50  # (cpu_frac, ram_frac)
        if priority == Priority.QUERY:
            return 0.33, 0.33
        return 0.25, 0.25  # batch

    def _compute_request(pipeline, pool, avail_cpu, avail_ram):
        # Use per-pipeline RAM hint if present; else use priority-based caps.
        cpu_frac, ram_frac = _desired_caps(pipeline.priority, pool)

        cpu_cap = max(1, pool.max_cpu_pool * cpu_frac)
        ram_cap = max(1, pool.max_ram_pool * ram_frac)

        info = s.pinfo.get(pipeline.pipeline_id, {})
        ram_hint = info.get("ram_hint", None)

        # CPU: cap by priority; also never exceed available.
        cpu_req = min(avail_cpu, cpu_cap)

        # RAM: use hint if it exists; else cap by priority; never exceed available.
        if ram_hint is None:
            ram_req = min(avail_ram, ram_cap)
        else:
            ram_req = min(avail_ram, max(1, min(pool.max_ram_pool, ram_hint)))

        # If we can't allocate at least 1 unit of each, don't schedule.
        if cpu_req < 1 or ram_req < 1:
            return None, None
        return cpu_req, ram_req

    # ---- update tick + incorporate new pipelines ----
    s.tick += 1

    for p in pipelines:
        s.waiting_pipelines.append(p)
        if p.pipeline_id not in s.pinfo:
            s.pinfo[p.pipeline_id] = {
                "arrival_tick": s.tick,
                "ram_hint": None,
                "oom_retries": 0,
                "allow_failed_retry": False,
            }

    # ---- process execution results (best-effort OOM retry hints) ----
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue
        # Only treat OOM as retriable; other failures are left to default behavior.
        if not _is_oom_error(getattr(r, "error", None)):
            continue

        for pid in _result_pipeline_ids(r):
            info = s.pinfo.get(pid)
            if not info:
                continue
            if info.get("oom_retries", 0) >= s.max_oom_retries:
                continue

            # Increase RAM hint aggressively to avoid repeated OOM churn.
            prev_hint = info.get("ram_hint", None)
            observed_ram = getattr(r, "ram", None)
            # If observed_ram is unavailable, fall back to doubling previous hint.
            if observed_ram is None:
                if prev_hint is None:
                    # No usable signal; set a modest default hint.
                    new_hint = 2
                else:
                    new_hint = prev_hint * 2
            else:
                new_hint = max(2, (observed_ram * 2), (prev_hint * 2 if prev_hint is not None else 0))

            info["ram_hint"] = new_hint
            info["oom_retries"] = info.get("oom_retries", 0) + 1
            info["allow_failed_retry"] = True

    # If nothing changed, do nothing (fast path)
    if not pipelines and not results:
        return [], []

    # ---- purge completed and (most) fatally failed pipelines from waiting list ----
    new_waiting = []
    for p in s.waiting_pipelines:
        st = p.runtime_status()
        if st.is_pipeline_successful():
            # done
            s.pinfo.pop(p.pipeline_id, None)
            continue

        has_failures = st.state_counts[OperatorState.FAILED] > 0
        if has_failures:
            info = s.pinfo.get(p.pipeline_id, {})
            # Only keep failed pipelines if we've explicitly marked them as retriable (OOM).
            if not info.get("allow_failed_retry", False):
                s.pinfo.pop(p.pipeline_id, None)
                continue

        new_waiting.append(p)
    s.waiting_pipelines = new_waiting

    # ---- build assignments ----
    suspensions = []
    assignments = []

    # Avoid assigning multiple ops from same pipeline in a single tick (reduces burstiness).
    scheduled_pipeline_ids = set()

    # Pre-sort by (score desc) to prioritize latency while aging provides fairness.
    # We'll only scan a prefix for efficiency.
    s.waiting_pipelines.sort(key=_score, reverse=True)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu < 1 or avail_ram < 1:
            continue

        # If we have multiple pools, prefer reserving pool 0 for interactive/query,
        # but allow batch backfill if no higher-priority runnable work exists.
        prefer_hp_only = (s.executor.num_pools > 1 and pool_id == 0)

        made = 0
        while made < s.max_assignments_per_pool:
            if avail_cpu < 1 or avail_ram < 1:
                break

            chosen = None
            chosen_ops = None

            # First pass: optionally restrict to high priority (pool 0 reservation).
            for pass_no in (0, 1):
                if chosen is not None:
                    break
                hp_only = (prefer_hp_only and pass_no == 0)

                scanned = 0
                for p in s.waiting_pipelines:
                    if scanned >= s.scan_limit:
                        break
                    scanned += 1

                    if p.pipeline_id in scheduled_pipeline_ids:
                        continue

                    if hp_only and (p.priority not in (Priority.INTERACTIVE, Priority.QUERY)):
                        continue

                    st = p.runtime_status()

                    # If failed and not explicitly retriable, skip (should be purged already, but safe).
                    if st.state_counts[OperatorState.FAILED] > 0:
                        info = s.pinfo.get(p.pipeline_id, {})
                        if not info.get("allow_failed_retry", False):
                            continue

                    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not ops:
                        continue

                    # Compute size request; if it can't fit, skip (try next pipeline).
                    cpu_req, ram_req = _compute_request(p, pool, avail_cpu, avail_ram)
                    if cpu_req is None or ram_req is None:
                        continue

                    chosen = (p, cpu_req, ram_req)
                    chosen_ops = ops
                    break

            if chosen is None:
                break

            p, cpu_req, ram_req = chosen

            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            scheduled_pipeline_ids.add(p.pipeline_id)
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            made += 1

    return suspensions, assignments
