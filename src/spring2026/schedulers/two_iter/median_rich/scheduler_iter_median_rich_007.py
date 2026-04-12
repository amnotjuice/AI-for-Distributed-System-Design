# policy_key: scheduler_iter_median_rich_007
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.056032
# generation_seconds: 39.05
# generated_at: 2026-04-12T01:48:31.224900
@register_scheduler_init(key="scheduler_iter_median_rich_007")
def scheduler_iter_median_rich_007_init(s):
    """Priority-aware RR with *aggressive RAM deflation* + per-pipeline OOM learning.

    Direction vs previous attempt:
    - Main latency killer in stats: massive RAM overallocation (allocated ~94%, consumed ~30%)
      causing queueing/timeouts. So we allocate much smaller RAM by default, then learn upward
      only when we observe OOMs for that specific pipeline.
    - True per-pipeline learning via mapping result.ops -> pipeline_id captured at assignment time.
    - Still simple: strict priority order, RR within priority, 1 op per assignment, fill pool.
    - No suspensions yet (kept off until we can reliably introspect running containers).
    """
    # Priority queues (RR within each)
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

    # Per-pipeline adaptive hints:
    # ram_floor: minimum RAM we will try next (driven by last OOM)
    # ram_mult: mild multiplier for baseline RAM fraction
    # cpu_mult: multiplier for CPU fraction (driven by timeouts/slowdowns)
    # drop: if non-oom failures observed, stop retrying the pipeline
    s.pstate = {}

    # Map operator object id -> pipeline_id, set on assignment; used to attribute results.
    s.op_to_pipeline = {}

    # Optional: gentle aging to avoid total starvation (counts how many scheduler steps waited)
    s.age = {}  # pipeline_id -> int


@register_scheduler(key="scheduler_iter_median_rich_007")
def scheduler_iter_median_rich_007_scheduler(s, results, pipelines):
    """Step function: enqueue -> update learning from results -> schedule by priority with lean RAM."""
    # -----------------------------
    # Helpers (local)
    # -----------------------------
    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _ensure_pstate(pid):
        if pid not in s.pstate:
            s.pstate[pid] = {
                "ram_floor": 0.0,   # absolute floor from prior OOMs
                "ram_mult": 1.0,    # mild scaling on base ram fraction
                "cpu_mult": 1.0,    # scaling on base cpu fraction
                "drop": False,      # non-oom failure => stop retrying
            }
        return s.pstate[pid]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg)

    def _pipeline_done_or_dropped(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        if s.pstate.get(p.pipeline_id, {}).get("drop", False):
            # if it has any failures, we drop it; otherwise let it run
            return st.state_counts.get(OperatorState.FAILED, 0) > 0
        return False

    def _pop_rr_pipeline(pri):
        """Round-robin selection within a priority queue; skips completed/dropped pipelines."""
        q = s.queues[pri]
        if not q:
            return None

        n = len(q)
        start = s.rr_cursor[pri] % max(1, n)
        tries = 0
        while tries < n and q:
            idx = (start + tries) % len(q)
            p = q[idx]
            if _pipeline_done_or_dropped(p):
                q.pop(idx)
                if idx < start:
                    start -= 1
                n = len(q)
                if n == 0:
                    s.rr_cursor[pri] = 0
                    return None
                continue

            # Keep it; advance cursor
            s.rr_cursor[pri] = (idx + 1) % max(1, len(q))
            return p

        return None

    def _has_backlog(pri):
        for p in s.queues[pri]:
            if _pipeline_done_or_dropped(p):
                continue
            st = p.runtime_status()
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                return True
        return False

    def _base_fractions(pri):
        # Lean RAM to avoid over-reservation; learn upward on OOM.
        # CPU also kept modest to allow concurrency (latency tends to improve).
        if pri == Priority.QUERY:
            return 0.20, 0.05  # cpu_frac, ram_frac
        if pri == Priority.INTERACTIVE:
            return 0.30, 0.08
        return 0.45, 0.10  # batch

    def _clamp(x, lo, hi):
        return max(lo, min(hi, x))

    def _compute_request(pool, pri, pid, avail_cpu, avail_ram):
        pst = _ensure_pstate(pid)
        cpu_frac, ram_frac = _base_fractions(pri)

        # CPU: modest slices; higher if we observed timeouts (cpu_mult grows).
        # Also cap query cpu to avoid hogging.
        cpu_req = pool.max_cpu_pool * cpu_frac * pst["cpu_mult"]
        if pri == Priority.QUERY:
            cpu_req = min(cpu_req, 2.0)
        elif pri == Priority.INTERACTIVE:
            cpu_req = min(cpu_req, 4.0)

        # RAM: strongly deflated baseline; enforce ram_floor (from OOM learning).
        ram_req = pool.max_ram_pool * ram_frac * pst["ram_mult"]
        ram_req = max(ram_req, pst["ram_floor"])

        # Always request at least a tiny slice so tiny pools can still make progress.
        cpu_req = _clamp(cpu_req, 1.0, pool.max_cpu_pool)
        ram_req = _clamp(ram_req, 1.0, pool.max_ram_pool)

        # Never exceed available resources in this pool at this moment.
        cpu_req = min(cpu_req, avail_cpu)
        ram_req = min(ram_req, avail_ram)

        return cpu_req, ram_req

    # -----------------------------
    # 1) Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        _ensure_pstate(p.pipeline_id)
        s.queues[p.priority].append(p)
        s.age[p.pipeline_id] = 0

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # -----------------------------
    # 2) Update per-pipeline learning from results (via op->pipeline mapping)
    # -----------------------------
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        # Attribute this result to a pipeline (best-effort)
        pid = None
        for op in getattr(r, "ops", []) or []:
            pid = s.op_to_pipeline.get(id(op))
            if pid is not None:
                break
        if pid is None:
            # Can't attribute reliably; skip to avoid poisoning unrelated pipelines.
            continue

        pst = _ensure_pstate(pid)
        err = getattr(r, "error", None)

        if _is_oom_error(err):
            # Learn a higher minimum RAM for this pipeline.
            # Use the failing container's allocated ram as a signal and bump.
            failing_ram = float(getattr(r, "ram", 0.0) or 0.0)
            # Next try: at least 2.2x failing allocation, plus slight multiplier bump.
            pst["ram_floor"] = max(pst["ram_floor"], failing_ram * 2.2, 1.0)
            pst["ram_mult"] = min(pst["ram_mult"] * 1.20, 8.0)
        elif _is_timeout_error(err):
            # If timing out, try more CPU (without inflating RAM and blocking everyone).
            pst["cpu_mult"] = min(pst["cpu_mult"] * 1.25, 6.0)
        else:
            # Non-oom failure: stop retrying this pipeline to avoid churn.
            pst["drop"] = True

    # Increment age for queued pipelines (used for a tiny fairness tweak)
    for pri in _priority_order():
        for p in s.queues[pri]:
            if p.pipeline_id in s.age:
                s.age[p.pipeline_id] += 1

    # -----------------------------
    # 3) Schedule assignments per pool
    # -----------------------------
    suspensions = []  # intentionally unused in this iteration
    assignments = []

    hp_backlog = _has_backlog(Priority.QUERY) or _has_backlog(Priority.INTERACTIVE)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft reservation: if high-priority backlog exists, cap batch so it can't consume all RAM.
        batch_cpu_cap = avail_cpu
        batch_ram_cap = avail_ram
        if hp_backlog:
            batch_cpu_cap = min(batch_cpu_cap, pool.max_cpu_pool * 0.55)
            batch_ram_cap = min(batch_ram_cap, pool.max_ram_pool * 0.55)

        # Keep placing single-op assignments until resources depleted or nothing runnable.
        while avail_cpu > 0 and avail_ram > 0:
            chosen = None
            chosen_pri = None
            chosen_ops = None

            # Strict priority selection, but with a tiny "aging bump" inside batch vs interactive
            # avoided for simplicity; we only do a minimal fairness tweak for batch later.
            for pri in _priority_order():
                p = _pop_rr_pipeline(pri)
                if p is None:
                    continue

                st = p.runtime_status()
                if _pipeline_done_or_dropped(p):
                    continue

                # Only schedule ops whose parents are complete, one op at a time (latency-friendly).
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    # Put it back and keep searching.
                    s.queues[pri].append(p)
                    continue

                chosen = p
                chosen_pri = pri
                chosen_ops = op_list
                break

            if chosen is None:
                break

            # Apply batch caps under hp backlog
            cpu_limit = avail_cpu
            ram_limit = avail_ram
            if chosen_pri == Priority.BATCH_PIPELINE and hp_backlog:
                cpu_limit = min(cpu_limit, batch_cpu_cap)
                ram_limit = min(ram_limit, batch_ram_cap)

            if cpu_limit <= 0 or ram_limit <= 0:
                # Can't fit this batch work now; push back and stop scheduling in this pool
                s.queues[chosen_pri].append(chosen)
                break

            cpu_req, ram_req = _compute_request(pool, chosen_pri, chosen.pipeline_id, cpu_limit, ram_limit)
            if cpu_req <= 0 or ram_req <= 0:
                s.queues[chosen_pri].append(chosen)
                break

            # Record op->pipeline mapping to attribute future failures precisely.
            for op in chosen_ops:
                s.op_to_pipeline[id(op)] = chosen.pipeline_id

            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen.pipeline_id,
                )
            )

            # Consume resources locally to avoid over-issuing in one tick
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Push pipeline back for future ops
            s.queues[chosen_pri].append(chosen)

            # Minimal fairness: if no high-priority backlog, occasionally let batch take bigger bites
            # by slightly relaxing caps over time (without increasing RAM baseline).
            # Implemented implicitly via hp_backlog condition only (kept simple here).

    return suspensions, assignments
