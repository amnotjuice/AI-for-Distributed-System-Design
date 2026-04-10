# policy_key: scheduler_est_030
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.056896
# generation_seconds: 50.80
# generated_at: 2026-04-10T10:09:32.552430
@register_scheduler_init(key="scheduler_est_030")
def scheduler_est_030_init(s):
    """
    Priority + memory-estimate-aware scheduler (no-preemption).

    Goals:
      - Protect QUERY / INTERACTIVE latency via strict priority ordering.
      - Reduce OOM-induced failures by sizing RAM using op.estimate.mem_peak_gb (noisy hint) + safety buffer.
      - Avoid starvation via round-robin within each priority class.
      - Retry OOM failures with increased RAM multiplier; drop only on non-OOM failures.
    """
    # Per-priority FIFO queues of pipeline_ids (round-robin within priority)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Pipeline registry: pipeline_id -> pipeline object (latest reference)
    s.pipe = {}

    # Per-pipeline state
    #  - ram_mult: multiplicative bump on OOM retries
    #  - dead: if a non-OOM failure occurred (do not retry)
    #  - arrival_seq: monotonic seq for stable tie-breaking
    s.pstate = {}
    s._arrival_seq = 0

    # Config knobs (keep conservative to reduce 720s penalties from failures)
    s.mem_safety = 1.35          # inflate estimator to reduce OOM risk
    s.mem_min_gb = 0.5           # avoid tiny allocations
    s.mem_fallback_frac = 0.25   # if estimate missing, use fraction of pool max RAM
    s.oom_backoff = 1.6          # multiplier bump on OOM
    s.ram_mult_cap = 8.0         # cap to avoid runaway allocations

    # How many pipelines per priority to look at per pool per tick (limits scanning)
    s.scan_depth = 24


@register_scheduler(key="scheduler_est_030")
def scheduler_est_030_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest arrivals into per-priority queues.
      2) Process results: on OOM -> bump RAM multiplier and retry; on other failure -> mark dead.
      3) For each pool, repeatedly pick the best next operator among top pipelines:
         - strict priority: QUERY > INTERACTIVE > BATCH
         - within priority: round-robin pipelines
         - memory-aware: only place if estimated RAM (buffered) fits in pool headroom
         - packing: prefer smaller estimated memory when multiple options exist
    """
    # ---------- helpers (local to keep policy self-contained) ----------
    def _prio_rank(prio):
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1  # Priority.BATCH_PIPELINE and anything else

    def _queue_for_prio(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg)

    def _ensure_pipeline_registered(p):
        pid = p.pipeline_id
        s.pipe[pid] = p
        if pid not in s.pstate:
            s._arrival_seq += 1
            s.pstate[pid] = {
                "ram_mult": 1.0,
                "dead": False,
                "arrival_seq": s._arrival_seq,
            }
            _queue_for_prio(p.priority).append(pid)
        else:
            # Pipeline may reappear as object reference; keep latest.
            pass

    def _pipeline_done_or_dead(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        ps = s.pstate.get(p.pipeline_id)
        if ps and ps.get("dead", False):
            return True
        # If the runtime marks any operator as FAILED, we treat it as failure
        # unless we explicitly decide to retry via results handling.
        # (We do not drop here; results handler drives OOM retry.)
        return False

    def _pick_assignable_op(p):
        st = p.runtime_status()
        # Only schedule ops whose parents are complete
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        # Schedule one op at a time per pipeline (reduces blast radius on mis-sizing)
        return ops[0]

    def _estimate_mem_gb(op, pool, p):
        # Estimator may be missing; treat missing as conservative-but-not-absurd.
        est = None
        try:
            est = op.estimate.mem_peak_gb
        except Exception:
            est = None

        if est is None:
            # Fall back: a fraction of pool max RAM; slightly smaller for BATCH to avoid blocking.
            base = max(s.mem_min_gb, pool.max_ram_pool * s.mem_fallback_frac)
            if p.priority == Priority.BATCH_PIPELINE:
                base = max(s.mem_min_gb, pool.max_ram_pool * (s.mem_fallback_frac * 0.8))
            return float(base)

        # Guard against weird estimator outputs
        try:
            est_f = float(est)
        except Exception:
            est_f = pool.max_ram_pool * s.mem_fallback_frac

        if est_f < 0:
            est_f = 0.0

        return max(s.mem_min_gb, est_f)

    def _ram_request_gb(op, pool, p):
        ps = s.pstate[p.pipeline_id]
        ram_mult = ps["ram_mult"]
        est_mem = _estimate_mem_gb(op, pool, p)
        req = est_mem * s.mem_safety * ram_mult
        # Clamp to pool max
        if req > pool.max_ram_pool:
            req = pool.max_ram_pool
        # Ensure minimum
        if req < s.mem_min_gb:
            req = s.mem_min_gb
        return float(req), float(est_mem)

    def _cpu_request(pool, p, avail_cpu):
        # Keep simple: prioritize latency for high-priority by giving them more CPU when available.
        # Also avoid consuming all CPU with one op when multiple can fit.
        maxc = float(pool.max_cpu_pool)
        av = float(avail_cpu)
        if av <= 0:
            return 0.0
        if p.priority == Priority.QUERY:
            target = min(av, max(1.0, maxc * 0.90))
        elif p.priority == Priority.INTERACTIVE:
            target = min(av, max(1.0, maxc * 0.75))
        else:
            target = min(av, max(1.0, maxc * 0.50))
        # Avoid too-tiny CPU allocations
        if target < 1.0:
            target = 1.0 if av >= 1.0 else av
        return float(target)

    def _clean_queue_inplace(q):
        # Remove finished/dead pipelines and pipelines no longer present.
        out = []
        seen = set()
        for pid in q:
            if pid in seen:
                continue
            seen.add(pid)
            p = s.pipe.get(pid)
            ps = s.pstate.get(pid)
            if p is None or ps is None:
                continue
            if _pipeline_done_or_dead(p):
                continue
            out.append(pid)
        q[:] = out

    # ---------- ingest new pipelines ----------
    for p in pipelines:
        _ensure_pipeline_registered(p)

    # ---------- process results (OOM retry + dead marking) ----------
    if results:
        for r in results:
            # Update pipeline ref if present (best effort)
            # (We may not always have the pipeline object here; it will arrive via WorkloadGenerator)
            # Handle failures
            if hasattr(r, "failed") and r.failed():
                # Try to find pipeline by id from r.ops if possible; otherwise by stored mapping.
                # r.ops is a list of ops; ops likely have pipeline_id? not guaranteed.
                pid = None
                try:
                    if r.ops and hasattr(r.ops[0], "pipeline_id"):
                        pid = r.ops[0].pipeline_id
                except Exception:
                    pid = None

                # If we cannot infer pid, we cannot adjust state; skip.
                if pid is None or pid not in s.pstate:
                    continue

                ps = s.pstate[pid]
                if _is_oom_error(getattr(r, "error", None)):
                    # Bump RAM multiplier and allow retry.
                    new_mult = ps["ram_mult"] * s.oom_backoff
                    if new_mult > s.ram_mult_cap:
                        new_mult = s.ram_mult_cap
                    ps["ram_mult"] = new_mult
                    # Move pipeline toward the front of its priority queue for faster recovery.
                    p = s.pipe.get(pid)
                    if p is not None:
                        q = _queue_for_prio(p.priority)
                        try:
                            q.remove(pid)
                        except Exception:
                            pass
                        q.insert(0, pid)
                else:
                    # Non-OOM failures are treated as terminal to avoid infinite retry loops.
                    ps["dead"] = True

    # Early exit if nothing changed and no new arrivals (but note: pool availability changes each tick;
    # still keep the early exit aligned with template expectations).
    if not pipelines and not results:
        return [], []

    # ---------- queue cleanup ----------
    _clean_queue_inplace(s.q_query)
    _clean_queue_inplace(s.q_interactive)
    _clean_queue_inplace(s.q_batch)

    suspensions = []
    assignments = []

    # ---------- scheduling across pools ----------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep assigning while there is capacity.
        while avail_cpu > 0 and avail_ram > 0:
            # Build candidate pipelines (top-K from each queue; strict priority still applied in scoring).
            cand_pids = []
            cand_pids.extend(s.q_query[: s.scan_depth])
            cand_pids.extend(s.q_interactive[: s.scan_depth])
            cand_pids.extend(s.q_batch[: s.scan_depth])

            best = None
            # best tuple: (prio_rank, -arrival_seq, -neg??) We'll maximize prio, then older first, then smaller mem.
            best_key = None

            for pid in cand_pids:
                p = s.pipe.get(pid)
                ps = s.pstate.get(pid)
                if p is None or ps is None or ps.get("dead", False):
                    continue
                if _pipeline_done_or_dead(p):
                    continue

                op = _pick_assignable_op(p)
                if op is None:
                    continue

                ram_req, est_mem = _ram_request_gb(op, pool, p)
                if ram_req > avail_ram:
                    continue

                cpu_req = _cpu_request(pool, p, avail_cpu)
                if cpu_req <= 0 or cpu_req > avail_cpu:
                    continue

                # Candidate score:
                #  - Higher priority first
                #  - Older arrival first (smaller arrival_seq should win)
                #  - Smaller estimated memory first (helps packing, reduces headroom fragmentation)
                key = (
                    _prio_rank(p.priority),
                    -ps["arrival_seq"],
                    -est_mem,  # note: we will invert again below by comparing properly
                )
                # We want: prio_rank DESC, arrival_seq ASC, est_mem ASC.
                # Using tuple with transforms:
                #   prio_rank DESC -> larger is better (ok)
                #   arrival_seq ASC -> use -arrival_seq (larger is better when arrival_seq smaller)
                #   est_mem ASC -> use -est_mem (larger is better when est_mem smaller)
                if best is None or key > best_key:
                    best = (p, op, cpu_req, ram_req)
                    best_key = key

            if best is None:
                break

            p, op, cpu_req, ram_req = best

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

            # Round-robin: move this pipeline to end of its queue.
            q = _queue_for_prio(p.priority)
            try:
                q.remove(p.pipeline_id)
            except Exception:
                pass
            q.append(p.pipeline_id)

            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)

    return suspensions, assignments
