# policy_key: scheduler_est_022
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042994
# generation_seconds: 48.39
# generated_at: 2026-04-10T10:02:08.594748
@register_scheduler_init(key="scheduler_est_022")
def scheduler_est_022_init(s):
    """Priority- and memory-aware scheduler with simple OOM-avoidance retries.

    Main ideas:
      - Always prefer QUERY > INTERACTIVE > BATCH when choosing the next runnable operator.
      - Use op.estimate.mem_peak_gb (noisy hint) to (a) pick a feasible pool and (b) size RAM with a safety margin.
      - On operator failures (likely OOM), increase a per-operator RAM multiplier and retry instead of dropping.
      - Avoid starvation by aging batch/interactive/query pipelines slightly over time.
      - Pack pools with multiple small containers rather than giving an entire pool to one op.
    """
    s.waiting: list = []  # pipelines in system (unordered; we pick by policy each tick)
    s.first_seen_tick = {}  # pipeline_id -> tick first observed
    s.tick = 0

    # Per-operator adaptive RAM multiplier (bump on failure)
    # key uses id(op) to avoid relying on a specific operator-id field.
    s.op_ram_mult = {}  # id(op) -> float

    # Small cache to avoid re-checking finished pipelines too much; best-effort cleanup.
    s._done_pipeline_ids = set()


@register_scheduler(key="scheduler_est_022")
def scheduler_est_022_scheduler(s, results, pipelines):
    """
    Scheduler step: decide suspensions (unused here) and new assignments.

    Notes:
      - We do not preempt in this version to avoid churn/wasted work.
      - We rely on retries via FAILED -> ASSIGNABLE and a growing RAM multiplier.
    """
    # ---------- helpers (kept inside for portability) ----------
    def _prio_rank(prio):
        # Smaller is better.
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE (or anything else)

    def _prio_weight(prio):
        if prio == Priority.QUERY:
            return 10.0
        if prio == Priority.INTERACTIVE:
            return 5.0
        return 1.0

    def _is_pipeline_terminal(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If any operator is FAILED, we still want to retry (ASSIGNABLE_STATES includes FAILED),
        # so don't treat as terminal here.
        return False

    def _get_assignable_ops(p, limit=4):
        status = p.runtime_status()
        # Only schedule ops whose parents are complete to avoid wasted admissions.
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        # Limit fan-out per pipeline so we don't over-parallelize one pipeline at the expense of others.
        return ops[:limit]

    def _est_mem_gb(op):
        est = None
        try:
            est = op.estimate.mem_peak_gb
        except Exception:
            est = None
        if est is None:
            return None
        # Guard against weird values
        try:
            if est < 0:
                return None
        except Exception:
            return None
        return float(est)

    def _op_ram_multiplier(op):
        return float(s.op_ram_mult.get(id(op), 1.0))

    def _bump_op_ram_multiplier(op):
        # Exponential backoff-ish, capped.
        k = id(op)
        cur = float(s.op_ram_mult.get(k, 1.0))
        nxt = cur * 1.5
        if nxt > 6.0:
            nxt = 6.0
        s.op_ram_mult[k] = nxt

    def _age_bonus(p):
        # Mild aging to prevent starvation without overriding strict priorities.
        # Age is in ticks; we assume scheduler is called frequently.
        first = s.first_seen_tick.get(p.pipeline_id, s.tick)
        age = max(0, s.tick - first)
        # Convert to a small score bonus: older pipelines get slightly better (lower) score.
        # Cap so we don't invert priorities.
        return min(0.25, age / 2000.0)

    def _candidate_score(p):
        # Lower is better: priority dominates, then age.
        return _prio_rank(p.priority) - _age_bonus(p)

    def _ram_request_for_pool(op, pool):
        """
        Compute a conservative RAM request for this op if placed in this pool.
        Uses estimator hint + safety margin + adaptive multiplier.
        """
        est = _est_mem_gb(op)
        mult = _op_ram_multiplier(op)

        # Base estimate: if missing, default to a conservative fraction of pool size.
        if est is None:
            base = 0.30 * float(pool.max_ram_pool)
        else:
            # Safety margin for noise; plus multiplier from failures.
            base = est * 1.20

        req = base * mult

        # Hard floors/ceilings to keep requests sensible.
        # Floor prevents tiny allocations that often OOM when estimator is absent.
        floor = min(1.0, 0.10 * float(pool.max_ram_pool))
        if req < floor:
            req = floor

        # Don't request more than the pool's maximum; if estimator says larger, it's likely infeasible.
        if req > float(pool.max_ram_pool):
            req = float(pool.max_ram_pool)

        return float(req)

    def _cpu_request(prio, pool, avail_cpu):
        """
        Keep some parallelism by capping per-container CPU.
        Queries get more CPU than batch to reduce latency.
        """
        # Per-container caps by priority.
        if prio == Priority.QUERY:
            cap = 4.0
        elif prio == Priority.INTERACTIVE:
            cap = 3.0
        else:
            cap = 2.0

        # Ensure at least 1 vCPU if any is available.
        req = min(float(avail_cpu), cap)
        if req < 1.0 and float(avail_cpu) >= 1.0:
            req = 1.0
        return float(req)

    def _fits(pool, cpu_req, ram_req):
        # Keep a tiny RAM headroom to reduce fragmentation/OOM cascades.
        ram_headroom = 0.25  # GB
        return (pool.avail_cpu_pool >= cpu_req) and (pool.avail_ram_pool >= (ram_req + ram_headroom))

    # ---------- ingest new pipelines ----------
    s.tick += 1

    for p in pipelines:
        if p.pipeline_id in s._done_pipeline_ids:
            continue
        s.waiting.append(p)
        if p.pipeline_id not in s.first_seen_tick:
            s.first_seen_tick[p.pipeline_id] = s.tick

    # ---------- learn from results (bump RAM on failures) ----------
    # We treat any failure as a reason to be more conservative on memory for the involved ops.
    for r in results:
        try:
            if r.failed():
                for op in getattr(r, "ops", []) or []:
                    _bump_op_ram_multiplier(op)
        except Exception:
            # If result structure is unexpected, ignore to keep scheduler robust.
            pass

    # Early exit when nothing changed (but still do light cleanup if needed).
    if not pipelines and not results and not s.waiting:
        return [], []

    # ---------- cleanup terminal pipelines ----------
    # Keep list small; avoid holding onto completed pipelines.
    new_waiting = []
    for p in s.waiting:
        if p.pipeline_id in s._done_pipeline_ids:
            continue
        if _is_pipeline_terminal(p):
            s._done_pipeline_ids.add(p.pipeline_id)
            continue
        new_waiting.append(p)
    s.waiting = new_waiting

    suspensions = []
    assignments = []

    # ---------- build pool-local scheduling decisions ----------
    # Strategy:
    #   For each pool, repeatedly select the "best" runnable op among pipelines that can fit,
    #   prioritizing QUERY/INTERACTIVE and smaller estimated memory to avoid OOM and improve packing.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Quick skip if no resources.
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Try to pack multiple assignments into the pool within this tick.
        # Bound iterations to avoid long scheduler runtimes.
        max_iters = 64
        iters = 0

        while iters < max_iters:
            iters += 1

            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

            # Consider pipelines by priority/aging.
            # We also limit to a prefix to keep runtime reasonable under many pipelines.
            candidates = sorted(s.waiting, key=_candidate_score)
            if len(candidates) > 64:
                candidates = candidates[:64]

            best = None  # (score_tuple, pipeline, op, cpu_req, ram_req)
            for p in candidates:
                # Get a small set of assignable ops from this pipeline.
                ops = _get_assignable_ops(p, limit=4)
                if not ops:
                    continue

                # Among this pipeline's ops, prefer smaller estimated memory (better packing, fewer OOMs).
                # If estimator missing, treat as "large-ish" so we don't crowd out known-small ops.
                def _op_sort_key(op):
                    em = _est_mem_gb(op)
                    if em is None:
                        em = 1e9
                    return (em, id(op))

                ops = sorted(ops, key=_op_sort_key)

                for op in ops:
                    ram_req = _ram_request_for_pool(op, pool)
                    cpu_req = _cpu_request(p.priority, pool, pool.avail_cpu_pool)

                    if cpu_req <= 0 or ram_req <= 0:
                        continue

                    if not _fits(pool, cpu_req, ram_req):
                        continue

                    # Composite score:
                    #   - priority/age first
                    #   - smaller RAM next (packing and lower OOM risk)
                    #   - slightly prefer smaller CPU (allows more concurrency) for batch
                    pr_score = _candidate_score(p)
                    estm = _est_mem_gb(op)
                    if estm is None:
                        estm = ram_req  # fallback
                    cpu_pen = cpu_req * (0.02 if p.priority == Priority.BATCH_PIPELINE else 0.005)
                    score_tuple = (pr_score, float(estm), float(ram_req), cpu_pen)

                    if best is None or score_tuple < best[0]:
                        best = (score_tuple, p, op, cpu_req, ram_req)

                    # If we found a query op that fits, we can break earlier to reduce scan cost.
                    if p.priority == Priority.QUERY and best is not None:
                        break
                if best is not None and best[1].priority == Priority.QUERY:
                    break

            if best is None:
                break

            _, p, op, cpu_req, ram_req = best

            # Final feasibility check (pool resources may have changed).
            if not _fits(pool, cpu_req, ram_req):
                break

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

            # Optimistically decrement pool resources for additional packing decisions in this tick.
            # (Simulator will enforce true accounting; this helps us avoid over-assigning.)
            try:
                pool.avail_cpu_pool -= cpu_req
                pool.avail_ram_pool -= ram_req
            except Exception:
                # If pool objects are immutable, stop packing to avoid over-assigning.
                break

    return suspensions, assignments
