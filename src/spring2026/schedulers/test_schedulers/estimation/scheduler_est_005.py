# policy_key: scheduler_est_005
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.056756
# generation_seconds: 48.30
# generated_at: 2026-04-10T07:50:01.131330
@register_scheduler_init(key="scheduler_est_005")
def scheduler_est_005_init(s):
    """Memory-aware, priority-weighted scheduler with conservative OOM recovery.

    Core ideas:
    - Always prefer QUERY > INTERACTIVE > BATCH to minimize the weighted objective.
    - Use op.estimate.mem_peak_gb (noisy hint) to avoid clearly infeasible placements and reduce OOMs.
    - On failure, assume possible OOM and retry the same operator with increased RAM "multiplier"
      (bounded by pool max RAM). Avoid dropping pipelines to prevent 720s penalties.
    - Soft-reserve some headroom for high-priority work: avoid filling a pool with BATCH if
      QUERY/INTERACTIVE are waiting and pool free RAM is getting low.
    """
    s.tick = 0

    # Priority queues of pipelines (keep order roughly FIFO within priority).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-pipeline tracking (lightweight).
    # meta[pipeline_id] = {"enqueued_tick": int, "last_seen_tick": int}
    s.meta = {}

    # Per-operator retry RAM multiplier (keyed by Python object id of the operator).
    # ram_mult[id(op)] = float >= 1.0
    s.ram_mult = {}

    # Track recent failures per operator to apply faster ramp-up.
    # fail_count[id(op)] = int
    s.fail_count = {}


@register_scheduler(key="scheduler_est_005")
def scheduler_est_005_scheduler(s, results, pipelines):
    def _enqueue(p):
        pid = p.pipeline_id
        if pid not in s.meta:
            s.meta[pid] = {"enqueued_tick": s.tick, "last_seen_tick": s.tick}
        else:
            s.meta[pid]["last_seen_tick"] = s.tick

        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _is_done_or_terminal(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _get_assignable_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        # Choose one op at a time (atomic scheduling unit).
        return ops[0]

    def _base_weight(priority):
        if priority == Priority.QUERY:
            return 100
        if priority == Priority.INTERACTIVE:
            return 50
        return 10

    def _oom_ramp(mult, fc):
        # Conservative but responsive ramp-up: grows with failures.
        # 1st fail: 1.6x, 2nd: ~2.2x, 3rd+: faster.
        if fc <= 0:
            return mult
        if fc == 1:
            return max(mult, 1.6)
        if fc == 2:
            return max(mult, 2.2)
        return max(mult, 2.2 * (1.35 ** (fc - 2)))

    def _est_mem_gb(op):
        est = getattr(op, "estimate", None)
        if est is None:
            return None
        v = getattr(est, "mem_peak_gb", None)
        if v is None:
            return None
        try:
            if v < 0:
                return None
        except Exception:
            return None
        return v

    def _required_ram(op, pool):
        # Use estimate as a hint, with safety factor and per-op retry multiplier.
        est = _est_mem_gb(op)
        mult = s.ram_mult.get(id(op), 1.0)
        # Safety factor: modest to reduce OOMs without over-allocating too aggressively.
        safety = 1.25

        if est is None:
            # Unknown: allocate a conservative slice, but bounded.
            req = max(1.0, 0.15 * pool.max_ram_pool)
        else:
            req = max(0.5, est * safety)

        req *= mult

        # Bound by pool maximum; if it can't fit even at max, caller will filter.
        req = min(req, pool.max_ram_pool)
        return req

    def _cpu_share(priority, pool, avail_cpu):
        # Favor latency for QUERY/INTERACTIVE: give them larger CPU shares.
        # Keep a lower bound of 1 vCPU when possible.
        maxc = pool.max_cpu_pool
        if priority == Priority.QUERY:
            cap = max(1.0, 0.80 * maxc)
        elif priority == Priority.INTERACTIVE:
            cap = max(1.0, 0.60 * maxc)
        else:
            cap = max(1.0, 0.40 * maxc)
        return min(avail_cpu, cap)

    def _has_high_pri_waiting():
        return len(s.q_query) > 0 or len(s.q_interactive) > 0

    def _soft_reserve_ram_for_high_pri(pool):
        # If high-priority work exists, avoid consuming the last chunk of RAM with batch.
        # This is a soft rule only applied when choosing batch candidates.
        if not _has_high_pri_waiting():
            return 0.0
        # Reserve 20% of pool RAM (bounded) to reduce head-of-line blocking for high pri.
        return min(0.20 * pool.max_ram_pool, 8.0)

    def _pick_next_pipeline_candidate():
        # Weighted priority + light aging to avoid indefinite starvation.
        # We keep FIFO-ish ordering but allow occasional batch progress.
        # Aging is based on ticks since enqueue.
        best = None
        best_score = None
        best_qname = None
        best_idx = None

        def consider(queue, qname):
            nonlocal best, best_score, best_qname, best_idx
            for i, p in enumerate(queue):
                if _is_done_or_terminal(p):
                    continue
                meta = s.meta.get(p.pipeline_id, None)
                age = 0
                if meta is not None:
                    age = max(0, s.tick - meta["enqueued_tick"])
                # Aging: small but enough that batch eventually bubbles up.
                # Query/interactive still dominate strongly.
                score = _base_weight(p.priority) + min(30, age // 5)
                if best_score is None or score > best_score:
                    best = p
                    best_score = score
                    best_qname = qname
                    best_idx = i

        consider(s.q_query, "q_query")
        consider(s.q_interactive, "q_interactive")
        consider(s.q_batch, "q_batch")

        if best is None:
            return None, None, None

        # Remove from its queue now; if not scheduled, we'll requeue to the back.
        if best_qname == "q_query":
            s.q_query.pop(best_idx)
        elif best_qname == "q_interactive":
            s.q_interactive.pop(best_idx)
        else:
            s.q_batch.pop(best_idx)

        return best, best_qname, best_score

    def _requeue(p):
        # Put back to the end (fairness).
        _enqueue(p)

    # ----------------------------
    # Ingest new pipelines
    # ----------------------------
    for p in pipelines:
        _enqueue(p)

    s.tick += 1

    # Process results: update RAM multiplier on failure to avoid repeated OOMs.
    # We assume failures are likely OOM (in this simulator context) and recover by bumping RAM.
    for r in results:
        if not r.failed():
            continue
        for op in getattr(r, "ops", []) or []:
            oid = id(op)
            s.fail_count[oid] = s.fail_count.get(oid, 0) + 1
            cur = s.ram_mult.get(oid, 1.0)
            bumped = _oom_ramp(cur, s.fail_count[oid])
            # Also incorporate the RAM used in the failed run as a floor (try larger next time).
            try:
                if getattr(r, "ram", None) is not None:
                    # If we failed at r.ram, push multiplier slightly above that footprint.
                    # (Actual req calculation will cap at pool max.)
                    bumped = max(bumped, 1.10 * (float(r.ram) / max(0.5, float(_est_mem_gb(op) or 0.5))))
            except Exception:
                pass
            s.ram_mult[oid] = min(16.0, max(1.0, bumped))

    # Early exit if nothing to do (keeps simulator fast).
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ----------------------------
    # Scheduling loop: fill pools with one-op assignments while resources remain.
    # ----------------------------
    # Simple multi-pool strategy:
    # - For each pool, repeatedly pick the globally best candidate pipeline/op that can fit in that pool.
    # - Memory-aware filtering: skip if estimated required RAM cannot fit in this pool's available RAM.
    # - Batch soft-reservation when high priority is waiting.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        while True:
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Look for a candidate that can run in this pool now.
            chosen = None
            chosen_op = None
            chosen_req_ram = None
            chosen_cpu = None
            chosen_priority = None

            # We'll search a bounded number of candidates to avoid O(n^2) scans on large queues.
            # Try up to 24 attempts per placement; if not found, give up for this pool this tick.
            attempts = 0
            deferred = []

            while attempts < 24:
                attempts += 1
                p, _, _ = _pick_next_pipeline_candidate()
                if p is None:
                    break

                # Drop completed pipelines silently.
                if _is_done_or_terminal(p):
                    continue

                op = _get_assignable_op(p)
                if op is None:
                    # Nothing assignable yet; keep it around.
                    deferred.append(p)
                    continue

                # Soft reserve: if this is batch and we have high-priority waiting, don't eat the last RAM.
                reserve = _soft_reserve_ram_for_high_pri(pool)
                if p.priority == Priority.BATCH_PIPELINE and reserve > 0 and avail_ram <= reserve:
                    deferred.append(p)
                    continue

                req_ram = _required_ram(op, pool)

                # Hard filter: must fit available and pool max (req_ram already capped to max).
                if req_ram > avail_ram:
                    deferred.append(p)
                    continue

                # Also filter obviously impossible estimates: if estimate exceeds pool max, defer.
                est = _est_mem_gb(op)
                if est is not None and est > pool.max_ram_pool:
                    # Can't ever fit in this pool; defer and hope another pool can take it.
                    deferred.append(p)
                    continue

                cpu = _cpu_share(p.priority, pool, avail_cpu)
                if cpu <= 0:
                    deferred.append(p)
                    continue

                chosen = p
                chosen_op = op
                chosen_req_ram = req_ram
                chosen_cpu = cpu
                chosen_priority = p.priority
                break

            # Requeue deferred candidates.
            for dp in deferred:
                _requeue(dp)

            if chosen is None:
                break

            # Create the assignment.
            # Allocate exactly chosen_req_ram (not all RAM) to allow concurrency in the pool.
            assignment = Assignment(
                ops=[chosen_op],
                cpu=chosen_cpu,
                ram=chosen_req_ram,
                priority=chosen_priority,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
            assignments.append(assignment)

            # Requeue the pipeline for its subsequent operators (or retry of failed ones if needed).
            _requeue(chosen)

            # Continue loop: pool.avail_* will be updated by the simulator after this tick;
            # but within this scheduler call, we only have the current snapshot.
            # To avoid over-committing in one tick, stop after a single assignment per pool.
            # This is a conservative choice that reduces contention and OOM cascades.
            break

    return suspensions, assignments
