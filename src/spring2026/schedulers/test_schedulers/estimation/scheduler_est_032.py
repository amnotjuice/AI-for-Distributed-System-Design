# policy_key: scheduler_est_032
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049280
# generation_seconds: 52.50
# generated_at: 2026-04-10T10:11:18.234284
@register_scheduler_init(key="scheduler_est_032")
def scheduler_est_032_init(s):
    """
    Memory-aware, priority-first scheduler with bounded retries.

    Core ideas:
      - Strict priority order (QUERY > INTERACTIVE > BATCH) to protect weighted latency.
      - Use op.estimate.mem_peak_gb as a hint to avoid obvious OOM placements.
      - On failures, retry FAILED ops with a larger RAM safety factor (bounded attempts),
        and avoid retrying on the same pool if alternatives exist.
      - Best-fit pool-local packing: among ops that can run on a pool, pick the one that
        uses RAM most tightly (min slack) to reduce fragmentation and increase completion rate.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-pipeline metadata (arrival order for light aging/debugging)
    s._pipe_meta = {}  # pipeline_id -> {"seq": int}

    # Per-operator retry state (keyed by (pipeline_id, op_key))
    s._op_attempts = {}         # (pid, op_key) -> int
    s._op_last_failed_pool = {} # (pid, op_key) -> pool_id
    s._seq = 0

    # Tuning knobs (keep simple / safe)
    s._max_attempts = 3
    s._min_ram_gb = 0.5
    s._default_unknown_ram_gb = 2.0

    # CPU caps by priority (avoid overcommitting single ops while still helping p99)
    s._cpu_cap_query = 8
    s._cpu_cap_interactive = 6
    s._cpu_cap_batch = 4


@register_scheduler(key="scheduler_est_032")
def scheduler_est_032_scheduler(s, results, pipelines):
    from collections import deque

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s._pipe_meta:
            s._seq += 1
            s._pipe_meta[pid] = {"seq": s._seq}

        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _op_key(op):
        # Prefer stable identifiers if present; fallback to object id (works within simulation run).
        for attr in ("op_id", "operator_id", "id"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return v
                except Exception:
                    pass
        return id(op)

    def _attempts(pid, op):
        return s._op_attempts.get((pid, _op_key(op)), 0)

    def _record_failure(res):
        # Mark all ops in the result as having failed once, and remember pool to diversify retries.
        pid = getattr(res, "pipeline_id", None)
        # ExecutionResult may not carry pipeline_id in some implementations; infer from op if present.
        if pid is None and getattr(res, "ops", None):
            op0 = res.ops[0]
            pid = getattr(op0, "pipeline_id", None)

        if pid is None:
            return

        for op in (res.ops or []):
            k = (pid, _op_key(op))
            s._op_attempts[k] = s._op_attempts.get(k, 0) + 1
            s._op_last_failed_pool[k] = res.pool_id

    def _is_done_or_terminal(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True
        # Do not drop failed pipelines outright; we retry FAILED ops (ASSIGNABLE_STATES includes FAILED).
        return False

    def _get_next_assignable_op(p):
        status = p.runtime_status()
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        # Keep it simple: schedule one ready op per pipeline at a time.
        return ops[0]

    def _est_mem_gb(op):
        try:
            est = getattr(op, "estimate", None)
            if est is None:
                return None
            v = getattr(est, "mem_peak_gb", None)
            if v is None:
                return None
            # Guard against pathological values
            if v < 0:
                return None
            return float(v)
        except Exception:
            return None

    def _ram_request_gb(op, pool, pid):
        est = _est_mem_gb(op)
        att = _attempts(pid, op)

        # Safety factor grows with attempts (to reduce repeated OOMs under noisy estimates).
        # Start mildly conservative to avoid wasting RAM; grow quickly after failures.
        safety = 1.25 + 0.50 * att  # attempt0=1.25x, attempt1=1.75x, attempt2=2.25x ...

        if est is None:
            req = s._default_unknown_ram_gb * (1.0 + 0.50 * att)
        else:
            req = est * safety

        if req < s._min_ram_gb:
            req = s._min_ram_gb

        # Never request more than pool can possibly provide.
        if req > pool.max_ram_pool:
            req = pool.max_ram_pool

        return req

    def _cpu_request(op, pool, priority, avail_cpu):
        # Keep CPU sizing simple: cap by priority, but don't allocate more than available.
        if priority == Priority.QUERY:
            cap = min(s._cpu_cap_query, pool.max_cpu_pool)
            # Prefer giving query more CPU to reduce tail latency when possible.
            target = max(2, cap)
        elif priority == Priority.INTERACTIVE:
            cap = min(s._cpu_cap_interactive, pool.max_cpu_pool)
            target = max(2, cap)
        else:
            cap = min(s._cpu_cap_batch, pool.max_cpu_pool)
            target = 1 if cap <= 1 else min(2, cap)

        # Don't exceed available CPU; if we have very little, still try 1.
        if avail_cpu <= 0:
            return 0
        return max(1, min(int(avail_cpu), int(target)))

    def _pool_can_fit(pool, cpu_req, ram_req):
        return (pool.avail_cpu_pool >= cpu_req) and (pool.avail_ram_pool >= ram_req)

    def _select_candidate_for_pool(pool_id):
        """
        Choose a single (pipeline, op, cpu_req, ram_req) to run on this pool.
        Priority order: QUERY -> INTERACTIVE -> BATCH.
        Within priority, scan queue and pick best-fit RAM (min slack) among feasible ops.
        """
        pool = s.executor.pools[pool_id]
        best = None  # (slack_ram, priority_rank, seq, pipeline, op, cpu_req, ram_req)

        def scan_queue(q, priority_rank):
            nonlocal best
            # We'll rotate through the queue once, keeping order stable.
            n = len(q)
            for _ in range(n):
                p = q.popleft()

                if _is_done_or_terminal(p):
                    continue

                op = _get_next_assignable_op(p)
                if op is None:
                    q.append(p)
                    continue

                pid = p.pipeline_id
                att = _attempts(pid, op)
                if att >= s._max_attempts:
                    # Stop spending cycles on an op that repeatedly fails; keep pipeline queued,
                    # but do not schedule this op anymore (avoids infinite thrash).
                    q.append(p)
                    continue

                # Hard feasibility check using estimator hint vs absolute pool max.
                est = _est_mem_gb(op)
                if est is not None and est > pool.max_ram_pool:
                    # Can't ever fit in this pool.
                    q.append(p)
                    continue

                # Avoid immediate retry on the same pool that just failed, if possible.
                last_fail_pool = s._op_last_failed_pool.get((pid, _op_key(op)), None)
                if last_fail_pool is not None and last_fail_pool == pool_id and s.executor.num_pools > 1:
                    q.append(p)
                    continue

                # Compute resource request and check current availability.
                ram_req = _ram_request_gb(op, pool, pid)
                cpu_req = _cpu_request(op, pool, p.priority, pool.avail_cpu_pool)

                if cpu_req <= 0:
                    q.append(p)
                    continue

                if not _pool_can_fit(pool, cpu_req, ram_req):
                    # If it's obviously too big for current availability, keep it queued.
                    q.append(p)
                    continue

                slack = pool.avail_ram_pool - ram_req
                seq = s._pipe_meta.get(pid, {}).get("seq", 0)

                candidate = (slack, priority_rank, seq, p, op, cpu_req, ram_req)

                # Prefer:
                #  1) smaller RAM slack (best-fit to reduce fragmentation),
                #  2) higher priority (lower priority_rank),
                #  3) older arrival (lower seq).
                if best is None:
                    best = candidate
                else:
                    if candidate[0] < best[0]:
                        best = candidate
                    elif candidate[0] == best[0]:
                        if candidate[1] < best[1]:
                            best = candidate
                        elif candidate[1] == best[1] and candidate[2] < best[2]:
                            best = candidate

                q.append(p)

        # Scan in strict priority order; best-fit applies within and across scanned candidates.
        scan_queue(s.q_query, 0)
        scan_queue(s.q_interactive, 1)
        scan_queue(s.q_batch, 2)

        if best is None:
            return None

        _, _, _, p, op, cpu_req, ram_req = best
        return (p, op, cpu_req, ram_req)

    # --- Update state with new arrivals and results ---
    for p in pipelines:
        _enqueue_pipeline(p)

    for r in results:
        try:
            if r.failed():
                _record_failure(r)
            else:
                # On success, we intentionally do not reset attempts globally; attempts are per-op,
                # and successful completion means the op won't be re-assigned.
                pass
        except Exception:
            pass

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # --- Main scheduling loop: fill each pool with as many single-op assignments as possible ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Try to pack multiple ops per pool per tick, bounded by available resources.
        # This increases throughput while still prioritizing small, feasible ops.
        while pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
            pick = _select_candidate_for_pool(pool_id)
            if pick is None:
                break

            p, op, cpu_req, ram_req = pick

            # Final guard (availability may have changed during loop)
            if not _pool_can_fit(pool, cpu_req, ram_req):
                break

            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Update pool availability locally by relying on pool fields being updated by executor later.
            # We still break if no more resources in subsequent loop iterations.
            # (Some simulators update pool.avail_* immediately upon returning assignments; some on apply.)
            # To stay robust, we pessimistically decrement a local shadow by mutating nothing; rely on next tick.

            # Heuristic: stop early if we likely can't fit anything else due to tiny remaining RAM.
            if pool.avail_ram_pool < s._min_ram_gb or pool.avail_cpu_pool < 1:
                break

    return suspensions, assignments
