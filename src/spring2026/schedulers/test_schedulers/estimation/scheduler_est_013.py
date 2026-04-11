# policy_key: scheduler_est_013
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048874
# generation_seconds: 56.96
# generated_at: 2026-04-10T07:57:11.331598
@register_scheduler_init(key="scheduler_est_013")
def scheduler_est_013_init(s):
    """Priority- and memory-aware scheduler with OOM-aware retries.

    Main ideas:
      - Maintain per-priority queues (QUERY > INTERACTIVE > BATCH).
      - Within a priority class, prefer assignable ops with smaller estimated peak memory.
      - Use op.estimate.mem_peak_gb as a placement filter to reduce OOM failures.
      - On OOM-like failures, retry the same op with a higher RAM multiplier (capped).
      - Avoid dropping pipelines on first failure; completion rate is heavily weighted.
    """
    # Queues store pipeline_ids; pipelines stored in dict for stable references.
    s.pipelines_by_id = {}  # pipeline_id -> Pipeline
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-(pipeline, op) retry and sizing state.
    # Key uses (pipeline_id, id(op)) to avoid relying on missing op ids.
    s.op_ram_mult = {}       # (pipeline_id, id(op)) -> float
    s.op_fail_count = {}     # (pipeline_id, id(op)) -> int
    s.op_last_fail_tick = {} # (pipeline_id, id(op)) -> int

    # Per-pipeline aging to prevent starvation.
    s.pipeline_age = {}      # pipeline_id -> int (ticks in queue without being scheduled)

    # Soft "clock" (ticks). We only need ordering/aging; simulator is deterministic.
    s.tick = 0

    # Parameters (conservative defaults; tuned for high completion and low tail latency).
    s.max_retries_oom = 4          # allow multiple OOM retries (RAM doubling)
    s.max_ram_mult = 8.0           # cap memory multiplier
    s.base_headroom = 1.15         # allocate a bit above the estimate to reduce OOM risk
    s.min_ram_gb = 0.25            # avoid too-tiny containers
    s.scan_limit_per_pool = 24     # limit queue scanning per pool tick
    s.force_place_after_age = 40   # after this many ticks, try harder even if estimate says "too big"


@register_scheduler(key="scheduler_est_013")
def scheduler_est_013(s, results: List["ExecutionResult"],
                      pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Scheduler step:
      1) Enqueue new pipelines.
      2) Process execution results to adjust retry state (OOM -> bump RAM multiplier).
      3) For each pool, repeatedly pick the best next operator that fits and assign it.
    """
    s.tick += 1

    # -----------------------
    # Helper functions
    # -----------------------
    def _prio_weight(prio):
        if prio == Priority.QUERY:
            return 10
        if prio == Priority.INTERACTIVE:
            return 5
        return 1

    def _is_oom_error(err) -> bool:
        # Best-effort classification; simulator error strings may vary.
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)

    def _op_est_mem(op):
        est = getattr(op, "estimate", None)
        mem = None
        if est is not None:
            mem = getattr(est, "mem_peak_gb", None)
        # Normalize bad values
        if mem is None:
            return None
        try:
            mem = float(mem)
        except Exception:
            return None
        if mem < 0:
            return None
        return mem

    def _key(pipeline_id, op):
        return (pipeline_id, id(op))

    def _get_ram_mult(pipeline_id, op):
        return s.op_ram_mult.get(_key(pipeline_id, op), 1.0)

    def _set_ram_mult(pipeline_id, op, mult):
        s.op_ram_mult[_key(pipeline_id, op)] = max(1.0, min(float(mult), s.max_ram_mult))

    def _inc_fail(pipeline_id, op):
        k = _key(pipeline_id, op)
        s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1
        s.op_last_fail_tick[k] = s.tick

    def _fail_count(pipeline_id, op):
        return s.op_fail_count.get(_key(pipeline_id, op), 0)

    def _pipeline_done_or_hard_failed(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return True

        # Do not "hard drop" pipelines just because an op is FAILED; we may retry on OOM.
        # But if there are FAILED ops and they exceeded retry budget, we consider it hard-failed.
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
        for op in failed_ops:
            if _fail_count(pipeline.pipeline_id, op) > s.max_retries_oom:
                return True
        return False

    def _pick_best_assignable_op(pipeline):
        """Return a single best assignable op for this pipeline, or None."""
        status = pipeline.runtime_status()

        # Only schedule ops whose parents are complete; keep it simple and safe.
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None

        # If there are FAILED ops, prefer retrying them first to improve completion rate.
        failed_ops = [op for op in ops if getattr(op, "state", None) == OperatorState.FAILED]
        candidate_ops = failed_ops if failed_ops else ops

        # Prefer smaller memory ops (estimate), tie-break by fewer failures.
        def _score(op):
            est_mem = _op_est_mem(op)
            mem_score = est_mem if est_mem is not None else 1e18
            return (mem_score, _fail_count(pipeline.pipeline_id, op))

        candidate_ops.sort(key=_score)
        return candidate_ops[0]

    def _ram_request_gb(pipeline, op, pool, avail_ram):
        """Compute RAM request based on estimate and retry state, bounded by pool/availability."""
        prio = pipeline.priority
        est_mem = _op_est_mem(op)
        mult = _get_ram_mult(pipeline.pipeline_id, op)

        # Priority-sensitive fallback when estimate missing:
        # high priorities get more conservative (larger) RAM to avoid failures.
        if est_mem is None:
            if prio == Priority.QUERY:
                base = max(s.min_ram_gb, 0.45 * pool.max_ram_pool)
            elif prio == Priority.INTERACTIVE:
                base = max(s.min_ram_gb, 0.35 * pool.max_ram_pool)
            else:
                base = max(s.min_ram_gb, 0.25 * pool.max_ram_pool)
            req = base * mult
        else:
            req = max(s.min_ram_gb, est_mem * s.base_headroom * mult)

        # Never request more than pool max; also don't exceed available.
        req = min(req, pool.max_ram_pool, avail_ram)
        return req

    def _cpu_request(pipeline, pool, avail_cpu):
        """Give more CPU to higher-priority work to reduce latency."""
        prio = pipeline.priority
        if prio == Priority.QUERY:
            target = 0.75 * pool.max_cpu_pool
        elif prio == Priority.INTERACTIVE:
            target = 0.55 * pool.max_cpu_pool
        else:
            target = 0.35 * pool.max_cpu_pool

        # Bound by availability; don't request 0 if there is CPU.
        cpu = min(avail_cpu, max(1e-6, target))
        return cpu

    def _can_fit_by_estimate(pipeline, op, pool, avail_ram):
        """Use estimate as a hint to filter placements; allow overrides when pipeline is old."""
        est_mem = _op_est_mem(op)
        if est_mem is None:
            return True

        mult = _get_ram_mult(pipeline.pipeline_id, op)
        # What we'd *like* to allocate (before availability cap).
        desired = max(s.min_ram_gb, est_mem * s.base_headroom * mult)

        # If it fits in available RAM, it's fine.
        if desired <= avail_ram:
            return True

        # If pipeline has waited long, allow trying "best effort" on this pool
        # (allocate what we can) to reduce incomplete penalties.
        age = s.pipeline_age.get(pipeline.pipeline_id, 0)
        if age >= s.force_place_after_age and pool.max_ram_pool >= est_mem:
            return True

        # Otherwise, likely to OOM or thrash; skip.
        return False

    # -----------------------
    # Enqueue new pipelines
    # -----------------------
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        s.pipeline_age.setdefault(p.pipeline_id, 0)
        # Insert into correct priority queue (FIFO within priority).
        s.queues[p.priority].append(p.pipeline_id)

    # -----------------------
    # Process results: bump RAM on OOM, track failures
    # -----------------------
    for r in results:
        if not r.failed():
            continue
        # Results may include multiple ops; bump each.
        for op in getattr(r, "ops", []) or []:
            pid = getattr(r, "pipeline_id", None)
            # If pipeline_id isn't on result, fall back to op's pipeline if available; else skip.
            if pid is None:
                pid = getattr(getattr(op, "pipeline_id", None), "pipeline_id", None)
            # In this simulator, we reliably get pipeline_id at assignment time; but be defensive.
            if pid is None:
                continue

            _inc_fail(pid, op)

            if _is_oom_error(getattr(r, "error", None)):
                # Double RAM multiplier on OOM-like failures.
                cur = s.op_ram_mult.get(_key(pid, op), 1.0)
                _set_ram_mult(pid, op, cur * 2.0)

                # Make sure pipeline is still in queue for retry.
                pipeline = s.pipelines_by_id.get(pid, None)
                if pipeline is not None and pid not in s.queues[pipeline.priority]:
                    s.queues[pipeline.priority].append(pid)

    # If no state changes that affect decisions, we can early exit.
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # -----------------------
    # Assignment loop per pool
    # -----------------------
    # Promote aged pipelines within each priority by moving them earlier occasionally.
    # This prevents indefinite starvation without adding complex fairness math.
    for prio, q in s.queues.items():
        # Age increments for all queued pipelines.
        for pid in q:
            s.pipeline_age[pid] = s.pipeline_age.get(pid, 0) + 1
        # Light aging-based bubble-up: bring the oldest to front.
        if len(q) > 1:
            oldest_idx = 0
            oldest_age = -1
            for i in range(min(len(q), 32)):  # only scan a prefix for speed
                age = s.pipeline_age.get(q[i], 0)
                if age > oldest_age:
                    oldest_age = age
                    oldest_idx = i
            if oldest_idx != 0:
                q[0], q[oldest_idx] = q[oldest_idx], q[0]

    # Iterate pools; for each pool, keep assigning while there's capacity.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Track available resources in this pool for this tick as we assign.
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Assign multiple ops per pool if possible.
        while avail_cpu > 0 and avail_ram > 0:
            chosen = None  # (pipeline, op)
            chosen_prio = None

            # Search priority queues in order; scan a limited number of candidates.
            for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                q = s.queues[prio]
                if not q:
                    continue

                best_local = None  # (pipeline, op, est_mem, fail_count, age)
                scanned = 0
                # We may rotate through the queue to find something that fits.
                for idx in range(min(len(q), s.scan_limit_per_pool)):
                    pid = q[idx]
                    pipeline = s.pipelines_by_id.get(pid, None)
                    if pipeline is None:
                        continue

                    # Drop from scheduling consideration if completed or exceeded retry budget.
                    if _pipeline_done_or_hard_failed(pipeline):
                        continue

                    op = _pick_best_assignable_op(pipeline)
                    if op is None:
                        continue

                    if not _can_fit_by_estimate(pipeline, op, pool, avail_ram):
                        scanned += 1
                        continue

                    est_mem = _op_est_mem(op)
                    fc = _fail_count(pipeline.pipeline_id, op)
                    age = s.pipeline_age.get(pid, 0)

                    # Scoring: prioritize low estimated memory (to pack well) and high age (to avoid starvation).
                    # Weight by priority implicitly by searching prio order (QUERY first).
                    mem_score = est_mem if est_mem is not None else (0.60 * pool.max_ram_pool)
                    # Age improves selection; failures penalize selection unless we need to finish.
                    score = (mem_score, fc, -age)

                    if best_local is None or score < best_local[0]:
                        best_local = (score, pipeline, op)
                    scanned += 1

                if best_local is not None:
                    _, pipeline, op = best_local
                    chosen = (pipeline, op)
                    chosen_prio = prio
                    break  # respect priority order strongly (objective weights)

            if chosen is None:
                break  # nothing feasible for this pool right now

            pipeline, op = chosen

            # Decide resource sizes.
            ram_req = _ram_request_gb(pipeline, op, pool, avail_ram)
            cpu_req = _cpu_request(pipeline, pool, avail_cpu)

            # If we can't allocate meaningful resources, stop trying in this pool this tick.
            if ram_req <= 0 or cpu_req <= 0:
                break

            # Create assignment; schedule a single op at a time (simple, reduces blast radius on OOM).
            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update local available resource counters.
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Reset pipeline age on successful scheduling attempt to reduce repeated head-of-line blocking.
            s.pipeline_age[pipeline.pipeline_id] = 0

            # Move pipeline to back of its queue to allow other pipelines of same priority to make progress.
            q = s.queues[chosen_prio]
            try:
                q.remove(pipeline.pipeline_id)
                q.append(pipeline.pipeline_id)
            except ValueError:
                pass

    # Opportunistically clean queues of completed/hard-failed pipelines to reduce scanning overhead.
    for prio, q in s.queues.items():
        new_q = []
        for pid in q:
            p = s.pipelines_by_id.get(pid, None)
            if p is None:
                continue
            if _pipeline_done_or_hard_failed(p):
                continue
            new_q.append(pid)
        s.queues[prio] = new_q

    return suspensions, assignments
