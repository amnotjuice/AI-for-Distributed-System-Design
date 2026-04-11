# policy_key: scheduler_low_036
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050495
# generation_seconds: 41.86
# generated_at: 2026-04-09T21:31:27.685700
@register_scheduler_init(key="scheduler_low_036")
def scheduler_low_036_init(s):
    """Priority-aware, failure-averse scheduler.

    Core ideas (kept intentionally simple vs. complex preemption policies):
      1) Strict priority ordering for admission (QUERY > INTERACTIVE > BATCH) to protect the score-dominant classes.
      2) Conservative RAM-first sizing to reduce OOMs (failures are extremely costly in the objective).
      3) Per-operator adaptive RAM bumping on failure (exponential backoff) and persistence of last-known-good sizes.
      4) Gentle fairness: round-robin within each priority, plus light "aging" for long-waiting batch pipelines
         to avoid indefinite starvation under sustained interactive load.
      5) Multi-assignment per pool per tick with simple packing, to improve utilization over naive FIFO.
    """
    from collections import deque

    # Waiting queues per priority (round-robin within each priority)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Pipeline arrival tick (for aging)
    s.pipeline_arrival_tick = {}

    # Operator resource model keyed by operator object identity (stable within a simulation run)
    # Stores last requested RAM and a failure count for exponential backoff.
    s.op_ram_req = {}      # op_key -> ram
    s.op_fail_count = {}   # op_key -> int

    # Track tick progression for aging and pacing decisions
    s.tick = 0

    # If a batch pipeline has waited longer than this, allow it to be scheduled alongside interactive
    # (still below QUERY). This reduces starvation without sacrificing query latency much.
    s.batch_aging_threshold_ticks = 40

    # Soft per-priority initial RAM fractions of pool max RAM (RAM-first sizing).
    s.ram_frac_query = 0.30
    s.ram_frac_interactive = 0.25
    s.ram_frac_batch = 0.20

    # Soft per-priority CPU fractions of pool max CPU (kept moderate to allow some parallelism).
    s.cpu_frac_query = 0.60
    s.cpu_frac_interactive = 0.45
    s.cpu_frac_batch = 0.35

    # Minimum allocation quanta
    s.min_cpu = 1.0
    s.min_ram = 0.5  # assumes RAM units support fractional; if not, simulation will coerce/compare floats


@register_scheduler(key="scheduler_low_036")
def scheduler_low_036_scheduler(s, results, pipelines):
    """
    Scheduling step:
      - Enqueue new pipelines by priority.
      - Update RAM model based on execution results (OOM-ish failure => bump RAM, success => persist).
      - For each pool, pack multiple ready operators using priority queues + aging.
    """
    from collections import deque

    def _enqueue_pipeline(p):
        # Record first-seen tick for aging.
        if p.pipeline_id not in s.pipeline_arrival_tick:
            s.pipeline_arrival_tick[p.pipeline_id] = s.tick

        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _op_key(op):
        # Use object identity (stable within run) as key without requiring op_id fields.
        return id(op)

    def _looks_like_oom(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _base_ram_for(priority, pool):
        frac = s.ram_frac_batch
        if priority == Priority.QUERY:
            frac = s.ram_frac_query
        elif priority == Priority.INTERACTIVE:
            frac = s.ram_frac_interactive
        return max(s.min_ram, pool.max_ram_pool * frac)

    def _base_cpu_for(priority, pool):
        frac = s.cpu_frac_batch
        if priority == Priority.QUERY:
            frac = s.cpu_frac_query
        elif priority == Priority.INTERACTIVE:
            frac = s.cpu_frac_interactive
        return max(s.min_cpu, pool.max_cpu_pool * frac)

    def _ram_request(op, priority, pool, avail_ram):
        # If we've seen this op before, reuse its last request (possibly bumped).
        k = _op_key(op)
        if k in s.op_ram_req:
            return min(avail_ram, pool.max_ram_pool, max(s.min_ram, s.op_ram_req[k]))
        # Otherwise, pick a conservative default to avoid OOMs.
        return min(avail_ram, pool.max_ram_pool, _base_ram_for(priority, pool))

    def _cpu_request(priority, pool, avail_cpu):
        # Use a modest CPU fraction to speed up high-priority latency while still enabling packing.
        req = _base_cpu_for(priority, pool)
        return min(avail_cpu, pool.max_cpu_pool, max(s.min_cpu, req))

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any failed operator exists, consider pipeline "failed" and don't keep retrying forever
        # unless we believe it's OOM and can be recovered. We only know that via recent results;
        # so here we just keep it in queue and let assignment retry eligible states.
        # If the sim marks failures terminal, get_ops(ASSIGNABLE_STATES) will return none.
        return False

    def _pick_next_pipeline_for_scheduling():
        # Priority with aging: QUERY first, then INTERACTIVE, then "aged" BATCH, then normal BATCH.
        # We do NOT reorder within priority except via round-robin.
        if s.q_query:
            return s.q_query, s.q_query[0]

        if s.q_interactive:
            return s.q_interactive, s.q_interactive[0]

        # Aging for batch: if oldest batch has waited long enough, treat it as interactive for eligibility.
        if s.q_batch:
            oldest = s.q_batch[0]
            waited = s.tick - s.pipeline_arrival_tick.get(oldest.pipeline_id, s.tick)
            if waited >= s.batch_aging_threshold_ticks:
                return s.q_batch, oldest

        if s.q_batch:
            return s.q_batch, s.q_batch[0]

        return None, None

    # Advance time
    s.tick += 1

    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # If nothing changed, exit early
    if not pipelines and not results:
        return [], []

    # Update RAM model from results
    for r in results:
        # Persist sizing per op; bump aggressively on OOM-like failure.
        # Note: ExecutionResult.ops is a list of ops for the container execution.
        if not getattr(r, "ops", None):
            continue

        if r.failed():
            oomish = _looks_like_oom(getattr(r, "error", None))
            if oomish:
                for op in r.ops:
                    k = _op_key(op)
                    prev = s.op_ram_req.get(k, r.ram if getattr(r, "ram", None) is not None else None)
                    if prev is None:
                        continue
                    fc = s.op_fail_count.get(k, 0) + 1
                    s.op_fail_count[k] = fc
                    # Exponential backoff with a floor; cap to a large but finite value (pool cap applied on request).
                    bumped = max(prev * 2.0, prev + 1.0)
                    s.op_ram_req[k] = bumped
            else:
                # Non-OOM failures: do not keep inflating RAM; let pipeline semantics decide.
                pass
        else:
            # Success: record last-known-good RAM request for each op.
            for op in r.ops:
                k = _op_key(op)
                if getattr(r, "ram", None) is not None:
                    s.op_ram_req[k] = r.ram
                # Reset failure count on success to avoid runaway RAM growth across independent executions.
                if k in s.op_fail_count:
                    s.op_fail_count[k] = 0

    suspensions = []
    assignments = []

    # Helper: rotate a deque by one (round-robin) after considering its head
    def _rr_rotate(q):
        if q:
            q.append(q.popleft())

    # Scheduling / packing loop per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Prevent infinite loops when no pipeline is schedulable this tick.
        attempts_without_assignment = 0
        max_attempts = (len(s.q_query) + len(s.q_interactive) + len(s.q_batch) + 1) * 2

        while avail_cpu > 0 and avail_ram > 0 and attempts_without_assignment < max_attempts:
            q, pipeline = _pick_next_pipeline_for_scheduling()
            if pipeline is None:
                break

            # Pop/rotate selection to ensure RR within priority.
            _rr_rotate(q)

            # Skip completed pipelines
            if _pipeline_done_or_failed(pipeline):
                attempts_without_assignment += 1
                continue

            status = pipeline.runtime_status()

            # Assign only when parents are complete; pick a single operator to keep atomicity & packing simple.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; try others.
                attempts_without_assignment += 1
                continue

            op = op_list[0]

            # Size request
            cpu_req = _cpu_request(pipeline.priority, pool, avail_cpu)
            ram_req = _ram_request(op, pipeline.priority, pool, avail_ram)

            # If we can't fit even minimal, stop packing this pool this tick.
            if cpu_req <= 0 or ram_req <= 0 or cpu_req > avail_cpu or ram_req > avail_ram:
                break

            # Record requested RAM for this op (so that if it later fails, we bump from this baseline).
            s.op_ram_req[_op_key(op)] = ram_req

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update remaining headroom and reset attempt counter because we made progress.
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            attempts_without_assignment = 0

    return suspensions, assignments
