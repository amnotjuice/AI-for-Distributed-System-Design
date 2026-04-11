# policy_key: scheduler_high_002
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.106052
# generation_seconds: 134.34
# generated_at: 2026-03-12T22:25:57.059455
@register_scheduler_init(key="scheduler_high_002")
def scheduler_high_002_init(s):
    """
    Priority-aware, latency-focused scheduler with small, incremental improvements over naive FIFO:

    1) Priority queues (QUERY > INTERACTIVE > BATCH) to reduce head-of-line blocking for latency-sensitive work.
    2) Conservative per-op "right sizing" (don't give one op the whole pool by default) to increase concurrency.
    3) Simple OOM-aware RAM retry: on OOM failures, retry the same op with more RAM (RAM-first scaling).
    4) Light reservation: when high-priority work is waiting, keep some headroom by throttling batch admissions.

    Notes:
    - No preemption (interface does not reliably expose running container ids for suspension).
    - Avoids scheduling multiple ops from the same pipeline concurrently (reduces interference).
    """
    from collections import deque

    # Simulation tick for basic aging/fairness heuristics (incremented each scheduler call).
    s.tick = 0

    # Per-priority waiting queues (store Pipeline objects).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track first enqueue time for aging and avoiding duplicate enqueues.
    s.pipeline_enqueued_tick = {}  # pipeline_id -> tick
    s.known_pipeline_ids = set()

    # OOM-adaptive state keyed by operator identity.
    s.op_ram_guess = {}            # op_key -> ram amount to request next time
    s.op_oom_retries = {}          # op_key -> count
    s.op_nonretriable = set()      # op_key that failed with non-OOM error (do not retry)

    # Policy knobs (kept simple and conservative).
    s.oom_retry_limit = 3

    # If any high-priority work is waiting, reserve this fraction of each pool against batch.
    s.batch_reserve_frac_cpu = 0.25
    s.batch_reserve_frac_ram = 0.25

    # Base per-op sizing as a fraction of pool capacity by priority.
    s.cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.ram_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Aging: if a batch pipeline waits long enough, allow it to compete with INTERACTIVE.
    s.batch_aging_ticks = 50

    # Determine assignable states robustly.
    try:
        s.assignable_states = ASSIGNABLE_STATES
    except NameError:
        s.assignable_states = [OperatorState.PENDING, OperatorState.FAILED]


@register_scheduler(key="scheduler_high_002")
def scheduler_high_002_scheduler(s, results: List["ExecutionResult"],
                                 pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    See init docstring for the policy overview.

    Implementation details:
    - Enqueue new pipelines into priority-specific deques.
    - Update OOM state from completed/failed results.
    - For each pool, greedily pack multiple assignments while resources remain:
        - pick the highest-priority pipeline with a ready operator (parents complete),
        - size cpu/ram by priority fraction with OOM RAM overrides,
        - avoid scheduling if the pipeline already has RUNNING/ASSIGNED ops.
    """
    s.tick += 1

    def _is_oom(err) -> bool:
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg)

    def _op_key(op) -> str:
        """
        Best-effort stable key for an operator across retries.
        Prefer explicit ids if available; fall back to repr(op).
        """
        for attr in ("op_id", "operator_id", "node_id", "uid", "id", "name"):
            v = getattr(op, attr, None)
            if v is not None:
                return f"{attr}:{v}"
        return f"repr:{repr(op)}"

    def _enqueue_pipeline(p: "Pipeline"):
        if p.pipeline_id in s.known_pipeline_ids:
            return
        s.known_pipeline_ids.add(p.pipeline_id)
        s.pipeline_enqueued_tick[p.pipeline_id] = s.tick

        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pipeline_done_or_dead(p: "Pipeline") -> bool:
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If the pipeline has failed ops, we *may* still retry them if they were OOM.
        # We decide retriable vs non-retriable at the operator level; so don't drop here.
        return False

    def _pipeline_inflight(p: "Pipeline") -> bool:
        st = p.runtime_status()
        # Avoid scheduling multiple ops per pipeline at once; reduces interference and churn.
        assigned = st.state_counts.get(OperatorState.ASSIGNED, 0)
        running = st.state_counts.get(OperatorState.RUNNING, 0)
        susp = st.state_counts.get(OperatorState.SUSPENDING, 0)
        return (assigned + running + susp) > 0

    def _next_ready_op(p: "Pipeline"):
        st = p.runtime_status()
        ops = st.get_ops(s.assignable_states, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    # 1) Ingest new pipelines.
    for p in pipelines:
        _enqueue_pipeline(p)

    # 2) Process results: learn OOM vs non-OOM failures and update RAM guesses.
    for r in results:
        if not r.failed():
            continue

        oom = _is_oom(r.error)
        for op in (r.ops or []):
            k = _op_key(op)
            if oom:
                s.op_oom_retries[k] = s.op_oom_retries.get(k, 0) + 1
                # RAM-first backoff: next time ask for more than last allocated.
                # If we don't know the unit, doubling is a safe monotone heuristic in simulation.
                prev = s.op_ram_guess.get(k, 0.0)
                try:
                    last = float(r.ram) if r.ram is not None else 0.0
                except Exception:
                    last = 0.0
                s.op_ram_guess[k] = max(prev, last * 2.0 if last > 0 else prev * 2.0 if prev > 0 else 1.0)
            else:
                s.op_nonretriable.add(k)

    # 3) Global signal: is any high-priority work waiting?
    # (Used to throttle batch to protect tail latency.)
    high_waiting = (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    def _pick_from_queue(q, effective_priority: "Priority", pool_id: int, avail_cpu: float, avail_ram: float):
        """
        Try to find one schedulable pipeline/op from the given queue.
        Uses a bounded rotation scan to avoid O(n^2) behavior and to maintain FIFO-ish fairness within a class.
        """
        n = len(q)
        for _ in range(n):
            p = q.popleft()

            # Drop finished pipelines lazily.
            if _pipeline_done_or_dead(p):
                continue

            # Avoid parallelism within a pipeline.
            if _pipeline_inflight(p):
                q.append(p)
                continue

            op = _next_ready_op(p)
            if op is None:
                q.append(p)
                continue

            k = _op_key(op)

            # If we have evidence this op fails for non-OOM reasons, don't retry (drop pipeline by skipping forever).
            # In practice, this mimics the baseline's "don't retry failures".
            if k in s.op_nonretriable:
                # Do not requeue this pipeline (effectively dropped).
                continue

            # If this op has repeatedly OOM'd, stop retrying to avoid infinite churn.
            if s.op_oom_retries.get(k, 0) > s.oom_retry_limit:
                continue

            pool = s.executor.pools[pool_id]

            # Base sizing by priority (fraction of pool capacity) with minimums to ensure progress.
            base_cpu = max(1.0, float(pool.max_cpu_pool) * float(s.cpu_frac.get(effective_priority, 0.25)))
            base_ram = max(1.0, float(pool.max_ram_pool) * float(s.ram_frac.get(effective_priority, 0.25)))

            # OOM override: ensure we meet the learned RAM guess.
            want_ram = max(base_ram, float(s.op_ram_guess.get(k, 0.0) or 0.0))
            want_cpu = base_cpu

            # Fit to availability.
            cpu = min(float(avail_cpu), float(want_cpu))
            ram = min(float(avail_ram), float(want_ram))

            # If we can't meet minimums, skip for now (keep queue order fair).
            if cpu < 1.0 or ram < max(1.0, want_ram):
                q.append(p)
                continue

            # Create assignment.
            a = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(a)

            # Requeue pipeline to allow subsequent ops later.
            q.append(p)
            return cpu, ram

        return None

    # 4) Pack each pool with as many ops as feasible, prioritizing latency-sensitive work.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # If multiple pools exist, lightly protect pool 0 for high priority by steering batch away when possible.
        protect_pool0 = (s.executor.num_pools > 1 and pool_id == 0 and high_waiting)

        # Greedy packing loop.
        while avail_cpu >= 1.0 and avail_ram >= 1.0:
            made = False

            # Highest priority first: QUERY
            picked = _pick_from_queue(s.q_query, Priority.QUERY, pool_id, avail_cpu, avail_ram)
            if picked is not None:
                cpu_used, ram_used = picked
                avail_cpu -= cpu_used
                avail_ram -= ram_used
                made = True
                continue

            # Next: INTERACTIVE
            picked = _pick_from_queue(s.q_interactive, Priority.INTERACTIVE, pool_id, avail_cpu, avail_ram)
            if picked is not None:
                cpu_used, ram_used = picked
                avail_cpu -= cpu_used
                avail_ram -= ram_used
                made = True
                continue

            # Batch aging: promote old batch pipelines to compete as INTERACTIVE (still submitted as BATCH).
            # This only affects selection order; the Assignment priority remains pipeline.priority.
            # We'll scan batch queue once for an aged candidate.
            aged_picked = None
            if len(s.q_batch) > 0:
                n = len(s.q_batch)
                for _ in range(n):
                    p = s.q_batch.popleft()
                    enq = s.pipeline_enqueued_tick.get(p.pipeline_id, s.tick)
                    if (s.tick - enq) >= s.batch_aging_ticks:
                        # Temporarily treat as interactive for sizing/selection.
                        s.q_batch.append(p)  # keep it in the queue; selection happens via helper
                        aged_picked = _pick_from_queue(s.q_batch, Priority.INTERACTIVE, pool_id, avail_cpu, avail_ram)
                        break
                    s.q_batch.append(p)

            if aged_picked is not None:
                cpu_used, ram_used = aged_picked
                avail_cpu -= cpu_used
                avail_ram -= ram_used
                made = True
                continue

            # Finally: BATCH, but throttle when high priority is waiting.
            if protect_pool0:
                # Don't run batch on pool 0 while high-priority is waiting.
                break

            # Batch reservation: keep some headroom if high-priority is queued.
            if high_waiting:
                reserve_cpu = float(pool.max_cpu_pool) * float(s.batch_reserve_frac_cpu)
                reserve_ram = float(pool.max_ram_pool) * float(s.batch_reserve_frac_ram)
                if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                    break

            picked = _pick_from_queue(s.q_batch, Priority.BATCH_PIPELINE, pool_id, avail_cpu, avail_ram)
            if picked is not None:
                cpu_used, ram_used = picked
                avail_cpu -= cpu_used
                avail_ram -= ram_used
                made = True
                continue

            if not made:
                break

    return suspensions, assignments
