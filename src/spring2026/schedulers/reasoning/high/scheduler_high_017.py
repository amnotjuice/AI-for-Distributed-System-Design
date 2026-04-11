# policy_key: scheduler_high_017
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.167330
# generation_seconds: 149.49
# generated_at: 2026-03-12T23:03:19.568663
@register_scheduler_init(key="scheduler_high_017")
def scheduler_high_017_init(s):
    """
    Priority-biased FIFO scheduler with small, practical improvements over naive FIFO:

    1) Priority queues: always try QUERY > INTERACTIVE > BATCH first (latency focus).
    2) OOM-aware retries: if an op fails with an OOM-like error, retry it with higher RAM.
    3) Simple aging: very old BATCH pipelines are allowed to "bubble up" in selection order
       (but still execute with their original Priority for metrics/accounting).

    Design goal: improve tail latency for high-priority work without adding risky complexity
    (no preemption, no multi-op bundling).
    """
    # Separate FIFO queues per priority (lists used to avoid imports)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track that we've already enqueued a pipeline_id to avoid duplicates
    s.enqueued_pipeline_ids = set()

    # Lightweight time for aging
    s.tick = 0
    s.enqueue_tick = {}  # pipeline_id -> tick first enqueued

    # OOM/backoff hints
    s.pipeline_ram_hint = {}  # pipeline_id -> last known good/needed RAM
    s.pipeline_cpu_hint = {}  # pipeline_id -> last used CPU (optional, conservative)
    s.op_ram_hint = {}        # op -> RAM hint (stronger than pipeline hint when present)
    s.op_cpu_hint = {}        # op -> CPU hint (rarely adjusted; placeholder)

    # Failure tracking to avoid infinite retries on non-OOM errors
    s.pipeline_terminal_failure = set()  # pipeline_id

    # Track inflight ops so we can attribute results back to pipelines
    s.op_to_pipeline_id = {}  # op -> pipeline_id

    # Tuning knobs (kept simple)
    s.batch_aging_threshold_ticks = 50
    s.max_oom_retries_per_op = 5
    s.op_oom_retries = {}  # op -> count

    # Resource sizing fractions by priority (start simple; high priority gets more)
    s.frac_cpu = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 0.85,
        Priority.BATCH_PIPELINE: 0.60,
    }
    s.frac_ram = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 0.85,
        Priority.BATCH_PIPELINE: 0.60,
    }


def _scheduler_high_017_is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _scheduler_high_017_queue_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _scheduler_high_017_effective_priority(s, pipeline) -> "Priority":
    # Simple aging: very old batch pipelines get considered earlier.
    if pipeline.priority != Priority.BATCH_PIPELINE:
        return pipeline.priority
    t0 = s.enqueue_tick.get(pipeline.pipeline_id, s.tick)
    waited = s.tick - t0
    if waited >= s.batch_aging_threshold_ticks:
        return Priority.INTERACTIVE
    return Priority.BATCH_PIPELINE


def _scheduler_high_017_priority_order_for_pool(s, pool_id: int):
    # Keep this minimal: always prioritize latency.
    # If multiple pools exist, we still allow all priorities on all pools; this remains
    # "small improvement" vs. introducing dedicated pools.
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _scheduler_high_017_try_dequeue_ready(s, q, pool, pool_id, scheduled_pipeline_ids):
    """
    Scan a single FIFO queue; return (pipeline, ops) if an assignable op is ready and
    can plausibly fit the pool given current hints. Otherwise, rotate pipeline to back.
    """
    if not q:
        return None, None

    # Bounded scan: at most len(q) rotations to avoid infinite loops.
    n = len(q)
    for _ in range(n):
        pipeline = q.pop(0)

        # De-dup / drop conditions
        pid = pipeline.pipeline_id
        if pid in scheduled_pipeline_ids:
            q.append(pipeline)
            continue

        if pid in s.pipeline_terminal_failure:
            # Drop permanently failed pipelines
            s.enqueued_pipeline_ids.discard(pid)
            s.enqueue_tick.pop(pid, None)
            continue

        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            # Cleanup completed pipelines
            s.enqueued_pipeline_ids.discard(pid)
            s.enqueue_tick.pop(pid, None)
            s.pipeline_ram_hint.pop(pid, None)
            s.pipeline_cpu_hint.pop(pid, None)
            continue

        # Find one ready op (keep single-op assignments to reduce head-of-line blocking)
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            q.append(pipeline)
            continue

        op = op_list[0]

        # Quick "fit" check using RAM hint (avoid repeatedly placing on too-small pools)
        # We'll still clamp at assignment time; this check is only to skip obvious mismatches.
        op_hint = s.op_ram_hint.get(op)
        pipe_hint = s.pipeline_ram_hint.get(pid)

        # If there's a hint, require the pool to be able to satisfy it at least in max terms.
        hint_ram = op_hint if op_hint is not None else pipe_hint
        if hint_ram is not None and hint_ram > pool.max_ram_pool:
            # This pool can never satisfy; rotate and let another pool attempt.
            q.append(pipeline)
            continue

        # Candidate found; keep it out of queue until we either assign or requeue explicitly.
        return pipeline, op_list

    return None, None


def _scheduler_high_017_size_allocation(s, pipeline, ops, pool, pool_id):
    """
    Decide cpu/ram allocation for a single-op assignment:
    - Start from a priority-based fraction of pool capacity.
    - Respect OOM-derived hints (per-op > per-pipeline).
    - Clamp to currently available resources.
    """
    prio = pipeline.priority

    # Baseline from pool capacity (not just avail) to be stable across temporary contention.
    base_cpu = pool.max_cpu_pool * s.frac_cpu.get(prio, 0.75)
    base_ram = pool.max_ram_pool * s.frac_ram.get(prio, 0.75)

    # Apply hints
    pid = pipeline.pipeline_id
    op = ops[0] if ops else None

    hint_cpu = None
    if op is not None and op in s.op_cpu_hint:
        hint_cpu = s.op_cpu_hint[op]
    elif pid in s.pipeline_cpu_hint:
        hint_cpu = s.pipeline_cpu_hint[pid]

    hint_ram = None
    if op is not None and op in s.op_ram_hint:
        hint_ram = s.op_ram_hint[op]
    elif pid in s.pipeline_ram_hint:
        hint_ram = s.pipeline_ram_hint[pid]

    cpu = base_cpu
    ram = base_ram
    if hint_cpu is not None:
        cpu = max(cpu, hint_cpu)
    if hint_ram is not None:
        ram = max(ram, hint_ram)

    # Clamp to pool maxima and current availability
    cpu = min(cpu, pool.max_cpu_pool, pool.avail_cpu_pool)
    ram = min(ram, pool.max_ram_pool, pool.avail_ram_pool)

    # If pool has tiny remnants, avoid zero-ish allocations
    if cpu <= 0 or ram <= 0:
        return 0, 0
    return cpu, ram


@register_scheduler(key="scheduler_high_017")
def scheduler_high_017_scheduler(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Scheduling step:
    - Enqueue new pipelines into per-priority FIFOs.
    - Process results to detect OOMs and update RAM hints; mark non-OOM failures as terminal.
    - For each pool, pick the next ready op from the highest effective priority queue that fits.
    """
    s.tick += 1

    # 1) Incorporate new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.enqueued_pipeline_ids or pid in s.pipeline_terminal_failure:
            continue
        q = _scheduler_high_017_queue_for_priority(s, p.priority)
        q.append(p)
        s.enqueued_pipeline_ids.add(pid)
        s.enqueue_tick.setdefault(pid, s.tick)

    # 2) Process execution results (update hints / terminal failures)
    # Note: we attribute results to pipeline_id via op_to_pipeline_id.
    for r in results:
        ops = getattr(r, "ops", None) or []
        failed = False
        try:
            failed = r.failed()
        except Exception:
            # If failed() isn't usable for some reason, fall back to error presence.
            failed = getattr(r, "error", None) is not None

        if not ops:
            continue

        # Best-effort: locate pool for max_ram reference
        pool_id = getattr(r, "pool_id", None)
        pool = None
        if pool_id is not None and 0 <= pool_id < s.executor.num_pools:
            pool = s.executor.pools[pool_id]

        for op in ops:
            pid = s.op_to_pipeline_id.pop(op, None)

            if not failed:
                # Success: we can keep conservative hints; clear per-op oom retry counter.
                if pid is not None:
                    # Remember last allocation as a mild hint (helps stabilize sizes)
                    s.pipeline_ram_hint[pid] = max(s.pipeline_ram_hint.get(pid, 0), getattr(r, "ram", 0) or 0)
                    s.pipeline_cpu_hint[pid] = max(s.pipeline_cpu_hint.get(pid, 0), getattr(r, "cpu", 0) or 0)
                s.op_oom_retries.pop(op, None)
                # Once the op succeeds, per-op hints aren't needed anymore.
                s.op_ram_hint.pop(op, None)
                s.op_cpu_hint.pop(op, None)
                continue

            # Failure handling
            is_oom = _scheduler_high_017_is_oom_error(getattr(r, "error", None))
            if not is_oom:
                # Treat unknown/non-OOM failures as terminal for the whole pipeline
                if pid is not None:
                    s.pipeline_terminal_failure.add(pid)
                continue

            # OOM: increase RAM hint for this op (and mildly for pipeline), capped by pool max if known
            prev_ram = getattr(r, "ram", 0) or 0
            if prev_ram <= 0 and pool is not None:
                prev_ram = 0.5 * pool.max_ram_pool  # fallback baseline if allocation unknown

            new_ram = prev_ram * 1.8 if prev_ram > 0 else prev_ram
            if pool is not None:
                new_ram = min(new_ram, pool.max_ram_pool)

            s.op_ram_hint[op] = max(s.op_ram_hint.get(op, 0), new_ram)

            # Track retries per op; if exceeded, mark pipeline terminal to avoid endless loops
            cnt = s.op_oom_retries.get(op, 0) + 1
            s.op_oom_retries[op] = cnt
            if cnt > s.max_oom_retries_per_op:
                if pid is not None:
                    s.pipeline_terminal_failure.add(pid)
                continue

            if pid is not None:
                # Pipeline hint increases more conservatively than the op hint
                s.pipeline_ram_hint[pid] = max(s.pipeline_ram_hint.get(pid, 0), new_ram * 0.75)

    # Early exit: nothing to do
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Avoid scheduling multiple ops from the same pipeline within one tick (simple latency/fairness)
    scheduled_pipeline_ids = set()

    # 3) Make assignments: at most one per pool (keeps policy simple and latency-friendly)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Determine pool-local priority order
        prio_order = _scheduler_high_017_priority_order_for_pool(s, pool_id)

        chosen_pipeline = None
        chosen_ops = None

        # Select across queues with effective priority (aging for batch)
        # We do a two-pass: build candidate queues in desired order, but allow aged batch to be
        # considered as interactive (selection only).
        for prio in prio_order:
            # For each target prio, scan all queues but filter by effective priority
            # (keeps behavior predictable without reshuffling queues).
            for q in (s.q_query, s.q_interactive, s.q_batch):
                if not q:
                    continue

                # Peek-scan by rotating: only consider pipelines whose effective prio matches `prio`
                n = len(q)
                found = False
                for _ in range(n):
                    p = q.pop(0)
                    eff = _scheduler_high_017_effective_priority(s, p)

                    if eff != prio:
                        q.append(p)
                        continue

                    # Put it back temporarily; let the standard dequeue helper do fit checks
                    q.insert(0, p)
                    candidate_pipeline, candidate_ops = _scheduler_high_017_try_dequeue_ready(
                        s, q, pool, pool_id, scheduled_pipeline_ids
                    )
                    if candidate_pipeline is not None:
                        chosen_pipeline, chosen_ops = candidate_pipeline, candidate_ops
                        found = True
                    break  # stop rotating once we've attempted this queue for this prio

                if found:
                    break
            if chosen_pipeline is not None:
                break

        if chosen_pipeline is None or not chosen_ops:
            continue

        # Size allocation for this op on this pool
        cpu, ram = _scheduler_high_017_size_allocation(s, chosen_pipeline, chosen_ops, pool, pool_id)
        if cpu <= 0 or ram <= 0:
            # Couldn't allocate; requeue pipeline to avoid dropping it
            q_back = _scheduler_high_017_queue_for_priority(s, chosen_pipeline.priority)
            q_back.append(chosen_pipeline)
            continue

        # Create the assignment
        assignment = Assignment(
            ops=chosen_ops,
            cpu=cpu,
            ram=ram,
            priority=chosen_pipeline.priority,
            pool_id=pool_id,
            pipeline_id=chosen_pipeline.pipeline_id,
        )
        assignments.append(assignment)

        # Track mapping so we can attribute results back to the pipeline
        for op in chosen_ops:
            s.op_to_pipeline_id[op] = chosen_pipeline.pipeline_id

        scheduled_pipeline_ids.add(chosen_pipeline.pipeline_id)

        # If pipeline still has more work later, requeue it for FIFO fairness within its class
        q_back = _scheduler_high_017_queue_for_priority(s, chosen_pipeline.priority)
        q_back.append(chosen_pipeline)

    return suspensions, assignments
