# policy_key: scheduler_low_048
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.038709
# generation_seconds: 43.83
# generated_at: 2026-03-14T02:39:22.145982
@register_scheduler_init(key="scheduler_low_048")
def scheduler_low_048_init(s):
    """Priority-aware FIFO with small, safe improvements over naive FIFO.

    Key ideas (kept intentionally simple/robust):
      1) Separate waiting queues by priority; always consider higher priority first.
      2) Right-size CPU/RAM instead of giving an operator the entire pool:
         - allocate a capped CPU share per op to allow concurrency and reduce HoL blocking.
      3) OOM-aware retry: if an op fails with an OOM-like error, bump its RAM request next time.
      4) Light "aging" for fairness: if BATCH waits long enough, treat it like INTERACTIVE for ordering.

    Notes:
      - No preemption is attempted (not enough portable introspection into running containers).
      - Placement uses simple pool preference: if multiple pools exist, prefer pool 0 for higher priority.
    """
    s.waiting = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}
    s.enqueued_at = {}  # pipeline_id -> tick when first seen (for aging)
    s.op_ram_bump = {}  # (pipeline_id, op_key) -> multiplier (>=1.0)
    s.tick = 0


def _priority_rank(pri):
    # Lower rank = higher priority
    if pri == Priority.QUERY:
        return 0
    if pri == Priority.INTERACTIVE:
        return 1
    return 2


def _is_oom_error(err):
    if err is None:
        return False
    s = str(err).lower()
    return ("oom" in s) or ("out of memory" in s) or ("memory" in s and "alloc" in s)


def _op_key(op):
    # Best-effort stable key across simulation objects.
    # If ops have a stable attribute, use it; else fall back to repr.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return (attr, str(v))
            except Exception:
                pass
    return ("repr", repr(op))


def _effective_priority(s, pipeline):
    # Aging: if a batch pipeline has waited long enough, treat it as INTERACTIVE for ordering.
    # This avoids starvation while still protecting QUERY/INTERACTIVE tail latency.
    pri = pipeline.priority
    if pri == Priority.BATCH_PIPELINE:
        t0 = s.enqueued_at.get(pipeline.pipeline_id, s.tick)
        if (s.tick - t0) >= 50:
            return Priority.INTERACTIVE
    return pri


def _pool_order_for_priority(num_pools, pri):
    # With multiple pools, prefer pool 0 for higher priority to reduce interference.
    if num_pools <= 1:
        return [0]
    if pri in (Priority.QUERY, Priority.INTERACTIVE):
        return [0] + list(range(1, num_pools))
    return list(range(1, num_pools)) + [0]


def _cpu_cap_for_priority(pool, pri):
    # Conservative caps to allow concurrency and reduce latency (avoid monopolizing the pool).
    # Use fractions of pool capacity, not availability, to behave consistently.
    max_cpu = pool.max_cpu_pool
    if pri == Priority.QUERY:
        return max(1.0, 0.50 * max_cpu)
    if pri == Priority.INTERACTIVE:
        return max(1.0, 0.35 * max_cpu)
    return max(1.0, 0.25 * max_cpu)


def _ram_cap_for_priority(pool, pri):
    # Similar to CPU: avoid giving all RAM to one op unless needed.
    max_ram = pool.max_ram_pool
    if pri == Priority.QUERY:
        return 0.60 * max_ram
    if pri == Priority.INTERACTIVE:
        return 0.50 * max_ram
    return 0.40 * max_ram


def _pick_next_pipeline(s):
    # Choose next pipeline from queues by effective priority, FIFO within class.
    # We scan in priority order and return the first runnable pipeline candidate.
    ordered = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    for pri in ordered:
        q = s.waiting[pri]
        if q:
            return q.pop(0)
    return None


@register_scheduler(key="scheduler_low_048")
def scheduler_low_048_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines -> enqueue by priority.
      - Process results -> if OOM failure, bump RAM multiplier for that op.
      - For each pool, schedule as many runnable operators as possible, prioritizing QUERY/INTERACTIVE.
    """
    s.tick += 1

    # Enqueue new pipelines
    for p in pipelines:
        if p.pipeline_id not in s.enqueued_at:
            s.enqueued_at[p.pipeline_id] = s.tick
        s.waiting[p.priority].append(p)

    # Learn from execution results (OOM-aware RAM bumps)
    for r in results:
        if getattr(r, "failed", None) is not None and r.failed() and _is_oom_error(getattr(r, "error", None)):
            # Bump per-op multiplier so next attempt requests more RAM.
            for op in getattr(r, "ops", []) or []:
                key = (getattr(r, "pipeline_id", None), _op_key(op))
                # pipeline_id may not be present in ExecutionResult in some sims; fall back to op-only key.
                if key[0] is None:
                    key = ("unknown_pipeline", _op_key(op))
                cur = s.op_ram_bump.get(key, 1.0)
                # Geometric growth with cap to avoid runaway.
                s.op_ram_bump[key] = min(cur * 1.5, 8.0)

    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Rebuild a single combined selection view each scheduling cycle:
    # We'll temporarily pull from queues, but requeue pipelines that aren't ready/runnable.
    requeue = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}

    def put_back(p):
        requeue[p.priority].append(p)

    # For each pool, schedule as much as we can.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep assigning while resources remain for at least a minimal container.
        # Minimal CPU=1.0, minimal RAM is whatever remains; we also apply caps.
        made_progress = True
        # Bound the loop to prevent pathological spinning if nothing is runnable.
        max_iters = 200
        iters = 0

        while made_progress and iters < max_iters:
            iters += 1
            made_progress = False

            # Peek across all queues: choose the best pipeline to try next for this pool.
            # We do not permanently remove until we successfully assign an op; otherwise we requeue.
            candidate = None
            candidate_eff_pri = None

            # Find best available candidate by effective priority (with aging).
            for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                if not s.waiting[pri]:
                    continue
                # Evaluate the head-of-line pipeline; if not runnable, we'll requeue it below.
                candidate = s.waiting[pri].pop(0)
                candidate_eff_pri = _effective_priority(s, candidate)
                break

            if candidate is None:
                break

            status = candidate.runtime_status()

            # Drop completed pipelines (shouldn't be re-queued).
            if status.is_pipeline_successful():
                continue

            # If pipeline has failures (non-OOM or repeated), we still keep it in queue because
            # the simulator's FAILED state may be retryable; we rely on get_ops(ASSIGNABLE_STATES).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable yet (waiting on parents or in-flight). Requeue to preserve FIFO.
                put_back(candidate)
                continue

            # Pool preference: if multiple pools, try to place high priority in preferred pools.
            # If we're in a non-preferred pool for this effective priority, requeue and let
            # other pools try first (reduces interference).
            preferred_order = _pool_order_for_priority(s.executor.num_pools, candidate_eff_pri)
            if preferred_order and preferred_order[0] != pool_id and candidate_eff_pri in (Priority.QUERY, Priority.INTERACTIVE):
                # Only defer once; put back and let next pool iteration potentially place it.
                put_back(candidate)
                continue

            # Determine CPU/RAM request with per-priority caps and OOM bump multiplier.
            cpu_cap = _cpu_cap_for_priority(pool, candidate_eff_pri)
            ram_cap = _ram_cap_for_priority(pool, candidate_eff_pri)

            req_cpu = min(avail_cpu, cpu_cap)
            # Ensure at least 1 CPU if any CPU is available.
            if req_cpu < 1.0 and avail_cpu >= 1.0:
                req_cpu = 1.0

            # RAM: apply bump multiplier learned from OOMs for this operator.
            op = op_list[0]
            bump_key = (candidate.pipeline_id, _op_key(op))
            bump = s.op_ram_bump.get(bump_key, 1.0)

            # Start with a fair share of available RAM, then bump, then cap.
            base_ram = min(avail_ram, ram_cap)
            req_ram = min(avail_ram, base_ram * bump)

            if req_cpu <= 0 or req_ram <= 0:
                put_back(candidate)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=candidate.priority,
                    pool_id=pool_id,
                    pipeline_id=candidate.pipeline_id,
                )
            )

            # Optimistically update available resources for further packing this tick.
            avail_cpu -= req_cpu
            avail_ram -= req_ram
            made_progress = True

            # Requeue the pipeline to allow other pipelines to interleave (reduces latency).
            put_back(candidate)

            # Stop if remaining resources are too small for another meaningful container.
            if avail_cpu < 1.0 or avail_ram <= 0:
                break

    # Restore queues, preserving order within each priority: remaining old queue first, then requeues.
    for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        # Items still in s.waiting[pri] were not considered/removed; keep them first.
        s.waiting[pri].extend(requeue[pri])

    return suspensions, assignments
