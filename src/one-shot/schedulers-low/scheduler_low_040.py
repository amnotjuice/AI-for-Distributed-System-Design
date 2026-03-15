# policy_key: scheduler_low_040
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.035867
# generation_seconds: 51.34
# generated_at: 2026-03-14T02:33:18.618636
@register_scheduler_init(key="scheduler_low_040")
def scheduler_low_040_init(s):
    """Priority-aware FIFO with simple right-sizing + OOM backoff.

    Improvements over naive FIFO:
      1) Separate waiting queues per priority; always try higher priority first.
      2) Pool preference: if multiple pools exist, prefer pool 0 for QUERY/INTERACTIVE.
      3) Avoid "give everything to one op": cap per-op CPU/RAM by priority to reduce
         head-of-line blocking and improve latency for interactive work.
      4) If an op fails with an OOM-like error, retry later with increased RAM for that op.
    """
    from collections import deque

    # Per-priority queues (FIFO within priority)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-(pipeline, op) RAM hint, increased on OOM-like failures
    s.ram_hint = {}  # (pipeline_id, op_key) -> ram_amount

    # Tuning knobs (kept intentionally simple)
    s.max_assignments_per_pool_per_tick = 2  # small to reduce bursty contention


def _priority_rank(priority):
    # Lower is "more important"
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and any others


def _pick_queue(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _op_key(op):
    # Best-effort stable identifier for an operator object across retries
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            try:
                return (attr, str(v))
            except Exception:
                pass
    return ("pyid", str(id(op)))


def _is_oom_error(err):
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)


def _cap_for_priority(pool, priority):
    # Caps (fractions of pool max) to prevent one op from monopolizing the pool.
    # Higher priority gets higher caps for lower latency.
    if priority == Priority.QUERY:
        cpu_frac, ram_frac = 0.75, 0.60
    elif priority == Priority.INTERACTIVE:
        cpu_frac, ram_frac = 0.60, 0.50
    else:  # batch
        cpu_frac, ram_frac = 0.40, 0.40
    cpu_cap = max(1.0, pool.max_cpu_pool * cpu_frac)
    ram_cap = max(1.0, pool.max_ram_pool * ram_frac)
    return cpu_cap, ram_cap


def _choose_pool_ids_for_priority(num_pools, priority):
    # If there are multiple pools, "prefer" pool 0 for latency-sensitive work,
    # but allow overflow to other pools.
    if num_pools <= 1:
        return [0]
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        return [0] + list(range(1, num_pools))
    # Batch prefers non-0 pools to reduce interference, but can use pool 0 if idle.
    return list(range(1, num_pools)) + [0]


@register_scheduler(key="scheduler_low_040")
def scheduler_low_040_scheduler(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Priority-first scheduling with limited per-op resource caps and OOM backoff.

    Core loop:
      - Enqueue new pipelines by priority
      - Process recent results to update per-op RAM hints on OOM
      - For each pool, assign up to a small number of ready ops, always preferring
        higher-priority pipelines and sizing within per-priority caps.
    """
    # Enqueue new pipelines
    for p in pipelines:
        _pick_queue(s, p.priority).append(p)

    # Update RAM hints based on failures (OOM backoff)
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue
        if not _is_oom_error(getattr(r, "error", None)):
            continue
        # Increase RAM hint for the failed ops, based on last attempt's RAM
        attempted_ram = getattr(r, "ram", None)
        if attempted_ram is None:
            continue
        # Attempted RAM might be 0/None in some models; guard a bit.
        new_hint = max(float(attempted_ram) * 2.0, float(attempted_ram) + 1.0)
        for op in getattr(r, "ops", []) or []:
            # r may not carry pipeline_id; rely on op carrying pipeline_id, else skip
            pid = getattr(op, "pipeline_id", None)
            if pid is None:
                continue
            s.ram_hint[(pid, _op_key(op))] = max(s.ram_hint.get((pid, _op_key(op)), 0.0), new_hint)

    # Fast exit if nothing to do
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    num_pools = s.executor.num_pools

    # Helper: attempt to schedule one op from a given priority into a specific pool
    def try_schedule_from_priority(pool_id, pool, priority, avail_cpu, avail_ram):
        q = _pick_queue(s, priority)
        if not q:
            return None

        cpu_cap, ram_cap = _cap_for_priority(pool, priority)

        # Rotate through a bounded number of pipelines to avoid head-of-line blocking
        # when the front pipeline isn't ready.
        rotations = min(len(q), 16)
        for _ in range(rotations):
            p = q.popleft()
            status = p.runtime_status()

            # Drop terminal pipelines
            has_failures = status.state_counts[OperatorState.FAILED] > 0
            if status.is_pipeline_successful() or has_failures:
                continue

            # Find at most one ready op to keep latency responsive
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                q.append(p)
                continue

            op = op_list[0]
            pid = p.pipeline_id

            # Default RAM sizing:
            # - Start with a modest share of available RAM (and cap it)
            # - If we have an OOM hint for this op, respect it (up to cap)
            base_ram = min(float(avail_ram), float(ram_cap))
            # Give batch smaller initial RAM to reduce blocking; interactive/query slightly higher.
            if priority == Priority.BATCH_PIPELINE:
                base_ram = min(base_ram, max(1.0, pool.max_ram_pool * 0.25))
            else:
                base_ram = min(base_ram, max(1.0, pool.max_ram_pool * 0.40))

            hinted_ram = s.ram_hint.get((pid, _op_key(op)), None)
            ram_req = base_ram if hinted_ram is None else min(float(hinted_ram), float(ram_cap), float(avail_ram))
            ram_req = max(1.0, ram_req)

            # CPU sizing (capped)
            cpu_req = min(float(avail_cpu), float(cpu_cap))
            cpu_req = max(1.0, cpu_req)

            # Must fit in current pool headroom
            if cpu_req > avail_cpu or ram_req > avail_ram:
                # Put pipeline back; maybe another pool/priority can use the headroom
                q.appendleft(p)
                return None

            # Schedule it
            a = Assignment(
                ops=op_list,
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            # Requeue pipeline for future ops
            q.append(p)
            return a

        return None

    # Main scheduling across pools
    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        made = 0
        while made < s.max_assignments_per_pool_per_tick:
            # Determine which priority classes we should attempt for this pool
            # Pool 0 is more latency-focused if present.
            if num_pools > 1 and pool_id == 0:
                priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            else:
                priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

            # For each priority in order, try to schedule something that fits
            chosen = None
            for pr in priority_order:
                # Some priorities prefer certain pools; if this pool isn't preferred,
                # still allow scheduling but try preferred pools first in aggregate by
                # iterating pools in natural order and using this function's pool_id.
                preferred_pools = _choose_pool_ids_for_priority(num_pools, pr)
                if preferred_pools and (pool_id not in preferred_pools[:1]) and (pool_id != preferred_pools[0]):
                    # Not first-choice pool; still eligible, so don't skip.
                    pass

                chosen = try_schedule_from_priority(pool_id, pool, pr, avail_cpu, avail_ram)
                if chosen is not None:
                    break

            if chosen is None:
                break

            assignments.append(chosen)
            made += 1
            # Update local headroom estimates to avoid oversubscription within this tick
            avail_cpu -= float(chosen.cpu)
            avail_ram -= float(chosen.ram)
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

    return suspensions, assignments
