# policy_key: scheduler_low_018
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044743
# generation_seconds: 41.47
# generated_at: 2026-03-12T21:48:04.823639
@register_scheduler_init(key="scheduler_low_018")
def scheduler_low_018_init(s):
    """Priority-aware FIFO with small, safe improvements over naive FIFO.

    Improvements vs. baseline:
    - Separate waiting queues by priority to reduce head-of-line blocking.
    - Allocate "reasonable slices" of CPU/RAM (instead of always all remaining) to improve concurrency.
    - Basic OOM-aware RAM backoff: if an operator fails with OOM, retry it with increased RAM next time.
    - Light anti-starvation: occasionally let batch through even when interactive keeps arriving.
    """
    from collections import deque

    # Per-priority FIFO queues
    s.waiting = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Track per-operator RAM overrides after OOMs:
    # key: (pipeline_id, op_identity) -> ram_override
    # We use id(op) as an identity handle since we don't know the operator schema.
    s.ram_override = {}

    # Anti-starvation knob: after several "high priority" assignments, allow a batch if waiting.
    s.hi_streak = 0
    s.hi_streak_limit = 6

    # Safety bounds for slice sizing
    s.min_cpu_slice = 1.0
    s.min_ram_slice = 0.25  # in whatever units the simulator uses (must be consistent with pool units)


@register_scheduler(key="scheduler_low_018")
def scheduler_low_018_scheduler(s, results, pipelines):
    """
    Scheduler loop:
    1) Enqueue new pipelines into per-priority FIFOs.
    2) Learn from failures: on OOM, increase RAM override for the failed operator(s).
    3) For each pool, repeatedly assign ready operators while resources remain:
       - Choose next pipeline by priority (with occasional batch admission to prevent starvation).
       - Assign one ready operator per pipeline per iteration.
       - Size CPU/RAM as a priority-dependent slice, respecting pool availability and learned RAM overrides.
    """
    # ---- Helpers (local to keep this file minimal and self-contained) ----
    def _prio_order_with_anti_starvation():
        # If batch is waiting and we've been serving high-priority for a while, give batch a turn.
        if s.waiting[Priority.BATCH_PIPELINE] and s.hi_streak >= s.hi_streak_limit:
            return [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _cpu_cap(pool, prio):
        # Conservative slices to allow concurrency and reduce tail latency from head-of-line blocking.
        if prio == Priority.QUERY:
            frac = 0.50
        elif prio == Priority.INTERACTIVE:
            frac = 0.35
        else:
            frac = 0.25
        cap = pool.max_cpu_pool * frac
        if cap < s.min_cpu_slice:
            cap = s.min_cpu_slice
        return cap

    def _ram_cap(pool, prio):
        # Slightly RAM-favor high priority to reduce OOM churn and retries.
        if prio == Priority.QUERY:
            frac = 0.55
        elif prio == Priority.INTERACTIVE:
            frac = 0.45
        else:
            frac = 0.30
        cap = pool.max_ram_pool * frac
        if cap < s.min_ram_slice:
            cap = s.min_ram_slice
        return cap

    def _is_oom_error(err):
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)

    def _pop_next_runnable_pipeline(prio):
        # Pop until we find a pipeline that is not terminal; keep non-terminal ones for later.
        q = s.waiting[prio]
        while q:
            p = q.popleft()
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            # If pipeline has failures other than OOM, we still keep it; simulator will reflect FAILED ops
            # and get_ops(ASSIGNABLE_STATES) can include FAILED for retry.
            return p
        return None

    def _requeue_pipeline(p):
        s.waiting[p.priority].append(p)

    def _pick_ready_op(p):
        st = p.runtime_status()
        # One operator at a time, only if parents are complete.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        return ops

    # ---- Ingest new pipelines ----
    for p in pipelines:
        s.waiting[p.priority].append(p)

    # ---- Learn from recent execution results ----
    for r in results:
        if getattr(r, "failed", None) and r.failed():
            # Only apply RAM backoff on OOM-like failures.
            if _is_oom_error(getattr(r, "error", None)):
                # Increase RAM for each op reported in the result.
                # Use observed attempted ram as baseline and double it (bounded later by pool max/avail).
                attempted_ram = getattr(r, "ram", None)
                if attempted_ram is None:
                    continue
                for op in getattr(r, "ops", []) or []:
                    key = (getattr(r, "pipeline_id", None), id(op))
                    prev = s.ram_override.get(key, 0)
                    new_override = max(prev, attempted_ram * 2)
                    s.ram_override[key] = new_override

    # Early exit if nothing changed that could influence decisions
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ---- Assign work per pool ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Greedily pack assignments while resources remain.
        # (We still size in slices, so we can place multiple ops in a pool.)
        while pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
            prio_list = _prio_order_with_anti_starvation()

            chosen_pipeline = None
            chosen_ops = None
            chosen_prio = None

            # Pick the highest-priority runnable pipeline with a ready operator.
            for prio in prio_list:
                # Try a bounded number of pops to avoid infinite loops if many are blocked on parents.
                attempts = min(len(s.waiting[prio]), 16)
                for _ in range(attempts):
                    p = _pop_next_runnable_pipeline(prio)
                    if p is None:
                        break
                    ops = _pick_ready_op(p)
                    if ops:
                        chosen_pipeline = p
                        chosen_ops = ops
                        chosen_prio = prio
                        break
                    # Not ready (likely blocked on parents); requeue and keep searching.
                    _requeue_pipeline(p)
                if chosen_pipeline is not None:
                    break

            if chosen_pipeline is None:
                break  # nothing runnable right now in this pool

            # Determine resource request with caps and learned OOM backoff.
            cpu_req = min(pool.avail_cpu_pool, _cpu_cap(pool, chosen_prio))
            ram_req = min(pool.avail_ram_pool, _ram_cap(pool, chosen_prio))

            # Apply per-op RAM override (take max override among chosen ops; we assign only 1 op anyway).
            op0 = chosen_ops[0]
            override_key = (chosen_pipeline.pipeline_id, id(op0))
            if override_key in s.ram_override:
                ram_req = max(ram_req, s.ram_override[override_key])

            # Final clamp to pool availability/max (and ensure >0)
            cpu_req = max(0.0, min(cpu_req, pool.avail_cpu_pool, pool.max_cpu_pool))
            ram_req = max(0.0, min(ram_req, pool.avail_ram_pool, pool.max_ram_pool))

            # If we can't allocate anything meaningful, stop trying in this pool.
            if cpu_req <= 0 or ram_req <= 0:
                _requeue_pipeline(chosen_pipeline)
                break

            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            # Update anti-starvation streak
            if chosen_prio in (Priority.QUERY, Priority.INTERACTIVE):
                s.hi_streak += 1
            else:
                s.hi_streak = 0

            # Requeue pipeline for subsequent operators.
            _requeue_pipeline(chosen_pipeline)

            # Note: pool.avail_* will be updated by the simulator after applying assignments;
            # we rely on current values for loop continuation, which is fine since we break if tight.

            # To avoid overscheduling in one tick based on stale avail, we conservatively exit after one assignment
            # if remaining resources are low.
            if pool.avail_cpu_pool - cpu_req < s.min_cpu_slice or pool.avail_ram_pool - ram_req < s.min_ram_slice:
                break

    return suspensions, assignments
