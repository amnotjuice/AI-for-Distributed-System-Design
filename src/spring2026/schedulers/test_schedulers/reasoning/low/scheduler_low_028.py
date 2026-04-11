# policy_key: scheduler_low_028
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.067127
# generation_seconds: 48.56
# generated_at: 2026-04-09T21:25:15.149883
@register_scheduler_init(key="scheduler_low_028")
def scheduler_low_028_init(s):
    """
    Priority-aware, failure-averse scheduler with conservative resource sizing.

    Main ideas:
    - Strictly prefer QUERY then INTERACTIVE then BATCH to reduce weighted latency.
    - Avoid pipeline failures (720s penalty) by retrying FAILED operators with RAM backoff
      when failure looks like OOM; cap retries to prevent infinite loops.
    - Protect high-priority work by reserving some pool headroom when queries/interactive
      are waiting, and throttle batch CPU allocations under contention.
    - Mild anti-starvation: if batches wait "too long" and no high-priority backlog,
      schedule them more aggressively.
    """
    from collections import deque

    # Priority queues of pipelines
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Bookkeeping
    s._tick = 0

    # Per-(pipeline, op) retry/backoff state
    # key -> {"attempts": int, "ram_mult": float, "last_ram": float}
    s._op_state = {}

    # Per-pipeline enqueue tick (for mild aging)
    s._enqueue_tick = {}

    # Limits
    s._max_oom_retries = 3
    s._max_other_retries = 1  # be conservative on non-OOM errors


def _sl028_is_oom_error(err) -> bool:
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memoryerror" in e) or ("killed" in e and "memory" in e)


def _sl028_pqueue(s, priority):
    # Map platform priorities to internal queues
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _sl028_op_key(pipeline, op):
    # Use (pipeline_id, id(op)) for stable hashing regardless of op object hashability
    return (pipeline.pipeline_id, id(op))


def _sl028_base_targets(priority, pool):
    """
    Choose baseline CPU/RAM targets per operator based on priority and pool size.
    RAM beyond minimum doesn't speed up, but under-alloc risks OOM => be conservative.
    """
    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    # Baseline CPU fractions (interactive/query faster; batch throttled)
    if priority == Priority.QUERY:
        cpu_frac = 0.80
        ram_frac = 0.55
    elif priority == Priority.INTERACTIVE:
        cpu_frac = 0.65
        ram_frac = 0.45
    else:
        cpu_frac = 0.45
        ram_frac = 0.35

    cpu_tgt = max(1.0, max_cpu * cpu_frac) if max_cpu > 0 else 0.0
    ram_tgt = max(0.0, max_ram * ram_frac)

    return cpu_tgt, ram_tgt


def _sl028_pick_pool_for_priority(s, priority, pools_avail):
    """
    Simple placement:
    - If multiple pools exist, prefer pool 0 for QUERY, pool 1 for INTERACTIVE (if present),
      otherwise choose the pool with most available RAM (OOM-avoidant) then CPU.
    """
    if s.executor.num_pools <= 1:
        return 0

    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1 if s.executor.num_pools > 1 else 0

    # For batch: choose best-fit by availability (more headroom => fewer failures)
    best = 0
    best_key = None
    for i in range(s.executor.num_pools):
        avail = pools_avail[i]
        key = (avail["ram"], avail["cpu"])
        if best_key is None or key > best_key:
            best_key = key
            best = i
    return best


def _sl028_pop_next_runnable_pipeline(s, require_parents_complete=True):
    """
    Choose next pipeline to schedule:
    - QUERY first, then INTERACTIVE, then BATCH.
    - Mild aging for batch: if no query/interactive waiting and some batches waited long,
      pull them preferentially.
    """
    # If any high-priority waiting, always serve them first.
    if s.q_query:
        return s.q_query.popleft()
    if s.q_interactive:
        return s.q_interactive.popleft()

    # No high priority backlog; allow batches.
    if not s.q_batch:
        return None

    # Mild aging: pick the oldest batch pipeline (linear scan; queues are small in sim).
    oldest_idx = 0
    oldest_tick = None
    for idx, p in enumerate(s.q_batch):
        t = s._enqueue_tick.get(p.pipeline_id, 0)
        if oldest_tick is None or t < oldest_tick:
            oldest_tick = t
            oldest_idx = idx

    # If oldest waited sufficiently, pull it; else FIFO.
    waited = s._tick - (oldest_tick if oldest_tick is not None else s._tick)
    if waited >= 30:
        p = s.q_batch[oldest_idx]
        del s.q_batch[oldest_idx]
        return p

    return s.q_batch.popleft()


@register_scheduler(key="scheduler_low_028")
def scheduler_low_028(s, results, pipelines):
    """
    Priority-first, RAM-backoff retry policy.

    Behavior:
    - Enqueue arrivals into per-priority queues.
    - Process execution results:
        * On OOM-like failures, increase RAM multiplier and allow retry (up to cap).
        * On other failures, allow very limited retry to avoid endless churn.
    - Assign operators to pools while:
        * Favoring high-priority latency.
        * Avoiding failures by allocating conservative RAM (plus backoff).
        * Throttling batch CPU when any high-priority pipeline is waiting.
    """
    s._tick += 1

    # Enqueue new pipelines
    for p in pipelines:
        _sl028_pqueue(s, p.priority).append(p)
        s._enqueue_tick[p.pipeline_id] = s._tick

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # Update backoff state from results
    for r in results:
        # Ignore successes
        if not hasattr(r, "failed") or not r.failed():
            continue

        # Increase backoff for each failed op
        for op in getattr(r, "ops", []) or []:
            key = (getattr(r, "pipeline_id", None), id(op))
            # If pipeline_id isn't present on result, we can't key reliably; fallback to op-only key.
            if key[0] is None:
                key = ("unknown", id(op))

            st = s._op_state.get(key, {"attempts": 0, "ram_mult": 1.0, "last_ram": float(getattr(r, "ram", 0.0) or 0.0)})
            st["attempts"] += 1
            st["last_ram"] = float(getattr(r, "ram", st.get("last_ram", 0.0)) or st.get("last_ram", 0.0))

            if _sl028_is_oom_error(getattr(r, "error", None)):
                # RAM backoff for OOM-like failures
                st["ram_mult"] = max(st["ram_mult"] * 1.6, 1.6)
            else:
                # Keep RAM multiplier mostly stable for non-OOM failures
                st["ram_mult"] = max(st["ram_mult"], 1.0)

            s._op_state[key] = st

    suspensions = []
    assignments = []

    # Snapshot pool availability (we'll update locally as we assign)
    pools_avail = []
    for i in range(s.executor.num_pools):
        pool = s.executor.pools[i]
        pools_avail.append(
            {"cpu": float(pool.avail_cpu_pool), "ram": float(pool.avail_ram_pool)}
        )

    # If high-priority waiting, reserve headroom by reducing effective capacity for batch
    high_waiting = bool(s.q_query) or bool(s.q_interactive)
    batch_cpu_cap_frac_under_contention = 0.35  # limit per-batch assignment CPU share when high-pri waiting

    # Try to schedule multiple assignments per tick, across pools
    # To avoid starvation and to reduce tail latencies, we do several "rounds".
    max_rounds = 8

    for _ in range(max_rounds):
        made_progress = False

        # Stop if no capacity anywhere
        if all(a["cpu"] <= 0.0 or a["ram"] <= 0.0 for a in pools_avail):
            break

        # Pick a pipeline to run next
        pipeline = _sl028_pop_next_runnable_pipeline(s, require_parents_complete=True)
        if pipeline is None:
            break

        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            continue

        # Determine which ops can run
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Nothing runnable now; requeue to preserve progress
            _sl028_pqueue(s, pipeline.priority).append(pipeline)
            continue

        op = op_list[0]
        pool_id = _sl028_pick_pool_for_priority(s, pipeline.priority, pools_avail)
        pool = s.executor.pools[pool_id]
        avail_cpu = pools_avail[pool_id]["cpu"]
        avail_ram = pools_avail[pool_id]["ram"]

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            # Try alternate pool if this one is empty
            alt = None
            best_key = None
            for i in range(s.executor.num_pools):
                if pools_avail[i]["cpu"] <= 0.0 or pools_avail[i]["ram"] <= 0.0:
                    continue
                key = (pools_avail[i]["ram"], pools_avail[i]["cpu"])
                if best_key is None or key > best_key:
                    best_key = key
                    alt = i
            if alt is None:
                _sl028_pqueue(s, pipeline.priority).appendleft(pipeline)
                break
            pool_id = alt
            pool = s.executor.pools[pool_id]
            avail_cpu = pools_avail[pool_id]["cpu"]
            avail_ram = pools_avail[pool_id]["ram"]

        # Compute resource request
        base_cpu_tgt, base_ram_tgt = _sl028_base_targets(pipeline.priority, pool)

        # Apply failure backoff if any
        key = _sl028_op_key(pipeline, op)
        st = s._op_state.get(key, {"attempts": 0, "ram_mult": 1.0, "last_ram": 0.0})
        attempts = int(st.get("attempts", 0))
        ram_mult = float(st.get("ram_mult", 1.0))
        last_ram = float(st.get("last_ram", 0.0))

        # Cap retries to avoid endless churn; if exceeded, let it fail naturally (no drops here).
        # We simply avoid scheduling it this tick, requeue behind others to reduce wasted time.
        if attempts > s._max_oom_retries + s._max_other_retries:
            _sl028_pqueue(s, pipeline.priority).append(pipeline)
            continue

        # RAM request: conservative baseline, and if we saw a failure, bump from last_ram
        ram_req = base_ram_tgt
        if last_ram > 0.0:
            ram_req = max(ram_req, last_ram * ram_mult)

        # CPU request: give queries more; throttle batch under contention
        cpu_req = base_cpu_tgt
        if pipeline.priority == Priority.BATCH_PIPELINE and high_waiting:
            cpu_req = min(cpu_req, float(pool.max_cpu_pool) * batch_cpu_cap_frac_under_contention)

        # Fit to availability
        cpu_req = min(cpu_req, avail_cpu)
        ram_req = min(ram_req, avail_ram)

        # Ensure at least minimal CPU if we schedule at all
        if cpu_req <= 0.0 or ram_req <= 0.0:
            # No resources right now; requeue and stop trying this round
            _sl028_pqueue(s, pipeline.priority).appendleft(pipeline)
            continue

        # Avoid allocating extremely tiny RAM slices that almost guarantee OOM:
        # If the pool is heavily fragmented (low avail), skip this pipeline this tick.
        if ram_req < float(pool.max_ram_pool) * 0.08:
            _sl028_pqueue(s, pipeline.priority).append(pipeline)
            continue

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
        )

        # Update local availability
        pools_avail[pool_id]["cpu"] -= cpu_req
        pools_avail[pool_id]["ram"] -= ram_req
        made_progress = True

        # Requeue pipeline for subsequent operators (or retries), to allow pipelined progress
        _sl028_pqueue(s, pipeline.priority).append(pipeline)

        if not made_progress:
            break

    return suspensions, assignments
