# policy_key: scheduler_low_042
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.055913
# generation_seconds: 41.25
# generated_at: 2026-04-09T21:36:20.159923
@register_scheduler_init(key="scheduler_low_042")
def scheduler_low_042_init(s):
    """Priority-aware, RAM-conservative scheduler with OOM-aware retries.

    Core ideas:
      - Keep separate FIFO queues per priority (QUERY, INTERACTIVE, BATCH).
      - Prefer scheduling higher priority work first (dominates weighted latency score).
      - Use small-to-moderate initial RAM allocations to avoid over-reserving, but
        react aggressively to OOM failures by increasing RAM hints (to reduce 720s penalties).
      - Cap CPU per assignment to reduce head-of-line blocking and improve concurrency.
      - Avoid starvation with a simple weighted round-robin pick order.
      - No preemption (kept simple and safe); relies on admission/placement and retry logic.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-pipeline hints and counters
    s.pipeline_ram_hint = {}     # pipeline_id -> ram to request next time (if any)
    s.pipeline_cpu_hint = {}     # pipeline_id -> cpu to request next time (if any)
    s.pipeline_fail_cnt = {}     # pipeline_id -> total failures observed
    s.pipeline_oom_cnt = {}      # pipeline_id -> OOM-like failures observed

    # Weighted RR sequence to avoid starving batch while strongly favoring query/interactive
    s.rr_seq = [Priority.QUERY, Priority.QUERY, Priority.INTERACTIVE, Priority.QUERY,
                Priority.INTERACTIVE, Priority.BATCH]
    s.rr_idx = 0

    # Track which pool a pipeline last ran on (helps locality / reduces oscillation)
    s.pipeline_last_pool = {}  # pipeline_id -> pool_id


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("killed process" in msg and "memory" in msg)


def _enqueue_pipeline(s, p):
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _pick_queue_by_rr(s):
    # Weighted round-robin across priority classes
    for _ in range(len(s.rr_seq)):
        pr = s.rr_seq[s.rr_idx]
        s.rr_idx = (s.rr_idx + 1) % len(s.rr_seq)
        if pr == Priority.QUERY and s.q_query:
            return s.q_query
        if pr == Priority.INTERACTIVE and s.q_interactive:
            return s.q_interactive
        if pr == Priority.BATCH_PIPELINE and s.q_batch:
            return s.q_batch

    # Fallback: any non-empty queue, in strict priority order
    if s.q_query:
        return s.q_query
    if s.q_interactive:
        return s.q_interactive
    if s.q_batch:
        return s.q_batch
    return None


def _initial_ram_fraction(priority) -> float:
    # More conservative for batch to reduce over-reservation; higher for query/interactive to avoid OOM penalties.
    if priority == Priority.QUERY:
        return 0.45
    if priority == Priority.INTERACTIVE:
        return 0.35
    return 0.22


def _cpu_fraction(priority) -> float:
    # Cap CPU to improve concurrency; query gets more to reduce tail latency.
    if priority == Priority.QUERY:
        return 0.70
    if priority == Priority.INTERACTIVE:
        return 0.60
    return 0.50


def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


def _compute_request(s, pipeline, pool):
    # RAM: use per-pipeline hint if available; else a fraction of pool max, but never exceed available.
    pid = pipeline.pipeline_id
    max_ram = pool.max_ram_pool
    avail_ram = pool.avail_ram_pool

    base_ram = _initial_ram_fraction(pipeline.priority) * max_ram
    hinted_ram = s.pipeline_ram_hint.get(pid, base_ram)
    req_ram = _clamp(hinted_ram, 1.0, max_ram)
    req_ram = min(req_ram, avail_ram)

    # CPU: cap by fraction of pool max; never exceed available.
    max_cpu = pool.max_cpu_pool
    avail_cpu = pool.avail_cpu_pool

    base_cpu = _cpu_fraction(pipeline.priority) * max_cpu
    hinted_cpu = s.pipeline_cpu_hint.get(pid, base_cpu)
    req_cpu = _clamp(hinted_cpu, 1.0, max_cpu)
    req_cpu = min(req_cpu, avail_cpu)

    # If RAM is very tight, avoid requesting too much CPU (reduces over-commit style behavior in some sims).
    if avail_ram > 0 and req_ram / max(avail_ram, 1e-9) > 0.85:
        req_cpu = min(req_cpu, max(1.0, 0.5 * avail_cpu))

    return req_cpu, req_ram


def _preferred_pool_order(s, pipeline_id):
    # Try last pool first for stability; then all pools.
    if pipeline_id in s.pipeline_last_pool:
        lp = s.pipeline_last_pool[pipeline_id]
        order = [lp] + [i for i in range(s.executor.num_pools) if i != lp]
        return order
    return list(range(s.executor.num_pools))


@register_scheduler(key="scheduler_low_042")
def scheduler_low_042(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue new pipelines into per-priority FIFOs.
      2) Process results:
          - On OOM-ish failure: increase RAM hint (multiplicative backoff), allow retry.
          - On non-OOM failure: allow limited retries; otherwise stop trying (avoid infinite loop).
          - On success: clear hints for finished pipelines (optional cleanup).
      3) For each pool: schedule at most one runnable operator (like baseline), picking
         pipelines by weighted RR and placing them on a preferred pool order.
    """
    # --- Admission: enqueue new pipelines ---
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # --- Process results: update hints/counters to reduce failures (720s penalties) ---
    for r in results:
        pid = getattr(r, "pipeline_id", None)
        # Some sims may not attach pipeline_id to results; fall back to op metadata if available
        if pid is None:
            pid = None

        if r.failed():
            # If we don't have pid, we can still update "generic" behavior; but most sims should have it.
            if pid is not None:
                s.pipeline_fail_cnt[pid] = s.pipeline_fail_cnt.get(pid, 0) + 1
                if _is_oom_error(r.error):
                    s.pipeline_oom_cnt[pid] = s.pipeline_oom_cnt.get(pid, 0) + 1

                    # Increase RAM hint aggressively to avoid repeated OOMs.
                    # Use last allocated ram if available; else bump current hint.
                    last_ram = getattr(r, "ram", None)
                    cur_hint = s.pipeline_ram_hint.get(pid, None)
                    base = last_ram if (last_ram is not None and last_ram > 0) else (cur_hint if cur_hint else 0.0)
                    if base <= 0:
                        # If no baseline known, start at a reasonable chunk of pool max next time.
                        # We can't know the exact pool, so just scale the existing hint via fallback constant.
                        base = 4.0

                    new_hint = base * 1.6
                    s.pipeline_ram_hint[pid] = new_hint

                    # CPU hint: keep moderate; OOM is RAM issue, but slightly reduce CPU to improve packing.
                    last_cpu = getattr(r, "cpu", None)
                    if last_cpu is not None and last_cpu > 0:
                        s.pipeline_cpu_hint[pid] = max(1.0, last_cpu * 0.9)
        else:
            # On success: keep last pool for stability; don't overfit CPU/RAM upward.
            if pid is not None:
                s.pipeline_last_pool[pid] = r.pool_id

    # Early exit if nothing to do
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Helper to decide if a pipeline should be abandoned (avoid infinite retries)
    def should_abandon(pipeline):
        pid = pipeline.pipeline_id
        fail_cnt = s.pipeline_fail_cnt.get(pid, 0)
        oom_cnt = s.pipeline_oom_cnt.get(pid, 0)

        # OOM retries are valuable (often fixable by RAM bump), but cap them.
        if oom_cnt >= 4:
            return True
        # Other failures: fewer retries; likely deterministic.
        if fail_cnt >= 6:
            return True
        return False

    # Try to schedule one operator per pool per tick (simple, stable, improves over naive via prioritization + RAM backoff)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # We will attempt several picks to find a runnable pipeline; avoid infinite loops with a bounded attempt count.
        attempts = 0
        scheduled = False

        while attempts < 12 and (s.q_query or s.q_interactive or s.q_batch):
            attempts += 1

            # Pick a pipeline from RR, but we may later re-enqueue if not runnable yet.
            q = _pick_queue_by_rr(s)
            if not q:
                break

            pipeline = q.pop(0)
            pid = pipeline.pipeline_id

            status = pipeline.runtime_status()

            # Drop completed pipelines from queues
            if status.is_pipeline_successful():
                # Cleanup hints to avoid unnecessary over-allocation in the future
                s.pipeline_ram_hint.pop(pid, None)
                s.pipeline_cpu_hint.pop(pid, None)
                continue

            # If failures exist, decide whether to keep retrying.
            has_failures = status.state_counts[OperatorState.FAILED] > 0
            if has_failures and should_abandon(pipeline):
                # Stop spending resources; leaving it in queue would cause churn.
                continue

            # Find a runnable op (parents complete); allow retrying FAILED ops (ASSIGNABLE_STATES includes FAILED).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable yet; requeue to check later
                _enqueue_pipeline(s, pipeline)
                continue

            # Placement: prefer last pool for this pipeline, else current pool.
            # If current pool isn't preferred, we can still run here, but we first try to honor preference by skipping.
            pref_order = _preferred_pool_order(s, pid)
            if pool_id != pref_order[0]:
                # If we have other work, defer this pipeline to avoid bouncing; otherwise allow it to run here.
                if (s.q_query or s.q_interactive or s.q_batch) and attempts < 6:
                    _enqueue_pipeline(s, pipeline)
                    continue

            # Compute request sizes
            req_cpu, req_ram = _compute_request(s, pipeline, pool)

            # If we cannot allocate meaningful resources, requeue and move on.
            if req_cpu <= 0 or req_ram <= 0:
                _enqueue_pipeline(s, pipeline)
                break

            assignment = Assignment(
                ops=op_list,
                cpu=req_cpu,
                ram=req_ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pid,
            )
            assignments.append(assignment)
            s.pipeline_last_pool[pid] = pool_id

            # Requeue pipeline for future ops (keeps progress flowing across many pipelines)
            _enqueue_pipeline(s, pipeline)

            scheduled = True
            break

        if not scheduled:
            continue

    return suspensions, assignments
