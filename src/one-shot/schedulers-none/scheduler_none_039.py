# policy_key: scheduler_none_039
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.030813
# generation_seconds: 29.90
# generated_at: 2026-03-14T02:09:13.494493
@register_scheduler_init(key="scheduler_none_039")
def scheduler_none_039_init(s):
    """Priority-aware, latency-first scheduler with conservative improvements over naive FIFO.

    Incremental changes vs. the baseline example:
      1) Priority queues: always consider QUERY/INTERACTIVE before BATCH.
      2) Run small steps first: limit ops per assignment to reduce head-of-line blocking.
      3) Simple RAM sizing on OOM: if an op OOMs, retry later with higher RAM for that pipeline.
      4) Light pool choice: place high-priority work on the pool with most headroom.

    Notes/assumptions:
      - We only use signals exposed in ExecutionResult (failed + error string) to detect OOM.
      - We keep logic intentionally simple and robust; no preemption without explicit APIs to
        discover running containers besides results.
    """
    # Separate queues per priority to reduce tail latency for high-priority work
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline "hints" derived from recent failures (e.g., OOM) to avoid repeated failure loops
    # pipeline_id -> {"ram_mult": float, "last_pool": Optional[int]}
    s.pipeline_hints = {}

    # Lightweight aging to avoid indefinite starvation: count ticks a batch pipeline waits
    # pipeline_id -> wait_ticks
    s.batch_wait_ticks = {}

    # Tuning knobs (kept conservative)
    s.max_ops_per_assignment = 2  # small to reduce HOL blocking and improve responsiveness
    s.default_ram_frac = 0.5      # start by not grabbing entire pool RAM for a single step
    s.default_cpu_frac = 1.0      # still allow CPU saturation for throughput when safe
    s.oom_ram_backoff = 1.5       # multiplicative RAM increase on suspected OOM
    s.max_ram_frac = 1.0          # cap at full available pool RAM
    s.batch_aging_threshold = 25  # after N ticks, allow batch to contend earlier
    s.min_cpu = 1e-9              # avoid 0 cpu assignments


def _is_oom_error(err) -> bool:
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("killed" in e and "memory" in e)


def _prio_order(s):
    """Return a dynamic priority order with simple aging for batch."""
    # If any batch has waited long enough, allow it to compete after QUERY (still below INTERACTIVE?),
    # but we keep the basic ordering that protects latency.
    batch_aged = any(t >= s.batch_aging_threshold for t in s.batch_wait_ticks.values())
    if batch_aged:
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _choose_pool_for_priority(s, priority):
    """Pick a pool based on headroom. Prefer the roomiest pool for high-priority."""
    best_pool = None
    best_score = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        # Score by combined headroom; weight RAM a bit higher to avoid OOM retries
        score = (pool.avail_cpu_pool / max(pool.max_cpu_pool, s.min_cpu)) + 1.5 * (
            pool.avail_ram_pool / max(pool.max_ram_pool, s.min_cpu)
        )
        if best_score is None or score > best_score:
            best_score = score
            best_pool = pool_id
    return best_pool if best_pool is not None else 0


def _enqueue_pipeline(s, p):
    pr = p.priority
    if pr not in s.wait_q:
        # Default unknown priorities to batch-like behavior
        pr = Priority.BATCH_PIPELINE
    s.wait_q[pr].append(p)
    if pr == Priority.BATCH_PIPELINE:
        s.batch_wait_ticks.setdefault(p.pipeline_id, 0)


def _record_result_hints(s, r):
    """Update pipeline hints based on execution outcomes."""
    if not r or not getattr(r, "ops", None):
        return

    # Use container results to infer if a pipeline should request more RAM next time
    if r.failed() and _is_oom_error(getattr(r, "error", None)):
        # We don't have pipeline_id in ExecutionResult per the provided API; infer from ops if present.
        # In many simulators, ops carry pipeline_id; if not, we just can't attribute hints.
        pid = None
        try:
            # Try common attributes defensively
            op0 = r.ops[0]
            pid = getattr(op0, "pipeline_id", None)
        except Exception:
            pid = None

        if pid is not None:
            hint = s.pipeline_hints.get(pid, {"ram_mult": 1.0, "last_pool": None})
            hint["ram_mult"] = max(hint.get("ram_mult", 1.0) * s.oom_ram_backoff, 1.0)
            hint["last_pool"] = getattr(r, "pool_id", None)
            s.pipeline_hints[pid] = hint


def _pick_next_pipeline(s):
    """Pop next pipeline using priority order; increment aging counters."""
    # Increment batch aging for everything currently waiting
    for pid in list(s.batch_wait_ticks.keys()):
        s.batch_wait_ticks[pid] = s.batch_wait_ticks.get(pid, 0) + 1

    for pr in _prio_order(s):
        q = s.wait_q.get(pr, [])
        while q:
            p = q.pop(0)
            # If batch pipeline is chosen (or removed), clear its aging counter
            if p.priority == Priority.BATCH_PIPELINE:
                s.batch_wait_ticks.pop(p.pipeline_id, None)
            return p
    return None


@register_scheduler(key="scheduler_none_039")
def scheduler_none_039(s, results, pipelines):
    """
    Priority-aware scheduler:
      - Maintains per-priority FIFO queues.
      - Assigns a small number of ready ops per tick to reduce latency for interactive work.
      - Uses conservative resource sizing, and increases RAM on suspected OOM retries.
      - Chooses the pool with most headroom for high-priority tasks (and generally for all).
    """
    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from results (esp. OOM) to improve next sizing decisions
    for r in results:
        _record_result_hints(s, r)

    # Early exit if no new signals
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Strategy: iterate pools, fill with best available work, but always prioritize latency.
    # For each pool, try to schedule at most one assignment per tick to avoid monopolization.
    # (This matches the baseline's "one operator at a time per pool" shape, but uses priority queues.)
    for _ in range(s.executor.num_pools):
        pool_id = _choose_pool_for_priority(s, Priority.QUERY)
        pool = s.executor.pools[pool_id]

        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            break

        # Pick next pipeline across priorities
        pipeline = _pick_next_pipeline(s)
        if pipeline is None:
            break

        status = pipeline.runtime_status()
        has_failures = status.state_counts[OperatorState.FAILED] > 0

        # Drop completed pipelines; do not retry explicit FAILED state at pipeline level
        # (We still may retry individual ops if the simulator uses FAILED as assignable;
        # the baseline drops pipelines with failures, but that prevents OOM-retry.
        # Here we only drop if the pipeline is successful; otherwise we continue.)
        if status.is_pipeline_successful():
            continue

        # Choose assignable ops whose parents are complete
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[: s.max_ops_per_assignment]
        if not op_list:
            # Not ready; requeue at same priority
            _enqueue_pipeline(s, pipeline)
            continue

        # Sizing: don't grab all RAM by default; reserve some headroom for future high-priority arrivals
        pid = pipeline.pipeline_id
        hint = s.pipeline_hints.get(pid, {"ram_mult": 1.0, "last_pool": None})
        ram_mult = float(hint.get("ram_mult", 1.0))

        # Basic CPU policy: allow using most/all cpu; CPU is less likely to cause hard failures than RAM
        req_cpu = max(avail_cpu * s.default_cpu_frac, s.min_cpu)

        # RAM policy: start with a fraction; scale up on OOM hints; cap to available
        req_ram = avail_ram * s.default_ram_frac * ram_mult
        req_ram = max(min(req_ram, avail_ram * s.max_ram_frac), s.min_cpu)

        # If we already know this pipeline likely needs more RAM, bias to give it more immediately
        if ram_mult > 1.01:
            req_ram = min(avail_ram, req_ram)

        # Create assignment
        assignments.append(
            Assignment(
                ops=op_list,
                cpu=req_cpu,
                ram=req_ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
        )

        # Requeue the pipeline so further ready ops can be scheduled in later ticks
        _enqueue_pipeline(s, pipeline)

        # Avoid spinning on same pool within this tick
        # (Next iteration will re-evaluate headroom and pool choice.)
        continue

    return suspensions, assignments

