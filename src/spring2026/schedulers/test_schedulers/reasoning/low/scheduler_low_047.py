# policy_key: scheduler_low_047
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045455
# generation_seconds: 37.14
# generated_at: 2026-04-09T21:40:08.700908
@register_scheduler_init(key="scheduler_low_047")
def scheduler_low_047_init(s):
    """Priority-aware, failure-averse scheduler.

    Main ideas:
      - Use three queues (QUERY > INTERACTIVE > BATCH) with mild aging to avoid starvation.
      - Place one ready operator per pool per tick (keeps logic simple and reduces OOM blast radius).
      - Conservative initial RAM sizing; on failures, exponentially increase RAM request for that operator
        and retry (pipeline failures are extremely costly in the objective).
      - CPU sizing: prioritize low latency for QUERY/INTERACTIVE but avoid monopolizing the whole pool
        so multiple pipelines can make progress.

    Notes:
      - We intentionally avoid aggressive preemption because the minimal public interface does not
        reliably expose all running containers to choose safe victims; a wrong preemption strategy
        can increase churn and failure rate, hurting the objective.
    """
    # Pipeline queues by priority
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Bookkeeping for resource learning keyed by (pipeline_id, op_key)
    # Values are dicts: {"ram_mult": float, "fail_count": int}
    s.op_learn = {}

    # Arrival order index to enable mild aging
    s._arrival_seq = 0
    s._pipeline_meta = {}  # pipeline_id -> {"arr_seq": int, "priority": Priority}


def _prio_rank(priority):
    # Smaller is better
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _enqueue_pipeline(s, pipeline):
    pid = pipeline.pipeline_id
    if pid not in s._pipeline_meta:
        s._pipeline_meta[pid] = {"arr_seq": s._arrival_seq, "priority": pipeline.priority}
        s._arrival_seq += 1

    if pipeline.priority == Priority.QUERY:
        s.q_query.append(pipeline)
    elif pipeline.priority == Priority.INTERACTIVE:
        s.q_interactive.append(pipeline)
    else:
        s.q_batch.append(pipeline)


def _pop_next_pipeline_with_aging(s):
    """Pick next pipeline with mild aging across the three priority queues.

    We mostly respect priority, but if BATCH/INTERACTIVE has waited long enough, it may jump ahead.
    This avoids indefinite starvation (which risks the 720s penalty for incomplete pipelines).
    """
    # If only one queue has elements, take from it.
    if s.q_query and not s.q_interactive and not s.q_batch:
        return s.q_query.pop(0)
    if s.q_interactive and not s.q_query and not s.q_batch:
        return s.q_interactive.pop(0)
    if s.q_batch and not s.q_query and not s.q_interactive:
        return s.q_batch.pop(0)

    candidates = []
    if s.q_query:
        candidates.append(s.q_query[0])
    if s.q_interactive:
        candidates.append(s.q_interactive[0])
    if s.q_batch:
        candidates.append(s.q_batch[0])

    # Aging threshold in "arrival sequence" units (not time); enough to ensure eventual progress.
    # Lower values => more fairness; higher => more strict priority.
    AGING = 25

    def eff_rank(p):
        meta = s._pipeline_meta.get(p.pipeline_id, {"arr_seq": 0})
        waited = max(0, s._arrival_seq - meta["arr_seq"])
        base = _prio_rank(p.priority)
        # Every AGING steps waited improves rank by 1, capped (can't beat QUERY by more than 2 levels).
        boost = min(2, waited // AGING)
        return max(0, base - boost)

    # Choose candidate with best effective rank; tie-break by older arrival
    best = None
    best_key = None
    for p in candidates:
        meta = s._pipeline_meta.get(p.pipeline_id, {"arr_seq": 0})
        key = (eff_rank(p), meta["arr_seq"])
        if best is None or key < best_key:
            best = p
            best_key = key

    # Pop from the corresponding queue
    if best is None:
        return None
    if best.priority == Priority.QUERY:
        return s.q_query.pop(0)
    if best.priority == Priority.INTERACTIVE:
        return s.q_interactive.pop(0)
    return s.q_batch.pop(0)


def _op_key(op):
    # Use a stable-ish key without relying on internal attributes that may not exist.
    # repr(op) typically includes operator identity; fallback to id(op) to avoid collisions.
    try:
        r = repr(op)
        if r:
            return r
    except Exception:
        pass
    return str(id(op))


def _learn_key(pipeline_id, op):
    return (pipeline_id, _op_key(op))


def _get_or_init_learn(s, pipeline_id, op):
    k = _learn_key(pipeline_id, op)
    if k not in s.op_learn:
        s.op_learn[k] = {"ram_mult": 1.0, "fail_count": 0}
    return s.op_learn[k]


def _update_from_results(s, results):
    """Update RAM multipliers on failures to reduce future OOM risk."""
    for r in results:
        # Some failures might be transient; but objective heavily punishes non-completion, so
        # we bias toward retrying with more RAM.
        if not r.failed():
            continue

        # Try to detect OOM-like failures; if unknown, still increase RAM but less aggressively.
        err = ""
        try:
            err = (r.error or "").lower()
        except Exception:
            err = ""

        oomish = ("oom" in err) or ("out of memory" in err) or ("memory" in err) or ("killed" in err)

        # We may not have pipeline_id on the result; we approximate by applying to each op in r.ops
        # keyed by (UNKNOWN_PIPELINE, op_key) if needed.
        # However, Assignment always provides pipeline_id; many simulators echo it back via ops metadata.
        # We will attempt to use op.pipeline_id if present; else use -1.
        for op in getattr(r, "ops", []) or []:
            pid = getattr(op, "pipeline_id", None)
            if pid is None:
                pid = getattr(r, "pipeline_id", None)
            if pid is None:
                pid = -1

            learn = _get_or_init_learn(s, pid, op)
            learn["fail_count"] += 1

            if oomish:
                # Exponential backoff on RAM to quickly converge to safe sizing.
                # Clamp multiplier to avoid absurd allocations that could block scheduling forever.
                learn["ram_mult"] = min(16.0, max(learn["ram_mult"] * 2.0, 2.0))
            else:
                # Non-OOM failure: small increase to be conservative.
                learn["ram_mult"] = min(16.0, max(learn["ram_mult"] * 1.25, 1.25))


def _cpu_target(priority, avail_cpu, pool_max_cpu):
    """CPU sizing policy."""
    if avail_cpu <= 0:
        return 0

    # Keep some headroom to allow concurrency and reduce tail latency from queueing.
    if priority == Priority.QUERY:
        frac = 0.75
    elif priority == Priority.INTERACTIVE:
        frac = 0.60
    else:
        frac = 0.35

    # Ensure small but non-trivial allocations even on big pools
    cpu = min(avail_cpu, max(1.0, pool_max_cpu * frac))
    return cpu


def _ram_target(s, pipeline, op, avail_ram, pool_max_ram):
    """RAM sizing policy with failure-driven backoff.

    We start by allocating a conservative share of pool RAM, then multiply after failures.
    """
    if avail_ram <= 0:
        return 0

    learn = _get_or_init_learn(s, pipeline.pipeline_id, op)
    mult = learn["ram_mult"]

    # Base allocation: high priority gets more RAM headroom to reduce OOM risk.
    if pipeline.priority == Priority.QUERY:
        base_frac = 0.60
    elif pipeline.priority == Priority.INTERACTIVE:
        base_frac = 0.50
    else:
        base_frac = 0.40

    base = max(1.0, pool_max_ram * base_frac)
    ram = min(avail_ram, base * mult)

    # If we have very little available, just take what's available; better to run than to stall forever.
    ram = max(1.0, min(avail_ram, ram))
    return ram


@register_scheduler(key="scheduler_low_047")
def scheduler_low_047(s, results, pipelines):
    """
    Scheduling step:
      1) Ingest new pipelines into priority queues.
      2) Update per-operator RAM multipliers based on failure signals.
      3) For each pool, schedule at most one ready operator from the best available pipeline
         (priority + aging), sizing CPU/RAM per the heuristics above.
    """
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    _update_from_results(s, results)

    suspensions = []
    assignments = []

    # We'll rotate through pools and try to dispatch one op per pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try a bounded number of attempts to find a runnable op without getting stuck.
        attempts = (len(s.q_query) + len(s.q_interactive) + len(s.q_batch))
        attempts = min(attempts, 64)

        requeue = []
        assigned = False

        for _ in range(attempts):
            pipeline = _pop_next_pipeline_with_aging(s)
            if pipeline is None:
                break

            status = pipeline.runtime_status()

            # Drop completed pipelines from queues
            if status.is_pipeline_successful():
                continue

            # We do not permanently drop failed pipelines here; they are costly.
            # Only skip if there are no assignable ops (e.g., waiting on parents).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Keep pipeline in circulation
                requeue.append(pipeline)
                continue

            # Size resources
            cpu = _cpu_target(pipeline.priority, avail_cpu, pool.max_cpu_pool)
            ram = _ram_target(s, pipeline, op_list[0], avail_ram, pool.max_ram_pool)

            if cpu <= 0 or ram <= 0:
                requeue.append(pipeline)
                continue

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )
            assigned = True
            break

        # Requeue pipelines we popped but didn't schedule this round.
        for p in requeue:
            _enqueue_pipeline(s, p)

        # If we didn't assign, nothing to do for this pool.
        if not assigned:
            continue

    return suspensions, assignments
