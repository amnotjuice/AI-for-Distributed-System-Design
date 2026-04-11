# policy_key: scheduler_medium_040
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.085691
# generation_seconds: 74.04
# generated_at: 2026-04-09T23:24:02.060718
@register_scheduler_init(key="scheduler_medium_040")
def scheduler_medium_040_init(s):
    """
    Priority-aware, OOM-avoiding scheduler with conservative sizing + retry backoff.

    Core ideas:
      - Strict priority ordering (QUERY > INTERACTIVE > BATCH) to protect weighted objective.
      - Generous RAM allocation (RAM doesn't speed up execution, but prevents costly OOM/failures).
      - Adaptive per-operator RAM estimate that increases on failures (especially OOM-like errors).
      - Mild fairness: batch makes progress when headroom exists; optional aging-based promotion.
      - Multi-pool placement: steer high priority to "best headroom" pools; keep batch off pool 0
        when possible to preserve interactive latency.
    """
    s.tick = 0

    # Per-priority FIFO queues (store pipeline_ids for stable dedupe)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # pipeline_id -> Pipeline (latest object reference)
    s.pipeline_by_id = {}

    # pipeline_id -> first tick when enqueued (for aging)
    s.enqueued_tick = {}

    # (pipeline_id, op_key) -> estimated RAM needed (in pool RAM units)
    s.op_ram_est = {}

    # (pipeline_id, op_key) -> retry count observed via failures
    s.op_retries = {}

    # (pipeline_id, op_key) -> last observed pool_id (helps keep sizing stable)
    s.op_last_pool = {}

    # Tuning knobs (kept simple and conservative)
    s.batch_aging_ticks = 250          # after this, batch is allowed to contend earlier
    s.reserve_frac_cpu = 0.25          # reserve on pool 0 when high-pri waiting
    s.reserve_frac_ram = 0.25
    s.max_cpu_per_op_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.40,
    }
    s.min_cpu_per_op = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.base_ram_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.25,
    }


def _prio_queue_name(priority):
    if priority == Priority.QUERY:
        return "q_query"
    if priority == Priority.INTERACTIVE:
        return "q_interactive"
    return "q_batch"


def _op_stable_key(op):
    # Try common identifiers; fall back to object id (stable within a simulation run).
    for attr in ("op_id", "operator_id", "task_id", "name", "key"):
        v = getattr(op, attr, None)
        if v is not None:
            return v
    return id(op)


def _is_oom_like(error):
    if not error:
        return False
    s = str(error).lower()
    return ("oom" in s) or ("out of memory" in s) or ("memory" in s and "alloc" in s) or ("killed" in s and "memory" in s)


def _pipeline_done(pipeline):
    status = pipeline.runtime_status()
    return status.is_pipeline_successful()


def _pipeline_has_runnable_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    return ops[0] if ops else None


def _queue_push_unique(q, pipeline_id):
    # Keep a unique FIFO: remove existing instance then append at end.
    try:
        q.remove(pipeline_id)
    except ValueError:
        pass
    q.append(pipeline_id)


def _queue_remove_if_present(q, pipeline_id):
    try:
        q.remove(pipeline_id)
    except ValueError:
        pass


def _any_high_pri_waiting(s):
    # Consider "waiting" if pipeline exists and has a runnable operator.
    for pid in s.q_query + s.q_interactive:
        p = s.pipeline_by_id.get(pid)
        if p and (not _pipeline_done(p)) and _pipeline_has_runnable_op(p):
            return True
    return False


def _pool_order_for_priority(s, priority):
    # Rank pools by headroom; for batch, prefer non-zero pools to protect pool 0.
    pool_ids = list(range(s.executor.num_pools))

    def score(pool_id):
        pool = s.executor.pools[pool_id]
        cpu = pool.avail_cpu_pool / (pool.max_cpu_pool + 1e-9)
        ram = pool.avail_ram_pool / (pool.max_ram_pool + 1e-9)
        # Prefer RAM headroom to avoid OOM (OOM is extremely costly in objective).
        base = 0.65 * ram + 0.35 * cpu
        if priority == Priority.BATCH_PIPELINE and pool_id == 0 and s.executor.num_pools > 1:
            base -= 0.25
        return base

    pool_ids.sort(key=score, reverse=True)
    return pool_ids


def _reserve_for_high_pri(s, pool_id):
    # Keep headroom on pool 0 when any high-priority work exists.
    if pool_id != 0:
        return 0.0, 0.0
    if not _any_high_pri_waiting(s):
        return 0.0, 0.0
    pool = s.executor.pools[pool_id]
    return pool.max_cpu_pool * s.reserve_frac_cpu, pool.max_ram_pool * s.reserve_frac_ram


def _choose_cpu(s, priority, pool_id, avail_cpu):
    pool = s.executor.pools[pool_id]
    min_cpu = min(s.min_cpu_per_op.get(priority, 1.0), avail_cpu)
    cap = pool.max_cpu_pool * s.max_cpu_per_op_frac.get(priority, 0.5)
    cpu = min(avail_cpu, max(min_cpu, cap))
    # Keep CPU somewhat granular and avoid absurdly tiny/huge values.
    return max(0.0, cpu)


def _default_ram_guess(s, priority, pool_id):
    pool = s.executor.pools[pool_id]
    # A conservative baseline that biases against OOM; batch is smaller to allow concurrency.
    return max(0.0, pool.max_ram_pool * s.base_ram_frac.get(priority, 0.30))


def _choose_ram(s, pipeline_id, op, priority, pool_id, avail_ram):
    pool = s.executor.pools[pool_id]
    opk = (pipeline_id, _op_stable_key(op))
    est = s.op_ram_est.get(opk, None)

    # If we've retried multiple times, get aggressive with RAM to stop the failure spiral.
    retries = s.op_retries.get(opk, 0)
    if retries >= 3:
        target = pool.max_ram_pool
    else:
        target = max(_default_ram_guess(s, priority, pool_id), est or 0.0)

        # Small nudge up with retries even if we didn't conclusively detect OOM.
        if retries == 1:
            target *= 1.15
        elif retries == 2:
            target *= 1.35

    # Clamp to pool maximum and available RAM.
    target = min(pool.max_ram_pool, target)
    target = min(avail_ram, target)
    return max(0.0, target)


def _virtual_priority_rank(s, pipeline_id, base_rank):
    # Smaller is better (scheduled earlier).
    t0 = s.enqueued_tick.get(pipeline_id, s.tick)
    age = s.tick - t0
    # Mild aging: after long wait, let batch compete closer to interactive (not above query).
    if base_rank >= 2 and age >= s.batch_aging_ticks:
        return 1.5  # between interactive (1) and batch (2)
    return float(base_rank)


def _get_ranked_pipeline_ids(s):
    # Build a single list of pipeline_ids ordered by (virtual_rank, enqueue_tick).
    ranked = []
    for pid in s.q_query:
        ranked.append(( _virtual_priority_rank(s, pid, 0), s.enqueued_tick.get(pid, 0), pid))
    for pid in s.q_interactive:
        ranked.append(( _virtual_priority_rank(s, pid, 1), s.enqueued_tick.get(pid, 0), pid))
    for pid in s.q_batch:
        ranked.append(( _virtual_priority_rank(s, pid, 2), s.enqueued_tick.get(pid, 0), pid))
    ranked.sort(key=lambda x: (x[0], x[1]))
    return [pid for _, _, pid in ranked]


@register_scheduler(key="scheduler_medium_040")
def scheduler_medium_040(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into priority queues.
      2) Update per-operator RAM estimate based on failures (OOM-like => multiplicative increase).
      3) For each pool, repeatedly place runnable ops by global priority order, with:
         - headroom reservation on pool 0 when high-pri exists,
         - adaptive RAM sizing to avoid OOM,
         - moderate CPU sizing to reduce latency for high priority.
    """
    s.tick += 1

    # Enqueue new pipelines.
    for p in pipelines:
        s.pipeline_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s.enqueued_tick:
            s.enqueued_tick[p.pipeline_id] = s.tick
        qname = _prio_queue_name(p.priority)
        _queue_push_unique(getattr(s, qname), p.pipeline_id)

    # Update estimates from results.
    for r in results:
        if not getattr(r, "ops", None):
            continue
        for op in r.ops:
            opk = (r.pipeline_id, _op_stable_key(op)) if hasattr(r, "pipeline_id") else None
            # If ExecutionResult doesn't expose pipeline_id, fall back to op-only keying using container_id scope.
            if opk is None:
                opk = ("_unknown_pipeline", (r.container_id, _op_stable_key(op)))

            s.op_last_pool[opk] = getattr(r, "pool_id", None)

            if r.failed():
                s.op_retries[opk] = s.op_retries.get(opk, 0) + 1

                pool_id = getattr(r, "pool_id", 0)
                if 0 <= pool_id < s.executor.num_pools:
                    pool = s.executor.pools[pool_id]
                    max_ram = pool.max_ram_pool
                else:
                    max_ram = None

                prev = s.op_ram_est.get(opk, 0.0)
                observed = float(getattr(r, "ram", 0.0) or 0.0)

                if _is_oom_like(getattr(r, "error", None)):
                    # Strong backoff on OOM-like failures.
                    new_est = max(prev, observed) * 1.7 if max(prev, observed) > 0 else (max_ram * 0.6 if max_ram else 0.0)
                else:
                    # Still increase modestly; failure is expensive and often correlates with under-provisioning.
                    new_est = max(prev, observed) * 1.25 if max(prev, observed) > 0 else (max_ram * 0.45 if max_ram else 0.0)

                if max_ram is not None and max_ram > 0:
                    new_est = min(new_est, max_ram)
                if new_est > 0:
                    s.op_ram_est[opk] = new_est

    # Fast path: if nothing changed, do nothing.
    if not pipelines and not results:
        return [], []

    # Clean out completed pipelines from queues.
    for pid, p in list(s.pipeline_by_id.items()):
        if p is None:
            continue
        if _pipeline_done(p):
            _queue_remove_if_present(s.q_query, pid)
            _queue_remove_if_present(s.q_interactive, pid)
            _queue_remove_if_present(s.q_batch, pid)

    suspensions = []
    assignments = []

    # Avoid scheduling multiple ops from the same pipeline within a single tick across pools.
    scheduled_this_tick = set()

    # Global ranked pipeline list (priority + FIFO + mild aging).
    ranked_pids = _get_ranked_pipeline_ids(s)

    # For each pool, attempt to schedule as many ops as fit.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Respect headroom reservation on pool 0 for high-priority work.
        reserve_cpu, reserve_ram = _reserve_for_high_pri(s, pool_id)

        # If this pool is not ideal for certain priority, we still allow placement; placement is handled
        # by filtering candidates based on resources and pool type below.
        made_progress = True
        while made_progress:
            made_progress = False

            # Recompute "effective availability" considering reservation (reservation only blocks low-pri work).
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram

            # Find next runnable op in global order that fits in this pool.
            chosen = None
            chosen_op = None
            chosen_cpu = 0.0
            chosen_ram = 0.0

            for pid in ranked_pids:
                if pid in scheduled_this_tick:
                    continue
                p = s.pipeline_by_id.get(pid)
                if not p or _pipeline_done(p):
                    continue

                op = _pipeline_has_runnable_op(p)
                if not op:
                    continue

                pr = p.priority

                # For batch, avoid pool 0 when there are other pools, unless pool 0 is clearly idle.
                if pr == Priority.BATCH_PIPELINE and pool_id == 0 and s.executor.num_pools > 1:
                    # Only allow batch on pool 0 if there's ample free headroom.
                    if (avail_cpu < pool.max_cpu_pool * 0.60) or (avail_ram < pool.max_ram_pool * 0.60):
                        continue

                # Apply reservation to low-priority candidates on pool 0.
                if pool_id == 0 and (pr == Priority.BATCH_PIPELINE):
                    if (eff_avail_cpu - reserve_cpu) <= 0 or (eff_avail_ram - reserve_ram) <= 0:
                        continue
                    candidate_avail_cpu = eff_avail_cpu - reserve_cpu
                    candidate_avail_ram = eff_avail_ram - reserve_ram
                else:
                    candidate_avail_cpu = eff_avail_cpu
                    candidate_avail_ram = eff_avail_ram

                cpu = _choose_cpu(s, pr, pool_id, candidate_avail_cpu)
                if cpu <= 0:
                    continue
                ram = _choose_ram(s, p.pipeline_id, op, pr, pool_id, candidate_avail_ram)
                if ram <= 0:
                    continue

                # Must fit.
                if cpu <= candidate_avail_cpu + 1e-9 and ram <= candidate_avail_ram + 1e-9:
                    chosen = p
                    chosen_op = op
                    chosen_cpu = cpu
                    chosen_ram = ram
                    break

            if chosen is None:
                break

            assignment = Assignment(
                ops=[chosen_op],
                cpu=chosen_cpu,
                ram=chosen_ram,
                priority=chosen.priority,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id
            )
            assignments.append(assignment)

            scheduled_this_tick.add(chosen.pipeline_id)
            avail_cpu -= chosen_cpu
            avail_ram -= chosen_ram
            made_progress = True

            if avail_cpu <= 0 or avail_ram <= 0:
                break

    # Keep pipelines in queues (no dropping). If a pipeline disappears, it will be cleaned later.
    return suspensions, assignments
