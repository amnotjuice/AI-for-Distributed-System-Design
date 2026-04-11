# policy_key: scheduler_low_050
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046071
# generation_seconds: 37.74
# generated_at: 2026-04-09T21:42:11.588274
@register_scheduler_init(key="scheduler_low_050")
def scheduler_low_050_init(s):
    """
    Priority-aware, completion-biased scheduler with conservative resource sizing.

    Goals (aligned to weighted latency + heavy failure penalty):
      - Prefer QUERY then INTERACTIVE then BATCH to reduce weighted latency.
      - Avoid dropping pipelines; retry failed ops a few times with RAM backoff (OOM-safe bias).
      - Prevent BATCH from consuming the last resources via per-pool reservations, so queries/interactive
        can start quickly (tail-latency protection without preemption).
      - Assign at most one ready operator per pipeline per scheduling step to reduce head-of-line blocking
        and avoid overcommitting unknown RAM minima.
    """
    from collections import deque

    # Per-priority waiting queues (pipelines may re-enter many times until completion)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-pipeline adaptive knobs & bookkeeping
    s.pipe_state = {}  # pipeline_id -> dict(ram_mult, cpu_mult, retries, last_pool_id)
    s.max_retries = 4  # keep small; infinite retries can waste time and still end at 720s penalty

    # "Soft" reservations to protect high-priority latency (applied only when scheduling BATCH).
    # Fractions of pool capacity we try to keep free for QUERY/INTERACTIVE arrivals.
    s.reserve_cpu_frac = 0.25
    s.reserve_ram_frac = 0.25

    # Priority CPU/RAM caps to promote concurrency (avoid giving everything to one op).
    # Multipliers apply to pool capacity, then further scaled by pipeline-level backoff.
    s.cpu_cap_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.35,
    }
    s.ram_cap_frac = {
        Priority.QUERY: 0.70,
        Priority.INTERACTIVE: 0.55,
        Priority.BATCH_PIPELINE: 0.45,
    }


def _q_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _ensure_pipe_state(s, pipeline):
    pid = pipeline.pipeline_id
    if pid not in s.pipe_state:
        s.pipe_state[pid] = {
            "ram_mult": 1.0,      # increases after failures to reduce OOM risk
            "cpu_mult": 1.0,      # modest increases after repeated failures (sometimes CPU timeouts)
            "retries": 0,
            "last_pool_id": None,
        }
    return s.pipe_state[pid]


def _pipeline_terminal(status):
    # Treat "successful" as terminal; otherwise it may still be schedulable (including FAILED ops for retry).
    return status.is_pipeline_successful()


def _pick_ready_op(pipeline):
    status = pipeline.runtime_status()
    # Only schedule operators whose parents are complete. Include FAILED to allow retries.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    # One operator at a time per pipeline to reduce uncertainty and improve fairness across pipelines.
    return op_list[:1]


def _compute_request(s, pool, pipeline, prio):
    pst = s.pipe_state[pipeline.pipeline_id]

    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool
    if avail_cpu <= 0 or avail_ram <= 0:
        return 0, 0

    # Cap CPU/RAM to improve concurrency, then apply pipeline-level multipliers (after failures).
    cpu_cap = max(1.0, pool.max_cpu_pool * s.cpu_cap_frac.get(prio, 0.35) * pst["cpu_mult"])
    ram_cap = max(1.0, pool.max_ram_pool * s.ram_cap_frac.get(prio, 0.45) * pst["ram_mult"])

    cpu = min(avail_cpu, cpu_cap)
    ram = min(avail_ram, ram_cap)

    # Minimum 1 unit requests (simulator uses floats; keep >0 to avoid no-ops).
    if cpu < 1.0:
        cpu = min(avail_cpu, 1.0)
    if ram < 1.0:
        ram = min(avail_ram, 1.0)

    return cpu, ram


@register_scheduler(key="scheduler_low_050")
def scheduler_low_050_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into priority queues.
      2) Update adaptive per-pipeline backoff on failures (RAM-first).
      3) For each pool, schedule work in strict priority order:
           QUERY -> INTERACTIVE -> BATCH
         with BATCH constrained by reservations to avoid starving high priority.
    """
    # Enqueue new arrivals
    for p in pipelines:
        _ensure_pipe_state(s, p)
        _q_for_priority(s, p.priority).append(p)

    # Adaptive backoff from results (failure-biased to improve completion rate).
    # We assume most failures are resource-related (e.g., OOM). RAM backoff reduces repeated failures.
    for r in results:
        if not r.failed():
            continue
        # Try to apply backoff based on the pipeline id carried in the result.
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            continue
        pst = s.pipe_state.get(pid)
        if pst is None:
            continue

        pst["retries"] += 1
        # RAM-first backoff; CPU backoff occasionally to prevent pathological slowdowns.
        pst["ram_mult"] = min(pst["ram_mult"] * 1.6, 3.0)
        if pst["retries"] % 2 == 0:
            pst["cpu_mult"] = min(pst["cpu_mult"] * 1.15, 2.0)

    # Early exit if nothing to do
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []  # no preemption in this "small improvement" policy
    assignments = []

    # Helper to (re)queue pipelines that are still active
    def requeue_pipeline(p):
        _q_for_priority(s, p.priority).append(p)

    # Main scheduling per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # If no resources at all, skip
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Keep attempting to place work while the pool has headroom.
        # Limit iterations to avoid infinite loops if no ops are ready.
        max_tries = 64
        tries = 0

        while tries < max_tries and pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
            tries += 1

            picked = None
            picked_queue = None

            # Strict priority selection: QUERY -> INTERACTIVE -> BATCH
            if s.q_query:
                picked_queue = s.q_query
                picked = picked_queue.popleft()
            elif s.q_interactive:
                picked_queue = s.q_interactive
                picked = picked_queue.popleft()
            elif s.q_batch:
                picked_queue = s.q_batch
                picked = picked_queue.popleft()
            else:
                break

            status = picked.runtime_status()
            pst = _ensure_pipe_state(s, picked)

            # Terminal pipelines are not rescheduled.
            if _pipeline_terminal(status):
                continue

            # Retry budget exhausted: stop spending resources on likely-broken pipelines, but do not drop
            # (we simply stop scheduling; simulator will penalize incomplete with 720s).
            if pst["retries"] > s.max_retries:
                continue

            # Choose a ready op; if none, requeue and try another pipeline.
            ops = _pick_ready_op(picked)
            if not ops:
                requeue_pipeline(picked)
                continue

            prio = picked.priority

            # For BATCH only: enforce soft reservations so queries/interactive can start quickly.
            if prio == Priority.BATCH_PIPELINE:
                reserve_cpu = pool.max_cpu_pool * s.reserve_cpu_frac
                reserve_ram = pool.max_ram_pool * s.reserve_ram_frac
                if pool.avail_cpu_pool <= reserve_cpu or pool.avail_ram_pool <= reserve_ram:
                    # Not enough "excess" resources; defer batch.
                    requeue_pipeline(picked)
                    break

                # Limit batch to the excess portion.
                cpu_excess = max(0.0, pool.avail_cpu_pool - reserve_cpu)
                ram_excess = max(0.0, pool.avail_ram_pool - reserve_ram)
                if cpu_excess < 1.0 or ram_excess < 1.0:
                    requeue_pipeline(picked)
                    break

                cpu_req, ram_req = _compute_request(s, pool, picked, prio)
                cpu_req = min(cpu_req, cpu_excess)
                ram_req = min(ram_req, ram_excess)
            else:
                cpu_req, ram_req = _compute_request(s, pool, picked, prio)

            # If we still can't make a meaningful request, requeue and stop trying on this pool.
            if cpu_req <= 0 or ram_req <= 0:
                requeue_pipeline(picked)
                break

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=picked.pipeline_id,
                )
            )

            pst["last_pool_id"] = pool_id

            # Requeue pipeline for further operators later (unless it becomes terminal next tick)
            requeue_pipeline(picked)

            # Only one assignment per loop iteration; allow pool state to update next tick.
            break

    return suspensions, assignments
