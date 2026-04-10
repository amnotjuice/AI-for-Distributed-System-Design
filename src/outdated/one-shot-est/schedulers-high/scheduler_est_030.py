# policy_key: scheduler_est_030
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.138116
# generation_seconds: 100.24
# generated_at: 2026-03-31T19:17:45.732042
@register_scheduler_init(key="scheduler_est_030")
def scheduler_est_030_init(s):
    """Priority-aware, estimate-driven scheduler (small, incremental improvements over naive FIFO).

    Improvements vs naive baseline:
      1) Priority queues: always try QUERY > INTERACTIVE > BATCH_PIPELINE to reduce tail latency.
      2) Right-size containers: use op.estimate.mem_peak_gb (plus slack) instead of grabbing the entire pool.
      3) OOM-aware retries: if an op OOMs, increase its RAM multiplier on next attempt (per pipeline+op key).
      4) Soft reservation: when high-priority work is waiting, keep a fraction of each pool unallocated from batch
         to reduce head-of-line blocking (no preemption required).
      5) Avoid duplicate assignment within the same scheduler tick.
    """
    # Per-priority waiting queues of Pipeline objects
    s.waiting_by_pri = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per (pipeline_id, op_id) RAM multiplier learned from OOMs/successes
    s.op_ram_mult = {}  # (pipeline_id, op_key) -> float

    # Pipelines that had a non-OOM failure (treat as fatal to avoid infinite retry loops)
    s.pipeline_fatal = {}  # pipeline_id -> bool


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)


def _op_identifier(op):
    # Try common stable identifiers first; fall back to Python object identity
    return getattr(op, "op_id", getattr(op, "operator_id", id(op)))


def _get_mem_est_gb(op) -> float:
    est_obj = getattr(op, "estimate", None)
    est = getattr(est_obj, "mem_peak_gb", None)
    if est is None:
        return 1.0
    try:
        est = float(est)
    except Exception:
        return 1.0
    return max(0.1, est)


def _priority_order():
    return (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE)


def _cpu_cap_for_priority(priority, hp_waiting_total: int) -> float:
    # Conservative caps to avoid a single op monopolizing a pool when multiple interactive jobs are queued.
    if priority == Priority.QUERY:
        return 8.0 if hp_waiting_total <= 1 else 4.0
    if priority == Priority.INTERACTIVE:
        return 6.0 if hp_waiting_total <= 1 else 3.0
    return 12.0


def _reserve_fractions(pool_id: int):
    # Slightly higher reservation on pool 0 (acts like an implicit "latency" pool if multiple pools exist).
    if pool_id == 0:
        return 0.35, 0.35
    return 0.25, 0.25


def _compute_request(s, pipeline, op, pool, hp_waiting_total: int):
    # RAM: estimate * learned_multiplier + slack, capped by pool max
    op_key = (pipeline.pipeline_id, _op_identifier(op))
    mult = s.op_ram_mult.get(op_key, 1.0)

    base_est = _get_mem_est_gb(op)
    # Small extra slack for high-priority to reduce retries
    slack = 0.75 if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE) else 0.5
    ram_req = base_est * mult + slack
    ram_req = min(ram_req, float(pool.max_ram_pool))
    ram_req = max(0.1, ram_req)

    # CPU: cap by priority, but never below 1
    cpu_cap = _cpu_cap_for_priority(pipeline.priority, hp_waiting_total)
    cpu_req = max(1.0, min(cpu_cap, float(pool.max_cpu_pool)))

    return cpu_req, ram_req


def _enqueue_pipeline(s, p):
    pri = getattr(p, "priority", Priority.BATCH_PIPELINE)
    if pri not in s.waiting_by_pri:
        pri = Priority.BATCH_PIPELINE
    s.waiting_by_pri[pri].append(p)


def _clean_and_get_assignable_op(s, pipeline, scheduled_ops_set):
    status = pipeline.runtime_status()

    # Drop completed pipelines
    if status.is_pipeline_successful():
        return None

    # Drop pipelines marked fatal due to non-OOM failure
    if s.pipeline_fatal.get(pipeline.pipeline_id, False):
        return None

    # Find a single parent-ready assignable op
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    if not op_list:
        return "NOOP"

    op = op_list[0]
    op_key = (pipeline.pipeline_id, _op_identifier(op))
    if op_key in scheduled_ops_set:
        return "NOOP"

    return op


def _try_pick_fitting_from_queue(s, queue, pool, cpu_budget, ram_budget, hp_waiting_total, scheduled_ops_set):
    """Round-robin scan to find a runnable op that fits budgets. Returns (pipeline, op, cpu, ram) or None."""
    n = len(queue)
    for _ in range(n):
        p = queue.pop(0)

        op = _clean_and_get_assignable_op(s, p, scheduled_ops_set)
        if op is None:
            # Drop this pipeline permanently (completed or fatal)
            continue

        # Keep pipeline in the queue for future ops regardless of whether we schedule now
        queue.append(p)

        if op == "NOOP":
            continue

        cpu_req, ram_req = _compute_request(s, p, op, pool, hp_waiting_total)

        # Fit check
        if cpu_req <= cpu_budget and ram_req <= ram_budget:
            return p, op, cpu_req, ram_req

    return None


@register_scheduler(key="scheduler_est_030")
def scheduler_est_030(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Priority-aware, estimate-driven scheduler with soft reservation + OOM-aware RAM growth.

    Returns:
      suspensions: currently unused (no preemption in this incremental version)
      assignments: list of Assignment objects to start executing ops
    """
    # Enqueue new arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Fast no-op: if nothing changed, scheduling decisions can't change
    if not pipelines and not results:
        return [], []

    # Update learning from execution results (OOM -> increase RAM multiplier; success -> slowly decrease)
    for r in results:
        if not getattr(r, "ops", None):
            continue
        op = r.ops[0]
        op_key = (getattr(r, "pipeline_id", None), _op_identifier(op))

        # If pipeline_id isn't present on result, attempt to infer from op if available; else skip learning.
        if op_key[0] is None:
            op_key = (getattr(op, "pipeline_id", None), _op_identifier(op))
        if op_key[0] is None:
            continue

        if r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                cur = s.op_ram_mult.get(op_key, 1.0)
                # Aggressive bump to avoid repeated OOM retries
                s.op_ram_mult[op_key] = min(cur * 1.6, 16.0)
            else:
                # Treat non-OOM failures as fatal (avoid infinite resubmission loops)
                s.pipeline_fatal[op_key[0]] = True
        else:
            # On success, cautiously reduce multiplier (but never below 1.0)
            cur = s.op_ram_mult.get(op_key, 1.0)
            s.op_ram_mult[op_key] = max(1.0, cur * 0.92)

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Local dedupe to prevent assigning the same (pipeline, op) more than once per tick
    scheduled_ops = set()

    # Determine whether high-priority backlog exists (enables batch reservations)
    hp_waiting_total = len(s.waiting_by_pri[Priority.QUERY]) + len(s.waiting_by_pri[Priority.INTERACTIVE])
    has_hp_backlog = hp_waiting_total > 0

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep scheduling until this pool is effectively full or no runnable ops fit.
        while avail_cpu >= 1.0 and avail_ram > 0.1:
            picked = None

            # Try priorities in strict order: QUERY > INTERACTIVE > BATCH
            for pri in _priority_order():
                cpu_budget = avail_cpu
                ram_budget = avail_ram

                # Soft reservation: when HP backlog exists, prevent BATCH from consuming reserved headroom.
                if pri == Priority.BATCH_PIPELINE and has_hp_backlog:
                    cpu_frac, ram_frac = _reserve_fractions(pool_id)

                    # Ensure reservation doesn't deadlock tiny pools (leave at least 1 vCPU and 0.5GB schedulable)
                    reserve_cpu = cpu_frac * float(pool.max_cpu_pool)
                    reserve_ram = ram_frac * float(pool.max_ram_pool)
                    reserve_cpu = max(0.0, min(reserve_cpu, max(0.0, float(pool.max_cpu_pool) - 1.0)))
                    reserve_ram = max(0.0, min(reserve_ram, max(0.0, float(pool.max_ram_pool) - 0.5)))

                    cpu_budget = max(0.0, avail_cpu - reserve_cpu)
                    ram_budget = max(0.0, avail_ram - reserve_ram)

                if cpu_budget < 1.0 or ram_budget <= 0.1:
                    continue

                queue = s.waiting_by_pri[pri]
                if not queue:
                    continue

                picked = _try_pick_fitting_from_queue(
                    s=s,
                    queue=queue,
                    pool=pool,
                    cpu_budget=cpu_budget,
                    ram_budget=ram_budget,
                    hp_waiting_total=hp_waiting_total,
                    scheduled_ops_set=scheduled_ops,
                )
                if picked is not None:
                    break

            if picked is None:
                # Nothing fits on this pool right now
                break

            pipeline, op, cpu_req, ram_req = picked
            op_key = (pipeline.pipeline_id, _op_identifier(op))
            scheduled_ops.add(op_key)

            # Clip to current pool availability (safety)
            cpu_req = max(1.0, min(cpu_req, avail_cpu))
            ram_req = max(0.1, min(ram_req, avail_ram))

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

            avail_cpu -= cpu_req
            avail_ram -= ram_req

    return suspensions, assignments
