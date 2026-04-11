# policy_key: scheduler_low_030
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.036584
# generation_seconds: 33.96
# generated_at: 2026-03-14T02:25:44.927091
@register_scheduler_init(key="scheduler_low_030")
def scheduler_low_030_init(s):
    """
    Priority-aware, low-risk improvement over naive FIFO.

    Key ideas (kept intentionally simple and robust):
    - Maintain separate FIFO queues per priority and always try higher priority first.
    - If high-priority work is waiting, keep a small resource reserve in each pool
      so batch work doesn't consume everything and inflate tail latency.
    - Basic OOM backoff: if an operator fails with an OOM-like error, retry later
      with a larger RAM request (per-pipeline, capped by pool capacity).

    Notes:
    - We avoid complex preemption because the minimal public interface here does not
      expose a reliable "currently running containers by priority" view.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-pipeline RAM multiplier/backoff after OOM-like failures.
    # Starts at 1.0; increases on OOM; used to scale up future RAM requests.
    s.pipeline_ram_mult = {}

    # Basic caps/knobs (conservative, small improvements over FIFO).
    s.reserve_frac_when_hp_waiting = 0.30  # keep 30% headroom if any high-priority is queued
    s.default_cpu_frac = 0.75              # don't grab 100% CPU by default (reduce interference)
    s.default_ram_frac = 0.75              # don't grab 100% RAM by default (reduce OOM cascades)
    s.oom_backoff_mult = 1.6               # multiply requested RAM after OOM-like failures
    s.max_ram_mult = 8.0                   # cap multiplier to avoid runaway


def _scheduler_low_030_enqueue(s, p):
    # Enqueue by priority into separate FIFO queues.
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _scheduler_low_030_hp_waiting(s):
    # High priority defined as QUERY or INTERACTIVE.
    return bool(s.q_query) or bool(s.q_interactive)


def _scheduler_low_030_pick_queue(s):
    # Strict priority ordering, FIFO within each class.
    if s.q_query:
        return s.q_query
    if s.q_interactive:
        return s.q_interactive
    return s.q_batch


def _scheduler_low_030_is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("killed" in e and "memory" in e)


def _scheduler_low_030_try_get_assignable_op(pipeline):
    status = pipeline.runtime_status()

    # Drop if already done.
    if status.is_pipeline_successful():
        return None, status, "done"

    # If there are any failed operators, we keep the pipeline (might have retries),
    # but we will only try to schedule ops that are in ASSIGNABLE_STATES and whose
    # parents are complete.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    if not op_list:
        return None, status, "blocked"
    return op_list, status, "ok"


@register_scheduler(key="scheduler_low_030")
def scheduler_low_030(s, results, pipelines):
    """
    Scheduler step:
    1) Incorporate new arrivals into per-priority queues.
    2) Learn from results: if an OOM-like failure occurred, increase per-pipeline RAM multiplier.
    3) For each pool, attempt to schedule one ready operator from the highest-priority queue,
       respecting a headroom reserve when high-priority work is waiting.
    """
    # Enqueue new pipelines
    for p in pipelines:
        _scheduler_low_030_enqueue(s, p)
        if p.pipeline_id not in s.pipeline_ram_mult:
            s.pipeline_ram_mult[p.pipeline_id] = 1.0

    # Update simple learning signals from execution results
    for r in results:
        if r.failed() and _scheduler_low_030_is_oom_error(r.error):
            # Increase RAM multiplier for the owning pipeline, so next assignment is larger.
            # r.ops may contain multiple ops but all belong to the same pipeline_id we schedule with.
            # We only have pipeline_id on Assignment, not on result, so we infer via priority queues:
            # best-effort: do nothing without pipeline_id. However, Eudoxia result objects commonly
            # reflect the pipeline_id via r.ops[0].pipeline_id in many setups; attempt safely.
            pid = None
            try:
                if r.ops and hasattr(r.ops[0], "pipeline_id"):
                    pid = r.ops[0].pipeline_id
            except Exception:
                pid = None

            if pid is not None:
                cur = s.pipeline_ram_mult.get(pid, 1.0)
                nxt = min(s.max_ram_mult, cur * s.oom_backoff_mult)
                s.pipeline_ram_mult[pid] = nxt

    # Early exit if nothing to do
    if not pipelines and not results and (not s.q_query and not s.q_interactive and not s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    hp_waiting = _scheduler_low_030_hp_waiting(s)

    # For each pool, schedule at most one operator per tick (simple, stable behavior)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If high-priority work exists, reserve headroom in each pool.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if hp_waiting:
            reserve_cpu = pool.max_cpu_pool * s.reserve_frac_when_hp_waiting
            reserve_ram = pool.max_ram_pool * s.reserve_frac_when_hp_waiting

        # Effective resources available to schedule lower priority work.
        eff_cpu = max(0.0, avail_cpu - reserve_cpu)
        eff_ram = max(0.0, avail_ram - reserve_ram)

        # If we have high-priority work waiting, we still allow high-priority to use full availability.
        # We'll implement this by attempting HP first with full (avail_*), else use effective for batch.
        attempted = 0
        scheduled_one = False

        # We'll pop from a chosen queue, but requeue pipelines we can't schedule now.
        # To avoid starving within a class, we rotate FIFO: pop-left, maybe append-right.
        # We will try a bounded number of attempts per pool to prevent O(n^2).
        max_attempts = (len(s.q_query) + len(s.q_interactive) + len(s.q_batch)) or 0
        max_attempts = min(max_attempts, 32)

        while attempted < max_attempts and not scheduled_one:
            attempted += 1
            q = _scheduler_low_030_pick_queue(s)
            if not q:
                break

            pipeline = q.popleft()

            op_list, status, state = _scheduler_low_030_try_get_assignable_op(pipeline)
            if state == "done":
                # Drop completed pipeline
                continue

            if state != "ok":
                # Not currently schedulable; keep it for later
                q.append(pipeline)
                continue

            # Determine resource request.
            # - For high priority: let it use most of the currently available resources.
            # - For batch: only use effective (headroom-preserving) resources.
            is_hp = pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE)
            base_cpu = avail_cpu if is_hp else eff_cpu
            base_ram = avail_ram if is_hp else eff_ram

            if base_cpu <= 0 or base_ram <= 0:
                # Can't schedule anything of this class in this pool right now; requeue and stop.
                q.appendleft(pipeline)
                break

            # Apply conservative fractions to reduce interference and OOM cascades.
            req_cpu = max(0.1, base_cpu * s.default_cpu_frac)
            req_ram = max(0.1, base_ram * s.default_ram_frac)

            # Apply OOM backoff multiplier for this pipeline (best-effort).
            mult = s.pipeline_ram_mult.get(pipeline.pipeline_id, 1.0)
            req_ram = min(avail_ram, req_ram * mult)

            # Clamp to available resources.
            req_cpu = min(avail_cpu, req_cpu)
            req_ram = min(avail_ram, req_ram)

            if req_cpu <= 0 or req_ram <= 0:
                q.append(pipeline)
                continue

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

            # Requeue the pipeline so later operators can be scheduled in future ticks.
            q.append(pipeline)
            scheduled_one = True

    return suspensions, assignments
