# policy_key: scheduler_low_007
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046589
# generation_seconds: 44.19
# generated_at: 2026-04-09T21:07:26.220841
@register_scheduler_init(key="scheduler_low_007")
def scheduler_low_007_init(s):
    """Priority-aware, completion-focused scheduler with conservative OOM-avoidance.

    Main ideas:
      - Always prefer QUERY then INTERACTIVE then BATCH, but avoid starving lower priorities.
      - Keep small resource reservations so future high-priority arrivals can start quickly.
      - Retry FAILED operators with increased RAM (OOM-avoidance via multiplicative backoff).
      - Prefer pipelines closer to completion (fewer remaining ops) within the same priority.
      - Keep policy simple: one assignment per pool per tick; no preemption (churn is costly).
    """
    # Pipelines currently known to the scheduler (pipeline_id -> Pipeline)
    s._pipelines = {}

    # Arrival order / aging support: pipeline_id -> tick when first seen
    s._arrival_tick = {}

    # Per-pipeline RAM backoff factor (increases on failure; capped by pool max at assignment time)
    s._ram_factor = {}

    # Per-pipeline failure count (to guide aggressiveness; we still keep retrying to avoid 720s penalty)
    s._failures = {}

    # Tick counter for aging; increments each scheduler invocation
    s._tick = 0

    # Configuration knobs (kept conservative; tuned to reduce failures and protect tail latency)
    s._max_retries_soft = 8  # soft cap; we still retry but will allocate more aggressively after this
    s._ram_backoff = 1.6
    s._ram_factor_cap = 16.0

    # Reservation fractions: preserve headroom for future higher-priority work
    # When scheduling BATCH, leave room for both QUERY and INTERACTIVE.
    s._reserve_for_batch_cpu = 0.30
    s._reserve_for_batch_ram = 0.30
    # When scheduling INTERACTIVE, leave some room for QUERY.
    s._reserve_for_interactive_cpu = 0.15
    s._reserve_for_interactive_ram = 0.15

    # Base allocation fractions of pool max RAM by priority (multiplied by ram_factor, capped by avail/max)
    s._base_ram_frac = {
        Priority.QUERY: 0.55,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.28,
    }
    # Base CPU caps as fractions of pool max CPU (QUERY can take all available)
    s._cpu_cap_frac = {
        Priority.QUERY: 1.00,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.55,
    }


@register_scheduler(key="scheduler_low_007")
def scheduler_low_007(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """See init docstring for policy overview."""
    s._tick += 1

    # ---- Helpers (kept local to avoid global imports) ----
    def _is_done_or_impossible(p: Pipeline) -> bool:
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _total_ops(p: Pipeline) -> int:
        # Pipeline.values is described as DAG of operators; treat as sized if possible.
        try:
            return len(p.values)
        except Exception:
            try:
                return sum(1 for _ in p.values)
            except Exception:
                return 0

    def _remaining_ops(p: Pipeline) -> int:
        st = p.runtime_status()
        total = _total_ops(p)
        completed = 0
        try:
            completed = st.state_counts.get(OperatorState.COMPLETED, 0)
        except Exception:
            completed = 0
        if total <= 0:
            # Fallback: count non-completed states if possible.
            try:
                pending = st.state_counts.get(OperatorState.PENDING, 0)
                failed = st.state_counts.get(OperatorState.FAILED, 0)
                assigned = st.state_counts.get(OperatorState.ASSIGNED, 0)
                running = st.state_counts.get(OperatorState.RUNNING, 0)
                suspending = st.state_counts.get(OperatorState.SUSPENDING, 0)
                return pending + failed + assigned + running + suspending
            except Exception:
                return 1
        rem = total - completed
        return rem if rem > 0 else 0

    def _ready_ops_one(p: Pipeline):
        st = p.runtime_status()
        # Prefer parent-complete ops to progress the DAG.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if ops:
            return ops[:1]
        return []

    def _priority_rank(pri) -> int:
        if pri == Priority.QUERY:
            return 0
        if pri == Priority.INTERACTIVE:
            return 1
        return 2

    def _sort_key(p: Pipeline):
        # Lower is better:
        #   - priority rank
        #   - fewer remaining ops (finish sooner -> better latency and completion)
        #   - older arrival (avoid starvation)
        #   - stable tie-breaker by pipeline_id
        arr = s._arrival_tick.get(p.pipeline_id, 0)
        return (_priority_rank(p.priority), _remaining_ops(p), arr, p.pipeline_id)

    def _reserved_for(priority, pool):
        # Return (reserve_cpu, reserve_ram) to keep for higher priorities.
        if priority == Priority.BATCH_PIPELINE:
            return (pool.max_cpu_pool * s._reserve_for_batch_cpu,
                    pool.max_ram_pool * s._reserve_for_batch_ram)
        if priority == Priority.INTERACTIVE:
            return (pool.max_cpu_pool * s._reserve_for_interactive_cpu,
                    pool.max_ram_pool * s._reserve_for_interactive_ram)
        return (0.0, 0.0)

    def _choose_resources(p: Pipeline, pool, avail_cpu: float, avail_ram: float):
        # CPU: cap by priority fraction of pool max, but never exceed availability.
        cpu_cap = pool.max_cpu_pool * s._cpu_cap_frac.get(p.priority, 0.6)
        cpu = min(avail_cpu, cpu_cap)
        if cpu <= 0:
            return 0.0, 0.0

        # RAM: base fraction of pool max * backoff factor; capped by avail.
        base_frac = s._base_ram_frac.get(p.priority, 0.30)
        factor = s._ram_factor.get(p.pipeline_id, 1.0)
        # After many failures, become more aggressive to avoid repeated 720s penalties.
        fail_cnt = s._failures.get(p.pipeline_id, 0)
        if fail_cnt >= s._max_retries_soft:
            factor = max(factor, 4.0)

        target_ram = pool.max_ram_pool * base_frac * factor
        # Ensure we at least allocate something meaningful; but stay within avail.
        ram = min(avail_ram, max(1.0, target_ram))
        if ram <= 0:
            return 0.0, 0.0
        return cpu, ram

    # ---- Incorporate new pipelines ----
    for p in pipelines:
        s._pipelines[p.pipeline_id] = p
        if p.pipeline_id not in s._arrival_tick:
            s._arrival_tick[p.pipeline_id] = s._tick
        s._ram_factor.setdefault(p.pipeline_id, 1.0)
        s._failures.setdefault(p.pipeline_id, 0)

    # ---- Process results (update backoff on failures; clear completed) ----
    for r in results:
        # If a container failed, we assume most common cause is undersized RAM (OOM),
        # so we back off RAM for that pipeline. This is intentionally simple and robust.
        if r.failed():
            pid = None
            # ExecutionResult doesn't explicitly expose pipeline_id in the provided list.
            # But it does include .ops; those ops belong to some pipeline. If pipeline_id
            # isn't available, we conservatively back off based on priority only by not using it.
            #
            # We try best-effort: ops may have a reference back to pipeline_id.
            try:
                if r.ops and hasattr(r.ops[0], "pipeline_id"):
                    pid = r.ops[0].pipeline_id
            except Exception:
                pid = None

            if pid is not None:
                s._failures[pid] = s._failures.get(pid, 0) + 1
                cur = s._ram_factor.get(pid, 1.0)
                nxt = min(s._ram_factor_cap, cur * s._ram_backoff)
                s._ram_factor[pid] = nxt

        # On success completion events, we keep learned RAM factor (it is safe);
        # we don't shrink to avoid oscillation.

    # Early exit if nothing changed and no new arrivals
    if not pipelines and not results:
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # ---- Build candidate list (skip completed pipelines) ----
    active = []
    for pid, p in list(s._pipelines.items()):
        if _is_done_or_impossible(p):
            # Keep state small; finished pipelines don't need scheduling.
            # (We don't delete arrival_tick so ties remain stable if reused ids; unlikely.)
            continue
        active.append(p)

    # Sort once per tick; within each pool we pick the first feasible with a ready op.
    active.sort(key=_sort_key)

    # ---- Schedule: one assignment per pool per tick ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        chosen_pipeline = None
        chosen_ops = None
        chosen_cpu = 0.0
        chosen_ram = 0.0

        # Try in sorted order; reservation logic applied per candidate priority.
        for p in active:
            ops = _ready_ops_one(p)
            if not ops:
                continue

            reserve_cpu, reserve_ram = _reserved_for(p.priority, pool)
            eff_avail_cpu = avail_cpu - reserve_cpu
            eff_avail_ram = avail_ram - reserve_ram

            # If we reserved too aggressively (negative), fall back to minimal slack:
            # allow scheduling if pool is otherwise idle.
            if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                # If there's no assignment yet and this is high priority, allow using full avail.
                if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                    eff_avail_cpu = avail_cpu
                    eff_avail_ram = avail_ram
                else:
                    continue

            cpu, ram = _choose_resources(p, pool, eff_avail_cpu, eff_avail_ram)
            if cpu <= 0 or ram <= 0:
                continue

            chosen_pipeline = p
            chosen_ops = ops
            chosen_cpu = cpu
            chosen_ram = ram
            break

        if chosen_pipeline is None:
            continue

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=chosen_cpu,
                ram=chosen_ram,
                priority=chosen_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

    return suspensions, assignments
