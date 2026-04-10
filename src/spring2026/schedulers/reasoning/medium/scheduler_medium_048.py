# policy_key: scheduler_medium_048
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.085817
# generation_seconds: 82.16
# generated_at: 2026-04-09T23:35:22.982849
@register_scheduler_init(key="scheduler_medium_048")
def scheduler_medium_048_init(s):
    """
    Priority-aware, OOM-averse, non-preemptive scheduler.

    Core ideas:
      - Strong preference to QUERY then INTERACTIVE, while ensuring BATCH makes progress via aging.
      - Soft reservations: avoid consuming the last headroom with BATCH when high-priority work exists.
      - OOM backoff: if an operator fails with an OOM-like error, retry with increased RAM (per-operator hint).
      - Conservative initial RAM sizing (by priority) to reduce failures (failures are heavily penalized).
      - Limited per-pipeline concurrency to reduce thrash and keep latency predictable.
    """
    # Logical clock for aging (increments each scheduler tick).
    s._tick = 0

    # Track pipelines and their arrival time for aging/fairness.
    s._pipelines_by_id = {}          # pipeline_id -> Pipeline
    s._arrival_tick = {}             # pipeline_id -> tick

    # Per-operator resource hints learned from failures (best-effort keys).
    s._op_ram_hint = {}              # op_key -> ram
    s._op_retry_count = {}           # op_key -> int

    # Cache of pool maxima (filled lazily).
    s._pool_max = None


@register_scheduler(key="scheduler_medium_048")
def scheduler_medium_048(s, results, pipelines):
    """
    Scheduling step:
      - Incorporate new arrivals.
      - Learn from execution results (OOM backoff).
      - For each pool, greedily fill available resources with ready operators, prioritizing:
          QUERY > INTERACTIVE > BATCH (with aging), while respecting soft reservations for BATCH.
      - No preemption (keeps wasted work/churn low and avoids relying on executor internals).
    """
    s._tick += 1

    def _prio_rank(p):
        # Higher is more important.
        if p == Priority.QUERY:
            return 3
        if p == Priority.INTERACTIVE:
            return 2
        return 1  # Priority.BATCH_PIPELINE

    def _base_score(priority):
        # Strongly bias to protect QUERY/INTERACTIVE tail latency.
        if priority == Priority.QUERY:
            return 1000.0
        if priority == Priority.INTERACTIVE:
            return 400.0
        return 50.0

    def _age_gain(priority):
        # Ensure batch progress without overwhelming latency-sensitive work.
        if priority == Priority.QUERY:
            return 0.2
        if priority == Priority.INTERACTIVE:
            return 0.6
        return 2.0

    def _max_inflight(priority):
        # Limit per-pipeline parallelism to reduce interference and keep scheduling stable.
        if priority == Priority.QUERY:
            return 3
        if priority == Priority.INTERACTIVE:
            return 2
        return 1

    def _op_key(op):
        # Best-effort stable key across retries; falls back to repr(op).
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if callable(v):
                        v = v()
                    return f"{attr}:{v}"
                except Exception:
                    pass
        return f"repr:{repr(op)}"

    def _oom_like(err):
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("cuda out of memory" in e) or ("killed process" in e)

    def _effective_score(pipeline):
        # Weighted by priority plus aging from arrival tick.
        at = s._arrival_tick.get(pipeline.pipeline_id, s._tick)
        age = max(0, s._tick - at)
        return _base_score(pipeline.priority) + _age_gain(pipeline.priority) * age

    def _cleanup_finished():
        # Drop successful pipelines from tracking (do NOT drop incomplete/failed; we want retries).
        to_delete = []
        for pid, p in s._pipelines_by_id.items():
            st = p.runtime_status()
            if st.is_pipeline_successful():
                to_delete.append(pid)
        for pid in to_delete:
            s._pipelines_by_id.pop(pid, None)
            s._arrival_tick.pop(pid, None)

    def _ready_op(pipeline):
        st = pipeline.runtime_status()
        # Avoid over-parallelizing within a pipeline.
        inflight = 0
        try:
            inflight = st.state_counts.get(OperatorState.RUNNING, 0) + st.state_counts.get(OperatorState.ASSIGNED, 0)
        except Exception:
            # If state_counts isn't dict-like, just proceed without limiting.
            inflight = 0
        if inflight >= _max_inflight(pipeline.priority):
            return None
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _suggest_ram(priority, pool, remaining_ram, opk):
        # Conservative starting sizes to reduce OOM risk (failures are extremely costly).
        hint = s._op_ram_hint.get(opk, None)
        if hint is not None:
            # Use learned hint, but don't exceed available.
            return min(float(hint), float(remaining_ram))

        frac = 0.40
        if priority == Priority.QUERY:
            frac = 0.65
        elif priority == Priority.INTERACTIVE:
            frac = 0.55

        target = float(pool.max_ram_pool) * frac
        # Ensure a meaningful allocation even on small pools.
        target = max(target, float(pool.max_ram_pool) * 0.10)
        return min(float(remaining_ram), target)

    def _suggest_cpu(priority, pool, remaining_cpu, opk):
        # Favor more CPU for latency-sensitive work.
        frac = 0.35
        if priority == Priority.QUERY:
            frac = 0.80
        elif priority == Priority.INTERACTIVE:
            frac = 0.65

        # If we've retried due to failures, slightly increase CPU to reduce chance of timeouts.
        retries = s._op_retry_count.get(opk, 0)
        frac = min(0.95, frac + 0.05 * min(retries, 3))

        target = float(pool.max_cpu_pool) * frac
        target = max(1.0, target)
        return min(float(remaining_cpu), target)

    # Initialize pool maxima cache if needed.
    if s._pool_max is None:
        s._pool_max = []
        for i in range(s.executor.num_pools):
            pool = s.executor.pools[i]
            s._pool_max.append((float(pool.max_cpu_pool), float(pool.max_ram_pool)))

    # Incorporate new arrivals.
    for p in pipelines:
        s._pipelines_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s._arrival_tick:
            s._arrival_tick[p.pipeline_id] = s._tick

    # Learn from results (OOM backoff / retry hints).
    for r in results:
        if r is None:
            continue
        if not r.failed():
            continue

        pool = s.executor.pools[r.pool_id]
        max_ram_cap = float(pool.max_ram_pool)

        scale = 2.0 if _oom_like(getattr(r, "error", None)) else 1.5
        # Update hints for each failed op in the container.
        for op in getattr(r, "ops", []) or []:
            k = _op_key(op)
            prev = s._op_ram_hint.get(k, None)
            if prev is None:
                prev = float(getattr(r, "ram", 0.0) or 0.0)
                if prev <= 0:
                    # If result doesn't provide a RAM number, start with a conservative guess.
                    prev = 0.50 * max_ram_cap

            new_hint = max(prev, float(getattr(r, "ram", prev) or prev) * scale)
            # If OOM-like and still small, jump more aggressively.
            if _oom_like(getattr(r, "error", None)) and new_hint < 0.60 * max_ram_cap:
                new_hint = max(new_hint, 0.70 * max_ram_cap)

            s._op_ram_hint[k] = min(new_hint, max_ram_cap)
            s._op_retry_count[k] = s._op_retry_count.get(k, 0) + 1

    _cleanup_finished()

    # Early exit if nothing to do.
    if not pipelines and not results and not s._pipelines_by_id:
        return [], []

    suspensions = []  # Non-preemptive policy.
    assignments = []

    # Pre-compute whether high-priority work exists (for soft reservations vs batch).
    def _has_waiting_high_priority():
        for p in s._pipelines_by_id.values():
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE) and _ready_op(p) is not None:
                return True
        return False

    # For each pool, greedily assign ready ops.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        remaining_cpu = float(pool.avail_cpu_pool)
        remaining_ram = float(pool.avail_ram_pool)

        if remaining_cpu <= 0 or remaining_ram <= 0:
            continue

        # Soft reservations: keep a slice of each pool for high priority if any exists.
        # (Only restrict BATCH; QUERY/INTERACTIVE can use everything.)
        reserve_cpu = 0.15 * float(pool.max_cpu_pool)
        reserve_ram = 0.15 * float(pool.max_ram_pool)

        # If pool_id == 0, bias it towards latency work (but still allow batch if no latency work is ready).
        latency_pool_bias = (pool_id == 0)

        # Local helper to pick best next pipeline for this pool.
        def _pick_candidate(allow_batch):
            best = None
            best_op = None
            best_score = None
            best_arrival = None

            for p in s._pipelines_by_id.values():
                # Optional bias: keep pool 0 mostly for latency-sensitive work.
                if latency_pool_bias and p.priority == Priority.BATCH_PIPELINE and not allow_batch:
                    continue

                op = _ready_op(p)
                if op is None:
                    continue

                if (not allow_batch) and (p.priority == Priority.BATCH_PIPELINE):
                    continue

                score = _effective_score(p)
                at = s._arrival_tick.get(p.pipeline_id, s._tick)

                if (best is None) or (score > best_score) or (score == best_score and at < best_arrival):
                    best = p
                    best_op = op
                    best_score = score
                    best_arrival = at

            return best, best_op

        # Fill the pool with multiple assignments if resources allow.
        # (Greedy; keeps code simple and generally improves throughput vs single-assignment.)
        while remaining_cpu > 0 and remaining_ram > 0:
            high_waiting = _has_waiting_high_priority()

            # On the latency-biased pool, only allow batch if no high-priority is ready anywhere.
            allow_batch_on_pool = (not high_waiting)
            if not latency_pool_bias:
                allow_batch_on_pool = True  # other pools can run batch concurrently

            # First pick from QUERY/INTERACTIVE if any ready; else allow batch depending on rules.
            cand, op = _pick_candidate(allow_batch=True)
            if cand is None or op is None:
                break

            # If batch and high-priority exists, enforce soft reservations.
            opk = _op_key(op)
            cpu_req = _suggest_cpu(cand.priority, pool, remaining_cpu, opk)
            ram_req = _suggest_ram(cand.priority, pool, remaining_ram, opk)

            # Ensure strictly positive allocations.
            if cpu_req <= 0:
                cpu_req = min(1.0, remaining_cpu)
            if ram_req <= 0:
                ram_req = remaining_ram

            # Enforce latency pool batch gating and reservations.
            if cand.priority == Priority.BATCH_PIPELINE:
                if latency_pool_bias and not allow_batch_on_pool:
                    # High-priority exists; do not schedule batch here.
                    break

                if high_waiting:
                    # Don't consume into reserved headroom with batch if high-priority exists.
                    if (remaining_cpu - cpu_req) < reserve_cpu or (remaining_ram - ram_req) < reserve_ram:
                        # Try to find a non-batch candidate instead.
                        cand2, op2 = _pick_candidate(allow_batch=False)
                        if cand2 is None or op2 is None:
                            # No non-batch available; keep headroom and stop scheduling batch on this pool.
                            break
                        cand, op = cand2, op2
                        opk = _op_key(op)
                        cpu_req = _suggest_cpu(cand.priority, pool, remaining_cpu, opk)
                        ram_req = _suggest_ram(cand.priority, pool, remaining_ram, opk)
                        if cpu_req <= 0:
                            cpu_req = min(1.0, remaining_cpu)
                        if ram_req <= 0:
                            ram_req = remaining_ram

            # Final feasibility check.
            if cpu_req > remaining_cpu or ram_req > remaining_ram or cpu_req <= 0 or ram_req <= 0:
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=cand.priority,
                    pool_id=pool_id,
                    pipeline_id=cand.pipeline_id,
                )
            )

            remaining_cpu -= float(cpu_req)
            remaining_ram -= float(ram_req)

            # Prevent pathological micro-fragmentation loops.
            if remaining_cpu < 0.5 or remaining_ram < 1e-9:
                break

    return suspensions, assignments
