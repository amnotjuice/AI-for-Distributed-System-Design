# policy_key: scheduler_none_012
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.063221
# generation_seconds: 46.36
# generated_at: 2026-04-09T21:54:23.372892
@register_scheduler_init(key="scheduler_none_012")
def scheduler_none_012_init(s):
    """Priority-aware, failure-averse scheduler with simple aging and OOM backoff.

    Design goals for the weighted-latency objective:
      - Strongly protect QUERY and INTERACTIVE latency (dominant weights).
      - Avoid failures (each failure/incomplete pipeline is extremely costly).
      - Keep batch making progress (aging) to avoid starvation/incomplete penalties.
      - Keep the policy simple and robust: single-op assignments, conservative CPU,
        and adaptive RAM on OOM (retry with larger RAM).
    """
    # Per-pipeline bookkeeping
    s.waiting_queue = []  # FIFO of pipeline_ids; we will re-rank when scheduling
    s.pipelines_by_id = {}

    # Aging: track when we first saw a pipeline (sim-time seconds if available, else tick counter)
    s.pipeline_first_seen_ts = {}

    # Retry sizing: pipeline_id -> {op_id -> {"ram_mult": float, "last_cpu": float}}
    # We conservatively inflate RAM on OOM to reduce repeat failures.
    s.op_retry = {}

    # Track an internal tick for deterministic aging if sim-time is not exposed.
    s._tick = 0

    # Control knobs (kept intentionally simple)
    s.query_pool_hint = 0  # prefer pool 0 for high-priority work when possible

    # Aging coefficients (in "priority points" per second)
    s.aging_per_sec = {
        Priority.QUERY: 0.0,          # already highest priority
        Priority.INTERACTIVE: 0.002,  # slight aging
        Priority.BATCH_PIPELINE: 0.006  # stronger aging to prevent starvation
    }

    # CPU sizing: don't grab entire pool; prefer moderate parallelism and leave headroom.
    s.cpu_target_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50
    }

    # RAM sizing: allocate a conservative share of pool for a single op to reduce OOM risk.
    s.ram_target_frac = {
        Priority.QUERY: 0.70,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.55
    }

    # Hard caps to prevent one assignment from monopolizing a pool entirely
    s.cpu_cap_frac = 0.85
    s.ram_cap_frac = 0.85

    # OOM backoff multipliers
    s.oom_ram_mult_initial = 1.25
    s.oom_ram_mult_max = 3.0

    # If an op fails without clear OOM info, still retry with slight RAM bump once
    s.generic_fail_ram_mult_initial = 1.10

    # Preemption: only preempt when a high-priority pipeline is blocked and we have no room.
    s.enable_preemption = True


@register_scheduler(key="scheduler_none_012")
def scheduler_none_012(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines, record arrival time, and queue them.
      2) Process execution results; on failure, mark per-op RAM backoff and requeue.
      3) For each pool, schedule at most one ready operator per tick, picking the best
         pipeline by (priority weight + aging), and sizing resources conservatively.
      4) If a QUERY/INTERACTIVE op cannot fit anywhere, preempt one low-priority container.
    """
    # Local helpers (no imports needed)
    def _now_ts():
        # Prefer simulator clock if available; otherwise use ticks as time base.
        if hasattr(s, "executor") and hasattr(s.executor, "now"):
            try:
                return float(s.executor.now)
            except Exception:
                pass
        if hasattr(s, "now"):
            try:
                return float(s.now)
            except Exception:
                pass
        return float(s._tick)

    def _priority_weight(priority):
        if priority == Priority.QUERY:
            return 3.0
        if priority == Priority.INTERACTIVE:
            return 2.0
        return 1.0

    def _pipeline_state(pipeline):
        try:
            return pipeline.runtime_status()
        except Exception:
            return None

    def _pipeline_done_or_failed(status):
        if status is None:
            return True
        try:
            if status.is_pipeline_successful():
                return True
            # If any failed op exists, we still want to retry failed ops (ASSIGNABLE includes FAILED),
            # but if the simulator treats pipeline as failed terminally we should drop.
            # We conservatively keep it unless it reports overall failure via state_counts heuristic.
            # If it has FAILED ops but none are retryable, it will naturally stop making progress.
            return False
        except Exception:
            return False

    def _get_ready_ops(status):
        # Assign exactly one operator at a time to reduce risk of overcommitting RAM/CPU.
        try:
            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            return ops[:1] if ops else []
        except Exception:
            return []

    def _op_id(op):
        # Best-effort stable identifier for per-op retry tracking
        if hasattr(op, "op_id"):
            return op.op_id
        if hasattr(op, "operator_id"):
            return op.operator_id
        if hasattr(op, "id"):
            return op.id
        return str(op)

    def _is_oom_error(err):
        if err is None:
            return False
        # Common patterns: string messages or exception objects with message
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _ensure_pipeline_registered(p):
        pid = p.pipeline_id
        if pid not in s.pipelines_by_id:
            s.pipelines_by_id[pid] = p
        if pid not in s.pipeline_first_seen_ts:
            s.pipeline_first_seen_ts[pid] = _now_ts()
        # Keep a queue of pipeline ids; avoid duplicates
        if pid not in s.waiting_queue:
            s.waiting_queue.append(pid)

    def _score_pipeline(pid):
        p = s.pipelines_by_id.get(pid)
        if p is None:
            return -1e9
        age = max(0.0, _now_ts() - s.pipeline_first_seen_ts.get(pid, _now_ts()))
        base = _priority_weight(p.priority)
        aging = s.aging_per_sec.get(p.priority, 0.0) * age
        return base + aging

    def _choose_pool_order(priority):
        # Prefer pool 0 for high priority if configured; otherwise natural order
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            if s.executor.num_pools <= 1:
                return [0]
            # Try hinted pool first, then others by current headroom
            hint = min(max(int(s.query_pool_hint), 0), s.executor.num_pools - 1)
            pools = list(range(s.executor.num_pools))
            if hint in pools:
                pools.remove(hint)
                pools = [hint] + pools
            # Sort remaining by available RAM (descending) to reduce OOM risk
            tail = pools[1:]
            tail.sort(key=lambda i: (s.executor.pools[i].avail_ram_pool, s.executor.pools[i].avail_cpu_pool), reverse=True)
            return [pools[0]] + tail
        # Batch: pack by available CPU then RAM (descending) to use capacity
        pools = list(range(s.executor.num_pools))
        pools.sort(key=lambda i: (s.executor.pools[i].avail_cpu_pool, s.executor.pools[i].avail_ram_pool), reverse=True)
        return pools

    def _size_resources(pool, priority, pid, op):
        avail_cpu = max(0.0, pool.avail_cpu_pool)
        avail_ram = max(0.0, pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            return 0.0, 0.0

        # Target a fraction of current available to avoid monopolizing and allow other work.
        cpu = avail_cpu * float(s.cpu_target_frac.get(priority, 0.5))
        ram = avail_ram * float(s.ram_target_frac.get(priority, 0.55))

        # Cap by pool maxima (defensive)
        cpu = min(cpu, pool.max_cpu_pool * float(s.cpu_cap_frac))
        ram = min(ram, pool.max_ram_pool * float(s.ram_cap_frac))

        # Lower bounds: need at least something to run (avoid 0 allocation)
        cpu = max(1.0, cpu) if pool.max_cpu_pool >= 1.0 else max(0.1, cpu)
        ram = max(0.1, ram)

        # Apply per-op RAM multiplier if we had OOM/failures before
        opid = _op_id(op)
        ram_mult = 1.0
        if pid in s.op_retry and opid in s.op_retry[pid]:
            ram_mult = float(s.op_retry[pid][opid].get("ram_mult", 1.0))
        ram = min(pool.max_ram_pool * float(s.ram_cap_frac), ram * ram_mult)

        # Also clamp to current availability so the assignment is feasible now
        cpu = min(cpu, avail_cpu)
        ram = min(ram, avail_ram)
        return cpu, ram

    def _pick_victim_container():
        # Best-effort: find a running low-priority container to suspend.
        # We assume s.executor may expose active containers per pool. If not, do nothing.
        best = None  # tuple(priority_rank, cpu, ram, container_id, pool_id)
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            containers = None
            # Try common attribute names
            for attr in ("containers", "running_containers", "active_containers"):
                if hasattr(pool, attr):
                    containers = getattr(pool, attr)
                    break
            if not containers:
                continue
            try:
                iterable = list(containers.values()) if isinstance(containers, dict) else list(containers)
            except Exception:
                continue
            for c in iterable:
                # Extract fields conservatively
                c_priority = getattr(c, "priority", None)
                container_id = getattr(c, "container_id", None)
                if container_id is None and hasattr(c, "id"):
                    container_id = getattr(c, "id")
                if container_id is None:
                    continue
                # Only consider batch as victims first; then interactive; never query.
                if c_priority == Priority.QUERY:
                    continue
                pr = 0
                if c_priority == Priority.BATCH_PIPELINE:
                    pr = 0
                elif c_priority == Priority.INTERACTIVE:
                    pr = 1
                else:
                    pr = 0
                c_cpu = float(getattr(c, "cpu", 0.0) or 0.0)
                c_ram = float(getattr(c, "ram", 0.0) or 0.0)
                cand = (pr, c_cpu + c_ram * 1e-6, c_cpu, c_ram, container_id, pool_id)
                if best is None or cand < best:
                    best = cand
        if best is None:
            return None
        return best[4], best[5]

    # Tick forward
    s._tick += 1

    # Ingest new pipelines
    for p in pipelines:
        _ensure_pipeline_registered(p)

    # Process results: handle failures by increasing RAM multiplier and requeue pipeline
    for r in results:
        if not hasattr(r, "ops") or not r.ops:
            continue

        # Find pipeline from result if possible; fall back to matching by scanning (cheap for small counts)
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            # Try to recover: many simulators carry pipeline_id on result, but if not, skip.
            pid = None
        if pid is not None and pid not in s.pipelines_by_id:
            # Might be a pipeline that arrived earlier but got evicted from our dict (shouldn't happen)
            pass

        if not r.failed():
            continue

        # Best-effort: infer pipeline by op ownership if pipeline_id unavailable
        if pid is None:
            # If we cannot map, we can't safely backoff; skip.
            continue

        p = s.pipelines_by_id.get(pid)
        if p is None:
            continue

        # Update retry state for each op in the result
        for op in r.ops:
            opid = _op_id(op)
            if pid not in s.op_retry:
                s.op_retry[pid] = {}
            if opid not in s.op_retry[pid]:
                s.op_retry[pid][opid] = {"ram_mult": 1.0, "last_cpu": None}

            prev = float(s.op_retry[pid][opid].get("ram_mult", 1.0))
            if _is_oom_error(getattr(r, "error", None)):
                new_mult = min(s.oom_ram_mult_max, max(prev, s.oom_ram_mult_initial) * 1.15)
            else:
                # Generic failure: small bump (often avoids borderline spills / peak)
                new_mult = min(s.oom_ram_mult_max, max(prev, s.generic_fail_ram_mult_initial))
            s.op_retry[pid][opid]["ram_mult"] = new_mult
            s.op_retry[pid][opid]["last_cpu"] = getattr(r, "cpu", None)

        # Requeue for retry
        _ensure_pipeline_registered(p)

    # Early exit if nothing changed and nothing to schedule
    if not pipelines and not results and not s.waiting_queue:
        return [], []

    suspensions = []
    assignments = []

    # Clean queue: remove completed pipelines to avoid wasting cycles
    cleaned = []
    for pid in s.waiting_queue:
        p = s.pipelines_by_id.get(pid)
        if p is None:
            continue
        st = _pipeline_state(p)
        if st is None:
            continue
        # Drop only if fully successful; keep around otherwise (including FAILED ops to retry)
        try:
            if st.is_pipeline_successful():
                continue
        except Exception:
            pass
        cleaned.append(pid)
    s.waiting_queue = cleaned

    # Build a sorted list of candidate pipelines by score (descending)
    # Keep stable tie-breaker by arrival time then pipeline_id.
    def _sort_key(pid):
        p = s.pipelines_by_id.get(pid)
        arr = s.pipeline_first_seen_ts.get(pid, 0.0)
        return (-_score_pipeline(pid), arr, str(pid), _priority_weight(p.priority) if p else 0.0)

    # We'll compute once per step
    ranked_pids = sorted(list(dict.fromkeys(s.waiting_queue)), key=_sort_key)

    # Try to schedule at most one op per pool per tick (simple, avoids oversubscription risk)
    used_pids = set()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        chosen = None  # (pid, op_list)
        for pid in ranked_pids:
            if pid in used_pids:
                continue
            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue
            st = _pipeline_state(p)
            if st is None or _pipeline_done_or_failed(st):
                continue
            op_list = _get_ready_ops(st)
            if not op_list:
                continue
            # Check whether the op can fit with our conservative sizing
            cpu, ram = _size_resources(pool, p.priority, pid, op_list[0])
            if cpu > 0 and ram > 0:
                chosen = (pid, p, op_list, cpu, ram)
                break

        if chosen is None:
            continue

        pid, p, op_list, cpu, ram = chosen
        assignment = Assignment(
            ops=op_list,
            cpu=cpu,
            ram=ram,
            priority=p.priority,
            pool_id=pool_id,
            pipeline_id=p.pipeline_id
        )
        assignments.append(assignment)
        used_pids.add(pid)

    # If we couldn't schedule any QUERY/INTERACTIVE but they exist ready, try one preemption
    if s.enable_preemption and not assignments:
        # Find if a high-priority op is ready somewhere but doesn't fit due to lack of headroom
        high_pid = None
        high_op_list = None
        high_p = None
        for pid in ranked_pids:
            p = s.pipelines_by_id.get(pid)
            if p is None or p.priority not in (Priority.QUERY, Priority.INTERACTIVE):
                continue
            st = _pipeline_state(p)
            if st is None:
                continue
            op_list = _get_ready_ops(st)
            if op_list:
                high_pid, high_op_list, high_p = pid, op_list, p
                break

        if high_pid is not None:
            victim = _pick_victim_container()
            if victim is not None:
                container_id, victim_pool_id = victim
                suspensions.append(Suspend(container_id=container_id, pool_id=victim_pool_id))

            # After scheduling the suspension, we do not also force an assignment in the same tick;
            # the simulator should free capacity next tick deterministically.

    return suspensions, assignments
