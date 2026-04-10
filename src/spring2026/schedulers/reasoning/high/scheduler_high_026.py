# policy_key: scheduler_high_026
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.161319
# generation_seconds: 207.80
# generated_at: 2026-04-10T02:02:38.682724
@register_scheduler_init(key="scheduler_high_026")
def scheduler_high_026_init(s):
    """
    Priority-aware, failure-avoiding scheduler.

    Core ideas:
      - Strictly prioritize QUERY then INTERACTIVE then BATCH.
      - Avoid OOM-induced failures by using conservative RAM sizing and exponential RAM backoff on OOM.
      - Prevent batch starvation with (a) aging-based batch "floor" reservation when batch is starving and
        (b) mild per-pipeline concurrency caps (queries can run up to 2 ops concurrently).
      - Best-effort preemption: if we can discover running containers, suspend lower-priority work to make room.
    """
    s.tick = 0

    # Track all pipelines we've seen (so we can schedule beyond the arrival tick).
    s.pipelines = {}          # pipeline_id -> Pipeline
    s.arrival_tick = {}       # pipeline_id -> tick
    s.doomed = set()          # pipeline_ids we stop scheduling (non-OOM failures or too many OOM retries)

    # Map operator object identity back to its pipeline, to attribute ExecutionResults.
    s.op_obj_to_pipeline = {}  # id(op) -> pipeline_id

    # Per-(pipeline, op) RAM estimate used for avoiding repeated OOMs.
    s.op_ram_est = {}         # (pipeline_id, op_key) -> ram_est
    s.op_retry_count = {}     # (pipeline_id, op_key) -> retry_count

    # Controls for retrying OOMs (more retries for higher priorities).
    s.max_oom_retries = {
        Priority.QUERY: 6,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 4,
    }

    # Conservative default sizing as a fraction of pool capacity (RAM is most important to avoid OOM loops).
    s.default_ram_frac = {
        Priority.QUERY: 0.40,
        Priority.INTERACTIVE: 0.33,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.default_cpu_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.35,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # If batch jobs have been waiting too long, reserve a small per-pool "floor" to guarantee progress.
    s.batch_starve_ticks = 140
    s.batch_floor_frac_cpu = 0.10
    s.batch_floor_frac_ram = 0.10

    # Concurrency caps: prevent one pipeline from hogging a pool; allow queries a little parallelism.
    s.max_ops_per_pipeline_per_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 1,
    }

    # Preemption throttle (best-effort; only if we can discover containers).
    s.max_preemptions_per_tick = 4


def _priority_rank(pri):
    # Higher is more important.
    if pri == Priority.QUERY:
        return 3
    if pri == Priority.INTERACTIVE:
        return 2
    return 1


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)


def _op_key(op) -> str:
    # Stable-ish key within a pipeline; fall back to str(op).
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                return f"{attr}:{getattr(op, attr)}"
            except Exception:
                pass
    try:
        return str(op)
    except Exception:
        return f"op@{id(op)}"


def _try_get_op_min_ram(op):
    for attr in ("min_ram", "ram_min", "min_memory", "memory_min", "ram", "memory"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is None:
                    continue
                # Keep it numeric; if it's something else, ignore.
                return float(v)
            except Exception:
                continue
    return None


def _try_get_op_min_cpu(op):
    for attr in ("min_cpu", "cpu_min", "vcpus_min", "cpu"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is None:
                    continue
                return float(v)
            except Exception:
                continue
    return None


def _remaining_ops_estimate(pipeline, status):
    # Prefer status.state_counts if available; otherwise fall back to len(pipeline.values) when possible.
    try:
        counts = status.state_counts
        total = 0
        for v in counts.values():
            total += int(v)
        completed = int(counts.get(OperatorState.COMPLETED, 0))
        remaining = max(0, total - completed)
        return remaining
    except Exception:
        pass

    vals = getattr(pipeline, "values", None)
    try:
        if isinstance(vals, dict):
            total = len(vals.values())
        elif vals is None:
            total = 0
        else:
            total = len(vals)
        return max(1, int(total))
    except Exception:
        return 1


def _iter_pipeline_ops(pipeline):
    vals = getattr(pipeline, "values", None)
    if vals is None:
        return
    try:
        if isinstance(vals, dict):
            for op in vals.values():
                yield op
        else:
            for op in vals:
                yield op
    except Exception:
        return


def _get_pool_containers(pool):
    # Best-effort introspection. Returns iterable of container-like objects/dicts.
    for attr in ("running_containers", "active_containers", "containers", "inflight_containers", "assigned_containers"):
        if hasattr(pool, attr):
            try:
                c = getattr(pool, attr)
                if c is None:
                    continue
                return c
            except Exception:
                continue
    return []


def _container_field(c, name, default=None):
    # Support both dict-like and attribute-like containers.
    if isinstance(c, dict):
        return c.get(name, default)
    if hasattr(c, name):
        try:
            return getattr(c, name)
        except Exception:
            return default
    return default


def _container_priority(c):
    pr = _container_field(c, "priority", None)
    if pr is None:
        return None
    return pr


def _container_id(c):
    cid = _container_field(c, "container_id", None)
    if cid is None:
        cid = _container_field(c, "id", None)
    return cid


def _container_resources(c):
    cpu = _container_field(c, "cpu", None)
    ram = _container_field(c, "ram", None)
    try:
        cpu = float(cpu) if cpu is not None else 0.0
    except Exception:
        cpu = 0.0
    try:
        ram = float(ram) if ram is not None else 0.0
    except Exception:
        ram = 0.0
    return cpu, ram


@register_scheduler(key="scheduler_high_026")
def scheduler_high_026(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    # Fast path: no new info.
    if not pipelines and not results:
        return [], []

    s.tick += 1

    # Cache global maxima (for bounding backoff); compute lazily.
    try:
        global_max_ram = max(p.max_ram_pool for p in s.executor.pools)
    except Exception:
        global_max_ram = None

    # Register new pipelines and build op->pipeline mapping for result attribution.
    for p in pipelines:
        s.pipelines[p.pipeline_id] = p
        if p.pipeline_id not in s.arrival_tick:
            s.arrival_tick[p.pipeline_id] = s.tick

        # Map operator object identity to pipeline id (best-effort).
        for op in _iter_pipeline_ops(p):
            s.op_obj_to_pipeline[id(op)] = p.pipeline_id

    # Process results: on OOM, backoff RAM and retry; on non-OOM failure, mark pipeline doomed.
    for r in results:
        if not r.failed():
            continue

        is_oom = _is_oom_error(getattr(r, "error", None))

        # Attribute each op in the result back to a pipeline id via op object identity mapping.
        ops = getattr(r, "ops", []) or []
        for op in ops:
            pid = s.op_obj_to_pipeline.get(id(op), None)
            if pid is None:
                # If we can't attribute the op, we can't safely adjust state; skip.
                continue

            # Pipeline may already be gone/unknown; still keep state consistent.
            pipe = s.pipelines.get(pid, None)
            pri = pipe.priority if pipe is not None else getattr(r, "priority", Priority.BATCH_PIPELINE)

            if not is_oom:
                s.doomed.add(pid)
                continue

            opk = _op_key(op)
            key = (pid, opk)

            prev = s.op_ram_est.get(key, None)
            try:
                allocated = float(getattr(r, "ram", 0) or 0)
            except Exception:
                allocated = 0.0

            # Exponential backoff: double the best-known "needed" RAM.
            # If we have no estimate yet, start from the last allocated RAM (or 1).
            base = prev if prev is not None else max(allocated, 1.0)
            bumped = max(base, allocated, 1.0) * 2.0

            if global_max_ram is not None:
                bumped = min(bumped, float(global_max_ram))

            s.op_ram_est[key] = bumped
            s.op_retry_count[key] = s.op_retry_count.get(key, 0) + 1

            max_r = s.max_oom_retries.get(pri, 4)
            if s.op_retry_count[key] > max_r:
                s.doomed.add(pid)

    # Build candidates per priority, skipping completed or doomed pipelines.
    candidates = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    batch_starving = False
    for pid, p in list(s.pipelines.items()):
        if pid in s.doomed:
            continue

        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue

        age = s.tick - s.arrival_tick.get(pid, s.tick)
        rem = _remaining_ops_estimate(p, st)

        candidates[p.priority].append((rem, age, s.arrival_tick.get(pid, 0), pid, p))

        if p.priority == Priority.BATCH_PIPELINE and age >= s.batch_starve_ticks:
            # Only consider it starving if it has at least one assignable op ready (avoid reserving for blocked DAGs).
            try:
                if st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True):
                    batch_starving = True
            except Exception:
                # If status inspection fails, treat it as potentially starving.
                batch_starving = True

    # Sort within each class: shortest remaining first, then oldest first, then arrival order.
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        candidates[pr].sort(key=lambda t: (t[0], -t[1], t[2]))

    # Helper to detect whether there is any ready work of a given priority.
    def _has_ready(pr):
        for _, _, _, pid, p in candidates[pr]:
            if pid in s.doomed:
                continue
            st = p.runtime_status()
            try:
                if st.is_pipeline_successful():
                    continue
                if st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True):
                    return True
            except Exception:
                continue
        return False

    query_ready = _has_ready(Priority.QUERY)
    interactive_ready = _has_ready(Priority.INTERACTIVE)
    batch_ready = _has_ready(Priority.BATCH_PIPELINE)

    # Scheduling outputs.
    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Track per-pipeline scheduled ops count within this tick for per-pipeline concurrency caps.
    scheduled_counts = {}

    # Preemption throttle within a tick.
    preemptions_left = getattr(s, "max_preemptions_per_tick", 0)

    # Process pools in descending "headroom" order to place big/interactive work first.
    pool_ids = list(range(s.executor.num_pools))
    try:
        pool_ids.sort(
            key=lambda i: (
                float(s.executor.pools[i].avail_cpu_pool) * float(s.executor.pools[i].avail_ram_pool)
            ),
            reverse=True,
        )
    except Exception:
        pass

    # Attempt best-effort preemption to make room for one high-priority op in a pool.
    def _maybe_preempt_for(pool_id, pool, target_priority, need_cpu, need_ram, avail_cpu, avail_ram):
        nonlocal preemptions_left
        if preemptions_left <= 0:
            return avail_cpu, avail_ram

        # Only preempt for QUERY/INTERACTIVE.
        if _priority_rank(target_priority) < _priority_rank(Priority.INTERACTIVE):
            return avail_cpu, avail_ram

        # If already enough, do nothing.
        if avail_cpu >= need_cpu and avail_ram >= need_ram:
            return avail_cpu, avail_ram

        containers = list(_get_pool_containers(pool) or [])
        if not containers:
            return avail_cpu, avail_ram

        # Sort preemptable containers from lowest priority upward.
        def _preempt_sort_key(c):
            pr = _container_priority(c)
            r = _priority_rank(pr) if pr is not None else 0
            cpu, ram = _container_resources(c)
            # Prefer preempting low-priority, then smaller "waste" first to reduce churn.
            return (r, cpu + ram)

        containers.sort(key=_preempt_sort_key)

        for c in containers:
            if preemptions_left <= 0:
                break

            c_pr = _container_priority(c)
            if c_pr is None:
                continue
            if _priority_rank(c_pr) >= _priority_rank(target_priority):
                continue  # don't preempt same/higher priority

            cid = _container_id(c)
            if cid is None:
                continue

            cpu_freed, ram_freed = _container_resources(c)
            if cpu_freed <= 0 and ram_freed <= 0:
                continue

            suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
            preemptions_left -= 1

            avail_cpu += cpu_freed
            avail_ram += ram_freed

            if avail_cpu >= need_cpu and avail_ram >= need_ram:
                break

        return avail_cpu, avail_ram

    # Core assignment loop per pool.
    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]

        try:
            avail_cpu = float(pool.avail_cpu_pool)
        except Exception:
            avail_cpu = 0.0
        try:
            avail_ram = float(pool.avail_ram_pool)
        except Exception:
            avail_ram = 0.0

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If batch is genuinely starving and there's high-priority backlog, reserve a small "floor" for batch.
        floor_cpu = 0.0
        floor_ram = 0.0
        if batch_starving and batch_ready and (query_ready or interactive_ready):
            try:
                floor_cpu = float(pool.max_cpu_pool) * float(s.batch_floor_frac_cpu)
                floor_ram = float(pool.max_ram_pool) * float(s.batch_floor_frac_ram)
            except Exception:
                floor_cpu = 0.0
                floor_ram = 0.0

        # Best-effort preemption: if queries are ready but we don't have enough for a typical query-sized op, try to preempt.
        if query_ready:
            try:
                q_need_cpu = max(1.0, float(pool.max_cpu_pool) * float(s.default_cpu_frac[Priority.QUERY]))
                q_need_ram = max(1.0, float(pool.max_ram_pool) * float(s.default_ram_frac[Priority.QUERY]))
            except Exception:
                q_need_cpu, q_need_ram = 1.0, 1.0

            avail_cpu, avail_ram = _maybe_preempt_for(
                pool_id, pool, Priority.QUERY, q_need_cpu, q_need_ram, avail_cpu, avail_ram
            )

        # Phased fill: QUERY -> INTERACTIVE -> BATCH.
        for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            while True:
                if avail_cpu < 1.0 or avail_ram < 1.0:
                    break

                # Enforce floor only to protect batch; never block queries because of the floor.
                cpu_budget = avail_cpu
                ram_budget = avail_ram
                if pr == Priority.INTERACTIVE and floor_cpu > 0 and floor_ram > 0:
                    cpu_budget = max(0.0, avail_cpu - floor_cpu)
                    ram_budget = max(0.0, avail_ram - floor_ram)

                # If there's no budget for this class, stop this phase.
                if cpu_budget < 1.0 or ram_budget < 1.0:
                    break

                picked = False

                # Scan candidates in this class for the first that has a ready op and fits budgets.
                for idx in range(len(candidates[pr])):
                    _, _, _, pid, p = candidates[pr][idx]
                    if pid in s.doomed:
                        continue

                    # Per-pipeline concurrency cap within a tick.
                    cap = s.max_ops_per_pipeline_per_tick.get(p.priority, 1)
                    if scheduled_counts.get(pid, 0) >= cap:
                        continue

                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        continue

                    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        continue

                    op = op_list[0]

                    # Determine RAM requirement: max(min_ram, learned_estimate, conservative_default_fraction).
                    min_ram = _try_get_op_min_ram(op)
                    min_ram = float(min_ram) if min_ram is not None else 1.0

                    opk = _op_key(op)
                    est = s.op_ram_est.get((pid, opk), None)

                    try:
                        default_ram = float(pool.max_ram_pool) * float(s.default_ram_frac.get(pr, 0.25))
                    except Exception:
                        default_ram = 1.0

                    ram_needed = max(min_ram, float(est) if est is not None else 0.0, default_ram, 1.0)

                    # Determine CPU target.
                    min_cpu = _try_get_op_min_cpu(op)
                    min_cpu = float(min_cpu) if min_cpu is not None else 1.0

                    try:
                        default_cpu = float(pool.max_cpu_pool) * float(s.default_cpu_frac.get(pr, 0.25))
                    except Exception:
                        default_cpu = 1.0

                    cpu_target = max(min_cpu, default_cpu, 1.0)

                    # Fit check: we refuse to downsize RAM below ram_needed (to avoid OOM loops),
                    # but we allow CPU to be clamped to what's available (latency may increase but completes).
                    if ram_needed > ram_budget:
                        continue
                    if min_cpu > cpu_budget:
                        continue

                    cpu_alloc = min(cpu_target, cpu_budget)
                    ram_alloc = ram_needed  # exact to avoid wasting and to respect OOM-driven estimation

                    # Final safety clamp (should be no-ops if above checks passed).
                    cpu_alloc = max(1.0, min(cpu_alloc, avail_cpu))
                    ram_alloc = max(1.0, min(ram_alloc, avail_ram))

                    # Create assignment.
                    assignments.append(
                        Assignment(
                            ops=op_list,
                            cpu=cpu_alloc,
                            ram=ram_alloc,
                            priority=p.priority,
                            pool_id=pool_id,
                            pipeline_id=p.pipeline_id,
                        )
                    )

                    avail_cpu -= cpu_alloc
                    avail_ram -= ram_alloc
                    scheduled_counts[pid] = scheduled_counts.get(pid, 0) + 1
                    picked = True
                    break

                if not picked:
                    break

        # If we still have leftover resources and batch floor was enforced, try to spend it on batch.
        # This helps throughput without harming query admission (queries already got first shot + preemption attempt).
        if avail_cpu >= 1.0 and avail_ram >= 1.0 and batch_ready:
            while True:
                if avail_cpu < 1.0 or avail_ram < 1.0:
                    break

                picked = False
                for idx in range(len(candidates[Priority.BATCH_PIPELINE])):
                    _, _, _, pid, p = candidates[Priority.BATCH_PIPELINE][idx]
                    if pid in s.doomed:
                        continue

                    cap = s.max_ops_per_pipeline_per_tick.get(p.priority, 1)
                    if scheduled_counts.get(pid, 0) >= cap:
                        continue

                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        continue

                    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        continue

                    op = op_list[0]
                    min_ram = _try_get_op_min_ram(op)
                    min_ram = float(min_ram) if min_ram is not None else 1.0

                    opk = _op_key(op)
                    est = s.op_ram_est.get((pid, opk), None)

                    try:
                        default_ram = float(pool.max_ram_pool) * float(s.default_ram_frac.get(Priority.BATCH_PIPELINE, 0.25))
                    except Exception:
                        default_ram = 1.0

                    ram_needed = max(min_ram, float(est) if est is not None else 0.0, default_ram, 1.0)
                    if ram_needed > avail_ram:
                        continue

                    min_cpu = _try_get_op_min_cpu(op)
                    min_cpu = float(min_cpu) if min_cpu is not None else 1.0
                    if min_cpu > avail_cpu:
                        continue

                    try:
                        default_cpu = float(pool.max_cpu_pool) * float(s.default_cpu_frac.get(Priority.BATCH_PIPELINE, 0.25))
                    except Exception:
                        default_cpu = 1.0
                    cpu_target = max(min_cpu, default_cpu, 1.0)

                    cpu_alloc = min(cpu_target, avail_cpu)
                    ram_alloc = ram_needed

                    assignments.append(
                        Assignment(
                            ops=op_list,
                            cpu=cpu_alloc,
                            ram=ram_alloc,
                            priority=p.priority,
                            pool_id=pool_id,
                            pipeline_id=p.pipeline_id,
                        )
                    )

                    avail_cpu -= cpu_alloc
                    avail_ram -= ram_alloc
                    scheduled_counts[pid] = scheduled_counts.get(pid, 0) + 1
                    picked = True
                    break

                if not picked:
                    break

    return suspensions, assignments
