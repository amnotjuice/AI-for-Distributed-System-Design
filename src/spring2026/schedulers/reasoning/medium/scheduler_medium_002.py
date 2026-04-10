# policy_key: scheduler_medium_002
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.121940
# generation_seconds: 58.79
# generated_at: 2026-04-09T22:30:05.659325
@register_scheduler_init(key="scheduler_medium_002")
def scheduler_medium_002_init(s):
    """Priority-aware, failure-averse scheduler with adaptive RAM sizing.

    Goals:
      - Protect QUERY and INTERACTIVE latency via strict priority ordering + mild pool preference.
      - Reduce 720s penalty risk by allocating conservative RAM initially and aggressively increasing on OOM.
      - Avoid starvation via simple aging (pipelines that wait longer gain effective priority).
      - Keep implementation incremental vs FIFO: no complex preemption; focus on completion + tail latency.

    State:
      - pipeline_map: pipeline_id -> Pipeline (latest object reference)
      - arrival_tick: pipeline_id -> tick when first seen
      - queues: priority -> list[pipeline_id] (stable FIFO per priority)
      - ram_est: (pipeline_id, op_key) -> estimated RAM needed (learned on OOM)
      - retry_count: (pipeline_id, op_key) -> number of failures observed
      - tick: monotonically increasing scheduler steps
    """
    s.tick = 0
    s.pipeline_map = {}
    s.arrival_tick = {}
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.ram_est = {}
    s.retry_count = {}


def _op_key(op):
    # Try common id attributes; fallback to Python object identity.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            # Prefer stable primitives
            if isinstance(v, (int, str)):
                return (attr, v)
    return ("pyid", id(op))


def _is_oom_error(err):
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)


def _base_weight(priority):
    # Higher means more important.
    if priority == Priority.QUERY:
        return 100.0
    if priority == Priority.INTERACTIVE:
        return 60.0
    return 10.0  # batch


def _pool_pref_bonus(priority, pool_id, num_pools):
    # Mild preference: if multiple pools exist, prefer pool 0 for QUERY/INTERACTIVE.
    if num_pools <= 1:
        return 0.0
    if pool_id == 0 and priority in (Priority.QUERY, Priority.INTERACTIVE):
        return 5.0
    if pool_id != 0 and priority == Priority.BATCH_PIPELINE:
        return 2.0
    return 0.0


def _desired_cpu(priority, avail_cpu, pool_max_cpu):
    # Give more CPU to higher priority to reduce latency. Cap so we don't grab the entire pool.
    if pool_max_cpu <= 0:
        pool_max_cpu = max(avail_cpu, 1.0)

    if priority == Priority.QUERY:
        frac = 0.70
        cap_frac_of_avail = 0.90
    elif priority == Priority.INTERACTIVE:
        frac = 0.55
        cap_frac_of_avail = 0.85
    else:
        frac = 0.45
        cap_frac_of_avail = 0.80

    target = frac * pool_max_cpu
    cap = cap_frac_of_avail * avail_cpu
    cpu = min(target, cap, avail_cpu)
    # Try to avoid allocating "too tiny" CPU which can stretch latency.
    if cpu <= 0:
        return 0.0
    if cpu < 1.0 and avail_cpu >= 1.0:
        cpu = 1.0
    return cpu


def _desired_ram(priority, avail_ram, pool_max_ram, learned_ram):
    # Conservative RAM to avoid OOMs (720s penalties are expensive).
    if pool_max_ram <= 0:
        pool_max_ram = max(avail_ram, 1.0)

    # Priority-based baseline fractions.
    if priority == Priority.QUERY:
        base_frac = 0.45
        cap_frac_of_avail = 0.95
    elif priority == Priority.INTERACTIVE:
        base_frac = 0.35
        cap_frac_of_avail = 0.92
    else:
        base_frac = 0.25
        cap_frac_of_avail = 0.90

    base = base_frac * pool_max_ram
    # If we learned a requirement (from OOM), honor it with a small safety margin.
    if learned_ram is not None and learned_ram > 0:
        base = max(base, 1.10 * learned_ram)

    ram = min(base, cap_frac_of_avail * avail_ram, avail_ram)
    if ram <= 0:
        return 0.0
    return ram


def _enqueue_pipeline(s, p):
    pid = p.pipeline_id
    s.pipeline_map[pid] = p
    if pid not in s.arrival_tick:
        s.arrival_tick[pid] = s.tick
        s.queues[p.priority].append(pid)
    else:
        # If priority changed (unlikely), move queues.
        old_p = None
        for pr, q in s.queues.items():
            if pid in q:
                old_p = pr
                break
        if old_p is not None and old_p != p.priority:
            s.queues[old_p] = [x for x in s.queues[old_p] if x != pid]
            s.queues[p.priority].append(pid)


def _cleanup_queues(s):
    # Remove pipelines that are already completed successfully.
    for pr in list(s.queues.keys()):
        newq = []
        for pid in s.queues[pr]:
            p = s.pipeline_map.get(pid)
            if p is None:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            newq.append(pid)
        s.queues[pr] = newq


def _pick_pipeline_for_pool(s, pool_id):
    # Choose the next pipeline to schedule on this pool, using:
    #   score = base_weight(priority) + aging + pool_preference
    # Try higher score first; within same score keep FIFO.
    num_pools = s.executor.num_pools

    best_pid = None
    best_score = None

    # If multiple pools exist, treat pool 0 as "high-priority preferred".
    # Still allow batch on pool 0 if there are no pending high-priority assignable ops.
    allow_batch_on_pool0 = True
    if num_pools > 1 and pool_id == 0:
        allow_batch_on_pool0 = False  # only if no query/interactive is ready

    def pipeline_has_ready_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return bool(ops)

    high_ready_exists = False
    if num_pools > 1 and pool_id == 0:
        for pr in (Priority.QUERY, Priority.INTERACTIVE):
            for pid in s.queues[pr]:
                p = s.pipeline_map.get(pid)
                if p is None:
                    continue
                if not p.runtime_status().is_pipeline_successful() and pipeline_has_ready_op(p):
                    high_ready_exists = True
                    break
            if high_ready_exists:
                break

    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        if num_pools > 1 and pool_id == 0 and pr == Priority.BATCH_PIPELINE and not allow_batch_on_pool0 and high_ready_exists:
            continue

        q = s.queues[pr]
        for pid in q:
            p = s.pipeline_map.get(pid)
            if p is None:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            # Only consider pipelines with at least one assignable operator ready.
            if not st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True):
                continue

            waited = max(0, s.tick - s.arrival_tick.get(pid, s.tick))
            # Aging: batch gains slowly; interactive gains a bit; query gains least (already high).
            if pr == Priority.BATCH_PIPELINE:
                aging = 0.30 * waited
            elif pr == Priority.INTERACTIVE:
                aging = 0.15 * waited
            else:
                aging = 0.05 * waited

            score = _base_weight(pr) + aging + _pool_pref_bonus(pr, pool_id, num_pools)

            if best_score is None or score > best_score:
                best_score = score
                best_pid = pid

    return best_pid


@register_scheduler(key="scheduler_medium_002")
def scheduler_medium_002(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines (stable FIFO per priority).
      2) Learn from failures (especially OOM) by increasing RAM estimates per operator.
      3) For each pool, assign at most one ready operator from the best pipeline for that pool.
         - Strictly prefer QUERY then INTERACTIVE then BATCH (with aging to prevent starvation).
         - Conservative RAM allocation + OOM backoff to reduce failure-driven 720s penalties.
    """
    s.tick += 1

    # Ingest arrivals.
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from execution results (OOM => bump RAM estimate; any failure => increment retry counter).
    if results:
        for r in results:
            if not getattr(r, "ops", None):
                continue
            for op in r.ops:
                pid = getattr(r, "pipeline_id", None)
                # Some simulators may not include pipeline_id in ExecutionResult; try to infer later.
                # We still track by op identity + pool_id if pid is missing, but primary key is (pipeline_id, op_key).
                if pid is None:
                    continue

                ok = _op_key(op)
                k = (pid, ok)

                if hasattr(r, "failed") and r.failed():
                    s.retry_count[k] = s.retry_count.get(k, 0) + 1

                    # If OOM, aggressively increase learned RAM (doubling-ish).
                    if _is_oom_error(getattr(r, "error", None)):
                        prev = s.ram_est.get(k, getattr(r, "ram", None) or 0.0)
                        # If previous is unknown/zero, start from the attempted allocation.
                        attempted = getattr(r, "ram", None) or prev or 0.0
                        bumped = max(prev, attempted)
                        # Multiplicative backoff; cap handled at assignment time by pool max/avail.
                        if bumped > 0:
                            bumped = 1.8 * bumped
                        s.ram_est[k] = max(s.ram_est.get(k, 0.0), bumped)

    # Cleanup completed pipelines from queues.
    _cleanup_queues(s)

    # If nothing changed, return quickly.
    if not pipelines and not results:
        return [], []

    suspensions = []  # no preemption in this incremental policy
    assignments = []

    # For each pool, try to schedule one ready operator.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        pid = _pick_pipeline_for_pool(s, pool_id)
        if pid is None:
            continue

        p = s.pipeline_map.get(pid)
        if p is None:
            continue

        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue

        # Pick exactly one ready operator (keeps scheduling stable; avoids over-allocating).
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            continue
        op = op_list[0]

        # Determine learned RAM estimate and retries.
        k = (pid, _op_key(op))
        learned_ram = s.ram_est.get(k)
        retries = s.retry_count.get(k, 0)

        # If we have repeated failures, be even more conservative by inflating learned RAM.
        if learned_ram is not None and retries >= 2:
            learned_ram = learned_ram * (1.0 + 0.15 * min(retries, 6))

        cpu = _desired_cpu(p.priority, avail_cpu, pool.max_cpu_pool)
        ram = _desired_ram(p.priority, avail_ram, pool.max_ram_pool, learned_ram)

        # If repeated OOMs, strongly bias to "just give it the pool" to finish and avoid 720 penalties.
        if retries >= 3:
            ram = min(avail_ram, pool.max_ram_pool)
            cpu = min(avail_cpu, max(1.0, 0.85 * pool.max_cpu_pool))

        if cpu <= 0 or ram <= 0:
            continue

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
        )

    return suspensions, assignments
