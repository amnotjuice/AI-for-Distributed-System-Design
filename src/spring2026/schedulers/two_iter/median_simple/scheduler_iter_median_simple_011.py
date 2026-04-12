# policy_key: scheduler_iter_median_simple_011
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 30.27
# generated_at: 2026-04-12T01:36:59.799817
@register_scheduler_init(key="scheduler_iter_median_simple_011")
def scheduler_iter_median_simple_011_init(s):
    """Iteration 2: stricter priority + stronger headroom reservation to cut tail/median latency.

    Changes vs prior attempt:
    - Two-phase scheduling per tick: schedule QUERY+INTERACTIVE first across all pools, then BATCH.
    - Much smaller default slices for high-priority ops to increase parallelism and reduce queueing.
    - Stronger headroom reservation: when any high-priority backlog exists, batch may only use
      "leftover" capacity after reserving a fixed minimum CPU/RAM per pool.
    - Keep logic simple/deterministic; no preemption (sim interface doesn't expose active containers).
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Minimal per-pipeline state; mainly used to drop hard-failed pipelines if desired later.
    s.pstate = {}  # pipeline_id -> dict


@register_scheduler(key="scheduler_iter_median_simple_011")
def scheduler_iter_median_simple_011_scheduler(s, results, pipelines):
    """
    Policy:
    - Enqueue new pipelines into per-priority FIFO queues.
    - Identify whether any high-priority runnable work exists (QUERY/INTERACTIVE).
    - For each pool:
        1) greedily schedule as many QUERY ops as possible (1 op per assignment) with small slices
        2) then schedule INTERACTIVE similarly
        3) finally schedule BATCH only if capacity remains beyond reserved headroom
    This reduces latency by preventing batch from consuming the last available CPU/RAM.
    """
    # -----------------------------
    # Helpers
    # -----------------------------
    def _ensure_pstate(pid):
        if pid not in s.pstate:
            s.pstate[pid] = {"drop": False}
        return s.pstate[pid]

    def _cleanup_front(q):
        """Pop completed pipelines from the front to keep queues fresh."""
        while q:
            p = q[0]
            st = p.runtime_status()
            if st.is_pipeline_successful():
                q.pop(0)
                continue
            # If we ever choose to drop pipelines, do it here.
            if s.pstate.get(p.pipeline_id, {}).get("drop", False):
                q.pop(0)
                continue
            break

    def _has_runnable(pri):
        q = s.queues[pri]
        # We do a small scan; queues are expected to be short-ish in the sim
        for p in q:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                return True
        return False

    def _pick_next_runnable(pri):
        """FIFO with rotation: find first pipeline that has a runnable op; rotate others to back."""
        q = s.queues[pri]
        n = len(q)
        for _ in range(n):
            _cleanup_front(q)
            if not q:
                return None, None
            p = q.pop(0)
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if ops:
                # rotate pipeline to back for fairness among same-priority pipelines
                q.append(p)
                return p, ops
            # Not runnable now; rotate it to back
            q.append(p)
        return None, None

    def _request_for_priority(pool, pri, avail_cpu, avail_ram, hp_backlog):
        """Resource shaping: small slices for HP, batch only uses leftover after reservation."""
        # Hard minimums (avoid requesting 0)
        min_cpu = 1.0
        min_ram = 1.0

        if pri == Priority.QUERY:
            # Maximize concurrency; query ops tend to be short.
            cpu = min(1.0, avail_cpu) if avail_cpu >= 1.0 else avail_cpu
            ram = min(pool.max_ram_pool * 0.06, avail_ram)  # small RAM slice; OOM handled by sim as failure
        elif pri == Priority.INTERACTIVE:
            cpu = min(max(1.0, pool.max_cpu_pool * 0.20), avail_cpu)  # typically 1-2 vCPU
            # If pool is tiny, keep it at 1 vCPU
            if cpu > 2.0:
                cpu = 2.0
            ram = min(pool.max_ram_pool * 0.12, avail_ram)
        else:
            # Batch: if hp backlog exists, preserve headroom in this pool.
            if hp_backlog:
                # Reserve a fixed headroom per pool for sudden QUERY/INTERACTIVE arrivals.
                reserve_cpu = max(2.0, pool.max_cpu_pool * 0.25)
                reserve_ram = max(2.0, pool.max_ram_pool * 0.20)
                usable_cpu = max(0.0, avail_cpu - reserve_cpu)
                usable_ram = max(0.0, avail_ram - reserve_ram)
                if usable_cpu < 1.0 or usable_ram < 1.0:
                    return 0.0, 0.0
                avail_cpu = usable_cpu
                avail_ram = usable_ram

            # When allowed, let batch take a bigger chunk to reduce overhead.
            cpu = min(max(2.0, pool.max_cpu_pool * 0.50), avail_cpu)
            ram = min(pool.max_ram_pool * 0.35, avail_ram)

        # Clamp to sane minimums if we decided to schedule something
        if cpu > 0:
            cpu = max(min_cpu, cpu)
        if ram > 0:
            ram = max(min_ram, ram)

        # Final clamps
        cpu = min(cpu, avail_cpu, pool.max_cpu_pool)
        ram = min(ram, avail_ram, pool.max_ram_pool)
        return cpu, ram

    # -----------------------------
    # Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        _ensure_pstate(p.pipeline_id)
        s.queues[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # Determine if there is any runnable high-priority backlog (global signal)
    hp_backlog = _has_runnable(Priority.QUERY) or _has_runnable(Priority.INTERACTIVE)

    suspensions = []
    assignments = []

    # -----------------------------
    # Scheduling across pools
    # -----------------------------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Phase 1: QUERY then INTERACTIVE (greedy)
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            made_progress = True
            while made_progress:
                made_progress = False
                if avail_cpu < 1.0 or avail_ram < 1.0:
                    break

                p, ops = _pick_next_runnable(pri)
                if p is None:
                    break

                cpu_req, ram_req = _request_for_priority(pool, pri, avail_cpu, avail_ram, hp_backlog)
                if cpu_req < 1.0 or ram_req < 1.0:
                    # Can't fit anything meaningful in this pool right now.
                    break

                assignments.append(
                    Assignment(
                        ops=ops,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                made_progress = True

        # Phase 2: BATCH (only after HP), and potentially restricted by headroom reservation
        made_progress = True
        while made_progress:
            made_progress = False
            if avail_cpu < 1.0 or avail_ram < 1.0:
                break

            p, ops = _pick_next_runnable(Priority.BATCH_PIPELINE)
            if p is None:
                break

            cpu_req, ram_req = _request_for_priority(pool, Priority.BATCH_PIPELINE, avail_cpu, avail_ram, hp_backlog)
            if cpu_req < 1.0 or ram_req < 1.0:
                # Headroom reservation blocks batch here; move to next pool.
                break

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            made_progress = True

    return suspensions, assignments
