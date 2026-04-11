# policy_key: scheduler_low_012
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.054051
# generation_seconds: 56.81
# generated_at: 2026-04-09T21:11:25.222441
@register_scheduler_init(key="scheduler_low_012")
def scheduler_low_012_init(s):
    """
    Priority-aware, completion-focused scheduler.

    Core ideas:
    - Maintain separate FIFO queues per priority, plus simple aging to avoid starvation.
    - Use "headroom reservation" so batch work cannot consume all resources, protecting
      query/interactive latency (dominant in the objective).
    - Conservative per-op sizing with caps (avoid giving one op the whole pool),
      but on failure (esp. OOM-like) increase requested RAM/CPU for that pipeline
      to improve completion probability (failures are heavily penalized).
    - Schedule at most a small number of ops per pool per tick to reduce churn and
      keep latency predictable for high priority arrivals.
    """
    s.tick = 0

    # Per-priority waiting queues hold pipeline_ids; actual pipeline objects live in pipeline_by_id
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []
    s.pipeline_by_id = {}

    # Enqueue time (for aging), and resource "desires" learned from failures.
    s.enqueue_tick = {}          # pipeline_id -> tick
    s.desired_ram = {}           # pipeline_id -> suggested RAM for next attempt
    s.desired_cpu = {}           # pipeline_id -> suggested CPU for next attempt
    s.fail_count = {}            # pipeline_id -> number of failed execution results observed

    # Track seen completions/failures so we can drop from queues.
    s.done = set()               # pipeline_ids completed successfully
    s.dead = set()               # pipeline_ids considered permanently failed (rare; we try hard not to)


@register_scheduler(key="scheduler_low_012")
def scheduler_low_012_scheduler(s, results, pipelines):
    """
    Scheduler step: ingest new pipelines + execution results, then place ready operators.

    Emphasis:
    - Always try to keep some pool headroom for query/interactive.
    - Prefer placing one ready operator per pool iteration (reduces monopolization).
    - Retry FAILED ops, increasing RAM/CPU targets based on last failed allocation.
    """
    # ---- small helpers (kept local; no imports needed) ----
    def _is_query(pri):
        return pri == Priority.QUERY

    def _is_interactive(pri):
        return pri == Priority.INTERACTIVE

    def _is_batch(pri):
        return pri == Priority.BATCH_PIPELINE

    def _base_weight(pri):
        # Mirrors objective weights (query dominates)
        if _is_query(pri):
            return 10
        if _is_interactive(pri):
            return 5
        return 1

    def _queue_for_priority(pri):
        if _is_query(pri):
            return s.q_query
        if _is_interactive(pri):
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        # Keep latest object (runtime_status evolves inside)
        s.pipeline_by_id[pid] = p
        if pid in s.done or pid in s.dead:
            return
        if pid not in s.enqueue_tick:
            s.enqueue_tick[pid] = s.tick
        q = _queue_for_priority(p.priority)
        if pid not in q:
            q.append(pid)

    def _pipeline_terminal(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True, "done"
        # If any failures exist, we still keep pipeline alive (we retry) unless we decide to kill.
        # Avoid marking dead here; failure is penalized heavily, so we default to retry.
        return False, None

    def _age_bonus(pid):
        # Simple aging that can elevate long-waiting low-priority pipelines without
        # significantly harming query/interactive latency.
        # Bonus grows slowly so batch does not dominate.
        enq = s.enqueue_tick.get(pid, s.tick)
        age = max(0, s.tick - enq)
        return age // 50  # every ~50 ticks adds +1 to effective score

    def _effective_score(pid):
        p = s.pipeline_by_id.get(pid)
        if p is None:
            return -10**9
        return _base_weight(p.priority) + _age_bonus(pid)

    def _pop_best_pid(candidates):
        # Choose the PID with max effective_score; break ties by FIFO order within the same list.
        best_pid = None
        best_score = None
        for pid in candidates:
            if pid in s.done or pid in s.dead:
                continue
            if pid not in s.pipeline_by_id:
                continue
            sc = _effective_score(pid)
            if best_pid is None or sc > best_score:
                best_pid = pid
                best_score = sc
        return best_pid

    def _get_next_ready_op(p):
        st = p.runtime_status()
        # Only schedule if parents complete; take a single op to reduce monopolization.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]

    def _reservation_fractions():
        # Reserve resources in each pool for high priority if there is any high priority demand.
        have_query_waiting = any(pid not in s.done and pid not in s.dead for pid in s.q_query)
        have_inter_waiting = any(pid not in s.done and pid not in s.dead for pid in s.q_interactive)

        # Reservation targets are intentionally modest; we still want throughput.
        # If query demand exists, reserve more.
        if have_query_waiting:
            return 0.35, 0.35  # (cpu_reserve, ram_reserve)
        if have_inter_waiting:
            return 0.20, 0.20
        return 0.0, 0.0

    def _cap_for_priority(pool, pri):
        # Cap per-assignment to avoid giving a single op the whole pool.
        # Queries get larger caps to reduce latency.
        if _is_query(pri):
            return max(1.0, pool.max_cpu_pool * 0.75), pool.max_ram_pool * 0.75
        if _is_interactive(pri):
            return max(1.0, pool.max_cpu_pool * 0.60), pool.max_ram_pool * 0.60
        return max(1.0, pool.max_cpu_pool * 0.50), pool.max_ram_pool * 0.50

    def _baseline_request(pool, pri, avail_cpu, avail_ram):
        # Conservative default requests: don't consume everything, leave room for others.
        if _is_query(pri):
            cpu = min(avail_cpu, max(1.0, pool.max_cpu_pool * 0.60))
            ram = min(avail_ram, pool.max_ram_pool * 0.55)
        elif _is_interactive(pri):
            cpu = min(avail_cpu, max(1.0, pool.max_cpu_pool * 0.45))
            ram = min(avail_ram, pool.max_ram_pool * 0.45)
        else:
            cpu = min(avail_cpu, max(1.0, pool.max_cpu_pool * 0.35))
            ram = min(avail_ram, pool.max_ram_pool * 0.35)
        return cpu, ram

    def _desired_request(pool, pid, pri, avail_cpu, avail_ram):
        # Use learned desires from failures; otherwise baseline.
        base_cpu, base_ram = _baseline_request(pool, pri, avail_cpu, avail_ram)

        want_cpu = s.desired_cpu.get(pid, base_cpu)
        want_ram = s.desired_ram.get(pid, base_ram)

        # Enforce caps + available.
        cpu_cap, ram_cap = _cap_for_priority(pool, pri)
        cpu = min(avail_cpu, cpu_cap, max(1.0, want_cpu))
        ram = min(avail_ram, ram_cap, max(0.0, want_ram))

        # If we have a lot of free resources, allow modest opportunistic scale-up for latency.
        if _is_query(pri) and avail_cpu >= cpu + 1.0:
            cpu = min(avail_cpu, cpu + 1.0)
        return cpu, ram

    # ---- advance time + ingest arrivals ----
    s.tick += 1

    for p in pipelines:
        _enqueue_pipeline(p)

    # ---- incorporate execution results (learn from failures, mark done) ----
    for r in results:
        # results may reference ops belonging to a pipeline; but we are given pipeline_id only at assignment time.
        # Best effort: update desires using r.ops[0].pipeline_id if present; otherwise use container-local signals.
        # We rely on pipeline objects to reflect state; desires are keyed by pipeline_id when available.
        pid = None
        try:
            # Some implementations include operator objects with pipeline_id
            if getattr(r, "ops", None):
                pid = getattr(r.ops[0], "pipeline_id", None)
        except Exception:
            pid = None

        if pid is not None and pid in s.pipeline_by_id:
            if r.failed():
                s.fail_count[pid] = s.fail_count.get(pid, 0) + 1

                # Increase next-request resources to reduce probability of repeated failure.
                # Favor RAM increases since OOMs are common and fatal.
                last_ram = getattr(r, "ram", None)
                last_cpu = getattr(r, "cpu", None)
                if last_ram is not None:
                    s.desired_ram[pid] = max(s.desired_ram.get(pid, 0.0), last_ram * 1.5)
                else:
                    s.desired_ram[pid] = max(s.desired_ram.get(pid, 0.0), 0.0)

                if last_cpu is not None:
                    # Mild CPU bump; do not explode CPU request.
                    s.desired_cpu[pid] = max(s.desired_cpu.get(pid, 0.0), last_cpu * 1.2)

                # Do NOT mark dead; failures are too costly. Keep retrying.
            # On success of an op, keep desires as-is; could be used for later ops in same pipeline.

    # Mark pipelines done if completed
    for pid, p in list(s.pipeline_by_id.items()):
        if pid in s.done or pid in s.dead:
            continue
        terminal, how = _pipeline_terminal(p)
        if terminal and how == "done":
            s.done.add(pid)

    # Clean queues from done/dead
    def _clean_queue(q):
        i = 0
        while i < len(q):
            pid = q[i]
            if pid in s.done or pid in s.dead or pid not in s.pipeline_by_id:
                q.pop(i)
            else:
                i += 1

    _clean_queue(s.q_query)
    _clean_queue(s.q_interactive)
    _clean_queue(s.q_batch)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Reservation depends on backlog (global, but applied per pool)
    cpu_reserve_frac, ram_reserve_frac = _reservation_fractions()

    # Limit how many new ops we launch per pool per tick (stability + latency)
    max_launches_per_pool = 2

    # ---- main placement loop: iterate pools, place highest priority ready ops ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        launches = 0

        while launches < max_launches_per_pool:
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Build a candidate list: always consider query+interactive first; batch only if resources beyond reserve.
            candidates = []

            # Prefer query then interactive strongly; aging can still help within each group.
            if s.q_query:
                candidates.extend(s.q_query)
            if s.q_interactive:
                candidates.extend(s.q_interactive)

            # Admit batch only if we have headroom beyond reserved resources OR no high-priority candidates.
            reserved_cpu = pool.max_cpu_pool * cpu_reserve_frac
            reserved_ram = pool.max_ram_pool * ram_reserve_frac
            can_run_batch = (avail_cpu > reserved_cpu and avail_ram > reserved_ram) or not candidates
            if can_run_batch and s.q_batch:
                candidates.extend(s.q_batch)

            pid = _pop_best_pid(candidates)
            if pid is None:
                break

            p = s.pipeline_by_id.get(pid)
            if p is None:
                # Drop unknown reference
                _clean_queue(s.q_query)
                _clean_queue(s.q_interactive)
                _clean_queue(s.q_batch)
                break

            # Ensure pipeline not terminal
            terminal, how = _pipeline_terminal(p)
            if terminal:
                if how == "done":
                    s.done.add(pid)
                # Remove from its queue(s)
                _clean_queue(_queue_for_priority(p.priority))
                continue

            op_list = _get_next_ready_op(p)
            if not op_list:
                # Not ready yet; rotate within its queue to avoid head-of-line blocking.
                q = _queue_for_priority(p.priority)
                if pid in q:
                    q.remove(pid)
                    q.append(pid)
                break

            # Enforce reservation for batch work: don't consume below reserved headroom if high-priority might arrive.
            # (Queries/interactive can use reserved.)
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram
            if _is_batch(p.priority) and (s.q_query or s.q_interactive):
                eff_avail_cpu = max(0.0, avail_cpu - reserved_cpu)
                eff_avail_ram = max(0.0, avail_ram - reserved_ram)
                if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                    # Can't run batch right now; rotate batch queue and stop trying in this pool.
                    q = s.q_batch
                    if pid in q:
                        q.remove(pid)
                        q.append(pid)
                    break

            cpu, ram = _desired_request(pool, pid, p.priority, eff_avail_cpu, eff_avail_ram)

            # Must allocate positive CPU and some RAM. If RAM computed to 0 due to constraints, do not schedule.
            if cpu <= 0 or ram <= 0:
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            launches += 1

            # Rotate the pipeline to the back of its queue to improve fairness across pipelines of same priority.
            q = _queue_for_priority(p.priority)
            if pid in q:
                q.remove(pid)
                q.append(pid)

    return suspensions, assignments
