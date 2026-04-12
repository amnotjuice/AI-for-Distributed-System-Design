# policy_key: scheduler_iter_median_simple_019
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045067
# generation_seconds: 39.89
# generated_at: 2026-04-12T01:42:11.752220
@register_scheduler_init(key="scheduler_iter_median_simple_019")
def scheduler_iter_median_simple_019_init(s):
    """Latency-oriented priority scheduler (incremental improvement over naive FIFO).

    Key changes (kept deliberately simple / low-risk):
    - Strict priority order: QUERY > INTERACTIVE > BATCH.
    - Smaller default slices for high-priority work to reduce head-of-line blocking and
      improve concurrency (more simultaneous query/interactive ops).
    - Stronger "headroom" protection: when high-priority backlog exists, heavily cap
      batch scheduling per-pool and preserve minimum free CPU/RAM for high-priority arrivals.
    - Per-pipeline OOM backoff: if an op OOMs, retry future ops from that pipeline with higher RAM.
      (Best-effort mapping from results to pipeline via optional result.pipeline_id, else by op identity.)
    - Simple fairness: round-robin within each priority queue.
    """
    from collections import deque

    s.q = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Per-pipeline adaptive state
    # - ram_mult: increases on OOM to reduce repeated failures/retries
    # - cpu_mult: currently conservative; kept for future iterations
    # - drop: mark pipeline as non-retriable on non-OOM failure
    s.pstate = {}  # pipeline_id -> dict

    # Cursor-like fairness is naturally handled by deque rotation, but we still want to avoid
    # infinite loops when nothing is runnable.
    s._max_rr_scan = 64


@register_scheduler(key="scheduler_iter_median_simple_019")
def scheduler_iter_median_simple_019_scheduler(s, results, pipelines):
    from collections import deque

    def ensure_pstate(pipeline_id):
        st = s.pstate.get(pipeline_id)
        if st is None:
            st = {"ram_mult": 1.0, "cpu_mult": 1.0, "drop": False}
            s.pstate[pipeline_id] = st
        return st

    def is_oom(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def pri_order():
        return (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE)

    def pipeline_done_or_dropped(p):
        rs = p.runtime_status()
        if rs.is_pipeline_successful():
            return True
        pst = s.pstate.get(p.pipeline_id)
        # If we decided to drop, don't keep it around.
        if pst is not None and pst.get("drop", False):
            return True
        return False

    def has_assignable_op(p):
        rs = p.runtime_status()
        if rs.is_pipeline_successful():
            return False
        # If failed and dropped, no.
        pst = s.pstate.get(p.pipeline_id)
        if pst is not None and pst.get("drop", False) and rs.state_counts.get(OperatorState.FAILED, 0) > 0:
            return False
        ops = rs.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return len(ops) > 0

    def get_one_assignable_op(p):
        rs = p.runtime_status()
        ops = rs.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]

    def global_hp_backlog():
        # Any runnable QUERY/INTERACTIVE pipeline indicates we should protect headroom.
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.q[pri]:
                if has_assignable_op(p):
                    return True
        return False

    def choose_cpu_ram(pool, pri, pipeline_id, avail_cpu, avail_ram):
        pst = ensure_pstate(pipeline_id)

        # Latency-first sizing:
        # - Queries: as small as possible to increase concurrency and reduce queueing.
        # - Interactive: small/moderate.
        # - Batch: larger only when allowed.
        if pri == Priority.QUERY:
            base_cpu = 1.0
            base_ram = max(1.0, pool.max_ram_pool * 0.06)
        elif pri == Priority.INTERACTIVE:
            base_cpu = 2.0 if pool.max_cpu_pool >= 2 else 1.0
            base_ram = max(1.0, pool.max_ram_pool * 0.12)
        else:
            # Batch: prefer throughput, but still avoid grabbing the entire pool.
            base_cpu = max(2.0, pool.max_cpu_pool * 0.50)
            base_ram = max(1.0, pool.max_ram_pool * 0.25)

        cpu = base_cpu * pst["cpu_mult"]
        ram = base_ram * pst["ram_mult"]

        # Clamp to available and pool maxima; also enforce minimum 1 CPU/1 RAM.
        cpu = max(1.0, min(cpu, avail_cpu, pool.max_cpu_pool))
        ram = max(1.0, min(ram, avail_ram, pool.max_ram_pool))
        return cpu, ram

    def build_op_identity_index():
        # Best-effort mapping from operator object identity -> pipeline_id.
        # This lets us attribute failures to the right pipeline when ExecutionResult
        # doesn't include pipeline_id.
        op_to_pid = {}
        for pri in pri_order():
            for p in s.q[pri]:
                rs = p.runtime_status()
                if rs.is_pipeline_successful():
                    continue
                # Index a broad set of potentially referenced ops.
                for state in (OperatorState.PENDING, OperatorState.ASSIGNED, OperatorState.RUNNING, OperatorState.FAILED):
                    ops = rs.get_ops([state], require_parents_complete=False)
                    for op in ops:
                        op_to_pid[id(op)] = p.pipeline_id
        return op_to_pid

    # 1) Enqueue new pipelines
    for p in pipelines:
        ensure_pstate(p.pipeline_id)
        s.q[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # 2) Update per-pipeline state based on results (OOM backoff, drop non-OOM failures)
    op_to_pid = None  # build lazily only if needed
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            # Try to infer from ops list
            ops_list = getattr(r, "ops", None) or []
            if ops_list:
                if op_to_pid is None:
                    op_to_pid = build_op_identity_index()
                pid = op_to_pid.get(id(ops_list[0]), None)

        if pid is None:
            # Can't attribute; skip rather than making global changes that can inflate RAM and hurt latency.
            continue

        pst = ensure_pstate(pid)
        if is_oom(getattr(r, "error", None)):
            # Increase RAM for future attempts from this pipeline; cap growth to avoid runaway.
            pst["ram_mult"] = min(pst["ram_mult"] * 1.8, 32.0)
        else:
            # Non-OOM failures: treat as terminal (no retries) to reduce wasted work.
            pst["drop"] = True

    # 3) Scheduling
    suspensions = []
    assignments = []

    hp_backlog = global_hp_backlog()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # When high-priority backlog exists, preserve headroom per pool for future high-priority arrivals.
        # This is intentionally "stronger" than the previous iteration to reduce tail/median latency.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if hp_backlog:
            reserve_cpu = min(1.0, pool.max_cpu_pool * 0.10)
            reserve_ram = min(max(1.0, pool.max_ram_pool * 0.10), pool.max_ram_pool)

        # Additionally, when hp backlog exists, limit batch to at most one assignment per pool per tick.
        batch_assignments_this_pool = 0
        batch_assignments_cap = 1 if hp_backlog else 10**9

        made_any = True
        # Loop while we can place more work. Hard-stop if we can't find runnable work.
        while made_any:
            made_any = False
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Enforce headroom (only against batch; high priority can use full remaining).
            eff_avail_cpu_for_batch = max(0.0, avail_cpu - reserve_cpu)
            eff_avail_ram_for_batch = max(0.0, avail_ram - reserve_ram)

            chosen = None
            chosen_pri = None

            # Strict priority: scan queues in order, round-robin by rotating deques.
            for pri in pri_order():
                q = s.q[pri]
                if not q:
                    continue

                # Bound scanning to avoid O(n^2) when many blocked pipelines exist.
                scans = min(len(q), s._max_rr_scan)
                for _ in range(scans):
                    p = q[0]
                    q.rotate(-1)  # round-robin fairness

                    # Drop completed/dropped pipelines eagerly.
                    if pipeline_done_or_dropped(p):
                        # Remove all occurrences of p from the deque's front if present
                        # (rotation means it might not be at front; we remove one occurrence by rebuilding).
                        q2 = deque([x for x in q if x.pipeline_id != p.pipeline_id])
                        s.q[pri] = q2
                        q = q2
                        if not q:
                            break
                        continue

                    if not has_assignable_op(p):
                        continue

                    # Batch gating under high-priority backlog.
                    if pri == Priority.BATCH_PIPELINE and batch_assignments_this_pool >= batch_assignments_cap:
                        continue

                    chosen = p
                    chosen_pri = pri
                    break

                if chosen is not None:
                    break

            if chosen is None:
                break

            ops = get_one_assignable_op(chosen)
            if not ops:
                # Race with state changes; try again.
                continue

            # Determine available resources based on priority and headroom protection.
            if chosen_pri == Priority.BATCH_PIPELINE and hp_backlog:
                cap_cpu = eff_avail_cpu_for_batch
                cap_ram = eff_avail_ram_for_batch
            else:
                cap_cpu = avail_cpu
                cap_ram = avail_ram

            if cap_cpu < 1.0 or cap_ram < 1.0:
                # Not enough to schedule this priority in this pool right now.
                # Avoid placing batch that would steal the reserved headroom.
                break

            cpu_req, ram_req = choose_cpu_ram(pool, chosen_pri, chosen.pipeline_id, cap_cpu, cap_ram)

            if cpu_req < 1.0 or ram_req < 1.0:
                break

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen.pipeline_id,
                )
            )

            avail_cpu -= cpu_req
            avail_ram -= ram_req
            made_any = True

            if chosen_pri == Priority.BATCH_PIPELINE:
                batch_assignments_this_pool += 1

    return suspensions, assignments
