# policy_key: scheduler_iter_median_simple_007
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043191
# generation_seconds: 36.38
# generated_at: 2026-04-12T01:11:46.774490
@register_scheduler_init(key="scheduler_iter_median_simple_007")
def scheduler_iter_median_simple_007_init(s):
    """Iteration 2: stricter priority, pool-aware gating, and per-operator OOM RAM backoff.

    Goal: reduce (weighted) latency by preventing batch from causing head-of-line blocking
    and by quickly converging on sufficient RAM after OOMs.

    Key changes vs prior attempt:
    - Stable per-priority FIFO deques (no pop/append churn).
    - Pool gating: if multiple pools exist, prefer running high-priority work on pool 0;
      when high-priority backlog exists, batch is limited to non-reserved pools (or capped if single pool).
    - Per-operator RAM multiplier learned from OOM results using result.ops (best-effort mapping).
    - Smaller CPU slices for QUERY/INTERACTIVE to increase concurrency and reduce queueing delay.
    """
    from collections import deque

    s.q = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Best-effort per-operator RAM learning:
    # op_key -> {"ram_mult": float, "last_ram": float}
    s.op_ram = {}

    # Pipeline id -> last seen priority (for safety) and to keep queue clean
    s.known_pipelines = {}

    # Simple knobs (kept in state for quick tuning)
    s.max_scan_per_pri = 64          # bound per-tick scanning work per priority
    s.ram_mult_increase = 1.8        # OOM backoff factor
    s.ram_mult_decrease = 0.95       # gentle decrease after success (avoids staying over-provisioned)
    s.ram_mult_min = 1.0
    s.ram_mult_max = 32.0


@register_scheduler(key="scheduler_iter_median_simple_007")
def scheduler_iter_median_simple_007_scheduler(s, results, pipelines):
    from collections import deque

    def pri_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and ("exceed" in msg or "limit" in msg))

    def op_key(op):
        # Prefer stable identifiers if present; otherwise fall back to python object id.
        return (
            getattr(op, "pipeline_id", None),
            getattr(op, "operator_id", None) or getattr(op, "op_id", None) or getattr(op, "node_id", None),
            getattr(op, "name", None) or getattr(op, "fn_name", None) or getattr(op, "sql", None),
            getattr(op, "stage_id", None),
            id(op),
        )

    def ensure_op_state(k):
        st = s.op_ram.get(k)
        if st is None:
            st = {"ram_mult": 1.0, "last_ram": None}
            s.op_ram[k] = st
        return st

    def enqueue_pipeline(p):
        s.known_pipelines[p.pipeline_id] = p.priority
        s.q[p.priority].append(p)

    def pipeline_done_or_dead(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If FAILED exists, we still consider it runnable because FAILED is in ASSIGNABLE_STATES
        # (simulator retries by moving FAILED->ASSIGNED), primarily for OOM recovery.
        # Non-OOM failures are not reliably detectable without pipeline_id in results, so we retry.
        return False

    def next_runnable_from_queue(pri):
        """Return (pipeline, ops_list) for the next pipeline with a runnable op, else (None, None).

        Keeps FIFO order by rotating inspected elements to the back.
        """
        q = s.q[pri]
        if not q:
            return None, None

        scans = min(len(q), s.max_scan_per_pri)
        for _ in range(scans):
            p = q.popleft()

            if pipeline_done_or_dead(p):
                continue

            st = p.runtime_status()
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if ops:
                # Put pipeline back for future ops (keeps it active)
                q.append(p)
                return p, ops

            # Not runnable now (parents incomplete etc.); rotate to back
            q.append(p)

        return None, None

    def hp_backlog_exists():
        # Quick check: scan a limited number of items to detect ready high-priority work
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            q = s.q[pri]
            scans = min(len(q), 16)
            for i in range(scans):
                p = q[0]
                q.rotate(-1)
                if pipeline_done_or_dead(p):
                    continue
                st = p.runtime_status()
                if st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True):
                    return True
        return False

    def pool_allows_priority(pool_id, pri, hp_backlog):
        # If we have multiple pools, keep pool 0 as a "latency" pool.
        if s.executor.num_pools <= 1:
            return True

        if pool_id == 0:
            # pool 0 always allows high priority; allow batch only if no high backlog
            if pri == Priority.BATCH_PIPELINE and hp_backlog:
                return False
            return True

        # non-zero pools: allow batch always; allow high priority too (spillover) if resources exist
        return True

    def shaped_caps(pool, pri, avail_cpu, avail_ram, hp_backlog):
        # When high-priority backlog exists, cap batch so it can't consume the whole pool.
        if pri == Priority.BATCH_PIPELINE and hp_backlog:
            return min(avail_cpu, pool.max_cpu_pool * 0.50), min(avail_ram, pool.max_ram_pool * 0.60)
        return avail_cpu, avail_ram

    def choose_request(pool, pri, ops, avail_cpu, avail_ram):
        # Smaller CPU slices for latency-sensitive work to increase parallelism and reduce queueing latency.
        if pri == Priority.QUERY:
            base_cpu = max(1.0, min(2.0, pool.max_cpu_pool * 0.20))
            base_ram = max(1.0, pool.max_ram_pool * 0.12)
        elif pri == Priority.INTERACTIVE:
            base_cpu = max(1.0, min(3.0, pool.max_cpu_pool * 0.30))
            base_ram = max(1.0, pool.max_ram_pool * 0.20)
        else:
            # Batch: larger chunks when permitted for throughput.
            base_cpu = max(1.0, min(pool.max_cpu_pool * 0.70, max(3.0, pool.max_cpu_pool * 0.50)))
            base_ram = max(1.0, pool.max_ram_pool * 0.35)

        # Apply learned RAM multiplier based on the specific operator
        k = op_key(ops[0])
        opst = ensure_op_state(k)
        ram_req = base_ram * opst["ram_mult"]
        cpu_req = base_cpu

        cpu_req = max(1.0, min(cpu_req, avail_cpu, pool.max_cpu_pool))
        ram_req = max(1.0, min(ram_req, avail_ram, pool.max_ram_pool))
        return cpu_req, ram_req

    # -----------------------------
    # Enqueue
    # -----------------------------
    for p in pipelines:
        enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # -----------------------------
    # Learn from results (OOM RAM backoff)
    # -----------------------------
    for r in results:
        # Best-effort: use the first op in the result as the key
        rops = getattr(r, "ops", None) or []
        if not rops:
            continue
        k = op_key(rops[0])
        opst = ensure_op_state(k)

        if hasattr(r, "failed") and r.failed():
            if is_oom_error(getattr(r, "error", None)):
                opst["ram_mult"] = min(opst["ram_mult"] * s.ram_mult_increase, s.ram_mult_max)
                opst["last_ram"] = getattr(r, "ram", opst["last_ram"])
        else:
            # Success: very gently decay multiplier toward 1.0 to avoid permanent overallocation.
            opst["ram_mult"] = max(s.ram_mult_min, opst["ram_mult"] * s.ram_mult_decrease)
            opst["last_ram"] = getattr(r, "ram", opst["last_ram"])

    # -----------------------------
    # Schedule (no preemption in this iteration: focus on admission/placement)
    # -----------------------------
    suspensions = []
    assignments = []

    hp_backlog = hp_backlog_exists()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep filling the pool; always select highest priority runnable op allowed in this pool.
        while avail_cpu > 0 and avail_ram > 0:
            chosen = None  # (pri, pipeline, ops)
            for pri in pri_order():
                if not pool_allows_priority(pool_id, pri, hp_backlog):
                    continue
                p, ops = next_runnable_from_queue(pri)
                if p is not None and ops:
                    chosen = (pri, p, ops)
                    break

            if chosen is None:
                break

            pri, p, ops = chosen

            cpu_cap, ram_cap = shaped_caps(pool, pri, avail_cpu, avail_ram, hp_backlog)
            if cpu_cap <= 0 or ram_cap <= 0:
                break

            cpu_req, ram_req = choose_request(pool, pri, ops, cpu_cap, ram_cap)
            if cpu_req <= 0 or ram_req <= 0:
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

            # Locally track remaining capacity to avoid overscheduling within this tick
            avail_cpu -= cpu_req
            avail_ram -= ram_req

    return suspensions, assignments
