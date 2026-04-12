# policy_key: scheduler_iter_best_rich_019
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050569
# generation_seconds: 43.75
# generated_at: 2026-04-12T00:33:18.273015
@register_scheduler_init(key="scheduler_iter_best_rich_019")
def scheduler_iter_best_rich_019_init(s):
    """Priority-aware scheduler focused on reducing weighted latency.

    Incremental improvements over the previous priority FIFO:
      - Dedicated capacity bias: prefer scheduling QUERY/INTERACTIVE in early pools; push BATCH to later pools.
      - Right-size memory: start with small RAM requests and learn per-operator required RAM from OOM/success.
        This reduces "allocated >> consumed" waste and increases concurrency (fewer timeouts).
      - Right-size CPU: learn per-operator CPU from timeout/success and bias more CPU to higher priorities.

    Design constraints:
      - No preemption/suspension (not enough stable visibility into running containers in the provided interface).
      - One operator scheduled at a time per pipeline (keeps semantics close to baseline).
    """
    # Per-priority round-robin queues of pipelines
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # If we detect a non-resource failure repeatedly we can stop rescheduling it (best-effort).
    s.dead_pipelines = set()

    # Learned per-operator resource estimates (keys include pipeline_id when available)
    s.op_ram_req = {}   # op_key -> required_ram (float)
    s.op_cpu_req = {}   # op_key -> preferred_cpu (float)

    # Track a small rolling failure count to avoid infinite thrash on hopeless ops
    s.op_fail_count = {}  # op_key -> int

    # Soft caps to prevent runaway backoff
    s.max_ram_mult = 16.0
    s.max_cpu_mult = 8.0

    def _op_key(op, pipeline_id):
        """Stable-ish key for operator across ticks/results."""
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = getattr(op, "name", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_rich_019")
def scheduler_iter_best_rich_019_scheduler(s, results, pipelines):
    """Priority-first, multi-pool biased placement with adaptive RAM/CPU sizing from execution feedback."""
    # ---------------- helpers ----------------
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and ("exceed" in msg or "alloc" in msg))

    def _is_timeout_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return "timeout" in msg or "timed out" in msg or "deadline" in msg

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _queues_in_priority_order():
        return [s.q_query, s.q_interactive, s.q_batch]

    def _pipeline_has_runnable_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return False, None
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            return False, None
        return True, op_list

    def _pop_next_runnable_pipeline(allow_batch: bool):
        # Round-robin scan within each priority queue; return first runnable pipeline.
        queues = [s.q_query, s.q_interactive] + ([s.q_batch] if allow_batch else [])
        for q in queues:
            if not q:
                continue
            n = len(q)
            for _ in range(n):
                p = q.pop(0)
                if p.pipeline_id in s.dead_pipelines:
                    continue
                ok, op_list = _pipeline_has_runnable_op(p)
                if ok:
                    return p, op_list
                # Not runnable yet; rotate to back.
                q.append(p)
        return None, None

    def _base_ram_frac(priority):
        # Start small to avoid wasting memory (prior run had alloc ~93% but consumption ~29%).
        # Adaptive learning will increase this only when OOM occurs.
        if priority == Priority.QUERY:
            return 0.04
        if priority == Priority.INTERACTIVE:
            return 0.06
        return 0.08  # batch

    def _base_cpu_frac(priority):
        # Give higher priorities enough CPU to avoid timeouts, but keep concurrency.
        if priority == Priority.QUERY:
            return 0.35
        if priority == Priority.INTERACTIVE:
            return 0.40
        return 0.50

    def _cpu_cap(priority, pool):
        # Upper bound per assignment to prevent single op monopolization
        max_cpu = pool.max_cpu_pool
        if priority == Priority.QUERY:
            return max(1.0, min(8.0, max_cpu * 0.60))
        if priority == Priority.INTERACTIVE:
            return max(1.0, min(12.0, max_cpu * 0.70))
        return max(1.0, min(16.0, max_cpu * 0.85))

    def _ram_cap(priority, pool):
        # Upper bound per assignment; memory learning should rarely hit this.
        max_ram = pool.max_ram_pool
        if priority == Priority.QUERY:
            return max_ram * 0.45
        if priority == Priority.INTERACTIVE:
            return max_ram * 0.55
        return max_ram * 0.70

    def _size_for(p, pool, op, avail_cpu, avail_ram):
        # Determine per-op key; pipeline_id always known here.
        op_key = s._op_key(op, p.pipeline_id)

        # --- RAM sizing ---
        learned_ram = s.op_ram_req.get(op_key, None)
        if learned_ram is None:
            # Start with a small fraction of pool RAM; learning increases on OOM.
            ram = pool.max_ram_pool * _base_ram_frac(p.priority)
        else:
            ram = learned_ram

        # Clamp RAM
        ram = min(ram, _ram_cap(p.priority, pool), pool.max_ram_pool, avail_ram)

        # --- CPU sizing ---
        learned_cpu = s.op_cpu_req.get(op_key, None)
        if learned_cpu is None:
            cpu = pool.max_cpu_pool * _base_cpu_frac(p.priority)
        else:
            cpu = learned_cpu

        # Clamp CPU
        cpu = min(cpu, _cpu_cap(p.priority, pool), pool.max_cpu_pool, avail_cpu)

        # Enforce tiny minimums so we don't place "zero-resource" work
        if cpu < 0.5:
            cpu = 0.0
        if ram < 0.5:
            ram = 0.0

        return cpu, ram, op_key

    # ---------------- ingest new pipelines ----------------
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ---------------- process results: learn RAM/CPU requirements ----------------
    for r in results:
        # We may not always have pipeline_id on result; use it if present, else None.
        r_pid = getattr(r, "pipeline_id", None)

        if r.failed():
            # Update failure counts and adjust resources based on error class.
            for op in (r.ops or []):
                op_key = s._op_key(op, r_pid)
                s.op_fail_count[op_key] = s.op_fail_count.get(op_key, 0) + 1
                fail_n = s.op_fail_count[op_key]

                # If an op is failing repeatedly, become more aggressive quickly.
                oom = _is_oom_error(r.error)
                tmo = _is_timeout_error(r.error)

                if oom:
                    # Increase required RAM substantially; keep some headroom to avoid repeated OOM.
                    cur = s.op_ram_req.get(op_key, max(1.0, float(getattr(r, "ram", 1.0))))
                    # Multiplicative backoff anchored on the attempted RAM.
                    attempted = max(1.0, float(getattr(r, "ram", cur)))
                    nxt = max(cur, attempted) * (2.0 if fail_n <= 2 else 1.5)
                    # Soft cap
                    pool_max_ram = None
                    if getattr(r, "pool_id", None) is not None:
                        try:
                            pool_max_ram = s.executor.pools[r.pool_id].max_ram_pool
                        except Exception:
                            pool_max_ram = None
                    if pool_max_ram is not None:
                        nxt = min(nxt, pool_max_ram)
                    s.op_ram_req[op_key] = nxt

                    # Also bump CPU a bit (OOM retries often benefit from finishing faster once they fit)
                    cur_cpu = s.op_cpu_req.get(op_key, max(1.0, float(getattr(r, "cpu", 1.0))))
                    s.op_cpu_req[op_key] = min(cur_cpu * 1.2, cur_cpu * s.max_cpu_mult)

                elif tmo:
                    # Increase CPU to reduce runtime / avoid deadline timeouts.
                    cur_cpu = s.op_cpu_req.get(op_key, max(1.0, float(getattr(r, "cpu", 1.0))))
                    attempted_cpu = max(1.0, float(getattr(r, "cpu", cur_cpu)))
                    nxt_cpu = max(cur_cpu, attempted_cpu) * (1.6 if fail_n <= 2 else 1.3)
                    # Soft cap by pool if possible
                    pool_max_cpu = None
                    if getattr(r, "pool_id", None) is not None:
                        try:
                            pool_max_cpu = s.executor.pools[r.pool_id].max_cpu_pool
                        except Exception:
                            pool_max_cpu = None
                    if pool_max_cpu is not None:
                        nxt_cpu = min(nxt_cpu, pool_max_cpu)
                    s.op_cpu_req[op_key] = min(nxt_cpu, max(1.0, attempted_cpu) * s.max_cpu_mult)

                    # Nudge RAM slightly upward too to reduce incidental memory-pressure failures.
                    cur_ram = s.op_ram_req.get(op_key, max(1.0, float(getattr(r, "ram", 1.0))))
                    attempted_ram = max(1.0, float(getattr(r, "ram", cur_ram)))
                    s.op_ram_req[op_key] = max(cur_ram, attempted_ram) * 1.1

                else:
                    # Unknown failure: after a few tries, stop thrashing this op by marking it "hopeless".
                    # We can only reliably kill a whole pipeline if we can map to pipeline_id; otherwise we just stop adapting.
                    if fail_n >= 3 and r_pid is not None:
                        s.dead_pipelines.add(r_pid)

        else:
            # Success: record that the attempted resources were sufficient.
            # Also gently decrease RAM estimate over time to reduce over-allocation.
            for op in (r.ops or []):
                op_key = s._op_key(op, r_pid)

                # On success, the actual required RAM is <= allocated RAM.
                # Use a conservative "upper bound" that slowly decays to improve concurrency.
                alloc_ram = max(1.0, float(getattr(r, "ram", 1.0)))
                prev = s.op_ram_req.get(op_key, alloc_ram)
                # Decay towards 0.85*alloc, but never below a small fraction of previous to avoid oscillations.
                target = alloc_ram * 0.85
                s.op_ram_req[op_key] = max(min(prev, alloc_ram), min(prev, target))

                # CPU: keep a preferred CPU near what succeeded; don't aggressively shrink to avoid timeouts.
                alloc_cpu = max(1.0, float(getattr(r, "cpu", 1.0)))
                prev_cpu = s.op_cpu_req.get(op_key, alloc_cpu)
                s.op_cpu_req[op_key] = max(prev_cpu * 0.9, alloc_cpu * 0.95)

                # Reset fail counter on success
                if op_key in s.op_fail_count:
                    s.op_fail_count[op_key] = 0

    suspensions = []  # no preemption in this iteration
    assignments = []

    # ---------------- placement / admission policy ----------------
    # Bias pools:
    #   - pool 0: QUERY + INTERACTIVE only (unless both empty)
    #   - pool 1: INTERACTIVE + (QUERY spillover) + limited BATCH
    #   - pools 2+: anything, but still priority-ordered
    #
    # This reduces interference on high priority work, improving weighted latency.
    def _allow_batch_in_pool(pool_id: int) -> bool:
        if pool_id == 0:
            return (len(s.q_query) == 0 and len(s.q_interactive) == 0)
        if pool_id == 1:
            # allow batch if no query pending; keep interactive responsive
            return (len(s.q_query) == 0)
        return True

    # To reduce churn, cap number of new assignments per pool per tick.
    # (Still fill pools, but avoid runaway micro-fragmentation from tiny allocations.)
    max_assignments_per_pool = 64

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        allow_batch = _allow_batch_in_pool(pool_id)
        made = 0

        while made < max_assignments_per_pool and avail_cpu > 0.5 and avail_ram > 0.5:
            p, op_list = _pop_next_runnable_pipeline(allow_batch=allow_batch)
            if not p:
                break

            op = op_list[0]
            cpu, ram, op_key = _size_for(p, pool, op, avail_cpu, avail_ram)

            # If we can't allocate enough, requeue and stop trying to pack this pool further this tick.
            if cpu <= 0.0 or ram <= 0.0:
                _enqueue_pipeline(p)
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
            made += 1
            avail_cpu -= cpu
            avail_ram -= ram

            # Requeue for round-robin fairness within its priority class
            _enqueue_pipeline(p)

    return suspensions, assignments
