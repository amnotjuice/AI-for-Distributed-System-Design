# policy_key: scheduler_iter_best_simple_017
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.055213
# generation_seconds: 42.10
# generated_at: 2026-04-11T22:48:57.544186
@register_scheduler_init(key="scheduler_iter_best_simple_017")
def scheduler_iter_best_simple_017_init(s):
    """Priority-first, packing-friendly scheduler to reduce weighted latency.

    Incremental improvements over naive FIFO / prior simple priority scheduler:
      1) Strict priority queues (QUERY > INTERACTIVE > BATCH) to minimize queueing for high weight work.
      2) Much smaller default RAM allocations (RAM doesn't speed execution), with OOM-triggered backoff per-operator.
         This increases concurrency and reduces head-of-line blocking for interactive/query workloads.
      3) Pool-aware placement: if multiple pools exist, reserve pool 0 primarily for QUERY+INTERACTIVE.
         Other pools focus on BATCH but will opportunistically run high-priority work if idle.
      4) Light CPU right-sizing: give QUERY/INTERACTIVE enough CPU to finish quickly without monopolizing pools.

    Intentionally still simple:
      - No preemption (insufficient stable visibility into running containers in the provided interface).
      - No SRPT; relies on packing + strict priority to cut tail/weighted latency.
    """
    # Per-priority waiting queues (simple lists as in template).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator RAM multiplier for OOM retries: op_key -> multiplier (1,2,4,...)
    s.op_ram_mult = {}

    # Per-operator "known-good" RAM hint from successful executions: op_key -> ram
    # (we keep the minimum observed successful RAM allocation as a tight packing hint)
    s.op_ram_hint = {}

    # Optional: track last-seen tick count for minor debugging/aging hooks
    s.tick = 0

    def _op_key(op):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return oid

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_simple_017")
def scheduler_iter_best_simple_017_scheduler(s, results, pipelines):
    """Strict priority + RAM packing + OOM backoff + pool-aware placement."""
    s.tick += 1

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _enqueue(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _has_high_priority_waiting():
        return bool(s.q_query) or bool(s.q_interactive)

    # --- ingest new pipelines ---
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # --- learn from results (RAM hints + OOM backoff) ---
    for r in results:
        # Update hints on successes: if an op succeeded with RAM=r.ram, we can try that (or less) next time.
        if not r.failed():
            if getattr(r, "ops", None):
                for op in (r.ops or []):
                    k = s._op_key(op)
                    prev = s.op_ram_hint.get(k, None)
                    if prev is None:
                        s.op_ram_hint[k] = r.ram
                    else:
                        # Keep the tightest known-good allocation to improve packing.
                        s.op_ram_hint[k] = min(prev, r.ram)
            continue

        # Failures: if OOM-like, increase multiplier for those ops.
        if _is_oom_error(r.error):
            for op in (r.ops or []):
                k = s._op_key(op)
                cur = s.op_ram_mult.get(k, 1.0)
                nxt = cur * 2.0
                if nxt > 32.0:
                    nxt = 32.0
                s.op_ram_mult[k] = nxt
        # For non-OOM failures we don't do anything special here; pipeline runtime_status()
        # will expose FAILED and ASSIGNABLE_STATES may allow retry; if simulator treats
        # FAILED as terminal, it will naturally stop being runnable.

    suspensions = []
    assignments = []

    def _pick_queue_for_pool(pool_id: int):
        # If we have multiple pools, keep pool 0 primarily for QUERY/INTERACTIVE to reduce interference.
        # Other pools focus on BATCH but can steal high-priority work if idle.
        if s.executor.num_pools >= 2 and pool_id == 0:
            return [s.q_query, s.q_interactive, s.q_batch]
        else:
            # Batch-first on non-reserved pools, but never block high priority if present and pool has headroom.
            if _has_high_priority_waiting():
                return [s.q_query, s.q_interactive, s.q_batch]
            return [s.q_batch, s.q_interactive, s.q_query]

    def _pop_next_runnable(queue_order):
        # Round-robin within each queue: rotate non-runnable pipelines to the back.
        for q in queue_order:
            if not q:
                continue
            n = len(q)
            for _ in range(n):
                p = q.pop(0)
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if ops:
                    return p, ops
                q.append(p)
        return None, None

    def _cpu_request(priority, avail_cpu, pool):
        # CPU sizing: enough to reduce service time for high priority, but keep concurrency.
        # Caps are absolute and relative; tune later based on Eudoxia runs.
        max_cpu = pool.max_cpu_pool

        if priority == Priority.QUERY:
            cap_abs = 4.0
            cap_rel = 0.50 * max_cpu
            target = min(cap_abs, cap_rel)
            # Ensure at least 1 vCPU if possible.
            cpu = min(avail_cpu, max(1.0, target))
        elif priority == Priority.INTERACTIVE:
            cap_abs = 6.0
            cap_rel = 0.60 * max_cpu
            target = min(cap_abs, cap_rel)
            cpu = min(avail_cpu, max(1.0, target))
        else:
            # Batch: be more conservative when high priority exists to preserve headroom.
            if _has_high_priority_waiting():
                cap_abs = 2.0
                cap_rel = 0.25 * max_cpu
            else:
                cap_abs = 8.0
                cap_rel = 0.75 * max_cpu
            target = min(cap_abs, cap_rel)
            cpu = min(avail_cpu, max(1.0, target)) if avail_cpu >= 1.0 else avail_cpu

        return cpu

    def _ram_request(priority, avail_ram, pool, op):
        # Default RAM is intentionally small to improve packing.
        # If we have a known-good hint, use it; if we previously OOMed, multiply up.
        max_ram = pool.max_ram_pool
        k = s._op_key(op)

        # Priority-specific small base fractions.
        if priority == Priority.QUERY:
            base = 0.04 * max_ram
        elif priority == Priority.INTERACTIVE:
            base = 0.06 * max_ram
        else:
            base = 0.10 * max_ram

        hint = s.op_ram_hint.get(k, None)
        if hint is not None:
            base = min(base, hint)

        mult = s.op_ram_mult.get(k, 1.0)
        ram = base * mult

        # Clamp to what's available and pool limits; also avoid zero allocations.
        ram = min(ram, avail_ram, max_ram)
        if ram <= 0.0 and avail_ram > 0.0:
            ram = min(avail_ram, max_ram)

        return ram

    # --- schedule work on each pool ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # On non-reserved pools, keep a small CPU/RAM buffer if high priority is waiting,
        # so we don't immediately fill the pool with batch and delay high-priority admission.
        if s.executor.num_pools >= 2 and pool_id != 0 and _has_high_priority_waiting():
            cpu_buffer = 1.0
            ram_buffer = 0.02 * pool.max_ram_pool
        else:
            cpu_buffer = 0.0
            ram_buffer = 0.0

        # Fill the pool while leaving the buffer.
        while avail_cpu - cpu_buffer > 0.01 and avail_ram - ram_buffer > 0.01:
            queue_order = _pick_queue_for_pool(pool_id)
            p, ops = _pop_next_runnable(queue_order)
            if p is None:
                break

            op = ops[0]
            cpu = _cpu_request(p.priority, avail_cpu - cpu_buffer, pool)
            ram = _ram_request(p.priority, avail_ram - ram_buffer, pool, op)

            # If we can't give at least some meaningful resources, put it back and stop on this pool.
            if cpu <= 0.01 or ram <= 0.01:
                _enqueue(p)
                break

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            avail_cpu -= cpu
            avail_ram -= ram

            # Rotate pipeline to the back of its priority queue for fairness within that priority.
            _enqueue(p)

    return suspensions, assignments
