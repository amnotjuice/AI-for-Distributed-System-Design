# policy_key: scheduler_iter_worst_simple_015
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051234
# generation_seconds: 40.32
# generated_at: 2026-04-12T00:44:40.155330
@register_scheduler_init(key="scheduler_iter_worst_simple_015")
def scheduler_iter_worst_simple_015_init(s):
    """
    Iteration 2: Priority-first multi-dispatch with pool reservation + failure-aware RAM backoff.

    Key improvements vs prior version / naive FIFO:
      - Strict priority queues (QUERY > INTERACTIVE > BATCH) and round-robin within each.
      - Fill each pool with multiple assignments per tick (not just one), improving utilization and
        reducing queueing delay for high priority (weighted latency).
      - Reserve pool 0 for latency-sensitive work when multiple pools exist (batch uses it only when idle).
      - Do NOT drop pipelines just because they contain FAILED ops; FAILED is retryable (ASSIGNABLE_STATES).
      - Learn RAM hints from failures and apply on subsequent retries (op-signature based).
      - Small anti-starvation: batch aging promotion to INTERACTIVE after a wait threshold.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    s.tick = 0
    s.enqueued_at = {}  # pipeline_id -> tick

    # Learn per-operator RAM requirement from observed failures (best-effort signature).
    s.op_ram_hint = {}  # op_sig -> ram
    s.op_cpu_hint = {}  # op_sig -> cpu (kept conservative; CPU bumps are small)

    # Batch aging promotion threshold (in scheduler ticks)
    s.batch_promotion_ticks = 150

    # Minimal slices to avoid 0 allocations
    s.min_cpu = 1
    s.min_ram = 1


@register_scheduler(key="scheduler_iter_worst_simple_015")
def scheduler_iter_worst_simple_015_scheduler(s, results, pipelines):
    """
    Priority-aware, multi-assignment scheduler.

    Dispatch model:
      - Maintain per-priority FIFO queues of pipelines.
      - For each pool, repeatedly pick the next runnable operator from the best available priority
        (with pool reservation rules) and assign it, updating local available CPU/RAM.
      - Requeue pipelines after consideration to preserve round-robin fairness.
      - Apply RAM backoff hints for ops that failed previously (especially OOM-like).
    """
    s.tick += 1

    # --- helpers (no module-level imports) ---
    def _base_rank(priority):
        if priority == Priority.QUERY:
            return 3
        if priority == Priority.INTERACTIVE:
            return 2
        return 1  # Priority.BATCH_PIPELINE or unknown -> lowest

    def _queue_for_priority(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid in s.enqueued_at:
            return
        s.enqueued_at[pid] = s.tick
        _queue_for_priority(p.priority).append(p)

    def _drop_bookkeeping(pid):
        if pid in s.enqueued_at:
            del s.enqueued_at[pid]

    def _effective_priority(p):
        # Anti-starvation: promote old batch to interactive for dispatch purposes.
        if p.priority == Priority.BATCH_PIPELINE:
            waited = s.tick - s.enqueued_at.get(p.pipeline_id, s.tick)
            if waited >= s.batch_promotion_ticks:
                return Priority.INTERACTIVE
        return p.priority

    def _op_sig(op):
        # Best-effort stable signature across retries.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        # Fallback: Python id (may not be stable across reconstruction, but works within-run often)
        return ("py_id", id(op))

    def _oom_like(err_text):
        e = (err_text or "").lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e)

    def _ready_ops(p):
        st = p.runtime_status()
        return st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []

    def _pipeline_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _pool_allows_priority(pool_id, eff_prio):
        # Reserve pool 0 for QUERY/INTERACTIVE when multiple pools exist.
        if s.executor.num_pools <= 1:
            return True
        if pool_id != 0:
            return True
        return eff_prio in (Priority.QUERY, Priority.INTERACTIVE)

    def _caps_for_eff_priority(eff_prio, pool):
        # CPU caps keep batch from consuming entire pools; for latency-sensitive, allow scale-up.
        if eff_prio == Priority.QUERY:
            return 1.0, 0.95
        if eff_prio == Priority.INTERACTIVE:
            return 0.95, 0.95
        return 0.60, 0.80  # batch: more conservative CPU cap; RAM cap higher to avoid OOMs

    def _pick_next_runnable(pool_id, allow_batch_on_pool0):
        """
        Return (pipeline, op_list, eff_priority) or (None, None, None).
        Implements strict priority + RR scan bounded by queue length.
        """
        # Consider queues ordered by effective priority, but keep stable RR by scanning deques.
        candidates = [s.q_query, s.q_interactive, s.q_batch]

        # If pool 0 is reserved, optionally disallow batch unless explicitly allowed.
        for q in candidates:
            qlen = len(q)
            for _ in range(qlen):
                p = q.popleft()

                # Drop completed pipelines eagerly.
                if _pipeline_done(p):
                    _drop_bookkeeping(p.pipeline_id)
                    continue

                eff_prio = _effective_priority(p)

                if (pool_id == 0) and (s.executor.num_pools > 1):
                    if eff_prio == Priority.BATCH_PIPELINE and not allow_batch_on_pool0:
                        # Put it back and keep scanning.
                        q.append(p)
                        continue
                    if not _pool_allows_priority(pool_id, eff_prio):
                        q.append(p)
                        continue

                ops = _ready_ops(p)
                if not ops:
                    q.append(p)
                    continue

                # Found runnable; requeue pipeline to preserve RR before returning.
                q.append(p)
                return p, ops[:1], eff_prio

        return None, None, None

    # --- ingest new pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if no new work and no new signals
    if not pipelines and not results:
        return [], []

    # --- learn from results (RAM backoff, slight CPU bump) ---
    for r in results:
        if hasattr(r, "failed") and r.failed():
            err = str(getattr(r, "error", "") or "")
            ops = getattr(r, "ops", []) or []

            # These are what the last attempt used.
            last_ram = int(getattr(r, "ram", 0) or 0)
            last_cpu = int(getattr(r, "cpu", 0) or 0)

            for op in ops:
                sig = _op_sig(op)

                prev_ram = int(s.op_ram_hint.get(sig, max(s.min_ram, last_ram if last_ram > 0 else s.min_ram)))
                prev_cpu = int(s.op_cpu_hint.get(sig, max(s.min_cpu, last_cpu if last_cpu > 0 else s.min_cpu)))

                if _oom_like(err):
                    # OOM: aggressively increase RAM (doubling) up to a reasonable bound later (pool max).
                    s.op_ram_hint[sig] = max(prev_ram, max(s.min_ram, (last_ram if last_ram > 0 else prev_ram) * 2))
                    # CPU won't fix OOM; keep it stable.
                    s.op_cpu_hint[sig] = prev_cpu
                else:
                    # Unknown failure: small bump to both (conservative).
                    s.op_ram_hint[sig] = max(prev_ram, (last_ram if last_ram > 0 else prev_ram) + 1)
                    s.op_cpu_hint[sig] = max(prev_cpu, (last_cpu if last_cpu > 0 else prev_cpu) + 1)

    suspensions = []  # No active preemption in this iteration (keep churn low)
    assignments = []

    # --- dispatch: fill each pool with as many assignments as possible ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = int(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pool 0 reservation: only allow batch on pool 0 if there is no runnable query/interactive.
        allow_batch_on_pool0 = False
        if pool_id == 0 and s.executor.num_pools > 1:
            # Quick probe: if anything runnable exists in high-priority queues, keep batch off pool 0.
            # (Bounded probes by queue lengths, but without modifying queues permanently.)
            has_hi_runnable = False
            for q in (s.q_query, s.q_interactive):
                for p in list(q):
                    if _pipeline_done(p):
                        continue
                    if _ready_ops(p):
                        has_hi_runnable = True
                        break
                if has_hi_runnable:
                    break
            allow_batch_on_pool0 = not has_hi_runnable

        # Keep assigning while resources remain.
        while avail_cpu >= s.min_cpu and avail_ram >= s.min_ram:
            p, op_list, eff_prio = _pick_next_runnable(pool_id, allow_batch_on_pool0)
            if p is None:
                break

            op = op_list[0]
            sig = _op_sig(op)

            # Decide request size: prioritize queueing delay and completion time for high priorities.
            cpu_cap_frac, ram_cap_frac = _caps_for_eff_priority(eff_prio, pool)
            cpu_cap = max(s.min_cpu, int(pool.max_cpu_pool * cpu_cap_frac))
            ram_cap = max(s.min_ram, int(pool.max_ram_pool * ram_cap_frac))

            # Base CPU: for QUERY, scale up more aggressively (use remaining CPU up to cap).
            if eff_prio == Priority.QUERY:
                base_cpu = min(cpu_cap, avail_cpu)
            elif eff_prio == Priority.INTERACTIVE:
                base_cpu = min(cpu_cap, max(2, min(avail_cpu, 4)))
            else:
                base_cpu = min(cpu_cap, max(1, min(avail_cpu, 2)))

            # RAM: request just enough (hinted) to avoid OOM; extra RAM doesn't speed execution.
            hinted_ram = int(s.op_ram_hint.get(sig, 4))
            base_ram = max(4, hinted_ram)
            req_ram = min(ram_cap, avail_ram, max(s.min_ram, base_ram))

            # CPU hints (rarely used): respect, but never exceed caps/availability.
            hinted_cpu = int(s.op_cpu_hint.get(sig, 0) or 0)
            req_cpu = min(cpu_cap, avail_cpu, max(s.min_cpu, base_cpu, hinted_cpu))

            if req_cpu < s.min_cpu or req_ram < s.min_ram:
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,  # keep original priority for accounting
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Update local availability for additional dispatches this tick.
            avail_cpu -= req_cpu
            avail_ram -= req_ram

    return suspensions, assignments
