# policy_key: scheduler_iter_worst_simple_010
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050898
# generation_seconds: 45.89
# generated_at: 2026-04-12T00:41:18.064427
@register_scheduler_init(key="scheduler_iter_worst_simple_010")
def scheduler_iter_worst_simple_010_init(s):
    """
    Priority-first, SRPT-ish scheduler with conservative batch packing and OOM-aware RAM backoff.

    Incremental improvements over naive FIFO / prior attempt:
      - Strict priority order (QUERY > INTERACTIVE > BATCH), with optional aging for BATCH.
      - Fill each pool with multiple assignments per tick (not just one), packing by remaining resources.
      - Choose among runnable ops using a simple "predicted runtime" (EWMA from observed results),
        approximating SRPT within each priority class to reduce queueing/weighted latency.
      - Keep pool 0 biased toward latency-sensitive work when multiple pools exist.
      - On failure, increase RAM hint aggressively for the specific op and retry (do NOT drop pipeline).
    """
    from collections import deque

    # Priority queues of pipelines (pipelines are requeued; ops picked when runnable)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Bookkeeping
    s.tick = 0
    s.pipeline_enqueued_at = {}  # pipeline_id -> tick first seen

    # Per-op learned hints
    s.op_ram_hint = {}   # (pipeline_id, op_key) -> ram units to request
    s.op_cpu_hint = {}   # (pipeline_id, op_key) -> cpu units to request
    s.op_time_ewma = {}  # (pipeline_id, op_key) -> predicted runtime (seconds or ticks), best-effort

    # Behavior knobs (kept conservative and simple)
    s.batch_promotion_ticks = 120  # reduce starvation a bit, but keep latency bias
    s.max_scan_per_queue = 50      # bound per-tick work
    s.min_cpu_slice = 1
    s.min_ram_slice = 1

    # EWMA smoothing for observed runtimes (if duration is available in ExecutionResult)
    s.ewma_alpha = 0.35


@register_scheduler(key="scheduler_iter_worst_simple_010")
def scheduler_iter_worst_simple_010_scheduler(s, results, pipelines):
    """
    Step:
      1) Enqueue new pipelines.
      2) Learn from results: OOM -> bump RAM hint; record runtime EWMA if available.
      3) For each pool, repeatedly assign runnable ops while resources remain:
           - Prefer latency-sensitive work on pool 0 (if multiple pools).
           - Pick highest effective priority, then smallest predicted runtime (SRPT-ish).
           - Cap batch per-assignment to preserve headroom and reduce interference.
    """
    import heapq
    from collections import deque

    s.tick += 1

    # ---------------- Helpers ----------------
    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _maybe_enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.pipeline_enqueued_at:
            s.pipeline_enqueued_at[pid] = s.tick
            _queue_for_priority(p.priority).append(p)

    def _drop_pipeline_bookkeeping(pid):
        s.pipeline_enqueued_at.pop(pid, None)

    def _effective_priority(p):
        # Promote long-waiting batch slightly to avoid infinite starvation.
        if p.priority == Priority.BATCH_PIPELINE:
            waited = s.tick - s.pipeline_enqueued_at.get(p.pipeline_id, s.tick)
            if waited >= s.batch_promotion_ticks:
                return Priority.INTERACTIVE
        return p.priority

    def _prio_rank(prio):
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1

    def _op_key(op):
        # Best-effort stable identity for learning. Prefer explicit ids if available.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _get_runnable_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return None
        # NOTE: allow retries of FAILED ops; do not drop pipeline just because some op failed.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]  # one op at a time per pipeline

    def _pool_bias_allows(pool_id, eff_prio):
        # If multiple pools, bias pool 0 for latency-sensitive work.
        if s.executor.num_pools <= 1:
            return True
        if pool_id == 0:
            # Prefer QUERY/INTERACTIVE on pool 0; allow batch only if nothing else fits later.
            return eff_prio in (Priority.QUERY, Priority.INTERACTIVE)
        else:
            # Non-zero pools can run anything.
            return True

    def _caps_for_priority(eff_prio, pool):
        # Caps are fractions of pool max per assignment.
        # Key change for latency: batch is heavily capped; interactive/query can burst.
        if eff_prio == Priority.QUERY:
            return 1.0, 1.0
        if eff_prio == Priority.INTERACTIVE:
            return 0.9, 0.95
        # BATCH
        return 0.35, 0.45

    def _size_for_op(p, op, pool_id, rem_cpu, rem_ram):
        pool = s.executor.pools[pool_id]
        eff_prio = _effective_priority(p)
        cpu_cap_frac, ram_cap_frac = _caps_for_priority(eff_prio, pool)

        # Cap per-assignment to avoid one big batch op blocking everything.
        cpu_cap = max(s.min_cpu_slice, int(pool.max_cpu_pool * cpu_cap_frac))
        ram_cap = max(s.min_ram_slice, int(pool.max_ram_pool * ram_cap_frac))

        key = (p.pipeline_id, _op_key(op))
        hinted_cpu = int(s.op_cpu_hint.get(key, 0) or 0)
        hinted_ram = int(s.op_ram_hint.get(key, 0) or 0)

        # Base sizing: give higher-priority ops more CPU to reduce their latency.
        if eff_prio == Priority.QUERY:
            base_cpu = min(8, pool.max_cpu_pool)
            base_ram = min(8, pool.max_ram_pool)
        elif eff_prio == Priority.INTERACTIVE:
            base_cpu = min(4, pool.max_cpu_pool)
            base_ram = min(6, pool.max_ram_pool)
        else:
            base_cpu = min(2, pool.max_cpu_pool)
            base_ram = min(4, pool.max_ram_pool)

        req_cpu = max(s.min_cpu_slice, base_cpu, hinted_cpu)
        req_ram = max(s.min_ram_slice, base_ram, hinted_ram)

        # Enforce per-assignment caps and remaining availability.
        req_cpu = min(req_cpu, cpu_cap, rem_cpu)
        req_ram = min(req_ram, ram_cap, rem_ram)

        # If we can't at least allocate minimum slices, refuse.
        if req_cpu < s.min_cpu_slice or req_ram < s.min_ram_slice:
            return 0, 0
        return req_cpu, req_ram

    def _predicted_time(p, op):
        # SRPT-ish: lower is better. If unknown, treat as large.
        key = (p.pipeline_id, _op_key(op))
        t = s.op_time_ewma.get(key, None)
        if t is None:
            return 1e18
        try:
            return float(t)
        except Exception:
            return 1e18

    # ---------------- Ingest pipelines ----------------
    for p in pipelines:
        _maybe_enqueue_pipeline(p)

    # Early exit
    if not pipelines and not results:
        return [], []

    # ---------------- Learn from results ----------------
    for r in results:
        # Update EWMA for observed runtime if present
        # (Eudoxia may expose different fields; we probe common names safely.)
        duration = None
        for attr in ("duration", "runtime", "runtime_s", "elapsed", "elapsed_s", "time"):
            if hasattr(r, attr):
                duration = getattr(r, attr)
                break

        pid = getattr(r, "pipeline_id", None)
        ops = getattr(r, "ops", None) or []

        # Best-effort update predicted time for these ops
        if pid is not None and duration is not None:
            try:
                d = float(duration)
                if d >= 0:
                    for op in ops:
                        k = (pid, _op_key(op))
                        prev = s.op_time_ewma.get(k, None)
                        if prev is None:
                            s.op_time_ewma[k] = d
                        else:
                            s.op_time_ewma[k] = (1.0 - s.ewma_alpha) * float(prev) + s.ewma_alpha * d
            except Exception:
                pass

        # On failure: aggressively bump RAM hint (OOM-like) for retry.
        if hasattr(r, "failed") and r.failed() and pid is not None:
            err = str(getattr(r, "error", "") or "")
            err_l = err.lower()
            oom_like = ("oom" in err_l) or ("out of memory" in err_l) or ("memory" in err_l)

            # Use actual attempted resources as baseline when available.
            tried_ram = int(getattr(r, "ram", 0) or 0)
            tried_cpu = int(getattr(r, "cpu", 0) or 0)

            for op in ops:
                k = (pid, _op_key(op))
                prev_ram = int(s.op_ram_hint.get(k, tried_ram or 0) or 0)
                prev_cpu = int(s.op_cpu_hint.get(k, tried_cpu or 0) or 0)

                if oom_like:
                    # Double RAM (bounded later by pool max); keep CPU steady.
                    base = tried_ram if tried_ram > 0 else max(prev_ram, 1)
                    s.op_ram_hint[k] = max(prev_ram, base * 2)
                    if prev_cpu > 0:
                        s.op_cpu_hint[k] = prev_cpu
                    elif tried_cpu > 0:
                        s.op_cpu_hint[k] = tried_cpu
                else:
                    # Conservative bump for unknown failures.
                    base = tried_ram if tried_ram > 0 else max(prev_ram, 1)
                    s.op_ram_hint[k] = max(prev_ram, int(base * 1.25) + 1)
                    if tried_cpu > 0:
                        s.op_cpu_hint[k] = max(prev_cpu, int(tried_cpu * 1.10) + 0)

    # ---------------- Build candidate heaps per priority ----------------
    # We'll construct candidates on-demand per pool to respect pool-bias and changing availability.
    def _collect_candidates(allow_batch_on_pool0):
        """
        Returns a heap of ( -prio_rank, predicted_time, fifo_order, pipeline, op, effective_priority )
        """
        heap = []
        fifo_counter = 0

        # Scan queues in priority order; bounded to keep per-tick complexity stable.
        for q in (s.q_query, s.q_interactive, s.q_batch):
            scanned = 0
            q_len = len(q)
            while scanned < q_len and scanned < s.max_scan_per_queue:
                p = q.popleft()
                scanned += 1

                st = p.runtime_status()
                if st.is_pipeline_successful():
                    _drop_pipeline_bookkeeping(p.pipeline_id)
                    continue

                eff_prio = _effective_priority(p)
                if not allow_batch_on_pool0 and eff_prio == Priority.BATCH_PIPELINE:
                    # Keep it in queue; not a candidate for this pool.
                    q.append(p)
                    continue

                op = _get_runnable_op(p)
                if op is None:
                    q.append(p)
                    continue

                pr = _prio_rank(eff_prio)
                pt = _predicted_time(p, op)
                heapq.heappush(heap, (-pr, pt, fifo_counter, p, op, eff_prio))
                fifo_counter += 1

                # Keep pipeline in queue for future ops; we do not remove it permanently here.
                q.append(p)

            # If we didn't scan the full queue, we still keep order by having rotated only scanned elements.

        return heap

    suspensions = []  # no preemption in this iteration (lack of stable running-container introspection)
    assignments = []

    # ---------------- Assign per pool, packing multiple ops ----------------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        rem_cpu = int(pool.avail_cpu_pool)
        rem_ram = int(pool.avail_ram_pool)
        if rem_cpu <= 0 or rem_ram <= 0:
            continue

        allow_batch_on_pool0 = True
        if s.executor.num_pools > 1 and pool_id == 0:
            # Strong bias: do not consider batch on pool 0 unless nothing else can be assigned.
            allow_batch_on_pool0 = False

        # First pass: candidates without batch on pool0 (if biased).
        heap = _collect_candidates(allow_batch_on_pool0=allow_batch_on_pool0)

        # If pool0 and we found no candidates, allow batch as a fallback to avoid idling.
        if not heap and s.executor.num_pools > 1 and pool_id == 0:
            heap = _collect_candidates(allow_batch_on_pool0=True)

        # Try to fill this pool with multiple assignments.
        # We may encounter candidates that don't fit current remaining resources; we'll defer them.
        deferred = []
        made_progress = True

        while heap and rem_cpu > 0 and rem_ram > 0 and made_progress:
            made_progress = False

            # Try a few candidates to find one that fits, to avoid O(N^2) scanning.
            tries = min(20, len(heap))
            picked = None

            for _ in range(tries):
                item = heapq.heappop(heap)
                neg_pr, pt, fifo, p, op, eff_prio = item

                # Respect pool bias even if candidates were collected broadly (defensive).
                if not _pool_bias_allows(pool_id, eff_prio):
                    deferred.append(item)
                    continue

                req_cpu, req_ram = _size_for_op(p, op, pool_id, rem_cpu, rem_ram)
                if req_cpu <= 0 or req_ram <= 0:
                    # Doesn't fit right now; defer and try another.
                    deferred.append(item)
                    continue

                picked = (p, op, req_cpu, req_ram, item)
                break

            # Push back deferred candidates (and any untried remain in heap already).
            for d in deferred:
                heapq.heappush(heap, d)
            deferred.clear()

            if picked is None:
                # Nothing fits under current remaining resources.
                break

            p, op, req_cpu, req_ram, item = picked

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,  # keep original priority for accounting
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            rem_cpu -= int(req_cpu)
            rem_ram -= int(req_ram)
            made_progress = True

    return suspensions, assignments
