# policy_key: scheduler_medium_044
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.077571
# generation_seconds: 107.32
# generated_at: 2026-04-09T23:29:33.935556
@register_scheduler_init(key="scheduler_medium_044")
def scheduler_medium_044_init(s):
    """Priority-aware, completion-oriented scheduler.

    Main ideas (small step up from FIFO):
      - Priority queues + weighted deficit round-robin to protect query/interactive latency
        while still guaranteeing some batch progress (anti-starvation).
      - Conservative, per-operator RAM estimation with OOM-driven backoff (increase RAM on OOM),
        aiming to reduce failure/incomplete pipelines (720s penalty).
      - Simple best-fit placement across pools using *shadow* available CPU/RAM accounting.
      - No preemption (keeps churn low; avoids relying on unexposed executor internals).
    """
    from collections import deque

    s.tick = 0

    # Active pipelines keyed by pipeline_id
    s.active = {}

    # Per-priority FIFO queues of pipeline_ids (round-robin within each class)
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.in_queue = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Weighted deficit round-robin state (bigger weight => more service)
    s.deficit = {
        Priority.QUERY: 0.0,
        Priority.INTERACTIVE: 0.0,
        Priority.BATCH_PIPELINE: 0.0,
    }
    s.weights = {
        Priority.QUERY: 10.0,
        Priority.INTERACTIVE: 5.0,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # RAM estimation per "operator signature" (learned from past runs and OOMs)
    s.ram_est = {}  # op_sig -> estimated_ram
    s.ram_max_mult = 0.95  # don't ever ask for 100% of pool RAM; keep tiny slack

    # Failure tracking per pipeline+op to avoid infinite retry loops
    s.pipe_op_fail = {}  # (pipeline_id, op_sig) -> count
    s.max_retries_per_op = 4

    # Batch anti-starvation: ensure at least one batch assignment periodically if backlogged
    s.last_batch_scheduled_tick = -10**9
    s.batch_starvation_ticks = 50

    # Base sizing knobs (fractions of pool capacity)
    s.base_ram_frac = {
        Priority.QUERY: 0.22,
        Priority.INTERACTIVE: 0.18,
        Priority.BATCH_PIPELINE: 0.14,
    }
    s.base_cpu_frac = {
        Priority.QUERY: 0.70,
        Priority.INTERACTIVE: 0.55,
        Priority.BATCH_PIPELINE: 0.40,
    }
    s.ram_cushion = {
        Priority.QUERY: 1.10,
        Priority.INTERACTIVE: 1.07,
        Priority.BATCH_PIPELINE: 1.05,
    }

    # If query backlog exists, be stricter about letting batch consume all pools
    s.query_backlog_cpu_reserve_frac = 0.15  # reserve this fraction of CPU per pool from batch


@register_scheduler(key="scheduler_medium_044")
def scheduler_medium_044(s, results, pipelines):
    """Scheduler step.

    Returns:
      suspensions: unused (no preemption)
      assignments: list of new container assignments
    """
    # ----------------------------
    # Helpers (local, no imports)
    # ----------------------------
    def _op_sig(op):
        # Try common identifiers; fall back to stable string repr.
        for attr in ("op_id", "operator_id", "id", "name"):
            v = getattr(op, attr, None)
            if v is not None:
                return f"{attr}:{v}"
        return f"repr:{repr(op)}"

    def _pipeline_id_from_result_or_op(r, op):
        pid = getattr(r, "pipeline_id", None)
        if pid is not None:
            return pid
        pid = getattr(op, "pipeline_id", None)
        if pid is not None:
            return pid
        return None

    def _is_oom(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _enqueue_pipeline(p):
        pr = p.priority
        pid = p.pipeline_id
        if pid not in s.in_queue[pr]:
            s.queues[pr].append(pid)
            s.in_queue[pr].add(pid)

    def _drop_from_queue_if_present(pr, pid):
        # We avoid O(n) deletions; use in_queue as membership guard and lazily skip on pop.
        s.in_queue[pr].discard(pid)

    def _cleanup_finished_and_abandoned():
        # Remove finished pipelines from active and queue-membership sets (lazy queue cleanup).
        to_remove = []
        for pid, p in s.active.items():
            st = p.runtime_status()
            if st.is_pipeline_successful():
                to_remove.append(pid)
        for pid in to_remove:
            pr = s.active[pid].priority
            _drop_from_queue_if_present(pr, pid)
            del s.active[pid]

    def _pool_shadow_state():
        avail_cpu = []
        avail_ram = []
        max_cpu = []
        max_ram = []
        for i in range(s.executor.num_pools):
            pool = s.executor.pools[i]
            avail_cpu.append(pool.avail_cpu_pool)
            avail_ram.append(pool.avail_ram_pool)
            max_cpu.append(pool.max_cpu_pool)
            max_ram.append(pool.max_ram_pool)
        return avail_cpu, avail_ram, max_cpu, max_ram

    def _reserve_cpu_for_query_if_needed(prio, i, avail_cpu_i, max_cpu_i, query_backlogged):
        # If queries are waiting, don't let batch saturate the last CPU.
        if (prio == Priority.BATCH_PIPELINE) and query_backlogged:
            reserve = max_cpu_i * s.query_backlog_cpu_reserve_frac
            return max(0.0, avail_cpu_i - reserve)
        return avail_cpu_i

    def _pick_priority(query_backlogged):
        # Force occasional batch to prevent indefinite starvation.
        if s.queues[Priority.BATCH_PIPELINE] and (s.tick - s.last_batch_scheduled_tick) >= s.batch_starvation_ticks:
            return Priority.BATCH_PIPELINE

        # Otherwise weighted deficit: choose the non-empty queue with maximum deficit.
        best_pr = None
        best_def = None
        for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if not s.queues[pr]:
                continue
            d = s.deficit[pr]
            # Small bias: if queries are backlogged, slightly favor them over interactive at equal deficit.
            if query_backlogged and pr == Priority.QUERY:
                d += 0.25
            if best_pr is None or d > best_def:
                best_pr, best_def = pr, d
        return best_pr

    def _choose_pool_for_op(prio, cpu_req, ram_req, avail_cpu, avail_ram, max_cpu, max_ram, query_backlogged):
        # Return pool_id or None.
        candidates = []
        for i in range(s.executor.num_pools):
            effective_avail_cpu = _reserve_cpu_for_query_if_needed(prio, i, avail_cpu[i], max_cpu[i], query_backlogged)
            if effective_avail_cpu >= cpu_req and avail_ram[i] >= ram_req:
                candidates.append(i)
        if not candidates:
            return None

        # Query: prefer most CPU headroom (lower latency).
        if prio == Priority.QUERY:
            best = None
            best_key = None
            for i in candidates:
                key = (avail_cpu[i], avail_ram[i])  # maximize CPU, then RAM
                if best is None or key > best_key:
                    best, best_key = i, key
            return best

        # Others: best-fit on RAM to reduce fragmentation; tie-break by CPU headroom.
        best = None
        best_key = None
        for i in candidates:
            key = (avail_ram[i], -avail_cpu[i])  # minimize RAM left, then maximize CPU left
            if best is None or key < best_key:
                best, best_key = i, key
        return best

    def _sizing(prio, pool_id, op_sig, avail_cpu, avail_ram, max_cpu, max_ram):
        # CPU sizing: fraction of pool max, capped by shadow availability.
        cpu_target = max(1.0, max_cpu[pool_id] * s.base_cpu_frac[prio])
        cpu_req = min(avail_cpu[pool_id], cpu_target)
        cpu_req = max(1.0, cpu_req) if avail_cpu[pool_id] >= 1.0 else avail_cpu[pool_id]

        # RAM sizing: learned estimate or conservative base fraction; cushion a bit.
        base = max(1.0, max_ram[pool_id] * s.base_ram_frac[prio])
        est = s.ram_est.get(op_sig, base)
        ram_req = est * s.ram_cushion[prio]

        # Cap to avoid asking for essentially all RAM; also cap by availability.
        ram_cap = max_ram[pool_id] * s.ram_max_mult
        ram_req = min(ram_req, ram_cap, avail_ram[pool_id])
        # Ensure positive request if any RAM available.
        ram_req = max(1.0, ram_req) if avail_ram[pool_id] >= 1.0 else avail_ram[pool_id]
        return cpu_req, ram_req

    # ----------------------------
    # Step start: time + arrivals
    # ----------------------------
    s.tick += 1

    for p in pipelines:
        # Avoid resurrecting completed pipelines; just track active arrivals.
        pid = p.pipeline_id
        s.active[pid] = p
        _enqueue_pipeline(p)

    # Update deficits every tick (even if no events) to ensure eventual service.
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        s.deficit[pr] += s.weights[pr]

    # ----------------------------
    # Process results: learn RAM + failure signals
    # ----------------------------
    if results:
        for r in results:
            failed = False
            try:
                failed = r.failed()
            except Exception:
                failed = False

            oom = _is_oom(getattr(r, "error", None))

            # Update RAM estimates based on outcomes:
            # - On OOM: bump aggressively.
            # - On other failure: bump modestly (may still be underprovisioning).
            # - On success: keep estimate no higher than what succeeded (light tightening).
            for op in getattr(r, "ops", []) or []:
                sig = _op_sig(op)
                prev = s.ram_est.get(sig, None)

                if failed:
                    # If we can attribute to a pipeline, track per-pipeline retry counter.
                    pid = _pipeline_id_from_result_or_op(r, op)
                    if pid is not None:
                        k = (pid, sig)
                        s.pipe_op_fail[k] = s.pipe_op_fail.get(k, 0) + 1

                    if oom:
                        # If we know the ram used, grow from that; otherwise grow from previous estimate.
                        used = getattr(r, "ram", None)
                        if used is not None:
                            new_est = max(float(used) * 1.35, (prev or 0.0) * 1.60, (prev or 1.0) + 1.0)
                        else:
                            new_est = max((prev or 1.0) * 1.60, (prev or 1.0) + 1.0)
                        s.ram_est[sig] = new_est
                    else:
                        used = getattr(r, "ram", None)
                        if used is not None:
                            new_est = max(float(used) * 1.15, (prev or 0.0))
                        else:
                            new_est = max((prev or 1.0) * 1.15, (prev or 1.0) + 0.5)
                        s.ram_est[sig] = new_est
                else:
                    used = getattr(r, "ram", None)
                    if used is not None:
                        used = float(used)
                        if prev is None:
                            s.ram_est[sig] = used
                        else:
                            # Tighten slowly: keep some conservatism to avoid oscillation/OOM.
                            s.ram_est[sig] = min(prev, used * 1.05)

    # Clean out completed pipelines so we don't keep re-queueing them.
    _cleanup_finished_and_abandoned()

    # ----------------------------
    # Build assignments
    # ----------------------------
    suspensions = []
    assignments = []

    avail_cpu, avail_ram, max_cpu, max_ram = _pool_shadow_state()
    query_backlogged = bool(s.queues[Priority.QUERY])

    # Upper bound on how many assignments we attempt in one tick to avoid pathological loops.
    # (Still allows filling multiple pools.)
    max_attempts = 4 * max(1, s.executor.num_pools) + 20

    stalled_rounds = 0
    attempts = 0

    while attempts < max_attempts:
        attempts += 1

        pr = _pick_priority(query_backlogged)
        if pr is None:
            break  # no work in queues

        # Pop next pipeline_id; lazily skip if it fell out of active or queue-membership.
        pid = None
        while s.queues[pr]:
            cand = s.queues[pr].popleft()
            if cand in s.in_queue[pr]:
                # Mark as not-in-queue; if still active we may re-enqueue later.
                s.in_queue[pr].discard(cand)
                if cand in s.active:
                    pid = cand
                    break
        if pid is None:
            # Queue had only stale entries.
            stalled_rounds += 1
            if stalled_rounds > 3:
                break
            continue

        p = s.active.get(pid)
        if p is None:
            continue

        st = p.runtime_status()
        if st.is_pipeline_successful():
            # Completed since enqueue; remove.
            del s.active[pid]
            continue

        # Choose a single ready operator (keeps OOM blast radius small).
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Nothing ready; re-enqueue to check later.
            _enqueue_pipeline(p)
            continue

        op = op_list[0]
        sig = _op_sig(op)

        # If this pipeline-op has failed too many times, give up to avoid infinite churn.
        # (Pipeline will become incomplete -> 720s penalty, but endless retries would also harm
        # query/interactive latency and still not complete.)
        k = (pid, sig)
        if s.pipe_op_fail.get(k, 0) > s.max_retries_per_op:
            # Abandon: stop scheduling it (lazy removal from queues).
            _drop_from_queue_if_present(pr, pid)
            del s.active[pid]
            continue

        # Find a pool where it fits given shadow availability.
        # We size against a chosen pool (RAM base fraction depends on pool max).
        # First, try each pool to compute a feasible (cpu,ram) and pick a best pool.
        best_pool = None
        best_cpu = None
        best_ram = None

        # Two-pass: for each pool compute request and see if it fits; then use selection policy.
        feasible = []
        for i in range(s.executor.num_pools):
            if avail_cpu[i] <= 0 or avail_ram[i] <= 0:
                continue
            cpu_req_i, ram_req_i = _sizing(pr, i, sig, avail_cpu, avail_ram, max_cpu, max_ram)

            # Apply query-protection reserve for batch on that pool.
            effective_avail_cpu = _reserve_cpu_for_query_if_needed(pr, i, avail_cpu[i], max_cpu[i], query_backlogged)
            if effective_avail_cpu >= cpu_req_i and avail_ram[i] >= ram_req_i and cpu_req_i > 0 and ram_req_i > 0:
                feasible.append((i, cpu_req_i, ram_req_i))

        if feasible:
            # Choose best pool among feasible.
            # Query: prefer max CPU headroom; Others: best-fit RAM.
            if pr == Priority.QUERY:
                best = None
                best_key = None
                for i, cpu_req_i, ram_req_i in feasible:
                    key = (avail_cpu[i], avail_ram[i])
                    if best is None or key > best_key:
                        best, best_key = (i, cpu_req_i, ram_req_i), key
                best_pool, best_cpu, best_ram = best
            else:
                best = None
                best_key = None
                for i, cpu_req_i, ram_req_i in feasible:
                    key = (avail_ram[i] - ram_req_i, -avail_cpu[i])  # minimize leftover RAM
                    if best is None or key < best_key:
                        best, best_key = (i, cpu_req_i, ram_req_i), key
                best_pool, best_cpu, best_ram = best

        if best_pool is None:
            # Can't place now; re-enqueue and try other work. Detect stalling.
            _enqueue_pipeline(p)
            stalled_rounds += 1
            if stalled_rounds >= 6:
                break
            continue

        # Place assignment and update shadow resources.
        stalled_rounds = 0
        assignments.append(
            Assignment(
                ops=[op],
                cpu=best_cpu,
                ram=best_ram,
                priority=p.priority,
                pool_id=best_pool,
                pipeline_id=p.pipeline_id,
            )
        )

        # Deficit accounting: charge 1 "unit" per assignment.
        s.deficit[pr] -= 1.0

        # Shadow accounting
        avail_cpu[best_pool] = max(0.0, avail_cpu[best_pool] - best_cpu)
        avail_ram[best_pool] = max(0.0, avail_ram[best_pool] - best_ram)

        if pr == Priority.BATCH_PIPELINE:
            s.last_batch_scheduled_tick = s.tick

        # Re-enqueue pipeline for its next operator (or to retry if still blocked later).
        _enqueue_pipeline(p)

    return suspensions, assignments
