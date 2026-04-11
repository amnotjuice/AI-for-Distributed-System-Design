# policy_key: scheduler_low_035
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045021
# generation_seconds: 42.66
# generated_at: 2026-04-09T21:30:45.827069
@register_scheduler_init(key="scheduler_low_035")
def scheduler_low_035_init(s):
    """Priority + headroom + OOM-adaptive sizing (conservative, completion-oriented).

    Goals:
      - Minimize weighted latency by strongly favoring QUERY/INTERACTIVE work.
      - Avoid failures (720s penalty) by retrying OOM-like failures with increased RAM.
      - Avoid starving BATCH by allowing progress when high-priority queues are empty
        and by using mild aging.

    Key ideas:
      - Maintain per-priority FIFO queues plus simple aging for batch.
      - Use pool headroom reservations when high-priority backlog exists (so batch won't
        consume the entire pool and block queries).
      - Conservative initial RAM allocation by priority; on failure, increase RAM
        multiplicatively up to pool max; cap retries to avoid endless thrash.
    """
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # pipeline_id -> { op_key -> {"ram": float, "cpu": float, "retries": int} }
    s.op_hints = {}
    # pipeline_id -> number of failures observed (any cause)
    s.pipeline_failures = {}
    # pipeline_id -> last tick index seen (for simple aging); we don't have a global tick,
    # so we approximate with a counter we increment each scheduler call with any activity.
    s._tick = 0
    # pipeline_id -> enqueued tick (for aging/fairness)
    s.enqueued_at = {}


def _prio_rank(priority):
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _is_oom_error(err):
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda oom" in msg) or ("memoryerror" in msg)


def _op_key(op):
    # Best-effort stable key across retries; fall back to repr.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return str(v)
            except Exception:
                pass
    return repr(op)


def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


def _pick_next_pipeline(s):
    # Strict priority with mild aging for batch:
    # - Always pick QUERY if any.
    # - Then INTERACTIVE.
    # - For BATCH: FIFO, but if a batch has been waiting "long enough" relative to others,
    #   it will still be first (FIFO already) — aging is mainly used to allow batch to run
    #   even if interactive keeps trickling in (handled by headroom + caps below).
    if s.wait_q[Priority.QUERY]:
        return s.wait_q[Priority.QUERY].pop(0)
    if s.wait_q[Priority.INTERACTIVE]:
        return s.wait_q[Priority.INTERACTIVE].pop(0)
    if s.wait_q[Priority.BATCH_PIPELINE]:
        return s.wait_q[Priority.BATCH_PIPELINE].pop(0)
    return None


def _any_high_prio_waiting(s):
    return bool(s.wait_q[Priority.QUERY] or s.wait_q[Priority.INTERACTIVE])


def _default_request_fraction(priority):
    # Conservative RAM-first allocation (helps avoid OOM penalty).
    # CPU is capped to reduce interference and allow concurrent progress.
    if priority == Priority.QUERY:
        return {"ram_frac": 0.55, "cpu_frac": 0.75}
    if priority == Priority.INTERACTIVE:
        return {"ram_frac": 0.45, "cpu_frac": 0.60}
    return {"ram_frac": 0.30, "cpu_frac": 0.55}


def _compute_request(s, pool, pipeline, op, avail_cpu, avail_ram):
    pr = pipeline.priority
    opk = _op_key(op)

    if pipeline.pipeline_id not in s.op_hints:
        s.op_hints[pipeline.pipeline_id] = {}
    hints = s.op_hints[pipeline.pipeline_id].get(opk, None)

    # Defaults based on pool capacity.
    fr = _default_request_fraction(pr)
    # Minimum CPU: 1 vCPU if any available.
    min_cpu = 1.0
    # Minimum RAM: small floor to avoid pathological tiny allocations.
    min_ram = 0.5

    # Base request: fraction of pool max (not just currently available), then clamp to available.
    base_cpu = pool.max_cpu_pool * fr["cpu_frac"]
    base_ram = pool.max_ram_pool * fr["ram_frac"]

    # If we have hints (from previous attempts), start from them.
    if hints:
        base_cpu = max(base_cpu, hints.get("cpu", 0.0) or 0.0)
        base_ram = max(base_ram, hints.get("ram", 0.0) or 0.0)

    # Ensure we don't request more than available.
    req_cpu = _clamp(base_cpu, min_cpu, max(min_cpu, avail_cpu))
    req_ram = _clamp(base_ram, min_ram, max(min_ram, avail_ram))

    # If high-priority work is waiting, avoid giving huge allocations to batch.
    if _any_high_prio_waiting(s) and pr == Priority.BATCH_PIPELINE:
        req_cpu = min(req_cpu, max(min_cpu, pool.max_cpu_pool * 0.35))
        req_ram = min(req_ram, max(min_ram, pool.max_ram_pool * 0.25))
        req_cpu = _clamp(req_cpu, min_cpu, max(min_cpu, avail_cpu))
        req_ram = _clamp(req_ram, min_ram, max(min_ram, avail_ram))

    return req_cpu, req_ram


@register_scheduler(key="scheduler_low_035")
def scheduler_low_035_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue new pipelines by priority.
      2) Process execution results to update RAM/CPU hints and retry state.
      3) For each pool, make assignments while preserving headroom for high-priority.
    """
    # Advance pseudo-tick when we have any new information
    if pipelines or results:
        s._tick += 1

    # Enqueue newly arrived pipelines
    for p in pipelines:
        s.wait_q[p.priority].append(p)
        s.enqueued_at[p.pipeline_id] = s._tick
        if p.pipeline_id not in s.pipeline_failures:
            s.pipeline_failures[p.pipeline_id] = 0

    # Process results: update hints and retry state (focus on OOM -> increase RAM)
    for r in results:
        # Note: results can include multiple ops; we update each op key.
        try:
            pid = getattr(r, "pipeline_id", None)
        except Exception:
            pid = None

        # If pipeline_id isn't provided in results, we still can key by op + priority only;
        # but we prefer pipeline-specific hints. We'll skip updates if we cannot attribute.
        if pid is None:
            continue

        if pid not in s.op_hints:
            s.op_hints[pid] = {}
        if pid not in s.pipeline_failures:
            s.pipeline_failures[pid] = 0

        if r.failed():
            s.pipeline_failures[pid] += 1

        # Update per-op sizing hints
        for op in (r.ops or []):
            opk = _op_key(op)
            if opk not in s.op_hints[pid]:
                s.op_hints[pid][opk] = {"ram": 0.0, "cpu": 0.0, "retries": 0}

            h = s.op_hints[pid][opk]
            # Persist last attempted sizes as a floor.
            try:
                h["ram"] = max(h.get("ram", 0.0) or 0.0, float(getattr(r, "ram", 0.0) or 0.0))
            except Exception:
                pass
            try:
                h["cpu"] = max(h.get("cpu", 0.0) or 0.0, float(getattr(r, "cpu", 0.0) or 0.0))
            except Exception:
                pass

            if r.failed():
                h["retries"] = int(h.get("retries", 0) or 0) + 1
                # If likely OOM: increase RAM aggressively to avoid repeated 720s penalties.
                if _is_oom_error(getattr(r, "error", None)):
                    # multiplicative bump; larger after repeated OOMs
                    bump = 1.6 if h["retries"] <= 2 else 2.0
                    h["ram"] = max(h["ram"], (getattr(r, "ram", 0.0) or 0.0) * bump, 1.0)
                else:
                    # Non-OOM failures: modest RAM bump (could still be memory-related),
                    # but avoid runaway.
                    h["ram"] = max(h["ram"], (getattr(r, "ram", 0.0) or 0.0) * 1.15, 1.0)

    # We avoid preemption to reduce churn/wasted work; instead reserve headroom.
    suspensions = []
    assignments = []

    # Clean queues: drop completed pipelines; keep incomplete/failed for retries (up to cap).
    def _requeue_if_needed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return False
        # If it has failures, we still retry (to avoid 720s penalty) up to a cap.
        # Cap is per pipeline to avoid infinite thrash.
        pid = p.pipeline_id
        if s.pipeline_failures.get(pid, 0) >= 6:
            # Too many failures: stop spending resources; leaving it incomplete will still
            # be penalized in the objective, but endless retries can harm overall score.
            return False
        return True

    # Rebuild queues in-place, removing completed / exceeded-failure pipelines.
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        newq = []
        for p in s.wait_q[pr]:
            if _requeue_if_needed(p):
                newq.append(p)
        s.wait_q[pr] = newq

    # Schedule across pools
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Headroom reservation to protect high-priority latency:
        # If any high-priority work is waiting, we keep some capacity unallocated to batch.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if _any_high_prio_waiting(s):
            reserve_cpu = pool.max_cpu_pool * 0.20
            reserve_ram = pool.max_ram_pool * 0.20

        # Try to fill the pool with multiple assignments, but do not over-assign.
        # To reduce risk and improve interactivity, schedule at most a few ops per tick/pool.
        max_assignments_this_pool = 4
        made = 0

        while made < max_assignments_this_pool and avail_cpu > 0 and avail_ram > 0:
            p = _pick_next_pipeline(s)
            if p is None:
                break

            # Skip pipelines that are now complete / too-failed; do not requeue.
            if not _requeue_if_needed(p):
                continue

            st = p.runtime_status()
            # Schedule only parent-ready ops, 1 at a time to limit blast radius.
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready; requeue to its priority queue.
                s.wait_q[p.priority].append(p)
                continue

            op = op_list[0]

            # If we're about to schedule batch while high-priority is waiting, ensure headroom.
            if p.priority == Priority.BATCH_PIPELINE and _any_high_prio_waiting(s):
                if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                    # Put batch back and stop filling this pool to preserve headroom.
                    s.wait_q[p.priority].insert(0, p)
                    break

            req_cpu, req_ram = _compute_request(s, pool, p, op, avail_cpu, avail_ram)

            # If request cannot fit, try smaller CPU first (CPU scaling is sublinear; RAM is critical).
            if req_ram > avail_ram and req_ram > 0:
                # Can't fit; requeue and stop trying in this pool.
                s.wait_q[p.priority].insert(0, p)
                break
            if req_cpu > avail_cpu and req_cpu > 0:
                req_cpu = max(1.0, avail_cpu)

            # Final fit check
            if req_cpu <= 0 or req_ram <= 0 or req_cpu > avail_cpu or req_ram > avail_ram:
                s.wait_q[p.priority].insert(0, p)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            made += 1
            avail_cpu -= req_cpu
            avail_ram -= req_ram

            # Requeue pipeline if it still has work remaining.
            s.wait_q[p.priority].append(p)

    return suspensions, assignments
