# policy_key: scheduler_est_010
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052164
# generation_seconds: 48.04
# generated_at: 2026-04-10T07:54:27.359840
@register_scheduler_init(key="scheduler_est_010")
def scheduler_est_010_init(s):
    """Priority-first, memory-aware, OOM-avoiding scheduler.

    Core ideas:
    - Strictly prefer higher priority (QUERY > INTERACTIVE > BATCH) to minimize weighted latency.
    - Use op.estimate.mem_peak_gb (noisy hint) to choose feasible pool placements and size RAM to avoid OOMs.
    - On failures, especially suspected OOMs, increase RAM "slack" for subsequent retries (per-pipeline backoff).
    - Avoid head-of-line blocking by scanning a small window in each priority queue and rotating pipelines.
    """
    from collections import deque

    # Per-priority waiting queues (pipelines can be present multiple ticks; we rotate for fairness).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Book-keeping
    s._known_pipelines = {}  # pipeline_id -> pipeline (latest object reference)
    s._retry_count = {}      # pipeline_id -> int
    s._oom_count = {}        # pipeline_id -> int (suspected OOMs)
    s._last_seen_prio = {}   # pipeline_id -> Priority


@register_scheduler(key="scheduler_est_010")
def scheduler_est_010_scheduler(s, results, pipelines):
    """
    Scheduler tick:
    1) Enqueue new pipelines into priority queues.
    2) Process results to update retry/OOM backoff signals.
    3) For each pool, greedily assign ready operators from highest-priority pipelines that fit in RAM.
       - Choose the smallest estimated-memory ready op among a small scan window to improve fit.
       - Allocate CPU fractionally by priority to reduce contention and preserve tail latency.
    """
    # ---- Helpers (defined inside to keep policy self-contained) ----
    def _prio_queue(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _prio_weight(priority):
        if priority == Priority.QUERY:
            return 10
        if priority == Priority.INTERACTIVE:
            return 5
        return 1

    def _cpu_target_frac(priority):
        # Higher priority gets more CPU to minimize latency.
        if priority == Priority.QUERY:
            return 0.75
        if priority == Priority.INTERACTIVE:
            return 0.55
        return 0.35

    def _looks_like_oom(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)

    def _pipeline_done_or_dead(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # We do NOT drop failed pipelines: failures are heavily penalized in the objective.
        return False

    def _get_ready_op(p):
        st = p.runtime_status()
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        # One operator at a time per pipeline to reduce interference and avoid cascading OOMs.
        return op_list[0]

    def _est_mem_gb(op):
        est = None
        try:
            if op is not None and getattr(op, "estimate", None) is not None:
                est = getattr(op.estimate, "mem_peak_gb", None)
        except Exception:
            est = None
        if est is None:
            return None
        try:
            est_f = float(est)
            if est_f < 0:
                return None
            return est_f
        except Exception:
            return None

    def _ram_request_gb(pool, p, op):
        # Base safety margin in GB to avoid tight fits.
        base_margin = 0.5

        pid = p.pipeline_id
        retries = s._retry_count.get(pid, 0)
        oomish = s._oom_count.get(pid, 0)

        # Slack increases with failures/oom-suspicions (multiplicative, capped).
        # This intentionally biases towards completion (avoid 720s penalty).
        slack = 1.15 + 0.35 * min(3, retries) + 0.35 * min(3, oomish)
        if slack > 3.0:
            slack = 3.0

        est = _est_mem_gb(op)
        if est is None:
            # Unknown mem: be conservative but not monopolizing.
            # Higher priority can take a larger fraction to reduce risk of OOM.
            if p.priority == Priority.QUERY:
                req = max(1.0, pool.max_ram_pool * 0.55)
            elif p.priority == Priority.INTERACTIVE:
                req = max(1.0, pool.max_ram_pool * 0.45)
            else:
                req = max(1.0, pool.max_ram_pool * 0.35)
            # Add some bump if we've already failed.
            req *= (1.0 + 0.15 * min(3, retries + oomish))
            # Never exceed pool max.
            return min(float(pool.max_ram_pool), float(req))

        # Estimated memory with slack + margin
        req = est * slack + base_margin
        # Keep within pool max
        req = min(float(pool.max_ram_pool), float(req))
        # Ensure some minimum allocation
        req = max(0.5, float(req))
        return req

    def _cpu_request(pool, p, avail_cpu):
        frac = _cpu_target_frac(p.priority)
        target = pool.max_cpu_pool * frac
        # Ensure at least 1 vCPU if anything is available.
        cpu = max(1.0, float(min(avail_cpu, target)))
        # Avoid allocating more than available.
        return float(min(cpu, avail_cpu))

    def _scan_choose_candidate(pool, q, max_scan, avail_cpu, avail_ram):
        # Pick the "best" feasible candidate within a bounded scan:
        # - Must have a ready op
        # - Must fit in available RAM (based on estimator/backoff)
        # - Prefer smaller estimated RAM request to improve packing and reduce OOM risk
        best_idx = None
        best_tuple = None  # (ram_req, -weight, retries)
        best = None  # (pipeline, op, ram_req)

        n = min(len(q), max_scan)
        for i in range(n):
            p = q[i]
            if _pipeline_done_or_dead(p):
                continue
            op = _get_ready_op(p)
            if op is None:
                continue

            ram_req = _ram_request_gb(pool, p, op)
            if ram_req > avail_ram:
                continue
            if avail_cpu <= 0:
                continue

            pid = p.pipeline_id
            retries = s._retry_count.get(pid, 0)
            weight = _prio_weight(p.priority)

            # Primary: smallest RAM requirement; Secondary: higher priority; Tertiary: higher retries first (finish to avoid 720s)
            cand_tuple = (float(ram_req), -int(weight), -int(retries))
            if best_tuple is None or cand_tuple < best_tuple:
                best_tuple = cand_tuple
                best_idx = i
                best = (p, op, ram_req)

        if best is None:
            return None

        # Rotate: move scanned elements ahead of best to the end, then pop best and append it (round-robin).
        # This avoids repeated scanning on the same head pipeline and reduces head-of-line blocking.
        # We only rotate within the scanned window to keep overhead small.
        for _ in range(best_idx):
            q.append(q.popleft())
        p = q.popleft()
        q.append(p)
        return best

    # ---- Enqueue new pipelines ----
    for p in pipelines:
        s._known_pipelines[p.pipeline_id] = p
        s._last_seen_prio[p.pipeline_id] = p.priority
        _prio_queue(p.priority).append(p)
        s._retry_count.setdefault(p.pipeline_id, 0)
        s._oom_count.setdefault(p.pipeline_id, 0)

    # ---- Process results: update backoff and refresh pipeline references if needed ----
    for r in results:
        # Update OOM/backoff signals when failures happen.
        if r is not None and r.failed():
            # Best-effort mapping: use pipeline_id from result if present; otherwise infer by ops owner (not available).
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                # Fall back: no pipeline_id in result API as given; we can only track "global" by priority,
                # but keep it simple: do nothing if we can't attribute.
                pass
            else:
                s._retry_count[pid] = s._retry_count.get(pid, 0) + 1
                if _looks_like_oom(getattr(r, "error", None)):
                    s._oom_count[pid] = s._oom_count.get(pid, 0) + 1

    # Early exit
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ---- Assignment loop: fill each pool greedily with feasible work ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Limit how many containers we launch per pool per tick to reduce churn.
        # (Keep small; most benefit comes from prioritization + mem-aware sizing.)
        max_launch = 4
        launched = 0

        while launched < max_launch and avail_cpu > 0 and avail_ram > 0:
            # Scan windows by priority: higher priority gets larger scan to reduce latency.
            cand = None

            # QUERY first
            cand = _scan_choose_candidate(pool, s.q_query, max_scan=24, avail_cpu=avail_cpu, avail_ram=avail_ram)
            # INTERACTIVE next
            if cand is None:
                cand = _scan_choose_candidate(pool, s.q_interactive, max_scan=16, avail_cpu=avail_cpu, avail_ram=avail_ram)
            # BATCH last
            if cand is None:
                cand = _scan_choose_candidate(pool, s.q_batch, max_scan=12, avail_cpu=avail_cpu, avail_ram=avail_ram)

            if cand is None:
                break

            p, op, ram_req = cand
            cpu_req = _cpu_request(pool, p, avail_cpu)

            # Final feasibility checks
            if ram_req > avail_ram or cpu_req <= 0:
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=float(cpu_req),
                    ram=float(ram_req),
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)
            launched += 1

    return suspensions, assignments
