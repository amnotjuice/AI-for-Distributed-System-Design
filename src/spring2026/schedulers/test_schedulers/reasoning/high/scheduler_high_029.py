# policy_key: scheduler_high_029
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.190061
# generation_seconds: 1577.58
# generated_at: 2026-04-10T02:35:06.538626
@register_scheduler_init(key="scheduler_high_029")
def scheduler_high_029_init(s):
    """
    Priority-protecting, completion-biased scheduler.

    Core ideas:
      1) Strict priority order (QUERY > INTERACTIVE > BATCH) with lightweight fairness (round-robin within each class).
      2) Conservative RAM sizing to avoid OOM (high penalty from failures/incompletes); learn per-operator RAM after failures.
      3) Soft headroom reservation so batch cannot fully saturate pools when higher-priority work exists/arrives.
      4) Retry FAILED operators a limited number of times with increased RAM; quarantine pathological pipelines to avoid
         wasting capacity and harming high-priority latency.
    """
    s.tick = 0

    # Per-priority FIFO queues (we'll rotate entries for round-robin fairness).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Pipeline metadata for aging/fairness and conservative behavior on retries.
    # {pipeline_id: {"arrival_tick": int}}
    s.pipeline_meta = {}

    # Map operator object id -> pipeline_id for attributing ExecutionResult back to pipelines.
    s.op_to_pipeline = {}

    # Learned per-operator RAM estimate (bytes/units as used by simulator).
    # Keys are (pipeline_id, op_obj_id) -> ram_est
    s.op_ram_est = {}

    # Retry counters per operator and per pipeline.
    s.op_retries = {}          # (pipeline_id, op_obj_id) -> int
    s.pipeline_failures = {}   # pipeline_id -> int

    # Quarantine pipelines that repeatedly fail (prevents wasting resources and harming QUERY/INTERACTIVE latency).
    s.quarantined = set()

    # Tuning knobs (keep simple and safe).
    s.max_oom_retries = 4
    s.max_other_retries = 2
    s.max_pipeline_failures = 8  # after this, quarantine

    # Soft reservation (fraction of pool capacity) to prevent batch from fully filling pools.
    s.reserve_cpu_frac = 0.15
    s.reserve_ram_frac = 0.15


@register_scheduler(key="scheduler_high_029")
def scheduler_high_029(s, results, pipelines):
    """
    See init() docstring for design overview.
    """
    s.tick += 1

    suspensions = []
    assignments = []

    def _safe_int(x, default=0):
        try:
            return int(x)
        except Exception:
            return default

    def _iter_pipeline_ops(p):
        # Best-effort extraction of operator objects from pipeline.values (structure may vary).
        v = getattr(p, "values", None)
        if v is None:
            return []
        try:
            # networkx-like
            nodes = getattr(v, "nodes", None)
            if callable(nodes):
                return list(nodes())
            if nodes is not None:
                return list(nodes)
        except Exception:
            pass
        if isinstance(v, dict):
            return list(v.values())
        if isinstance(v, (list, tuple, set)):
            return list(v)
        try:
            return list(v)
        except Exception:
            return []

    def _enqueue_pipeline(p):
        pid = getattr(p, "pipeline_id", None)
        if pid is None:
            return
        if pid not in s.pipeline_meta:
            s.pipeline_meta[pid] = {"arrival_tick": s.tick}
            s.pipeline_failures.setdefault(pid, 0)

            # Build op->pipeline index for attributing failures.
            for op in _iter_pipeline_ops(p):
                try:
                    s.op_to_pipeline[id(op)] = pid
                except Exception:
                    continue
        s.queues[p.priority].append(p)

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)

    def _pipeline_age_ticks(p):
        pid = getattr(p, "pipeline_id", None)
        meta = s.pipeline_meta.get(pid, None)
        if not meta:
            return 0
        return max(0, s.tick - meta.get("arrival_tick", s.tick))

    def _pipeline_ready_ops(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return []
        # Only consider ops whose parents are complete to avoid wasting compute.
        try:
            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        except Exception:
            return []
        return ops or []

    def _op_est_ram(pipeline_id, op, pool):
        # Try learned estimate first; otherwise fallback to conservative fraction of pool RAM.
        key = (pipeline_id, id(op))
        est = s.op_ram_est.get(key, None)
        if est is not None:
            return est

        # Best-effort: check common operator fields if present (may not exist).
        for attr in ("ram", "min_ram", "ram_min", "memory", "mem", "ram_req"):
            val = getattr(op, attr, None)
            if isinstance(val, (int, float)) and val > 0:
                return val

        # Default unknown RAM to a conservative baseline to reduce OOM risk.
        # We bias higher for QUERY/INTERACTIVE to avoid the heavy 720s penalty cascade.
        max_ram = getattr(pool, "max_ram_pool", 0) or 0
        return 0.60 * max_ram

    def _reserve_amounts(pool, high_pri_backlog, query_backlog):
        max_cpu = getattr(pool, "max_cpu_pool", 0) or 0
        max_ram = getattr(pool, "max_ram_pool", 0) or 0

        # Keep a small headroom always; keep more when high priority exists (to protect immediate scheduling).
        base_cpu = max(1, _safe_int(max_cpu * 0.05, 1)) if max_cpu else 0
        base_ram = max(1, _safe_int(max_ram * 0.05, 1)) if max_ram else 0

        if high_pri_backlog > 0:
            # Be more protective when we have active high-priority pressure.
            return (
                max(base_cpu, _safe_int(max_cpu * s.reserve_cpu_frac, base_cpu)),
                max(base_ram, _safe_int(max_ram * s.reserve_ram_frac, base_ram)),
            )

        if query_backlog > 0:
            # If only queries exist (should be covered by high_pri_backlog anyway), keep slightly more.
            return (
                max(base_cpu, _safe_int(max_cpu * 0.10, base_cpu)),
                max(base_ram, _safe_int(max_ram * 0.10, base_ram)),
            )

        return (base_cpu, base_ram)

    def _cpu_target(priority, pool, high_pri_backlog):
        max_cpu = getattr(pool, "max_cpu_pool", 0) or 0
        if max_cpu <= 0:
            return 0

        # Base caps: protect tail latency for QUERY/INTERACTIVE; allow batch concurrency.
        if priority == Priority.QUERY:
            base = min(max_cpu, 16)
            base = max(base, 2)
        elif priority == Priority.INTERACTIVE:
            base = min(max_cpu, 8)
            base = max(base, 1)
        else:
            base = min(max_cpu, 4)
            base = max(base, 1)

        # If there is a backlog of high priority work, bias toward concurrency (smaller per-op CPU)
        # while keeping QUERY reasonably large to minimize end-to-end latency.
        if high_pri_backlog > 1:
            divisor = min(4, high_pri_backlog)
            if priority == Priority.QUERY:
                base = max(2, base // divisor)
            elif priority == Priority.INTERACTIVE:
                base = max(1, base // divisor)

        return int(base)

    def _ram_target(priority, pool, remaining_ram, op_ram_est, is_retry):
        max_ram = getattr(pool, "max_ram_pool", 0) or 0
        if max_ram <= 0:
            return 0

        # Conservative RAM fractions; extra RAM has no perf cost but reduces OOM failures.
        if priority == Priority.QUERY:
            frac = 0.85
        elif priority == Priority.INTERACTIVE:
            frac = 0.75
        else:
            frac = 0.50

        # On retries (especially after OOM), be more aggressive.
        if is_retry:
            frac = min(0.95, frac + 0.15)

        baseline = frac * max_ram
        target = max(op_ram_est, baseline)

        # Clamp to remaining RAM.
        if remaining_ram is not None:
            target = min(target, remaining_ram)

        # Ensure at least 1 unit if any RAM exists.
        return max(1, target)

    def _pop_next_pipeline_with_ready_op(queue, skip_pids_set):
        """
        Round-robin scan: find a pipeline with at least one ready op.
        We rotate the chosen pipeline to the tail for fairness.
        """
        if not queue:
            return None, None

        n = len(queue)
        for _ in range(n):
            p = queue.pop(0)
            pid = getattr(p, "pipeline_id", None)

            # Drop completed pipelines.
            try:
                if p.runtime_status().is_pipeline_successful():
                    continue
            except Exception:
                pass

            if pid in s.quarantined:
                queue.append(p)
                continue
            if pid in skip_pids_set:
                queue.append(p)
                continue

            ready_ops = _pipeline_ready_ops(p)
            if not ready_ops:
                queue.append(p)
                continue

            # Choose smallest-estimated-RAM ready op to reduce fragmentation and avoid "can't fit" head-of-line blocks.
            pool_dummy = None  # not used here
            best_op = None
            best_est = None
            for op in ready_ops:
                try:
                    # We don't know pool here; use learned estimate if present, else leave for later sizing.
                    est = s.op_ram_est.get((pid, id(op)), None)
                    if est is None:
                        est = 0
                except Exception:
                    est = 0
                if best_op is None or est < best_est:
                    best_op, best_est = op, est

            queue.append(p)  # rotate
            return p, best_op

        return None, None

    # Enqueue arrivals.
    for p in pipelines:
        _enqueue_pipeline(p)

    # Process results to learn RAM needs and apply retry/quarantine logic.
    for r in results or []:
        if not getattr(r, "failed", lambda: False)():
            continue

        # Attribute failures to pipelines via operator identity, if possible.
        err = getattr(r, "error", None)
        oom = _is_oom_error(err)

        failed_ops = getattr(r, "ops", []) or []
        for op in failed_ops:
            pid = None
            try:
                pid = s.op_to_pipeline.get(id(op), None)
            except Exception:
                pid = None

            # If we can't identify the pipeline, we can still store a global-ish estimate keyed by (None, op_id)
            # but it won't be used; so only act when pid is known.
            if pid is None:
                continue

            s.pipeline_failures[pid] = s.pipeline_failures.get(pid, 0) + 1
            if s.pipeline_failures[pid] >= s.max_pipeline_failures:
                s.quarantined.add(pid)
                continue

            k = (pid, id(op))
            s.op_retries[k] = s.op_retries.get(k, 0) + 1

            # Increase RAM estimate when we see OOM; otherwise keep prior estimate.
            if oom:
                # Escalate based on last allocation reported in the result.
                last_ram = getattr(r, "ram", None)
                try:
                    last_ram = float(last_ram) if last_ram is not None else None
                except Exception:
                    last_ram = None

                prev = s.op_ram_est.get(k, 0)
                if last_ram is not None and last_ram > 0:
                    new_est = max(prev, last_ram * 1.8)
                else:
                    # No signal; still escalate modestly.
                    new_est = max(prev, prev * 1.5, 1)

                # Cap will be applied at assignment time by remaining/max pool RAM.
                s.op_ram_est[k] = new_est

            # Quarantine at operator-level retry caps (to avoid burning time and harming high-priority latency).
            retry_cap = s.max_oom_retries if oom else s.max_other_retries
            if s.op_retries[k] > retry_cap:
                s.quarantined.add(pid)

    # Early exit if nothing to do.
    has_waiting = any(len(q) > 0 for q in s.queues.values())
    if not has_waiting and not pipelines and not results:
        return suspensions, assignments

    # Compute global backlog signals (ready-to-run only).
    def _count_ready_in_queue(queue):
        c = 0
        for p in queue:
            pid = getattr(p, "pipeline_id", None)
            if pid in s.quarantined:
                continue
            try:
                if _pipeline_ready_ops(p):
                    c += 1
            except Exception:
                continue
        return c

    query_ready = _count_ready_in_queue(s.queues[Priority.QUERY])
    inter_ready = _count_ready_in_queue(s.queues[Priority.INTERACTIVE])
    high_pri_ready = query_ready + inter_ready

    # Scheduling loop per pool: fill with highest priority work first, keep soft headroom from batch.
    for pool_id in range(getattr(s.executor, "num_pools", 0) or 0):
        pool = s.executor.pools[pool_id]
        remaining_cpu = getattr(pool, "avail_cpu_pool", 0) or 0
        remaining_ram = getattr(pool, "avail_ram_pool", 0) or 0
        if remaining_cpu <= 0 or remaining_ram <= 0:
            continue

        reserve_cpu, reserve_ram = _reserve_amounts(pool, high_pri_ready, query_ready)

        # Avoid infinite scanning when tasks cannot fit in this pool's remaining resources.
        skip_pids = set()

        # Pack multiple assignments until we can't place anything else.
        while remaining_cpu > 0 and remaining_ram > 0:
            chosen_p = None
            chosen_op = None
            chosen_priority = None

            # Priority order: QUERY > INTERACTIVE > BATCH (batch may be aged, but we keep it simple).
            for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                # If high priority exists, be stricter about batch admission.
                if pr == Priority.BATCH_PIPELINE and high_pri_ready > 0:
                    # Require enough headroom to immediately admit a future high-priority op.
                    if remaining_cpu <= reserve_cpu or remaining_ram <= reserve_ram:
                        continue

                p, op = _pop_next_pipeline_with_ready_op(s.queues[pr], skip_pids)
                if p is None or op is None:
                    continue

                # Lightweight aging for batch so it doesn't starve forever:
                # If a batch pipeline is very old, allow it even under some high-priority pressure,
                # but still respect minimum headroom.
                if pr == Priority.BATCH_PIPELINE and high_pri_ready > 0:
                    age = _pipeline_age_ticks(p)
                    if age > 100 and remaining_cpu > reserve_cpu and remaining_ram > reserve_ram:
                        pass
                    else:
                        # Keep stricter gate for batch during high-priority backlog.
                        # Also avoid batch if we'd dip below reserve.
                        pass

                chosen_p, chosen_op, chosen_priority = p, op, pr
                break

            if chosen_p is None:
                break

            pid = chosen_p.pipeline_id
            status = chosen_p.runtime_status()
            is_retry_pipeline = False
            try:
                is_retry_pipeline = (status.state_counts.get(OperatorState.FAILED, 0) > 0)
            except Exception:
                is_retry_pipeline = False

            # If quarantined, don't schedule.
            if pid in s.quarantined:
                skip_pids.add(pid)
                continue

            # Compute resource request.
            cpu_req = _cpu_target(chosen_priority, pool, high_pri_ready)
            cpu_req = min(cpu_req, remaining_cpu)
            if chosen_priority == Priority.QUERY and cpu_req < 2 and remaining_cpu >= 2:
                cpu_req = 2

            op_est = _op_est_ram(pid, chosen_op, pool)
            ram_req = _ram_target(chosen_priority, pool, remaining_ram, op_est, is_retry_pipeline)

            # Enforce headroom for batch (and partially for interactive when queries exist).
            if chosen_priority == Priority.BATCH_PIPELINE:
                if (remaining_cpu - cpu_req) < reserve_cpu or (remaining_ram - ram_req) < reserve_ram:
                    skip_pids.add(pid)
                    continue
            if chosen_priority == Priority.INTERACTIVE and query_ready > 0:
                # Keep a smaller reserve when queries are waiting.
                q_res_cpu = max(1, _safe_int(getattr(pool, "max_cpu_pool", 0) * 0.10, 1))
                q_res_ram = max(1, _safe_int(getattr(pool, "max_ram_pool", 0) * 0.10, 1))
                if (remaining_cpu - cpu_req) < q_res_cpu or (remaining_ram - ram_req) < q_res_ram:
                    skip_pids.add(pid)
                    continue

            # Must fit.
            if cpu_req <= 0 or ram_req <= 0 or cpu_req > remaining_cpu or ram_req > remaining_ram:
                skip_pids.add(pid)
                continue

            # If this operator has exceeded retry caps, quarantine to avoid harming higher-priority latency.
            k = (pid, id(chosen_op))
            retries = s.op_retries.get(k, 0)
            # We don't know whether last failure was OOM; apply a conservative overall cap.
            if retries > (s.max_oom_retries + s.max_other_retries):
                s.quarantined.add(pid)
                skip_pids.add(pid)
                continue

            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen_p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            remaining_cpu -= cpu_req
            remaining_ram -= ram_req

            # Update backlog counts approximately to improve subsequent sizing decisions within the tick.
            # (We only decrement when we schedule a ready op of that class.)
            if chosen_priority == Priority.QUERY and query_ready > 0:
                query_ready -= 1
            if chosen_priority in (Priority.QUERY, Priority.INTERACTIVE) and high_pri_ready > 0:
                high_pri_ready -= 1

            # Reset skip set after a successful placement to re-consider previously skipped pipelines
            # (resources have changed; another pipeline may now fit).
            skip_pids.clear()

    return suspensions, assignments
