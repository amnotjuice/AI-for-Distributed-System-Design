# policy_key: scheduler_high_002
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.140266
# generation_seconds: 159.33
# generated_at: 2026-04-10T00:18:01.735617
@register_scheduler_init(key="scheduler_high_002")
def scheduler_high_002_init(s):
    """
    Priority-protecting, OOM-aware scheduler.

    Core ideas (small improvements over FIFO):
      - Separate queues per priority; prefer QUERY then INTERACTIVE then BATCH.
      - Light pool "roles" when multiple pools exist to reduce interference.
      - OOM-aware retries: on OOM failure, increase RAM for that (pipeline, op) and retry.
      - Avoid starvation: periodically inject a BATCH op even under high-priority load.
      - Conservative sizing for high priority (enough CPU/RAM to reduce latency and OOM risk),
        without giving the entire pool to a single op.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.in_queue = set()  # pipeline_id currently present in some queue
    s.known_pipelines = {}  # pipeline_id -> Pipeline
    s.pipeline_priority = {}  # pipeline_id -> Priority

    # Map op uid -> pipeline_id (best-effort) so we can attribute ExecutionResult failures.
    s.op_to_pipeline = {}  # op_uid -> pipeline_id

    # Per-(pipeline, op) retry/backoff state.
    s.op_retries = {}  # (pipeline_id, op_uid) -> int
    s.op_ram_scale = {}  # (pipeline_id, op_uid) -> float (multiplier applied to base RAM)
    s.op_last_request = {}  # (pipeline_id, op_uid) -> (cpu, ram)

    # If we cannot attribute an OOM to a specific op/pipeline, fall back to per-class boost.
    s.class_ram_boost = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # Burst controls to reduce starvation and reduce extreme query dominance.
    s.hp_streak = 0
    s.query_streak = 0
    s.max_query_burst = 4
    s.batch_inject_every = 12  # after this many high-priority assignments, force one batch if possible

    # Retry limits (avoid infinite churn if an op cannot fit even at max RAM).
    s.max_oom_retries = 4
    s.max_total_retries = 6
    s.give_up_pipelines = set()  # pipeline_ids we will stop scheduling (best-effort)

    # Determine pool "roles" for placement bias.
    # Roles:
    #   - "mixed": all priorities
    #   - "high": prefer QUERY/INTERACTIVE
    #   - "query": prefer QUERY
    #   - "interactive": prefer INTERACTIVE
    #   - "batch": prefer BATCH
    s.pool_roles = {}
    try:
        npools = s.executor.num_pools
    except Exception:
        npools = 1

    if npools <= 1:
        s.pool_roles[0] = "mixed"
    elif npools == 2:
        s.pool_roles[0] = "high"
        s.pool_roles[1] = "batch"
    else:
        s.pool_roles[0] = "query"
        s.pool_roles[1] = "interactive"
        for i in range(2, npools):
            s.pool_roles[i] = "batch"


@register_scheduler(key="scheduler_high_002")
def scheduler_high_002(s, results, pipelines):
    """
    Scheduling step:
      - Enqueue new pipelines by priority.
      - Process results to detect OOM and backoff RAM for that op (retry instead of dropping).
      - Assign ready ops with priority bias and mild anti-starvation injection.
    """
    def _op_uid(op):
        # Best-effort stable identifier across results/status.
        for attr in ("op_id", "operator_id", "id", "uid", "name"):
            try:
                v = getattr(op, attr)
                if callable(v):
                    v = v()
                if v is not None:
                    return v
            except Exception:
                pass
        return id(op)

    def _iter_pipeline_ops(p):
        # Best-effort traversal of pipeline.values (may be list/dict/networkx-like).
        vals = getattr(p, "values", None)
        if vals is None:
            return []
        try:
            if hasattr(vals, "values"):
                return list(vals.values())
            return list(vals)
        except Exception:
            return []

    def _is_oom_error(err):
        if not err:
            return False
        try:
            e = str(err).lower()
        except Exception:
            return False
        return ("oom" in e) or ("out of memory" in e) or ("memoryerror" in e)

    def _base_ram(pool, prio):
        # Moderate defaults (avoid too many OOMs; keep some concurrency).
        # RAM beyond minimum doesn't speed up; avoid allocating near-full-pool by default.
        max_ram = float(pool.max_ram_pool)
        if prio == Priority.QUERY:
            frac = 0.25
        elif prio == Priority.INTERACTIVE:
            frac = 0.20
        else:
            frac = 0.15
        base = max(1.0, max_ram * frac)
        # Don't exceed a soft cap to avoid single-op monopolization.
        return min(base, max_ram * 0.60)

    def _base_cpu(pool, prio, role):
        max_cpu = float(pool.max_cpu_pool)
        # Slightly higher CPU for high-priority to reduce latency; keep headroom for concurrency.
        if prio == Priority.QUERY:
            frac = 0.50
            soft_cap = 0.70
        elif prio == Priority.INTERACTIVE:
            frac = 0.40
            soft_cap = 0.65
        else:
            # If this is a batch-designated pool, allow batch to run a bit beefier to finish.
            if role == "batch":
                frac = 0.55
                soft_cap = 0.80
            else:
                frac = 0.30
                soft_cap = 0.60

        base = max(1.0, max_cpu * frac)
        return min(base, max_cpu * soft_cap)

    def _queue_nonempty(prio):
        q = s.queues.get(prio, [])
        return len(q) > 0

    def _hp_waiting():
        return _queue_nonempty(Priority.QUERY) or _queue_nonempty(Priority.INTERACTIVE)

    def _allowed_priorities_for_pool(role, hp_waiting):
        # Prefer to keep high priority isolated, but allow spillover to avoid idling.
        if role == "mixed":
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        if role == "high":
            if hp_waiting:
                return [Priority.QUERY, Priority.INTERACTIVE]
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        if role == "query":
            if _queue_nonempty(Priority.QUERY):
                return [Priority.QUERY]
            if _queue_nonempty(Priority.INTERACTIVE):
                return [Priority.INTERACTIVE]
            # Only run batch here if no high-priority waiting anywhere.
            if not hp_waiting:
                return [Priority.BATCH_PIPELINE]
            return []
        if role == "interactive":
            if _queue_nonempty(Priority.INTERACTIVE):
                return [Priority.INTERACTIVE]
            if _queue_nonempty(Priority.QUERY):
                return [Priority.QUERY]
            if not hp_waiting:
                return [Priority.BATCH_PIPELINE]
            return []
        # role == "batch"
        if _queue_nonempty(Priority.BATCH_PIPELINE):
            # If high priority is waiting, still let batch run in batch pool (to avoid 720s),
            # but batch pool is already isolated; keep it batch-first.
            return [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]
        # If batch empty, allow spillover to prevent idle.
        return [Priority.INTERACTIVE, Priority.QUERY]

    def _pick_priority(allowed):
        # Anti-starvation: inject one batch after a streak of high-priority assignments.
        if (Priority.BATCH_PIPELINE in allowed and _queue_nonempty(Priority.BATCH_PIPELINE)
                and s.hp_streak >= s.batch_inject_every):
            return Priority.BATCH_PIPELINE

        # Reduce long query bursts if interactive is waiting.
        if (Priority.QUERY in allowed and _queue_nonempty(Priority.QUERY)
                and _queue_nonempty(Priority.INTERACTIVE)
                and s.query_streak >= s.max_query_burst
                and Priority.INTERACTIVE in allowed):
            return Priority.INTERACTIVE

        # Default preference order.
        for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if pr in allowed and _queue_nonempty(pr):
                return pr
        return None

    def _pop_runnable_from_priority(prio):
        """
        Rotate through the priority queue to find a pipeline with at least one
        assignable op whose parents are complete.

        Returns: (pipeline, [op]) or (None, None)
        """
        q = s.queues.get(prio, [])
        if not q:
            return None, None

        # Bounded scan to avoid infinite loops.
        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            pid = p.pipeline_id

            # Skip pipelines we decided to give up on (best-effort, reduces interference).
            if pid in s.give_up_pipelines:
                s.in_queue.discard(pid)
                continue

            st = p.runtime_status()

            # Completed: remove permanently.
            if st.is_pipeline_successful():
                s.in_queue.discard(pid)
                continue

            # Find ready ops (including FAILED to allow retry).
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                op = ops[0]
                # Keep pipeline enqueued for future ops; rotate it to the back.
                q.append(p)
                return p, [op]

            # Not runnable right now; rotate to back.
            q.append(p)

        return None, None

    def _get_pipeline_id_from_result_op(op):
        uid = _op_uid(op)
        return s.op_to_pipeline.get(uid)

    # Enqueue new pipelines.
    for p in pipelines:
        pid = p.pipeline_id
        s.known_pipelines[pid] = p
        s.pipeline_priority[pid] = p.priority

        # Record op->pipeline mapping (best-effort).
        for op in _iter_pipeline_ops(p):
            try:
                s.op_to_pipeline[_op_uid(op)] = pid
            except Exception:
                pass

        if pid not in s.in_queue:
            s.queues[p.priority].append(p)
            s.in_queue.add(pid)

    # Process results (OOM-aware RAM backoff; retry instead of dropping).
    for r in results:
        if not r.failed():
            continue

        is_oom = _is_oom_error(getattr(r, "error", None))
        prio = getattr(r, "priority", None)

        # Attempt to attribute failure to (pipeline, op).
        attributed_any = False
        for op in getattr(r, "ops", []) or []:
            pid = _get_pipeline_id_from_result_op(op)
            if pid is None:
                continue
            opid = _op_uid(op)
            key = (pid, opid)

            retries = s.op_retries.get(key, 0) + 1
            s.op_retries[key] = retries

            if is_oom:
                # Exponential backoff on RAM for this op.
                scale = s.op_ram_scale.get(key, 1.0)
                scale = min(scale * 1.7, 16.0)
                s.op_ram_scale[key] = scale
            else:
                # Non-OOM failures: small bump (often correlated with under-provisioning too).
                scale = s.op_ram_scale.get(key, 1.0)
                scale = min(scale * 1.2, 8.0)
                s.op_ram_scale[key] = scale

            # If we've retried too many times, give up on this pipeline (avoid hogging).
            if retries >= s.max_total_retries or (is_oom and retries >= s.max_oom_retries):
                s.give_up_pipelines.add(pid)

            attributed_any = True

        # If we couldn't attribute to a pipeline/op, adjust per-class RAM boost cautiously.
        if (not attributed_any) and prio is not None:
            if is_oom:
                s.class_ram_boost[prio] = min(s.class_ram_boost.get(prio, 1.0) * 1.10, 4.0)
            else:
                s.class_ram_boost[prio] = min(s.class_ram_boost.get(prio, 1.0) * 1.05, 3.0)

    # Early exit if nothing changed that could affect decisions.
    if not pipelines and not results:
        return [], []

    suspensions = []  # Not used (preemption requires visibility into running containers).
    assignments = []

    hp_waiting = _hp_waiting()

    # Assign work per pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        role = s.pool_roles.get(pool_id, "mixed")
        allowed = _allowed_priorities_for_pool(role, hp_waiting)

        local_cpu = float(pool.avail_cpu_pool)
        local_ram = float(pool.avail_ram_pool)

        if local_cpu < 1.0 or local_ram < 1.0 or not allowed:
            continue

        # Limit assignments per pool per tick to reduce churn and keep decisions stable.
        max_assignments_this_pool = 4
        made = 0

        while made < max_assignments_this_pool and local_cpu >= 1.0 and local_ram >= 1.0:
            prio = _pick_priority(allowed)
            if prio is None:
                break

            p, op_list = _pop_runnable_from_priority(prio)
            if p is None:
                # Try any other allowed priority before giving up for this pool.
                found = False
                for alt in allowed:
                    if alt == prio:
                        continue
                    p, op_list = _pop_runnable_from_priority(alt)
                    if p is not None:
                        prio = alt
                        found = True
                        break
                if not found:
                    break

            op = op_list[0]
            pid = p.pipeline_id
            opid = _op_uid(op)
            key = (pid, opid)

            # Compute request sizes with OOM-aware backoff.
            cpu_req = _base_cpu(pool, prio, role)
            ram_req = _base_ram(pool, prio) * s.class_ram_boost.get(prio, 1.0) * s.op_ram_scale.get(key, 1.0)

            # If we've retried before, ensure monotonic RAM growth across attempts.
            last = s.op_last_request.get(key)
            if last is not None:
                _, last_ram = last
                ram_req = max(ram_req, float(last_ram))

            # Clamp to pool limits and current availability (avoid requesting more than we can place).
            cpu_req = max(1.0, min(cpu_req, float(pool.max_cpu_pool), local_cpu))
            ram_req = max(1.0, min(ram_req, float(pool.max_ram_pool), local_ram))

            # If we still cannot allocate a minimal container, stop.
            if cpu_req < 1.0 or ram_req < 1.0:
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            s.op_last_request[key] = (cpu_req, ram_req)

            # Update local availability and streak counters.
            local_cpu -= cpu_req
            local_ram -= ram_req
            made += 1

            if prio == Priority.QUERY:
                s.hp_streak += 1
                s.query_streak += 1
            elif prio == Priority.INTERACTIVE:
                s.hp_streak += 1
                s.query_streak = 0
            else:
                s.hp_streak = 0
                s.query_streak = 0

    return suspensions, assignments
