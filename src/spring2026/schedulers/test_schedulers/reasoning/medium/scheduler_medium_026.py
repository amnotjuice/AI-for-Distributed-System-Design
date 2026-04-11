# policy_key: scheduler_medium_026
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.072979
# generation_seconds: 113.92
# generated_at: 2026-04-09T23:04:00.179752
@register_scheduler_init(key="scheduler_medium_026")
def scheduler_medium_026_init(s):
    """
    Priority-first, failure-averse scheduler.

    Core ideas:
      1) Strictly protect QUERY/INTERACTIVE latency while guaranteeing some BATCH progress.
      2) Avoid the 720s penalty by aggressively retrying failures with RAM backoff (OOM-aware).
      3) If multiple pools exist, bias one pool toward background work to reduce interference.
    """
    # Per-priority FIFO queues of pipelines
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Resource hints keyed by (pipeline_id, op_key) when available, with a fallback by op_key
    s.op_ram_hint = {}          # (pipeline_id, op_key) -> ram
    s.op_ram_hint_global = {}   # op_key -> ram

    # Failure tracking to decide RAM backoff / suppress non-OOM infinite retries
    s.op_fail_count = {}        # (pipeline_id, op_key) -> count
    s.op_nonoom_fail_count = {} # (pipeline_id, op_key) -> count

    # Pipelines that appear to be deterministically failing (non-OOM) get deprioritized/blocked
    s.dead_pipelines = set()    # pipeline_id

    # Ensure batch makes progress even under constant high-priority arrivals
    s.hp_since_batch = 0
    s.batch_every = 8  # schedule ~1 batch op per 8 high-priority ops (when batch exists)

    # For multi-pool cases, keep one pool biased toward batch when high-priority traffic exists
    s.batch_pool_id = None

    # Lightweight tick counter for any future aging logic
    s.step = 0


@register_scheduler(key="scheduler_medium_026")
def scheduler_medium_026(s, results: List[ExecutionResult],
                         pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    def _prio_rank(prio):
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2  # batch

    def _is_oom_error(err) -> bool:
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _get_pipeline_id_from_obj(obj):
        for attr in ("pipeline_id", "pipelineId", "pipeline"):
            if hasattr(obj, attr):
                try:
                    v = getattr(obj, attr)
                    # Some systems store pipeline object; try to extract id
                    if hasattr(v, "pipeline_id"):
                        return getattr(v, "pipeline_id")
                    return v
                except Exception:
                    pass
        return None

    def _get_pipeline_id_from_result(res):
        pid = _get_pipeline_id_from_obj(res)
        if pid is not None:
            return pid
        # Try from op metadata if present
        try:
            if getattr(res, "ops", None):
                pid = _get_pipeline_id_from_obj(res.ops[0])
                if pid is not None:
                    return pid
        except Exception:
            pass
        return None

    def _op_key(op):
        # Prefer stable identifiers if available
        for attr in ("operator_id", "op_id", "id", "name", "key"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return f"{attr}:{v}"
                except Exception:
                    pass
        # Fallback: string representation (often contains type and some identity)
        try:
            return f"repr:{repr(op)}"
        except Exception:
            return f"objid:{id(op)}"

    def _base_fracs(priority):
        # CPU: favor latency for query/interactive, keep batch smaller for concurrency.
        # RAM: moderately generous to reduce OOM loops (extra RAM doesn't slow execution).
        if priority == Priority.QUERY:
            return 0.70, 0.55  # cpu_frac, ram_frac
        if priority == Priority.INTERACTIVE:
            return 0.55, 0.50
        return 0.30, 0.40

    def _pick_next_priority(prefer_batch: bool) -> Priority:
        q_q = s.queues[Priority.QUERY]
        q_i = s.queues[Priority.INTERACTIVE]
        q_b = s.queues[Priority.BATCH_PIPELINE]

        # In a batch-biased pool, pull batch first when high-priority load exists.
        if prefer_batch and q_b:
            return Priority.BATCH_PIPELINE

        # Make sure batch progresses periodically when it exists.
        if q_b and (s.hp_since_batch >= s.batch_every) and (q_q or q_i):
            return Priority.BATCH_PIPELINE

        if q_q:
            return Priority.QUERY
        if q_i:
            return Priority.INTERACTIVE
        return Priority.BATCH_PIPELINE

    # Enqueue new arrivals
    for p in pipelines:
        s.queues[p.priority].append(p)

    # Fast path: if nothing changed, no new decisions
    if not pipelines and not results:
        return [], []

    s.step += 1
    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Initialize batch pool preference
    if s.batch_pool_id is None and s.executor.num_pools > 1:
        s.batch_pool_id = s.executor.num_pools - 1

    # Update hints from execution results (RAM backoff on failures; suppress repeated non-OOM)
    for res in results:
        try:
            if not res.failed():
                continue
        except Exception:
            # If failed() isn't reliable, fall back to error presence
            if getattr(res, "error", None) is None:
                continue

        pid = _get_pipeline_id_from_result(res)
        try:
            ops = getattr(res, "ops", None) or []
            op0 = ops[0] if ops else None
        except Exception:
            op0 = None

        ok = _op_key(op0) if op0 is not None else "op:unknown"
        pool_id = getattr(res, "pool_id", None)
        pool = s.executor.pools[pool_id] if (pool_id is not None and 0 <= pool_id < s.executor.num_pools) else None

        is_oom = _is_oom_error(getattr(res, "error", None))
        assigned_ram = float(getattr(res, "ram", 0.0) or 0.0)

        # Track failure counts
        if pid is not None:
            key = (pid, ok)
            s.op_fail_count[key] = s.op_fail_count.get(key, 0) + 1
            if not is_oom:
                s.op_nonoom_fail_count[key] = s.op_nonoom_fail_count.get(key, 0) + 1
        else:
            key = None

        # If it looks like a non-OOM deterministic failure, avoid burning cycles forever.
        if pid is not None and not is_oom:
            if s.op_nonoom_fail_count.get((pid, ok), 0) >= 2:
                s.dead_pipelines.add(pid)

        # RAM backoff on OOM (and also mildly on unknown failures)
        if pool is not None:
            max_ram = float(getattr(pool, "max_ram_pool", 0.0) or 0.0)
        else:
            max_ram = 0.0

        if max_ram > 0.0:
            if is_oom:
                # Strong backoff to escape OOM quickly (avoid repeated 720s outcomes)
                next_ram = min(max_ram, max(assigned_ram * 2.0, max_ram * 0.35))
            else:
                # Mild backoff even for unknown errors (sometimes misclassified OOM)
                next_ram = min(max_ram, max(assigned_ram * 1.25, max_ram * 0.25))

            if pid is not None:
                s.op_ram_hint[(pid, ok)] = max(s.op_ram_hint.get((pid, ok), 0.0), next_ram)
            s.op_ram_hint_global[ok] = max(s.op_ram_hint_global.get(ok, 0.0), next_ram)

    # Determine if there is any high-priority backlog (used for multi-pool biasing)
    hp_waiting = bool(s.queues[Priority.QUERY]) or bool(s.queues[Priority.INTERACTIVE])

    # Process pools (largest available CPU first to speed up latency-sensitive work)
    pool_order = list(range(s.executor.num_pools))
    try:
        pool_order.sort(key=lambda i: float(s.executor.pools[i].avail_cpu_pool), reverse=True)
    except Exception:
        pass

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        max_cpu = float(getattr(pool, "max_cpu_pool", avail_cpu) or avail_cpu)
        max_ram = float(getattr(pool, "max_ram_pool", avail_ram) or avail_ram)

        # In multi-pool setups, bias one pool toward batch while high-priority load exists
        prefer_batch = (s.batch_pool_id is not None) and (pool_id == s.batch_pool_id) and hp_waiting

        # Fill the pool with as many assignments as feasible this tick
        # We rotate through queues to find an assignable op (parents complete).
        spins = 0
        max_spins = 200  # bound work per tick across empty/not-ready pipelines
        while avail_cpu > 0 and avail_ram > 0 and spins < max_spins:
            spins += 1

            # If all queues empty, stop
            if not (s.queues[Priority.QUERY] or s.queues[Priority.INTERACTIVE] or s.queues[Priority.BATCH_PIPELINE]):
                break

            prio = _pick_next_priority(prefer_batch=prefer_batch)
            q = s.queues[prio]
            if not q:
                # If chosen queue empty, try next by strict order
                # (avoid getting stuck when prefer_batch but no batch exists)
                if s.queues[Priority.QUERY]:
                    prio = Priority.QUERY
                elif s.queues[Priority.INTERACTIVE]:
                    prio = Priority.INTERACTIVE
                else:
                    prio = Priority.BATCH_PIPELINE
                q = s.queues[prio]
                if not q:
                    break

            # Pop one pipeline from selected queue (FIFO)
            pipeline = q.pop(0)

            # Drop completed pipelines; keep failed/incomplete for retries (avoid 720 penalty)
            status = pipeline.runtime_status()
            if status.is_pipeline_successful():
                continue

            # If marked dead, do not keep rescheduling it (avoid thrashing other workloads)
            if pipeline.pipeline_id in s.dead_pipelines:
                # Do not requeue -> it will remain incomplete and incur the penalty,
                # but we avoid harming the weighted score of latency-sensitive work.
                continue

            # Find one assignable operator whose parents are complete
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready to run anything now; requeue and continue scanning
                s.queues[pipeline.priority].append(pipeline)
                continue

            op0 = op_list[0]
            ok = _op_key(op0)

            # Base sizing: prioritize completion (RAM) and latency (CPU) for query/interactive.
            cpu_frac, ram_frac = _base_fracs(pipeline.priority)
            base_cpu = max(1.0, max_cpu * cpu_frac)
            base_ram = max(0.0, max_ram * ram_frac)

            # If this pipeline has prior failures and no specific hint, modestly increase RAM
            try:
                has_failures = status.state_counts[OperatorState.FAILED] > 0
            except Exception:
                has_failures = False
            if has_failures:
                base_ram = min(max_ram, max(base_ram, max_ram * 0.50))

            # Apply any learned RAM hint (pipeline-specific preferred; fallback to global)
            hint_ram = s.op_ram_hint.get((pipeline.pipeline_id, ok), 0.0)
            hint_ram = max(hint_ram, s.op_ram_hint_global.get(ok, 0.0))

            # Choose final request within available resources
            req_cpu = min(avail_cpu, base_cpu)

            # For batch, if high-priority backlog exists, keep CPU smaller to avoid interfering
            if pipeline.priority == Priority.BATCH_PIPELINE and hp_waiting and not prefer_batch:
                req_cpu = min(req_cpu, max(1.0, max_cpu * 0.20))

            # RAM: prefer being generous to avoid OOM; cap by available RAM
            req_ram = min(avail_ram, max(base_ram, hint_ram))

            # If we can't allocate meaningful resources, requeue and stop filling this pool
            if req_cpu <= 0 or req_ram <= 0:
                s.queues[pipeline.priority].append(pipeline)
                break

            # Create assignment
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update local availability (greedy packing)
            avail_cpu -= req_cpu
            avail_ram -= req_ram

            # Update batch fairness counter
            if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                s.hp_since_batch += 1
            else:
                s.hp_since_batch = 0

            # Requeue pipeline so it can continue later (once this op completes)
            s.queues[pipeline.priority].append(pipeline)

    return suspensions, assignments
