# policy_key: scheduler_low_028
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049027
# generation_seconds: 56.64
# generated_at: 2026-03-14T02:24:30.017071
@register_scheduler_init(key="scheduler_low_028")
def scheduler_low_028_init(s):
    """
    Priority-aware FIFO with simple retry sizing.

    Improvements over naive FIFO:
      - Separate queues per priority; always schedule higher-priority work first.
      - Prevent batch from consuming the entire pool when higher-priority work is waiting
        (simple headroom reservation).
      - On failures that look like OOM, increase RAM next retry (and slightly reduce CPU),
        to reduce repeated fast-fail churn that harms latency.
      - Schedule multiple ops per pool per tick as resources allow, but keep bias toward
        high-priority latency by reserving headroom.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-pipeline adaptive sizing state (best-effort; we don't have per-op curves here).
    # Fields: ram_mult, cpu_mult, oom_retries
    s.pipe_state = {}
    # Aging counter to allow occasional batch progress even under sustained interactive load
    s.nonbatch_picks_since_batch = 0


def _prio_rank(priority):
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _looks_like_oom(err):
    if err is None:
        return False
    try:
        e = str(err).lower()
    except Exception:
        return False
    return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "alloc" in e)


def _ensure_pipe_state(s, pipeline):
    pid = pipeline.pipeline_id
    if pid not in s.pipe_state:
        s.pipe_state[pid] = {
            "ram_mult": 1.0,
            "cpu_mult": 1.0,
            "oom_retries": 0,
        }
    return s.pipe_state[pid]


def _pipeline_done_or_failed(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If any operator is FAILED, consider pipeline failed for now (no retries) unless the
    # framework expects FAILED to be reschedulable; ASSIGNABLE_STATES includes FAILED so we
    # still can retry an op. We keep pipeline if there are assignable ops remaining.
    return False


def _has_assignable_op(pipeline):
    status = pipeline.runtime_status()
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    return op_list[0:1]


def _cleanup_queues(s):
    # Remove pipelines that have completed; keep others (including those with FAILED ops,
    # since FAILED is assignable and may represent retryable failures in this simulator).
    for prio, q in s.queues.items():
        newq = []
        for p in q:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                # Done
                continue
            newq.append(p)
        s.queues[prio] = newq


def _pick_next_pipeline(s, allow_batch, want_batch_fairness):
    """
    Return the next pipeline to consider (or None).
    Policy:
      - Always try QUERY then INTERACTIVE.
      - If batch is allowed and either:
          * no higher-priority work exists, or
          * fairness trigger says we should let batch run occasionally,
        then allow BATCH.
    """
    # Always prefer high priority
    if s.queues[Priority.QUERY]:
        s.nonbatch_picks_since_batch += 1
        return s.queues[Priority.QUERY].pop(0)

    if s.queues[Priority.INTERACTIVE]:
        s.nonbatch_picks_since_batch += 1
        return s.queues[Priority.INTERACTIVE].pop(0)

    if allow_batch and s.queues[Priority.BATCH_PIPELINE]:
        # Fairness: reset counter when we pick batch
        s.nonbatch_picks_since_batch = 0
        return s.queues[Priority.BATCH_PIPELINE].pop(0)

    # If we didn't allow batch, but only batch exists, return None.
    # Caller may loosen allow_batch later in the tick.
    return None


def _compute_reservations(pool, have_high_waiting):
    """
    Reserve headroom for high-priority arrivals to protect tail latency.
    This is coarse: we reserve fractions of *max* pool capacity.
    """
    # If high-priority work exists, keep some headroom; otherwise reserve nothing.
    if not have_high_waiting:
        return 0.0, 0.0
    # Keep enough to start at least one interactive/query container promptly.
    reserve_cpu = 0.25 * pool.max_cpu_pool
    reserve_ram = 0.25 * pool.max_ram_pool
    return reserve_cpu, reserve_ram


def _base_fracs_for_priority(priority):
    # Bigger slices for higher priority to reduce latency (finish sooner, fewer context switches).
    if priority == Priority.QUERY:
        return 0.50, 0.40  # cpu_frac, ram_frac
    if priority == Priority.INTERACTIVE:
        return 0.35, 0.35
    return 0.20, 0.25


@register_scheduler(key="scheduler_low_028")
def scheduler_low_028(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    # Enqueue new pipelines into their priority queues.
    for p in pipelines:
        _ensure_pipe_state(s, p)
        s.queues[p.priority].append(p)

    # Update sizing hints from execution results (best-effort).
    for r in results:
        # We only react to failures; successes do not necessarily mean "optimal".
        if hasattr(r, "failed") and r.failed():
            # Try to locate pipeline state by matching pipeline_id via ops' pipeline_id if present;
            # if not available, fall back to priority-based no-op. Most simulator objects should
            # carry pipeline_id via operator membership, but we avoid hard assumptions.
            pid = None
            try:
                # Some implementations attach pipeline_id to result directly.
                pid = getattr(r, "pipeline_id", None)
            except Exception:
                pid = None

            # If not available, attempt to read from the first op (common pattern).
            if pid is None:
                try:
                    if r.ops and hasattr(r.ops[0], "pipeline_id"):
                        pid = r.ops[0].pipeline_id
                except Exception:
                    pid = None

            if pid is not None:
                st = s.pipe_state.get(pid)
                if st is None:
                    st = {"ram_mult": 1.0, "cpu_mult": 1.0, "oom_retries": 0}
                    s.pipe_state[pid] = st

                if _looks_like_oom(getattr(r, "error", None)):
                    st["oom_retries"] += 1
                    # Increase RAM aggressively at first, then taper; cap to avoid runaway.
                    # Also reduce CPU a bit to lower peak memory pressure from parallelism.
                    st["ram_mult"] = min(4.0, st["ram_mult"] * 1.6)
                    st["cpu_mult"] = max(0.5, st["cpu_mult"] * 0.9)
                else:
                    # Unknown failure: small backoff on CPU, tiny increase on RAM.
                    st["ram_mult"] = min(3.0, st["ram_mult"] * 1.15)
                    st["cpu_mult"] = max(0.6, st["cpu_mult"] * 0.95)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    _cleanup_queues(s)

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Determine whether any high-priority work is waiting globally (for reservation decisions).
    have_high_waiting_global = bool(s.queues[Priority.QUERY] or s.queues[Priority.INTERACTIVE])

    # Fairness trigger: if we picked non-batch many times in a row, allow batch even if high exists.
    want_batch_fairness = s.nonbatch_picks_since_batch >= 20

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        reserve_cpu, reserve_ram = _compute_reservations(pool, have_high_waiting_global)

        # Keep assigning while there are resources.
        # Admission rule for batch: only if headroom remains OR fairness trigger OR no high waiting.
        while avail_cpu > 0 and avail_ram > 0:
            have_high_waiting = bool(s.queues[Priority.QUERY] or s.queues[Priority.INTERACTIVE])
            allow_batch = (not have_high_waiting) or want_batch_fairness or (
                (avail_cpu - reserve_cpu) > 0 and (avail_ram - reserve_ram) > 0
            )

            p = _pick_next_pipeline(s, allow_batch=allow_batch, want_batch_fairness=want_batch_fairness)
            if p is None:
                break

            # Skip if already done.
            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable now; push to back of its queue.
                s.queues[p.priority].append(p)
                # Avoid tight loop when nothing is runnable.
                # If *all* queues are blocked, we'll exit naturally after cycling a bit,
                # but we add a small safeguard.
                if not (s.queues[Priority.QUERY] or s.queues[Priority.INTERACTIVE] or (allow_batch and s.queues[Priority.BATCH_PIPELINE])):
                    break
                continue

            pst = _ensure_pipe_state(s, p)
            cpu_frac, ram_frac = _base_fracs_for_priority(p.priority)

            # Compute desired slice from pool max, modified by adaptive multipliers.
            desired_cpu = pool.max_cpu_pool * cpu_frac * pst["cpu_mult"]
            desired_ram = pool.max_ram_pool * ram_frac * pst["ram_mult"]

            # Enforce minimums (avoid 0 allocations). CPU is in vCPUs; RAM in whatever units simulator uses.
            desired_cpu = max(1.0, desired_cpu)
            desired_ram = max(1.0, desired_ram)

            # Enforce reservations: batch should not dip into reserved headroom when high-priority exists.
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram
            if have_high_waiting and p.priority == Priority.BATCH_PIPELINE and not want_batch_fairness:
                eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                eff_avail_ram = max(0.0, avail_ram - reserve_ram)

            cpu = min(desired_cpu, eff_avail_cpu)
            ram = min(desired_ram, eff_avail_ram)

            # If we can't allocate meaningful resources, put pipeline back and stop filling this pool.
            if cpu < 1.0 or ram < 1.0:
                s.queues[p.priority].append(p)
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

            avail_cpu -= cpu
            avail_ram -= ram

            # Put pipeline back for remaining ops later (keeps FIFO-ish behavior within priority).
            s.queues[p.priority].append(p)

    return suspensions, assignments
