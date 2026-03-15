# policy_key: scheduler_low_003
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.047392
# generation_seconds: 55.18
# generated_at: 2026-03-12T21:36:17.655523
@register_scheduler_init(key="scheduler_low_003")
def scheduler_low_003_init(s):
    """Priority-aware FIFO with conservative right-sizing and OOM-driven RAM backoff.

    Incremental improvements over naive FIFO:
      1) Separate waiting queues by priority to avoid head-of-line blocking.
      2) Pack multiple runnable ops per tick (instead of at most one per pool).
      3) Reserve headroom for high-priority (QUERY/INTERACTIVE) so BATCH can't consume all capacity.
      4) If an op fails with an OOM-like error, retry with increased RAM (bounded exponential backoff).

    Notes:
      - Preemption is intentionally not used here (small improvement step), because the minimal API
        surface shown does not include enumerating running containers to pick victims safely.
      - We rely on FAILED being assignable (per ASSIGNABLE_STATES) so retries happen naturally.
    """
    # Separate per-priority queues; keep FIFO within each priority class.
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-(pipeline, op) RAM multiplier for OOM backoff.
    # Keyed by a stable string derived from pipeline_id and op identity.
    s.op_ram_mult = {}  # str -> float

    # Track last successful (cpu, ram) per op key to start future attempts closer to known-good.
    s.op_last_good = {}  # str -> (cpu: float, ram: float)

    # Simple knobs (kept conservative and easy to reason about).
    s.reserved_frac_for_hp = 0.30   # reserve this fraction of each pool for HP if any HP is waiting
    s.max_ram_mult = 8.0            # cap OOM backoff
    s.base_ram_frac = {             # baseline RAM request as a fraction of pool max
        Priority.QUERY: 0.35,
        Priority.INTERACTIVE: 0.25,
        Priority.BATCH_PIPELINE: 0.15,
    }
    s.base_cpu_frac = {             # baseline CPU request as a fraction of pool max
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }


@register_scheduler(key="scheduler_low_003")
def scheduler_low_003_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Enqueue new pipelines into per-priority FIFO queues.
      - Update RAM multipliers based on failed results (OOM/backoff).
      - For each pool, assign as many runnable ops as resources allow:
          * Prefer QUERY > INTERACTIVE > BATCH.
          * If any HP is waiting, limit BATCH to (1 - reserved_frac_for_hp) of pool capacity.
          * Right-size cpu/ram based on priority and prior successful allocations.
    """
    # ----------------------------
    # Helpers (defined inside to keep code self-contained)
    # ----------------------------
    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _op_key(pipeline, op):
        # Prefer stable identifiers if available; otherwise fall back to object id.
        if hasattr(op, "op_id"):
            oid = getattr(op, "op_id")
        elif hasattr(op, "operator_id"):
            oid = getattr(op, "operator_id")
        elif hasattr(op, "id"):
            oid = getattr(op, "id")
        else:
            oid = id(op)
        return f"{pipeline.pipeline_id}:{oid}"

    def _has_hp_waiting():
        return (len(s.wait_q[Priority.QUERY]) > 0) or (len(s.wait_q[Priority.INTERACTIVE]) > 0)

    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _queue_nonempty(prio):
        q = s.wait_q.get(prio, [])
        return len(q) > 0

    def _pop_next_pipeline(prio):
        q = s.wait_q[prio]
        if not q:
            return None
        return q.pop(0)

    def _push_pipeline(prio, p):
        s.wait_q[prio].append(p)

    def _drop_if_done_or_terminal_failure(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True

        # If there are FAILED ops, we *usually* still keep the pipeline because FAILED is assignable
        # and we want retries. However, some failures may be non-retriable; we only treat OOM as
        # explicitly retriable based on ExecutionResult feedback. Without feedback, keep it queued.
        return False

    def _get_runnable_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _choose_request(prio, pool, avail_cpu, avail_ram, last_good, ram_mult):
        # Start from last known-good if present; otherwise a baseline fraction of pool.
        if last_good is not None:
            base_cpu, base_ram = last_good
            cpu_req = base_cpu
            ram_req = base_ram
        else:
            cpu_req = max(1.0, pool.max_cpu_pool * s.base_cpu_frac.get(prio, 0.25))
            ram_req = max(1.0, pool.max_ram_pool * s.base_ram_frac.get(prio, 0.15))

        # Apply OOM backoff multiplier to RAM; keep CPU unchanged (simple, incremental step).
        ram_req = ram_req * ram_mult

        # Clamp to currently available.
        cpu_req = min(cpu_req, avail_cpu)
        ram_req = min(ram_req, avail_ram)

        # Avoid zero-sized assignments.
        if cpu_req <= 0 or ram_req <= 0:
            return None

        return cpu_req, ram_req

    # ----------------------------
    # Enqueue new pipelines
    # ----------------------------
    for p in pipelines:
        # Default unknown priorities into batch-like behavior (defensive).
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s.wait_q:
            pr = Priority.BATCH_PIPELINE
        _push_pipeline(pr, p)

    # ----------------------------
    # Update state from results (OOM backoff + last-good sizing)
    # ----------------------------
    if results:
        for r in results:
            # We only have op objects on the result; update last-good for those ops if succeeded.
            # If failed with OOM-like error: increase RAM multiplier for that (pipeline, op).
            # Note: r.ops may contain multiple ops; treat each individually.
            op_list = getattr(r, "ops", []) or []
            # We cannot rely on having the pipeline object here; key by container run context:
            # try to incorporate pipeline_id if present on ops; else fall back to op identity only.
            for op in op_list:
                # Best-effort pipeline id extraction (some simulators attach it).
                if hasattr(op, "pipeline_id"):
                    pid = getattr(op, "pipeline_id")
                else:
                    pid = "unknown"
                # Use op_id/operator_id when available.
                if hasattr(op, "op_id"):
                    oid = getattr(op, "op_id")
                elif hasattr(op, "operator_id"):
                    oid = getattr(op, "operator_id")
                elif hasattr(op, "id"):
                    oid = getattr(op, "id")
                else:
                    oid = id(op)
                k = f"{pid}:{oid}"

                if hasattr(r, "failed") and r.failed() and _is_oom_error(getattr(r, "error", None)):
                    cur = s.op_ram_mult.get(k, 1.0)
                    nxt = cur * 2.0
                    if nxt > s.max_ram_mult:
                        nxt = s.max_ram_mult
                    s.op_ram_mult[k] = nxt
                else:
                    # Record last known-good sizing if it looks like a success (or non-OOM failure).
                    # We only treat as "good" if not failed().
                    if hasattr(r, "failed") and (not r.failed()):
                        s.op_last_good[k] = (getattr(r, "cpu", None), getattr(r, "ram", None))
                        # If it succeeded, we can also relax RAM multiplier slowly toward 1.0.
                        cur = s.op_ram_mult.get(k, 1.0)
                        if cur > 1.0:
                            s.op_ram_mult[k] = max(1.0, cur / 2.0)

    # Early exit if nothing changed that could produce new decisions.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ----------------------------
    # Core scheduling loop: per pool, pack as many ops as possible
    # ----------------------------
    hp_waiting = _has_hp_waiting()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If HP is waiting, cap batch usage to leave headroom.
        # This is a soft reservation: HP can still use all resources when scheduled.
        batch_cpu_cap = avail_cpu
        batch_ram_cap = avail_ram
        if hp_waiting:
            batch_cpu_cap = max(0.0, pool.max_cpu_pool * (1.0 - s.reserved_frac_for_hp))
            batch_ram_cap = max(0.0, pool.max_ram_pool * (1.0 - s.reserved_frac_for_hp))

        # To avoid infinite loops, bound number of assignments per pool per tick.
        # This also preserves some "tick fairness" across pools.
        max_assignments_this_pool = 16
        made_progress = True

        while made_progress and max_assignments_this_pool > 0:
            made_progress = False

            for prio in _prio_order():
                if not _queue_nonempty(prio):
                    continue

                # If HP waiting, limit batch consumption by caps (but only when selecting batch).
                eff_avail_cpu = avail_cpu
                eff_avail_ram = avail_ram
                if prio == Priority.BATCH_PIPELINE and hp_waiting:
                    eff_avail_cpu = min(eff_avail_cpu, batch_cpu_cap)
                    eff_avail_ram = min(eff_avail_ram, batch_ram_cap)

                if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                    continue

                # Peek/pop pipelines until we find one with a runnable op (or drop finished).
                scanned = 0
                qlen = len(s.wait_q[prio])
                chosen_pipeline = None
                while scanned < qlen:
                    p = _pop_next_pipeline(prio)
                    scanned += 1
                    if p is None:
                        break
                    if _drop_if_done_or_terminal_failure(p):
                        continue
                    op = _get_runnable_op(p)
                    if op is None:
                        # Not runnable yet; keep it in the same FIFO to be reconsidered later.
                        _push_pipeline(prio, p)
                        continue
                    chosen_pipeline = p
                    chosen_op = op
                    break

                if chosen_pipeline is None:
                    continue

                # Determine per-op request with OOM backoff.
                ok = _op_key(chosen_pipeline, chosen_op)
                ram_mult = s.op_ram_mult.get(ok, 1.0)

                last_good = s.op_last_good.get(ok, None)
                # last_good may contain Nones if prior record was partial; sanitize.
                if last_good is not None:
                    lg_cpu, lg_ram = last_good
                    if lg_cpu is None or lg_ram is None:
                        last_good = None

                req = _choose_request(prio, pool, eff_avail_cpu, eff_avail_ram, last_good, ram_mult)
                if req is None:
                    # Can't fit; put pipeline back and move on.
                    _push_pipeline(prio, chosen_pipeline)
                    continue

                cpu_req, ram_req = req

                assignments.append(
                    Assignment(
                        ops=[chosen_op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=prio,
                        pool_id=pool_id,
                        pipeline_id=chosen_pipeline.pipeline_id,
                    )
                )

                # Update available pool resources for further packing in this tick.
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                max_assignments_this_pool -= 1
                made_progress = True

                # Put pipeline back so subsequent runnable ops can be scheduled later.
                _push_pipeline(prio, chosen_pipeline)

                # Break to re-check priority order with updated resources.
                break

            if avail_cpu <= 0 or avail_ram <= 0:
                break

    return suspensions, assignments
