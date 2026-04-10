# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r19
@register_scheduler_init(key="scheduler_low_011_r19")
def scheduler_low_011_r19_init(s):
    """Priority-aware, latency-first scheduler (incremental improvements over the prior version).

    Key changes vs the previous attempt:
    - Fix OOM retry logic so pipelines with FAILED ops are not prematurely dropped if the failure is retryable.
    - De-duplicate pipeline queue entries (avoid repeated enqueues causing unfairness and overhead).
    - Allow multiple assignments per pool per tick (bounded), scheduling high-priority work first to cut queueing delay.
    - Keep headroom for high-priority work by reserving some CPU/RAM when high-priority backlog exists.
    - Choose among ready ops using a simple "smaller-first" heuristic based on learned RAM (SRPT-ish proxy).
    """
    # FIFO queues per priority (pipelines are de-duplicated with in_queue sets)
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.in_queue = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Track pipeline priority for quick lookup
    s.pipeline_pri = {}

    # Learned per-operator resource hints and outcomes
    # Keys are (pipeline_id, op_obj_id)
    s.op_hints = {}  # {"ram": float, "cpu": float}
    s.op_ram_success = {}  # minimal known-sufficient RAM from successful executions
    s.op_attempts = {}  # OOM-retry attempts
    s.op_last_fail_oom = {}  # whether last failure was OOM-like (retryable)

    # Map operator object identity -> pipeline_id (to recover pipeline_id from ExecutionResult)
    s.op_to_pipeline = {}

    # Control knobs (kept simple and conservative)
    s.max_retries_per_op = 4
    s.max_assignments_per_pool_per_tick = 4
    s.max_assignments_per_pipeline_per_tick = 2

    # Pool preference: treat pool 0 as the "latency" pool when multiple pools exist
    s.interactive_pool_id = 0

    # Reservation fractions when any high-priority backlog exists (to protect tail latency)
    s.reserve_fracs = {
        "default": {"cpu": 0.25, "ram": 0.25},
        "interactive_pool": {"cpu": 0.40, "ram": 0.40},
    }

    # Default RAM fractions (only used when we have no learned signal)
    s.default_ram_frac = {
        Priority.QUERY: 0.40,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.60,
    }

    # Default CPU caps for high priority to avoid monopolizing very large pools
    s.hp_cpu_cap = {
        Priority.QUERY: 16.0,
        Priority.INTERACTIVE: 12.0,
    }

    # Step counter (useful for future aging, not yet used heavily)
    s.step = 0


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_high_priority(pr):
    return pr in (Priority.QUERY, Priority.INTERACTIVE)


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_key(pipeline_id, op):
    return (pipeline_id, id(op))


def _safe_get_pool(s, pool_id):
    if pool_id is None:
        return None
    if not isinstance(pool_id, int):
        return None
    if pool_id < 0 or pool_id >= s.executor.num_pools:
        return None
    return s.executor.pools[pool_id]


def _pipeline_droppable(s, pipeline):
    """Drop pipeline only if it is successful or has a non-retryable failure."""
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True

    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    if not failed_ops:
        return False

    # If any failed op is not a known OOM-retryable failure (or exhausted retries), treat as fatal.
    for op in failed_ops:
        k = _op_key(pipeline.pipeline_id, op)
        if not s.op_last_fail_oom.get(k, False):
            return True
        if s.op_attempts.get(k, 0) > s.max_retries_per_op:
            return True

    # All current failures look like retryable OOM and within retry budget: keep pipeline alive.
    return False


def _choose_ready_op(s, pipeline):
    """Pick among assignable ops using smallest-estimated-RAM-first as a proxy for short tasks."""
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
    if not ops:
        return None

    pid = pipeline.pipeline_id

    def est_ram(op):
        k = _op_key(pid, op)
        # Prefer known-sufficient RAM from success; else a hint (e.g., inflated due to OOM); else unknown large.
        if k in s.op_ram_success:
            try:
                return float(s.op_ram_success[k])
            except Exception:
                return 1e18
        if k in s.op_hints and "ram" in s.op_hints[k]:
            try:
                return float(s.op_hints[k]["ram"])
            except Exception:
                return 1e18
        return 1e18

    best = ops[0]
    best_ram = est_ram(best)
    for op in ops[1:]:
        r = est_ram(op)
        if r < best_ram:
            best = op
            best_ram = r
    return best


def _maybe_enqueue_pipeline(s, pipeline):
    """Enqueue pipeline once (de-duplicated)."""
    pr = pipeline.priority if pipeline.priority in s.waiting_queues else Priority.BATCH_PIPELINE
    s.pipeline_pri[pipeline.pipeline_id] = pr
    if pipeline.pipeline_id not in s.in_queue[pr]:
        s.waiting_queues[pr].append(pipeline)
        s.in_queue[pr].add(pipeline.pipeline_id)


def _pop_next_runnable_pipeline(s, pr, scheduled_counts):
    """Round-robin scan for a pipeline with a ready op; returns (pipeline, op) or (None, None)."""
    q = s.waiting_queues[pr]
    if not q:
        return None, None

    n = len(q)
    for _ in range(n):
        p = q.pop(0)
        s.in_queue[pr].discard(p.pipeline_id)

        # Enforce per-tick cap per pipeline (keeps fairness; still allows some parallelism)
        if scheduled_counts.get(p.pipeline_id, 0) >= s.max_assignments_per_pipeline_per_tick:
            _maybe_enqueue_pipeline(s, p)
            continue

        # Drop only if truly done or fatal
        if _pipeline_droppable(s, p):
            continue

        op = _choose_ready_op(s, p)
        if op is None:
            # Not runnable yet; keep it for later
            _maybe_enqueue_pipeline(s, p)
            continue

        return p, op

    return None, None


def _compute_reserve(s, pool_id, pool, hp_backlog):
    if hp_backlog <= 0:
        return 0.0, 0.0

    if s.executor.num_pools <= 1:
        # Single pool: still reserve some headroom when HP exists
        fr = s.reserve_fracs["interactive_pool"]
    else:
        fr = s.reserve_fracs["interactive_pool"] if pool_id == s.interactive_pool_id else s.reserve_fracs["default"]

    return pool.max_cpu_pool * fr["cpu"], pool.max_ram_pool * fr["ram"]


def _desired_hp_cpu(s, pool, pr, hp_backlog, avail_cpu):
    # If only one HP op is waiting, "burst" to finish quickly.
    if hp_backlog <= 1:
        cpu = min(avail_cpu, pool.max_cpu_pool)
    else:
        # Split capacity as backlog grows to reduce queueing delay.
        if hp_backlog == 2:
            frac = 0.70
        elif hp_backlog <= 4:
            frac = 0.50
        else:
            frac = 0.33
        cpu = min(avail_cpu, pool.max_cpu_pool * frac)

    # Prevent monopolization on very large machines (still allows decent scale-up).
    cap = s.hp_cpu_cap.get(pr, 16.0)
    cpu = min(cpu, cap)
    return max(1.0, cpu)


def _desired_ram(s, pool, pr, pipeline_id, op, avail_ram):
    k = _op_key(pipeline_id, op)

    # Base request if we know nothing: a fraction of the pool max (conservative for HP, larger for batch).
    frac = s.default_ram_frac.get(pr, 0.50)
    req = max(1.0, min(avail_ram, pool.max_ram_pool * frac))

    # If we have a known successful RAM, we can safely reduce to that (improves packing and concurrency).
    if k in s.op_ram_success:
        try:
            succ = float(s.op_ram_success[k])
            req = max(1.0, min(req, succ))
        except Exception:
            pass

    # If we have an OOM-driven hint, respect it (avoid repeated OOMs).
    hint = s.op_hints.get(k)
    if hint and "ram" in hint:
        try:
            req = max(req, float(hint["ram"]))
        except Exception:
            pass

    # Cap by availability.
    return max(1.0, min(req, avail_ram))


@register_scheduler(key="scheduler_low_011_r19")
def scheduler_low_011_r19(s, results, pipelines):
    """
    Latency-first priority scheduler:
    - Updates OOM retry hints from execution results (and does NOT drop OOM-failed pipelines prematurely).
    - Schedules multiple containers per pool per tick, but always prioritizes QUERY/INTERACTIVE over BATCH.
    - Reserves headroom for high-priority work when high-priority backlog exists.
    - Uses a small-op-first heuristic among ready ops to cut queueing time and improve tail latency.
    """
    s.step += 1

    # Enqueue new arrivals (de-duplicated).
    for p in pipelines:
        _maybe_enqueue_pipeline(s, p)

    # Process results: learn RAM requirements and mark failures as retryable/non-retryable.
    for r in results:
        ops = getattr(r, "ops", None) or []
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        for op in ops:
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                pid = s.op_to_pipeline.get(id(op))
            if pid is None:
                continue

            k = _op_key(pid, op)

            if failed:
                if _is_oom_error(getattr(r, "error", None)):
                    s.op_last_fail_oom[k] = True
                    s.op_attempts[k] = int(s.op_attempts.get(k, 0)) + 1

                    # Increase RAM hint exponentially (bounded by pool max if known).
                    prev_hint = s.op_hints.get(k, {})
                    prev_ram = float(prev_hint.get("ram", 0.0) or 0.0)
                    obs_ram = float(getattr(r, "ram", 0.0) or 0.0)
                    baseline = prev_ram if prev_ram > 0 else (obs_ram if obs_ram > 0 else 1.0)
                    new_ram = max(1.0, baseline * 2.0)

                    pool = _safe_get_pool(s, getattr(r, "pool_id", None))
                    if pool is not None:
                        new_ram = min(new_ram, pool.max_ram_pool)

                    # Keep CPU hint as last used (or 1.0) but don't overcomplicate.
                    prev_cpu = float(prev_hint.get("cpu", getattr(r, "cpu", 1.0) or 1.0) or 1.0)
                    s.op_hints[k] = {"ram": new_ram, "cpu": max(1.0, prev_cpu)}
                else:
                    s.op_last_fail_oom[k] = False
            else:
                # Success: record minimal known-sufficient RAM.
                obs_ram = getattr(r, "ram", None)
                if obs_ram is not None:
                    try:
                        obs_ram_f = float(obs_ram)
                        if obs_ram_f > 0:
                            old = s.op_ram_success.get(k)
                            s.op_ram_success[k] = obs_ram_f if old is None else min(float(old), obs_ram_f)
                            # If we previously inflated a hint but later succeeded with less (rare), clamp it down.
                            if k in s.op_hints and "ram" in s.op_hints[k]:
                                try:
                                    s.op_hints[k]["ram"] = max(1.0, min(float(s.op_hints[k]["ram"]), s.op_ram_success[k]))
                                except Exception:
                                    pass
                    except Exception:
                        pass

    # If nothing changed, early exit.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Local view of pool availability (we decrement as we create assignments this tick).
    pool_avail = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pool_avail[pool_id] = [float(pool.avail_cpu_pool), float(pool.avail_ram_pool)]

    # Backlog snapshot to drive headroom decisions (we update as we schedule).
    hp_backlog = len(s.waiting_queues[Priority.QUERY]) + len(s.waiting_queues[Priority.INTERACTIVE])

    # Track per-tick per-pipeline assignment counts (fairness + avoid accidental over-parallelization).
    scheduled_counts = {}

    # Scheduling: iterate pools and place multiple assignments per pool per tick, always HP-first.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu, avail_ram = pool_avail[pool_id]
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        reserve_cpu, reserve_ram = _compute_reserve(s, pool_id, pool, hp_backlog)

        assigned_here = 0
        while assigned_here < s.max_assignments_per_pool_per_tick:
            avail_cpu, avail_ram = pool_avail[pool_id]
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Always try HP first.
            chosen_pr = None
            chosen_p = None
            chosen_op = None

            for pr in (Priority.QUERY, Priority.INTERACTIVE):
                p, op = _pop_next_runnable_pipeline(s, pr, scheduled_counts)
                if p is not None:
                    chosen_pr, chosen_p, chosen_op = pr, p, op
                    break

            if chosen_p is None:
                # No HP runnable; consider batch.
                # Protect the "interactive" pool if HP backlog exists (keep it mostly free).
                if hp_backlog > 0 and s.executor.num_pools > 1 and pool_id == s.interactive_pool_id:
                    break

                # Batch admission with headroom: only use capacity beyond reserves when HP backlog exists.
                eff_cpu = avail_cpu if hp_backlog <= 0 else max(0.0, avail_cpu - reserve_cpu)
                eff_ram = avail_ram if hp_backlog <= 0 else max(0.0, avail_ram - reserve_ram)
                if eff_cpu < 1.0 or eff_ram < 1.0:
                    break

                p, op = _pop_next_runnable_pipeline(s, Priority.BATCH_PIPELINE, scheduled_counts)
                if p is None:
                    break
                chosen_pr, chosen_p, chosen_op = Priority.BATCH_PIPELINE, p, op

            # Compute resource request.
            if _is_high_priority(chosen_pr):
                req_cpu = _desired_hp_cpu(s, pool, chosen_pr, hp_backlog, avail_cpu)
                req_ram = _desired_ram(s, pool, chosen_pr, chosen_p.pipeline_id, chosen_op, avail_ram)
                # Cap to pool availability.
                req_cpu = max(1.0, min(req_cpu, avail_cpu))
                req_ram = max(1.0, min(req_ram, avail_ram))
            else:
                # Batch: use headroom-constrained availability if HP backlog exists.
                eff_cpu = avail_cpu if hp_backlog <= 0 else max(0.0, avail_cpu - reserve_cpu)
                eff_ram = avail_ram if hp_backlog <= 0 else max(0.0, avail_ram - reserve_ram)
                if eff_cpu < 1.0 or eff_ram < 1.0:
                    # Can't place batch without violating reserve; re-enqueue and stop here.
                    _maybe_enqueue_pipeline(s, chosen_p)
                    break
                req_cpu = max(1.0, min(eff_cpu, pool.max_cpu_pool))
                req_ram = _desired_ram(s, pool, chosen_pr, chosen_p.pipeline_id, chosen_op, eff_ram)
                req_cpu = max(1.0, min(req_cpu, eff_cpu))
                req_ram = max(1.0, min(req_ram, eff_ram))

            # Place if it fits; otherwise re-enqueue and stop (avoid thrashing in a tight loop).
            if req_cpu > avail_cpu or req_ram > avail_ram:
                _maybe_enqueue_pipeline(s, chosen_p)
                break

            # Record mapping so results can be tied back to pipeline_id.
            s.op_to_pipeline[id(chosen_op)] = chosen_p.pipeline_id
            s.pipeline_pri[chosen_p.pipeline_id] = chosen_pr

            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=chosen_pr,
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            # Update local pool availability and per-pipeline counts.
            pool_avail[pool_id][0] = avail_cpu - req_cpu
            pool_avail[pool_id][1] = avail_ram - req_ram
            scheduled_counts[chosen_p.pipeline_id] = int(scheduled_counts.get(chosen_p.pipeline_id, 0)) + 1

            # Re-enqueue pipeline for future ops.
            _maybe_enqueue_pipeline(s, chosen_p)

            # Update HP backlog if we just scheduled HP work (roughly; enough for reserve decisions).
            if _is_high_priority(chosen_pr) and hp_backlog > 0:
                hp_backlog -= 1

            assigned_here += 1

    return suspensions, assignments