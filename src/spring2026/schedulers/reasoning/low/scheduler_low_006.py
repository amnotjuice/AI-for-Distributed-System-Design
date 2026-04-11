# policy_key: scheduler_low_006
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050189
# generation_seconds: 42.90
# generated_at: 2026-03-12T21:38:46.749728
@register_scheduler_init(key="scheduler_low_006")
def scheduler_low_006_init(s):
    """Priority-aware FIFO with small, incremental improvements over naive FIFO.

    Main ideas:
    - Maintain a single waiting queue but pick work by priority (QUERY > INTERACTIVE > BATCH).
    - Avoid allocating *all* resources to a single op; cap CPU to improve concurrency while
      still giving higher-priority work a larger cap (latency focus).
    - Simple adaptive RAM retry on OOM: if an op fails with an OOM-like error, requeue and
      increase its RAM request next time (up to pool max).
    - Do not keep retrying non-OOM failures: treat them as terminal and drop the pipeline.
    """
    s.waiting_queue = []  # list[Pipeline]
    s.tick = 0

    # Per pipeline bookkeeping
    s.enqueue_tick = {}  # pipeline_id -> tick enqueued (for simple aging)
    s.pipeline_terminal_failure = set()  # pipeline_id that should be dropped

    # Per (pipeline, op) resource hints (learned from failures)
    s.ram_hint = {}  # (pipeline_id, op_key) -> ram
    s.cpu_hint = {}  # (pipeline_id, op_key) -> cpu (optional; conservative)

    # Last seen failure info per pipeline (for deciding retry/drop)
    s.last_failure_is_oom = {}  # pipeline_id -> bool


@register_scheduler(key="scheduler_low_006")
def scheduler_low_006_scheduler(s, results, pipelines):
    """
    Scheduler step:
    - Incorporate new pipelines into a waiting queue.
    - Process execution results to learn OOM hints and decide whether to retry or drop.
    - For each pool, repeatedly assign runnable ops from the highest-urgency pipelines first.
    """
    def _prio_rank(p):
        # Higher number => higher priority in our sorting
        if p == Priority.QUERY:
            return 3
        if p == Priority.INTERACTIVE:
            return 2
        return 1  # Priority.BATCH_PIPELINE (and anything else)

    def _is_oom_error(err):
        if not err:
            return False
        msg = str(err).lower()
        # Keep this permissive; simulator error strings may vary.
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)

    def _op_key(op):
        # Try stable identifiers if present; otherwise fall back to object id.
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "name", None) or id(op)

    def _pipeline_alive(p):
        if p.pipeline_id in s.pipeline_terminal_failure:
            return False
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return False
        return True

    def _pick_next_pipeline(waiting):
        # Priority-first with simple aging:
        # effective_score = prio_rank*1_000_000 + age
        # Age is ticks since enqueue; prevents total starvation.
        best_i = None
        best_score = None
        for i, p in enumerate(waiting):
            if not _pipeline_alive(p):
                continue
            st = p.runtime_status()
            # If it has failures and last failure isn't OOM, we will drop it.
            if st.state_counts.get(OperatorState.FAILED, 0) > 0 and not s.last_failure_is_oom.get(p.pipeline_id, False):
                s.pipeline_terminal_failure.add(p.pipeline_id)
                continue

            age = s.tick - s.enqueue_tick.get(p.pipeline_id, s.tick)
            score = _prio_rank(p.priority) * 1_000_000 + age
            if best_score is None or score > best_score:
                best_score = score
                best_i = i
        if best_i is None:
            return None
        return waiting.pop(best_i)

    def _cpu_cap_for(priority, pool_max_cpu):
        # Favor latency for high-priority: allow bigger single-op CPU,
        # but still avoid consuming the whole pool to improve overall responsiveness.
        if priority == Priority.QUERY:
            frac = 0.75
            min_cap = 1
        elif priority == Priority.INTERACTIVE:
            frac = 0.60
            min_cap = 1
        else:  # batch
            frac = 0.40
            min_cap = 1
        cap = int(max(min_cap, pool_max_cpu * frac))
        return max(1, cap)

    def _ram_cap_for(priority, pool_max_ram):
        # RAM capping is riskier (can cause OOM). Keep it high, but don't always give all RAM.
        # If pool is large, this still leaves headroom for concurrency.
        if priority == Priority.QUERY:
            frac = 0.85
        elif priority == Priority.INTERACTIVE:
            frac = 0.80
        else:
            frac = 0.75
        cap = max(1, int(pool_max_ram * frac))
        return cap

    # --- advance clock ---
    s.tick += 1

    # --- ingest new pipelines ---
    for p in pipelines:
        if p.pipeline_id not in s.enqueue_tick:
            s.enqueue_tick[p.pipeline_id] = s.tick
        s.waiting_queue.append(p)

    # --- process results: learn OOM and decide retry/drop ---
    for r in results:
        # Update last_failure_is_oom: if multiple failures, OOM should win to allow retry.
        if r.failed():
            is_oom = _is_oom_error(getattr(r, "error", None))
            prev = s.last_failure_is_oom.get(r.pipeline_id, False)
            s.last_failure_is_oom[r.pipeline_id] = prev or is_oom

            if is_oom:
                # Learn RAM hint for each op that participated in this container.
                for op in getattr(r, "ops", []) or []:
                    k = (r.pipeline_id, _op_key(op))
                    # Exponential backoff on RAM, bounded by pool max.
                    pool = s.executor.pools[r.pool_id]
                    new_hint = int(min(pool.max_ram_pool, max(r.ram * 2, r.ram + 1)))
                    old_hint = s.ram_hint.get(k, 0)
                    s.ram_hint[k] = max(old_hint, new_hint)
            else:
                # Non-OOM failures are treated as terminal for the pipeline.
                s.pipeline_terminal_failure.add(r.pipeline_id)

    # early exit if no state changes that affect our decisions
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # --- scheduling loop ---
    # We avoid preemption here (no reliable API shown to enumerate running containers).
    # Instead, we prioritize admissions/assignments so high-priority work starts ASAP.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Assign multiple ops per pool if resources allow, but keep it bounded to avoid churn.
        # This improves responsiveness vs. "one op per pool" while remaining conservative.
        max_assignments_this_pool = 4
        made = 0

        while made < max_assignments_this_pool and avail_cpu > 0 and avail_ram > 0:
            pipeline = _pick_next_pipeline(s.waiting_queue)
            if pipeline is None:
                break

            st = pipeline.runtime_status()
            if st.is_pipeline_successful():
                continue
            if pipeline.pipeline_id in s.pipeline_terminal_failure:
                continue

            # Pick one runnable op whose parents are complete.
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable right now; requeue to try later.
                s.waiting_queue.append(pipeline)
                continue

            op = op_list[0]
            opk = (pipeline.pipeline_id, _op_key(op))

            # Compute CPU/RAM request:
            cpu_cap = _cpu_cap_for(pipeline.priority, pool.max_cpu_pool)
            ram_cap = _ram_cap_for(pipeline.priority, pool.max_ram_pool)

            # If we learned an OOM hint, honor it (as long as it fits).
            learned_ram = s.ram_hint.get(opk, 0)

            # Choose requested resources within available amounts.
            req_cpu = min(avail_cpu, cpu_cap)
            if req_cpu <= 0:
                s.waiting_queue.append(pipeline)
                break

            # RAM: avoid too-small allocations if we have a learned hint.
            req_ram = min(avail_ram, ram_cap)
            if learned_ram > 0:
                # If we can't meet the learned minimum, don't schedule it on this pool now.
                if learned_ram > avail_ram:
                    s.waiting_queue.append(pipeline)
                    break
                req_ram = min(avail_ram, max(req_ram, learned_ram))

            if req_ram <= 0:
                s.waiting_queue.append(pipeline)
                break

            # Final guard: if we have a learned hint that exceeds cap but fits avail,
            # prefer the hint to avoid repeated OOMs.
            if learned_ram > req_ram and learned_ram <= avail_ram:
                req_ram = learned_ram

            assignment = Assignment(
                ops=op_list,
                cpu=req_cpu,
                ram=req_ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update local availability and requeue the pipeline for further ops later.
            avail_cpu -= req_cpu
            avail_ram -= req_ram
            made += 1
            s.waiting_queue.append(pipeline)

    # Clean up: drop completed / terminal pipelines from the queue.
    cleaned = []
    for p in s.waiting_queue:
        if not _pipeline_alive(p):
            continue
        cleaned.append(p)
    s.waiting_queue = cleaned

    return suspensions, assignments
