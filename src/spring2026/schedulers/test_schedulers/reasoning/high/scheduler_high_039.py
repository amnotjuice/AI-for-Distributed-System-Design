# policy_key: scheduler_high_039
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.153325
# generation_seconds: 173.45
# generated_at: 2026-04-10T03:04:18.079005
@register_scheduler_init(key="scheduler_high_039")
def scheduler_high_039_init(s):
    """
    Priority-aware, failure-averse scheduler.

    Main ideas:
      - Always prefer QUERY > INTERACTIVE > BATCH, with aging to avoid starvation
      - Be conservative about OOMs: remember per-operator RAM guesses; on failure, exponentially increase RAM and retry
      - Avoid letting batch consume the last headroom when high-priority work is runnable (soft per-pool reservations)
      - Keep per-pipeline progress moving (favor fewer remaining ops within the same priority class)
    """
    s.tick = 0

    # Active pipelines keyed by pipeline_id (we keep references to the Pipeline objects).
    s.active = {}
    s.arrival_tick = {}
    s.pipeline_last_progress = {}

    # Per-operator tracking (keyed by id(op) since ops are objects).
    s.op_ram_guess = {}       # "safe" RAM to request next time
    s.op_fail_count = {}      # consecutive-ish failures
    s.op_to_pipeline = {}     # op -> pipeline_id mapping captured on assignment
    s.inflight_ops = set()    # ops we've assigned but haven't seen a result for yet

    # Tuning knobs
    s.batch_promote_age_ticks = 250  # old batch gets promoted to interactive class to avoid starvation
    s.reserve_frac = 0.15           # reserve 15% of pool for high-priority when HP is runnable
    s.max_ram_bump = 8.0            # cap multiplicative bumps per failure (still capped by pool max RAM)
    s.cpu_min = 1.0                # we can always run with at least 1 vCPU if available


def _base_class(priority):
    # Lower is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _effective_class(priority, age_ticks, promote_age_ticks):
    # Promote old batch to avoid indefinite starvation.
    if priority == Priority.BATCH_PIPELINE and age_ticks >= promote_age_ticks:
        return 1
    return _base_class(priority)


def _remaining_ops(status):
    # Count non-completed ops as a rough "remaining work" proxy.
    total = 0
    for st in (
        OperatorState.PENDING,
        OperatorState.FAILED,
        OperatorState.ASSIGNED,
        OperatorState.RUNNING,
        OperatorState.SUSPENDING,
    ):
        try:
            total += status.state_counts.get(st, 0)
        except Exception:
            pass
    return max(1, total)


def _default_ram(pool, eff_class):
    # RAM is "free" for speed but expensive for concurrency. Bias higher for HP to reduce OOM retries.
    max_ram = float(pool.max_ram_pool)
    if eff_class == 0:   # query
        frac = 0.25
    elif eff_class == 1:  # interactive (and promoted batch)
        frac = 0.20
    else:               # batch
        frac = 0.12
    # Ensure at least 1 unit if possible.
    return max(1.0, max_ram * frac)


def _cpu_target(pool, eff_class, hp_backlog):
    max_cpu = float(pool.max_cpu_pool)

    # Conservative caps to allow more parallelism; give more when backlog is small.
    if eff_class == 0:  # query
        cap = min(8.0, max(2.0, max_cpu * 0.50))
        if hp_backlog <= 1:
            cap = min(max_cpu, cap * 1.5)
        return cap
    if eff_class == 1:  # interactive (and promoted batch)
        cap = min(4.0, max(1.0, max_cpu * 0.33))
        if hp_backlog <= 1:
            cap = min(max_cpu, cap * 1.25)
        return cap

    # batch
    cap = min(2.0, max(1.0, max_cpu * 0.25))
    return cap


def _is_oom_error(err):
    if err is None:
        return False
    try:
        return "oom" in str(err).lower() or "out of memory" in str(err).lower()
    except Exception:
        return False


def _pool_reserve(pool, reserve_frac):
    # Soft reservations (only enforced for batch when HP is runnable/fittable).
    return (float(pool.max_cpu_pool) * reserve_frac, float(pool.max_ram_pool) * reserve_frac)


def _hp_fittable_in_pool(candidates, pool, avail_cpu, avail_ram, assigned_pipelines, inflight_ops, promote_age_ticks):
    # If any query/interactive (effective_class <= 1) can fit with minimal CPU, treat HP as fittable.
    for c in candidates:
        if c["pipeline_id"] in assigned_pipelines:
            continue
        if c["op_id"] in inflight_ops:
            continue
        if c["eff_class"] > 1:
            continue
        # Minimal CPU check (we can always scale down CPU; RAM is the critical constraint for OOM avoidance).
        if avail_cpu < 1.0:
            return False
        if avail_ram >= c["ram_req"](pool):
            return True
    return False


@register_scheduler(key="scheduler_high_039")
def scheduler_high_039(s, results, pipelines):
    s.tick += 1

    # ---- Incorporate new arrivals ----
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.active:
            s.active[pid] = p
            s.arrival_tick[pid] = s.tick
            s.pipeline_last_progress[pid] = s.tick

    # Fast early exit
    if not results and not pipelines:
        # Still attempt scheduling; the simulator may rely on scheduling ticks even without changes.
        pass

    # ---- Process execution results: update RAM guesses & failure counts ----
    for r in results:
        # Clear inflight markers and update per-op estimates
        for op in getattr(r, "ops", []) or []:
            oid = id(op)
            s.inflight_ops.discard(oid)

            pid = s.op_to_pipeline.get(oid)
            if pid is not None:
                s.pipeline_last_progress[pid] = s.tick

            if r.failed():
                prev = float(s.op_ram_guess.get(oid, max(float(getattr(r, "ram", 1.0)), 1.0)))
                fc = int(s.op_fail_count.get(oid, 0)) + 1
                s.op_fail_count[oid] = fc

                # Exponential RAM bumps; stronger bump when we believe it's OOM.
                bump_mult = 2.0 if _is_oom_error(getattr(r, "error", None)) else 1.5
                bump_mult = min(bump_mult ** min(fc, 3), s.max_ram_bump)

                desired = max(prev, float(getattr(r, "ram", prev))) * bump_mult

                # Cap by the pool's max RAM (if available); otherwise keep the desired guess.
                try:
                    max_ram_pool = float(s.executor.pools[r.pool_id].max_ram_pool)
                    desired = min(desired, max_ram_pool)
                except Exception:
                    pass

                s.op_ram_guess[oid] = max(1.0, desired)
            else:
                # On success, remember the RAM that worked as a safe upper bound.
                used = float(getattr(r, "ram", 0.0))
                if used > 0:
                    s.op_ram_guess[oid] = max(float(s.op_ram_guess.get(oid, 0.0)), used)
                # Slowly forgive failures so we don't permanently over-penalize an op after transient errors.
                if oid in s.op_fail_count and s.op_fail_count[oid] > 0:
                    s.op_fail_count[oid] = max(0, int(s.op_fail_count[oid]) - 1)

    # ---- Remove completed pipelines from active set ----
    for pid, p in list(s.active.items()):
        try:
            if p.runtime_status().is_pipeline_successful():
                s.active.pop(pid, None)
                s.arrival_tick.pop(pid, None)
                s.pipeline_last_progress.pop(pid, None)
        except Exception:
            # Keep pipeline if status can't be read this tick.
            pass

    # ---- Build runnable candidates (one runnable op per pipeline per tick) ----
    candidates = []
    hp_backlog = 0

    for pid, p in s.active.items():
        try:
            status = p.runtime_status()
        except Exception:
            continue

        if status.is_pipeline_successful():
            continue

        try:
            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
        except Exception:
            ops = []

        # Choose the first assignable op that's not already inflight.
        chosen = None
        for o in ops:
            if id(o) not in s.inflight_ops:
                chosen = o
                break
        if chosen is None:
            continue

        age = int(s.tick - s.arrival_tick.get(pid, s.tick))
        eff_class = _effective_class(p.priority, age, s.batch_promote_age_ticks)
        rem = _remaining_ops(status)
        fail_cnt = int(s.op_fail_count.get(id(chosen), 0))

        if eff_class <= 1:
            hp_backlog += 1

        def _ram_req_fn_factory(op_id, pipeline_priority):
            # pool-dependent RAM request
            def _ram_req(pool):
                # Base guess: previous safe guess, else default by effective class on this pool.
                age2 = int(s.tick - s.arrival_tick.get(pid, s.tick))
                eff2 = _effective_class(pipeline_priority, age2, s.batch_promote_age_ticks)
                guess = s.op_ram_guess.get(op_id)
                if guess is None:
                    guess = _default_ram(pool, eff2)

                # If we've already failed a few times, bias upward slightly (still capped at pool max RAM).
                fc2 = int(s.op_fail_count.get(op_id, 0))
                if fc2 > 0 and guess < float(pool.max_ram_pool):
                    # Gentle extra bias; the hard bumps happen on failure events.
                    guess = min(float(pool.max_ram_pool), float(guess) * (1.0 + 0.10 * min(fc2, 5)))

                return float(max(1.0, min(float(pool.max_ram_pool), float(guess))))
            return _ram_req

        candidates.append(
            {
                "pipeline": p,
                "pipeline_id": pid,
                "priority": p.priority,
                "age": age,
                "eff_class": eff_class,
                "remaining": rem,
                "fail_cnt": fail_cnt,
                "op": chosen,
                "op_id": id(chosen),
                "ram_req": _ram_req_fn_factory(id(chosen), p.priority),
            }
        )

    # If nothing runnable, we're done.
    if not candidates:
        return [], []

    # Sort candidates once; we'll scan this list repeatedly for each pool.
    # Order: priority class -> fewest remaining ops -> oldest -> most failed first (to avoid accumulating penalties).
    candidates.sort(
        key=lambda c: (
            c["eff_class"],
            c["remaining"],
            -c["age"],
            -c["fail_cnt"],
            c["pipeline_id"],
        )
    )

    # ---- Assign ops to pools with soft HP reservation ----
    suspensions = []
    assignments = []
    assigned_pipelines = set()

    # Prefer scheduling on pools with more headroom first.
    pool_ids = list(range(s.executor.num_pools))
    try:
        pool_ids.sort(
            key=lambda i: (
                float(s.executor.pools[i].avail_ram_pool) + float(s.executor.pools[i].avail_cpu_pool)
            ),
            reverse=True,
        )
    except Exception:
        pass

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < s.cpu_min or avail_ram <= 0:
            continue

        res_cpu, res_ram = _pool_reserve(pool, s.reserve_frac)

        # Only enforce reservation if high-priority is both present AND fittable in this pool.
        hp_fittable = (hp_backlog > 0) and _hp_fittable_in_pool(
            candidates, pool, avail_cpu, avail_ram, assigned_pipelines, s.inflight_ops, s.batch_promote_age_ticks
        )

        while avail_cpu >= s.cpu_min and avail_ram > 0:
            pick_idx = None
            pick_cpu = None
            pick_ram = None

            for idx, c in enumerate(candidates):
                if c["pipeline_id"] in assigned_pipelines:
                    continue
                if c["op_id"] in s.inflight_ops:
                    continue

                # Compute resource requests for this pool.
                ram_need = float(c["ram_req"](pool))
                if ram_need > avail_ram:
                    continue

                # Choose CPU request; can scale down if needed, but keep at least 1.
                cpu_cap = float(_cpu_target(pool, c["eff_class"], hp_backlog))
                cpu_need = min(avail_cpu, cpu_cap)
                if cpu_need < s.cpu_min:
                    continue

                # Soft reservation: prevent batch from eating into reserved headroom when HP is runnable here.
                if hp_fittable and c["eff_class"] >= 2:
                    if (avail_cpu - res_cpu) < cpu_need or (avail_ram - res_ram) < ram_need:
                        continue

                pick_idx = idx
                pick_cpu = cpu_need
                pick_ram = ram_need
                break

            if pick_idx is None:
                break

            c = candidates.pop(pick_idx)

            # Create the assignment (one op at a time for lower interference and better fairness).
            op_list = [c["op"]]
            a = Assignment(
                ops=op_list,
                cpu=pick_cpu,
                ram=pick_ram,
                priority=c["priority"],
                pool_id=pool_id,
                pipeline_id=c["pipeline_id"],
            )
            assignments.append(a)

            # Track inflight to avoid duplicate scheduling before status updates.
            for op in op_list:
                oid = id(op)
                s.inflight_ops.add(oid)
                s.op_to_pipeline[oid] = c["pipeline_id"]

            assigned_pipelines.add(c["pipeline_id"])

            # Update available resources in this pool for subsequent placements.
            avail_cpu -= float(pick_cpu)
            avail_ram -= float(pick_ram)

    return suspensions, assignments
