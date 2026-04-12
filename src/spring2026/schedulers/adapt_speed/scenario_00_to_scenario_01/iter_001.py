@register_scheduler_init(key="scheduler_low_001")
def scheduler_low_001_init(s):
    from collections import deque

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Dedup / membership tracking (pipelines may be passed repeatedly each tick).
    s.pipelines_by_id = {}          # pipeline_id -> Pipeline (latest reference)
    s.pipeline_arrival_tick = {}    # pipeline_id -> tick first seen
    s.pipeline_in_queue = {}        # pipeline_id -> Priority if enqueued else None

    # Operator resource learning (shared across pipelines via "signature").
    s.op_ram_est = {}               # op_sig -> estimated RAM
    s.op_ooms = {}                  # op_sig -> count
    s.op_fails = {}                 # op_sig -> count
    s.op_success = {}               # op_sig -> count

    # Per-(pipeline, op_sig) failure counts to avoid pathological thrashing.
    s.pipe_op_ooms = {}             # (pipeline_id, op_sig) -> count
    s.pipe_op_fails = {}            # (pipeline_id, op_sig) -> count

    # Ticks / aging.
    s._tick = 0
    s.batch_aging_ticks = 80  # promote long-waiting batch to interactive for selection

    # Initial RAM guess as fraction of pool max (moderate to avoid chronic OOM, but not huge).
    s.init_ram_frac = {
        Priority.QUERY: 0.18,
        Priority.INTERACTIVE: 0.15,
        Priority.BATCH_PIPELINE: 0.10,
    }

    # Safety factor applied to the learned estimate at allocation time.
    s.ram_safety = {
        Priority.QUERY: 1.12,
        Priority.INTERACTIVE: 1.10,
        Priority.BATCH_PIPELINE: 1.06,
    }

    # CPU targets (favor concurrency; sublinear speedups make huge single-op CPU allocations wasteful).
    s.cpu_target_frac = {
        Priority.QUERY: 0.18,
        Priority.INTERACTIVE: 0.14,
        Priority.BATCH_PIPELINE: 0.10,
    }
    s.cpu_cap_frac = {
        Priority.QUERY: 0.40,
        Priority.INTERACTIVE: 0.30,
        Priority.BATCH_PIPELINE: 0.20,
    }
    s.min_cpu = 1.0

    # Reservations to protect tail latency without preemption.
    s.reserve_cpu_frac = 0.20
    s.reserve_ram_frac = 0.22

    # Retry behavior: keep completion high, but ramp RAM quickly on OOM.
    s.oom_backoff_mult = 2.1
    s.fail_backoff_mult = 1.25
    s.max_pipe_op_ooms = 10
    s.max_pipe_op_fails = 6


@register_scheduler(key="scheduler_low_001")
def scheduler_low_001_scheduler(s, results, pipelines):
    from collections import deque

    s._tick += 1

    def _rank(priority):
        if priority == Priority.QUERY:
            return 0
        if priority == Priority.INTERACTIVE:
            return 1
        return 2

    def _safe_str(x, max_len=160):
        try:
            v = str(x)
        except Exception:
            v = repr(x)
        if len(v) > max_len:
            return v[:max_len]
        return v

    def _op_sig(op):
        # Aim: stable across pipelines when possible (name/type/code-ish), without relying on object repr.
        parts = [type(op).__name__]
        for attr in ("name", "op_name", "fn_name", "kind", "type", "sql", "query", "udf_name", "callable_name"):
            if hasattr(op, attr):
                val = getattr(op, attr)
                if val is None:
                    continue
                parts.append(f"{attr}={_safe_str(val, 140)}")
        # Some operator objects only have an id; include it as weak signal (still useful within run).
        for attr in ("op_id", "operator_id", "id"):
            if hasattr(op, attr):
                val = getattr(op, attr)
                if val is None:
                    continue
                parts.append(f"{attr}={_safe_str(val, 60)}")
                break
        return "|".join(parts)

    def _is_oom_error(err):
        if not err:
            return False
        msg = _safe_str(err, 300).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("out_of_memory" in msg) or ("memoryerror" in msg)

    def _remove_deque_at(q, idx):
        # Remove element at position idx from deque and return it.
        q.rotate(-idx)
        item = q.popleft()
        q.rotate(idx)
        return item

    def _effective_priority(p):
        pr = p.priority
        if pr == Priority.BATCH_PIPELINE:
            at = s.pipeline_arrival_tick.get(p.pipeline_id, s._tick)
            if (s._tick - at) >= s.batch_aging_ticks:
                return Priority.INTERACTIVE
        return pr

    def _initial_ram_guess(pool, pr):
        frac = s.init_ram_frac.get(pr, 0.12)
        # Ensure non-trivial guess but never exceed a reasonable slice of pool.
        guess = pool.max_ram_pool * frac
        # Avoid tiny values in small pools; keep at least 1.0
        return max(1.0, float(guess))

    def _ram_need(pool, op_sig, pr, avail_ram):
        base = s.op_ram_est.get(op_sig)
        if base is None or base <= 0:
            base = _initial_ram_guess(pool, pr)
        safety = s.ram_safety.get(pr, 1.08)
        need = float(base) * float(safety)
        # Clamp to pool max; if pool is tight, we'll only run if it fits.
        need = min(need, float(pool.max_ram_pool))
        # Also can't allocate more than available.
        return min(need, float(avail_ram))

    def _cpu_need(pool, pr, avail_cpu):
        target = float(pool.max_cpu_pool) * float(s.cpu_target_frac.get(pr, 0.10))
        cap = float(pool.max_cpu_pool) * float(s.cpu_cap_frac.get(pr, 0.20))
        cpu = max(s.min_cpu, min(float(avail_cpu), max(target, s.min_cpu)))
        cpu = min(cpu, max(s.min_cpu, cap), float(avail_cpu))
        return cpu

    def _has_ready_high_priority():
        # Bounded scan for ready QUERY/INTERACTIVE ops.
        scan = 48
        for pr in (Priority.QUERY, Priority.INTERACTIVE):
            q = s.queues[pr]
            n = min(len(q), scan)
            for i in range(n):
                p = q[i]
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                if st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True):
                    return True
        return False

    def _enqueue_if_needed(p):
        pid = p.pipeline_id
        if s.pipeline_in_queue.get(pid) is not None:
            return
        st = p.runtime_status()
        if st.is_pipeline_successful():
            s.pipeline_in_queue[pid] = None
            return
        # If there is any remaining assignable work (even if not parent-ready), keep it in queue.
        # This avoids pipelines being forgotten if they were popped but not requeued due to edge cases.
        remaining = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=False)
        if remaining:
            s.queues[p.priority].append(p)
            s.pipeline_in_queue[pid] = p.priority

    # --- Update learning from results ---
    for r in results:
        if not r or not getattr(r, "ops", None):
            continue

        pid = getattr(r, "pipeline_id", None)
        attempted_ram = getattr(r, "ram", None)
        attempted_cpu = getattr(r, "cpu", None)
        was_fail = False
        was_oom = False
        try:
            was_fail = r.failed()
        except Exception:
            was_fail = False
        if was_fail:
            was_oom = _is_oom_error(getattr(r, "error", None))

        for op in r.ops:
            sig = _op_sig(op)

            if was_fail:
                s.op_fails[sig] = s.op_fails.get(sig, 0) + 1
                if was_oom:
                    s.op_ooms[sig] = s.op_ooms.get(sig, 0) + 1
                if pid is not None:
                    key = (pid, sig)
                    s.pipe_op_fails[key] = s.pipe_op_fails.get(key, 0) + 1
                    if was_oom:
                        s.pipe_op_ooms[key] = s.pipe_op_ooms.get(key, 0) + 1

                prev = s.op_ram_est.get(sig, None)
                if prev is None or prev <= 0:
                    prev = float(attempted_ram) if attempted_ram is not None else 1.0

                if attempted_ram is None:
                    attempted_ram = prev

                mult = s.oom_backoff_mult if was_oom else s.fail_backoff_mult
                # Increase aggressively on OOM to converge in 1-2 tries.
                new_est = max(float(prev) * float(mult), float(attempted_ram) * float(mult))
                # Small additive bump to avoid stuck at tiny values.
                new_est = max(new_est, float(prev) + 1.0)
                s.op_ram_est[sig] = new_est
            else:
                s.op_success[sig] = s.op_success.get(sig, 0) + 1
                # If we succeeded with *less* RAM than our current estimate, we can gently reduce.
                # (Success at lower cap implies true peak <= that cap in this sim model.)
                if attempted_ram is not None:
                    prev = s.op_ram_est.get(sig, None)
                    if prev is None:
                        s.op_ram_est[sig] = float(attempted_ram)
                    else:
                        if float(attempted_ram) < float(prev):
                            # Gentle decay toward observed safe cap, with a small safety margin.
                            decayed = max(float(attempted_ram) * 1.12, float(prev) * 0.88)
                            s.op_ram_est[sig] = decayed
                        else:
                            # Keep the larger (conservative) estimate.
                            s.op_ram_est[sig] = float(prev)

    # --- Ingest pipelines (dedup) ---
    for p in pipelines:
        pid = p.pipeline_id
        s.pipelines_by_id[pid] = p
        if pid not in s.pipeline_arrival_tick:
            s.pipeline_arrival_tick[pid] = s._tick
            s.pipeline_in_queue[pid] = None
        # Ensure pipelines remain enqueued while incomplete.
        _enqueue_if_needed(p)

    suspensions = []
    assignments = []

    high_ready = _has_ready_high_priority()

    # --- Scheduling ---
    # Fill each pool with fit-aware picks; backfill with lower priority while reserving headroom.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_cpu_frac) if high_ready else 0.0
        reserve_ram = float(pool.max_ram_pool) * float(s.reserve_ram_frac) if high_ready else 0.0
        reserve_cpu = min(reserve_cpu, float(pool.max_cpu_pool) * 0.35)
        reserve_ram = min(reserve_ram, float(pool.max_ram_pool) * 0.35)

        placed = 0
        place_limit = 64

        # Priority order for selection attempts (effective batch aging is handled during scoring).
        pr_order = (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE)

        while placed < place_limit and avail_cpu >= s.min_cpu and avail_ram >= 1.0:
            best = None
            best_pr = None
            best_q = None
            best_idx = None
            best_op = None
            best_cpu = None
            best_ram = None
            best_score = None

            # Search candidates across priorities; choose the one with best (priority, age, fit) score.
            for pr in pr_order:
                q = s.queues[pr]
                if not q:
                    continue

                scan = min(len(q), 72)
                for i in range(scan):
                    p = q[i]
                    pid = p.pipeline_id
                    st = p.runtime_status()

                    if st.is_pipeline_successful():
                        # Lazy clean-up: remove completed pipelines.
                        _remove_deque_at(q, i)
                        s.pipeline_in_queue[pid] = None
                        break  # deque mutated; restart this pr scan next outer iteration

                    # Determine effective priority (batch aging).
                    eff_pr = _effective_priority(p)
                    eff_rank = _rank(eff_pr)

                    ready_ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                    if not ready_ops:
                        continue

                    # Pick the ready op that best fits (prefer smaller RAM need to increase packing).
                    chosen_op = None
                    chosen_ram_need = None
                    chosen_cpu_need = None

                    for op in ready_ops[:8]:
                        sig = _op_sig(op)

                        # Avoid infinite thrash on one impossible op (still keep pipeline in queue).
                        if pid is not None:
                            ooms = s.pipe_op_ooms.get((pid, sig), 0)
                            fails = s.pipe_op_fails.get((pid, sig), 0)
                            if ooms > s.max_pipe_op_ooms or fails > s.max_pipe_op_fails:
                                continue

                        cpu_need = _cpu_need(pool, eff_pr, avail_cpu)
                        if cpu_need < s.min_cpu:
                            continue

                        ram_need = _ram_need(pool, sig, eff_pr, avail_ram)

                        # Require full "need" to be available to reduce OOM churn.
                        # Since _ram_need already caps at avail_ram, we additionally ensure it
                        # isn't being truncated by low availability when estimate is larger.
                        est = s.op_ram_est.get(sig, None)
                        if est is None or est <= 0:
                            est = _initial_ram_guess(pool, eff_pr)
                        full_need = min(float(pool.max_ram_pool), float(est) * float(s.ram_safety.get(eff_pr, 1.08)))
                        if full_need > float(avail_ram) + 1e-9:
                            continue  # don't start an op we can't size properly now

                        # Reservations: don't let batch consume reserved headroom when high-priority is ready.
                        if pr == Priority.BATCH_PIPELINE and high_ready:
                            if (avail_cpu - cpu_need) < reserve_cpu or (avail_ram - full_need) < reserve_ram:
                                continue

                        # Candidate is feasible.
                        if chosen_op is None or full_need < chosen_ram_need:
                            chosen_op = op
                            chosen_ram_need = full_need
                            chosen_cpu_need = cpu_need

                    if chosen_op is None:
                        continue

                    age = s._tick - s.pipeline_arrival_tick.get(pid, s._tick)
                    # Score: prioritize effective priority, then older (anti-starvation), then smaller fit.
                    score = (
                        eff_rank,                     # lower is better
                        -min(age, 10_000),            # older is better
                        float(chosen_ram_need),       # smaller is better for packing
                    )

                    if best is None or score < best_score:
                        best = p
                        best_pr = pr
                        best_q = q
                        best_idx = i
                        best_op = chosen_op
                        best_cpu = float(chosen_cpu_need)
                        best_ram = float(chosen_ram_need)
                        best_score = score

            if best is None:
                break

            # Remove from its queue (so we don't duplicate).
            chosen_pipeline = _remove_deque_at(best_q, best_idx)
            s.pipeline_in_queue[chosen_pipeline.pipeline_id] = None

            # Issue assignment (single-op container).
            assignment = Assignment(
                ops=[best_op],
                cpu=min(best_cpu, avail_cpu),
                ram=min(best_ram, avail_ram),
                priority=chosen_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
            assignments.append(assignment)

            avail_cpu -= float(assignment.cpu)
            avail_ram -= float(assignment.ram)
            placed += 1

            # Re-enqueue pipeline if still incomplete (keeps it flowing through DAG).
            # Always re-enqueue; readiness will be checked next tick.
            _enqueue_if_needed(chosen_pipeline)

    return suspensions, assignments
