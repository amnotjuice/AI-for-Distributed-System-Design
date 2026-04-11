# policy_key: scheduler_medium_035
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.083017
# generation_seconds: 91.13
# generated_at: 2026-04-09T23:16:56.036164
@register_scheduler_init(key="scheduler_medium_035")
def scheduler_medium_035_init(s):
    """
    Priority + aging + OOM-adaptive sizing scheduler.

    Goals (aligned to objective):
      - Keep QUERY/INTERACTIVE latency low via strict priority ordering.
      - Avoid 720s penalties by aggressively mitigating OOMs (increase RAM on retry).
      - Avoid starvation by adding simple aging for lower priorities.
      - Avoid wasting cluster time on non-retryable failures (after a small cap).
    """
    # Logical time (ticks) for aging.
    s._tick = 0

    # Waiting pipelines and metadata.
    s._waiting = []  # List[Pipeline]
    s._arrival_tick = {}  # pipeline_id -> tick

    # Per-operator learned hints (absolute resource units used by the simulator).
    s._ram_hint = {}  # op_key -> ram
    s._cpu_hint = {}  # op_key -> cpu

    # Failure bookkeeping to drive retries and to stop futile work.
    s._oom_failures = {}      # op_key -> count
    s._nonoom_failures = {}   # op_key -> count
    s._op_to_pipeline = {}    # op_key -> pipeline_id
    s._terminal_pipelines = set()  # pipeline_ids we stop scheduling (doomed/non-retryable)

    # Tunables.
    s._max_nonoom_retries = 2
    s._max_oom_retries = 8

    # Aging knobs: batch gets more aging to ensure eventual progress.
    s._aging_per_tick = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 1.50,
    }


@register_scheduler(key="scheduler_medium_035")
def scheduler_medium_035(s, results, pipelines):
    def _is_oom_error(err) -> bool:
        if not err:
            return False
        msg = str(err).lower()
        return (
            "oom" in msg
            or "out of memory" in msg
            or "cuda out of memory" in msg
            or "memoryerror" in msg
            or "malloc" in msg and "fail" in msg
        )

    def _safe_len(x, default=None):
        try:
            return len(x)
        except Exception:
            return default

    def _op_key(pipeline_id, op):
        # Prefer stable identifiers if present; otherwise fall back to repr(op).
        op_id = (
            getattr(op, "op_id", None)
            or getattr(op, "operator_id", None)
            or getattr(op, "task_id", None)
            or getattr(op, "name", None)
        )
        if op_id is None:
            op_id = repr(op)
        return (pipeline_id, op_id)

    def _priority_base(pri):
        # Strongly prefer query/interactive to minimize weighted latency.
        if pri == Priority.QUERY:
            return 10000.0
        if pri == Priority.INTERACTIVE:
            return 6000.0
        return 2000.0

    def _default_cpu(pri, pool, avail_cpu):
        max_cpu = getattr(pool, "max_cpu_pool", avail_cpu)
        if pri == Priority.QUERY:
            target = 0.75 * max_cpu
        elif pri == Priority.INTERACTIVE:
            target = 0.55 * max_cpu
        else:
            target = 0.35 * max_cpu
        cpu = min(avail_cpu, max(1.0, target))
        return cpu

    def _default_ram(pri, pool, avail_ram):
        max_ram = getattr(pool, "max_ram_pool", avail_ram)
        # Start moderately to reduce OOM risk; RAM doesn't speed up (per simulator),
        # so prefer safety for high priority, while not fully monopolizing the pool.
        if pri == Priority.QUERY:
            target = 0.35 * max_ram
        elif pri == Priority.INTERACTIVE:
            target = 0.28 * max_ram
        else:
            target = 0.22 * max_ram

        # Ensure a small positive request.
        ram = min(avail_ram, max(0.01 * max_ram, target))
        return ram

    def _choose_resources(pipeline, op, pool, avail_cpu, avail_ram):
        p_id = pipeline.pipeline_id
        pri = pipeline.priority
        k = _op_key(p_id, op)
        s._op_to_pipeline[k] = p_id

        cpu = s._cpu_hint.get(k)
        ram = s._ram_hint.get(k)

        if cpu is None:
            cpu = _default_cpu(pri, pool, avail_cpu)
        else:
            cpu = min(avail_cpu, max(1.0, float(cpu)))

        if ram is None:
            ram = _default_ram(pri, pool, avail_ram)
        else:
            ram = min(avail_ram, max(0.01 * getattr(pool, "max_ram_pool", avail_ram), float(ram)))

        # Cap by pool maxima (absolute units).
        max_cpu = getattr(pool, "max_cpu_pool", avail_cpu)
        max_ram = getattr(pool, "max_ram_pool", avail_ram)
        cpu = min(cpu, max_cpu)
        ram = min(ram, max_ram)

        return cpu, ram

    def _pipeline_score(p):
        # Higher is better.
        pri = p.priority
        base = _priority_base(pri)
        waited = max(0, s._tick - s._arrival_tick.get(p.pipeline_id, s._tick))
        aging = waited * s._aging_per_tick.get(pri, 0.5)

        st = p.runtime_status()
        completed = st.state_counts.get(OperatorState.COMPLETED, 0)

        total = _safe_len(getattr(p, "values", None), default=None)
        if total is None:
            remaining = 1
        else:
            remaining = max(1, total - completed)

        # Mild SRPT-like bias: prefer pipelines closer to completion (reduces tail).
        # Keep this small so we don't starve longer pipelines, especially batch.
        srpt = -1.5 * remaining

        # If pipeline already has failed ops, don't deprioritize (we want to finish),
        # but we also don't want to thrash on non-retryable failures (handled elsewhere).
        return base + aging + srpt

    # --- Update state with arrivals ---
    for p in pipelines:
        if p.pipeline_id not in s._arrival_tick:
            s._arrival_tick[p.pipeline_id] = s._tick
        s._waiting.append(p)

    # --- Learn from execution results (OOM-adaptive sizing) ---
    for r in results:
        if not r or not getattr(r, "ops", None):
            continue

        # We need pipeline_id; infer from op if possible, else via last known mapping.
        op0 = r.ops[0]
        pid = getattr(op0, "pipeline_id", None)
        if pid is None:
            # If we previously scheduled this op, we likely stored the mapping;
            # but we still need a key. Try a few identifiers and scan mapping.
            op_id = (
                getattr(op0, "op_id", None)
                or getattr(op0, "operator_id", None)
                or getattr(op0, "task_id", None)
                or getattr(op0, "name", None)
            )
            if op_id is not None:
                # Find a matching (pipeline_id, op_id) in our mapping.
                for (p_id, o_id), _ in s._op_to_pipeline.items():
                    if o_id == op_id:
                        pid = p_id
                        break

        if pid is None:
            # Can't safely learn without a stable key; skip.
            continue

        k = _op_key(pid, op0)
        s._op_to_pipeline[k] = pid

        if r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                s._oom_failures[k] = s._oom_failures.get(k, 0) + 1

                # Increase RAM aggressively to avoid repeated OOM and 720 penalties.
                # Use observed allocation as a baseline; double on each OOM.
                prev = s._ram_hint.get(k, None)
                observed = getattr(r, "ram", None)
                if observed is None:
                    observed = prev if prev is not None else 0.0

                # Try doubling; keep a small epsilon so we actually move upward.
                new_ram = max(float(observed) * 2.0, (float(prev) * 2.0 if prev is not None else 0.0), float(observed) + 1e-6)

                # Also consider slightly increasing CPU only for very small allocations.
                prev_cpu = s._cpu_hint.get(k, None)
                observed_cpu = getattr(r, "cpu", None)
                if prev_cpu is None and observed_cpu is not None:
                    prev_cpu = float(observed_cpu)
                if prev_cpu is not None:
                    s._cpu_hint[k] = max(1.0, float(prev_cpu))

                s._ram_hint[k] = new_ram

                # If it's OOM-ing too many times, keep retrying but it may be unschedulable;
                # do not mark terminal (still potentially solvable by a larger pool).
                # However, after many OOMs, force the hint high to encourage placement on big pools.
                if s._oom_failures[k] >= s._max_oom_retries:
                    s._ram_hint[k] = max(s._ram_hint[k], float(observed) * 4.0)
            else:
                s._nonoom_failures[k] = s._nonoom_failures.get(k, 0) + 1
                # Non-OOM failures are likely code/data errors; avoid infinite churn.
                if s._nonoom_failures[k] >= s._max_nonoom_retries:
                    s._terminal_pipelines.add(pid)
        else:
            # On success, keep hints (do not shrink aggressively; stability > micro-optimization).
            pass

    # --- Clean waiting list: drop completed and terminal pipelines ---
    new_waiting = []
    for p in s._waiting:
        pid = p.pipeline_id
        if pid in s._terminal_pipelines:
            continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        new_waiting.append(p)
    s._waiting = new_waiting

    # If nothing changed, early exit.
    if not pipelines and not results and not s._waiting:
        s._tick += 1
        return [], []

    suspensions = []
    assignments = []

    # Helper to pick next runnable (pipeline, op) for a given pool and available resources.
    def _pick_next_for_pool(pool, rem_cpu, rem_ram):
        best = None  # (score, pipeline, op)
        for p in s._waiting:
            if p.pipeline_id in s._terminal_pipelines:
                continue

            st = p.runtime_status()
            # Quick skip if already done.
            if st.is_pipeline_successful():
                continue

            # Pick a ready op (parents complete).
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            op = ops[0]
            k = _op_key(p.pipeline_id, op)

            # If this op has repeated non-OOM failures, treat pipeline as terminal.
            if s._nonoom_failures.get(k, 0) >= s._max_nonoom_retries:
                s._terminal_pipelines.add(p.pipeline_id)
                continue

            # Check whether the hinted resources could possibly fit this pool;
            # if not, skip so another pool may pick it up.
            hinted_ram = s._ram_hint.get(k, None)
            if hinted_ram is not None:
                max_ram = getattr(pool, "max_ram_pool", rem_ram)
                if float(hinted_ram) > max_ram:
                    continue

            score = _pipeline_score(p)
            if best is None or score > best[0]:
                best = (score, p, op)

        if best is None:
            return None

        _, p, op = best
        cpu, ram = _choose_resources(p, op, pool, rem_cpu, rem_ram)

        # Must fit current remaining capacity.
        if cpu > rem_cpu or ram > rem_ram:
            return None

        return p, op, cpu, ram

    # Scheduling strategy across pools:
    # - If multiple pools, we bias pool 0 toward QUERY/INTERACTIVE by simply scheduling best-by-score
    #   (which heavily prefers those priorities). Other pools also schedule best-by-score, but since
    #   query/interactive should drain quickly, batch will tend to occupy secondary pools.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        rem_cpu = pool.avail_cpu_pool
        rem_ram = pool.avail_ram_pool

        if rem_cpu <= 0 or rem_ram <= 0:
            continue

        # Fill the pool with as many assignments as we can safely account for locally.
        # (We decrement rem_cpu/rem_ram to avoid over-assigning within a single tick.)
        while rem_cpu > 0 and rem_ram > 0:
            picked = _pick_next_for_pool(pool, rem_cpu, rem_ram)
            if picked is None:
                break

            p, op, cpu, ram = picked
            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            rem_cpu -= cpu
            rem_ram -= ram

            # Lightweight anti-head-of-line: after scheduling one op from a pipeline on this pool
            # in this tick, move it to the end to give others a chance.
            try:
                s._waiting.remove(p)
                s._waiting.append(p)
            except Exception:
                pass

    s._tick += 1
    return suspensions, assignments
