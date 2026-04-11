# policy_key: scheduler_high_017
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.140109
# generation_seconds: 155.14
# generated_at: 2026-04-10T01:06:40.872866
@register_scheduler_init(key="scheduler_high_017")
def scheduler_high_017_init(s):
    """Priority-aware, completion-focused scheduler with OOM-adaptive RAM sizing.

    Main ideas:
    - Strict priority order for dispatch: QUERY > INTERACTIVE > BATCH.
    - Leave headroom in each pool when scheduling BATCH to reduce tail latency for arrivals of QUERY/INTERACTIVE.
    - Never drop pipelines just because they had FAILED ops; treat FAILED as retryable.
    - Detect OOM-like failures from ExecutionResult.error and exponentially increase per-operator RAM hints (clamped to pool max).
    - Mild fairness: round-robin within each priority queue and simple aging for BATCH to avoid indefinite starvation.
    """
    s.tick = 0

    # Priority queues (round-robin lists). We store Pipeline objects directly.
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track arrivals and pipeline objects by id.
    s.pipeline_by_id = {}
    s.arrival_tick = {}

    # Per-(pipeline, op) resource hints & failure counters.
    s.ram_hint = {}        # (pipeline_id, op_key) -> ram
    s.cpu_hint = {}        # (pipeline_id, op_key) -> cpu
    s.fail_count = {}      # (pipeline_id, op_key) -> int
    s.oom_count = {}       # (pipeline_id, op_key) -> int

    # Conservative headroom reserved from BATCH scheduling only.
    s.batch_headroom_cpu_frac = 0.15
    s.batch_headroom_ram_frac = 0.15

    # Limit how much we scan queues per pick to avoid quadratic behavior.
    s.scan_limit = 64


@register_scheduler(key="scheduler_high_017")
def scheduler_high_017(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    # Helper functions kept inside to avoid imports/externals.
    def _is_oom_error(err) -> bool:
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("out-of-memory" in e) or ("memoryerror" in e)

    def _op_key(op):
        # Prefer stable identifiers if present; fall back to object id.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # Some ids can be callable properties; guard.
                    return v() if callable(v) else v
                except Exception:
                    pass
        return id(op)

    def _assignable_states():
        # Use simulator-provided constant if available; otherwise derive.
        try:
            return ASSIGNABLE_STATES
        except NameError:
            return [OperatorState.PENDING, OperatorState.FAILED]

    def _pipeline_remaining_ops(p) -> int:
        st = p.runtime_status()
        rem = 0
        try:
            for state, cnt in st.state_counts.items():
                if state != OperatorState.COMPLETED:
                    rem += int(cnt)
        except Exception:
            # Fallback: treat as unknown remaining.
            rem = 10
        return rem

    def _pipeline_done(p) -> bool:
        try:
            return p.runtime_status().is_pipeline_successful()
        except Exception:
            return False

    def _get_ready_ops(p):
        st = p.runtime_status()
        return st.get_ops(_assignable_states(), require_parents_complete=True)

    def _cleanup_queues():
        # Remove completed pipelines and dead references.
        alive = set(s.pipeline_by_id.keys())
        for pr in list(s.queues.keys()):
            newq = []
            for p in s.queues[pr]:
                if p is None:
                    continue
                pid = getattr(p, "pipeline_id", None)
                if pid is None or pid not in alive:
                    continue
                if _pipeline_done(p):
                    continue
                newq.append(p)
            s.queues[pr] = newq

    def _pick_pipeline(prio, scheduled_pids: set):
        # Round-robin scan with small heuristic:
        # - For BATCH, prefer older pipelines (aging) and smaller remaining ops (finish sooner).
        q = s.queues.get(prio, [])
        if not q:
            return None

        scan_n = min(len(q), s.scan_limit)
        best_idx = None
        best_score = None

        for i in range(scan_n):
            p = q[i]
            pid = p.pipeline_id
            if pid in scheduled_pids:
                continue
            if _pipeline_done(p):
                continue

            # Must have at least one ready op to be a good pick; otherwise skip (but keep in queue).
            try:
                ready = _get_ready_ops(p)
            except Exception:
                ready = []
            if not ready:
                continue

            rem = _pipeline_remaining_ops(p)
            age = s.tick - s.arrival_tick.get(pid, s.tick)

            if prio == Priority.BATCH_PIPELINE:
                # Higher age should win; fewer remaining ops should win.
                # Convert to a single score where lower is better.
                score = rem - min(50, age // 5)
            else:
                # For QUERY/INTERACTIVE: prefer fewer remaining ops (approx SRPT).
                score = rem

            if best_score is None or score < best_score:
                best_score = score
                best_idx = i

        if best_idx is None:
            # No schedulable pipeline found in scan window.
            return None

        # Pop the chosen pipeline and push it to the end (round-robin fairness).
        p = q.pop(best_idx)
        q.append(p)
        return p

    def _base_cpu_for_priority(prio, pool_max_cpu):
        # Small, safe caps; higher priority gets more CPU to reduce end-to-end latency.
        if prio == Priority.QUERY:
            return min(4.0, float(pool_max_cpu))
        if prio == Priority.INTERACTIVE:
            return min(2.0, float(pool_max_cpu))
        return min(1.0, float(pool_max_cpu))

    def _base_ram_for_priority(prio, pool_max_ram):
        # More conservative RAM for higher priority to reduce OOM retries (which inflate latency).
        if prio == Priority.QUERY:
            return 0.30 * float(pool_max_ram)
        if prio == Priority.INTERACTIVE:
            return 0.22 * float(pool_max_ram)
        return 0.14 * float(pool_max_ram)

    def _choose_resources(prio, pool, pid, op):
        ok = _op_key(op)

        # RAM: use hint if any; else conservative base; clamp to pool max.
        base_ram = _base_ram_for_priority(prio, pool.max_ram_pool)
        hint_ram = s.ram_hint.get((pid, ok), 0.0)
        ram = max(base_ram, float(hint_ram))

        # If we have repeated OOMs, jump quickly toward pool max to avoid more retries.
        oom_n = s.oom_count.get((pid, ok), 0)
        if oom_n >= 2:
            ram = max(ram, 0.75 * float(pool.max_ram_pool))
        if oom_n >= 3:
            ram = float(pool.max_ram_pool)

        # CPU: use hint if any; else base; clamp.
        base_cpu = _base_cpu_for_priority(prio, pool.max_cpu_pool)
        hint_cpu = s.cpu_hint.get((pid, ok), 0.0)
        cpu = max(base_cpu, float(hint_cpu))

        # Clamp to pool max.
        cpu = min(cpu, float(pool.max_cpu_pool))
        ram = min(ram, float(pool.max_ram_pool))

        # Avoid pathological tiny allocations.
        cpu = max(0.1, cpu)
        ram = max(0.1, ram)
        return cpu, ram

    # ---- Tick accounting and ingestion ----
    s.tick += 1

    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.pipeline_by_id:
            s.pipeline_by_id[pid] = p
            s.arrival_tick[pid] = s.tick
            s.queues[p.priority].append(p)
        else:
            # If we somehow see the same pipeline again, keep the newest reference.
            s.pipeline_by_id[pid] = p

    # ---- Learn from results (primarily OOM -> increase RAM hint) ----
    for r in results:
        if not hasattr(r, "ops") or not r.ops:
            continue
        pid = getattr(r, "pipeline_id", None)

        # If pipeline_id isn't present on result, best-effort: infer by scanning known pipelines for matching priority.
        # (Kept minimal to avoid heavy work; if missing, we still update using a None pid namespace.)
        if pid is None:
            pid = None

        if r.failed():
            oom = _is_oom_error(getattr(r, "error", None))
            for op in r.ops:
                ok = _op_key(op)
                k = (pid, ok)
                s.fail_count[k] = s.fail_count.get(k, 0) + 1
                if oom:
                    s.oom_count[k] = s.oom_count.get(k, 0) + 1
                    prev = float(s.ram_hint.get(k, 0.0))
                    last = float(getattr(r, "ram", 0.0) or 0.0)

                    # Exponential backoff from last tried RAM, with a floor on growth.
                    # Use 1.8x to converge quickly without instantly pinning max.
                    new_hint = max(prev, last * 1.8, last + 1.0)

                    # Keep a modest minimum so we don't oscillate at tiny sizes.
                    new_hint = max(new_hint, 0.1)
                    s.ram_hint[k] = new_hint
                else:
                    # For non-OOM failures, a small CPU bump can help if failures are timeouts/slowdowns.
                    prev_cpu = float(s.cpu_hint.get(k, 0.0))
                    last_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                    s.cpu_hint[k] = max(prev_cpu, last_cpu * 1.25, last_cpu + 0.5, 0.5)

    # Remove completed pipelines from queues and tracking.
    completed = []
    for pid, p in list(s.pipeline_by_id.items()):
        if _pipeline_done(p):
            completed.append(pid)
    for pid in completed:
        s.pipeline_by_id.pop(pid, None)
        s.arrival_tick.pop(pid, None)
        # Keep hints around (could be reused in repeated workloads), but they are namespaced by pid/op anyway.

    _cleanup_queues()

    # Early exit if nothing changed; keep deterministic behavior.
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Local mutable pool state so we don't over-assign within a single tick.
    pool_state = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pool_state[pool_id] = {
            "avail_cpu": float(pool.avail_cpu_pool),
            "avail_ram": float(pool.avail_ram_pool),
        }

    def _pool_budget(pool_id, prio):
        pool = s.executor.pools[pool_id]
        a_cpu = pool_state[pool_id]["avail_cpu"]
        a_ram = pool_state[pool_id]["avail_ram"]

        if prio == Priority.BATCH_PIPELINE:
            # Reserve headroom to absorb QUERY/INTERACTIVE arrivals.
            head_cpu = max(0.0, float(pool.max_cpu_pool) * float(s.batch_headroom_cpu_frac))
            head_ram = max(0.0, float(pool.max_ram_pool) * float(s.batch_headroom_ram_frac))
            return max(0.0, a_cpu - head_cpu), max(0.0, a_ram - head_ram)
        return a_cpu, a_ram

    def _best_pool_for(prio):
        # Choose a pool with the most relevant budget for this priority.
        best = None
        best_val = None
        for pool_id in range(s.executor.num_pools):
            b_cpu, b_ram = _pool_budget(pool_id, prio)
            if b_cpu <= 0.0 or b_ram <= 0.0:
                continue
            val = b_cpu if prio != Priority.BATCH_PIPELINE else (b_ram + 0.25 * b_cpu)
            if best_val is None or val > best_val:
                best_val = val
                best = pool_id
        return best

    scheduled_pids = set()

    # Global dispatch loop: try to place higher priority work first across all pools.
    for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        # Keep placing as long as we can make progress for this priority.
        for _ in range(1024):
            pool_id = _best_pool_for(prio)
            if pool_id is None:
                break

            b_cpu, b_ram = _pool_budget(pool_id, prio)
            if b_cpu <= 0.0 or b_ram <= 0.0:
                break

            p = _pick_pipeline(prio, scheduled_pids)
            if p is None:
                break

            pid = p.pipeline_id
            ready_ops = _get_ready_ops(p)
            if not ready_ops:
                # Not schedulable right now; leave it in the queue.
                continue

            op = ready_ops[0]
            pool = s.executor.pools[pool_id]
            cpu, ram = _choose_resources(prio, pool, pid, op)

            # Clamp to this pool's current budget.
            cpu = min(cpu, b_cpu)
            ram = min(ram, b_ram)

            # If still can't fit, skip this pipeline for now (avoid head-of-line blocking).
            if cpu <= 0.0 or ram <= 0.0:
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            pool_state[pool_id]["avail_cpu"] -= cpu
            pool_state[pool_id]["avail_ram"] -= ram
            scheduled_pids.add(pid)

    return suspensions, assignments
