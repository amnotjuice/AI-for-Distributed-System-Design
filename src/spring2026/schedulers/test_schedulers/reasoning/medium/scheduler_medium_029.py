# policy_key: scheduler_medium_029
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.102071
# generation_seconds: 110.10
# generated_at: 2026-04-09T23:08:47.242893
@register_scheduler_init(key="scheduler_medium_029")
def scheduler_medium_029_init(s):
    """
    Priority-weighted fair scheduler with conservative resource sizing + OOM-aware RAM backoff.

    Goals for the objective:
    - Keep QUERY/INTERACTIVE latency low via priority-weighted fairness (deficit round-robin).
    - Avoid failures (720s penalty) by starting with modest RAM buffers and escalating RAM on failure.
    - Avoid starvation with aging for long-waiting BATCH pipelines.
    - Keep code/simple assumptions: no preemption; only admission/placement/sizing/retry behavior.
    """
    # Tick counter (discrete time for aging/deficit accounting)
    s.tick = 0

    # Pipeline registry and per-pipeline adaptive sizing state
    #   ram_mult: multiplicative factor for requested RAM (increases on failure/OOM)
    #   cpu_mult: multiplicative factor for requested CPU (small increase on repeated failures)
    #   last_failed_ram: last RAM size that failed (used as a floor on the next attempt)
    #   failures: count of failures observed
    #   arrival_tick: first time pipeline observed
    s.pipeline_map = {}   # pipeline_id -> Pipeline
    s.pipeline_meta = {}  # pipeline_id -> dict

    # Priority queues (store pipeline_ids); keep one queue entry per pipeline_id
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.in_queue = set()

    # Deficit counters for weighted fairness (QUERY dominates score)
    s.deficit = {
        Priority.QUERY: 0.0,
        Priority.INTERACTIVE: 0.0,
        Priority.BATCH_PIPELINE: 0.0,
    }
    s.quantum = {
        Priority.QUERY: 10.0,
        Priority.INTERACTIVE: 5.0,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # Aging: after this many ticks waiting, allow one batch to "jump" occasionally
    s.batch_aging_threshold = 50
    s.batch_aging_stride = 10  # once threshold hit, allow jump every N ticks

    # Per-tick assignment limits (avoid overly aggressive packing in one simulator tick)
    s.max_total_assignments_per_tick = 32

    # Base sizing knobs (RAM buffer above min; CPU fraction of pool)
    s.base_ram_buffer = {
        Priority.QUERY: 1.20,
        Priority.INTERACTIVE: 1.15,
        Priority.BATCH_PIPELINE: 1.10,
    }
    s.base_cpu_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.25,
    }


@register_scheduler(key="scheduler_medium_029")
def scheduler_medium_029(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue new pipelines.
      2) Update per-pipeline RAM/CPU multipliers from failures.
      3) Add fairness quanta (deficit round-robin) + batch aging.
      4) Iteratively choose next pipeline -> choose best feasible pool -> assign 1 ready op.
    """
    def _get_pipeline_id_from_op(op):
        # Try common attribute patterns; return None if not found
        if op is None:
            return None
        if hasattr(op, "pipeline_id"):
            return getattr(op, "pipeline_id")
        if hasattr(op, "pipeline") and getattr(op, "pipeline") is not None:
            pl = getattr(op, "pipeline")
            if hasattr(pl, "pipeline_id"):
                return getattr(pl, "pipeline_id")
        if hasattr(op, "pipelineId"):
            return getattr(op, "pipelineId")
        return None

    def _get_pipeline_id_from_result(r):
        if hasattr(r, "pipeline_id"):
            return getattr(r, "pipeline_id")
        if hasattr(r, "ops") and r.ops:
            for op in r.ops:
                pid = _get_pipeline_id_from_op(op)
                if pid is not None:
                    return pid
        return None

    def _coerce_number(x):
        try:
            if x is None:
                return None
            if isinstance(x, bool):
                return None
            return float(x)
        except Exception:
            return None

    def _op_min_ram_hint(op):
        # Best-effort extraction of a "minimum RAM" hint from operator object
        # If unknown, return None and we will use a conservative default based on pool size.
        if op is None:
            return None
        for name in ("ram_min", "min_ram", "min_ram_gb", "min_memory", "memory_min", "memory", "ram", "mem"):
            if hasattr(op, name):
                v = _coerce_number(getattr(op, name))
                if v is not None and v > 0:
                    return v
        return None

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        s.pipeline_map[pid] = p
        if pid not in s.pipeline_meta:
            s.pipeline_meta[pid] = {
                "ram_mult": 1.0,
                "cpu_mult": 1.0,
                "last_failed_ram": 0.0,
                "failures": 0,
                "arrival_tick": s.tick,
            }
        if pid not in s.in_queue:
            s.queues[p.priority].append(pid)
            s.in_queue.add(pid)

    def _pipeline_active(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return False
        # Keep around even if it has failures: failed ops are retryable (FAILED is assignable)
        return True

    def _pick_next_pipeline_id():
        # Batch aging: if oldest batch has waited long enough, occasionally pick it to prevent starvation
        bq = s.queues[Priority.BATCH_PIPELINE]
        if bq:
            pid0 = bq[0]
            meta0 = s.pipeline_meta.get(pid0)
            if meta0 is not None:
                waited = s.tick - meta0.get("arrival_tick", s.tick)
                if waited >= s.batch_aging_threshold and ((waited - s.batch_aging_threshold) % s.batch_aging_stride == 0):
                    pid = bq.pop(0)
                    s.in_queue.discard(pid)
                    return pid

        # Deficit round-robin with weights (query > interactive > batch)
        # Add more quantum if all deficits are depleted.
        if s.deficit[Priority.QUERY] < 1 and s.deficit[Priority.INTERACTIVE] < 1 and s.deficit[Priority.BATCH_PIPELINE] < 1:
            for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                s.deficit[pr] += s.quantum[pr]

        # Prefer higher priority when deficits allow; still fair due to deficit accounting.
        for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if s.deficit[pr] >= 1 and s.queues[pr]:
                s.deficit[pr] -= 1
                pid = s.queues[pr].pop(0)
                s.in_queue.discard(pid)
                return pid

        # If no deficit-eligible queue, fall back to highest priority non-empty (and top up deficit slightly).
        for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if s.queues[pr]:
                s.deficit[pr] += 0.5  # mild encouragement to not stall
                pid = s.queues[pr].pop(0)
                s.in_queue.discard(pid)
                return pid

        return None

    def _cpu_request(priority, pool_max_cpu, pool_avail_cpu, cpu_mult):
        # Priority-dependent target, leaving headroom for other work (esp. to keep completion rate high)
        frac = s.base_cpu_frac.get(priority, 0.30)
        target = max(1.0, frac * float(pool_max_cpu))
        target *= float(cpu_mult)

        # Avoid grabbing the entire pool; keep at least 20% free if possible.
        cap = max(1.0, 0.80 * float(pool_avail_cpu))
        return max(1.0, min(target, float(pool_avail_cpu), cap, float(pool_max_cpu)))

    def _ram_request(priority, op_min_hint, pool_max_ram, last_failed_ram, ram_mult):
        # Base default if operator doesn't expose a RAM hint: small portion of pool, minimum 0.5
        if op_min_hint is None:
            op_min_hint = max(0.5, 0.05 * float(pool_max_ram))

        buf = s.base_ram_buffer.get(priority, 1.12)
        req = float(op_min_hint) * float(buf) * float(ram_mult)

        # If we previously failed at some RAM size, don't go below 1.5x that attempt.
        if last_failed_ram and last_failed_ram > 0:
            req = max(req, 1.5 * float(last_failed_ram))

        # Never exceed pool max
        req = min(req, float(pool_max_ram))

        # Ensure > 0
        return max(0.1, req)

    def _choose_pool(priority, ram_need, cpu_need, local_avail_cpu, local_avail_ram, pools):
        feasible = []
        for i in range(len(pools)):
            if local_avail_cpu[i] >= cpu_need and local_avail_ram[i] >= ram_need:
                feasible.append(i)
        if not feasible:
            return None

        # Pool choice heuristic:
        # - QUERY/INTERACTIVE: prefer more remaining headroom (reduce interference, reduce chance of requeue stalls)
        # - BATCH: prefer best-fit RAM (pack) to preserve headroom for interactive
        best_i = None
        best_score = None
        for i in feasible:
            rem_ram = local_avail_ram[i] - ram_need
            rem_cpu = local_avail_cpu[i] - cpu_need
            if priority in (Priority.QUERY, Priority.INTERACTIVE):
                score = rem_ram + 0.1 * rem_cpu  # bigger is better
            else:
                score = -abs(rem_ram) - 0.05 * abs(rem_cpu)  # closer fit is better
            if best_score is None or score > best_score:
                best_score = score
                best_i = i
        return best_i

    # Advance tick
    s.tick += 1

    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Update deficit quanta every tick (keeps scheduler responsive even without arrivals/results)
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        s.deficit[pr] += s.quantum[pr] * 0.05  # small steady refill to reduce stalls

    # React to execution results (OOM/failure -> bump RAM; repeated failures -> tiny CPU bump)
    for r in results:
        if not hasattr(r, "failed") or not r.failed():
            continue
        pid = _get_pipeline_id_from_result(r)
        if pid is None:
            continue
        if pid not in s.pipeline_meta:
            # In case we missed the pipeline arrival (defensive)
            s.pipeline_meta[pid] = {
                "ram_mult": 1.0,
                "cpu_mult": 1.0,
                "last_failed_ram": 0.0,
                "failures": 0,
                "arrival_tick": s.tick,
            }
        meta = s.pipeline_meta[pid]
        meta["failures"] = int(meta.get("failures", 0)) + 1

        err = ""
        if hasattr(r, "error") and r.error is not None:
            try:
                err = str(r.error).lower()
            except Exception:
                err = ""

        # Treat anything that looks like OOM as RAM issue; otherwise still increase RAM modestly
        is_oom = ("oom" in err) or ("out of memory" in err) or ("memoryerror" in err) or ("killed" in err and "memory" in err)

        # Track last failed RAM attempt as a floor
        if hasattr(r, "ram"):
            try:
                meta["last_failed_ram"] = max(float(meta.get("last_failed_ram", 0.0)), float(r.ram))
            except Exception:
                pass

        # Escalate RAM more aggressively on OOM-like failures
        meta["ram_mult"] = min(16.0, float(meta.get("ram_mult", 1.0)) * (1.6 if is_oom else 1.25))

        # Small CPU bump after repeated failures (could be timeouts/slowdowns modeled as failures)
        if meta["failures"] >= 2:
            meta["cpu_mult"] = min(2.5, float(meta.get("cpu_mult", 1.0)) * 1.10)

        # Ensure pipeline is queued for retry
        p = s.pipeline_map.get(pid)
        if p is not None:
            _enqueue_pipeline(p)

    # If nothing to do, early exit
    if not s.queues[Priority.QUERY] and not s.queues[Priority.INTERACTIVE] and not s.queues[Priority.BATCH_PIPELINE]:
        return [], []

    # Local snapshot of pool availability; we decrement locally as we create assignments this tick.
    pools = [s.executor.pools[i] for i in range(s.executor.num_pools)]
    local_avail_cpu = [float(p.avail_cpu_pool) for p in pools]
    local_avail_ram = [float(p.avail_ram_pool) for p in pools]

    suspensions = []  # no preemption in this policy
    assignments = []

    # Iteratively assign up to a bounded number of operators per tick
    total_budget = int(max(1, s.max_total_assignments_per_tick))
    blocked_rounds = 0

    while total_budget > 0:
        pid = _pick_next_pipeline_id()
        if pid is None:
            break

        p = s.pipeline_map.get(pid)
        if p is None:
            # Unknown pipeline reference; skip
            continue

        # Drop completed pipelines from scheduling
        if not _pipeline_active(p):
            continue

        st = p.runtime_status()

        # Find exactly one ready operator (atomic scheduling unit)
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not ready yet (parents running). Requeue to try later.
            _enqueue_pipeline(p)
            blocked_rounds += 1
            # If we keep hitting only blocked pipelines, stop to avoid burning the tick.
            if blocked_rounds >= (len(s.in_queue) + 8):
                break
            continue

        blocked_rounds = 0
        op = op_list[0]
        meta = s.pipeline_meta.get(pid, None)
        if meta is None:
            meta = {
                "ram_mult": 1.0,
                "cpu_mult": 1.0,
                "last_failed_ram": 0.0,
                "failures": 0,
                "arrival_tick": s.tick,
            }
            s.pipeline_meta[pid] = meta

        # Compute resource request using per-pipeline adaptive multipliers
        op_min_hint = _op_min_ram_hint(op)

        # First, pick a "reference" pool size for sizing (use max pool to avoid undersizing when multiple pools exist)
        max_pool_cpu = max(1.0, max(float(pl.max_cpu_pool) for pl in pools))
        max_pool_ram = max(0.1, max(float(pl.max_ram_pool) for pl in pools))

        # Initial CPU/RAM needs derived from a "typical" large pool, then validated per pool in placement.
        cpu_need_ref = _cpu_request(p.priority, max_pool_cpu, max_pool_cpu, meta.get("cpu_mult", 1.0))
        ram_need_ref = _ram_request(p.priority, op_min_hint, max_pool_ram, meta.get("last_failed_ram", 0.0), meta.get("ram_mult", 1.0))

        # Choose a pool where it fits given CURRENT local availability; then re-size to that pool's scale/limits.
        # Try with reference sizes first; if it doesn't fit anywhere, we'll attempt downsizing CPU (not RAM) to improve feasibility.
        chosen = _choose_pool(p.priority, ram_need_ref, min(cpu_need_ref, max_pool_cpu), local_avail_cpu, local_avail_ram, pools)

        if chosen is None:
            # Try reducing CPU request to fit (helps completion without risking OOM).
            cpu_need_ref2 = max(1.0, 0.5 * float(cpu_need_ref))
            chosen = _choose_pool(p.priority, ram_need_ref, cpu_need_ref2, local_avail_cpu, local_avail_ram, pools)
            if chosen is None:
                # Can't place now; requeue and try later
                _enqueue_pipeline(p)
                continue
            cpu_need_ref = cpu_need_ref2

        pool = pools[chosen]

        # Final CPU/RAM for the chosen pool: recompute with chosen pool's max/avail to avoid over-allocating.
        cpu_need = _cpu_request(p.priority, float(pool.max_cpu_pool), float(local_avail_cpu[chosen]), meta.get("cpu_mult", 1.0))
        ram_need = _ram_request(p.priority, op_min_hint, float(pool.max_ram_pool), meta.get("last_failed_ram", 0.0), meta.get("ram_mult", 1.0))

        # If still doesn't fit due to local availability, try smaller CPU (RAM stays to avoid failures).
        if ram_need > local_avail_ram[chosen]:
            # Try to place on a different pool that has more RAM headroom
            chosen2 = _choose_pool(p.priority, ram_need, 1.0, local_avail_cpu, local_avail_ram, pools)
            if chosen2 is None:
                _enqueue_pipeline(p)
                continue
            chosen = chosen2
            pool = pools[chosen]
            cpu_need = _cpu_request(p.priority, float(pool.max_cpu_pool), float(local_avail_cpu[chosen]), meta.get("cpu_mult", 1.0))

        if cpu_need > local_avail_cpu[chosen]:
            cpu_need = max(1.0, min(float(local_avail_cpu[chosen]), cpu_need))

        if ram_need > local_avail_ram[chosen] or cpu_need > local_avail_cpu[chosen] or ram_need <= 0 or cpu_need <= 0:
            _enqueue_pipeline(p)
            continue

        # Create assignment
        assignments.append(
            Assignment(
                ops=op_list,
                cpu=cpu_need,
                ram=ram_need,
                priority=p.priority,
                pool_id=chosen,
                pipeline_id=p.pipeline_id,
            )
        )

        # Update local availability
        local_avail_cpu[chosen] -= float(cpu_need)
        local_avail_ram[chosen] -= float(ram_need)

        # Requeue pipeline so it can progress to subsequent operators after this one completes
        _enqueue_pipeline(p)

        total_budget -= 1

    return suspensions, assignments
