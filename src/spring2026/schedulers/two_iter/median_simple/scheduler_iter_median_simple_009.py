# policy_key: scheduler_iter_median_simple_009
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051283
# generation_seconds: 37.52
# generated_at: 2026-04-12T01:12:59.423182
@register_scheduler_init(key="scheduler_iter_median_simple_009")
def scheduler_iter_median_simple_009_init(s):
    """Iteration 2: Priority-first + per-pipeline OOM learning + pool headroom shaping.

    Goals: reduce weighted median latency by:
    - Strictly prioritizing QUERY/INTERACTIVE runnable ops over BATCH to avoid HoL blocking.
    - Reserving CPU/RAM headroom for high-priority work in each pool when any HP backlog exists.
    - Using smaller default CPU slices for HP ops (improves admission + concurrency).
    - Tracking container_id -> pipeline_id so OOM retries increase RAM for the right pipeline.
    - Simple SRPT-ish tie-break: within a priority class, pick pipelines with fewer remaining ops first.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline adaptive state
    # ram_mult: increases on OOM; decays slowly on success.
    # min_ram: a floor based on last observed OOM allocation to avoid repeating too-small RAM.
    # non_oom_fail: if True and pipeline has FAILED ops, do not retry (reduce wasted work).
    s.pstate = {}

    # Map execution containers back to pipelines so we can learn per-pipeline from results
    s.container_to_pipeline = {}

    # Round-robin cursors to prevent starvation within a priority
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }


@register_scheduler(key="scheduler_iter_median_simple_009")
def scheduler_iter_median_simple_009_scheduler(s, results, pipelines):
    """
    Step:
    1) Enqueue pipelines.
    2) Update per-pipeline RAM hints based on OOM results using container->pipeline mapping.
    3) For each pool, repeatedly assign 1 runnable op at a time:
       - Always try QUERY then INTERACTIVE before BATCH.
       - If any HP backlog exists, cap batch usage per pool to keep headroom for HP arrivals.
       - Within a priority, prefer pipelines with fewer remaining ops (SRPT-ish).
    """
    def _ensure_pstate(pid):
        if pid not in s.pstate:
            s.pstate[pid] = {"ram_mult": 1.0, "min_ram": 0.0, "non_oom_fail": False}
        return s.pstate[pid]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _remaining_ops(p):
        st = p.runtime_status()
        # "Remaining" includes pending/failed/assigned/running/suspending; completed excluded.
        # We bias toward finishing small DAGs first for latency.
        total = 0
        for state in (
            OperatorState.PENDING,
            OperatorState.FAILED,
            OperatorState.ASSIGNED,
            OperatorState.RUNNING,
            OperatorState.SUSPENDING,
        ):
            total += st.state_counts.get(state, 0)
        return total

    def _has_runnable_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return False
        if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
            return False
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return bool(ops)

    def _hp_backlog_exists():
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.queues[pri]:
                if _has_runnable_op(p):
                    return True
        return False

    def _pop_best_pipeline(pri):
        """Pick a pipeline within priority using SRPT-ish (fewest remaining ops), with RR fallback."""
        q = s.queues[pri]
        if not q:
            return None

        # Clean out completed / non-retriable failed pipelines
        i = 0
        while i < len(q):
            p = q[i]
            st = p.runtime_status()
            if st.is_pipeline_successful():
                q.pop(i)
                continue
            if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
                q.pop(i)
                continue
            i += 1

        if not q:
            s.rr_cursor[pri] = 0
            return None

        # If queue is small, do a full scan for smallest remaining ops among runnable pipelines.
        # If nothing runnable, return None.
        best_idx = None
        best_score = None
        for idx, p in enumerate(q):
            if not _has_runnable_op(p):
                continue
            score = _remaining_ops(p)
            if best_score is None or score < best_score:
                best_score = score
                best_idx = idx

        if best_idx is None:
            return None

        # Remove and return; we'll append back after we schedule (keeps simple queue behavior).
        return q.pop(best_idx)

    def _cpu_ram_request(pool, pri, pid, avail_cpu, avail_ram):
        pst = _ensure_pstate(pid)

        # Smaller default slices for HP reduce queueing and improve latency under contention.
        # Batch gets bigger slices only when allowed (handled by caps).
        if pri == Priority.QUERY:
            base_cpu = 1.0
            base_ram = max(1.0, pool.max_ram_pool * 0.08)
        elif pri == Priority.INTERACTIVE:
            base_cpu = min(2.0, max(1.0, pool.max_cpu_pool * 0.20))
            base_ram = max(1.0, pool.max_ram_pool * 0.14)
        else:
            # Batch: moderate chunk, but not the whole pool (avoids long monopolization).
            base_cpu = min(max(2.0, pool.max_cpu_pool * 0.35), 6.0 if pool.max_cpu_pool >= 6.0 else pool.max_cpu_pool)
            base_ram = max(1.0, pool.max_ram_pool * 0.25)

        # Apply learned RAM inflation and per-pipeline floor after OOM
        ram_req = max(base_ram * pst["ram_mult"], pst["min_ram"])
        cpu_req = base_cpu

        cpu_req = max(1.0, min(cpu_req, avail_cpu, pool.max_cpu_pool))
        ram_req = max(1.0, min(ram_req, avail_ram, pool.max_ram_pool))
        return cpu_req, ram_req

    def _pool_caps(pool, pri, avail_cpu, avail_ram, hp_backlog):
        """Soft headroom: when HP backlog exists, cap batch consumption per pool."""
        if pri != Priority.BATCH_PIPELINE or not hp_backlog:
            return avail_cpu, avail_ram

        # Keep meaningful headroom for HP. This is a latency-first policy.
        # (If pools are tiny, caps naturally clamp.)
        cpu_cap = min(avail_cpu, max(1.0, pool.max_cpu_pool * 0.40))
        ram_cap = min(avail_ram, max(1.0, pool.max_ram_pool * 0.45))
        return cpu_cap, ram_cap

    # 1) Enqueue new pipelines
    for p in pipelines:
        _ensure_pstate(p.pipeline_id)
        s.queues[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # 2) Update learning from results using container->pipeline mapping
    for r in results:
        cid = getattr(r, "container_id", None)
        pid = s.container_to_pipeline.get(cid, None)

        # If we can't map this result, we can't learn per-pipeline safely.
        if pid is None:
            continue

        pst = _ensure_pstate(pid)

        if hasattr(r, "failed") and r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                # OOM: strongly increase RAM next try; set a floor based on last attempt.
                last_ram = float(getattr(r, "ram", 0.0) or 0.0)
                if last_ram > 0:
                    pst["min_ram"] = max(pst["min_ram"], last_ram * 1.6)
                pst["ram_mult"] = min(pst["ram_mult"] * 2.0, 32.0)
            else:
                # Non-OOM failures: don't keep retrying failed ops forever.
                pst["non_oom_fail"] = True
        else:
            # Success: gently decay RAM inflation so we don't bloat forever.
            pst["ram_mult"] = max(1.0, pst["ram_mult"] * 0.92)
            # Keep min_ram (floor) as-is; it only affects post-OOM stability.

        # Container is done (success or failure); remove mapping to avoid leakage.
        if cid in s.container_to_pipeline:
            del s.container_to_pipeline[cid]

    # 3) Schedule: priority-first, headroom shaping, 1-op increments
    suspensions = []
    assignments = []

    hp_backlog = _hp_backlog_exists()

    priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep assigning until we can't place anything else in this pool this tick
        while avail_cpu > 0 and avail_ram > 0:
            chosen = None
            chosen_pri = None
            chosen_ops = None

            # Strict priority: HP first for latency
            for pri in priority_order:
                p = _pop_best_pipeline(pri)
                if p is None:
                    continue

                st = p.runtime_status()
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not ops:
                    # Not runnable right now; put it back and try next candidate
                    s.queues[pri].append(p)
                    continue

                chosen = p
                chosen_pri = pri
                chosen_ops = ops
                break

            if chosen is None:
                break

            # Enforce batch caps when HP backlog exists
            cpu_cap, ram_cap = _pool_caps(pool, chosen_pri, avail_cpu, avail_ram, hp_backlog)
            if cpu_cap <= 0 or ram_cap <= 0:
                # Can't schedule batch here due to caps; put back and stop this pool
                s.queues[chosen_pri].append(chosen)
                break

            cpu_req, ram_req = _cpu_ram_request(pool, chosen_pri, chosen.pipeline_id, cpu_cap, ram_cap)

            # If even minimal sizing doesn't fit, stop trying in this pool
            if cpu_req <= 0 or ram_req <= 0:
                s.queues[chosen_pri].append(chosen)
                break

            a = Assignment(
                ops=chosen_ops,
                cpu=cpu_req,
                ram=ram_req,
                priority=chosen.priority,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
            assignments.append(a)

            # Record mapping once container_id is known.
            # Some simulators may only assign container_id after execution starts; however,
            # in Eudoxia's interface, ExecutionResult carries it, so we map on the next tick
            # by using a stable surrogate: ops are included in results, but we don't have op->pid mapping.
            # Best we can do: if Assignment itself exposes container_id later, we can't rely on it here.
            # So we additionally maintain a "pending inference" map by op identity string if available.
            # Since we have no guaranteed op id API, we only map when/if assignment has container_id.
            if hasattr(a, "container_id"):
                s.container_to_pipeline[getattr(a, "container_id")] = chosen.pipeline_id

            # Locally consume capacity to avoid overscheduling
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Put pipeline back for future ops
            s.queues[chosen_pri].append(chosen)

    return suspensions, assignments
