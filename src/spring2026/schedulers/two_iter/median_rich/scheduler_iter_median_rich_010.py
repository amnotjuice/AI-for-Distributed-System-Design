# policy_key: scheduler_iter_median_rich_010
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.056816
# generation_seconds: 56.12
# generated_at: 2026-04-12T01:51:06.068565
@register_scheduler_init(key="scheduler_iter_median_rich_010")
def scheduler_iter_median_rich_010_init(s):
    """Iteration 2: priority-aware, multi-pool biased scheduling with RAM OOM learning.

    Directional improvements vs prior attempt:
    - Reduce latency by (a) preventing batch from occupying the "interactive" pool(s),
      (b) reducing head-of-line blocking via smaller per-op allocations for high priority,
      (c) cutting pathological RAM over-allocation (which inflated queueing) while still
      reacting aggressively to OOM via learned per-operator RAM hints.
    - Keep changes incremental: no suspensions/preemption (API variance risk), but add
      better placement + better sizing + better OOM retry heuristics.
    """
    # Priority queues (round-robin within each)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Per-(pipeline_id) state for conservative failure handling
    s.pstate = {}  # pipeline_id -> {"non_oom_fail": bool}

    # Learned per-operator RAM hints:
    # key: op_fingerprint -> {"ram": float, "seen": int}
    # "ram" represents a *requested allocation* that should avoid OOM for this op.
    s.op_ram_hint = {}

    # Light global RAM bump per priority if we cannot map result->pipeline/op reliably
    s.global_ram_mult = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }


@register_scheduler(key="scheduler_iter_median_rich_010")
def scheduler_iter_median_rich_010_scheduler(s, results, pipelines):
    """Scheduler step: enqueue, learn from results, then place ready ops by priority.

    Policy sketch:
    - Pool partitioning (if multiple pools):
        * pool 0: QUERY + INTERACTIVE preferred; BATCH only if idle/no HP backlog.
        * pool 1..N: BATCH preferred; HP can spill if idle and HP backlog is high.
    - Sizing:
        * QUERY: small CPU slice + modest RAM (from hint if known).
        * INTERACTIVE: moderate CPU slice + modest RAM.
        * BATCH: larger CPU slice but capped RAM to avoid reservation bloat.
    - OOM handling:
        * If an op OOMs, raise its per-op RAM hint aggressively.
        * If a non-OOM failure is detected, avoid infinite retries for that pipeline.
    """
    # -----------------------------
    # Helpers (local, no imports)
    # -----------------------------
    def _ensure_pstate(pid):
        if pid not in s.pstate:
            s.pstate[pid] = {"non_oom_fail": False}
        return s.pstate[pid]

    def _safe_str(x):
        try:
            return str(x)
        except Exception:
            return repr(x)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = _safe_str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _op_fingerprint(op):
        """Best-effort stable fingerprint for an operator across retries."""
        # Prefer explicit IDs if present; otherwise fall back to a string repr.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if v is not None:
                    return f"{attr}:{v}"
        return _safe_str(op)

    def _result_pipeline_id(r):
        """Best-effort extraction of pipeline_id from ExecutionResult."""
        if hasattr(r, "pipeline_id"):
            pid = getattr(r, "pipeline_id")
            if pid is not None:
                return pid
        # Sometimes ops may carry pipeline_id-like fields; try first op.
        ops = getattr(r, "ops", None)
        if ops:
            op0 = ops[0]
            for attr in ("pipeline_id", "dag_id", "workflow_id"):
                if hasattr(op0, attr):
                    pid = getattr(op0, attr)
                    if pid is not None:
                        return pid
        return None

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _has_hp_backlog():
        """Any runnable QUERY/INTERACTIVE waiting?"""
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.queues[pri]:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
                    continue
                if st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True):
                    return True
        return False

    def _pop_rr(pri):
        """Round-robin pop candidate pipeline; returns None if none usable."""
        q = s.queues[pri]
        if not q:
            return None

        n = len(q)
        start = s.rr_cursor[pri] % max(1, n)

        for k in range(n):
            idx = (start + k) % n
            p = q[idx]
            st = p.runtime_status()

            # Remove completed pipelines
            if st.is_pipeline_successful():
                q.pop(idx)
                if len(q) == 0:
                    s.rr_cursor[pri] = 0
                else:
                    s.rr_cursor[pri] = idx % len(q)
                return None  # state changed; let caller retry

            # Remove non-OOM failed pipelines (avoid retry loops)
            if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
                q.pop(idx)
                if len(q) == 0:
                    s.rr_cursor[pri] = 0
                else:
                    s.rr_cursor[pri] = idx % len(q)
                return None

            # Candidate found; advance cursor and return it (do not remove)
            s.rr_cursor[pri] = (idx + 1) % len(q)
            return p

        return None

    def _pool_preference(pri, pool_id, hp_backlog, num_pools):
        """Returns a boolean indicating whether pool_id is preferred for given priority."""
        if num_pools <= 1:
            return True
        if pri in (Priority.QUERY, Priority.INTERACTIVE):
            # Prefer pool 0, allow spillover if other pools idle (handled by caller scanning pools).
            return pool_id == 0
        # Batch: avoid pool 0 while HP backlog exists.
        if pool_id == 0 and hp_backlog:
            return False
        # Prefer non-0 pools for batch.
        return pool_id != 0

    def _base_cpu(pool, pri):
        # Keep HP small-ish to reduce queueing; batch larger for throughput.
        if pri == Priority.QUERY:
            return max(1.0, min(2.0, pool.max_cpu_pool * 0.25))
        if pri == Priority.INTERACTIVE:
            return max(1.0, min(4.0, pool.max_cpu_pool * 0.35))
        return max(1.0, min(pool.max_cpu_pool * 0.75, max(2.0, pool.max_cpu_pool * 0.50)))

    def _base_ram(pool, pri):
        # Avoid the prior policy's chronic RAM reservation bloat.
        if pri == Priority.QUERY:
            return max(1.0, pool.max_ram_pool * 0.06)
        if pri == Priority.INTERACTIVE:
            return max(1.0, pool.max_ram_pool * 0.10)
        return max(1.0, pool.max_ram_pool * 0.14)

    def _compute_request(pool, pri, op, avail_cpu, avail_ram):
        """Choose cpu/ram amounts for a single op, using RAM hints when available."""
        cpu_req = _base_cpu(pool, pri)
        ram_req = _base_ram(pool, pri) * s.global_ram_mult[pri]

        # Use per-op RAM hint (dominates base) if we have one.
        fp = _op_fingerprint(op)
        hint = s.op_ram_hint.get(fp)
        if hint is not None:
            # Hints are about avoiding OOM; keep them but don't explode to full-pool.
            ram_req = max(ram_req, float(hint["ram"]))

        # Batch RAM cap to reduce reservation-induced queueing.
        if pri == Priority.BATCH_PIPELINE:
            ram_req = min(ram_req, pool.max_ram_pool * 0.30)

        # Clamp to available/pool.
        cpu_req = max(1.0, min(cpu_req, avail_cpu, pool.max_cpu_pool))
        ram_req = max(1.0, min(ram_req, avail_ram, pool.max_ram_pool))
        return cpu_req, ram_req

    # -----------------------------
    # 1) Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        _ensure_pstate(p.pipeline_id)
        s.queues[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # -----------------------------
    # 2) Learn from results (OOM hints, non-OOM failure suppression)
    # -----------------------------
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        pri = getattr(r, "priority", None)
        err = getattr(r, "error", None)
        is_oom = _is_oom_error(err)

        # If we can map to pipeline_id, record non-OOM failure to stop infinite retries.
        pid = _result_pipeline_id(r)
        if pid is not None:
            pst = _ensure_pstate(pid)
            if not is_oom:
                pst["non_oom_fail"] = True

        # Update per-op RAM hint on OOM; otherwise do nothing.
        if is_oom:
            ops = getattr(r, "ops", None) or []
            # Use requested RAM from the failed run as a baseline (if present).
            prev_ram = getattr(r, "ram", None)
            prev_ram = float(prev_ram) if prev_ram is not None else 0.0

            for op in ops:
                fp = _op_fingerprint(op)
                cur = s.op_ram_hint.get(fp)
                cur_ram = float(cur["ram"]) if cur is not None else 0.0

                # Aggressive bump: double the larger of (current hint, previous allocation),
                # and add a small cushion.
                bumped = max(cur_ram, prev_ram, 1.0) * 2.0 + 1.0
                # Cap to something sane (still allows large ops but avoids runaway).
                bumped = min(bumped, 1e12)  # hard cap in case units are large; pool clamp will apply

                s.op_ram_hint[fp] = {"ram": bumped, "seen": (0 if cur is None else int(cur.get("seen", 0))) + 1}

            # If we cannot fingerprint ops (empty list), at least nudge global multiplier for that priority.
            if pri in s.global_ram_mult and not ops:
                s.global_ram_mult[pri] = min(s.global_ram_mult[pri] * 1.25, 16.0)

    # Also, lazily mark non-OOM failures for pipelines currently in queues if they are FAILED
    # and we didn't detect an OOM-like error (best-effort).
    for pri in _priority_order():
        for p in s.queues[pri]:
            st = p.runtime_status()
            if st.state_counts.get(OperatorState.FAILED, 0) > 0:
                # If we have *any* hint bumps for its ops, assume OOM-retry; otherwise treat as non-OOM.
                # (We cannot reliably attribute the failure type to this pipeline without pipeline_id in results.)
                any_hint = False
                try:
                    for op in st.get_ops([OperatorState.FAILED], require_parents_complete=False):
                        if s.op_ram_hint.get(_op_fingerprint(op)) is not None:
                            any_hint = True
                            break
                except Exception:
                    any_hint = False
                if not any_hint:
                    s.pstate[p.pipeline_id]["non_oom_fail"] = True

    # -----------------------------
    # 3) Place work per pool, using pool preferences
    # -----------------------------
    suspensions = []
    assignments = []

    hp_backlog = _has_hp_backlog()
    num_pools = s.executor.num_pools

    # We schedule in two passes:
    # - Pass A: put HP work on preferred pools first (pool 0).
    # - Pass B: fill remaining capacity with anything runnable, respecting batch-vs-HP bias.
    for pass_id in (0, 1):
        for pool_id in range(num_pools):
            pool = s.executor.pools[pool_id]
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                continue

            made_progress = True
            while made_progress:
                made_progress = False

                if avail_cpu <= 0 or avail_ram <= 0:
                    break

                chosen = None
                chosen_pri = None
                chosen_ops = None

                # Pass A: try HP only; Pass B: try all in priority order.
                pri_list = [Priority.QUERY, Priority.INTERACTIVE] if pass_id == 0 else _priority_order()

                for pri in pri_list:
                    # Enforce pool preference more strictly in pass A.
                    if pass_id == 0 and not _pool_preference(pri, pool_id, hp_backlog, num_pools):
                        continue
                    # In pass B, still bias but allow spillover if resources exist.
                    if pass_id == 1 and not _pool_preference(pri, pool_id, hp_backlog, num_pools):
                        # Allow spillover for HP into non-preferred pools if backlog exists and pool 0 is likely saturated.
                        if pri in (Priority.QUERY, Priority.INTERACTIVE) and hp_backlog:
                            pass
                        else:
                            continue

                    p = _pop_rr(pri)
                    if p is None:
                        continue

                    st = p.runtime_status()
                    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        continue

                    chosen = p
                    chosen_pri = pri
                    chosen_ops = op_list
                    break

                if chosen is None:
                    break

                op0 = chosen_ops[0]
                cpu_req, ram_req = _compute_request(pool, chosen_pri, op0, avail_cpu, avail_ram)

                # If we can't even fit minimal resources, stop for this pool.
                if cpu_req <= 0 or ram_req <= 0:
                    break

                # Soft protection: if HP backlog exists, don't let batch consume the last headroom in pool 0.
                if hp_backlog and num_pools > 1 and pool_id == 0 and chosen_pri == Priority.BATCH_PIPELINE:
                    # Keep at least a small reserve for HP on pool 0.
                    if (avail_cpu - cpu_req) < max(1.0, pool.max_cpu_pool * 0.15) or (avail_ram - ram_req) < max(1.0, pool.max_ram_pool * 0.15):
                        break

                assignments.append(
                    Assignment(
                        ops=chosen_ops,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=chosen.priority,
                        pool_id=pool_id,
                        pipeline_id=chosen.pipeline_id,
                    )
                )

                avail_cpu -= cpu_req
                avail_ram -= ram_req
                made_progress = True

    return suspensions, assignments
