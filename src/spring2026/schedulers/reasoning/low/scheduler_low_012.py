# policy_key: scheduler_low_012
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043595
# generation_seconds: 50.79
# generated_at: 2026-03-12T21:43:33.974832
@register_scheduler_init(key="scheduler_low_012")
def scheduler_low_012_init(s):
    """Priority-aware FIFO with soft reservations + simple OOM-based RAM backoff.

    Goals (small, obvious improvements over naive FIFO):
      1) Reduce latency for high-priority pipelines by always selecting them first.
      2) Avoid batch starvation of interactive work by keeping a soft resource reserve.
      3) Reduce repeated OOMs by tracking per-operator RAM needs and backing off on OOM.

    Notes:
      - No preemption: the minimal public interface shown doesn't expose a reliable way
        to enumerate running containers for targeted suspension.
      - We schedule 1 ready op per assignment, but allow multiple assignments per pool
        per tick if resources allow.
    """
    s.tick = 0

    # A single waiting list of pipeline_ids preserves FIFO arrival order globally,
    # while selection is priority-aware at scheduling time.
    s.waiting_order = []          # List[pipeline_id]
    s.waiting_pipelines = {}      # pipeline_id -> Pipeline
    s.enqueue_tick = {}           # pipeline_id -> tick enqueued (for optional aging)

    # Per-operator RAM estimate that increases after OOM failures.
    # Keyed by (pipeline_id, op_repr) to avoid collisions and keep it simple.
    s.op_ram_est = {}             # (pipeline_id, op_key) -> ram_estimate

    # Remember last known pipeline priority for convenience.
    s.pipeline_priority = {}      # pipeline_id -> Priority


@register_scheduler(key="scheduler_low_012")
def scheduler_low_012(s, results, pipelines):
    """
    Scheduler loop:
      - Ingest new pipelines into a FIFO-ordered waiting structure.
      - Update RAM estimates based on OOM failures from execution results.
      - For each pool, repeatedly assign ready ops:
          * Always prefer higher priority.
          * Batch uses only "non-reserved" headroom (soft reserve) to protect latency.
          * CPU/RAM sizing is modest by default; higher priority gets more CPU.
    """
    s.tick += 1

    # ----------------------------
    # Helpers (local, no imports)
    # ----------------------------
    def _prio_rank(prio):
        # Smaller is higher priority.
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE and anything else

    def _is_oom_error(err):
        if err is None:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "killed" in e)

    def _op_key(op):
        # We don't know stable op identifiers; repr/str is the safest available handle.
        try:
            return str(op)
        except Exception:
            return repr(op)

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any op failed, we treat pipeline as failed and don't retry indefinitely.
        # (If the platform wants retries, this policy can be extended later.)
        has_failures = st.state_counts[OperatorState.FAILED] > 0
        return has_failures

    def _first_ready_op(p):
        st = p.runtime_status()
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        return op_list

    def _pick_next_pipeline(eligible_pipeline_ids):
        # Choose the next pipeline among eligible by:
        #   1) highest priority (lowest rank)
        #   2) FIFO within same priority (by waiting_order scanning)
        best_idx = None
        best_rank = None
        for idx, pid in enumerate(s.waiting_order):
            if pid not in eligible_pipeline_ids:
                continue
            p = s.waiting_pipelines.get(pid)
            if p is None:
                continue
            pr = _prio_rank(p.priority)
            if best_rank is None or pr < best_rank:
                best_rank = pr
                best_idx = idx
                if best_rank == 0:
                    # Can't beat QUERY; still keep scanning? Not necessary for FIFO tie,
                    # because we're already scanning in order and first QUERY wins.
                    break
        if best_idx is None:
            return None
        pid = s.waiting_order.pop(best_idx)
        return s.waiting_pipelines.get(pid)

    def _default_cpu_for_priority(pool, prio, avail_cpu):
        # High priority gets more CPU to minimize latency.
        # Keep it bounded so we can still place multiple ops when possible.
        max_cpu = pool.max_cpu_pool
        if prio == Priority.QUERY:
            target = max(1.0, 0.75 * max_cpu)
        elif prio == Priority.INTERACTIVE:
            target = max(1.0, 0.60 * max_cpu)
        else:
            target = max(1.0, 0.40 * max_cpu)
        return min(avail_cpu, target)

    def _default_ram_for_priority(pool, prio, avail_ram):
        max_ram = pool.max_ram_pool
        if prio == Priority.QUERY:
            target = max(1.0, 0.30 * max_ram)
        elif prio == Priority.INTERACTIVE:
            target = max(1.0, 0.25 * max_ram)
        else:
            target = max(1.0, 0.20 * max_ram)
        return min(avail_ram, target)

    # ---------------------------------
    # Ingest new pipelines (FIFO order)
    # ---------------------------------
    for p in pipelines:
        pid = p.pipeline_id
        # Avoid double-enqueue if the generator repeats references.
        if pid not in s.waiting_pipelines:
            s.waiting_pipelines[pid] = p
            s.waiting_order.append(pid)
            s.enqueue_tick[pid] = s.tick
            s.pipeline_priority[pid] = p.priority
        else:
            # Refresh reference/priority if needed.
            s.waiting_pipelines[pid] = p
            s.pipeline_priority[pid] = p.priority

    # ---------------------------------
    # Update RAM estimates from results
    # ---------------------------------
    for r in results:
        # Only treat failures; on success we don't shrink (keep conservative).
        if not r.failed():
            continue
        if not _is_oom_error(r.error):
            continue

        # We assume r.ops refers to the op(s) attempted. We'll update each.
        try:
            ops = list(r.ops) if r.ops is not None else []
        except Exception:
            ops = []

        # Backoff: double the RAM for the failed op(s), using the attempted RAM as base.
        for op in ops:
            opk = _op_key(op)
            # We may not know the pipeline_id reliably from result; use a best-effort field.
            # If not available, fall back to a global key.
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                pid = "_unknown_pipeline"
            key = (pid, opk)
            prev = s.op_ram_est.get(key, None)
            base = float(getattr(r, "ram", 0) or 0)
            if base <= 0:
                base = prev if prev is not None else 1.0
            new_est = max(base * 2.0, (prev * 1.5) if prev is not None else 0.0, 1.0)
            s.op_ram_est[key] = new_est

    # Early exit if no new info came in; preserve determinism and reduce overhead.
    if not pipelines and not results:
        return [], []

    # ---------------------------------
    # Drop completed/failed pipelines
    # ---------------------------------
    # We compact waiting_order lazily while scheduling; here we also remove definitely done.
    to_remove = set()
    for pid, p in list(s.waiting_pipelines.items()):
        if _pipeline_done_or_failed(p):
            to_remove.add(pid)
            del s.waiting_pipelines[pid]
            s.enqueue_tick.pop(pid, None)
            s.pipeline_priority.pop(pid, None)

    if to_remove:
        s.waiting_order = [pid for pid in s.waiting_order if pid not in to_remove]

    # ----------------------------
    # Scheduling decisions
    # ----------------------------
    suspensions = []
    assignments = []

    # Soft reservations to protect latency:
    # - Batch can only consume up to (avail - reserve).
    # - High priority can consume reserved resources if present.
    # Keep reserve small to avoid starving batch.
    reserve_cpu_frac = 0.25
    reserve_ram_frac = 0.25

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        reserve_cpu = reserve_cpu_frac * float(pool.max_cpu_pool)
        reserve_ram = reserve_ram_frac * float(pool.max_ram_pool)

        # Build eligible pipeline set: those still waiting and with at least one ready op.
        eligible = set()
        for pid in s.waiting_order:
            p = s.waiting_pipelines.get(pid)
            if p is None:
                continue
            if _pipeline_done_or_failed(p):
                continue
            if _first_ready_op(p):
                eligible.add(pid)

        # Keep placing ops while the pool has resources and we find work.
        # Hard stop to avoid infinite loops on pathological cases.
        max_placements = 64
        placements = 0

        while placements < max_placements and avail_cpu > 0 and avail_ram > 0 and eligible:
            p = _pick_next_pipeline(eligible)
            if p is None:
                break

            pid = p.pipeline_id
            prio = p.priority
            op_list = _first_ready_op(p)

            # If not actually ready anymore, requeue and continue.
            if not op_list:
                # Put back to preserve FIFO ordering.
                if pid in s.waiting_pipelines:
                    s.waiting_order.append(pid)
                eligible.discard(pid)
                continue

            # Determine batch headroom (soft reservation).
            # For batch, we won't schedule if only reserved resources are left.
            cpu_headroom = avail_cpu
            ram_headroom = avail_ram
            if prio == Priority.BATCH_PIPELINE:
                cpu_headroom = max(0.0, avail_cpu - reserve_cpu)
                ram_headroom = max(0.0, avail_ram - reserve_ram)

            if cpu_headroom <= 0 or ram_headroom <= 0:
                # Can't place this priority right now; requeue and try others.
                if pid in s.waiting_pipelines:
                    s.waiting_order.append(pid)
                eligible.discard(pid)
                continue

            # CPU sizing
            cpu_req = _default_cpu_for_priority(pool, prio, cpu_headroom)

            # RAM sizing with OOM-based backoff
            opk = _op_key(op_list[0])
            # Prefer result-linked estimates if the simulator exposes pipeline_id on results;
            # otherwise use this pipeline_id-based key for estimates going forward.
            est_key = (pid, opk)
            ram_est = s.op_ram_est.get(est_key, None)
            if ram_est is None:
                ram_req = _default_ram_for_priority(pool, prio, ram_headroom)
            else:
                # Use the estimate but don't take the entire pool; keep bounded.
                ram_req = min(ram_headroom, max(1.0, float(ram_est)))

            # Final guardrails (never request more than available headroom)
            cpu_req = min(cpu_headroom, max(1.0, float(cpu_req)))
            ram_req = min(ram_headroom, max(1.0, float(ram_req)))

            # Create assignment
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Update local availability and requeue pipeline to allow fair round-robin
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            if pid in s.waiting_pipelines:
                s.waiting_order.append(pid)

            # Recompute eligibility for this pipeline (it may still have more ready ops,
            # but we keep 1-op granularity and round-robin via requeue).
            # We'll keep it eligible if still ready, otherwise drop from eligible set.
            if _first_ready_op(p):
                eligible.add(pid)
            else:
                eligible.discard(pid)

            placements += 1

    return suspensions, assignments
