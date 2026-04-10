# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r11
@register_scheduler_init(key="scheduler_low_011_r11")
def scheduler_low_011_r11_init(s):
    """Iteration 2: Priority-aware, latency-focused scheduler that fixes obvious throughput/starvation issues.

    Key changes vs naive FIFO and the previous iteration:
    - Separate FIFO queues per priority, but *fair-share between QUERY and INTERACTIVE* (simple alternation)
      to avoid starving INTERACTIVE under sustained QUERY load.
    - Fill each pool with *multiple* assignments per tick (packing), instead of leaving idle CPU/RAM.
      This reduces queueing delay and improves latency under load.
    - Keep a small reserve of CPU/RAM when high-priority work exists, so BATCH can't consume the last headroom.
    - OOM-aware retries: if an op fails with OOM, retry it with increased RAM (exponential backoff) up to a cap.
    """
    collections = __import__("collections")
    s.deque = collections.deque

    s.queues = {
        Priority.QUERY: s.deque(),            # items: (enqueue_tick, Pipeline)
        Priority.INTERACTIVE: s.deque(),
        Priority.BATCH_PIPELINE: s.deque(),
    }

    s.tick = 0

    # Learned per-op RAM hints and last failure type (keyed by id(op) to match ExecutionResult.ops objects).
    s.op_ram_hint = {}       # op_id -> ram
    s.op_attempts = {}       # op_id -> retries attempted (only meaningful for OOM)
    s.op_last_error = {}     # op_id -> str(error)

    # Small, robust knobs
    s.max_retries_per_op = 4
    s.scan_limit = 64  # max items to scan per priority queue before giving up for this pool/iteration

    # Keep a small amount of headroom for high priority so BATCH doesn't block quick admission.
    s.reserve_cpu_frac = 0.15
    s.reserve_ram_frac = 0.15

    # Alternate between QUERY and INTERACTIVE when both have backlog.
    s.hp_rr = 0

    # If multiple pools exist, treat pool 0 as "latency" pool (only a mild preference via ordering below).
    s.interactive_pool_id = 0


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _get_op_min_ram(op):
    # Best-effort introspection across potential operator schemas.
    for attr in (
        "min_ram",
        "ram_min",
        "required_ram",
        "min_memory",
        "memory_min",
        "required_memory",
        "peak_ram",
        "peak_memory",
    ):
        v = getattr(op, attr, None)
        if v is None:
            continue
        try:
            fv = float(v)
            if fv > 0:
                return fv
        except Exception:
            pass

    # Sometimes nested under a dict-like "resources" or "stats"
    for container_attr in ("resources", "resource", "stats", "profile"):
        d = getattr(op, container_attr, None)
        if not isinstance(d, dict):
            continue
        for k in ("min_ram", "ram_min", "required_ram", "min_memory", "required_memory", "peak_ram"):
            if k in d:
                try:
                    fv = float(d[k])
                    if fv > 0:
                        return fv
                except Exception:
                    pass
    return None


def _get_next_assignable_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _cpu_request(s, pool, pr, rem_cpu, hp_backlog):
    # Small CPU quanta under load to improve concurrency and reduce queueing delay (latency).
    # Scale up a bit when high-priority backlog is small (reduce single-op runtime).
    max_cpu = float(pool.max_cpu_pool)
    rem_cpu = float(rem_cpu)

    if pr == Priority.BATCH_PIPELINE:
        base = 2.0
        # If no high-priority backlog, give batch more CPU to drain.
        if hp_backlog == 0:
            base = min(6.0, max(2.0, 0.4 * max_cpu))
        return max(1.0, min(base, rem_cpu))

    # QUERY / INTERACTIVE
    if hp_backlog <= max(1, s.executor.num_pools):
        # Light high-priority load: scale up to reduce runtime.
        base = min(8.0, max(4.0, 0.6 * max_cpu if pr == Priority.QUERY else 0.5 * max_cpu))
    else:
        # Heavy load: keep quanta modest to reduce tail from queueing.
        base = 4.0 if pr == Priority.QUERY else 3.0

    return max(1.0, min(base, rem_cpu))


def _ram_request(s, pool, pr, op, rem_ram):
    # Prefer using operator minimum RAM if available; otherwise use a conservative fraction of pool max.
    max_ram = float(pool.max_ram_pool)
    rem_ram = float(rem_ram)

    min_ram = _get_op_min_ram(op)
    if min_ram is not None:
        # Small headroom to avoid borderline OOM.
        base = min_ram * 1.10
    else:
        # Conservative defaults; OOM retries will inflate if needed.
        frac = 0.35 if pr in (Priority.QUERY, Priority.INTERACTIVE) else 0.30
        base = max(1.0, max_ram * frac)

    return max(1.0, min(base, rem_ram))


@register_scheduler(key="scheduler_low_011_r11")
def scheduler_low_011_r11(s, results, pipelines):
    """
    Priority-aware packing scheduler with fair-sharing between QUERY and INTERACTIVE.

    Main loop:
    - Enqueue new pipelines into per-priority FIFO queues (store enqueue tick for potential future aging).
    - Process results to learn OOM RAM hints (per op object) and allow retrying FAILED ops.
    - For each pool, repeatedly pick a pipeline/op that is ready and fits remaining resources, prioritizing:
        * QUERY / INTERACTIVE (alternating when both backlog) over BATCH
        * But allow BATCH to run when it doesn't consume reserved headroom needed for high priority.
    """
    s.tick += 1

    # Enqueue new arrivals
    for p in pipelines:
        pr = p.priority if p.priority in s.queues else Priority.BATCH_PIPELINE
        s.queues[pr].append((s.tick, p))

    # Learn from results (OOM -> increase RAM hint; success -> clear last_error)
    for r in results:
        ops = getattr(r, "ops", []) or []
        if not ops:
            continue

        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        if failed:
            err = getattr(r, "error", None)
            oom = _is_oom_error(err)
            for op in ops:
                oid = id(op)
                s.op_last_error[oid] = str(err)

                if oom:
                    prev_attempts = int(s.op_attempts.get(oid, 0))
                    s.op_attempts[oid] = prev_attempts + 1

                    # Inflate RAM hint using observed allocation as baseline when available.
                    baseline = s.op_ram_hint.get(oid, None)
                    if baseline is None:
                        try:
                            baseline = float(getattr(r, "ram", 0.0) or 0.0)
                        except Exception:
                            baseline = 0.0
                    if baseline <= 0:
                        baseline = 1.0

                    new_hint = max(1.0, float(baseline) * 2.0)
                    # Keep the maximum hint we've learned so far.
                    prev_hint = s.op_ram_hint.get(oid, 0.0) or 0.0
                    s.op_ram_hint[oid] = max(float(prev_hint), float(new_hint))
        else:
            # Success: clear last_error so we don't incorrectly treat future scheduling as a "failed op".
            for op in ops:
                oid = id(op)
                if oid in s.op_last_error:
                    del s.op_last_error[oid]
                if oid in s.op_attempts:
                    del s.op_attempts[oid]
                # Keep s.op_ram_hint as a stable learned "safe" size.

    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    def hp_backlog_len():
        return len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE])

    def pick_order_for_pool(pool_id, hp_backlog):
        # When high-priority exists, alternate QUERY and INTERACTIVE (and mildly prefer INTERACTIVE on pool 0).
        if hp_backlog > 0:
            if pool_id == getattr(s, "interactive_pool_id", 0):
                hp_first, hp_second = Priority.INTERACTIVE, Priority.QUERY
            else:
                if (s.hp_rr % 2) == 0:
                    hp_first, hp_second = Priority.QUERY, Priority.INTERACTIVE
                else:
                    hp_first, hp_second = Priority.INTERACTIVE, Priority.QUERY
            return [hp_first, hp_second, Priority.BATCH_PIPELINE]

        # If no high priority pending, drain batch first for throughput.
        return [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]

    def try_pick_from_queue(q, pr, pool, rem_cpu, rem_ram, hp_backlog, reserve_cpu, reserve_ram):
        """Scan within one priority queue for a ready op that fits in remaining resources."""
        if not q:
            return None

        scan = min(len(q), int(s.scan_limit))
        for _ in range(scan):
            enq_tick, p = q.popleft()

            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            op = _get_next_assignable_op(p)
            if op is None:
                # Not ready yet; keep it in FIFO order.
                q.append((enq_tick, p))
                continue

            oid = id(op)
            last_err = s.op_last_error.get(oid, None)
            if last_err is not None and (not _is_oom_error(last_err)):
                # Non-OOM failure: don't keep cycling this pipeline forever; drop it from scheduling.
                continue

            if last_err is not None and _is_oom_error(last_err):
                # OOM: retry only within budget.
                if int(s.op_attempts.get(oid, 0)) > int(s.max_retries_per_op):
                    continue

            cpu_req = _cpu_request(s, pool, pr, rem_cpu, hp_backlog)
            ram_req = _ram_request(s, pool, pr, op, rem_ram)

            # Apply learned RAM hint (cap by pool max and remaining).
            hint = s.op_ram_hint.get(oid, None)
            if hint is not None:
                try:
                    ram_req = max(float(ram_req), float(hint))
                except Exception:
                    pass
                ram_req = min(float(ram_req), float(pool.max_ram_pool), float(rem_ram))

            # If high-priority exists, avoid letting batch consume the last headroom.
            if pr == Priority.BATCH_PIPELINE and hp_backlog > 0:
                if (float(rem_cpu) - float(cpu_req)) < float(reserve_cpu) or (float(rem_ram) - float(ram_req)) < float(reserve_ram):
                    q.append((enq_tick, p))
                    continue

            # Fit check
            if float(cpu_req) <= float(rem_cpu) and float(ram_req) <= float(rem_ram):
                return (enq_tick, p, op, float(cpu_req), float(ram_req))

            # Doesn't fit; keep FIFO order.
            q.append((enq_tick, p))

        return None

    # Pack each pool with multiple assignments per tick (big latency win vs single-assignment-per-pool).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        rem_cpu = float(pool.avail_cpu_pool)
        rem_ram = float(pool.avail_ram_pool)
        if rem_cpu <= 0 or rem_ram <= 0:
            continue

        # Compute reservation only if we currently have high-priority backlog.
        hp_backlog = hp_backlog_len()
        reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_cpu_frac) if hp_backlog > 0 else 0.0
        reserve_ram = float(pool.max_ram_pool) * float(s.reserve_ram_frac) if hp_backlog > 0 else 0.0

        # Keep assigning while we can make progress and still have meaningful capacity.
        while rem_cpu >= 1.0 and rem_ram >= 1.0:
            hp_backlog = hp_backlog_len()
            order = pick_order_for_pool(pool_id, hp_backlog)

            picked = None
            for pr in order:
                q = s.queues.get(pr, None)
                if q is None or len(q) == 0:
                    continue
                picked = try_pick_from_queue(q, pr, pool, rem_cpu, rem_ram, hp_backlog, reserve_cpu, reserve_ram)
                if picked is not None:
                    enq_tick, p, op, cpu_req, ram_req = picked
                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu_req,
                            ram=ram_req,
                            priority=pr,
                            pool_id=pool_id,
                            pipeline_id=p.pipeline_id,
                        )
                    )
                    rem_cpu -= cpu_req
                    rem_ram -= ram_req

                    # Re-enqueue pipeline so it can make progress on subsequent ops.
                    q.append((enq_tick, p))

                    # Advance alternation only when we actually scheduled high-priority work.
                    if pr in (Priority.QUERY, Priority.INTERACTIVE):
                        s.hp_rr += 1
                    break

            if picked is None:
                break

    return suspensions, assignments