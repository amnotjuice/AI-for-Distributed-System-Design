# policy_key: scheduler_iter_best_simple_013
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046099
# generation_seconds: 30.72
# generated_at: 2026-04-11T22:46:17.391255
@register_scheduler_init(key="scheduler_iter_best_simple_013")
def scheduler_iter_best_simple_013_init(s):
    """Priority-first, reservation-based scheduler to reduce weighted latency.

    Incremental improvements over the prior attempt:
      - Strict priority admission: never start BATCH if QUERY/INTERACTIVE backlog exists (within a pool).
      - Per-pool headroom reservation: keep CPU/RAM reserved for higher priorities to absorb bursts.
      - Smarter CPU sizing: give QUERY more CPU (finish faster), keep BATCH small (avoid interference).
      - OOM-aware RAM floor per operator: remember last failing RAM and retry above that.

    Intentionally still simple:
      - No explicit preemption (not enough stable visibility into running containers in the given API).
      - No runtime prediction beyond basic priority.
    """
    # Separate FIFO queues per priority (round-robin fairness within each).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator RAM floor guess keyed by operator object id from results/ops lists.
    # Value is the minimum RAM we should request next time to avoid repeated OOMs.
    s.op_ram_floor = {}

    # Track pipelines we've decided to stop retrying (non-OOM failures).
    s.dead_pipelines = set()

    # Small epsilon to avoid spinning on tiny fragments.
    s.eps = 1e-6


@register_scheduler(key="scheduler_iter_best_simple_013")
def scheduler_iter_best_simple_013_scheduler(s, results, pipelines):
    """Priority-aware scheduler with pool reservations and OOM-adaptive RAM floors."""
    # -----------------------
    # Helpers (no imports)
    # -----------------------
    def is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def enqueue(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def pipeline_is_done_or_dead(p) -> bool:
        if p.pipeline_id in s.dead_pipelines:
            return True
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def pop_next_runnable_from_queue(q):
        """Round-robin scan: return (pipeline, op_list) or (None, None)."""
        if not q:
            return None, None
        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            if pipeline_is_done_or_dead(p):
                continue
            st = p.runtime_status()
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                return p, op_list
            # Not runnable yet; rotate to back.
            q.append(p)
        return None, None

    def backlog_exists_for_higher_than(priority) -> bool:
        """Whether any runnable work exists at higher priority than `priority`."""
        if priority == Priority.BATCH_PIPELINE:
            # If either QUERY or INTERACTIVE has any runnable op, do not start new BATCH.
            for q in (s.q_query, s.q_interactive):
                p, ops = pop_next_runnable_from_queue(q)
                if p is not None:
                    # Put it back; we only peeked.
                    q.insert(0, p)
                    return True
            return False
        if priority == Priority.INTERACTIVE:
            p, ops = pop_next_runnable_from_queue(s.q_query)
            if p is not None:
                s.q_query.insert(0, p)
                return True
            return False
        return False  # QUERY has nothing above it

    def pick_next_runnable():
        """Strict priority: QUERY > INTERACTIVE > BATCH."""
        p, ops = pop_next_runnable_from_queue(s.q_query)
        if p is not None:
            return p, ops
        p, ops = pop_next_runnable_from_queue(s.q_interactive)
        if p is not None:
            return p, ops
        p, ops = pop_next_runnable_from_queue(s.q_batch)
        if p is not None:
            return p, ops
        return None, None

    def pool_reservations(priority, pool):
        """Return (reserve_cpu, reserve_ram) that must be kept free for higher/equal priorities."""
        # Reserve more for higher priorities to reduce weighted latency/tail spikes.
        if priority == Priority.QUERY:
            return 0.0, 0.0
        if priority == Priority.INTERACTIVE:
            return pool.max_cpu_pool * 0.20, pool.max_ram_pool * 0.20
        # BATCH: keep substantial headroom for bursty interactive/query arrivals.
        return pool.max_cpu_pool * 0.45, pool.max_ram_pool * 0.45

    def size_request(priority, pool, op, avail_cpu, avail_ram):
        """Heuristic sizing: favor QUERY completion, cap BATCH to avoid interference, honor RAM floor."""
        # CPU caps (try to complete high priority quickly, but avoid full monopolization).
        if priority == Priority.QUERY:
            cpu_cap = max(1.0, pool.max_cpu_pool * 0.55)
            ram_frac = 0.18
        elif priority == Priority.INTERACTIVE:
            cpu_cap = max(1.0, pool.max_cpu_pool * 0.40)
            ram_frac = 0.25
        else:
            cpu_cap = max(1.0, pool.max_cpu_pool * 0.20)
            ram_frac = 0.30

        cpu = min(avail_cpu, cpu_cap)

        # RAM request is a base fraction plus any learned floor from prior OOMs.
        base_ram = pool.max_ram_pool * ram_frac
        learned_floor = s.op_ram_floor.get(id(op), 0.0)
        ram = max(base_ram, learned_floor)
        ram = min(ram, avail_ram, pool.max_ram_pool)

        # Avoid tiny allocations.
        if cpu < 0.5:
            cpu = 0.0
        if ram < (pool.max_ram_pool * 0.02):
            ram = 0.0
        return cpu, ram

    # -----------------------
    # Ingest pipelines
    # -----------------------
    for p in pipelines:
        enqueue(p)

    # Early exit if no new information.
    if not pipelines and not results:
        return [], []

    # -----------------------
    # Process results: update RAM floors on OOM; stop retrying obvious non-OOM failures
    # -----------------------
    for r in results:
        if not r.failed():
            continue

        if is_oom_error(r.error):
            # If we OOM'd at RAM=r.ram, next time ask for materially more.
            # Use a multiplicative bump, capped by pool capacity at assignment time.
            bump = max(r.ram * 1.6, r.ram + 0.1)
            for op in (r.ops or []):
                k = id(op)
                cur = s.op_ram_floor.get(k, 0.0)
                if bump > cur:
                    s.op_ram_floor[k] = bump
        else:
            # Best-effort: if pipeline_id is present on result, mark dead.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    # -----------------------
    # Main scheduling loop: fill each pool while respecting reservations
    # -----------------------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Take a local snapshot and decrement as we plan assignments.
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        while avail_cpu > s.eps and avail_ram > s.eps:
            p, op_list = pick_next_runnable()
            if p is None:
                break

            # Do not start lower priority work if higher priority backlog exists (strict).
            if p.priority != Priority.QUERY and backlog_exists_for_higher_than(p.priority):
                # Put it back and stop scheduling lower prio into this pool this tick.
                enqueue(p)
                break

            reserve_cpu, reserve_ram = pool_reservations(p.priority, pool)
            usable_cpu = avail_cpu - reserve_cpu
            usable_ram = avail_ram - reserve_ram
            if usable_cpu <= 0.5 or usable_ram <= (pool.max_ram_pool * 0.02):
                # Not enough headroom beyond reservations; avoid filling with lower priority.
                enqueue(p)
                break

            op = op_list[0]
            cpu, ram = size_request(p.priority, pool, op, usable_cpu, usable_ram)

            if cpu <= s.eps or ram <= s.eps:
                enqueue(p)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Update local headroom, and re-enqueue for round-robin fairness.
            avail_cpu -= cpu
            avail_ram -= ram
            enqueue(p)

    return suspensions, assignments
