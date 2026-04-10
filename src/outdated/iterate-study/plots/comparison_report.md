# Comparison: two-shot-avg-low vs two-shot-avg-rich-low

Source policy baseline: **134.54** (scheduler_low_011.py)
Naive scheduler baseline: **12999.70**

Both groups: N=20 iterations, same source scheduler, model=gpt-5.2-2025-12-11, effort=low.
Difference: `avg-low` uses **minimal feedback** (single scalar), `avg-rich-low` uses **rich feedback** (per-trace SimulatorStats breakdown across 10 traces).

| Metric | avg-low | avg-rich-low |
|--------|---------|--------------|
| **Functional schedulers** | 14/20 | 18/20 |
| **Beats source policy** | 5/20 (25%) | 1/20 (5%) |
| **Failure modes** | 6× simulation_error | 1× simulation_error, 1× total_timeout |
| | | |
| *Adjusted latency — median* | 134.5 | 260.8 |
| *Adjusted latency — mean* | 191.1 | 2555.1 |
| *Adjusted latency — min* | 129.2 | 133.6 |
| *Adjusted latency — max* | 401.0 | 27201.8 |
| | | |
| *OOM count — total* | 8172 | 106055 |
| *OOM count — median/scheduler* | 0 | 1052 |
| *OOM count — max* | 5239 | 29015 |
| | | |
| *Completion rate — median* | 10.3% | 9.1% |
| *CR query — median* | 97.9% | 81.4% |
| *CR interactive — median* | 0.1% | 3.3% |
| *CR batch — median* | 0.0% | 0.0% |
| | | |
| *Latency query — median (s)* | 17.35s | 16.21s |
| *Latency interactive — median (s)* | 228.00s | 231.09s |
| | | |
| *RAM utilization — mean of means* | 0.2433 | 0.4434 |
| *Throughput — median (c/s)* | 0.4392 | 0.8602 |
| *Suspension rate — median* | 0.0000 | 0.0000 |

## Key Findings

1. **Rich feedback yields more functional schedulers** (18/20 vs 14/20) but far fewer that beat the source policy (1/20 vs 5/20).
2. **Rich feedback causes massive OOM**: total OOM 106,055 vs 8,172 — a 13× increase. The LLM uses the per-trace stats to allocate RAM more aggressively (closer to actual peak usage), leaving little headroom and triggering the OOM killer at scale.
3. **Query pipeline completion degrades under rich feedback**: median CR drops from 97.9% → 81.4%. Since query carries 10× weight in `adjusted_latency`, even a modest drop in query CR inflates the penalised latency substantially.
4. **Interactive pipelines improve slightly** under rich feedback (CR 0.14% → 3.3%), but this is not enough to offset the query CR degradation.
5. **Batch pipelines never complete** in either group (CR ≈ 0%). The scheduler concentrates all resources on query/interactive and starves batch entirely.
6. **Throughput is actually higher** for rich-low (0.86 vs 0.44 c/s), meaning more containers complete per second — but the OOM cascade kills query pipelines before they finish, so latency suffers despite higher raw throughput.
7. **Two catastrophic outliers** in rich-low (r11=27,201, r13=11,774) pull the mean to 2,555 vs median 260. These correspond to schedulers where query CR approaches 0, causing `adjusted_latency → ∞`.