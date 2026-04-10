# Simulation Experiments

## Overview

Goal: evaluate scheduler quality faster without distorting the ranking. We tested 4 approaches: limiting time per trace, per tick, shorter simulation, and capping the queue.

## Experiments

| Experiment | Config | Rho | Speedup |
|---|---|---|---|
| **per_trace_timeout** | 60s wall-clock per trace | 1.000 | 2.3x |
| **per_tick_timeout** | 50ms per scheduler tick | 1.000 | 1.4x |
| **shorter_sim** | duration=200s | 0.62–0.80 | 2.2x |
| **max_outstanding** | queue cap=200 | 0.38–0.73 | 1.2x |

**Rho**: Spearman rank correlation vs baseline (1.0 = identical ranking).
