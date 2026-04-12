"""Experiment 08: how many iterations to adapt a scheduler to a new scenario.

Fig 8 from the paper.
- Take a scheduler optimized for scenario A (from experiment 06)
- Transfer it to scenario B and iterate using scenario B's train trace
- Measure: latency vs iteration for each (A, B) pair

N_SCENARIOS must match experiment 06 so that both use the same strided sampling
(stride = len(scenarios.csv) // N_SCENARIOS). With 144 scenarios and N=10,
stride=14, giving one scenario per broad workload/arrival/resource combination:
  pos 0: batch-heavy / bursty  / balanced     / v1 / basic
  pos 1: batch-heavy / bursty  / cpu-bound    / v4 / basic
  pos 2: batch-heavy / steady  / balanced     / v3 / basic
  pos 3: batch-heavy / steady  / memory-bound / v2 / basic
  pos 4: canonical   / bursty  / cpu-bound    / v1 / basic
  pos 5: canonical   / bursty  / memory-bound / v4 / basic
  pos 6: canonical   / steady  / cpu-bound    / v3 / basic
  pos 7: priority-heavy / bursty  / balanced     / v2 / basic
  pos 8: priority-heavy / bursty  / memory-bound / v1 / basic
  pos 9: priority-heavy / steady  / balanced     / v4 / basic

PAIRS: (source_pos, target_pos) into the sampled list above.
"""

PARAM_OVERRIDES = {
    "per_trace_timeout": None,
}

N_SCENARIOS = 10   # must match experiment 06
N_ITERATIONS = 20
REASONING_EFFORT = "medium"
FEEDBACK_MODE = "rich"
MODEL = "gpt-5.2-2025-12-11"

# (source_pos, target_pos) into the strided-sampled scenario list
PAIRS = [
    (0, 7),   # batch-heavy/bursty/balanced   → priority-heavy/bursty/balanced   [workload shift]
    (7, 0),   # priority-heavy/bursty/balanced → batch-heavy/bursty/balanced      [workload shift reversed]
    (0, 4),   # batch-heavy/bursty/balanced   → canonical/bursty/cpu-bound        [workload + resource]
    (2, 8),   # batch-heavy/steady/balanced   → priority-heavy/bursty/memory-bound [workload + arrival + resource]
    (4, 1),   # canonical/bursty/cpu-bound    → batch-heavy/bursty/cpu-bound      [workload shift, same resource]
    (6, 9),   # canonical/steady/cpu-bound    → priority-heavy/steady/balanced    [workload + resource, same arrival]
]
