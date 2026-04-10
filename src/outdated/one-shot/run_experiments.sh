#!/bin/bash
# Run all experiments across all effort levels.
# Usage: ./run_experiments.sh

set -e

PYTHON="/Users/juice/Desktop/wiscurd/AI-for-Distributed-System-Design/.venv/bin/python"
EFFORTS=("schedulers-none" "schedulers-low" "schedulers-medium" "schedulers-high")
EXPERIMENTS=("per_tick_timeout" "shorter_sim" "max_outstanding")

for exp in "${EXPERIMENTS[@]}"; do
    echo "============================================"
    echo "EXPERIMENT: $exp"
    echo "============================================"
    for effort in "${EFFORTS[@]}"; do
        echo "--- $exp / $effort ---"
        $PYTHON analyze.py "$effort/" --experiment "$exp"
        echo ""
    done
done

echo "ALL DONE"
