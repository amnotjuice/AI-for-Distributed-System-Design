#!/usr/bin/env bash
# Run all four iterate-study experiments sequentially.
#
# Experiments:
#   1. two-shot           (high effort, best scheduler, minimal feedback)
#   2. two-shot-rich      (high effort, best scheduler, rich feedback)
#   3. two-shot-avg-low   (low effort, avg scheduler, minimal feedback)
#   4. two-shot-avg-rich-low (low effort, avg scheduler, rich feedback)
#
# Usage: bash src/iterate-study/run_all.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=========================================="
echo "Experiment 1/4: two-shot (high, best, minimal)"
echo "=========================================="
bash "$SCRIPT_DIR/run_experiment.sh" \
    --effort high --selection best --feedback minimal --subdir two-shot

echo ""
echo "=========================================="
echo "Experiment 2/4: two-shot-rich (high, best, rich)"
echo "=========================================="
bash "$SCRIPT_DIR/run_experiment.sh" \
    --effort high --selection best --feedback rich --subdir two-shot-rich

echo ""
echo "=========================================="
echo "Experiment 3/4: two-shot-avg-low (low, avg, minimal)"
echo "=========================================="
bash "$SCRIPT_DIR/run_experiment.sh" \
    --effort low --selection avg --feedback minimal --subdir two-shot-avg-low

echo ""
echo "=========================================="
echo "Experiment 4/4: two-shot-avg-rich-low (low, avg, rich)"
echo "=========================================="
bash "$SCRIPT_DIR/run_experiment.sh" \
    --effort low --selection avg --feedback rich --subdir two-shot-avg-rich-low

echo ""
echo "=========================================="
echo "ALL DONE"
echo "=========================================="
