#!/usr/bin/env bash
# Workflow: generate 50 low-effort estimator-aware schedulers (v2 prompt),
# then evaluate under sigma=0.0 and sigma=0.5.
#
# Output layout (does NOT touch existing output/):
#   src/one-shot-est/schedulers-low/          ← generated schedulers
#   src/one-shot-est/output-low/sigma_0.0/    ← analysis results
#   src/one-shot-est/output-low/sigma_0.5/
#
# Usage:  bash src/one-shot-est/run_low_v2.sh

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

PYTHON=".venv/bin/python"
EST_DIR="src/one-shot-est"

echo "============================================"
echo "Step 1: Generate 50 schedulers (effort=low)"
echo "============================================"
$PYTHON "$EST_DIR/generate.py" --n 50 --effort low

echo ""
echo "============================================"
echo "Step 2: Analyze under sigma=0.0"
echo "============================================"
$PYTHON "$EST_DIR/analyze.py" \
    --scheduler-dir "$EST_DIR/schedulers-low" \
    --output-dir "$EST_DIR/output-low" \
    --condition sigma_0.0

echo ""
echo "============================================"
echo "Step 3: Analyze under sigma=0.5"
echo "============================================"
$PYTHON "$EST_DIR/analyze.py" \
    --scheduler-dir "$EST_DIR/schedulers-low" \
    --output-dir "$EST_DIR/output-low" \
    --condition sigma_0.5

echo ""
echo "============================================"
echo "DONE"
echo "============================================"
echo "Schedulers: $EST_DIR/schedulers-low/"
echo "Results:    $EST_DIR/output-low/sigma_0.0/analysis.jsonl"
echo "            $EST_DIR/output-low/sigma_0.5/analysis.jsonl"
