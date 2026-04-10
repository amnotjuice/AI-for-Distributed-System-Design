#!/usr/bin/env bash
# Workflow: generate 20 low-effort estimator-aware schedulers (v3 prompt, aggressive estimate usage),
# then evaluate under sigma=0.0 and sigma=0.5.
#
# Output layout:
#   src/one-shot-est/schedulers-low-v3/           ← generated schedulers
#   src/one-shot-est/output-low-v3/sigma_0.0/     ← analysis results
#   src/one-shot-est/output-low-v3/sigma_0.5/
#
# Usage:  bash src/one-shot-est/run_low_v3.sh

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

PYTHON=".venv/bin/python"
EST_DIR="src/one-shot-est"

echo "============================================"
echo "Step 1: Generate 20 schedulers (effort=low)"
echo "============================================"
$PYTHON "$EST_DIR/generate.py" --n 20 --effort low --scheduler-dir "$(pwd)/$EST_DIR/schedulers-low-v3"

echo ""
echo "============================================"
echo "Step 2: Analyze under sigma=0.0"
echo "============================================"
$PYTHON "$EST_DIR/analyze.py" \
    --scheduler-dir "$EST_DIR/schedulers-low-v3" \
    --output-dir "$EST_DIR/output-low-v3" \
    --condition sigma_0.0

echo ""
echo "============================================"
echo "Step 3: Analyze under sigma=0.5"
echo "============================================"
$PYTHON "$EST_DIR/analyze.py" \
    --scheduler-dir "$EST_DIR/schedulers-low-v3" \
    --output-dir "$EST_DIR/output-low-v3" \
    --condition sigma_0.5

echo ""
echo "============================================"
echo "DONE"
echo "============================================"
echo "Schedulers: $EST_DIR/schedulers-low-v3/"
echo "Results:    $EST_DIR/output-low-v3/sigma_0.0/analysis.jsonl"
echo "            $EST_DIR/output-low-v3/sigma_0.5/analysis.jsonl"
