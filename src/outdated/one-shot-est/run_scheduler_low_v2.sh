#!/usr/bin/env bash
# Workflow: generate 20 low-effort estimator-aware schedulers (v2 prompt),
# then evaluate under sigma=0.0 and sigma=0.5.
#
# Output layout (does NOT touch existing output/):
#   src/one-shot-est/schedulers-low-v2/            <- generated schedulers
#   src/one-shot-est/output-low-v2/sigma_0.0/      <- analysis results
#   src/one-shot-est/output-low-v2/sigma_0.5/
#
# Usage: bash src/one-shot-est/run_scheduler_low_v2.sh

set -euo pipefail
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

PYTHON="${PYTHON:-.venv/bin/python}"
EST_DIR="src/one-shot-est"
SCHEDULER_DIR="$REPO_ROOT/$EST_DIR/schedulers-low-v2"
OUTPUT_DIR="$REPO_ROOT/$EST_DIR/output-low-v2"
NUM_SAMPLES="${NUM_SAMPLES:-20}"

echo "============================================"
echo "Step 1: Generate ${NUM_SAMPLES} schedulers (effort=low)"
echo "============================================"
"$PYTHON" "$EST_DIR/generate.py" \
    --n "$NUM_SAMPLES" \
    --effort low \
    --scheduler-dir "$SCHEDULER_DIR"

echo ""
echo "============================================"
echo "Step 2: Analyze under sigma=0.0"
echo "============================================"
"$PYTHON" "$EST_DIR/analyze.py" \
    --scheduler-dir "$SCHEDULER_DIR" \
    --output-dir "$OUTPUT_DIR" \
    --condition sigma_0.0

echo ""
echo "============================================"
echo "Step 3: Analyze under sigma=0.5"
echo "============================================"
"$PYTHON" "$EST_DIR/analyze.py" \
    --scheduler-dir "$SCHEDULER_DIR" \
    --output-dir "$OUTPUT_DIR" \
    --condition sigma_0.5

echo ""
echo "============================================"
echo "DONE"
echo "============================================"
echo "Schedulers: $SCHEDULER_DIR/"
echo "Results:    $OUTPUT_DIR/sigma_0.0/analysis.jsonl"
echo "            $OUTPUT_DIR/sigma_0.5/analysis.jsonl"
