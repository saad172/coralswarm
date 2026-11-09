#!/usr/bin/env bash
# Download bge-small-en-v1.5 ONNX model and tokenizer from HuggingFace.
#
# Usage:
#   bash scripts/download_model.sh
#
# Files are placed in ./models/ relative to the repo root (or the working
# directory from which this script is run).  Set MODELS_DIR to override.
set -euo pipefail

MODELS_DIR="${MODELS_DIR:-models}"
HF_BASE="https://huggingface.co/BAAI/bge-small-en-v1.5/resolve/main"

mkdir -p "$MODELS_DIR"

echo "Downloading bge-small-en-v1.5 ONNX model (~33 MB)..."
curl -L --progress-bar \
  "${HF_BASE}/onnx/model.onnx" \
  -o "${MODELS_DIR}/bge-small-en-v1.5.onnx"

echo "Downloading tokenizer.json..."
curl -L --progress-bar \
  "${HF_BASE}/tokenizer.json" \
  -o "${MODELS_DIR}/bge-small-en-v1.5-tokenizer.json"

echo "Models downloaded to ${MODELS_DIR}/:"
ls -lh "${MODELS_DIR}/bge-small-en-v1.5"*
echo "Done."
