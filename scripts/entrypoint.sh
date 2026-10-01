#!/usr/bin/env bash
set -e

echo "=================================================="
echo "    Starting Marker Document Conversion Service   "
echo "=================================================="

# Check GPU availability
if command -v nvidia-smi &> /dev/null; then
    echo "[Info] NVIDIA GPU status:"
    nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader || true
else
    echo "[Warning] nvidia-smi not found in PATH."
fi

# Ensure cache directories exist
mkdir -p "${HF_HOME:-/data/cache/huggingface}"
mkdir -p "${TORCH_HOME:-/data/cache/torch}"
mkdir -p "${DATALAB_CACHE_DIR:-/data/cache/datalab}"
mkdir -p "${UPLOAD_DIRECTORY:-/tmp/marker_uploads}"

# Verify llama-server
if command -v llama-server &> /dev/null; then
    echo "[Info] llama-server binary detected at $(which llama-server)"
else
    echo "[Warning] llama-server not found in PATH; Surya may attempt fallback."
fi

echo "[Info] Launching FastAPI service on ${HOST:-0.0.0.0}:${PORT:-8090}..."
exec uvicorn server.app:app --host "${HOST:-0.0.0.0}" --port "${PORT:-8090}" --workers 1
