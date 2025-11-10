#!/usr/bin/env bash
set -euo pipefail

# ==== Start vLLM Server on Compute Node ====
# Use this when you're already on a compute node and want to start the server manually

# ==== Configuration ====
SIF_IMG="${SIF_IMG:-/work/hdd/bbjs/chuang14/apptainer/vllm_v2.sif}"
PORT="${PORT:-8901}"
TP_SIZE="${TP_SIZE:-1}"
MODEL="Qwen/Qwen3-Omni-30B-A3B-Captioner"

echo "=== Starting vLLM Server ==="
echo "Image: $SIF_IMG"
echo "Port: $PORT"
echo "Model: $MODEL"
echo "Tensor Parallel Size: $TP_SIZE"
echo "GPU(s): ${CUDA_VISIBLE_DEVICES:-all}"
echo "=========================="

# === define helper for apptainer ===
appt() {
  apptainer exec --cleanenv --nv \
    -B /u/chuang14 \
    -B /work/nvme/bbjs/chuang14 \
    -B /work/hdd/bbjs/chuang14 \
    -B /work/hdd/bbjs/shared \
    "$SIF_IMG" "$@"
}

# === step 1: install dependencies inside container ===
echo "Checking dependencies inside container..."
appt python3 - <<'PY'
import sys, subprocess
for pkg in ["qwen-omni-utils","soundfile"]:
    try: __import__(pkg.replace("-","_"))
    except Exception:
        subprocess.check_call([sys.executable,"-m","pip","install","--user","-U",pkg])
print("Server dependencies OK")
PY

# === step 2: launch vLLM server ===
echo ""
echo "Starting vLLM server on port $PORT..."
echo "Press Ctrl+C to stop the server"
echo ""

# Force vLLM v0 API (stable, v1 is experimental and buggy)
export VLLM_USE_V1=0

appt bash -lc "
  set -e
  export VLLM_USE_V1=0
  vllm serve ${MODEL} \
    --host 0.0.0.0 --port ${PORT} \
    --max-model-len 32768 \
    --max-num-seqs 512 \
    --gpu-memory-utilization 0.95 \
    --dtype auto \
    --limit-mm-per-prompt '{\"audio\":1}' \
    ${TP_SIZE:+--tensor-parallel-size ${TP_SIZE}}
"
