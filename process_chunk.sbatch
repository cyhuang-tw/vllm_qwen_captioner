#!/usr/bin/env bash
#SBATCH -J vllm-caption
#SBATCH -p gpuH200x8
#SBATCH --account=bbjs-delta-gpu
#SBATCH --gres=gpu:1
#SBATCH -c 8
#SBATCH --mem=128G
#SBATCH -t 24:00:00

set -euo pipefail

# ==== Process one SCP chunk: Start server + Run client ====
# This script is designed to be submitted by batch_submit.sh

# Required environment variables (passed via --export)
SIF_IMG="${SIF_IMG:?ERROR: SIF_IMG not set}"
AUDIO_SCP="${AUDIO_SCP:?ERROR: AUDIO_SCP not set}"
OUT_DIR="${OUT_DIR:?ERROR: OUT_DIR not set}"
PORT="${PORT:-8901}"
TP_SIZE="${TP_SIZE:-1}"
MAX_WORKERS="${MAX_WORKERS:-2000}"
MAX_QUEUE_SIZE="${MAX_QUEUE_SIZE:-999999}"
RUN_ID="${RUN_ID:-${SLURM_JOB_ID:-manual}}"
MODEL="Qwen/Qwen3-Omni-30B-A3B-Captioner"

# Create output directory
mkdir -p "$OUT_DIR"

echo "========================================="
echo "=== vLLM Caption Processing Job ==="
echo "========================================="
echo "Node:           $(hostname)"
echo "Job ID:         ${SLURM_JOB_ID}"
echo "GPU(s):         ${CUDA_VISIBLE_DEVICES:-unset}"
echo "Image:          $SIF_IMG"
echo "Audio SCP:      $AUDIO_SCP"
echo "Output Dir:     $OUT_DIR"
echo "Port:           $PORT"
echo "TP Size:        $TP_SIZE"
echo "Max Workers:    $MAX_WORKERS"
echo "Max Queue:      $MAX_QUEUE_SIZE"
echo "Run ID:         $RUN_ID"
echo "========================================="

# Check if SCP file exists
if [ ! -f "$AUDIO_SCP" ]; then
    echo "ERROR: SCP file not found: $AUDIO_SCP"
    exit 1
fi

echo "SCP file contains $(wc -l < "$AUDIO_SCP") lines"
echo ""

# === Define apptainer helper ===
appt() {
  apptainer exec --cleanenv --nv \
    -B /u/chuang14 \
    -B /work/nvme/bbjs/chuang14 \
    -B /work/hdd/bbjs/chuang14 \
    -B /work/hdd/bbjs/shared \
    "$SIF_IMG" "$@"
}

# === Step 1: Install client dependencies on host ===
echo "[1/4] Installing client dependencies..."
python3 - <<'PY'
import sys, subprocess
for pkg in ["requests"]:
    try:
        __import__(pkg)
    except ImportError:
        print(f"Installing {pkg}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", pkg])
print("Client dependencies OK")
PY
echo ""

# === Step 2: Install server dependencies in container ===
echo "[2/4] Installing server dependencies in container..."
appt python3 - <<'PY'
import sys, subprocess
for pkg in ["qwen-omni-utils", "soundfile"]:
    try:
        __import__(pkg.replace("-", "_"))
    except Exception:
        print(f"Installing {pkg}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", "-U", pkg])
print("Server dependencies OK")
PY
echo ""

# === Step 3: Start vLLM server in background ===
echo "[3/4] Starting vLLM server on port $PORT..."

# Force vLLM v0 API (stable)
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
" &

SERVER_PID=$!
echo "Server started with PID: $SERVER_PID"

# Setup cleanup trap
cleanup() {
  echo ""
  echo "Cleaning up: Stopping vLLM server (PID=$SERVER_PID)..."
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  echo "Server stopped."
}
trap cleanup EXIT INT TERM

# Wait for server to be ready
echo "Waiting for server to be ready (max 10 minutes)..."
python3 - <<PY
import time, urllib.request, sys
url = f"http://127.0.0.1:${PORT}/v1/models"
max_wait = 1200  # 20 minutes
deadline = time.time() + max_wait

while time.time() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            if r.status == 200:
                print("Server is ready!")
                sys.exit(0)
    except Exception:
        pass
    time.sleep(2)

print(f"ERROR: Server not reachable after {max_wait/60:.1f} minutes.", file=sys.stderr)
sys.exit(1)
PY

echo ""
echo "[4/4] Starting caption client..."
echo "Processing: $AUDIO_SCP"
echo ""

# === Step 4: Run caption client ===
# Temporarily disable exit-on-error to capture the exit code
set +e
python3 -u client_caption_wavscp.py \
  --scp "$AUDIO_SCP" \
  --base-url "http://127.0.0.1:${PORT}/v1" \
  --model "$MODEL" \
  --out-jsonl "$OUT_DIR/captions-${RUN_ID}.jsonl" \
  --out-tsv "$OUT_DIR/captions-${RUN_ID}.tsv" \
  --max-workers "$MAX_WORKERS" \
  --timeout 300 \
  --resume \
  --max-retries 3 \
  --checkpoint-interval 100 \
  --max-queue-size "$MAX_QUEUE_SIZE" \
  --queue-check-interval 2.0

EXIT_CODE=$?
set -e  # Re-enable exit-on-error

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "========================================="
    echo "=== Processing Complete! ==="
    echo "========================================="
    echo "Output files:"
    echo "  $OUT_DIR/captions-${RUN_ID}.jsonl"
    echo "  $OUT_DIR/captions-${RUN_ID}.tsv"
    echo "========================================="
else
    echo "========================================="
    echo "ERROR: Client failed with exit code $EXIT_CODE"
    echo "========================================="
    echo "Check the output above for error details."
    echo "Partial results may be in:"
    echo "  $OUT_DIR/captions-${RUN_ID}.jsonl"
    echo "========================================="
    exit $EXIT_CODE
fi
