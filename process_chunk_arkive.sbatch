#!/usr/bin/env bash
#SBATCH -J vllm-ark
#SBATCH -p gpuH200x8
#SBATCH --account=bbjs-delta-gpu
#SBATCH --gres=gpu:1
#SBATCH -c 8
#SBATCH --mem=128G
#SBATCH -t 24:00:00

set -euo pipefail

# Required env vars
SIF_IMG="${SIF_IMG:?ERROR: SIF_IMG not set}"
ARKDIR="${ARKDIR:?ERROR: ARKDIR not set}"
OUT_DIR="${OUT_DIR:?ERROR: OUT_DIR not set}"
PORT="${PORT:-8801}"
TP_SIZE="${TP_SIZE:-1}"
MAX_WORKERS="${MAX_WORKERS:-2000}"
MAX_TOKENS="${MAX_TOKENS:-200}"
TEMPERATURE="${TEMPERATURE:-0.2}"
RTF_SAMPLE_RATE="${RTF_SAMPLE_RATE:-0}"
START_IDX="${START_IDX:-0}"
END_IDX="${END_IDX:-}"
RUN_ID="${RUN_ID:-${SLURM_JOB_ID:-manual}}"
MODEL="Qwen/Qwen3-Omni-30B-A3B-Captioner"

mkdir -p "$OUT_DIR"

echo "========================================="
echo "=== Arkive Caption Processing Job ==="
echo "========================================="
echo "Node:           $(hostname)"
echo "Job ID:         ${SLURM_JOB_ID}"
echo "GPU(s):         ${CUDA_VISIBLE_DEVICES:-unset}"
echo "Image:          $SIF_IMG"
echo "Arkive Dir:     $ARKDIR"
echo "Output Dir:     $OUT_DIR"
echo "Port:           $PORT"
echo "TP Size:        $TP_SIZE"
echo "Max Workers:    $MAX_WORKERS"
echo "Max Tokens:     $MAX_TOKENS"
echo "Temperature:    $TEMPERATURE"
echo "RTF Sample:     $RTF_SAMPLE_RATE"
echo "Start Idx:      $START_IDX"
echo "End Idx:        ${END_IDX:-end}"
echo "Run ID:         $RUN_ID"
echo "========================================="

if [ ! -d "$ARKDIR" ]; then
    echo "ERROR: Arkive dir not found: $ARKDIR"
    exit 1
fi

appt() {
  apptainer exec --cleanenv --nv \
    -B /u/chuang14 \
    -B /work/nvme/bbjs/chuang14 \
    -B /work/hdd/bbjs/chuang14 \
    -B /work/hdd/bbjs/shared \
    "$SIF_IMG" "$@"
}

echo "[1/4] Installing client dependencies on host..."
python3 - <<'PY'
import sys, subprocess
pkgs = ["requests","pyarrow","numpy"]
try:
    import torch, torchaudio
    print("Using torch+torchaudio")
except Exception:
    pkgs.append("soundfile")
for pkg in pkgs:
    try:
        __import__(pkg.replace("-","_"))
    except Exception:
        print(f"Installing {pkg}...")
        subprocess.check_call([sys.executable,"-m","pip","install","--user",pkg])
print("Client dependencies OK")
PY
echo ""

echo "[2/4] Installing server dependencies in container..."
appt python3 - <<'PY'
import sys, subprocess
for pkg in ["qwen-omni-utils","soundfile"]:
    try:
        __import__(pkg.replace("-","_"))
    except Exception:
        print(f"Installing {pkg}...")
        subprocess.check_call([sys.executable,"-m","pip","install","--user","-U",pkg])
print("Server dependencies OK")
PY
echo ""

echo "[3/4] Starting vLLM server on port $PORT..."
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
echo "Server PID: $SERVER_PID"

cleanup() {
  echo "Cleaning up server PID=$SERVER_PID..."
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "Waiting for server readiness..."
python3 - <<PY
import time, urllib.request, sys
url = f"http://127.0.0.1:${PORT}/v1/models"
deadline = time.time() + 1200
while time.time() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            if r.status == 200:
                print("Server is ready!")
                sys.exit(0)
    except Exception:
        pass
    time.sleep(2)
print("ERROR: Server not reachable in time.", file=sys.stderr)
sys.exit(1)
PY

echo ""
echo "[4/4] Starting arkive client..."
echo ""

set +e
python3 -u client_caption_arkive.py \
  --arkdir "$ARKDIR" \
  --base-url "http://127.0.0.1:${PORT}/v1" \
  --model "$MODEL" \
  --out-jsonl "$OUT_DIR/captions-arkive-${RUN_ID}.jsonl" \
  --out-tsv "$OUT_DIR/captions-arkive-${RUN_ID}.tsv" \
  --max-workers "$MAX_WORKERS" \
  --max-tokens "$MAX_TOKENS" \
  --temperature "$TEMPERATURE" \
  --timeout 300 \
  --max-retries 3 \
  --checkpoint-interval 100 \
  --rtf-sample-rate "$RTF_SAMPLE_RATE" \
  --start-idx "$START_IDX" \
  ${END_IDX:+--end-idx "$END_IDX"} \
  --resume
EXIT_CODE=$?
set -e

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "========================================="
    echo "=== Processing Complete! ==="
    echo "========================================="
    echo "Output files:"
    echo "  $OUT_DIR/captions-arkive-${RUN_ID}.jsonl"
    echo "  $OUT_DIR/captions-arkive-${RUN_ID}.tsv"
    echo "========================================="
else
    echo "========================================="
    echo "ERROR: Client failed with exit code $EXIT_CODE"
    echo "========================================="
    echo "Partial results may be in:"
    echo "  $OUT_DIR/captions-arkive-${RUN_ID}.jsonl"
    echo "========================================="
    exit $EXIT_CODE
fi
