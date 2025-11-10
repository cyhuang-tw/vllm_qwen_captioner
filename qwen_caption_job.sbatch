#!/usr/bin/env bash
#SBATCH -J qwen-cap
#SBATCH -p gpuH200x8-interactive
#SBATCH --account=bbjs-delta-gpu
#SBATCH --gres=gpu:1
#SBATCH -c 8
#SBATCH --mem=96G
#SBATCH -t 1:00:00
#SBATCH -o logs/%x-%j.out

set -euo pipefail

# === parameters passed from run.sh ===
SIF_IMG="${SIF_IMG:?need SIF_IMG}"
AUDIO_SCP="${AUDIO_SCP:?need AUDIO_SCP}"
OUT_DIR="${OUT_DIR:-$PWD/out}"
PORT="${PORT:-8901}"
TP_SIZE="${TP_SIZE:-1}"
MAX_WORKERS="${MAX_WORKERS:-2000}"
MAX_QUEUE_SIZE="${MAX_QUEUE_SIZE:-999999}"
MODEL="Qwen/Qwen3-Omni-30B-A3B-Captioner"

mkdir -p "$OUT_DIR"
echo "=== Node Info ==="
echo "Host: $(hostname)"
echo "GPU(s): ${CUDA_VISIBLE_DEVICES:-unset}"
echo "Image: $SIF_IMG"
echo "Audio SCP: $AUDIO_SCP"
echo "OUT_DIR=$OUT_DIR"
echo "PORT=$PORT  TP_SIZE=$TP_SIZE  MAX_WORKERS=$MAX_WORKERS  MAX_QUEUE_SIZE=$MAX_QUEUE_SIZE"
echo "================="

# === define helper for apptainer ===
appt() {
  apptainer exec --cleanenv --nv \
    -B /u/chuang14 \
    -B /work/nvme/bbjs/chuang14 \
    -B /work/hdd/bbjs/chuang14 \
    -B /work/hdd/bbjs/shared \
    "$SIF_IMG" "$@"
}

# === step 0: ensure Python deps on host ===
python3 - <<'PY'
import sys, subprocess
for pkg in ["requests"]:
    try: __import__(pkg)
    except ImportError:
        subprocess.check_call([sys.executable,"-m","pip","install","--user",pkg])
print("Client deps OK")
PY

# === step 1: ensure qwen-omni-utils inside container ===
appt python3 - <<'PY'
import sys, subprocess
for pkg in ["qwen-omni-utils","soundfile"]:
    try: __import__(pkg.replace("-","_"))
    except Exception:
        subprocess.check_call([sys.executable,"-m","pip","install","--user","-U",pkg])
print("Server deps OK")
PY

# === step 2: launch vLLM server ===
# Force vLLM v0 API (stable, v1 is experimental and buggy in 0.9.3)
export VLLM_USE_V1=0

appt bash -lc "
  set -e
  export VLLM_USE_V1=0
  vllm serve ${MODEL} \
    --host 0.0.0.0 --port ${PORT} \
    --max-model-len 32768 \
    --gpu-memory-utilization 0.90 \
    --dtype auto \
    --limit-mm-per-prompt '{\"audio\":1}' \
    ${TP_SIZE:+--tensor-parallel-size ${TP_SIZE}}
" &
SERVER_PID=$!

cleanup() {
  echo "Stopping vLLM server (PID=$SERVER_PID)"
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# === step 3: wait until server ready ===
WAIT_MINUTES=20  # change this to whatever you like
echo "Waiting up to ${WAIT_MINUTES} minutes for vLLM server on port ${PORT}..."

python3 - <<PY
import time, urllib.request, sys, os
url=f"http://127.0.0.1:${PORT}/v1/models"
max_wait = int(os.environ.get("WAIT_MINUTES", "20")) * 60
deadline = time.time() + max_wait

while time.time() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            if r.status == 200:
                print("Server is up."); sys.exit(0)
    except Exception:
        pass
    time.sleep(2)

print(f"Server not reachable after {max_wait/60:.1f} minutes.", file=sys.stderr)
sys.exit(1)
PY

# === step 4: run caption client ===
python3 -u client_caption_wavscp.py \
  --scp "$AUDIO_SCP" \
  --base-url "http://127.0.0.1:${PORT}/v1" \
  --model "$MODEL" \
  --out-jsonl "$OUT_DIR/captions-${SLURM_JOB_ID}.jsonl" \
  --out-tsv "$OUT_DIR/captions-${SLURM_JOB_ID}.tsv" \
  --max-workers "$MAX_WORKERS" \
  --timeout 180 \
  --resume \
  --max-retries 3 \
  --checkpoint-interval 100 \
  --max-queue-size "$MAX_QUEUE_SIZE" \
  --queue-check-interval 2.0

echo "All done."

