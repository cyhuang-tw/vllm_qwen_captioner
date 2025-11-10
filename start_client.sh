#!/usr/bin/env bash
set -euo pipefail

# ==== Start Client on Compute Node ====
# Use this when you're already on a compute node with a running vLLM server

# ==== Configuration ====
AUDIO_SCP="${AUDIO_SCP:-$PWD/large_wav.scp}"
# AUDIO_SCP="test_audio.scp"
OUT_DIR="${OUT_DIR:-$PWD/out}"
PORT="${PORT:-8901}"
MAX_WORKERS="${MAX_WORKERS:-2000}"
MAX_QUEUE_SIZE="${MAX_QUEUE_SIZE:-999999}"
MODEL="Qwen/Qwen3-Omni-30B-A3B-Captioner"
JOBID="${SLURM_JOB_ID:-manual-$(date +%s)}"

echo "=== Starting Client ==="
echo "Audio SCP: $AUDIO_SCP"
echo "Output Dir: $OUT_DIR"
echo "Server URL: http://127.0.0.1:${PORT}/v1"
echo "Max Workers: $MAX_WORKERS"
echo "Max Queue Size: $MAX_QUEUE_SIZE"
echo "Job ID: $JOBID"
echo "======================="

# Create output directory
mkdir -p "$OUT_DIR"

# === step 0: ensure Python deps on host ===
echo "Checking client dependencies..."
python3 - <<'PY'
import sys, subprocess
for pkg in ["requests"]:
    try: __import__(pkg)
    except ImportError:
        subprocess.check_call([sys.executable,"-m","pip","install","--user",pkg])
print("Client dependencies OK")
PY

# === step 1: wait for server (optional, you can skip this if server is already running) ===
WAIT_FOR_SERVER="${WAIT_FOR_SERVER:-yes}"
if [[ "$WAIT_FOR_SERVER" == "yes" ]]; then
    echo ""
    echo "Waiting for vLLM server on port ${PORT}..."
    python3 - <<PY
import time, urllib.request, sys
url=f"http://127.0.0.1:${PORT}/v1/models"
max_wait = 300  # 5 minutes
deadline = time.time() + max_wait

while time.time() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            if r.status == 200:
                print("Server is up!"); sys.exit(0)
    except Exception:
        pass
    time.sleep(2)

print(f"Server not reachable after {max_wait/60:.1f} minutes.", file=sys.stderr)
sys.exit(1)
PY
fi

# === step 2: run caption client ===
echo ""
echo "Starting caption client..."
echo ""

python3 -u client_caption_wavscp.py \
  --scp "$AUDIO_SCP" \
  --base-url "http://127.0.0.1:${PORT}/v1" \
  --model "$MODEL" \
  --out-jsonl "$OUT_DIR/captions-${JOBID}.jsonl" \
  --out-tsv "$OUT_DIR/captions-${JOBID}.tsv" \
  --max-workers "$MAX_WORKERS" \
  --timeout 300 \
  --max-retries 3 \
  --checkpoint-interval 100 \
  --max-queue-size "$MAX_QUEUE_SIZE" \
  --queue-check-interval 2.0
# --resume

echo ""
echo "All done!"
echo "Output files:"
echo "  $OUT_DIR/captions-${JOBID}.jsonl"
echo "  $OUT_DIR/captions-${JOBID}.tsv"
