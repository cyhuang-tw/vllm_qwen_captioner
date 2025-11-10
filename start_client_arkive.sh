#!/usr/bin/env bash
set -euo pipefail

# ==== Start Arkive Client on Compute Node ====
# Use this when you're already on a compute node with a running vLLM server
# This client reads audio from the arkive format (memory-mapped binary files)

# ==== Configuration ====
ARKDIR="${ARKDIR:-/work/hdd/bbjs/shared/corpora/laion_audio_300m/arkive}"
OUT_DIR="${OUT_DIR:-$PWD/out}"
PORT="${PORT:-8901}"
MAX_WORKERS="${MAX_WORKERS:-2000}"
MAX_TOKENS="${MAX_TOKENS:-200}"  # lower = faster, higher captions but slower RTF
TEMPERATURE="${TEMPERATURE:-0.2}"
RTF_SAMPLE_RATE="${RTF_SAMPLE_RATE:-0.25}"  # 0 = disable per-sample decode, 0.1 = 10% sampling for RTF
MODEL="Qwen/Qwen3-Omni-30B-A3B-Captioner"
JOBID="${SLURM_JOB_ID:-manual-$(date +%s)}"
RUN_ID="${RUN_ID:-$JOBID}"  # Stable run identifier for resume; override to reuse outputs

# Optional: process a subset of the data
START_IDX="${START_IDX:-0}"
END_IDX="${END_IDX:-}"  # If empty, will process until the end

echo "=== Starting Arkive Client ==="
echo "Arkive Dir: $ARKDIR"
echo "Output Dir: $OUT_DIR"
echo "Server URL: http://127.0.0.1:${PORT}/v1"
echo "Max Workers: $MAX_WORKERS"
echo "Max Tokens: $MAX_TOKENS"
echo "Temperature: $TEMPERATURE"
echo "RTF Sample Rate: $RTF_SAMPLE_RATE"
echo "Job ID: $JOBID"
echo "Run ID: $RUN_ID"
if [[ -n "$END_IDX" ]]; then
    echo "Processing range: [$START_IDX, $END_IDX)"
else
    echo "Processing range: [$START_IDX, end)"
fi
echo "======================="

# Create output directory
mkdir -p "$OUT_DIR"

# === step 0: ensure Python deps on host ===
echo "Checking client dependencies..."
python3 - <<'PY'
import sys, subprocess
required_pkgs = ["requests", "pyarrow", "numpy"]

# Try torchaudio first
try:
    import torch
    import torchaudio
    print("Using torchaudio for audio processing")
except ImportError:
    # Fall back to soundfile
    required_pkgs.append("soundfile")
    print("Torchaudio not found, will use soundfile instead")

for pkg in required_pkgs:
    try:
        __import__(pkg.replace("-", "_"))
    except ImportError:
        print(f"Installing {pkg}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", pkg])

print("Client dependencies OK")
PY

# === step 1: wait for server (optional) ===
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

# === step 2: run arkive caption client ===
echo ""
echo "Starting arkive caption client..."
echo ""

# If resuming without explicit RUN_ID, try to pick the most recent existing run
if [[ "${RESUME:-no}" == "yes" ]]; then
    if [[ -z "${RUN_ID_OVERRIDE_SET:-}" ]]; then :; fi  # placeholder to avoid shellcheck
    if [[ -z "${RUN_ID_SET:-}" ]]; then :; fi
    # Detect whether user provided RUN_ID explicitly by comparing to default JOBID
    if [[ "$RUN_ID" == "$JOBID" ]]; then
        latest_jsonl=$(ls -t "$OUT_DIR"/captions-arkive-*.jsonl 2>/dev/null | head -n 1 || true)
        if [[ -n "$latest_jsonl" ]]; then
            base=$(basename "$latest_jsonl")
            RUN_ID="${base#captions-arkive-}"
            RUN_ID="${RUN_ID%.jsonl}"
            echo "Resume enabled: reusing latest run outputs with RUN_ID=$RUN_ID"
        else
            echo "Resume enabled but no previous outputs found in $OUT_DIR; starting a fresh run with RUN_ID=$RUN_ID"
        fi
    else
        echo "Resume enabled with user-provided RUN_ID=$RUN_ID"
    fi
fi

# Build command with optional arguments
CMD=(
    python3 -u client_caption_arkive.py
    --arkdir "$ARKDIR"
    --base-url "http://127.0.0.1:${PORT}/v1"
    --model "$MODEL"
    --out-jsonl "$OUT_DIR/captions-arkive-${RUN_ID}.jsonl"
    --out-tsv "$OUT_DIR/captions-arkive-${RUN_ID}.tsv"
    --max-workers "$MAX_WORKERS"
    --max-tokens "$MAX_TOKENS"
    --temperature "$TEMPERATURE"
    --timeout 300
    --max-retries 3
    --checkpoint-interval 100
    --rtf-sample-rate "$RTF_SAMPLE_RATE"
    --start-idx "$START_IDX"
)

# Add end-idx if specified
if [[ -n "$END_IDX" ]]; then
    CMD+=(--end-idx "$END_IDX")
fi

# Add resume flag if requested
if [[ "${RESUME:-no}" == "yes" ]]; then
    CMD+=(--resume)
fi

# Execute the command
"${CMD[@]}"

echo ""
echo "All done!"
echo "Output files:"
echo "  $OUT_DIR/captions-arkive-${RUN_ID}.jsonl"
echo "  $OUT_DIR/captions-arkive-${RUN_ID}.tsv"
